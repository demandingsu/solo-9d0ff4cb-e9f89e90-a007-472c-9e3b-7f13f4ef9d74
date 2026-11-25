"""业务逻辑。

关键设计
--------
完整路径 = 工时 -> 分派(同工单) -> 归科(同项目) -> 预算科目。

路径核算只用一条集合式 GROUP BY SQL 完成，服务端不逐条展开路径
（不按工时逐行 JOIN、不在 Python 里追链），同科目不同路径仍按不同路径计数。
归科侧只连接当月 ``active`` 版本的整表，准备中的 staging 与已废弃 discarded
版本物理上不参与供数和纠错。

归科整月覆盖（两阶段，避免长占 SQLite 写锁）
--------------------------------------------
1. 准备：创建 staging 版本，按固定小块（CHUNK_SIZE 行）分批写入，
   每个小块独立短事务、块间释放写锁——其他月份的录入/纠错可以在块间提交；
2. 发布：单个短事务只做固定几行状态翻转（旧 active -> discarded、
   staging -> active、epoch + 1），写入量与批行数无关；
3. 乐观并发：准备时读取 month_meta.attr_epoch，发布时在写事务内复核，
   期间同月任何归科追加/覆盖/纠错撤除/整月清空都会推进 epoch，
   本批覆盖即退回（409）并废弃 staging，已提交改动原样保留；
4. 失败/重启：staging 永不生效，启动时统一清理，当月可重新覆盖。

纠错在单条 BEGIN IMMEDIATE 事务内完成：撤除前先读已提交快照做校验，
撤除与供数变化同一事务提交；任一校验失败即 ROLLBACK，数据保持原状。
"""

from __future__ import annotations

import os
import sqlite3
import time
from typing import Any

from .database import CHUNK_SIZE
from .schemas import (
    BulkAttribution,
    BulkDispatch,
    BulkWorkHour,
)

# 仅用于并发演示/测试：每个暂存块提交后 sleep 若干秒，把“准备中”窗口拉长，
# 默认 0（生产无任何额外延迟），由环境变量 APP_STAGE_DELAY_SECONDS 控制。
_STAGE_DELAY = float(os.getenv("APP_STAGE_DELAY_SECONDS", "0") or 0)

class AppError(Exception):
    """业务错误，携带 HTTP 状态码与中文原因。"""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


# ---------------------------------------------------------------------------
# 集合式供数核算（单条 SQL，不逐行展开路径）
# ---------------------------------------------------------------------------
# LEFT JOIN 保证没有任何路径的工时也出现在结果里（path_count = 0）。
# 归科只连当月 active 版本：staging/discarded 的行在 SQL 层就不可见，
# “未发布数据不参与供数”由连接条件保证，不依赖调用方过滤。
# COUNT(e.row_id) 同时完成两件事：
#   1. 归科缺失的连接结果为 NULL，不计入路径；
#   2. 同一工时有多条(分派,归科)组合时逐组合计数——即使预算科目相同，
#      也视为两条路径，path_count >= 2 即判重复扣留。
PATH_SQL = """
SELECT h.row_id                                  AS work_row_id,
       h.work_order                              AS work_order,
       h.minutes                                 AS minutes,
       COUNT(e.row_id)                           AS path_count,
       MIN(e.budget_subject)                    AS budget_subject
FROM work_hour h
LEFT JOIN dispatch d
       ON d.month = h.month AND d.work_order = h.work_order
LEFT JOIN attribution_version v
       ON v.month = h.month AND v.status = 'active'
LEFT JOIN attribution_entry e
       ON e.version_id = v.version_id AND e.project = d.project
WHERE h.month = ?
GROUP BY h.row_id, h.work_order, h.minutes
ORDER BY h.row_id
"""


def _path_rows(conn: sqlite3.Connection, month: str) -> list[sqlite3.Row]:
    return list(conn.execute(PATH_SQL, (month,)).fetchall())


def _split_supply(rows: list[sqlite3.Row]) -> dict[str, list[dict[str, Any]]]:
    suppliable: list[dict[str, Any]] = []
    withheld: list[dict[str, Any]] = []
    for r in rows:
        if r["path_count"] == 1:
            suppliable.append(
                {
                    "work_row_id": r["work_row_id"],
                    "work_order": r["work_order"],
                    "minutes": r["minutes"],
                    "budget_subject": r["budget_subject"],
                    "path_count": 1,
                }
            )
        else:
            withheld.append(
                {
                    "work_row_id": r["work_row_id"],
                    "work_order": r["work_order"],
                    "minutes": r["minutes"],
                    "path_count": r["path_count"],
                    "reason": "no_path" if r["path_count"] == 0 else "duplicate",
                }
            )
    return {"suppliable": suppliable, "withheld": withheld}


def query_supply(conn: sqlite3.Connection, month: str) -> dict[str, Any]:
    rows = _path_rows(conn, month)
    result = _split_supply(rows)
    return {"month": month, **result}


# ---------------------------------------------------------------------------
# 录入（工时/分派：整月覆盖或追加；同月行号唯一由主键兜底）
# ---------------------------------------------------------------------------
def _check_unique(rows: list[Any], label: str) -> None:
    ids = [r.row_id for r in rows]
    dup = sorted({i for i in ids if ids.count(i) > 1})
    if dup:
        raise AppError(422, f"{label}内存在重复行号: {dup}，同月行号必须唯一")


def _bulk_upsert(
    conn: sqlite3.Connection,
    month: str,
    payload: Any,
    table: str,
    columns: tuple[str, ...],
    label: str,
) -> dict[str, Any]:
    _check_unique(payload.rows, label)
    try:
        conn.execute("BEGIN IMMEDIATE")
        if payload.replace:
            conn.execute(f"DELETE FROM {table} WHERE month = ?", (month,))
        placeholders = ", ".join("?" for _ in columns)
        sql = (
            f"INSERT INTO {table} (month, {', '.join(columns)}) "
            f"VALUES (?, {placeholders})"
        )
        conn.executemany(
            sql,
            [(month, *(getattr(r, c) for c in columns)) for r in payload.rows],
        )
        conn.execute("COMMIT")
    except sqlite3.IntegrityError as exc:
        conn.execute("ROLLBACK")
        raise AppError(409, f"{label}写入失败（行号与已有数据冲突或约束不满足）: {exc}")
    except sqlite3.OperationalError as exc:
        _rollback_if_open(conn)
        if _is_locked(exc):
            raise AppError(409, "数据库正被其他纠错事务占用，请基于最新已提交数据重试")
        raise
    return {"month": month, "table": table, "inserted": len(payload.rows), "replace": payload.replace}


def load_work_hours(conn: sqlite3.Connection, month: str, payload: BulkWorkHour) -> dict[str, Any]:
    return _bulk_upsert(conn, month, payload, "work_hour", ("row_id", "work_order", "minutes"), "工时表")


def load_dispatches(conn: sqlite3.Connection, month: str, payload: BulkDispatch) -> dict[str, Any]:
    return _bulk_upsert(conn, month, payload, "dispatch", ("row_id", "work_order", "project"), "分派表")


# ---------------------------------------------------------------------------
# 归科录入：版本化暂存 + 常量发布（整月覆盖）/ active 版本内追加
# ---------------------------------------------------------------------------
def _get_epoch(conn: sqlite3.Connection, month: str) -> int:
    """读取归科乐观并发版本号；从未写过归科的月份为 0。"""
    row = conn.execute(
        "SELECT attr_epoch FROM month_meta WHERE month = ?", (month,)
    ).fetchone()
    return int(row["attr_epoch"]) if row is not None else 0


def _bump_epoch(conn: sqlite3.Connection, month: str) -> None:
    """同月任何归科已提交改动都推进 epoch，使在途覆盖发布时退回。"""
    conn.execute(
        "INSERT INTO month_meta (month, attr_epoch) VALUES (?, 1) "
        "ON CONFLICT(month) DO UPDATE SET attr_epoch = attr_epoch + 1",
        (month,),
    )


def _active_version_id(conn: sqlite3.Connection, month: str) -> int | None:
    row = conn.execute(
        "SELECT version_id FROM attribution_version WHERE month = ? AND status = 'active'",
        (month,),
    ).fetchone()
    return None if row is None else int(row["version_id"])


def _discard_version(conn: sqlite3.Connection, version_id: int) -> None:
    """标记暂存版本废弃；best-effort，清不掉的残留由启动 GC 兜底。"""
    try:
        conn.execute(
            "UPDATE attribution_version SET status = 'discarded' "
            "WHERE version_id = ? AND status = 'staging'",
            (version_id,),
        )
    except sqlite3.OperationalError:
        pass


def _gc_version_entries(conn: sqlite3.Connection, version_id: int) -> None:
    """分小块短事务物理清理废弃版本的行，块间释放写锁。"""
    while True:
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "DELETE FROM attribution_entry WHERE version_id = ? "
                "AND rowid IN (SELECT rowid FROM attribution_entry "
                "              WHERE version_id = ? LIMIT ?)",
                (version_id, version_id, CHUNK_SIZE),
            )
            removed = cur.rowcount
            if removed == 0:
                conn.execute(
                    "DELETE FROM attribution_version WHERE version_id = ?", (version_id,)
                )
            conn.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            _rollback_if_open(conn)
            if _is_locked(exc):
                return  # 下次请求或启动时继续清理
            raise
        if removed < CHUNK_SIZE:
            return


def _purge_month_dead_versions(conn: sqlite3.Connection, month: str) -> None:
    """准备新覆盖前，清理同月遗留的 staging/discarded 版本（失败/重启残留）。"""
    dead = [
        r[0]
        for r in conn.execute(
            "SELECT version_id FROM attribution_version "
            "WHERE month = ? AND status IN ('staging', 'discarded') ORDER BY version_id",
            (month,),
        ).fetchall()
    ]
    for version_id in dead:
        _gc_version_entries(conn, version_id)


def _append_attributions(
    conn: sqlite3.Connection, month: str, payload: BulkAttribution
) -> dict[str, Any]:
    """追加写入：直接写入当月 active 版本（无则建版本），同事务推进 epoch。"""
    try:
        conn.execute("BEGIN IMMEDIATE")
        version_id = _active_version_id(conn, month)
        if version_id is None:
            cur = conn.execute(
                "INSERT INTO attribution_version (month, status, activated_at) "
                "VALUES (?, 'active', datetime('now'))",
                (month,),
            )
            version_id = int(cur.lastrowid)
        conn.executemany(
            "INSERT INTO attribution_entry (version_id, row_id, project, budget_subject) "
            "VALUES (?, ?, ?, ?)",
            [
                (version_id, r.row_id, r.project, r.budget_subject)
                for r in payload.rows
            ],
        )
        # 追加也是同月归科的已提交改动：推进 epoch，让在途整月覆盖发布时退回。
        _bump_epoch(conn, month)
        conn.execute("COMMIT")
    except sqlite3.IntegrityError as exc:
        conn.execute("ROLLBACK")
        raise AppError(409, f"归科表写入失败（行号与已有数据冲突或约束不满足）: {exc}")
    except sqlite3.OperationalError as exc:
        _rollback_if_open(conn)
        if _is_locked(exc):
            raise AppError(409, "数据库正被其他写事务占用，请基于最新已提交数据重试")
        raise
    return {
        "month": month,
        "table": "attribution",
        "inserted": len(payload.rows),
        "replace": False,
    }


def _prepare_attribution_stage(
    conn: sqlite3.Connection, month: str, rows: list[Any]
) -> tuple[int, int]:
    """准备阶段：清残留 -> 建 staging -> 分小块短事务写行。

    块与块之间没有任何事务挂起，SQLite 写锁在块间释放，
    其他月份（以及同月纠错等短事务）可以在整块覆盖写完前提交。
    返回 (staging_version_id, 准备时刻 epoch)。
    """
    _purge_month_dead_versions(conn, month)
    epoch_at_prepare = _get_epoch(conn, month)

    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "INSERT INTO attribution_version (month, status) VALUES (?, 'staging')",
            (month,),
        )
        version_id = int(cur.lastrowid)
        conn.execute("COMMIT")
    except sqlite3.OperationalError as exc:
        _rollback_if_open(conn)
        if _is_locked(exc):
            raise AppError(409, "数据库正被其他写事务占用，请基于最新已提交数据重试")
        raise

    try:
        for start in range(0, len(rows), CHUNK_SIZE):
            chunk = rows[start : start + CHUNK_SIZE]
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                "INSERT INTO attribution_entry "
                "(version_id, row_id, project, budget_subject) VALUES (?, ?, ?, ?)",
                [(version_id, r.row_id, r.project, r.budget_subject) for r in chunk],
            )
            conn.execute("COMMIT")
            # 块间不持任何事务/写锁；测试场景可注入延迟以稳定复现并发窗口。
            if _STAGE_DELAY > 0:
                time.sleep(_STAGE_DELAY)
    except sqlite3.OperationalError as exc:
        _rollback_if_open(conn)
        _discard_version(conn, version_id)
        if _is_locked(exc):
            raise AppError(409, "数据库正被其他写事务占用，请基于最新已提交数据重试")
        raise
    except Exception:
        _rollback_if_open(conn)
        _discard_version(conn, version_id)
        raise
    return version_id, epoch_at_prepare


def publish_attribution_stage(
    conn: sqlite3.Connection, month: str, version_id: int, epoch_at_prepare: int
) -> None:
    """发布阶段：常量短事务翻转版本，写行数与批大小无关。

    事务内复核 epoch 与 staging 身份；若期间同月归科被追加/覆盖/撤除/清空，
    直接抛 409 并把本批 staging 标记废弃，已提交改动原样保留。
    """
    try:
        conn.execute("BEGIN IMMEDIATE")
        current_epoch = _get_epoch(conn, month)
        stage = conn.execute(
            "SELECT status FROM attribution_version WHERE version_id = ?",
            (version_id,),
        ).fetchone()
        if current_epoch != epoch_at_prepare or stage is None or stage["status"] != "staging":
            conn.execute("COMMIT")  # 未改任何业务数据，提交空事务释放锁
            _discard_version(conn, version_id)
            raise AppError(
                409,
                f"{month} 归科表在本批覆盖准备期间已被其他请求改动"
                "（追加/覆盖/纠错撤除/整月清空），本批覆盖退回，已提交改动保留；"
                "请基于最新数据重新覆盖",
            )
        # 以下均为固定行数的状态翻转，与本批多少行无关：
        #   旧 active 至多 1 行 -> discarded；staging 1 行 -> active；epoch 1 行 +1。
        conn.execute(
            "UPDATE attribution_version SET status = 'discarded' "
            "WHERE month = ? AND status = 'active'",
            (month,),
        )
        conn.execute(
            "UPDATE attribution_version SET status = 'active', activated_at = datetime('now') "
            "WHERE version_id = ?",
            (version_id,),
        )
        _bump_epoch(conn, month)
        conn.execute("COMMIT")
    except sqlite3.OperationalError as exc:
        _rollback_if_open(conn)
        if _is_locked(exc):
            raise AppError(409, "数据库正被其他写事务占用，请基于最新已提交数据重试")
        raise

    # 旧版本行的物理清理放到锁外分小块做，不影响发布的常量写特征。
    old_ids = [
        r[0]
        for r in conn.execute(
            "SELECT version_id FROM attribution_version "
            "WHERE month = ? AND status = 'discarded'",
            (month,),
        ).fetchall()
    ]
    for old_id in old_ids:
        _gc_version_entries(conn, old_id)


def load_attributions(
    conn: sqlite3.Connection, month: str, payload: BulkAttribution
) -> dict[str, Any]:
    _check_unique(payload.rows, "归科表")
    if not payload.replace:
        return _append_attributions(conn, month, payload)

    version_id, epoch_at_prepare = _prepare_attribution_stage(conn, month, payload.rows)
    try:
        publish_attribution_stage(conn, month, version_id, epoch_at_prepare)
    except AppError:
        # 冲突退回：staging 已在发布函数内标记废弃，旧 active 整表保持生效。
        raise
    return {
        "month": month,
        "table": "attribution",
        "inserted": len(payload.rows),
        "replace": True,
    }


# ---------------------------------------------------------------------------
# 纠错：撤除一条分派/归科，撤除与供数变化一起生效
# ---------------------------------------------------------------------------
def _is_locked(exc: sqlite3.OperationalError) -> bool:
    msg = str(exc).lower()
    return "locked" in msg or "busy" in msg


def _rollback_if_open(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.OperationalError:
        pass  # 无活动事务


def _record_exists(conn: sqlite3.Connection, table: str, month: str, row_id: int) -> bool:
    if table == "attribution":
        # 归科行只认当月 active 版本，staging 中的行视同不存在。
        row = conn.execute(
            "SELECT 1 FROM attribution_version v "
            "JOIN attribution_entry e ON e.version_id = v.version_id "
            "WHERE v.month = ? AND v.status = 'active' AND e.row_id = ?",
            (month, row_id),
        ).fetchone()
        return row is not None
    cur = conn.execute(
        f"SELECT 1 FROM {table} WHERE month = ? AND row_id = ?", (month, row_id)
    )
    return cur.fetchone() is not None


def _target_path_ids(
    conn: sqlite3.Connection, month: str, work_row_id: int, table: str, remove_row_id: int
) -> int:
    """该待撤除记录落在目标工时完整路径上的条数（集合 SQL）。"""
    if table == "dispatch":
        sql = """
        SELECT COUNT(*)
        FROM work_hour h
        JOIN dispatch d ON d.month = h.month AND d.work_order = h.work_order
        JOIN attribution_version v ON v.month = h.month AND v.status = 'active'
        JOIN attribution_entry a ON a.version_id = v.version_id AND a.project = d.project
        WHERE h.month = ? AND h.row_id = ? AND d.row_id = ?
        """
    else:
        sql = """
        SELECT COUNT(*)
        FROM work_hour h
        JOIN dispatch d ON d.month = h.month AND d.work_order = h.work_order
        JOIN attribution_version v ON v.month = h.month AND v.status = 'active'
        JOIN attribution_entry a ON a.version_id = v.version_id AND a.project = d.project
        WHERE h.month = ? AND h.row_id = ? AND a.row_id = ?
        """
    return int(conn.execute(sql, (month, work_row_id, remove_row_id)).fetchone()[0])


def correct(
    conn: sqlite3.Connection, month: str, work_row_id: int, remove: str, remove_row_id: int
) -> dict[str, Any]:
    table = "dispatch" if remove == "dispatch" else "attribution"
    try:
        # 串行化：并发纠错以各自 BEGIN IMMEDIATE 前的已提交数据为准，
        # 后到者阻塞/失败，不会在同一份数据上各改各的。
        conn.execute("BEGIN IMMEDIATE")

        if not _record_exists(conn, "work_hour", month, work_row_id):
            raise AppError(404, f"工时行号 {work_row_id} 在 {month} 不存在，未做任何改动")
        if not _record_exists(conn, table, month, remove_row_id):
            raise AppError(404, f"{remove} 行号 {remove_row_id} 在 {month} 不存在，未做任何改动")

        # 撤除前快照（已提交数据 + 本事务尚未修改）
        before = {r["work_row_id"]: r for r in _path_rows(conn, month)}
        target_before = before.get(work_row_id)
        if target_before is None:  # 理论上不会发生，防御
            raise AppError(404, f"工时行号 {work_row_id} 无核算数据，未做任何改动")

        on_path = _target_path_ids(conn, month, work_row_id, table, remove_row_id)
        if on_path == 0:
            raise AppError(
                422,
                f"{remove} 行号 {remove_row_id} 不在工时行 {work_row_id} 的任何完整路径上，"
                "撤除它无法纠正该工时，未做任何改动",
            )

        before_count = target_before["path_count"]
        if before_count == 1:
            raise AppError(
                409,
                f"工时行 {work_row_id} 当前仅有 1 条完整路径、本就可供数，"
                "撤除会使其失去科目，拒绝执行，未做任何改动",
            )
        if before_count == 0:
            raise AppError(
                409,
                f"工时行 {work_row_id} 当前没有完整路径（无科目可归），"
                "撤除记录无法使其供数，拒绝执行，未做任何改动",
            )

        if table == "dispatch":
            conn.execute(
                "DELETE FROM dispatch WHERE month = ? AND row_id = ?",
                (month, remove_row_id),
            )
        else:
            # 归科行从当月 active 版本撤除，同事务推进 epoch：
            # 任何正在准备中的同月覆盖发布时都会因此退回。
            conn.execute(
                """
                DELETE FROM attribution_entry
                WHERE row_id = ?
                  AND version_id = (
                        SELECT version_id FROM attribution_version
                        WHERE month = ? AND status = 'active'
                  )
                """,
                (remove_row_id, month),
            )
            _bump_epoch(conn, month)
        conn.execute(
            "INSERT INTO correction_audit (month, work_row_id, removed_table, removed_row_id) "
            "VALUES (?, ?, ?, ?)",
            (month, work_row_id, remove, remove_row_id),
        )

        # 撤除后重新集合核算
        after = {r["work_row_id"]: r for r in _path_rows(conn, month)}
        target_after = after[work_row_id]
        if target_after["path_count"] != 1:
            raise AppError(
                409,
                f"撤除后工时行 {work_row_id} 仍有 {target_after['path_count']} 条路径，"
                "不满足“恰好一条完整路径”，已回滚，数据保持撤除前状态",
            )

        # 原本可供数的工时必须全部保有科目
        broken = [
            wid
            for wid, r in before.items()
            if r["path_count"] == 1 and after[wid]["path_count"] != 1
        ]
        if broken:
            raise AppError(
                409,
                f"撤除会使原本可供数的工时行 {sorted(broken)} 失去科目，"
                "违反保科规则，已回滚，数据保持撤除前状态",
            )

        restored = sorted(
            wid
            for wid, r in before.items()
            if r["path_count"] >= 2 and after[wid]["path_count"] == 1
        )

        conn.execute("COMMIT")
    except AppError:
        _rollback_if_open(conn)
        raise
    except sqlite3.OperationalError as exc:
        _rollback_if_open(conn)
        if _is_locked(exc):
            raise AppError(409, "数据库正被其他纠错事务占用，请基于最新已提交数据重试")
        raise
    except Exception:
        _rollback_if_open(conn)
        raise

    final = _split_supply(list(after.values()))
    return {
        "month": month,
        "work_row_id": work_row_id,
        "removed": {"table": remove, "row_id": remove_row_id},
        "restored_work_row_ids": restored,
        **final,
    }


def reset_month(conn: sqlite3.Connection, month: str) -> dict[str, Any]:
    """测试辅助：清空某月全部数据。

    归科版本连同 meta 一并清理并推进 epoch，使在途覆盖在发布时退回。
    """
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM work_hour WHERE month = ?", (month,))
        conn.execute("DELETE FROM dispatch WHERE month = ?", (month,))
        conn.execute(
            "DELETE FROM correction_audit WHERE month = ?", (month,)
        )
        conn.execute(
            "DELETE FROM attribution_entry WHERE version_id IN ("
            "  SELECT version_id FROM attribution_version WHERE month = ?)",
            (month,),
        )
        conn.execute("DELETE FROM attribution_version WHERE month = ?", (month,))
        # 不删 meta 行：epoch +1 即可让准备中的覆盖失效；行缺失（首月场景）也会失效。
        conn.execute(
            "INSERT INTO month_meta (month, attr_epoch) VALUES (?, 1) "
            "ON CONFLICT(month) DO UPDATE SET attr_epoch = attr_epoch + 1",
            (month,),
        )
        conn.execute("COMMIT")
    except sqlite3.OperationalError as exc:
        _rollback_if_open(conn)
        if _is_locked(exc):
            raise AppError(409, "数据库正被其他事务占用，请重试")
        raise
    return {"month": month, "cleared": True}


def list_audit(conn: sqlite3.Connection, month: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT id, month, work_row_id, removed_table, removed_row_id, created_at "
        "FROM correction_audit WHERE month = ? ORDER BY id",
        (month,),
    ).fetchall()
    return [dict(r) for r in rows]
