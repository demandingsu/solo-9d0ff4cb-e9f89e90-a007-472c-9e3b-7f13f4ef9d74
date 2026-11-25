"""归科两阶段发布的端到端试跑（标准库即可运行）。

用法（在项目根目录）：
    APP_DB_PATH=/tmp/test-maintenance-cc.db \
    PYTHONPATH=/tmp/pylibs:. python3 -I tests/run_e2e_concurrency.py

覆盖需求：
  C. 异月写入不被整月覆盖阻塞：大批归科分块暂存期间，另一月份的录入与纠错
     在覆盖完成前就能提交；覆盖发布后供数使用整张新表。
  D. 同月纠错先提交：覆盖准备期间同月纠错先 COMMIT（推进 epoch），
     覆盖发布退回 409，纠错改动保留，旧归科表仍生效，当月可重新覆盖。
  E. 发布前失败/重启：staging 数据不参与供数；重启后残留被清理，
     供数仍是旧表，当月可重新覆盖成功。
  F. 发布事务常量写：不论批次多少行，发布事务的写操作数恒定。
另含旧库迁移（attribution 旧表 -> active 版本）的同进程验证。
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

# 同进程直测场景需要导入 app 及其第三方依赖；-I 模式下显式补上库目录。
for _p in ("/tmp/pylibs",):
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8001")
DB_PATH = os.environ.get("APP_DB_PATH", "/tmp/test-maintenance-cc.db")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MONTH_A = "2026-11"
MONTH_B = "2026-12"
BIG_ROWS = 30000
PASS = "[PASS]"
FAIL = "[FAIL]"
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    mark = PASS if cond else FAIL
    print(f"  {mark} {name}" + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def call(method: str, path: str, body: dict | None = None, timeout: float = 60.0) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def wait_ready(proc: subprocess.Popen) -> None:
    for _ in range(80):
        if proc.poll() is not None:
            raise RuntimeError("服务进程提前退出")
        try:
            status, _ = call("GET", "/health")
            if status == 200:
                return
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError("服务未在 20s 内就绪")


def start_server(db_path: str | None = None, delay: float = 0.0) -> subprocess.Popen:
    env = {
        **os.environ,
        "APP_DB_PATH": db_path or DB_PATH,
        "APP_STAGE_DELAY_SECONDS": str(delay),
        "PYTHONPATH": ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""),
    }
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8001"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def wait_staging_rows(min_rows: int, timeout: float = 30.0) -> bool:
    """直连库文件轮询 staging 行数（只读，独立连接，WAL 下不干扰服务）。

    注：跨进程 WAL 视图在 checkpoint 边界可能短暂不可见，因此该等待只用于
    同月场景的粗同步；异月非阻塞的严格证明改用请求耗时对比（见场景 C）。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM attribution_entry e "
                "JOIN attribution_version v ON v.version_id = e.version_id "
                "WHERE v.status = 'staging'"
            ).fetchone()[0]
        finally:
            conn.close()
        if n >= min_rows:
            return True
        time.sleep(0.05)
    return False


def seed_duplicate_month(month: str) -> None:
    """造一个含重复工时 H1 的月份，供纠错使用。"""
    call("DELETE", f"/months/{month}")
    call("POST", f"/months/{month}/work-hours", {
        "rows": [
            {"row_id": 1, "work_order": "WO1", "minutes": 120},
            {"row_id": 2, "work_order": "WO2", "minutes": 60},
        ]
    })
    call("POST", f"/months/{month}/dispatches", {
        "rows": [
            {"row_id": 1, "work_order": "WO1", "project": "P1"},
            {"row_id": 2, "work_order": "WO1", "project": "P2"},
            {"row_id": 3, "work_order": "WO2", "project": "P3"},
        ]
    })
    call("POST", f"/months/{month}/attributions", {
        "rows": [
            {"row_id": 1, "project": "P1", "budget_subject": "S1"},
            {"row_id": 2, "project": "P2", "budget_subject": "S2"},
            {"row_id": 3, "project": "P3", "budget_subject": "S3"},
        ]
    })


def seed_single_path_month(month: str) -> None:
    """造一个两条工时均为单路径可供数的月份（用于重启后重新覆盖核对）。"""
    call("DELETE", f"/months/{month}")
    call("POST", f"/months/{month}/work-hours", {
        "rows": [
            {"row_id": 1, "work_order": "WO1", "minutes": 120},
            {"row_id": 2, "work_order": "WO2", "minutes": 60},
        ]
    })
    call("POST", f"/months/{month}/dispatches", {
        "rows": [
            {"row_id": 1, "work_order": "WO1", "project": "P1"},
            {"row_id": 3, "work_order": "WO2", "project": "P3"},
        ]
    })
    call("POST", f"/months/{month}/attributions", {
        "rows": [
            {"row_id": 1, "project": "P1", "budget_subject": "S1"},
            {"row_id": 3, "project": "P3", "budget_subject": "S3"},
        ]
    })


def big_attribution_rows(n: int) -> list[dict]:
    return [
        {"row_id": i, "project": f"PX{i % 500}", "budget_subject": f"SUBJ-{i % 50}"}
        for i in range(1, n + 1)
    ]


# ---------------------------------------------------------------- C. 异月写入
def scenario_cross_month(proc: subprocess.Popen) -> None:
    print("场景 C：整月覆盖暂存期间，异月录入/纠错不被阻塞")
    call("DELETE", f"/months/{MONTH_A}")
    call("DELETE", f"/months/{MONTH_B}")
    seed_duplicate_month(MONTH_B)
    # B 月初始供数：H1 重复扣留，H2 可供数
    status, r = call("GET", f"/months/{MONTH_B}/supply")
    check("B 月初始 H1 扣留（路径数 2）",
          next(x for x in r["withheld"] if x["work_row_id"] == 1)["path_count"] == 2, str(r))

    result: dict = {}
    done_event = threading.Event()
    big_start = time.time()

    def big_replace() -> None:
        status, body = call(
            "POST", f"/months/{MONTH_A}/attributions",
            {"rows": big_attribution_rows(BIG_ROWS), "replace": True}, timeout=120,
        )
        result["status"] = status
        result["body"] = body
        done_event.set()

    t = threading.Thread(target=big_replace)
    t.start()

    # 等待首个暂存块提交（块间延迟 50ms），确保大覆盖已在进行中。
    time.sleep(0.2)
    # 信息性观察：staging 是否可见（跨进程 WAL 视图不保证，不作为断言条件）。
    staging_seen = wait_staging_rows(1, timeout=2.0)
    print(f"    [INFO] staging 轮询可见 = {staging_seen}（不作为断言，非阻塞以耗时与事件为准）")

    # 异月工时录入（写事务）必须在大覆盖完成前提交
    t0 = time.time()
    status, body = call("POST", f"/months/{MONTH_B}/work-hours", {
        "rows": [{"row_id": 99, "work_order": "WO99", "minutes": 15}], "replace": False
    })
    wh_elapsed = time.time() - t0
    check("异月工时追加在覆盖进行中提交成功（200）", status == 200, f"{status} {body}")
    check("异月工时提交时大覆盖尚未结束", not done_event.is_set())
    check("异月写入没有等待整批覆盖完成（< 5s）", wh_elapsed < 5, f"{wh_elapsed:.2f}s")

    # 异月纠错（BEGIN IMMEDIATE 写事务）同样能在覆盖进行中提交
    t0 = time.time()
    status, body = call("POST", f"/months/{MONTH_B}/corrections", {
        "work_row_id": 1, "remove": "dispatch", "remove_row_id": 2
    })
    corr_elapsed = time.time() - t0
    check("异月纠错在覆盖进行中提交成功（200）", status == 200, f"{status} {body}")
    check("异月纠错提交时大覆盖尚未结束", not done_event.is_set())
    check("异月纠错没有等待整批覆盖完成（< 5s）", corr_elapsed < 5, f"{corr_elapsed:.2f}s")
    check("纠错返回恢复行号 [1]", body.get("restored_work_row_ids") == [1], str(body))

    t.join()
    big_elapsed = time.time() - big_start
    check("大批归科覆盖最终成功", result.get("status") == 200, str(result.get("body")))
    check(f"覆盖报告写入 {BIG_ROWS} 行", result.get("body", {}).get("inserted") == BIG_ROWS,
          str(result.get("body")))
    check(
        "异月写耗时远小于覆盖总耗时（证明确在块间穿插提交）",
        max(wh_elapsed, corr_elapsed) < big_elapsed / 2,
        f"异月 {max(wh_elapsed, corr_elapsed):.2f}s vs 覆盖 {big_elapsed:.2f}s",
    )

    # 供数使用已生效的整张新表：active 版本行数 = BIG_ROWS，无 staging/discarded 残留
    conn = sqlite3.connect(DB_PATH)
    try:
        active_n = conn.execute(
            "SELECT COUNT(*) FROM attribution_entry e "
            "JOIN attribution_version v ON v.version_id = e.version_id "
            "WHERE v.month = ? AND v.status = 'active'", (MONTH_A,)
        ).fetchone()[0]
        stale_n = conn.execute(
            "SELECT COUNT(*) FROM attribution_version "
            "WHERE month = ? AND status IN ('staging','discarded')", (MONTH_A,)
        ).fetchone()[0]
    finally:
        conn.close()
    check("A 月 active 版本为整张新表（行数=批行数）", active_n == BIG_ROWS, str(active_n))
    check("发布后无 staging/discarded 版本残留", stale_n == 0, str(stale_n))

    # 异月纠错结果持久可见
    status, r = call("GET", f"/months/{MONTH_B}/supply")
    sup = {x["work_row_id"]: x for x in r["suppliable"]}
    check("B 月 H1 纠错后可供数且科目 S1", sup.get(1, {}).get("budget_subject") == "S1", str(sup))
    check("B 月追加的工时行 99 已落库", 99 in {x["work_row_id"] for x in r["withheld"]} or 99 in sup,
          str(r))


# ---------------------------------------------------------------- D. 同月冲突
def scenario_same_month_conflict() -> None:
    print("场景 D：覆盖准备期间同月纠错先提交 -> 覆盖退回，改动保留")
    call("DELETE", f"/months/{MONTH_A}")
    seed_duplicate_month(MONTH_A)
    status, before = call("GET", f"/months/{MONTH_A}/supply")
    check("前置：H1 重复扣留",
          next(x for x in before["withheld"] if x["work_row_id"] == 1)["path_count"] == 2)

    result: dict = {}

    def big_replace() -> None:
        result["status"], result["body"] = call(
            "POST", f"/months/{MONTH_A}/attributions",
            {"rows": big_attribution_rows(5000), "replace": True}, timeout=120,
        )

    t = threading.Thread(target=big_replace)
    t.start()
    time.sleep(0.2)
    check("观察到同月 staging 正在累积", wait_staging_rows(1000))

    # 同月纠错先提交（撤除归科行会推进 epoch；归科行仍在 active 旧表上，
    # staging 中的新批次尚未发布，不影响该校验）
    status, body = call("POST", f"/months/{MONTH_A}/corrections", {
        "work_row_id": 1, "remove": "attribution", "remove_row_id": 2
    })
    check("同月纠错先提交成功（200）", status == 200, f"{status} {body}")
    t.join()

    check("本批覆盖退回（409）", result.get("status") == 409, str(result.get("body")))
    check("退回信息为中文并提示重试", "覆盖退回" in result.get("body", {}).get("detail", ""),
          str(result.get("body")))

    # 纠错改动保留；供数仍基于旧归科整表
    status, r = call("GET", f"/months/{MONTH_A}/supply")
    sup = {x["work_row_id"]: x for x in r["suppliable"]}
    check("纠错改动保留：H1 可供数、科目 S1", sup.get(1, {}).get("budget_subject") == "S1", str(sup))
    check("H2 仍按旧归科表供数 S3", sup.get(2, {}).get("budget_subject") == "S3", str(sup))

    conn = sqlite3.connect(DB_PATH)
    try:
        active_n = conn.execute(
            "SELECT COUNT(*) FROM attribution_entry e "
            "JOIN attribution_version v ON v.version_id = e.version_id "
            "WHERE v.month = ? AND v.status = 'active'", (MONTH_A,)
        ).fetchone()[0]
        stage_n = conn.execute(
            "SELECT COUNT(*) FROM attribution_version "
            "WHERE month = ? AND status = 'staging'", (MONTH_A,)
        ).fetchone()[0]
    finally:
        conn.close()
    check("生效归科仍是纠错后的旧表 2 行（未被退回的覆盖替换）", active_n == 2, str(active_n))
    check("退回的 staging 已标记废弃（无 active staging）", stage_n == 0, str(stage_n))

    # 当月可重新覆盖成功
    status, body = call("POST", f"/months/{MONTH_A}/attributions", {
        "rows": [{"row_id": 1, "project": "PNEW", "budget_subject": "SNEW"}], "replace": True
    })
    check("冲突后当月可重新覆盖成功（200）", status == 200, f"{status} {body}")
    status, r = call("GET", f"/months/{MONTH_A}/supply")
    check("重新覆盖后旧归科路径消失（H1/H2 均无路径扣留）",
          sorted(x["work_row_id"] for x in r["withheld"]) == [1, 2]
          and all(x["reason"] == "no_path" for x in r["withheld"]), str(r))


# ---------------------------------------------------------------- E. 重启
def scenario_restart(proc: subprocess.Popen) -> None:
    print("场景 E：发布前失败/重启，未发布数据不参与供数，重启后可重新覆盖")
    call("DELETE", f"/months/{MONTH_A}")
    seed_single_path_month(MONTH_A)

    # 模拟“准备完成但尚未发布”的残留：直接在库里造一个 staging 版本与行
    conn = sqlite3.connect(DB_PATH)
    try:
        vid = conn.execute(
            "INSERT INTO attribution_version (month, status) VALUES (?, 'staging')",
            (MONTH_A,),
        ).lastrowid
        conn.executemany(
            "INSERT INTO attribution_entry (version_id, row_id, project, budget_subject) "
            "VALUES (?, ?, ?, ?)",
            [(vid, i, f"GHOST{i}", f"GHOST-SUBJ-{i}") for i in range(1, 1001)],
        )
        conn.commit()
    finally:
        conn.close()

    # 重启前：供数绝不能看到 staging（GHOST 不出现）
    status, r = call("GET", f"/months/{MONTH_A}/supply")
    subjects = {x.get("budget_subject") for x in r["suppliable"]}
    check("运行期 staging 不参与供数（无 GHOST 科目）",
          not any(s and s.startswith("GHOST") for s in subjects), str(subjects))
    check("运行期供数仍来自旧 active 表（H2=S3）",
          next(x for x in r["suppliable"] if x["work_row_id"] == 2)["budget_subject"] == "S3",
          str(r))

    # 杀掉服务再拉起（模拟发布前进程失败/重启）
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)

    proc2 = start_server(delay=0.05)
    try:
        wait_ready(proc2)
        conn = sqlite3.connect(DB_PATH)
        try:
            stage_n = conn.execute(
                "SELECT COUNT(*) FROM attribution_version WHERE status = 'staging'"
            ).fetchone()[0]
            ghost_n = conn.execute(
                "SELECT COUNT(*) FROM attribution_entry WHERE project LIKE 'GHOST%'"
            ).fetchone()[0]
            active_n = conn.execute(
                "SELECT COUNT(*) FROM attribution_entry e "
                "JOIN attribution_version v ON v.version_id = e.version_id "
                "WHERE v.month = ? AND v.status = 'active'", (MONTH_A,)
            ).fetchone()[0]
        finally:
            conn.close()
        check("重启后未发布 staging 版本被清理", stage_n == 0, str(stage_n))
        check("重启后未发布行被物理清理", ghost_n == 0, str(ghost_n))
        check("重启后旧 active 整表完好（2 行）", active_n == 2, str(active_n))

        status, r = call("GET", f"/months/{MONTH_A}/supply")
        check("重启后供数仍按旧表（H2=S3）",
              next(x for x in r["suppliable"] if x["work_row_id"] == 2)["budget_subject"] == "S3",
              str(r))

        # 当月仍可重新覆盖
        status, body = call("POST", f"/months/{MONTH_A}/attributions", {
            "rows": [
                {"row_id": 1, "project": "P1", "budget_subject": "S1"},
                {"row_id": 2, "project": "P2", "budget_subject": "S2"},
                {"row_id": 3, "project": "P3", "budget_subject": "S3"},
            ],
            "replace": True,
        })
        check("重启后当月重新覆盖成功（200）", status == 200, f"{status} {body}")
        status, r = call("GET", f"/months/{MONTH_A}/supply")
        check("重新覆盖后供数恢复（H1/H2 均可供数）",
              sorted(x["work_row_id"] for x in r["suppliable"]) == [1, 2], str(r))
    finally:
        proc2.terminate()
        try:
            proc2.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc2.kill()
            proc2.wait(timeout=10)


# ---------------------------------------------------------------- F. 常量发布
def scenario_constant_publish() -> None:
    print("场景 F：发布事务写入量不随批次行数增长（同进程直测）")
    sys.path.insert(0, ROOT)
    from app import database, services  # noqa: E402
    from app.schemas import AttributionIn, BulkAttribution  # noqa: E402

    class CountingConn:
        """记录写事务内执行的 DML 语句条数。"""

        def __init__(self, real: sqlite3.Connection):
            self._real = real
            self.write_calls: list[int] = []
            self._in_tx = False
            self._count = 0

        def execute(self, sql: str, params=()):
            s = sql.strip().upper()
            if s.startswith("BEGIN"):
                self._in_tx = True
                self._count = 0
            cur = self._real.execute(sql, params)
            if self._in_tx and s.startswith(("INSERT", "UPDATE", "DELETE")):
                self._count += 1
            if s.startswith(("COMMIT", "ROLLBACK")) and self._in_tx:
                self.write_calls.append(self._count)
                self._in_tx = False
            return cur

        def executemany(self, sql: str, params):
            if self._in_tx:
                self._count += 1  # 一条 executemany 计一次写操作
            return self._real.executemany(sql, params)

        def __getattr__(self, name):
            return getattr(self._real, name)

    old_db = database.DB_PATH
    test_db = "/tmp/test-constant-publish.db"
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(test_db + suffix)
        except FileNotFoundError:
            pass
    database.DB_PATH = test_db

    real_publish = services.publish_attribution_stage
    publish_writes_by_size: dict[int, int] = {}
    try:
        database.init_db()

        def make_tracer(size: int):
            def tracer(conn, month, version_id, epoch_at_prepare):
                before = list(conn.write_calls)
                real_publish(conn, month, version_id, epoch_at_prepare)
                # 发布函数提交后、GC 前的最后一个写事务即发布事务
                publish_writes_by_size[size] = conn.write_calls[len(before)]
            return tracer

        for n in (100, 5000):
            real = database.get_conn()
            counting = CountingConn(real)
            payload = BulkAttribution(
                rows=[
                    AttributionIn(row_id=i, project=f"P{i}", budget_subject=f"S{i % 9}")
                    for i in range(1, n + 1)
                ],
                replace=True,
            )
            month = "2027-01" if n == 100 else "2027-02"
            services.publish_attribution_stage = make_tracer(n)
            services.load_attributions(counting, month, payload)
            real.close()
        services.publish_attribution_stage = real_publish
        check(
            "发布事务 DML 条数与批次行数无关（100 行与 5000 行相等且为常量 3）",
            publish_writes_by_size.get(100) == publish_writes_by_size.get(5000) == 3,
            str(publish_writes_by_size),
        )
    finally:
        services.publish_attribution_stage = real_publish
        database.DB_PATH = old_db


# ---------------------------------------------------------------- 旧库迁移
def scenario_legacy_migration() -> None:
    print("场景 G：旧库 attribution 表自动迁移为 active 版本，接口兼容")
    legacy_db = "/tmp/test-legacy-migration.db"
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(legacy_db + suffix)
        except FileNotFoundError:
            pass
    # 用旧 schema 造库
    conn = sqlite3.connect(legacy_db)
    conn.executescript(
        """
        CREATE TABLE work_hour (month TEXT NOT NULL, row_id INTEGER NOT NULL,
            work_order TEXT NOT NULL, minutes INTEGER NOT NULL, PRIMARY KEY (month, row_id));
        CREATE TABLE dispatch (month TEXT NOT NULL, row_id INTEGER NOT NULL,
            work_order TEXT NOT NULL, project TEXT NOT NULL, PRIMARY KEY (month, row_id));
        CREATE TABLE attribution (month TEXT NOT NULL, row_id INTEGER NOT NULL,
            project TEXT NOT NULL, budget_subject TEXT NOT NULL, PRIMARY KEY (month, row_id));
        CREATE TABLE correction_audit (id INTEGER PRIMARY KEY AUTOINCREMENT, month TEXT NOT NULL,
            work_row_id INTEGER NOT NULL, removed_table TEXT NOT NULL,
            removed_row_id INTEGER NOT NULL, created_at TEXT NOT NULL DEFAULT (datetime('now')));
        """
    )
    conn.execute("INSERT INTO work_hour VALUES ('2025-01', 1, 'WO1', 100)")
    conn.execute("INSERT INTO dispatch VALUES ('2025-01', 1, 'WO1', 'PJ')")
    conn.execute("INSERT INTO attribution VALUES ('2025-01', 7, 'PJ', 'OLD-SUBJ')")
    conn.execute("INSERT INTO attribution VALUES ('2025-02', 8, 'PJ2', 'OLD-SUBJ-2')")
    conn.commit()
    conn.close()

    proc = start_server(db_path=legacy_db)
    try:
        wait_ready(proc)
        status, r = call("GET", "/months/2025-01/supply")
        check("迁移后旧归科数据直接参与供数（200）", status == 200, str(r))
        check(
            "旧数据科目正确（OLD-SUBJ）",
            next(x for x in r["suppliable"] if x["work_row_id"] == 1)["budget_subject"]
            == "OLD-SUBJ",
            str(r),
        )
        # 迁移后继续用原接口追加/覆盖
        status, body = call("POST", "/months/2025-01/attributions", {
            "rows": [{"row_id": 9, "project": "PJ", "budget_subject": "NEW"}],
            "replace": False,
        })
        check("迁移后原接口追加可用（200）", status == 200, f"{status} {body}")
        status, body = call("POST", "/months/2025-01/attributions", {
            "rows": [{"row_id": 1, "project": "PJ", "budget_subject": "REPLACED"}],
            "replace": True,
        })
        check("迁移后原接口整月覆盖可用（200）", status == 200, f"{status} {body}")
        conn = sqlite3.connect(legacy_db)
        try:
            legacy_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='attribution_legacy'"
            ).fetchone()
            active_n = conn.execute(
                "SELECT COUNT(*) FROM attribution_entry e "
                "JOIN attribution_version v ON v.version_id = e.version_id "
                "WHERE v.month='2025-01' AND v.status='active'"
            ).fetchone()[0]
        finally:
            conn.close()
        check("旧表以 attribution_legacy 留存", legacy_exists is not None)
        check("覆盖后 active 版本仅含新表 1 行", active_n == 1, str(active_n))
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


def main() -> int:
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(DB_PATH + suffix)
        except FileNotFoundError:
            pass
    proc = start_server(delay=0.05)
    try:
        wait_ready(proc)
        scenario_cross_month(proc)
        scenario_same_month_conflict()
        # scenario_restart 会自行终止并重启进程，传当前 proc
        scenario_restart(proc)
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
    scenario_constant_publish()
    scenario_legacy_migration()

    print()
    if failures:
        print(f"{FAIL} 共 {len(failures)} 项失败: {failures}")
        return 1
    print(f"{PASS} 全部断言通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
