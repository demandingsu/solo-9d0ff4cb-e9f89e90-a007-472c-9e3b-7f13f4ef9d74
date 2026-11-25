"""SQLite 连接与建表。

三张业务表均以 (month, row_id) 为主键，保证“同月每表行号唯一”。
连接开启 WAL 与 busy_timeout，写事务统一使用 BEGIN IMMEDIATE，
使并发写在数据库层串行化，快照语义由事务隔离保证。

归科表的版本化改造（2026-11）
----------------------------
旧表 ``attribution`` 改为版本化存储：

* ``attribution_version``：每个归科批次一个版本，状态
  staging（准备中，未发布）/ active（当前生效）/ discarded（已被取代）；
* ``attribution_entry``：归科行，按版本存放。供数与纠错只连接
  active 版本的整表，staging/discarded 数据物理上不可见；
* ``month_meta``：每月归科乐观并发版本号 epoch，任何同月归科写操作
  （追加/覆盖发布/纠错撤除/整月清空）都会使待发布覆盖的校验失败。

整月覆盖先分小块事务写 staging（块间释放写锁，其他月份可写入），
发布事务只翻转固定几行状态，写入量不随批行数增长。
启动时自动把旧库的 ``attribution`` 表迁移为 active 版本，接口与旧数据兼容。
"""

from __future__ import annotations

import os
import sqlite3

DB_PATH = os.getenv("APP_DB_PATH", "/data/maintenance.db")

# 单次 GC / 暂存写入的行数上限：每个小块独立短事务，避免长占写锁。
CHUNK_SIZE = 500

SCHEMA = """
CREATE TABLE IF NOT EXISTS work_hour (
    month      TEXT NOT NULL,
    row_id     INTEGER NOT NULL,
    work_order TEXT NOT NULL,
    minutes    INTEGER NOT NULL CHECK (minutes >= 0),
    PRIMARY KEY (month, row_id)
);

CREATE TABLE IF NOT EXISTS dispatch (
    month      TEXT NOT NULL,
    row_id     INTEGER NOT NULL,
    work_order TEXT NOT NULL,
    project    TEXT NOT NULL,
    PRIMARY KEY (month, row_id)
);

CREATE TABLE IF NOT EXISTS correction_audit (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    month           TEXT NOT NULL,
    work_row_id     INTEGER NOT NULL,
    removed_table   TEXT NOT NULL CHECK (removed_table IN ('dispatch', 'attribution')),
    removed_row_id  INTEGER NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dispatch_month_wo     ON dispatch(month, work_order);
CREATE INDEX IF NOT EXISTS idx_dispatch_month_proj   ON dispatch(month, project);

-- 归科版本：staging=准备中未发布，active=当月生效中，discarded=已被取代
CREATE TABLE IF NOT EXISTS attribution_version (
    version_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    month         TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('staging', 'active', 'discarded')),
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    activated_at  TEXT
);

CREATE INDEX IF NOT EXISTS idx_attr_ver_month_status
    ON attribution_version(month, status);

-- 不变量兜底：每月至多一个 active 归科版本，供数始终对应整张生效表
CREATE UNIQUE INDEX IF NOT EXISTS idx_attr_ver_one_active
    ON attribution_version(month) WHERE status = 'active';

-- 归科行按版本存放；主键 (version_id, row_id) 保持“同版本（同月）行号唯一”
CREATE TABLE IF NOT EXISTS attribution_entry (
    version_id     INTEGER NOT NULL,
    row_id         INTEGER NOT NULL,
    project        TEXT NOT NULL,
    budget_subject TEXT NOT NULL,
    PRIMARY KEY (version_id, row_id)
);

CREATE INDEX IF NOT EXISTS idx_attr_entry_version
    ON attribution_entry(version_id);

-- 每月归科乐观并发版本号：准备覆盖时读数，发布时比对，变化即退回
CREATE TABLE IF NOT EXISTS month_meta (
    month       TEXT PRIMARY KEY,
    attr_epoch  INTEGER NOT NULL DEFAULT 0
);
"""


def get_conn() -> sqlite3.Connection:
    """每个请求一条独立连接，autocommit 模式，事务显式 BEGIN/COMMIT。"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    # check_same_thread=False：FastAPI 的同步依赖与端点可能落在线程池的不同线程上，
    # 而每条连接仅服务单个请求、不被多线程并发共享；写一致性由 BEGIN IMMEDIATE
    # 与 busy_timeout 保证，因此允许跨线程使用同一条连接。
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (name,)
    ).fetchone()
    return row is not None


def _migrate_legacy_attribution(conn: sqlite3.Connection) -> None:
    """把旧库的 attribution 表整体迁移为版本化 active 表。

    改名 + 按月建 active 版本 + 逐行复制 + 写 epoch 全部在一个事务内，
    任一步失败整体回滚，下次启动可重试；迁移后旧表以 attribution_legacy
    留存备查，线上读写只走新表。
    """
    if not _table_exists(conn, "attribution"):
        return
    conn.execute("BEGIN IMMEDIATE")
    try:
        # 正常路径下 attribution_legacy 不会已存在；防御性处理：先留存再改名。
        if not _table_exists(conn, "attribution_legacy"):
            conn.execute("ALTER TABLE attribution RENAME TO attribution_legacy")
        months = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT month FROM attribution_legacy ORDER BY month"
            ).fetchall()
        ]
        for month in months:
            cur = conn.execute(
                "INSERT INTO attribution_version (month, status, activated_at) "
                "VALUES (?, 'active', datetime('now'))",
                (month,),
            )
            version_id = cur.lastrowid
            conn.execute(
                "INSERT INTO attribution_entry (version_id, row_id, project, budget_subject) "
                "SELECT ?, row_id, project, budget_subject FROM attribution_legacy "
                "WHERE month = ?",
                (version_id, month),
            )
            # 旧库已有归科数据，epoch 从 1 起，使任何迁移前记下的旧快照失效。
            conn.execute(
                "INSERT INTO month_meta (month, attr_epoch) VALUES (?, 1) "
                "ON CONFLICT(month) DO UPDATE SET attr_epoch = attr_epoch + 1",
                (month,),
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _purge_non_active_versions(conn: sqlite3.Connection) -> None:
    """启动时清理未发布/已废弃版本的物理行。

    服务启动时尚无请求在途，所有 staging 都属于上次失败或被 kill 的残留，
    绝不能参与供数；分小块短事务删除，避免一次性大锁。
    """
    dead_ids = [
        r[0]
        for r in conn.execute(
            "SELECT version_id FROM attribution_version "
            "WHERE status IN ('staging', 'discarded') ORDER BY version_id"
        ).fetchall()
    ]
    for version_id in dead_ids:
        while True:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "DELETE FROM attribution_entry WHERE version_id = ? "
                    "AND rowid IN (SELECT rowid FROM attribution_entry "
                    "              WHERE version_id = ? LIMIT ?)",
                    (version_id, version_id, CHUNK_SIZE),
                )
                removed = cur.rowcount
                if removed == 0:
                    conn.execute(
                        "DELETE FROM attribution_version WHERE version_id = ?",
                        (version_id,),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            if removed < CHUNK_SIZE:
                break

    # 孤儿 entry（版本行已不在）兜底清理
    while True:
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "DELETE FROM attribution_entry WHERE rowid IN ("
                "  SELECT e.rowid FROM attribution_entry e "
                "  LEFT JOIN attribution_version v ON v.version_id = e.version_id "
                "  WHERE v.version_id IS NULL LIMIT ?)",
                (CHUNK_SIZE,),
            )
            removed = cur.rowcount
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        if removed < CHUNK_SIZE:
            break


def init_db() -> None:
    conn = get_conn()
    try:
        conn.executescript(SCHEMA)
        _migrate_legacy_attribution(conn)
        _purge_non_active_versions(conn)
    finally:
        conn.close()
