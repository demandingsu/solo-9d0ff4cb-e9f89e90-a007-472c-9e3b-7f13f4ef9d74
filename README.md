# 道路养护工时纠错服务

道路养护工时必须归到预算科目才能入仓。本服务用 FastAPI + SQLite 管理同月三张表，
判定每条工时的完整路径（工单 → 项目 → 预算科目），对重复工时提供事务化纠错。

## 一、数据模型（同月每表行号唯一）

| 表 | 含义 | 关键字段 | 主键 |
| --- | --- | --- | --- |
| `work_hour` | 工时表 | 工单号 `work_order`、分钟数 `minutes` | `(month, row_id)` |
| `dispatch` | 分派表 | 工单 → 项目 | `(month, row_id)` |
| `attribution_version` / `attribution_entry` | 归科版本表 / 归科行表 | 项目 → 预算科目 `budget_subject` | 版本 id + `(version_id, row_id)` |
| `correction_audit` | 纠错审计 | 撤除的表/行号、对应工时行 | 自增 id |
| `month_meta` | 每月归科乐观并发版本号 `attr_epoch` | — | `month` |

归科表为版本化存储：每次整月覆盖生成一个版本，状态为 `staging`（准备中）→
`active`（生效中）→ `discarded`（已被取代）。供数与纠错只连接当月 **active 整张表**，
staging/discarded 在 SQL 连接层即不可见；部分唯一索引
`idx_attr_ver_one_active` 兜底“每月至多一个 active 版本”。

## 二、供数核算规则（集合式，不逐条展开路径）

完整路径 = 工时按工单号连分派、分派按项目连归科（仅 active 版本）。核算由**一条集合式
GROUP BY SQL** 完成（见 `app/services.py` 的 `PATH_SQL`），服务端不按工时逐行 JOIN、
不在 Python 追链：

- `path_count = 1`：**可供数**，返回工时及唯一预算科目；
- `path_count = 0`：扣留，原因 `no_path`；
- `path_count >= 2`：扣留，原因 `duplicate`。**两条路径即使落到同一预算科目也算重复**
  （计数按 分派×归科 组合，而非按科目去重）。

## 三、归科整月覆盖：两阶段发布（不阻塞异月、写入量恒定、乐观并发）

旧实现用一个 `BEGIN IMMEDIATE` 大事务删整月再逐行插入，批次越大写锁越久，其他月份
都要排队等锁。现改为两阶段：

1. **准备（staging，分块短事务）**：建 staging 版本后，按 `CHUNK_SIZE`（默认 500 行）
   分小块写入，**每块独立事务、块间释放写锁**。其他月份的录入与纠错在块间即可提交，
   不再等整批写完。
2. **发布（常量短事务）**：单个事务只做 3 条固定 DML——旧 active 置 discarded、
   staging 置 active、`attr_epoch + 1`，**写操作数与批次行数无关**（100 行与 5000 行
   均为 3 条）。旧版本行的物理清理放到锁外分小块进行。
3. **乐观并发（同月改动先提交则本批退回）**：准备时读取 `month_meta.attr_epoch`，
   发布事务内复核。期间同月发生过任何**归科追加、覆盖发布、纠错撤除、整月清空**都会
   推进 epoch（清空在无 meta 行时也会使复核失败），本批覆盖返回 **409** 并把 staging
   标记废弃，**已提交改动原样保留**，客户端可基于最新数据重新覆盖。
4. **失败/重启安全**：staging 数据永远不参与供数与纠错；服务启动时
   `init_db()` 统一清理所有 staging/discarded 残留与孤儿行，当月可立即重新覆盖。

**旧库兼容**：启动时若发现旧 `attribution` 表，在单事务内改名为 `attribution_legacy`
并按月迁移为 active 版本（写入 `attr_epoch=1`），迁移幂等、失败整体回滚可重试；
HTTP 接口路径、请求体与响应字段完全不变。

## 四、纠错事务语义

`POST /months/{month}/corrections` 指定工时行号 + 要撤除的一条分派或归科记录，
在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 以**撤除前已提交数据**为快照校验：工时行、被撤记录必须存在（归科只认 active 版本）；
   被撤记录必须在该工时的某条完整路径上；目标工时当前必须为重复（路径数 ≥ 2）；
2. 撤除记录并写审计；撤除归科时同事务推进 `attr_epoch`，使在途覆盖发布时退回；
3. 撤除后重新集合核算，强制两条不变量：
   - 目标工时恰好剩 1 条路径（能供数）；
   - 原本可供数的工时全部保有科目（共享归科被误撤会在此被拦）；
4. 校验通过才 COMMIT，撤除与供数变化一起生效，返回 `restored_work_row_ids`
   （本次由扣留恢复为可供数的全部工时行号）；
5. 任一校验失败即 ROLLBACK，数据保持撤除前状态，响应体用中文说明原因
   （404 不存在 / 422 撤错记录 / 409 违反供数或保科规则/覆盖冲突）。

并发：写事务均为 `BEGIN IMMEDIATE` + WAL + `busy_timeout`，SQLite 写锁使并发写在
数据库层串行化；归科覆盖另用 epoch 乐观锁保证“准备期间的同月已提交改动不被覆盖吞掉”。
锁冲突返回 409 提示基于最新数据重试。

## 五、启动（Docker Compose）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000/docs 即可录入、查询、纠错
```

SQLite 文件落在命名卷 `maintenance-data:/data/maintenance.db`，容器重建数据不丢。
可选环境变量 `APP_STAGE_DELAY_SECONDS`（默认 0）：每个暂存块后暂停若干秒，
仅用于在 /docs 或测试中稳定复现“覆盖准备中”的并发窗口，生产保持默认即可。

## 六、接口一览（均在 /docs 可试）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/months/{month}/work-hours` | 录入工时表（`replace=true` 整月覆盖，`false` 追加，行号冲突 409） |
| POST | `/months/{month}/dispatches` | 录入分派表 |
| POST | `/months/{month}/attributions` | 录入归科表：覆盖=暂存+常量发布（冲突 409 退回），追加=写 active 版本 |
| GET | `/months/{month}/supply` | 供数视图：只基于 active 归科整表，可供数带科目，扣留带路径数与原因 |
| POST | `/months/{month}/corrections` | 纠错：撤除一条分派/归科 |
| GET | `/months/{month}/audit` | 纠错撤除审计 |
| DELETE | `/months/{month}` | 测试辅助：清空某月 |
| GET | `/health` | 健康检查（容器 HEALTHCHECK） |

`month` 格式固定 `YYYY-MM`。示例：

```bash
curl -X POST localhost:8000/months/2026-11/work-hours -H 'Content-Type: application/json' \
  -d '{"rows":[{"row_id":1,"work_order":"WO1","minutes":120}]}'
curl localhost:8000/months/2026-11/supply
curl -X POST localhost:8000/months/2026-11/corrections -H 'Content-Type: application/json' \
  -d '{"work_row_id":1,"remove":"dispatch","remove_row_id":2}'
```

## 七、实测结果

本机无 Docker 守护进程，使用与镜像同大版本的 Python 3.11 + 同款依赖实跑
（真实 uvicorn 进程 + HTTP + 线程并发，仅用标准库）。

### 7.1 原有 e2e 全量回归：`tests/run_e2e.py`，38/38 断言通过

- 同科目重复路径仍判重复（两条分派同项目同科目，path_count=2 扣留）；
- 无路径工时 path_count=0、原因 `no_path`；
- **共享归科**：撤除被共用的归科记录会打掉其他工时唯一路径 → 409 回滚，数据不变；
- 撤除无关记录 422、对可供数工时撤除 409，失败后数据保持原状；
- 正确撤除后目标恢复供数、返回恢复行号、其他可供数工时不受影响，审计落表；
- **并发纠错**：两个不同重复工时同时撤除均成功；同一工时两个并发撤除恰好一个成功，
  另一个基于已提交数据失败，最终数据自洽。

### 7.2 新增两阶段发布 e2e：`tests/run_e2e_concurrency.py`，全部断言通过（连跑 2 次稳定）

- **C 异月写入不被阻塞**：30000 行归科整月覆盖分块暂存期间，另一月份的工时追加与
  纠错请求均在覆盖线程结束前成功提交（`threading.Event` 证明），且耗时远小于覆盖总耗时；
  覆盖发布后 active 版本恰为整张 30000 行新表，无 staging/discarded 残留。
- **D 同月纠错先提交**：覆盖暂存期间同月纠错先 COMMIT，覆盖发布返回 **409 中文退回**；
  纠错撤除的改动保留、旧 active 表（撤除后 2 行）继续生效，staging 被标记废弃，
  当月随后可重新覆盖成功。
- **E 发布前失败/重启**：手工构造未发布 staging（1000 行 GHOST 数据），运行期供数
  完全看不到 GHOST；kill 进程重启后 staging 版本与行被启动 GC 清空，旧 active 整表完好，
  供数不变，当月重新覆盖成功。
- **F 发布事务常量写**：同进程计数 DML，100 行与 5000 行批次的发布事务均为恒定 3 条写操作。
- **G 旧库迁移**：用旧 schema 造库（含 `attribution` 旧表、跨两个月数据），启动后自动
  迁移为 active 版本并直接参与供数，旧表改名 `attribution_legacy` 留存；迁移后原接口
  追加/覆盖均可用；连续两次启动迁移幂等，部分唯一索引等 schema 完整。

### 7.3 补充黑盒验证（独立脚本）

- **追加先提交 / 整月清空先提交**：两类同月改动都使在途覆盖 409 退回，改动保留；
  清空后当月可重新覆盖。
- **两个并发整月覆盖**：恰好一个 200 成功、另一个 409 退回，胜出方整张表生效。
- 追加行号冲突 409、批内重复行号 422、响应结构与旧接口完全一致、无归科月份供数 no_path。
- `/docs` 与 `/openapi.json` 返回 200，归科与纠错端点均在 OpenAPI 中。
- `docker-compose.yml` YAML 解析通过，全部 Python 源 `py_compile` 通过。

## 八、余项

1. 当前环境无 Docker 守护进程，镜像未实际 `docker compose build`，仅做 compose/Dockerfile
   静态校验；逻辑已用同版本依赖在本机完整试跑，部署时执行 `docker compose up --build` 即可。
2. 入参为 JSON 批量录入；如需 Excel/CSV 导入可在现有服务上层加解析，表结构无需改动。
3. 认证鉴权、分页未纳入本次范围；服务默认监听 8000，建议部署在内网或网关之后。
4. 测试依赖仅用标准库；若团队引入 pytest，可将两个 `tests/run_e2e*.py` 的断言直接迁移。
5. 被取代版本（discarded）的物理清理在请求锁外与启动时分块进行；极端情况下若清理反复
   遇锁，残留会在下次同表写入或下次启动时继续清理，不影响供数正确性（查询只连 active）。
