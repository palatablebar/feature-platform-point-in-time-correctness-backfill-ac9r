# migrations

本目录保存表结构快照与演进说明。

* 运行期建表：`src/config/database.py:init_db()` 用 SQLAlchemy 2.0 `create_all`
  在容器启动时幂等建表（演示环境；生产应改为版本化迁移）。
* `0001_initial.sql` 是同一套模型的**参考 DDL**，用于评审对照与手工重建，
  不参与容器启动流程。

新增字段时：先改 `src/models/*.py`，再往本目录追加 `000N_*.sql` 快照并更新
本文件，保证「模型 ↔ 迁移」可追溯。

演进记录：

* `0002_backfill_versioning.sql` — 版本化定向回填：
  `feature_version.effective_from`、`sample_row.feature_provenance`、
  `backfill_task.feature_versions` 三个新列 + `backfill_result` 台账表。
  运行期由 `database.py:_apply_lightweight_migrations()` 对已存在的库幂等
  补列（information_schema 判断）；新表由 `create_all` 直接创建。
