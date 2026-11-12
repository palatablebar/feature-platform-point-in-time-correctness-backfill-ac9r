-- 版本化定向回填（feature version effective_from + backfill_result 台账）
-- 面向已存在的 PostgreSQL 库；全新库由 SQLAlchemy create_all 直接建出新结构。
-- 全部语句幂等：列/表存在时跳过，可重复执行。

-- 1) 特征版本生效时间：回填只对 label_time >= effective_from 的样本应用该版本
ALTER TABLE feature_version ADD COLUMN IF NOT EXISTS effective_from TIMESTAMPTZ NOT NULL DEFAULT now();

-- 2) 样本行逐特征列的版本溯源
ALTER TABLE sample_row ADD COLUMN IF NOT EXISTS feature_provenance JSON NOT NULL DEFAULT '{}';

-- 3) 回填任务锁定的目标特征版本：{ "feature_id": version_no }
ALTER TABLE backfill_task ADD COLUMN IF NOT EXISTS feature_versions JSON NOT NULL DEFAULT '{}';

-- 4) 逐样本 × 特征的回填生效台账（同任务重复运行不新增、不覆盖）
CREATE TABLE IF NOT EXISTS backfill_result (
    id                   SERIAL PRIMARY KEY,
    backfill_id          INTEGER NOT NULL REFERENCES backfill_task(id),
    sample_row_id        INTEGER NOT NULL,
    job_id               INTEGER NOT NULL,
    label_row_id         INTEGER NOT NULL,
    entity_key           VARCHAR(48) NOT NULL,
    label_time           TIMESTAMPTZ NOT NULL,
    feature_id           INTEGER NOT NULL,
    feature_code         VARCHAR(64) NOT NULL,
    candidate_value      DOUBLE PRECISION NOT NULL,
    candidate_version_no INTEGER NOT NULL,
    effective_from       TIMESTAMPTZ NOT NULL,
    previous_value       DOUBLE PRECISION,
    previous_version_no  INTEGER,
    status               VARCHAR(24) NOT NULL,
    is_conflict          BOOLEAN NOT NULL DEFAULT false,
    decision_reason      VARCHAR(255) NOT NULL DEFAULT '',
    applied_at           TIMESTAMPTZ,
    created_at           TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_backfill_result UNIQUE (backfill_id, sample_row_id, feature_code)
);

CREATE INDEX IF NOT EXISTS ix_backfill_result_task ON backfill_result (backfill_id);
CREATE INDEX IF NOT EXISTS ix_backfill_result_status ON backfill_result (status);
CREATE INDEX IF NOT EXISTS ix_backfill_result_conflict ON backfill_result (is_conflict);

-- 历史数据补录：把已有特征版本的生效时间锚到其创建时刻（默认 now() 已满足，
-- 这里仅显式兜底 NULL 场景；NOT NULL 约束下通常无行可更新）。
UPDATE feature_version SET effective_from = created_at WHERE effective_from IS NULL;
