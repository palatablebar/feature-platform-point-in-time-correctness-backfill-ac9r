"""Feature backfill task + full/incremental consistency result.

一个回填任务两部分产物：

* ``result_json`` 仍保存 agg_engine 的 full/incremental 一致性报告（checksum、
  差异样本、负例对照）；
* 逐样本 × 特征的定向回填结果落在 :class:`BackfillResult`，由版本化写入层
  （``services/backfill_apply.py``）按「特征版本生效时间 + 样本标签时间」
  判定 replaced / retained / skipped_stale / skipped_duplicate。
"""
from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from src.config.database import Base


class BackfillTask(Base):
    __tablename__ = "backfill_task"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(48), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default="full")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    range_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    range_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    base_watermark: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    feature_ids: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    # 任务发起时每个目标特征锁定的版本：{str(feature_id): version_no}。
    # 旧回填迟到时凭它与样本当前值的版本比较，决定保留还是替换。
    feature_versions: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    full_checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    incremental_checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    invariant_hold: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    result_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_by: Mapped[str] = mapped_column(String(160), nullable=False, default="system")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_seed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BackfillResult(Base):
    """逐样本 × 特征的定向回填结果（版本化写入层的审计台账）。

    同一任务重复运行不新增行（``uq_backfill_result`` 兜底），也不覆盖已有行；
    页面据此核对每个样本行的替换 / 保留 / 冲突状态。
    """

    __tablename__ = "backfill_result"
    __table_args__ = (
        UniqueConstraint("backfill_id", "sample_row_id", "feature_code", name="uq_backfill_result"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    backfill_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("backfill_task.id"), nullable=False, index=True
    )
    sample_row_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    job_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    label_row_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    entity_key: Mapped[str] = mapped_column(String(48), nullable=False)
    label_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    feature_id: Mapped[int] = mapped_column(Integer, nullable=False)
    feature_code: Mapped[str] = mapped_column(String(64), nullable=False)
    # 本次回填候选值及其口径版本
    candidate_value: Mapped[float] = mapped_column(Float, nullable=False)
    candidate_version_no: Mapped[int] = mapped_column(Integer, nullable=False)
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # 判定时样本上的当前值与当前版本（审计快照，不随后续回填变化）
    previous_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    previous_version_no: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, index=True)
    is_conflict: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    decision_reason: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
