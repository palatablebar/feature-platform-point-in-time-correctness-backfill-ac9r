"""Backfill task DTOs."""
from pydantic import BaseModel, Field

from src.constants.enums import BackfillMode


class BackfillCreate(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    mode: BackfillMode = BackfillMode.FULL
    range_start: str
    range_end: str
    feature_ids: list[int] = Field(default_factory=list, max_length=12)


class BackfillOut(BaseModel):
    id: int
    code: str
    name: str
    mode: str
    status: str
    range_start: str | None
    range_end: str | None
    base_watermark: str | None
    feature_ids: list[int]
    feature_versions: dict[str, int] = Field(default_factory=dict)
    full_checksum: str | None
    incremental_checksum: str | None
    invariant_hold: bool | None
    created_by: str
    started_at: str | None
    finished_at: str | None
    created_at: str | None
    report: dict | None = None


class BackfillResultOut(BaseModel):
    id: int
    backfill_id: int
    sample_row_id: int
    job_id: int
    label_row_id: int
    entity_key: str
    label_time: str | None
    feature_id: int
    feature_code: str
    candidate_value: float
    candidate_version_no: int
    effective_from: str | None
    previous_value: float | None
    previous_version_no: int | None
    status: str
    is_conflict: bool
    decision_reason: str
    applied_at: str | None
    created_at: str | None
