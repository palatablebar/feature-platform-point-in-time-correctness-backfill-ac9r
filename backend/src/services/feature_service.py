"""Feature definition lifecycle: create / edit / publish / version trail.

Rules:
* ``code`` is the natural key (seed upsert + API uniqueness).
* every edit bumps ``version`` and appends an immutable ``feature_version`` row;
* publish flips ``status`` to ``published`` and appends a release row;
* published definitions cannot be silently edited — ``PATCH`` is allowed but
  the caller must pass ``change_note`` (the audit trail keeps the old snapshot).
"""
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.constants.enums import BoundaryPolicy, FailureReason, FeatureStatus
from src.constants.error_codes import AppError, ErrorCode
from src.models import FeatureDef, FeatureVersion
from src.services.pit_engine import FeatureSpec
from src.utils.logger import log_event
from src.utils.serialize import feature_out, feature_version_out
from src.utils.timefmt import ensure_utc, iso_z, utcnow

WINDOW_SECONDS = {"1d": 86400, "7d": 7 * 86400, "30d": 30 * 86400}


def list_features(db: Session) -> list[FeatureDef]:
    return list(db.scalars(select(FeatureDef).order_by(FeatureDef.id)))


def get_feature(db: Session, feature_id: int) -> FeatureDef:
    feature = db.get(FeatureDef, feature_id)
    if feature is None:
        raise AppError(ErrorCode.FEATURE_NOT_FOUND, detail={"feature_id": feature_id})
    return feature


def snapshot(feature: FeatureDef) -> dict:
    return {
        "id": feature.id,
        "code": feature.code,
        "name": feature.name,
        "event_type": feature.event_type,
        "amount_min": float(feature.amount_min) if feature.amount_min is not None else None,
        "window_seconds": feature.window_seconds,
        "window_label": feature.window_label,
        "boundary_policy": feature.boundary_policy,
        "aggregate_kind": feature.aggregate_kind,
        "status": feature.status,
        "version": feature.version,
    }


def create_feature(db: Session, payload, user) -> FeatureDef:
    code = payload.code
    if db.scalar(select(FeatureDef).where(FeatureDef.code == code)) is not None:
        raise AppError(ErrorCode.FEATURE_CODE_DUPLICATED, detail={"code": code})
    window_seconds = WINDOW_SECONDS[payload.window_label]
    if window_seconds <= 0:
        raise AppError(ErrorCode.FEATURE_WINDOW_INVALID)
    feature = FeatureDef(
        code=code,
        name=payload.name,
        description=payload.description,
        event_type=payload.event_type.value if payload.event_type else None,
        amount_min=payload.amount_min,
        window_seconds=window_seconds,
        window_label=payload.window_label,
        boundary_policy=payload.boundary_policy.value,
        aggregate_kind=payload.aggregate_kind.value,
        status=FeatureStatus.DRAFT.value,
        version=1,
        owner_id=user.id,
    )
    db.add(feature)
    db.flush()
    db.add(
        FeatureVersion(
            feature_id=feature.id,
            version_no=1,
            snapshot=snapshot(feature),
            change_note="初始版本",
            changed_by=user.email,
            effective_from=utcnow(),
        )
    )
    db.commit()
    db.refresh(feature)
    log_event("FEATURE_CREATED", code=feature.code, by=user.email)
    return feature


def _append_version(
    feature: FeatureDef,
    user_email: str,
    change_note: str,
    effective_from=None,
) -> FeatureVersion:
    """追加一条不可变版本快照；``effective_from`` 缺省取当前 UTC 时间。"""
    return FeatureVersion(
        feature_id=feature.id,
        version_no=feature.version,
        snapshot=snapshot(feature),
        change_note=change_note,
        changed_by=user_email,
        effective_from=effective_from or utcnow(),
    )


def update_feature(db: Session, feature_id: int, payload, user, request=None) -> FeatureDef:
    feature = get_feature(db, feature_id)
    before = snapshot(feature)
    changes = payload.model_dump(exclude_unset=True)
    change_note = changes.pop("change_note", "") or "更新特征定义"
    if "window_label" in changes and changes["window_label"]:
        feature.window_label = changes["window_label"]
        feature.window_seconds = WINDOW_SECONDS[changes["window_label"]]
    if "event_type" in changes:
        value = changes["event_type"]
        feature.event_type = value.value if hasattr(value, "value") else value
    if "amount_min" in changes:
        feature.amount_min = changes["amount_min"]
    if "boundary_policy" in changes and changes["boundary_policy"]:
        value = changes["boundary_policy"]
        feature.boundary_policy = value.value if hasattr(value, "value") else value
        if feature.boundary_policy == BoundaryPolicy.STRICT.value and before["boundary_policy"] != feature.boundary_policy:
            change_note = change_note or "修正边界策略为 strict（排除标签时刻）"
    if "aggregate_kind" in changes and changes["aggregate_kind"]:
        value = changes["aggregate_kind"]
        feature.aggregate_kind = value.value if hasattr(value, "value") else value
    for text_field in ("name", "description"):
        if text_field in changes and changes[text_field] is not None:
            setattr(feature, text_field, changes[text_field])

    feature.version = int(feature.version) + 1
    feature.updated_at = utcnow()
    effective_from = None
    if getattr(payload, "effective_from", None):
        from src.utils.timefmt import BadTimestamp, parse_ts

        try:
            effective_from = parse_ts(payload.effective_from)
        except BadTimestamp as exc:
            raise AppError(
                ErrorCode.BAD_TIMESTAMP,
                message="effective_from 无法解析为时间戳",
                detail={"raw": payload.effective_from},
            ) from exc
    db.add(_append_version(feature, user.email, change_note, effective_from))
    db.commit()
    db.refresh(feature)
    log_event(
        "FEATURE_UPDATED",
        code=feature.code,
        version=feature.version,
        boundary=feature.boundary_policy,
        by=user.email,
    )
    return feature


def publish_feature(db: Session, feature_id: int, user) -> FeatureDef:
    feature = get_feature(db, feature_id)
    feature.status = FeatureStatus.PUBLISHED.value
    feature.published_at = utcnow()
    feature.version = int(feature.version) + 1
    db.add(_append_version(feature, user.email, "发布到样本构建可用", utcnow()))
    db.commit()
    db.refresh(feature)
    log_event("FEATURE_PUBLISHED", code=feature.code, version=feature.version, by=user.email)
    return feature


def versions(db: Session, feature_id: int) -> list[FeatureVersion]:
    return list(
        db.scalars(
            select(FeatureVersion)
            .where(FeatureVersion.feature_id == feature_id)
            .order_by(FeatureVersion.version_no.desc())
        )
    )


def to_specs(db: Session, feature_ids: list[int]) -> list[FeatureSpec]:
    features = [get_feature(db, feature_id) for feature_id in feature_ids]
    if not features:
        raise AppError(ErrorCode.FEATURE_WINDOW_INVALID, message="未选择任何特征", status_code=400)
    specs: list[FeatureSpec] = []
    for feature in features:
        if feature.window_seconds <= 0:
            raise AppError(ErrorCode.FEATURE_WINDOW_INVALID, detail={"code": feature.code})
        specs.append(
            FeatureSpec(
                id=feature.id,
                code=feature.code,
                name=feature.name,
                window_seconds=int(feature.window_seconds),
                window_label=feature.window_label,
                boundary=feature.boundary_policy,
                aggregate=feature.aggregate_kind,
                event_type=feature.event_type,
                amount_min=float(feature.amount_min) if feature.amount_min is not None else None,
                description=feature.description,
            )
        )
    return specs


def snapshot_for_job(features: list[FeatureSpec]) -> list[dict]:
    return [
        {
            "code": spec.code,
            "name": spec.name,
            "window_label": spec.window_label,
            "window_seconds": spec.window_seconds,
            "boundary_policy": spec.boundary,
            "aggregate_kind": spec.aggregate,
            "event_type": spec.event_type,
            "amount_min": spec.amount_min,
        }
        for spec in features
    ]


def validate_feature_ids(db: Session, feature_ids: list[int]) -> list[FeatureDef]:
    features = list(db.scalars(select(FeatureDef).where(FeatureDef.id.in_(feature_ids))))
    found = {feature.id for feature in features}
    missing = [fid for fid in feature_ids if fid not in found]
    if missing:
        raise AppError(
            ErrorCode.FEATURE_NOT_FOUND,
            message="部分特征定义不存在",
            detail={"missing_feature_ids": missing},
        )
    return features


def ensure_runnable(features: list[FeatureDef]) -> None:
    if not features:
        raise AppError(ErrorCode.FEATURE_WINDOW_INVALID, message="未选择任何特征")
    for feature in features:
        if feature.window_seconds <= 0:
            raise AppError(ErrorCode.FEATURE_WINDOW_INVALID, detail={"code": feature.code})
    if len(features) > 12:
        raise AppError(ErrorCode.FEATURE_WINDOW_INVALID, message="一次最多 12 个特征")
    if not features:
        raise AppError(ErrorCode.FEATURE_WINDOW_INVALID, message=FailureReason.NO_FEATURE.value)


def out(feature: FeatureDef) -> dict:
    return feature_out(feature)


def version_out(version: FeatureVersion) -> dict:
    return feature_version_out(version)


# --------------------------------------------------------------------------- #
# 版本生效时间（effective_from）解析：回填「按特征版本和样本标签时间判定何时生效」
# --------------------------------------------------------------------------- #
def version_at_label_time(db: Session, feature_id: int, label_time) -> FeatureVersion:
    """返回 ``effective_from <= label_time`` 的最新版本；没有则回退到最早版本。

    回退分支兼容历史数据（旧版本行可能没有显式生效时间，由迁移/seed 补成
    特征创建时刻），保证任何样本都能解析到一个口径版本。
    """
    label_time = ensure_utc(label_time)
    versions = list(
        db.scalars(
            select(FeatureVersion)
            .where(FeatureVersion.feature_id == feature_id)
            .order_by(FeatureVersion.version_no.asc())
        )
    )
    if not versions:
        raise AppError(ErrorCode.FEATURE_NOT_FOUND, detail={"feature_id": feature_id})
    applicable = [version for version in versions if ensure_utc(version.effective_from) <= label_time]
    return applicable[-1] if applicable else versions[0]


def latest_version(db: Session, feature_id: int) -> FeatureVersion:
    version = db.scalar(
        select(FeatureVersion)
        .where(FeatureVersion.feature_id == feature_id)
        .order_by(FeatureVersion.version_no.desc())
        .limit(1)
    )
    if version is None:
        raise AppError(ErrorCode.FEATURE_NOT_FOUND, detail={"feature_id": feature_id})
    return version


def spec_from_version(version: FeatureVersion) -> FeatureSpec:
    """把版本快照还原成 pit_engine 可计算的 FeatureSpec。"""
    snap = version.snapshot or {}
    return FeatureSpec(
        id=int(snap.get("id") or 0),
        code=str(snap["code"]),
        name=str(snap.get("name", snap["code"])),
        window_seconds=int(snap["window_seconds"]),
        window_label=str(snap["window_label"]),
        boundary=str(snap["boundary_policy"]),
        aggregate=str(snap["aggregate_kind"]),
        event_type=snap.get("event_type"),
        amount_min=snap.get("amount_min"),
        description=str(snap.get("description", "")),
    )
