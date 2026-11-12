"""Engine / session factory shared by the API and the Celery worker."""
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from src.config.settings import settings


class Base(DeclarativeBase):
    pass


engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=8,
    max_overflow=4,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db(max_attempts: int = 30, delay: float = 2.0) -> None:
    """Create all tables, waiting for PostgreSQL to accept connections."""
    import src.models  # noqa: F401  (register mappers)

    last_error = None
    for _ in range(max_attempts):
        try:
            Base.metadata.create_all(bind=engine)
            _apply_lightweight_migrations()
            return
        except Exception as exc:  # pragma: no cover - infra retry
            last_error = exc
            time.sleep(delay)
    raise RuntimeError(f"database bootstrap failed: {last_error}")


def _apply_lightweight_migrations() -> None:
    """幂等补列：``create_all`` 只建新表，已存在表的新列需要手动 ALTER。

    用 information_schema 判断列是否存在，重复执行无副作用；演示环境无正式
    迁移工具，这是 README 约定的「参考 DDL + create_all」之外的最小补充。
    """
    from sqlalchemy import text

    added_columns = [
        ("feature_version", "effective_from", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
        ("sample_row", "feature_provenance", "JSON NOT NULL DEFAULT '{}'"),
        ("backfill_task", "feature_versions", "JSON NOT NULL DEFAULT '{}'"),
    ]
    with engine.begin() as conn:
        for table, column, ddl_type in added_columns:
            exists = conn.execute(
                text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name = :table AND column_name = :column"
                ),
                {"table": table, "column": column},
            ).scalar()
            if not exists:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}"))
