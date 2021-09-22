import datetime

import sqlalchemy as sa
from sqlalchemy import func

from eos.models import UsageData, UsageDataPoint


def patch_sqlite_on_conflict_do_nothing():
    # H/t https://stackoverflow.com/a/64902371/51685
    from sqlalchemy.ext.compiler import compiles
    from sqlalchemy.sql import Insert

    @compiles(Insert, "sqlite")
    def suffix_insert(insert, compiler, **kwargs):
        stmt = compiler.visit_insert(insert, **kwargs)
        if insert.dialect_kwargs.get("sqlite_on_conflict_do_nothing"):
            stmt += " ON CONFLICT DO NOTHING"
        return stmt

    Insert.argument_for("sqlite", "on_conflict_do_nothing", False)


def get_metadata(bind=None, data_table_name="eos_data"):
    metadata = sa.MetaData(bind=bind)
    sa.Table(
        data_table_name,
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("site_id", sa.Integer),
        sa.Column("customer_id", sa.Integer),
        sa.Column("timestamp", sa.Integer),
        sa.Column("dt", sa.DateTime),
        sa.Column("temperature", sa.Float, nullable=True),
        sa.Column("consumption", sa.Float, nullable=True),
        sa.Column("spot_price", sa.Float, nullable=True),
        sa.Column("data", sa.JSON),
        sa.UniqueConstraint("site_id", "customer_id", "timestamp"),
    )
    metadata.data_table_name = data_table_name
    return metadata


def populate_usage(metadata: sa.MetaData, usage: UsageData):
    engine = metadata.bind
    data_table: sa.Table = metadata.tables[metadata.data_table_name]
    conn = engine.connect()
    kwargs = {}
    if engine.dialect.name == "sqlite":
        patch_sqlite_on_conflict_do_nothing()
        kwargs["sqlite_on_conflict_do_nothing"] = True
    return conn.execute(data_table.insert(**kwargs), list(generate_sql_params(usage)))


def find_extant_dates(
    metadata: sa.MetaData,
    start_date: datetime.date,
    end_date: datetime.date,
    site_id: str,
):
    data_table: sa.Table = metadata.tables[metadata.data_table_name]
    ts0 = datetime.datetime.combine(start_date, datetime.time()).timestamp()
    ts1 = datetime.datetime.combine(end_date, datetime.time(23, 59, 59)).timestamp()
    q = (
        sa.sql.select([sa.cast(data_table.c.dt, sa.Date)])
        .where(
            data_table.c.timestamp.between(ts0, ts1)
            & (data_table.c.site_id == (site_id))
        )
        .distinct()
    )
    return {r[0] for r in metadata.bind.connect().execute(q)}


def generate_sql_params(usage: UsageData):
    point: UsageDataPoint
    for _, point in sorted(usage.hourly_usage_data.items()):
        yield {
            "site_id": usage.site.metering_point_code,
            "customer_id": 0,
            "timestamp": point.timestamp,
            "dt": point.timestamp.date(),
            "data": point.as_dict(),
            "temperature": point.temperature,
            "consumption": point.usage,
            "spot_price": None,  # TODO: Not available in EOv2 yet...
        }
