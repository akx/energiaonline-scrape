import datetime

import sqlalchemy as sa
from sqlalchemy import func

from eos.models import UsageData


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
    assert usage.resolution == "hourly"
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
    customer_id: str,
    site_id: str,
):
    data_table: sa.Table = metadata.tables[metadata.data_table_name]
    ts0 = datetime.datetime.combine(start_date, datetime.time()).timestamp()
    ts1 = datetime.datetime.combine(end_date, datetime.time(23, 59, 59)).timestamp()
    q = (
        sa.sql.select([sa.cast(data_table.c.dt, sa.Date)])
        .where(
            data_table.c.timestamp.between(ts0, ts1)
            & (data_table.c.customer_id == (customer_id))
            & (data_table.c.site_id == (site_id))
        )
        .distinct()
    )
    return {r[0] for r in metadata.bind.connect().execute(q)}


def generate_sql_params(usage: UsageData):
    for data in usage.data:
        data = {
            k: v
            for (k, v) in data.items()
            if not (k.endswith("_ROUND") or k.endswith("_UNIT"))
        }
        date = datetime.datetime.fromisoformat(data.pop("date"))
        start_date = datetime.datetime.fromisoformat(data.pop("startDate"))
        end_date = datetime.datetime.fromisoformat(data.pop("endDate"))
        assert (end_date - start_date).total_seconds() == 3600
        assert date == start_date
        utc_ts = date.astimezone(datetime.timezone.utc)
        yield {
            "site_id": usage.site_id,
            "customer_id": usage.customer_id,
            "timestamp": utc_ts.timestamp(),
            "dt": date,
            "data": data,
            "temperature": data.get("TEMP"),
            "consumption": data.get("PS"),
            "spot_price": data.get("SPOT"),
        }
