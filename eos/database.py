import sqlalchemy as sa

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


def generate_sql_params(usage: UsageData):
    point: UsageDataPoint
    for _, point in sorted(usage.hourly_usage_data.items()):
        yield {
            "site_id": usage.site.metering_point_code,
            "customer_id": 0,
            "timestamp": int(point.timestamp.timestamp()),
            "dt": point.timestamp.date(),
            "data": point.as_dict(),
            "temperature": point.temperature,
            "consumption": point.usage,
            "spot_price": None,  # TODO: Not available in EOv2 yet...
        }
