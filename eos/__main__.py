import datetime
import json
import logging

import click
import envparse

import eos.scrape.delivery_sites as dss
import eos.scrape.usage as us
from eos.configuration import Configuration
from eos.context import Context
from eos.scrape.auth import do_login
from eos.utils import fix_date_defaults

envparse.Env.read_envfile()

log = logging.getLogger("eos")


@click.group()
@click.option("-u", "--username", envvar="EO_USERNAME", required=True)
@click.option("-p", "--password", envvar="EO_PASSWORD", required=True)
def main(*, username, password):
    cfg = Configuration(username=username, password=password)
    click.get_current_context().meta["ecs"] = Context(cfg=cfg)
    logging.basicConfig(level=logging.DEBUG)


@main.command(name="sites")
def list_delivery_sites():
    ctx: Context = click.get_current_context().meta["ecs"]
    do_login(ctx.sess, ctx.cfg)
    for site in dss.get_delivery_sites(ctx.sess):
        print(json.dumps(site.asdict()))


def parse_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()


@main.command(name="usage")
@click.option("-s", "--site", required=True)
@click.option("--start-date", type=parse_date)
@click.option("--end-date", type=parse_date)
@click.option(
    "--resolution",
    default="hourly",
    type=click.Choice(["daily", "hourly"]),
)
def get_usage(site: str, start_date, end_date, resolution):
    start_date, end_date = fix_date_defaults(start_date, end_date)
    ctx: Context = click.get_current_context().meta["ecs"]
    do_login(ctx.sess, ctx.cfg)
    site_obj = next(
        (s for s in dss.get_delivery_sites(ctx.sess) if s.metering_point_code == site),
        None,
    )
    if not site_obj:
        raise ValueError(f"Site not found for MPC {site}")

    usage = us.get_usage(
        sess=ctx.sess,
        site=site_obj,
    )

    usage_data = (
        usage.daily_usage_data if resolution == "daily" else usage.hourly_usage_data
    )
    start_datetime = datetime.datetime.combine(
        date=start_date, time=datetime.time(0, 0, 0)
    )
    end_datetime = datetime.datetime.combine(
        date=end_date, time=datetime.time(23, 59, 59)
    )
    for ts, datum in sorted(usage_data.items()):
        if start_datetime <= ts <= end_datetime:
            print(json.dumps(datum.as_dict()))


@main.command(name="update_database")
@click.option("-s", "--site", envvar="EO_SITE_ID", required=True)
@click.option("-c", "--customer", envvar="EO_CUSTOMER_ID", required=True)
@click.option(
    "--db", "--database-url", "database_url", envvar="EO_DATABASE_URL", required=True
)
@click.option("--start-date", type=parse_date)
@click.option("--end-date", type=parse_date)
@click.option("--back-days", type=int, default=7)
def update_database(site, customer, database_url, start_date, end_date, back_days):
    start_date, end_date = fix_date_defaults(start_date, end_date, back_days=back_days)
    log.info(f"Requesting and updating usage for {start_date}..{end_date}")
    ctx: Context = click.get_current_context().meta["ecs"]
    import sqlalchemy
    import eos.database as ed

    engine = sqlalchemy.create_engine(database_url)
    metadata = ed.get_metadata(engine)
    metadata.create_all()
    extant_dates = ed.find_extant_dates(
        metadata,
        start_date=start_date,
        end_date=end_date,
        customer_id=customer,
        site_id=site,
    )
    if len(extant_dates) >= (end_date - start_date).days:
        log.info("Nothing to do, all data already found.")
        return
    if extant_dates:
        log.info(f"Will skip requests for {len(extant_dates)} previously fetched dates")

    do_login(ctx.sess, ctx.cfg)
    usage = us.get_usage(
        sess=ctx.sess,
        site_id=site,
        customer_id=customer,
        start_date=start_date,
        end_date=end_date,
        resolution="hourly",
        date_filter=lambda date: date not in extant_dates,
    )
    if usage.data:
        ed.populate_usage(metadata, usage)


if __name__ == "__main__":
    main()
