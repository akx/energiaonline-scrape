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
from eos.utils import fix_date_defaults, find_site_with_code

envparse.Env.read_envfile()

log = logging.getLogger("eos")


@click.group()
@click.option("-u", "--username", envvar="EO_USERNAME", required=True)
@click.option("-p", "--password", envvar="EO_PASSWORD", required=True)
@click.option("-du", "--delegate-user", envvar="EO_DELEGATE_USERNAME")
def main(*, username, password, delegate_user):
    cfg = Configuration(
        username=username,
        password=password,
        delegate_username=delegate_user,
    )
    click.get_current_context().meta["ecs"] = Context(cfg=cfg)
    logging.basicConfig(level=logging.DEBUG)


@main.command(name="sites")
def list_delivery_sites():
    ctx: Context = click.get_current_context().meta["ecs"]
    do_login(ctx.sess, ctx.cfg)
    sites = list(dss.get_delivery_sites(ctx.sess))
    if not sites:
        raise click.ClickException("No delivery sites found")
    for site in sites:
        print(json.dumps(site.asdict()))


def parse_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()


@main.command(name="usage")
@click.option("-s", "--site", "site_id", required=True)
@click.option("--start-date", type=parse_date)
@click.option("--end-date", type=parse_date)
@click.option(
    "--resolution",
    default="hourly",
    type=click.Choice(["daily", "hourly"]),
)
def get_usage(site_id: str, start_date, end_date, resolution):
    start_date, end_date = fix_date_defaults(start_date, end_date)
    ctx: Context = click.get_current_context().meta["ecs"]
    do_login(ctx.sess, ctx.cfg)
    site = find_site_with_code(ctx.sess, site_id)

    usage = us.get_usage(
        sess=ctx.sess,
        site=site,
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
@click.option("-s", "--site", "site_id", envvar="EO_SITE_ID", required=True)
@click.option(
    "--db", "--database-url", "database_url", envvar="EO_DATABASE_URL", required=True
)
def update_database(site_id, database_url):
    ctx: Context = click.get_current_context().meta["ecs"]
    import sqlalchemy
    import eos.database as ed

    engine = sqlalchemy.create_engine(database_url)
    metadata = ed.get_metadata(engine)
    metadata.create_all()
    do_login(ctx.sess, ctx.cfg)
    site = find_site_with_code(ctx.sess, metering_point_code=site_id)
    usage = us.get_usage(
        sess=ctx.sess,
        site=site,
    )
    log.info(
        f"Hourly usage data entries: {len(usage.hourly_usage_data)}: "
        f"{min(usage.hourly_usage_data)} .. {max(usage.hourly_usage_data)}"
    )
    ed.populate_usage(metadata, usage)


if __name__ == "__main__":
    main()
