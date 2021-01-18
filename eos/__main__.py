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

envparse.Env.read_envfile()


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
    sites = list(dss.get_delivery_sites(ctx.sess))
    for site in sites:
        print(
            json.dumps(
                {
                    "site_id": site.site_id,
                    "customer_id": site.customer_id,
                    "name": site.name or site.content_html,
                }
            )
        )


def parse_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()


@main.command(name="usage")
@click.option("-s", "--site", required=True)
@click.option("-c", "--customer", required=True)
@click.option("--start-date", type=parse_date)
@click.option("--end-date", type=parse_date)
@click.option(
    "--resolution",
    default="hourly",
    type=click.Choice(us.USAGE_RESOLUTION_CHOICES),
)
def get_usage(site, customer, start_date, end_date, resolution):
    if not end_date:
        end_date = datetime.date.today() - datetime.timedelta(days=1)
    if not start_date:
        start_date = end_date - datetime.timedelta(days=30)
    ctx: Context = click.get_current_context().meta["ecs"]
    do_login(ctx.sess, ctx.cfg)
    usage = us.get_usage(
        sess=ctx.sess,
        site_id=site,
        customer_id=customer,
        start_date=start_date,
        end_date=end_date,
        resolution=resolution,
    )
    print(json.dumps(usage.as_dict(), indent=2, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    main()
