import datetime
from typing import Tuple, Optional

import requests

from eos.models import DeliverySite
from eos.scrape import delivery_sites as dss


def fix_date_defaults(
    start_date: Optional[datetime.date],
    end_date: Optional[datetime.date],
    back_days: int = 30,
) -> Tuple[datetime.date, datetime.date]:
    if not end_date:
        end_date = datetime.date.today() - datetime.timedelta(days=1)
    if not start_date:
        start_date = end_date - datetime.timedelta(days=back_days)
    return start_date, end_date


def find_site_with_code(sess: requests.Session, metering_point_code: str) -> DeliverySite:
    for site in dss.get_delivery_sites(sess):
        if site.metering_point_code == metering_point_code:
            return site
    raise ValueError(f"Site not found for MPC {metering_point_code}")
