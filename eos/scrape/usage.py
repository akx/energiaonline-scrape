import datetime
import json
import logging
from typing import Iterable, Optional, Callable

import requests
from dateutil.relativedelta import relativedelta

from eos.models import UsageData
from eos.scrape.delivery_sites import get_delivery_sites
from eos.scrape.utils import get_reporting_token

log = logging.getLogger(__name__)


def iter_dates(
    d1: datetime.date, d2: datetime.date, **delta_kwargs
) -> Iterable[datetime.date]:
    d = d1
    dt = relativedelta(**delta_kwargs)
    while d <= d2:
        yield d
        d += dt


USAGE_RESOLUTION_CHOICES = {
    "hourly",
    "daily",
}


def get_usage(
    *,
    sess: requests.Session,
    site_id: str,
    customer_id: str,
    resolution: str,
    start_date: datetime.date,
    end_date: datetime.date,
    date_filter: Optional[Callable[[datetime.date], bool]] = None,
) -> UsageData:
    token = get_reporting_token(sess)
    list(get_delivery_sites(sess, token))  # required for correct sequence, who knows...
    headers = {
        "Accept": "*/*",
        "Origin": "https://www.energiaonline.fi",
        "Referer": "https://www.energiaonline.fi/EnergyReporting/EnergyReporting",
        "X-Requested-With": "XMLHttpRequest",
        "__RequestVerificationToken": token,
    }

    log.info("Making ShowDeliverysite request...")
    sess.cookies.set("SelectedLanguage", "fi")

    resp = sess.post(
        url="https://www.energiaonline.fi/EnergyReporting/ShowDeliverysite",
        headers=headers,
        data={
            "code": site_id,
            "customerCode": customer_id,
        },
    )
    resp.raise_for_status()
    delivery_site_info = resp.json()

    datums = []
    if resolution == "hourly":
        date_iter = iter_dates(start_date, end_date, days=1)
        get_data = lambda date: {
            "view": "DayP",
            "resolution": "",
            "options": "SPOT,TEMP",
            "dateStart": date.isoformat(),
            "dateEnd": date.isoformat(),
        }
    elif resolution == "daily":
        date_iter = iter_dates(start_date, end_date, months=1)
        get_data = lambda date: {
            "view": "MonthP",
            "resolution": "",
            "options": "SPOT,TEMP",
            "dateStart": date.isoformat(),
            "dateEnd": (date + relativedelta(months=1)).isoformat(),
        }
    else:
        raise ValueError(f"Unknown resolution {resolution}")

    for i, date in enumerate(date_iter, 1):
        if date_filter and not date_filter(date):
            log.info(f"Skipping request for {date}")
            continue
        log.info(f"Making {resolution} ShowReportingView request {i} ({date})...")
        resp = sess.post(
            url="https://www.energiaonline.fi/EnergyReporting/ShowReportingView",
            headers=headers,
            data=get_data(date),
        )
        resp.raise_for_status()
        report_data = resp.json()
        cfg = json.loads(report_data["config"])
        datums.extend(cfg["dataProvider"])

    return UsageData(
        customer_id=customer_id,
        site_id=site_id,
        resolution=resolution,
        data=datums,
        delivery_site_info=delivery_site_info,
    )
