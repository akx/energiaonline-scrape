import datetime
import json
import logging
import re
from typing import Any, Dict, Iterator, Tuple

import requests

from eos.models import UsageData, DeliverySite, UsageDataPoint

log = logging.getLogger(__name__)


def parse_js_variable(html: str, name: str) -> Any:
    match = re.search(f"var {name} = (.+?);", html, flags=re.MULTILINE)
    if not match:
        raise ValueError(f"No match for {name}")

    data = match.group(1)

    # Replace new Date invocations with bare numbers
    data = re.sub(r"new Date\(([-0-9]+)\)", r"\1", data)

    return json.loads(data)


def parse_series(
    series: dict, *, truncate_to_day: bool = False
) -> Iterator[Tuple[datetime.datetime, Any]]:
    for timestamp, value in series.get("Data", []):
        ts = datetime.datetime.fromtimestamp(timestamp / 1000)
        if truncate_to_day:
            ts = ts.replace(hour=0, minute=0, second=0, microsecond=0)
        yield (ts, value)


def parse_usage_data(
    model_data: dict, bucket: str
) -> Dict[datetime.datetime, UsageDataPoint]:
    # TODO: handle timezone information?
    truncate_to_day = bucket != "Hours"
    bucketed_data = model_data[bucket]
    temp_series = bucketed_data.get("Temperature")
    temperatures = dict(parse_series(temp_series, truncate_to_day=truncate_to_day))
    hourly_cons_and_temp = {}
    for cons in bucketed_data["Consumptions"]:
        series = cons["Series"]
        for timestamp, value in parse_series(series, truncate_to_day=truncate_to_day):
            hourly_cons_and_temp[timestamp] = UsageDataPoint(
                resolution=series["Resolution"],
                timestamp=timestamp,
                usage=value,
                temperature=temperatures.get(timestamp),
            )
    return hourly_cons_and_temp


def get_usage(
    *,
    sess: requests.Session,
    site: DeliverySite,
) -> UsageData:
    resp = sess.get(
        url=f"https://energiaonline.turkuenergia.fi/Reporting/CustomerConsumption?meteringPointCode={site.metering_point_code}&mpSourceCompanyCode={site.source_company_code}&networkCode={site.network_company_code}&loadLastYearData=True",
    )
    resp.raise_for_status()
    text = resp.content.decode("UTF-8")
    model_data = parse_js_variable(text, "model")
    return UsageData(
        site=site,
        hourly_usage_data=parse_usage_data(model_data, bucket="Hours"),
        daily_usage_data=parse_usage_data(model_data, bucket="Days"),
    )
