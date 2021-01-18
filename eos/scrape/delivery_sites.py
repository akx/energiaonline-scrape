import logging
from typing import Optional, Iterable

import bs4
import requests

from eos.models import DeliverySite
from eos.scrape.utils import get_reporting_token

log = logging.getLogger(__name__)


def get_delivery_sites(
    sess: requests.Session, token: Optional[str] = None
) -> Iterable[DeliverySite]:
    if not token:
        token = get_reporting_token(sess)
    log.info("Loading delivery sites...")
    resp = sess.post(
        url="https://www.energiaonline.fi/EnergyReporting/GetDeliverysites",
        headers={
            "__requestverificationtoken": token,
            "Referer": "https://www.energiaonline.fi/EnergyReporting/EnergyReporting",
        },
        data={
            "showHistorical": "false",
            "fetchSharedContractDeliverysites": "true",
        },
    )
    resp.raise_for_status()
    json = resp.json()
    d = bs4.BeautifulSoup(json["Content"], features="html.parser")
    opt: bs4.Tag
    for opt in d.find_all("option"):
        if "data-customer" in opt.attrs and "data-deliverysite" in opt.attrs:
            yield DeliverySite(
                customer_id=opt.attrs["data-customer"],
                site_id=opt.attrs["data-deliverysite"],
                content_html=opt.attrs.get("data-content"),
            )
