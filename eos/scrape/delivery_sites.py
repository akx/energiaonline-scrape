import logging
from typing import Iterable

import requests

from eos.models import DeliverySite

log = logging.getLogger(__name__)


def get_delivery_sites(sess: requests.Session) -> Iterable[DeliverySite]:
    log.info("Loading delivery sites...")
    resp = sess.post(
        url="https://energiaonline.turkuenergia.fi/eServices/Online/GetEffectiveMeteringPoints",
        headers={},
    )
    resp.raise_for_status()
    for site in resp.json():
        yield DeliverySite(
            metering_point_code=site["MeteringPointCode"],
            network_company_code=site["NetworkCompanyCode"],
            source_company_code=site["SourceCompanyCode"],
            customer_code=site["CustomerCode"],
            original_data=site,
        )
