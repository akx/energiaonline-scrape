import logging

import requests

from eos.configuration import Configuration
from eos.scrape.utils import get_csrf_token

log = logging.getLogger(__name__)


def do_login(sess: requests.Session, cfg: Configuration):
    log.info("Logging in")
    resp = sess.get("https://www.energiaonline.fi/")
    resp.raise_for_status()
    tok = get_csrf_token(resp)
    resp = sess.post(
        url="https://www.energiaonline.fi/Authentication/Login",
        data={
            "UserName": cfg.username,
            "Password": cfg.password,
            "Configuration": cfg.configuration,
        },
        headers={
            "Referer": "https://www.energiaonline.fi/Home/Index",
            "__requestverificationtoken": tok,
        },
    )
    resp.raise_for_status()
    if "Asiakasnumero tai salasana oli väärä" in resp.text:
        raise RuntimeError("Invalid user credentials")
    if resp.url != "https://www.energiaonline.fi/Authentication/Login":
        raise RuntimeError("Unexpected redirect URL after login")
    log.info("Login successful, doing post redirect")
    resp = sess.get("https://www.energiaonline.fi" + resp.json()["Redirect"])
    resp.raise_for_status()
