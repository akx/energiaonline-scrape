import logging

import requests

from eos.configuration import Configuration
from eos.scrape.utils import get_csrf_token

log = logging.getLogger(__name__)


def do_login(sess: requests.Session, cfg: Configuration):
    log.info("Logging in")
    resp = sess.get("https://energiaonline.turkuenergia.fi/")
    resp.raise_for_status()
    tok = get_csrf_token(resp)
    resp = sess.post(
        url="https://energiaonline.turkuenergia.fi/eServices/Online/Login",
        data={
            "UserName": cfg.username,
            "Password": cfg.password,
            "__RequestVerificationToken": tok,
        },
        headers={
            "Referer": "https://energiaonline.turkuenergia.fi/eServices/Online",
        },
    )
    resp.raise_for_status()
    if "Asiakasnumero tai salasana oli väärä" in resp.text:
        raise RuntimeError("Invalid user credentials")
    if resp.url != "https://energiaonline.turkuenergia.fi/eServices/Online":
        raise RuntimeError(f"Unexpected redirect URL {resp.url} after login")
    log.info("Login successful")
