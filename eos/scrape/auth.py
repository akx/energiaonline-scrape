import logging
import re
from urllib.parse import urljoin

import requests

from eos.configuration import Configuration
from eos.scrape.consts import ONLINE_HOME_URL, EO_DOMAIN
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
            "Referer": ONLINE_HOME_URL,
        },
    )
    resp.raise_for_status()
    if "Asiakasnumero tai salasana oli väärä" in resp.text:
        raise RuntimeError("Invalid user credentials")
    if resp.url != ONLINE_HOME_URL:
        raise RuntimeError(f"Unexpected redirect URL {resp.url} after login")
    if cfg.delegate_username:
        log.info("Switching to delegate user")
        switch_urls = list(re.findall(r"/eServices/Online/ChangeSelectedUser/\d+\?userName=[^\"]+", resp.text))
        matching_switch_url = next((url for url in switch_urls if url.endswith(f"={cfg.delegate_username}")), None)
        if not matching_switch_url:
            raise RuntimeError(f"No ChangeSelectedUser URL for {cfg.delegate_username} found")
        resp = sess.get(
            url=urljoin(EO_DOMAIN, matching_switch_url),
            headers={
                "Referer": ONLINE_HOME_URL,
            },
        )
        if resp.url != ONLINE_HOME_URL:
            raise RuntimeError(f"Unexpected redirect URL {resp.url} after delegate switch")
    log.info("Login successful")
