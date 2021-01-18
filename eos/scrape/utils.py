import logging
import re

log = logging.getLogger(__name__)


def get_csrf_token(resp):
    m = re.search(
        r'<input name="__RequestVerificationToken" type="hidden" value="(.+?)" />',
        resp.text,
    )
    if m:
        return m.group(1)
    raise ValueError("No CSRF token in response")


def get_reporting_token(sess):
    log.info("Loading energy reporting view to acquire CSRF token...")
    del sess.cookies["__RequestVerificationToken"]
    resp = sess.get("https://www.energiaonline.fi/EnergyReporting/EnergyReporting")
    resp.raise_for_status()
    token = get_csrf_token(resp)
    return token
