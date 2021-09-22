import os
from dataclasses import dataclass, field

import requests

from eos.configuration import Configuration


def get_session() -> requests.Session:
    sess = requests.Session()
    sess.verify = os.path.join(os.path.dirname(__file__), "certificates.pem")
    return sess


@dataclass
class Context:
    cfg: Configuration
    sess: requests.Session = field(default_factory=get_session)
