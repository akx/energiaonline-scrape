from dataclasses import dataclass, field

import requests

from eos.configuration import Configuration


@dataclass
class Context:
    cfg: Configuration
    sess: requests.Session = field(default_factory=requests.Session)
