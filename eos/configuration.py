from dataclasses import dataclass
from typing import Optional


@dataclass
class Configuration:
    username: str
    password: str
    configuration: str = "CABTKUP"
    delegate_username: Optional[str] = None
