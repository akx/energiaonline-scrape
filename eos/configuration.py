from dataclasses import dataclass


@dataclass
class Configuration:
    username: str
    password: str
    configuration: str = "CABTKUP"
