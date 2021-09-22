from dataclasses import dataclass, asdict
from typing import Optional, List


@dataclass
class DeliverySite:
    metering_point_code: str
    network_company_code: str
    source_company_code: str
    customer_code: Optional[str]
    original_data: dict

    @property
    def name(self) -> Optional[str]:
        return self.original_data.get("StreetAddress")

    def asdict(self):
        d = asdict(self)
        d["name"] = self.name
        d.pop("original_data")
        return d


@dataclass
class UsageData:
    resolution: str
    customer_id: str
    site_id: str
    delivery_site_info: dict
    data: List[dict]

    def as_dict(self):
        return asdict(self)
