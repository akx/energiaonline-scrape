import datetime
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict


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
class UsageDataPoint:
    resolution: str
    timestamp: datetime.datetime
    usage: float
    temperature: Optional[float]

    def as_dict(self):
        d = asdict(self)
        d["timestamp"] = d["timestamp"].isoformat()
        return d


@dataclass
class UsageData:
    site: DeliverySite
    hourly_usage_data: Dict[datetime.datetime, UsageDataPoint]
    daily_usage_data: Dict[datetime.datetime, UsageDataPoint]
