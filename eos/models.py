from dataclasses import dataclass, asdict
from typing import Optional, List

import bs4


@dataclass
class DeliverySite:
    site_id: str
    customer_id: str
    content_html: str = None

    @property
    def name(self) -> Optional[str]:
        s = bs4.BeautifulSoup(self.content_html, features="html.parser")
        text_tag: bs4.Tag = s.find("span", class_="optionText")
        if text_tag:
            return text_tag.get_text(strip=True, separator=" ")
        return None


@dataclass
class UsageData:
    resolution: str
    delivery_site_info: dict
    data: List[dict]

    def as_dict(self):
        return asdict(self)
