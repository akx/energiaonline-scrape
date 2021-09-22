import datetime
from typing import Tuple, Optional


def fix_date_defaults(
    start_date: Optional[datetime.date],
    end_date: Optional[datetime.date],
    back_days: int = 30,
) -> Tuple[datetime.date, datetime.date]:
    if not end_date:
        end_date = datetime.date.today() - datetime.timedelta(days=1)
    if not start_date:
        start_date = end_date - datetime.timedelta(days=back_days)
    return start_date, end_date
