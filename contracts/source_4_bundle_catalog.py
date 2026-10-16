"""
Source 4: Bundle Catalog (Reference)
-------------------------------------
Reference data loaded into DynamoDB `bundle_catalog`.
Key: bundle_id (String).
Refreshed daily from the product catalog.

The scoring Lambda and rules engine read this to know:
  - what a bundle should cost (price)
  - what it should allow (allowance_amount + allowance_type)
  - how long it lasts (validity_days)
  - which segment it targets (segment)

Bundle abuse detection depends on this: a subscriber consuming
10x the allowance, or buying a bundle repeatedly within its
validity window, is suspicious.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class AllowanceType(str, Enum):
    VOICE = "voice"       # minutes
    DATA = "data"         # megabytes
    SMS = "sms"           # message count
    COMBO = "combo"       # mixed (e.g. 5GB + 500min)


class BundleSegment(str, Enum):
    CONSUMER = "consumer"
    BUSINESS = "business"
    YOUTH = "youth"
    ROAMING = "roaming"


class BundleCatalogEntry(BaseModel):
    """One bundle definition — the reference row for `bundle_catalog`."""

    bundle_id: str = Field(..., min_length=1, max_length=64)
    name: str = Field(..., min_length=1, max_length=128)
    price: Decimal = Field(..., ge=0, description="Catalog price (non-negative)")
    allowance_type: AllowanceType
    allowance_amount: int = Field(..., ge=0, description="Minutes / MB / SMS count depending on type")
    validity_days: int = Field(..., ge=1, le=365, description="How long the bundle lasts")
    segment: BundleSegment
    last_updated: datetime

    @field_validator("last_updated")
    @classmethod
    def last_updated_not_in_future(cls, v: datetime) -> datetime:
        from datetime import timezone
        now = datetime.now(timezone.utc)
        v_aware = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        if v_aware > now:
            raise ValueError("last_updated is in the future")
        return v