"""
Source 5: Bundle Purchases Stream
----------------------------------
Streaming events from the telco's e-commerce / app / retail channels.
Transport: Kinesis (bundle-purchases-stream) → Lambda → S3 raw/bundles/purchases/

Each event = one bundle purchase by a subscriber.

Bundle abuse patterns show up here:
  - Same subscriber buying the same bundle repeatedly in a short window.
  - Purchases from a channel that doesn't match the subscriber's usual pattern.
  - Purchases immediately after a SIM swap event.
"""

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class PurchaseChannel(str, Enum):
    RETAIL = "retail"
    ONLINE = "online"
    CALL_CENTER = "call_center"
    DEALER = "dealer"
    APP = "app"
    USSD = "ussd"                  # self-service menu, common in prepaid telco


class BundlePurchase(BaseModel):
    """One bundle purchase event."""

    purchase_id: str = Field(..., min_length=1, max_length=64)
    subscriber_id: str = Field(..., min_length=1, max_length=64)
    bundle_id: str = Field(..., min_length=1, max_length=64, description="References bundle_catalog.bundle_id")
    channel: PurchaseChannel
    price: Decimal = Field(..., ge=0, description="Amount paid (non-negative)")
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def timestamp_not_in_future(cls, v: datetime) -> datetime:
        now = datetime.now(timezone.utc)
        v_aware = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        if v_aware > now:
            raise ValueError("timestamp is in the future")
        return v