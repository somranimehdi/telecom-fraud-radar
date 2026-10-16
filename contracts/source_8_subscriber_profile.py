"""
Source 8: Subscriber Profile (Reference)
-----------------------------------------
Reference data loaded into DynamoDB `subscriber_profile`.
Key: subscriber_id (String).
Refreshed daily from the CRM / BSS.

The scoring Lambda reads this per event to enrich features:
  - kyc_status       → subscription-fraud signal
  - account_age_days → new accounts are higher risk
  - segment          → business vs consumer, high-value vs low
  - lines_per_identity → one person with 50 lines is suspicious
  - risk_score_baseline → prior risk from the risk engine
"""

from datetime import datetime
from enum import Enum
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class KYCStatus(str, Enum):
    VERIFIED = "verified"
    PENDING = "pending"
    REJECTED = "rejected"
    UNVERIFIED = "unverified"


class Segment(str, Enum):
    CONSUMER = "consumer"
    BUSINESS = "business"
    YOUTH = "youth"
    ROAMING = "roaming"
    ENTERPRISE = "enterprise"


class SubscriberProfile(BaseModel):
    """One subscriber's profile — the reference row for `subscriber_profile`."""

    subscriber_id: str = Field(..., min_length=1, max_length=64)
    kyc_status: KYCStatus
    account_age_days: int = Field(..., ge=0, description="Days since account creation")
    segment: Segment
    lines_per_identity: int = Field(..., ge=1, le=1000, description="Lines registered to the same identity")
    risk_score_baseline: Decimal = Field(..., ge=0, le=1, description="Prior risk score, 0.0 to 1.0")
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