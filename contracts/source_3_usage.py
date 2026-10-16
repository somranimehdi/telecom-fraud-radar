"""
Source 3: Usage CDRs (Voice + Data)
------------------------------------
Batch files dropped daily into S3:
  - raw/usage/voice/
  - raw/usage/data/

Voice CDRs: one row per call.
Data CDRs:  one row per data session.

These are the primary features for fraud detection:
  - IRSF (international revenue share fraud) shows up as short, high-cost intl calls.
  - SIM swap takeover shows up as a spike in intl voice + data after a swap event.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class VoiceCDR(BaseModel):
    """One voice call record."""

    call_id: str = Field(..., min_length=1, max_length=64)
    subscriber_id: str = Field(..., min_length=1, max_length=64)
    destination_number: str = Field(..., pattern=r"^\+?[0-9]{6,15}$")
    duration_sec: int = Field(..., ge=0, le=86400, description="Call duration in seconds (0–24h)")
    cost: Decimal = Field(..., ge=0, description="Charged amount (non-negative)")
    timestamp: datetime
    country_code: str = Field(..., min_length=2, max_length=3, description="Destination country (ISO 3166-1 alpha-2 or calling code)")

    @field_validator("timestamp")
    @classmethod
    def timestamp_not_in_future(cls, v: datetime) -> datetime:
        now = datetime.now(timezone.utc)
        v_aware = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        if v_aware > now:
            raise ValueError("timestamp is in the future")
        return v

    @field_validator("country_code")
    @classmethod
    def country_code_uppercase(cls, v: str) -> str:
        return v.upper()


class DataCDR(BaseModel):
    """One data session record."""

    session_id: str = Field(..., min_length=1, max_length=64)
    subscriber_id: str = Field(..., min_length=1, max_length=64)
    url_domain: Optional[str] = Field(None, max_length=253, description="Domain accessed, if known (CDR privacy rules may strip this)")
    bytes_used: int = Field(..., ge=0, description="Bytes transferred in the session")
    cost: Decimal = Field(..., ge=0, description="Charged amount (non-negative)")
    timestamp: datetime
    country_code: str = Field(..., min_length=2, max_length=3)

    @field_validator("timestamp")
    @classmethod
    def timestamp_not_in_future(cls, v: datetime) -> datetime:
        now = datetime.now(timezone.utc)
        v_aware = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        if v_aware > now:
            raise ValueError("timestamp is in the future")
        return v

    @field_validator("country_code")
    @classmethod
    def country_code_uppercase(cls, v: str) -> str:
        return v.upper()