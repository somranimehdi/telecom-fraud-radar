"""
Source 1: SIM Activation Stream
--------------------------------
Streaming events from the telco's activation system.
Transport: Kinesis (sim-activation-stream) → Lambda → S3 bronze raw/activation/

Event types:
  ACTIVATED  - new SIM activated
  SWAPPED    - SIM swap (existing number moved to new SIM)
  SUSPENDED  - SIM temporarily suspended
  CLOSED     - SIM permanently closed
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class ActivationEventType(str, Enum):
    ACTIVATED = "ACTIVATED"
    SWAPPED = "SWAPPED"
    SUSPENDED = "SUSPENDED"
    CLOSED = "CLOSED"


class ActivationChannel(str, Enum):
    RETAIL = "retail"
    ONLINE = "online"
    CALL_CENTER = "call_center"
    DEALER = "dealer"
    APP = "app"


class SimActivationEvent(BaseModel):
    """A single SIM activation lifecycle event."""

    event_id: str = Field(..., min_length=1, max_length=64, description="Unique event identifier")
    subscriber_id: str = Field(..., min_length=1, max_length=64, description="Subscriber (account) identifier")
    msisdn: str = Field(..., pattern=r"^\+?[0-9]{8,15}$", description="Phone number, E.164-ish")
    imsi: str = Field(..., pattern=r"^[0-9]{14,16}$", description="SIM's IMSI (14-16 digits)")
    event_type: ActivationEventType
    timestamp: datetime
    channel: ActivationChannel

    @field_validator("timestamp")
    @classmethod
    def timestamp_not_in_future(cls, v: datetime) -> datetime:
        """Reject events timestamped in the future."""
        from datetime import timezone
        now = datetime.now(timezone.utc)
        v_aware = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        if v_aware > now:
            raise ValueError("timestamp is in the future")
        return v