"""
Source 2: Revenue Batch (Prepaid + Postpaid)
---------------------------------------------
Batch files from billing system, dropped daily into S3:
  - raw/revenue/prepaid/
  - raw/revenue/postpaid/

Each row = one payment / invoice / adjustment.
Transport: direct S3 upload (no streaming).
"""

from datetime import datetime
from enum import Enum
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class PaymentType(str, Enum):
    TOPUP = "topup"                 # prepaid credit top-up
    INVOICE_PAYMENT = "invoice_payment"   # postpaid bill payment
    ADJUSTMENT = "adjustment"       # manual adjustment (credit/debit)
    REFUND = "refund"


class PaymentStatus(str, Enum):
    COMPLETED = "completed"
    PENDING = "pending"
    FAILED = "failed"
    REVERSED = "reversed"


class RevenueTransaction(BaseModel):
    """One revenue transaction (payment, invoice, adjustment, or refund)."""

    transaction_id: str = Field(..., min_length=1, max_length=64)
    subscriber_id: str = Field(..., min_length=1, max_length=64)
    payment_type: PaymentType
    amount: Decimal = Field(..., description="Signed amount; negative for refunds")
    currency: str = Field(..., min_length=3, max_length=3, description="ISO 4217 code")
    timestamp: datetime
    status: PaymentStatus

    @field_validator("currency")
    @classmethod
    def currency_uppercase(cls, v: str) -> str:
        """Normalize currency to uppercase ISO code."""
        return v.upper()

    @field_validator("timestamp")
    @classmethod
    def timestamp_not_in_future(cls, v: datetime) -> datetime:
        from datetime import timezone
        now = datetime.now(timezone.utc)
        v_aware = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        if v_aware > now:
            raise ValueError("timestamp is in the future")
        return v