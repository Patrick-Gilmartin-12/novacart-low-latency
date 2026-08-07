"""Pydantic schema contracts for Bronze → Silver validation."""
from __future__ import annotations
from datetime import date
from typing import Annotated, Literal, Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict


class OrderRow(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=False)

    order_id: str
    customer_id: str
    product_id: str
    order_date: date
    quantity: Annotated[int, Field(gt=0)]
    unit_price: Annotated[float, Field(ge=0)]
    status: Literal["pending", "shipped", "delivered", "cancelled", "returned"]

    @field_validator("status", mode="before")
    @classmethod
    def normalise_status(cls, v: str) -> str:
        return v.lower() if isinstance(v, str) else v


class CustomerRow(BaseModel):
    customer_id: str
    first_name: str
    last_name: str
    email: str
    city: str
    country: str
    signup_date: date
    tier: Optional[str] = "standard"

    @field_validator("email")
    @classmethod
    def email_has_at(cls, v: str) -> str:
        if "@" not in v:
            raise ValueError(f"invalid email: {v}")
        return v.lower()


class ProductRow(BaseModel):
    product_id: str
    name: str
    category: str
    unit_cost: Annotated[float, Field(ge=0)]
    supplier_id: str
    updated_at: str  # ISO string from SQLite
