from dataclasses import dataclass


@dataclass
class Payload:
    sku: str
    amount: float


@dataclass
class Event:
    event_id: str
    event_type: str
    event_ts: str
    customer_id: str
    order_id: str
    payload: Payload
