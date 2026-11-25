from dataclasses import dataclass
from decimal import Decimal

from src.readers.dictionary_resolver import ResolvedEvent


@dataclass(frozen=True)
class StatusTotal:
    status: str
    count: int
    watt_hours: Decimal


@dataclass(frozen=True)
class StatusSummary:
    totals: tuple[StatusTotal, ...]
    events: tuple[ResolvedEvent, ...]


def _coefficients(values):
    parts = [value.as_tuple() for value in values]
    scale = min(part.exponent for part in parts)
    total = 0
    for part in parts:
        coefficient = 0
        for digit in part.digits:
            coefficient = 10 * coefficient + digit
        if coefficient:
            scaled = coefficient * 10 ** (part.exponent - scale)
            total += -scaled if part.sign else scaled
    exact = Decimal(total).as_tuple()
    return Decimal((exact.sign, exact.digits, scale))


def summarize_statuses(events):
    ordered = tuple(events)
    values_by_status = {}
    for event in ordered:
        values_by_status.setdefault(event.status, []).append(event.value)
    totals = tuple(
        StatusTotal(status, len(values), _coefficients(values))
        for status, values in sorted(values_by_status.items())
    )
    return StatusSummary(totals, ordered)
