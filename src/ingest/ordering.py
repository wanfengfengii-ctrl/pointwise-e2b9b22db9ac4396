class EventOrderError(ValueError):
    pass


def order_events(events):
    ordered = tuple(events)
    for position, (previous, current) in enumerate(zip(ordered, ordered[1:]), 2):
        if previous.sequence >= current.sequence:
            raise EventOrderError(
                f'non-increasing sequence at row {position}: {previous.sequence}, {current.sequence}'
            )
    return ordered
