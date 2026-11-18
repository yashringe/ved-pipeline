"""Replay VED trips to the ingest API as a partner would, with seeded faults.

Every trip is delivered once, shortly after it ends. On top of that a seeded RNG
injects the delivery problems the pipeline must survive:

* duplicate   - the same content re-sent 1-6 h later (only ``sent_at`` differs)
* late        - delivered 26-72 h after the trip ended
* partial     - the first 60 % of points sent mid-trip, the full trip later
* speed_spike - one speed sample replaced by 320 km/h (a content fault)

Deliveries are sent in simulated arrival order (by partner ``sent_at``). The ground
truth goes to ``injected_faults.json`` so DQ results can be checked against it, and a
``.replayed`` marker makes ``docker compose up`` re-runs a no-op.
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import json
import logging
import os
import random
import time
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from vedpipe.config import Settings, setup_logging
from vedpipe.sim.ved import DYNAMIC_DIR, MANIFEST, iter_trip_payloads, week_date

log = logging.getLogger(__name__)

SPIKE_KMH = 320.0
PARTIAL_SHARE = 0.6
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


@dataclass(frozen=True)
class FaultRates:
    duplicate: float = 0.02
    late: float = 0.03
    partial: float = 0.01
    speed_spike: float = 0.01

    @classmethod
    def from_env(cls) -> FaultRates:
        return cls(
            duplicate=float(os.environ.get("FAULT_DUPLICATE_RATE", cls.duplicate)),
            late=float(os.environ.get("FAULT_LATE_RATE", cls.late)),
            partial=float(os.environ.get("FAULT_PARTIAL_RATE", cls.partial)),
            speed_spike=float(os.environ.get("FAULT_SPEED_SPIKE_RATE", cls.speed_spike)),
        )


@dataclass(frozen=True)
class Delivery:
    """One payload as it leaves the partner; ``kind`` is for bookkeeping only."""

    sent_at: datetime
    kind: str  # normal | late | partial | duplicate
    payload: dict[str, Any] = field(repr=False)


def _with_sent_at(payload: dict[str, Any], sent_at: datetime) -> dict[str, Any]:
    return {**payload, "sent_at": sent_at.isoformat(timespec="milliseconds")}


def plan_deliveries(
    trips: Iterable[dict[str, Any]],
    rates: FaultRates,
    seed: int,
    ground_truth: list[dict[str, Any]] | None = None,
) -> Iterator[Delivery]:
    """Yield deliveries in ``sent_at`` order with bounded memory.

    Input trips must be ordered by start time. Any delivery is sent after its trip
    starts, so once a trip starting at S is reached nothing pending at or before S can
    be overtaken: those deliveries are released. Memory therefore holds roughly the
    longest delay window (72 h of trips), not the whole year.

    The RNG draws a fixed number of values per trip, in a fixed order, so the plan is
    reproducible for a seed regardless of which faults fire.
    """
    rng = random.Random(seed)
    pending: list[tuple[datetime, int, Delivery]] = []
    sequence = itertools.count()

    def push(delivery: Delivery) -> None:
        heapq.heappush(pending, (delivery.sent_at, next(sequence), delivery))

    for trip in trips:
        start = datetime.fromisoformat(trip["started_at"])
        while pending and pending[0][0] <= start:
            yield heapq.heappop(pending)[2]

        u_spike, u_late, u_partial, u_dup, u_pick = (rng.random() for _ in range(5))
        normal_delay = timedelta(seconds=rng.uniform(30, 900))
        late_delay = timedelta(hours=rng.uniform(26, 72))
        dup_after = timedelta(hours=rng.uniform(1, 6))

        points = trip["points"]
        end = start + timedelta(milliseconds=max(p["t"] for p in points))
        faults: dict[str, Any] = {}

        speed_idx = [i for i, p in enumerate(points) if "speed_kmh" in p]
        if u_spike < rates.speed_spike and speed_idx:
            i = speed_idx[int(u_pick * len(speed_idx))]
            points = [*points[:i], {**points[i], "speed_kmh": SPIKE_KMH}, *points[i + 1 :]]
            trip = {**trip, "points": points}
            faults["speed_spike"] = {"t": points[i]["t"]}

        if u_partial < rates.partial and len(points) >= 2:
            cut = min(len(points) - 1, max(1, int(len(points) * PARTIAL_SHARE)))
            mid_trip = start + timedelta(milliseconds=points[cut - 1]["t"])
            push(Delivery(mid_trip, "partial", _with_sent_at({**trip, "points": points[:cut]},
                                                             mid_trip)))
            faults["partial"] = {"points_sent_first": cut}

        is_late = u_late < rates.late
        sent_at = end + (late_delay if is_late else normal_delay)
        push(Delivery(sent_at, "late" if is_late else "normal", _with_sent_at(trip, sent_at)))
        if is_late:
            faults["late"] = {"delay_h": round(late_delay.total_seconds() / 3600, 2)}

        if u_dup < rates.duplicate:
            resent = sent_at + dup_after
            push(Delivery(resent, "duplicate", _with_sent_at(trip, resent)))
            faults["duplicate"] = {"resent_after_h": round(dup_after.total_seconds() / 3600, 2)}

        if faults and ground_truth is not None:
            ground_truth.append(
                {"vehicle_id": trip["vehicle_id"], "trip_id": trip["trip_id"], "faults": faults}
            )

    while pending:
        yield heapq.heappop(pending)[2]


def batched(items: Iterable[Delivery], size: int) -> Iterator[list[Delivery]]:
    """``itertools.batched`` for Python 3.11."""
    iterator = iter(items)
    while batch := list(itertools.islice(iterator, size)):
        yield batch


class ReplayError(RuntimeError):
    """A batch was rejected permanently or retries were exhausted."""


class Sender:
    """POSTs batches with retries on transport errors, 429 and 5xx (honours Retry-After)."""

    def __init__(
        self,
        api_url: str,
        api_key: str,
        session: requests.Session | None = None,
        max_attempts: int = 6,
        base_backoff_s: float = 0.5,
        timeout_s: float = 30.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._url = api_url.rstrip("/") + "/v1/trips/batch"
        self._headers = {"X-API-Key": api_key}
        self._session = session or requests.Session()
        self._max_attempts = max_attempts
        self._base_backoff_s = base_backoff_s
        self._timeout_s = timeout_s
        self._sleep = sleep

    def send(self, payloads: list[dict[str, Any]]) -> None:
        for attempt in range(1, self._max_attempts + 1):
            retry_after: float | None = None
            try:
                response = self._session.post(
                    self._url, json={"trips": payloads}, headers=self._headers,
                    timeout=self._timeout_s,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                reason = f"transport error: {exc}"
            else:
                if response.status_code == 202:
                    return
                if response.status_code not in RETRYABLE_STATUS:
                    raise ReplayError(
                        f"batch rejected with {response.status_code}: {response.text[:500]}"
                    )
                reason = f"HTTP {response.status_code}"
                header = response.headers.get("Retry-After", "")
                retry_after = float(header) if header.isdigit() else None
            if attempt == self._max_attempts:
                raise ReplayError(f"giving up after {attempt} attempts ({reason})")
            delay = retry_after or min(30.0, self._base_backoff_s * 2 ** (attempt - 1))
            log.warning("attempt %d failed (%s); retrying in %.1fs", attempt, reason, delay)
            self._sleep(delay)


def weekly_csvs(data_dir: Path) -> list[Path]:
    """Weekly CSVs chosen by fetch (manifest order), else every CSV present, by week."""
    manifest = data_dir / MANIFEST
    if manifest.exists():
        names = json.loads(manifest.read_text())["csv_files"]
        return [data_dir / DYNAMIC_DIR / name for name in names]
    found = [p for p in (data_dir / DYNAMIC_DIR).glob("*.csv") if week_date(p.name)]
    return sorted(found, key=lambda p: week_date(p.name))


def replay(settings: Settings, rates: FaultRates, seed: int, batch_size: int) -> Counter[str]:
    marker = settings.ved_data_dir / ".replayed"
    if marker.exists():
        log.info("%s exists: trips were already replayed; delete it to replay again", marker)
        return Counter(json.loads(marker.read_text()))
    csvs = weekly_csvs(settings.ved_data_dir)
    if not csvs:
        raise SystemExit(f"no weekly CSVs under {settings.ved_data_dir}; run the fetch step first")

    truth: list[dict[str, Any]] = []
    sent: Counter[str] = Counter()
    sender = Sender(settings.api_url, settings.api_key)
    plan = plan_deliveries(iter_trip_payloads(csvs), rates, seed, truth)
    for number, batch in enumerate(batched(plan, batch_size), start=1):
        sender.send([d.payload for d in batch])
        sent.update(d.kind for d in batch)
        if number % 50 == 0:
            log.info("sent %d deliveries so far", sum(sent.values()))

    report = {"seed": seed, "rates": asdict(rates), "deliveries": dict(sent), "trips": truth}
    (settings.ved_data_dir / "injected_faults.json").write_text(json.dumps(report, indent=2))
    marker.write_text(json.dumps(dict(sent)))
    log.info("replay finished: %s; %d trips with injected faults", dict(sent), len(truth))
    return sent


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=int(os.environ.get("SIM_SEED", "42")))
    parser.add_argument("--batch-size", type=int,
                        default=int(os.environ.get("SIM_BATCH_SIZE", "20")))
    args = parser.parse_args(argv)
    setup_logging()
    replay(Settings.from_env(), FaultRates.from_env(), args.seed, args.batch_size)


if __name__ == "__main__":
    main()
