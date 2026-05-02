"""
Tier 1 — Threshold Rule Engine.

Evaluates user-configured alert rules against live sensor data on every 5 s tick.

Design:
  - Rules are loaded from analytical.db and cached for 60 s so UI changes propagate quickly.
  - A rule must trigger for 2 consecutive ticks before it fires (debounce single bad readings).
  - On firing    → writes alert_event(event_type="fired")    to analytical.db.
  - On resolving → writes alert_event(event_type="resolved") to analytical.db.
  - Only evaluates metrics whose quality == "good" (stale/error values don't trigger rules).
  - All exceptions are caught; tick() never raises.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from analytics_engine.analytical_store import AnalyticalStore

logger = logging.getLogger(__name__)

_RULES_CACHE_TTL_S = 60     # seconds between rule reloads from DB
_DEBOUNCE_TICKS    = 2      # consecutive True ticks required before firing

_CONDITION_OPS = {
    "gt":  lambda v, t: v > t,
    "lt":  lambda v, t: v < t,
    "gte": lambda v, t: v >= t,
    "lte": lambda v, t: v <= t,
    "eq":  lambda v, t: abs(v - t) < 1e-9,
}
_CONDITION_SYM = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<=", "eq": "=="}


class RulesEngine:
    """
    Thread-safe Tier 1 rule evaluator.
    Call tick(devices) from the sensor-analytics background worker.
    """

    def __init__(self, store: "AnalyticalStore") -> None:
        self._store           = store
        self._lock            = threading.Lock()
        self._rules_cache:    list[dict] = []
        self._rules_loaded_at: float = 0.0
        # Firing state — keyed by rule id
        self._fired:        dict[int, int]  = {}   # rule_id → fired_at_ms
        self._consecutive:  dict[int, int]  = {}   # rule_id → consecutive True ticks

    # ── Public entry point ────────────────────────────────────────────────────

    def tick(self, devices: list[dict]) -> None:
        """Evaluate all enabled rules against the live device snapshot."""
        self._maybe_reload_rules()
        if not self._rules_cache:
            return

        # Build fast lookups
        metric_map: dict[tuple, tuple[float, str]] = {}
        name_map:   dict[tuple, str]               = {}
        for d in devices:
            src = d.get("source", "")
            did = d.get("device_id", "")
            name_map[(src, did)] = d.get("name", did)
            for mkey, m in (d.get("metrics") or {}).items():
                v = m.get("value")
                q = m.get("quality", "good")
                if v is not None and isinstance(v, (int, float)):
                    metric_map[(src, did, mkey)] = (float(v), q)

        now_ms = int(time.time() * 1000)

        with self._lock:
            for rule in self._rules_cache:
                self._evaluate_rule(rule, metric_map, name_map, now_ms)

    def reload(self) -> None:
        """Force immediate rule cache refresh (call after UI creates/deletes a rule)."""
        self._rules_loaded_at = 0.0

    def active_alerts(self) -> list[dict]:
        """Current set of rules in fired state — list of {rule_id, fired_at_ms}."""
        with self._lock:
            return [{"rule_id": rid, "fired_at_ms": ts} for rid, ts in self._fired.items()]

    # ── Internals ─────────────────────────────────────────────────────────────

    def _maybe_reload_rules(self) -> None:
        if time.time() - self._rules_loaded_at < _RULES_CACHE_TTL_S:
            return
        try:
            rules = self._store.get_alert_rules(enabled_only=True)
            with self._lock:
                self._rules_cache    = rules
                self._rules_loaded_at = time.time()
            logger.debug("rules: loaded %d enabled rule(s)", len(rules))
        except Exception as exc:
            logger.warning("rules: failed to reload from DB: %s", exc)

    def _evaluate_rule(
        self,
        rule:       dict,
        metric_map: dict,
        name_map:   dict,
        now_ms:     int,
    ) -> None:
        rid  = rule["id"]
        key  = (rule["source"], rule["device_id"], rule["metric_name"])
        entry = metric_map.get(key)

        if entry is None:
            # Device / metric not live — don't change fired state
            return

        value, quality = entry
        if quality != "good":
            # Never evaluate on stale or error readings
            return

        op   = _CONDITION_OPS.get(rule["condition"])
        fires = op(value, float(rule["threshold"])) if op else False

        if fires:
            self._consecutive[rid] = self._consecutive.get(rid, 0) + 1
        else:
            self._consecutive[rid] = 0

        confirmed = self._consecutive.get(rid, 0) >= _DEBOUNCE_TICKS
        was_fired = rid in self._fired

        if confirmed and not was_fired:
            self._fired[rid] = now_ms
            self._write_event(rule, "fired", value, now_ms, name_map)

        elif not fires and was_fired:
            del self._fired[rid]
            self._write_event(rule, "resolved", value, now_ms, name_map)

    def _write_event(
        self,
        rule:     dict,
        etype:    str,
        value:    float,
        now_ms:   int,
        name_map: dict,
    ) -> None:
        dev_name = name_map.get((rule["source"], rule["device_id"]), rule["device_id"])
        sym      = _CONDITION_SYM.get(rule["condition"], rule["condition"])
        metric   = rule["metric_name"]
        thresh   = rule["threshold"]

        if etype == "fired":
            msg = f"{metric} = {value:.4g}  {sym} threshold {thresh}"
            logger.warning(
                "ALERT fired    rule_id=%-4d  device=%-20s  %s",
                rule["id"], dev_name, msg,
            )
        else:
            msg = f"{metric} cleared  (was {sym} {thresh},  last value {value:.4g})"
            logger.info(
                "ALERT resolved  rule_id=%-4d  device=%-20s  %s",
                rule["id"], dev_name, msg,
            )

        try:
            self._store.add_alert_event({
                "rule_id":        rule["id"],
                "source":         rule["source"],
                "device_id":      rule["device_id"],
                "metric_name":    metric,
                "event_type":     etype,
                "severity":       rule["severity"],
                "message":        msg,
                "value_at_event": value,
                "timestamp_ms":   now_ms,
            })
        except Exception as exc:
            logger.error("rules: failed to write alert_event (rule_id=%d): %s", rule["id"], exc)
