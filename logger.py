import json
import logging
import os
import sys
import threading
from datetime import datetime
from typing import Any, Dict, Optional

class DecisionLogger:
    """Thread-safe structured audit logger writing plain-English rationales and raw metrics to decisions.jsonl."""

    def __init__(self, log_path: str = "decisions.jsonl"):
        self.log_path = log_path
        self._lock = threading.Lock()
        # Initialize or append to file
        os.makedirs(os.path.dirname(os.path.abspath(log_path)), exist_ok=True)

    def log_decision(
        self,
        ticker: str,
        simulated_date: str,
        signals: Dict[str, Any],
        composite_score: float,
        action: str,
        quantity: int,
        executed: bool,
        reason: str,
        market_price: float,
        latency_ms: float = 0.0,
        extra: Optional[Dict[str, Any]] = None,
    ):
        record = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "simulated_date": simulated_date,
            "ticker": ticker,
            "signals": signals,
            "composite_score": round(composite_score, 4),
            "action": action,
            "quantity": quantity,
            "executed": executed,
            "reason": reason,
            "market_price": round(market_price, 2),
            "latency_ms": round(latency_ms, 2),
            "extra": extra or {},
        }
        with self._lock:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")

    def log_order(
        self,
        order_id: str,
        ticker: str,
        action: str,
        quantity: int,
        date: str,
        status: str,
        fill_price: float,
        rejection_reason: Optional[str] = None,
        latency_ms: float = 0.0,
    ):
        record = {
            "type": "order_execution",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "order_id": order_id,
            "ticker": ticker,
            "action": action,
            "quantity": quantity,
            "simulated_date": date,
            "status": status,
            "fill_price": round(fill_price, 2),
            "rejection_reason": rejection_reason,
            "latency_ms": round(latency_ms, 2),
        }
        with self._lock:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")


def setup_logger(name: str = "manthan_bot", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not logger.handlers:
        try:
            if hasattr(sys.stdout, "reconfigure"):
                sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(getattr(logging, level.upper(), logging.INFO))
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

audit_logger = DecisionLogger("decisions.jsonl")
system_logger = setup_logger("manthan_bot")
