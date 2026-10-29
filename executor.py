import concurrent.futures
import time
from typing import Any, Dict, List, Optional, Tuple
from api_client import ApiClient
from config import config
from logger import audit_logger, system_logger
from risk import RiskDecision
from state import PortfolioState

class OrderExecutor:
    """Pre-validates, prioritizes, and executes trading orders while honoring rate limits and tracking latencies."""

    def __init__(self, api_client: ApiClient, state: PortfolioState, valid_tickers: Optional[List[str]] = None):
        self.api_client = api_client
        self.state = state
        self.valid_tickers = set(valid_tickers or [])

    def update_valid_tickers(self, tickers: List[str]):
        self.valid_tickers = set(tickers)

    def pre_validate(self, decision: RiskDecision, current_price: float) -> Tuple[bool, str]:
        """Strict client-side validation to eliminate rejections before network dispatch."""
        if decision.action == "DO_NOTHING":
            return False, "Action is DO_NOTHING"

        if decision.ticker not in self.valid_tickers:
            return False, f"INVALID_TICKER: {decision.ticker} is not in active universe"

        if decision.quantity <= 0 or not isinstance(decision.quantity, int):
            return False, f"INVALID_QUANTITY: {decision.quantity} must be positive integer"

        if self.state.stage != "simulation" or self.state.status != "RUNNING":
            return False, f"SIMULATION_NOT_RUNNING: Stage is {self.state.stage}/{self.state.status}"

        if self.state.available_cash < 0:
            return False, "ACCOUNT_BLOCKED: Cash balance is negative"

        # Validate action specific constraints
        if decision.action == "SELL":
            if decision.ticker not in self.state.longs:
                return False, f"NO_LONG_POSITION: No shares owned for {decision.ticker}"
            if self.state.longs[decision.ticker].quantity < decision.quantity:
                return False, f"INSUFFICIENT_HOLDINGS: Owned {self.state.longs[decision.ticker].quantity} < requested {decision.quantity}"

        elif decision.action == "COVER":
            if decision.ticker not in self.state.shorts:
                return False, f"NO_SHORT_POSITION: No open short position for {decision.ticker}"
            if self.state.shorts[decision.ticker].quantity < decision.quantity:
                return False, f"INSUFFICIENT_SHORT_POSITION: Shorted {self.state.shorts[decision.ticker].quantity} < requested {decision.quantity}"

        elif decision.action == "BUY":
            cost = (decision.quantity * current_price) + config.BROKERAGE_FEE
            if cost > self.state.available_cash:
                return False, f"INSUFFICIENT_CAPITAL: Cost INR {cost:.2f} > available cash INR {self.state.available_cash:.2f}"

        elif decision.action == "SHORT":
            margin = (decision.quantity * current_price) + config.BROKERAGE_FEE
            if margin > self.state.available_cash:
                return False, f"INSUFFICIENT_MARGIN: Required INR {margin:.2f} > available cash INR {self.state.available_cash:.2f}"

        return True, "VALID"

    def execute_decisions(
        self,
        decisions: List[RiskDecision],
        simulated_date: str,
        prices: Dict[str, float],
        max_time_budget_s: float = 2.2,
    ) -> List[Dict[str, Any]]:
        """Executes a prioritized batch of decisions within the per-day time budget."""
        # Sort by priority ascending (1 = highest: stops/covers, 10 = entries)
        sorted_decisions = sorted(decisions, key=lambda d: d.priority)
        results = []
        start_time = time.monotonic()

        for d in sorted_decisions:
            # Check elapsed time budget for latency safety
            elapsed = time.monotonic() - start_time
            if elapsed >= max_time_budget_s:
                if d.priority > 3:  # Only skip lower priority entries, never skip stops/covers
                    system_logger.warning(
                        f"Time budget reached ({elapsed:.2f}s >= {max_time_budget_s}s); skipping low priority order {d.ticker} {d.action}"
                    )
                    continue

            curr_price = prices.get(d.ticker, 0.0)
            is_valid, reason = self.pre_validate(d, curr_price)
            if not is_valid:
                system_logger.debug(f"Pre-validation skipped {d.ticker} {d.action}: {reason}")
                continue

            order_start = time.monotonic()
            resp = self.api_client.send_order(
                ticker=d.ticker,
                action=d.action,
                quantity=d.quantity,
                date=simulated_date,
            )
            order_latency = (time.monotonic() - order_start) * 1000.0

            # Evaluate API response
            status = resp.get("status", "error")
            error_code = resp.get("error_code")
            order_id = resp.get("order_id", f"ord_{int(time.time()*1000)}")
            fill_price = resp.get("fill_price", resp.get("price", curr_price))

            if status == "ok" or ("order" in resp and resp.get("status") != "error"):
                # Apply optimistic state update
                if d.action == "BUY":
                    self.state.apply_buy(d.ticker, d.quantity, fill_price, simulated_date)
                elif d.action == "SELL":
                    self.state.apply_sell(d.ticker, d.quantity, fill_price, simulated_date)
                elif d.action == "SHORT":
                    self.state.apply_short(d.ticker, d.quantity, fill_price, simulated_date)
                elif d.action == "COVER":
                    self.state.apply_cover(d.ticker, d.quantity, fill_price, simulated_date)

                audit_logger.log_order(
                    order_id=order_id,
                    ticker=d.ticker,
                    action=d.action,
                    quantity=d.quantity,
                    date=simulated_date,
                    status="FILLED",
                    fill_price=fill_price,
                    latency_ms=order_latency,
                )
                system_logger.info(
                    f"EXECUTED [{d.action}] {d.quantity} shares of {d.ticker} @ INR {fill_price:.2f} (latency: {order_latency:.1f}ms) | Reason: {d.reason}"
                )
                results.append({"status": "FILLED", "ticker": d.ticker, "action": d.action, "fill_price": fill_price})
            else:
                self.state.rejection_count += 1
                system_logger.error(
                    f"ORDER REJECTED [{d.action}] {d.ticker}: Code={error_code}, Message={resp.get('message')}"
                )
                audit_logger.log_order(
                    order_id=order_id,
                    ticker=d.ticker,
                    action=d.action,
                    quantity=d.quantity,
                    date=simulated_date,
                    status="REJECTED",
                    fill_price=curr_price,
                    rejection_reason=error_code or resp.get("message", "UNKNOWN_ERROR"),
                    latency_ms=order_latency,
                )
                results.append({"status": "REJECTED", "ticker": d.ticker, "error": error_code})

        return results
