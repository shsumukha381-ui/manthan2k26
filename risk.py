from typing import Any, Dict, List, Optional, Tuple
from config import config
from state import PortfolioState

class RiskDecision:
    def __init__(
        self,
        action: str,  # 'BUY', 'SELL', 'SHORT', 'COVER', 'DO_NOTHING'
        ticker: str,
        quantity: int,
        reason: str,
        priority: int = 10,  # 1 is highest (stops/covers), 10 is lowest (new entries)
        expected_gain: float = 0.0,
    ):
        self.action = action
        self.ticker = ticker
        self.quantity = quantity
        self.reason = reason
        self.priority = priority
        self.expected_gain = expected_gain

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "ticker": self.ticker,
            "quantity": self.quantity,
            "reason": self.reason,
            "priority": self.priority,
            "expected_gain": self.expected_gain,
        }


class RiskManager:
    """Rigorous Risk Management & Capital Allocation Veto Layer."""

    def __init__(self, state: PortfolioState):
        self.state = state

    def evaluate_position_risk(
        self,
        ticker: str,
        current_price: float,
        signal_data: Dict[str, Any],
    ) -> Optional[RiskDecision]:
        """Evaluates exits, stop-losses, and profit targets for existing positions."""
        composite = signal_data.get("final_composite", 0.0)
        news_score = signal_data.get("news_score", 0.0)
        current_day = self.state.current_day_number
        days_rem = self.state.days_remaining

        # --- 1. Evaluate Long Position ---
        if ticker in self.state.longs:
            pos = self.state.longs[ticker]
            pnl_pct = (current_price - pos.avg_price) / pos.avg_price

            # A. Hard Stop-Loss
            if pnl_pct <= -config.STOP_LOSS_PCT:
                return RiskDecision(
                    action="SELL",
                    ticker=ticker,
                    quantity=pos.quantity,
                    reason=f"HARD STOP-LOSS triggered: Loss {pnl_pct:.2%} exceeds threshold {-config.STOP_LOSS_PCT:.2%}",
                    priority=1,
                )

            # B. Trailing Stop
            gain_from_entry = (pos.highest_price - pos.avg_price) / pos.avg_price
            if gain_from_entry >= config.TRAILING_STOP_ACTIVATION_PCT:
                pullback_from_peak = (pos.highest_price - current_price) / pos.highest_price
                if pullback_from_peak >= config.TRAILING_STOP_CALLBACK_PCT:
                    return RiskDecision(
                        action="SELL",
                        ticker=ticker,
                        quantity=pos.quantity,
                        reason=f"TRAILING STOP triggered: Pullback {pullback_from_peak:.2%} from peak {pos.highest_price:.2f} (locked in profit)",
                        priority=2,
                    )

            # C. Target Take-Profit
            if pnl_pct >= config.TAKE_PROFIT_PCT:
                return RiskDecision(
                    action="SELL",
                    ticker=ticker,
                    quantity=pos.quantity,
                    reason=f"TAKE PROFIT target reached: Gain {pnl_pct:.2%} >= {config.TAKE_PROFIT_PCT:.2%}",
                    priority=3,
                )

            # D. Severe Bearish News Shock
            if news_score <= -0.45:
                return RiskDecision(
                    action="SELL",
                    ticker=ticker,
                    quantity=pos.quantity,
                    reason=f"EMERGENCY EXIT: Severe negative news shock detected (score={news_score:.2f})",
                    priority=1,
                )

            # E. Signal Reversal (Composite Turned Bearish)
            if composite <= config.SELL_THRESHOLD:
                return RiskDecision(
                    action="SELL",
                    ticker=ticker,
                    quantity=pos.quantity,
                    reason=f"SIGNAL REVERSAL exit: Composite score dropped to {composite:.2f} <= {config.SELL_THRESHOLD:.2f}",
                    priority=4,
                )

            # F. Tax-Loss Harvesting (Sections 70 & 71)
            # If position is slightly down and composite is negative, and we have net realized gains to offset
            if (
                pnl_pct < -0.015
                and composite < 0.0
                and self.state.realized_stcg_gains > self.state.realized_stcg_losses
            ):
                return RiskDecision(
                    action="SELL",
                    ticker=ticker,
                    quantity=pos.quantity,
                    reason=f"TAX-LOSS HARVESTING: Selling loser ({pnl_pct:.2%}) to offset INR {self.state.realized_stcg_gains:.2f} realized capital gains",
                    priority=5,
                )

        # --- 2. Evaluate Short Position ---
        if ticker in self.state.shorts:
            pos = self.state.shorts[ticker]
            short_pnl_pct = (pos.avg_price - current_price) / pos.avg_price  # positive is profit

            # A. Bi-Weekly 10-Day Settlement Square-Off Protection
            # Cycle day in 10-day settlement
            cycle_day = current_day % config.SETTLEMENT_CYCLE_DAYS
            is_near_settlement = cycle_day in [0, config.SETTLEMENT_CYCLE_DAYS - 1, config.SETTLEMENT_CYCLE_DAYS - 2]
            if is_near_settlement or days_rem <= config.END_OF_SIM_DAYS_REMAINING:
                return RiskDecision(
                    action="COVER",
                    ticker=ticker,
                    quantity=pos.quantity,
                    reason=f"PRE-SETTLEMENT COVER: Proactively covering short before Day {current_day} settlement / end of sim",
                    priority=1,
                )

            # B. Short Stop-Loss
            if short_pnl_pct <= -config.SHORT_STOP_LOSS_PCT:
                return RiskDecision(
                    action="COVER",
                    ticker=ticker,
                    quantity=pos.quantity,
                    reason=f"SHORT STOP-LOSS hit: Price surged {abs(short_pnl_pct):.2%}, covering to protect margin",
                    priority=1,
                )

            # C. Short Take-Profit or Reversal
            if short_pnl_pct >= 0.045 or composite >= config.COVER_THRESHOLD:
                return RiskDecision(
                    action="COVER",
                    ticker=ticker,
                    quantity=pos.quantity,
                    reason=f"SHORT PROFIT COVER: Target gain reached ({short_pnl_pct:.2%}) or signal reversed ({composite:.2f})",
                    priority=3,
                )

        return None

    def evaluate_entry(
        self,
        ticker: str,
        current_price: float,
        signal_data: Dict[str, Any],
    ) -> RiskDecision:
        """Evaluates entry eligibility (BUY or SHORT) or vetoes into DO NOTHING."""
        composite = signal_data.get("final_composite", 0.0)
        news_score = signal_data.get("news_score", 0.0)
        current_day = self.state.current_day_number
        days_rem = self.state.days_remaining

        # Check circuit breaker
        if self.state.check_circuit_breaker():
            return RiskDecision(
                action="DO_NOTHING",
                ticker=ticker,
                quantity=0,
                reason="CIRCUIT BREAKER ACTIVE: Max drawdown threshold exceeded; pausing entries to preserve capital",
            )

        # Check simulation stage
        if self.state.stage != "simulation" or self.state.status != "RUNNING":
            return RiskDecision(
                action="DO_NOTHING",
                ticker=ticker,
                quantity=0,
                reason=f"STAGE GATED: Platform stage is '{self.state.stage}' / '{self.state.status}' (must be simulation/RUNNING)",
            )

        # Near end of simulation - do not enter new positions
        if days_rem <= config.END_OF_SIM_DAYS_REMAINING:
            return RiskDecision(
                action="DO_NOTHING",
                ticker=ticker,
                quantity=0,
                reason=f"END OF SIMULATION: Only {days_rem} days remaining; no new entries permitted",
            )

        total_wealth = self.state.get_total_wealth()
        free_cash = self.state.available_cash
        required_cash_cushion = total_wealth * config.CASH_RESERVE_PCT

        # Ensure cash cushion preserved
        deployable_cash = max(0.0, free_cash - required_cash_cushion)

        # --- BUY Opportunity ---
        if composite >= config.BUY_THRESHOLD:
            # Check maximum concurrent positions
            active_positions = len(self.state.longs) + len(self.state.shorts)
            if active_positions >= config.MAX_PORTFOLIO_POSITIONS:
                return RiskDecision(
                    action="DO_NOTHING",
                    ticker=ticker,
                    quantity=0,
                    reason=f"DIVERSIFICATION CAP: Max portfolio positions ({config.MAX_PORTFOLIO_POSITIONS}) reached",
                )

            # Slot sizing
            max_stock_capital = total_wealth * config.MAX_STOCK_ALLOCATION_PCT
            # Scale-in factor on early days
            if current_day <= 2:
                max_stock_capital *= config.SCALE_IN_FACTOR

            target_allocation = min(max_stock_capital, deployable_cash)
            # Subtract brokerage buffer
            target_allocation = max(0.0, target_allocation - config.BROKERAGE_FEE)

            if current_price <= 0:
                return RiskDecision(action="DO_NOTHING", ticker=ticker, quantity=0, reason="Invalid price")

            quantity = int(target_allocation / current_price)
            if quantity < 1:
                return RiskDecision(
                    action="DO_NOTHING",
                    ticker=ticker,
                    quantity=0,
                    reason=f"CAPITAL CONSTRAINT: Deployable cash (INR {deployable_cash:.2f}) insufficient for 1 share @ INR {current_price:.2f}",
                )

            # Fee Awareness & Friction Sanity Check:
            round_trip_friction = (2 * config.BROKERAGE_FEE) + config.DP_CHARGE
            expected_gain = (quantity * current_price) * config.TAKE_PROFIT_PCT
            if expected_gain < (config.MIN_EXPECTED_PROFIT_RATIO * round_trip_friction):
                return RiskDecision(
                    action="DO_NOTHING",
                    ticker=ticker,
                    quantity=0,
                    reason=f"FRICTION FILTER: Expected gain INR {expected_gain:.2f} < 3x round-trip fees (INR {round_trip_friction * 3:.2f}); skipping low-margin churn",
                )

            return RiskDecision(
                action="BUY",
                ticker=ticker,
                quantity=quantity,
                reason=f"BUY SIGNAL: Composite score {composite:.2f} >= {config.BUY_THRESHOLD:.2f}. Allocated INR {quantity * current_price:.2f}",
                priority=6,
                expected_gain=expected_gain,
            )

        # --- SHORT Opportunity ---
        if composite <= config.SHORT_THRESHOLD and news_score <= -0.25:
            # Shorting requires strict criteria: strong bearish composite AND negative news
            cycle_day = current_day % config.SETTLEMENT_CYCLE_DAYS
            if cycle_day in [0, 8, 9, 7]:
                return RiskDecision(
                    action="DO_NOTHING",
                    ticker=ticker,
                    quantity=0,
                    reason=f"SHORT SETTLEMENT VETO: Settlement approaching on cycle day {cycle_day}; new shorts barred",
                )

            # Check max short exposure
            if (self.state.margin_locked / total_wealth) >= config.MAX_SHORT_EXPOSURE_PCT:
                return RiskDecision(
                    action="DO_NOTHING",
                    ticker=ticker,
                    quantity=0,
                    reason=f"SHORT EXPOSURE CAP: Current short margin {self.state.margin_locked / total_wealth:.1%} reached max {config.MAX_SHORT_EXPOSURE_PCT:.1%}",
                )

            max_short_capital = total_wealth * 0.10  # 10% slot for short
            target_allocation = min(max_short_capital, deployable_cash) - config.BROKERAGE_FEE
            quantity = int(target_allocation / current_price) if current_price > 0 else 0

            if quantity < 1:
                return RiskDecision(
                    action="DO_NOTHING",
                    ticker=ticker,
                    quantity=0,
                    reason="INSUFFICIENT MARGIN for short trade",
                )

            return RiskDecision(
                action="SHORT",
                ticker=ticker,
                quantity=quantity,
                reason=f"SHORT SIGNAL: Strongly bearish composite {composite:.2f} with negative news ({news_score:.2f}). Margin INR {quantity * current_price:.2f}",
                priority=7,
            )

        return RiskDecision(
            action="DO_NOTHING",
            ticker=ticker,
            quantity=0,
            reason=f"DO NOTHING: Composite score {composite:.2f} within neutral band [{config.SELL_THRESHOLD:.2f}, {config.BUY_THRESHOLD:.2f}]",
        )
