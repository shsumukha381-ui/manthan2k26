import json
from typing import Any, Dict, List
from config import config
from logger import system_logger
from state import PortfolioState

class PerformanceReporter:
    """Generates an end-of-simulation scorecard and jury defense summary."""

    @staticmethod
    def generate_report(
        state: PortfolioState,
        decision_latencies: List[float],
        output_file: str = "simulation_report.json",
    ) -> Dict[str, Any]:
        final_wealth = state.get_total_wealth()
        net_return_pct = ((final_wealth - config.INITIAL_CAPITAL) / config.INITIAL_CAPITAL) * 100.0
        max_drawdown_pct = 0.0
        if state.peak_wealth > 0:
            max_drawdown_pct = ((state.peak_wealth - final_wealth) / state.peak_wealth) * 100.0

        # Calculate trade win rate from closed trades
        closed_trades = [t for t in state.trades_history if "net_pnl" in t]
        winning_trades = [t for t in closed_trades if t["net_pnl"] > 0]
        win_rate = (len(winning_trades) / len(closed_trades) * 100.0) if closed_trades else 0.0

        avg_latency = (sum(decision_latencies) / len(decision_latencies)) if decision_latencies else 0.0
        max_latency = max(decision_latencies) if decision_latencies else 0.0

        total_fees = state.total_brokerage_paid + state.total_dp_charges_paid
        est_tax = state.calculate_estimated_tax()

        report = {
            "title": "MANTHAN 2026 Autonomous Trading Systems Challenge - Performance Scorecard",
            "initial_capital": config.INITIAL_CAPITAL,
            "final_total_wealth": round(final_wealth, 2),
            "net_return_pct": round(net_return_pct, 2),
            "cash_remaining": round(state.available_cash, 2),
            "margin_locked": round(state.margin_locked, 2),
            "holdings_value": round(state.total_long_market_value(), 2),
            "peak_wealth": round(state.peak_wealth, 2),
            "max_drawdown_pct": round(max_drawdown_pct, 2),
            "total_orders_executed": state.order_count,
            "total_orders_rejected": state.rejection_count,
            "rejection_rate_pct": round(
                (state.rejection_count / max(1, state.order_count + state.rejection_count)) * 100.0, 2
            ),
            "total_closed_trades": len(closed_trades),
            "win_rate_pct": round(win_rate, 2),
            "total_brokerage_fees_inr": round(state.total_brokerage_paid, 2),
            "total_dp_charges_inr": round(state.total_dp_charges_paid, 2),
            "total_friction_inr": round(total_fees, 2),
            "estimated_stcg_tax_inr": round(est_tax, 2),
            "average_decision_latency_ms": round(avg_latency, 2),
            "maximum_decision_latency_ms": round(max_latency, 2),
            "active_long_positions": {k: {"qty": v.quantity, "avg_price": round(v.avg_price, 2)} for k, v in state.longs.items()},
            "active_short_positions": {k: {"qty": v.quantity, "avg_price": round(v.avg_price, 2)} for k, v in state.shorts.items()},
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        system_logger.info(f"Scorecard written to {output_file}")
        return report

    @staticmethod
    def print_summary(report: Dict[str, Any]):
        summary = f"""
============================================================
       MANTHAN 2026 TRADING BOT PERFORMANCE SCORECARD       
============================================================
Initial Virtual Capital : INR {report['initial_capital']:,.2f}
Final Total Wealth      : INR {report['final_total_wealth']:,.2f}
Net Profit / Loss       : {report['net_return_pct']:+.2f}%
Cash Remaining          : INR {report['cash_remaining']:,.2f}
Long Holdings Value     : INR {report['holdings_value']:,.2f}
Peak Net Worth          : INR {report['peak_wealth']:,.2f}
Maximum Drawdown        : {report['max_drawdown_pct']:.2f}%
------------------------------------------------------------
Total Executed Orders   : {report['total_orders_executed']}
Total Rejected Orders   : {report['total_orders_rejected']} (Error rate: {report['rejection_rate_pct']}%)
Trade Win Rate          : {report['win_rate_pct']:.1f}% ({report['total_closed_trades']} closed trades)
Brokerage Paid          : INR {report['total_brokerage_fees_inr']:,.2f}
DP Charges Paid         : INR {report['total_dp_charges_inr']:,.2f}
Est. Capital Gains Tax  : INR {report['estimated_stcg_tax_inr']:,.2f}
------------------------------------------------------------
Avg Decision Latency    : {report['average_decision_latency_ms']:.2f} ms (Target < 1500 ms)
Max Decision Latency    : {report['maximum_decision_latency_ms']:.2f} ms
============================================================
"""
        print(summary)
