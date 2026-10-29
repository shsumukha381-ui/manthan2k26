import os
import queue
import sys
import time

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from api_client import ApiClient
from config import config
from executor import OrderExecutor
from heartbeat import HeartbeatManager
from indicators import IndicatorsManager
from kafka_client import KafkaMarketConsumer, DayBatch
from logger import audit_logger, system_logger
from mock.exchange import app as fastapi_app, exchange as mock_exchange_state
from mock.replayer import MarketReplayer
from report import PerformanceReporter
from risk import RiskManager
from sentiment import SentimentAnalyzer
from signals import compute_composite_signal
from state import PortfolioState

class MockHttpAdapterApiClient(ApiClient):
    """ApiClient routed directly into FastAPI TestClient for in-process accelerated simulation."""

    def __init__(self):
        super().__init__()
        self.client = TestClient(fastapi_app)

    def _request(self, method: str, path: str, data=None, params=None, auth_required=True, max_retries=2):
        headers = self._headers(auth_required=auth_required)
        full_path = path if path.startswith("/api/v1") else f"/api/v1{path}"
        start_t = time.monotonic()
        resp = self.client.request(
            method=method,
            url=full_path,
            headers=headers,
            json=data,
            params=params,
        )
        latency_ms = (time.monotonic() - start_t) * 1000.0
        try:
            payload = resp.json()
            if isinstance(payload, dict):
                payload["_latency_ms"] = latency_ms
                payload["_status_code"] = resp.status_code
            return payload
        except Exception:
            return {"status": "error", "_status_code": resp.status_code, "_latency_ms": latency_ms}


def run_accelerated_backtest(total_days: int = 30):
    print("================================================================")
    print("      RUNNING ACCELERATED 30-DAY MOCK SIMULATION & BACKTEST     ")
    print("================================================================")

    # 1. Setup in-process mock exchange and client
    mock_api = MockHttpAdapterApiClient()
    msg_queue = queue.Queue()
    replayer = MarketReplayer(target_queue=msg_queue)

    # Initialize bot components
    state = PortfolioState(initial_cash=config.INITIAL_CAPITAL)
    heartbeat_mgr = HeartbeatManager(mock_api)
    indicators_mgr = IndicatorsManager()
    sentiment_analyzer = SentimentAnalyzer(decay_half_life_days=config.NEWS_DECAY_HALF_LIFE_DAYS)
    risk_manager = RiskManager(state)
    executor = OrderExecutor(mock_api, state)
    kafka_consumer = KafkaMarketConsumer(
        on_preflight_callback=heartbeat_mgr.submit_seq,
        mock_queue=msg_queue,
    )
    kafka_consumer.connect()

    # 2. Preflight discovery & warm start
    stocks = mock_api.get_stocks()
    tickers = [s["ticker"] for s in stocks]
    print(f"[Init] Discovered tickers: {tickers}")
    executor.update_valid_tickers(tickers)
    kafka_consumer.aggregator.update_expected_tickers(tickers)

    print("[Init] Warm-starting indicators with 200 days history...")
    for t in tickers:
        history = mock_api.get_stock_history(t, days=200)
        indicators_mgr.warm_start_ticker(t, history)

    # 3. Generate simulation scenario events
    events = replayer.generate_scenario_data(total_days=total_days)
    replayer.push_all_events(events)

    # Update state stage
    state.stage = "simulation"
    state.status = "RUNNING"
    state.total_days = total_days
    mock_exchange_state.stage = "simulation"
    mock_exchange_state.status = "RUNNING"

    decision_latencies = []

    # 4. Run the 30-day simulation loop
    print("\n[Simulation] Ingesting Kafka batches & executing autonomous decisions...")
    for day_num in range(1, total_days + 1):
        batch = kafka_consumer.poll_day_batch(timeout_s=0.5)
        if not batch or not batch.ticks:
            continue

        cycle_start = time.monotonic()
        date = batch.simulated_date
        state.current_simulated_date = date
        state.current_day_number = day_num
        state.days_remaining = max(0, total_days - day_num)

        # Update mock exchange clock & prices
        day_prices = {ticker: candle.close for ticker, candle in batch.ticks.items()}
        mock_exchange_state.advance_day(date, day_prices)

        # A. Update indicators
        for ticker, candle in batch.ticks.items():
            state.update_price(ticker, candle.close)
            indicators_mgr.update_tick(
                ticker=ticker,
                open_p=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close,
                volume=candle.volume,
                date=date,
            )

        # B. Ingest news
        for news_item in batch.news:
            score = sentiment_analyzer.add_news(news_item.ticker, news_item.headline, day_num)
            print(f"  Day {day_num:02d} NEWS [{news_item.ticker}] (Score={score:+.2f}): {news_item.headline[:70]}...")

        # C. Market regime
        market_breadth = indicators_mgr.get_market_breadth()

        # D. Generate decisions for all tickers
        decisions = []
        for ticker in tickers:
            buf = indicators_mgr.get_or_create(ticker)
            curr_price = day_prices.get(ticker, buf.closes[-1])
            decayed_sent, last_headline = sentiment_analyzer.get_decayed_sentiment(ticker, day_num)

            signal_data = compute_composite_signal(
                buf=buf,
                news_score=decayed_sent,
                headline=last_headline,
                market_breadth=market_breadth,
            )

            # Evaluate exits first
            pos_dec = risk_manager.evaluate_position_risk(ticker, curr_price, signal_data)
            if pos_dec:
                decision = pos_dec
            else:
                decision = risk_manager.evaluate_entry(ticker, curr_price, signal_data)

            # Log decision
            audit_logger.log_decision(
                ticker=ticker,
                simulated_date=date,
                signals={
                    "trend": signal_data["trend_score"],
                    "mean_reversion": signal_data["mean_reversion_score"],
                    "momentum": signal_data["momentum_score"],
                    "news": signal_data["news_score"],
                },
                composite_score=signal_data["final_composite"],
                action=decision.action,
                quantity=decision.quantity,
                executed=(decision.action != "DO_NOTHING"),
                reason=decision.reason,
                market_price=curr_price,
            )

            if decision.action != "DO_NOTHING":
                decisions.append(decision)

        # E. Execute prioritized decisions
        if decisions:
            executor.execute_decisions(
                decisions=decisions,
                simulated_date=date,
                prices=day_prices,
                max_time_budget_s=1.5,
            )

        cycle_latency_ms = (time.monotonic() - cycle_start) * 1000.0
        decision_latencies.append(cycle_latency_ms)

        # Reconcile with exchange portfolio
        port_res = mock_api.get_portfolio()
        if port_res.get("status") == "ok":
            state.reconcile_from_api(port_res)

        print(
            f"  Day {day_num:02d} ({date}) | Total Wealth: INR {state.get_total_wealth():,.2f} | Cash: INR {state.available_cash:,.2f} | Latency: {cycle_latency_ms:.2f}ms"
        )

    # 5. Simulation Complete - Generate and print Report
    report = PerformanceReporter.generate_report(
        state=state,
        decision_latencies=decision_latencies,
        output_file="mock_simulation_report.json",
    )
    PerformanceReporter.print_summary(report)
    return report

if __name__ == "__main__":
    run_accelerated_backtest(total_days=30)
