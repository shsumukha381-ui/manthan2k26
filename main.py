import os
import signal
import sys
import time
from typing import Dict, List, Optional

from api_client import ApiClient
from config import config
from executor import OrderExecutor
from heartbeat import HeartbeatManager
from indicators import IndicatorsManager
from kafka_client import KafkaMarketConsumer, DayBatch
from logger import audit_logger, system_logger
from report import PerformanceReporter
from risk import RiskManager, RiskDecision
from sentiment import SentimentAnalyzer
from signals import compute_composite_signal
from stage_monitor import StageMonitor
from state import PortfolioState

class ManthanTradingBot:
    """Production-grade Autonomous Trading Bot for MANTHAN 2026."""

    def __init__(self, api_client: Optional[ApiClient] = None, kafka_consumer: Optional[KafkaMarketConsumer] = None):
        self.config = config
        self.state = PortfolioState(initial_cash=config.INITIAL_CAPITAL)
        self.api_client = api_client or ApiClient()
        self.heartbeat_mgr = HeartbeatManager(self.api_client)
        self.stage_monitor = StageMonitor(self.api_client, self.state)
        self.indicators_mgr = IndicatorsManager()
        self.sentiment_analyzer = SentimentAnalyzer(decay_half_life_days=config.NEWS_DECAY_HALF_LIFE_DAYS)
        self.risk_manager = RiskManager(self.state)
        self.executor = OrderExecutor(self.api_client, self.state)
        self.kafka_consumer = kafka_consumer or KafkaMarketConsumer(
            on_preflight_callback=self.heartbeat_mgr.submit_seq
        )

        self.tradable_tickers: List[str] = []
        self.decision_latencies: List[float] = []
        self._running = False

    def startup(self):
        """Executes the mandatory hackathon startup sequence."""
        system_logger.info("==================================================")
        system_logger.info("   MANTHAN 2026 AUTONOMOUS TRADING BOT STARTUP    ")
        system_logger.info(f"Team: {self.config.TEAM_USERNAME} | Mode: {self.config.MODE.upper()}")
        system_logger.info(f"Gateway: {self.config.api_base_url} | Kafka: {self.config.kafka_broker}")
        system_logger.info("==================================================")

        # 1. Start heartbeat thread early to catch any standby preflight sequence
        self.heartbeat_mgr.start()

        # 2. Dynamic Stock Universe Discovery
        system_logger.info("Fetching dynamic stock universe via GET /api/v1/stocks...")
        stocks = self.api_client.get_stocks()
        if not stocks:
            system_logger.warning("No stocks returned or gateway offline; defaulting to standard test tickers")
            self.tradable_tickers = ["ALPHACORP", "BETATECH", "GAMMAINDUSTRIES", "DELTALABS"]
        else:
            self.tradable_tickers = [s.get("ticker", s.get("symbol", "")) for s in stocks if s.get("ticker") or s.get("symbol")]
            system_logger.info(f"Discovered {len(self.tradable_tickers)} dynamic stocks: {self.tradable_tickers}")

        self.executor.update_valid_tickers(self.tradable_tickers)
        self.kafka_consumer.aggregator.update_expected_tickers(self.tradable_tickers)

        # 3. Warm-start Technical Indicator Buffers
        system_logger.info("Warm-starting technical indicator buffers via GET /stocks/:ticker/history?days=200...")
        for ticker in self.tradable_tickers:
            history = self.api_client.get_stock_history(ticker, days=config.HISTORY_LOOKBACK_DAYS)
            if history:
                self.indicators_mgr.warm_start_ticker(ticker, history)
                system_logger.debug(f"Primed {len(history)} historical bars for {ticker}")

        # 4. Connect Kafka Consumer
        self.kafka_consumer.connect()

        # 5. Start Stage Monitor and synchronize lifecycle
        self.stage_monitor.sync_stage_once()
        self.stage_monitor.start()

        # 6. Synchronize initial portfolio
        portfolio_res = self.api_client.get_portfolio()
        if portfolio_res and portfolio_res.get("status") == "ok":
            self.state.reconcile_from_api(portfolio_res)

        self._running = True
        system_logger.info(f"Startup complete. Bot ready in stage: {self.state.stage}/{self.state.status}")

    def run(self):
        """Main autonomous trading loop processing daily market batches."""
        try:
            self.startup()

            # Pre-flight phase wait
            while self._running and not (self.state.stage == "simulation" and self.state.status == "RUNNING"):
                system_logger.info(
                    f"Awaiting LIVE simulation... Current Stage: {self.state.stage} Status: {self.state.status}"
                )
                time.sleep(2.0)
                self.stage_monitor.sync_stage_once()
                if self.state.status == "COMPLETED":
                    system_logger.info("Simulation completed during wait.")
                    break

            system_logger.info(">>> LIVE SIMULATION ACTIVE! COMMENCING AUTOMATED TRADING <<<")

            while self._running:
                # Check lifecycle stage
                if self.state.status in ["PAUSED", "COMPLETED"]:
                    if self.state.status == "COMPLETED":
                        system_logger.info("Simulation status is COMPLETED. Gracefully unwinding.")
                        break
                    time.sleep(1.0)
                    continue

                # Poll for day's aggregated market batch
                batch = self.kafka_consumer.poll_day_batch(timeout_s=1.5)
                if not batch or not batch.ticks:
                    continue

                self.process_day_batch(batch)

        except KeyboardInterrupt:
            system_logger.warning("Bot interrupted by operator.")
        except Exception as e:
            system_logger.critical(f"Unhandled exception in trading engine: {e}", exc_info=True)
        finally:
            self.shutdown()

    def process_day_batch(self, batch: DayBatch):
        """Processes one simulated trading day end-to-end within the ~1.5s latency budget."""
        cycle_start = time.monotonic()
        date = batch.simulated_date
        self.state.current_simulated_date = date

        # 1. Update prices & indicators for all received ticks
        day_prices: Dict[str, float] = {}
        for ticker, candle in batch.ticks.items():
            self.state.update_price(ticker, candle.close)
            day_prices[ticker] = candle.close
            self.indicators_mgr.update_tick(
                ticker=ticker,
                open_p=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close,
                volume=candle.volume,
                date=date,
            )

        # 2. Ingest breaking corporate news
        for news_item in batch.news:
            score = self.sentiment_analyzer.add_news(
                ticker=news_item.ticker,
                headline=news_item.headline,
                day_number=self.state.current_day_number,
            )
            system_logger.info(f"NEWS FLASH [{news_item.ticker}] (Score={score:+.2f}): {news_item.headline}")

        # 3. Assess overall market breadth & regime
        market_breadth = self.indicators_mgr.get_market_breadth()

        # 4. Multi-factor Signal & Risk Evaluation for every stock
        decisions: List[RiskDecision] = []

        for ticker in self.tradable_tickers:
            buf = self.indicators_mgr.get_or_create(ticker)
            if not buf.closes:
                continue

            curr_price = day_prices.get(ticker, buf.closes[-1])
            decayed_sent, last_headline = self.sentiment_analyzer.get_decayed_sentiment(
                ticker, self.state.current_day_number
            )

            # Signal generation
            signal_data = compute_composite_signal(
                buf=buf,
                news_score=decayed_sent,
                headline=last_headline,
                market_breadth=market_breadth,
            )

            # Risk veto & position evaluation
            # Check exits for existing holdings first
            pos_decision = self.risk_manager.evaluate_position_risk(ticker, curr_price, signal_data)
            if pos_decision:
                decision = pos_decision
            else:
                # Evaluate new entry
                decision = self.risk_manager.evaluate_entry(ticker, curr_price, signal_data)

            # Structured Audit Trail Logging (decisions.jsonl)
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

        # 5. Execute prioritized decisions
        if decisions:
            self.executor.execute_decisions(
                decisions=decisions,
                simulated_date=date,
                prices=day_prices,
                max_time_budget_s=2.0,
            )

        # 6. Track cycle latency
        cycle_latency_ms = (time.monotonic() - cycle_start) * 1000.0
        self.decision_latencies.append(cycle_latency_ms)

        system_logger.info(
            f"Day {self.state.current_day_number:02d} ({date}) Complete | Wealth: INR {self.state.get_total_wealth():,.2f} | Latency: {cycle_latency_ms:.1f}ms | Orders: {len(decisions)}"
        )

        # Periodic portfolio reconciliation (every 5 days)
        if self.state.current_day_number % 5 == 0:
            port = self.api_client.get_portfolio()
            if port and port.get("status") == "ok":
                self.state.reconcile_from_api(port)

        self.state.current_day_number += 1

    def shutdown(self):
        """Cleans up threads, closes sockets, and outputs scorecard."""
        self._running = False
        self.heartbeat_mgr.stop()
        self.stage_monitor.stop()
        self.kafka_consumer.close()

        # Generate final performance report
        report = PerformanceReporter.generate_report(
            state=self.state,
            decision_latencies=self.decision_latencies,
            output_file=self.config.REPORT_FILE,
        )
        PerformanceReporter.print_summary(report)

if __name__ == "__main__":
    bot = ManthanTradingBot()
    bot.run()
