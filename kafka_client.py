import json
import queue
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from config import config
from logger import system_logger

@dataclass
class Candle:
    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    simulated_date: str

@dataclass
class NewsItem:
    ticker: str
    headline: str
    description: str
    simulated_date: str

@dataclass
class DayBatch:
    simulated_date: str
    ticks: Dict[str, Candle] = field(default_factory=dict)
    news: List[NewsItem] = field(default_factory=list)


class DayBatchAggregator:
    """Aggregates all ticks and T+150ms news flashes for a single simulated date, timing out safely."""

    def __init__(self, expected_tickers: Optional[List[str]] = None):
        self.expected_tickers = set(expected_tickers or [])
        self.current_batch: Optional[DayBatch] = None
        self.batch_start_mono: float = 0.0

    def update_expected_tickers(self, tickers: List[str]):
        self.expected_tickers = set(tickers)

    def process_tick(self, date: str, candle: Candle) -> Optional[DayBatch]:
        """Adds tick to current batch. If date has advanced, returns the completed previous batch."""
        completed_batch = None
        if self.current_batch is None:
            self.current_batch = DayBatch(simulated_date=date)
            self.batch_start_mono = time.monotonic()
        elif self.current_batch.simulated_date != date:
            # Simulated date rollover!
            completed_batch = self.current_batch
            self.current_batch = DayBatch(simulated_date=date)
            self.batch_start_mono = time.monotonic()

        self.current_batch.ticks[candle.ticker] = candle
        return completed_batch

    def process_news(self, date: str, news: NewsItem):
        if self.current_batch is None:
            self.current_batch = DayBatch(simulated_date=date)
            self.batch_start_mono = time.monotonic()
        self.current_batch.news.append(news)

    def is_batch_ready(self) -> bool:
        """Determines if the batch is ready: all tickers received + news window waited, or hard timeout hit."""
        if not self.current_batch:
            return False

        elapsed_ms = (time.monotonic() - self.batch_start_mono) * 1000.0

        # Hard timeout safety: if elapsed exceeds DAY_TIMEOUT_MS, never wait further
        if elapsed_ms >= config.DAY_TIMEOUT_MS:
            return True

        # Check if all expected tickers arrived AND (news window elapsed or mock mode)
        has_all_tickers = self.expected_tickers and (self.expected_tickers.issubset(self.current_batch.ticks.keys()))
        if config.MODE == "mock" and has_all_tickers:
            return True

        news_window_elapsed = elapsed_ms >= config.NEWS_WAIT_MS
        return bool(has_all_tickers and news_window_elapsed)

    def flush(self) -> Optional[DayBatch]:
        batch = self.current_batch
        self.current_batch = None
        return batch


class KafkaMarketConsumer:
    """Handles Kafka ingestion for 'market.ticks' and 'market.news' with mock fallback support."""

    def __init__(
        self,
        on_preflight_callback: Optional[Callable[[int], None]] = None,
        mock_queue: Optional[queue.Queue] = None,
    ):
        self.on_preflight = on_preflight_callback
        self.mock_queue = mock_queue
        self.consumer = None
        self._running = False
        self.aggregator = DayBatchAggregator()

    def connect(self):
        if config.MODE == "mock" or self.mock_queue is not None:
            system_logger.info("KafkaMarketConsumer: Operating in MOCK mode.")
            self._running = True
            return

        try:
            import confluent_kafka
            conf = {
                "bootstrap.servers": config.kafka_broker,
                "group.id": config.consumer_group,
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
                "session.timeout.ms": 10000,
                "max.poll.interval.ms": 30000,
            }
            self.consumer = confluent_kafka.Consumer(conf)
            self.consumer.subscribe([config.KAFKA_TOPIC_TICKS, config.KAFKA_TOPIC_NEWS])
            self._running = True
            system_logger.info(f"Kafka consumer subscribed to {config.KAFKA_TOPIC_TICKS}, {config.KAFKA_TOPIC_NEWS}")
        except Exception as e:
            system_logger.error(f"Failed to connect to Kafka broker at {config.kafka_broker}: {e}")
            raise

    def close(self):
        self._running = False
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass

    def poll_day_batch(self, timeout_s: float = 1.0) -> Optional[DayBatch]:
        """Polls messages until a day's batch is aggregated or timeout occurs."""
        start_time = time.monotonic()

        while self._running:
            # Check if current aggregator has a ready batch
            if self.aggregator.is_batch_ready():
                return self.aggregator.flush()

            # Poll from either mock queue or confluent-kafka
            raw_msg = None
            topic = None

            if self.mock_queue is not None:
                try:
                    item = self.mock_queue.get(timeout=0.05)
                    topic = item.get("topic")
                    raw_msg = item.get("value")
                except queue.Empty:
                    raw_msg = None
            elif self.consumer is not None:
                msg = self.consumer.poll(0.05)
                if msg is not None and not msg.error():
                    topic = msg.topic()
                    raw_msg = msg.value().decode("utf-8")

            if raw_msg is not None:
                try:
                    payload = json.loads(raw_msg) if isinstance(raw_msg, str) else raw_msg
                    event_type = payload.get("event_type", payload.get("event"))

                    # Preflight Heartbeat Tick
                    if event_type == "preflight_tick":
                        seq = payload.get("seq")
                        if seq is None and "data" in payload:
                            seq = payload["data"].get("seq")
                        if seq is not None and self.on_preflight:
                            self.on_preflight(int(seq))

                    # Daily Market Price Tick
                    elif event_type == "price_tick":
                        date = payload.get("simulated_date", "")
                        d = payload.get("data", {})
                        candle = Candle(
                            ticker=d.get("ticker", ""),
                            open=float(d.get("open", 0.0)),
                            high=float(d.get("high", 0.0)),
                            low=float(d.get("low", 0.0)),
                            close=float(d.get("close", 0.0)),
                            volume=float(d.get("volume", 0.0)),
                            simulated_date=date,
                        )
                        completed = self.aggregator.process_tick(date, candle)
                        if completed:
                            return completed

                    # Corporate News Flash
                    elif event_type == "news_flash":
                        date = payload.get("simulated_date", "")
                        d = payload.get("data", {})
                        news_item = NewsItem(
                            ticker=d.get("ticker", ""),
                            headline=d.get("headline", ""),
                            description=d.get("description", ""),
                            simulated_date=date,
                        )
                        self.aggregator.process_news(date, news_item)

                except Exception as e:
                    system_logger.warning(f"Malformed Kafka message ignored: {e}")

            if time.monotonic() - start_time >= timeout_s:
                if self.aggregator.current_batch and len(self.aggregator.current_batch.ticks) > 0:
                    return self.aggregator.flush()
                return None

        return None
