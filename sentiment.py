import math
import re
import threading
from typing import Dict, List, Optional, Tuple

class SentimentAnalyzer:
    """Deterministic, microsecond-latency keyword and phrase sentiment analyzer with negation and intensity handling, and multi-day exponential decay."""

    # Core lexicons weighted [-2.0 to +2.0]
    BULLISH_TERMS: Dict[str, float] = {
        "RECORD PROFIT": 2.0,
        "PROFIT SURGES": 2.0,
        "MASSIVE GROWTH": 2.0,
        "BEAT ESTIMATES": 1.8,
        "RECORD EXPORTS": 1.8,
        "NET PROFIT SURGES": 2.0,
        "CONTRACT WIN": 1.7,
        "MEGA DEAL": 1.8,
        "STRATEGIC PARTNERSHIP": 1.6,
        "SURGE": 1.3,
        "SURGES": 1.3,
        "PROFIT": 1.2,
        "PROFITS": 1.2,
        "GROWTH": 1.0,
        "RECORD": 1.2,
        "BEAT": 1.2,
        "BEATS": 1.2,
        "EXPANSION": 1.1,
        "EXPANDS": 1.1,
        "DIVIDEND": 1.2,
        "DIVIDENDS": 1.2,
        "PARTNERSHIP": 1.2,
        "AWARDED": 1.3,
        "UPGRADE": 1.4,
        "UPGRADED": 1.4,
        "OUTPERFORM": 1.4,
        "BUYBACK": 1.3,
        "ACQUISITION": 1.1,
        "REVENUE JUMP": 1.5,
        "RALLY": 1.2,
        "HIGHER": 0.8,
        "SOARS": 1.5,
        "BOOST": 1.1,
    }

    BEARISH_TERMS: Dict[str, float] = {
        "ACCOUNTING FRAUD": -2.5,
        "PROBE ORDERED": -2.2,
        "CEO RESIGNATION": -2.0,
        "REGULATORY SCRUTINY": -2.0,
        "MASSIVE LOSS": -2.2,
        "DEFAULT": -2.5,
        "BANKRUPTCY": -2.5,
        "FRAUD": -2.2,
        "SCRUTINY": -1.8,
        "PROBE": -1.8,
        "INVESTIGATION": -1.8,
        "RESIGNATION": -1.6,
        "RESIGNS": -1.6,
        "DOWNGRADE": -1.7,
        "DOWNGRADED": -1.7,
        "PLUNGE": -1.5,
        "PLUNGES": -1.5,
        "LOSS": -1.3,
        "LOSSES": -1.3,
        "CRASH": -1.8,
        "DROP": -1.0,
        "DROPS": -1.0,
        "SLUMP": -1.3,
        "SLUMPS": -1.3,
        "PENALTY": -1.5,
        "VIOLATION": -1.6,
        "DEFICIT": -1.2,
        "WEAK": -1.0,
        "MISSES ESTIMATES": -1.6,
        "DISMISSAL": -1.4,
        "SHUTDOWN": -1.8,
        "RAID": -2.0,
    }

    NEGATIONS = {"NOT", "NO", "NEVER", "FAILS", "FAILED", "WITHOUT", "UNABLE", "DENIES"}
    INTENSIFIERS = {"MASSIVE": 1.5, "RECORD": 1.4, "HISTORIC": 1.5, "SURGES": 1.3, "HUGE": 1.3, "EXTREME": 1.5}

    def __init__(self, decay_half_life_days: float = 3.0):
        self.decay_half_life = decay_half_life_days
        # Memory of news events per ticker: list of (day_number, score, headline)
        self.news_history: Dict[str, List[Tuple[int, float, str]]] = {}
        self.decay_constant = math.log(2) / max(self.decay_half_life, 0.1)

    def analyze_text(self, text: str) -> float:
        """Evaluates text into a bounded sentiment score between -1.0 and +1.0."""
        if not text:
            return 0.0

        upper_text = text.upper()
        clean_text = re.sub(r"[^A-Z0-9\s%]", " ", upper_text)
        words = clean_text.split()
        score = 0.0

        # Check for phrase matches first
        for phrase, weight in self.BULLISH_TERMS.items():
            if " " in phrase and phrase in upper_text:
                score += weight * 1.2
        for phrase, weight in self.BEARISH_TERMS.items():
            if " " in phrase and phrase in upper_text:
                score += weight * 1.2

        # Token-by-token evaluation with negation window of 2 words
        for idx, word in enumerate(words):
            is_negated = False
            for lookback in [1, 2]:
                if idx - lookback >= 0 and words[idx - lookback] in self.NEGATIONS:
                    is_negated = True
                    break

            multiplier = 1.0
            # Intensity multiplier if preceded by intensifier or followed by percentage
            if idx > 0 and words[idx - 1] in self.INTENSIFIERS:
                multiplier *= self.INTENSIFIERS[words[idx - 1]]
            if idx + 1 < len(words) and ("%" in words[idx + 1] or words[idx + 1].isdigit()):
                multiplier *= 1.3

            if word in self.BULLISH_TERMS and " " not in word:
                val = self.BULLISH_TERMS[word] * multiplier
                score += (-val * 0.9) if is_negated else val
            elif word in self.BEARISH_TERMS and " " not in word:
                val = self.BEARISH_TERMS[word] * multiplier
                score += (-val * 0.5) if is_negated else val

        # Bounded between -1.0 and +1.0 using tanh squashing
        normalized = math.tanh(score / 2.5)
        return round(normalized, 4)

    def add_news(self, ticker: str, headline: str, day_number: int) -> float:
        score = self.analyze_text(headline)
        if ticker not in self.news_history:
            self.news_history[ticker] = []
        self.news_history[ticker].append((day_number, score, headline))
        return score

    def get_decayed_sentiment(self, ticker: str, current_day: int) -> Tuple[float, Optional[str]]:
        """Calculates exponentially decayed cumulative sentiment for a ticker."""
        if ticker not in self.news_history or not self.news_history[ticker]:
            return 0.0, None

        total_score = 0.0
        most_recent_headline = self.news_history[ticker][-1][2]

        for day, score, _ in self.news_history[ticker]:
            days_elapsed = max(0, current_day - day)
            decay_factor = math.exp(-self.decay_constant * days_elapsed)
            total_score += score * decay_factor

        # Bounded between -1.0 and +1.0
        clamped = max(-1.0, min(1.0, total_score))
        return round(clamped, 4), most_recent_headline
