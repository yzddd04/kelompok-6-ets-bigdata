"""
Producer API — CoinGecko Simple Price
Polling harga BTC, ETH, BNB setiap 60 detik dari CoinGecko,
kirim ke Kafka topic `crypto-api` dengan key = simbol koin.

# [Ahmad Yazid Arifuddin — 5027241040]: file ini dan semua fungsinya.

Jalankan:
    python kafka/producer_api.py
"""

import json
import time
import logging
import signal
import sys
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_API = "crypto-api"

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINS = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "binancecoin": "BNB",
}
POLL_INTERVAL_SEC = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer_api] %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        enable_idempotence=True,
        retries=5,
        linger_ms=50,
    )


def fetch_prices() -> dict:
    params = {
        "ids": ",".join(COINS.keys()),
        "vs_currencies": "usd,idr",
        "include_24hr_change": "true",
        "include_last_updated_at": "true",
    }
    resp = requests.get(COINGECKO_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def to_events(raw: dict) -> list[dict]:
    now_iso = datetime.now(timezone.utc).isoformat()
    events = []
    for gecko_id, symbol in COINS.items():
        data = raw.get(gecko_id)
        if not data:
            continue
        events.append({
            "symbol": symbol,
            "gecko_id": gecko_id,
            "price_usd": float(data.get("usd", 0.0)),
            "price_idr": float(data.get("idr", 0.0)),
            "change_24h": float(data.get("usd_24h_change", 0.0)),
            "last_updated_at": int(data.get("last_updated_at", 0)),
            "timestamp": now_iso,
            "source": "coingecko",
        })
    return events


_running = True


def _graceful_stop(signum, _frame):
    global _running
    log.info("Sinyal %s diterima — menghentikan producer...", signum)
    _running = False


def main():
    signal.signal(signal.SIGINT, _graceful_stop)
    signal.signal(signal.SIGTERM, _graceful_stop)

    producer = build_producer()
    log.info("Terhubung ke Kafka %s — mulai polling CoinGecko tiap %ds",
             KAFKA_BOOTSTRAP, POLL_INTERVAL_SEC)

    while _running:
        start = time.monotonic()
        try:
            raw = fetch_prices()
            events = to_events(raw)
            for ev in events:
                future = producer.send(TOPIC_API, key=ev["symbol"], value=ev)
                try:
                    meta = future.get(timeout=10)
                    log.info("Kirim %s USD=%.2f chg=%.2f%% -> %s[p=%d o=%d]",
                             ev["symbol"], ev["price_usd"], ev["change_24h"],
                             meta.topic, meta.partition, meta.offset)
                except KafkaError as e:
                    log.error("Gagal kirim %s: %s", ev["symbol"], e)
            producer.flush()
        except requests.HTTPError as e:
            # CoinGecko sering balas 429 (rate limit) — backoff manual
            log.warning("HTTP error dari CoinGecko: %s", e)
        except requests.RequestException as e:
            log.warning("Network error: %s", e)
        except Exception as e:
            log.exception("Error tak terduga: %s", e)

        elapsed = time.monotonic() - start
        sleep_for = max(0.0, POLL_INTERVAL_SEC - elapsed)
        for _ in range(int(sleep_for)):
            if not _running:
                break
            time.sleep(1)

    producer.flush()
    producer.close(timeout=10)
    log.info("Producer API ditutup.")


if __name__ == "__main__":
    sys.exit(main())
