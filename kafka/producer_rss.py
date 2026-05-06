"""
Producer RSS — CoinDesk (+ Cointelegraph sebagai backup)
Polling RSS feed setiap 5 menit, dedup via set of hash(URL),
kirim ke Kafka topic `crypto-rss` dengan key = hash 8 karakter dari URL.

# [Ahmad Yazid Arifuddin — 5027241040]: file ini dan semua fungsinya.

Jalankan:
    python kafka/producer_rss.py
"""

import hashlib
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import feedparser
from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_RSS = "crypto-rss"

RSS_FEEDS = [
    ("coindesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("cointelegraph", "https://cointelegraph.com/rss"),
]
POLL_INTERVAL_SEC = 300

SEEN_FILE = Path(__file__).parent / "buffer" / "rss_seen.json"
SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer_rss] %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            log.warning("File seen rusak — reset.")
    return set()


def save_seen(seen: set[str]) -> None:
    # Batasi ukuran cache supaya tidak membengkak
    if len(seen) > 5000:
        seen = set(list(seen)[-5000:])
    SEEN_FILE.write_text(json.dumps(list(seen)), encoding="utf-8")


def url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:8]


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        # enable_idempotence=True,
        retries=5,
        linger_ms=50,
    )


def parse_entry(entry, source: str) -> dict:
    return {
        "source": source,
        "title": (entry.get("title") or "").strip(),
        "link": (entry.get("link") or "").strip(),
        "summary": (entry.get("summary") or "").strip(),
        "published": entry.get("published") or entry.get("updated") or "",
        "author": entry.get("author") or "",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


_running = True


def _graceful_stop(signum, _frame):
    global _running
    log.info("Sinyal %s diterima — menghentikan producer...", signum)
    _running = False


def main():
    signal.signal(signal.SIGINT, _graceful_stop)
    signal.signal(signal.SIGTERM, _graceful_stop)

    producer = build_producer()
    seen = load_seen()
    log.info("Terhubung ke Kafka %s — %d URL sudah dalam cache",
             KAFKA_BOOTSTRAP, len(seen))

    while _running:
        start = time.monotonic()
        new_count = 0
        for source, feed_url in RSS_FEEDS:
            try:
                parsed = feedparser.parse(feed_url)
                if parsed.bozo:
                    log.warning("Feed %s bermasalah: %s", source, parsed.bozo_exception)
                for entry in parsed.entries:
                    link = (entry.get("link") or "").strip()
                    if not link:
                        continue
                    key = url_hash(link)
                    if key in seen:
                        continue
                    event = parse_entry(entry, source)
                    event["url_hash"] = key
                    try:
                        future = producer.send(TOPIC_RSS, key=key, value=event)
                        meta = future.get(timeout=10)
                        log.info("Kirim [%s] '%s' -> %s[p=%d o=%d]",
                                 source, event["title"][:70],
                                 meta.topic, meta.partition, meta.offset)
                        seen.add(key)
                        new_count += 1
                    except KafkaError as e:
                        log.error("Gagal kirim entry %s: %s", key, e)
            except Exception as e:
                log.exception("Error parsing feed %s: %s", source, e)

        if new_count:
            producer.flush()
            save_seen(seen)
            log.info("Batch selesai: %d artikel baru dikirim", new_count)
        else:
            log.info("Tidak ada artikel baru pada poll ini")

        elapsed = time.monotonic() - start
        sleep_for = max(0.0, POLL_INTERVAL_SEC - elapsed)
        for _ in range(int(sleep_for)):
            if not _running:
                break
            time.sleep(1)

    producer.flush()
    producer.close(timeout=10)
    save_seen(seen)
    log.info("Producer RSS ditutup.")


if __name__ == "__main__":
    sys.exit(main())
