"""
Consumer Kafka -> HDFS
Membaca dari dua topic (crypto-api, crypto-rss) secara paralel (threading),
flush buffer ke HDFS tiap FLUSH_INTERVAL_SEC (3 menit) sebagai file JSON timestamp.

Bonus +2: Menulis ke HDFS pakai library Python `hdfs` (InsecureClient) — bukan subprocess.

Juga menyimpan salinan lokal (live_api.json, live_rss.json) ke dashboard/data/
untuk konsumsi oleh Flask dashboard.

# [Erlinda Annisa Zahra — 5027241108]: file ini dan semua fungsinya.

Jalankan:
    python kafka/consumer_to_hdfs.py
"""

import json
import logging
import signal
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from hdfs import InsecureClient
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP = "localhost:9092"
HDFS_URL = "http://localhost:9870"
HDFS_USER = "root"

TOPIC_API = "crypto-api"
TOPIC_RSS = "crypto-rss"
GROUP_ID = "crypto-hdfs-sink"

HDFS_DIRS = {
    TOPIC_API: "/data/crypto/api",
    TOPIC_RSS: "/data/crypto/rss",
}

FLUSH_INTERVAL_SEC = 180         # flush ke HDFS tiap 3 menit
LIVE_COPY_MAX_ITEMS = 50         # salinan lokal untuk dashboard

DASHBOARD_DATA_DIR = Path(__file__).parent.parent / "dashboard" / "data"
DASHBOARD_DATA_DIR.mkdir(parents=True, exist_ok=True)
LIVE_FILES = {
    TOPIC_API: DASHBOARD_DATA_DIR / "live_api.json",
    TOPIC_RSS: DASHBOARD_DATA_DIR / "live_rss.json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [consumer] %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


class TopicSink:
    """Buffer in-memory + flush berkala ke HDFS + update salinan live lokal."""

    def __init__(self, topic: str, hdfs_client: InsecureClient):
        self.topic = topic
        self.client = hdfs_client
        self.hdfs_dir = HDFS_DIRS[topic]
        self.live_path = LIVE_FILES[topic]
        self.buffer: list[dict] = []
        self.live_ring: deque[dict] = deque(maxlen=LIVE_COPY_MAX_ITEMS)
        self.lock = threading.Lock()
        self.ensure_hdfs_dir()
        self._load_existing_live()

    def ensure_hdfs_dir(self) -> None:
        try:
            self.client.makedirs(self.hdfs_dir)
            log.info("HDFS dir siap: %s", self.hdfs_dir)
        except Exception as e:
            log.warning("Tidak bisa makedirs %s: %s", self.hdfs_dir, e)

    def _load_existing_live(self) -> None:
        if self.live_path.exists():
            try:
                data = json.loads(self.live_path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    for item in data[-LIVE_COPY_MAX_ITEMS:]:
                        self.live_ring.append(item)
            except json.JSONDecodeError:
                pass

    def add(self, event: dict) -> None:
        with self.lock:
            self.buffer.append(event)
            self.live_ring.append(event)
        self._write_live_copy()

    def _write_live_copy(self) -> None:
        # Ditulis setiap event tiba supaya dashboard bisa tampilkan data paling baru.
        try:
            with self.lock:
                snapshot = list(self.live_ring)
            tmp = self.live_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2),
                           encoding="utf-8")
            tmp.replace(self.live_path)
        except Exception as e:
            log.warning("Gagal menulis live copy %s: %s", self.live_path, e)

    def flush_to_hdfs(self) -> int:
        with self.lock:
            if not self.buffer:
                return 0
            batch = self.buffer
            self.buffer = []

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"{ts}.json"
        hdfs_path = f"{self.hdfs_dir}/{filename}"
        payload = json.dumps(batch, ensure_ascii=False, indent=2).encode("utf-8")
        try:
            with self.client.write(hdfs_path, overwrite=True) as writer:
                writer.write(payload)
            log.info("FLUSH %s: %d event -> %s (%d bytes)",
                     self.topic, len(batch), hdfs_path, len(payload))
            return len(batch)
        except Exception as e:
            log.error("Gagal flush ke HDFS %s: %s", hdfs_path, e)
            # Kembalikan batch ke buffer agar tidak hilang
            with self.lock:
                self.buffer = batch + self.buffer
            return 0


def consume_topic(topic: str, sink: TopicSink, stop_event: threading.Event):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=1000,
    )
    log.info("Consumer topic=%s group_id=%s siap", topic, GROUP_ID)

    while not stop_event.is_set():
        try:
            for msg in consumer:
                if stop_event.is_set():
                    break
                event = msg.value
                # Sisipkan metadata Kafka supaya bisa dilihat di Spark kalau perlu
                event.setdefault("_kafka", {})
                event["_kafka"] = {
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "key": msg.key,
                }
                sink.add(event)
                log.debug("recv %s key=%s offset=%d", topic, msg.key, msg.offset)
        except Exception as e:
            log.exception("Error di consumer %s: %s", topic, e)
            time.sleep(2)

    consumer.close()
    log.info("Consumer %s ditutup.", topic)


def flush_scheduler(sinks: dict[str, TopicSink], stop_event: threading.Event):
    while not stop_event.is_set():
        # Tidur dulu supaya flush pertama terjadi setelah 1 interval
        for _ in range(FLUSH_INTERVAL_SEC):
            if stop_event.is_set():
                break
            time.sleep(1)
        for topic, sink in sinks.items():
            sink.flush_to_hdfs()


def main():
    hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
    # Pastikan root folder ada
    try:
        hdfs_client.makedirs("/data/crypto/hasil")
    except Exception as e:
        log.warning("Gagal memastikan /data/crypto/hasil: %s", e)

    sinks = {
        TOPIC_API: TopicSink(TOPIC_API, hdfs_client),
        TOPIC_RSS: TopicSink(TOPIC_RSS, hdfs_client),
    }

    stop_event = threading.Event()

    def _stop(signum, _frame):
        log.info("Sinyal %s diterima — menghentikan consumer...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    threads = [
        threading.Thread(target=consume_topic,
                         args=(TOPIC_API, sinks[TOPIC_API], stop_event),
                         name="consumer-api", daemon=True),
        threading.Thread(target=consume_topic,
                         args=(TOPIC_RSS, sinks[TOPIC_RSS], stop_event),
                         name="consumer-rss", daemon=True),
        threading.Thread(target=flush_scheduler, args=(sinks, stop_event),
                         name="flush-scheduler", daemon=True),
    ]
    for t in threads:
        t.start()

    log.info("Consumer siap. Flush tiap %ds. Ctrl+C untuk stop.", FLUSH_INTERVAL_SEC)

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()

    # Flush terakhir sebelum exit
    log.info("Flush terakhir sebelum exit...")
    for sink in sinks.values():
        sink.flush_to_hdfs()

    for t in threads:
        t.join(timeout=5)
    log.info("Consumer ditutup bersih.")


if __name__ == "__main__":
    sys.exit(main())
