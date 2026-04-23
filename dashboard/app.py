"""
Flask Dashboard — CryptoWatch
Serving layer yang menggabungkan hasil Spark (historis) + event live dari Kafka consumer.

# [Erlinda Annisa Zahra — 5027241108]: file ini + templates/index.html + static/{style.css,app.js}.

Berjalan di http://localhost:5000
Endpoint:
    GET /             -> HTML dashboard
    GET /api/data     -> JSON gabungan untuk frontend (fetched tiap 30 detik)
"""

import json
import logging
from pathlib import Path

from flask import Flask, jsonify, render_template


DATA_DIR = Path(__file__).parent / "data"
SPARK_RESULTS = DATA_DIR / "spark_results.json"
LIVE_API = DATA_DIR / "live_api.json"
LIVE_RSS = DATA_DIR / "live_rss.json"

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["JSON_SORT_KEYS"] = False

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [dashboard] %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        log.warning("File %s rusak: %s", path, e)
        return default


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    spark_results = load_json(SPARK_RESULTS, default={})
    live_api = load_json(LIVE_API, default=[])
    live_rss = load_json(LIVE_RSS, default=[])

    # Ambil harga terbaru per simbol dari buffer live API
    latest_by_symbol: dict[str, dict] = {}
    for ev in live_api:
        sym = ev.get("symbol")
        if not sym:
            continue
        prev = latest_by_symbol.get(sym)
        if prev is None or ev.get("timestamp", "") > prev.get("timestamp", ""):
            latest_by_symbol[sym] = ev

    # Berita terbaru (sort desc by timestamp, ambil 10 teratas)
    top_news = sorted(
        live_rss,
        key=lambda a: a.get("timestamp", ""),
        reverse=True,
    )[:10]

    return jsonify({
        "spark": spark_results,
        "latest_prices": list(latest_by_symbol.values()),
        "live_api_count": len(live_api),
        "live_rss_count": len(live_rss),
        "top_news": top_news,
        "has_data": {
            "spark": bool(spark_results),
            "live_api": bool(live_api),
            "live_rss": bool(live_rss),
        }
    })


@app.route("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Dashboard siap di http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
