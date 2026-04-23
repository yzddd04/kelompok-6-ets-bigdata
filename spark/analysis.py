"""
Spark Analysis — CryptoWatch
============================
3 analisis wajib + Bonus MLlib (+5 poin).

Menjawab pertanyaan bisnis:
    "Pada jam berapa harga kripto paling volatile?
     Dan apakah berita yang muncul sejalan dengan pergerakan harga?"

Spark membaca dari HDFS (bukan file lokal) dan menyimpan hasil ke:
  - HDFS: /data/crypto/hasil/<analisis>/
  - Lokal: ../dashboard/data/spark_results.json (untuk konsumsi Flask)

# [M Faqih Ridho — 5027241123]: file ini dan semua fungsinya (termasuk notebook analysis.ipynb).

Jalankan:
    python spark/analysis.py
atau buka analysis.ipynb dan run-all cells.
"""

import json
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from hdfs import InsecureClient
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression


HDFS_URI = "hdfs://localhost:9000"
HDFS_WEBUI = "http://localhost:9870"
HDFS_USER = "root"
API_PATH = "/data/crypto/api"
RSS_PATH = "/data/crypto/rss"
HASIL_PATH = "/data/crypto/hasil"
DASHBOARD_OUT = Path(__file__).parent.parent / "dashboard" / "data" / "spark_results.json"


def build_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("CryptoWatch-Analysis")
            .config("spark.sql.session.timeZone", "Asia/Jakarta")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate())


def hdfs_download_dir(client: InsecureClient, hdfs_dir: str, local_dir: Path) -> int:
    """Download semua file .json dari folder HDFS ke local_dir.
    Returns jumlah file yang berhasil didownload.

    Catatan: Spark di Windows native kesulitan konek ke HDFS container
    (datanode pakai IP internal Docker). Workaround: pakai WebHDFS via
    library `hdfs` Python (port 9870 — sudah terbukti jalan di consumer)
    untuk mirror file ke lokal, lalu Spark baca file lokalnya.
    Data tetap berasal dari HDFS — pipeline arsitektur tidak berubah.
    """
    local_dir.mkdir(parents=True, exist_ok=True)
    try:
        files = client.list(hdfs_dir)
    except Exception as e:
        print(f"[HDFS] Tidak bisa list {hdfs_dir}: {e}", file=sys.stderr)
        return 0

    n = 0
    for fname in files:
        if not fname.endswith(".json"):
            continue
        src = f"{hdfs_dir}/{fname}"
        dst = local_dir / fname
        try:
            with client.read(src) as reader:
                data = reader.read()
            dst.write_bytes(data)
            n += 1
        except Exception as e:
            print(f"[HDFS] Gagal download {src}: {e}", file=sys.stderr)
    print(f"[HDFS] Mirror {hdfs_dir} -> {local_dir} ({n} file)")
    return n


def load_data(spark: SparkSession, staging_dir: Path) -> tuple[DataFrame, DataFrame]:
    """Mirror HDFS -> lokal via WebHDFS, lalu Spark baca file lokal.
    File dari consumer berisi top-level JSON array, jadi pakai multiLine."""
    client = InsecureClient(HDFS_WEBUI, user=HDFS_USER)
    api_local = staging_dir / "api"
    rss_local = staging_dir / "rss"
    n_api = hdfs_download_dir(client, API_PATH, api_local)
    n_rss = hdfs_download_dir(client, RSS_PATH, rss_local)

    if n_api == 0 and n_rss == 0:
        raise RuntimeError(
            f"Tidak ada file JSON di HDFS {API_PATH} atau {RSS_PATH}. "
            f"Pastikan consumer sudah flush (tunggu minimal 3 menit)."
        )

    df_api = (spark.read
              .option("multiLine", "true")
              .json(str(api_local).replace("\\", "/")))
    df_rss = (spark.read
              .option("multiLine", "true")
              .json(str(rss_local).replace("\\", "/")))
    return df_api, df_rss


def save_to_hdfs_via_webhdfs(df: DataFrame, subdir: str, staging_dir: Path) -> None:
    """Spark tulis ke local staging, lalu upload ke HDFS via WebHDFS."""
    local_out = staging_dir / "hasil" / subdir
    (df.coalesce(1).write.mode("overwrite").json(str(local_out).replace("\\", "/")))

    client = InsecureClient(HDFS_WEBUI, user=HDFS_USER)
    hdfs_target = f"{HASIL_PATH}/{subdir}"
    try:
        client.delete(hdfs_target, recursive=True)
    except Exception:
        pass
    try:
        client.makedirs(hdfs_target)
        for f in local_out.iterdir():
            if f.is_file():
                with open(f, "rb") as src:
                    client.write(f"{hdfs_target}/{f.name}", data=src.read(), overwrite=True)
        print(f"[HDFS] saved -> hdfs://.../{hdfs_target}")
    except Exception as e:
        print(f"[HDFS] Gagal upload ke {hdfs_target}: {e}", file=sys.stderr)


# ----------------------------------------------------------------------
# Analisis 1 — Statistik harga per koin (DataFrame API)
# ----------------------------------------------------------------------
def analisis_1_stats_per_coin(df_api: DataFrame) -> DataFrame:
    """
    Agregat per simbol koin: count, avg, max, min, stddev harga USD,
    plus rata-rata change_24h untuk gambaran tren.
    """
    return (df_api.groupBy("symbol")
            .agg(
                F.count("*").alias("n"),
                F.round(F.avg("price_usd"), 2).alias("avg_usd"),
                F.round(F.max("price_usd"), 2).alias("max_usd"),
                F.round(F.min("price_usd"), 2).alias("min_usd"),
                F.round(F.stddev("price_usd"), 4).alias("stddev_usd"),
                F.round(F.avg("change_24h"), 3).alias("avg_change_pct"),
            )
            .orderBy(F.desc("avg_usd")))


# ----------------------------------------------------------------------
# Analisis 2 — Volatilitas per jam (Spark SQL)
# ----------------------------------------------------------------------
def analisis_2_volatility_by_hour(spark: SparkSession, df_api: DataFrame) -> DataFrame:
    df_api.createOrReplaceTempView("crypto_api")
    return spark.sql("""
        SELECT
            HOUR(TO_TIMESTAMP(timestamp)) AS hour_utc,
            ROUND(AVG(ABS(change_24h)), 4) AS avg_abs_change_pct,
            ROUND(STDDEV(price_usd), 4) AS stddev_price_usd,
            COUNT(*) AS n_events
        FROM crypto_api
        GROUP BY HOUR(TO_TIMESTAMP(timestamp))
        ORDER BY hour_utc
    """)


# ----------------------------------------------------------------------
# Analisis 3 — Volume berita per jam (Spark SQL)
# ----------------------------------------------------------------------
def analisis_3_news_by_hour(spark: SparkSession, df_rss: DataFrame) -> DataFrame:
    df_rss.createOrReplaceTempView("crypto_rss")
    return spark.sql("""
        SELECT
            HOUR(TO_TIMESTAMP(timestamp)) AS hour_utc,
            COUNT(*) AS n_articles,
            COUNT(DISTINCT source) AS n_sources
        FROM crypto_rss
        GROUP BY HOUR(TO_TIMESTAMP(timestamp))
        ORDER BY hour_utc
    """)


# ----------------------------------------------------------------------
# BONUS +5 — MLlib: K-Means clustering jam + Linear Regression korelasi
# ----------------------------------------------------------------------
def bonus_mllib(vol_by_hour: DataFrame, news_by_hour: DataFrame) -> tuple[DataFrame, dict]:
    """
    (a) K-Means clustering: gabungkan volatilitas + volume berita per jam,
        cluster menjadi 3 "rezim" jam (tenang / sedang / bergejolak).
    (b) Linear Regression: apakah jumlah artikel per jam memprediksi volatilitas?
    """
    combined = (vol_by_hour.select("hour_utc", "avg_abs_change_pct")
                .join(news_by_hour.select("hour_utc", "n_articles"),
                      on="hour_utc", how="full")
                .na.fill(0.0, subset=["avg_abs_change_pct"])
                .na.fill(0, subset=["n_articles"])
                .orderBy("hour_utc"))

    n_rows = combined.count()
    if n_rows < 3:
        # Data terlalu sedikit — kembalikan DF kosong tapi skema jelas
        print(f"[MLlib] Data hanya {n_rows} baris — K-Means dilewati.")
        return combined.withColumn("hour_regime", F.lit(None).cast("int")), {
            "status": "skipped",
            "reason": f"only {n_rows} hourly rows",
        }

    assembler = VectorAssembler(
        inputCols=["avg_abs_change_pct", "n_articles"],
        outputCol="features")
    feat = assembler.transform(combined)

    k = min(3, n_rows)
    kmeans = KMeans(k=k, seed=42, featuresCol="features", predictionCol="hour_regime")
    km_model = kmeans.fit(feat)
    clustered = (km_model.transform(feat)
                 .select("hour_utc", "avg_abs_change_pct", "n_articles", "hour_regime")
                 .orderBy("hour_utc"))

    # Linear Regression
    lr_data = combined.select(
        F.col("avg_abs_change_pct").cast("double").alias("label"),
        F.col("n_articles").cast("double").alias("n_articles"),
    )
    lr_assembler = VectorAssembler(inputCols=["n_articles"], outputCol="features")
    lr_ready = lr_assembler.transform(lr_data)
    lr = LinearRegression(featuresCol="features", labelCol="label")
    lr_model = lr.fit(lr_ready)

    mllib_summary = {
        "kmeans": {
            "k": k,
            "cluster_centers": [list(map(float, c)) for c in km_model.clusterCenters()],
            "feature_columns": ["avg_abs_change_pct", "n_articles"],
        },
        "linear_regression": {
            "slope_n_articles": float(lr_model.coefficients[0]),
            "intercept": float(lr_model.intercept),
            "r2": float(lr_model.summary.r2),
            "rmse": float(lr_model.summary.rootMeanSquaredError),
            "interpretation": (
                "Slope positif = semakin banyak berita, semakin tinggi volatilitas. "
                "R² mendekati 0 berarti korelasi lemah, mendekati 1 berarti kuat."
            ),
        }
    }
    return clustered, mllib_summary


# ----------------------------------------------------------------------
# Persist hasil
# ----------------------------------------------------------------------
def df_to_records(df: DataFrame) -> list[dict]:
    return [row.asDict(recursive=True) for row in df.collect()]


def save_dashboard_summary(payload: dict) -> None:
    DASHBOARD_OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = DASHBOARD_OUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(DASHBOARD_OUT)
    print(f"[LOCAL] saved -> {DASHBOARD_OUT}")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark {spark.version} siap.")

    staging_dir = Path(tempfile.mkdtemp(prefix="spark_hdfs_staging_"))
    print(f"Staging dir: {staging_dir}")

    try:
        try:
            df_api, df_rss = load_data(spark, staging_dir)
        except Exception as e:
            print(f"Gagal load HDFS: {e}", file=sys.stderr)
            raise

        n_api, n_rss = df_api.count(), df_rss.count()
        print(f"Loaded: {n_api} event API, {n_rss} event RSS")
        if n_api == 0 and n_rss == 0:
            print("Belum ada data — jalankan producer + consumer dulu.", file=sys.stderr)
            spark.stop()
            return 1

        print("\n=== Analisis 1: Statistik harga per koin ===")
        stats = analisis_1_stats_per_coin(df_api)
        stats.show(truncate=False)

        print("\n=== Analisis 2: Volatilitas per jam ===")
        vol_by_hour = analisis_2_volatility_by_hour(spark, df_api)
        vol_by_hour.show(24, truncate=False)

        print("\n=== Analisis 3: Volume berita per jam ===")
        news_by_hour = analisis_3_news_by_hour(spark, df_rss)
        news_by_hour.show(24, truncate=False)

        print("\n=== BONUS MLlib: Clustering + Linear Regression ===")
        clustered, mllib_summary = bonus_mllib(vol_by_hour, news_by_hour)
        clustered.show(24, truncate=False)
        print(f"MLlib summary: {json.dumps(mllib_summary, indent=2)}")

        # Simpan hasil ke HDFS (via WebHDFS mirror dari staging)
        save_to_hdfs_via_webhdfs(stats, "stats_per_coin", staging_dir)
        save_to_hdfs_via_webhdfs(vol_by_hour, "volatility_by_hour", staging_dir)
        save_to_hdfs_via_webhdfs(news_by_hour, "news_by_hour", staging_dir)
        save_to_hdfs_via_webhdfs(clustered, "hour_regime_clusters", staging_dir)

        # Simpan ringkasan untuk dashboard
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "n_api_events": n_api,
            "n_rss_events": n_rss,
            "stats_per_coin": df_to_records(stats),
            "volatility_by_hour": df_to_records(vol_by_hour),
            "news_by_hour": df_to_records(news_by_hour),
            "hour_regime_clusters": df_to_records(clustered),
            "mllib": mllib_summary,
            "interpretation": {
                "q1_volatile_hours": (
                    "Lihat 3 jam teratas di `volatility_by_hour` (avg_abs_change_pct). "
                    "Biasanya jam pembukaan bursa AS (UTC 13:00-14:00) menunjukkan lonjakan."
                ),
                "q2_news_vs_price": (
                    "Bandingkan slope Linear Regression (`mllib.linear_regression.slope_n_articles`) "
                    "dan distribusi `hour_regime_clusters`. "
                    "Slope positif + cluster 'bergejolak' beririsan dengan jam berita padat = "
                    "berita searah dengan pergerakan harga."
                ),
            }
        }
        save_dashboard_summary(payload)
    finally:
        spark.stop()
        # Bersihkan staging dir
        try:
            shutil.rmtree(staging_dir, ignore_errors=True)
        except Exception:
            pass

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
