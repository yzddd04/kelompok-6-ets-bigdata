// CryptoWatch Dashboard — frontend logic
// Fetch /api/data tiap 30 detik, update semua panel + Chart.js.
// [Erlinda Annisa Zahra — 5027241108]: file ini dan seluruh rendering panel + chart.

const REFRESH_MS = 30_000;
let hourlyChart = null;

const fmt = {
  usd: n => (n == null ? "–" : "$" + Number(n).toLocaleString("en-US", { maximumFractionDigits: 2 })),
  idr: n => (n == null ? "–" : "Rp" + Number(n).toLocaleString("id-ID", { maximumFractionDigits: 0 })),
  pct: n => (n == null ? "–" : Number(n).toFixed(2) + "%"),
  time: iso => {
    if (!iso) return "–";
    try { return new Date(iso).toLocaleTimeString("id-ID", { hour: "2-digit", minute: "2-digit", second: "2-digit" }); }
    catch { return iso; }
  },
};

function setStatus(ok, text) {
  const dot = document.getElementById("status-dot");
  const txt = document.getElementById("status-text");
  dot.classList.remove("ok", "err");
  dot.classList.add(ok ? "ok" : "err");
  txt.textContent = text;
}

function setHealth(apiCount, rssCount, hasSpark) {
  const text = document.getElementById("health-text");
  const foot = document.getElementById("health-foot");
  if (apiCount > 0 && rssCount > 0 && hasSpark) {
    text.textContent = "All Systems Go";
    text.style.color = "var(--up)";
    foot.textContent = "kafka · hdfs · spark · dashboard";
  } else if (apiCount > 0 || rssCount > 0) {
    text.textContent = "Streaming";
    text.style.color = "var(--warn)";
    foot.textContent = hasSpark ? "spark batch ok · stream live" : "tunggu spark batch";
  } else {
    text.textContent = "Idle";
    text.style.color = "var(--muted)";
    foot.textContent = "menunggu producer mengirim event";
  }
}

function renderPrices(latest) {
  const grid = document.getElementById("price-grid");
  if (!latest || latest.length === 0) {
    grid.innerHTML = `<div class="empty-state">⏳ Menunggu data live dari consumer…</div>`;
    return;
  }
  grid.innerHTML = latest
    .sort((a, b) => a.symbol.localeCompare(b.symbol))
    .map(row => {
      const sym = (row.symbol || "?").toUpperCase();
      const cls = (row.change_24h ?? 0) >= 0 ? "up" : "down";
      const arrow = (row.change_24h ?? 0) >= 0 ? "▲" : "▼";
      const badgeCls = sym.toLowerCase().slice(0, 3);
      return `
        <div class="price-card">
          <div class="symbol-badge ${badgeCls}">${sym.slice(0, 3)}</div>
          <div class="price-info">
            <div class="symbol">${sym}</div>
            <div class="timestamp">${fmt.time(row.timestamp)}</div>
            <span class="change-badge ${cls}">${arrow} ${fmt.pct(row.change_24h)}</span>
          </div>
          <div class="price-values">
            <div class="usd">${fmt.usd(row.price_usd)}</div>
            <div class="idr">${fmt.idr(row.price_idr)}</div>
          </div>
        </div>
      `;
    }).join("");
}

function renderStats(stats) {
  const tbody = document.getElementById("stats-body");
  if (!stats || stats.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" class="empty">Belum ada hasil Spark — jalankan spark/analysis.py</td></tr>`;
    return;
  }
  tbody.innerHTML = stats.map(r => {
    const cls = (r.avg_change_pct ?? 0) >= 0 ? "up" : "down";
    const arrow = (r.avg_change_pct ?? 0) >= 0 ? "▲" : "▼";
    return `<tr>
      <td><b>${r.symbol}</b></td>
      <td class="num">${r.n ?? "–"}</td>
      <td class="num">${fmt.usd(r.avg_usd)}</td>
      <td class="num">${fmt.usd(r.max_usd)}</td>
      <td class="num">${fmt.usd(r.min_usd)}</td>
      <td class="num">${r.stddev_usd ?? "–"}</td>
      <td class="num ${cls}">${arrow} ${fmt.pct(r.avg_change_pct)}</td>
    </tr>`;
  }).join("");
}

function renderHourlyChart(vol, news) {
  const ctx = document.getElementById("hourlyChart").getContext("2d");

  const hours = Array.from({ length: 24 }, (_, h) => h);
  const volMap = Object.fromEntries((vol || []).map(r => [r.hour_utc, r.avg_abs_change_pct]));
  const newsMap = Object.fromEntries((news || []).map(r => [r.hour_utc, r.n_articles]));

  const volData = hours.map(h => volMap[h] ?? null);
  const newsData = hours.map(h => newsMap[h] ?? 0);

  // Gradient untuk line
  const lineGrad = ctx.createLinearGradient(0, 0, 0, 280);
  lineGrad.addColorStop(0, "rgba(255,95,109,0.35)");
  lineGrad.addColorStop(1, "rgba(255,95,109,0.0)");

  const data = {
    labels: hours.map(h => h.toString().padStart(2, "0") + ":00"),
    datasets: [
      {
        type: "line",
        label: "Volatilitas (|change_24h| %)",
        data: volData,
        yAxisID: "y",
        borderColor: "#ff5f6d",
        backgroundColor: lineGrad,
        borderWidth: 2.5,
        tension: 0.35,
        pointRadius: 3,
        pointBackgroundColor: "#ff5f6d",
        pointBorderColor: "#0a0e17",
        pointBorderWidth: 2,
        pointHoverRadius: 6,
        fill: true,
      },
      {
        type: "bar",
        label: "Jumlah Artikel RSS",
        data: newsData,
        yAxisID: "y1",
        backgroundColor: "rgba(122, 167, 255, 0.55)",
        borderColor: "#7aa7ff",
        borderWidth: 1,
        borderRadius: 4,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
      legend: {
        labels: {
          color: "#e6edf6",
          font: { family: "Inter", size: 12, weight: "600" },
          padding: 14,
          usePointStyle: true,
          pointStyle: "circle",
        }
      },
      tooltip: {
        mode: "index",
        backgroundColor: "rgba(15, 20, 32, 0.95)",
        borderColor: "rgba(120, 140, 180, 0.22)",
        borderWidth: 1,
        titleFont: { family: "Inter", weight: "700" },
        bodyFont: { family: "JetBrains Mono", size: 12 },
        padding: 12,
        cornerRadius: 8,
      },
    },
    scales: {
      x: {
        ticks: { color: "#8590a8", font: { family: "JetBrains Mono", size: 11 } },
        grid: { color: "rgba(120, 140, 180, 0.06)" },
      },
      y: {
        type: "linear", position: "left",
        title: { display: true, text: "Volatilitas (%)", color: "#ff5f6d", font: { family: "Inter", weight: "600" } },
        ticks: { color: "#ff5f6d", font: { family: "JetBrains Mono", size: 11 } },
        grid: { color: "rgba(120, 140, 180, 0.06)" },
      },
      y1: {
        type: "linear", position: "right",
        title: { display: true, text: "Jumlah Artikel", color: "#7aa7ff", font: { family: "Inter", weight: "600" } },
        ticks: { color: "#7aa7ff", precision: 0, font: { family: "JetBrains Mono", size: 11 } },
        grid: { drawOnChartArea: false },
      },
    },
  };

  if (hourlyChart) {
    hourlyChart.data = data;
    hourlyChart.options = options;
    hourlyChart.update();
  } else {
    hourlyChart = new Chart(ctx, { data, options });
  }
}

function renderMLlib(mllib) {
  const stats = document.getElementById("mllib-stats");
  const clusters = document.getElementById("mllib-clusters");
  if (!mllib || !mllib.linear_regression) {
    stats.innerHTML = `<span class="empty">Menunggu hasil MLlib…</span>`;
    clusters.innerHTML = "";
    return;
  }
  const lr = mllib.linear_regression || {};
  const km = mllib.kmeans || {};
  const slope = lr.slope_n_articles ?? 0;
  const slopeCls = slope >= 0 ? "up" : "down";

  stats.innerHTML = `
    <div class="cluster-title">Linear Regression</div>
    <div class="mllib-metric">
      <span class="label">Slope (artikel → volatilitas)</span>
      <span class="value ${slopeCls}">${slope.toFixed(4)}</span>
    </div>
    <div class="mllib-metric">
      <span class="label">R² (kekuatan korelasi)</span>
      <span class="value">${(lr.r2 ?? 0).toFixed(4)}</span>
    </div>
    <div class="mllib-metric">
      <span class="label">RMSE</span>
      <span class="value">${(lr.rmse ?? 0).toFixed(4)}</span>
    </div>
    <div class="mllib-metric">
      <span class="label">Intercept</span>
      <span class="value">${(lr.intercept ?? 0).toFixed(4)}</span>
    </div>
  `;

  const centers = (km.cluster_centers || []);
  if (centers.length === 0) {
    clusters.innerHTML = `<div class="cluster-title">K-Means</div><span class="empty">–</span>`;
    return;
  }
  clusters.innerHTML = `
    <div class="cluster-title">K-Means (k=${km.k ?? "?"}) — Centroid</div>
    ${centers.map((c, i) => `
      <div class="cluster-pill">
        <span class="pill-label">Cluster ${i}</span>
        <span class="pill-vals">vol ${c[0].toFixed(2)}% · ${c[1].toFixed(1)} berita</span>
      </div>
    `).join("")}
  `;
}

function renderNews(items) {
  const ul = document.getElementById("news-list");
  const tag = document.getElementById("news-count-tag");
  if (!items || items.length === 0) {
    ul.innerHTML = `<li class="empty">Menunggu artikel RSS dari consumer…</li>`;
    if (tag) tag.textContent = "0 artikel";
    return;
  }
  if (tag) tag.textContent = `${items.length} artikel`;
  ul.innerHTML = items.map(a => {
    const src = (a.source || "rss").toLowerCase().replace(/[^a-z]/g, "");
    return `
      <li>
        <a href="${a.link}" target="_blank" rel="noopener noreferrer">${a.title || "(tanpa judul)"}</a>
        <div class="news-meta">
          <span class="source-badge ${src}">${a.source || "rss"}</span>
          ${a.published || fmt.time(a.timestamp)}
        </div>
      </li>
    `;
  }).join("");
}

async function refresh() {
  try {
    const res = await fetch("/api/data", { cache: "no-store" });
    if (!res.ok) throw new Error("HTTP " + res.status);
    const d = await res.json();

    const apiCount = d.live_api_count ?? 0;
    const rssCount = d.live_rss_count ?? 0;
    const hasSpark = !!(d.spark && d.spark.stats_per_coin && d.spark.stats_per_coin.length);

    document.getElementById("api-count").textContent = apiCount.toLocaleString();
    document.getElementById("rss-count").textContent = rssCount.toLocaleString();
    document.getElementById("last-update").textContent = fmt.time(new Date().toISOString());
    setHealth(apiCount, rssCount, hasSpark);

    renderPrices(d.latest_prices);
    renderStats(d.spark?.stats_per_coin);
    renderHourlyChart(d.spark?.volatility_by_hour, d.spark?.news_by_hour);
    renderMLlib(d.spark?.mllib);
    renderNews(d.top_news);

    setStatus(true, "live");
  } catch (e) {
    console.error(e);
    setStatus(false, "offline");
  }
}

refresh();
setInterval(refresh, REFRESH_MS);
