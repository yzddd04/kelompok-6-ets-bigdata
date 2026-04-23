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

function renderPrices(latest) {
  const tbody = document.getElementById("prices-body");
  if (!latest || latest.length === 0) {
    tbody.innerHTML = `<tr><td colspan="5" class="empty">Menunggu data live dari consumer…</td></tr>`;
    return;
  }
  tbody.innerHTML = latest
    .sort((a, b) => a.symbol.localeCompare(b.symbol))
    .map(row => {
      const cls = row.change_24h >= 0 ? "up" : "down";
      const arrow = row.change_24h >= 0 ? "▲" : "▼";
      return `<tr>
        <td><b>${row.symbol}</b></td>
        <td class="num">${fmt.usd(row.price_usd)}</td>
        <td class="num">${fmt.idr(row.price_idr)}</td>
        <td class="num ${cls}">${arrow} ${fmt.pct(row.change_24h)}</td>
        <td>${fmt.time(row.timestamp)}</td>
      </tr>`;
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
    return `<tr>
      <td><b>${r.symbol}</b></td>
      <td class="num">${r.n ?? "–"}</td>
      <td class="num">${fmt.usd(r.avg_usd)}</td>
      <td class="num">${fmt.usd(r.max_usd)}</td>
      <td class="num">${fmt.usd(r.min_usd)}</td>
      <td class="num">${r.stddev_usd ?? "–"}</td>
      <td class="num ${cls}">${fmt.pct(r.avg_change_pct)}</td>
    </tr>`;
  }).join("");
}

function renderHourlyChart(vol, news) {
  const ctx = document.getElementById("hourlyChart").getContext("2d");

  // Gabungkan berdasarkan hour_utc (0..23)
  const hours = Array.from({ length: 24 }, (_, h) => h);
  const volMap = Object.fromEntries((vol || []).map(r => [r.hour_utc, r.avg_abs_change_pct]));
  const newsMap = Object.fromEntries((news || []).map(r => [r.hour_utc, r.n_articles]));

  const volData = hours.map(h => volMap[h] ?? null);
  const newsData = hours.map(h => newsMap[h] ?? 0);

  const data = {
    labels: hours.map(h => h.toString().padStart(2, "0") + ":00"),
    datasets: [
      {
        type: "line",
        label: "Volatilitas (|change_24h| %)",
        data: volData,
        yAxisID: "y",
        borderColor: "#f85149",
        backgroundColor: "rgba(248,81,73,0.15)",
        borderWidth: 2,
        tension: 0.25,
        pointRadius: 3,
      },
      {
        type: "bar",
        label: "Jumlah Artikel RSS",
        data: newsData,
        yAxisID: "y1",
        backgroundColor: "rgba(88,166,255,0.55)",
        borderColor: "#58a6ff",
        borderWidth: 1,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
      legend: { labels: { color: "#e6edf3" } },
      tooltip: { mode: "index" },
    },
    scales: {
      x: {
        ticks: { color: "#8b949e" },
        grid: { color: "rgba(255,255,255,0.05)" },
      },
      y: {
        type: "linear", position: "left",
        title: { display: true, text: "Volatilitas (%)", color: "#8b949e" },
        ticks: { color: "#f85149" },
        grid: { color: "rgba(255,255,255,0.05)" },
      },
      y1: {
        type: "linear", position: "right",
        title: { display: true, text: "Jumlah Artikel", color: "#8b949e" },
        ticks: { color: "#58a6ff", precision: 0 },
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
  const box = document.getElementById("mllib-box");
  if (!mllib || !mllib.linear_regression) {
    box.innerHTML = `<span class="empty">Menunggu hasil MLlib…</span>`;
    return;
  }
  const lr = mllib.linear_regression || {};
  const km = mllib.kmeans || {};
  const centers = (km.cluster_centers || [])
    .map((c, i) => `<div class="kv"><span>Cluster ${i}</span><span>vol=${c[0].toFixed(2)}% · berita=${c[1].toFixed(1)}</span></div>`)
    .join("");
  box.innerHTML = `
    <div class="kv"><span>LR slope (artikel → volatilitas)</span><span>${(lr.slope_n_articles ?? 0).toFixed(4)}</span></div>
    <div class="kv"><span>R²</span><span>${(lr.r2 ?? 0).toFixed(4)}</span></div>
    <div class="kv"><span>RMSE</span><span>${(lr.rmse ?? 0).toFixed(4)}</span></div>
    <h3 style="margin-top:10px">K-Means (k=${km.k ?? "?"}) — centroid</h3>
    ${centers || '<span class="empty">–</span>'}
  `;
}

function renderNews(items) {
  const ul = document.getElementById("news-list");
  if (!items || items.length === 0) {
    ul.innerHTML = `<li class="empty">Menunggu artikel RSS dari consumer…</li>`;
    return;
  }
  ul.innerHTML = items.map(a => `
    <li>
      <a href="${a.link}" target="_blank" rel="noopener noreferrer">${a.title || "(tanpa judul)"}</a>
      <div class="news-meta">
        <span class="source-badge">${a.source || "rss"}</span>
        ${a.published || fmt.time(a.timestamp)}
      </div>
    </li>
  `).join("");
}

async function refresh() {
  try {
    const res = await fetch("/api/data", { cache: "no-store" });
    if (!res.ok) throw new Error("HTTP " + res.status);
    const d = await res.json();

    document.getElementById("api-count").textContent = d.live_api_count ?? 0;
    document.getElementById("rss-count").textContent = d.live_rss_count ?? 0;
    document.getElementById("last-update").textContent = fmt.time(new Date().toISOString());

    renderPrices(d.latest_prices);
    renderStats(d.spark?.stats_per_coin);
    renderHourlyChart(d.spark?.volatility_by_hour, d.spark?.news_by_hour);
    renderMLlib(d.spark?.mllib);
    renderNews(d.top_news);

    setStatus(true, "live");
  } catch (e) {
    console.error(e);
    setStatus(false, "offline: " + e.message);
  }
}

refresh();
setInterval(refresh, REFRESH_MS);
