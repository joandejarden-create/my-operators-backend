(function () {
  "use strict";

  const els = {
    tier: document.getElementById("filterTier"),
    strategy: document.getElementById("filterStrategy"),
    status: document.getElementById("filterStatus"),
    region: document.getElementById("filterRegion"),
    sort: document.getElementById("filterSort"),
    btnRefresh: document.getElementById("btnRefresh"),
    btnGenerate: document.getElementById("btnGenerate"),
    tbody: document.getElementById("buildoutTableBody"),
    error: document.getElementById("error"),
    summary: document.getElementById("summary"),
  };

  const COLSPAN = 15;

  function statusClass(status) {
    return String(status || "")
      .toLowerCase()
      .replace(/\s+/g, "-");
  }

  function pctBar(current, target) {
    if (!target) return "—";
    const pct = Math.min(100, Math.round((current / target) * 100));
    return current + " / " + target + " (" + pct + "%)";
  }

  function buildQuery() {
    const q = new URLSearchParams();
    if (els.tier.value) q.set("priorityTier", els.tier.value);
    if (els.strategy.value) q.set("buildStrategy", els.strategy.value);
    if (els.status.value) q.set("buildStatus", els.status.value);
    if (els.region.value) q.set("region", els.region.value);
    const s = q.toString();
    return s ? "?" + s : "";
  }

  function tierOrder(tier) {
    const map = { "Tier 1": 1, "Tier 2": 2, "Tier 3": 3, Future: 4 };
    return map[tier] || 9;
  }

  function sortRows(rows) {
    const mode = els.sort.value || "sequence";
    const copy = rows.slice();
    if (mode === "country") {
      copy.sort(function (a, b) {
        return String(a.country).localeCompare(String(b.country));
      });
      return copy;
    }
    copy.sort(function (a, b) {
      const t = tierOrder(a.priorityTier) - tierOrder(b.priorityTier);
      if (t !== 0) return t;
      const s = (a.recommendedBuildSequence ?? 999) - (b.recommendedBuildSequence ?? 999);
      if (s !== 0) return s;
      return String(a.buildStatus || "").localeCompare(String(b.buildStatus || ""));
    });
    return copy;
  }

  async function load() {
    els.error.textContent = "";
    els.tbody.innerHTML = "<tr><td colspan='" + COLSPAN + "'>Loading…</td></tr>";
    try {
      const res = await fetch("/api/radar-buildout/countries" + buildQuery(), {
        headers: { "ngrok-skip-browser-warning": "true" },
      });
      const data = await res.json();
      if (!data.ok) throw new Error(data.message || data.error || "API error");
      render(sortRows(data.countries || []));
      els.summary.textContent = data.totalCount + " countries";
    } catch (err) {
      els.error.textContent = err.message || String(err);
      els.tbody.innerHTML = "";
    }
  }

  function truncate(s, max) {
    const t = String(s == null ? "" : s);
    if (t.length <= max) return t;
    return t.slice(0, max - 1) + "…";
  }

  function render(rows) {
    if (!rows.length) {
      els.tbody.innerHTML = "<tr><td colspan='" + COLSPAN + "'>No countries match filters.</td></tr>";
      return;
    }
    els.tbody.innerHTML = rows
      .map(function (r) {
        const notes = r.buildApproachNotes || r.notes || "";
        const nextMarket = r.nextBuildMarket || "—";
        const titleAttr = notes ? ' title="' + esc(notes) + '"' : "";
        return (
          "<tr>" +
          "<td>" + esc(r.recommendedBuildSequence != null ? r.recommendedBuildSequence : "—") + "</td>" +
          "<td><strong>" + esc(r.country) + "</strong></td>" +
          "<td class='muted'" + titleAttr + ">" + esc(truncate(nextMarket, 28)) + "</td>" +
          "<td>" + esc(r.region) + "</td>" +
          "<td>" + esc(r.buildStrategy) + "</td>" +
          "<td>" + esc(r.priorityTier) + "</td>" +
          "<td><span class='status-pill " + statusClass(r.buildStatus) + "'>" + esc(r.buildStatus) + "</span></td>" +
          "<td>" + pctBar(r.current.demandAnchors, r.targets.demandAnchors) + "</td>" +
          "<td>" + pctBar(r.current.travelInfrastructure, r.targets.travelInfrastructure) + "</td>" +
          "<td>" + pctBar(r.current.totalRadarPoints, r.targets.totalRadarPoints) + "</td>" +
          "<td>" + esc(r.coverage.sourceCoveragePct) + "%</td>" +
          "<td>" + esc(r.coverage.coordinateCoveragePct) + "%</td>" +
          "<td>" + esc(r.lastBuildDate || "—") + "</td>" +
          "<td class='muted'" + titleAttr + ">" + esc(truncate(r.nextRecommendedAction || "—", 40)) + "</td>" +
          "<td><div class='actions'>" +
          "<a href='/deal-capture-radar-with-ranked-list.html'>View Radar</a> · " +
          "<a href='/demand-anchors-import.html'>Demand Import</a> · " +
          "<a href='/travel-infrastructure-import.html'>Travel Import</a>" +
          "</div></td>" +
          "</tr>"
        );
      })
      .join("");
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  els.btnRefresh.addEventListener("click", load);
  els.btnGenerate.addEventListener("click", function () {
    alert(
      "Run from terminal:\nnpm run generate:cala-radar-build-plans\n\nAdd --apply to write Airtable build plans after dry-run review."
    );
  });
  [els.tier, els.strategy, els.status, els.region, els.sort].forEach(function (el) {
    el.addEventListener("change", load);
  });

  load();
})();
