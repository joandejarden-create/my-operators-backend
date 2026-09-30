/**
 * GDI PDF Report HTML composer V1 — Dealality report-system family.
 * Customer-safe markup only (no internal IDs, no Markdown).
 */

function esc(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function money(n) {
  if (n == null || !Number.isFinite(n)) return "—";
  return "$" + Math.round(n).toLocaleString("en-US");
}

function priClass(p) {
  if (p === "HIGH") return "gdi-pdf-pri gdi-pdf-pri--high";
  if (p === "MEDIUM") return "gdi-pdf-pri gdi-pdf-pri--med";
  return "gdi-pdf-pri gdi-pdf-pri--watch";
}

function whoLine(who) {
  if (!who) return "Conference / Meetings Team";
  if (who.kind === "NAMED" && who.name) {
    return `${who.name}${who.title ? `, ${who.title}` : ""}`;
  }
  return who.title || "Conference / Meetings Team";
}

const LOGO =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png";

/**
 * @param {object} data — getGdiPdfReportData result (available:true)
 * @returns {string} full HTML document body inner (host content)
 */
export function buildGdiPdfReportBodyHtml(data) {
  const hotel = data.hotel || {};
  const meta = data.reportMetadata || {};
  const ex = data.executiveSummary || {};
  const rev = ex.revenueScenario;

  const cover = `
<section class="bas-cover-page brand-alignment-snapshot gdi-pdf-cover" data-gdi-pdf-section="cover">
  <div class="bas-cover-geometric" aria-hidden="true"></div>
  <div class="bas-cover-inner">
    <div class="bas-cover-brand">
      <img class="bas-cover-logo-img" src="${LOGO}" alt="Dealality" />
    </div>
    <p class="bas-cover-kicker">GROUP &amp; DEMAND INTELLIGENCE</p>
    <h1 class="bas-cover-title">${esc(hotel.name)}</h1>
    <p class="bas-cover-sub">${esc(hotel.market || hotel.city || "")}</p>
    <p class="bas-cover-meta">Report date ${esc(meta.reportDate)}</p>
  </div>
</section>`;

  const kpi = `
<section class="drs-section gdi-pdf-section" data-gdi-pdf-section="executive">
  <h2 class="drs-h2">Commercial Demand Snapshot</h2>
  <div class="drs-kpi-band">
    <div class="drs-kpi"><div class="drs-kpi__label">Customer-ready</div><div class="drs-kpi__value">${ex.customerReadyCount ?? 0}</div></div>
    <div class="drs-kpi"><div class="drs-kpi__label">Action set</div><div class="drs-kpi__value">${ex.actionSetCount ?? 0}</div></div>
    <div class="drs-kpi"><div class="drs-kpi__label">High priority</div><div class="drs-kpi__value">${ex.highPriority ?? 0}</div></div>
    <div class="drs-kpi"><div class="drs-kpi__label">Medium priority</div><div class="drs-kpi__value">${ex.mediumPriority ?? 0}</div></div>
    <div class="drs-kpi"><div class="drs-kpi__label">Future watch</div><div class="drs-kpi__value">${ex.futureWatchCount ?? 0}</div></div>
    <div class="drs-kpi"><div class="drs-kpi__label">Named contacts</div><div class="drs-kpi__value">${ex.namedContactCoveragePct ?? 0}%</div></div>
  </div>
  ${
    rev
      ? `<div class="gdi-pdf-rev-block">
    <h3 class="drs-h3">Estimated room-revenue scenarios</h3>
    <div class="drs-kpi-band drs-kpi-band--3">
      <div class="drs-kpi"><div class="drs-kpi__label">Low</div><div class="drs-kpi__value">${money(rev.low)}</div></div>
      <div class="drs-kpi"><div class="drs-kpi__label">Base</div><div class="drs-kpi__value">${money(rev.base)}</div></div>
      <div class="drs-kpi"><div class="drs-kpi__label">High</div><div class="drs-kpi__value">${money(rev.high)}</div></div>
    </div>
    <p class="gdi-pdf-disclaimer">${esc(rev.disclaimer)}</p>
    <p class="gdi-pdf-note">${esc(rev.scopeNote)}</p>
  </div>`
      : ""
  }
  <p class="gdi-pdf-lede">Public contact path coverage on the action set: <strong>${ex.publicContactPathCoveragePct ?? 0}%</strong>.</p>
</section>`;

  const top5 = (data.immediatePursuits || [])
    .map((c, i) => {
      return `
<article class="gdi-pdf-card gdi-pdf-avoid-break">
  <header class="gdi-pdf-card__head">
    <span class="${priClass(c.priority)}">${esc(c.priority)}</span>
    <span class="gdi-pdf-card__num">${i + 1}</span>
  </header>
  <h3 class="gdi-pdf-card__title">${esc(c.opportunity)}</h3>
  <p class="gdi-pdf-card__org">${esc(c.organization || "")} · ${esc(c.segment)} · ${esc(c.datesCycle || "Dates TBD")}</p>
  <dl class="gdi-pdf-dl">
    <div><dt>Why it matters</dt><dd>${esc(c.whyNow)}</dd></div>
    <div><dt>Why this hotel</dt><dd>${esc(c.whyHotel)}</dd></div>
    <div><dt>Who to pursue</dt><dd>${esc(whoLine(c.who))}${c.contactPath?.display && c.contactPath.display !== "Not yet supported" ? ` · ${esc(c.contactPath.display)}` : ""}</dd></div>
    <div><dt>Next action</dt><dd>${esc(c.recommendedAction)}</dd></div>
    ${c.revenue ? `<div><dt>Room revenue scenario</dt><dd>L/B/H ${money(c.revenue.low)} / ${money(c.revenue.base)} / ${money(c.revenue.high)}</dd></div>` : ""}
  </dl>
</article>`;
    })
    .join("\n");

  const immediate = `
<section class="drs-section gdi-pdf-section" data-gdi-pdf-section="immediate">
  <h2 class="drs-h2">Top Immediate Pursuits</h2>
  <p class="gdi-pdf-lede">Highest-actionability opportunities for the sales team to work first.</p>
  ${top5 || "<p>No immediate pursuits available.</p>"}
</section>`;

  const oppRows = (data.topOpportunities || [])
    .map((c) => {
      return `
<article class="gdi-pdf-card gdi-pdf-card--compact gdi-pdf-avoid-break">
  <header class="gdi-pdf-card__head">
    <span class="${priClass(c.priority)}">${esc(c.priority)}</span>
    <span class="gdi-pdf-muted">${esc(c.segment)}</span>
  </header>
  <h3 class="gdi-pdf-card__title">${esc(c.opportunity)}</h3>
  <p class="gdi-pdf-card__org">${esc(c.organization || "")} · ${esc(c.datesCycle || "Dates TBD")} · Fit ${c.hotelFit ?? "—"}</p>
  <p><strong>Placement:</strong> ${esc(c.destinationStatus)}</p>
  <p><strong>Peak rooms / nights:</strong> ${esc(c.peakRoomsLabel)} / ${esc(c.nightsLabel)}</p>
  <p><strong>Why this hotel:</strong> ${esc(c.whyHotel)}</p>
  <p><strong>Why now:</strong> ${esc(c.whyNow)}</p>
  <p><strong>Who:</strong> ${esc(whoLine(c.who))} · ${esc(c.contactPath?.display || "Not yet supported")}</p>
  <p><strong>Action:</strong> ${esc(c.recommendedAction)}</p>
  ${c.revenue ? `<p><strong>Revenue scenario:</strong> L/B/H ${money(c.revenue.low)} / ${money(c.revenue.base)} / ${money(c.revenue.high)}</p>` : ""}
  <p class="gdi-pdf-muted">Confidence: ${esc(c.confidence)}</p>
</article>`;
    })
    .join("\n");

  const opportunities = `
<section class="drs-section gdi-pdf-section" data-gdi-pdf-section="opportunities">
  <h2 class="drs-h2">Top Opportunities</h2>
  <p class="gdi-pdf-lede">Prioritized action set (${(data.topOpportunities || []).length} opportunities).</p>
  ${oppRows}
</section>`;

  const plan = data.actionPlan || { actNow: [], develop: [] };
  const planBlock = (title, rows) => `
  <h3 class="drs-h3">${esc(title)}</h3>
  <table class="drs-table gdi-pdf-table">
    <thead><tr><th>Opportunity</th><th>Owner</th><th>Action</th><th>Outcome</th><th>Timing</th></tr></thead>
    <tbody>
      ${(rows || [])
        .map(
          (r) =>
            `<tr><td>${esc(r.opportunity)}</td><td>${esc(r.ownerRole)}</td><td>${esc(r.action)}</td><td>${esc(r.desiredOutcome)}</td><td>${esc(r.timing)}</td></tr>`
        )
        .join("")}
    </tbody>
  </table>`;

  const watchRows = (data.futureWatch || [])
    .map(
      (c) => `
<article class="gdi-pdf-card gdi-pdf-card--watch gdi-pdf-avoid-break">
  <h3 class="gdi-pdf-card__title">${esc(c.opportunity)}</h3>
  <p><strong>Why it matters:</strong> ${esc(c.whyNow)}</p>
  <p><strong>Current status:</strong> ${esc(c.currentBlocker || "Monitoring")}</p>
  <p><strong>Trigger:</strong> ${esc(c.trigger || "Next public update")}</p>
</article>`
    )
    .join("\n");

  const actionPlan = `
<section class="drs-section gdi-pdf-section" data-gdi-pdf-section="action-plan">
  <h2 class="drs-h2">30-Day Action Plan</h2>
  ${planBlock("Act now", plan.actNow)}
  ${planBlock("Develop", plan.develop)}
  <h3 class="drs-h3">Watch</h3>
  ${watchRows || "<p>No future-watch items in this snapshot.</p>"}
</section>`;

  const pipe = data.pipeline || {};
  const segs = Object.entries(data.opportunitySummary?.segmentMix || {});
  const pipeline = `
<section class="drs-section gdi-pdf-section" data-gdi-pdf-section="pipeline">
  <h2 class="drs-h2">Opportunity Pipeline</h2>
  <div class="drs-kpi-band drs-kpi-band--3">
    <div class="drs-kpi"><div class="drs-kpi__label">Ready</div><div class="drs-kpi__value">${pipe.ready ?? 0}</div></div>
    <div class="drs-kpi"><div class="drs-kpi__label">Action set</div><div class="drs-kpi__value">${pipe.actionSet ?? 0}</div></div>
    <div class="drs-kpi"><div class="drs-kpi__label">Future watch</div><div class="drs-kpi__value">${pipe.futureWatch ?? 0}</div></div>
  </div>
  <h3 class="drs-h3">Segment mix (action set)</h3>
  <table class="drs-table">
    <thead><tr><th>Segment</th><th>Count</th></tr></thead>
    <tbody>
      ${segs.map(([k, v]) => `<tr><td>${esc(k)}</td><td>${v}</td></tr>`).join("") || "<tr><td colspan='2'>None</td></tr>"}
    </tbody>
  </table>
</section>`;

  const hi = data.supportingIntelligence || {};
  const supporting = `
<section class="drs-section gdi-pdf-section" data-gdi-pdf-section="hotel-intel">
  <h2 class="drs-h2">Supporting Hotel Intelligence</h2>
  <p class="gdi-pdf-lede">Why this hotel can compete for these groups.</p>
  <ul class="gdi-pdf-list">
    ${hi.rooms != null ? `<li><strong>Guestrooms:</strong> ~${esc(hi.rooms)}</li>` : ""}
    ${hi.meetingSqFt != null ? `<li><strong>Total meeting space:</strong> ~${esc(Number(hi.meetingSqFt).toLocaleString("en-US"))} sq ft</li>` : ""}
    ${hi.largestMeetingSqFt != null ? `<li><strong>Largest meeting room:</strong> ~${esc(Number(hi.largestMeetingSqFt).toLocaleString("en-US"))} sq ft</li>` : ""}
    ${hi.territoryLabel ? `<li><strong>Demand territory:</strong> ${esc(hi.territoryLabel)}</li>` : ""}
  </ul>
  ${(hi.demandAnchors || []).length
    ? `<p><strong>Demand anchors:</strong> ${esc((hi.demandAnchors || []).join("; "))}</p>`
    : ""}
</section>`;

  const methodology = `
<section class="drs-section gdi-pdf-section" data-gdi-pdf-section="methodology">
  <h2 class="drs-h2">Notes</h2>
  <ul class="gdi-pdf-list">
    ${(data.methodology || []).map((m) => `<li>${esc(m)}</li>`).join("")}
  </ul>
</section>`;

  return `
<div class="drs-report gdi-pdf-report brand-alignment-snapshot"
  id="gdi-pdf-host"
  data-gdi-pdf-ready="1"
  data-gdi-hotel-name="${esc(hotel.name)}"
  data-gdi-report-type="${esc(meta.reportTypeLabel || "Group & Demand Intelligence")}"
  data-gdi-report-version="${esc(meta.reportVersion)}"
>
${cover}
${kpi}
${immediate}
${opportunities}
${actionPlan}
${pipeline}
${supporting}
${methodology}
</div>`;
}

export function buildGdiPdfDocumentHtml(data, { cssText = "" } = {}) {
  const body = buildGdiPdfReportBodyHtml(data);
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>${esc(data.hotel?.name || "Hotel")} — Group &amp; Demand Intelligence</title>
  <style>
${cssText}
  </style>
</head>
<body>
${body}
</body>
</html>`;
}
