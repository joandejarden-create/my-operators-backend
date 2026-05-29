/**
 * Render My Deal Flow + pipeline KPI strip; sync weekly history to /api/brand-workspace/kpi-history.
 */

const KPI_CONFIG = {
  brand: {
    flowTitle: "My Deal Flow",
    pipelineTitle: "Deal pipeline by stage",
    needsActionLabel: "Brand action",
    awaitingLabel: "Awaiting owner",
    requestsSentLabel: "Requests sent (7d)",
    requestsSentHint:
      "Counts rows whose owner request was sent to your brand in the last 7 days (request-sent date). Same filters as the table.",
    inReviewLabel: "Under review",
    pipelineStages: [
      { label: "New inbound", bucket: "new" },
      { label: "Under review", bucket: "active-review" },
      { label: "Terms & proposal", bucket: "terms-proposal" },
      { label: "Deal room & finalist", bucket: "nda-room,advanced" },
      { label: "Closed", bucket: "closed" },
      { label: "Passed", bucket: "passed" },
    ],
  },
  operator: {
    flowTitle: "My Deal Flow",
    pipelineTitle: "Operating pipeline by stage",
    needsActionLabel: "Operator action",
    awaitingLabel: "Awaiting owner",
    requestsSentLabel: "Inbound (7d)",
    requestsSentHint:
      "Counts operating opportunities owners sent to your company in the last 7 days (request-sent date). Same filters as the table.",
    inReviewLabel: "Under review",
    pipelineStages: [
      { label: "New inbound", bucket: "new" },
      { label: "Under review", bucket: "active-review" },
      { label: "Terms review", bucket: "terms-proposal" },
      { label: "NDA & advanced", bucket: "nda-room,advanced" },
      { label: "Closed", bucket: "closed" },
      { label: "Passed", bucket: "passed" },
    ],
  },
  owner: {
    flowTitle: "My Deal Flow",
    pipelineTitle: "Deal pipeline by stage",
    needsActionLabel: "Owner action",
    awaitingLabel: "Awaiting brand",
    requestsSentLabel: "Outreach sent (7d)",
    requestsSentHint:
      "Counts brand outreach rows you sent in the last 7 days (request-sent date). Matches what brands see as new inbound.",
    inReviewLabel: "In progress",
    pipelineStages: [
      { label: "Submitted", bucket: "new" },
      { label: "Under review", bucket: "active-review" },
      { label: "Bid submitted", bucket: "terms-proposal" },
      { label: "Negotiation", bucket: "nda-room,advanced" },
      { label: "Signed", bucket: "closed" },
      { label: "Passed", bucket: "passed" },
    ],
  },
};

const WEEKS_STORAGE = "dealality_workspace_kpi_weeks_v4";

function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatWoWTrend(cur, prev, invertColors) {
  if (prev == null || prev === undefined || Number.isNaN(Number(prev))) {
    return { text: "—", trend: "neutral" };
  }
  const d = cur - prev;
  if (d === 0) {
    return { text: "Same vs last wk", trend: "neutral", title: "Same vs last week" };
  }
  const pos = d > 0;
  const trend = invertColors ? (pos ? "negative" : "positive") : pos ? "positive" : "negative";
  const arrow = pos ? "▲" : "▼";
  const sign = pos ? "+" : "";
  const full = `${arrow} ${sign}${d} vs last week`;
  return { text: `${arrow} ${sign}${d} vs last wk`, trend, title: full };
}

function formatRolling7dDelta(cur, prevWindowCount, invertColors) {
  if (prevWindowCount == null || Number.isNaN(Number(prevWindowCount))) {
    return { text: "—", trend: "neutral" };
  }
  const d = cur - prevWindowCount;
  if (d === 0) {
    return { text: "No change · 7d", trend: "neutral", title: "No change (7d)" };
  }
  const pos = d > 0;
  const trend = invertColors ? (pos ? "negative" : "positive") : pos ? "positive" : "negative";
  const arrow = pos ? "▲" : "▼";
  const sign = pos ? "+" : "";
  const full = `${arrow} ${sign}${d} (7d)`;
  return { text: `${arrow} ${sign}${d} · 7d`, trend, title: full };
}

function readLocalWeeks(scopeKey) {
  try {
    const root = JSON.parse(localStorage.getItem(WEEKS_STORAGE) || "{}");
    return root[scopeKey] && typeof root[scopeKey] === "object" ? root[scopeKey] : {};
  } catch {
    return {};
  }
}

function writeLocalWeeks(scopeKey, weeksObj) {
  try {
    const wkKeys = Object.keys(weeksObj).sort();
    wkKeys.slice(0, Math.max(0, wkKeys.length - 14)).forEach((k) => {
      delete weeksObj[k];
    });
    const root = JSON.parse(localStorage.getItem(WEEKS_STORAGE) || "{}");
    root[scopeKey] = weeksObj;
    localStorage.setItem(WEEKS_STORAGE, JSON.stringify(root));
  } catch {
    /* quota */
  }
}

let syncTimer = null;

function scheduleKpiServerSync(scopeKey, weekKey, snapshot) {
  if (syncTimer) clearTimeout(syncTimer);
  syncTimer = setTimeout(() => {
    syncTimer = null;
    const base = window.location.origin || "";
    fetch(base + "/api/brand-workspace/kpi-history", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scopeKey, weekKey, snapshot }),
    }).catch(() => {});
  }, 900);
}

async function fetchServerWeeks(scopeKey) {
  try {
    const base = window.location.origin || "";
    const res = await fetch(
      base + "/api/brand-workspace/kpi-history?scopeKey=" + encodeURIComponent(scopeKey)
    );
    if (!res.ok) return {};
    const j = await res.json();
    return j.success && j.weeks && typeof j.weeks === "object" ? j.weeks : {};
  } catch {
    return {};
  }
}

function pipelineRowsForBucket(enriched, bucketSpec, Pipeline) {
  const parts = String(bucketSpec || "").split(",");
  if (bucketSpec === "closed") {
    return enriched.filter(
      (r) => r.workspaceBucket === "archived" && !Pipeline.isPassedArchived(r)
    );
  }
  if (bucketSpec === "passed") {
    return enriched.filter(
      (r) => r.workspaceBucket === "archived" && Pipeline.isPassedArchived(r)
    );
  }
  if (parts.length > 1) {
    const set = new Set(parts);
    return enriched.filter((r) => set.has(r.workspaceBucket));
  }
  return enriched.filter((r) => r.workspaceBucket === bucketSpec);
}

function renderPipelineMeta(parts, Pipeline) {
  if (!parts || !parts.length) return "";
  return (
    '<div class="bdd-kpi-pipeline-meta-stack">' +
    parts
      .map(
        (p) =>
          '<span class="bdd-kpi-pipeline-meta-line' +
          (p.warn ? " bdd-pipeline-meta-warn" : "") +
          '">' +
          esc(p.html) +
          "</span>"
      )
      .join("") +
    "</div>"
  );
}

function renderInsightsStrip(items, stuckPopoverId) {
  const cards = items.map((it) => {
    const badgeClass =
      it.trend === "positive" ? "positive" : it.trend === "negative" ? "negative" : "neutral";
    const cardHi = it.highlight ? " bdd-kpi-metric-card--action" : "";
    const cardTitleAttr = it.cardTitle ? ' title="' + esc(it.cardTitle) + '"' : "";
    let infoBtn = "";
    if (it.atRisk) {
      infoBtn =
        '<span class="bdd-kpi-info-wrap--card">' +
        '<button type="button" class="bdd-kpi-why-btn" popovertarget="' +
        esc(stuckPopoverId || "workspaceKpiStuckPopover") +
        '" ' +
        'aria-haspopup="dialog" aria-label="How Stuck at risk is calculated">ℹ</button></span>';
    }
    const infoCls = infoBtn ? " bdd-kpi-metric-card--has-info" : "";
    const badgeTitleAttr = it.badgeTitle
      ? ' title="' + esc(it.badgeTitle) + '"'
      : "";
    return (
      '<div class="bdd-kpi-metric-card' +
      cardHi +
      infoCls +
      '"' +
      cardTitleAttr +
      ">" +
      '<div class="bdd-kpi-metric-card__label-wrap">' +
      '<span class="bdd-kpi-metric-card__label">' +
      esc(it.label) +
      "</span>" +
      infoBtn +
      "</div>" +
      '<span class="bdd-kpi-metric-card__value">' +
      esc(it.value) +
      "</span>" +
      '<div class="bdd-kpi-metric-card__footer">' +
      '<span class="bdd-insight-card__badge ' +
      badgeClass +
      '"' +
      badgeTitleAttr +
      ">" +
      esc(it.badgeText) +
      "</span>" +
      "</div>" +
      "</div>"
    );
  });
  return (
    '<div class="bdd-kpi-strip bdd-kpi-strip--insights bdd-kpi-strip--cards" role="group" aria-label="My deal flow">' +
    cards.join("") +
    "</div>"
  );
}

function renderPipelineStrip(stages) {
  const cards = stages.map((s) => {
    const meta =
      typeof s.metaHtml === "string" && s.metaHtml
        ? s.metaHtml
        : '<span class="bdd-insight-card__badge neutral">—</span>';
    return (
      '<div class="bdd-kpi-metric-card bdd-kpi-metric-card--pipeline">' +
      '<div class="bdd-kpi-metric-card__label-wrap">' +
      '<span class="bdd-kpi-metric-card__label">' +
      esc(s.label) +
      "</span></div>" +
      '<span class="bdd-kpi-metric-card__value bdd-kpi-metric-card__value--pipe">' +
      esc(s.count) +
      "</span>" +
      '<div class="bdd-kpi-metric-card__footer bdd-kpi-metric-card__footer--meta bdd-kpi-pipeline-meta-cell">' +
      meta +
      "</div></div>"
    );
  });
  return (
    '<div class="bdd-kpi-strip bdd-kpi-strip--pipeline bdd-kpi-strip--cards" role="group" aria-label="Deal pipeline by stage">' +
    cards.join("") +
    "</div>"
  );
}

/**
 * @param {object} opts
 * @param {'brand'|'owner'|'operator'} opts.persona
 * @param {object[]} opts.rows
 * @param {object} opts.filterParts
 * @param {typeof import('../../lib/deal-workspace-pipeline.js')} opts.Pipeline
 */
export async function updateWorkspaceInsights(opts) {
  const Pipeline = opts.Pipeline;
  const persona =
    opts.persona === "owner" ? "owner" : opts.persona === "operator" ? "operator" : "brand";
  const cfg = KPI_CONFIG[persona] || KPI_CONFIG.brand;
  const insEl = document.getElementById(opts.insightsElId || "bddKpiInsights");
  const pipeEl = document.getElementById(opts.pipelineElId || "bddKpiPipeline");
  const wrap = opts.wrapElId ? document.getElementById(opts.wrapElId) : null;
  if (!insEl || !pipeEl) return;

  const rows = opts.rows || [];
  const enriched = rows.map((r) => Pipeline.enrichWorkspaceRow(r));
  const snap = Pipeline.computeWorkspaceKpiSnapshot(rows, persona);

  const now = Date.now();
  const newRollingPrev7d = Pipeline.countRequestSentInRange(
    enriched,
    now - 14 * 86400000,
    now - 7 * 86400000
  );

  const scopeKey = Pipeline.buildKpiScopeKey(persona, opts.filterParts || {});
  const wk = Pipeline.isoWeekKey(new Date());
  const prevWk = Pipeline.prevIsoWeekKey(wk);
  const serverWeeks = await fetchServerWeeks(scopeKey);
  const localWeeks = readLocalWeeks(scopeKey);
  const mergedWeeks = { ...localWeeks, ...serverWeeks };
  const prev = prevWk ? mergedWeeks[prevWk] : null;

  mergedWeeks[wk] = snap;
  writeLocalWeeks(scopeKey, mergedWeeks);
  scheduleKpiServerSync(scopeKey, wk, snap);

  const dashWoW = { text: "—", trend: "neutral" };
  const tNewRoll = formatRolling7dDelta(snap.newRolling7d, newRollingPrev7d, false);
  const tRisk = formatWoWTrend(snap.atRisk, prev ? prev.atRisk : null, true);

  const insightsHtml = renderInsightsStrip(
    [
    {
      label: cfg.needsActionLabel,
      value: snap.needsAction,
      badgeText: dashWoW.text,
      trend: dashWoW.trend,
      highlight: true,
    },
    {
      label: cfg.awaitingLabel,
      value: snap.awaitingCounterparty,
      badgeText: dashWoW.text,
      trend: dashWoW.trend,
    },
    {
      label: "Stuck / at risk",
      value: snap.atRisk,
      badgeText: tRisk.text,
      badgeTitle: tRisk.title,
      trend: tRisk.trend,
      atRisk: true,
    },
    {
      label: cfg.requestsSentLabel,
      value: snap.newRolling7d,
      badgeText: tNewRoll.text,
      badgeTitle: tNewRoll.title,
      trend: tNewRoll.trend,
      cardTitle: cfg.requestsSentHint,
    },
    {
      label: cfg.inReviewLabel,
      value: snap.inReview,
      badgeText: dashWoW.text,
      trend: dashWoW.trend,
    },
    ],
    opts.stuckPopoverId || "workspaceKpiStuckPopover"
  );

  const pipelineHtml = renderPipelineStrip(
    cfg.pipelineStages.map((st) => {
      const list = pipelineRowsForBucket(enriched, st.bucket, Pipeline);
      const metaParts = Pipeline.pipelineStageMeta(list);
      return {
        label: st.label,
        count: list.length,
        metaHtml: renderPipelineMeta(metaParts, Pipeline),
      };
    })
  );

  insEl.innerHTML = insightsHtml;
  pipeEl.innerHTML = pipelineHtml;
  insEl.removeAttribute("aria-busy");
  pipeEl.removeAttribute("aria-busy");
  if (wrap) wrap.hidden = false;

  if (opts.flowTitleElId !== false) {
    const flowTitle = document.getElementById(opts.flowTitleElId || "workspaceInsightsFlowTitle");
    if (flowTitle) flowTitle.textContent = cfg.flowTitle;
  }
  if (opts.pipelineTitleElId !== false) {
    const pipeTitle = document.getElementById(opts.pipelineTitleElId || "workspaceInsightsPipelineTitle");
    if (pipeTitle) pipeTitle.textContent = cfg.pipelineTitle;
  }

  if (typeof window !== "undefined" && snap.mirror?.ties) {
    const t = snap.mirror.ties;
    if (!t.ownerAwaitingBrandEqualsBrandNeedsAction || !t.ownerNeedsActionEqualsBrandAwaitingOwner) {
      console.warn("[workspace-kpi] Mirror mismatch on this row set", snap.mirror);
    }
    if (Pipeline.auditWorkspaceKpiMirror) {
      const audit = Pipeline.auditWorkspaceKpiMirror(rows);
      if (!audit.ok) console.warn("[workspace-kpi] Audit", audit.violations);
    }
  }

  return snap;
}

export { KPI_CONFIG, formatWoWTrend, formatRolling7dDelta };
