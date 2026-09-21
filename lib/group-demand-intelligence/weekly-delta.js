/**
 * GDI Weekly Opportunity Delta — reusable for every hotel weekly refresh.
 * Matches current candidates against a prior canonical baseline.
 * Does not invent Bethesda/event/person-specific production rules.
 */

export const WEEKLY_DELTA_VERSION = "gdi_weekly_delta_v1";

export const WEEKLY_DELTA_STATE = Object.freeze({
  NEW: "NEW",
  UPDATED: "UPDATED",
  REACTIVATED: "REACTIVATED",
  UNCHANGED: "UNCHANGED",
  CLOSED_DOWNGRADED: "CLOSED_DOWNGRADED",
  NOT_IN_CURRENT: "NOT_IN_CURRENT",
});

/** Fields that count as material commercial changes. */
export const MATERIAL_FIELDS = Object.freeze([
  "eventStartDate",
  "eventEndDate",
  "destinationStatus",
  "venueStatus",
  "venueSourcingStatus",
  "opportunityType",
  "priority",
  "roomDemandStatus",
  "hostHotel",
  "venue",
  "primaryContactName",
  "primaryContactEmail",
  "primaryContactPhone",
]);

const INACTIVE_PRIORITIES = new Set([
  "WATCHLIST",
  "DISQUALIFIED",
  "TOO_EARLY",
]);
const INACTIVE_TYPES = new Set([
  "FUTURE_CYCLE",
  "CLOSED_DISQUALIFIED",
  "WATCH",
]);
const ACTIONABLE_PRIORITIES = new Set(["HIGH_PRIORITY", "MEDIUM_PRIORITY"]);
const OPEN_SOURCING = new Set([
  "HOTEL_VENUE_TBD",
  "RFP_ACTIVE_SOURCING",
  "OPEN_UNRESOLVED",
  "PARTIALLY_PLACED",
]);

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function yearOf(opp = {}) {
  const d = String(opp.eventStartDate || opp.eventEndDate || "").slice(0, 4);
  if (/^\d{4}$/.test(d)) return d;
  const m = String(opp.title || "").match(/\b(20\d{2})\b/);
  return m ? m[1] : "";
}

function stripTitleNoise(title) {
  return norm(title)
    .replace(/\b20\d{2}\b/g, " ")
    .replace(
      /\b(?:\d+(?:st|nd|rd|th)|annual|interim|hybrid|virtual|in person)\b/g,
      " "
    )
    .replace(/\b(?:meeting|conference|congress|symposium|forum|summit|expo)\b/g, " $& ")
    .replace(/\s+/g, " ")
    .trim();
}

function collectDomains(opp = {}) {
  const urls = [];
  for (const s of opp.sources || opp.evidenceSources || []) {
    if (!s) continue;
    if (typeof s === "string") urls.push(s);
    else urls.push(s.url, s.sourceUrl);
  }
  if (opp.officialSource) urls.push(opp.officialSource);
  const hosts = new Set();
  for (const u of urls.filter(Boolean)) {
    try {
      hosts.add(new URL(u).hostname.replace(/^www\./, ""));
    } catch {
      /* ignore */
    }
  }
  return [...hosts];
}

function acronyms(title, org) {
  const out = new Set();
  const paren = String(title || "").match(/\(([A-Z][A-Z0-9]{1,7})\)/g) || [];
  for (const p of paren) out.add(norm(p.replace(/[()]/g, "")));
  const orgParen = String(org || "").match(/\(([A-Z][A-Z0-9]{1,7})\)/g) || [];
  for (const p of orgParen) out.add(norm(p.replace(/[()]/g, "")));
  const lead = String(title || "").match(/^([A-Z]{2,8})\b/);
  if (lead) out.add(norm(lead[1]));
  return [...out];
}

/**
 * Stable series + cycle keys (generic).
 */
export function deriveEventIdentity(opp = {}) {
  const org = norm(opp.organizationName).slice(0, 48);
  const seriesTitle = stripTitleNoise(opp.title);
  const year = yearOf(opp);
  const market = norm(opp.destinationStatus || opp.location || "")
    .replace(/[^a-z0-9]+/g, " ")
    .slice(0, 40);
  const eventSeriesId =
    opp.eventSeriesId ||
    opp.eventSeriesKey ||
    `series:${org || "org"}|${seriesTitle || "event"}`;
  const eventCycleId =
    opp.eventCycleId || `cycle:${eventSeriesId}|${year || "noyear"}|${market || "nomarket"}`;
  return { eventSeriesId, eventCycleId, seriesTitle, year, org, market };
}

export function snapshotBaselineOpportunity(opp = {}) {
  const id = deriveEventIdentity(opp);
  const pc = opp.primaryContact || {};
  return {
    opportunityId: opp.id || opp.opportunityId,
    eventSeriesId: id.eventSeriesId,
    eventCycleId: id.eventCycleId,
    title: opp.title || null,
    organizationName: opp.organizationName || null,
    eventYear: id.year || null,
    eventStartDate: opp.eventStartDate || null,
    eventEndDate: opp.eventEndDate || null,
    destinationStatus: opp.destinationStatus || null,
    location: opp.location || null,
    officialDomains: collectDomains(opp),
    venue: opp.venue || null,
    venueStatus: opp.venueStatus || null,
    hostHotel: opp.hostHotel || null,
    venueSourcingStatus: opp.venueSourcingStatus || opp.sourcingStatus || null,
    opportunityType: opp.opportunityType || null,
    priority: opp.priority || null,
    hotelFitScore: opp.hotelFitScore ?? null,
    roomDemandStatus: opp.roomDemandStatus || null,
    primaryContactName: pc.name || null,
    primaryContactEmail: pc.email || null,
    primaryContactPhone: pc.phone || null,
    firstSeenAt: opp.firstSeenAt || opp.createdAt || null,
    lastSeenAt: opp.lastSeenAt || opp.lastVerifiedAt || opp.updatedAt || null,
    lastMaterialChangeAt: opp.lastMaterialChangeAt || null,
    firstSeenRunId: opp.firstSeenRunId || null,
    lastSeenRunId: opp.lastSeenRunId || null,
    lastMaterialChangeRunId: opp.lastMaterialChangeRunId || null,
    validationStatus: opp.validationStatus || null,
    actionStatus: opp.actionStatus || opp.bookingWindowStatus || null,
    outcomeStatus: opp.outcomeStatus || null,
    customerValidation: opp.customerValidation || null,
    customerAction: opp.customerAction || null,
    customerOutcome: opp.customerOutcome || null,
  };
}

function materialView(opp = {}) {
  const pc = opp.primaryContact || {};
  return {
    eventStartDate: opp.eventStartDate || null,
    eventEndDate: opp.eventEndDate || null,
    destinationStatus: opp.destinationStatus || null,
    venueStatus: opp.venueStatus || null,
    venueSourcingStatus: opp.venueSourcingStatus || opp.sourcingStatus || null,
    opportunityType: opp.opportunityType || null,
    priority: opp.priority || null,
    roomDemandStatus: opp.roomDemandStatus || null,
    hostHotel: opp.hostHotel || null,
    venue: opp.venue || null,
    primaryContactName: pc.name || opp.primaryContactName || null,
    primaryContactEmail: pc.email || opp.primaryContactEmail || null,
    primaryContactPhone: pc.phone || opp.primaryContactPhone || null,
  };
}

function diffMaterial(prior, current) {
  const a = materialView(prior);
  const b = materialView(current);
  const changedFields = [];
  for (const field of MATERIAL_FIELDS) {
    const oldValue = a[field] ?? null;
    const newValue = b[field] ?? null;
    if (String(oldValue || "") !== String(newValue || "")) {
      // Ignore empty→empty and UNKNOWN→UNKNOWN noise
      if (!oldValue && !newValue) continue;
      if (String(oldValue) === "UNKNOWN" && String(newValue) === "UNKNOWN") continue;
      changedFields.push({ field, oldValue, newValue });
    }
  }
  return changedFields;
}

function isInactive(opp = {}) {
  return (
    INACTIVE_PRIORITIES.has(String(opp.priority || "")) ||
    INACTIVE_TYPES.has(String(opp.opportunityType || "")) ||
    /DISQUALIFIED|CLOSED|CANCELLED|PAST/i.test(String(opp.priority || "")) ||
    /DISQUALIFIED|CLOSED|CANCELLED/i.test(String(opp.opportunityType || ""))
  );
}

function isActionableNow(opp = {}) {
  if (opp.actionabilityV3 === "TRUE_ACTIONABLE") return true;
  if (opp.weeklyActionable === true) return true;
  if (ACTIONABLE_PRIORITIES.has(String(opp.priority || ""))) {
    const vs = String(opp.venueSourcingStatus || "");
    if (OPEN_SOURCING.has(vs) || /OVERFLOW/i.test(String(opp.opportunityType || ""))) {
      return true;
    }
  }
  return false;
}

function becameClosed(prior, current) {
  if (isInactive(prior)) return false; // already inactive — not a new downgrade this week
  const vs = String(current.venueSourcingStatus || current.priority || "");
  const type = String(current.opportunityType || "");
  if (/FULLY_PLACED|NO_OVERFLOW|CANCELLED|CLOSED|NOT_RELEVANT|DISQUALIFIED/i.test(vs)) {
    return true;
  }
  if (/CLOSED_DISQUALIFIED|DISQUALIFIED/i.test(type) && !isInactive(prior)) return true;
  if (current.actionabilityV3 === "INVALID" && isActionableNow(prior)) return true;
  const past =
    current.actionabilityV3 === "INVALID" &&
    /PAST/i.test(String(current.hygieneV3?.failureClass || current.failureClass || ""));
  return Boolean(past);
}

/**
 * Score whether current candidate matches a prior baseline row.
 */
export function scoreCanonicalMatch(prior, current) {
  const p = deriveEventIdentity(prior);
  const c = deriveEventIdentity(current);
  let score = 0;
  const reasons = [];

  if (prior.opportunityId && (current.id === prior.opportunityId || current.opportunityId === prior.opportunityId)) {
    return { score: 100, reasons: ["exact_id"], matched: true };
  }

  if (p.eventCycleId && p.eventCycleId === c.eventCycleId) {
    score += 55;
    reasons.push("event_cycle_id");
  } else if (p.eventSeriesId && p.eventSeriesId === c.eventSeriesId) {
    score += 25;
    reasons.push("event_series_id");
    if (p.year && c.year && p.year === c.year) {
      score += 30;
      reasons.push("same_year_cycle");
    } else if (p.year && c.year && p.year !== c.year) {
      // Different year = different cycle → not a match for identity reuse as same opp
      score -= 40;
      reasons.push("different_year_cycle");
    }
  }

  if (p.org && c.org && (p.org === c.org || p.org.includes(c.org) || c.org.includes(p.org))) {
    score += 18;
    reasons.push("org");
  }

  const pt = p.seriesTitle;
  const ct = c.seriesTitle;
  if (pt && ct) {
    if (pt === ct) {
      score += 22;
      reasons.push("title_exact_norm");
    } else if (pt.includes(ct) || ct.includes(pt)) {
      score += 14;
      reasons.push("title_alias");
    }
  }

  const pa = new Set(acronyms(prior.title, prior.organizationName));
  const ca = acronyms(current.title, current.organizationName);
  if (ca.some((a) => pa.has(a) && a.length >= 2)) {
    score += 12;
    reasons.push("acronym");
  }

  const pd = new Set(prior.officialDomains || collectDomains(prior));
  const cd = collectDomains(current);
  if (cd.some((d) => pd.has(d))) {
    score += 15;
    reasons.push("official_domain");
  }

  if (p.year && c.year && p.year === c.year) {
    score += 8;
    reasons.push("year");
  }

  if (
    prior.eventStartDate &&
    current.eventStartDate &&
    String(prior.eventStartDate).slice(0, 10) === String(current.eventStartDate).slice(0, 10)
  ) {
    score += 10;
    reasons.push("same_start_date");
  }

  return { score, reasons, matched: score >= 55 };
}

export function matchCurrentToBaseline(baselineRows = [], currentOpp = {}) {
  let best = null;
  for (const prior of baselineRows) {
    const m = scoreCanonicalMatch(prior, currentOpp);
    if (!best || m.score > best.score) {
      best = { prior, ...m };
    }
  }
  if (!best || !best.matched) return { matched: false, prior: null, score: best?.score || 0, reasons: best?.reasons || [] };
  return { matched: true, prior: best.prior, score: best.score, reasons: best.reasons };
}

/**
 * Classify one current candidate against baseline.
 * @param {object} opts
 * @param {string} opts.runId
 * @param {string} [opts.nowIso]
 * @param {boolean} [opts.currentIsTrueActionable]
 */
export function classifyWeeklyDelta(baselineRows, currentOpp, opts = {}) {
  const runId = opts.runId || "unknown_run";
  const nowIso = opts.nowIso || new Date().toISOString();
  const match = matchCurrentToBaseline(baselineRows, currentOpp);
  const trueActionable = Boolean(
    opts.currentIsTrueActionable != null
      ? opts.currentIsTrueActionable
      : currentOpp.actionabilityV3 === "TRUE_ACTIONABLE" ||
          currentOpp.weeklyActionable === true
  );

  if (!match.matched) {
    if (!trueActionable) {
      return {
        weeklyDeltaState: WEEKLY_DELTA_STATE.UNCHANGED,
        isNewThisWeek: false,
        match,
        note: "unmatched_non_actionable_skipped_as_new",
        skipPersist: true,
      };
    }
    return {
      weeklyDeltaState: WEEKLY_DELTA_STATE.NEW,
      isNewThisWeek: true,
      match,
      firstSeenAt: nowIso,
      firstSeenRunId: runId,
      lastSeenAt: nowIso,
      lastSeenRunId: runId,
      lastMaterialChangeAt: nowIso,
      lastMaterialChangeRunId: runId,
      changedFields: [],
    };
  }

  const prior = match.prior;
  const changedFields = diffMaterial(prior, {
    ...currentOpp,
    primaryContact: currentOpp.primaryContact || {
      name: currentOpp.primaryContactName,
      email: currentOpp.primaryContactEmail,
      phone: currentOpp.primaryContactPhone,
    },
  });

  // Source-only corroboration: no material field change
  const sourceOnly =
    changedFields.length === 0 &&
    collectDomains(currentOpp).some((d) => (prior.officialDomains || []).includes(d));

  if (becameClosed(prior, currentOpp)) {
    return {
      weeklyDeltaState: WEEKLY_DELTA_STATE.CLOSED_DOWNGRADED,
      isNewThisWeek: false,
      match,
      firstSeenAt: prior.firstSeenAt,
      firstSeenRunId: prior.firstSeenRunId,
      lastSeenAt: nowIso,
      lastSeenRunId: runId,
      lastMaterialChangeAt: nowIso,
      lastMaterialChangeRunId: runId,
      changedFields,
    };
  }

  const reactivated =
    isInactive(prior) &&
    trueActionable &&
    (OPEN_SOURCING.has(String(currentOpp.venueSourcingStatus || "")) ||
      changedFields.some((c) =>
        ["venueSourcingStatus", "opportunityType", "roomDemandStatus", "priority"].includes(
          c.field
        )
      ));

  if (reactivated) {
    return {
      weeklyDeltaState: WEEKLY_DELTA_STATE.REACTIVATED,
      isNewThisWeek: false,
      match,
      firstSeenAt: prior.firstSeenAt,
      firstSeenRunId: prior.firstSeenRunId,
      lastSeenAt: nowIso,
      lastSeenRunId: runId,
      lastMaterialChangeAt: nowIso,
      lastMaterialChangeRunId: runId,
      changedFields,
    };
  }

  if (changedFields.length > 0) {
    return {
      weeklyDeltaState: WEEKLY_DELTA_STATE.UPDATED,
      isNewThisWeek: false,
      match,
      firstSeenAt: prior.firstSeenAt,
      firstSeenRunId: prior.firstSeenRunId,
      lastSeenAt: nowIso,
      lastSeenRunId: runId,
      lastMaterialChangeAt: nowIso,
      lastMaterialChangeRunId: runId,
      changedFields,
    };
  }

  return {
    weeklyDeltaState: WEEKLY_DELTA_STATE.UNCHANGED,
    isNewThisWeek: false,
    match,
    firstSeenAt: prior.firstSeenAt,
    firstSeenRunId: prior.firstSeenRunId,
    lastSeenAt: nowIso,
    lastSeenRunId: runId,
    lastMaterialChangeAt: prior.lastMaterialChangeAt || null,
    lastMaterialChangeRunId: prior.lastMaterialChangeRunId || null,
    changedFields: [],
    sourceOnly,
  };
}

/**
 * Full weekly delta pass.
 */
export function computeWeeklyDelta({
  baselineOpportunities = [],
  currentCandidates = [],
  runId,
  nowIso,
  trueActionableIds = null,
} = {}) {
  const baselineRows = baselineOpportunities.map((o) =>
    o.opportunityId ? o : snapshotBaselineOpportunity(o)
  );
  const trueSet = trueActionableIds
    ? new Set(trueActionableIds)
    : new Set(
        currentCandidates
          .filter((c) => c.actionabilityV3 === "TRUE_ACTIONABLE" || c.weeklyActionable)
          .map((c) => c.id || c.opportunityId)
      );

  const matchedPriorIds = new Set();
  const rows = [];

  for (const cur of currentCandidates) {
    const id = cur.id || cur.opportunityId;
    const classified = classifyWeeklyDelta(baselineRows, cur, {
      runId,
      nowIso,
      currentIsTrueActionable: trueSet.has(id) || cur.actionabilityV3 === "TRUE_ACTIONABLE",
    });
    if (classified.skipPersist) continue;
    if (classified.match?.prior?.opportunityId) {
      matchedPriorIds.add(classified.match.prior.opportunityId);
    }
    rows.push({
      opportunityId:
        classified.weeklyDeltaState === WEEKLY_DELTA_STATE.NEW
          ? id
          : classified.match?.prior?.opportunityId || id,
      candidateId: id,
      title: cur.title,
      organizationName: cur.organizationName,
      ...classified,
      current: {
        eventStartDate: cur.eventStartDate,
        destinationStatus: cur.destinationStatus,
        opportunityType: cur.opportunityType,
        priority: cur.priority,
        venueSourcingStatus: cur.venueSourcingStatus,
        actionabilityV3: cur.actionabilityV3 || null,
      },
    });
  }

  // Priors not seen this week stay historically preserved (NOT_IN_CURRENT annotation only)
  for (const prior of baselineRows) {
    if (matchedPriorIds.has(prior.opportunityId)) continue;
    rows.push({
      opportunityId: prior.opportunityId,
      candidateId: null,
      title: prior.title,
      organizationName: prior.organizationName,
      weeklyDeltaState: WEEKLY_DELTA_STATE.NOT_IN_CURRENT,
      isNewThisWeek: false,
      match: { matched: true, prior, score: 0, reasons: ["absent_from_current_run"] },
      firstSeenAt: prior.firstSeenAt,
      firstSeenRunId: prior.firstSeenRunId,
      lastSeenAt: prior.lastSeenAt,
      lastSeenRunId: prior.lastSeenRunId,
      changedFields: [],
      current: null,
    });
  }

  const counts = {
    NEW: 0,
    UPDATED: 0,
    REACTIVATED: 0,
    UNCHANGED: 0,
    CLOSED_DOWNGRADED: 0,
    NOT_IN_CURRENT: 0,
  };
  for (const r of rows) {
    counts[r.weeklyDeltaState] = (counts[r.weeklyDeltaState] || 0) + 1;
  }

  return {
    version: WEEKLY_DELTA_VERSION,
    runId,
    counts,
    rows,
  };
}

/**
 * Derive customer-facing isNewThisWeek from durable firstSeenRunId.
 */
export function deriveIsNewThisWeek(opp = {}, latestCompletedWeeklyRunId) {
  if (!latestCompletedWeeklyRunId) return false;
  return String(opp.firstSeenRunId || "") === String(latestCompletedWeeklyRunId);
}

/**
 * Apply delta annotations onto a canonical opportunity bag without resetting history.
 */
export function applyWeeklyDeltaToOpportunities({
  existingOpportunities = [],
  delta,
  runId,
  latestCompletedWeeklyRunId,
  newOpportunityBuilders = {},
} = {}) {
  const byId = new Map(
    existingOpportunities.map((o) => [o.id || o.opportunityId, { ...o }])
  );
  const customerFields = [
    "customerValidation",
    "customerAction",
    "customerOutcome",
    "validationStatus",
    "actionStatus",
    "outcomeStatus",
  ];

  for (const row of delta.rows || []) {
    if (row.weeklyDeltaState === WEEKLY_DELTA_STATE.NOT_IN_CURRENT) continue;

    if (row.weeklyDeltaState === WEEKLY_DELTA_STATE.NEW) {
      const built =
        newOpportunityBuilders[row.candidateId] ||
        newOpportunityBuilders[row.opportunityId] ||
        null;
      if (!built) continue;
      const id = built.id || row.opportunityId;
      if (byId.has(id)) continue; // zero duplicates
      byId.set(id, {
        ...built,
        id,
        firstSeenAt: row.firstSeenAt,
        firstSeenRunId: runId,
        lastSeenAt: row.lastSeenAt,
        lastSeenRunId: runId,
        lastMaterialChangeAt: row.lastMaterialChangeAt,
        lastMaterialChangeRunId: runId,
        weeklyDeltaState: WEEKLY_DELTA_STATE.NEW,
        isNewThisWeek: true,
      });
      continue;
    }

    const existing = byId.get(row.opportunityId);
    if (!existing) continue;

    const preserved = {};
    for (const f of customerFields) {
      if (existing[f] != null) preserved[f] = existing[f];
    }

    byId.set(row.opportunityId, {
      ...existing,
      ...preserved,
      firstSeenAt: existing.firstSeenAt || row.firstSeenAt,
      firstSeenRunId: existing.firstSeenRunId || row.firstSeenRunId,
      lastSeenAt: row.lastSeenAt || existing.lastSeenAt,
      lastSeenRunId: row.lastSeenRunId || runId,
      lastMaterialChangeAt:
        row.weeklyDeltaState === WEEKLY_DELTA_STATE.UNCHANGED
          ? existing.lastMaterialChangeAt || null
          : row.lastMaterialChangeAt || existing.lastMaterialChangeAt,
      lastMaterialChangeRunId:
        row.weeklyDeltaState === WEEKLY_DELTA_STATE.UNCHANGED
          ? existing.lastMaterialChangeRunId || null
          : row.lastMaterialChangeRunId || existing.lastMaterialChangeRunId,
      weeklyDeltaState: row.weeklyDeltaState,
      weeklyChangedFields: row.changedFields || [],
      isNewThisWeek: deriveIsNewThisWeek(
        { firstSeenRunId: existing.firstSeenRunId || row.firstSeenRunId },
        latestCompletedWeeklyRunId || runId
      ),
    });
  }

  // Clear isNewThisWeek on opps not from this run's NEW set
  for (const [id, opp] of byId) {
    const isNew = deriveIsNewThisWeek(opp, latestCompletedWeeklyRunId || runId);
    opp.isNewThisWeek = isNew;
    if (!opp.weeklyDeltaState) {
      opp.weeklyDeltaState = isNew
        ? WEEKLY_DELTA_STATE.NEW
        : WEEKLY_DELTA_STATE.UNCHANGED;
    }
    byId.set(id, opp);
  }

  return [...byId.values()];
}

export function weeklyDeltaSummaryCounts(opportunities = [], latestRunId) {
  let neu = 0;
  let updated = 0;
  let reactivated = 0;
  for (const o of opportunities) {
    const state = o.weeklyDeltaState;
    const isNew =
      o.isNewThisWeek === true ||
      (latestRunId && deriveIsNewThisWeek(o, latestRunId));
    if (isNew || state === WEEKLY_DELTA_STATE.NEW) neu += 1;
    else if (state === WEEKLY_DELTA_STATE.UPDATED) updated += 1;
    else if (state === WEEKLY_DELTA_STATE.REACTIVATED) reactivated += 1;
  }
  return { newThisWeek: neu, updated, reactivated };
}
