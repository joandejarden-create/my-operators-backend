/**
 * GDI Semantic Commercial Remediation V1
 *
 * Shared presentation-layer corrections for recurring semantic-audit P1 clusters.
 * Preserves research evidence and commercial qualification; only customer-facing
 * prose / action framing / evidence-URL hygiene are adjusted.
 *
 * Classification: REUSABLE_PRODUCT_CONTRACT
 */

export const GDI_SEMANTIC_COMMERCIAL_REMEDIATION_V1 =
  "gdi-semantic-commercial-remediation-v1";
export const GDI_SEMANTIC_COMMERCIAL_REMEDIATION_REVISION = "v1.0";

function text(v) {
  return v == null ? "" : String(v).replace(/\s+/g, " ").trim();
}

function normKey(v) {
  return text(v).toLowerCase();
}

const TOURNAMENT_HOUSING_RE =
  /\btournament\s+housing\s+lead\b|\btournament\s+housing\b/gi;
const SPORTS_SEGMENT_RE =
  /\b(?:sport|sports|athletic|tournament|league|cup|championship|race|marathon|triathlon|soccer|football|baseball|basketball|hockey|tennis|golf|esport)\b/i;
const PIPELINE_ROLE_PAREN_RE =
  /\s*\(\s*(?:SPONSOR|EXHIBITOR|PARTNER|VENDOR|BUYER|SUPPLIER|ATTENDEE|DELEGATE|MEDIA|SPEAKER)\s*\)\s*$/i;
const PIPELINE_SLUG_PREFIX_RE =
  /^(?:PR\s+Teams?|Pipeline|Account\s+Slug|Demand\s+Slug|YCAMP|Y\s*Camp)\s*[—–-]\s*/i;
const URGENCY_NOW_RE =
  /\b(?:act\s+now|pursue\s+now|contact\s+now|book\s+now|urgent(?:ly)?|immediate(?:ly)?|this\s+week|asap|essential\s+as\s+the\s+event\s+date\s+approaches|planning\s+for\s+hotel\s+accommodations\s+is\s+essential)\b/i;
const SIZED_GROUP_LEAD_RE = /^sized\s+group\s+opportunity\.\s*/i;
const VAGUE_LOCATION_RE = /^Confirmed county\s*\/\s*facility geography$/i;
const MONITOR_ACTION_RE =
  /\b(?:monitor|watch|do\s+not\s+pursue|past\s+cycle|future(?:\s*\/\s*|\s+)next[- ]?cycle|insufficient)\b/i;
const WIN_ROOM_BLOCK_RE = /\bwin\s+room\s+block\b/i;
const CLOSED_OR_MONITOR_SOURCING_RE =
  /\b(?:closed|monitor(?:\s+only)?|host\s+hotel\s+contracted|competitor\s+(?:venue|hotel|booking|capture)|already\s+contracted)\b/i;

function isSportsContext(opp = {}) {
  const ids = [
    opp.segment,
    opp.demandSegment,
    opp.demandSegmentId,
    opp.customerDemandSegmentId,
    opp.opportunityTypeFilter,
    opp.opportunityTypeFilterCategory,
  ]
    .map(normKey)
    .filter(Boolean);
  if (
    ids.some((id) =>
      /sports?(_entertainment|_group)?|athletic/.test(id.replace(/\s+/g, "_"))
    )
  ) {
    return true;
  }
  const blob = [
    opp.segment,
    opp.demandSegment,
    opp.eventType,
    opp.eventTypeLabel,
    opp.category,
    opp.classification,
    opp.title,
    opp.displayTitle,
    opp.eventName,
    opp.opportunityType,
  ]
    .map(text)
    .filter(Boolean)
    .join(" ");
  return SPORTS_SEGMENT_RE.test(blob);
}

function scrubTournamentHousingLeak(value, opp) {
  const s = text(value);
  if (!s || !TOURNAMENT_HOUSING_RE.test(s)) return value;
  if (isSportsContext(opp)) return value;
  return text(
    s.replace(TOURNAMENT_HOUSING_RE, (m) =>
      /lead/i.test(m) ? "housing / hotel-reservation lead" : "housing demand"
    )
  );
}

function scrubPipelineSlugTitle(raw) {
  let t = text(raw);
  if (!t) return t;
  t = t.replace(PIPELINE_ROLE_PAREN_RE, "").trim();
  if (PIPELINE_SLUG_PREFIX_RE.test(t)) {
    const stripped = t.replace(PIPELINE_SLUG_PREFIX_RE, "").trim();
    if (stripped.length >= 4) t = stripped;
  }
  // "YCAMP 2026 — Event Name" style research pipeline leftovers
  t = t.replace(/^YCAMP(?:\s+20\d{2})?\s*[—–-]\s*/i, "").trim();
  return t;
}

function dedupeEvidenceSources(list) {
  if (!Array.isArray(list) || !list.length) return list;
  const seen = new Set();
  const out = [];
  for (const item of list) {
    if (item == null) continue;
    if (typeof item === "string") {
      const key = normKey(item);
      if (!key || seen.has(key)) continue;
      seen.add(key);
      out.push(item);
      continue;
    }
    if (typeof item === "object") {
      const url = text(item.url || item.href || item.link || item.sourceUrl);
      const label = text(item.label || item.title || item.name || item.source);
      const key = normKey(url || label || JSON.stringify(item));
      if (!key || seen.has(key)) continue;
      seen.add(key);
      out.push(item);
      continue;
    }
    out.push(item);
  }
  return out;
}

function textsEqualLoose(a, b) {
  const left = normKey(a);
  const right = normKey(b);
  if (!left || !right) return false;
  if (left === right) return true;
  // Near-duplicate: one is a prefix of the other (>= 80 chars shared)
  const shorter = left.length <= right.length ? left : right;
  const longer = left.length > right.length ? left : right;
  return shorter.length >= 80 && longer.startsWith(shorter);
}

function clearDuplicatedWhyActNow(opp) {
  const why = text(opp.whyActNow || opp.whyNow || opp.urgencyReason);
  const potential = text(
    opp.potentialBusiness ||
      opp.potentialBusinessSummary ||
      opp.commercialPotential ||
      opp.whatIsTheOpportunity
  );
  if (!why || !potential || !textsEqualLoose(why, potential)) return opp;
  const next = { ...opp };
  const monitorSafe =
    "Timing is watch-only until a confirmed lodging path or open sourcing signal appears — do not treat potential demand as an active chase.";
  if (next.whyActNow != null) next.whyActNow = monitorSafe;
  if (next.whyNow != null) next.whyNow = monitorSafe;
  if (next.urgencyReason != null) next.urgencyReason = monitorSafe;
  if (next.drawerSections && typeof next.drawerSections === "object") {
    next.drawerSections = { ...next.drawerSections };
    if (next.drawerSections.whyActNow != null) {
      next.drawerSections.whyActNow = monitorSafe;
    }
    if (next.drawerSections.whyNow != null) {
      next.drawerSections.whyNow = monitorSafe;
    }
  }
  return next;
}

function scrubMalformedPotentialBusinessLead(value) {
  const s = text(value);
  if (!s || !SIZED_GROUP_LEAD_RE.test(s)) return value;
  const rest = s.replace(SIZED_GROUP_LEAD_RE, "").trim();
  if (!rest) return "Sized group opportunity — validate lodging accessibility before pursuit.";
  return rest.charAt(0).toUpperCase() + rest.slice(1);
}

function scrubSentenceCaseAndPlaceholders(value) {
  let s = text(value);
  if (!s) return value;
  s = s.replace(
    /;\s*hotel arrangements still need confirmation\.?/gi,
    ". Hotel arrangements still need confirmation."
  );
  s = s.replace(/\bBooth\s+Booth\b/gi, "Exhibitor booth contact");
  s = s.replace(/\bOTHER_RELEVANT\b/g, "Other contact");
  // Drop unpublished peak-room "Size signal" claims that contradict overview
  s = s.replace(/\bSize signal:\s*\d+\s*peak rooms\.?/gi, "").replace(/\s{2,}/g, " ").trim();
  return s;
}

function alignUrgencyWithMonitorAction(opp) {
  const action = text(
    opp.recommendedAction || opp.suggestedAction || opp.customerRecommendedAction
  );
  const disqualified =
    /disqualified/i.test(text(opp.priority || opp.cardPriority || opp.customerFacingState)) ||
    /Past cycle\s*[—–-]\s*do not pursue/i.test(action);
  if ((!action || !MONITOR_ACTION_RE.test(action)) && !disqualified) return opp;
  const fields = ["whyActNow", "whyNow", "urgencyReason", "urgencyCopy"];
  let changed = false;
  const next = { ...opp };
  for (const f of fields) {
    const v = text(next[f]);
    if (!v) continue;
    if (!URGENCY_NOW_RE.test(v) && !/pursue|win room|book the block/i.test(v)) {
      continue;
    }
    next[f] =
      "Monitor stance — no active chase window. Revisit when sourcing opens or a confirmed lodging path appears.";
    changed = true;
  }
  if (next.drawerSections && typeof next.drawerSections === "object") {
    const ds = { ...next.drawerSections };
    for (const f of ["whyActNow", "whyNow", "urgency"]) {
      const v = text(ds[f]);
      if (!v) continue;
      if (URGENCY_NOW_RE.test(v) || /pursue|win room|book the block/i.test(v)) {
        ds[f] =
          "Monitor stance — no active chase window. Revisit when sourcing opens or a confirmed lodging path appears.";
        changed = true;
      }
    }
    if (changed) next.drawerSections = ds;
  }
  return changed ? next : opp;
}

function scrubWinRoomBlockOnClosedMonitor(opp) {
  const sourcing = text(
    opp.hotelSourcingStatusLabel ||
      opp.targetHotelSourcingStatusLabel ||
      opp.hotelSourcingStatus ||
      opp.sourcingStatus
  );
  const closed =
    CLOSED_OR_MONITOR_SOURCING_RE.test(sourcing) ||
    /host\s+hotel\s+contracted/i.test(text(opp.whyHotelDemand)) ||
    /host\s+hotel\s+contracted/i.test(text(opp.hotelRelevanceThesis));
  if (!closed) return opp;
  const fields = [
    "recommendedAction",
    "suggestedAction",
    "customerRecommendedAction",
    "salesAction",
    "nextBestAction",
  ];
  let changed = false;
  const next = { ...opp };
  for (const f of fields) {
    const v = text(next[f]);
    if (!v || !WIN_ROOM_BLOCK_RE.test(v)) continue;
    next[f] =
      "Monitor — host / competitor lodging already contracted. Do not pursue as an open room-block win.";
    changed = true;
  }
  return changed ? next : opp;
}

function scrubStringFields(opp, scrubber) {
  const fields = [
    "title",
    "displayTitle",
    "drawerHeaderTitle",
    "opportunityName",
    "eventName",
    "eventDisplayName",
    "canonicalEventDisplayTitle",
    "whyHotelDemand",
    "hotelRelevanceThesis",
    "whyThisHotel",
    "potentialBusiness",
    "potentialBusinessSummary",
    "potentialBusinessForThisHotel",
    "potentialHotelBusiness",
    "commercialPotential",
    "whatIsTheOpportunity",
    "whyActNow",
    "whyNow",
    "urgencyReason",
    "recommendedAction",
    "suggestedAction",
    "customerRecommendedAction",
    "salesAction",
    "nextBestAction",
    "opportunitySummary",
    "summary",
    "description",
  ];
  let changed = false;
  const next = { ...opp };
  for (const f of fields) {
    if (next[f] == null) continue;
    const before = next[f];
    const after = scrubber(before, next);
    if (after !== before) {
      next[f] = after;
      changed = true;
    }
  }
  if (next.drawerSections && typeof next.drawerSections === "object") {
    const ds = { ...next.drawerSections };
    for (const [k, v] of Object.entries(ds)) {
      if (typeof v !== "string") continue;
      const after = scrubber(v, next);
      if (after !== v) {
        ds[k] = after;
        changed = true;
      }
    }
    if (changed) next.drawerSections = ds;
  }
  return changed ? next : opp;
}

/**
 * Apply shared semantic-commercial remediations to one opportunity presentation object.
 * Idempotent for already-clean rows.
 */
export function applySemanticCommercialRemediation(opp = {}) {
  if (!opp || typeof opp !== "object") return opp;
  let next = { ...opp };

  // 1) Evidence URL / source dedupe
  if (Array.isArray(next.evidenceSources)) {
    next.evidenceSources = dedupeEvidenceSources(next.evidenceSources);
  }
  if (Array.isArray(next.sources)) {
    next.sources = dedupeEvidenceSources(next.sources);
  }
  if (Array.isArray(next.evidenceUrls)) {
    next.evidenceUrls = dedupeEvidenceSources(next.evidenceUrls);
  }
  if (next.drawerSections && typeof next.drawerSections === "object") {
    const ds = { ...next.drawerSections };
    if (Array.isArray(ds.evidenceSources)) {
      ds.evidenceSources = dedupeEvidenceSources(ds.evidenceSources);
    }
    if (Array.isArray(ds.sources)) {
      ds.sources = dedupeEvidenceSources(ds.sources);
    }
    next.drawerSections = ds;
  }

  // 1b) Vague geography placeholder
  for (const f of [
    "eventLocationSummary",
    "location",
    "eventLocationCity",
    "eventsOpportunityOverview",
  ]) {
    if (f === "eventsOpportunityOverview" && next[f] && typeof next[f] === "object") {
      const loc = text(next[f].location);
      if (VAGUE_LOCATION_RE.test(loc)) {
        next[f] = { ...next[f], location: "Location to be confirmed" };
      }
      continue;
    }
    if (VAGUE_LOCATION_RE.test(text(next[f]))) {
      next[f] = "Location to be confirmed";
    }
  }

  // 2) Tournament-housing template leak (non-sports)
  next = scrubStringFields(next, scrubTournamentHousingLeak);

  // 2b) Truncated "sized group opportunity." lead
  next = scrubStringFields(next, (v) => scrubMalformedPotentialBusinessLead(v));

  // 2c) Sentence-case glitches + exhibitor placeholder roles + unpublished size signals
  next = scrubStringFields(next, (v) => scrubSentenceCaseAndPlaceholders(v));

  // 3) Pipeline slug / role-tag title cleanup (pre-title-contract)
  for (const f of [
    "title",
    "displayTitle",
    "drawerHeaderTitle",
    "opportunityName",
    "eventName",
    "eventDisplayName",
    "canonicalEventDisplayTitle",
  ]) {
    if (next[f] == null) continue;
    const cleaned = scrubPipelineSlugTitle(next[f]);
    if (cleaned !== text(next[f])) next[f] = cleaned;
  }

  // 4) Potential Business must not duplicate into Why Act Now
  next = clearDuplicatedWhyActNow(next);

  // 4b) Nested commercial intelligence brief uses the same fields
  for (const nestKey of ["commercialIntelligenceBrief", "salesBrief"]) {
    const nest = next[nestKey];
    if (!nest || typeof nest !== "object") continue;
    let nested = clearDuplicatedWhyActNow({
      ...nest,
      potentialBusiness:
        nest.potentialBusinessForThisHotel ||
        nest.potentialHotelBusiness ||
        nest.potentialBusiness,
      whyActNow: nest.whyActNow || nest.whyNow,
      whyNow: nest.whyNow || nest.whyActNow,
    });
    nested = alignUrgencyWithMonitorAction({
      ...nested,
      recommendedAction: next.recommendedAction || nest.recommendedAction,
    });
    const potBody = scrubMalformedPotentialBusinessLead(
      nest.potentialBusinessForThisHotel ||
        nest.potentialHotelBusiness ||
        nest.potentialBusiness
    );
    next[nestKey] = {
      ...nest,
      whyActNow: nested.whyActNow ?? nest.whyActNow,
      whyNow: nested.whyNow ?? nest.whyNow,
      potentialBusinessForThisHotel: potBody,
      potentialHotelBusiness: potBody,
      potentialBusiness: potBody,
    };
  }

  // 5) Closed / monitor sourcing must not say Win Room Block
  next = scrubWinRoomBlockOnClosedMonitor(next);

  // 6) Urgency copy must not conflict with monitor / watch actions
  next = alignUrgencyWithMonitorAction(next);

  // 7) Customer-facing room-demand enum scrub on overview fields
  for (const f of ["roomDemandLive", "roomDemandStatusLabel", "roomDemandStatus"]) {
    const v = text(next[f]);
    if (!v || !/^LODGING_DEMAND_/i.test(v)) continue;
    const map = {
      LODGING_DEMAND_CONFIRMED: "Lodging demand confirmed",
      LODGING_DEMAND_LIKELY: "Lodging demand likely (unconfirmed)",
      LODGING_DEMAND_UNKNOWN: "Room demand not yet published",
      LODGING_DEMAND_UNLIKELY: "Lodging demand unlikely",
      LODGING_DEMAND_NONE: "No lodging demand indicated",
    };
    next[f] = map[v.toUpperCase().replace(/[\s-]+/g, "_")] || "Room demand not yet published";
  }

  next.semanticCommercialRemediationV1 = true;
  next.semanticCommercialRemediationRevision =
    GDI_SEMANTIC_COMMERCIAL_REMEDIATION_REVISION;
  return next;
}

export const __test = {
  dedupeEvidenceSources,
  scrubTournamentHousingLeak,
  scrubPipelineSlugTitle,
  clearDuplicatedWhyActNow,
  alignUrgencyWithMonitorAction,
  scrubWinRoomBlockOnClosedMonitor,
  textsEqualLoose,
};
