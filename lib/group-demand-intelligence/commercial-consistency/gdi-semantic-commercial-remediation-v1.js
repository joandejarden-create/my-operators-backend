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
export const GDI_SEMANTIC_COMMERCIAL_REMEDIATION_REVISION = "v1.1";

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

function opportunityTargetYear(opp = {}) {
  const blob = [
    opp.displayTitle,
    opp.title,
    opp.eventName,
    opp.eventDisplayName,
    opp.canonicalEventDisplayTitle,
  ]
    .map(text)
    .join(" ");
  const years = [...blob.matchAll(/\b(20\d{2})\b/g)].map((m) => Number(m[1]));
  if (!years.length) return null;
  return Math.max(...years);
}

function evidenceUrlYear(url) {
  const s = text(url);
  if (!s) return null;
  // Path/year patterns like /msc-2009/ or /en/2009/agenda — not query params.
  const m = s.match(/\/(?:msc[-_])?(19\d{2}|20\d{2})(?:\/|$)/i);
  return m ? Number(m[1]) : null;
}

/**
 * Drop clearly historical edition URLs from customer-facing evidence when the
 * opportunity targets a much later year (presentation hygiene only).
 */
function filterStaleHistoricalEvidence(list, opp) {
  if (!Array.isArray(list) || !list.length) return list;
  const target = opportunityTargetYear(opp);
  if (!target || target < 2020) return list;
  const filtered = list.filter((item) => {
    const url =
      typeof item === "string"
        ? item
        : text(item?.url || item?.href || item?.link || item?.sourceUrl);
    const y = evidenceUrlYear(url);
    if (y == null) return true;
    // Keep recent edition pages; drop decade-old archives for a future edition.
    return y >= target - 3;
  });
  // Never wipe the evidence list entirely — keep original if filter emptied it.
  return filtered.length ? filtered : list;
}

function isStaleHistoricalUrl(url, opp) {
  const y = evidenceUrlYear(url);
  const target = opportunityTargetYear(opp);
  return y != null && target != null && y < target - 3;
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

const HARD_DEMAND_CLAIM_RE =
  /\b(?:attracts?\s+over\s+\d+|large\s+number\s+of\s+attendees|significant\s+(?:room\s+)?demand|draw\s+a\s+large\s+number|creating\s+(?:significant\s+)?demand\s+for\s+hotel|expected\s+to\s+draw\s+a\s+large|typically\s+draws\s+large\s+crowds|necessitating\s+hotel\s+accommodations\s+for\s+participants)\b/i;
const PLACEHOLDER_CONTACT_NAME_RE =
  /^(?:Booth\s+Booth|Exhibitor\s+booth\s+contact|Other\s+contact|N\/?A|TBD|Unknown|Placeholder|Contact\s+TBD)$/i;
const UNSUPPORTED_TREND_URGENCY_RE =
  /\b(?:increasing\s+trend|makes\s+this\s+an\s+opportune\s+time|allowing\s+for\s+advance\s+planning|opportune\s+time\s+for\s+hotels|advance\s+planning\s+is\s+crucial)\b/i;
const MONITOR_SAFE_TIMING =
  "Timing is watch-only until a confirmed lodging path or open sourcing signal appears — do not treat potential demand as an active chase.";

function isPlaceholderContactName(name) {
  return PLACEHOLDER_CONTACT_NAME_RE.test(text(name));
}

function demotePlaceholderContact(contact) {
  if (!contact || typeof contact !== "object") return contact;
  const name = text(contact.name);
  const role = text(
    contact.role || contact.contactRole || contact.title || contact.whyRelevant
  );
  const placeholderName = isPlaceholderContactName(name) || /^Booth(\s+Booth)?$/i.test(name);
  if (!placeholderName && !/^OTHER_RELEVANT$/i.test(role)) {
    if (/OTHER_RELEVANT/i.test(role)) {
      return {
        ...contact,
        role: "Other contact",
        contactRole: "Other contact",
        title:
          contact.title && /OTHER_RELEVANT/i.test(String(contact.title))
            ? "Other contact"
            : contact.title,
      };
    }
    return contact;
  }
  if (!placeholderName) {
    return {
      ...contact,
      role: "Other contact",
      contactRole: "Other contact",
      title:
        contact.title && /OTHER_RELEVANT/i.test(String(contact.title))
          ? "Other contact"
          : contact.title,
    };
  }
  return {
    ...contact,
    name: null,
    customerFacingNamed: false,
    contactQuality: "RESEARCH_GAP",
    contactQualityLabel: "Contact research required",
    contactPathClass: "RESEARCH_GAP",
    role: null,
    contactRole: null,
    title: null,
    whyThisContact: null,
    whyRelevant: null,
    researchGapCopy:
      "Named exhibitor / programme contact not yet established — use the organizer contact page.",
  };
}

function scrubPlaceholderContacts(opp) {
  let changed = false;
  const next = { ...opp };
  if (next.primaryContact && typeof next.primaryContact === "object") {
    const demoted = demotePlaceholderContact(next.primaryContact);
    if (demoted !== next.primaryContact) {
      next.primaryContact = demoted;
      changed = true;
    }
    if (demoted && demoted.customerFacingNamed === false) {
      next.contactPathClass = "RESEARCH_GAP";
      next.contactName = null;
      next.buyerName = null;
      changed = true;
    }
  }
  if (isPlaceholderContactName(next.contactName) || /^Booth(\s+Booth)?$/i.test(text(next.contactName))) {
    next.contactName = null;
    next.contactPathClass = "RESEARCH_GAP";
    changed = true;
  }
  if (Array.isArray(next.contacts) && next.contacts.length) {
    next.contacts = next.contacts.map((c) => demotePlaceholderContact(c));
    changed = true;
  }
  if (Array.isArray(next.backupContacts) && next.backupContacts.length) {
    next.backupContacts = next.backupContacts.map((c) => demotePlaceholderContact(c));
    changed = true;
  }
  if (/^OTHER_RELEVANT$/i.test(text(next.contactRole))) {
    next.contactRole = "Other contact";
    changed = true;
  }
  return changed ? next : opp;
}

function attendanceUnpublished(opp) {
  const overview =
    opp.eventsOpportunityOverview && typeof opp.eventsOpportunityOverview === "object"
      ? opp.eventsOpportunityOverview
      : {};
  const attendance = text(
    overview.attendance ||
      overview.attendanceLabel ||
      opp.attendanceLabel ||
      opp.attendance ||
      opp.expectedAttendance
  );
  if (/not yet published|unpublished|unknown|not published|^$/.test(attendance)) {
    return true;
  }
  const peak = text(
    overview.peakRooms ||
      overview.peakRoomDemand ||
      opp.peakRooms ||
      opp.peakRoomDemand ||
      opp.sizeSignal
  );
  return !peak || /not yet|unpublished|unknown|^0$/.test(peak);
}

function scrubUnpublishedAttendanceClaims(opp) {
  if (!attendanceUnpublished(opp)) return opp;
  const soft =
    "Event scale is not yet published — hotel demand remains a hypothesis until attendance or lodging signals are confirmed.";
  const fields = [
    "whyThisHotel",
    "whyHotelDemand",
    "hotelOpportunityThesis",
    "hotelRelevanceThesis",
    "summaryWhyHotel",
  ];
  let changed = false;
  const next = { ...opp };
  for (const f of fields) {
    const v = text(next[f]);
    if (!v || !HARD_DEMAND_CLAIM_RE.test(v)) continue;
    next[f] = soft;
    changed = true;
  }
  for (const listKey of ["whyThisHotelAdvantages", "whyThisHotelLimitations"]) {
    if (!Array.isArray(next[listKey])) continue;
    const cleaned = next[listKey]
      .map((t) => text(t).replace(/\s{2,}/g, " ").trim())
      .filter((t) => t && !HARD_DEMAND_CLAIM_RE.test(t));
    if (cleaned.length !== next[listKey].length) {
      next[listKey] = cleaned;
      changed = true;
    }
  }
  return changed ? next : opp;
}

function scrubUnsupportedTrendUrgency(opp) {
  const fields = [
    "potentialBusiness",
    "potentialBusinessForThisHotel",
    "potentialHotelBusiness",
    "commercialPotential",
    "whyActNow",
    "whyNow",
  ];
  let changed = false;
  const next = { ...opp };
  for (const f of fields) {
    const v = text(next[f]);
    if (!v || !UNSUPPORTED_TREND_URGENCY_RE.test(v)) continue;
    next[f] = MONITOR_SAFE_TIMING;
    changed = true;
  }
  if (next.drawerSections && typeof next.drawerSections === "object") {
    const ds = { ...next.drawerSections };
    for (const f of ["whyNow", "whyActNow", "potentialBusiness"]) {
      const v = text(ds[f]);
      if (v && UNSUPPORTED_TREND_URGENCY_RE.test(v)) {
        ds[f] = MONITOR_SAFE_TIMING;
        changed = true;
      }
    }
    if (changed) next.drawerSections = ds;
  }
  return changed ? next : opp;
}

function scrubPastCycleUrgentWhyHotel(opp) {
  const action = text(
    opp.recommendedAction || opp.suggestedAction || opp.customerRecommendedAction
  );
  const priority = text(opp.priority || opp.cardPriority || opp.customerFacingState);
  const pastOrClosed =
    /disqualified/i.test(priority) ||
    /Past cycle\s*[—–-]\s*do not pursue/i.test(action) ||
    /wrong[- ]destination/i.test(action) ||
    /Close as wrong-destination/i.test(action);
  if (!pastOrClosed) return opp;
  const monitorFit =
    "Property fit is relationship / next-cycle context only — do not treat current-cycle attendance or room-demand claims as an active chase for this hotel.";
  const fields = [
    "whyThisHotel",
    "whyHotelDemand",
    "hotelOpportunityThesis",
    "hotelRelevanceThesis",
    "summaryWhyHotel",
  ];
  let changed = false;
  const next = { ...opp };
  for (const f of fields) {
    const v = text(next[f]);
    if (!v || !HARD_DEMAND_CLAIM_RE.test(v)) continue;
    next[f] = monitorFit;
    changed = true;
  }
  // Advantage / limitation lists that smuggle unpublished size signals
  for (const listKey of ["whyThisHotelAdvantages", "whyThisHotelLimitations"]) {
    if (!Array.isArray(next[listKey])) continue;
    const cleaned = next[listKey]
      .map((t) =>
        text(t)
          .replace(/\bSize signal:\s*\d+\s*peak rooms\.?/gi, "")
          .replace(/\s{2,}/g, " ")
          .trim()
      )
      .filter((t) => t && !HARD_DEMAND_CLAIM_RE.test(t));
    if (cleaned.length !== next[listKey].length) {
      next[listKey] = cleaned;
      changed = true;
    }
  }
  if (next.commercialIntelligenceBrief && typeof next.commercialIntelligenceBrief === "object") {
    const cib = { ...next.commercialIntelligenceBrief };
    for (const f of ["whyThisHotel", "whyHotelDemand", "hotelOpportunityThesis"]) {
      const v = text(cib[f]);
      if (v && HARD_DEMAND_CLAIM_RE.test(v)) {
        cib[f] = monitorFit;
        changed = true;
      }
    }
    if (Array.isArray(cib.whyThisHotelAdvantages)) {
      cib.whyThisHotelAdvantages = cib.whyThisHotelAdvantages
        .map((t) => text(t).replace(/\bSize signal:\s*\d+\s*peak rooms\.?/gi, "").trim())
        .filter((t) => t && !HARD_DEMAND_CLAIM_RE.test(t));
    }
    next.commercialIntelligenceBrief = cib;
  }
  return changed ? next : opp;
}

function alignUrgencyWithMonitorAction(opp) {
  const action = text(
    opp.recommendedAction || opp.suggestedAction || opp.customerRecommendedAction
  );
  const disqualified =
    /disqualified/i.test(text(opp.priority || opp.cardPriority || opp.customerFacingState)) ||
    /Past cycle\s*[—–-]\s*do not pursue/i.test(action) ||
    /wrong[- ]destination|Close as wrong-destination/i.test(action);
  if ((!action || !MONITOR_ACTION_RE.test(action)) && !disqualified) return opp;
  const fields = ["whyActNow", "whyNow", "urgencyReason", "urgencyCopy"];
  let changed = false;
  const next = { ...opp };
  for (const f of fields) {
    const v = text(next[f]);
    if (!v) continue;
    if (
      !URGENCY_NOW_RE.test(v) &&
      !/pursue|win room|book the block|advance planning.*crucial|securing hotel bookings/i.test(v)
    ) {
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
      if (
        URGENCY_NOW_RE.test(v) ||
        /pursue|win room|book the block|advance planning.*crucial/i.test(v)
      ) {
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

  // 1) Evidence URL / source dedupe + drop decade-stale edition archives
  if (Array.isArray(next.evidenceSources)) {
    next.evidenceSources = filterStaleHistoricalEvidence(
      dedupeEvidenceSources(next.evidenceSources),
      next
    );
  }
  if (Array.isArray(next.sources)) {
    next.sources = filterStaleHistoricalEvidence(dedupeEvidenceSources(next.sources), next);
  }
  if (Array.isArray(next.evidenceUrls)) {
    next.evidenceUrls = filterStaleHistoricalEvidence(
      dedupeEvidenceSources(next.evidenceUrls),
      next
    );
  }
  if (Array.isArray(next.evidence)) {
    next.evidence = filterStaleHistoricalEvidence(next.evidence, next);
  }
  for (const urlField of [
    "officialSource",
    "locationEvidenceUrl",
    "commercialEvidenceUrl",
    "eventDateEvidenceUrl",
    "attendanceEvidenceUrl",
    "peakRoomsEvidenceUrl",
  ]) {
    const y = evidenceUrlYear(next[urlField]);
    const target = opportunityTargetYear(next);
    if (y != null && target && y < target - 3) {
      // Prefer a remaining fresh evidence URL when available.
      const fresh =
        (Array.isArray(next.evidenceSources) &&
          next.evidenceSources
            .map((e) => (typeof e === "string" ? e : text(e?.url)))
            .find(Boolean)) ||
        (Array.isArray(next.sources) &&
          next.sources.map((e) => (typeof e === "string" ? e : text(e?.url))).find(Boolean)) ||
        null;
      next[urlField] = fresh || null;
    }
  }
  if (next.sourceObject && typeof next.sourceObject === "object") {
    const y = evidenceUrlYear(next.sourceObject.sourceUrl);
    const target = opportunityTargetYear(next);
    if (y != null && target && y < target - 3) {
      const fresh =
        (Array.isArray(next.evidenceSources) &&
          next.evidenceSources
            .map((e) => (typeof e === "string" ? e : text(e?.url)))
            .find(Boolean)) ||
        null;
      next.sourceObject = {
        ...next.sourceObject,
        sourceUrl: fresh || null,
        sourceTitle: fresh ? next.sourceObject.sourceTitle : null,
      };
    }
  }
  if (next.drawerSections && typeof next.drawerSections === "object") {
    const ds = { ...next.drawerSections };
    if (Array.isArray(ds.evidenceSources)) {
      ds.evidenceSources = filterStaleHistoricalEvidence(
        dedupeEvidenceSources(ds.evidenceSources),
        next
      );
    }
    if (Array.isArray(ds.sources)) {
      ds.sources = filterStaleHistoricalEvidence(dedupeEvidenceSources(ds.sources), next);
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

  // 6b) Past-cycle / DISQUALIFIED must not keep hard attendance demand claims in Why This Hotel
  next = scrubPastCycleUrgentWhyHotel(next);

  // 6b2) Unpublished attendance must not keep hard crowd / room-demand claims
  next = scrubUnpublishedAttendanceClaims(next);

  // 6b3) Demote Booth Booth / OTHER_RELEVANT placeholder exhibitor contacts
  next = scrubPlaceholderContacts(next);

  // 6b4) Strip unsupported market-trend / advance-planning urgency phrasing
  next = scrubUnsupportedTrendUrgency(next);

  // 6c) Soften potential-business urgency on past-cycle / wrong-destination rows
  {
    const action = text(next.recommendedAction || next.suggestedAction);
    const priority = text(next.priority || next.cardPriority);
    if (
      /disqualified/i.test(priority) ||
      /Past cycle\s*[—–-]\s*do not pursue/i.test(action) ||
      /wrong[- ]destination|Close as wrong-destination/i.test(action)
    ) {
      for (const f of [
        "potentialBusiness",
        "potentialBusinessForThisHotel",
        "potentialHotelBusiness",
        "commercialPotential",
      ]) {
        const v = text(next[f]);
        if (
          v &&
          /advance planning.*crucial|securing hotel bookings|act now|opportune time/i.test(v)
        ) {
          next[f] =
            "Future-cycle / relationship context only — do not treat current packaging as an open lodging chase.";
        }
      }
    }
  }

  // 6d) Collapse implausible month-long timing labels to month+year when span ≥ 20 days
  // and start day is the 1st (common research placeholder packaging).
  for (const f of ["timing", "cardTiming", "eventTimingLabel", "timingLabel"]) {
    const v = text(next[f]);
    const m = v.match(/(\d{4}-\d{2}-\d{2})\s*[→–-]\s*(\d{4}-\d{2}-\d{2})/);
    if (!m) continue;
    const start = m[1];
    const end = m[2];
    const days = (new Date(end) - new Date(start)) / 86400000;
    if (days >= 20 && /-\d{2}-01$/.test(start)) {
      const [y, mo] = start.split("-");
      const months = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
      ];
      next[f] = `${months[Number(mo) - 1]} ${y} (exact dates TBD)`;
    }
  }

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

  // 8) Ensure What-is-happening is never a bare EXHIBITOR/VENDOR BLOCK title stub
  {
    const what = text(
      next.whatIsHappening || next.summaryWhat || next.tileCustomerSummary
    );
    const title = text(next.displayTitle || next.title || next.eventName);
    const isBlockStub =
      /—\s*(EXHIBITOR|VENDOR|SPONSOR|PARTNER)\s+BLOCK\s*$/i.test(what) ||
      /—\s*(EXHIBITOR|VENDOR|SPONSOR|PARTNER)\s+BLOCK\s*$/i.test(title) ||
      (what && title && what.toLowerCase() === title.toLowerCase() && what.length < 80);
    if (!what || what.length < 80 || isBlockStub) {
      const org =
        text(
          next.displayCompanyName ||
            next.organizationName ||
            next.companyName ||
            next.eventsOpportunityOverview?.organizerName
        ) || "the organizer";
      const event =
        text(next.eventDisplayName || next.eventName || next.displayTitle || next.title) ||
        "this programme";
      const narrative = `${event.replace(
        /\s*[—–-]\s*(EXHIBITOR|VENDOR|SPONSOR|PARTNER)\s+BLOCK\s*$/i,
        ""
      ).trim()} is tracked as an exhibitor / vendor programme contact for ${org}; confirm edition status, destination, and lodging accessibility before treating hotel demand as confirmed.`;
      next.whatIsHappening = narrative;
      if (!text(next.summaryWhat) || isBlockStub) next.summaryWhat = narrative;
      if (!text(next.tileCustomerSummary) || isBlockStub) {
        next.tileCustomerSummary = narrative;
      }
    }
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
