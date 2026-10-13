/**
 * Future-watch trigger policy — next research date (evidence-first, then heuristic).
 */

import {
  TRIGGER_TYPE,
  DATE_PROVENANCE,
  DEFAULT_HEURISTIC_POLICY,
  STOP_CONDITION,
  WATCH_STATUS,
} from "./constants.js";

function addDays(isoOrDate, days) {
  const d = new Date(isoOrDate);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function daysBetween(a, b) {
  const ms = new Date(b) - new Date(a);
  return Math.round(ms / 86400000);
}

/**
 * Try to extract an explicit open/publish date from page text or notes.
 * Returns { date: YYYY-MM-DD, condition } or null.
 */
export function extractEvidenceBasedDate(text = "", triggerType = TRIGGER_TYPE.HOUSING_OPEN) {
  const t = String(text || "");
  // Explicit ISO / YYYY-MM-DD near housing/registration language
  const patterns = [
    /(?:housing|accommodation|alojamiento|registration|inscripci[oó]n)\s+(?:opens?|abre|available|disponib\w*)\s*(?:on|el|:)?\s*(\d{4}-\d{2}-\d{2})/i,
    /(?:opens?|abre)\s+(?:on\s+)?(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})/i,
    /(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4}).{0,40}(?:housing|registration|accommodation)/i,
  ];
  const months = {
    january: 1,
    february: 2,
    march: 3,
    april: 4,
    may: 5,
    june: 6,
    july: 7,
    august: 8,
    september: 9,
    october: 10,
    november: 11,
    december: 12,
  };

  const iso = t.match(
    /(?:housing|accommodation|alojamiento|registration).{0,40}(\d{4}-\d{2}-\d{2})/i
  );
  if (iso) {
    return {
      date: iso[1],
      condition: `${triggerType} evidenced open date ${iso[1]}`,
      provenance: DATE_PROVENANCE.EVIDENCE_BASED,
    };
  }

  const m1 = t.match(
    /(?:opens?|abre)\s+(?:on\s+)?(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})/i
  );
  if (m1) {
    const mm = String(months[m1[2].toLowerCase()]).padStart(2, "0");
    const dd = String(m1[1]).padStart(2, "0");
    const date = `${m1[3]}-${mm}-${dd}`;
    return {
      date,
      condition: `${triggerType} evidenced open date ${date}`,
      provenance: DATE_PROVENANCE.EVIDENCE_BASED,
    };
  }

  // "90 days before" style
  const before = t.match(/(\d{1,3})\s*days?\s+before\s+(?:the\s+)?(?:event|conference|housing)/i);
  if (before) {
    return {
      daysBeforeEvent: Number(before[1]),
      condition: `${triggerType} opens ${before[1]} days before event (page evidence)`,
      provenance: DATE_PROVENANCE.EVIDENCE_BASED,
    };
  }

  return null;
}

/**
 * Parse a coarse event date from futureCycle / event string (YYYY or YYYY-MM).
 */
export function inferEventAnchorDate(candidate = {}) {
  const blob = `${candidate.futureCycle || ""} ${candidate.event || ""} ${candidate.dates || ""}`;
  const ymd = blob.match(/\b(202[6-9])-(\d{2})-(\d{2})\b/);
  if (ymd) return `${ymd[1]}-${ymd[2]}-${ymd[3]}`;
  const ym = blob.match(/\b(202[6-9])-(\d{2})\b/);
  if (ym) return `${ym[1]}-${ym[2]}-15`;
  const y = blob.match(/\b(202[6-9])\b/);
  if (y) return `${y[1]}-06-15`; // mid-year anchor — heuristic only
  return null;
}

/**
 * Derive next research window for a watch.
 */
export function deriveNextResearchSchedule({
  triggerType = TRIGGER_TYPE.HOUSING_OPEN,
  candidate = {},
  pageText = "",
  now = new Date(),
  policy = DEFAULT_HEURISTIC_POLICY,
  watchStatus = null,
} = {}) {
  const today = new Date(now).toISOString().slice(0, 10);
  const evidence = extractEvidenceBasedDate(pageText, triggerType);

  if (watchStatus === WATCH_STATUS.PUBLIC_DATA_CEILING || triggerType === "PUBLIC_DATA_CEILING") {
    const days = policy.PUBLIC_DATA_CEILING?.quarterlyDays || 90;
    const next = addDays(today, days);
    return {
      nextTriggerType: TRIGGER_TYPE.OTHER,
      nextTriggerCondition: "public_data_ceiling — source-change or quarterly only",
      nextResearchDate: next,
      researchWindowStart: next,
      researchWindowEnd: addDays(next, 14),
      dateProvenance: DATE_PROVENANCE.HEURISTIC,
      stopConditions: [STOP_CONDITION.PUBLIC_DATA_CEILING],
    };
  }

  if (evidence?.date) {
    return {
      nextTriggerType: triggerType,
      nextTriggerCondition: evidence.condition,
      nextResearchDate: evidence.date,
      researchWindowStart: addDays(evidence.date, -7),
      researchWindowEnd: addDays(evidence.date, 14),
      dateProvenance: DATE_PROVENANCE.EVIDENCE_BASED,
      stopConditions: [],
    };
  }

  const eventDate = inferEventAnchorDate(candidate);
  const cfg = policy[triggerType] || policy.DEFAULT;

  if (evidence?.daysBeforeEvent && eventDate) {
    const next = addDays(eventDate, -evidence.daysBeforeEvent);
    return {
      nextTriggerType: triggerType,
      nextTriggerCondition: evidence.condition,
      nextResearchDate: next < today ? today : next,
      researchWindowStart: addDays(eventDate, -(evidence.daysBeforeEvent + 14)),
      researchWindowEnd: addDays(eventDate, -(evidence.daysBeforeEvent - 14)),
      dateProvenance: DATE_PROVENANCE.EVIDENCE_BASED,
      stopConditions: [],
    };
  }

  if (eventDate && cfg.minDaysBefore != null) {
    const mid = Math.round((cfg.minDaysBefore + cfg.maxDaysBefore) / 2);
    const next = addDays(eventDate, -mid);
    return {
      nextTriggerType: triggerType,
      nextTriggerCondition: `heuristic ${mid}d before event (${eventDate}) — not sourced fact`,
      nextResearchDate: next < today ? today : next,
      researchWindowStart: addDays(eventDate, -cfg.maxDaysBefore),
      researchWindowEnd: addDays(eventDate, -cfg.minDaysBefore),
      dateProvenance: DATE_PROVENANCE.HEURISTIC,
      stopConditions: [],
      eventAnchorDate: eventDate,
    };
  }

  const fallback = cfg.fallbackDays || cfg.quarterlyDays || policy.DEFAULT.fallbackDays;
  const next = addDays(today, fallback);
  return {
    nextTriggerType: triggerType,
    nextTriggerCondition: `heuristic +${fallback}d from last check — not sourced fact`,
    nextResearchDate: next,
    researchWindowStart: next,
    researchWindowEnd: addDays(next, 14),
    dateProvenance: DATE_PROVENANCE.HEURISTIC,
    stopConditions: [],
  };
}

/**
 * Map primary blocker / prior outcome → canonical trigger.
 */
export function inferTriggerFromBlocker(primaryBlocker, priorNote = "") {
  const b = String(primaryBlocker || "");
  const n = String(priorNote || "");
  if (/WRONG_MARKET|DESTINATION/i.test(n)) return TRIGGER_TYPE.DESTINATION_CONFIRMED;
  if (/HOUSING|COMMERCIAL_OPENNESS|LODGING/i.test(b) || /housing/i.test(n)) {
    return TRIGGER_TYPE.HOUSING_OPEN;
  }
  if (/REGISTRATION/i.test(b)) return TRIGGER_TYPE.REGISTRATION_OPEN;
  if (/FUTURE_CYCLE/i.test(b)) return TRIGGER_TYPE.FUTURE_CYCLE_PUBLISHED;
  if (/DATE/i.test(b)) return TRIGGER_TYPE.DATES_CONFIRMED;
  if (/HOTEL_SELECTION/i.test(b)) return TRIGGER_TYPE.HOTEL_ANNOUNCED;
  if (/OVERFLOW/i.test(b)) return TRIGGER_TYPE.OVERFLOW_SIGNAL;
  if (/MARKET/i.test(b)) return TRIGGER_TYPE.DESTINATION_CONFIRMED;
  return TRIGGER_TYPE.OTHER;
}

export { addDays, daysBetween };
