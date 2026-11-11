/**
 * Canonical semantic contract for GDI ten-section drawer content.
 * Headings alone are insufficient — each section has a validated meaning.
 */
export const SEGMENT_SEMANTIC_CIB_CONTRACT_V1 =
  "gdi-segment-semantic-cib-contract-v1";

export const SEMANTIC_SECTION_CONTRACT = Object.freeze({
  opportunity_overview: Object.freeze({
    id: "opportunity_overview",
    meaning: "Identity line + concise segment-specific metadata only",
    forbid: Object.freeze([
      "long research paragraphs in identity",
      "duplicated organization name",
      "commercial proposal lists in metadata fields",
    ]),
  }),
  what_is_happening: Object.freeze({
    id: "what_is_happening",
    meaning: "Verified activity / programme narrative",
    forbid: Object.freeze(["generic industry profile", "hotel sales pitch"]),
  }),
  potential_business: Object.freeze({
    id: "potential_business",
    meaning: "Hotel-specific commercial demand thesis for this opportunity",
    forbid: Object.freeze(["activity chronology alone", "scaling lodging boilerplate for catering-only"]),
  }),
  why_this_hotel: Object.freeze({
    id: "why_this_hotel",
    meaning: "Evidence-grounded hotel advantages and limitations",
    forbid: Object.freeze([
      "generic same-market-only advantage when richer hotel evidence exists",
      "fit-scoring engine commentary",
    ]),
  }),
  why_act_now: Object.freeze({
    id: "why_act_now",
    meaning: "Timing trigger or honest monitoring rationale",
    forbid: Object.freeze([
      "full operational quotation checklists",
      "invented urgency",
    ]),
  }),
  facts_and_uncertainties: Object.freeze({
    id: "facts_and_uncertainties",
    meaning: "Source-backed facts vs unanswered commercial questions",
    forbid: Object.freeze(["source titles as facts", "raw extraction fragments"]),
  }),
  who_to_contact: Object.freeze({
    id: "who_to_contact",
    meaning: "Verified people and routes; authority not assumed",
    forbid: Object.freeze(["discarding named evidence without eligibility reason"]),
  }),
  recommended_next_step: Object.freeze({
    id: "recommended_next_step",
    meaning: "Segment-appropriate sales action + qualification questions",
    forbid: Object.freeze([
      "Identify who owns corporate travel",
      "visiting-staff lodging",
      "cross-segment Scaling fallback",
    ]),
  }),
  supporting_evidence: Object.freeze({
    id: "supporting_evidence",
    meaning: "Deduped evidence cards with provenance",
    forbid: Object.freeze(["duplicate URLs", "research dumps as rows"]),
  }),
  hotel_validation: Object.freeze({
    id: "hotel_validation",
    meaning: "Existing Hotel Validation / Action / Outcome component only",
    forbid: Object.freeze(["alternate feedback forms"]),
  }),
});

/** Forbidden Scaling corporate-travel fallbacks in non-Scaling segments. */
export const FORBIDDEN_CROSS_SEGMENT_NEXT_STEP_PATTERNS = Object.freeze([
  /Identify who owns corporate travel/i,
  /visiting-staff lodging/i,
  /preferred-hotel pattern/i,
  /corporate travel and visiting-staff/i,
]);

/** Association/Events congress-route boilerplate must not land on catering/gov/relocation. */
export const FORBIDDEN_ASSOCIATION_EVENTS_ROUTE_PATTERNS = Object.freeze([
  /meetings\s*\/\s*congress\s*\/\s*operations route/i,
  /association staff vs PCO vs housing/i,
  /Identify the meetings\s*\/\s*congress\s*\/\s*housing owner/i,
]);

export function looksGenericOrgEmail(email) {
  const e = text(email).toLowerCase();
  if (!e.includes("@")) return false;
  const local = e.split("@")[0] || "";
  return /^(info|contacto|contact|hello|hola|admin|office|ventas|sales|reception|recepcion|general|enquiries|inquiries|support)$/i.test(
    local,
  );
}

/**
 * Compress research prose titles into short customer-facing role labels.
 * Long evidence titles (>90 chars) previously triggered Association Events
 * buyer scrub as "webpage excerpt".
 */
export function normalizeContactDisplayTitle(rawTitle, whyRelevant = "") {
  const t = text(rawTitle);
  const why = text(whyRelevant);
  const blob = `${t} ${why}`;
  if (/directora\s+general/i.test(blob)) return "Directora General";
  if (/director\s+general/i.test(blob)) return "Director General";
  if (/\bceo\b/i.test(blob)) return "CEO";
  if (/\bpresidenta?\b/i.test(blob) && /usem|organiz/i.test(blob)) {
    return /presidenta/i.test(blob) ? "Presidenta" : "Presidente";
  }
  if (/confirmation|routing|rsvp|registro|event.?route/i.test(blob)) {
    return "Event Confirmation / Routing Contact";
  }
  if (!t) {
    return /leadership|organizat/i.test(why) ? "Organizational Leadership" : null;
  }
  if (
    t.length <= 80 &&
    !/identified in|unverified|podcast promotion|for the .+ monthly/i.test(t)
  ) {
    return t;
  }
  const first = text(t.split(/[;—–]/)[0] || "");
  if (first && first.length <= 80) return first;
  return (first || t).slice(0, 72) || "Named contact";
}

export function pickPersonEmail(contact = {}) {
  const field = text(contact.email);
  if (field && !looksGenericOrgEmail(field)) return field;
  const fromWhy = extractEmailFromText(contact.whyRelevant || contact.whyRelevantToHotelDemand || "");
  if (!fromWhy) return field || null;
  if (looksGenericOrgEmail(fromWhy)) return null;
  return fromWhy;
}

export function assertNoForbiddenAssociationEventsRoute(nextStep, segmentId) {
  if (
    !segmentId ||
    segmentId === "associations_conferences_events" ||
    /association|conference.?event/i.test(String(segmentId))
  ) {
    return [];
  }
  const t = text(nextStep);
  return FORBIDDEN_ASSOCIATION_EVENTS_ROUTE_PATTERNS.filter((re) => re.test(t)).map(
    (re) => re.source,
  );
}

export const FORBIDDEN_GENERIC_WHY_HOTEL_ONLY = Object.freeze([
  /^[^.]{0,80}sits in the same commercial market as the documented programme activity\.?$/i,
]);

export function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Short programme label — never a research essay.
 */
export function shortProgrammeIdentity(candidate = {}, deep = {}) {
  const fromCandidate = text(candidate.programmeName);
  if (fromCandidate && fromCandidate.length <= 90 && !/\.\s/.test(fromCandidate)) {
    return fromCandidate
      .replace(/\by\b/gi, "/")
      .replace(/\s+and\s+/gi, " / ");
  }
  const raw = text(deep.programmeIdentity || "");
  // Prefer text before first period if first clause is short
  const firstClause = raw.split(/[.;]/)[0] || "";
  const afterEmDash = firstClause.includes("—")
    ? text(firstClause.split("—").slice(1).join("—"))
    : firstClause;
  if (afterEmDash && afterEmDash.length <= 100) return afterEmDash;
  if (fromCandidate) return fromCandidate.slice(0, 90);
  return afterEmDash.slice(0, 90) || "Programme";
}

export function buildIdentityLine(org, programme) {
  const o = text(org) || "Organization";
  const p = text(programme) || "Programme";
  if (p.toLowerCase() === o.toLowerCase()) return o;
  if (p.toLowerCase().startsWith(o.toLowerCase())) return p;
  return `${o} — ${p}`;
}

export function extractEmailFromText(blob) {
  const m = String(blob || "").match(
    /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i,
  );
  return m ? m[0] : null;
}

export function looksLikeSourceTitleAsFact(fact) {
  const f = text(fact);
  if (!f) return true;
  if (/^https?:\/\//i.test(f)) return true;
  if (/^(Official|Published|September|LinkedIn|Source:)/i.test(f) && f.length < 60 && !/\b(lists|announced|confirms|displays)\b/i.test(f)) {
    return /claim|source|url/i.test(f);
  }
  return false;
}

export function assertNoForbiddenCrossSegmentNextStep(nextStep, segmentId) {
  if (
    !segmentId ||
    segmentId === "corporate_accounts_scaling" ||
    segmentId === "scaling_companies"
  ) {
    return [];
  }
  const t = text(nextStep);
  return FORBIDDEN_CROSS_SEGMENT_NEXT_STEP_PATTERNS.filter((re) => re.test(t)).map(
    (re) => re.source,
  );
}
