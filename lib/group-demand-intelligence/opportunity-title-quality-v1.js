/**
 * GDI Opportunity Title Quality V1 — reusable validation + sanitization.
 *
 * Separates:
 *   OPPORTUNITY TITLE  — identifiable org/event/activity
 *   COMMERCIAL THESIS  — why the hotel might win business (not title)
 *   RESEARCH STATUS    — watch / TBD / pending (not title)
 *   NEXT ACTION        — sales step (not title)
 *
 * Blocks meeting credentials, nav chrome, and speculative hotel claims
 * from becoming customer-visible titles. Used by Bethesda 88 repair and
 * customer-surface / Full E2E finalization gates.
 */

export const GDI_OPPORTUNITY_TITLE_QUALITY_V1 = "gdi-opportunity-title-quality-v1";

export const TITLE_DEFECT = Object.freeze({
  MEETING_CREDENTIALS: "MEETING_CREDENTIALS",
  RAW_SOURCE_TEXT: "RAW_SOURCE_TEXT",
  SOURCE_NAV_CONTAMINATION: "SOURCE_NAV_CONTAMINATION",
  RESEARCH_STATUS_IN_TITLE: "RESEARCH_STATUS_IN_TITLE",
  UNCONFIRMED_DESTINATION: "UNCONFIRMED_DESTINATION",
  SPECULATIVE_OVERFLOW: "SPECULATIVE_OVERFLOW",
  INTERNAL_WORKFLOW: "INTERNAL_WORKFLOW",
  OVERLONG: "OVERLONG",
  GENERIC_PLACEHOLDER: "GENERIC_PLACEHOLDER",
  URL_OR_HTML: "URL_OR_HTML",
  TITLE_SOURCE_INVALID: "TITLE_SOURCE_INVALID",
});

const MAX_TITLE_LEN = 110;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/** Never echo meeting credentials into logs / reports. */
export function redactSensitiveTitle(title) {
  const t = text(title);
  if (!t) return t;
  if (
    /meeting\s*id/i.test(t) ||
    /pass\s*code|passcode/i.test(t) ||
    /if\s+prompted/i.test(t)
  ) {
    return "[REDACTED_JOINING_INSTRUCTION_TITLE]";
  }
  return t;
}

export function hasMeetingCredentialContamination(title) {
  const t = text(title);
  if (!t) return false;
  if (/if\s+prompted/i.test(t)) return true;
  if (/meeting\s*id/i.test(t) && /\d{2,}/.test(t)) return true;
  if (/pass\s*code|passcode/i.test(t)) return true;
  return false;
}

/**
 * Classify title defects. Does not invent replacements.
 */
export function classifyOpportunityTitleDefects(title, { organizationName = "" } = {}) {
  const t = text(title);
  const org = text(organizationName);
  const defects = [];
  if (!t) {
    defects.push(TITLE_DEFECT.TITLE_SOURCE_INVALID);
    return defects;
  }
  if (hasMeetingCredentialContamination(t)) {
    defects.push(TITLE_DEFECT.MEETING_CREDENTIALS);
    defects.push(TITLE_DEFECT.TITLE_SOURCE_INVALID);
  }
  if (/skip to main content|ncif conferences|browse\s*&\s*search|cookie (?:notice|policy)/i.test(t)) {
    defects.push(TITLE_DEFECT.SOURCE_NAV_CONTAMINATION);
  }
  if (/https?:\/\/|www\.|<html|href=/i.test(t)) {
    defects.push(TITLE_DEFECT.URL_OR_HTML);
  }
  if (/\|/.test(t) && /skip to|ncif|frederick/i.test(t)) {
    defects.push(TITLE_DEFECT.RAW_SOURCE_TEXT);
  }
  if (
    /destination TBD|location TBD|venue TBD|hotel\/venue not named|hotel TBA|not yet announced in title/i.test(
      t
    )
  ) {
    defects.push(TITLE_DEFECT.UNCONFIRMED_DESTINATION);
  }
  if (
    /monitoring for|research continuation|housing pending|hotel & travel open|needs confirmation|to be shared closer/i.test(
      t
    )
  ) {
    defects.push(TITLE_DEFECT.RESEARCH_STATUS_IN_TITLE);
  }
  if (
    /overflow lodging potential|vendor\/attendee overflow|recommended-hotel outreach|(?:\(|—)\s*overflow watch\b/i.test(
      t
    )
  ) {
    defects.push(TITLE_DEFECT.SPECULATIVE_OVERFLOW);
  }
  if (
    /disqualified|not a Bethesda Marriott fit|likely convention-scale;\s*not/i.test(t)
  ) {
    defects.push(TITLE_DEFECT.INTERNAL_WORKFLOW);
  }
  if (/Potential Business Opportunity|Strategic Opportunity|Partnership Prospect/i.test(t)) {
    defects.push(TITLE_DEFECT.GENERIC_PLACEHOLDER);
  }
  if (t.length > MAX_TITLE_LEN) {
    defects.push(TITLE_DEFECT.OVERLONG);
  }
  if (org && t === org) {
    defects.push(TITLE_DEFECT.GENERIC_PLACEHOLDER);
  }
  return [...new Set(defects)];
}

/**
 * Lightweight sanitizer for pipeline use — strips nav chrome and credentials.
 * Does not invent event identity when credentials wipe the title.
 */
export function sanitizeOpportunityTitle(title, { fallback = null, organizationName = "" } = {}) {
  let t = text(title);
  if (!t) return text(fallback) || text(organizationName) || null;

  if (hasMeetingCredentialContamination(t)) {
    return text(fallback) || null;
  }

  t = t.replace(/\s*\|\s*NCIF Conferences[\s\S]*$/i, "");
  t = t.replace(/Skip to main content[\s\S]*$/i, "");
  t = t.replace(/\s*NCI at Frederick[\s\S]*$/i, "");
  t = t.replace(/\s*CTN Dissemination Library[\s\S]*Browse[\s\S]*$/i, "");
  t = t.replace(/\s{2,}/g, " ").trim();

  // Strip common research-status / speculative clauses from em-dash tails.
  t = t.replace(
    /\s*[—–-]\s*(destination TBD|location TBD|venue TBD|hotel\/venue not named|housing pending|Hotel & Travel open|monitoring for 20\d{2} dates|campus event with vendor\/attendee overflow lodging potential|recommended-hotel outreach window)\s*$/i,
    ""
  );
  t = t.replace(/\s*\((?:overflow watch|medical overflow watch|convention-scale overflow watch)\)\s*$/i, "");
  t = t.replace(/\s*[—–-]\s*Bethesda North host \(overflow watch\)\s*$/i, " — Bethesda North Marriott");
  t = t.replace(/\s*[—–-]\s*Local meetings research continuation\s*$/i, " — Local meetings");
  t = t.replace(/\s*[—–-]\s*Civic meetings research continuation\s*$/i, " — Civic meetings & special events");
  t = t.replace(/\s*[—–-]\s*Community meetings research continuation\s*$/i, " — Community & rental events");
  t = t.replace(/\s*[—–-]\s*Corporate meetings research continuation\s*$/i, " — Corporate meetings");
  t = t.replace(/\s*\(2027 venue TBD\)\s*$/i, " 2027");
  t = t.replace(
    /\s*[—–-]\s*Washington, DC \(likely convention-scale; not a Bethesda Marriott fit\)\s*$/i,
    " — Washington, DC"
  );
  t = t.replace(/\s*[—–-]\s*already contracted at\s+/i, " — ");
  t = t.replace(/\s*[—–-]\s*Washington, DC area \(hotel\/venue not named\)\s*$/i, "");
  t = t.replace(/\s*[—–-]\s*Arlington, VA \(hotel\/venue not named\)\s*$/i, " — Arlington, VA");
  t = t.replace(/\s*[—–-]\s*Washington, DC \(hotel\/venue not named\)\s*$/i, " — Washington, DC");
  t = t.replace(/\s*[—–-]\s*Washington, DC \(convention-scale overflow watch\)\s*$/i, " — Washington, DC");
  t = t.replace(/\s*[—–-]\s*WEWCC \(medical overflow watch\)\s*$/i, " — Walter E. Washington Convention Center");
  t = t.replace(/\s*[—–-]\s*WEWCC \+ Marriott Marquis \(overflow watch\)\s*$/i, " — WEWCC + Marriott Marquis");
  t = t.replace(/\s*[—–-]\s*Bethesda, MD \(housing pending\)\s*$/i, " — Bethesda, MD");
  t = t.replace(/\s*[—–-]\s*SoccerPlex stay-to-play \(overflow watch\)\s*$/i, " — SoccerPlex stay-to-play");
  t = t.replace(/\s*[—–-]\s*Overflow lodging research\s*$/i, "");
  t = t.replace(/\s+location TBD\s*\(([^)]+)\)\s*$/i, " ($1)");
  t = t.replace(/\s*\(destination TBD\)\s*$/i, "");
  t = t.replace(/\s*[—–-]\s*destination TBD\s*$/i, "");

  t = t.replace(/\s{2,}/g, " ").trim();
  if (t.length > MAX_TITLE_LEN) t = t.slice(0, MAX_TITLE_LEN - 1).replace(/\s+\S*$/, "").trim();

  if (!t || t.length < 6) return text(fallback) || text(organizationName) || null;
  return t;
}

/**
 * Customer-safe display title — never returns credential-bearing strings.
 */
export function customerSafeOpportunityTitle(row = {}) {
  const raw = text(row.title || row.displayTitle || row.eventName || "");
  if (hasMeetingCredentialContamination(raw) || /TITLE_SOURCE_INVALID/i.test(raw)) {
    const org = text(row.organizationName);
    return org
      ? `${org} — title source invalid (research continuation)`
      : "Untitled opportunity — title source invalid";
  }
  const cleaned = sanitizeOpportunityTitle(raw, {
    fallback: row.eventName || row.seriesName || null,
    organizationName: row.organizationName,
  });
  return cleaned || text(row.organizationName) || "Untitled opportunity";
}

/**
 * Gate: should this title block customer publication until repaired?
 */
export function titleBlocksCustomerPublication(title) {
  const defects = classifyOpportunityTitleDefects(title);
  return defects.some((d) =>
    [
      TITLE_DEFECT.MEETING_CREDENTIALS,
      TITLE_DEFECT.TITLE_SOURCE_INVALID,
      TITLE_DEFECT.RAW_SOURCE_TEXT,
      TITLE_DEFECT.SOURCE_NAV_CONTAMINATION,
      TITLE_DEFECT.URL_OR_HTML,
    ].includes(d)
  );
}

export function assessOpportunityTitle(row = {}) {
  const title = text(row.title || row.displayTitle || "");
  const defects = classifyOpportunityTitleDefects(title, {
    organizationName: row.organizationName,
  });
  const proposed = sanitizeOpportunityTitle(title, {
    fallback: row.eventName || null,
    organizationName: row.organizationName,
  });
  return {
    id: text(row.id || row.opportunityId) || null,
    beforeTitle: redactSensitiveTitle(title),
    defects,
    isDefective: defects.length > 0,
    blocksCustomerPublication: titleBlocksCustomerPublication(title),
    proposedTitle: proposed ? redactSensitiveTitle(proposed) : null,
    customerSafeTitle: customerSafeOpportunityTitle(row),
  };
}
