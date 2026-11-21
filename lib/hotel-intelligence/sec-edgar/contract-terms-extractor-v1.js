/**
 * Extract structured hotel contract terms from filing text / CMBS snippets.
 * Never invents values — uses NOT_DISCLOSED / NOT_VERIFIED.
 */

import {
  DISCLOSURE_CLASS,
} from "./contract-terms-schema-proposal-v4.js";

export const CONTRACT_TERMS_EXTRACTOR_V1 = "contract-terms-extractor-v1";

function clip(s, n = 280) {
  return String(s || "").replace(/\s+/g, " ").trim().slice(0, n);
}

/** Normalize SEC HTML artifacts: "20 th", &nbsp;, smart quotes. */
function normalizeFilingText(s) {
  return String(s || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#1[47][0-9];/g, '"')
    .replace(/[""]/g, '"')
    .replace(/(\d+)\s*(?:st|nd|rd|th)\b/gi, "$1")
    .replace(/\s+/g, " ")
    .trim();
}

const WORD_NUM = Object.freeze({
  one: 1,
  two: 2,
  three: 3,
  four: 4,
  five: 5,
  six: 6,
  seven: 7,
  eight: 8,
  nine: 9,
  ten: 10,
  eleven: 11,
  twelve: 12,
  fifteen: 15,
  twenty: 20,
  thirtieth: 30,
  twentieth: 20,
  tenth: 10,
  fifth: 5,
});

function parseWordOrNum(token) {
  const t = String(token || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
  if (WORD_NUM[t] != null) return WORD_NUM[t];
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

function evidence(field, value, excerpt, meta = {}) {
  return {
    field,
    value: value ?? null,
    disclosureClass: value == null ? DISCLOSURE_CLASS.NOT_DISCLOSED : DISCLOSURE_CLASS.EXPLICITLY_DISCLOSED,
    excerpt: excerpt ? clip(excerpt) : null,
    sourceUrl: meta.sourceUrl || null,
    accession: meta.accession || null,
    filingDate: meta.filingDate || null,
    confidence: value == null ? "UNVERIFIED" : meta.confidence || "MEDIUM",
    verificationStatus: value == null ? "NOT_VERIFIED" : "EVIDENCE_COLLECTED",
    isInference: false,
  };
}

/**
 * Regex extractors tuned for CMBS 424B franchise schedule + narrative franchise language.
 */
export function extractContractTermsFromText(text, meta = {}) {
  const t = normalizeFilingText(text);
  const ev = [];
  const terms = {
    originalTermYears: null,
    commencementYear: null,
    expirationYear: null,
    renewalOptionsCount: null,
    renewalOptionYearsEach: null,
    renewalMode: null,
    renewalNoticeDays: null,
    renewalExerciseParty: null,
    automaticRenewal: null,
    terminationForCause: null,
    terminationWithoutCause: null,
    performanceTest: null,
    changeOfControl: null,
    royaltyMention: null,
    managementFeeMention: null,
  };

  // Term length: "20-year franchise" / "initial term of 20 years" / "twentieth (20) anniversary"
  let m = t.match(
    /\b(?:initial\s+)?(?:franchise|management|license|operating)?\s*(?:agreement\s+)?(?:term|period)\s+(?:of\s+)?(\d{1,2})\s*(?:year|yr)s?\b/i,
  );
  if (!m) m = t.match(/\b(\d{1,2})\s*[-–]?\s*year\s+(?:franchise|management|license)\b/i);
  if (!m) {
    m = t.match(
      /\b(?:twentieth|tenth|fifteenth|twenty|ten|fifteen|\d{1,2})\s*(?:\(\s*(\d{1,2})\s*\))?\s*(?:st|nd|rd|th)?\s*anniversary\b/i,
    );
  }
  if (m) {
    const years = parseWordOrNum(m[2] || m[1]) || Number(m[1]);
    if (years) {
      terms.originalTermYears = years;
      ev.push(evidence("originalTermYears", terms.originalTermYears, m[0], meta));
    } else {
      ev.push(evidence("originalTermYears", null, null, meta));
    }
  } else {
    ev.push(evidence("originalTermYears", null, null, meta));
  }

  // Opening / franchise commencement year near hotel rows is often year-built — do not treat as contract commencement unless explicit
  m = t.match(
    /\b(?:commenc(?:ed|ement)|effective)\s+(?:date|as\s+of)?[:\s]+(?:on\s+)?(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\w+\s+\d{1,2},?\s+\d{4}|\d{4})\b/i,
  );
  if (m) {
    terms.commencementRaw = m[1];
    ev.push(evidence("commencementRaw", m[1], m[0], meta));
  } else {
    ev.push(evidence("commencementRaw", null, null, meta));
  }

  // Renewal options: "two (2) 10-year renewal options" / "up to two (2) additional renewal terms of five (5) years each"
  m = t.match(
    /\b(?:up\s+to\s+)?(\d+|one|two|three|four|five)\s*(?:\((\d+)\))?\s*(?:\d+\s*[-–]?\s*)?(?:year|yr)?\s*(?:renewal|extension)\s+options?\b/i,
  );
  if (!m) {
    m = t.match(
      /\b(?:up\s+to\s+)?(\d+|one|two|three|four|five)\s*(?:\((\d+)\))?\s*additional\s+renewal\s+terms?\b/i,
    );
  }
  if (!m) {
    m = t.match(
      /\b(\d+|one|two|three|four|five)\s*\(?\d*\)?\s*(?:ten|five|ten\-year|5\-year|10\-year)?\s*(?:year\s+)?renewal\s+options?\b/i,
    );
  }
  if (m) {
    terms.renewalOptionsCount = parseWordOrNum(m[2] || m[1]);
    ev.push(evidence("renewalOptionsCount", terms.renewalOptionsCount, m[0], meta));
  } else {
    ev.push(evidence("renewalOptionsCount", null, null, meta));
  }

  m = t.match(
    /\brenewal\s+terms?\s+of\s+(\d+|one|two|three|four|five|ten)\s*(?:\((\d+)\))?\s*years?\s+each\b/i,
  );
  if (!m) {
    m = t.match(
      /\b(?:each|per\s+option)?\s*(?:of\s+)?(\d{1,2})\s*[-–]?\s*year\s+(?:renewal|extension)\s+options?\b/i,
    );
  }
  if (!m) m = t.match(/\b(\d{1,2})\s*[-–]?\s*year\s+renewal\s+options?\b/i);
  if (m) {
    terms.renewalOptionYearsEach = parseWordOrNum(m[2] || m[1]);
    ev.push(evidence("renewalOptionYearsEach", terms.renewalOptionYearsEach, m[0], meta));
  } else {
    ev.push(evidence("renewalOptionYearsEach", null, null, meta));
  }

  if (/\bautomatic(?:ally)?\s+renew/i.test(t)) {
    terms.automaticRenewal = true;
    terms.renewalMode = "AUTOMATIC_CONDITIONAL";
    ev.push(evidence("renewalMode", terms.renewalMode, "automatic renew", meta));
  } else if (
    /\brenew(?:al)?(?:\s+options?)?\s+(?:at\s+)?(?:the\s+)?option\s+of\b/i.test(t) ||
    /\bat\s+the\s+option\s+of\s+the\s+franchisee\b/i.test(t) ||
    /\belect\s+to\s+renew\b/i.test(t) ||
    /\bright[^.]{0,60}to\s+(?:elect\s+to\s+)?renew\b/i.test(t)
  ) {
    terms.renewalMode = "ELECTIVE";
    ev.push(evidence("renewalMode", terms.renewalMode, "elective renewal", meta));
  } else {
    ev.push(evidence("renewalMode", null, null, meta));
  }

  m = t.match(
    /\b(?:not\s+less\s+than|at\s+least|no\s+later\s+than)\s+(\d{1,3}|ninety|sixty|thirty|one\s+hundred\s+eighty)\s*(?:\((\d{1,3})\))?\s*\(?\d*\)?\s*days?\s+(?:prior|before|notice|written\s+notice)\b/i,
  );
  if (!m) m = t.match(/\b(\d{1,3})\s+days?\s+(?:prior|advance)\s+notice\b/i);
  if (!m) {
    m = t.match(
      /\bat\s+least\s+(ninety|sixty|thirty|\d{1,3})\s*(?:\((\d{1,3})\))?\s*\(?\d*\)?\s*days?\b/i,
    );
  }
  if (m) {
    const wordDays = { ninety: 90, sixty: 60, thirty: 30 };
    const raw = String(m[1]).toLowerCase().replace(/\s+/g, " ");
    terms.renewalNoticeDays =
      wordDays[raw] || parseWordOrNum(m[2] || m[1]) || Number(m[2] || m[1]) || null;
    ev.push(evidence("renewalNoticeDays", terms.renewalNoticeDays, m[0], meta));
  } else {
    ev.push(evidence("renewalNoticeDays", null, null, meta));
  }

  if (/\bowner\s+shall\s+have\s+the\s+right[^.]{0,80}renew/i.test(t)) {
    terms.renewalExerciseParty = "OWNER";
    ev.push(evidence("renewalExerciseParty", terms.renewalExerciseParty, "owner right to renew", meta));
  } else if (/\bfranchisee\b[^.]{0,80}\brenew/i.test(t) || /\brenew[^.]{0,80}\bfranchisee\b/i.test(t)) {
    terms.renewalExerciseParty = "FRANCHISEE_OWNER";
    ev.push(evidence("renewalExerciseParty", terms.renewalExerciseParty, "franchisee renew", meta));
  } else if (/\bfranchisor\b[^.]{0,60}\bright\s+to\s+renew\b/i.test(t)) {
    terms.renewalExerciseParty = "FRANCHISOR";
    ev.push(evidence("renewalExerciseParty", terms.renewalExerciseParty, "franchisor right to renew", meta));
  } else {
    ev.push(evidence("renewalExerciseParty", null, null, meta));
  }

  terms.terminationForCause = /\bterminat(?:e|ion)\s+for\s+cause\b/i.test(t);
  ev.push(
    evidence(
      "terminationForCause",
      terms.terminationForCause || null,
      terms.terminationForCause ? "termination for cause" : null,
      meta,
    ),
  );

  terms.terminationWithoutCause = /\bterminat(?:e|ion)\s+without\s+cause\b/i.test(t);
  ev.push(
    evidence(
      "terminationWithoutCause",
      terms.terminationWithoutCause || null,
      terms.terminationWithoutCause ? "termination without cause" : null,
      meta,
    ),
  );

  terms.performanceTest =
    /\bperformance\s+(?:test|standard|threshold)\b/i.test(t) ||
    /\bfail(?:ure)?\s+to\s+(?:meet|satisfy)\s+(?:the\s+)?(?:performance|revpar|occupancy)\b/i.test(t);
  ev.push(
    evidence(
      "performanceTest",
      terms.performanceTest || null,
      terms.performanceTest ? "performance test" : null,
      meta,
    ),
  );

  terms.changeOfControl =
    /\bchange\s+of\s+control\b/i.test(t) || /\bsale\s+of\s+(?:the\s+)?(?:hotel|property)\b/i.test(t);
  ev.push(
    evidence(
      "changeOfControl",
      terms.changeOfControl || null,
      terms.changeOfControl ? "change of control / sale" : null,
      meta,
    ),
  );

  m = t.match(/\b(?:royalt(?:y|ies)|franchise\s+fee)\s+(?:of\s+)?(\d+(?:\.\d+)?)\s*%/i);
  if (m) {
    terms.royaltyMention = `${m[1]}%`;
    ev.push(evidence("royaltyMention", terms.royaltyMention, m[0], meta));
  } else {
    ev.push(evidence("royaltyMention", null, null, meta));
  }

  m = t.match(/\b(?:base\s+)?management\s+fee\s+(?:of\s+)?(\d+(?:\.\d+)?)\s*%/i);
  if (!m) {
    m = t.match(
      /\b(?:base\s+management\s+fee|management\s+fee)[^.]{0,80}(\d+(?:\.\d+)?)\s*(?:percent|%)/i,
    );
  }
  if (m) {
    terms.managementFeeMention = `${m[1]}%`;
    ev.push(evidence("managementFeeMention", terms.managementFeeMention, m[0], meta));
  } else {
    ev.push(evidence("managementFeeMention", null, null, meta));
  }

  const substantiveFields = new Set([
    "originalTermYears",
    "commencementRaw",
    "renewalOptionsCount",
    "renewalOptionYearsEach",
    "renewalMode",
    "renewalNoticeDays",
    "renewalExerciseParty",
    "royaltyMention",
    "managementFeeMention",
    "terminationForCause",
    "terminationWithoutCause",
    "performanceTest",
    "changeOfControl",
  ]);
  const disclosedCount = ev.filter(
    (e) => e.value != null && e.value !== false && substantiveFields.has(e.field),
  ).length;
  return {
    version: CONTRACT_TERMS_EXTRACTOR_V1,
    terms,
    evidence: ev,
    disclosedFieldCount: disclosedCount,
    totalFieldsProbed: ev.length,
  };
}

/**
 * Merge baseline expiration finding with extracted terms.
 */
export function enrichAgreementProfile(agreement, extraction, baseline = {}) {
  const terms = extraction?.terms || {};
  return {
    contractId: agreement.contractId,
    hotelName: agreement.hotelName,
    city: agreement.city,
    stateRegion: agreement.stateRegion,
    brandFamily: agreement.brandFamily,
    agreementType: agreement.agreementType,
    baseline: {
      scheduledExpiration: baseline.primaryDate || null,
      dateMeaning: baseline.dateMeaning || null,
      currentStatus: baseline.currentStatus || null,
      sourceUrl: (agreement.sourceUrls || [])[0] || null,
      accession: (agreement.sourceAccessions || [])[0] || null,
    },
    extracted: terms,
    evidence: extraction?.evidence || [],
    disclosedFieldCount: extraction?.disclosedFieldCount || 0,
    incrementalBeyondExpiration: Boolean(
      terms.originalTermYears != null ||
        terms.renewalOptionsCount != null ||
        terms.renewalNoticeDays != null ||
        terms.renewalMode != null ||
        terms.royaltyMention != null ||
        terms.managementFeeMention != null ||
        terms.terminationForCause ||
        terms.terminationWithoutCause ||
        terms.performanceTest ||
        terms.changeOfControl,
    ),
  };
}

export default {
  extractContractTermsFromText,
  enrichAgreementProfile,
};
