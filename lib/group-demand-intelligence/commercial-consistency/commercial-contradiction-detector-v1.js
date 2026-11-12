/**
 * Deterministic commercial contradiction detector for GDI opportunities.
 * Severity: CRITICAL (false booked/open RFP) > HIGH > MEDIUM > LOW (format).
 */

import {
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
  normalizeTargetHotelSourcingFromLegacy,
} from "./commercial-status-dimensions-v1.js";
import { getCmiEvidenceEntry } from "./cmi-evidence-ledger-v1.js";

export const CONTRADICTION_CODE = Object.freeze({
  CONFIRMED_COMPETITOR_BUT_UNCONFIRMED_TARGET_SOURCE_WITH_AMBIGUOUS_LABEL:
    "CONFIRMED_COMPETITOR_BUT_UNCONFIRMED_TARGET_SOURCE_WITH_AMBIGUOUS_LABEL",
  OPEN_RFP_WITHOUT_SOURCING_EVIDENCE: "OPEN_RFP_WITHOUT_SOURCING_EVIDENCE",
  OVERFLOW_CLAIM_WITHOUT_OVERFLOW_EVIDENCE:
    "OVERFLOW_CLAIM_WITHOUT_OVERFLOW_EVIDENCE",
  COMPETITOR_VENUE_SOURCE_MISMATCH: "COMPETITOR_VENUE_SOURCE_MISMATCH",
  CONTRACTED_BLOCK_WITHOUT_BLOCK_EVIDENCE:
    "CONTRACTED_BLOCK_WITHOUT_BLOCK_EVIDENCE",
  ATTENDANCE_MISSING_WHEN_SOURCE_AVAILABLE:
    "ATTENDANCE_MISSING_WHEN_SOURCE_AVAILABLE",
  FUTURE_EVENT_UNCONFIRMED_BUT_PRESENTED_AS_BOOKED:
    "FUTURE_EVENT_UNCONFIRMED_BUT_PRESENTED_AS_BOOKED",
  CONTACT_ROLE_EXCEEDS_VERIFIED_REMIT: "CONTACT_ROLE_EXCEEDS_VERIFIED_REMIT",
  SALES_ACTION_EXCEEDS_COMMERCIAL_READINESS:
    "SALES_ACTION_EXCEEDS_COMMERCIAL_READINESS",
  SOURCE_CONTRADICTS_OPPORTUNITY_NARRATIVE:
    "SOURCE_CONTRADICTS_OPPORTUNITY_NARRATIVE",
  DUPLICATE_ACCOUNT_EVENT_CYCLE: "DUPLICATE_ACCOUNT_EVENT_CYCLE",
  CMI_VERIFICATION_UNSUPPORTED: "CMI_VERIFICATION_UNSUPPORTED",
});

export const CONTRADICTION_SEVERITY = Object.freeze({
  CRITICAL: "CRITICAL",
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function finding(code, severity, message, evidence = {}) {
  return { code, severity, message, evidence };
}

/**
 * Detect contradictions on a single opportunity (after optional remediation).
 */
export function detectOpportunityContradictions(opp = {}, opts = {}) {
  const findings = [];
  const id = String(opp.id || opp.opportunityId || "");
  const ledger = getCmiEvidenceEntry(id);
  // Human narrative only. Exclude status/type labels (e.g. "Overflow Possible",
  // "Overflow / Housing") — those are classification chips, not claims that the
  // target hotel has an open lodging path.
  const blob = [
    opp.title,
    opp.whyNow,
    opp.summaryWhat,
    opp.summaryWhyMatters,
    opp.summaryWhyHotel,
    opp.recommendedAction,
    opp.suggestedAction,
    opp.lodgingEvidence,
    opp.fitExplanation,
  ]
    .map(text)
    .join(" | ");

  const sourcing = normalizeTargetHotelSourcingFromLegacy(opp);
  const competitorHotel = text(
    opp.competitorHotel || opp.competitorVenue || opp.likelyCompetitor
  );
  const action = text(opp.recommendedAction || opp.suggestedAction);
  const attendance = text(
    opp.advertisedAttendance || opp.attendance || opp.estimatedAttendance
  );

  // Ambiguous affirmative open-path language while competitor confirmed /
  // target sourcing unconfirmed. Proper disclaimers ("overflow unconfirmed",
  // "sourcing not established", "do not request an RFP") are not contradictions.
  const properlyDisclaimed =
    /overflow[\s\S]{0,48}(unconfirmed|not established|not evidenced|hypothes)/i.test(
      blob
    ) ||
    /sourcing (is |remains )?(not established|unconfirmed|unknown)/i.test(blob) ||
    /do not request an rfp/i.test(blob) ||
    /lodging demand path not established/i.test(blob);
  const impliesOpenPath =
    /lodging demand path open|housing open(?!\s*announcement)|overflow possible(?![\s\S]{0,40}(unconfirmed|not established))|open lodging path/i.test(
      blob
    );
  if (
    competitorHotel &&
    (sourcing === TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED ||
      sourcing === TARGET_HOTEL_SOURCING_STATUS.UNKNOWN) &&
    impliesOpenPath &&
    !properlyDisclaimed
  ) {
    findings.push(
      finding(
        CONTRADICTION_CODE.CONFIRMED_COMPETITOR_BUT_UNCONFIRMED_TARGET_SOURCE_WITH_AMBIGUOUS_LABEL,
        CONTRADICTION_SEVERITY.HIGH,
        "Competitor named while target sourcing unconfirmed, but copy still implies open lodging path.",
        { competitorHotel, sourcing }
      )
    );
  }

  if (
    /request rfp|send.*rfp|rfp\/site visit/i.test(action) &&
    sourcing !== TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED &&
    sourcing !== TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED
  ) {
    findings.push(
      finding(
        CONTRADICTION_CODE.OPEN_RFP_WITHOUT_SOURCING_EVIDENCE,
        CONTRADICTION_SEVERITY.CRITICAL,
        "Recommended action requests RFP without confirmed open/overflow sourcing.",
        { action: action.slice(0, 180), sourcing }
      )
    );
  }

  if (
    (/overflow/i.test(id) || /overflow/i.test(blob)) &&
    sourcing !== TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED &&
    !/overflow[\s\S]{0,48}(unconfirmed|not established|not evidenced|hypothes)/i.test(
      blob
    )
  ) {
    if (
      /overflow housing(?!\s*(hypothesis|\(unconfirmed\)))|overflow play|overflow\s*\/\s*housing opportunity|overflow opportunity(?!\s*(\(unconfirmed\)|hypothesis))/i.test(
        blob
      )
    ) {
      findings.push(
        finding(
          CONTRADICTION_CODE.OVERFLOW_CLAIM_WITHOUT_OVERFLOW_EVIDENCE,
          CONTRADICTION_SEVERITY.HIGH,
          "Overflow presented without overflow-confirmed sourcing status.",
          { sourcing }
        )
      );
    }
  }

  if (ledger?.priorIncorrectCompetitorHotel) {
    const prior = text(ledger.priorIncorrectCompetitorHotel);
    const current = text(
      opp.competitorHotel || ledger.competitorHotel || competitorHotel
    );
    if (
      prior &&
      current &&
      prior.toLowerCase() !== current.toLowerCase() &&
      text(opp.commercialConsistencyPrior?.competitorHotel || "").toLowerCase() ===
        prior.toLowerCase()
    ) {
      // Already remediated — informational only if prior still visible in raw fields
    }
    if (
      !opp.commercialConsistencyRemediatedAt &&
      prior &&
      competitorHotel &&
      prior.toLowerCase() === competitorHotel.toLowerCase() &&
      ledger.competitorHotel &&
      text(ledger.competitorHotel).toLowerCase() !== prior.toLowerCase()
    ) {
      findings.push(
        finding(
          CONTRADICTION_CODE.COMPETITOR_VENUE_SOURCE_MISMATCH,
          CONTRADICTION_SEVERITY.CRITICAL,
          `Competitor hotel "${competitorHotel}" conflicts with ledger-corrected identity.`,
          {
            priorIncorrect: prior,
            expected: ledger.competitorHotel,
          }
        )
      );
    }
  }

  if (
    /contracted block|official room block|verified booking/i.test(blob) &&
    ledger &&
    !(ledger.claims || []).some(
      (c) =>
        (c.claimType === "COMPETITOR_ROOM_BLOCK" ||
          c.claimType === "COMPETITOR_HOST_HOTEL" ||
          c.claimType === "COMPETITOR_EVENT_VENUE") &&
        c.status === "VERIFIED"
    )
  ) {
    findings.push(
      finding(
        CONTRADICTION_CODE.CONTRACTED_BLOCK_WITHOUT_BLOCK_EVIDENCE,
        CONTRADICTION_SEVERITY.CRITICAL,
        "Contracted/verified booking language without ledger-verified claim.",
        { id }
      )
    );
  }

  if (
    ledger?.advertisedAttendance &&
    (/attendance not yet published|attendance unknown|unknown/i.test(attendance) ||
      !attendance)
  ) {
    findings.push(
      finding(
        CONTRADICTION_CODE.ATTENDANCE_MISSING_WHEN_SOURCE_AVAILABLE,
        CONTRADICTION_SEVERITY.MEDIUM,
        `Attendance available from source (${ledger.advertisedAttendance}) but record still unknown/missing.`,
        { advertisedAttendance: ledger.advertisedAttendance }
      )
    );
  }

  if (
    opp.cmiVerified === true &&
    ledger &&
    ledger.verifiedCmi === false
  ) {
    findings.push(
      finding(
        CONTRADICTION_CODE.CMI_VERIFICATION_UNSUPPORTED,
        CONTRADICTION_SEVERITY.CRITICAL,
        "Record marked verified CMI but ledger demotes verification.",
        { demotionReason: ledger.demotionReason }
      )
    );
  }

  if (
    /hotel selection|room-block inclusion|housing owner/i.test(
      text(opp.primaryContact?.relationshipToOpportunity || opp.whyThisContact)
    ) &&
    /chair|president-elect|speaker/i.test(
      text(opp.primaryContactRole || opp.primaryContact?.title)
    ) &&
    !/activity-connected|not verified hotel-booking/i.test(
      text(opp.contactRoleNote || opp.whyThisContact)
    )
  ) {
    findings.push(
      finding(
        CONTRADICTION_CODE.CONTACT_ROLE_EXCEEDS_VERIFIED_REMIT,
        CONTRADICTION_SEVERITY.MEDIUM,
        "Contact portrayed as hotel-selection authority without booking-buyer verification.",
        {
          role: opp.primaryContactRole || opp.primaryContact?.title,
        }
      )
    );
  }

  if (
    /sales ready|strong qualification|true_actionable/i.test(blob) &&
    (sourcing === TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED ||
      sourcing === TARGET_HOTEL_SOURCING_STATUS.UNKNOWN ||
      opp.commercialActionability === "COMPETITIVE_MARKET_INTELLIGENCE")
  ) {
    if (/request rfp|host pitch|primary host/i.test(action + blob)) {
      findings.push(
        finding(
          CONTRADICTION_CODE.SALES_ACTION_EXCEEDS_COMMERCIAL_READINESS,
          CONTRADICTION_SEVERITY.HIGH,
          "Sales action exceeds verified commercial readiness.",
          { sourcing, action: action.slice(0, 120) }
        )
      );
    }
  }

  if (
    ledger &&
    (ledger.claims || []).some((c) => c.status === "NOT_SUPPORTED") &&
    /lodging demand path open|housing open|overflow housing opportunity/i.test(
      blob
    )
  ) {
    findings.push(
      finding(
        CONTRADICTION_CODE.SOURCE_CONTRADICTS_OPPORTUNITY_NARRATIVE,
        CONTRADICTION_SEVERITY.HIGH,
        "Narrative claims open lodging path while ledger marks sourcing/overflow unsupported.",
        { id }
      )
    );
  }

  // Optional duplicate scan against sibling ids
  for (const otherId of opts.siblingIds || []) {
    if (!otherId || otherId === id) continue;
    const a = id.replace(/_overflow$/, "");
    const b = String(otherId).replace(/_overflow$/, "");
    if (a === b || (a.startsWith(b) || b.startsWith(a))) {
      findings.push(
        finding(
          CONTRADICTION_CODE.DUPLICATE_ACCOUNT_EVENT_CYCLE,
          CONTRADICTION_SEVERITY.MEDIUM,
          `Possible duplicate account/event cycle vs ${otherId}`,
          { otherId }
        )
      );
    }
  }

  const maxSeverity = findings.reduce((acc, f) => {
    const order = ["LOW", "MEDIUM", "HIGH", "CRITICAL"];
    return order.indexOf(f.severity) > order.indexOf(acc) ? f.severity : acc;
  }, "LOW");

  return {
    opportunityId: id || null,
    findingCount: findings.length,
    maxSeverity: findings.length ? maxSeverity : null,
    findings,
  };
}

export function detectPortfolioContradictions(opportunities = [], opts = {}) {
  const ids = (opportunities || []).map((o) =>
    String(o.id || o.opportunityId || "")
  );
  const rows = [];
  for (const opp of opportunities || []) {
    rows.push(
      detectOpportunityContradictions(opp, {
        ...opts,
        siblingIds: ids,
      })
    );
  }
  return {
    schema: "gdi-commercial-contradiction-portfolio-v1",
    count: rows.length,
    withFindings: rows.filter((r) => r.findingCount > 0).length,
    critical: rows.filter((r) => r.maxSeverity === "CRITICAL").length,
    high: rows.filter((r) => r.maxSeverity === "HIGH").length,
    rows,
  };
}
