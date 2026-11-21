/**
 * Calculate contractual decision windows from verified / disclosed terms.
 * Vague language → UNDETERMINED (never invent precision).
 * Date-semantic gate is mandatory — opening/loan/unresolved dates fail closed.
 */

import { DISCLOSURE_CLASS } from "./contract-terms-schema-proposal-v4.js";
import { assertDateEligibleForDecisionWindow } from "./contract-date-semantic-gate-v1.js";
import { auditContractDateObservation } from "./contract-date-meaning-audit-v1.js";

export const CONTRACT_TERMS_DECISION_WINDOWS_V1 = "contract-terms-decision-windows-v1";

function parseDate(d) {
  if (!d) return null;
  const x = new Date(d);
  return Number.isNaN(x.getTime()) ? null : x;
}

function ymd(d) {
  if (!d) return null;
  return d.toISOString().slice(0, 10);
}

function addDays(d, days) {
  const x = new Date(d.getTime());
  x.setUTCDate(x.getUTCDate() + days);
  return x;
}

/**
 * @param {object} args
 * @param {string|Date|null} args.scheduledExpiration
 * @param {number|null} args.renewalNoticeDays
 * @param {number|null} args.renewalOptionsCount
 * @param {boolean} args.terminationWithoutCause
 * @param {boolean} args.performanceTest
 * @param {boolean} args.changeOfControl
 * @param {string|null} [args.explicitNoticeDeadline]
 * @param {string|null} [args.explicitOptionDeadline]
 * @param {object} [args.dateObservation] — snippet/dateMeaning for semantic audit
 * @param {object} [args.dateAudit] — precomputed audit
 * @param {boolean} [args.portfolioLevelOnly]
 * @param {boolean} [args.hotelAttributed]
 * @param {boolean} [args.skipDateSemanticGate] — tests only; production must not skip
 */
export function calculateDecisionWindows(args = {}) {
  const windows = [];
  const assumptions = [];

  const audit =
    args.dateAudit ||
    (args.dateObservation
      ? auditContractDateObservation(args.dateObservation)
      : args.scheduledExpiration
        ? auditContractDateObservation({
            primaryDate: args.scheduledExpiration,
            dateMeaning: args.dateMeaning || null,
            evidenceSnippet: args.evidenceSnippet || "",
            hotelName: args.hotelName,
          })
        : null);

  if (!args.skipDateSemanticGate) {
    const gate = assertDateEligibleForDecisionWindow({
      audit,
      portfolioLevelOnly: Boolean(args.portfolioLevelOnly),
      hotelAttributed: args.hotelAttributed !== false,
    });
    if (!gate.ok) {
      return {
        version: CONTRACT_TERMS_DECISION_WINDOWS_V1,
        windows: [
          {
            kind: "SCHEDULED_EXPIRATION",
            date: null,
            disclosureClass: DISCLOSURE_CLASS.UNDETERMINED,
            commercialRelevance: "REQUIRES_MORE_RESEARCH",
            notes: `BLOCKED_BY_DATE_SEMANTIC_GATE: ${gate.code} — ${gate.reason}`,
          },
        ],
        assumptions: [`dateSemanticGate=${gate.code}`],
        nextDecisionWindow: null,
        speculativeLeadsPublished: false,
        dateSemanticGate: gate,
        blocked: true,
      };
    }
  }

  const exp = parseDate(args.scheduledExpiration);

  if (exp) {
    windows.push({
      kind: "SCHEDULED_EXPIRATION",
      date: ymd(exp),
      disclosureClass: DISCLOSURE_CLASS.EXPLICITLY_DISCLOSED,
      commercialRelevance: "CONTRACTUALLY_CONFIRMED",
      notes: "Hotel-attributed contractual expiration after date-semantic validation",
    });
  } else {
    windows.push({
      kind: "SCHEDULED_EXPIRATION",
      date: null,
      disclosureClass: DISCLOSURE_CLASS.UNDETERMINED,
      commercialRelevance: "REQUIRES_MORE_RESEARCH",
      notes: "No verified scheduled expiration",
    });
  }

  if (args.explicitNoticeDeadline) {
    const d = parseDate(args.explicitNoticeDeadline);
    windows.push({
      kind: "RENEWAL_NOTICE_DEADLINE",
      date: ymd(d),
      disclosureClass: DISCLOSURE_CLASS.EXPLICITLY_DISCLOSED,
      commercialRelevance: "CONTRACTUALLY_CONFIRMED",
      notes: "Explicit notice deadline in source",
    });
  } else if (exp && Number.isFinite(args.renewalNoticeDays) && args.renewalNoticeDays > 0) {
    const notice = addDays(exp, -Number(args.renewalNoticeDays));
    assumptions.push(`notice = expiration − ${args.renewalNoticeDays} days`);
    windows.push({
      kind: "RENEWAL_NOTICE_DEADLINE",
      date: ymd(notice),
      disclosureClass: DISCLOSURE_CLASS.CALCULATED_FROM_VERIFIED_TERMS,
      commercialRelevance: "EVIDENCE_BACKED_POTENTIAL_WINDOW",
      notes: "Calculated from disclosed notice-days and verified expiration",
    });
  } else {
    windows.push({
      kind: "RENEWAL_NOTICE_DEADLINE",
      date: null,
      disclosureClass: DISCLOSURE_CLASS.UNDETERMINED,
      commercialRelevance: "REQUIRES_MORE_RESEARCH",
      notes: "Notice days not disclosed or expiration missing — not calculated from vague language",
    });
  }

  if (args.explicitOptionDeadline) {
    windows.push({
      kind: "OPTION_EXERCISE_DEADLINE",
      date: ymd(parseDate(args.explicitOptionDeadline)),
      disclosureClass: DISCLOSURE_CLASS.EXPLICITLY_DISCLOSED,
      commercialRelevance: "CONTRACTUALLY_CONFIRMED",
      notes: "Explicit option exercise deadline",
    });
  } else if (args.renewalOptionsCount > 0 && exp) {
    windows.push({
      kind: "OPTION_EXERCISE_DEADLINE",
      date: null,
      disclosureClass: DISCLOSURE_CLASS.CONDITIONAL,
      commercialRelevance: "EVIDENCE_BACKED_POTENTIAL_WINDOW",
      notes: `${args.renewalOptionsCount} renewal option(s) disclosed; precise exercise deadline not stated`,
    });
  } else {
    windows.push({
      kind: "OPTION_EXERCISE_DEADLINE",
      date: null,
      disclosureClass: DISCLOSURE_CLASS.UNDETERMINED,
      commercialRelevance: "REQUIRES_MORE_RESEARCH",
      notes: "No option exercise deadline determinable",
    });
  }

  windows.push({
    kind: "EARLIEST_TERMINATION_WINDOW",
    date: null,
    disclosureClass: args.terminationWithoutCause
      ? DISCLOSURE_CLASS.CONDITIONAL
      : DISCLOSURE_CLASS.UNDETERMINED,
    commercialRelevance: args.terminationWithoutCause
      ? "EVIDENCE_BACKED_POTENTIAL_WINDOW"
      : "REQUIRES_MORE_RESEARCH",
    notes: args.terminationWithoutCause
      ? "Without-cause termination language present; precise window not dated"
      : "No without-cause termination date calculated",
  });

  windows.push({
    kind: "PERFORMANCE_TEST_REVIEW",
    date: null,
    disclosureClass: args.performanceTest
      ? DISCLOSURE_CLASS.CONDITIONAL
      : DISCLOSURE_CLASS.NOT_DISCLOSED,
    commercialRelevance: args.performanceTest
      ? "EVIDENCE_BACKED_POTENTIAL_WINDOW"
      : "REQUIRES_MORE_RESEARCH",
    notes: args.performanceTest
      ? "Performance-test language present; review period not dated"
      : "No performance-test provision found",
  });

  windows.push({
    kind: "CHANGE_OF_CONTROL_TRIGGER",
    date: null,
    disclosureClass: args.changeOfControl
      ? DISCLOSURE_CLASS.CONDITIONAL
      : DISCLOSURE_CLASS.NOT_DISCLOSED,
    commercialRelevance: args.changeOfControl
      ? "EVIDENCE_BACKED_POTENTIAL_WINDOW"
      : "REQUIRES_MORE_RESEARCH",
    notes: args.changeOfControl
      ? "Sale/change-of-control language present; event-triggered (not calendar dated)"
      : "No CoC provision found",
  });

  const dated = windows.filter((w) => w.date);
  const next = dated
    .map((w) => ({ ...w, _t: new Date(w.date).getTime() }))
    .filter((w) => !Number.isNaN(w._t))
    .sort((a, b) => a._t - b._t)[0];

  return {
    version: CONTRACT_TERMS_DECISION_WINDOWS_V1,
    windows,
    assumptions,
    nextDecisionWindow: next
      ? {
          kind: next.kind,
          date: next.date,
          disclosureClass: next.disclosureClass,
          commercialRelevance: next.commercialRelevance,
        }
      : null,
    speculativeLeadsPublished: false,
    dateSemanticGate: { ok: true, code: "DATE_SEMANTIC_OK", audit },
    blocked: false,
  };
}

export default { calculateDecisionWindows };
