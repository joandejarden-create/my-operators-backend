/**
 * Permanent GDI commercial-expiration & future-cycle validation gate V1.
 *
 * Temporal status is SEPARATE from commercial readiness (Sales Ready / Account
 * Development). Apply during discovery triage, commercial qualification,
 * weekly refresh, customer publication staging, and final certification.
 *
 * Does NOT invent next-year editions. Does NOT treat source-article age alone
 * as commercial expiration for ongoing corporate/project demand.
 */

export const GDI_COMMERCIAL_EXPIRATION_GATE_V1 =
  "gdi-commercial-expiration-gate-v1";

export const COMMERCIAL_TEMPORAL_DISPOSITION = Object.freeze({
  ACTIVE_FUTURE_EVENT: "ACTIVE_FUTURE_EVENT",
  ACTIVE_ONGOING_DEMAND: "ACTIVE_ONGOING_DEMAND",
  FUTURE_CYCLE_VERIFIED: "FUTURE_CYCLE_VERIFIED",
  FUTURE_CYCLE_RESEARCH_REQUIRED: "FUTURE_CYCLE_RESEARCH_REQUIRED",
  EXPIRED_ONE_TIME_EVENT: "EXPIRED_ONE_TIME_EVENT",
  HISTORICAL_ACCOUNT_DEVELOPMENT: "HISTORICAL_ACCOUNT_DEVELOPMENT",
  DATE_UNVERIFIED: "DATE_UNVERIFIED",
  DUPLICATE_EVENT_RECORD: "DUPLICATE_EVENT_RECORD",
});

/** Customer pursuit shortlist — commercially live temporal statuses. */
export const CUSTOMER_PURSUIT_DISPOSITIONS = Object.freeze([
  COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_FUTURE_EVENT,
  COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_ONGOING_DEMAND,
  COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_VERIFIED,
]);

/** May remain visible with an explicit verification / research flag. */
export const CUSTOMER_VERIFICATION_FLAG_DISPOSITIONS = Object.freeze([
  COMMERCIAL_TEMPORAL_DISPOSITION.DATE_UNVERIFIED,
  COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_RESEARCH_REQUIRED,
]);

/** Must leave active customer pursuit (research/watch only). */
export const RESEARCH_WATCH_DISPOSITIONS = Object.freeze([
  COMMERCIAL_TEMPORAL_DISPOSITION.EXPIRED_ONE_TIME_EVENT,
  COMMERCIAL_TEMPORAL_DISPOSITION.HISTORICAL_ACCOUNT_DEVELOPMENT,
  COMMERCIAL_TEMPORAL_DISPOSITION.DUPLICATE_EVENT_RECORD,
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function parseIsoDate(v) {
  const s = text(v);
  if (!s || /^(dates?\s*tbd|ongoing|timing\s*tbd|tbd)$/i.test(s)) return null;
  const m = s.match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}

function yearIn(s) {
  const m = text(s).match(/\b(20[2-3]\d)\b/);
  return m ? Number(m[1]) : null;
}

/**
 * @param {string} referenceDate YYYY-MM-DD
 * @param {object} opportunity
 * @param {object} [hints] optional audit/override hints
 */
export function classifyCommercialTemporalStatus(
  opportunity = {},
  {
    referenceDate = null,
    dispositionOverride = null,
    duplicateOf = null,
    oneTimeVsRecurring = null,
    actualEventStart = null,
    actualEventEnd = null,
    storedIsResearchTimestamp = false,
    allowCustomerVerificationFlags = true,
  } = {},
) {
  const ref = parseIsoDate(referenceDate) || new Date().toISOString().slice(0, 10);
  const id = text(opportunity.id || opportunity.opportunityId);
  const title = text(opportunity.displayTitle || opportunity.title);
  const blob = [
    title,
    opportunity.whatIsHappening,
    opportunity.potentialHotelBusiness,
    opportunity.whyActNow,
    JSON.stringify(opportunity.paidDiscoveryV3 || {}),
  ]
    .map(text)
    .join(" ");

  if (dispositionOverride && COMMERCIAL_TEMPORAL_DISPOSITION[dispositionOverride]) {
    const d = dispositionOverride;
    return finalize(d, {
      id,
      title,
      referenceDate: ref,
      provenance: "disposition_override",
      duplicateOf: duplicateOf || opportunity.duplicateOf || null,
      customerPursuitEligible: isCustomerPursuitEligible(d, {
        allowVerificationFlags: allowCustomerVerificationFlags,
      }),
      researchWatchOnly: RESEARCH_WATCH_DISPOSITIONS.includes(d),
    });
  }

  if (duplicateOf || opportunity.duplicateOf || opportunity.duplicateEventOf) {
    return finalize(COMMERCIAL_TEMPORAL_DISPOSITION.DUPLICATE_EVENT_RECORD, {
      id,
      title,
      referenceDate: ref,
      provenance: "duplicate_pointer",
      duplicateOf:
        duplicateOf ||
        opportunity.duplicateOf ||
        opportunity.duplicateEventOf,
      customerPursuitEligible: false,
      researchWatchOnly: true,
    });
  }

  const start =
    parseIsoDate(actualEventStart) ||
    parseIsoDate(opportunity.eventStartDate) ||
    parseIsoDate(opportunity.tileEventStartDate) ||
    parseIsoDate(opportunity.datePillDisplayLabel);
  const end =
    parseIsoDate(actualEventEnd) ||
    parseIsoDate(opportunity.eventEndDate) ||
    parseIsoDate(opportunity.activityEndDate);

  const ongoingSignals =
    /ongoing|preferred lodging|partnership|operations expansion|public-works|infrastructure|remodel|project|factory|mobility|training-site|convenio|programa 20|season /i.test(
      blob,
    ) ||
    /ongoing/i.test(text(opportunity.datePillDisplayLabel)) ||
    opportunity.preferredLodgingPartnershipV1 === true ||
    text(opportunity.demandOpportunitySubtype) === "PREFERRED_LODGING_PARTNERSHIP";

  const recurringSignals =
    /recurring|annual|anual|edition|edición|congreso|congress|forum|expo|cumbre|asamblea|marat/i.test(
      blob,
    );

  // Research-timestamp contamination: do not treat as event date.
  const reliableStart = storedIsResearchTimestamp ? null : start;

  if (ongoingSignals && !/one[- ]time|open house|already concluded|took place|reportedly gathered/i.test(blob)) {
    return finalize(COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_ONGOING_DEMAND, {
      id,
      title,
      referenceDate: ref,
      provenance: "ongoing_demand_signals",
      actualEventStart: reliableStart,
      actualEventEnd: end,
      customerPursuitEligible: true,
      researchWatchOnly: false,
    });
  }

  if (end && end < ref && !recurringSignals) {
    return finalize(COMMERCIAL_TEMPORAL_DISPOSITION.EXPIRED_ONE_TIME_EVENT, {
      id,
      title,
      referenceDate: ref,
      provenance: "event_end_before_reference",
      actualEventStart: reliableStart,
      actualEventEnd: end,
      customerPursuitEligible: false,
      researchWatchOnly: true,
      organizationMeritsFutureAccountDevelopment: recurringSignals,
    });
  }

  if (reliableStart && reliableStart >= ref) {
    return finalize(COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_FUTURE_EVENT, {
      id,
      title,
      referenceDate: ref,
      provenance: "event_start_on_or_after_reference",
      actualEventStart: reliableStart,
      actualEventEnd: end,
      customerPursuitEligible: true,
      researchWatchOnly: false,
    });
  }

  if (
    reliableStart &&
    reliableStart < ref &&
    /congreso|forum|cumbre|expo|asamblea|open house|conference|marat/i.test(title) &&
    !/2027|2028|próxim|proxim|next edition|próxima edición/i.test(blob)
  ) {
    return finalize(COMMERCIAL_TEMPORAL_DISPOSITION.EXPIRED_ONE_TIME_EVENT, {
      id,
      title,
      referenceDate: ref,
      provenance:
        "past_event_start_without_verified_future_cycle — do not auto-roll year",
      actualEventStart: reliableStart,
      actualEventEnd: end,
      customerPursuitEligible: false,
      researchWatchOnly: true,
      organizationMeritsFutureAccountDevelopment: recurringSignals,
    });
  }

  if (/2027|2028/.test(blob) && /confirmed|verified|official/i.test(blob)) {
    return finalize(COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_VERIFIED, {
      id,
      title,
      referenceDate: ref,
      provenance: "verified_future_year_in_evidence",
      actualEventStart: reliableStart,
      customerPursuitEligible: true,
      researchWatchOnly: false,
    });
  }

  if (recurringSignals && reliableStart && reliableStart < ref) {
    return finalize(
      COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_RESEARCH_REQUIRED,
      {
        id,
        title,
        referenceDate: ref,
        provenance: "recurring_past_edition_needs_next_cycle_evidence",
        actualEventStart: reliableStart,
        customerPursuitEligible: allowCustomerVerificationFlags,
        researchWatchOnly: !allowCustomerVerificationFlags,
        organizationMeritsFutureAccountDevelopment: true,
      },
    );
  }

  if (!reliableStart && !ongoingSignals) {
    const y = yearIn(title);
    if (y && y < Number(ref.slice(0, 4)) && /academy|programa|cursos/i.test(title)) {
      return finalize(COMMERCIAL_TEMPORAL_DISPOSITION.EXPIRED_ONE_TIME_EVENT, {
        id,
        title,
        referenceDate: ref,
        provenance: "title_program_year_before_reference",
        customerPursuitEligible: false,
        researchWatchOnly: true,
      });
    }
    return finalize(COMMERCIAL_TEMPORAL_DISPOSITION.DATE_UNVERIFIED, {
      id,
      title,
      referenceDate: ref,
      provenance: storedIsResearchTimestamp
        ? "stored_date_was_research_timestamp_not_event_date"
        : "no_reliable_event_date",
      customerPursuitEligible: allowCustomerVerificationFlags,
      researchWatchOnly: !allowCustomerVerificationFlags,
    });
  }

  return finalize(COMMERCIAL_TEMPORAL_DISPOSITION.DATE_UNVERIFIED, {
    id,
    title,
    referenceDate: ref,
    provenance: "fallback_unverified",
    actualEventStart: reliableStart,
    customerPursuitEligible: allowCustomerVerificationFlags,
    researchWatchOnly: !allowCustomerVerificationFlags,
  });
}

function finalize(disposition, extra = {}) {
  return {
    version: GDI_COMMERCIAL_EXPIRATION_GATE_V1,
    disposition,
    ...extra,
  };
}

export function isCustomerPursuitEligible(
  disposition,
  { allowVerificationFlags = true } = {},
) {
  const d = text(disposition);
  if (CUSTOMER_PURSUIT_DISPOSITIONS.includes(d)) return true;
  if (
    allowVerificationFlags &&
    CUSTOMER_VERIFICATION_FLAG_DISPOSITIONS.includes(d)
  ) {
    return true;
  }
  return false;
}

/**
 * Partition opportunities for staging / weekly refresh.
 * Preserves all records; marks research/watch separately.
 */
export function applyCommercialExpirationGate(
  opportunities = [],
  opts = {},
) {
  const rows = [];
  const customerVisible = [];
  const researchWatch = [];
  for (const opp of opportunities) {
    const classification = classifyCommercialTemporalStatus(opp, opts);
    const stamped = {
      ...opp,
      commercialTemporalDisposition: classification.disposition,
      commercialExpirationGateV1: classification,
    };
    rows.push(stamped);
    if (classification.customerPursuitEligible) customerVisible.push(stamped);
    else researchWatch.push(stamped);
  }
  return {
    version: GDI_COMMERCIAL_EXPIRATION_GATE_V1,
    referenceDate: opts.referenceDate || null,
    counts: {
      input: opportunities.length,
      customerVisible: customerVisible.length,
      researchWatch: researchWatch.length,
      byDisposition: rows.reduce((m, r) => {
        const d = r.commercialTemporalDisposition;
        m[d] = (m[d] || 0) + 1;
        return m;
      }, {}),
    },
    customerVisible,
    researchWatch,
    rows,
  };
}

/**
 * Fail closed for publication certification when expired/duplicate remain
 * in the customer-facing candidate without explicit allowlist.
 */
export function assertCustomerInventoryPassesExpirationGate(
  opportunities = [],
  opts = {},
) {
  const failed = [];
  const rows = [];
  for (const opp of opportunities) {
    const classification = classifyCommercialTemporalStatus(opp, {
      ...opts,
      dispositionOverride:
        opts.dispositionOverride ||
        opp.commercialTemporalDisposition ||
        opp.commercialExpirationGateV1?.disposition ||
        null,
      duplicateOf:
        opts.duplicateOf ||
        opp.duplicateOf ||
        opp.duplicateEventOf ||
        opp.commercialExpirationGateV1?.duplicateOf ||
        null,
      storedIsResearchTimestamp:
        opts.storedIsResearchTimestamp ||
        opp.commercialExpirationGateV1?.storedIsResearchTimestamp ||
        false,
      actualEventStart:
        opts.actualEventStart ||
        opp.commercialExpirationGateV1?.actualEventStart ||
        null,
      actualEventEnd:
        opts.actualEventEnd ||
        opp.commercialExpirationGateV1?.actualEventEnd ||
        null,
    });
    const stamped = {
      ...opp,
      commercialTemporalDisposition: classification.disposition,
      commercialExpirationGateV1: classification,
    };
    rows.push(stamped);
    if (RESEARCH_WATCH_DISPOSITIONS.includes(classification.disposition)) {
      failed.push(text(opp.id));
    }
  }
  return {
    ok: failed.length === 0,
    version: GDI_COMMERCIAL_EXPIRATION_GATE_V1,
    rows,
    blockers: failed,
    failedInventoryPopulation: failed,
  };
}

export default {
  GDI_COMMERCIAL_EXPIRATION_GATE_V1,
  COMMERCIAL_TEMPORAL_DISPOSITION,
  classifyCommercialTemporalStatus,
  applyCommercialExpirationGate,
  assertCustomerInventoryPassesExpirationGate,
  isCustomerPursuitEligible,
};
