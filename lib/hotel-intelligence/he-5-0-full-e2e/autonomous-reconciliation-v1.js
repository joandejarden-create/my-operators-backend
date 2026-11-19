/**
 * HE 5.0 autonomous research reconciliation supervisor.
 *
 * Detect gap → reuse → free research → adjudicate → optional bounded paid →
 * resolve or classify → continue next gap. Does NOT stop for routine ambiguity.
 * Stops only at production Airtable apply / budget / schema / subscription gates.
 */

import {
  APPLY_GATE,
  HE50_GLOBAL_USD_CAP,
  HE50_PILOT_RUN_ID,
  HE50_PILOT_VERSION,
  HE50_BETHESDA_HPC_ID,
} from "./contract.js";
import { createHe50PilotBudgetLedger } from "./pilot-budget-ledger.js";
import { CONFLICT_RESOLUTIONS } from "./mutation-reconciliation-v1.js";

export const RESOLUTION_CLASS = Object.freeze({
  RESOLVED_VERIFIED: "RESOLVED_VERIFIED",
  RESOLVED_WITH_HISTORICAL_CONTEXT: "RESOLVED_WITH_HISTORICAL_CONTEXT",
  RESOLVED_NOT_APPLICABLE: "RESOLVED_NOT_APPLICABLE",
  UNRESOLVED_CONFLICT: "UNRESOLVED_CONFLICT",
  UNRESOLVED_INSUFFICIENT_EVIDENCE: "UNRESOLVED_INSUFFICIENT_EVIDENCE",
  UNRESOLVED_PROVIDER_RESTRICTED: "UNRESOLVED_PROVIDER_RESTRICTED",
  UNRESOLVED_BUDGET_LIMIT: "UNRESOLVED_BUDGET_LIMIT",
  REQUIRES_FOUNDER_DECISION: "REQUIRES_FOUNDER_DECISION",
});

export const AUTONOMOUS_RECON_VERSION = "he-5-0-autonomous-reconciliation-v1";

/**
 * Priority score: higher = research first.
 * commercialValue 0–5, identityImpact 0–5, reusability 0–5, resolveLikelihood 0–5
 */
export function prioritizeGap(gap) {
  const commercialValue = Number(gap.commercialValue ?? 3);
  const identityImpact = Number(gap.identityImpact ?? 3);
  const heCompleteness = Number(gap.heCompleteness ?? 2);
  const reusability = Number(gap.reusability ?? 2);
  const resolveLikelihood = Number(gap.resolveLikelihood ?? 3);
  const evidenceWeakness = Number(gap.evidenceWeakness ?? 2);
  return (
    commercialValue * 3 +
    identityImpact * 4 +
    heCompleteness * 2 +
    reusability * 3 +
    resolveLikelihood * 2 +
    evidenceWeakness
  );
}

function issue(partial) {
  return {
    issueId: partial.issueId,
    hotelKey: partial.hotelKey,
    hotelName: partial.hotelName,
    claim: partial.claim,
    whyInsufficient: partial.whyInsufficient,
    commercialValue: partial.commercialValue ?? 3,
    identityImpact: partial.identityImpact ?? 3,
    heCompleteness: partial.heCompleteness ?? 2,
    reusability: partial.reusability ?? 2,
    resolveLikelihood: partial.resolveLikelihood ?? 3,
    evidenceWeakness: partial.evidenceWeakness ?? 2,
    domain: partial.domain || "property",
  };
}

/** Seed issues for the eight-hotel pilot (detected conflicts + material gaps). */
export function detectPilotIssues() {
  return [
    issue({
      issueId: "ac_rooms_140_vs_146",
      hotelKey: "ac_hotel_newtown_square",
      hotelName: "AC Hotel Newtown Square",
      claim: "Guest room inventory is either 140 or 146",
      whyInsufficient: "Ellis Preserve 140 vs third-party OTA 146 without adjudication",
      commercialValue: 4,
      identityImpact: 5,
      heCompleteness: 4,
      resolveLikelihood: 5,
    }),
    issue({
      issueId: "beth_renovation_2015_vs_2017",
      hotelKey: "bethesda_marriott",
      hotelName: "Bethesda Marriott",
      claim: "Last renovation year is 2015 or 2017",
      whyInsufficient: "Multiple dates without event-role separation",
      commercialValue: 3,
      identityImpact: 3,
      heCompleteness: 3,
      resolveLikelihood: 5,
    }),
    issue({
      issueId: "beth_year_built",
      hotelKey: "bethesda_marriott",
      hotelName: "Bethesda Marriott",
      claim: "Year Built missing on HPC",
      whyInsufficient: "HPC field empty; public sources available",
      commercialValue: 3,
      identityImpact: 4,
      heCompleteness: 4,
      resolveLikelihood: 5,
    }),
    issue({
      issueId: "cap_cana_vs_dreams_identity",
      hotelKey: "secrets_cap_cana",
      hotelName: "Secrets Cap Cana Resort & Spa",
      claim: "HPC match might be Dreams Cap Cana neighbor",
      whyInsufficient: "Near-neighbor collision list included Dreams",
      commercialValue: 5,
      identityImpact: 5,
      heCompleteness: 5,
      reusability: 3,
      resolveLikelihood: 5,
    }),
    issue({
      issueId: "cap_cana_rooms",
      hotelKey: "secrets_cap_cana",
      hotelName: "Secrets Cap Cana Resort & Spa",
      claim: "Room / suite inventory unknown on matched HPC",
      whyInsufficient: "Identity verified but rooms not enriched",
      commercialValue: 4,
      identityImpact: 4,
      heCompleteness: 4,
      resolveLikelihood: 5,
    }),
    issue({
      issueId: "papagayo_census_to_hpc",
      hotelKey: "secrets_papagayo",
      hotelName: "Secrets Papagayo Costa Rica",
      claim: "Hotel Census match needs duplicate-safe HPC promotion",
      whyInsufficient: "No HPC id; census present",
      commercialValue: 5,
      identityImpact: 5,
      heCompleteness: 4,
      resolveLikelihood: 5,
    }),
    issue({
      issueId: "hgi_rooms_confidence",
      hotelKey: "hgi_newtown_square_radnor",
      hotelName: "Hilton Garden Inn Newtown Square Radnor",
      claim: "Rooms 138 only MEDIUM (Ellis Preserve)",
      whyInsufficient: "Single secondary listing class",
      commercialValue: 3,
      identityImpact: 4,
      heCompleteness: 3,
      resolveLikelihood: 4,
    }),
    issue({
      issueId: "jw_rooms_alias",
      hotelKey: "jw_marriott_costa_elena",
      hotelName: "JW Marriott Costa Elena Resort All-Inclusive",
      claim: "Confirm 415 rooms + Dreams Las Mareas alias",
      whyInsufficient: "Need official corroboration before HPC null-fill",
      commercialValue: 4,
      identityImpact: 4,
      heCompleteness: 3,
      resolveLikelihood: 5,
    }),
    issue({
      issueId: "carton_rooms",
      hotelKey: "carton_house_fairmont",
      hotelName: "Carton House, a Fairmont Hotel",
      claim: "Confirm 169 bedrooms for NEW_HPC CREATE",
      whyInsufficient: "Need multi-source for CREATE packet",
      commercialValue: 3,
      identityImpact: 4,
      resolveLikelihood: 5,
    }),
    issue({
      issueId: "st_lucia_rooms",
      hotelKey: "secrets_st_lucia",
      hotelName: "Secrets St. Lucia",
      claim: "Confirm 355 rooms for NEW_HPC CREATE",
      whyInsufficient: "Need official corroboration",
      commercialValue: 3,
      identityImpact: 4,
      resolveLikelihood: 5,
    }),
    issue({
      issueId: "mullen_holdings_vs_proposed",
      hotelKey: "cohort",
      hotelName: "Eight-hotel Mullen cohort",
      claim: "Portfolio listing implies verified current ownership",
      whyInsufficient: "Public listing ≠ legal ownership proof",
      commercialValue: 5,
      identityImpact: 5,
      reusability: 5,
      resolveLikelihood: 4,
      domain: "ownership",
    }),
    issue({
      issueId: "beth_current_lender",
      hotelKey: "bethesda_marriott",
      hotelName: "Bethesda Marriott",
      claim: "Current lender / mortgagee identity",
      whyInsufficient: "Ramsfield historical; CMBS terms without named lender org",
      commercialValue: 4,
      identityImpact: 2,
      heCompleteness: 3,
      reusability: 2,
      resolveLikelihood: 2,
      evidenceWeakness: 4,
      domain: "ownership",
    }),
    issue({
      issueId: "beth_market_alerts_news",
      hotelKey: "bethesda_marriott",
      hotelName: "Bethesda Marriott",
      claim: "Hotel News & Articles empty",
      whyInsufficient: "No property-linked Market Alerts rows",
      commercialValue: 2,
      identityImpact: 1,
      heCompleteness: 3,
      resolveLikelihood: 2,
      domain: "news",
    }),
    issue({
      issueId: "guest_sentiment_licensing",
      hotelKey: "bethesda_marriott",
      hotelName: "Bethesda Marriott",
      claim: "Live Guest Sentiment ratings persistence",
      whyInsufficient: "Provider/licensing gates; founder-preview staged only",
      commercialValue: 3,
      identityImpact: 1,
      resolveLikelihood: 1,
      domain: "reputation",
    }),
  ].map((g) => ({ ...g, priority: prioritizeGap(g) }))
    .sort((a, b) => b.priority - a.priority);
}

/**
 * Adjudicate a single issue using free-evidence corpus (no network in this module).
 * Network research results are injected via `evidenceByIssueId`.
 */
export function adjudicateIssue(issueRow, evidenceByIssueId = {}) {
  const ev = evidenceByIssueId[issueRow.issueId] || {};
  const actions = ev.researchActions || [
    { pass: 1, action: "reuse_existing_dealality", result: "inspected" },
  ];

  // Built-in resolutions from prior free research (autonomous — no founder prompt)
  const builtins = {
    ac_rooms_140_vs_146: {
      resolutionClass: RESOLUTION_CLASS.RESOLVED_VERIFIED,
      resolvedValue: 140,
      discardedAlt: 146,
      rationale: CONFLICT_RESOLUTIONS.ac_rooms.rationale,
      sources: CONFLICT_RESOLUTIONS.ac_rooms.sources,
      mutationHint: { opId: "M-NEW-ac_hotel_newtown_square", field: "rooms", value: 140, confidence: "HIGH" },
    },
    beth_renovation_2015_vs_2017: {
      resolutionClass: RESOLUTION_CLASS.RESOLVED_WITH_HISTORICAL_CONTEXT,
      resolvedValue: CONFLICT_RESOLUTIONS.bethesda_renovation.events,
      rationale: CONFLICT_RESOLUTIONS.bethesda_renovation.rationale,
      sources: ["Bethesda Magazine 2015", "EIN 2017", "Travel Weekly", "CMBS", "Cvent"],
      mutationHint: { opId: "M-BETH-002", preserveDualEvents: true },
    },
    beth_year_built: {
      resolutionClass: RESOLUTION_CLASS.RESOLVED_VERIFIED,
      resolvedValue: 1979,
      rationale: "Travel Weekly / CMBS / Cvent convergent Year Built 1979",
      sources: [
        "https://www.travelweekly.com/Hotels/Bethesda-MD/Bethesda-Marriott-p55149814",
        "https://cmbs.news/property/bethesda-marriott-bethesda-md/",
      ],
      mutationHint: { opId: "M-BETH-001", field: "Year Built", value: 1979 },
    },
    cap_cana_vs_dreams_identity: {
      resolutionClass: RESOLUTION_CLASS.RESOLVED_VERIFIED,
      resolvedValue: {
        secretsHpcId: "recd7XAM6tLjzQhaI",
        dreamsHpcId: "reclS16CMUS6eJgpC",
        distinct: true,
      },
      rationale: CONFLICT_RESOLUTIONS.secrets_cap_cana.rationale,
      sources: ["live HPC probe", "Hyatt Inclusive Secrets vs Dreams pages"],
      mutationHint: { opId: "M-UPD-secrets_cap_cana", action: "NO_OP" },
    },
    papagayo_census_to_hpc: {
      resolutionClass: RESOLUTION_CLASS.RESOLVED_VERIFIED,
      resolvedValue: {
        hotelCensusId: "recyX1wLsHBJYRPw7",
        rooms: 202,
        promote: true,
        doNotDuplicate: true,
      },
      rationale: CONFLICT_RESOLUTIONS.secrets_papagayo.promoteRules.join("; "),
      sources: ["Hotel Census live read", "Hyatt Inclusive rooms page"],
      mutationHint: { opId: "M-PROMOTE-secrets_papagayo", promote: true },
    },
    jw_rooms_alias: {
      resolutionClass: RESOLUTION_CLASS.RESOLVED_WITH_HISTORICAL_CONTEXT,
      resolvedValue: { rooms: 415, formerName: "Dreams Las Mareas" },
      rationale: "Marriott news: 415 rooms; property formerly Dreams Las Mareas — alias historical context",
      sources: [
        "https://news.marriott.com/news/2026/10/06/jw-marriott-debuts-its-first-all-inclusive-resort-with-the-opening-of-jw-marriott-costa-elena-resort-and-spa",
      ],
      mutationHint: { opId: "M-UPD-jw_marriott_costa_elena", rooms: 415 },
    },
    carton_rooms: {
      resolutionClass: RESOLUTION_CLASS.RESOLVED_VERIFIED,
      resolvedValue: 169,
      rationale: "Official Carton House / Accor ALL: 169 bedrooms (18 in The House)",
      sources: ["https://www.cartonhouse.com/the-resort", "https://all.accor.com/hotel/B7D0/index.en.shtml"],
      mutationHint: { opId: "M-NEW-carton_house_fairmont", rooms: 169 },
    },
    st_lucia_rooms: {
      resolutionClass: RESOLUTION_CLASS.RESOLVED_VERIFIED,
      resolvedValue: 355,
      rationale: "Official Hyatt Inclusive Secrets St Lucia: 355 rooms and suites",
      sources: [
        "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/st-lucia/st-lucia-resort-spa/accommodations/rooms-suites/",
      ],
      mutationHint: { opId: "M-NEW-secrets_st_lucia", rooms: 355 },
    },
    mullen_holdings_vs_proposed: {
      resolutionClass: RESOLUTION_CLASS.RESOLVED_WITH_HISTORICAL_CONTEXT,
      resolvedValue: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
      rationale: CONFLICT_RESOLUTIONS.mullen_portfolio.rule,
      sources: ["mullencap.com/portfolio", "founder pilot rules"],
      mutationHint: { forbidAutoOwnedBy: true },
      founderDecision: false,
    },
    beth_current_lender: {
      resolutionClass: RESOLUTION_CLASS.UNRESOLVED_INSUFFICIENT_EVIDENCE,
      resolvedValue: null,
      rationale:
        "Free sources establish historical Ramsfield acquisition financing and current CMBS loan terms, but do not independently name today's mortgagee for an HI FINANCED_BY edge. Paid escalation deferred: low resolveLikelihood vs identity-critical gaps already resolved; evidence-only CMBS note remains staged.",
      sources: [
        "https://www.ramsfieldrealestate.com/projects/bethesda-marriott",
        "https://cmbs.news/property/bethesda-marriott-bethesda-md/",
      ],
      paidEscalation: {
        considered: true,
        executed: false,
        reason: "diminishing_return_after_free_exhaustion_identity_priority_complete",
      },
    },
    beth_market_alerts_news: {
      resolutionClass: RESOLUTION_CLASS.UNRESOLVED_INSUFFICIENT_EVIDENCE,
      resolvedValue: null,
      rationale: "Genuine absence of property-linked Market Alerts — not a filter bug. Discovery optional; not blocking dry-run.",
      sources: ["HE-QL-038 Market Alerts diagnosis"],
    },
    guest_sentiment_licensing: {
      resolutionClass: RESOLUTION_CLASS.UNRESOLVED_PROVIDER_RESTRICTED,
      resolvedValue: null,
      rationale: "Live reputation persistence gated by licensing; staged founder-preview only. Not a routine factual ambiguity.",
      sources: ["HE-QL-038"],
      // Not REQUIRES_FOUNDER_DECISION — licensing is a known product gate already decided
    },
  };

  // Injected free-pass evidence overrides / fills (Cap Cana rooms, HGI)
  if (ev.resolutionClass) {
    return {
      issueId: issueRow.issueId,
      hotelKey: issueRow.hotelKey,
      claim: issueRow.claim,
      priority: issueRow.priority,
      researchActions: actions.concat(ev.researchActionsExtra || []),
      resolutionClass: ev.resolutionClass,
      resolvedValue: ev.resolvedValue,
      rationale: ev.rationale,
      sources: ev.sources || [],
      mutationHint: ev.mutationHint || null,
      paidSpendUsd: ev.paidSpendUsd || 0,
      requiresFounderDecision: ev.resolutionClass === RESOLUTION_CLASS.REQUIRES_FOUNDER_DECISION,
    };
  }

  const built = builtins[issueRow.issueId];
  if (built) {
    return {
      issueId: issueRow.issueId,
      hotelKey: issueRow.hotelKey,
      claim: issueRow.claim,
      priority: issueRow.priority,
      researchActions: actions.concat([
        { pass: 2, action: "free_authoritative_adjudication", result: built.resolutionClass },
      ]),
      ...built,
      paidSpendUsd: 0,
      requiresFounderDecision: built.resolutionClass === RESOLUTION_CLASS.REQUIRES_FOUNDER_DECISION,
    };
  }

  return {
    issueId: issueRow.issueId,
    hotelKey: issueRow.hotelKey,
    claim: issueRow.claim,
    priority: issueRow.priority,
    researchActions: actions,
    resolutionClass: RESOLUTION_CLASS.UNRESOLVED_INSUFFICIENT_EVIDENCE,
    resolvedValue: null,
    rationale: "No autonomous adjudication rule matched; continue other gaps",
    sources: [],
    paidSpendUsd: 0,
    requiresFounderDecision: false,
  };
}

/**
 * Free-pass evidence newly gathered this autonomous wave (no paid).
 * Cap Cana rooms 457; HGI rooms 138 HIGH.
 */
export const AUTONOMOUS_FREE_PASS_EVIDENCE_V1 = Object.freeze({
  cap_cana_rooms: {
    researchActions: [
      { pass: 1, action: "reuse_hpc_identity", result: "matched_secrets" },
      { pass: 2, action: "official_hyatt_inclusive_rooms_page", result: "457_suites" },
      { pass: 2, action: "meetings_fast_facts_corroboration", result: "457" },
      { pass: 2, action: "census_prior_hit_rooms", result: "457_aligned" },
    ],
    resolutionClass: RESOLUTION_CLASS.RESOLVED_VERIFIED,
    resolvedValue: 457,
    rationale:
      "Official Hyatt Inclusive Collection rooms + meetings pages state 457 suites; aligns with prior Hotel Census hit rooms=457 for Secrets Cap Cana. Not Dreams inventory.",
    sources: [
      "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/dominican-republic/cap-cana-resort-spa/accommodations/rooms-suites/",
      "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/dominican-republic/cap-cana-resort-spa/events/meetings-incentives/",
    ],
    mutationHint: {
      opId: "M-UPD-secrets_cap_cana-rooms",
      action: "UPDATE",
      field: "Rooms / Keys",
      value: 457,
      confidence: "HIGH",
      note: "Identity NO_OP retained; rooms null-fill is a separate READY mutation",
    },
    paidSpendUsd: 0,
  },
  hgi_rooms_confidence: {
    researchActions: [
      { pass: 1, action: "reuse_ellis_preserve", result: "138" },
      { pass: 2, action: "cvent_guest_rooms", result: "138" },
      { pass: 2, action: "hilton_events_page", result: "138_guest_rooms" },
      { pass: 2, action: "expedia_all_138_rooms", result: "138" },
    ],
    resolutionClass: RESOLUTION_CLASS.RESOLVED_VERIFIED,
    resolvedValue: 138,
    rationale:
      "Cvent (138 total; 66/66/5 mix), Hilton meetings page (138 guest rooms), Expedia, Ellis Preserve, and IMC construction (138) converge — upgrade MEDIUM→HIGH.",
    sources: [
      "https://www.cvent.com/venues/newtown-square/hotel/hilton-garden-inn-newtown-square-radnor/venue-80f98513-8db5-4046-a20c-7881d33eb20a",
      "https://www.hilton.com/en-gb/hotels/phlnsgi-hilton-garden-inn-newtown-square-radnor/events/",
      "https://newtownsquarehotels.com/hotels/hilton-garden-inn/",
    ],
    mutationHint: {
      opId: "M-NEW-hgi_newtown_square_radnor",
      field: "rooms",
      value: 138,
      confidence: "HIGH",
    },
    paidSpendUsd: 0,
  },
});

/**
 * Run autonomous reconciliation across all detected pilot issues.
 * @param {object} [opts]
 * @param {object} [opts.ledger]
 * @param {boolean} [opts.enablePaid=false]
 */
export function runAutonomousReconciliation(opts = {}) {
  const enablePaid = opts.enablePaid === true;
  const ledger =
    opts.ledger ||
    createHe50PilotBudgetLedger({
      globalUsdCap: HE50_GLOBAL_USD_CAP,
      runId: HE50_PILOT_RUN_ID,
      durablePath: opts.ledgerPath || null,
    });

  const issues = detectPilotIssues();
  const evidenceMap = { ...AUTONOMOUS_FREE_PASS_EVIDENCE_V1, ...(opts.extraEvidence || {}) };
  const results = [];

  for (const iss of issues) {
    const result = adjudicateIssue(iss, evidenceMap);
    ledger.recordFreePass({
      operation_id: `auto_recon_${iss.issueId}`,
      hotel_id: iss.hotelKey,
      pilot_key: iss.hotelKey,
      provider: "autonomous_reconciliation_free",
      research_category:
        iss.domain === "ownership" ? "ownership_stakeholders" : "property_identity_physical",
      verified_findings:
        String(result.resolutionClass).startsWith("RESOLVED")
          ? [iss.issueId]
          : [],
      result_class: result.resolutionClass,
      evidence_refs: result.sources || [],
    });

    // Paid escalation gate — only when unresolved, high value, enablePaid, cost-bounded
    if (
      enablePaid &&
      result.resolutionClass === RESOLUTION_CLASS.UNRESOLVED_INSUFFICIENT_EVIDENCE &&
      iss.resolveLikelihood >= 3 &&
      iss.commercialValue >= 4 &&
      iss.identityImpact >= 4
    ) {
      const est = 0.05; // example bounded lite search estimate
      const reserve = ledger.reserve({
        operation_id: `auto_paid_${iss.issueId}`,
        cost_usd: est,
        provider: "exa",
        hotel_id: iss.hotelKey,
        research_category: "property_identity_physical",
        allowCategoryReallocation: true,
        request_type: "bounded_lite_search",
        fields_researched: [iss.issueId],
      });
      if (reserve.ok) {
        // Do not execute unbounded live paid here without transport — release reservation
        ledger.release(`auto_paid_${iss.issueId}`, {
          result_class: "RELEASED_NO_TRANSPORT_IN_THIS_PASS",
        });
        result.paidEscalation = {
          considered: true,
          reservedThenReleased: true,
          estimatedMaxUsd: est,
          reason: "enablePaid true but live Exa transport not invoked this pass — free adjudication preferred",
        };
      }
    }

    results.push(result);
  }

  const byClass = {};
  for (const r of results) {
    byClass[r.resolutionClass] = (byClass[r.resolutionClass] || 0) + 1;
  }

  const founderOnly = results.filter((r) => r.requiresFounderDecision);
  const resolved = results.filter((r) => String(r.resolutionClass).startsWith("RESOLVED"));
  const unresolved = results.filter((r) => String(r.resolutionClass).startsWith("UNRESOLVED"));

  // Derived mutation amendments from autonomous resolutions
  const mutationAmendments = [];
  for (const r of results) {
    if (!r.mutationHint) continue;
    if (r.issueId === "cap_cana_rooms" && r.resolutionClass === RESOLUTION_CLASS.RESOLVED_VERIFIED) {
      mutationAmendments.push({
        id: "M-UPD-secrets_cap_cana-rooms",
        action: "UPDATE",
        canonicalEntityId: "recd7XAM6tLjzQhaI",
        field: "Rooms / Keys",
        existingValue: "empty_or_unenriched",
        proposedValue: 457,
        verificationStatus: "VERIFIED_FREE_AUTONOMOUS",
        founderRecommendation: "READY",
        supportingSource: (r.sources || []).join(" | "),
        reason: "Autonomous free pass: official 457 suites after identity verified ≠ Dreams",
      });
    }
  }

  return {
    contract: "HE_5_0_AUTONOMOUS_RECONCILIATION_RESULT_V1",
    version: AUTONOMOUS_RECON_VERSION,
    pilotVersion: HE50_PILOT_VERSION,
    runId: HE50_PILOT_RUN_ID,
    generatedAt: new Date().toISOString(),
    applyGate: {
      ...APPLY_GATE,
      stopMessage:
        "STOP — AWAITING SEPARATE FOUNDER AUTHORIZATION FOR PRODUCTION AIRTABLE APPLY.",
    },
    issuesDetected: issues.length,
    issuesInvestigated: results.length,
    byClass,
    resolvedCount: resolved.length,
    unresolvedCount: unresolved.length,
    founderDecisionCount: founderOnly.length,
    results,
    mutationAmendments,
    budget: ledger.snapshot(),
    paidPolicy: {
      enablePaid,
      globalCapUsd: HE50_GLOBAL_USD_CAP,
      note: "Paid only when free exhausted, cost-bounded, commercially justified, and budget remains",
    },
    stopReason: "apply_gate_only",
    note: "Routine factual ambiguities were adjudicated autonomously; founder gate is Airtable apply only.",
  };
}
