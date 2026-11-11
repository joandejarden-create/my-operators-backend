/**
 * Bethesda Scaling Companies — commercial completion audit (saved-evidence only).
 * No new paid providers. Reuses V5 mechanism classes + V4.1 contact ownership.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  DEMAND_MECHANISM_CLASS,
  assessDemandMechanism,
} from "./autonomous-demand-discovery-v5.js";
import {
  CONTACT_OWNERSHIP_CLASS,
  classifyContactOwnership,
} from "./commercial-spine-restoration-v41.js";

export const SCALING_COMMERCIAL_COMPLETION_V1 =
  "gdi-bethesda-scaling-companies-commercial-completion-v1";

export const COMMERCIAL_VERDICT = Object.freeze({
  PURSUE_NOW: "PURSUE_NOW",
  DEVELOP_ACCOUNT: "DEVELOP_ACCOUNT",
  MONITOR_FUTURE: "MONITOR_FUTURE",
  HOLD_RESEARCH: "HOLD_RESEARCH",
  EXCLUDE: "EXCLUDE",
});

/** Internal audit labels (not customer-facing statuses). */
export const CONTACT_AUDIT_CLASS = Object.freeze({
  VERIFIED_RELEVANT_BUYER: "VERIFIED_RELEVANT_BUYER",
  PROBABLE_RELEVANT_BUYER: "PROBABLE_RELEVANT_BUYER",
  VERIFIED_FUNCTIONAL_ROUTE: "VERIFIED_FUNCTIONAL_ROUTE",
  GENERAL_ORGANIZATION_ROUTE: "GENERAL_ORGANIZATION_ROUTE",
  UNVERIFIED_CONTACT: "UNVERIFIED_CONTACT",
  INVALID_OR_MISATTRIBUTED_CONTACT: "INVALID_OR_MISATTRIBUTED_CONTACT",
  MISSING: "MISSING",
});

export const EIGHT_CANONICAL = Object.freeze([
  {
    organizationName: "Miles & Stockbridge",
    candidateId: "scw2_7bb7730e224d",
    opportunityId: "gdi_sc_5c5d41eb670e6c7b",
    originalPubLabel: "PASS_PUBLICATION_READY_INTERNAL",
  },
  {
    organizationName: "Matthews",
    candidateId: "scw2_823fadd0fb3a",
    opportunityId: "gdi_sc_b36b66547864e8ce",
    originalPubLabel: "PASS_PUBLICATION_READY_INTERNAL",
  },
  {
    organizationName: "Goldman Edwards",
    candidateId: "scw2_550c83123c25",
    opportunityId: "gdi_sc_5cad926613ca1a01",
    originalPubLabel: "PASS_PUBLICATION_READY_INTERNAL",
  },
  {
    organizationName: "IMC Construction",
    candidateId: "scw2_2477186321d7",
    opportunityId: "gdi_sc_a5768a99d4e89875",
    originalPubLabel: "PASS_PUBLICATION_READY_INTERNAL",
  },
  {
    organizationName: "BioHub Maryland",
    candidateId: "scw2_c54880ef8fa8",
    opportunityId: "gdi_sc_e7dd46732dd5bb7a",
    originalPubLabel: "PASS_PUBLICATION_READY_INTERNAL",
  },
  {
    organizationName: "REGENXBIO",
    candidateId: "scw2_b05b2692dae3",
    opportunityId: "gdi_sc_9d14aee7ff250b11",
    originalPubLabel: "HOLD_CONTACT",
  },
  {
    organizationName: "MilliporeSigma",
    candidateId: "scw2_0cb3513c750e",
    opportunityId: "gdi_sc_32cb8ce0dfca4c5a",
    originalPubLabel: "HOLD_CONTACT",
  },
  {
    organizationName: "Risk Ledger",
    candidateId: "scw2_ed82c1673b67",
    opportunityId: "gdi_sc_20c58b2523659caf",
    originalPubLabel: "HOLD_CONTACT",
  },
]);

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const LIVE_DIR = path.join(
  ROOT,
  "reports/gdi-bethesda-scaling-companies-full-e2e-v1/live",
);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Map V4.1 ownership + live email quirks → internal contact audit class.
 */
export function auditContactRoute({
  email = null,
  ownershipClass = null,
  namedStakeholder = null,
  sourceUrl = "",
} = {}) {
  const em = text(email).toLowerCase();
  if (!em) {
    return {
      auditClass: CONTACT_AUDIT_CLASS.MISSING,
      salesUsableAsHotelSourcingRoute: false,
      note: "No contact route recovered",
    };
  }
  if (/^(press|media|pr)@/i.test(em) || /customersupport\.anz@/i.test(em)) {
    return {
      auditClass: CONTACT_AUDIT_CLASS.INVALID_OR_MISATTRIBUTED_CONTACT,
      salesUsableAsHotelSourcingRoute: false,
      note: "Press/media or wrong-region support mailbox — not a hotel-sourcing route",
      email: em,
    };
  }
  if (ownershipClass === CONTACT_OWNERSHIP_CLASS.FUNCTIONAL_DEPARTMENT) {
    return {
      auditClass: CONTACT_AUDIT_CLASS.VERIFIED_FUNCTIONAL_ROUTE,
      salesUsableAsHotelSourcingRoute: true,
      note: "Functional department inbox on organization domain",
      email: em,
    };
  }
  if (ownershipClass === CONTACT_OWNERSHIP_CLASS.POTENTIAL_DECISION_MAKER) {
    const title = text(namedStakeholder?.title || namedStakeholder?.responsibilityClass);
    const hotelRelevant = /travel|meeting|event|workplace|facilities|procurement|training|project|operations|housing/i.test(
      title,
    );
    return {
      auditClass: hotelRelevant
        ? CONTACT_AUDIT_CLASS.PROBABLE_RELEVANT_BUYER
        : CONTACT_AUDIT_CLASS.UNVERIFIED_CONTACT,
      salesUsableAsHotelSourcingRoute: hotelRelevant,
      note: hotelRelevant
        ? "Named person with hotel-relevant role language"
        : "Named/personish email without verified hotel-sourcing authority",
      email: em,
      personName: namedStakeholder?.name || null,
    };
  }
  if (
    ownershipClass === CONTACT_OWNERSHIP_CLASS.GENERAL_ORGANIZATION_CONTACT
    || /^(info|contact|hello|office|admin)@/i.test(em)
  ) {
    return {
      auditClass: CONTACT_AUDIT_CLASS.GENERAL_ORGANIZATION_ROUTE,
      salesUsableAsHotelSourcingRoute: false,
      note: "General organization inbox — usable for account intro only, not verified hotel sourcing",
      email: em,
    };
  }
  if (ownershipClass === CONTACT_OWNERSHIP_CLASS.UNVERIFIED_CONTACT_ROUTE) {
    return {
      auditClass: CONTACT_AUDIT_CLASS.UNVERIFIED_CONTACT,
      salesUsableAsHotelSourcingRoute: false,
      note: "Unverified contact ownership",
      email: em,
    };
  }
  if (/connectcre|bizjournals|prnewswire|commerce\.maryland/i.test(sourceUrl)) {
    return {
      auditClass: CONTACT_AUDIT_CLASS.UNVERIFIED_CONTACT,
      salesUsableAsHotelSourcingRoute: false,
      note: "Contact recovered from third-party reporting — verify on org-owned pages before outreach",
      email: em,
    };
  }
  return {
    auditClass: CONTACT_AUDIT_CLASS.UNVERIFIED_CONTACT,
    salesUsableAsHotelSourcingRoute: false,
    note: `Ownership class ${ownershipClass || "unknown"}`,
    email: em,
  };
}

/**
 * Commercial publication completeness — schema PASS is insufficient.
 * Requires ORS-style deep research marker + mechanism beyond office-only + usable route.
 */
export function assessScalingCommercialPublicationGate({
  mechanismClass = null,
  contactAuditClass = null,
  orsDeepResearchCompleted = false,
  relatedEntityFollowUpsExecuted = false,
  localConnectionSpecific = false,
  hotelUseSpecific = false,
} = {}) {
  const fail = [];
  if (!orsDeepResearchCompleted) {
    fail.push("ORS_DEEP_RESEARCH_NOT_EXECUTED");
  }
  if (mechanismClass === DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND) {
    fail.push("UNSUPPORTED_HOTEL_DEMAND");
  }
  if (!localConnectionSpecific) fail.push("LOCAL_CONNECTION_GENERIC_OR_MISSING");
  if (!hotelUseSpecific) fail.push("HOTEL_USE_SPECULATIVE_OR_MISSING");
  if (
    !contactAuditClass
    || contactAuditClass === CONTACT_AUDIT_CLASS.MISSING
    || contactAuditClass === CONTACT_AUDIT_CLASS.INVALID_OR_MISATTRIBUTED_CONTACT
  ) {
    fail.push("NO_LEGITIMATE_CONTACT_ROUTE");
  }
  // GENERAL org inbox alone cannot certify customer/internal publication PASS
  if (contactAuditClass === CONTACT_AUDIT_CLASS.GENERAL_ORGANIZATION_ROUTE) {
    fail.push("GENERAL_INBOX_NOT_HOTEL_SOURCING_ROUTE");
  }
  if (contactAuditClass === CONTACT_AUDIT_CLASS.UNVERIFIED_CONTACT) {
    fail.push("CONTACT_UNVERIFIED");
  }

  const commerciallyComplete = fail.length === 0;
  const accountDevelopmentEligible =
    mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND
    || mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND;

  return {
    commerciallyPublicationReady: commerciallyComplete,
    accountDevelopmentEligible,
    failReasons: fail,
    note:
      "Schema/Wyndham field completeness ≠ commercial publication readiness. "
      + "GENERAL_ORGANIZATION_ROUTE supports DEVELOP_ACCOUNT intro, not PASS.",
    relatedEntityFollowUpsExecuted: relatedEntityFollowUpsExecuted === true,
  };
}

/**
 * Curated commercial overlay from saved live evidence (no invented facts).
 * Maps office/facility/training triggers → hotel-demand classes honestly.
 */
export function curatedCommercialAssessment(organizationName, liveRow = {}) {
  const org = text(organizationName);
  const activity = text(
    liveRow.salesBrief?.activity
      || liveRow.opportunityPayload?.localConnection
      || liveRow.opportunityPayload?.datedGrowthEvidence
      || liveRow.opportunityPayload?.programName,
  );
  const hotelUse = text(
    liveRow.salesBrief?.whyBethesda
      || liveRow.opportunityPayload?.proposedHotelUse,
  );
  const sourceUrl = text(
    liveRow.salesBrief?.sources?.[0]
      || liveRow.opportunityPayload?.officialSource
      || liveRow.opportunityPayload?.sources?.[0]?.url,
  );

  // Build V5 mechanism input with explicit hotel-relevant language only when evidence supports it
  const base = {
    organizationName: org,
    programName: activity,
    demandMechanism: activity,
    demandSignalSummary: activity,
    sourceUrl,
  };

  // Expert overlay: distinguish trigger vs hotel demand using stored evidence only
  const overlays = {
    "BioHub Maryland": {
      demandMechanism:
        "Montgomery County Training & Education Center — biopharma workforce training with mock GMP labs; instructors and partner-company participants may require lodging/meeting overflow near Rockville/Bethesda",
      mechanismOverride: DEMAND_MECHANISM_CLASS.INFERRED_DEMAND,
      verifiedVsInferred:
        "VERIFIED: physical training center exists. INFERRED: trainee/instructor overnight and offsite meeting demand (not stated as hotel contracts).",
      verdict: COMMERCIAL_VERDICT.DEVELOP_ACCOUNT,
      hotelFit: "STRONG_CORRIDOR_FIT",
      liveResearchPriority: "HIGH",
      unanswered: [
        "Training calendar / cohort dates and class sizes",
        "Whether participants travel from outside Montgomery County",
        "Program ops / logistics owner (not only info@)",
        "Any existing hotel preferred-supplier relationships",
      ],
      briefJustified: true,
    },
    "Miles & Stockbridge": {
      demandMechanism:
        "North Bethesda office opening (Sep 2025) — business trigger only; no public evidence of client events, attorney travel lodging, or firm training at hotels",
      mechanismOverride: DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND,
      verifiedVsInferred:
        "VERIFIED: office opened North Bethesda. UNSUPPORTED as hotel demand: visiting attorney / client-event thesis not evidenced.",
      verdict: COMMERCIAL_VERDICT.HOLD_RESEARCH,
      hotelFit: "GEOGRAPHIC_PROXIMITY_ONLY",
      liveResearchPriority: "MEDIUM",
      unanswered: [
        "Evidence of multi-office attorney travel into North Bethesda",
        "Firm retreats, CLEs, client seminars using hotels",
        "Role of bbernstein@ (press vs workplace/admin)",
        "Office managing partner / admin contact for facilities",
      ],
      briefJustified: false,
      briefDeferReason:
        "Office opening without evidenced hotel-demand mechanism — brief would overstate sales opportunity",
    },
    Matthews: {
      demandMechanism:
        "Bethesda office opening to expand DC presence — expansion trigger; no documented recurring meetings, training, or visiting-team lodging",
      mechanismOverride: DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND,
      verifiedVsInferred:
        "VERIFIED: Bethesda office announced. UNSUPPORTED: hotel use thesis is speculative travel from expansion.",
      verdict: COMMERCIAL_VERDICT.HOLD_RESEARCH,
      hotelFit: "GEOGRAPHIC_PROXIMITY_ONLY",
      liveResearchPriority: "MEDIUM",
      unanswered: [
        "Nature of Matthews business line and client meeting pattern",
        "National vs local staffing of Bethesda office",
        "Workplace / travel / meetings owner beyond contact@",
      ],
      briefJustified: false,
      briefDeferReason: "Expansion announcement without hotel-demand evidence",
    },
    "Goldman Edwards": {
      demandMechanism:
        "HQ relocation to Bethesda adjacent to Marriott International HQ — relocation trigger; adjacency to Marriott Int’l is not Bethesda Marriott room demand",
      mechanismOverride: DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND,
      verifiedVsInferred:
        "VERIFIED: HQ relocation announcement. UNSUPPORTED: visiting teams/training/external meetings not evidenced; Marriott Int’l adjacency ≠ Bethesda Marriott capture.",
      verdict: COMMERCIAL_VERDICT.MONITOR_FUTURE,
      hotelFit: "WEAK_OR_COMPETITIVE_CONFUSION",
      liveResearchPriority: "LOW",
      unanswered: [
        "Post-move visiting consultant / client lodging patterns",
        "Whether firm uses nearby hotels for overflow",
      ],
      briefJustified: false,
      briefDeferReason:
        "Relocation without hotel-demand mechanism; risk of confusing Marriott International HQ with Bethesda Marriott property",
    },
    "IMC Construction": {
      demandMechanism:
        "New Bethesda regional hub for DC-area construction operations — project mobilization and visiting trade/project staff are a credible inferred lodging pattern for contractors; specific project room blocks not verified",
      mechanismOverride: DEMAND_MECHANISM_CLASS.INFERRED_DEMAND,
      verifiedVsInferred:
        "VERIFIED: Bethesda regional hub opening. INFERRED: visiting project-team lodging from multi-site construction operations. NOT VERIFIED: Dan Baker as hotel buyer; specific projects/room nights.",
      verdict: COMMERCIAL_VERDICT.DEVELOP_ACCOUNT,
      hotelFit: "CREDIBLE_PROJECT_CORRIDOR_FIT",
      liveResearchPriority: "HIGH",
      unanswered: [
        "Active Bethesda/DC project list generating overnight stays",
        "Dan Baker exact title and hotel-sourcing relevance (currently influencer/entry)",
        "Project controls / travel / procurement contacts",
        "Typical crew lodging practices (extended stay vs full-service)",
      ],
      briefJustified: true,
    },
    REGENXBIO: {
      demandMechanism:
        "Rockville HQ and GMP facility in Montgomery County life-sciences corridor — recurring scientific/technical visitors, training, and project mobilization are credible inferred demand; lodging volumes unverified",
      mechanismOverride: DEMAND_MECHANISM_CLASS.INFERRED_DEMAND,
      verifiedVsInferred:
        "VERIFIED: Rockville HQ/GMP campus exists. INFERRED: visitor/specialist lodging and meetings. NOT VERIFIED: hotel contracts or named travel buyer.",
      verdict: COMMERCIAL_VERDICT.HOLD_RESEARCH,
      hotelFit: "STRONG_CORRIDOR_FIT",
      liveResearchPriority: "HIGH",
      unanswered: [
        "Visitor/training/meeting patterns at Rockville campus",
        "Corporate travel / facilities / workplace owner",
        "Legitimate contact route (none recovered live)",
        "Whether primary segment should remain scaling_companies vs corporate_meetings",
      ],
      briefJustified: true,
      briefNote: "Account-development brief only — no contact route yet",
    },
    MilliporeSigma: {
      demandMechanism:
        "Rockville biosafety testing facility expansion/consolidation — commissioning, specialist travel, and technical collaboration visits are credible inferred demand; ANZ support mailbox is invalid for Bethesda outreach",
      mechanismOverride: DEMAND_MECHANISM_CLASS.INFERRED_DEMAND,
      verifiedVsInferred:
        "VERIFIED: Rockville biosafety facility activity reported. INFERRED: project/specialist lodging. INVALID: customersupport.anz@ as sourcing route.",
      verdict: COMMERCIAL_VERDICT.HOLD_RESEARCH,
      hotelFit: "STRONG_CORRIDOR_FIT",
      liveResearchPriority: "HIGH",
      primarySegment: "scaling_companies",
      segmentNote:
        "Facility expansion fits scaling_companies standing account; if institutional campus patterns dominate, keep scaling as primary with corporate_meetings secondary — do not invent institutional account status",
      unanswered: [
        "US/Rockville facilities or travel contact (not ANZ)",
        "Commissioning / training schedule producing overnight demand",
        "Existing preferred hotels for Rockville visitors",
      ],
      briefJustified: true,
      briefNote: "Brief justified for internal pursuit planning; contact HOLD",
    },
    "Risk Ledger": {
      demandMechanism:
        "Maryland office announcement (state commerce press) without verified Bethesda/Rockville operating footprint or hotel-relevant activity in stored evidence",
      mechanismOverride: DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND,
      verifiedVsInferred:
        "ANNOUNCEMENT ONLY: Maryland office plans. Local connection in live artifact was generic DMV wording. press@ invalid. No defensible Bethesda Marriott demand mechanism in saved evidence.",
      verdict: COMMERCIAL_VERDICT.EXCLUDE,
      hotelFit: "INSUFFICIENT_LOCAL_CONNECTION",
      liveResearchPriority: "NONE",
      unanswered: ["Confirm whether Maryland office opened and where"],
      briefJustified: false,
      briefDeferReason:
        "No defensible Bethesda demand mechanism; announcement-only local connection",
    },
  };

  const overlay = overlays[org] || {
    demandMechanism: activity,
    mechanismOverride: null,
    verifiedVsInferred: "Insufficient curated overlay",
    verdict: COMMERCIAL_VERDICT.HOLD_RESEARCH,
    hotelFit: "UNKNOWN",
    liveResearchPriority: "LOW",
    unanswered: ["Complete evidence review"],
    briefJustified: false,
    briefDeferReason: "Unknown organization",
  };

  const mechInput = {
    ...base,
    demandMechanism: overlay.demandMechanism,
    programName: overlay.demandMechanism,
    demandSignalSummary: overlay.demandMechanism,
  };
  const auto = assessDemandMechanism(mechInput);
  const mechanismClass =
    overlay.mechanismOverride || auto.mechanismClass;

  return {
    organizationName: org,
    sourceUrl,
    activitySnippet: activity.slice(0, 280),
    hotelUseSnippet: hotelUse.slice(0, 280),
    mechanismClass,
    autoMechanismClass: auto.mechanismClass,
    verifiedVsInferred: overlay.verifiedVsInferred,
    commercialVerdict: overlay.verdict,
    hotelFit: overlay.hotelFit,
    liveResearchPriority: overlay.liveResearchPriority,
    unansweredQuestions: overlay.unanswered || [],
    briefJustified: overlay.briefJustified === true,
    briefDeferReason: overlay.briefDeferReason || null,
    briefNote: overlay.briefNote || null,
    primarySegment: overlay.primarySegment || "scaling_companies",
    segmentNote: overlay.segmentNote || null,
    demandMechanismNarrative: overlay.demandMechanism,
  };
}

export function loadLiveDeepRows() {
  const p = path.join(LIVE_DIR, "05-full-deep-research-findings.json");
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function reconstructResearchTrace(meta, liveRow) {
  const deep = liveRow?.deepResearch || {};
  const sources = []
    .concat(liveRow?.salesBrief?.sources || [])
    .concat(
      (liveRow?.opportunityPayload?.sources || []).map((s) => s.url || s),
    )
    .filter(Boolean);
  const uniqueSources = [...new Set(sources.map(String))];

  const orsExecuted = liveRow?.opportunityPayload?.orsDeepResearchCompleted === true
    || liveRow?.orsDeepResearchCompleted === true
    || liveRow?.deepResearch?.orsCompleted === true;

  const stopReason = orsExecuted
    ? "ORS_COMPLETED_OR_BUDGET"
    : "LIVE_ORCHESTRATOR_BYPASSED_ORS_SHALLOW_FETCH_THEN_SCHEMA_GATE";

  return {
    organizationName: meta.organizationName,
    candidateId: meta.candidateId,
    opportunityId: meta.opportunityId,
    originalPublicationLabel: meta.originalPubLabel,
    originalDiscoverySource: uniqueSources[0] || null,
    urlsFetchedOrCited: uniqueSources,
    pageTextExtractedChars: Number(deep.pageChars || 0),
    independentSourcesExamined: uniqueSources.length,
    relatedEntityLeadsGenerated: 0,
    relatedEntityLeadsExecuted: 0,
    buyerWhoStatus: deep.whoStatus || null,
    namedStakeholders: deep.namedStakeholders || [],
    contactExtracted: deep.contactEmail || null,
    researchQuestionsRemaining: true,
    actualStoppingReason: stopReason,
    actualSpendUsd: Number(deep.costUsd || 0),
    orsDeepResearchCompleted: orsExecuted,
    technicalWorkflowComplete: Boolean(liveRow?.scalingCardOk),
    commercialResearchComplete: false,
    furtherInvestigationValue:
      meta.organizationName === "Risk Ledger"
        ? "LOW_OR_NONE"
        : meta.organizationName === "Goldman Edwards"
          ? "LOW"
          : "POSITIVE",
    defectFlags: orsExecuted
      ? []
      : [
          "ORS_NOT_INVOKED",
          "NO_RELATED_ENTITY_DRAIN",
          "NO_EVIDENCE_DRIVEN_REPLAN",
          "SCHEMA_PASS_WITHOUT_COMMERCIAL_DEPTH",
        ],
  };
}

export function auditEightCandidates({ deepRows = null } = {}) {
  const rows = deepRows || loadLiveDeepRows();
  const byName = new Map(rows.map((r) => [r.organizationName, r]));
  const audits = [];

  for (const meta of EIGHT_CANONICAL) {
    const live = byName.get(meta.organizationName) || null;
    const trace = reconstructResearchTrace(meta, live);
    const commercial = curatedCommercialAssessment(meta.organizationName, live || {});

    const email = live?.deepResearch?.contactEmail || null;
    const sourceUrl = trace.originalDiscoverySource || "";
    const domainGuess = (() => {
      try {
        return new URL(sourceUrl).hostname.replace(/^www\./, "");
      } catch {
        return "";
      }
    })();
    const ownership = classifyContactOwnership({
      email,
      pageUrl: sourceUrl,
      organizationName: meta.organizationName,
      organizationDomain: domainGuess,
      personName: live?.deepResearch?.namedStakeholders?.[0]?.name || null,
      roleTitle: live?.deepResearch?.namedStakeholders?.[0]?.title || null,
      publisherKind: /connectcre|bizjournals|commerce\.maryland|ewingcole|savills/i.test(
        sourceUrl,
      )
        ? "INDEPENDENT_NEWS"
        : "ORGANIZATION_OWNED",
    });
    // Force invalid for known bad mailboxes regardless of ownership heuristic
    let contactAudit = auditContactRoute({
      email,
      ownershipClass: ownership.ownershipClass,
      namedStakeholder: live?.deepResearch?.namedStakeholders?.[0] || null,
      sourceUrl,
    });
    if (/press@|customersupport\.anz@/i.test(email || "")) {
      contactAudit = auditContactRoute({
        email,
        ownershipClass: CONTACT_OWNERSHIP_CLASS.UNVERIFIED_CONTACT_ROUTE,
        sourceUrl,
      });
    }
    // Personish emails on press pages without role → UNVERIFIED
    if (
      meta.organizationName === "Miles & Stockbridge"
      && /bbernstein@/i.test(email || "")
    ) {
      contactAudit = {
        auditClass: CONTACT_AUDIT_CLASS.UNVERIFIED_CONTACT,
        salesUsableAsHotelSourcingRoute: false,
        note: "Personish email from office-opening press context — role/hotel-sourcing authority not verified",
        email,
      };
    }
    // IMC Dan Baker is influencer per live WHO
    if (meta.organizationName === "IMC Construction") {
      contactAudit = {
        auditClass: CONTACT_AUDIT_CLASS.GENERAL_ORGANIZATION_ROUTE,
        salesUsableAsHotelSourcingRoute: false,
        note: "info@ is general org route; Dan Baker classified REFERRAL/INFLUENCER — not verified hotel buyer",
        email: email || "info@imcconstruction.com",
        namedPerson: "Dan Baker",
        namedPersonClass: "INFLUENCER_OR_GENERAL",
      };
    }

    const localSpecific = !/expansion\/office activity reported in the DMV market near Bethesda Marriott/i.test(
      commercial.activitySnippet,
    )
      && /bethesda|rockville|montgomery|north bethesda|gaithersburg|silver spring/i.test(
        commercial.activitySnippet,
      );
    const hotelUseSpecific = /training|project|visiting|meeting|lodging|commission|cohort|instructor|specialist/i.test(
      commercial.hotelUseSnippet + commercial.demandMechanismNarrative,
    )
      && commercial.mechanismClass !== DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND;

    const pubGate = assessScalingCommercialPublicationGate({
      mechanismClass: commercial.mechanismClass,
      contactAuditClass: contactAudit.auditClass,
      orsDeepResearchCompleted: trace.orsDeepResearchCompleted,
      relatedEntityFollowUpsExecuted: false,
      localConnectionSpecific: localSpecific,
      hotelUseSpecific,
    });

    const originalPass = meta.originalPubLabel === "PASS_PUBLICATION_READY_INTERNAL";
    const stillDeservesCommercialPass = pubGate.commerciallyPublicationReady === true;
    const correctedPubLabel = stillDeservesCommercialPass
      ? "PASS_PUBLICATION_READY_INTERNAL"
      : pubGate.accountDevelopmentEligible
        ? "HOLD_ACCOUNT_DEVELOPMENT"
        : commercial.commercialVerdict === COMMERCIAL_VERDICT.EXCLUDE
          ? "EXCLUDE"
          : commercial.commercialVerdict === COMMERCIAL_VERDICT.MONITOR_FUTURE
            ? "HOLD_MONITOR"
            : "HOLD_RESEARCH";

    audits.push({
      ...meta,
      trace,
      commercial,
      contactOwnership: ownership,
      contactAudit,
      publicationGate: pubGate,
      originalPass,
      stillDeservesCommercialPass,
      correctedPublicationLabel: correctedPubLabel,
      structuralSchemaComplete: live?.scalingCardOk === true,
      commerciallyComplete: pubGate.commerciallyPublicationReady,
    });
  }

  return audits;
}

export function buildCommercialFunnel(audits) {
  const count = (pred) => audits.filter(pred).length;
  const pubReady = count((a) => a.stillDeservesCommercialPass);
  return {
    existingNetNewFromPriorCampaign: 8,
    fullyResearchedCommercially: count((a) => a.commerciallyComplete),
    researchBlockedOrIncomplete: count((a) => !a.trace.orsDeepResearchCompleted),
    verifiedHotelDemand: count(
      (a) => a.commercial.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND,
    ),
    credibleInferredDemand: count(
      (a) => a.commercial.mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND,
    ),
    unsupportedDemand: count(
      (a) => a.commercial.mechanismClass === DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND,
    ),
    validBethesdaFit: count((a) =>
      /STRONG|CREDIBLE/i.test(a.commercial.hotelFit),
    ),
    verifiedRelevantBuyers: count((a) =>
      /VERIFIED_RELEVANT|PROBABLE_RELEVANT|VERIFIED_FUNCTIONAL/i.test(
        a.contactAudit.auditClass,
      ),
    ),
    legitimateContactRoutesIncludingGeneral: count((a) =>
      a.contactAudit.auditClass
      && a.contactAudit.auditClass !== CONTACT_AUDIT_CLASS.MISSING
      && a.contactAudit.auditClass !== CONTACT_AUDIT_CLASS.INVALID_OR_MISATTRIBUTED_CONTACT,
    ),
    hotelSourcingUsableRoutes: count(
      (a) => a.contactAudit.salesUsableAsHotelSourcingRoute === true,
    ),
    pursueNow: count((a) => a.commercial.commercialVerdict === COMMERCIAL_VERDICT.PURSUE_NOW),
    developAccount: count(
      (a) => a.commercial.commercialVerdict === COMMERCIAL_VERDICT.DEVELOP_ACCOUNT,
    ),
    monitorFuture: count(
      (a) => a.commercial.commercialVerdict === COMMERCIAL_VERDICT.MONITOR_FUTURE,
    ),
    holdResearch: count(
      (a) => a.commercial.commercialVerdict === COMMERCIAL_VERDICT.HOLD_RESEARCH,
    ),
    exclude: count((a) => a.commercial.commercialVerdict === COMMERCIAL_VERDICT.EXCLUDE),
    structurallyComplete: count((a) => a.structuralSchemaComplete),
    commerciallyComplete: count((a) => a.commerciallyComplete),
    publicationEligible: pubReady,
    originalPassCount: count((a) => a.originalPass),
    originalPassStillDeserved: count((a) => a.originalPass && a.stillDeservesCommercialPass),
    priorCampaignSpendUsd: 0.194,
    costPerPublicationReady: pubReady
      ? "N/A_THIS_AUDIT_ZERO_PAID"
      : "UNDEFINED_ZERO_DENOMINATOR",
    note: "Prior $0.194 proved discovery+schema packing, not five commercially complete opportunities.",
  };
}

/**
 * Detect premature commercial stop in scaling live orchestrator results.
 */
export function detectPrematureScalingResearchStop(liveRow = {}) {
  const ors = liveRow?.orsDeepResearchCompleted === true
    || liveRow?.deepResearch?.orsCompleted === true;
  const schemaOk = liveRow?.scalingCardOk === true;
  const who = liveRow?.deepResearch?.whoStatus;
  const relatedExecuted = Number(liveRow?.deepResearch?.relatedEntityLeadsExecuted || 0) > 0;
  const reasons = [];
  if (!ors && schemaOk) reasons.push("SCHEMA_PASS_WITHOUT_ORS");
  if (!ors) reasons.push("ORS_BYPASSED");
  if (!relatedExecuted) reasons.push("RELATED_ENTITY_FOLLOWUPS_NOT_EXECUTED");
  if (who === "PAGES_SCANNED_NO_NAMED_STAKEHOLDER" || who === "ZERO_PAGES_SCANNED") {
    reasons.push("BUYER_WHO_INCOMPLETE");
  }
  if (/info@|contact@|hello@/i.test(liveRow?.deepResearch?.contactEmail || "")) {
    reasons.push("STOPPED_ON_GENERAL_INBOX");
  }
  return {
    premature: reasons.includes("SCHEMA_PASS_WITHOUT_ORS") || reasons.includes("ORS_BYPASSED"),
    reasons,
  };
}
