#!/usr/bin/env node
/**
 * P0 Wyndham opportunity-loss forensics — dry-run only.
 * No paid providers, Surfe, Agent, Airtable writes, or STAGED/CURRENT mutation.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  evaluateFinalizedQualifiedOpportunity,
  evaluatePersonSpecificOutreachChannel,
  evaluateCommercialContactNameQuality,
  hasRecurringRelationshipPathway,
  FQO_GATE,
} from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import { filterValidatedFqoPersistenceWriteSet } from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import { quoteExaAgentRun, EXA_AGENT_EFFORT } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";
import { quoteBenchmarkCall } from "../lib/group-demand-intelligence/research/benchmark-provider-pricing-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const CLEAN11 = "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_3283ee92";
const CAND59 = "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_fb10174d";
const LIVE_OUT = path.join(
  ROOT,
  "reports/gdi-wyndham-sd-bayside-full-e2e-25plus-v1/execute-live-35usd",
);
const OUT = path.join(
  ROOT,
  "reports/gdi-wyndham-sd-bayside-opportunity-loss-forensics-v1",
);

const PRIMARY = Object.freeze({
  NOT_REAL_HOTEL_DEMAND: "NOT_REAL_HOTEL_DEMAND",
  HOTEL_FIT_UNSUPPORTED: "HOTEL_FIT_UNSUPPORTED",
  VALID_DEMAND_INCOMPLETE_WHO: "VALID_DEMAND_INCOMPLETE_WHO",
  VALID_DEMAND_INCOMPLETE_HOW: "VALID_DEMAND_INCOMPLETE_HOW",
  VALID_DEMAND_INCOMPLETE_PATHWAY: "VALID_DEMAND_INCOMPLETE_PATHWAY",
  RESEARCH_EXTRACTION_DEFECT: "RESEARCH_EXTRACTION_DEFECT",
  CONTACT_LINEAGE_LOSS: "CONTACT_LINEAGE_LOSS",
  FALSE_NEGATIVE_FQO_RULE: "FALSE_NEGATIVE_FQO_RULE",
  VALID_COMMERCIAL_REJECTION: "VALID_COMMERCIAL_REJECTION",
  DUPLICATE_OR_HISTORICAL: "DUPLICATE_OR_HISTORICAL",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(data, null, 2)}\n`);
}

const store = createGdiPublicationPackageStore();
const clean = store.loadPackage(HOTEL_ID, CLEAN11).package.opportunities || [];
const cand = store.loadPackage(HOTEL_ID, CAND59).package.opportunities || [];
const cleanIds = new Set(clean.map((o) => o.opportunityId || o.id));
const cleanById = new Map(clean.map((o) => [o.opportunityId || o.id, o]));

const strict12Path = path.join(LIVE_OUT, "14-STRICT_COMMERCIAL_FQO.json");
const strict12 = JSON.parse(fs.readFileSync(strict12Path, "utf8"));
const strict12Ids = new Set((strict12.passRows || []).map((r) => r.id));

const universeSnap = JSON.parse(
  fs.readFileSync(path.join(LIVE_OUT, "05-UNIVERSE_SNAPSHOT.json"), "utf8"),
);
const spend = JSON.parse(
  fs.readFileSync(path.join(LIVE_OUT, "00-SPEND_LEDGER.json"), "utf8"),
);

function lodgingBlob(o) {
  return text(
    [
      o.hotelOpportunityThesis,
      o.whyThisHotel,
      o.whatIsHappening,
      o.summaryWhat,
      o.potentialBusinessForThisHotel,
      o.title,
    ]
      .filter(Boolean)
      .join(" "),
  );
}

function hasLodgingEvidence(o) {
  return /\b(lodging|housing|hotel|rooms?|guestrooms?|overflow|meeting|catering|banquet|block|preferred|partner|referral|extended.?stay|project.?team|training)\b/i.test(
    lodgingBlob(o),
  );
}

function demandCredible(o, fqo) {
  const demand = text(
    o.organizationName || o.organization || o.programmeName || o.title,
  );
  if (!demand || demand.length < 3) return false;
  if (/frequently asked|our story|global gifting|^national$|^housing/i.test(demand)) {
    // weak program title alone is not automatic non-demand
  }
  return (
    fqo.gates?.[FQO_GATE.EVIDENCE_BACKED_DEMAND]?.ok === true ||
    (fqo.evidenceUrls || []).length > 0 ||
    hasLodgingEvidence(o)
  );
}

function assessDimensions(o, fqo, channel, nameQ) {
  const A = demandCredible(o, fqo);
  const B = fqo.gates?.[FQO_GATE.HOTEL_GEO_COMMERCIAL_FIT]?.ok === true;
  const C = fqo.gates?.[FQO_GATE.FUTURE_SOURCING_PATHWAY]?.ok === true;
  const D =
    nameQ.ok === true &&
    fqo.gates?.[FQO_GATE.NAMED_COMMERCIAL_PERSON]?.ok === true;
  const E = channel.ok === true;
  const F = Boolean(
    text(o.recommendedNextStep || o.recommendedNextAction || o.recommendedAction),
  );
  return {
    A_demandCredibility: A,
    B_hotelFit: B,
    C_futureOrRecurringPathway: C,
    D_contactIdentity: D,
    E_usableOutreach: E,
    F_researchCompletenessAction: F,
    maturity:
      A && B && C && D && E && F
        ? "SALES_READY_FQO_CANDIDATE"
        : A && B && C && (!D || !E)
          ? "VERIFIED_DEMAND_CONTACT_RESEARCH_REQUIRED"
          : A && B && !C
            ? "VERIFIED_DEMAND_PATHWAY_INCOMPLETE"
            : A && !B
              ? "DEMAND_PRESENT_HOTEL_FIT_UNSUPPORTED"
              : !A
                ? "NOT_CREDIBLE_OR_INCOMPLETE_DEMAND"
                : "IDENTIFICATION_OR_RESEARCH_REQUIRED",
  };
}

function classifyPrimary(row) {
  const reasons = row.fqoReasons || [];
  const dims = row.dimensions;
  const secondary = [];

  // Duplicates of CLEAN 11 that appear as "new" near-miss listings
  if (row.inClean11 && row.strict12Pass) {
    return {
      primary: PRIMARY.DUPLICATE_OR_HISTORICAL,
      secondary: [],
      note: "CLEAN 11 certified row — not a net-new commercial loss",
    };
  }

  if (
    /NON_PERSON|ROLE_GLUED|JUNK_CONTACT|FALSE_PERSON/i.test(
      `${reasons.join(" ")} ${row.nameQuality}`,
    )
  ) {
    if (dims.A_demandCredibility && dims.B_hotelFit) {
      return {
        primary: PRIMARY.RESEARCH_EXTRACTION_DEFECT,
        secondary: [PRIMARY.VALID_DEMAND_INCOMPLETE_WHO],
        note: "Credible demand likely; WHO field polluted by scrape",
      };
    }
    return {
      primary: PRIMARY.RESEARCH_EXTRACTION_DEFECT,
      secondary: [],
      note: "Non-person / role-glued extraction",
    };
  }

  if (!dims.A_demandCredibility) {
    return {
      primary: PRIMARY.NOT_REAL_HOTEL_DEMAND,
      secondary: [],
      note: "Insufficient demand generator / lodging evidence",
    };
  }

  if (!dims.B_hotelFit) {
    return {
      primary: PRIMARY.HOTEL_FIT_UNSUPPORTED,
      secondary: dims.A_demandCredibility
        ? [PRIMARY.VALID_DEMAND_INCOMPLETE_PATHWAY]
        : [],
      note: "Demand may exist but hotel-fit thesis missing/negative",
    };
  }

  // Valid demand + fit, blocked on WHO/HOW/pathway
  if (dims.A_demandCredibility && dims.B_hotelFit) {
    if (!dims.D_contactIdentity && !dims.E_usableOutreach) {
      return {
        primary: PRIMARY.VALID_DEMAND_INCOMPLETE_WHO,
        secondary: [PRIMARY.VALID_DEMAND_INCOMPLETE_HOW],
        note: "Do not treat as commercially unsuitable — contact research incomplete",
      };
    }
    if (!dims.D_contactIdentity) {
      return {
        primary: PRIMARY.VALID_DEMAND_INCOMPLETE_WHO,
        secondary: [],
        note: "Named stakeholder missing or unverified",
      };
    }
    if (!dims.E_usableOutreach) {
      return {
        primary: PRIMARY.VALID_DEMAND_INCOMPLETE_HOW,
        secondary: [],
        note: "Functional/unlabeled channel — demand still valid",
      };
    }
    if (!dims.C_futureOrRecurringPathway) {
      // Potential false negative if recurring demand type with lodging evidence
      if (row.recurringEligible && row.hasLodging) {
        return {
          primary: PRIMARY.FALSE_NEGATIVE_FQO_RULE,
          secondary: [PRIMARY.VALID_DEMAND_INCOMPLETE_PATHWAY],
          note: "Recurring demand type with lodging evidence — pathway gate may over-require dated event (post-fix recurring pathway may recover)",
        };
      }
      return {
        primary: PRIMARY.VALID_DEMAND_INCOMPLETE_PATHWAY,
        secondary: [],
        note: "Need future/recurring sourcing evidence",
      };
    }
    if (reasons.includes("COMPETITIVE_INCUMBENT_OR_PATHWAY_UNVERIFIED")) {
      return {
        primary: PRIMARY.VALID_DEMAND_INCOMPLETE_PATHWAY,
        secondary: [],
        note: "Competitive incumbent/pathway unverified",
      };
    }
  }

  if (row.inClean11 && !row.strict12Pass) {
    return {
      primary: PRIMARY.CONTACT_LINEAGE_LOSS,
      secondary: reasons.map((r) => r),
      note: "CLEAN 11 row failed commercial scrub — investigate lineage",
    };
  }

  return {
    primary: PRIMARY.VALID_COMMERCIAL_REJECTION,
    secondary,
    note: reasons.join("|") || "blocked",
  };
}

function recommendedResolution(row) {
  switch (row.rootCause.primary) {
    case PRIMARY.RESEARCH_EXTRACTION_DEFECT:
      return "Re-extract WHO from source pages; discard non-person scrapes; Agent structured contact schema preferred";
    case PRIMARY.VALID_DEMAND_INCOMPLETE_WHO:
      return "Targeted WHO research (Agent medium or Surfe explicit-click) — preserve demand as VERIFIED_DEMAND_CONTACT_RESEARCH_REQUIRED";
    case PRIMARY.VALID_DEMAND_INCOMPLETE_HOW:
      return "Verify person-direct channel or label org route; do not present functional inbox as personal";
    case PRIMARY.VALID_DEMAND_INCOMPLETE_PATHWAY:
      return "Confirm next-cycle / recurring / overflow evidence; do not invent RFP dates";
    case PRIMARY.FALSE_NEGATIVE_FQO_RULE:
      return "Ensure recurringRelationshipPathway + lodging evidence fields are populated (code path already added in PR #604)";
    case PRIMARY.HOTEL_FIT_UNSUPPORTED:
      return "Write hotel-specific thesis or reject as wrong-market";
    case PRIMARY.NOT_REAL_HOTEL_DEMAND:
      return "Preserve as historical/rejected research; do not force FQO";
    case PRIMARY.DUPLICATE_OR_HISTORICAL:
      return "Deduplicate presentation lists; keep CLEAN 11 authoritative";
    case PRIMARY.CONTACT_LINEAGE_LOSS:
      return "Lossless merge from CLEAN 11 / prior inventory before requalify";
    default:
      return "Founder review";
  }
}

const rows = cand.map((o) => {
  const id = o.opportunityId || o.id;
  const name = text(o.primaryContact?.name);
  const nameQ = evaluateCommercialContactNameQuality(name);
  const channel = evaluatePersonSpecificOutreachChannel(o);
  const fqoStrict = evaluateFinalizedQualifiedOpportunity(o, {
    historicalApprovedInventory: false,
  });
  const fqoHist = evaluateFinalizedQualifiedOpportunity(o, {
    historicalApprovedInventory: cleanIds.has(id),
  });
  const dims = assessDimensions(o, fqoHist, channel, nameQ);
  const row = {
    opportunityId: id,
    title: text(o.title).slice(0, 140),
    organizationName: text(o.organizationName || o.organization).slice(0, 100),
    program: text(o.programmeName || o.demandProgram || o.title).slice(0, 100),
    segment: text(o.segmentId || o.fourteenSegment || o.demandType) || null,
    discoverySource: /gdi_wyndham_live_/.test(id)
      ? "LIVE_EXA_SEARCH_14SEG"
      : cleanIds.has(id)
        ? "CLEAN11_OR_PRIOR_INVENTORY"
        : "PRIOR_RESEARCH_OR_PROSPECT",
    existingVsNew: cleanIds.has(id) ? "CLEAN11_EXISTING" : "NEW_OR_OTHER",
    inClean11: cleanIds.has(id),
    demandEvidence: {
      urlCount: (fqoHist.evidenceUrls || []).length,
      mode: fqoHist.evidenceMode || null,
      lodgingLanguage: hasLodgingEvidence(o),
      what: text(o.whatIsHappening || o.summaryWhat).slice(0, 200),
    },
    hotelFit: {
      ok: dims.B_hotelFit,
      thesis: text(o.hotelOpportunityThesis || o.whyThisHotel).slice(0, 200),
    },
    pathway: {
      ok: dims.C_futureOrRecurringPathway,
      eventStartDate: o.eventStartDate || o.tileEventStartDate || null,
      recurringRelationship: hasRecurringRelationshipPathway(o),
      futureStamp: o.futureDemandPathway === true,
    },
    who: {
      name: name || null,
      role: text(o.primaryContact?.role || o.primaryContact?.title) || null,
      nameQuality: nameQ.reason || "OK",
    },
    how: {
      email: text(o.primaryContact?.email).toLowerCase() || null,
      linkedin: text(o.primaryContact?.linkedinUrl) || null,
      phone: text(o.primaryContact?.phone) || null,
      channelOk: channel.ok,
      channels: (channel.channels || []).map((c) => c.kind),
      rejected: (channel.rejected || []).map((r) => r.reason),
    },
    machinePass59: true,
    validatedBuggyWouldKeep: null, // filled below
    validatedFixedWouldKeep: null,
    strict12Pass: strict12Ids.has(id),
    fqoPassPostFix: fqoHist.finalizedQualified === true,
    fqoReasons: fqoHist.reasons || [],
    fqoReasonsNoHist: fqoStrict.reasons || [],
    nameQuality: nameQ.reason || "OK",
    dimensions: dims,
    hasLodging: hasLodgingEvidence(o),
    recurringEligible:
      hasRecurringRelationshipPathway(o) ||
      /scaling|planner|travel_trade|preferred|relocation|projects_|local_catering|corporate_meetings/i.test(
        text(o.segmentId || o.demandType || o.title),
      ),
  };
  row.rootCause = classifyPrimary(row);
  row.recommendedResolution = recommendedResolution(row);
  return row;
});

// VALIDATED layers
const buggy = filterValidatedFqoPersistenceWriteSet(
  cand.map((o) => ({
    ...o,
    // simulate old bug by forcing historical on evaluate via wrapper
  })),
  { allowHistoricalSchemaCompat: true },
);
// Old bug simulation
function oldBuggyKeep(opps) {
  const kept = [];
  for (const opp of opps) {
    const fqo = evaluateFinalizedQualifiedOpportunity(opp, {
      historicalApprovedInventory: true,
    });
    if (fqo.finalizedQualified) kept.push(opp.opportunityId || opp.id);
  }
  return new Set(kept);
}
const buggyIds = oldBuggyKeep(cand);
const fixedValidated = filterValidatedFqoPersistenceWriteSet(cand, {
  allowHistoricalSchemaCompat: true,
  historicalApprovedOpportunityIds: [...cleanIds],
});
const fixedIds = new Set(
  fixedValidated.kept.map((o) => o.opportunityId || o.id),
);

for (const r of rows) {
  r.validatedBuggyWouldKeep = buggyIds.has(r.opportunityId);
  r.validatedFixedWouldKeep = fixedIds.has(r.opportunityId);
}

const excluded47 = rows.filter((r) => !r.strict12Pass);
const primaryTally = {};
for (const r of excluded47) {
  primaryTally[r.rootCause.primary] =
    (primaryTally[r.rootCause.primary] || 0) + 1;
}

// Rule contribution
const RULES = [
  "NAMED_COMMERCIAL_PERSON_REQUIRED",
  "NON_PERSON_CONTACT_NAME",
  "NON_PERSON_OR_JUNK_CONTACT_NAME",
  "ROLE_GLUED_PERSON_NAME",
  "PERSON_SPECIFIC_CHANNEL_REQUIRED",
  "NO_VALID_FUTURE_SOURCING_PATHWAY",
  "MISSING_HOTEL_FIT_THESIS",
  "COMPETITIVE_INCUMBENT_OR_PATHWAY_UNVERIFIED",
  "MISSING_HOTEL_SPECIFIC_COMMERCIAL_ACTION",
  "MISSING_EVIDENCE_URLS",
];

function ruleHit(r, rule) {
  return (r.fqoReasons || []).includes(rule) || r.nameQuality === rule;
}

const ruleStats = RULES.map((rule) => {
  const affected = excluded47.filter((r) => ruleHit(r, rule));
  const unique = affected.filter(
    (r) => (r.fqoReasons || []).filter((x) => RULES.includes(x) || true).length &&
      (r.fqoReasons || []).filter((x) => RULES.includes(x)).length === 1 &&
      ruleHit(r, rule),
  );
  // better unique: only this rule among RULES
  const uniquely = affected.filter((r) => {
    const hits = RULES.filter((rr) => ruleHit(r, rr));
    return hits.length === 1 && hits[0] === rule;
  });
  return {
    rule,
    affectedCount: affected.length,
    uniquelyExcludedCount: uniquely.length,
    alsoBlockedByOthers: affected.length - uniquely.length,
    correctRejectExamples: affected
      .filter((r) =>
        /RESEARCH_EXTRACTION_DEFECT|NOT_REAL_HOTEL_DEMAND|VALID_COMMERCIAL_REJECTION/.test(
          r.rootCause.primary,
        ),
      )
      .slice(0, 3)
      .map((r) => ({ id: r.opportunityId, title: r.title, why: r.rootCause.note })),
    potentialFalseRejectExamples: affected
      .filter((r) =>
        /VALID_DEMAND_INCOMPLETE_|FALSE_NEGATIVE/.test(r.rootCause.primary),
      )
      .slice(0, 3)
      .map((r) => ({ id: r.opportunityId, title: r.title, why: r.rootCause.note })),
  };
});

// Counterfactuals — potential FQO-eligible if evidence supplied (not actual PASS)
function counterfactual(label, mutate) {
  let wouldBecomeEligible = 0;
  const examples = [];
  for (const r of excluded47) {
    if (r.strict12Pass) continue;
    const base = cand.find((o) => (o.opportunityId || o.id) === r.opportunityId);
    if (!base) continue;
    const cloned = structuredClone(base);
    mutate(cloned, r);
    const fqo = evaluateFinalizedQualifiedOpportunity(cloned, {
      historicalApprovedInventory: cleanIds.has(r.opportunityId),
    });
    if (fqo.finalizedQualified) {
      wouldBecomeEligible += 1;
      if (examples.length < 5) {
        examples.push({
          id: r.opportunityId,
          title: r.title,
          primaryCause: r.rootCause.primary,
        });
      }
    }
  }
  return { label, wouldBecomeEligibleForFqoEvaluation: wouldBecomeEligible, examples };
}

const counterfactuals = [
  counterfactual("IF_NAME_EXTRACTION_FIXED_TO_PLAUSIBLE_PERSON", (o, r) => {
    if (!evaluateCommercialContactNameQuality(o.primaryContact?.name).ok) {
      o.primaryContact = {
        ...(o.primaryContact || {}),
        name: "Alex Rivera",
        role: o.primaryContact?.role || "Meetings Manager",
      };
    }
  }),
  counterfactual("IF_PERSON_DIRECT_EMAIL_OR_LINKEDIN_SUPPLIED", (o) => {
    o.primaryContact = {
      ...(o.primaryContact || {}),
      name:
        evaluateCommercialContactNameQuality(o.primaryContact?.name).ok
          ? o.primaryContact.name
          : "Alex Rivera",
      email: "alex.rivera@example.org",
      emailType: "DIRECT_WORK",
      linkedinUrl:
        o.primaryContact?.linkedinUrl ||
        "https://www.linkedin.com/in/alex-rivera-meetings",
    };
  }),
  counterfactual("IF_RECURRING_PATHWAY_EVIDENCE_POPULATED", (o) => {
    o.segmentId = o.segmentId || "corporate_accounts_scaling";
    o.recurringRelationshipPathway = true;
    o.accountDevelopmentReactivated = true;
    o.hotelOpportunityThesis =
      o.hotelOpportunityThesis ||
      "Recurring corporate lodging and meeting demand for Wyndham San Diego Bayside preferred-account development.";
    o.whyThisHotel = o.hotelOpportunityThesis;
    o.futureCycleOrSourcingWindow =
      "Recurring account development pathway — verify current cycle.";
    o.hotelMarketScopeEligible = true;
  }),
  counterfactual("IF_WHO_AND_HOW_AND_PATHWAY_ALL_SUPPLIED", (o) => {
    o.primaryContact = {
      name: "Alex Rivera",
      role: "Director of Meetings",
      email: "alex.rivera@example.org",
      emailType: "DIRECT_WORK",
      linkedinUrl: "https://www.linkedin.com/in/alex-rivera-meetings",
      emailSource: "https://example.org/staff",
    };
    o.segmentId = o.segmentId || "corporate_accounts_scaling";
    o.recurringRelationshipPathway = true;
    o.hotelOpportunityThesis =
      "Credible downtown waterfront lodging fit for recurring San Diego program demand at Wyndham San Diego Bayside.";
    o.whyThisHotel = o.hotelOpportunityThesis;
    o.hotelMarketScopeEligible = true;
    o.recommendedNextStep =
      "Contact Alex to introduce Wyndham San Diego Bayside for the next housing/meeting cycle.";
    o.futureCycleOrSourcingWindow = "Recurring / next-cycle sourcing window.";
    o.eventStartDate = o.eventStartDate || "2027-06-15";
    o.sourceUrls = o.sourceUrls?.length
      ? o.sourceUrls
      : ["https://example.org/evidence"];
  }),
];

// Top 20 promising blocked (valid demand, not extraction junk / not duplicate)
const top20 = excluded47
  .filter(
    (r) =>
      r.dimensions.A_demandCredibility &&
      r.rootCause.primary !== PRIMARY.DUPLICATE_OR_HISTORICAL &&
      r.rootCause.primary !== PRIMARY.NOT_REAL_HOTEL_DEMAND &&
      r.rootCause.primary !== PRIMARY.RESEARCH_EXTRACTION_DEFECT,
  )
  .sort((a, b) => {
    const score = (r) =>
      (r.dimensions.B_hotelFit ? 20 : 0) +
      (r.hasLodging ? 15 : 0) +
      (r.dimensions.C_futureOrRecurringPathway ? 10 : 0) +
      (r.recurringEligible ? 8 : 0) +
      (/VALID_DEMAND_INCOMPLETE/.test(r.rootCause.primary) ? 12 : 0) +
      (r.discoverySource === "LIVE_EXA_SEARCH_14SEG" ? 5 : 0);
    return score(b) - score(a);
  })
  .slice(0, 20)
  .map((r, i) => ({
    rank: i + 1,
    opportunityId: r.opportunityId,
    title: r.title,
    organizationName: r.organizationName,
    primaryCause: r.rootCause.primary,
    maturity: r.dimensions.maturity,
    who: r.who.name,
    resolution: r.recommendedResolution,
  }));

// Matched benchmark cohort 30
const priorityThemes = [
  { re: /scaling|ESET|corporate account|expansion/i, theme: "scaling_corporate" },
  { re: /planner|SPARGO|onPeak|ConferenceDirect|housing|intermediar|DMC/i, theme: "planners_intermediaries" },
  { re: /venue|preferred|partnership|Nolita|wedding|museum/i, theme: "venues_partnerships" },
  { re: /association|ACG|ASCRS|AANS|ACS|AWWA|convention|WEST|Comic|BIO/i, theme: "associations_competitive" },
  { re: /project|reloc|extended|infrastructure|construction|training/i, theme: "projects_extended_stay" },
];
const cohort = [];
const used = new Set();
for (const theme of priorityThemes) {
  for (const r of rows) {
    if (cohort.length >= 30) break;
    if (used.has(r.opportunityId)) continue;
    const blob = `${r.title} ${r.organizationName} ${r.segment || ""}`;
    if (theme.re.test(blob) || (theme.theme === "scaling_corporate" && r.recurringEligible)) {
      used.add(r.opportunityId);
      cohort.push({
        opportunityId: r.opportunityId,
        title: r.title,
        theme: theme.theme,
        primaryCause: r.rootCause.primary,
        maturity: r.dimensions.maturity,
        strict12Pass: r.strict12Pass,
        fqoPassPostFix: r.fqoPassPostFix,
      });
    }
  }
}
// fill remaining from top promising
for (const r of top20) {
  if (cohort.length >= 30) break;
  if (used.has(r.opportunityId)) continue;
  used.add(r.opportunityId);
  cohort.push({
    opportunityId: r.opportunityId,
    title: r.title,
    theme: "promising_blocked_fill",
    primaryCause: r.primaryCause,
    maturity: r.maturity,
  });
}

const searchQuote = quoteBenchmarkCall({
  provider: "exa",
  operation: "search",
  params: { type: "lite", numResults: 6 },
});
const contentsQuote = quoteBenchmarkCall({
  provider: "exa",
  operation: "contents",
  params: { urls: ["https://a", "https://b", "https://c"], text: true },
});
const agentMed = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM });
const agentLow = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.LOW });

const dimensionBreakdown = {
  verifiedDemandContactResearchRequired: rows.filter(
    (r) => r.dimensions.maturity === "VERIFIED_DEMAND_CONTACT_RESEARCH_REQUIRED",
  ).length,
  verifiedDemandPathwayIncomplete: rows.filter(
    (r) => r.dimensions.maturity === "VERIFIED_DEMAND_PATHWAY_INCOMPLETE",
  ).length,
  salesReadyFqoCandidate: rows.filter(
    (r) => r.dimensions.maturity === "SALES_READY_FQO_CANDIDATE",
  ).length,
  demandPresentHotelFitUnsupported: rows.filter(
    (r) => r.dimensions.maturity === "DEMAND_PRESENT_HOTEL_FIT_UNSUPPORTED",
  ).length,
  notCredibleOrIncomplete: rows.filter(
    (r) => r.dimensions.maturity === "NOT_CREDIBLE_OR_INCOMPLETE_DEMAND",
  ).length,
  identificationOrResearchRequired: rows.filter(
    (r) => r.dimensions.maturity === "IDENTIFICATION_OR_RESEARCH_REQUIRED",
  ).length,
};

const bottleneck = (() => {
  const scores = {
    missing_source_evidence: excluded47.filter((r) =>
      /MISSING_EVIDENCE|NOT_REAL_HOTEL_DEMAND|HOTEL_FIT/.test(r.rootCause.primary),
    ).length,
    poor_name_contact_extraction: excluded47.filter(
      (r) => r.rootCause.primary === PRIMARY.RESEARCH_EXTRACTION_DEFECT,
    ).length,
    incomplete_who_how_research: excluded47.filter((r) =>
      /INCOMPLETE_WHO|INCOMPLETE_HOW/.test(r.rootCause.primary),
    ).length,
    pathway_or_event_assumptions: excluded47.filter((r) =>
      /INCOMPLETE_PATHWAY|FALSE_NEGATIVE/.test(r.rootCause.primary),
    ).length,
    duplicate_or_lineage: excluded47.filter((r) =>
      /DUPLICATE|LINEAGE/.test(r.rootCause.primary),
    ).length,
  };
  const ranked = Object.entries(scores).sort((a, b) => b[1] - a[1]);
  return { scores, primaryBottleneck: ranked[0]?.[0] || null, ranked };
})();

const clean11Dupes = rows.filter((r) => r.inClean11);
const summary = {
  at: new Date().toISOString(),
  authScope: "P0_OPPORTUNITY_LOSS_FORENSICS_NO_SPEND",
  funnel: {
    universeReconciledApprox: universeSnap.count || universeSnap.total || universeSnap.opportunityCount || 303,
    machinePass59: rows.length,
    validatedBuggyKeep: buggyIds.size,
    validatedFixedKeep: fixedIds.size,
    strictCommercial12: strict12Ids.size,
    strict12NetNewBeyondClean11: [...strict12Ids].filter((id) => !cleanIds.has(id))
      .length,
    postFixFqo18Note: "PR #604 post-fix FQO dry-run ≈18 — not this forensic baseline",
    excludedMachinePassMinusStrict12: excluded47.length,
  },
  primaryRootCauseTally: primaryTally,
  dimensionBreakdown,
  bottleneck,
  counterfactuals,
  top20PromisingBlocked: top20,
  clean11: {
    count: clean.length,
    presentInCandidate59: clean11Dupes.length,
    allInStrict12: clean11Dupes.every((r) => r.strict12Pass),
    materialDefects: clean11Dupes.filter((r) => !r.strict12Pass),
    duplicateNearMissExplanation:
      "CLEAN 11 IDs reappear inside candidate 59 and prior near-miss lists — duplicates, not new defects",
  },
  ruleStats,
  benchmarkCohort30: cohort.slice(0, 30),
  providerEconomics: {
    liveWyndhamSearchSpendUsd: spend.spentUsd?.aggregate || null,
    liveCalls: spend.calls?.length || null,
    searchLiteApproxUsd: searchQuote.maxUsd,
    contents3pagesApproxUsd: contentsQuote.maxUsd,
    agentLowUsd: agentLow.maxUsd,
    agentMediumUsd: agentMed.maxUsd,
    proposedBenchmarkCeilingUsd: 10,
    proposedAllocation: {
      searchOnlyArmUsd: 2.5,
      agentLedArmUsd: 5.0,
      hybridArmUsd: 2.0,
      reserveUsd: 0.5,
      note: "PROPOSAL only — no spend without separate founder GO",
    },
  },
  integrity: {
    airtableWrites: 0,
    paidSpendThisTaskUsd: 0,
    surfe: "OFF",
    agent: "OFF",
    wyndhamStagedFrozenClean11: true,
  },
};

writeJson("00-SUMMARY.json", summary);
writeJson("01-FULL_RECORD_RECONCILIATION.json", { count: rows.length, rows });
writeJson("02-EXCLUDED_47_FORENSICS.json", {
  count: excluded47.length,
  rows: excluded47,
});
writeJson("03-RULE_WATERFALL.json", { rules: ruleStats, counterfactuals });
writeJson("04-TOP20_PROMISING_BLOCKED.json", { rows: top20 });
writeJson("05-BENCHMARK_COHORT_30.json", {
  policy: "Matched starting evidence; Search vs Agent vs Hybrid; $10 proposal only",
  cohort: cohort.slice(0, 30),
  metrics: [
    "correct_named_stakeholders",
    "verified_role",
    "usable_outreach_route",
    "source_accuracy",
    "future_or_recurring_pathway",
    "hotel_specific_action",
    "false_person_extraction",
    "unsupported_sourcing_claims",
    "strict_fqo_conversion",
    "time_and_usd_per_new_actionable",
  ],
});
writeJson("06-DIMENSION_MATURITY.json", { dimensionBreakdown, rows: rows.map((r) => ({
  id: r.opportunityId,
  maturity: r.dimensions.maturity,
  dims: r.dimensions,
  primary: r.rootCause.primary,
})) });

console.log(
  JSON.stringify(
    {
      funnel: summary.funnel,
      primaryRootCauseTally: primaryTally,
      bottleneck: bottleneck.ranked.slice(0, 4),
      counterfactuals: counterfactuals.map((c) => ({
        label: c.label,
        n: c.wouldBecomeEligibleForFqoEvaluation,
      })),
      top5: top20.slice(0, 5),
      cohortSize: Math.min(30, cohort.length),
    },
    null,
    2,
  ),
);
