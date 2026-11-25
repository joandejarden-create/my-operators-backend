#!/usr/bin/env node
/**
 * P0 Wyndham commercial conversion remediation — dry-run only.
 * No Airtable writes, STAGED/CURRENT, Surfe, or paid providers.
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
} from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import { filterValidatedFqoPersistenceWriteSet } from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const CLEAN11 = "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_3283ee92";
const CAND_59 = "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_fb10174d";
const OUT = path.join(
  ROOT,
  "reports/gdi-wyndham-sd-bayside-commercial-conversion-remediation-v1",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(data, null, 2)}\n`);
}

const store = createGdiPublicationPackageStore();
const clean = store.loadPackage(HOTEL_ID, CLEAN11).package.opportunities || [];
const cand = store.loadPackage(HOTEL_ID, CAND_59).package.opportunities || [];
const cleanIds = new Set(clean.map((o) => o.opportunityId || o.id));
const cleanById = new Map(clean.map((o) => [o.opportunityId || o.id, o]));

function classifyIssue(row) {
  const reasons = row.fqoReasons || [];
  const cq = row.commercialFlags || [];
  if (cq.includes("FALSE_PERSON_EXTRACTION") || reasons.some((r) => /NON_PERSON|ROLE_GLUED|JUNK_CONTACT/.test(r))) {
    return {
      class: "INCORRECT_EXTRACTION",
      falseNegative: false,
      note: "Scraped non-person / role-glued name — legitimate fail",
    };
  }
  if (
    cq.includes("FUNCTIONAL_AS_PERSON_DIRECT") ||
    reasons.includes("PERSON_SPECIFIC_CHANNEL_REQUIRED")
  ) {
    if (row.hasLabeledOrgRoute) {
      return {
        class: "QUALIFICATION_LOGIC",
        falseNegative: true,
        note: "Labeled org route may exist but channel gate failed — review",
      };
    }
    return {
      class: "MISSING_RESEARCH_OR_MISLABELED_CHANNEL",
      falseNegative: false,
      note: "Functional/generic channel without labeled org route",
    };
  }
  if (reasons.includes("NO_VALID_FUTURE_SOURCING_PATHWAY")) {
    if (row.recurringEligibleDemandType && row.hasLodgingEvidence) {
      return {
        class: "DEMAND_TYPE_ASSUMPTION",
        falseNegative: true,
        note: "Recurring demand type with lodging evidence previously failed without dated event",
      };
    }
    return {
      class: "MISSING_RESEARCH",
      falseNegative: false,
      note: "No dated event and no corroborated recurring pathway",
    };
  }
  if (reasons.includes("COMPETITIVE_INCUMBENT_OR_PATHWAY_UNVERIFIED")) {
    return {
      class: "MISSING_RESEARCH",
      falseNegative: false,
      note: "Competitive incumbent/pathway unverified",
    };
  }
  if (reasons.includes("MISSING_HOTEL_FIT_THESIS")) {
    return {
      class: "MISSING_RESEARCH",
      falseNegative: false,
      note: "Hotel-fit thesis missing or negative",
    };
  }
  return {
    class: "OTHER",
    falseNegative: false,
    note: reasons.join("|") || "excluded",
  };
}

function assess(o, { asHistorical } = {}) {
  const id = o.opportunityId || o.id;
  const name = text(o.primaryContact?.name || o.primaryContactName);
  const email = text(o.primaryContact?.email).toLowerCase();
  const role = text(o.primaryContact?.role || o.primaryContact?.title);
  const seg = text(
    o.segmentId || o.fourteenSegment || o.demandSegment || o.demandType || "",
  );
  const nameQ = evaluateCommercialContactNameQuality(name);
  const channel = evaluatePersonSpecificOutreachChannel(o);
  const fqoBeforeHistorical = evaluateFinalizedQualifiedOpportunity(o, {
    historicalApprovedInventory: false,
  });
  const fqo = evaluateFinalizedQualifiedOpportunity(o, {
    historicalApprovedInventory: asHistorical === true || cleanIds.has(id),
  });
  const commercialFlags = [];
  if (!nameQ.ok) {
    commercialFlags.push(
      nameQ.reason === "ROLE_GLUED_PERSON_NAME"
        ? "ROLE_GLUED"
        : "FALSE_PERSON_EXTRACTION",
    );
  }
  if (
    email &&
    channel.rejected?.some((r) => r.reason === "GENERIC_OR_FUNCTIONAL_ORGANIZATION_INBOX")
  ) {
    commercialFlags.push("FUNCTIONAL_AS_PERSON_DIRECT");
  }
  if (!email && !o.primaryContact?.linkedinUrl && !o.primaryContact?.phone) {
    commercialFlags.push("NO_USABLE_CHANNEL");
  }
  const lodgingBlob = `${o.hotelOpportunityThesis || ""} ${o.whyThisHotel || ""} ${o.whatIsHappening || ""}`;
  const row = {
    opportunityId: id,
    title: text(o.title).slice(0, 120),
    organizationName: text(o.organizationName || o.organization).slice(0, 100),
    segment: seg || null,
    existingVsNew: cleanIds.has(id) ? "CLEAN11_EXISTING" : "NEW_OR_OTHER",
    demandEvidence: {
      urlCount: (fqo.evidenceUrls || []).length,
      mode: fqo.evidenceMode || null,
      what: text(o.whatIsHappening || o.summaryWhat).slice(0, 180),
    },
    hotelFit: {
      ok: fqo.gates?.HOTEL_GEO_COMMERCIAL_FIT?.ok === true,
      thesis: text(o.hotelOpportunityThesis || o.whyThisHotel).slice(0, 180),
    },
    who: { name: name || null, role: role || null, nameQuality: nameQ.reason || "OK" },
    outreach: {
      email: email || null,
      linkedin: text(o.primaryContact?.linkedinUrl) || null,
      channelOk: channel.ok,
      channels: channel.channels,
      rejected: channel.rejected,
    },
    pathway: {
      futureOk: fqo.gates?.FUTURE_SOURCING_PATHWAY?.ok === true,
      recurringRelationship: hasRecurringRelationshipPathway(o),
      eventStartDate: o.eventStartDate || o.tileEventStartDate || null,
      futureDemandPathwayStamp: o.futureDemandPathway === true,
    },
    machinePassInCandidate59: true,
    fqoPass: fqo.finalizedQualified === true,
    fqoPassWithoutHistorical: fqoBeforeHistorical.finalizedQualified === true,
    fqoReasons: fqo.reasons || [],
    commercialFlags,
    hasLabeledOrgRoute: Boolean(
      text(o.primaryContact?.organizationContactRouteLabel) ||
        text(o.organizationContactRouteLabel) ||
        text(o.approvedCommercialRoute),
    ),
    recurringEligibleDemandType: hasRecurringRelationshipPathway(o) ||
      /scaling|planner|travel_trade|preferred|relocation|projects_|local_catering|corporate_meetings/i.test(
        seg,
      ),
    hasLodgingEvidence: /\b(lodging|housing|hotel|rooms?|overflow|meeting|catering|partner)/i.test(
      lodgingBlob,
    ),
  };
  row.issue = classifyIssue(row);
  return row;
}

const rows59 = cand.map((o) => assess(o));
const fqoPassAfter = rows59.filter((r) => r.fqoPass);
const fqoFailAfter = rows59.filter((r) => !r.fqoPass);

// Prior VALIDATED dry-run used allowHistoricalSchemaCompat blanket — simulate old vs new
const validatedOldStyle = filterValidatedFqoPersistenceWriteSet(cand, {
  allowHistoricalSchemaCompat: true,
  // OLD buggy behavior simulation: do not pass historical IDs; rely on fixed filter
});
// Force-compare: re-implement old bug locally for audit only
function filterOldBuggy(opps) {
  const kept = [];
  const deferred = [];
  for (const opp of opps) {
    const fqo = evaluateFinalizedQualifiedOpportunity(opp, {
      historicalApprovedInventory: true, // BUG
    });
    if (fqo.finalizedQualified) kept.push(opp);
    else deferred.push({ id: opp.opportunityId || opp.id, reasons: fqo.reasons });
  }
  return { kept, deferred, keptCount: kept.length };
}
const oldBuggy = filterOldBuggy(cand);
const validatedNew = filterValidatedFqoPersistenceWriteSet(cand, {
  allowHistoricalSchemaCompat: true,
  historicalApprovedOpportunityIds: [...cleanIds],
});

const excludedFrom59toFqo = fqoFailAfter.map((r) => ({
  ...r,
  exclusionLayer: "MACHINE_PASS_BUT_FQO_FAIL_AFTER_FIX",
}));

const clean11Reconcile = clean.map((c) => {
  const id = c.opportunityId || c.id;
  const inCand = cand.find((o) => (o.opportunityId || o.id) === id);
  const cleanAssess = assess(c, { asHistorical: true });
  const candAssess = inCand ? assess(inCand, { asHistorical: true }) : null;
  const nearMissListedAsNew =
    candAssess &&
    candAssess.existingVsNew === "CLEAN11_EXISTING" &&
    // was listed in continuation commercialIds / near-miss prose
    true;
  return {
    opportunityId: id,
    title: text(c.title).slice(0, 100),
    inCandidate59: Boolean(inCand),
    clean11StillFqoPass: cleanAssess.fqoPass,
    candidateStillFqoPass: candAssess ? candAssess.fqoPass : null,
    whoClean: cleanAssess.who,
    whoCand: candAssess?.who || null,
    channelClean: cleanAssess.outreach.email || cleanAssess.outreach.linkedin,
    channelCand: candAssess
      ? candAssess.outreach.email || candAssess.outreach.linkedin
      : null,
    defect:
      cleanAssess.fqoPass === false
        ? {
            material: true,
            reasons: cleanAssess.fqoReasons,
            note: "CLEAN 11 row fails post-fix FQO — report; do not silent-downgrade STAGED",
          }
        : nearMissListedAsNew
          ? {
              material: false,
              reasons: [],
              note: "Appeared in near-miss/commercial lists as duplicate of CLEAN 11 — not a new opportunity",
            }
          : { material: false, reasons: [], note: "OK" },
  };
});

// Contact resolution taxonomy on FQO fails
const contactTaxonomy = {};
for (const r of fqoFailAfter) {
  let key = "OTHER";
  if (r.commercialFlags.includes("FALSE_PERSON_EXTRACTION") || r.commercialFlags.includes("ROLE_GLUED")) {
    key = "FALSE_PERSON_EXTRACTION";
  } else if (!r.who.name) {
    key = "NAMED_PERSON_NOT_IDENTIFIED";
  } else if (r.commercialFlags.includes("FUNCTIONAL_AS_PERSON_DIRECT")) {
    key = "FUNCTIONAL_MAILBOX_ONLY";
  } else if (r.commercialFlags.includes("NO_USABLE_CHANNEL")) {
    key = "NAMED_PERSON_WITHOUT_USABLE_CHANNEL";
  } else if (r.fqoReasons.includes("PERSON_SPECIFIC_CHANNEL_REQUIRED")) {
    key = "NAMED_PERSON_WITH_ORG_ROUTE_UNLABELED";
  } else if (r.who.role && /scraped|unknown|n\/a/i.test(r.who.role)) {
    key = "NAMED_PERSON_ROLE_UNVERIFIED";
  } else if (r.fqoPassWithoutHistorical === false && cleanIds.has(r.opportunityId)) {
    key = "CONTACT_EVIDENCE_PRESENT_BUT_LINEAGE_FLAG";
  }
  contactTaxonomy[key] = (contactTaxonomy[key] || 0) + 1;
  r.contactResolutionClass = key;
}

const surfeCandidates = fqoFailAfter
  .filter((r) => {
    if (r.contactResolutionClass === "FALSE_PERSON_EXTRACTION") return false;
    if (!r.hasLodgingEvidence && !r.hotelFit.ok) return false;
    // Priority: channel/WHO blockers on otherwise evidenced demand
    if (
      r.contactResolutionClass === "NAMED_PERSON_WITHOUT_USABLE_CHANNEL" ||
      r.contactResolutionClass === "FUNCTIONAL_MAILBOX_ONLY" ||
      r.contactResolutionClass === "NAMED_PERSON_NOT_IDENTIFIED" ||
      r.contactResolutionClass === "NAMED_PERSON_WITH_ORG_ROUTE_UNLABELED"
    ) {
      return true;
    }
    // Secondary: pathway-held with a plausible person name worth verifying
    if (
      r.who.name &&
      looksLikePersonName(r.who.name) &&
      r.hasLodgingEvidence &&
      (r.fqoReasons.includes("NO_VALID_FUTURE_SOURCING_PATHWAY") ||
        r.fqoReasons.includes("PERSON_SPECIFIC_CHANNEL_REQUIRED") ||
        r.fqoReasons.includes("COMPETITIVE_INCUMBENT_OR_PATHWAY_UNVERIFIED"))
    ) {
      return true;
    }
    return false;
  })
  .sort((a, b) => {
    const score = (r) =>
      (r.contactResolutionClass === "FUNCTIONAL_MAILBOX_ONLY" ? 30 : 0) +
      (r.contactResolutionClass === "NAMED_PERSON_WITHOUT_USABLE_CHANNEL"
        ? 25
        : 0) +
      (r.hasLodgingEvidence ? 10 : 0) +
      (r.hotelFit.ok ? 10 : 0) +
      (r.recurringEligibleDemandType ? 5 : 0);
    return score(b) - score(a);
  })
  .slice(0, 40)
  .map((r, i) => ({
    rank: i + 1,
    opportunityId: r.opportunityId,
    title: r.title,
    organizationName: r.organizationName,
    segment: r.segment,
    who: r.who.name,
    contactClass: r.contactResolutionClass,
    whySurfe:
      r.contactResolutionClass === "FUNCTIONAL_MAILBOX_ONLY"
        ? "Resolve person-direct email/LinkedIn; keep functional inbox as labeled org route only"
        : r.contactResolutionClass === "NAMED_PERSON_WITHOUT_USABLE_CHANNEL"
          ? "Verify work email or LinkedIn for named stakeholder"
          : r.fqoReasons.includes("NO_VALID_FUTURE_SOURCING_PATHWAY")
            ? "Confirm recurring/account pathway timing with named stakeholder"
            : "Identify or verify relevant named entry point for evidenced demand",
    expectedValue: "High — demand evidence present; WHO/HOW is the blocker",
  }));

const bySeg = {};
for (const r of fqoPassAfter) {
  const s = r.segment || "unlabeled";
  bySeg[s] = (bySeg[s] || 0) + 1;
}

const rootCause = {};
for (const r of fqoFailAfter) {
  const c = r.issue.class;
  rootCause[c] = (rootCause[c] || 0) + 1;
}

const falseNegatives = fqoFailAfter.filter((r) => r.issue.falseNegative);
const recoveredRecurring = fqoPassAfter.filter(
  (r) =>
    !cleanIds.has(r.opportunityId) &&
    r.pathway.recurringRelationship === true,
);

const bethS = store.readPointer("recLuxvwwxID7U2B8", "STAGED");
const bethC = store.readPointer("recLuxvwwxID7U2B8", "CURRENT");
const wyS = store.readPointer(HOTEL_ID, "STAGED");
const wyC = store.readPointer(HOTEL_ID, "CURRENT");

const summary = {
  at: new Date().toISOString(),
  authScope: "P0_COMMERCIAL_CONVERSION_REMEDIATION_DRY_RUN",
  layers: {
    machinePass59: cand.length,
    validatedOldBuggyHistoricalBlanket: oldBuggy.keptCount,
    validatedNewAfterFix: validatedNew.keptCount,
    fqoPassAfterCommercialFix: fqoPassAfter.length,
    clean11RetainedInPass: fqoPassAfter.filter((r) => cleanIds.has(r.opportunityId))
      .length,
    newFqoAfterFix: fqoPassAfter.filter((r) => !cleanIds.has(r.opportunityId)).length,
    priorStrictAdHoc12Note:
      "Prior ad-hoc scrub was outside FQO; post-fix FQO is the enforced gate",
  },
  rootCauseTally: rootCause,
  contactTaxonomy,
  segmentDistributionPass: bySeg,
  falseNegativeCandidatesStillHeld: falseNegatives.length,
  recoveredRecurringNew: recoveredRecurring.map((r) => ({
    id: r.opportunityId,
    title: r.title,
    seg: r.segment,
  })),
  clean11: {
    count: clean.length,
    stillPass: clean11Reconcile.filter((r) => r.clean11StillFqoPass).length,
    materialDefects: clean11Reconcile.filter((r) => r.defect.material),
    duplicateNearMissExplanation:
      "CLEAN 11 contacts listed as near-misses were the same certified IDs present in candidate 59 — not newly discovered defects",
  },
  surfeRankedCount: surfeCandidates.length,
  integrity: {
    bethesdaStaged: bethS?.publicationVersion || null,
    bethesdaCurrent: bethC?.publicationVersion || null,
    wyndhamStaged: wyS?.publicationVersion || null,
    wyndhamCurrent: wyC?.publicationVersion || null,
    wyndhamStagedStillClean11: (wyS?.publicationVersion || null) === CLEAN11,
    airtableWrites: 0,
    paidSpendUsd: 0,
  },
  passIds: fqoPassAfter.map((r) => ({
    id: r.opportunityId,
    title: r.title,
    who: r.who.name,
    existingVsNew: r.existingVsNew,
    seg: r.segment,
  })),
};

writeJson("00-SUMMARY.json", summary);
writeJson("01-RECORD_RECONCILIATION_59.json", {
  count: rows59.length,
  rows: rows59,
});
writeJson("02-EXCLUSIONS_AFTER_FIX.json", {
  count: excludedFrom59toFqo.length,
  rows: excludedFrom59toFqo,
});
writeJson("03-CLEAN11_RECONCILE.json", { rows: clean11Reconcile });
writeJson("04-SURFE_RANKED_QUEUE.json", {
  policy: "explicit-click only; do not auto-invoke",
  rows: surfeCandidates,
});
writeJson("05-VALIDATED_WRITE_SET_DRY_RUN.json", {
  oldBuggyKeptCount: oldBuggy.keptCount,
  newKeptCount: validatedNew.keptCount,
  newKeptIds: validatedNew.kept.map((o) => o.opportunityId || o.id),
  deferredSample: validatedNew.deferred.slice(0, 40),
});

console.log(JSON.stringify(summary, null, 2));
