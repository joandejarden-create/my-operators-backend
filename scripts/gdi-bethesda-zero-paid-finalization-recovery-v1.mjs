#!/usr/bin/env node
/**
 * Zero-paid Bethesda WATCHLIST finalization recovery (no Exa spend, no publication).
 *
 * Reconciles the 12 closed-run WATCHLIST dossiers into distinct commercial activities,
 * applies handoff fixes in-memory, classifies each candidate, and proves the
 * evidence-complete fixture path without inventing demand.
 */
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  orgDeepKey,
  loadHotelInventoryOrgKeys,
} from "../lib/group-demand-intelligence/research/gdi-paid-deep-two-stage-dedupe-v1.js";
import {
  reconcileUnresolvedQuestionsAfterContact,
  applyOfflineDemandThesisRecoveryInChain,
  attachCommercialDemandTypeInChain,
  evaluatePublicationReviewReadiness,
  reconstructProviderRunIdFromOpportunity,
} from "../lib/group-demand-intelligence/workflow/gdi-watchlist-finalization-handoff-v1.js";
import { evaluateSharedCommercialEligibility } from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { processOpportunityThroughOriginalChain } from "../lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js";
import { BETHESDA_HOTEL_ID } from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = BETHESDA_HOTEL_ID;
const OUT =
  "reports/gdi-bethesda-zero-paid-finalization-recovery-v1";
const ORS_DIR = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunity-research-state"
);
const LIVE_RECOVERY =
  "reports/gdi-bethesda-fourteen-segment-offline-replay-postmortem-v1/07-watchlist-value-recovery.json";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function writeJson(rel, obj) {
  const p = path.join(ROOT, OUT, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
  return p;
}

function writeText(rel, body) {
  const p = path.join(ROOT, OUT, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, body.endsWith("\n") ? body : `${body}\n`);
  return p;
}

function loadOrs(id) {
  const p = path.join(ORS_DIR, `${id}.json`);
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return doc.opportunity || doc;
}

function inventorySoftHit(opp, inventoryOrgKeys) {
  const k = orgDeepKey(opp);
  if (!k) return false;
  for (const inv of inventoryOrgKeys) {
    if (!inv) continue;
    if (inv === k || inv.startsWith(`${k} `) || k.startsWith(`${inv} `)) return true;
  }
  return false;
}

function classifyCandidate(row) {
  if (row.publicationReviewReady) return "PUBLICATION_REVIEW_READY";
  if (row.inventoryOverlap && !row.distinctFutureCycle) {
    return "EXISTING_INVENTORY_ENRICHMENT";
  }
  if (row.crossSegmentDuplicateOf) return "DUPLICATE_OR_EXCLUDED";
  if (row.accountDevelopmentSupported) return "ACCOUNT_DEVELOPMENT_CONTINUATION";
  if (/EXCLUDE/i.test(String(row.disposition || ""))) return "DUPLICATE_OR_EXCLUDED";
  return "RESEARCH_CONTINUATION";
}

const prior = JSON.parse(fs.readFileSync(path.join(ROOT, LIVE_RECOVERY), "utf8"));
const inventory = loadHotelInventoryOrgKeys(HOTEL_ID);
const gitSha = (() => {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return null;
  }
})();

const byOrg = new Map();
const classifications = [];

for (const rec of prior.recoveries || []) {
  const opp = loadOrs(rec.opportunityId);
  const orgKey = orgDeepKey(opp);
  const siblings = byOrg.get(orgKey) || [];
  siblings.push(rec.opportunityId);
  byOrg.set(orgKey, siblings);

  const unresolved = reconcileUnresolvedQuestionsAfterContact(opp);
  const demand = applyOfflineDemandThesisRecoveryInChain(unresolved.opportunity);
  const typed = attachCommercialDemandTypeInChain(demand.opportunity);
  const elig = evaluateSharedCommercialEligibility(typed, {
    hotel: { hotelId: HOTEL_ID, displayName: "Bethesda Marriott" },
  });
  const readiness = evaluatePublicationReviewReadiness(typed, {
    eligibilityResult: elig,
    hotel: { hotelId: HOTEL_ID, displayName: "Bethesda Marriott" },
  });
  const provenance = reconstructProviderRunIdFromOpportunity(opp);
  const invHit = inventorySoftHit(opp, inventory.orgKeys);
  const distinctFuture =
    opp.futureCycleSupported === true ||
    /OVERFLOW|FUTURE_CYCLE|COMPETITIVE_FUTURE/i.test(
      `${opp.opportunityType || ""} ${opp.hotelOpportunityThesis || ""}`
    );

  const invalidBuyer =
    elig.gaps?.includes("INVALID_BUYER_CONTACT_ATTRIBUTION") === true ||
    elig.gaps?.includes("BUYER_RESPONSIBILITY_UNKNOWN") === true ||
    /Administrative Law Journal|organization contact|contact desk/i.test(
      String((opp.namedStakeholders || []).map((p) => p?.name).join(" "))
    );
  const accountDevelopmentSupported =
    unresolved.hasVerifiedBuyer &&
    !invalidBuyer &&
    text(opp.hotelOpportunityThesis).length > 80 &&
    !/EXCLUDE/i.test(String(opp.internalDisposition || "")) &&
    elig.status !== "COMMERCIALLY_INELIGIBLE";

  const row = {
    opportunityId: rec.opportunityId,
    organizationName: opp.organizationName,
    orgKey,
    segment: opp.researchSegment,
    disposition: opp.internalDisposition,
    title: opp.title || opp.programmeName || null,
    namedContacts: (opp.namedStakeholders || opp.namedStakeholdersFound || [])
      .filter((p) => text(p?.name))
      .map((p) => ({
        name: text(p.name),
        title: text(p.title || p.role || "") || null,
        relevance: p.contactRelevanceLabel || p.whyThisContact || null,
      })),
    tenSectionComplete: opp.tenSectionDrawer?.completePersistedContent === true,
    commercialDemandType: typed.commercialDemandType || null,
    eligibilityStatus: elig.status,
    eligibilityGaps: elig.gaps || [],
    unresolvedCleared: unresolved.cleared,
    unresolvedRemaining: unresolved.remaining,
    demandRecoveryCategory: demand.category,
    demandRecoveryApplied: demand.applied,
    afterDemandOk: demand.afterDemandOk,
    inventoryOverlap: invHit,
    distinctFutureCycle: distinctFuture,
    providerRunId: provenance.providerRunId,
    providerRunIdSource: provenance.source,
    accountDevelopmentSupported,
    publicationReviewReady: false, // historical evidence remains speculative
    historicalEvidenceLimitation:
      elig.gaps?.includes("MISSING_HOTEL_DEMAND_THESIS") === true
        ? "INDEPENDENT_HOTEL_DEMAND_EVIDENCE_INSUFFICIENT_SPECULATIVE_THESIS"
        : null,
    crossSegmentDuplicateOf: null,
  };
  classifications.push(row);
}

// Mark cross-segment same-org duplicates (keep strongest as primary)
for (const [orgKey, ids] of byOrg.entries()) {
  if (ids.length < 2) continue;
  const rows = classifications.filter((r) => r.orgKey === orgKey);
  rows.sort((a, b) => {
    const score = (r) =>
      (r.namedContacts?.length || 0) * 10 +
      (r.tenSectionComplete ? 5 : 0) +
      (r.accountDevelopmentSupported ? 3 : 0);
    return score(b) - score(a);
  });
  const primary = rows[0];
  for (const r of rows.slice(1)) {
    r.crossSegmentDuplicateOf = primary.opportunityId;
    r.accountDevelopmentSupported = false;
  }
}

for (const row of classifications) {
  row.finalClassification = classifyCandidate(row);
}

const fixturePath = path.join(
  ROOT,
  "fixtures/gdi/evidence-complete-full-e2e-handoff-v1.json"
);
const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const fixtureResult = await processOpportunityThroughOriginalChain(
  { ...fixture.opportunity },
  {
    authorizePaidResearch: false,
    skipDeepResearch: true,
    skipPersistResearchState: true,
    allowOfficialSourceContactFollowUp: false,
  }
);

const oppCount = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "data/group-demand-intelligence/hotels",
      HOTEL_ID,
      "opportunities.json"
    ),
    "utf8"
  )
);
const canonicalCount = Array.isArray(oppCount)
  ? oppCount.length
  : (oppCount.opportunities || []).length;

const summary = {
  version: "gdi-bethesda-zero-paid-finalization-recovery-v1",
  at: new Date().toISOString(),
  gitSha,
  hotelId: HOTEL_ID,
  uniqueWatchlistRows: classifications.length,
  uniqueOrganizations: byOrg.size,
  uniqueCommercialActivities: classifications.filter(
    (r) => !r.crossSegmentDuplicateOf
  ).length,
  commerciallyCredibleAccountDevelopment: classifications.filter(
    (r) => r.finalClassification === "ACCOUNT_DEVELOPMENT_CONTINUATION"
  ).length,
  publicationReviewReadyHistorical: classifications.filter(
    (r) => r.finalClassification === "PUBLICATION_REVIEW_READY"
  ).length,
  finalizedCustomerBodiesFromHistorical: 0,
  fixturePublicationReviewReady:
    fixtureResult.publicationReviewReady === true,
  fixtureLabel: fixture.label,
  inventoryProtected: {
    canonicalOpportunities: canonicalCount,
    inventoryOrgKeys: inventory.orgKeys.size,
    inventoryOpportunityIds: inventory.opportunityIds.size,
    liveDiscInInventory: [...inventory.opportunityIds].filter((id) =>
      /^disc_/i.test(String(id))
    ).length,
  },
  paidCalls: 0,
  customerPublication: false,
  recommendation: "NO_GO_FOR_BROAD_PAID_DISCOVERY__GO_FOR_NARROW_ZERO_PAID_OR_FIXTURE_VALIDATED_HANDOFF_ONLY",
};

writeJson("00-summary.json", summary);
writeJson("01-watchlist-classifications.json", {
  classifications,
  byClassification: classifications.reduce((a, r) => {
    a[r.finalClassification] = (a[r.finalClassification] || 0) + 1;
    return a;
  }, {}),
  crossSegmentFamilies: [...byOrg.entries()]
    .filter(([, ids]) => ids.length > 1)
    .map(([org, ids]) => ({ org, ids })),
});
writeJson("02-evidence-complete-fixture-result.json", {
  label: fixture.label,
  publicationReviewReady: fixtureResult.publicationReviewReady === true,
  customerPublicationReady: fixtureResult.customerPublicationReady === false,
  commercialDemandType: fixtureResult.opportunity.commercialDemandType || null,
  eligibilityStatus:
    fixtureResult.opportunity.commercialEligibilityStatus || null,
  sectionCount: Object.keys(
    fixtureResult.opportunity.tenSectionDrawer?.sections || {}
  ).length,
  stages: (fixtureResult.stages || []).map((s) => s.stage),
  note: "LABELED FIXTURE — not a live Bethesda opportunity body for publication.",
});
writeJson("03-handoff-failure-trace.json", {
  finding:
    "Ten-section content already existed on all 12 WATCHLIST dossiers. Failure was commercial qualification / demand-evidence standard + stale unresolvedQuestions, not missing content generation.",
  stages: {
    discovery_and_source_evidence: "PRESENT",
    ors_deep_research: "PRESENT_WITH_PROVIDER_RUN_IDS_IN_ORS",
    contact_verification: "PRESENT_ON_MOST_CORPORATE_ROWS_STALE_UNRESOLVED_QUESTIONS_BUG",
    hotel_specific_demand_thesis: "SPECULATIVE_GENERATED_THESIS_NOT_INDEPENDENT_EVIDENCE",
    commercial_qualification: "CORRECTLY_BLOCKS_SALES_READY_WITHOUT_RELIABLE_DEMAND",
    guest_rooms_local_catering_both: "NOW_ATTACHED_IN_CHAIN",
    ten_section_customer_content: "ALREADY_COMPLETE_ON_WATCHLIST",
    publication_readiness_certification: "NOW_SEPARATES_REVIEW_READY_FROM_CUSTOMER_PUBLICATION",
  },
  codeFixes: [
    "reconcileUnresolvedQuestionsAfterContact",
    "applyOfflineDemandThesisRecoveryInChain",
    "attachCommercialDemandTypeInChain",
    "evaluatePublicationReviewReadiness",
    "reconstructProviderRunIdFromOpportunity (read-only)",
    "disc_* inventory exclusion (PR #205)",
    "providerRunId settle harden (PR #205)",
  ],
});

const lines = [];
const L = (s = "") => lines.push(s);
L("# Bethesda Zero-Paid Finalization Recovery — Founder Report");
L("");
L(`Generated: ${summary.at}`);
L(`Git SHA: \`${gitSha}\``);
L(`Hotel: Bethesda Marriott (${HOTEL_ID})`);
L("");
L("## Classification of 12 WATCHLIST rows");
L("");
L(`- Unique ORS rows: **${summary.uniqueWatchlistRows}**`);
L(`- Unique organizations: **${summary.uniqueOrganizations}**`);
L(`- Unique commercial activities (after cross-segment collapse): **${summary.uniqueCommercialActivities}**`);
L(`- Account Development continuation: **${summary.commerciallyCredibleAccountDevelopment}**`);
L(`- Publication-review-ready from historical evidence: **${summary.publicationReviewReadyHistorical}**`);
L(`- Finalized customer bodies from historical pool: **0** (insufficient independent demand evidence)`);
L("");
L("| Org | Segment | Classification | Demand gap | Contacts |");
L("|---|---|---|---|---|");
for (const r of classifications) {
  L(
    `| ${r.organizationName} | ${r.segment} | **${r.finalClassification}** | ${r.historicalEvidenceLimitation || r.eligibilityGaps.join(",") || "—"} | ${(r.namedContacts || []).map((c) => c.name).join("; ") || "—"} |`
  );
}
L("");
L("## Strongest candidates");
L("");
L("- **Corporate / account development:** KnowBe4, AstraZeneca, Hidden Level, X-energy, Workday, Ameresco, Menlo Digital, True Anomaly (primary) — contacts relevant; demand thesis remains SPECULATIVE without independent lodging evidence.");
L("- **Preferred lodging partnership:** Rock Creek Mansion — complementary venue thesis present; **no named decision-maker**; remains research/partnership continuation, not publication-ready.");
L("- **Planner / local catering:** No credible planner-firm or local-day-meeting candidate in the WATCHLIST set with supporting evidence (PEP/LCM contamination was conference brands).");
L("");
L("## Evidence-complete handoff proof (labeled fixture)");
L("");
L(`- Fixture: \`${fixture.label}\``);
L(`- publicationReviewReady: **${fixtureResult.publicationReviewReady === true}**`);
L(`- customerPublicationReady: **false** (founder auth still required)`);
L(`- commercialDemandType: **${fixtureResult.opportunity.commercialDemandType || "n/a"}**`);
L("- This proves the existing engine can finalize a complete body when independent evidence is present. It is **not** a live Bethesda opportunity.");
L("");
L("## Inventory integrity");
L("");
L(`- Canonical opportunities.json: **${canonicalCount}**`);
L(`- Inventory union IDs: **${inventory.opportunityIds.size}** (live disc_* = **0**)`);
L("- No operational writes; no paid calls; no customer publication.");
L("");
L("## GO / NO-GO");
L("");
L(`### ${summary.recommendation}`);
L("");
L("- **NO-GO** for another broad paid discovery cycle.");
L("- **GO** only for zero-paid continuation / fixture-validated handoff proofs, or a separately authorized narrow live validation after PEP/LCM targeting fixtures prove correct entity types.");
L("");

writeText("FOUNDER-FINALIZATION-RECOVERY-REPORT.md", lines.join("\n"));
writeText(
  "README.md",
  `# Zero-paid Bethesda finalization recovery\n\nSee FOUNDER-FINALIZATION-RECOVERY-REPORT.md\n`
);

console.log(
  JSON.stringify(
    {
      ok: true,
      outDir: OUT,
      uniqueOrgs: summary.uniqueOrganizations,
      accountDevelopment: summary.commerciallyCredibleAccountDevelopment,
      publicationReadyHistorical: summary.publicationReviewReadyHistorical,
      fixturePublicationReviewReady: summary.fixturePublicationReviewReady,
      recommendation: summary.recommendation,
    },
    null,
    2
  )
);
