#!/usr/bin/env node
/**
 * Zero-paid Bethesda 67 sales-baseline + Exa/cached CMI reconciliation.
 * Stages founder publication-review package. Does NOT publish. Does NOT call Exa.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  loadBethesda67SalesBaselineManifest,
  annotateInventoryPopulations,
  assessStagedCustomerReadiness,
  classifyCompetitiveMarketIntelligence,
  classifySalesBaselineMaturity,
  buildInventoryPopulationCounts,
  selectBethesdaSalesOpportunityInventory,
  isBethesda67CustomerPublicationApproved,
  CMI_CLASS,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import {
  buildCustomerExportCsv,
  GDI_CUSTOMER_CSV_HEADERS,
} from "../lib/group-demand-intelligence/customer-csv-export.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-67-sales-baseline-cmi-v1"
);

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function readJson(rel) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
}

function writeJson(name, data) {
  const p = path.join(OUT, name);
  fs.writeFileSync(p, JSON.stringify(data, null, 2));
  return p;
}

ensureDir(OUT);

const manifest = loadBethesda67SalesBaselineManifest({ forceReload: true });
const forensic = readJson(
  "reports/gdi-bethesda-67-to-19-forensic-reconciliation-v1/01-frozen67-vs-facing19-raw.json"
);
const forensicById = new Map((forensic.missingRows || []).map((r) => [r.id, r]));

const storeDoc = readJson(
  `data/group-demand-intelligence/hotels/${BETHESDA_HOTEL_ID}/opportunities.json`
);
const storeOpps = storeDoc.opportunities || [];
const union = mergeBethesdaInternalResearchOpportunities(
  BETHESDA_HOTEL_ID,
  storeOpps
);
const unionOpps = union.opportunities || storeOpps;

const byId = new Map();
for (const o of unionOpps) {
  byId.set(String(o.id || o.opportunityId), o);
}

const baselineRows = [];
const missingBodies = [];
for (const id of manifest.baselineIds) {
  const opp = byId.get(id);
  if (!opp) {
    missingBodies.push(id);
    continue;
  }
  baselineRows.push(opp);
}

const annotated = annotateInventoryPopulations(baselineRows, {
  hotelId: BETHESDA_HOTEL_ID,
});

const maturityBreakdown = {};
const readinessBreakdown = {};
const stagedReview = [];
for (const opp of annotated) {
  const forensicRow = forensicById.get(opp.id) || null;
  const maturity = classifySalesBaselineMaturity(opp, forensicRow);
  maturityBreakdown[maturity.key] = (maturityBreakdown[maturity.key] || 0) + 1;
  if (manifest.stagedIds.includes(opp.id)) {
    const readiness = assessStagedCustomerReadiness(opp, forensicRow);
    readinessBreakdown[readiness.readiness] =
      (readinessBreakdown[readiness.readiness] || 0) + 1;
    stagedReview.push({
      id: opp.id,
      title: opp.title || opp.displayTitle || "",
      organization: opp.organizationName || forensicRow?.organization || "",
      segment: opp.segment || opp.demandSegmentId || "",
      ...readiness,
      forensicClassification: forensicRow?.classification || null,
      forensicSubcategory: forensicRow?.subcategory || null,
      forensicRecovery: forensicRow?.recovery || null,
      inStore: storeOpps.some((o) => o.id === opp.id),
      inUnionOnly: !storeOpps.some((o) => o.id === opp.id),
    });
  }
}

// CMI: scan full union (not only 67) for verified competitor bookings — zero paid
const allAnnotated = annotateInventoryPopulations(unionOpps, {
  hotelId: BETHESDA_HOTEL_ID,
});
const cmiVerified = [];
const cmiNeedsVerify = [];
for (const opp of allAnnotated) {
  const cmi = classifyCompetitiveMarketIntelligence(opp);
  const row = {
    id: opp.id,
    title: opp.title || opp.displayTitle || "",
    organization: opp.organizationName || "",
    competitorHotel: cmi.competitorHotel,
    reason: cmi.reason,
    on67Baseline: manifest.baselineIds.includes(opp.id),
    sources: (opp.sources || [])
      .map((s) => (typeof s === "string" ? s : s?.url))
      .filter(Boolean)
      .slice(0, 5),
  };
  if (cmi.class === CMI_CLASS.COMPETITIVE_MARKET_INTELLIGENCE) {
    cmiVerified.push(row);
  } else if (cmi.class === CMI_CLASS.NEEDS_FURTHER_VERIFICATION) {
    cmiNeedsVerify.push(row);
  }
}

const facing = filterCustomerFacingOpportunities(
  filterSalespersonView(storeOpps),
  { hotelId: BETHESDA_HOTEL_ID }
);
const approved = isBethesda67CustomerPublicationApproved();
const customerSelection = selectBethesdaSalesOpportunityInventory(unionOpps, {
  founderReview: false,
});
const founderSelection = selectBethesdaSalesOpportunityInventory(unionOpps, {
  founderReview: true,
});

const populationCounts = buildInventoryPopulationCounts(
  annotateInventoryPopulations(
    [
      ...founderSelection.selected,
      ...allAnnotated.filter((o) => o.cmiVerified && !manifest.baselineIds.includes(o.id)),
    ],
    { hotelId: BETHESDA_HOTEL_ID }
  )
);

// CSV of founder-review 67 (not customer publish)
const founderCsv = buildCustomerExportCsv(
  annotateInventoryPopulations(founderSelection.selected, {
    hotelId: BETHESDA_HOTEL_ID,
  }),
  { hotelId: BETHESDA_HOTEL_ID, enrich: true }
);
fs.writeFileSync(
  path.join(OUT, "Bethesda Marriott - Founder Review 67 Sales Baseline.csv"),
  founderCsv,
  "utf8"
);

const customerCsv = buildCustomerExportCsv(
  annotateInventoryPopulations(facing, { hotelId: BETHESDA_HOTEL_ID }),
  { hotelId: BETHESDA_HOTEL_ID, enrich: true }
);
fs.writeFileSync(
  path.join(OUT, "Bethesda Marriott - Customer Published 19.csv"),
  customerCsv,
  "utf8"
);

const reconciliation = {
  version: "gdi-bethesda-67-sales-baseline-cmi-v1",
  generatedAt: new Date().toISOString(),
  hotelId: BETHESDA_HOTEL_ID,
  zeroPaid: true,
  autoPublish: false,
  customerPublicationApproved: approved,
  baseline: {
    expected: 67,
    recoveredWithBodies: baselineRows.length,
    missingBodies,
    publishedIdsIntact: manifest.publishedIds.every((id) => byId.has(id)),
    publishedFacingCount: facing.length,
    founderReviewCount: founderSelection.selected.length,
    founderMissingIds: founderSelection.missingIds,
    storeCount: storeOpps.length,
    unionCount: Array.isArray(unionOpps) ? unionOpps.length : 0,
  },
  maturityBreakdown,
  staged48: {
    count: stagedReview.length,
    readinessBreakdown,
    rows: stagedReview,
  },
  cmi: {
    verifiedCount: cmiVerified.length,
    needsFurtherVerificationCount: cmiNeedsVerify.length,
    verified: cmiVerified,
    needsFurtherVerification: cmiNeedsVerify,
    note: "Classified from cached opportunity fields only — no new Exa calls.",
  },
  populationCounts,
  customerGate: {
    untilApprovalFacing: facing.length,
    afterApprovalExpected: 67,
    approvalReceiptPath:
      "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/bethesda-67-customer-publication-approval.json",
    envFlag: "GDI_BETHESDA_67_CUSTOMER_PUBLICATION_APPROVED",
  },
  csv: {
    founderReviewColumns: GDI_CUSTOMER_CSV_HEADERS.length,
    founderReviewRows: founderSelection.selected.length,
    customerPublishedRows: facing.length,
  },
};

writeJson("00-reconciliation-summary.json", reconciliation);
writeJson("01-67-id-manifest.json", {
  publishedIds: manifest.publishedIds,
  stagedIds: manifest.stagedIds,
  baselineIds: manifest.baselineIds,
});
writeJson("02-staged-48-publication-review.json", stagedReview);
writeJson("03-cmi-verified.json", cmiVerified);
writeJson("04-cmi-needs-verification.json", cmiNeedsVerify);
writeJson("05-maturity-breakdown.json", maturityBreakdown);
writeJson("06-population-counts.json", populationCounts);

const md = `# Bethesda 67 Sales Baseline + CMI — Founder Publication Review

**Generated:** ${reconciliation.generatedAt}  
**Hotel:** Bethesda Marriott (\`${BETHESDA_HOTEL_ID}\`)  
**Zero paid:** YES · **Auto-publish:** NO · **Customer approval:** ${approved ? "APPROVED" : "NOT APPROVED (stay at 19)"}

## 1. Exact 67-ID baseline reconciliation

| Check | Result |
|-------|--------|
| Expected baseline IDs | **67** |
| Bodies recovered (store ∪ historical union) | **${baselineRows.length}** |
| Missing bodies | **${missingBodies.length}** ${missingBodies.length ? missingBodies.join(", ") : ""} |
| Published 19 intact in store | **${reconciliation.baseline.publishedIdsIntact}** |
| Current customer-facing | **${facing.length}** |
| Founder-review selectable | **${founderSelection.selected.length}** |

## 2. Commercial maturity breakdown (67 with bodies)

\`\`\`json
${JSON.stringify(maturityBreakdown, null, 2)}
\`\`\`

## 3. Customer-readiness of the additional 48

\`\`\`json
${JSON.stringify(readinessBreakdown, null, 2)}
\`\`\`

See \`02-staged-48-publication-review.json\` for the full per-record review package.

## 4. Publication gate

Until founder writes approval receipt or sets \`GDI_BETHESDA_67_CUSTOMER_PUBLICATION_APPROVED=1\`, the hotel customer/share Opportunities feed remains the **approved 19**.

Proposed post-approval inventory: **67 Sales Opportunities** with honest maturity labels (not 67 Sales Ready).

## 5–7. Existing Exa / cached CMI inventory

| Class | Count |
|-------|------:|
| Verified COMPETITIVE_MARKET_INTELLIGENCE | **${cmiVerified.length}** |
| Needs further verification | **${cmiNeedsVerify.length}** |

No new Exa calls. Classification uses existing competitor venue / booking fields only.

## 8. Populations

**${populationCounts.headline}**

Sales Ready among sales inventory: ${populationCounts.salesReadyAmongSales}  
Account Development among sales inventory: ${populationCounts.accountDevelopmentAmongSales}

## 9–11. Artifacts

- Founder-review CSV: \`Bethesda Marriott - Founder Review 67 Sales Baseline.csv\` (${founderSelection.selected.length} rows × ${GDI_CUSTOMER_CSV_HEADERS.length} cols)
- Customer published CSV: \`Bethesda Marriott - Customer Published 19.csv\` (${facing.length} rows)
- CMI lists: \`03-cmi-verified.json\`, \`04-cmi-needs-verification.json\`

## 12–14. Next

UI Opportunity Type populations + API wiring land in the same PR. CI/merge recorded after merge.
`;

fs.writeFileSync(
  path.join(OUT, "FOUNDER-67-SALES-BASELINE-CMI-REPORT.md"),
  md,
  "utf8"
);

console.log(
  JSON.stringify(
    {
      out: OUT,
      baselineRecovered: baselineRows.length,
      missingBodies: missingBodies.length,
      facing: facing.length,
      founderReview: founderSelection.selected.length,
      cmiVerified: cmiVerified.length,
      cmiNeedsVerify: cmiNeedsVerify.length,
      approved,
      headline: populationCounts.headline,
    },
    null,
    2
  )
);
