#!/usr/bin/env node
/**
 * Permanent commercial-expiration gate + Sheraton active-inventory cleanup.
 * Zero paid spend. Does not mutate V2 fingerprint 6f9c0416… — stages NEW package.
 *
 *   node scripts/gdi-commercial-expiration-gate-sheraton-cleanup-v1.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";
import {
  applyCommercialExpirationGate,
  assertCustomerInventoryPassesExpirationGate,
  COMMERCIAL_TEMPORAL_DISPOSITION,
} from "../lib/group-demand-intelligence/commercial-expiration-gate-v1.js";
import { INVENTORY_POPULATION } from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { attachSalesOpportunityFilterFields } from "../lib/group-demand-intelligence/sales-opportunity-quick-filters-v1.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { GDI_FULL_E2E_HOTEL_TEMPLATE_V1 } from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recCAGqkERSXgiku4";
const HOTEL_NAME = "Sheraton Guadalajara Expo";
const REF = "2026-11-14";
const V2_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261113_commercial_e2e_remediation_v2";
const V2_FP =
  "6f9c0416be28c2409cf9fcccfb78cc45dd3a5178b27d4e0009f2622a21efd092";
const PAID_V3_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261114_paid_full_e2e_discovery_v3";
const OUT = path.join(
  ROOT,
  "reports/gdi-commercial-expiration-gate-sheraton-cleanup-v1",
);
const AUDIT_DIR = path.join(
  ROOT,
  "reports/gdi-sheraton-historical-opportunity-audit-v1",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(obj, null, 2) + "\n");
}
function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

fs.mkdirSync(OUT, { recursive: true });

const store = createGdiPublicationPackageStore({
  root: path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
});
const v2 = store.loadPackage(HOTEL_ID, V2_VERSION);
if (!v2.ok || v2.package.contentFingerprintSha256 !== V2_FP) {
  console.error("V2 package missing/mismatch — refuse to mutate baseline");
  process.exit(3);
}
const paidV3 = store.loadPackage(HOTEL_ID, PAID_V3_VERSION);
const auditRows = JSON.parse(
  fs.readFileSync(path.join(AUDIT_DIR, "01-FULL_55_AUDIT.json"), "utf8"),
).audits;
const auditById = new Map(auditRows.map((a) => [a.opportunityId, a]));
const dupReg = JSON.parse(
  fs.readFileSync(path.join(AUDIT_DIR, "03-DUPLICATE_REGISTER.json"), "utf8"),
);
const expiredList = JSON.parse(
  fs.readFileSync(path.join(AUDIT_DIR, "04-VERIFIED_EXPIRED_EVENTS.json"), "utf8"),
);

// --- 1. Finalize audit membership (mutually exclusive dispositions) ---
const membership = {
  referenceDate: REF,
  totalReviewed: auditRows.length,
  mutualExclusivity: true,
  note: "Each of the 55 records has exactly one primary disposition. Scorecard subtotals (future-cycle potential, requiring verification) are derived flags and must not be summed with primary dispositions.",
  byDisposition: {},
  derivedFlags: {
    organizationMeritsFutureAccountDevelopment: auditRows.filter(
      (a) => a.organizationMeritsFutureAccountDevelopment,
    ).length,
    recommendCustomerVisible: auditRows.filter((a) => a.recommendCustomerVisible)
      .length,
    recommendResearchWatch: auditRows.filter((a) => !a.recommendCustomerVisible)
      .length,
  },
  ids: {},
};
for (const a of auditRows) {
  membership.byDisposition[a.disposition] =
    (membership.byDisposition[a.disposition] || 0) + 1;
  if (!membership.ids[a.disposition]) membership.ids[a.disposition] = [];
  membership.ids[a.disposition].push(a.opportunityId);
}
const sumPrimary = Object.values(membership.byDisposition).reduce(
  (s, n) => s + n,
  0,
);
if (sumPrimary !== 55) {
  console.error("Audit membership does not partition 55", membership.byDisposition);
  process.exit(3);
}
writeJson("01-AUDIT_MEMBERSHIP_VALIDATED.json", membership);
writeJson("02-EXPIRED_OPPORTUNITY_IDS.json", {
  count: expiredList.count,
  events: expiredList.events,
});
writeJson("03-DUPLICATE_CLUSTERS.json", dupReg);

// --- Build cleaned inventory: audit retain + paid net-new ---
const retainFromAudit = auditRows.filter((a) => a.recommendCustomerVisible);
const researchWatchFromAudit = auditRows.filter((a) => !a.recommendCustomerVisible);

const v2ById = new Map(v2.package.opportunities.map((o) => [o.id, o]));
const paidNetNew = paidV3.ok
  ? paidV3.package.opportunities.filter((o) => String(o.id).startsWith("paid3_"))
  : [];

const candidateOps = [];
for (const a of retainFromAudit) {
  const base = v2ById.get(a.opportunityId);
  if (!base) continue;
  candidateOps.push({
    ...base,
    duplicateOf: a.duplicateOf || null,
    commercialTemporalDisposition: a.disposition,
    commercialExpirationGateV1: {
      disposition: a.disposition,
      provenance: a.dateProvenance,
      fromHistoricalAudit: true,
      storedIsResearchTimestamp: a.storedIsResearchTimestamp,
      actualEventStart: a.actualEventStart,
      actualEventEnd: a.actualEventEnd,
    },
  });
}
for (const o of paidNetNew) {
  candidateOps.push({
    ...o,
    commercialTemporalDisposition: null, // classify via gate
  });
}

const applied = applyCommercialExpirationGate(candidateOps, {
  referenceDate: REF,
  // For audit-stamped rows, pass override via each opp's commercialTemporalDisposition
  // handled inside loop:
});

// Re-apply with per-row overrides (gate helper uses opts.dispositionOverride globally;
// stamp manually for audit rows)
const stamped = [];
for (const o of candidateOps) {
  const a = auditById.get(o.id);
  const classification = a
    ? {
        disposition: a.disposition,
        provenance: a.dateProvenance,
        customerPursuitEligible: a.recommendCustomerVisible,
        researchWatchOnly: !a.recommendCustomerVisible,
        duplicateOf: a.duplicateOf || null,
        actualEventStart: a.actualEventStart,
        actualEventEnd: a.actualEventEnd,
        fromHistoricalAudit: true,
        version: "gdi-commercial-expiration-gate-v1",
      }
    : applyCommercialExpirationGate([o], { referenceDate: REF }).rows[0]
        .commercialExpirationGateV1;
  const disposition = classification.disposition;
  const eligible =
    classification.customerPursuitEligible === true ||
    [
      COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_FUTURE_EVENT,
      COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_ONGOING_DEMAND,
      COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_VERIFIED,
      COMMERCIAL_TEMPORAL_DISPOSITION.DATE_UNVERIFIED,
      COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_RESEARCH_REQUIRED,
    ].includes(disposition);

  let next = {
    ...o,
    commercialTemporalDisposition: disposition,
    commercialExpirationGateV1: classification,
    inventoryPopulations: eligible
      ? [INVENTORY_POPULATION.SALES_OPPORTUNITIES]
      : [],
    hotelMarketScopeEligible: eligible,
  };
  next = attachSalesOpportunityFilterFields(next);
  stamped.push(next);
}

const customerVisible = stamped.filter((o) =>
  (o.inventoryPopulations || []).includes(INVENTORY_POPULATION.SALES_OPPORTUNITIES),
);
const researchWatch = [
  ...stamped.filter((o) => !(o.inventoryPopulations || []).length),
  ...researchWatchFromAudit.map((a) => ({
    opportunityId: a.opportunityId,
    title: a.title,
    disposition: a.disposition,
    recommend: a.recommendDisposition,
    preservedResearch: true,
  })),
];

// Deduplicate customer visible by id
const seen = new Set();
const uniqueCustomer = [];
for (const o of customerVisible) {
  if (seen.has(o.id)) continue;
  seen.add(o.id);
  // Avoid repeating the sourcing badge phrase in narrative fields
  const sourcing = text(o.hotelSourcingStatusLabel);
  const stripSourcing = (s) => {
    let out = text(s);
    if (!out) return out;
    out = out.replace(/Status:\s*hotel sourcing unconfirmed[;.:\s]*/gi, "");
    out = out.replace(/Hotel Sourcing Unconfirmed/gi, "");
    out = out.replace(/hotel sourcing unconfirmed/gi, "");
    return out.replace(/\s{2,}/g, " ").trim();
  };
  const drawer = o.sharedTenSectionDrawer
    ? {
        ...o.sharedTenSectionDrawer,
        sections: (o.sharedTenSectionDrawer.sections || []).map((sec) => ({
          ...sec,
          body: stripSourcing(sec.body),
        })),
      }
    : o.sharedTenSectionDrawer;
  uniqueCustomer.push({
    ...o,
    whyActNow: stripSourcing(o.whyActNow) || o.whyActNow,
    whatIsHappening: stripSourcing(o.whatIsHappening) || o.whatIsHappening,
    potentialHotelBusiness:
      stripSourcing(o.potentialHotelBusiness) || o.potentialHotelBusiness,
    potentialBusiness:
      stripSourcing(o.potentialBusiness) || o.potentialBusiness,
    whyThisHotel: stripSourcing(o.whyThisHotel) || o.whyThisHotel,
    recommendedNextStep:
      stripSourcing(o.recommendedNextStep) || o.recommendedNextStep,
    recommendedAction:
      stripSourcing(o.recommendedAction) || o.recommendedAction,
    summaryWhat: stripSourcing(o.summaryWhat) || o.summaryWhat,
    summary: stripSourcing(o.summary) || o.summary,
    cardWhyNowLine: stripSourcing(o.cardWhyNowLine) || o.cardWhyNowLine,
    guestroomThesis: stripSourcing(o.guestroomThesis) || o.guestroomThesis,
    tileClassificationLine: null,
    sharedTenSectionDrawer: drawer,
  });
}

writeJson("04-CONFIRMED_FUTURE_AND_ONGOING.json", {
  future: uniqueCustomer.filter((o) =>
    [
      COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_FUTURE_EVENT,
      COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_VERIFIED,
    ].includes(o.commercialTemporalDisposition),
  ).map((o) => ({ id: o.id, title: o.title, disposition: o.commercialTemporalDisposition })),
  ongoing: uniqueCustomer.filter(
    (o) =>
      o.commercialTemporalDisposition ===
      COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_ONGOING_DEMAND,
  ).map((o) => ({ id: o.id, title: o.title })),
});
writeJson("05-FUTURE_CYCLE_RESEARCH_QUEUE.json", {
  paidSpendAuthorized: false,
  note: "Evidence needed for next cycle — do not invent dates/venues/sourcing. Separate budget required for paid API.",
  candidates: auditRows
    .filter(
      (a) =>
        a.organizationMeritsFutureAccountDevelopment ||
        a.disposition ===
          COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_RESEARCH_REQUIRED ||
        a.disposition === COMMERCIAL_TEMPORAL_DISPOSITION.DATE_UNVERIFIED ||
        a.disposition === COMMERCIAL_TEMPORAL_DISPOSITION.EXPIRED_ONE_TIME_EVENT,
    )
    .map((a) => ({
      opportunityId: a.opportunityId,
      title: a.title,
      organization: a.organization,
      disposition: a.disposition,
      evidenceNeeded: [
        "Next edition official dates",
        "City/venue commitment",
        "Whether hotel sourcing is open",
        "Named lodging/meetings entry point",
      ],
      priority:
        a.disposition ===
        COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_RESEARCH_REQUIRED
          ? "HIGH"
          : a.organizationMeritsFutureAccountDevelopment
            ? "MEDIUM"
            : "LOW",
    })),
});
writeJson("06-DATE_UNVERIFIED.json", {
  ids: membership.ids.DATE_UNVERIFIED || [],
  rows: (membership.ids.DATE_UNVERIFIED || []).map((id) => {
    const a = auditById.get(id);
    return { id, title: a?.title, provenance: a?.dateProvenance };
  }),
});
writeJson("07-FINAL_ACTIVE_CUSTOMER_INVENTORY.json", {
  count: uniqueCustomer.length,
  fromAuditRetain: retainFromAudit.length,
  fromPaidNetNew: paidNetNew.length,
  opportunities: uniqueCustomer.map((o) => ({
    id: o.id,
    title: o.displayTitle || o.title,
    disposition: o.commercialTemporalDisposition,
    source: String(o.id).startsWith("paid3_") ? "PAID_V3_NET_NEW" : "AUDIT_RETAIN",
  })),
});
writeJson("08-RESEARCH_WATCH_INVENTORY.json", {
  auditMoved: researchWatchFromAudit.length,
  rows: researchWatchFromAudit.map((a) => ({
    id: a.opportunityId,
    title: a.title,
    disposition: a.disposition,
    recommend: a.recommendDisposition,
    duplicateOf: a.duplicateOf,
  })),
});

const gateCert = assertCustomerInventoryPassesExpirationGate(uniqueCustomer, {
  referenceDate: REF,
});
const quality = runPublicationQualityGate({
  opportunities: uniqueCustomer,
  referenceDate: REF,
});
writeJson("09-EXPIRATION_GATE_TEST_RESULTS.json", {
  unitTestHint: "npm run test:gdi-commercial-expiration-gate-v1",
  assertCustomerInventory: {
    ok: gateCert.ok,
    blockers: gateCert.blockers,
  },
  publicationQualityGate: {
    ok: quality.ok,
    blockingCount: quality.blockingCount,
    blocking: quality.blocking.slice(0, 20),
  },
  membershipPartitionOk: sumPrimary === 55,
});

const publicationVersion = `gdi_pub_${HOTEL_ID}_${new Date()
  .toISOString()
  .slice(0, 10)
  .replace(/-/g, "")}_expiration_gate_cleanup_v1`;
const pkg = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  opportunities: uniqueCustomer,
  publicationVersion,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  approvalStatus: "PENDING_FOUNDER_APPROVAL",
  notes: {
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    commercialExpirationGate: "gdi-commercial-expiration-gate-v1",
    preservesV2: { version: V2_VERSION, fingerprint: V2_FP },
    reconcilesPaidV3: paidV3.ok
      ? {
          version: PAID_V3_VERSION,
          fingerprint: paidV3.package.contentFingerprintSha256,
        }
      : null,
    paidSpendThisRunUsd: 0,
    templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  },
});
const integrity = verifyPublicationPackageIntegrity(pkg);
if (!integrity.ok) {
  console.error("PACKAGE_INTEGRITY_FAILED", integrity.fails);
  process.exit(2);
}
if (!quality.ok) {
  console.warn(
    "QUALITY_GATE_WARN",
    quality.blocking.map((b) => b.code + ":" + b.opportunityId).slice(0, 10),
  );
}
const written = store.writePackage(pkg);
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  supersedes: {
    publicationVersion: PAID_V3_VERSION,
    contentFingerprintSha256: paidV3.ok
      ? paidV3.package.contentFingerprintSha256
      : null,
    preservesV2: { version: V2_VERSION, fingerprint: V2_FP },
  },
});

fs.writeFileSync(
  path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    HOTEL_ID,
    "opportunities.json",
  ),
  JSON.stringify(
    {
      schema: "gdi-opportunities-doc-v1",
      hotelId: HOTEL_ID,
      updatedAt: new Date().toISOString(),
      source: "gdi-commercial-expiration-gate-sheraton-cleanup-v1",
      opportunities: uniqueCustomer,
      researchWatchPreserved: researchWatchFromAudit.map((a) => a.opportunityId),
    },
    null,
    2,
  ) + "\n",
);

let airtablePlan = { configured: isGdiOpportunityAirtableConfigured(), writes: 0 };
try {
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    dryRun: true,
    opportunities: uniqueCustomer,
  });
  airtablePlan = {
    configured: true,
    dryRun: true,
    writes: 0,
    authorizationRequired: true,
    wouldCreate: sync.summary?.created,
    wouldUpdate: sync.summary?.updated,
    total: sync.summary?.total,
    note: "Research-watch rows remain in dossiers; not deleted. Live writes need founder auth.",
  };
} catch (e) {
  airtablePlan.error = String(e?.message || e);
}
writeJson("10-AIRTABLE_DRY_RUN.json", airtablePlan);
writeJson("11-NEW_STAGED_PUBLICATION.json", {
  status: "STAGED_FOR_FOUNDER_REVIEW",
  publicationVersion,
  fingerprint: pkg.contentFingerprintSha256,
  opportunityCount: uniqueCustomer.length,
  inventoryCounts: pkg.inventoryCounts,
  packagePath: written.path,
  preservesV2Fingerprint: V2_FP,
  paidSpendUsd: 0,
});
writeJson("12-PAID_E2E_RECONCILIATION.json", {
  paidV3Present: paidV3.ok,
  paidNetNewIncluded: paidNetNew.length,
  expiredNotCountedAsIncremental: true,
  note: "Cleaned baseline = audit retain + paid net-new future/ongoing. No new paid spend this run. Remaining $24.80 from prior V3 ledger is NOT auto-authorized here.",
});
writeJson("13-GITHUB_STATUS.json", {
  branch: "rem/gdi-commercial-expiration-gate-sheraton-cleanup-v1",
  headSha: sh("git rev-parse HEAD"),
  noBethesdaMutation: true,
  noCustomerPublished: true,
  noPaidSpend: true,
  v2FingerprintUntouched: V2_FP,
});

const founder = `# GDI Permanent Commercial Expiration Gate & Sheraton Cleanup V1

**Reference date:** ${REF}  
**V2 fingerprint preserved:** \`${V2_FP}\`  
**New staged fingerprint:** \`${pkg.contentFingerprintSha256}\`  
**Paid spend this run:** **$0**

## Audit membership (mutually exclusive)

| Disposition | Count |
|-------------|------:|
${Object.entries(membership.byDisposition)
  .map(([k, v]) => `| ${k} | ${v} |`)
  .join("\n")}
| **Total** | **${sumPrimary}** |

Derived (do **not** add to 55): future-cycle org potential **${membership.derivedFlags.organizationMeritsFutureAccountDevelopment}** · proposed visible **${membership.derivedFlags.recommendCustomerVisible}** · research/watch **${membership.derivedFlags.recommendResearchWatch}**.

## Expired one-time (exact)
**${expiredList.count}** IDs: ${(expiredList.events || []).map((e) => `\`${e.opportunityId}\``).join(", ")}

## Final active customer inventory
**${uniqueCustomer.length}** opportunities (audit retain + paid V3 net-new, expiration-gated).

## Research / watch
**${researchWatchFromAudit.length}** historical records moved out of active pursuit (research preserved).

## Gate
Permanent module: \`lib/group-demand-intelligence/commercial-expiration-gate-v1.js\`  
Wired into publication quality certification + weekly deep-research planning.  
Tests: \`npm run test:gdi-commercial-expiration-gate-v1\`

## Preview
\`http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged\`

No CUSTOMER_PUBLISHED. Bethesda untouched. Airtable dry-run only.
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);

console.log(
  JSON.stringify(
    {
      membershipOk: sumPrimary === 55,
      expired: expiredList.count,
      duplicates: dupReg.count,
      activeCustomer: uniqueCustomer.length,
      researchWatch: researchWatchFromAudit.length,
      fingerprint: pkg.contentFingerprintSha256,
      gateOk: gateCert.ok,
      qualityOk: quality.ok,
    },
    null,
    2,
  ),
);
