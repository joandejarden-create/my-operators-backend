#!/usr/bin/env node
/**
 * Prepare Spice Island seven-ID durable publication manifest + architecture report.
 * Classification approved by founder — publication NOT authorized.
 * No production writes, deploys, paid research, or tokens.
 */
import { mkdirSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { mergeLocalShadowCommercialOpportunities } from "../lib/group-demand-intelligence/local-shadow-commercial-portfolio-v1.js";
import { resolveCanonicalCustomerFacingTitle } from "../lib/group-demand-intelligence/demand-segments/universal-opportunity-identity-v1.js";
import {
  createGdiPublishedInventoryRegistry,
  mergePublishedInventoryIntoOpportunities,
  COMMERCIAL_PUBLICATION_CLASS,
} from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import "../load-env.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-durable-publication-registry-spice-island-v1"
);
const HOTEL = "recKRJjcPnb4tVDDS";
const HOTEL_NAME = "Spice Island Beach Resort";

/** Exact IDs from certified recovery package — founder classification approval. */
const APPROVED = [
  {
    opportunityId: "gdi_s4912_ba7b1cb8a3ab",
    classification: COMMERCIAL_PUBLICATION_CLASS.SALES_READY,
  },
  {
    opportunityId: "gdi_s4912_b956aa67a4a2",
    classification: COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT,
  },
  {
    opportunityId: "gdi_s4912_b6d9b1929c05",
    classification: COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT,
  },
  {
    opportunityId: "gdi_s4912_cb2d71ab1b15",
    classification: COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT,
  },
  {
    opportunityId: "gdi_s4912_cc2a8da6ca6e",
    classification: COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT,
  },
  {
    opportunityId: "gdi_s4912_03deda917778",
    classification: COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT,
  },
  {
    opportunityId: "gdi_s4912_3cafec77a21f",
    classification: COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT,
  },
];
const EXCLUDED = [
  {
    opportunityId: "gdi_sc_f255eb5e9202",
    reason: "Aventa PAST_CLOSED — founder rejected",
  },
];

function sampleUrls(o, n = 5) {
  return [
    ...new Set(
      (o.sources || [])
        .map((s) => (typeof s === "string" ? s : s?.url))
        .filter((u) => /^https?:\/\//i.test(String(u || "")))
    ),
  ].slice(0, n);
}

function buildSnapshot(o, classification) {
  return {
    id: o.id,
    hotelId: HOTEL,
    title: resolveCanonicalCustomerFacingTitle(o),
    organizationName: o.organizationName || o.company || o.companyName || null,
    segment: o.segment || o.researchSegment || null,
    whyThisHotel: o.whyThisHotel || o.summaryWhyHotel || null,
    summaryWhyHotel: o.summaryWhyHotel || o.whyThisHotel || null,
    hotelFitRationale: o.hotelFitRationale || o.whyThisHotel || null,
    recommendedNextStep:
      o.recommendedNextStep || o.recommendedAction || o.commercialNextStep || null,
    primaryContact: o.primaryContact || null,
    organizationContactUrl: o.organizationContactUrl || o.publicContactPath || null,
    sources: (o.sources || []).slice(0, 20),
    evidenceUrls: sampleUrls(o, 20),
    eventStartDate: o.eventStartDate || o.startDate || null,
    eventEndDate: o.eventEndDate || o.endDate || null,
    commercialInventoryClass: classification,
    customerFacing: true,
  };
}

mkdirSync(OUT, { recursive: true });
const merged = mergeLocalShadowCommercialOpportunities(HOTEL, []);

const manifestRows = [];
const validationFailures = [];
for (const spec of APPROVED) {
  const o = merged.find((x) => x.id === spec.opportunityId);
  if (!o) {
    validationFailures.push({
      opportunityId: spec.opportunityId,
      error: "MISSING_FROM_LOCAL_INVENTORY",
    });
    continue;
  }
  const title = resolveCanonicalCustomerFacingTitle(o);
  const contact = o.primaryContact || {};
  const evidence = sampleUrls(o, 50);
  const next = String(
    o.recommendedNextStep || o.recommendedAction || o.commercialNextStep || ""
  ).trim();
  const hotelRelevance = String(
    o.whyThisHotel || o.summaryWhyHotel || o.hotelFitRationale || ""
  ).trim();
  const fails = [];
  if (!title) fails.push("missing_canonical_title");
  if (!contact.name) fails.push("missing_named_contact");
  if (!hotelRelevance) fails.push("missing_hotel_relevance");
  if (!evidence.length) fails.push("missing_evidence_urls");
  if (next.length < 20) fails.push("missing_sales_next_step");
  if (fails.length) {
    validationFailures.push({ opportunityId: spec.opportunityId, fails });
  }
  manifestRows.push({
    opportunityId: spec.opportunityId,
    commercialClassification: spec.classification,
    canonicalTitle: title,
    contact: {
      name: contact.name || null,
      role: contact.role || contact.title || null,
    },
    hotelRelevanceSnippet: hotelRelevance.slice(0, 200),
    evidenceUrlCount: evidence.length,
    evidenceSample: evidence.slice(0, 3),
    salesNextStep: next.slice(0, 220),
    organizationContactUrl: o.organizationContactUrl || o.publicContactPath || null,
    validationOk: fails.length === 0,
    validationFails: fails,
    snapshot: buildSnapshot(o, spec.classification),
  });
}

// Duplicate protection among approved set
const idSet = new Set(manifestRows.map((r) => r.opportunityId));
const duplicateCheck = {
  approvedCount: APPROVED.length,
  uniqueIds: idSet.size,
  ok: idSet.size === APPROVED.length && !idSet.has(EXCLUDED[0].opportunityId),
};

// Isolated simulation publish of the seven (temp root — not production)
const simRoot = path.join(os.tmpdir(), `gdi-spice-pub-prep-${Date.now()}`);
mkdirSync(simRoot, { recursive: true });
const simReg = createGdiPublishedInventoryRegistry({ root: simRoot });
const simApproval = {
  authorized: true,
  authorizedBy: "founder_classification_approved_simulation_only",
  authorizationPhrase:
    "CLASSIFICATION_APPROVED_SIMULATION — not production publication authorization",
  note: "Local durability prep only",
};
const simPublish = [];
for (const row of manifestRows) {
  const r = simReg.publishOpportunity({
    hotelId: HOTEL,
    opportunityId: row.opportunityId,
    classification: row.commercialClassification,
    approval: simApproval,
    sourceResearchReference:
      "reports/gdi-spice-island-commercial-readiness-recovery-v1",
    snapshot: row.snapshot,
  });
  simPublish.push({
    opportunityId: row.opportunityId,
    publishedVersion: r.row.publishedVersion,
    state: r.row.publicationState,
  });
}
// Leakage check: research-only Aventa must not appear in strict merge
const leakTest = mergePublishedInventoryIntoOpportunities(
  HOTEL,
  [
    ...manifestRows.map((r) => r.snapshot),
    {
      id: EXCLUDED[0].opportunityId,
      title: "Aventa — must not leak",
      customerVisible: true,
    },
  ],
  { registry: simReg, strictPublishedOnly: true }
);

const durabilityTest = spawnSync(
  process.execPath,
  [path.join(ROOT, "scripts/test-gdi-published-inventory-registry-v1.mjs")],
  { encoding: "utf8", cwd: ROOT }
);
let durabilityJson = null;
try {
  durabilityJson = JSON.parse(durabilityTest.stdout || "{}");
} catch {
  durabilityJson = { ok: false, parseError: true, raw: (durabilityTest.stdout || "").slice(0, 500) };
}

const architecture = {
  recommended: {
    primarySoT: "Airtable GDI base (opportunity payload + publication registry fields/table)",
    rationale: [
      "Production already uses GDI_OPPORTUNITIES_PERSISTENCE=airtable — weekly upserts need no Railway redeploy",
      "Spice Island already on GDI_REVIEWED_PROSPECT_HOTEL_IDS / built-in 33-hotel allowlist",
      "Avoids per-hotel lib/ packs (Waterstone clean-28 is a lean-deploy fallback, not weekly SoT)",
      "Share path already loads via loadOpportunitiesCanonical → filterCustomerFacingOpportunities",
    ],
    secondaryMirror:
      "Filesystem registry on Railway volume (GDI_PUBLISHED_INVENTORY_ROOT) — same schema as local simulation; mirrors ADP durable-publication pattern; required for share-token registry parity (GDI_SHARE_REGISTRY_DIR already volume-oriented)",
    notRecommendedAsPrimary: [
      "lib/ client-facing JSON packs — require code deploy for inventory changes",
      "Ephemeral container disk / data/* railwayignored local-shadow alone",
    ],
  },
  existingCompatibility: {
    airtableGdiBase: "AIRTABLE_GDI_BASE_ID / opportunity-persistence.js",
    reviewedProspectAllowlist: "Spice Island already authorized for list path",
    gdishareV1: "issueGdiShareCapability + /group-demand-intelligence-share.html; reads hotel opportunities via loadOppDoc",
    waterstoneLibPack: "Keep as emergency freeze fallback only; migrate Waterstone IDs into registry later under separate GO",
    adpFilesystemDurableStore: "Pattern reuse for volume layout + atomic writes + history",
  },
  schema: {
    contract: "GDI_PUBLISHED_INVENTORY_REGISTRY_V1",
    requiredFields: [
      "hotelId",
      "opportunityId",
      "commercialClassification (SALES_READY | ACCOUNT_DEVELOPMENT)",
      "publicationState (STAGED | PUBLISHED | REVOKED | SUPERSEDED)",
      "publishedVersion",
      "publishedAt",
      "sourceResearchReference",
      "approval { authorized, authorizedBy, authorizedAt, authorizationPhrase }",
      "revokedAt / revokeReason",
      "lastVerifiedAt",
      "contentHash",
      "audit history (append-only PUBLISH|UPDATE|REVOKE events)",
    ],
    separation:
      "Research eligibility computed at read time; publication requires explicit approval record — never auto-promote",
    accountDevelopmentFirstClass: true,
  },
};

const migrationRollback = {
  migrate: [
    "1. Deploy registry module + merge hook behind GDI_PUBLISHED_INVENTORY_REGISTRY_V1=0 (no behavior change)",
    "2. Create Airtable publication registry table (or payload stamps) matching schema",
    "3. Mount Railway volume; set GDI_PUBLISHED_INVENTORY_ROOT (mirror) + keep GDI_SHARE_REGISTRY_DIR",
    "4. Dual-write publish API: Airtable primary, FS mirror secondary",
    "5. Enable merge flag per hotel after founder GO — Spice first",
    "6. Do not migrate Waterstone/Bethesda inventories until separate authorization",
  ],
  rollback: [
    "Set GDI_PUBLISHED_INVENTORY_REGISTRY_V1=0",
    "Revoke Spice rows in registry (history preserved)",
    "Share continues on prior Airtable/reviewed-prospect path",
    "Lib-pack Waterstone fallback unchanged",
  ],
};

const activationSequence = {
  status: "PREPARED_NOT_EXECUTED",
  steps: [
    "Deploy validated publication infrastructure (flag off initially)",
    "Verify preservation of all existing published GDI opportunities (Bethesda/Waterstone/etc.)",
    "Synchronize Spice Island seven approved records into durable registry + Airtable",
    "Verify production inventory count/classes/titles for Spice",
    "Issue Spice Island gdishare.v1 capability via issueGdiShareCapability",
    "Test logged-out external access on /group-demand-intelligence-share.html",
    "Return verified URL for Sheldon",
  ],
  gdishareReadPath:
    "After flag enable: loadOppDoc → mergePublishedInventoryIntoOpportunities(hotelId, opportunities) before filterCustomerFacingOpportunities; share sanitize continues unchanged",
};

const authPhrase =
  "FOUNDER GO — GDI durable publication registry production activation: deploy GDI_PUBLISHED_INVENTORY_REGISTRY_V1, preserve existing published inventories, sync Spice Island exact seven IDs, verify production, issue gdishare.v1 for recKRJjcPnb4tVDDS";

const executive = {
  contract: "GDI_DURABLE_PUBLICATION_REGISTRY_SPICE_ISLAND_V1",
  generatedAt: new Date().toISOString(),
  status: "FOUNDER_GO_REQUIRED_FOR_PRODUCTION_ACTIVATION",
  hotel: { id: HOTEL, name: HOTEL_NAME },
  classificationApprovalHonored: true,
  publicationAuthorized: false,
  exactSevenPublicationIds: APPROVED.map((a) => a.opportunityId),
  excluded: EXCLUDED,
  manifestValidation: {
    ok: validationFailures.length === 0 && duplicateCheck.ok,
    validationFailures,
    duplicateCheck,
  },
  durabilityTests: {
    suite: "scripts/test-gdi-published-inventory-registry-v1.mjs",
    ok: durabilityJson?.ok === true,
    passed: durabilityJson?.passed ?? null,
    failed: durabilityJson?.failed ?? null,
    exitCode: durabilityTest.status,
  },
  simulationPublish: {
    root: simRoot,
    note: "Isolated temp simulation only — not production",
    rows: simPublish,
    avantaExcludedFromStrictMerge: !leakTest.some(
      (o) => o.id === EXCLUDED[0].opportunityId
    ),
  },
  recommendedStorageArchitecture: architecture,
  migrationAndRollback: migrationRollback,
  productionDeploymentRisks: [
    "Enabling merge without dual-write could hide Airtable-only rows if strictPublishedOnly mis-set — default merge is additive",
    "Railway volume not yet attached — FS mirror empty until mount + env",
    "Migrating Waterstone away from lib pack without registry rows would drop share inventory — out of scope until separate GO",
    "Share token issuance still requires production GDI_SHARE_CAPABILITY_SECRET (already configured)",
    "Classification approval ≠ publication approval — this package stops before sync/token",
  ],
  activationSequence,
  exactAuthorizationPhraseRequired: authPhrase,
  notExecuted: [
    "production writes",
    "Railway deployments",
    "Airtable publication upserts",
    "paid research",
    "share-token issuance",
    "existing inventory migration",
  ],
};

writeFileSync(path.join(OUT, "00-executive-verdict.json"), JSON.stringify(executive, null, 2));
writeFileSync(
  path.join(OUT, "01-recommended-architecture.json"),
  JSON.stringify(architecture, null, 2)
);
writeFileSync(
  path.join(OUT, "02-spice-island-seven-id-publication-manifest.json"),
  JSON.stringify(
    {
      hotelId: HOTEL,
      hotelName: HOTEL_NAME,
      approvedCount: APPROVED.length,
      excluded: EXCLUDED,
      rows: manifestRows.map(({ snapshot, ...rest }) => rest),
      snapshots: manifestRows.map((r) => ({
        opportunityId: r.opportunityId,
        classification: r.commercialClassification,
        snapshot: r.snapshot,
      })),
    },
    null,
    2
  )
);
writeFileSync(
  path.join(OUT, "03-durability-test-results.json"),
  JSON.stringify(
    { durabilityJson, simulationPublish: executive.simulationPublish },
    null,
    2
  )
);
writeFileSync(
  path.join(OUT, "04-migration-rollback-and-activation.json"),
  JSON.stringify(
    { migrationRollback, activationSequence, risks: executive.productionDeploymentRisks },
    null,
    2
  )
);

const md = `# GDI Durable Publication Registry — Spice Island Prep

**Status:** FOUNDER_GO_REQUIRED_FOR_PRODUCTION_ACTIVATION  
**Generated:** ${executive.generatedAt}

## Founder classification (honored)

| Class | Count | IDs |
|-------|------:|-----|
| Sales Ready | 1 | \`gdi_s4912_ba7b1cb8a3ab\` (CARPHA) |
| Account Development | 6 | Beautiful Weddings, Hangers, A'yesha, St. James, Savor 10, Kenwood |
| Rejected / excluded | 1 | Aventa \`gdi_sc_f255eb5e9202\` |

Classification approval ≠ publication authorization.

## 1. Recommended storage architecture

**Primary SoT: Airtable GDI base** (already production opportunity persistence; weekly updates without redeploy).  
**Secondary mirror: Railway volume filesystem registry** (\`GDI_PUBLISHED_INVENTORY_ROOT\`) using the implemented \`GDI_PUBLISHED_INVENTORY_REGISTRY_V1\` schema (ADP durable-store pattern).  
**Not primary:** Waterstone-style \`lib/\` packs (code deploy required).

## 2. Existing-system compatibility

- Reviewed-prospect allowlist already includes Spice Island  
- \`gdishare.v1\` reads hotel opportunities via \`loadOppDoc\` — merge hook prepared, **not enabled**  
- Waterstone lib pack retained as freeze fallback until separate migration GO  

## 3. Migration / rollback

See \`04-migration-rollback-and-activation.json\`.

## 4. Durability tests

Suite: \`node scripts/test-gdi-published-inventory-registry-v1.mjs\` → **${durabilityJson?.ok ? "PASS" : "FAIL"}** (${durabilityJson?.passed ?? "?"}/${(durabilityJson?.passed ?? 0) + (durabilityJson?.failed ?? 0)})

Covered: publish, update, revoke, history, unauthorized promotion block, restart reload, simulated volume redeploy, multi-hotel, weekly incremental, unpublished research leakage prevention, Account Development first-class.

## 5. Exact seven Spice Island publication IDs

${APPROVED.map((a) => `- \`${a.opportunityId}\` — ${a.classification}`).join("\n")}

Manifest validation: **${executive.manifestValidation.ok ? "PASS" : "FAIL"}**

## 6. Production deployment risks

${executive.productionDeploymentRisks.map((r) => `- ${r}`).join("\n")}

## 7. Required founder authorization

\`\`\`
${authPhrase}
\`\`\`

No production writes, deployments, paid research, or token issuance executed.
`;

writeFileSync(path.join(OUT, "README.md"), md);
console.log(
  JSON.stringify(
    {
      status: executive.status,
      exactSeven: executive.exactSevenPublicationIds,
      manifestOk: executive.manifestValidation.ok,
      durabilityOk: executive.durabilityTests.ok,
      authPhrase,
    },
    null,
    2
  )
);
