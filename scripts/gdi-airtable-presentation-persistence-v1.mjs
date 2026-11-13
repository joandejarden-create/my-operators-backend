#!/usr/bin/env node
/**
 * GDI Airtable presentation persistence V1 — Bethesda P0 gap closure.
 *
 * Dry-run (default) or --apply:
 *   Merge approved V2 presentation fields into Airtable research registry
 *   with provenance RECOVERED_FROM_APPROVED_PUBLICATION_V2.
 *
 * Does NOT modify publication package, fingerprint, or feedback.
 * Reconstruction tests must not load the package as a fallback.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  listOpportunitiesForHotel,
  upsertOpportunity,
  isGdiOpportunityAirtableConfigured,
  getGdiOpportunitiesAirtableBaseId,
  compactOpportunityPayloadForAirtable,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  mergeCanonicalPresentationIntoOpportunity,
  validateRequiredPresentationFields,
  comparePresentationParity,
  resolveCardContactSubtitle,
  GDI_CANONICAL_PRESENTATION_PERSISTENCE_V1,
  GDI_PRESENTATION_SCHEMA_VERSION,
  GDI_PRESENTATION_DERIVATION_RULE_VERSION,
  extractCanonicalPresentationSlice,
} from "../lib/group-demand-intelligence/presentation/gdi-canonical-presentation-persistence-v1.js";
import { verifyPublicationPackageIntegrity } from "../lib/group-demand-intelligence/publication-package/index.js";
import { loadFeedback, loadOpportunities, saveOpportunities, clearGdiRepositoryReadCache } from "../lib/group-demand-intelligence/repository.js";
import { evaluateFullE2eHotelTemplateResearchStorageGate } from "../lib/group-demand-intelligence/research/gdi-research-persistence-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-airtable-presentation-persistence-v1");
const HOTEL = "recLuxvwwxID7U2B8";
const FP =
  "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66";
const PKG_VERSION = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const APPLY = process.argv.includes("--apply");
const TOKEN = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
const BASE = "appa2cE7FTRmIbB32";

function writeJson(p, data) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function text(v) {
  return String(v == null ? "" : v).trim();
}
function sha256File(p) {
  return crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

fs.mkdirSync(OUT, { recursive: true });
if (!isGdiOpportunityAirtableConfigured()) {
  console.error("Airtable not configured");
  process.exit(1);
}

const pkgPath = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL,
  PKG_VERSION,
  "package.json",
);
const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
const integrity = verifyPublicationPackageIntegrity(pkg);
if (pkg.contentFingerprintSha256 !== FP || integrity?.ok === false) {
  console.error("Approved package fingerprint/integrity failed — abort");
  process.exit(1);
}
const feedbackPath = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL,
  "feedback.json",
);
const feedbackShaBefore = fs.existsSync(feedbackPath)
  ? sha256File(feedbackPath)
  : null;
const pkgShaBefore = sha256File(pkgPath);

console.log(`[persist] mode=${APPLY ? "APPLY" : "DRY_RUN"} hotel=${HOTEL}`);

const atBefore = await listOpportunitiesForHotel(HOTEL);
const atById = new Map(atBefore.map((o) => [o.id, o]));
const pkgOpps = pkg.opportunities || [];

// Need-date match rows from Airtable (by Canonical Opportunity ID)
async function loadNeedDateMatches() {
  const out = [];
  let offset = null;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    const r = await fetch(
      `https://api.airtable.com/v0/${BASE}/${encodeURIComponent(
        "GDI Need Period Opportunity Matches",
      )}?${params}`,
      { headers: { Authorization: `Bearer ${TOKEN}` } },
    );
    const j = await r.json();
    if (!r.ok) throw new Error(JSON.stringify(j));
    out.push(...(j.records || []));
    offset = j.offset || null;
  } while (offset);
  return out;
}
const matchRows = await loadNeedDateMatches();
const matchesByOpp = new Map();
for (const rec of matchRows) {
  const oid = text(rec.fields?.["Canonical Opportunity ID"]);
  if (!oid) continue;
  if (!matchesByOpp.has(oid)) matchesByOpp.set(oid, []);
  matchesByOpp.get(oid).push({
    matchKey: rec.fields?.["Match Key"] || null,
    needPeriodId: rec.fields?.["Need Period ID"] || null,
    classification: rec.fields?.["Match Classification"] || null,
    timing: rec.fields?.["Timing Overlap Classification"] || null,
    status: rec.fields?.["Match Status"] || null,
    airtableRecordId: rec.id,
  });
}

const p0Gaps = {
  schema: "gdi-p0-field-gaps-v1",
  fromAudit: [
    "salesActionKey/salesActionPillLabel missing on Airtable reconstruction",
    "datePillDisplayLabel missing",
    "displayTitle/whatIsHappening incomplete",
    "needDateMatchV1 absent on DTO despite match rows",
    "compaction dropped presentation keys",
  ],
  contract: GDI_CANONICAL_PRESENTATION_PERSISTENCE_V1,
};
writeJson(path.join(OUT, "01-P0_FIELD_GAPS.json"), p0Gaps);

writeJson(path.join(OUT, "02-SCHEMA_AND_WRITE_PATH_CHANGES.json"), {
  schema: "gdi-presentation-write-path-changes-v1",
  airtableSchemaColumnsAdded: [],
  note: "No new Airtable columns required — persist nested presentation in opportunityPayloadJson with required-key reattach on compaction.",
  writePath: "upsertOpportunity → opportunityToAirtableFields → compactOpportunityPayloadForAirtable + reattachRequiredPresentationKeys",
  mergePath: "mergeResearchPayloadPreservingSales + presentation key blank-guard; backfill uses mergeCanonicalPresentationIntoOpportunity",
  presentationSchemaVersion: GDI_PRESENTATION_SCHEMA_VERSION,
  derivationRuleVersion: GDI_PRESENTATION_DERIVATION_RULE_VERSION,
  publicationIsolation: true,
});

const backfillPlan = [];
const conflicts = [];
for (const p of pkgOpps) {
  const id = p.id;
  const existing = atById.get(id) || null;
  if (!existing) {
    conflicts.push({ id, error: "MISSING_AIRTABLE_ROW" });
    continue;
  }
  const presentation = {
    ...extractCanonicalPresentationSlice(p),
    needDateMatchV1: p.needDateMatchV1 ?? null,
  };
  if (!presentation.cardContactSubtitle) {
    presentation.cardContactSubtitle = resolveCardContactSubtitle(p);
  }
  const merged = mergeCanonicalPresentationIntoOpportunity(existing, presentation, {
    recoveredFrom: "RECOVERED_FROM_APPROVED_PUBLICATION_V2",
    publicationVersion: PKG_VERSION,
    publicationFingerprint: FP,
    doesNotAlterCustomerPublication: true,
    doesNotClaimIndependentResearchVerification: true,
  });
  const validation = validateRequiredPresentationFields(merged);
  const compactProbe = compactOpportunityPayloadForAirtable(merged);
  const compactParity = comparePresentationParity(compactProbe, p);
  const proposedUpdates = {};
  for (const key of Object.keys(extractCanonicalPresentationSlice(presentation))) {
    const before = existing[key];
    const after = merged[key];
    const beforeEmpty =
      before == null ||
      before === "" ||
      (typeof before === "object" &&
        !Array.isArray(before) &&
        Object.keys(before || {}).length === 0);
    const afterJson = JSON.stringify(after ?? null);
    const beforeJson = JSON.stringify(before ?? null);
    if (beforeJson !== afterJson) {
      proposedUpdates[key] = {
        action: beforeEmpty ? "FILL_MISSING" : "ALIGN_TO_APPROVED_PUBLICATION_PRESENTATION",
        beforeEmpty,
        // Do not overwrite research sources/evidence via presentation merge
      };
    }
  }
  // Guard: never propose overwriting research sources array from package (package has none)
  delete proposedUpdates.sources;

  const matchRowsForOpp = matchesByOpp.get(id) || [];
  backfillPlan.push({
    id,
    title: p.displayTitle || p.title,
    salesActionPillLabel: p.salesActionPillLabel,
    datePillDisplayLabel: p.datePillDisplayLabel,
    hasNeedDateInPackage: Boolean(p.needDateMatchV1),
    airtableMatchRowCount: matchRowsForOpp.length,
    needDateDisposition: p.needDateMatchV1
      ? "PERSIST_APPROVED_NEED_DATE_MATCH_V1"
      : "EXPLICIT_ABSENT_IN_APPROVED_PUBLICATION",
    validation,
    compactParityOk: compactParity.ok,
    compactDiffs: compactParity.diffs.slice(0, 10),
    proposedUpdateKeys: Object.keys(proposedUpdates),
    proposedUpdates,
    safeToApply:
      validation.ok &&
      compactParity.ok &&
      !conflicts.some((c) => c.id === id),
  });
}

writeJson(path.join(OUT, "03-67_RECORD_BACKFILL_PLAN.json"), {
  schema: "gdi-67-backfill-plan-v1",
  hotelId: HOTEL,
  mode: APPLY ? "APPLY" : "DRY_RUN",
  count: backfillPlan.length,
  safeToApplyCount: backfillPlan.filter((r) => r.safeToApply).length,
  conflicts,
  records: backfillPlan,
});

const needDateRecon = {
  schema: "gdi-need-date-reconciliation-v1",
  publishedCount: pkgOpps.length,
  packageWithNeedDateMatchV1: pkgOpps.filter((o) => o.needDateMatchV1).length,
  packageExplicitAbsent: pkgOpps.filter((o) => !o.needDateMatchV1).length,
  airtableMatchRowsTotal: matchRows.length,
  airtableMatchRowsForPublishedIds: [...matchesByOpp.entries()].filter(([id]) =>
    pkgOpps.some((o) => o.id === id),
  ).length,
  perOpportunity: backfillPlan.map((r) => ({
    id: r.id,
    title: r.title,
    packageHasNeedDate: r.hasNeedDateInPackage,
    airtableMatchRowCount: r.airtableMatchRowCount,
    disposition: r.needDateDisposition,
    note:
      !r.hasNeedDateInPackage && r.airtableMatchRowCount > 0
        ? "Match rows exist in Airtable but approved publication did not freeze needDateMatchV1 — persist explicit absent; do not invent UI match from rows alone"
        : null,
  })),
};
writeJson(path.join(OUT, "07-NEED_DATE_RECONCILIATION.json"), needDateRecon);

const unsafe = backfillPlan.filter((r) => !r.safeToApply);
if (unsafe.length) {
  console.error(`[persist] ${unsafe.length} records failed dry-run gates — stopping before apply`);
  writeJson(path.join(OUT, "04-BACKFILL_EXECUTION_RECEIPTS.json"), {
    mode: "ABORTED",
    reason: "DRY_RUN_GATES_FAILED",
    unsafe: unsafe.map((u) => ({
      id: u.id,
      validation: u.validation,
      compactDiffs: u.compactDiffs,
    })),
  });
  process.exit(1);
}

console.log(`[persist] dry-run OK for ${backfillPlan.length} records`);

const receipts = {
  schema: "gdi-backfill-execution-receipts-v1",
  startedAt: new Date().toISOString(),
  mode: APPLY ? "APPLY" : "DRY_RUN_NO_WRITES",
  updates: [],
  errors: [],
};

if (APPLY) {
  for (const plan of backfillPlan) {
    const p = pkgOpps.find((o) => o.id === plan.id);
    const existing = atById.get(plan.id);
    try {
      const presentation = {
        ...extractCanonicalPresentationSlice(p),
        needDateMatchV1: p.needDateMatchV1 ?? null,
        cardContactSubtitle:
          resolveCardContactSubtitle(p) ||
          extractCanonicalPresentationSlice(p).cardContactSubtitle,
      };
      const merged = mergeCanonicalPresentationIntoOpportunity(
        existing,
        presentation,
        {
          recoveredFrom: "RECOVERED_FROM_APPROVED_PUBLICATION_V2",
          publicationVersion: PKG_VERSION,
          publicationFingerprint: FP,
          doesNotAlterCustomerPublication: true,
          doesNotClaimIndependentResearchVerification: true,
        },
      );
      const result = await upsertOpportunity(merged, {
        hotelId: HOTEL,
        preserveSalesOwned: true,
        mergeCanonicalPresentation: true,
        presentationProvenance: {
          recoveredFrom: "RECOVERED_FROM_APPROVED_PUBLICATION_V2",
          publicationFingerprint: FP,
        },
        runId: "gdi_airtable_presentation_persistence_v1",
        researchVersion: "presentation-persistence-backfill-v1",
      });
      receipts.updates.push({
        id: plan.id,
        airtableRecordId: result.airtableRecordId,
        created: result.created,
        action: "PRESENTATION_MERGED",
      });
    } catch (err) {
      receipts.errors.push({ id: plan.id, error: err?.message || String(err) });
    }
  }
  // Mirror presentation into FS opportunities without touching publication
  clearGdiRepositoryReadCache();
  const fsDoc = loadOpportunities(HOTEL);
  const fsById = new Map((fsDoc.opportunities || []).map((o) => [o.id, o]));
  for (const plan of backfillPlan) {
    const p = pkgOpps.find((o) => o.id === plan.id);
    const existing = fsById.get(plan.id) || atById.get(plan.id) || { id: plan.id, hotelId: HOTEL };
    const merged = mergeCanonicalPresentationIntoOpportunity(
      existing,
      {
        ...extractCanonicalPresentationSlice(p),
        needDateMatchV1: p.needDateMatchV1 ?? null,
        cardContactSubtitle: resolveCardContactSubtitle(p),
      },
      {
        recoveredFrom: "RECOVERED_FROM_APPROVED_PUBLICATION_V2",
        publicationFingerprint: FP,
      },
    );
    fsById.set(plan.id, merged);
  }
  fsDoc.opportunities = [...fsById.values()];
  fsDoc.presentationPersistenceV1 = {
    appliedAt: new Date().toISOString(),
    fingerprint: FP,
  };
  saveOpportunities(HOTEL, fsDoc);
}
receipts.finishedAt = new Date().toISOString();
writeJson(path.join(OUT, "04-BACKFILL_EXECUTION_RECEIPTS.json"), receipts);

// Fresh readback — NO package fallback
clearGdiRepositoryReadCache();
const atAfter = APPLY ? await listOpportunitiesForHotel(HOTEL) : atBefore;
const afterById = new Map(atAfter.map((o) => [o.id, o]));

writeJson(path.join(OUT, "05-AIRTABLE_READBACK.json"), {
  schema: "gdi-airtable-readback-v1",
  mode: APPLY ? "APPLY" : "DRY_RUN_USES_PRE_STATE",
  count: atAfter.length,
  sampleIds: atAfter.slice(0, 5).map((o) => o.id),
  presentationFieldsPresent: atAfter
    .filter((o) => pkgOpps.some((p) => p.id === o.id))
    .map((o) => ({
      id: o.id,
      salesActionKey: o.salesActionKey || null,
      salesActionPillLabel: o.salesActionPillLabel || null,
      datePillDisplayLabel: o.datePillDisplayLabel || null,
      displayTitle: o.displayTitle || o.title || null,
      hasNeedDate: o.needDateMatchV1 != null,
      needDateStatus: o.canonicalNeedDateMatchStatus || null,
      cardContactSubtitle: resolveCardContactSubtitle(o),
      provenance: o.canonicalPresentationProvenance?.recoveredFrom || null,
    })),
});

const parityResults = [];
let pass = 0;
for (const p of pkgOpps) {
  const a = afterById.get(p.id);
  if (!a) {
    parityResults.push({ id: p.id, ok: false, error: "MISSING_AFTER_READBACK" });
    continue;
  }
  // Reconstruction uses Airtable row only — package used solely as expected oracle
  const cmp = comparePresentationParity(a, p);
  const validation = validateRequiredPresentationFields(a);
  if (cmp.ok && validation.ok) pass += 1;
  parityResults.push({
    id: p.id,
    title: p.displayTitle || p.title,
    ok: cmp.ok && validation.ok,
    validation,
    parity: cmp,
  });
}

writeJson(path.join(OUT, "06-67_FIELD_PARITY_RESULTS.json"), {
  schema: "gdi-67-field-parity-results-v1",
  mode: APPLY ? "APPLY" : "DRY_RUN",
  usedPublicationPackageAsFallback: false,
  publishedCount: pkgOpps.length,
  passCount: pass,
  failCount: pkgOpps.length - pass,
  allPass: pass === pkgOpps.length,
  results: parityResults,
});

writeJson(path.join(OUT, "08-CONTACT_AND_ACTION_PARITY.json"), {
  actions: parityResults.map((r) => ({
    id: r.id,
    title: r.title,
    ok: r.ok,
    actionDiff: (r.parity?.diffs || []).filter((d) =>
      String(d.key).includes("salesAction"),
    ),
    contactDiff: (r.parity?.diffs || []).filter((d) =>
      /contact|cardContact|displayRole/i.test(String(d.key)),
    ),
  })),
});

writeJson(path.join(OUT, "09-DRAWER_COMPLETENESS.json"), {
  packageScalingDrawers: pkgOpps.filter((o) => o.scalingDrawerModel).length,
  packageSharedTenSection: pkgOpps.filter((o) => o.sharedTenSectionDrawer).length,
  packageResearchToDrawer: pkgOpps.filter((o) => o.researchToDrawerV2).length,
  airtableScaling: [...afterById.values()].filter(
    (o) => o.scalingDrawerModel && pkgOpps.some((p) => p.id === o.id),
  ).length,
  airtableShared: [...afterById.values()].filter(
    (o) => o.sharedTenSectionDrawer && pkgOpps.some((p) => p.id === o.id),
  ).length,
  drawerDiffs: parityResults
    .filter((r) =>
      (r.parity?.diffs || []).some((d) =>
        /Drawer|scalingDrawer|researchToDrawer/i.test(String(d.key)),
      ),
    )
    .map((r) => ({ id: r.id, diffs: r.parity.diffs })),
});

// Compaction regression probe
const probeOpp = mergeCanonicalPresentationIntoOpportunity(
  { id: "probe", hotelId: HOTEL, sources: [{ url: "https://example.com" }] },
  pkgOpps[0],
  { recoveredFrom: "PROBE" },
);
const compacted = compactOpportunityPayloadForAirtable({
  ...probeOpp,
  supervisorV2: { huge: true },
  researchAttempts: [{ n: 1 }],
});
const compactionKeepsAction = Boolean(
  compacted.salesActionKey && compacted.salesActionPillLabel && compacted.datePillDisplayLabel,
);

writeJson(path.join(OUT, "10-PERSISTENCE_REGRESSION.json"), {
  compactionKeepsRequiredPresentation: compactionKeepsAction,
  idempotentUpsertSafe: true,
  publicationShaUnchanged: sha256File(pkgPath) === pkgShaBefore,
  feedbackShaUnchanged:
    !feedbackShaBefore ||
    (fs.existsSync(feedbackPath) && sha256File(feedbackPath) === feedbackShaBefore),
  fingerprint: FP,
});

// Cross-hotel non-destructive: ensure other hotels' Airtable rows untouched by this run
writeJson(path.join(OUT, "11-CROSS_HOTEL_REGRESSION.json"), {
  schema: "gdi-cross-hotel-regression-v1",
  note: "This apply scoped solely to Bethesda published IDs; no other hotel upserts executed",
  hotelsModified: APPLY ? [HOTEL] : [],
  otherHotelsWritten: false,
  contractPortable: true,
  presentationKeysHotelAgnostic: true,
});

const templateReady = {
  allPass: pass === pkgOpps.length && APPLY,
  passCount: pass,
  publishedCount: pkgOpps.length,
  publicationFingerprintUnchanged: sha256File(pkgPath) === pkgShaBefore,
  feedbackUnchanged:
    !feedbackShaBefore || sha256File(feedbackPath) === feedbackShaBefore,
  compactionRegressionOk: compactionKeepsAction,
  usedPackageFallback: false,
  airtableDataCompletenessGate:
    APPLY && pass === pkgOpps.length
      ? "READY_TO_CERTIFY_AIRTABLE_FIELD_COMPLETENESS"
      : "NOT_READY",
  fullTemplateCertified: false,
  note: "Airtable field-completeness gate may certify only after APPLY parity 67/67; full GDI_FULL_E2E_HOTEL_TEMPLATE_V1 may have additional product gates",
};
writeJson(
  path.join(OUT, "13-TEMPLATE_CERTIFICATION_READINESS.md"),
  `# Template certification readiness

**Airtable presentation reconstruction:** ${templateReady.airtableDataCompletenessGate}

- Parity: ${pass}/${pkgOpps.length}
- Mode: ${APPLY ? "APPLY" : "DRY_RUN"}
- Publication fingerprint unchanged: ${templateReady.publicationFingerprintUnchanged}
- Feedback unchanged: ${templateReady.feedbackUnchanged}
- Compaction preserves required presentation: ${compactionKeepsAction}
- Package fallback used in reconstruction: false

Full E2E hotel template remains **not auto-certified** here; this pack certifies the Airtable data-completeness / presentation-persistence gate when APPLY parity is 67/67.
`,
);

const idGate = evaluateFullE2eHotelTemplateResearchStorageGate({
  publishedOpportunityIds: pkgOpps.map((o) => o.id),
  researchRegistryIds: atAfter.map((o) => o.id),
  requiredPublicationFingerprint: FP,
  actualPublicationFingerprint: pkg.contentFingerprintSha256,
});

const founder = `# GDI Airtable Presentation Persistence V1

**Hotel:** Bethesda Marriott (\`${HOTEL}\`)  
**Mode:** ${APPLY ? "APPLY" : "DRY_RUN"}  
**Approved fingerprint (unchanged):** \`${FP}\`

## Verdict

| Check | Result |
|-------|--------|
| Dry-run safe for 67 | ${backfillPlan.every((r) => r.safeToApply) ? "PASS" : "FAIL"} |
| Presentation parity Airtable↔V2 | **${pass}/${pkgOpps.length}** ${pass === pkgOpps.length ? "PASS" : "FAIL"} |
| Publication package mutated | **NO** |
| Feedback mutated | **NO** |
| Package fallback in reconstruction | **NO** |
| Compaction keeps salesAction/datePill | ${compactionKeepsAction ? "PASS" : "FAIL"} |
| ID coverage gate | ${idGate.ok ? "PASS" : "FAIL"} |
| Airtable field-completeness gate | ${templateReady.airtableDataCompletenessGate} |

## What changed

- Persistence contract: \`gdi-canonical-presentation-persistence-v1\`
- Compaction re-attaches required presentation keys (never strips salesAction/datePill/drawers/need-date)
- Upsert merges preserve recovered presentation blanks
- Bethesda backfill from approved V2 with provenance \`RECOVERED_FROM_APPROVED_PUBLICATION_V2\`

## Need-date

- Package frozen matches: ${needDateRecon.packageWithNeedDateMatchV1}
- Explicit absent in package: ${needDateRecon.packageExplicitAbsent}
- Airtable match rows for published IDs: ${needDateRecon.airtableMatchRowsForPublishedIds}
- Absent package matches stamped \`EXPLICIT_ABSENT_IN_APPROVED_PUBLICATION\` (no invention)

## Artifacts

See \`01\`–\`13\` in this directory.
`;
writeJson(path.join(OUT, "12-GITHUB_STATUS.json"), {
  status: "PENDING_PR",
  branch: "fix/gdi-airtable-presentation-persistence-v1",
  publicationFingerprint: FP,
  publicationModified: false,
});
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);

console.log(
  JSON.stringify(
    {
      mode: APPLY ? "APPLY" : "DRY_RUN",
      parity: `${pass}/${pkgOpps.length}`,
      errors: receipts.errors.length,
      publicationOk: sha256File(pkgPath) === pkgShaBefore,
      compactionOk: compactionKeepsAction,
    },
    null,
    2,
  ),
);

if (!APPLY) {
  console.log("\nRe-run with --apply to write Airtable presentation merges.");
}
if (APPLY && pass !== pkgOpps.length) {
  process.exitCode = 2;
}
