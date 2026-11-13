#!/usr/bin/env node
/**
 * GDI research storage reconciliation V1 — Bethesda.
 *
 * Phases: snapshot → 86-id reconcile → validate 27 → disposition 19 →
 * dry-run → optional --apply CREATE-only Airtable upserts → FS mirror append →
 * quarantine stale data/ CURRENT → refresh dry-run → report pack.
 *
 * Does NOT modify bundled V2 publication package or fingerprint.
 * Does NOT create new opportunity IDs. Does NOT paid discovery.
 * Does NOT update existing Airtable rows (create-only for missing IDs).
 *
 * Usage:
 *   node scripts/gdi-research-storage-reconciliation-v1.mjs --dry-run
 *   node scripts/gdi-research-storage-reconciliation-v1.mjs --apply
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  listOpportunitiesForHotel,
  upsertOpportunity,
  findOpportunityRecordById,
  isGdiOpportunityAirtableConfigured,
  getGdiOpportunitiesAirtableBaseId,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  loadOpportunities,
  saveOpportunities,
  loadFeedback,
  hotelDir,
  clearGdiRepositoryReadCache,
} from "../lib/group-demand-intelligence/repository.js";
import {
  createGdiPublicationPackageStore,
  resolvePublicationPackageRoot,
} from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { verifyPublicationPackageIntegrity } from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  GDI_RESEARCH_PERSISTENCE_CONTRACT_V1,
  GDI_RESEARCH_STORAGE_ROLES,
  buildHistoricalRegistryUpsertCandidate,
  evaluateFullE2eHotelTemplateResearchStorageGate,
  assertPublishedIdsCoveredByResearchRegistry,
} from "../lib/group-demand-intelligence/research/gdi-research-persistence-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const APPROVED_FP =
  "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66";
const APPROVED_VERSION = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const OUT = path.join(ROOT, "reports/gdi-research-storage-reconciliation-v1");
const SNAP_DIR = path.join(OUT, "pre-migration-snapshots");
const APPLY = process.argv.includes("--apply");
const DRY_RUN = !APPLY;

function text(v) {
  return String(v == null ? "" : v).trim();
}
function sha256File(filePath) {
  const buf = fs.readFileSync(filePath);
  return {
    path: path.relative(ROOT, filePath).replace(/\\/g, "/"),
    bytes: buf.length,
    sha256: crypto.createHash("sha256").update(buf).digest("hex"),
  };
}
function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function copyFile(src, dest) {
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.copyFileSync(src, dest);
  return sha256File(dest);
}
function idsOf(list) {
  return new Set((list || []).map((o) => text(o.id || o.opportunityId)).filter(Boolean));
}

fs.mkdirSync(OUT, { recursive: true });
fs.mkdirSync(SNAP_DIR, { recursive: true });

const hotelRoot = hotelDir(HOTEL);
const bundledPkgPath = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL,
  APPROVED_VERSION,
  "package.json",
);
const bundledCurrentPath = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL,
  "CURRENT.json",
);
const dataCurrentPath = path.join(
  ROOT,
  "data/group-demand-intelligence/publication-packages",
  HOTEL,
  "CURRENT.json",
);
const reconPath = path.join(hotelRoot, "reconstructed-opportunities-v1.json");
const fsOppPath = path.join(hotelRoot, "opportunities.json");
const feedbackPath = path.join(hotelRoot, "feedback.json");
const freezePath = path.join(
  ROOT,
  "reports/gdi-local-master-publication-v1/local-master-snapshot/local-live-opportunities.json",
);

console.log(`[gdi-recon] mode=${APPLY ? "APPLY" : "DRY_RUN"} hotel=${HOTEL}`);

if (!isGdiOpportunityAirtableConfigured()) {
  console.error("Airtable not configured — aborting");
  process.exit(1);
}

// ─── 1. Pre-migration snapshots ─────────────────────────────────────────────
const snapshotFiles = [];
const snapPairs = [
  [fsOppPath, "opportunities.json"],
  [reconPath, "reconstructed-opportunities-v1.json"],
  [feedbackPath, "feedback.json"],
  [bundledPkgPath, "bundled-v2-package.json"],
  [bundledCurrentPath, "bundled-CURRENT.json"],
  [dataCurrentPath, "data-CURRENT.json"],
];
for (const [src, name] of snapPairs) {
  if (!fs.existsSync(src)) {
    snapshotFiles.push({ name, missing: true });
    continue;
  }
  snapshotFiles.push({ name, ...copyFile(src, path.join(SNAP_DIR, name)), source: sha256File(src) });
}

// Snapshot research-state file list (not full 200 bodies — index + hashes of names)
const orsDir = path.join(hotelRoot, "opportunity-research-state");
const orsFiles = fs.existsSync(orsDir)
  ? fs.readdirSync(orsDir).filter((n) => n.endsWith(".json"))
  : [];
const runsDir = path.join(hotelRoot, "runs");
const runIds = fs.existsSync(runsDir)
  ? fs.readdirSync(runsDir).filter((n) =>
      fs.existsSync(path.join(runsDir, n, "run.json")),
    )
  : [];
writeJson(path.join(SNAP_DIR, "research-state-index.json"), {
  opportunityResearchStateCount: orsFiles.length,
  opportunityResearchStateIds: orsFiles.map((n) => n.replace(/\.json$/i, "")),
  runIds,
});

const airtableBefore = await listOpportunitiesForHotel(HOTEL);
writeJson(path.join(SNAP_DIR, "airtable-opportunities.json"), {
  hotelId: HOTEL,
  baseId: getGdiOpportunitiesAirtableBaseId(),
  fetchedAt: new Date().toISOString(),
  count: airtableBefore.length,
  ids: airtableBefore.map((o) => o.id),
  opportunities: airtableBefore,
});

const pkg = JSON.parse(fs.readFileSync(bundledPkgPath, "utf8"));
const integrity = verifyPublicationPackageIntegrity(pkg);
const fsDoc = loadOpportunities(HOTEL);
const recon = JSON.parse(fs.readFileSync(reconPath, "utf8"));
const feedback = loadFeedback(HOTEL);
const freeze = fs.existsSync(freezePath)
  ? JSON.parse(fs.readFileSync(freezePath, "utf8"))
  : null;
const bundledCurrent = JSON.parse(fs.readFileSync(bundledCurrentPath, "utf8"));
const dataCurrent = fs.existsSync(dataCurrentPath)
  ? JSON.parse(fs.readFileSync(dataCurrentPath, "utf8"))
  : null;

const pkgIds = idsOf(pkg.opportunities);
const fsIds = idsOf(fsDoc.opportunities);
const atIds = idsOf(airtableBefore);
const reconIds = idsOf(recon.opportunities);
const feedbackOppIds = new Set(
  (feedback.items || []).map((i) => text(i.opportunityId)).filter(Boolean),
);

const allIds = new Set([...pkgIds, ...atIds]);
const surfaceRows = new Map(
  (
    freeze?.bethesdaInternalUnion?.commercialSurface?.summary?.rows || []
  ).map((r) => [text(r.id), r]),
);

const preMigrationSnapshot = {
  schema: "gdi-research-storage-reconciliation-pre-migration-v1",
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL,
  mode: APPLY ? "APPLY" : "DRY_RUN",
  airtable: {
    baseId: getGdiOpportunitiesAirtableBaseId(),
    count: airtableBefore.length,
    ids: [...atIds].sort(),
  },
  fsOpportunities: { count: fsIds.size, ids: [...fsIds].sort() },
  reconstructed: { count: reconIds.size, ids: [...reconIds].sort() },
  publication: {
    version: APPROVED_VERSION,
    fingerprint: pkg.contentFingerprintSha256,
    expectedFingerprint: APPROVED_FP,
    fingerprintMatch: pkg.contentFingerprintSha256 === APPROVED_FP,
    integrityOk: integrity?.ok !== false,
    count: pkgIds.size,
  },
  feedback: {
    itemCount: (feedback.items || []).length,
    opportunityIds: [...feedbackOppIds].sort(),
    sha256: sha256File(feedbackPath).sha256,
  },
  bundledCurrent,
  dataCurrent,
  snapshotFiles,
  researchState: {
    opportunityResearchStateCount: orsFiles.length,
    runCount: runIds.length,
  },
  counts: {
    published: pkgIds.size,
    airtable: atIds.size,
    intersection: [...pkgIds].filter((id) => atIds.has(id)).length,
    packageOnly: [...pkgIds].filter((id) => !atIds.has(id)).length,
    airtableOnly: [...atIds].filter((id) => !pkgIds.has(id)).length,
    distinctUnion: allIds.size,
  },
};
const priorSnapPath = path.join(OUT, "01-PRE_MIGRATION_SNAPSHOT.json");
const priorSnap = fs.existsSync(priorSnapPath)
  ? JSON.parse(fs.readFileSync(priorSnapPath, "utf8"))
  : null;
// Preserve the true pre-migration baseline (59) once captured; later idempotent
// runs see 86 and must not rewrite the founder baseline artifact.
if (
  priorSnap?.counts?.airtable === 59 &&
  airtableBefore.length >= 59 &&
  priorSnap.counts.packageOnly === 27
) {
  writeJson(path.join(OUT, "01-PRE_MIGRATION_SNAPSHOT.latest-run.json"), preMigrationSnapshot);
} else {
  writeJson(priorSnapPath, preMigrationSnapshot);
}

// ─── 2. 86-ID reconciliation ────────────────────────────────────────────────
const reconById = new Map((recon.opportunities || []).map((o) => [text(o.id), o]));
const pkgById = new Map((pkg.opportunities || []).map((o) => [text(o.id), o]));
const atById = new Map(airtableBefore.map((o) => [text(o.id), o]));
const fsById = new Map((fsDoc.opportunities || []).map((o) => [text(o.id), o]));

const reconciliation86 = [...allIds].sort().map((id) => {
  const surface = surfaceRows.get(id) || null;
  const r = reconById.get(id) || null;
  const p = pkgById.get(id) || null;
  const a = atById.get(id) || null;
  const f = fsById.get(id) || null;
  return {
    id,
    title: text(
      p?.displayTitle ||
        p?.title ||
        a?.title ||
        a?.displayTitle ||
        r?.displayTitle ||
        r?.title ||
        f?.title,
    ),
    inPublication: pkgIds.has(id),
    inAirtable: atIds.has(id),
    inFsMirror: fsIds.has(id),
    inReconstructed: reconIds.has(id),
    inFeedback: feedbackOppIds.has(id),
    inOpportunityResearchState: orsFiles.some(
      (n) => n.replace(/\.json$/i, "") === id,
    ),
    commercialSurface: surface?.surface || null,
    commercialSurfaceReason: surface?.reason || null,
    airtablePriority: a?.priority || null,
    packagePriority: p?.priority || null,
    reconstructionStatus: r?.reconstructionStatus || null,
    researchStatus: r?.researchStatus || null,
    duplicateOfCanonical: r?.duplicateOfCanonical || null,
    relatedIds: r?.relatedIds || null,
    sourceCount: Array.isArray(r?.sources)
      ? r.sources.length
      : Array.isArray(a?.sources)
        ? a.sources.length
        : 0,
    hasContact: Boolean(
      text(p?.primaryContact?.name || a?.primaryContact?.name || r?.primaryContact?.name),
    ),
    membership:
      pkgIds.has(id) && atIds.has(id)
        ? "BOTH_PUBLISHED_AND_AIRTABLE"
        : pkgIds.has(id)
          ? "PUBLICATION_ONLY"
          : "AIRTABLE_ONLY",
  };
});
writeJson(path.join(OUT, "02-86_ID_RECONCILIATION.json"), {
  schema: "gdi-86-id-reconciliation-v1",
  hotelId: HOTEL,
  distinctCount: reconciliation86.length,
  expectedUnion: 86,
  records: reconciliation86,
});

// ─── 3. Validate 27 ─────────────────────────────────────────────────────────
const packageOnlyIds = [...pkgIds].filter((id) => !atIds.has(id));
const validation27 = [];
const upsertCandidates = [];
const quarantined = [];

for (const id of packageOnlyIds.sort()) {
  const built = buildHistoricalRegistryUpsertCandidate({
    hotelId: HOTEL,
    packageOpportunity: pkgById.get(id),
    reconstructedOpportunity: reconById.get(id),
    publicationVersion: APPROVED_VERSION,
    publicationFingerprint: APPROVED_FP,
  });
  const row = {
    id,
    title: text(pkgById.get(id)?.displayTitle || pkgById.get(id)?.title),
    validation: built,
  };
  validation27.push(row);
  if (built.ok) upsertCandidates.push(built);
  else quarantined.push(row);
}
const validationDoc = {
  schema: "gdi-27-record-validation-v1",
  hotelId: HOTEL,
  packageOnlyCount: packageOnlyIds.length,
  safeToUpsert: upsertCandidates.length,
  quarantined: quarantined.length,
  records: validation27,
  quarantinedRecords: quarantined,
};
const priorValPath = path.join(OUT, "03-27_RECORD_VALIDATION.json");
const priorVal = fs.existsSync(priorValPath)
  ? JSON.parse(fs.readFileSync(priorValPath, "utf8"))
  : null;
if (priorVal?.safeToUpsert === 27 && packageOnlyIds.length === 0) {
  writeJson(path.join(OUT, "03-27_RECORD_VALIDATION.latest-run.json"), validationDoc);
} else {
  writeJson(priorValPath, validationDoc);
}

// ─── 4. Disposition 19 ──────────────────────────────────────────────────────
const airtableOnlyIds = [...atIds].filter((id) => !pkgIds.has(id));
function dispositionFor(id, atRow, surface) {
  const title = text(atRow?.title || atRow?.displayTitle);
  const priority = text(atRow?.priority);
  const surf = surface?.surface || null;
  const reason = surface?.reason || null;
  if (surf === "ARCHIVE_DISQUALIFIED" || priority === "DISQUALIFIED") {
    return {
      disposition: "REJECTED_OR_ARCHIVED",
      preserveInAirtable: true,
      promoteToCustomerPublication: false,
      rationale: reason || "DISQUALIFIED_OR_ARCHIVE_SURFACE",
    };
  }
  if (surf === "RESEARCH_PIPELINE") {
    return {
      disposition: "RESEARCH_STAGE",
      preserveInAirtable: true,
      promoteToCustomerPublication: false,
      rationale: reason || "RESEARCH_PIPELINE",
    };
  }
  if (surf === "MARKET_INTELLIGENCE") {
    return {
      disposition: "MARKET_INTELLIGENCE_NOT_CUSTOMER_INVENTORY",
      preserveInAirtable: true,
      promoteToCustomerPublication: false,
      rationale: reason || "MARKET_INTELLIGENCE",
    };
  }
  if (/meeting id|passcode/i.test(title)) {
    return {
      disposition: "QUARANTINED_JUNK_TITLE",
      preserveInAirtable: true,
      promoteToCustomerPublication: false,
      rationale: "Malformed discovery title; keep for audit, never publish",
    };
  }
  if (/nci rna|ctn annual|ctn dissemination|series_request|series_new_in_the_library|series_nci/i.test(id + " " + title)) {
    return {
      disposition: "RESEARCH_STAGE_OR_SERIES_VARIANT",
      preserveInAirtable: true,
      promoteToCustomerPublication: false,
      rationale:
        "Series/variant or research-pipeline relative of commercial events; excluded from sales inventory 67",
    };
  }
  return {
    disposition: "LEGITIMATELY_EXCLUDED_FROM_PUBLICATION",
    preserveInAirtable: true,
    promoteToCustomerPublication: false,
    rationale:
      surface
        ? `commercial_surface=${surf}`
        : "Present in Airtable research registry but not selected into approved customer publication 67",
  };
}

const dispositions19 = airtableOnlyIds.sort().map((id) => {
  const atRow = atById.get(id);
  const surface = surfaceRows.get(id);
  return {
    id,
    title: text(atRow?.title || atRow?.displayTitle),
    airtablePriority: atRow?.priority || null,
    commercialSurface: surface?.surface || null,
    commercialSurfaceReason: surface?.reason || null,
    ...dispositionFor(id, atRow, surface),
  };
});
writeJson(path.join(OUT, "04-19_RECORD_DISPOSITIONS.json"), {
  schema: "gdi-19-airtable-only-dispositions-v1",
  hotelId: HOTEL,
  count: dispositions19.length,
  note: "No automatic promotion to customer-visible publication",
  records: dispositions19,
});

// ─── 5. Dry-run Airtable sync plan ──────────────────────────────────────────
const dryRunPlan = {
  schema: "gdi-airtable-dry-run-v1",
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL,
  baseId: getGdiOpportunitiesAirtableBaseId(),
  table: "Group Demand Opportunities",
  mode: "CREATE_ONLY_MISSING_IDS",
  willNotUpdateExistingAirtableRows: true,
  willNotDeleteAnyRows: true,
  willNotModifyPublication: true,
  candidates: [],
  skippedAlreadyPresent: [],
  quarantined: quarantined.map((q) => ({
    id: q.id,
    error: q.validation?.error,
    blockers: q.validation?.blockers || null,
  })),
};

for (const built of upsertCandidates) {
  const existing = await findOpportunityRecordById(built.id);
  if (existing) {
    dryRunPlan.skippedAlreadyPresent.push({
      id: built.id,
      airtableRecordId: existing.id,
      action: "SKIP_ALREADY_IN_AIRTABLE",
    });
    continue;
  }
  dryRunPlan.candidates.push({
    id: built.id,
    title: text(built.candidate.title || built.candidate.displayTitle),
    action: "CREATE",
    missingOriginalResearchBody: built.notes?.missingOriginalResearchBody || false,
    sourceCount: built.notes?.sourceCount || 0,
    hotelId: built.candidate.hotelId,
    priority: built.candidate.priority || null,
    opportunityType: built.candidate.opportunityType || null,
  });
}
const priorDryPath = path.join(OUT, "05-AIRTABLE_DRY_RUN.json");
const priorDry = fs.existsSync(priorDryPath)
  ? JSON.parse(fs.readFileSync(priorDryPath, "utf8"))
  : null;
if (
  priorDry?.candidates?.length === 27 &&
  dryRunPlan.candidates.length === 0 &&
  airtableBefore.length >= 86
) {
  writeJson(path.join(OUT, "05-AIRTABLE_DRY_RUN.latest-run.json"), dryRunPlan);
} else {
  writeJson(priorDryPath, dryRunPlan);
}

console.log(
  `[gdi-recon] dry-run create=${dryRunPlan.candidates.length} skipPresent=${dryRunPlan.skippedAlreadyPresent.length} quarantine=${dryRunPlan.quarantined.length}`,
);

// ─── 6. Apply (authorized) ──────────────────────────────────────────────────
const receipts = {
  schema: "gdi-migration-receipts-v1",
  startedAt: new Date().toISOString(),
  mode: APPLY ? "APPLY" : "DRY_RUN_NO_WRITES",
  creates: [],
  errors: [],
  fsMirrorAppended: [],
};

if (APPLY) {
  for (const built of upsertCandidates) {
    const id = built.id;
    try {
      const existing = await findOpportunityRecordById(id);
      if (existing) {
        receipts.creates.push({
          id,
          action: "SKIPPED_ALREADY_EXISTS",
          airtableRecordId: existing.id,
        });
        continue;
      }
      const result = await upsertOpportunity(built.candidate, {
        hotelId: HOTEL,
        runId: "gdi_research_storage_reconciliation_v1",
        researchVersion: "gdi-research-registry-historical-upsert-v1",
        hotelIdentityStatus: "CANONICAL",
        preserveSalesOwned: true,
      });
      if (result.created !== true) {
        // Safety: upsert found a race-create as update — record and continue
        receipts.creates.push({
          id,
          action: "UNEXPECTED_UPDATE_NOT_CREATE",
          airtableRecordId: result.airtableRecordId,
        });
      } else {
        receipts.creates.push({
          id,
          action: "CREATED",
          airtableRecordId: result.airtableRecordId,
          created: true,
        });
      }
    } catch (err) {
      receipts.errors.push({
        id,
        error: err?.message || String(err),
        code: err?.code || null,
      });
    }
  }

  // Append created (and skipped-existing) candidates into FS mirror without dropping 59
  clearGdiRepositoryReadCache();
  const liveFs = loadOpportunities(HOTEL);
  const liveIds = idsOf(liveFs.opportunities);
  const appended = [];
  for (const built of upsertCandidates) {
    if (liveIds.has(built.id)) continue;
    liveFs.opportunities.push(built.candidate);
    liveIds.add(built.id);
    appended.push(built.id);
  }
  if (appended.length) {
    liveFs.researchRegistryReconciliationV1 = {
      appendedAt: new Date().toISOString(),
      appendedIds: appended,
      note: "Historical CREATE-only registry recovery; does not alter customer publication",
    };
    saveOpportunities(HOTEL, liveFs);
  }
  receipts.fsMirrorAppended = appended;
}
receipts.finishedAt = new Date().toISOString();
// Dry-run must not clobber an APPLY receipt from a prior authorized migration.
if (APPLY) {
  writeJson(path.join(OUT, "06-MIGRATION_RECEIPTS.json"), receipts);
} else {
  writeJson(path.join(OUT, "06-MIGRATION_RECEIPTS.dry-run.json"), receipts);
  const priorApplyPath = path.join(OUT, "06-MIGRATION_RECEIPTS.json");
  if (!fs.existsSync(priorApplyPath)) {
    writeJson(priorApplyPath, {
      ...receipts,
      note: "Placeholder — run --apply to produce authoritative CREATE receipts",
    });
  }
}

// ─── 7. Post readback ───────────────────────────────────────────────────────
clearGdiRepositoryReadCache();
const airtableAfter = await listOpportunitiesForHotel(HOTEL);
const atAfterIds = idsOf(airtableAfter);
const fsAfter = loadOpportunities(HOTEL);
const fsAfterIds = idsOf(fsAfter.opportunities);
const coverage = assertPublishedIdsCoveredByResearchRegistry({
  publishedOpportunityIds: [...pkgIds],
  researchRegistryIds: [...atAfterIds],
});
const stillMissing = [...pkgIds].filter((id) => !atAfterIds.has(id));
const dupCheck = (() => {
  const seen = new Map();
  for (const o of airtableAfter) {
    const id = text(o.id);
    seen.set(id, (seen.get(id) || 0) + 1);
  }
  return [...seen.entries()].filter(([, n]) => n > 1).map(([id, n]) => ({ id, count: n }));
})();

const postReadback = {
  schema: "gdi-post-migration-readback-v1",
  generatedAt: new Date().toISOString(),
  mode: APPLY ? "APPLY" : "DRY_RUN",
  airtableCountBefore: airtableBefore.length,
  airtableCountAfter: airtableAfter.length,
  fsCountAfter: fsAfterIds.size,
  publishedCoveredByAirtable: coverage,
  stillMissingFromAirtable: stillMissing,
  airtableOnlyStillPresent: airtableOnlyIds.every((id) => atAfterIds.has(id)),
  airtableOnlyCount: airtableOnlyIds.filter((id) => atAfterIds.has(id)).length,
  duplicateOpportunityIds: dupCheck,
  publicationFingerprintUnchanged:
    JSON.parse(fs.readFileSync(bundledPkgPath, "utf8")).contentFingerprintSha256 ===
    APPROVED_FP,
  feedbackShaUnchanged:
    sha256File(feedbackPath).sha256 === preMigrationSnapshot.feedback.sha256,
  bundledPackageShaUnchanged:
    sha256File(bundledPkgPath).sha256 ===
    snapshotFiles.find((s) => s.name === "bundled-v2-package.json")?.sha256,
};
writeJson(path.join(OUT, "07-POST_MIGRATION_READBACK.json"), postReadback);

// ─── 8. Durable evidence integrity ──────────────────────────────────────────
const evidenceIntegrity = {
  schema: "gdi-durable-evidence-integrity-v1",
  contract: GDI_RESEARCH_PERSISTENCE_CONTRACT_V1,
  roles: GDI_RESEARCH_STORAGE_ROLES,
  reconstructedSha256: sha256File(reconPath).sha256,
  reconstructedPreserved:
    sha256File(reconPath).sha256 ===
    snapshotFiles.find((s) => s.name === "reconstructed-opportunities-v1.json")?.sha256,
  opportunityResearchStateCount: orsFiles.length,
  runCount: runIds.length,
  historicalUpsertsWithSources: upsertCandidates.filter(
    (c) => (c.notes?.sourceCount || 0) > 0,
  ).length,
  historicalUpsertsMissingOriginalBody: upsertCandidates.filter(
    (c) => c.notes?.missingOriginalResearchBody,
  ).length,
  note: "Full evidence remains on FS (reconstructed + research-state). Airtable holds compacted registry payload + provenance stamp.",
};
writeJson(path.join(OUT, "08-DURABLE_EVIDENCE_INTEGRITY.json"), evidenceIntegrity);

// ─── 9. CURRENT pointer audit + quarantine ──────────────────────────────────
const pubRootResolved = resolvePublicationPackageRoot(process.env);
const pointerAudit = {
  schema: "gdi-current-pointer-audit-v1",
  before: {
    bundledCurrent,
    dataCurrent,
    resolvePublicationPackageRoot: path.relative(ROOT, pubRootResolved).replace(/\\/g, "/"),
  },
  action: null,
  after: null,
  productionLoaderNote:
    "resolvePublicationPackageRoot prefers bundled when any hotel CURRENT exists under bundled-publication-packages; stale data/ CURRENT must not silently win.",
};

if (APPLY) {
  if (
    dataCurrent &&
    text(dataCurrent.contentFingerprintSha256).toLowerCase() !== APPROVED_FP
  ) {
    const quarantineName = `CURRENT.quarantined-stale-${String(
      dataCurrent.contentFingerprintSha256 || "unknown",
    ).slice(0, 12)}-${new Date().toISOString().slice(0, 10)}.json`;
    const quarantinePath = path.join(path.dirname(dataCurrentPath), quarantineName);
    fs.renameSync(dataCurrentPath, quarantinePath);
    // Also quarantine STAGED if it points at old recon package
    const dataStagedPath = path.join(path.dirname(dataCurrentPath), "STAGED.json");
    let stagedQuarantined = null;
    if (fs.existsSync(dataStagedPath)) {
      const staged = JSON.parse(fs.readFileSync(dataStagedPath, "utf8"));
      if (
        text(staged.contentFingerprintSha256).toLowerCase() !== APPROVED_FP &&
        text(staged.publicationVersion).includes("reconciliation_v1")
      ) {
        const stagedQ = `STAGED.quarantined-stale-${new Date().toISOString().slice(0, 10)}.json`;
        fs.renameSync(dataStagedPath, path.join(path.dirname(dataCurrentPath), stagedQ));
        stagedQuarantined = stagedQ;
      }
    }
    pointerAudit.action = {
      type: "QUARANTINE_STALE_DATA_CURRENT",
      quarantinedTo: path.relative(ROOT, quarantinePath).replace(/\\/g, "/"),
      stagedQuarantined,
      bundledCurrentUnchanged: true,
      approvedFingerprintPreserved: APPROVED_FP,
    };
  } else {
    pointerAudit.action = {
      type: "NO_QUARANTINE_NEEDED",
      reason: dataCurrent
        ? "data CURRENT already matches approved fingerprint or absent"
        : "data CURRENT missing",
    };
  }
} else {
  pointerAudit.action = {
    type: "DRY_RUN_WOULD_QUARANTINE",
    wouldQuarantine:
      Boolean(dataCurrent) &&
      text(dataCurrent.contentFingerprintSha256).toLowerCase() !== APPROVED_FP,
    staleFingerprint: dataCurrent?.contentFingerprintSha256 || null,
  };
}

const storeAfter = createGdiPublicationPackageStore({ env: process.env });
const loadedCustomer = storeAfter.loadCustomerVisiblePackage(HOTEL);
pointerAudit.after = {
  resolvePublicationPackageRoot: path
    .relative(ROOT, resolvePublicationPackageRoot(process.env))
    .replace(/\\/g, "/"),
  dataCurrentExists: fs.existsSync(dataCurrentPath),
  bundledCurrent,
  loadCustomerVisiblePackage: {
    ok: loadedCustomer.ok === true,
    version: loadedCustomer.package?.publicationVersion || null,
    fingerprint: loadedCustomer.package?.contentFingerprintSha256 || null,
    error: loadedCustomer.ok ? null : loadedCustomer.error,
  },
  cannotSilentlyServeSuperseded441773:
    loadedCustomer.ok === true &&
    text(loadedCustomer.package?.contentFingerprintSha256).toLowerCase() ===
      APPROVED_FP,
};
writeJson(path.join(OUT, "09-CURRENT_POINTER_AUDIT.json"), pointerAudit);

// ─── 10. Research refresh dry-run (non-destructive) ─────────────────────────
const refreshDryRun = {
  schema: "gdi-research-refresh-dry-run-v1",
  generatedAt: new Date().toISOString(),
  paidDiscovery: false,
  checks: {
    allPublishedIdsDiscoverableInRegistry: coverage.ok || (!APPLY && stillMissing.length === packageOnlyIds.length),
    publishedIdsMissingFromRegistry: stillMissing,
    noDuplicateIdsInAirtable: dupCheck.length === 0,
    airtableOnlyPreserved: postReadback.airtableOnlyStillPresent,
    reconstructedEvidenceRetrievable: evidenceIntegrity.reconstructedPreserved,
    feedbackIntact: postReadback.feedbackShaUnchanged,
    publicationUnchanged: postReadback.publicationFingerprintUnchanged,
    fsMirrorContainsPublishedOrPending:
      [...pkgIds].filter((id) => fsAfterIds.has(id) || (!APPLY && packageOnlyIds.includes(id)))
        .length === pkgIds.size ||
      (APPLY && [...pkgIds].every((id) => fsAfterIds.has(id) || atAfterIds.has(id))),
  },
  note: "Dry-run only — no paid providers, no research execution writes beyond authorized registry CREATE when --apply",
};
refreshDryRun.ok = Object.entries(refreshDryRun.checks)
  .filter(([k]) => k !== "publishedIdsMissingFromRegistry")
  .every(([, v]) => v === true || typeof v === "number");
// tighter ok:
refreshDryRun.ok =
  refreshDryRun.checks.noDuplicateIdsInAirtable &&
  refreshDryRun.checks.airtableOnlyPreserved &&
  refreshDryRun.checks.reconstructedEvidenceRetrievable &&
  refreshDryRun.checks.feedbackIntact &&
  refreshDryRun.checks.publicationUnchanged &&
  (APPLY ? refreshDryRun.checks.allPublishedIdsDiscoverableInRegistry : true);
writeJson(path.join(OUT, "10-RESEARCH_REFRESH_DRY_RUN.json"), refreshDryRun);

// ─── 11. Cross-hotel contract ───────────────────────────────────────────────
const templateGate = evaluateFullE2eHotelTemplateResearchStorageGate({
  publishedOpportunityIds: [...pkgIds],
  researchRegistryIds: [...atAfterIds],
  requiredPublicationFingerprint: APPROVED_FP,
  actualPublicationFingerprint: pkg.contentFingerprintSha256,
  publicationMutatedByResearchSync: !postReadback.publicationFingerprintUnchanged,
});
writeJson(path.join(OUT, "11-CROSS_HOTEL_STORAGE_CONTRACT.json"), {
  schema: "gdi-cross-hotel-storage-contract-v1",
  contract: GDI_RESEARCH_PERSISTENCE_CONTRACT_V1,
  roles: GDI_RESEARCH_STORAGE_ROLES,
  templateGate,
  rules: [
    "Airtable is canonical mutable research registry per hotel",
    "FS retains full evidence, runs, historical recovery bags",
    "Publication packages are immutable FOUNDER_APPROVED customer content",
    "Feedback is separate and merge-at-read",
    "Research refresh must not drop registry IDs that are customer-published",
    "Research-stage / archive rows need not be published",
    "Pre-publish gate: publishedIds ⊆ researchRegistryIds",
    "Bundled CURRENT preferred over data/ CURRENT to avoid stale Railway lean fallback",
  ],
});

console.log(
  JSON.stringify(
    {
      mode: APPLY ? "APPLY" : "DRY_RUN",
      createPlan: dryRunPlan.candidates.length,
      created: receipts.creates.filter((c) => c.action === "CREATED").length,
      errors: receipts.errors.length,
      airtableAfter: airtableAfter.length,
      coverageOk: coverage.ok,
      stillMissing: stillMissing.length,
      publicationOk: postReadback.publicationFingerprintUnchanged,
    },
    null,
    2,
  ),
);

if (!APPLY) {
  console.log(
    "\n[gdi-recon] DRY-RUN complete. Re-run with --apply to CREATE missing Airtable rows + quarantine stale data/ CURRENT.",
  );
}
