/**
 * Reconcile Factory 2.0 master queue across:
 * - Active/Live Airtable queue (if present)
 * - Every founder-authorized research batch
 * - All staged brand folders (Draft/Under Review depth work)
 *
 * Never discards pending brands because a cohort run only covered six records.
 * Preserves statuses, candidate hashes, batch tags, and audit history.
 */
import {
  BE_FACTORY_2_0_BATCH_V1,
  FACTORY_2_STATUSES,
  PROTECTED_BRAND_IDS,
  AUTOGRAPH_BRAND_ID,
  DEFAULT_PAID_RESEARCH_BUDGET_USD,
  normalizeFactory2Status,
  PERSISTENCE_STATES,
} from "./be-factory-2-contracts-v1.js";
import {
  AUTHORIZED_FACTORY_2_BATCHES,
  listAuthorizedBrandEntries,
  authorizedBrandCount,
  authorizedBatchCount,
} from "./be-factory-2-authorized-batches-v1.js";
import {
  loadQueue,
  saveQueue,
  loadBrandStage,
  listStagedBrandIds,
  buildPortfolioDashboard,
  writeJson,
  reportsRoot,
  stagingRoot,
  ensureDirs,
} from "./be-factory-2-staging-store-v1.js";
import { join } from "path";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function inferPersistenceAxes(stage, queueRow) {
  const status = normalizeFactory2Status(stage?.factoryStatus || queueRow?.status);
  const hasCandidate = Boolean(stage?.candidate || stage?.candidateHash);
  const approved =
    status === FACTORY_2_STATUSES.APPROVED_FOR_WRITEBACK ||
    status === FACTORY_2_STATUSES.PERSISTED_VERIFIED ||
    status === FACTORY_2_STATUSES.PUBLISHED ||
    stage?.approvalStatus === "APPROVED";
  const writebackAuthorized = Boolean(stage?.writebackAuthorized || queueRow?.writebackAuthorized);
  const published = status === FACTORY_2_STATUSES.PUBLISHED;

  let structuredResearch = PERSISTENCE_STATES.NOT_STARTED;
  if (hasCandidate) structuredResearch = PERSISTENCE_STATES.STRUCTURED_STAGED;
  if (status === FACTORY_2_STATUSES.PERSISTED_VERIFIED || published) {
    structuredResearch = PERSISTENCE_STATES.STRUCTURED_PERSISTED;
  }

  let presentationWriteback = PERSISTENCE_STATES.WRITEBACK_PENDING_APPROVAL;
  if (PROTECTED_BRAND_IDS.includes(queueRow?.brandId || stage?.brandId)) {
    presentationWriteback = PERSISTENCE_STATES.PRESENTATION_PERSISTED_VERIFIED;
  } else if (writebackAuthorized || approved) {
    presentationWriteback = PERSISTENCE_STATES.WRITEBACK_AUTHORIZED;
  }
  if (status === FACTORY_2_STATUSES.PERSISTED_VERIFIED) {
    presentationWriteback = PERSISTENCE_STATES.PRESENTATION_PERSISTED_VERIFIED;
  }

  let customerPublication = published
    ? PERSISTENCE_STATES.CUSTOMER_PUBLISHED
    : "NOT_PUBLISHED";

  return { structuredResearch, presentationWriteback, customerPublication };
}

/**
 * Upsert a brand into the master queue map without wiping prior fields.
 */
function upsertBrand(byId, incoming) {
  const id = incoming.brandId;
  if (!id) return;
  const prev = byId.get(id) || {};
  const status = normalizeFactory2Status(
    incoming.status || prev.status || FACTORY_2_STATUSES.QUEUED
  );
  const batchIds = [
    ...new Set([
      ...(prev.batchIds || []),
      ...(incoming.batchIds || []),
      ...(incoming.batchId ? [incoming.batchId] : []),
      ...(prev.batchId ? [prev.batchId] : []),
    ].filter(Boolean)),
  ];
  byId.set(id, {
    ...prev,
    ...incoming,
    brandId: id,
    brandName: nz(incoming.brandName) || nz(prev.brandName) || id,
    parentCompany: nz(incoming.parentCompany) || nz(prev.parentCompany),
    brandStatus: nz(incoming.brandStatus) || nz(prev.brandStatus) || "",
    aliases: [...new Set([...(prev.aliases || []), ...(incoming.aliases || [])])],
    batchId: incoming.batchId || prev.batchId || batchIds[0] || null,
    batchIds,
    researchClass: incoming.researchClass || prev.researchClass || "EXISTING_ACTIVE",
    status,
    writebackAuthorized: Boolean(incoming.writebackAuthorized || prev.writebackAuthorized),
    estimatedResearchSpendUsd:
      incoming.estimatedResearchSpendUsd ??
      prev.estimatedResearchSpendUsd ??
      DEFAULT_PAID_RESEARCH_BUDGET_USD,
    note: incoming.note || prev.note || "",
    candidateHash: incoming.candidateHash || prev.candidateHash || null,
    assembledHash: incoming.assembledHash || prev.assembledHash || null,
  });
}

/**
 * Reconcile and persist master queue + portfolio dashboard.
 * @param {{ repoRoot: string }} opts
 */
export function reconcileMasterQueue(opts) {
  const { repoRoot } = opts;
  ensureDirs(repoRoot);

  const existing = loadQueue(repoRoot) || { brands: [] };
  /** @type {Map<string, object>} */
  const byId = new Map();

  // 1) Preserve every existing queue row
  for (const b of existing.brands || []) {
    upsertBrand(byId, {
      ...b,
      status: normalizeFactory2Status(b.status),
      source: "existing_queue",
    });
  }

  // 2) Register every authorized batch brand (including Draft/Under Review)
  for (const entry of listAuthorizedBrandEntries()) {
    const stage = loadBrandStage(repoRoot, entry.brandId);
    const prev = byId.get(entry.brandId);
    let status = normalizeFactory2Status(stage?.factoryStatus || prev?.status);
    if (!status || status === FACTORY_2_STATUSES.QUEUED) {
      status = stage?.factoryStatus
        ? normalizeFactory2Status(stage.factoryStatus)
        : prev?.status
          ? normalizeFactory2Status(prev.status)
          : FACTORY_2_STATUSES.QUEUED;
    }
    upsertBrand(byId, {
      brandId: entry.brandId,
      brandName: entry.brandName,
      parentCompany: entry.parentCompany,
      aliases: entry.aliases,
      batchId: entry.batchId,
      batchIds: entry.batchIds,
      researchClass: entry.researchClass,
      status,
      candidateHash: stage?.candidateHash || stage?.candidate?.presentationBlocksHashSha256 || null,
      assembledHash: stage?.assembledHash || null,
      note:
        prev?.note ||
        `Authorized ${entry.batchId} — ${entry.researchClass}; preserve staged work`,
      source: "authorized_batch",
    });
  }

  // 3) Protect Curio / Ascend
  for (const id of PROTECTED_BRAND_IDS) {
    const stage = loadBrandStage(repoRoot, id);
    upsertBrand(byId, {
      brandId: id,
      brandName: byId.get(id)?.brandName || id,
      status: FACTORY_2_STATUSES.SKIPPED_PROTECTED,
      researchClass: "EXISTING_ACTIVE",
      batchId: "PROTECTED_CURIO_ASCEND",
      note: "Curio/Ascend verified Presentation — excluded from reprocessing",
      candidateHash: stage?.candidateHash || null,
      source: "protected",
    });
  }

  function preferBatchId(prevId, stageTag) {
    const a = nz(prevId);
    const b = nz(stageTag);
    if (!a) return b || null;
    if (!b) return a;
    // Prefer canonical AUTHORIZED_* style (BATCH_3) over runner tags (batch-3)
    if (/^BATCH_\d+$/i.test(a) || /^FACTORY_|^NAMED_|^PROTECTED_/.test(a)) return a;
    if (/^BATCH_\d+$/i.test(b) || /^FACTORY_|^NAMED_|^PROTECTED_/.test(b)) return b;
    return a;
  }

  // 4) Any staged folder not yet in queue (orphan safety net)
  for (const id of listStagedBrandIds(repoRoot)) {
    if (byId.has(id)) {
      const stage = loadBrandStage(repoRoot, id);
      if (!stage) continue;
      const prev = byId.get(id);
      upsertBrand(byId, {
        ...prev,
        status: normalizeFactory2Status(stage.factoryStatus || prev.status),
        candidateHash:
          stage.candidateHash ||
          stage.candidate?.presentationBlocksHashSha256 ||
          prev.candidateHash,
        assembledHash: stage.assembledHash || prev.assembledHash,
        batchId: preferBatchId(prev.batchId, stage.batchTag),
      });
      continue;
    }
    const stage = loadBrandStage(repoRoot, id);
    upsertBrand(byId, {
      brandId: id,
      brandName: stage?.brandName || id,
      parentCompany: stage?.parentCompany || "",
      status: normalizeFactory2Status(stage?.factoryStatus || FACTORY_2_STATUSES.QUEUED),
      researchClass: "EXISTING_CANONICAL",
      batchId: stage?.batchTag || "STAGED_ORPHAN",
      candidateHash: stage?.candidateHash || null,
      assembledHash: stage?.assembledHash || null,
      note: "Staged brand folder reconciled into master queue",
      source: "staged_orphan",
    });
  }

  // 5) Attach persistence axes + priority ordering
  const brands = [...byId.values()].map((b) => {
    const stage = loadBrandStage(repoRoot, b.brandId);
    const axes = inferPersistenceAxes(stage, b);
    return {
      ...b,
      status: normalizeFactory2Status(b.status),
      persistence: axes,
      presentationCount:
        b.presentationCount ??
        stage?.researchCompletion?.liveBlocks ??
        0,
      estimatedEffort: b.estimatedEffort || "M",
      estimatedHours: b.estimatedHours || 4,
      writebackAuthorized: false, // never silent-authorize
    };
  });

  brands.sort((a, b) => {
    if (a.brandId === AUTOGRAPH_BRAND_ID) return -1;
    if (b.brandId === AUTOGRAPH_BRAND_ID) return 1;
    if (a.status === FACTORY_2_STATUSES.SKIPPED_PROTECTED) return 1;
    if (b.status === FACTORY_2_STATUSES.SKIPPED_PROTECTED) return -1;
    // Depth-researched remediation / certified first, then authorized batches, then remaining QUEUED
    const rank = (s) => {
      const n = normalizeFactory2Status(s);
      if (n === FACTORY_2_STATUSES.CERTIFIED_READY_FOR_REVIEW) return 0;
      if (n === FACTORY_2_STATUSES.APPROVED_FOR_WRITEBACK) return 1;
      if (n === FACTORY_2_STATUSES.PERSISTED_VERIFIED) return 2;
      if (n === FACTORY_2_STATUSES.NEEDS_REMEDIATION) return 3;
      if (n === FACTORY_2_STATUSES.RESEARCHING) return 4;
      if (n === FACTORY_2_STATUSES.QUEUED) return 5;
      if (n === FACTORY_2_STATUSES.BLOCKED) return 6;
      return 7;
    };
    const rd = rank(a.status) - rank(b.status);
    if (rd !== 0) return rd;
    // Prefer authorized-batch brands over unscoped Active/Live inventory
    const aAuth = a.batchId || (a.batchIds && a.batchIds.length) ? 0 : 1;
    const bAuth = b.batchId || (b.batchIds && b.batchIds.length) ? 0 : 1;
    if (aAuth !== bAuth) return aAuth - bAuth;
    const ba = String(a.batchId || "");
    const bb = String(b.batchId || "");
    if (ba !== bb) return ba.localeCompare(bb);
    return String(a.brandName).localeCompare(String(b.brandName));
  });

  brands.forEach((b, i) => {
    b.priority = i + 1;
  });

  const queueDoc = {
    contract: BE_FACTORY_2_0_BATCH_V1,
    kind: "MASTER_QUEUE",
    generatedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    reconciliation: {
      authorizedBatches: authorizedBatchCount(),
      authorizedBrandSlots: authorizedBrandCount(),
      priorQueueCount: (existing.brands || []).length,
      stagedFolderCount: listStagedBrandIds(repoRoot).length,
      masterQueueCount: brands.length,
      note:
        "Master queue merges Active/Live inventory + all authorized batches + staged folders. Draft/Under Review depth brands are first-class queue members.",
    },
    universe: {
      ...(existing.universe || {}),
      masterQueueCount: brands.length,
      authorizedBatchCount: authorizedBatchCount(),
      authorizedBrandCount: authorizedBrandCount(),
      protectedExcluded: [...PROTECTED_BRAND_IDS],
    },
    authorizedBatches: AUTHORIZED_FACTORY_2_BATCHES.map((b) => ({
      batchId: b.batchId,
      label: b.label,
      researchClass: b.researchClass,
      brandCount: b.brands.length,
      brandIds: b.brands.map((x) => x.brandId),
    })),
    brands,
  };

  saveQueue(repoRoot, queueDoc);
  const dashboard = buildPortfolioDashboard(repoRoot);

  const audit = {
    contract: "BE_FACTORY_2_0_MASTER_QUEUE_RECONCILE_V1",
    generatedAt: new Date().toISOString(),
    reconciliation: queueDoc.reconciliation,
    statusCounts: dashboard.summary,
    authorizedBatches: queueDoc.authorizedBatches,
    nextTen: (dashboard.nextTenScheduled || []).map((r) => ({
      brandId: r.brandId,
      brandName: r.brandName,
      status: r.factoryStatus,
      batchId: r.batchId,
    })),
  };
  writeJson(join(stagingRoot(repoRoot), "master-queue-reconcile.json"), audit);
  writeJson(join(reportsRoot(repoRoot), "70-master-queue-reconcile.json"), audit);

  return { queue: queueDoc, dashboard, audit };
}

/**
 * Upsert one depth-batch result into the master queue (create row if missing).
 */
export function upsertQueueBrandFromDepthResult(repoRoot, result, pack = {}) {
  const queue = loadQueue(repoRoot) || {
    contract: BE_FACTORY_2_0_BATCH_V1,
    brands: [],
  };
  if (!Array.isArray(queue.brands)) queue.brands = [];
  let row = queue.brands.find((b) => b.brandId === result.brandId);
  if (!row) {
    row = {
      brandId: result.brandId,
      brandName: result.brandName || pack.brandName || result.brandId,
      parentCompany: pack.parentCompany || "",
      priority: queue.brands.length + 1,
      status: normalizeFactory2Status(result.factoryStatus),
      researchClass: pack.researchClass || "EXISTING_CANONICAL",
      batchId: pack.batchTag || result.batchTag || null,
      batchIds: [pack.batchTag || result.batchTag].filter(Boolean),
      writebackAuthorized: false,
      estimatedResearchSpendUsd: 0,
      note: "Upserted from depth batch — was missing from Active/Live-only queue",
    };
    queue.brands.push(row);
  } else {
    row.status = normalizeFactory2Status(result.factoryStatus);
    if (pack.batchTag) {
      row.batchId = pack.batchTag;
      row.batchIds = [...new Set([...(row.batchIds || []), pack.batchTag])];
    }
    if (result.candidateHash) row.candidateHash = result.candidateHash;
  }
  queue.updatedAt = new Date().toISOString();
  saveQueue(repoRoot, queue);
  return row;
}
