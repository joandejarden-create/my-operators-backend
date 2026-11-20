/**
 * Brand Explorer — Founder Approval → Airtable Publication workflow V1.
 *
 * Default mode: DRY_RUN / isolated test. Production Airtable writes and customer
 * promotion remain disabled until:
 *   1. configs/brand-explorer-founder-publication-activation-v1.json sets
 *      productionWritesAuthorized=true (separate founder auth), AND
 *   2. BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES=1 is set in the process env, AND
 *   3. Gate 1 + Gate 2 + founder brand approval + mandatory content gates pass.
 *
 * Contract: BE_FOUNDER_APPROVAL_PUBLICATION_V1
 */
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  BE_PRESENTATION_TABLE,
  BE_BRAND_BASICS_TABLE,
  BE_CANONICAL_TAB_IDS,
  BE_WRITE_CLASSES,
} from "./be-airtable-integration-sot-v1.js";
import {
  BE_LOCAL_PRODUCT_PARITY_GATE,
  evaluateLocalProductParityGate,
  persistLocalProductParityEvidence,
} from "./be-local-product-parity-gate-v1.js";

export const BE_FOUNDER_APPROVAL_PUBLICATION_V1 = "BE_FOUNDER_APPROVAL_PUBLICATION_V1";

export const PUBLICATION_STEPS = Object.freeze([
  "LOCK_CANDIDATE",
  "VALIDATE_GATES",
  "LOCAL_PRODUCT_PARITY",
  "BUILD_WRITE_PLAN",
  "VERIFY_IDENTITY_CONCURRENCY",
  "CREATE_SNAPSHOT",
  "APPLY_WRITES",
  "STAGED_BOUNDARY",
  "READBACK_VERIFY",
  "PROMOTE_CUSTOMER",
  "POST_PUBLISH_VERIFY",
  "RECORD_AUDIT",
  "COMPLETE_OR_ROLLBACK",
]);

export const PUBLICATION_MODES = Object.freeze({
  DRY_RUN: "DRY_RUN",
  ISOLATED_TEST: "ISOLATED_TEST",
  PRODUCTION: "PRODUCTION",
});

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..", "..");

export function activationConfigPath(root = REPO_ROOT) {
  return join(root, "configs", "brand-explorer-founder-publication-activation-v1.json");
}

export function loadActivationConfig(root = REPO_ROOT) {
  const path = activationConfigPath(root);
  if (!existsSync(path)) {
    return {
      contract: "BE_FOUNDER_PUBLICATION_ACTIVATION_V1",
      productionWritesAuthorized: false,
      automaticPublicationEnabled: false,
    };
  }
  return JSON.parse(readFileSync(path, "utf8"));
}

export function productionWritesEnabled(opts = {}) {
  const root = opts.root || REPO_ROOT;
  const activation = opts.activation || loadActivationConfig(root);
  const envFlag = String(process.env.BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES || "") === "1";
  return Boolean(activation.productionWritesAuthorized && envFlag);
}

export function hashPresentationBlocks(blocks) {
  const normalized = (Array.isArray(blocks) ? blocks : [])
    .map((b) => ({
      slotKey: String(b.slotKey || "").trim(),
      title: String(b.title || "").trim(),
      body: String(b.body || "").trim(),
      sort: typeof b.sort === "number" ? b.sort : Number(b.sort) || 0,
      imageUrl: b.imageUrl || null,
      active: b.active === false ? false : true,
    }))
    .filter((b) => b.slotKey)
    .sort((a, b) => a.slotKey.localeCompare(b.slotKey) || a.sort - b.sort);
  return createHash("sha256").update(JSON.stringify(normalized)).digest("hex");
}

export function lockCandidateVersion(input) {
  const brandId = String(input.brandId || "").trim();
  const candidateVersion = String(input.candidateVersion || "").trim();
  const blocks = input.proposedBlocks || [];
  const contentHash = hashPresentationBlocks(blocks);
  const lockId = createHash("sha256")
    .update([brandId, candidateVersion, contentHash, input.lockedAt || ""].join("|"))
    .digest("hex")
    .slice(0, 24);
  return {
    step: "LOCK_CANDIDATE",
    ok: Boolean(brandId && candidateVersion && blocks.length),
    lockId: `be-pub-lock-${lockId}`,
    brandId,
    candidateVersion,
    contentHash,
    blockCount: blocks.length,
    lockedAt: input.lockedAt || new Date().toISOString(),
    immutable: true,
  };
}

/**
 * Fail-closed gate validation. Curio/Ascend remain NOT APPROVED until founder says otherwise.
 */
export function validatePublicationGates(input) {
  const failures = [];
  const warnings = [];
  const gate1 = input.gate1 || {};
  const gate2 = input.gate2 || {};
  const brandDecision = input.brandDecision || {};
  const scorecard = input.scorecard || {};
  const activation = input.activation || loadActivationConfig(input.root);

  if (!gate1.founderApproved) {
    failures.push({
      code: "GATE1_NOT_APPROVED",
      message: "Gate 1 (Renderer Parity) is not founder-approved.",
    });
  }
  if (gate2.status === "BLOCKED" || !gate2.founderApproved) {
    failures.push({
      code: "GATE2_BLOCKED",
      message: "Gate 2 (content reconstruction) is blocked or not founder-approved.",
    });
  }
  const decision = String(brandDecision.decision || "").toLowerCase();
  if (!["approve_proposed", "approved", "approve_and_publish"].includes(decision)) {
    failures.push({
      code: "FOUNDER_BRAND_NOT_APPROVED",
      message: "Brand-level founder approval is required before publication.",
      observed: brandDecision.decision || null,
    });
  }
  if (scorecard.founderApprovalStatus && scorecard.founderApprovalStatus !== "FOUNDER_APPROVED") {
    failures.push({
      code: "SCORECARD_NOT_FOUNDER_APPROVED",
      message: "Candidate scorecard is not FOUNDER_APPROVED.",
      observed: scorecard.founderApprovalStatus,
    });
  }
  if (Number(scorecard.priorityBlocked || 0) > 0) {
    failures.push({
      code: "PRIORITY_GATES_BLOCKED",
      message: "Substantive priority gates remain blocked.",
      observed: scorecard.priorityBlocked,
    });
  }
  if (Number(scorecard.researchGapCount || 0) > 0) {
    warnings.push({
      code: "RESEARCH_GAPS_REMAIN",
      message: "Research gaps remain on the candidate.",
      observed: scorecard.researchGapCount,
    });
  }
  if (input.requireZeroOutstandingRequirements && Number(input.outstandingRequirementCount || 0) > 0) {
    failures.push({
      code: "OUTSTANDING_REQUIREMENTS",
      message: "Mandatory founder requirements remain outstanding.",
      observed: input.outstandingRequirementCount,
    });
  }
  if (!activation.productionWritesAuthorized && input.mode === PUBLICATION_MODES.PRODUCTION) {
    failures.push({
      code: "PRODUCTION_WRITES_NOT_AUTHORIZED",
      message: "Production publishing capability is not founder-authorized.",
    });
  }
  if (Array.isArray(input.forbiddenBrands) && input.forbiddenBrands.includes(input.brandId)) {
    // Soft note only when gates already fail; still list for clarity
    warnings.push({
      code: "BENCHMARK_BRAND_NOT_APPROVED",
      message: "Curio/Ascend remain NOT APPROVED for publication.",
    });
  }

  // BE_LOCAL_PRODUCT_PARITY_GATE — required before writeback/promotion (fail closed).
  // Callers may pass a fresh PASS report via localProductParity / reportPath.
  if (input.skipLocalProductParityGate === true) {
    warnings.push({
      code: "LOCAL_PRODUCT_PARITY_SKIPPED_UNSAFE",
      message:
        "skipLocalProductParityGate is set — publication orchestrator still refuses production writes without an explicit parity PASS step.",
    });
  } else {
    const parityEval = evaluateLocalProductParityGate({
      brandId: input.brandId,
      expectedCandidateHash: input.expectedCandidateHash || input.candidateHash || null,
      expectedApplicationCommit: input.expectedApplicationCommit || input.applicationCommit || null,
      report: input.localProductParity || input.localProductParityReport || null,
      reportPath: input.localProductParityReportPath || null,
      maxAgeMs: input.localProductParityMaxAgeMs,
      nowMs: input.nowMs,
      approvedExceptions: input.localProductParityApprovedExceptions,
    });
    if (!parityEval.ok) {
      for (const f of parityEval.failures) {
        failures.push({
          code: f.code === "LOCAL_PRODUCT_PARITY_REQUIRED" ? "LOCAL_PRODUCT_PARITY_REQUIRED" : f.code,
          message: f.message || "BE_LOCAL_PRODUCT_PARITY_GATE blocked publication.",
          gate: BE_LOCAL_PRODUCT_PARITY_GATE,
          detail: f,
        });
      }
    }
  }

  return {
    step: "VALIDATE_GATES",
    ok: failures.length === 0,
    failures,
    warnings,
    gate1Status: gate1.founderApproved ? "PASS" : "FAIL",
    gate2Status: gate2.founderApproved ? "PASS" : "FAIL",
    localProductParityRequired: true,
    founderBrandDecision: brandDecision.decision || null,
  };
}

function patchFieldsFromBlock(block) {
  const fields = {};
  if (block.title != null) fields.Title = String(block.title);
  if (block.body != null) fields.Body = String(block.body);
  if (block.sort != null && Number.isFinite(Number(block.sort))) fields["Sort Order"] = Number(block.sort);
  if (block.active === false) fields.Active = false;
  else if (block.active === true) fields.Active = true;
  // Images are not auto-uploaded in dry-run; plan records intended URL only
  if (block.imageUrl) {
    fields.__intendedImageUrl = String(block.imageUrl);
  }
  return fields;
}

/**
 * Build an exact Presentation-table write plan. Never includes Brand Basics child
 * intelligence tables, HPC, ADP, GDI, or unrelated brands.
 */
export function buildAirtableWritePlan(input) {
  const brandId = String(input.brandId || "").trim();
  const currentBlocks = Array.isArray(input.currentBlocks) ? input.currentBlocks : [];
  const proposedBlocks = Array.isArray(input.proposedBlocks) ? input.proposedBlocks : [];
  const approvedSlotKeys = new Set(
    (input.approvedSlotKeys || []).map((k) => String(k).trim()).filter(Boolean)
  );
  const approveAllChanged = Boolean(input.approveAllChanged);

  const currentBySlot = new Map();
  for (const b of currentBlocks) {
    const k = String(b.slotKey || "").trim();
    if (!k) continue;
    currentBySlot.set(k, b);
  }
  const proposedBySlot = new Map();
  for (const b of proposedBlocks) {
    const k = String(b.slotKey || "").trim();
    if (!k) continue;
    proposedBySlot.set(k, b);
  }

  const ops = [];
  const allSlots = new Set([...currentBySlot.keys(), ...proposedBySlot.keys()]);
  for (const slotKey of [...allSlots].sort()) {
    const cur = currentBySlot.get(slotKey) || null;
    const prop = proposedBySlot.get(slotKey) || null;
    const before = cur
      ? { title: cur.title || "", body: cur.body || "", imageUrl: cur.imageUrl || null, sort: cur.sort ?? null }
      : null;
    const after = prop
      ? { title: prop.title || "", body: prop.body || "", imageUrl: prop.imageUrl || null, sort: prop.sort ?? null }
      : null;
    const changed =
      JSON.stringify(before) !== JSON.stringify(after) || Boolean(cur) !== Boolean(prop);
    if (!changed) continue;
    if (!approveAllChanged && approvedSlotKeys.size && !approvedSlotKeys.has(slotKey)) {
      ops.push({
        op: "SKIP_NOT_APPROVED",
        table: BE_PRESENTATION_TABLE,
        slotKey,
        recordId: cur?.recordId || null,
        reason: "Slot changed but not in founder-approved set",
      });
      continue;
    }
    if (prop && cur?.recordId) {
      ops.push({
        op: "UPDATE",
        table: BE_PRESENTATION_TABLE,
        recordId: cur.recordId,
        brandBasicsId: brandId,
        slotKey,
        before,
        after,
        fields: patchFieldsFromBlock(prop),
        writeClass: BE_WRITE_CLASSES.FOUNDER_PUBLISH,
      });
    } else if (prop && !cur) {
      ops.push({
        op: "CREATE",
        table: BE_PRESENTATION_TABLE,
        recordId: null,
        brandBasicsId: brandId,
        slotKey,
        before: null,
        after,
        fields: {
          "Slot Key": slotKey,
          Brand: [brandId],
          ...patchFieldsFromBlock(prop),
          Active: true,
        },
        writeClass: BE_WRITE_CLASSES.FOUNDER_PUBLISH,
      });
    } else if (!prop && cur?.recordId) {
      ops.push({
        op: "DEACTIVATE",
        table: BE_PRESENTATION_TABLE,
        recordId: cur.recordId,
        brandBasicsId: brandId,
        slotKey,
        before,
        after: null,
        fields: { Active: false },
        writeClass: BE_WRITE_CLASSES.FOUNDER_PUBLISH,
        note: "Deactivate rather than delete — recoverable",
      });
    }
  }

  const childRecordChanges = ops.filter((o) => o.op === "CREATE" || o.op === "DEACTIVATE");
  return {
    step: "BUILD_WRITE_PLAN",
    ok: true,
    targetBaseEnv: "AIRTABLE_BASE_ID",
    targetTable: BE_PRESENTATION_TABLE,
    brandBasicsTable: BE_BRAND_BASICS_TABLE,
    brandBasicsId: brandId,
    excludedTables: [
      "Hotel Property Census",
      "Hotel Census",
      "Brand Setup - Fee Structure",
      "Brand Setup - Deal Terms",
      "Brand Setup - Legal Terms",
      "any ADP / GDI / HE tables",
    ],
    operations: ops,
    counts: {
      update: ops.filter((o) => o.op === "UPDATE").length,
      create: ops.filter((o) => o.op === "CREATE").length,
      deactivate: ops.filter((o) => o.op === "DEACTIVATE").length,
      skippedNotApproved: ops.filter((o) => o.op === "SKIP_NOT_APPROVED").length,
    },
    childRecordChanges,
    idempotencyKey: createHash("sha256")
      .update(JSON.stringify({ brandId, ops: ops.map((o) => [o.op, o.slotKey, o.recordId]) }))
      .digest("hex"),
  };
}

export function verifyIdentityAndConcurrency(input) {
  const brandId = String(input.brandId || "").trim();
  const expectedHash = String(input.baselineContentHash || "").trim();
  const liveHash = String(input.liveContentHash || "").trim();
  const failures = [];
  if (!/^rec[a-zA-Z0-9]{14}$/.test(brandId)) {
    failures.push({
      code: "INVALID_BRAND_BASICS_ID",
      message: "Brand Basics record id failed identity check.",
      observed: brandId,
    });
  }
  if (expectedHash && liveHash && expectedHash !== liveHash) {
    failures.push({
      code: "CONCURRENT_MODIFICATION",
      message: "Live Presentation hash differs from baseline snapshot — refuse publish.",
      expectedHash,
      liveHash,
    });
  }
  return {
    step: "VERIFY_IDENTITY_CONCURRENCY",
    ok: failures.length === 0,
    failures,
    brandBasicsId: brandId,
    baselineContentHash: expectedHash || null,
    liveContentHash: liveHash || null,
  };
}

export function createRecoverableSnapshot(input) {
  const root = input.root || REPO_ROOT;
  const brandId = String(input.brandId || "").trim();
  const dir = join(root, "reports", "brand-explorer", "comparison-studio", "publication-snapshots");
  mkdirSync(dir, { recursive: true });
  const stamp = (input.lockedAt || new Date().toISOString()).replace(/[:.]/g, "-");
  const snapshot = {
    contract: "BE_PUBLICATION_SNAPSHOT_V1",
    brandId,
    candidateVersion: input.candidateVersion || null,
    contentHash: input.contentHash || null,
    createdAt: new Date().toISOString(),
    table: BE_PRESENTATION_TABLE,
    blocks: input.currentBlocks || [],
    writePlanIdempotencyKey: input.writePlanIdempotencyKey || null,
  };
  const path = join(dir, `${brandId}-${stamp}.json`);
  if (input.persist !== false) {
    writeFileSync(path, JSON.stringify(snapshot, null, 2));
  }
  return {
    step: "CREATE_SNAPSHOT",
    ok: true,
    path: input.persist === false ? null : path,
    relativePath: input.persist === false
      ? null
      : `reports/brand-explorer/comparison-studio/publication-snapshots/${brandId}-${stamp}.json`,
    blockCount: (input.currentBlocks || []).length,
    recoverable: true,
  };
}

export function applyWritesGated(input) {
  const mode = input.mode || PUBLICATION_MODES.DRY_RUN;
  const writesEnabled = productionWritesEnabled({
    root: input.root,
    activation: input.activation,
  });
  const gatesOk = Boolean(input.gatesOk);
  const concurrencyOk = Boolean(input.concurrencyOk);
  // Fail closed: missing/false localParityOk must block production applies.
  const parityAllowsWrite = input.localParityOk === true;

  if (
    mode !== PUBLICATION_MODES.PRODUCTION ||
    !writesEnabled ||
    !gatesOk ||
    !concurrencyOk ||
    !parityAllowsWrite
  ) {
    return {
      step: "APPLY_WRITES",
      ok: true,
      applied: false,
      airtableWrites: 0,
      mode,
      reason: !writesEnabled
        ? "Production writes disabled (activation + env gate)"
        : !gatesOk
          ? "Gate validation failed — writes refused"
          : !concurrencyOk
            ? "Concurrency check failed — writes refused"
            : !parityAllowsWrite
              ? `${BE_LOCAL_PRODUCT_PARITY_GATE} PASS required before production writeback`
            : `Mode ${mode} does not apply production writes`,
      localParityOk: parityAllowsWrite,
      wouldApply: (input.writePlan?.operations || []).filter((o) =>
        ["UPDATE", "CREATE", "DEACTIVATE"].includes(o.op)
      ).length,
    };
  }

  // Production path is intentionally not implemented until founder activates it.
  return {
    step: "APPLY_WRITES",
    ok: false,
    applied: false,
    airtableWrites: 0,
    mode,
    reason:
      "PRODUCTION path skeleton only — live Airtable writer not enabled in this increment. Refuse rather than invent writes.",
  };
}

export function stagedPublicationBoundary(input) {
  return {
    step: "STAGED_BOUNDARY",
    ok: true,
    publicExposure: "BLOCKED",
    customerFacingPromotion: false,
    note:
      "Profiles remain internal until post-write verification and explicit promote step succeed.",
    stagingFlag: "bePublicationStaged=1",
    applied: Boolean(input.applied),
  };
}

export function readbackVerifyPlan(input) {
  const ops = (input.writePlan?.operations || []).filter((o) =>
    ["UPDATE", "CREATE", "DEACTIVATE"].includes(o.op)
  );
  return {
    step: "READBACK_VERIFY",
    ok: true,
    simulated: input.mode !== PUBLICATION_MODES.PRODUCTION || !input.applied,
    plannedChecks: ops.map((o) => ({
      recordId: o.recordId,
      slotKey: o.slotKey,
      expected: o.after,
      fields: Object.keys(o.fields || {}).filter((k) => !k.startsWith("__")),
    })),
    note: input.applied
      ? "Would read back each patched Presentation row and compare Title/Body/Active/Sort Order"
      : "Dry-run: readback checks planned but not executed against Airtable",
  };
}

export function promoteCustomerFacing(input) {
  const writesEnabled = productionWritesEnabled({
    root: input.root,
    activation: input.activation,
  });
  if (!input.applied || !input.readbackOk || !writesEnabled) {
    return {
      step: "PROMOTE_CUSTOMER",
      ok: true,
      promoted: false,
      reason: "Promotion blocked until successful apply + readback + production authorization",
    };
  }
  return {
    step: "PROMOTE_CUSTOMER",
    ok: false,
    promoted: false,
    reason: "Customer promotion writer not enabled in this increment",
  };
}

export function postPublishVerifyPlan(input) {
  return {
    step: "POST_PUBLISH_VERIFY",
    ok: true,
    simulated: !input.promoted,
    requiredTabs: [...BE_CANONICAL_TAB_IDS],
    checks: [
      "ten_tab_navigation",
      "product_components_present",
      "images_resolve",
      "critical_sections_non_empty",
      "customer_url_renders",
    ],
    note: input.promoted
      ? "Would verify published profile in customer Brand Explorer"
      : "Dry-run: post-publish verification plan recorded only",
  };
}

export function buildRollbackPlan(input) {
  return {
    step: "COMPLETE_OR_ROLLBACK",
    ok: true,
    rollbackAvailable: Boolean(input.snapshotPath || input.snapshotRelativePath),
    snapshotPath: input.snapshotPath || input.snapshotRelativePath || null,
    strategy: [
      "Restore Presentation rows from recoverable snapshot (Title/Body/Active/Sort Order)",
      "Keep Brand Basics and unrelated tables untouched",
      "Leave customer promotion flag off if verification failed",
      "Record rollback audit entry with original lockId + contentHash",
    ],
    publicationStatus: input.applied ? "APPLIED_PENDING_VERIFY" : "NOT_APPLIED",
  };
}

/**
 * Run the full workflow. Default DRY_RUN — never writes Airtable.
 */
export function runFounderApprovalPublication(input = {}) {
  const mode = input.mode || PUBLICATION_MODES.DRY_RUN;
  const root = input.root || REPO_ROOT;
  const activation = input.activation || loadActivationConfig(root);
  const startedAt = new Date().toISOString();
  const steps = [];

  const lock = lockCandidateVersion({
    brandId: input.brandId,
    candidateVersion: input.candidateVersion,
    proposedBlocks: input.proposedBlocks,
    lockedAt: startedAt,
  });
  steps.push(lock);

  const gates = validatePublicationGates({
    ...input,
    activation,
    root,
    mode,
    candidateHash: lock.contentHash,
    expectedCandidateHash: lock.contentHash,
    expectedApplicationCommit: input.expectedApplicationCommit || input.applicationCommit || null,
    forbiddenBrands: input.forbiddenBrands || [
      "receQkxgjlezsc1xg", // Curio
      "reclkgOzvAcBheUSo", // Ascend
    ],
  });
  steps.push(gates);

  const localParity = evaluateLocalProductParityGate({
    brandId: input.brandId,
    expectedCandidateHash: lock.contentHash,
    expectedApplicationCommit: input.expectedApplicationCommit || input.applicationCommit || null,
    report: input.localProductParity || input.localProductParityReport || null,
    reportPath: input.localProductParityReportPath || null,
    maxAgeMs: input.localProductParityMaxAgeMs,
    nowMs: input.nowMs,
    approvedExceptions: input.localProductParityApprovedExceptions,
  });
  if (input.persistParityEvidence !== false && localParity.report && input.persistAudit !== false) {
    try {
      const persisted = persistLocalProductParityEvidence({
        root,
        evidence: {
          ...localParity.report,
          brandId: input.brandId,
          candidateHash: lock.contentHash,
          publicationLockId: lock.lockId,
          gateEvaluationStatus: localParity.status,
        },
      });
      localParity.auditRelativePath = persisted.relativePath;
      localParity.auditPath = persisted.path;
    } catch {
      // Persistence failures are recorded but do not invent a PASS.
      localParity.persistError = true;
    }
  }
  steps.push(localParity);

  const writePlan = buildAirtableWritePlan({
    brandId: input.brandId,
    currentBlocks: input.currentBlocks,
    proposedBlocks: input.proposedBlocks,
    approvedSlotKeys: input.approvedSlotKeys,
    approveAllChanged: input.approveAllChanged,
  });
  steps.push(writePlan);

  const concurrency = verifyIdentityAndConcurrency({
    brandId: input.brandId,
    baselineContentHash: input.baselineContentHash || hashPresentationBlocks(input.currentBlocks),
    liveContentHash: input.liveContentHash || hashPresentationBlocks(input.currentBlocks),
  });
  steps.push(concurrency);

  const snapshot = createRecoverableSnapshot({
    root,
    brandId: input.brandId,
    candidateVersion: input.candidateVersion,
    contentHash: lock.contentHash,
    currentBlocks: input.currentBlocks,
    writePlanIdempotencyKey: writePlan.idempotencyKey,
    lockedAt: startedAt,
    persist: input.persistSnapshot !== false && mode !== PUBLICATION_MODES.ISOLATED_TEST,
  });
  steps.push(snapshot);

  const apply = applyWritesGated({
    mode,
    root,
    activation,
    gatesOk: gates.ok && localParity.ok,
    concurrencyOk: concurrency.ok,
    localParityOk: localParity.ok,
    writePlan,
  });
  steps.push(apply);

  const staged = stagedPublicationBoundary({ applied: apply.applied });
  steps.push(staged);

  const readback = readbackVerifyPlan({
    mode,
    applied: apply.applied,
    writePlan,
  });
  steps.push(readback);

  const promote = promoteCustomerFacing({
    root,
    activation,
    applied: apply.applied,
    readbackOk: readback.ok,
  });
  steps.push(promote);

  const postVerify = postPublishVerifyPlan({ promoted: promote.promoted });
  steps.push(postVerify);

  const rollback = buildRollbackPlan({
    snapshotPath: snapshot.path,
    snapshotRelativePath: snapshot.relativePath,
    applied: apply.applied,
  });
  steps.push(rollback);

  const blocked = !gates.ok || !localParity.ok || !concurrency.ok || !lock.ok;
  const result = {
    contract: BE_FOUNDER_APPROVAL_PUBLICATION_V1,
    mode,
    startedAt,
    finishedAt: new Date().toISOString(),
    brandId: input.brandId,
    brandSlug: input.brandSlug || null,
    brandName: input.brandName || null,
    candidateVersion: input.candidateVersion || null,
    lockId: lock.lockId,
    contentHash: lock.contentHash,
    productionWritesEnabled: productionWritesEnabled({ root, activation }),
    activation: {
      productionWritesAuthorized: Boolean(activation.productionWritesAuthorized),
      automaticPublicationEnabled: Boolean(activation.automaticPublicationEnabled),
    },
    localProductParityGate: {
      gate: BE_LOCAL_PRODUCT_PARITY_GATE,
      status: localParity.status,
      ok: localParity.ok,
      auditRelativePath: localParity.auditRelativePath || null,
    },
    status: blocked
      ? "BLOCKED_GATES"
      : apply.applied
        ? "APPLIED"
        : "DRY_RUN_COMPLETE",
    airtableWrites: apply.airtableWrites || 0,
    publicationApplied: Boolean(apply.applied),
    customerPromoted: Boolean(promote.promoted),
    steps,
    writePlanSummary: writePlan.counts,
    idempotencyKey: writePlan.idempotencyKey,
    audit: {
      approver: input.approver || null,
      brandDecision: input.brandDecision || null,
      evidencePaths: [
        snapshot.relativePath,
        localParity.auditRelativePath || null,
        input.reviewPath || null,
      ].filter(Boolean),
    },
    restartable: true,
    idempotent: true,
    note:
      "Ordinary approved publications will use this workflow. Production Airtable writes require separate founder activation plus BE_LOCAL_PRODUCT_PARITY_GATE PASS. Curio/Ascend remain NOT APPROVED.",
  };

  if (input.persistAudit !== false) {
    const auditDir = join(root, "reports", "brand-explorer", "comparison-studio", "publication-runs");
    mkdirSync(auditDir, { recursive: true });
    const safeId = String(input.brandId || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_");
    const auditPath = join(
      auditDir,
      `${safeId}-${result.status}-${startedAt.replace(/[:.]/g, "-")}.json`
    );
    writeFileSync(auditPath, JSON.stringify(result, null, 2));
    result.auditPath = auditPath;
    result.auditRelativePath = `reports/brand-explorer/comparison-studio/publication-runs/${safeId}-${result.status}-${startedAt.replace(/[:.]/g, "-")}.json`;
  }

  return result;
}
