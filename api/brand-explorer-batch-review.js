/**
 * Brand Explorer Batch Review API (Factory 2.0).
 * Read portfolio + record per-candidate batch approvals. No production writeback here.
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  buildPortfolioDashboard,
  loadBrandStage,
  loadQueue,
  readJson,
  stagingRoot,
} from "../lib/brand-explorer/factory-2/be-factory-2-staging-store-v1.js";
import { recordBatchApprovals } from "../lib/brand-explorer/factory-2/be-factory-2-batch-approval-v1.js";
import { FACTORY_2_STATUSES } from "../lib/brand-explorer/factory-2/be-factory-2-contracts-v1.js";

const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

function sendJson(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(body));
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf8");
        resolve(raw ? JSON.parse(raw) : {});
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

/** GET /api/brand-explorer-batch-review/portfolio */
export function getBrandExplorerBatchPortfolio(req, res) {
  try {
    const filter = String(req.query?.status || "").trim();
    const dashboard = buildPortfolioDashboard(REPO_ROOT);
    let rows = dashboard.rows || [];
    if (filter && Object.values(FACTORY_2_STATUSES).includes(filter)) {
      rows = rows.filter((r) => r.factoryStatus === filter);
    }
    return sendJson(res, 200, {
      ok: true,
      contract: dashboard.contract,
      generatedAt: dashboard.generatedAt,
      summary: dashboard.summary,
      filter: filter || null,
      rows,
      productionWritebackDefault: false,
    });
  } catch (err) {
    return sendJson(res, 500, { ok: false, error: String(err?.message || err) });
  }
}

/** GET /api/brand-explorer-batch-review/brand/:brandId */
export function getBrandExplorerBatchBrand(req, res) {
  try {
    const brandId = String(req.params?.brandId || "").trim();
    if (!brandId) return sendJson(res, 400, { ok: false, error: "brandId required" });
    const stage = loadBrandStage(REPO_ROOT, brandId);
    if (!stage) return sendJson(res, 404, { ok: false, error: "Brand not staged" });
    return sendJson(res, 200, {
      ok: true,
      brandId,
      factoryStatus: stage.factoryStatus,
      approvalStatus: stage.approvalStatus,
      candidateHash: stage.candidateHash,
      assembledHash: stage.assembledHash,
      certification: stage.certification,
      writePlan: stage.writePlan,
      researchSpend: stage.researchSpend,
      approval: stage.approval || null,
      compareStudioUrl: `/app#/brand-explorer-compare?id=${brandId}&beInternalPreview=1`,
      proposedProductUrl: `/app#/brand-explorer-combined?id=${brandId}&beInternalPreview=1&beProfileMode=proposed`,
      writebackAuthorized: false,
    });
  } catch (err) {
    return sendJson(res, 500, { ok: false, error: String(err?.message || err) });
  }
}

/** GET /api/brand-explorer-batch-review/queue */
export function getBrandExplorerBatchQueue(req, res) {
  try {
    const queue = loadQueue(REPO_ROOT);
    if (!queue) return sendJson(res, 404, { ok: false, error: "Queue not built yet" });
    return sendJson(res, 200, { ok: true, queue });
  } catch (err) {
    return sendJson(res, 500, { ok: false, error: String(err?.message || err) });
  }
}

/**
 * POST /api/brand-explorer-batch-review/approve
 * Body: { approvedBy, selections: [{ brandId, candidateHash }] }
 * Records per-candidate approvals only — does not write Airtable Presentation.
 */
export async function postBrandExplorerBatchApprove(req, res) {
  try {
    const body = await parseBody(req);
    const result = recordBatchApprovals(REPO_ROOT, {
      approvedBy: body.approvedBy || req.dealalityUser?.email || "admin",
      selections: body.selections || [],
    });
    return sendJson(res, result.ok ? 200 : 400, {
      ...result,
      productionWritebackExecuted: false,
      note: "Approvals recorded against exact candidate hashes. Production Presentation unchanged.",
    });
  } catch (err) {
    return sendJson(res, 500, { ok: false, error: String(err?.message || err) });
  }
}

/** GET /api/brand-explorer-batch-review/approvals/:approvalId */
export function getBrandExplorerBatchApproval(req, res) {
  try {
    const id = String(req.params?.approvalId || "").trim();
    const doc = readJson(join(stagingRoot(REPO_ROOT), "approvals", `${id}.json`), null);
    if (!doc) return sendJson(res, 404, { ok: false, error: "Approval not found" });
    return sendJson(res, 200, { ok: true, approval: doc });
  } catch (err) {
    return sendJson(res, 500, { ok: false, error: String(err?.message || err) });
  }
}
