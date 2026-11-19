/**
 * Brand Explorer Comparison Studio — Approve & Publish API.
 *
 * Default: DRY_RUN only. Never enables production Airtable writes unless
 * founder activation config + env flag are both set (still refused until
 * live writer is implemented and gates pass).
 */
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  PUBLICATION_MODES,
  runFounderApprovalPublication,
  loadActivationConfig,
  productionWritesEnabled,
  hashPresentationBlocks,
} from "../lib/brand-explorer/be-founder-approval-publication-v1.js";

function repoRoot() {
  return process.cwd();
}

function sanitizeId(id) {
  return String(id || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 80);
}

function reviewPath(brandId) {
  return join(repoRoot(), "reports", "brand-explorer", "comparison-studio", `${sanitizeId(brandId)}-founder-review.json`);
}

function loadReview(brandId) {
  const path = reviewPath(brandId);
  if (!existsSync(path)) return { reviews: {}, brandDecision: null };
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return { reviews: {}, brandDecision: null };
  }
}

function loadJsonSafe(relOrAbs) {
  if (!relOrAbs) return null;
  const path = relOrAbs.startsWith("/") || /^[A-Za-z]:\\/.test(relOrAbs)
    ? join(repoRoot(), String(relOrAbs).replace(/^\//, "").replace(/\//g, "\\"))
    : join(repoRoot(), relOrAbs);
  // Also try public path for overlay URLs like /brand-explorer-phase2e-preview/...
  const candidates = [
    path,
    join(repoRoot(), "public", String(relOrAbs).replace(/^\//, "")),
    join(repoRoot(), String(relOrAbs).replace(/^\//, "")),
  ];
  for (const p of candidates) {
    if (existsSync(p)) {
      try {
        return JSON.parse(readFileSync(p, "utf8"));
      } catch {
        /* continue */
      }
    }
  }
  return null;
}

function blocksFromCandidate(overlay) {
  if (!overlay) return [];
  const blocks = (overlay.brandExplorer && overlay.brandExplorer.blocks) || overlay.blocks || [];
  return Array.isArray(blocks) ? blocks : [];
}

/**
 * POST /api/brand-explorer-compare/publish
 * Body: {
 *   brandId, brandSlug?, brandName?,
 *   candidateVersion?, overlayUrl?,
 *   currentBlocks?, proposedBlocks?,
 *   baselineContentHash?, liveContentHash?,
 *   mode?: DRY_RUN|ISOLATED_TEST|PRODUCTION,
 *   approveAndPublish?: boolean,
 *   approver?: string,
 *   gate1?, gate2?, scorecard?,
 *   approvedSlotKeys?, approveAllChanged?
 * }
 */
export function postBrandExplorerComparePublish(req, res) {
  try {
    const body = req.body || {};
    const brandId = String(body.brandId || "").trim();
    if (!brandId) {
      return res.status(400).json({ success: false, error: "brandId required" });
    }

    const activation = loadActivationConfig(repoRoot());
    const requestedMode = String(body.mode || PUBLICATION_MODES.DRY_RUN).toUpperCase();
    let mode = PUBLICATION_MODES.DRY_RUN;
    if (requestedMode === PUBLICATION_MODES.ISOLATED_TEST) mode = PUBLICATION_MODES.ISOLATED_TEST;
    if (requestedMode === PUBLICATION_MODES.PRODUCTION) {
      // Force dry-run unless both activation and env are set — still will not write
      mode = productionWritesEnabled({ activation })
        ? PUBLICATION_MODES.PRODUCTION
        : PUBLICATION_MODES.DRY_RUN;
    }

    const review = loadReview(brandId);
    let brandDecision = body.brandDecision || review.brandDecision || null;
    if (body.approveAndPublish) {
      brandDecision = {
        decision: "approve_and_publish",
        brandId,
        recordedAt: new Date().toISOString(),
        airtableWrites: 0,
        publicationApplied: false,
        note: "Approve & Publish requested from Comparison Studio (workflow may still be blocked by gates / activation).",
      };
      // Persist intent without claiming publication applied
      const path = reviewPath(brandId);
      mkdirSync(join(repoRoot(), "reports", "brand-explorer", "comparison-studio"), {
        recursive: true,
      });
      const payload = {
        ...review,
        brandId,
        brandDecision,
        updatedAt: new Date().toISOString(),
        airtableWrites: 0,
        publicationApplied: false,
        auditTrail: [
          ...(Array.isArray(review.auditTrail) ? review.auditTrail : []),
          {
            at: new Date().toISOString(),
            type: "approve_and_publish_requested",
            mode,
          },
        ],
      };
      writeFileSync(path, JSON.stringify(payload, null, 2));
    }

    let proposedBlocks = Array.isArray(body.proposedBlocks) ? body.proposedBlocks : null;
    let currentBlocks = Array.isArray(body.currentBlocks) ? body.currentBlocks : null;
    if (!proposedBlocks && body.overlayUrl) {
      const overlay = loadJsonSafe(body.overlayUrl);
      proposedBlocks = blocksFromCandidate(overlay);
    }
    if (!proposedBlocks) proposedBlocks = [];
    if (!currentBlocks) currentBlocks = [];

    const approvedFromReviews = Object.values(review.reviews || {})
      .filter((r) => r && (r.decision === "approved" || r.decision === "improved"))
      .map((r) => r.slotKey)
      .filter(Boolean);

    const result = runFounderApprovalPublication({
      root: repoRoot(),
      mode,
      brandId,
      brandSlug: body.brandSlug || null,
      brandName: body.brandName || null,
      candidateVersion: body.candidateVersion || "phase2h-component-parity",
      currentBlocks,
      proposedBlocks,
      baselineContentHash: body.baselineContentHash || hashPresentationBlocks(currentBlocks),
      liveContentHash: body.liveContentHash || hashPresentationBlocks(currentBlocks),
      brandDecision,
      gate1: body.gate1 || { founderApproved: false },
      gate2: body.gate2 || { status: "BLOCKED", founderApproved: false },
      scorecard: body.scorecard || {
        founderApprovalStatus: "NOT_FOUNDER_APPROVED",
        priorityBlocked: 0,
        researchGapCount: 0,
      },
      approvedSlotKeys: body.approvedSlotKeys || approvedFromReviews,
      approveAllChanged: Boolean(body.approveAllChanged),
      outstandingRequirementCount: body.outstandingRequirementCount || 0,
      requireZeroOutstandingRequirements: Boolean(body.requireZeroOutstandingRequirements),
      approver: body.approver || "founder",
      reviewPath: `reports/brand-explorer/comparison-studio/${sanitizeId(brandId)}-founder-review.json`,
      activation,
      persistSnapshot: mode !== PUBLICATION_MODES.ISOLATED_TEST,
      persistAudit: true,
    });

    return res.json({
      success: true,
      ...result,
      message:
        result.status === "BLOCKED_GATES"
          ? "Approve & Publish recorded intent / dry-run only. Gates blocked — no Airtable writes."
          : result.publicationApplied
            ? "Publication applied"
            : "Dry-run complete — no Airtable writes. Production publishing remains inactive.",
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      error: err?.message || String(err),
      airtableWrites: 0,
      publicationApplied: false,
    });
  }
}

export function getBrandExplorerComparePublishStatus(req, res) {
  try {
    const activation = loadActivationConfig(repoRoot());
    return res.json({
      success: true,
      contract: "BE_FOUNDER_APPROVAL_PUBLICATION_V1",
      productionWritesEnabled: productionWritesEnabled({ activation }),
      activation: {
        productionWritesAuthorized: Boolean(activation.productionWritesAuthorized),
        automaticPublicationEnabled: Boolean(activation.automaticPublicationEnabled),
        note: activation.note || null,
      },
      envFlag: String(process.env.BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES || "") === "1",
      defaultMode: PUBLICATION_MODES.DRY_RUN,
      curioAscend: "NOT_APPROVED",
      gate1: "AWAITING_FOUNDER_APPROVAL",
      gate2: "BLOCKED",
    });
  } catch (err) {
    return res.status(500).json({ success: false, error: err?.message || String(err) });
  }
}
