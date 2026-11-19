/**
 * Brand Explorer Comparison Studio — local founder review persistence.
 * Writes ONLY under reports/brand-explorer/comparison-studio/ (never Airtable).
 */
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";

function repoRoot() {
  return process.cwd();
}

function reviewDir() {
  const dir = join(repoRoot(), "reports", "brand-explorer", "comparison-studio");
  mkdirSync(dir, { recursive: true });
  return dir;
}

function sanitizeId(id) {
  return String(id || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 80);
}

export function postBrandExplorerCompareReview(req, res) {
  try {
    const brandId = sanitizeId(req.body?.brandId || req.body?.entry?.brandId);
    const all = req.body?.all && typeof req.body.all === "object" ? req.body.all : null;
    const entry = req.body?.entry || null;
    const brandDecision = req.body?.brandDecision || null;
    const publicationPackage = req.body?.publicationPackage || null;
    if (!brandId) {
      return res.status(400).json({ success: false, error: "brandId required" });
    }
    const path = join(reviewDir(), `${brandId}-founder-review.json`);
    let payload = {
      contract: "BE_COMPARISON_STUDIO_REVIEW_V1",
      brandId,
      updatedAt: new Date().toISOString(),
      airtableWrites: 0,
      publicationApplied: false,
      reviews: {},
      brandDecision: null,
      publicationPackageDraft: null,
      auditTrail: [],
    };
    if (existsSync(path)) {
      try {
        payload = { ...payload, ...JSON.parse(readFileSync(path, "utf8")) };
      } catch {
        /* replace */
      }
    }
    if (all) payload.reviews = all;
    else if (entry && entry.slotKey) {
      payload.reviews = payload.reviews || {};
      payload.reviews[entry.slotKey] = entry;
      payload.auditTrail = Array.isArray(payload.auditTrail) ? payload.auditTrail : [];
      payload.auditTrail.push({
        at: new Date().toISOString(),
        type: "subsection_decision",
        slotKey: entry.slotKey,
        decision: entry.decision || null,
      });
    }
    if (brandDecision && typeof brandDecision === "object") {
      payload.brandDecision = {
        ...brandDecision,
        recordedAt: new Date().toISOString(),
        airtableWrites: 0,
        publicationApplied: false,
        note:
          brandDecision.note ||
          "Founder brand-level decision recorded. Does NOT write Airtable or publish.",
      };
      payload.auditTrail = Array.isArray(payload.auditTrail) ? payload.auditTrail : [];
      payload.auditTrail.push({
        at: new Date().toISOString(),
        type: "brand_decision",
        decision: brandDecision.decision || null,
      });
    }
    if (publicationPackage && typeof publicationPackage === "object") {
      payload.publicationPackageDraft = {
        ...publicationPackage,
        preparedAt: new Date().toISOString(),
        status: "DRAFT_NOT_APPLIED",
        airtableWrites: 0,
        applied: false,
      };
      payload.auditTrail = Array.isArray(payload.auditTrail) ? payload.auditTrail : [];
      payload.auditTrail.push({
        at: new Date().toISOString(),
        type: "publication_package_draft",
        changeCount: Array.isArray(publicationPackage.approvedChanges)
          ? publicationPackage.approvedChanges.length
          : 0,
      });
    }
    payload.updatedAt = new Date().toISOString();
    payload.airtableWrites = 0;
    payload.publicationApplied = false;
    writeFileSync(path, JSON.stringify(payload, null, 2));
    return res.json({
      success: true,
      path: `reports/brand-explorer/comparison-studio/${brandId}-founder-review.json`,
      writes: 0,
      publicationApplied: false,
    });
  } catch (err) {
    return res.status(500).json({ success: false, error: err?.message || String(err) });
  }
}

export function getBrandExplorerCompareReview(req, res) {
  try {
    const brandId = sanitizeId(req.query.brandId);
    const path = join(reviewDir(), `${brandId}-founder-review.json`);
    if (!existsSync(path)) return res.json({ success: true, reviews: {} });
    const payload = JSON.parse(readFileSync(path, "utf8"));
    return res.json({ success: true, ...payload });
  } catch (err) {
    return res.status(500).json({ success: false, error: err?.message || String(err) });
  }
}
