/**
 * Durable last-verify status for GDI Admin Full Content Parity column.
 * Local/volume JSON — not Airtable. No share tokens stored.
 */
import fs from "node:fs";
import path from "node:path";
import { resolveGdiPublishedInventoryRoot } from "../durable-publication/resolve-gdi-published-registry-root-v1.js";

export const GDI_FULL_CONTENT_PARITY_STATUS_STORE_V1 =
  "gdi-full-content-parity-status-store-v1";

function resolveStorePath(env = process.env, opts = {}) {
  if (opts.storePath) return opts.storePath;
  const root =
    opts.root ||
    resolveGdiPublishedInventoryRoot(env) ||
    path.join(process.cwd(), "data/group-demand-intelligence/published-inventory");
  return path.join(root, "parity-status", "last-verify-by-hotel-v1.json");
}

function emptyDoc() {
  return {
    version: GDI_FULL_CONTENT_PARITY_STATUS_STORE_V1,
    updatedAt: null,
    hotels: {},
  };
}

export function loadGdiParityStatusStore(opts = {}) {
  const file = resolveStorePath(opts.env || process.env, opts);
  try {
    if (!fs.existsSync(file)) return emptyDoc();
    const doc = JSON.parse(fs.readFileSync(file, "utf8"));
    return doc && typeof doc === "object" ? doc : emptyDoc();
  } catch {
    return emptyDoc();
  }
}

export function getHotelParityStatus(hotelId, opts = {}) {
  const doc = loadGdiParityStatusStore(opts);
  return doc.hotels?.[String(hotelId || "").trim()] || null;
}

/**
 * Persist a hotel verify summary (no share JWTs, no full opportunity bodies).
 */
export function recordHotelParityStatus(hotelId, verifyResult = {}, opts = {}) {
  const id = String(hotelId || "").trim();
  if (!id) return { ok: false, code: "HOTEL_ID_REQUIRED" };
  const file = resolveStorePath(opts.env || process.env, opts);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const doc = loadGdiParityStatusStore(opts);
  const diffs = (verifyResult.criticalDifferences || []).slice(0, 40).map((d) => ({
    opportunityId: d.opportunityId || null,
    fieldPath: d.fieldPath || d.path || null,
    approvedValue:
      typeof d.approvedValue === "string"
        ? d.approvedValue.slice(0, 200)
        : d.approvedValue ?? d.left ?? null,
    publishedValue:
      typeof d.publishedValue === "string"
        ? d.publishedValue.slice(0, 200)
        : d.publishedValue ?? d.right ?? null,
    rootCause: d.rootCause || null,
    requiredCorrection: d.requiredCorrection || null,
  }));
  doc.hotels[id] = {
    hotelId: id,
    hotelName: verifyResult.hotelName || null,
    status: verifyResult.status || null,
    ok: verifyResult.ok === true,
    publishedOpportunityCount: verifyResult.publishedOpportunityCount ?? null,
    publicationVersion: verifyResult.publicationVersion || null,
    publicationId: verifyResult.publicationId || null,
    fieldsCompared: verifyResult.fieldsCompared || 0,
    criticalDifferenceCount: (verifyResult.criticalDifferences || []).length,
    differences: diffs,
    verifiedAt: verifyResult.verifiedAt || new Date().toISOString(),
    legacySnapshotsMissingApprovedDetail:
      verifyResult.legacySnapshotsMissingApprovedDetail === true,
  };
  doc.updatedAt = new Date().toISOString();
  fs.writeFileSync(file, JSON.stringify(doc, null, 2) + "\n", "utf8");
  return { ok: true, hotelId: id, status: doc.hotels[id], storePath: file };
}
