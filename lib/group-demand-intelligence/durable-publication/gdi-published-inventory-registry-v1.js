/**
 * GDI_PUBLISHED_INVENTORY_REGISTRY_V1
 *
 * Durable customer-publication registry. Separates:
 *   - research eligibility (computed)
 *   - publication authorization (founder-approved registry rows)
 *
 * ACCOUNT_DEVELOPMENT is a first-class customer-facing classification.
 *
 * Storage adapter: filesystem (local simulation + Railway volume mirror).
 * Production write path preference: Airtable upsert of the same schema
 * (see reports/gdi-durable-publication-registry-spice-island-v1/).
 *
 * Does NOT auto-publish from research. Does NOT issue share tokens.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import {
  GDI_PUBLISHED_INVENTORY_REGISTRY_V1,
  resolveGdiPublishedInventoryRoot,
} from "./resolve-gdi-published-registry-root-v1.js";
import { validateCustomerFacingPublishedSnapshot } from "./gdi-customer-facing-published-snapshot-v1.js";

export { GDI_PUBLISHED_INVENTORY_REGISTRY_V1 };

export const PUBLICATION_STATE = Object.freeze({
  STAGED: "STAGED",
  PUBLISHED: "PUBLISHED",
  REVOKED: "REVOKED",
  SUPERSEDED: "SUPERSEDED",
});

export const COMMERCIAL_PUBLICATION_CLASS = Object.freeze({
  SALES_READY: "SALES_READY",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
});

export const CUSTOMER_FACING_PUBLICATION_CLASSES = Object.freeze([
  COMMERCIAL_PUBLICATION_CLASS.SALES_READY,
  COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT,
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function sha256Json(obj) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(obj))
    .digest("hex");
}

function atomicWriteJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
  try {
    fs.renameSync(tmp, filePath);
  } catch {
    fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
    try {
      fs.unlinkSync(tmp);
    } catch {
      /* best-effort */
    }
  }
}

function readJson(filePath, fallback) {
  if (!fs.existsSync(filePath)) return fallback;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function emptyRegistry() {
  return {
    version: GDI_PUBLISHED_INVENTORY_REGISTRY_V1,
    updatedAt: null,
    hotels: {},
  };
}

/**
 * Create a filesystem-backed published inventory registry.
 */
export function createGdiPublishedInventoryRegistry(options = {}) {
  const env = options.env || process.env;
  const root = resolveGdiPublishedInventoryRoot(env, options);
  const paths = {
    root,
    registryFile: path.join(root, "registry", "current-registry-v1.json"),
    historyDir: path.join(root, "history"),
    snapshotsDir: path.join(root, "snapshots"),
    locksDir: path.join(root, "locks"),
  };

  function ensureLayout() {
    for (const d of [
      path.dirname(paths.registryFile),
      paths.historyDir,
      paths.snapshotsDir,
      paths.locksDir,
    ]) {
      fs.mkdirSync(d, { recursive: true });
    }
  }

  function lockFile(hotelId) {
    return path.join(paths.locksDir, `${hotelId}.lock`);
  }

  function withHotelLock(hotelId, fn) {
    ensureLayout();
    const lp = lockFile(hotelId);
    let fd;
    try {
      fd = fs.openSync(lp, "wx");
      fs.writeFileSync(
        fd,
        JSON.stringify({ hotelId, pid: process.pid, at: nowIso() }),
        "utf8"
      );
    } catch (err) {
      const e = new Error(`GDI_PUBLISHED_INVENTORY_LOCK_HELD:${hotelId}`);
      e.code = "GDI_PUBLISHED_INVENTORY_LOCK_HELD";
      e.cause = err;
      throw e;
    }
    try {
      return fn();
    } finally {
      try {
        fs.closeSync(fd);
      } catch {
        /* ignore */
      }
      try {
        fs.unlinkSync(lp);
      } catch {
        /* ignore */
      }
    }
  }

  function loadRegistry() {
    ensureLayout();
    return readJson(paths.registryFile, emptyRegistry());
  }

  function saveRegistry(doc) {
    const next = { ...doc, updatedAt: nowIso(), version: GDI_PUBLISHED_INVENTORY_REGISTRY_V1 };
    atomicWriteJson(paths.registryFile, next);
    return next;
  }

  function appendHistory(hotelId, opportunityId, event) {
    const dir = path.join(paths.historyDir, hotelId, opportunityId);
    fs.mkdirSync(dir, { recursive: true });
    const stamp = nowIso().replace(/[:.]/g, "-");
    const file = path.join(dir, `${stamp}_${event.type}.json`);
    atomicWriteJson(file, event);
    return file;
  }

  function writeSnapshot(hotelId, opportunityId, version, snapshot) {
    const dir = path.join(paths.snapshotsDir, hotelId, opportunityId);
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, `v${version}.json`);
    atomicWriteJson(file, {
      hotelId,
      opportunityId,
      publishedVersion: version,
      snapshot,
      contentHash: sha256Json(snapshot),
      writtenAt: nowIso(),
    });
    return file;
  }

  function hotelBucket(reg, hotelId) {
    if (!reg.hotels[hotelId]) {
      reg.hotels[hotelId] = { hotelId, opportunities: {} };
    }
    return reg.hotels[hotelId];
  }

  function assertCustomerFacingClass(classification) {
    const c = String(classification || "").trim();
    if (!CUSTOMER_FACING_PUBLICATION_CLASSES.includes(c)) {
      const err = new Error(
        `UNAUTHORIZED_PUBLICATION_CLASS:${c || "missing"} — only SALES_READY or ACCOUNT_DEVELOPMENT may publish`
      );
      err.code = "UNAUTHORIZED_PUBLICATION_CLASS";
      throw err;
    }
    return c;
  }

  function assertApproval(approval = {}) {
    if (!approval || approval.authorized !== true) {
      const err = new Error(
        "PUBLICATION_REQUIRES_EXPLICIT_APPROVAL — research eligibility is not publication authorization"
      );
      err.code = "PUBLICATION_REQUIRES_EXPLICIT_APPROVAL";
      throw err;
    }
    return {
      authorized: true,
      authorizedBy: text(approval.authorizedBy) || "founder",
      authorizedAt: text(approval.authorizedAt) || nowIso(),
      authorizationPhrase: text(approval.authorizationPhrase) || null,
      note: text(approval.note) || null,
    };
  }

  /**
   * Publish (or first-time publish) an approved opportunity.
   * Research eligibility flags on the snapshot are ignored for authorization.
   */
  function publishOpportunity(input = {}) {
    const hotelId = text(input.hotelId);
    const opportunityId = text(input.opportunityId);
    if (!hotelId || !opportunityId) {
      throw new Error("hotelId_and_opportunityId_required");
    }
    const classification = assertCustomerFacingClass(input.classification);
    const approval = assertApproval(input.approval);
    const snapshot = input.snapshot && typeof input.snapshot === "object" ? input.snapshot : {};
    if (text(snapshot.id) && text(snapshot.id) !== opportunityId) {
      throw new Error("snapshot_id_mismatch");
    }
    if (input.skipCommercialContentGate !== true) {
      const gate = validateCustomerFacingPublishedSnapshot({
        ...snapshot,
        id: opportunityId,
      });
      if (!gate.ok) {
        const err = new Error(
          `PUBLICATION_CONTENT_GATE_FAILED:${opportunityId}:${gate.fails.join(",")}`,
        );
        err.code = "PUBLICATION_CONTENT_GATE_FAILED";
        err.gate = gate;
        throw err;
      }
    }

    return withHotelLock(hotelId, () => {
      const reg = loadRegistry();
      const bucket = hotelBucket(reg, hotelId);
      const existing = bucket.opportunities[opportunityId] || null;
      if (existing?.publicationState === PUBLICATION_STATE.PUBLISHED) {
        const err = new Error(
          `ALREADY_PUBLISHED:${opportunityId} — use updatePublishedOpportunity`
        );
        err.code = "ALREADY_PUBLISHED";
        throw err;
      }

      const publishedVersion = (existing?.publishedVersion || 0) + 1;
      const row = {
        hotelId,
        opportunityId,
        commercialClassification: classification,
        publicationState: PUBLICATION_STATE.PUBLISHED,
        publishedVersion,
        publishedAt: nowIso(),
        lastVerifiedAt: nowIso(),
        sourceResearchReference: text(input.sourceResearchReference) || null,
        approval,
        contentHash: sha256Json(snapshot),
        revokedAt: null,
        revokeReason: null,
        customerFacing: true,
      };
      bucket.opportunities[opportunityId] = row;
      writeSnapshot(hotelId, opportunityId, publishedVersion, {
        ...snapshot,
        id: opportunityId,
        hotelId,
        commercialInventoryClass: classification,
        publicationState: PUBLICATION_STATE.PUBLISHED,
        publishedVersion,
      });
      appendHistory(hotelId, opportunityId, {
        type: "PUBLISH",
        at: row.publishedAt,
        hotelId,
        opportunityId,
        publishedVersion,
        commercialClassification: classification,
        approval,
        contentHash: row.contentHash,
      });
      saveRegistry(reg);
      return { ok: true, row };
    });
  }

  function updatePublishedOpportunity(input = {}) {
    const hotelId = text(input.hotelId);
    const opportunityId = text(input.opportunityId);
    const approval = assertApproval(input.approval);
    const classification = input.classification
      ? assertCustomerFacingClass(input.classification)
      : null;
    const snapshot = input.snapshot && typeof input.snapshot === "object" ? input.snapshot : null;

    return withHotelLock(hotelId, () => {
      const reg = loadRegistry();
      const existing = hotelBucket(reg, hotelId).opportunities[opportunityId];
      if (!existing || existing.publicationState !== PUBLICATION_STATE.PUBLISHED) {
        const err = new Error(`NOT_PUBLISHED:${opportunityId}`);
        err.code = "NOT_PUBLISHED";
        throw err;
      }
      const publishedVersion = existing.publishedVersion + 1;
      const nextClass = classification || existing.commercialClassification;
      const nextSnapshot = snapshot || { id: opportunityId };
      if (snapshot && input.skipCommercialContentGate !== true) {
        const gate = validateCustomerFacingPublishedSnapshot({
          ...nextSnapshot,
          id: opportunityId,
        });
        if (!gate.ok) {
          const err = new Error(
            `PUBLICATION_CONTENT_GATE_FAILED:${opportunityId}:${gate.fails.join(",")}`,
          );
          err.code = "PUBLICATION_CONTENT_GATE_FAILED";
          err.gate = gate;
          throw err;
        }
      }
      const row = {
        ...existing,
        commercialClassification: nextClass,
        publishedVersion,
        lastVerifiedAt: nowIso(),
        sourceResearchReference:
          text(input.sourceResearchReference) || existing.sourceResearchReference,
        approval,
        contentHash: sha256Json(nextSnapshot),
        publicationState: PUBLICATION_STATE.PUBLISHED,
        customerFacing: true,
      };
      hotelBucket(reg, hotelId).opportunities[opportunityId] = row;
      // Prior version remains in snapshots/history as SUPERSEDED event
      appendHistory(hotelId, opportunityId, {
        type: "UPDATE",
        at: nowIso(),
        hotelId,
        opportunityId,
        publishedVersion,
        previousVersion: existing.publishedVersion,
        commercialClassification: nextClass,
        approval,
        contentHash: row.contentHash,
        priorState: PUBLICATION_STATE.SUPERSEDED,
      });
      writeSnapshot(hotelId, opportunityId, publishedVersion, {
        ...nextSnapshot,
        id: opportunityId,
        hotelId,
        commercialInventoryClass: nextClass,
        publicationState: PUBLICATION_STATE.PUBLISHED,
        publishedVersion,
      });
      saveRegistry(reg);
      return { ok: true, row };
    });
  }

  function revokePublication(input = {}) {
    const hotelId = text(input.hotelId);
    const opportunityId = text(input.opportunityId);
    const approval = assertApproval(input.approval);
    return withHotelLock(hotelId, () => {
      const reg = loadRegistry();
      const existing = hotelBucket(reg, hotelId).opportunities[opportunityId];
      if (!existing) {
        const err = new Error(`NOT_FOUND:${opportunityId}`);
        err.code = "NOT_FOUND";
        throw err;
      }
      const row = {
        ...existing,
        publicationState: PUBLICATION_STATE.REVOKED,
        revokedAt: nowIso(),
        revokeReason: text(input.reason) || "founder_revoke",
        customerFacing: false,
        approval,
        lastVerifiedAt: nowIso(),
      };
      hotelBucket(reg, hotelId).opportunities[opportunityId] = row;
      appendHistory(hotelId, opportunityId, {
        type: "REVOKE",
        at: row.revokedAt,
        hotelId,
        opportunityId,
        publishedVersion: existing.publishedVersion,
        reason: row.revokeReason,
        approval,
      });
      saveRegistry(reg);
      return { ok: true, row };
    });
  }

  function listPublished(hotelId, opts = {}) {
    const reg = loadRegistry();
    const bucket = reg.hotels[text(hotelId)] || { opportunities: {} };
    const rows = Object.values(bucket.opportunities || {});
    if (opts.includeRevoked) return rows;
    return rows.filter((r) => r.publicationState === PUBLICATION_STATE.PUBLISHED);
  }

  function listAllHotelsPublished(opts = {}) {
    const reg = loadRegistry();
    const out = [];
    for (const hotelId of Object.keys(reg.hotels || {})) {
      out.push(...listPublished(hotelId, opts).map((r) => ({ ...r, hotelId })));
    }
    return out;
  }

  function getHistory(hotelId, opportunityId) {
    const dir = path.join(paths.historyDir, text(hotelId), text(opportunityId));
    if (!fs.existsSync(dir)) return [];
    return fs
      .readdirSync(dir)
      .filter((f) => f.endsWith(".json"))
      .sort()
      .map((f) => readJson(path.join(dir, f), null))
      .filter(Boolean);
  }

  function loadSnapshot(hotelId, opportunityId, version) {
    const file = path.join(
      paths.snapshotsDir,
      text(hotelId),
      text(opportunityId),
      `v${version}.json`
    );
    return readJson(file, null);
  }

  function getPublishedOpportunityIds(hotelId) {
    return listPublished(hotelId).map((r) => r.opportunityId);
  }

  /**
   * Reject research-only promotion attempts (no approval).
   */
  function assertCannotPromoteFromResearchAlone(input = {}) {
    try {
      assertApproval(input.approval);
      return { ok: false, blocked: false };
    } catch (err) {
      return { ok: true, blocked: true, code: err.code, message: err.message };
    }
  }

  return {
    version: GDI_PUBLISHED_INVENTORY_REGISTRY_V1,
    root: paths.root,
    paths,
    loadRegistry,
    publishOpportunity,
    updatePublishedOpportunity,
    revokePublication,
    listPublished,
    listAllHotelsPublished,
    getHistory,
    loadSnapshot,
    getPublishedOpportunityIds,
    assertCannotPromoteFromResearchAlone,
    PUBLICATION_STATE,
    COMMERCIAL_PUBLICATION_CLASS,
  };
}

/**
 * Merge published registry snapshots into an opportunity list for customer surfaces.
 * Unpublished research rows are not added. Revoked rows are excluded.
 * Does nothing when registry empty for hotel.
 */
export function mergePublishedInventoryIntoOpportunities(
  hotelId,
  opportunities = [],
  opts = {}
) {
  const registry =
    opts.registry ||
    createGdiPublishedInventoryRegistry({
      env: opts.env || process.env,
      root: opts.root,
    });
  const published = registry.listPublished(hotelId);
  if (!published.length) return opportunities || [];

  const byId = new Map();
  for (const o of opportunities || []) {
    if (o?.id) byId.set(String(o.id), o);
  }

  for (const row of published) {
    const snapDoc = registry.loadSnapshot(
      hotelId,
      row.opportunityId,
      row.publishedVersion
    );
    const snap = snapDoc?.snapshot || { id: row.opportunityId };
    const existing = byId.get(row.opportunityId);
    const classification = row.commercialClassification;
    const isSalesReady =
      classification === COMMERCIAL_PUBLICATION_CLASS.SALES_READY;
    const isAccountDevelopment =
      classification === COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT;
    // Stamp inventory presentation fields the customer DTO already understands so
    // published Account Development / Sales Ready are not reclassified as
    // EXCLUDED / FUTURE_OPPORTUNITY by research-derived lane heuristics.
    const inventoryLane = isSalesReady
      ? "SALES_OPPORTUNITY"
      : isAccountDevelopment
        ? "ACCOUNT_DEVELOPMENT"
        : null;
    const inventoryLaneLabel = isSalesReady
      ? "Sales opportunity"
      : isAccountDevelopment
        ? "Account development"
        : null;
    const sources =
      Array.isArray(snap.sources) && snap.sources.length
        ? snap.sources
        : Array.isArray(snap.evidenceUrls)
          ? snap.evidenceUrls.map((url) =>
              typeof url === "string" ? { url } : url
            )
          : existing?.sources || [];
    const certifiedNext =
      String(
        snap.recommendedNextStep ||
          snap.recommendedAction ||
          existing?.recommendedNextStep ||
          existing?.recommendedAction ||
          "",
      )
        .replace(/\s+/g, " ")
        .trim() || null;
    const mergedRaw = {
      ...(existing || {}),
      ...snap,
      id: row.opportunityId,
      hotelId,
      commercialInventoryClass: classification,
      publicationState: row.publicationState,
      publishedVersion: row.publishedVersion,
      publishedAt: row.publishedAt,
      customerVisible: true,
      customerFacing: true,
      gdiPublishedInventoryRegistry: true,
      doesNotAffectProduction: false,
      localShadowOnly: false,
      inventoryLane,
      inventoryLaneLabel,
      inventorySalesReady: isSalesReady,
      accountDevelopmentPublished: isAccountDevelopment,
      prospectCommercialClassLabel: isSalesReady
        ? "Sales Ready"
        : isAccountDevelopment
          ? "Account Development"
          : null,
      // Keep presentation aliases aligned so drawer builders / DTOs cannot fall
      // through to generic association procurement templates.
      recommendedNextStep: certifiedNext || snap.recommendedNextStep || null,
      recommendedAction: certifiedNext || snap.recommendedAction || null,
      recommendedNextAction: certifiedNext || null,
      sources,
      evidenceUrls: Array.isArray(snap.evidenceUrls)
        ? snap.evidenceUrls
        : existing?.evidenceUrls || [],
    };
    // Prefer non-empty commercial narrative from either existing research or
    // the durable snapshot so lean historical snaps cannot wipe richer overlays
    // and enriched snaps can stand alone without local-shadow.
    const preferText = (a, b) => {
      const as = String(a || "").replace(/\s+/g, " ").trim();
      const bs = String(b || "").replace(/\s+/g, " ").trim();
      if (as && (!bs || as.length >= bs.length)) return a;
      return b || a || null;
    };
    const preferList = (a, b) =>
      Array.isArray(a) && a.length ? a : Array.isArray(b) && b.length ? b : a || b || null;
    const preferObj = (a, b) =>
      a && typeof a === "object" && (a.name || Object.keys(a).length)
        ? a
        : b && typeof b === "object"
          ? b
          : a || b || null;
    const merged = {
      ...mergedRaw,
      whatIsHappening: preferText(snap.whatIsHappening, existing?.whatIsHappening),
      summaryWhat: preferText(snap.summaryWhat || snap.whatIsHappening, existing?.summaryWhat),
      potentialHotelBusiness: preferText(
        snap.potentialHotelBusiness || snap.potentialBusiness,
        existing?.potentialHotelBusiness || existing?.potentialBusiness,
      ),
      potentialBusiness: preferText(
        snap.potentialBusiness || snap.potentialHotelBusiness,
        existing?.potentialBusiness || existing?.potentialHotelBusiness,
      ),
      whyThisHotel: preferText(snap.whyThisHotel, existing?.whyThisHotel),
      summaryWhyHotel: preferText(
        snap.summaryWhyHotel || snap.whyThisHotel,
        existing?.summaryWhyHotel,
      ),
      whyActNow: preferText(
        snap.whyActNow || snap.whyNow,
        existing?.whyActNow || existing?.whyNow,
      ),
      whyNow: preferText(
        snap.whyNow || snap.whyActNow,
        existing?.whyNow || existing?.whyActNow,
      ),
      establishedFacts: preferList(snap.establishedFacts, existing?.establishedFacts),
      uncertainties: preferList(snap.uncertainties, existing?.uncertainties),
      qualificationQuestions: preferList(
        snap.qualificationQuestions,
        existing?.qualificationQuestions,
      ),
      primaryContact: preferObj(snap.primaryContact, existing?.primaryContact),
      secondaryContact: preferObj(snap.secondaryContact, existing?.secondaryContact),
      commercialIntelligenceBrief: preferObj(
        snap.commercialIntelligenceBrief || snap.salesBrief,
        existing?.commercialIntelligenceBrief || existing?.salesBrief,
      ),
      salesBrief: preferObj(
        snap.salesBrief || snap.commercialIntelligenceBrief,
        existing?.salesBrief || existing?.commercialIntelligenceBrief,
      ),
    };
    byId.set(row.opportunityId, merged);
  }

  // Drop revoked-only? listPublished already excludes revoked.
  // Optional: hide research-only rows not in published set when strict mode on.
  if (opts.strictPublishedOnly === true) {
    const allow = new Set(published.map((r) => r.opportunityId));
    return [...byId.values()].filter((o) => allow.has(String(o.id)));
  }

  return [...byId.values()];
}
