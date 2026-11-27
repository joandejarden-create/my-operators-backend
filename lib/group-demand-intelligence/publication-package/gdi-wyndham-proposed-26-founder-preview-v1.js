/**
 * Read-only founder preview of Wyndham locked proposed write-set (26)
 * as GDI opportunity rows for List/Tiles/drawer.
 *
 * Does NOT activate STAGED, publish CURRENT, or write Airtable.
 * Contract: FOUNDER_PROPOSED_WRITE_SET_PREVIEW — not customer CURRENT.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "./index.js";
import {
  loadWyndhamLockedProposedWriteSet,
  buildWyndhamProposedWriteSetFounderReview,
} from "./gdi-proposed-write-set-founder-review-v1.js";

export const GDI_WYNDHAM_PROPOSED_26_FOUNDER_PREVIEW_V1 =
  "gdi-wyndham-proposed-26-founder-preview-v1";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

export const WYNDHAM_HOTEL_ID = "recnPB3Q02NO8Ma1o";

const DOSSIER_DIR = path.join(
  ROOT,
  "reports/gdi-wyndham-commercial-full-e2e-queue-20usd-v1/dossiers",
);
const TURNER_ARTIFACT = path.join(
  ROOT,
  "reports/gdi-wyndham-sd-bayside-paid-live-cert-25usd-v1/agent-deep-continuation/disc_75ee8629955e.json",
);
const PKG59_VERSION = "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_fb10174d";
const UNIFIED_BUCKETS = path.join(
  ROOT,
  "reports/gdi-wyndham-final-commercial-completion-v1/07-UNIFIED_INVENTORY_BUCKETS.json",
);

function text(v) {
  return String(v == null ? "" : v).trim();
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function packageRowMap(hotelId, publicationVersion) {
  const store = createGdiPublicationPackageStore({});
  const loaded = store.loadPackage(hotelId, publicationVersion);
  if (!loaded?.ok) return new Map();
  const opps = Array.isArray(loaded.package?.opportunities)
    ? loaded.package.opportunities
    : [];
  return new Map(
    opps
      .map((o) => [text(o?.id || o?.opportunityId), o])
      .filter(([id]) => id),
  );
}

function stagedRowMap(hotelId) {
  const store = createGdiPublicationPackageStore({});
  const pointer = store.readPointer(hotelId, "STAGED");
  if (!pointer?.publicationVersion) return { map: new Map(), pointer: null };
  return {
    map: packageRowMap(hotelId, pointer.publicationVersion),
    pointer,
  };
}

function slimFromRaw(raw, meta = {}) {
  const id = text(raw?.id || raw?.opportunityId || meta.opportunityId);
  const org = text(
    raw?.organizationName || raw?.organization || meta.org || "",
  );
  const title = text(
    raw?.displayTitle ||
      raw?.title ||
      raw?.programmeName ||
      raw?.demandTitle ||
      org ||
      id,
  );
  const contactName = text(
    raw?.primaryContact?.name ||
      raw?.tileContactPreview?.name ||
      meta.contact ||
      "",
  );
  const contactRole = text(
    raw?.primaryContact?.role ||
      raw?.tileContactPreview?.role ||
      meta.role ||
      "",
  );
  const priority = text(raw?.priority || "MEDIUM") || "MEDIUM";
  return {
    id,
    opportunityId: id,
    hotelId: WYNDHAM_HOTEL_ID,
    title,
    displayTitle: title,
    organizationName: org,
    priority,
    priorityFilterKey:
      priority === "HIGH"
        ? "HIGH_PRIORITY"
        : priority === "WATCH"
          ? "WATCH"
          : "MEDIUM_PRIORITY",
    demandType: text(raw?.demandType || meta.segment || "") || null,
    summaryWhat: text(
      raw?.summaryWhat || raw?.whatIsHappening || raw?.hotelOpportunityThesis || "",
    ),
    whatIsHappening: text(raw?.whatIsHappening || raw?.summaryWhat || ""),
    primaryContact: contactName
      ? {
          name: contactName,
          role: contactRole || null,
          email: raw?.primaryContact?.email || null,
          phone: raw?.primaryContact?.phone || null,
        }
      : null,
    tileContactPreview: contactName
      ? { name: contactName, role: contactRole || null }
      : null,
    primaryCommercialEligible: true,
    commercialSurfaceDisposition: "COMMERCIAL_OPPORTUNITY",
    founderProposedPreview: true,
    customerFacing: false,
    founderReviewBucket: meta.bucket || null,
    founderVisibilityState: meta.visibilityState || null,
    publicationStatus: meta.publicationStatus || "NOT_PUBLISHED",
    _publicationPackageId: raw?._publicationPackageId || null,
    _publicationVersion: raw?._publicationVersion || null,
    _founderPreviewSource: meta.hydrationSource || null,
  };
}

function hydrateOne(row, stagedMap, pkg59Map) {
  const id = text(row.opportunityId);
  const meta = {
    opportunityId: id,
    org: row.org,
    contact: row.contact,
    role: row.role,
    segment: row.segment,
    bucket: row.bucket,
    visibilityState: row.visibilityState,
    publicationStatus: row.publicationStatus,
  };

  if (stagedMap.has(id)) {
    return slimFromRaw(
      { ...stagedMap.get(id), id },
      { ...meta, hydrationSource: "STAGED_CLEAN11" },
    );
  }
  if (pkg59Map.has(id)) {
    return slimFromRaw(
      { ...pkg59Map.get(id), id },
      { ...meta, hydrationSource: "HISTORICAL_PKG59_READ_ONLY" },
    );
  }
  const dossierPath = path.join(DOSSIER_DIR, `${id}.json`);
  if (fs.existsSync(dossierPath)) {
    const doc = readJson(dossierPath);
    const opp = doc.opportunity || doc;
    return slimFromRaw(
      {
        ...opp,
        id,
        title: opp.title || opp.programmeName || meta.org,
        organizationName: opp.organizationName || meta.org,
        primaryContact:
          opp.primaryContact ||
          (meta.contact
            ? { name: meta.contact, role: meta.role || null }
            : null),
      },
      { ...meta, hydrationSource: "QUEUE_DOSSIER" },
    );
  }
  if (id === "disc_75ee8629955e" && fs.existsSync(TURNER_ARTIFACT)) {
    const doc = readJson(TURNER_ARTIFACT);
    const opp = doc.opportunity || doc;
    return slimFromRaw(
      {
        ...opp,
        id,
        title: doc.title || opp.title || meta.org,
        organizationName: doc.organizationName || meta.org,
        primaryContact: {
          name: meta.contact || "David Aguirre",
          role: meta.role || "Project Manager",
        },
      },
      { ...meta, hydrationSource: "PAID_CERT_CONTINUATION_ARTIFACT" },
    );
  }

  // Fail-closed placeholder — still listable for founder review identity.
  return slimFromRaw(
    {
      id,
      title: meta.org || id,
      organizationName: meta.org || null,
      primaryContact: meta.contact
        ? { name: meta.contact, role: meta.role || null }
        : null,
      priority: "MEDIUM",
    },
    { ...meta, hydrationSource: "METADATA_ONLY_GAP" },
  );
}

/**
 * @param {{ bucket?: 'proposed'|'hold'|'continuation' }} opts
 */
export function resolveWyndhamFounderInventoryPreview(opts = {}) {
  const bucket = text(opts.bucket || "proposed").toLowerCase() || "proposed";
  const review = buildWyndhamProposedWriteSetFounderReview({});
  if (!review.ok) {
    return { ok: false, error: review.error || "REVIEW_UNAVAILABLE", review };
  }

  const { map: stagedMap, pointer: stagedPointer } = stagedRowMap(
    WYNDHAM_HOTEL_ID,
  );
  const pkg59Map = packageRowMap(WYNDHAM_HOTEL_ID, PKG59_VERSION);

  if (bucket === "hold") {
    const opportunities = (review.hold || []).map((row) =>
      hydrateOne(
        { ...row, visibilityState: "HOLD_NOT_QUALIFIED" },
        stagedMap,
        pkg59Map,
      ),
    );
    return {
      ok: true,
      mode: "PACKAGE",
      opportunities,
      count: opportunities.length,
      meta: {
        previewSource: "FOUNDER_HOLD_REVIEW_NOT_QUALIFIED",
        listScope: "FOUNDER_HOLD_REVIEW",
        hotelId: WYNDHAM_HOTEL_ID,
        publicationVersion: null,
        stagedPublicationVersion: stagedPointer?.publicationVersion || null,
        governance: review.governance,
        contract: GDI_WYNDHAM_PROPOSED_26_FOUNDER_PREVIEW_V1,
        customerFacing: false,
        noStagedActivate: true,
        noCurrentPublish: true,
      },
    };
  }

  if (bucket === "continuation") {
    let topWho = [];
    try {
      if (fs.existsSync(UNIFIED_BUCKETS)) {
        topWho = readJson(UNIFIED_BUCKETS).topWhoRecovery || [];
      }
    } catch {
      topWho = [];
    }
    const opportunities = topWho.slice(0, 25).map((row) =>
      slimFromRaw(
        {
          id: text(row.id),
          title: row.title || row.org,
          organizationName: row.org,
          priority: "WATCH",
        },
        {
          opportunityId: text(row.id),
          org: row.org,
          bucket: "WHO_HOW_CONTINUATION",
          visibilityState: "RESEARCH_CONTINUATION_NOT_QUALIFIED",
          publicationStatus: "NOT_PUBLISHED",
          hydrationSource: "UNIFIED_BUCKETS_TOP_WHO",
        },
      ),
    );
    return {
      ok: true,
      mode: "PACKAGE",
      opportunities,
      count: opportunities.length,
      meta: {
        previewSource: "FOUNDER_CONTINUATION_REVIEW_NOT_QUALIFIED",
        listScope: "FOUNDER_CONTINUATION_REVIEW",
        hotelId: WYNDHAM_HOTEL_ID,
        publicationVersion: null,
        governance: {
          ...review.governance,
          includedAsQualified: false,
        },
        contract: GDI_WYNDHAM_PROPOSED_26_FOUNDER_PREVIEW_V1,
        customerFacing: false,
        note: "WHO/HOW continuation candidates — not auto-qualified",
      },
    };
  }

  // Default: locked proposed qualified 26
  const hydrationGaps = [];
  const opportunities = (review.proposedQualified || []).map((row) => {
    const opp = hydrateOne(row, stagedMap, pkg59Map);
    if (opp._founderPreviewSource === "METADATA_ONLY_GAP") {
      hydrationGaps.push(opp.id);
    }
    return opp;
  });

  return {
    ok: true,
    mode: "PACKAGE",
    opportunities,
    count: opportunities.length,
    meta: {
      previewSource: "PROPOSED_WRITE_SET_NOT_ACTIVATED",
      listScope: "FOUNDER_PROPOSED_WRITE_SET",
      hotelId: WYNDHAM_HOTEL_ID,
      publicationVersion: null,
      stagedPublicationVersion: stagedPointer?.publicationVersion || null,
      stagedPackageCount: review.counts?.stagedPackageCount ?? null,
      lockedProposedQualified: review.counts?.lockedProposedQualified ?? 26,
      proposedPresentInStaged: review.counts?.proposedPresentInStaged ?? null,
      proposedMissingFromStaged: review.counts?.proposedMissingFromStaged ?? null,
      governance: review.governance,
      contract: GDI_WYNDHAM_PROPOSED_26_FOUNDER_PREVIEW_V1,
      customerFacing: false,
      noStagedActivate: true,
      noCurrentPublish: true,
      hydrationGaps,
      rootCause: review.rootCause,
    },
  };
}

export function findWyndhamFounderPreviewOpportunity(opportunityId, opts = {}) {
  const id = text(opportunityId);
  if (!id) return { ok: false, error: "opportunityId_required" };
  for (const bucket of ["proposed", "hold", "continuation"]) {
    const resolved = resolveWyndhamFounderInventoryPreview({
      ...opts,
      bucket,
    });
    if (!resolved.ok) continue;
    const hit = (resolved.opportunities || []).find(
      (o) => text(o.id || o.opportunityId) === id,
    );
    if (hit) {
      return {
        ok: true,
        opportunity: hit,
        bucket,
        meta: resolved.meta,
      };
    }
  }
  return { ok: false, error: "not_found_in_founder_preview" };
}

export function loadWyndhamLockedIdsForTests() {
  return loadWyndhamLockedProposedWriteSet({});
}
