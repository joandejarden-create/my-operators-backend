/**
 * Portfolio discovery + aggregate full-content parity audit.
 * Discovers hotels from selectable GDI hotels ∪ published registry ∪ share registry.
 * Never invents a manually maintained shortlist.
 */
import { listGdiSelectableHotels } from "../repository.js";
import { createGdiPublishedInventoryRegistry } from "../durable-publication/gdi-published-inventory-registry-v1.js";
import { readGdiShareRegistry } from "../share/gdi-signed-share-capability-v1.js";
import {
  GDI_FULL_CONTENT_PARITY_STATUS,
  GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
} from "../contracts/gdi-canonical-customer-presentation-contract-v1.js";
import {
  verifyGdiHotelFullContentParity,
  GDI_HOTEL_FULL_CONTENT_PARITY_VERIFY_V1,
} from "./gdi-hotel-full-content-parity-verify-v1.js";

export const GDI_PORTFOLIO_FULL_CONTENT_PARITY_V1 =
  "gdi-portfolio-full-content-parity-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Discover the complete GDI hotel universe for parity inventory.
 */
export function discoverGdiParityHotelUniverse(opts = {}) {
  const byId = new Map();

  const upsert = (hotelId, patch = {}) => {
    const id = text(hotelId);
    if (!id) return;
    const prev = byId.get(id) || {
      hotelId: id,
      hotelName: null,
      sources: [],
    };
    // Prefer stable hotel display names over share-token labels.
    const nextName =
      prev.hotelName && !/^(hotel4-iso|cross-market|test|production-distribution|hilton-development)/i.test(
        String(prev.hotelName),
      )
        ? prev.hotelName
        : patch.hotelName || prev.hotelName || null;
    byId.set(id, {
      ...prev,
      ...patch,
      hotelId: id,
      hotelName: nextName,
      sources: [...new Set([...(prev.sources || []), ...(patch.sources || [])])],
    });
  };

  let selectable = opts.hotels;
  if (!selectable) {
    if (typeof opts.listHotels === "function") {
      selectable = opts.listHotels();
    } else {
      try {
        selectable = listGdiSelectableHotels() || [];
      } catch {
        selectable = [];
      }
    }
  }
  for (const h of selectable || []) {
    const id = h.hotelId || h.id;
    upsert(id, {
      hotelName: h.displayName || h.hotelName || h.name || null,
      sources: ["selectable_hotels"],
    });
  }

  const registry =
    opts.registry ||
    createGdiPublishedInventoryRegistry({
      env: opts.env || process.env,
      root: opts.root,
    });
  const regDoc = registry.loadRegistry?.() || { hotels: {} };
  for (const [hotelId, hotel] of Object.entries(regDoc.hotels || {})) {
    upsert(hotelId, {
      hotelName: hotel.hotelName || hotel.displayName || null,
      sources: ["published_registry"],
      publishedOpportunityCount:
        hotel.currentPublication?.opportunityCount ||
        hotel.currentPublication?.opportunityIds?.length ||
        0,
      publicationId: hotel.currentPublication?.publicationId || null,
      publicationVersion:
        hotel.currentPublication?.oneClickPublishVersion || null,
      publishedAt: hotel.currentPublication?.publishedAt || null,
      manifestHash: hotel.currentPublication?.manifestHash || null,
    });
  }

  try {
    const shareReg = opts.shareRegistry || readGdiShareRegistry();
    for (const row of Object.values(shareReg?.tokens || {})) {
      if (String(row?.status || "").toUpperCase() !== "ACTIVE") continue;
      upsert(row.hotelId, {
        hotelName: row.hotelName || row.label || null,
        sources: ["active_share_registry"],
        hasActiveShare: true,
        shareTokenId: row.tokenId || row.tid || null,
      });
    }
  } catch {
    /* share registry optional */
  }

  return [...byId.values()].sort((a, b) =>
    String(a.hotelName || a.hotelId).localeCompare(
      String(b.hotelName || b.hotelId),
    ),
  );
}

/**
 * Build inventory rows (read-only classification) without live deep verify.
 */
export function buildGdiParityHotelInventory(opts = {}) {
  const universe = discoverGdiParityHotelUniverse(opts);
  const registry =
    opts.registry ||
    createGdiPublishedInventoryRegistry({
      env: opts.env || process.env,
      root: opts.root,
    });

  return universe.map((h) => {
    const published = registry.listPublished?.(h.hotelId) || [];
    const pub =
      registry.loadRegistry?.()?.hotels?.[h.hotelId]?.currentPublication || null;
    const publishedCount =
      published.length ||
      pub?.opportunityCount ||
      pub?.opportunityIds?.length ||
      0;
    let schemaReadyCount = 0;
    let legacyCount = 0;
    for (const row of published) {
      const doc = registry.loadSnapshot?.(
        h.hotelId,
        row.opportunityId,
        row.publishedVersion,
      );
      const snap = doc?.snapshot;
      if (snap?.approvedCustomerFacingDetail) schemaReadyCount += 1;
      else if (snap) legacyCount += 1;
    }

    let fullContentParityStatus = GDI_FULL_CONTENT_PARITY_STATUS.NOT_PUBLISHED;
    const shareStatus = h.hasActiveShare ? "ACTIVE" : "NONE";

    if (publishedCount > 0) {
      if (legacyCount > 0 && schemaReadyCount === 0) {
        // Published under legacy snapshots — migration candidate, not production PASS.
        fullContentParityStatus = GDI_FULL_CONTENT_PARITY_STATUS.MISMATCH;
      } else if (legacyCount > 0) {
        fullContentParityStatus = GDI_FULL_CONTENT_PARITY_STATUS.MISMATCH;
      } else {
        fullContentParityStatus =
          GDI_FULL_CONTENT_PARITY_STATUS.VERIFICATION_PENDING;
      }
    } else if (h.hasActiveShare) {
      // Active customer share without durable registry publication under the
      // canonical contract — cannot claim production full-content parity.
      fullContentParityStatus = GDI_FULL_CONTENT_PARITY_STATUS.NOT_VERIFIABLE;
    }

    return {
      hotelId: h.hotelId,
      hotelName: h.hotelName,
      approvedOpportunityCount: null,
      publishedOpportunityCount: publishedCount,
      publicationVersion:
        pub?.oneClickPublishVersion || h.publicationVersion || null,
      publicationId: pub?.publicationId || h.publicationId || null,
      publishedAt: pub?.publishedAt || h.publishedAt || null,
      canonicalSchemaVersion: GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
      shareStatus,
      shareTokenId: h.shareTokenId || null,
      fullContentParityStatus,
      schemaReadySnapshotCount: schemaReadyCount,
      legacySnapshotCount: legacyCount,
      migrationCandidate:
        publishedCount === 0 && h.hasActiveShare === true
          ? "LEGACY_SHARE_WITHOUT_CANONICAL_REGISTRY"
          : legacyCount > 0
            ? "LEGACY_SNAPSHOTS_MISSING_APPROVED_DETAIL"
            : null,
      discoverySources: h.sources,
    };
  });
}

/**
 * Run hotel verify across published inventory rows.
 */
export async function runGdiPortfolioFullContentParityAudit(opts = {}) {
  const inventory = buildGdiParityHotelInventory(opts);
  const hotelResults = [];
  let totalOpportunitiesVerified = 0;
  let totalFieldsCompared = 0;
  let totalCriticalDifferences = 0;

  for (const row of inventory) {
    if (row.publishedOpportunityCount <= 0) {
      hotelResults.push({
        ...row,
        verify: {
          ok: false,
          status: row.fullContentParityStatus,
          skipped: true,
        },
      });
      continue;
    }

    const shareOpts = opts.shareTokensByHotelId?.[row.hotelId] || null;
    const verify = await verifyGdiHotelFullContentParity(row.hotelId, {
      root: opts.root,
      env: opts.env,
      hotelName: row.hotelName,
      liveShare: Boolean(shareOpts && opts.liveShare),
      shareToken: shareOpts || undefined,
      prodBase: opts.prodBase,
      requireLiveShare: opts.requireLiveShare === true,
      registry: opts.registry,
    });

    totalOpportunitiesVerified += verify.publishedOpportunityCount || 0;
    totalFieldsCompared += verify.fieldsCompared || 0;
    totalCriticalDifferences += (verify.criticalDifferences || []).length;

    hotelResults.push({
      ...row,
      fullContentParityStatus: verify.status,
      verify,
    });
  }

  const publishedHotels = hotelResults.filter(
    (h) => h.publishedOpportunityCount > 0,
  );
  const verifiedHotels = hotelResults.filter(
    (h) => h.fullContentParityStatus === GDI_FULL_CONTENT_PARITY_STATUS.VERIFIED,
  );
  const mismatchHotels = hotelResults.filter(
    (h) => h.fullContentParityStatus === GDI_FULL_CONTENT_PARITY_STATUS.MISMATCH,
  );
  const notPublishedHotels = hotelResults.filter(
    (h) =>
      h.fullContentParityStatus === GDI_FULL_CONTENT_PARITY_STATUS.NOT_PUBLISHED,
  );
  const notVerifiableHotels = hotelResults.filter(
    (h) =>
      h.fullContentParityStatus ===
      GDI_FULL_CONTENT_PARITY_STATUS.NOT_VERIFIABLE,
  );
  const pendingHotels = hotelResults.filter(
    (h) =>
      h.fullContentParityStatus ===
      GDI_FULL_CONTENT_PARITY_STATUS.VERIFICATION_PENDING,
  );

  // Portfolio PASS only when every published+verifiable hotel is VERIFIED.
  // Unpublished / NOT_VERIFIABLE are reported separately and do not block PASS,
  // but MISMATCH or pending among published hotels do.
  const publishedVerifiable = publishedHotels.filter(
    (h) =>
      h.fullContentParityStatus !==
      GDI_FULL_CONTENT_PARITY_STATUS.NOT_VERIFIABLE,
  );
  const portfolioPass =
    publishedVerifiable.length > 0 &&
    publishedVerifiable.every(
      (h) =>
        h.fullContentParityStatus === GDI_FULL_CONTENT_PARITY_STATUS.VERIFIED,
    ) &&
    mismatchHotels.length === 0;

  return {
    contractVersion: GDI_PORTFOLIO_FULL_CONTENT_PARITY_V1,
    verifyContractVersion: GDI_HOTEL_FULL_CONTENT_PARITY_VERIFY_V1,
    canonicalSchemaVersion: GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
    status: portfolioPass
      ? "GDI_UNIVERSAL_PORTFOLIO_FULL_CONTENT_PARITY_PASS"
      : "GDI_UNIVERSAL_PORTFOLIO_FULL_CONTENT_PARITY_INCOMPLETE",
    generatedAt: new Date().toISOString(),
    summary: {
      TOTAL_GDI_HOTELS: hotelResults.length,
      PUBLISHED_HOTELS: publishedHotels.length,
      VERIFIED_HOTELS: verifiedHotels.length,
      MISMATCH_HOTELS: mismatchHotels.length,
      NOT_PUBLISHED_HOTELS: notPublishedHotels.length,
      NOT_VERIFIABLE_HOTELS: notVerifiableHotels.length,
      VERIFICATION_PENDING_HOTELS: pendingHotels.length,
      TOTAL_OPPORTUNITIES_VERIFIED: totalOpportunitiesVerified,
      TOTAL_FIELDS_COMPARED: totalFieldsCompared,
      TOTAL_CRITICAL_DIFFERENCES: totalCriticalDifferences,
    },
    hotels: hotelResults,
    mismatches: hotelResults.flatMap((h) => h.verify?.criticalDifferences || []),
  };
}
