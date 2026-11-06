/**
 * verifyGdiHotelFullContentParity(hotelId, options)
 *
 * Universal hotel-scoped full-content parity verifier.
 * Reuses Spice-proven freeze / hydrate / EXTERNAL_SHARE DTO / recursive compare.
 *
 * Does not publish, rotate share tokens, or write Airtable.
 */
import {
  createGdiPublishedInventoryRegistry,
} from "../durable-publication/gdi-published-inventory-registry-v1.js";
import { hydrateOpportunityFromPublishedCommercialFields } from "../durable-publication/gdi-customer-facing-published-snapshot-v1.js";
import { projectOpportunityForPresentation } from "./gdi-universal-presentation-pipeline-v1.js";
import {
  GDI_PRESENTATION_SURFACES,
} from "../contracts/gdi-presentation-contract-v1.js";
import {
  compareApprovedFullContent,
  assertContactMultiplicity,
} from "./gdi-full-content-parity-v1.js";
import {
  assertApprovedCustomerFacingDetailPresent,
  GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
  GDI_FULL_CONTENT_PARITY_STATUS,
} from "../contracts/gdi-canonical-customer-presentation-contract-v1.js";
import { readGdiShareRegistry } from "../share/gdi-signed-share-capability-v1.js";

export const GDI_HOTEL_FULL_CONTENT_PARITY_VERIFY_V1 =
  "gdi-hotel-full-content-parity-verify-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
  return res.json();
}

function activeShareForHotel(hotelId) {
  try {
    const reg = readGdiShareRegistry();
    const tokens = reg?.tokens || {};
    const active = Object.values(tokens).filter(
      (row) =>
        String(row?.hotelId || "") === String(hotelId) &&
        String(row?.status || "").toUpperCase() === "ACTIVE",
    );
    if (!active.length) return null;
    // Prefer non-test labels; otherwise first ACTIVE.
    const preferred =
      active.find((r) => !/test/i.test(String(r.label || ""))) || active[0];
    return {
      tokenId: preferred.tokenId || preferred.tid || null,
      label: preferred.label || null,
      status: preferred.status,
    };
  } catch {
    return null;
  }
}

function loadPublishedRows(hotelId, opts = {}) {
  const registry =
    opts.registry ||
    createGdiPublishedInventoryRegistry({
      env: opts.env || process.env,
      root: opts.root,
    });
  const published = registry.listPublished(hotelId) || [];
  const publication =
    registry.loadRegistry()?.hotels?.[hotelId]?.currentPublication || null;
  const rows = [];
  for (const row of published) {
    const doc = registry.loadSnapshot(
      hotelId,
      row.opportunityId,
      row.publishedVersion,
    );
    const snap = doc?.snapshot || null;
    rows.push({
      opportunityId: row.opportunityId,
      publishedVersion: row.publishedVersion,
      classification: row.commercialClassification,
      snapshot: snap,
      contentHash: doc?.contentHash || row.contentHash || null,
    });
  }
  return { registry, publication, rows };
}

/**
 * Pre-pointer gate: verify staged snapshot objects before registry advance.
 * Used by one-click publish so a failed full-content check never activates.
 */
export function verifyStagedPublicationFullContentParity(
  hotelId,
  stagedRows = [],
) {
  const results = [];
  const criticalDifferences = [];
  for (const snap of stagedRows || []) {
    const opportunityId = snap?.id || snap?.opportunityId || null;
    const row = {
      opportunityId,
      snapshot: snap,
      publishedVersion: snap?.publishedVersion || null,
    };
    const dtoVerify = verifySnapshotToShareDto(hotelId, row);
    results.push(dtoVerify);
    if (!dtoVerify.ok) {
      for (const d of dtoVerify.diffs || []) {
        criticalDifferences.push({
          hotelId,
          opportunityId,
          fieldPath: d.path,
          approvedValue: d.left,
          publishedValue: d.right,
          kind: d.kind,
          stage: dtoVerify.stage,
          rootCause:
            dtoVerify.stage === "snapshot_schema"
              ? "LEGACY_OR_INCOMPLETE_SNAPSHOT"
              : "FULL_CONTENT_MISMATCH",
          requiredCorrection:
            "Do not activate publication. Repair approvedCustomerFacingDetail freeze.",
        });
      }
    }
  }
  const ok = results.length > 0 && results.every((r) => r.ok === true);
  return {
    ok,
    stage: "staged_pre_pointer",
    hotelId,
    opportunityCount: results.length,
    results,
    criticalDifferences,
  };
}

/**
 * Compare frozen approved detail to EXTERNAL_SHARE detail DTO projection.
 */
function verifySnapshotToShareDto(hotelId, snapRow) {
  const snap = snapRow.snapshot;
  const schemaGate = assertApprovedCustomerFacingDetailPresent(snap || {});
  if (!schemaGate.ok) {
    return {
      ok: false,
      stage: "snapshot_schema",
      opportunityId: snapRow.opportunityId,
      schemaGate,
      diffs: schemaGate.fails.map((f) => ({
        path: f,
        kind: "missing_on_right",
        left: "required",
        right: null,
      })),
    };
  }

  const hydrated = hydrateOpportunityFromPublishedCommercialFields({
    ...snap,
    id: snap.id || snapRow.opportunityId,
    hotelId,
    publishedPresentationLocked: true,
    gdiPublishedInventoryRegistry: true,
  });
  const projected = projectOpportunityForPresentation(hydrated, {
    hotelId,
    surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
    view: "detail",
  });
  const dto = projected.dto || {};
  if (!dto.approvedCustomerFacingDetail) {
    return {
      ok: false,
      stage: "external_share_dto",
      opportunityId: snapRow.opportunityId,
      diffs: [
        {
          path: "approvedCustomerFacingDetail",
          kind: "missing_on_right",
          left: "present",
          right: null,
        },
      ],
    };
  }

  const cmp = compareApprovedFullContent(hydrated, dto, { hotelId });
  const contacts = assertContactMultiplicity(dto, { requireTwoContact: false });
  const fieldsCompared =
    cmp.schema?.pathCount ||
    cmp.schema?.paths?.length ||
    (Array.isArray(cmp.diffs) ? Math.max(cmp.diffCount || 0, 1) : 0);
  return {
    ok: cmp.ok === true && contacts.ok === true,
    stage: "snapshot_to_share_dto",
    opportunityId: snapRow.opportunityId,
    usedFrozenApprovedDetail: cmp.usedFrozenApprovedDetail === true,
    diffCount: cmp.diffCount || 0,
    fieldsCompared,
    diffs: (cmp.diffs || []).slice(0, 40),
    classification: cmp.classification || null,
    contacts,
    salesActionPillLabel: dto.salesActionPillLabel || null,
    primary: dto.primaryContact?.name || null,
    secondary: dto.secondaryContact?.name || null,
  };
}

/**
 * Optional live share detail compare (requires share JWT capability string).
 */
async function verifyLiveShareDetail(hotelId, snapRow, opts = {}) {
  const share = opts.shareToken || opts.share || null;
  const base = opts.prodBase || opts.baseUrl || null;
  if (!share || !base) {
    return { ok: null, skipped: true, reason: "share_or_base_missing" };
  }
  const id = snapRow.opportunityId;
  const url = `${base}/api/group-demand-intelligence/share/hotels/${encodeURIComponent(
    hotelId,
  )}/opportunities/${encodeURIComponent(id)}?share=${encodeURIComponent(share)}`;
  try {
    const data = await fetchJson(url);
    const live = data.opportunity || data;
    if (!live?.approvedCustomerFacingDetail) {
      return {
        ok: false,
        stage: "live_share_detail",
        opportunityId: id,
        diffs: [
          {
            path: "approvedCustomerFacingDetail",
            kind: "missing_on_right",
            left: "present_on_snapshot",
            right: null,
          },
        ],
      };
    }
    const hydrated = hydrateOpportunityFromPublishedCommercialFields({
      ...snapRow.snapshot,
      id,
      hotelId,
      publishedPresentationLocked: true,
      gdiPublishedInventoryRegistry: true,
    });
    const cmp = compareApprovedFullContent(hydrated, live, { hotelId });
    return {
      ok: cmp.ok === true,
      stage: "live_share_detail",
      opportunityId: id,
      usedFrozenApprovedDetail: cmp.usedFrozenApprovedDetail === true,
      diffCount: cmp.diffCount || 0,
      diffs: (cmp.diffs || []).slice(0, 40),
      liveAction: live.salesActionPillLabel || null,
      livePrimary: live.primaryContact?.name || null,
      liveSecondary: live.secondaryContact?.name || null,
    };
  } catch (err) {
    return {
      ok: false,
      stage: "live_share_detail",
      opportunityId: id,
      error: String(err?.message || err),
    };
  }
}

/**
 * @param {string} hotelId
 * @param {object} [options]
 * @param {string} [options.root] published-inventory root
 * @param {string} [options.prodBase] live production base URL
 * @param {string} [options.shareToken] full gdishare.v1.… capability (not tokenId)
 * @param {boolean} [options.liveShare=false] fetch live share details when shareToken set
 * @param {string} [options.hotelName]
 */
export async function verifyGdiHotelFullContentParity(hotelId, options = {}) {
  const id = text(hotelId);
  if (!id) {
    return {
      ok: false,
      status: GDI_FULL_CONTENT_PARITY_STATUS.MISMATCH,
      code: "HOTEL_ID_REQUIRED",
      contractVersion: GDI_HOTEL_FULL_CONTENT_PARITY_VERIFY_V1,
    };
  }

  const shareMeta = activeShareForHotel(id);
  const { publication, rows } = loadPublishedRows(id, options);

  if (!publication || !rows.length) {
    return {
      ok: false,
      status: GDI_FULL_CONTENT_PARITY_STATUS.NOT_PUBLISHED,
      contractVersion: GDI_HOTEL_FULL_CONTENT_PARITY_VERIFY_V1,
      canonicalSchemaVersion: GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
      hotelId: id,
      hotelName: options.hotelName || null,
      approvedOpportunityCount: null,
      publishedOpportunityCount: 0,
      publicationVersion: null,
      publicationId: null,
      shareStatus: shareMeta ? "ACTIVE" : "NONE",
      shareTokenId: shareMeta?.tokenId || null,
      opportunities: [],
      criticalDifferences: [],
      fieldsCompared: 0,
      verifiedAt: new Date().toISOString(),
    };
  }

  // Published but no share capability string supplied and no way to verify live
  // still allows offline snapshot→DTO verification. Live share absence is
  // NOT_VERIFIABLE only when caller requires liveShare and no token.
  if (options.requireLiveShare === true && !options.shareToken) {
    return {
      ok: false,
      status: GDI_FULL_CONTENT_PARITY_STATUS.NOT_VERIFIABLE,
      contractVersion: GDI_HOTEL_FULL_CONTENT_PARITY_VERIFY_V1,
      canonicalSchemaVersion: GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
      hotelId: id,
      hotelName: options.hotelName || null,
      publishedOpportunityCount: rows.length,
      publicationVersion: publication?.oneClickPublishVersion || null,
      publicationId: publication?.publicationId || null,
      shareStatus: shareMeta ? "ACTIVE_TOKEN_ID_ONLY" : "NONE",
      shareTokenId: shareMeta?.tokenId || null,
      reason:
        "Live share verification required but share JWT capability was not provided (tokenId alone is insufficient).",
      opportunities: [],
      criticalDifferences: [],
      fieldsCompared: 0,
      verifiedAt: new Date().toISOString(),
    };
  }

  const opportunityResults = [];
  const criticalDifferences = [];
  let fieldsCompared = 0;

  for (const row of rows) {
    const dtoVerify = verifySnapshotToShareDto(id, row);
    fieldsCompared += dtoVerify.fieldsCompared || dtoVerify.diffCount || 0;
    let liveVerify = { skipped: true };
    if (options.liveShare === true && options.shareToken) {
      liveVerify = await verifyLiveShareDetail(id, row, options);
      if (liveVerify && liveVerify.diffCount) {
        fieldsCompared += liveVerify.diffCount;
      }
    }
    const ok =
      dtoVerify.ok === true &&
      (liveVerify.skipped || liveVerify.ok === true);
    if (!ok) {
      for (const d of dtoVerify.diffs || []) {
        criticalDifferences.push({
          hotelId: id,
          opportunityId: row.opportunityId,
          fieldPath: d.path,
          approvedValue: d.left,
          publishedValue: d.right,
          kind: d.kind,
          stage: dtoVerify.stage,
          rootCause:
            dtoVerify.stage === "external_share_dto"
              ? "EXTERNAL_SHARE_DTO_DROPPED_APPROVED_DETAIL"
              : dtoVerify.stage === "snapshot_schema"
                ? "LEGACY_OR_INCOMPLETE_SNAPSHOT"
                : "FULL_CONTENT_MISMATCH",
          requiredCorrection:
            dtoVerify.stage === "snapshot_schema"
              ? "Republish hotel through one-click full-content freeze (authorized)."
              : "Ensure EXTERNAL_SHARE detail preserves approvedCustomerFacingDetail; do not rebuild from list DTO.",
        });
      }
      for (const d of liveVerify.diffs || []) {
        criticalDifferences.push({
          hotelId: id,
          opportunityId: row.opportunityId,
          fieldPath: d.path,
          approvedValue: d.left,
          publishedValue: d.right,
          kind: d.kind,
          stage: liveVerify.stage,
          rootCause: "LIVE_SHARE_MISMATCH",
          requiredCorrection:
            "Redeploy presentation code or republish complete approved snapshots.",
        });
      }
    }
    opportunityResults.push({
      opportunityId: row.opportunityId,
      publishedVersion: row.publishedVersion,
      ok,
      dtoVerify,
      liveVerify,
    });
  }

  const allOk = opportunityResults.every((r) => r.ok);
  const legacy = opportunityResults.some(
    (r) => r.dtoVerify?.stage === "snapshot_schema" && r.ok === false,
  );

  return {
    ok: allOk,
    status: allOk
      ? GDI_FULL_CONTENT_PARITY_STATUS.VERIFIED
      : GDI_FULL_CONTENT_PARITY_STATUS.MISMATCH,
    contractVersion: GDI_HOTEL_FULL_CONTENT_PARITY_VERIFY_V1,
    canonicalSchemaVersion: GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
    hotelId: id,
    hotelName: options.hotelName || null,
    publishedOpportunityCount: rows.length,
    publicationVersion:
      publication?.oneClickPublishVersion ||
      publication?.presentationSchemaVersion ||
      null,
    publicationId: publication?.publicationId || null,
    publishedAt: publication?.publishedAt || null,
    manifestHash: publication?.manifestHash || null,
    shareStatus: shareMeta ? "ACTIVE" : "NONE",
    shareTokenId: shareMeta?.tokenId || null,
    legacySnapshotsMissingApprovedDetail: legacy,
    opportunities: opportunityResults,
    criticalDifferences,
    fieldsCompared,
    verifiedAt: new Date().toISOString(),
  };
}

export function summarizeHotelParityStatus(verifyResult) {
  if (!verifyResult) {
    return GDI_FULL_CONTENT_PARITY_STATUS.VERIFICATION_PENDING;
  }
  return (
    verifyResult.status ||
    (verifyResult.ok
      ? GDI_FULL_CONTENT_PARITY_STATUS.VERIFIED
      : GDI_FULL_CONTENT_PARITY_STATUS.MISMATCH)
  );
}
