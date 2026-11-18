#!/usr/bin/env node
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import {
  loadPublishedEvidenceIndex,
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { resolveAdpMonthlyReviewEligibilityV1 } from "../lib/ai-demand-positioning/monthly-review/resolve-adp-monthly-review-eligibility-v1.js";
import { listInternalOwnerAppPropertyInventory } from "../lib/ai-demand-positioning/share/adp-owner-app-admin-inventory-v1.js";

const cov = buildPublishedAdpReviewCoverageV1({});
const needs = (cov.properties || []).filter(
  (p) => p.coverageStatus === "NEEDS_REBUILD"
);
const blocked = (cov.properties || []).filter((p) =>
  String(p.coverageStatus || "").startsWith("BLOCK")
);

const remainingNeedsRebuild = needs.map((p) => {
  const id = p.propertyId;
  const m = loadPublishedManifest(id);
  const r = loadPublishedReport(id);
  const e = loadPublishedEvidenceIndex(id);
  let elig;
  try {
    elig = resolveAdpMonthlyReviewEligibilityV1(id);
  } catch (err) {
    elig = { eligible: false, status: String(err.message || err).slice(0, 160) };
  }
  const hasEvidence = Boolean(e && e.available !== false);
  return {
    propertyId: id,
    name: p.propertyName || p.name,
    coverageReason: p.coverageReason,
    pdfStatus: p.pdfStatus || null,
    hasManifest: Boolean(m),
    manifestSource:
      m?._adpManifestSource ||
      (m?.durableStore ? "durable" : m ? "legacy_or_unknown" : null),
    period: m?.latestPeriodId || null,
    publishStatus: m?.publishStatus || null,
    hasReport: Boolean(r),
    hasEvidence,
    eligible: elig?.eligible === true,
    eligibilityStatus: elig?.status || null,
    reconstructionPossibleFromExistingEvidence: Boolean(
      m && r && elig?.eligible === true
    ),
    newResearchRunRequired: !(m && r),
  };
});

let inventory = null;
try {
  inventory = listInternalOwnerAppPropertyInventory({});
} catch (err) {
  inventory = { error: String(err.message || err).slice(0, 200) };
}
const rows = Array.isArray(inventory)
  ? inventory
  : inventory?.properties || inventory?.items || inventory?.hotels || [];
const ids = rows.map((x) => x.propertyId || x.id).filter(Boolean);
const calaIds = [
  "adp_sheraton_guadalajara_expo",
  "adp_wyndham_alltra_punta_cana",
  "adp_voco_cancun_zona_hotelera",
];

console.log(
  JSON.stringify(
    {
      publishedCount: cov.publishedCount,
      reviewReadyCount: cov.reviewReadyCount,
      needsRebuildCount: needs.length,
      blockedCount: blocked.length || cov.blockedCount,
      ownerInventoryCount: ids.length || inventory?.count || null,
      calaDropdown: calaIds.map((id) => ({
        id,
        inOwnerInventory: ids.includes(id),
      })),
      bethesdaPresent: ids.includes("adp_bethesda_marriott"),
      remainingNeedsRebuild,
      blocked: blocked.map((p) => ({
        propertyId: p.propertyId,
        reason: p.coverageReason,
      })),
    },
    null,
    2
  )
);
