/**
 * ADP_OWNER_APP_ADMIN_INVENTORY_V1
 *
 * Founder/admin owner-app property selector inventory.
 * Decoupled from customerDropdownVisible so internal workspace cannot silently
 * lose canonical hotels when customer publication flags flip.
 *
 * Customer entitlement continues to use listPropertyProfiles() elsewhere.
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  formatPropertyLocationLine,
  formatPropertySelectorLabel,
} from "../../dealality/property-display-label.js";
import { loadPropertyProfile } from "../data-model.js";
import { CANONICAL_ADP_31_PROPERTY_IDS } from "../governance/canonical-adp-31-property-scope-v1.js";
import {
  ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS,
  ADP_AIMBRIDGE_CALA_INTERNAL_INVENTORY_V1,
  isAimbridgeCalaInternalPropertyId,
} from "../governance/adp-aimbridge-cala-internal-inventory-v1.js";
import {
  isOwnerAdpReportSelectorEligible,
  isBppResearchOnlyPeer,
} from "../governance/adp-property-registry-scope-v1.js";
import { isAdpResearchExcluded } from "../contracts/adp-portfolio-eligibility-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "../governance/adp-operational-registry-hygiene-v1.js";
import { listAllAdpFixtureProfiles } from "../review/list-adp-reviewer-properties-v1.js";
import { loadStagedManifest } from "../review/adp-staged-report-store-v1.js";
import { loadPublishedManifest } from "../published-snapshot.js";
import { createFilesystemDurablePublishedStore } from "../durable-publication/filesystem-durable-store-v1.js";

export const ADP_OWNER_APP_ADMIN_INVENTORY_V1 = "ADP_OWNER_APP_ADMIN_INVENTORY_V1";

export const ADP_OWNER_APP_REPORT_READINESS = Object.freeze({
  AVAILABLE: "available",
  STAGED: "staged",
  INCOMPLETE: "incomplete",
  BLOCKED: "blocked",
});

const READINESS_LABEL = Object.freeze({
  available: "Available",
  staged: "Staged",
  incomplete: "Incomplete",
  blocked: "Blocked",
});

function humanizePropertyId(propertyId) {
  return String(propertyId || "")
    .replace(/^adp_/, "")
    .split("_")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

function hasDurableCurrentPointer(propertyId, cwd = process.cwd()) {
  const id = String(propertyId || "").trim();
  if (!id) return false;
  return existsSync(join(cwd, "data/adp-durable-published/current", id, "manifest.json"));
}

/**
 * Inspect durable/runtime/staged artifacts. CURRENT pointer alone is not enough.
 * @param {string} propertyId
 * @param {{ cwd?: string }} [opts]
 */
export function assessOwnerAppReportArtifacts(propertyId, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const id = String(propertyId || "").trim();
  const out = {
    propertyId: id || null,
    hasCurrentPointer: false,
    hasDurableSnapshotReport: false,
    hasDurableSnapshotEvidence: false,
    evidenceItemCount: 0,
    durablePeriodId: null,
    sourceStagedVersionId: null,
    stagedStoreVersionId: null,
    hasRuntimePeriod: false,
    reportReadSource: "none",
    presentationReady: false,
  };
  if (!id) return out;

  out.hasCurrentPointer = hasDurableCurrentPointer(id, cwd);
  const staged = loadStagedManifest(id, cwd);
  out.stagedStoreVersionId = staged?.latestStagedVersionId || null;

  try {
    const store = createFilesystemDurablePublishedStore({
      env: process.env,
      root: join(cwd, "data/adp-durable-published"),
      allowMissingRegistry: true,
    });
    const entry = store.getCurrentEntry(id);
    if (entry?.periodId) {
      out.durablePeriodId = entry.periodId;
      out.sourceStagedVersionId = entry.sourceStagedVersionId || null;
      const reportPath = join(
        cwd,
        "data/adp-durable-published/snapshots",
        id,
        entry.reportFile || `report-${entry.periodId}.json`
      );
      const evidencePath = join(
        cwd,
        "data/adp-durable-published/snapshots",
        id,
        entry.evidenceFile || `evidence-${entry.periodId}.json`
      );
      out.hasDurableSnapshotReport = existsSync(reportPath);
      out.hasDurableSnapshotEvidence = existsSync(evidencePath);
      if (out.hasDurableSnapshotEvidence) {
        try {
          const ev = JSON.parse(readFileSync(evidencePath, "utf8"));
          out.evidenceItemCount = Array.isArray(ev?.items) ? ev.items.length : 0;
        } catch {
          out.evidenceItemCount = 0;
        }
      }
      if (out.hasDurableSnapshotReport) {
        out.reportReadSource = "durable_snapshot";
      }
    }
  } catch {
    /* registry unavailable — fall through */
  }

  if (!out.hasDurableSnapshotReport && loadPublishedManifest(id)) {
    out.reportReadSource = "published_manifest";
  } else if (!out.hasDurableSnapshotReport && out.stagedStoreVersionId) {
    out.reportReadSource = "staged_store";
  }

  const runtimePath = out.durablePeriodId
    ? join(cwd, "data/ai-demand-positioning/runtime", `${out.durablePeriodId}.json`)
    : null;
  out.hasRuntimePeriod = Boolean(runtimePath && existsSync(runtimePath));

  // Retrievable durable snapshot ≠ customer-certified presentation package.
  out.presentationReady = false;
  return out;
}

/**
 * @param {string} propertyId
 * @param {object|null} profile
 * @param {{ cwd?: string }} [opts]
 * @returns {"available"|"staged"|"incomplete"|"blocked"}
 */
export function resolveOwnerAppReportReadiness(propertyId, profile = null, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const id = String(propertyId || "").trim();
  if (!id) return ADP_OWNER_APP_REPORT_READINESS.INCOMPLETE;
  if (isAdpResearchExcluded(id)) return ADP_OWNER_APP_REPORT_READINESS.BLOCKED;

  const artifacts = assessOwnerAppReportArtifacts(id, { cwd });
  if (artifacts.hasDurableSnapshotReport || artifacts.reportReadSource === "published_manifest") {
    return ADP_OWNER_APP_REPORT_READINESS.AVAILABLE;
  }
  if (artifacts.stagedStoreVersionId) {
    return ADP_OWNER_APP_REPORT_READINESS.STAGED;
  }
  // Pointer-only CURRENT without snapshot report is incomplete, not available.
  if (artifacts.hasCurrentPointer || artifacts.hasRuntimePeriod) {
    return ADP_OWNER_APP_REPORT_READINESS.INCOMPLETE;
  }
  if (!profile || !profile.name) return ADP_OWNER_APP_REPORT_READINESS.INCOMPLETE;
  return ADP_OWNER_APP_REPORT_READINESS.INCOMPLETE;
}

const CANONICAL_31_SET = new Set(CANONICAL_ADP_31_PROPERTY_IDS);

export const ADP_OWNER_APP_EXECUTION_SCOPE = Object.freeze({
  CANONICAL_ADP_31: "CANONICAL_ADP_31",
  INTERNAL_ADDITIONAL_OUTSIDE_CANONICAL_31:
    "INTERNAL_ADDITIONAL_OUTSIDE_CANONICAL_31",
});

function toSelectorRow(propertyId, profile, readiness, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const id = String(propertyId || "").trim();
  const name =
    (profile && profile.name) ||
    humanizePropertyId(id) ||
    id;
  const city = profile?.city || "";
  const state = profile?.state || "";
  const region = profile?.region || "";
  const country = profile?.country || "";
  const inCanonical31 = CANONICAL_31_SET.has(id);
  const executionScope = inCanonical31
    ? ADP_OWNER_APP_EXECUTION_SCOPE.CANONICAL_ADP_31
    : ADP_OWNER_APP_EXECUTION_SCOPE.INTERNAL_ADDITIONAL_OUTSIDE_CANONICAL_31;
  const artifacts = assessOwnerAppReportArtifacts(id, { cwd });
  const baseLabel = formatPropertySelectorLabel({
    name,
    city,
    state,
    region,
    country,
  });
  const readinessLabel = READINESS_LABEL[readiness] || readiness;
  // Readiness and execution-scope markers stay independent.
  const parts = [baseLabel];
  if (!inCanonical31) {
    parts.push("Outside canonical 31");
  }
  if (readiness !== ADP_OWNER_APP_REPORT_READINESS.AVAILABLE) {
    parts.push(readinessLabel);
  }
  const label = parts.join(" · ");

  return {
    propertyId: id,
    name,
    city,
    state,
    region,
    country,
    locationLine: formatPropertyLocationLine({ city, state, region, country }),
    label,
    optionLabel: label,
    chainScale: profile?.chainScale || "",
    affiliation: profile?.affiliation || "",
    customerDropdownVisible: profile?.customerDropdownVisible !== false,
    founderPreviewEligible: profile?.founderPreviewEligible === true,
    reportReadiness: readiness,
    reportReadinessLabel: readinessLabel,
    reportReadSource: artifacts.reportReadSource,
    hasDurableSnapshotReport: artifacts.hasDurableSnapshotReport,
    hasDurableSnapshotEvidence: artifacts.hasDurableSnapshotEvidence,
    evidenceItemCount: artifacts.evidenceItemCount,
    durablePeriodId: artifacts.durablePeriodId,
    presentationReady: artifacts.presentationReady,
    canonicalAdp31Scope: inCanonical31,
    executionScope,
    surface: "owner_app_admin",
    inventoryScope: ADP_OWNER_APP_ADMIN_INVENTORY_V1,
  };
}

/**
 * Canonical 31 ∪ Aimbridge CALA internal registry ∪ selector-eligible fixtures
 * (including customer-hidden Full ADP hotels such as YOTEL).
 * Excludes BPP research-only peers and synthetics.
 * Never expands CANONICAL_ADP_31_PROPERTY_IDS.
 *
 * @param {{ cwd?: string }} [opts]
 * @returns {object[]}
 */
export function listInternalOwnerAppPropertyInventory(opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const byId = new Map();

  for (const id of CANONICAL_ADP_31_PROPERTY_IDS) {
    if (isAdpSyntheticOrTestPropertyId(id)) continue;
    const profile = loadPropertyProfile(id) || { propertyId: id };
    const art = assessOwnerAppReportArtifacts(id, { cwd });
    if (
      profile &&
      isBppResearchOnlyPeer(profile, { hasStaged: false, hasPublished: false }) &&
      !isOwnerAdpReportSelectorEligible(profile, {
        hasStaged: Boolean(art.stagedStoreVersionId),
        hasPublished: art.hasDurableSnapshotReport,
      })
    ) {
      continue;
    }
    const readiness = resolveOwnerAppReportReadiness(id, profile, { cwd });
    byId.set(id, toSelectorRow(id, profile, readiness, { cwd }));
  }

  // Aimbridge CALA — durable HPC-linked identities outside canonical 31.
  for (const hotel of ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS) {
    const id = hotel.propertyId;
    if (!id || isAdpSyntheticOrTestPropertyId(id) || byId.has(id)) continue;
    const profile =
      loadPropertyProfile(id) ||
      {
        propertyId: id,
        name: hotel.name,
        city: hotel.city,
        country: hotel.country,
        affiliation: hotel.affiliation,
        censusRecordId: hotel.censusRecordId,
        customerDropdownVisible: false,
        fullAdpReportCommissioned: true,
      };
    const readiness = resolveOwnerAppReportReadiness(id, profile, { cwd });
    const row = toSelectorRow(id, profile, readiness, { cwd });
    row.censusRecordId = hotel.censusRecordId;
    row.internalInventoryCohort = ADP_AIMBRIDGE_CALA_INTERNAL_INVENTORY_V1;
    row.measurementPeriodId = hotel.measurementPeriodId;
    row.outstandingWork = [...(hotel.outstandingWork || [])];
    row.aimbridgeCalaInternal = isAimbridgeCalaInternalPropertyId(id);
    byId.set(id, row);
  }

  for (const profile of listAllAdpFixtureProfiles(cwd)) {
    const id = profile.propertyId;
    if (!id || isAdpSyntheticOrTestPropertyId(id)) continue;
    if (byId.has(id)) continue;
    const art = assessOwnerAppReportArtifacts(id, { cwd });
    const artifacts = {
      hasStaged: Boolean(art.stagedStoreVersionId),
      hasPublished: art.hasDurableSnapshotReport || Boolean(loadPublishedManifest(id)),
    };
    if (!isOwnerAdpReportSelectorEligible(profile, artifacts)) continue;
    const readiness = resolveOwnerAppReportReadiness(id, profile, { cwd });
    byId.set(id, toSelectorRow(id, profile, readiness, { cwd }));
  }

  return [...byId.values()].sort((a, b) =>
    String(a.name).localeCompare(String(b.name))
  );
}

export function listInternalOwnerAppPropertyIds(opts = {}) {
  return listInternalOwnerAppPropertyInventory(opts).map((p) => p.propertyId);
}
