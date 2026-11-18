/**
 * ADP_OWNER_APP_ADMIN_INVENTORY_V1
 *
 * Founder/admin owner-app property selector inventory.
 * Decoupled from customerDropdownVisible so internal workspace cannot silently
 * lose canonical hotels when customer publication flags flip.
 *
 * Customer entitlement continues to use listPropertyProfiles() elsewhere.
 */

import { existsSync } from "fs";
import { join } from "path";
import {
  formatPropertyLocationLine,
  formatPropertySelectorLabel,
} from "../../dealality/property-display-label.js";
import { loadPropertyProfile } from "../data-model.js";
import { CANONICAL_ADP_31_PROPERTY_IDS } from "../governance/canonical-adp-31-property-scope-v1.js";
import {
  isOwnerAdpReportSelectorEligible,
  isBppResearchOnlyPeer,
} from "../governance/adp-property-registry-scope-v1.js";
import { isAdpResearchExcluded } from "../contracts/adp-portfolio-eligibility-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "../governance/adp-operational-registry-hygiene-v1.js";
import { listAllAdpFixtureProfiles } from "../review/list-adp-reviewer-properties-v1.js";
import { loadStagedManifest } from "../review/adp-staged-report-store-v1.js";
import { loadPublishedManifest } from "../published-snapshot.js";

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

function hasDurableCurrent(propertyId, cwd = process.cwd()) {
  const id = String(propertyId || "").trim();
  if (!id) return false;
  return existsSync(join(cwd, "data/adp-durable-published/current", id));
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

  const durable = hasDurableCurrent(id, cwd);
  const published = Boolean(loadPublishedManifest(id));
  const staged = Boolean(loadStagedManifest(id, cwd)?.latestStagedVersionId);

  if (durable || published) return ADP_OWNER_APP_REPORT_READINESS.AVAILABLE;
  if (staged) return ADP_OWNER_APP_REPORT_READINESS.STAGED;
  if (!profile || !profile.name) return ADP_OWNER_APP_REPORT_READINESS.INCOMPLETE;
  return ADP_OWNER_APP_REPORT_READINESS.INCOMPLETE;
}

const CANONICAL_31_SET = new Set(CANONICAL_ADP_31_PROPERTY_IDS);

export const ADP_OWNER_APP_EXECUTION_SCOPE = Object.freeze({
  CANONICAL_ADP_31: "CANONICAL_ADP_31",
  INTERNAL_ADDITIONAL_OUTSIDE_CANONICAL_31:
    "INTERNAL_ADDITIONAL_OUTSIDE_CANONICAL_31",
});

function toSelectorRow(propertyId, profile, readiness) {
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
    canonicalAdp31Scope: inCanonical31,
    executionScope,
    surface: "owner_app_admin",
    inventoryScope: ADP_OWNER_APP_ADMIN_INVENTORY_V1,
  };
}

/**
 * Canonical 31 ∪ selector-eligible fixture profiles (including customer-hidden
 * Full ADP hotels such as YOTEL). Excludes BPP research-only peers and synthetics.
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
    if (
      profile &&
      isBppResearchOnlyPeer(profile, { hasStaged: false, hasPublished: false }) &&
      !isOwnerAdpReportSelectorEligible(profile, {
        hasStaged: hasDurableCurrent(id, cwd),
        hasPublished: hasDurableCurrent(id, cwd),
      })
    ) {
      continue;
    }
    const readiness = resolveOwnerAppReportReadiness(id, profile, { cwd });
    byId.set(id, toSelectorRow(id, profile, readiness));
  }

  for (const profile of listAllAdpFixtureProfiles(cwd)) {
    const id = profile.propertyId;
    if (!id || isAdpSyntheticOrTestPropertyId(id)) continue;
    if (byId.has(id)) continue;
    const artifacts = {
      hasStaged: Boolean(loadStagedManifest(id, cwd)?.latestStagedVersionId),
      hasPublished: hasDurableCurrent(id, cwd) || Boolean(loadPublishedManifest(id)),
    };
    if (!isOwnerAdpReportSelectorEligible(profile, artifacts)) continue;
    const readiness = resolveOwnerAppReportReadiness(id, profile, { cwd });
    byId.set(id, toSelectorRow(id, profile, readiness));
  }

  return [...byId.values()].sort((a, b) =>
    String(a.name).localeCompare(String(b.name))
  );
}

export function listInternalOwnerAppPropertyIds(opts = {}) {
  return listInternalOwnerAppPropertyInventory(opts).map((p) => p.propertyId);
}
