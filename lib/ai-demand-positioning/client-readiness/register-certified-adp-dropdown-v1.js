/**
 * Shared post-certification registration for ADP property dropdown.
 *
 * Canonical dropdown path:
 *   public/js/ai-demand-positioning/ai-demand-positioning.js
 *     → GET /api/ai-demand-positioning/properties
 *     → listPropertyProfiles() (lib/ai-demand-positioning/data-model.js)
 *     → excludes profiles with customerDropdownVisible === false
 *
 * After CERTIFIED official publish, flip fixture flags so the property is
 * discoverable by the same mechanism as all other ADP hotels.
 * No hotel-specific UI exceptions.
 */

import { existsSync, readFileSync, readdirSync, writeFileSync } from "fs";
import { join } from "path";

export const ADP_DROPDOWN_REGISTRATION_VERSION = "adp_dropdown_registration_v1";

function fixturesDir() {
  return join(process.cwd(), "fixtures/ai-demand-positioning");
}

export function findPropertyProfileFixturePath(propertyId) {
  const dir = fixturesDir();
  if (!existsSync(dir)) return null;
  for (const file of readdirSync(dir)) {
    if (!file.endsWith("-property-profile.json")) continue;
    const p = join(dir, file);
    try {
      const data = JSON.parse(readFileSync(p, "utf8"));
      if (data.propertyId === propertyId) return p;
    } catch {
      /* skip */
    }
  }
  return null;
}

/**
 * Enable customer ADP dropdown visibility after CERTIFIED + published.
 * Idempotent. Does not invent hotels or bypass certification.
 *
 * @param {string} propertyId
 * @param {{ certificationStatus?: string, force?: boolean }} [opts]
 */
export function registerCertifiedAdpPropertyForCustomerDropdown(propertyId, opts = {}) {
  const id = String(propertyId || "").trim();
  if (!id.startsWith("adp_")) {
    return { ok: false, reason: "invalid_property_id", propertyId: id || null };
  }

  const status = String(opts.certificationStatus || "").toUpperCase();
  const certified =
    opts.force === true ||
    status === "CERTIFIED" ||
    status === "CERTIFIED_WITH_DISCLOSURES" ||
    status === "CERTIFIED_COMPLETE";

  if (!certified) {
    return {
      ok: false,
      reason: "not_certified",
      propertyId: id,
      certificationStatus: status || null,
      version: ADP_DROPDOWN_REGISTRATION_VERSION,
    };
  }

  const path = findPropertyProfileFixturePath(id);
  if (!path) {
    return {
      ok: false,
      reason: "profile_fixture_not_found",
      propertyId: id,
      version: ADP_DROPDOWN_REGISTRATION_VERSION,
    };
  }

  const profile = JSON.parse(readFileSync(path, "utf8"));
  const before = {
    customerDropdownVisible: profile.customerDropdownVisible,
    officialBaselinePublished: profile.officialBaselinePublished,
  };

  let changed = false;
  if (profile.customerDropdownVisible !== true) {
    profile.customerDropdownVisible = true;
    changed = true;
  }
  if (profile.officialBaselinePublished !== true) {
    profile.officialBaselinePublished = true;
    changed = true;
  }
  if (profile.baselineState !== "ADP_OFFICIAL_BASELINE_PUBLISHED") {
    profile.baselineState = "ADP_OFFICIAL_BASELINE_PUBLISHED";
    changed = true;
  }

  if (changed) {
    profile.dropdownRegistration = {
      version: ADP_DROPDOWN_REGISTRATION_VERSION,
      registeredAt: new Date().toISOString(),
      certificationStatus: status || "CERTIFIED",
      note: "Shared post-certification dropdown registration — not a UI hardcode",
    };
    writeFileSync(path, JSON.stringify(profile, null, 2) + "\n", "utf8");
  }

  return {
    ok: true,
    propertyId: id,
    path,
    changed,
    before,
    after: {
      customerDropdownVisible: profile.customerDropdownVisible,
      officialBaselinePublished: profile.officialBaselinePublished,
      baselineState: profile.baselineState,
    },
    version: ADP_DROPDOWN_REGISTRATION_VERSION,
  };
}
