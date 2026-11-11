/**
 * ADP_OPERATIONAL_REGISTRY_HYGIENE_V1
 *
 * Canonical real-hotel membership for operational admin / portfolio surfaces.
 * Synthetic and development-test fixtures must never appear in operational lists.
 *
 * Operational AI Admin universe = frozen canonical 31 ∪ commissioned Full ADP
 * report properties (staged/greenfield). Research-only BPP peers and synthetics
 * remain excluded. Publication is not required for admin visibility.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ADP_PROPERTY_REGISTRY_ROLES,
  classifyAdpPropertyRegistryRole,
  isBppResearchOnlyPeer,
} from "./adp-property-registry-scope-v1.js";
import { CANONICAL_ADP_31_PROPERTY_IDS } from "./canonical-adp-31-property-scope-v1.js";

export const ADP_OPERATIONAL_REGISTRY_HYGIENE_V1 =
  "ADP_OPERATIONAL_REGISTRY_HYGIENE_V1";

export const ADP_SYNTHETIC_TEST_PROPERTY_IDS = Object.freeze([
  "adp_smoke_test_synthetic",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

const MATRIX_PATH = path.join(
  ROOT,
  "reports/dealality-adp-core-loyalty-execution-v1/FINAL_31_HOTEL_CERTIFICATION_MATRIX.json"
);

const AI_ADMIN_REGISTRY_PATH = path.join(
  ROOT,
  "data/ai-demand-positioning/operational/adp-full-e2e-ai-admin-registry-v1.json"
);

const STAGED_ROOT = path.join(ROOT, "data/ai-demand-positioning/staged");
const FIXTURES_DIR = path.join(ROOT, "fixtures/ai-demand-positioning");

/**
 * @param {string|null|undefined} propertyId
 * @returns {boolean}
 */
export function isAdpSyntheticOrTestPropertyId(propertyId) {
  const id = String(propertyId || "").trim();
  if (!id) return false;
  if (ADP_SYNTHETIC_TEST_PROPERTY_IDS.includes(id)) return true;
  if (/_smoke_test_/i.test(id)) return true;
  if (/_synthetic$/i.test(id)) return true;
  if (/^adp_test_/i.test(id)) return true;
  // Durable-storage / unit-test leftovers must never inflate Admin published counts.
  if (/^adp_durable_/i.test(id)) return true;
  if (/_fixture_/i.test(id) || /_fixture$/i.test(id)) return true;
  if (/_wire_hotel$/i.test(id)) return true;
  return false;
}

/**
 * Controlling 31-hotel Core/Loyalty certification population.
 * Membership comes from the tracked scope module. A local operational matrix,
 * when present, must name the same hotels. It does not add hotels or statuses.
 * @returns {string[]}
 */
export function listCanonicalAdp31PropertyIds() {
  const ids = [...CANONICAL_ADP_31_PROPERTY_IDS].sort();
  if (ids.length !== 31 || new Set(ids).size !== 31) {
    throw new Error(
      `canonical_31_count_mismatch:expected=31 actual=${new Set(ids).size}`
    );
  }
  if (!ids.includes("adp_bethesda_marriott")) {
    throw new Error("canonical_31_missing_bethesda_marriott");
  }
  if (ids.some((id) => isAdpSyntheticOrTestPropertyId(id))) {
    throw new Error("canonical_31_contains_synthetic");
  }
  if (fs.existsSync(MATRIX_PATH)) {
    const doc = JSON.parse(fs.readFileSync(MATRIX_PATH, "utf8"));
    const local = [...new Set(
      (doc.matrix || [])
        .map((row) => String(row?.propertyId || "").trim())
        .filter(Boolean)
        .filter((id) => !isAdpSyntheticOrTestPropertyId(id))
    )].sort();
    const same =
      local.length === ids.length && local.every((id, index) => id === ids[index]);
    if (!same) {
      throw new Error("canonical_31_local_matrix_scope_drift");
    }
  }
  return ids;
}

function readJsonSafe(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function loadProfileForOperational(propertyId) {
  const slugGuess = String(propertyId || "")
    .replace(/^adp_/, "")
    .replace(/_/g, "-");
  const candidates = [
    path.join(FIXTURES_DIR, `${slugGuess}-property-profile.json`),
    path.join(FIXTURES_DIR, `${propertyId}-property-profile.json`),
  ];
  // Prefer exact filename scans for greenfield fixtures.
  if (fs.existsSync(FIXTURES_DIR)) {
    for (const file of fs.readdirSync(FIXTURES_DIR)) {
      if (!file.endsWith("-property-profile.json")) continue;
      const doc = readJsonSafe(path.join(FIXTURES_DIR, file));
      if (doc?.propertyId === propertyId) return doc;
    }
  }
  for (const p of candidates) {
    if (!fs.existsSync(p)) continue;
    const doc = readJsonSafe(p);
    if (doc?.propertyId === propertyId) return doc;
  }
  return null;
}

function hasStagedReport(propertyId) {
  const dir = path.join(STAGED_ROOT, propertyId);
  if (!fs.existsSync(dir)) return false;
  const manifest = readJsonSafe(path.join(dir, "manifest.json"));
  if (manifest?.latestVersionId || manifest?.versionId || manifest?.reportFile) {
    return true;
  }
  try {
    return fs.readdirSync(dir).some((f) => f.endsWith(".json"));
  } catch {
    return false;
  }
}

function hasPublishedReport(propertyId) {
  const id = String(propertyId || "").trim();
  if (!id) return false;
  const ephemeral = path.join(
    ROOT,
    "data/ai-demand-positioning/published",
    id,
    "manifest.json"
  );
  if (fs.existsSync(ephemeral)) return true;
  // Durable-prefer production: ephemeral published/ may be absent; still treat
  // durable current as published so Admin universe includes all sealed hotels.
  // Inline env check (no import) — avoids circular init with published-snapshot.
  const mode = String(process.env.ADP_DURABLE_PUBLISHED_MODE || "")
    .trim()
    .toLowerCase();
  if (mode === "prefer" || mode === "required" || mode === "dual") {
    const root = String(
      process.env.ADP_DURABLE_PUBLISHED_ROOT ||
        "/app/data/adp-durable-published"
    ).trim();
    if (root && fs.existsSync(path.join(root, "current", id, "manifest.json"))) {
      return true;
    }
  }
  return false;
}

/**
 * Extra Full ADP report properties beyond the frozen 31 (greenfield staged, etc.).
 * Never includes research-only BPP peers or synthetic fixtures.
 * @returns {string[]}
 */
export function listFullAdpReportExtrasBeyond31() {
  const canon = new Set(listCanonicalAdp31PropertyIds());
  const candidates = new Set();

  const registry = readJsonSafe(AI_ADMIN_REGISTRY_PATH);
  if (registry?.properties && typeof registry.properties === "object") {
    for (const [id, row] of Object.entries(registry.properties)) {
      if (row && row.eligible === false) continue;
      candidates.add(String(id).trim());
    }
  }

  if (fs.existsSync(STAGED_ROOT)) {
    for (const entry of fs.readdirSync(STAGED_ROOT, { withFileTypes: true })) {
      if (entry.isDirectory() && entry.name.startsWith("adp_")) {
        candidates.add(entry.name);
      }
    }
  }

  const extras = [];
  for (const id of candidates) {
    if (!id || canon.has(id)) continue;
    if (isAdpSyntheticOrTestPropertyId(id)) continue;
    const registryRow =
      registry?.properties && typeof registry.properties === "object"
        ? registry.properties[id]
        : null;
    const registryRole = String(
      registryRow?.registryRole || registryRow?.role || ""
    ).trim();
    const profile = loadProfileForOperational(id) || { propertyId: id };
    const staged = hasStagedReport(id);
    const published = hasPublishedReport(id);
    if (isBppResearchOnlyPeer(profile, { hasStaged: staged, hasPublished: published })) {
      continue;
    }
    const role = classifyAdpPropertyRegistryRole(profile, {
      hasStaged: staged,
      hasPublished: published,
    });
    if (role === ADP_PROPERTY_REGISTRY_ROLES.BPP_RESEARCH_ONLY_PEER) continue;
    if (role === ADP_PROPERTY_REGISTRY_ROLES.HPC_CENSUS_ONLY) continue;
    const adminRegisteredFullAdp =
      registryRole === ADP_PROPERTY_REGISTRY_ROLES.FULL_ADP_REPORT_PROPERTY ||
      registryRole === ADP_PROPERTY_REGISTRY_ROLES.PUBLISHED_ADP_PROPERTY;
    if (
      !staged &&
      !published &&
      !adminRegisteredFullAdp &&
      profile.fullAdpReportCommissioned !== true &&
      profile.standaloneAdpReportAuthorized !== true
    ) {
      continue;
    }
    if (
      role === ADP_PROPERTY_REGISTRY_ROLES.FULL_ADP_REPORT_PROPERTY ||
      role === ADP_PROPERTY_REGISTRY_ROLES.PUBLISHED_ADP_PROPERTY ||
      adminRegisteredFullAdp ||
      staged ||
      published ||
      profile.fullAdpReportCommissioned === true
    ) {
      extras.push(id);
    }
  }
  return [...new Set(extras)].sort();
}

/**
 * Operational hotel IDs for admin / coverage / portfolio certification.
 * Canonical 31 ∪ commissioned Full ADP report properties (may be unpublished).
 * Never synthetic fixtures or research-only BPP peers.
 * @returns {string[]}
 */
export function listOperationalAdpPropertyIds() {
  return [...new Set([...listCanonicalAdp31PropertyIds(), ...listFullAdpReportExtrasBeyond31()])]
    .filter((id) => !isAdpSyntheticOrTestPropertyId(id))
    .sort();
}

/**
 * Filter any ID list to operational real hotels only.
 * @param {Iterable<string>} propertyIds
 * @returns {string[]}
 */
export function filterOperationalAdpPropertyIds(propertyIds) {
  const operational = new Set(listOperationalAdpPropertyIds());
  return [...new Set([...(propertyIds || [])])]
    .map((id) => String(id || "").trim())
    .filter(Boolean)
    .filter((id) => !isAdpSyntheticOrTestPropertyId(id))
    .filter((id) => operational.has(id) || id.startsWith("adp_"))
    .filter((id) => !isAdpSyntheticOrTestPropertyId(id));
}

/**
 * Assert operational lists never contain synthetic fixtures.
 * Non-canonical IDs are allowed only when they are legitimate Full ADP extras.
 * @param {Iterable<string>} propertyIds
 * @param {{ allowNonCanonical?: boolean }} [opts]
 */
export function assertOperationalRegistryHygiene(propertyIds, opts = {}) {
  const ids = [...(propertyIds || [])].map((id) => String(id || "").trim()).filter(Boolean);
  const synthetic = ids.filter((id) => isAdpSyntheticOrTestPropertyId(id));
  const canonical = new Set(listCanonicalAdp31PropertyIds());
  const allowedExtras = new Set(listFullAdpReportExtrasBeyond31());
  const nonCanonical = opts.allowNonCanonical
    ? []
    : ids.filter(
        (id) =>
          !canonical.has(id) &&
          !isAdpSyntheticOrTestPropertyId(id) &&
          !allowedExtras.has(id)
      );
  const researchPeerLeaks = ids.filter((id) => {
    if (canonical.has(id) || isAdpSyntheticOrTestPropertyId(id)) return false;
    const profile = loadProfileForOperational(id) || { propertyId: id };
    return isBppResearchOnlyPeer(profile, {
      hasStaged: hasStagedReport(id),
      hasPublished: hasPublishedReport(id),
    });
  });
  return {
    gate: ADP_OPERATIONAL_REGISTRY_HYGIENE_V1,
    pass:
      synthetic.length === 0 &&
      researchPeerLeaks.length === 0 &&
      (opts.allowNonCanonical || nonCanonical.length === 0),
    count: ids.length,
    expectedCanonicalCount: 31,
    expectedOperationalMin: 31,
    greenfieldExtrasCount: ids.filter((id) => allowedExtras.has(id)).length,
    syntheticLeaks: synthetic,
    researchPeerLeaks,
    nonCanonicalIds: nonCanonical,
    bethesdaPresent: ids.includes("adp_bethesda_marriott"),
  };
}
