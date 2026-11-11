/**
 * ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1
 *
 * Every Full ADP E2E hotel that stages a commissioned report must appear in
 * AI Admin operational inventory — independently of customer publication.
 *
 * Does NOT publish. Does NOT create HPC records. Does NOT authorize paid research.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  isAdpSyntheticOrTestPropertyId,
  listCanonicalAdp31PropertyIds,
  listOperationalAdpPropertyIds,
} from "./adp-operational-registry-hygiene-v1.js";
import {
  ADP_PROPERTY_REGISTRY_ROLES,
  classifyAdpPropertyRegistryRole,
  isBppResearchOnlyPeer,
} from "./adp-property-registry-scope-v1.js";
import { loadPropertyProfile } from "../data-model.js";
import {
  listStagedPropertyIds,
  loadStagedManifest,
  loadStagedOwnerPayload,
} from "../review/adp-staged-report-store-v1.js";
import { loadPublishedManifest } from "../published-snapshot.js";
import { listAllAdpFixtureProfiles } from "../review/list-adp-reviewer-properties-v1.js";

export const ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1 =
  "ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1";

export const ADP_FULL_ADP_REPORT_PROPERTY = "FULL_ADP_REPORT_PROPERTY";
export const ADP_BPP_RESEARCH_ONLY_PEER = "BPP_RESEARCH_ONLY_PEER";
export const ADP_HPC_CENSUS_ONLY = "HPC_CENSUS_ONLY";
export const ADP_PREFLIGHT_CANDIDATE = "ADP_PREFLIGHT_CANDIDATE";
export const ADP_PUBLISHED_ADP_PROPERTY = "PUBLISHED_ADP_PROPERTY";
export const ADP_TEST_ONLY_SYNTHETIC = "TEST_ONLY_SYNTHETIC";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

const REGISTRY_REL =
  "data/ai-demand-positioning/operational/adp-full-e2e-ai-admin-registry-v1.json";

function registryPath(cwd = ROOT) {
  return path.join(cwd, REGISTRY_REL);
}

function emptyRegistry() {
  return {
    contract: ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1,
    schemaVersion: 1,
    updatedAt: null,
    properties: {},
  };
}

/**
 * @param {string} [cwd]
 */
export function loadAiAdminOperationalRegistry(cwd = process.cwd()) {
  const p = registryPath(cwd);
  if (!fs.existsSync(p)) return emptyRegistry();
  try {
    const doc = JSON.parse(fs.readFileSync(p, "utf8"));
    if (!doc || typeof doc !== "object") return emptyRegistry();
    if (!doc.properties || typeof doc.properties !== "object") {
      return { ...emptyRegistry(), ...doc, properties: {} };
    }
    return doc;
  } catch {
    return emptyRegistry();
  }
}

function writeRegistry(doc, cwd = process.cwd()) {
  const p = registryPath(cwd);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const next = {
    ...doc,
    contract: ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(p, `${JSON.stringify(next, null, 2)}\n`, "utf8");
  return next;
}

/**
 * Eligibility class for AI Admin operational rows.
 * @param {string} propertyId
 * @param {{ cwd?: string }} [opts]
 */
export function classifyAiAdminOperationalEligibility(propertyId, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const id = String(propertyId || "").trim();
  if (!id) {
    return { propertyId: null, role: null, eligible: false, reason: "missing_id" };
  }
  if (isAdpSyntheticOrTestPropertyId(id)) {
    return {
      propertyId: id,
      role: ADP_TEST_ONLY_SYNTHETIC,
      eligible: false,
      reason: "synthetic_excluded",
    };
  }

  const profile = loadPropertyProfile(id) || null;
  const staged = loadStagedManifest(id, cwd);
  const stagedPayload = loadStagedOwnerPayload(id, cwd);
  const hasStaged = Boolean(
    staged?.latestVersionId ||
      staged?.versionId ||
      (stagedPayload?.ok !== false && stagedPayload?.versionId)
  );
  const hasPublished = Boolean(loadPublishedManifest(id));
  const role = classifyAdpPropertyRegistryRole(profile || { propertyId: id }, {
    hasStaged,
    hasPublished,
  });

  if (role === ADP_PROPERTY_REGISTRY_ROLES.BPP_RESEARCH_ONLY_PEER) {
    return {
      propertyId: id,
      role: ADP_BPP_RESEARCH_ONLY_PEER,
      eligible: false,
      reason: "research_only_bpp_peer",
      hasStaged,
      hasPublished,
    };
  }
  if (role === ADP_PROPERTY_REGISTRY_ROLES.HPC_CENSUS_ONLY) {
    return {
      propertyId: id,
      role: ADP_HPC_CENSUS_ONLY,
      eligible: false,
      reason: "hpc_census_only",
      hasStaged,
      hasPublished,
    };
  }
  if (role === ADP_PROPERTY_REGISTRY_ROLES.ADP_PREFLIGHT_CANDIDATE && !hasStaged) {
    return {
      propertyId: id,
      role: ADP_PREFLIGHT_CANDIDATE,
      eligible: false,
      reason: "preflight_without_staged_report",
      hasStaged,
      hasPublished,
    };
  }

  const commissioned =
    profile?.fullAdpReportCommissioned === true ||
    profile?.standaloneAdpReportAuthorized === true ||
    hasStaged ||
    hasPublished ||
    listCanonicalAdp31PropertyIds().includes(id);

  if (!commissioned) {
    return {
      propertyId: id,
      role,
      eligible: false,
      reason: "not_commissioned_full_adp_report",
      hasStaged,
      hasPublished,
    };
  }

  return {
    propertyId: id,
    role:
      hasPublished
        ? ADP_PUBLISHED_ADP_PROPERTY
        : ADP_FULL_ADP_REPORT_PROPERTY,
    eligible: true,
    reason: hasPublished
      ? "published_adp_property"
      : hasStaged
        ? "staged_full_adp_report"
        : "canonical_31_operational",
    hasStaged,
    hasPublished,
    stagedVersionId:
      stagedPayload?.versionId ||
      staged?.latestVersionId ||
      staged?.versionId ||
      null,
    hotelName: profile?.name || stagedPayload?.payload?.property?.name || null,
    hpcId: profile?.censusRecordId || profile?.hpcHotelId || null,
    publicationAuthorized: false,
  };
}

/**
 * Discover Full ADP report property IDs beyond the frozen 31 from staged store,
 * durable registry, and commissioned fixture profiles.
 * @param {{ cwd?: string }} [opts]
 * @returns {string[]}
 */
export function discoverFullAdpReportPropertyIdsBeyond31(opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const canon = new Set(listCanonicalAdp31PropertyIds());
  const candidates = new Set();

  const registry = loadAiAdminOperationalRegistry(cwd);
  for (const id of Object.keys(registry.properties || {})) {
    if (registry.properties[id]?.eligible === false) continue;
    candidates.add(id);
  }

  for (const id of listStagedPropertyIds(cwd)) {
    candidates.add(id);
  }

  for (const profile of listAllAdpFixtureProfiles(cwd)) {
    const id = profile.propertyId;
    if (!id || canon.has(id)) continue;
    if (isAdpSyntheticOrTestPropertyId(id)) continue;
    if (isBppResearchOnlyPeer(profile, { hasStaged: false, hasPublished: false })) {
      continue;
    }
    if (
      profile.fullAdpReportCommissioned === true ||
      profile.standaloneAdpReportAuthorized === true ||
      profile.registryRole === ADP_PROPERTY_REGISTRY_ROLES.FULL_ADP_REPORT_PROPERTY ||
      profile.registryRole === ADP_PROPERTY_REGISTRY_ROLES.PUBLISHED_ADP_PROPERTY
    ) {
      candidates.add(id);
    }
  }

  const out = [];
  for (const id of candidates) {
    if (canon.has(id)) continue;
    if (isAdpSyntheticOrTestPropertyId(id)) continue;
    const cls = classifyAiAdminOperationalEligibility(id, { cwd });
    if (cls.eligible) out.push(id);
  }
  return [...new Set(out)].sort();
}

/**
 * Authoritative AI Admin / operational Full ADP report property universe:
 * canonical 31 ∪ discovered/registered Full ADP report properties.
 * Excludes research-only peers, census-only, synthetic.
 * @param {{ cwd?: string }} [opts]
 * @returns {string[]}
 */
export function listFullAdpReportOperationalPropertyIds(_opts = {}) {
  // Hygiene owns the authoritative union (31 ∪ staged/registered Full ADP extras).
  return listOperationalAdpPropertyIds();
}

/**
 * Register a staged Full ADP E2E hotel into the AI Admin operational registry.
 * Never sets published / Live / issues external shares.
 *
 * @param {{
 *   propertyId: string,
 *   stagedVersionId?: string|null,
 *   runId?: string|null,
 *   customerDeliveryCertified?: boolean,
 *   bppCertified?: boolean,
 *   cwd?: string,
 *   actor?: string,
 * }} args
 */
export function registerFullAdpE2eHotelInAiAdmin(args = {}) {
  const cwd = args.cwd || process.cwd();
  const id = String(args.propertyId || "").trim();
  if (!id) {
    return { ok: false, error: "propertyId_required", published: false };
  }
  if (isAdpSyntheticOrTestPropertyId(id)) {
    return {
      ok: false,
      error: "synthetic_excluded",
      propertyId: id,
      published: false,
    };
  }

  const cls = classifyAiAdminOperationalEligibility(id, { cwd });
  if (isBppResearchOnlyPeer(loadPropertyProfile(id) || { propertyId: id }, {
    hasStaged: cls.hasStaged,
    hasPublished: cls.hasPublished,
  })) {
    return {
      ok: false,
      error: "research_only_bpp_peer_excluded",
      propertyId: id,
      role: ADP_BPP_RESEARCH_ONLY_PEER,
      published: false,
    };
  }

  if (!cls.hasStaged && !cls.hasPublished && !listCanonicalAdp31PropertyIds().includes(id)) {
    return {
      ok: false,
      error: "staged_or_published_report_required",
      propertyId: id,
      published: false,
      classification: cls,
    };
  }

  const registry = loadAiAdminOperationalRegistry(cwd);
  const prev = registry.properties[id] || null;
  const stagedVersionId =
    args.stagedVersionId || cls.stagedVersionId || prev?.stagedVersionId || null;

  registry.properties[id] = {
    propertyId: id,
    hotelName: cls.hotelName || prev?.hotelName || id,
    hpcId: cls.hpcId || prev?.hpcId || null,
    role: cls.role || ADP_FULL_ADP_REPORT_PROPERTY,
    eligible: true,
    registryRole: cls.role || ADP_FULL_ADP_REPORT_PROPERTY,
    stagedVersionId,
    runId: args.runId || prev?.runId || null,
    customerDeliveryCertified: args.customerDeliveryCertified === true,
    bppCertified: args.bppCertified === true,
    publicationStatus: "NOT_AUTHORIZED",
    published: false,
    customerPublished: false,
    registeredAt: prev?.registeredAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    actor: args.actor || "adp_full_e2e_ai_admin_auto_registration_v1",
    contract: ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1,
  };

  writeRegistry(registry, cwd);

  const operational = listFullAdpReportOperationalPropertyIds({ cwd });
  const discoverable = operational.includes(id);

  return {
    ok: discoverable,
    gate: ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1,
    propertyId: id,
    stagedVersionId,
    discoverable,
    published: false,
    publicationStatus: "NOT_AUTHORIZED",
    operationalCount: operational.length,
    canonical31Count: 31,
    greenfieldExtrasCount: Math.max(0, operational.length - 31),
    record: registry.properties[id],
    note:
      "Registered for AI Admin operational discoverability. Customer publication unchanged (NOT_AUTHORIZED).",
  };
}

/**
 * Verify AI Admin discoverability for a property (API-layer / registry).
 * Does not open a browser.
 */
export function assessAiAdminDiscoverability(propertyId, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const id = String(propertyId || "").trim();
  const failures = [];
  const operational = listFullAdpReportOperationalPropertyIds({ cwd });
  const inOps = operational.includes(id);
  if (!inOps) failures.push("MISSING_FROM_OPERATIONAL_REGISTRY");

  const cls = classifyAiAdminOperationalEligibility(id, { cwd });
  if (!cls.eligible) failures.push(`INELIGIBLE:${cls.reason}`);
  if (cls.role === ADP_BPP_RESEARCH_ONLY_PEER) {
    failures.push("RESEARCH_ONLY_PEER_LEAK");
  }
  if (isAdpSyntheticOrTestPropertyId(id)) failures.push("SYNTHETIC_LEAK");

  const registry = loadAiAdminOperationalRegistry(cwd);
  const record = registry.properties[id] || null;
  if (opts.requireRegistryRecord && !record) {
    failures.push("REGISTRY_RECORD_MISSING");
  }
  if (record?.published === true || record?.customerPublished === true) {
    // Not a failure for published hotels; only flag accidental publish during greenfield register.
    if (opts.expectUnpublished === true) {
      failures.push("UNEXPECTED_PUBLISHED_FLAG");
    }
  }
  if (
    opts.expectedStagedVersionId &&
    cls.stagedVersionId &&
    cls.stagedVersionId !== opts.expectedStagedVersionId
  ) {
    failures.push("STAGED_VERSION_MISMATCH");
  }

  return {
    ok: failures.length === 0 && inOps,
    gate: ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1,
    propertyId: id,
    inOperationalRegistry: inOps,
    operationalCount: operational.length,
    classification: cls,
    registryRecord: record,
    published: false,
    failures,
  };
}

/**
 * Gate used by Full ADP E2E after staging / customer-delivery: register + assess.
 * Failure blocks unrestricted customer-delivery certification.
 */
export function ensureFullAdpE2eAiAdminRegistration({
  propertyId,
  stagedVersionId = null,
  runId = null,
  customerDeliveryCertified = false,
  bppCertified = false,
  cwd = process.cwd(),
} = {}) {
  const registration = registerFullAdpE2eHotelInAiAdmin({
    propertyId,
    stagedVersionId,
    runId,
    customerDeliveryCertified,
    bppCertified,
    cwd,
    actor: "adp_autonomous_hotel_full_e2e",
  });
  const assessment = assessAiAdminDiscoverability(propertyId, {
    cwd,
    requireRegistryRecord: true,
    expectUnpublished: true,
    expectedStagedVersionId: stagedVersionId || null,
  });

  const ok = registration.ok === true && assessment.ok === true;
  return {
    ok,
    gate: ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1,
    blocksCustomerDeliveryCertification: !ok,
    registration,
    assessment,
    published: false,
  };
}
