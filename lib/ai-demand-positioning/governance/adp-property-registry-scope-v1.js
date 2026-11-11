/**
 * ADP property registry scope roles — keep customer/founder report selectors
 * separate from research-only BPP peer identities.
 *
 * Contract: ADP_PROPERTY_REGISTRY_SCOPE_V1
 */

export const ADP_PROPERTY_REGISTRY_SCOPE_V1 = "ADP_PROPERTY_REGISTRY_SCOPE_V1";

export const ADP_PROPERTY_REGISTRY_ROLES = Object.freeze({
  FULL_ADP_REPORT_PROPERTY: "FULL_ADP_REPORT_PROPERTY",
  BPP_RESEARCH_ONLY_PEER: "BPP_RESEARCH_ONLY_PEER",
  HPC_CENSUS_ONLY: "HPC_CENSUS_ONLY",
  ADP_PREFLIGHT_CANDIDATE: "ADP_PREFLIGHT_CANDIDATE",
  PUBLISHED_ADP_PROPERTY: "PUBLISHED_ADP_PROPERTY",
});

const DRAFT_PEER_STATUSES = new Set([
  "DRAFT_PEER_LOYALTY_ONLY",
  "BPP_RESEARCH_ONLY_PEER",
  "RESEARCH_ONLY_PEER",
]);

const DRAFT_PEER_BASELINES = new Set([
  "DRAFT_PEER_NOT_CERTIFIED",
  "BPP_RESEARCH_ONLY",
]);

/**
 * @param {object|null|undefined} profile
 * @param {{ hasStaged?: boolean, hasPublished?: boolean }=} artifacts
 * @returns {string}
 */
export function classifyAdpPropertyRegistryRole(profile, artifacts = {}) {
  const p = profile && typeof profile === "object" ? profile : {};
  const id = String(p.propertyId || "").trim();
  const explicit = String(p.registryRole || p.adpRegistryRole || "").trim();
  if (explicit && Object.values(ADP_PROPERTY_REGISTRY_ROLES).includes(explicit)) {
    // Explicit research-only never upgrades merely because Loyalty observations exist.
    if (explicit === ADP_PROPERTY_REGISTRY_ROLES.BPP_RESEARCH_ONLY_PEER) {
      return explicit;
    }
    if (
      explicit === ADP_PROPERTY_REGISTRY_ROLES.FULL_ADP_REPORT_PROPERTY &&
      artifacts.hasPublished
    ) {
      return ADP_PROPERTY_REGISTRY_ROLES.PUBLISHED_ADP_PROPERTY;
    }
    return explicit;
  }

  const greenfield = String(p.greenfieldStatus || "").trim();
  const baseline = String(p.baselineState || "").trim();
  const peerOf = p.stewardship?.peerOf || p.peerOf || null;
  const ownerSelectorEligible = p.ownerSelectorEligible;
  const founderReviewVisible = p.founderReviewVisible;

  const markedResearchOnly =
    ownerSelectorEligible === false ||
    founderReviewVisible === false ||
    DRAFT_PEER_STATUSES.has(greenfield) ||
    DRAFT_PEER_BASELINES.has(baseline) ||
    Boolean(peerOf) ||
    /^draft_/i.test(id);

  if (markedResearchOnly) {
    // Commissioned Full ADP subjects may temporarily carry peerOf metadata — only
    // treat as research-only when there is no standalone report AND no explicit
    // Full ADP commission flag.
    const commissionedFullAdp =
      p.fullAdpReportCommissioned === true ||
      p.standaloneAdpReportAuthorized === true ||
      greenfield === "FULL_ADP_E2E_COMMISSIONED" ||
      greenfield === "GREENFIELD_FULL_ADP" ||
      (id.startsWith("adp_") && (artifacts.hasStaged || artifacts.hasPublished));

    if (!commissionedFullAdp && !(artifacts.hasStaged || artifacts.hasPublished)) {
      return ADP_PROPERTY_REGISTRY_ROLES.BPP_RESEARCH_ONLY_PEER;
    }
  }

  if (artifacts.hasPublished) {
    return ADP_PROPERTY_REGISTRY_ROLES.PUBLISHED_ADP_PROPERTY;
  }
  if (artifacts.hasStaged || p.fullAdpReportCommissioned === true) {
    return ADP_PROPERTY_REGISTRY_ROLES.FULL_ADP_REPORT_PROPERTY;
  }
  if (greenfield === "ADP_PREFLIGHT_CANDIDATE" || p.adpPreflightOnly === true) {
    return ADP_PROPERTY_REGISTRY_ROLES.ADP_PREFLIGHT_CANDIDATE;
  }
  if (p.hpcCensusOnly === true || greenfield === "HPC_CENSUS_ONLY") {
    return ADP_PROPERTY_REGISTRY_ROLES.HPC_CENSUS_ONLY;
  }
  if (id.startsWith("adp_")) {
    return ADP_PROPERTY_REGISTRY_ROLES.FULL_ADP_REPORT_PROPERTY;
  }
  return ADP_PROPERTY_REGISTRY_ROLES.HPC_CENSUS_ONLY;
}

/**
 * Research-only BPP peers must not appear in owner/founder ADP report selectors.
 * @param {object|null|undefined} profile
 * @param {{ hasStaged?: boolean, hasPublished?: boolean }=} artifacts
 */
export function isBppResearchOnlyPeer(profile, artifacts = {}) {
  return (
    classifyAdpPropertyRegistryRole(profile, artifacts) ===
    ADP_PROPERTY_REGISTRY_ROLES.BPP_RESEARCH_ONLY_PEER
  );
}

/**
 * Properties eligible for founder review / owner ADP report property dropdown.
 * Includes commissioned Full ADP hotels even when the staged report is still pending.
 * Excludes research-only BPP peers and census-only HPC rows.
 */
export function isOwnerAdpReportSelectorEligible(profile, artifacts = {}) {
  const role = classifyAdpPropertyRegistryRole(profile, artifacts);
  if (role === ADP_PROPERTY_REGISTRY_ROLES.BPP_RESEARCH_ONLY_PEER) return false;
  if (role === ADP_PROPERTY_REGISTRY_ROLES.HPC_CENSUS_ONLY) return false;
  if (profile?.ownerSelectorEligible === false && isBppResearchOnlyPeer(profile, artifacts)) {
    return false;
  }
  return (
    role === ADP_PROPERTY_REGISTRY_ROLES.FULL_ADP_REPORT_PROPERTY ||
    role === ADP_PROPERTY_REGISTRY_ROLES.PUBLISHED_ADP_PROPERTY ||
    role === ADP_PROPERTY_REGISTRY_ROLES.ADP_PREFLIGHT_CANDIDATE ||
    artifacts.hasStaged === true ||
    artifacts.hasPublished === true ||
    profile?.fullAdpReportCommissioned === true ||
    profile?.standaloneAdpReportAuthorized === true
  );
}

/**
 * Fields written onto draft peer Loyalty profiles so future E2E runs stay scoped.
 */
export function draftPeerRegistryScopeFields({ peerOf } = {}) {
  return {
    registryRole: ADP_PROPERTY_REGISTRY_ROLES.BPP_RESEARCH_ONLY_PEER,
    greenfieldStatus: "DRAFT_PEER_LOYALTY_ONLY",
    baselineState: "DRAFT_PEER_NOT_CERTIFIED",
    customerDropdownVisible: false,
    ownerSelectorEligible: false,
    founderReviewVisible: false,
    fullAdpReportCommissioned: false,
    standaloneAdpReportAuthorized: false,
    stewardship: {
      note: "Draft peer profile for authorized BPP Loyalty measurement only — not a customer-facing ADP report property.",
      peerOf: peerOf || null,
      registryScope: ADP_PROPERTY_REGISTRY_SCOPE_V1,
    },
  };
}
