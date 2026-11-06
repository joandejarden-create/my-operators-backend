/**
 * Canonical ADP hotel selector for founder local review.
 * Includes every eligible ADP hotel regardless of customerDropdownVisible.
 * Customer properties endpoint must NOT use this — keep entitlement filters intact.
 */

import { existsSync, readdirSync, readFileSync } from "fs";
import { join } from "path";
import {
  formatPropertyLocationLine,
  formatPropertySelectorLabel,
} from "../../dealality/property-display-label.js";
import { loadPropertyProfile } from "../data-model.js";
import { loadPublishedManifest } from "../published-snapshot.js";
import { loadStagedManifest } from "./adp-staged-report-store-v1.js";
import {
  getHotelPublicationRecord,
  loadPublicationRegistry,
} from "./adp-publication-registry-v1.js";
import {
  ADP_RESEARCH_EXCLUDED_PROPERTY_IDS,
  isAdpResearchExcluded,
  isAdpPortfolioEligible,
} from "../contracts/adp-portfolio-eligibility-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "../governance/adp-operational-registry-hygiene-v1.js";
import {
  classifyAdpPropertyRegistryRole,
  isBppResearchOnlyPeer,
  isOwnerAdpReportSelectorEligible,
  ADP_PROPERTY_REGISTRY_ROLES,
} from "../governance/adp-property-registry-scope-v1.js";

export const ADP_REVIEWER_PROPERTY_SELECTOR_V1 = "adp_reviewer_property_selector_v1";
export {
  classifyAdpPropertyRegistryRole,
  isBppResearchOnlyPeer,
  isOwnerAdpReportSelectorEligible,
  ADP_PROPERTY_REGISTRY_ROLES,
};

/** Re-export shared eligibility contract (empty exclusions under 31/31 policy). */
export { ADP_RESEARCH_EXCLUDED_PROPERTY_IDS, isAdpResearchExcluded, isAdpPortfolioEligible };

function publishedPeriodId(manifest) {
  if (!manifest || typeof manifest !== "object") return null;
  return (
    manifest.latestPeriodId ||
    manifest.periodId ||
    manifest.currentPeriodId ||
    manifest.reportVersion ||
    null
  );
}

const INVENTORY_CANDIDATES = [
  "reports/dealality-adp-full-e2e-all-hotels-v1/ADP_HOTEL_INVENTORY.json",
  "reports/dealality-full-e2e-operating-system-v1/ADP_HOTEL_INVENTORY.json",
];

function loadCanonicalInventory(cwd = process.cwd()) {
  for (const rel of INVENTORY_CANDIDATES) {
    const p = join(cwd, rel);
    if (!existsSync(p)) continue;
    try {
      return JSON.parse(readFileSync(p, "utf8"));
    } catch {
      /* next */
    }
  }
  return null;
}

/**
 * All fixture property profiles including customerDropdownVisible:false.
 */
export function listAllAdpFixtureProfiles(cwd = process.cwd()) {
  const fixturesDir = join(cwd, "fixtures/ai-demand-positioning");
  if (!existsSync(fixturesDir)) return [];
  const files = readdirSync(fixturesDir).filter((f) =>
    f.endsWith("-property-profile.json")
  );
  const profiles = [];
  for (const file of files) {
    try {
      const data = JSON.parse(readFileSync(join(fixturesDir, file), "utf8"));
      if (!data.propertyId || !data.name) continue;
      profiles.push(data);
    } catch {
      /* skip bad fixture */
    }
  }
  return profiles.sort((a, b) => String(a.name).localeCompare(String(b.name)));
}

function availableReviewActions(row) {
  const actions = ["review_staged", "view_published", "compare_versions"];
  if (row.stagedReportVersion) {
    actions.push("request_changes");
    if (
      row.certificationStatus === "CERTIFIED" ||
      row.certificationStatus === "READY_FOR_REVIEW" ||
      row.publicationState === "STAGED" ||
      row.publicationState === "READY_FOR_REVIEW"
    ) {
      actions.push("approve_for_publication");
    }
  }
  if (row.publicationState === "APPROVED_FOR_PUBLICATION") {
    actions.push("publish");
  }
  return actions;
}

function resolveNextAction(row) {
  if (row.eligibility === "EXCLUDED" || row.researchEligibility === "EXCLUDED") {
    if (row.hasStaged || row.hasPublished) return "HISTORICAL_REVIEW_ONLY";
    return "SKIP_EXCLUDED_UNPROCESSED";
  }
  if (!row.marketRegistryPresent && row.competitiveCertification === "NOT_CERTIFIED") {
    return "BUILD_GOVERNED_MARKET_REGISTRY";
  }
  if (row.publicationState === "CERTIFICATION_FAILED") return "REPAIR_CERTIFICATION";
  if (row.publicationState === "CHANGES_REQUESTED") return "ADDRESS_REVIEW_COMMENTS";
  if (row.publicationState === "APPROVED_FOR_PUBLICATION") {
    return "AWAITING_PUBLICATION_AUTHORIZATION";
  }
  if (row.publicationState === "PUBLISHED" || row.publicationState === "DELIVERY_VERIFIED") {
    return "MONITOR_OR_NEXT_PERIOD";
  }
  if (row.stagedReportVersion) return "REVIEW_STAGED_REPORT";
  if (row.publishedReportVersion) return "STAGE_LATEST_FROM_PIPELINE";
  return "RUN_ADP_FULL_E2E";
}

/**
 * Build reviewer hotel selector entries from inventory + fixtures + registry.
 *
 * Concepts (kept separate):
 * - researchEligibility / eligibility: may run new ADP Full E2E (ELIGIBLE|EXCLUDED)
 * - founderReviewVisible: show in authorized local review dropdown
 * - customerDropdownVisible: customer entitlement surface (untouched here)
 * - registryRole: FULL_ADP_REPORT_PROPERTY vs BPP_RESEARCH_ONLY_PEER (etc.)
 *
 * @param {{ cwd?: string, includeExcluded?: boolean, includeHistoricalExcluded?: boolean, includeResearchOnlyPeers?: boolean }} opts
 *   includeExcluded — include research-excluded hotels even with no report (AI Admin)
 *   includeHistoricalExcluded — include research-excluded hotels that have staged/published
 *     historical artifacts (default true for founder review). Does not authorize new research.
 *   includeResearchOnlyPeers — include BPP research-only peers (admin/research tools only;
 *     default false so owner/founder ADP report selectors stay commissioned-report scoped).
 */
export function listAdpReviewerProperties({
  cwd = process.cwd(),
  includeExcluded = false,
  includeHistoricalExcluded = true,
  includeResearchOnlyPeers = false,
} = {}) {
  const inventory = loadCanonicalInventory(cwd);
  const byId = new Map();

  // Seed from fixture profiles (including customerDropdownVisible:false Full ADP drafts).
  // BPP research-only peers are seeded only when includeResearchOnlyPeers is set —
  // otherwise they remain loadable via loadPropertyProfile for Loyalty/BPP math.
  // Synthetic / smoke-test IDs never enter operational AI Admin discovery.
  for (const profile of listAllAdpFixtureProfiles(cwd)) {
    const id = profile.propertyId;
    if (isAdpSyntheticOrTestPropertyId(id)) continue;
    if (
      !includeResearchOnlyPeers &&
      isBppResearchOnlyPeer(profile, { hasStaged: false, hasPublished: false })
    ) {
      continue;
    }
    const city = profile.city || "";
    const state = profile.state || "";
    const region = profile.region || "";
    const country = profile.country || "";
    const registryRole = classifyAdpPropertyRegistryRole(profile, {
      hasStaged: false,
      hasPublished: false,
    });
    byId.set(id, {
      propertyId: id,
      hotelName: profile.name,
      name: profile.name,
      hpcId: profile.censusRecordId || profile.hpcId || null,
      market: profile.market || profile.city || "",
      city,
      state,
      region,
      country,
      locationLine: formatPropertyLocationLine({ city, state, region, country }),
      label: formatPropertySelectorLabel({
        name: profile.name,
        city,
        state,
        region,
        country,
      }),
      optionLabel: null,
      customerDropdownVisible: profile.customerDropdownVisible !== false,
      registryRole,
      researchEligibility: isAdpResearchExcluded(id) ? "EXCLUDED" : "ELIGIBLE",
      eligibility: isAdpResearchExcluded(id) ? "EXCLUDED" : "ELIGIBLE",
      exclusionReason: isAdpResearchExcluded(id)
        ? "Listed in ADP_RESEARCH_EXCLUDED_PROPERTY_IDS"
        : null,
      marketRegistryPresent: null,
      competitiveCertification: null,
      measurementPeriod: null,
      latestRunId: null,
      stagedReportVersion: null,
      publishedReportVersion: null,
      certificationStatus: null,
      publicationState: null,
      publicationStatus: null,
      availableReviewActions: [],
      nextAction: null,
      surface: "review",
    });
  }

  // Overlay canonical inventory (authoritative eligibility + HPC + registry)
  if (inventory?.hotels?.length) {
    for (const h of inventory.hotels) {
      const id = h.adpPropertyId;
      if (!id || isAdpSyntheticOrTestPropertyId(id)) continue;
      const prior = byId.get(id) || {
        propertyId: id,
        hotelName: h.hotelName,
        name: h.hotelName,
        city: "",
        state: "",
        region: "",
        country: "",
        customerDropdownVisible: false,
      };
      const profile = loadPropertyProfile(id) || {};
      const city = prior.city || profile.city || "";
      const state = prior.state || profile.state || "";
      const region = prior.region || profile.region || "";
      const country = prior.country || profile.country || "";
      // Inventory sometimes stores adp_* as hotelName — prefer fixture display name
      const invName = String(h.hotelName || "").trim();
      const invNameIsId = !invName || invName === id || invName.startsWith("adp_");
      const name =
        (!invNameIsId && invName) ||
        prior.hotelName ||
        profile.name ||
        (!invNameIsId ? invName : null) ||
        id;
      byId.set(id, {
        ...prior,
        propertyId: id,
        hotelName: name,
        name,
        hpcId: h.hpcId || prior.hpcId || null,
        market: h.market || prior.market || profile.market || city || "",
        city,
        state,
        region,
        country,
        locationLine: formatPropertyLocationLine({ city, state, region, country }),
        label: formatPropertySelectorLabel({ name, city, state, region, country }),
        // Shared eligibility contract wins over stale inventory EXCLUDED rows.
        researchEligibility: isAdpResearchExcluded(id)
          ? "EXCLUDED"
          : "ELIGIBLE",
        eligibility: isAdpResearchExcluded(id) ? "EXCLUDED" : "ELIGIBLE",
        exclusionReason: isAdpResearchExcluded(id)
          ? h.exclusionReason ||
            prior.exclusionReason ||
            "Listed in ADP_RESEARCH_EXCLUDED_PROPERTY_IDS"
          : null,
        marketRegistryPresent: h.marketRegistryPresent ?? prior.marketRegistryPresent,
        competitiveCertification: h.competitiveGate?.certified
          ? "CERTIFIED"
          : h.marketRegistryPresent === false
            ? "NOT_CERTIFIED"
            : prior.competitiveCertification,
        measurementPeriod: h.runtime?.periodId || prior.measurementPeriod,
        latestRunId: h.runtime?.periodId || prior.latestRunId,
        observationCount: h.observationCount ?? null,
      });
    }
  }

  // Published manifests for hotels not in fixtures
  const publishedRoot = join(cwd, "data/ai-demand-positioning/published");
  if (existsSync(publishedRoot)) {
    for (const d of readdirSync(publishedRoot)) {
      if (!d.startsWith("adp_") || byId.has(d)) continue;
      if (isAdpSyntheticOrTestPropertyId(d)) continue;
      const manifest = loadPublishedManifest(d);
      if (!manifest) continue;
      const name = manifest.propertyName || d;
      byId.set(d, {
        propertyId: d,
        hotelName: name,
        name,
        hpcId: null,
        market: manifest.market || "",
        city: manifest.city || "",
        state: manifest.state || "",
        region: "",
        country: manifest.country || "",
        locationLine: formatPropertyLocationLine({
          city: manifest.city,
          state: manifest.state,
          country: manifest.country,
        }),
        label: formatPropertySelectorLabel({
          name,
          city: manifest.city,
          state: manifest.state,
          country: manifest.country,
        }),
        customerDropdownVisible: false,
        researchEligibility: isAdpResearchExcluded(d) ? "EXCLUDED" : "ELIGIBLE",
        eligibility: isAdpResearchExcluded(d) ? "EXCLUDED" : "ELIGIBLE",
        exclusionReason: isAdpResearchExcluded(d)
          ? "Listed in ADP_RESEARCH_EXCLUDED_PROPERTY_IDS"
          : null,
        marketRegistryPresent: null,
        competitiveCertification: null,
        measurementPeriod: manifest.periodId || null,
        latestRunId: manifest.periodId || null,
        stagedReportVersion: null,
        publishedReportVersion: manifest.periodId || null,
        certificationStatus: null,
        publicationState: null,
        publicationStatus: null,
        availableReviewActions: [],
        nextAction: null,
        surface: "review",
      });
    }
  }

  const reg = loadPublicationRegistry(cwd);
  const out = [];

  for (const row of byId.values()) {
    if (isAdpSyntheticOrTestPropertyId(row.propertyId)) continue;
    const staged = loadStagedManifest(row.propertyId, cwd);
    const published = loadPublishedManifest(row.propertyId);
    const pubRec = reg.hotels?.[row.propertyId] || getHotelPublicationRecord(row.propertyId, cwd);

    row.researchEligibility =
      row.researchEligibility ||
      (isAdpResearchExcluded(row.propertyId) ? "EXCLUDED" : "ELIGIBLE");
    row.eligibility = row.researchEligibility;

    row.stagedReportVersion =
      staged?.latestStagedVersionId || pubRec?.stagedReportVersion || null;
    row.publishedReportVersion =
      publishedPeriodId(published) ||
      pubRec?.publishedReportVersion ||
      null;
    row.measurementPeriod =
      staged?.periodId ||
      publishedPeriodId(published) ||
      pubRec?.measurementPeriod ||
      row.measurementPeriod;
    row.latestRunId =
      staged?.runId || pubRec?.runId || row.latestRunId || row.measurementPeriod;
    row.hasStaged = Boolean(row.stagedReportVersion);
    row.hasPublished = Boolean(row.publishedReportVersion);
    row.hasHistoricalReport = row.hasStaged || row.hasPublished;

    const profile = loadPropertyProfile(row.propertyId) || {
      propertyId: row.propertyId,
      name: row.hotelName || row.name,
    };
    const artifacts = {
      hasStaged: row.hasStaged,
      hasPublished: row.hasPublished,
    };
    row.registryRole = classifyAdpPropertyRegistryRole(profile, artifacts);

    // Research-only BPP peers: keep out of owner/founder report selectors by default.
    // Admin/research tools may pass includeResearchOnlyPeers=true.
    if (
      !includeResearchOnlyPeers &&
      (row.registryRole === ADP_PROPERTY_REGISTRY_ROLES.BPP_RESEARCH_ONLY_PEER ||
        !isOwnerAdpReportSelectorEligible(profile, artifacts))
    ) {
      continue;
    }

    // Research-excluded with no artifact: AI Admin (includeExcluded) only.
    // Research-excluded WITH historical report: founder review by default.
    if (row.researchEligibility === "EXCLUDED") {
      if (includeExcluded) {
        /* keep */
      } else if (includeHistoricalExcluded && row.hasHistoricalReport) {
        /* keep — historical founder review */
      } else {
        continue;
      }
    }

    row.certificationStatus =
      pubRec?.certificationResult?.passed === false
        ? "CERTIFICATION_FAILED"
        : pubRec?.certificationResult?.passed === true
          ? "CERTIFIED"
          : staged
            ? "STAGED"
            : published
              ? "PUBLISHED_ARTIFACT"
              : "NO_REPORT";
    row.publicationState =
      pubRec?.publicationState ||
      (staged ? "STAGED" : published ? "PUBLISHED" : "RESEARCH_IN_PROGRESS");
    row.publicationStatus = row.publicationState;
    // Prefer staged for review when present; otherwise historical published.
    row.defaultReviewVersion = row.hasStaged
      ? "staged"
      : row.hasPublished
        ? "published"
        : null;
    row.founderReviewVisible =
      row.registryRole !== ADP_PROPERTY_REGISTRY_ROLES.BPP_RESEARCH_ONLY_PEER;
    row.ownerSelectorEligible = row.founderReviewVisible;
    row.newResearchAuthorized = row.researchEligibility === "ELIGIBLE";
    row.optionLabel = `${row.label}${
      row.stagedReportVersion
        ? " · staged"
        : row.publishedReportVersion
          ? " · published (historical)"
          : " · no staged report"
    }`;
    row.availableReviewActions = availableReviewActions(row);
    row.nextAction = resolveNextAction(row);
    out.push(row);
  }

  return out.sort((a, b) => String(a.hotelName || a.name).localeCompare(String(b.hotelName || b.name)));
}

export function getAdpReviewerProperty(propertyId, opts = {}) {
  const id = String(propertyId || "").trim();
  return listAdpReviewerProperties(opts).find((p) => p.propertyId === id) || null;
}
