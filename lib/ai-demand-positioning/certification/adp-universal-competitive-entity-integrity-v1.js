/**
 * ADP_UNIVERSAL_COMPETITIVE_ENTITY_INTEGRITY_V1
 *
 * Permanent competitive-intelligence integrity contract for the full ADP portfolio.
 * Fail-closed gates for CORE inflation, cross-market contamination, and non-hotel entities.
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile } from "../data-model.js";
import { isCrossMarketLeak } from "../customer/customer-entity-resolution-v1.js";
import { isNonHotelEntity } from "../intelligence/competitor-name-resolution.js";
import { coreIdsForIntent } from "../metrics/presence-benchmark-v1.js";
import { getCanonicalHotelsForProperty } from "../metrics/adp-property-entity-registries.js";
import { AUTONOMOUS_CORE_PEER_CAP } from "./adp-autonomous-competitor-entity-resolution-v1.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { buildIndependentReplacementBank } from "../prompt-universe/adp-independent-unprompted-core-universe-v1.js";

export const ADP_UNIVERSAL_COMPETITIVE_ENTITY_INTEGRITY_V1 =
  "ADP_UNIVERSAL_COMPETITIVE_ENTITY_INTEGRITY_V1";

export const COMPETITIVE_ENTITY_CLASSES = Object.freeze({
  CERTIFIED_COMPETITIVE_SET: "CERTIFIED_COMPETITIVE_SET",
  ORGANIC_VALID_HOTEL: "ORGANIC_VALID_HOTEL",
  OUT_OF_MARKET_HOTEL: "OUT_OF_MARKET_HOTEL",
  INVALID_ENTITY: "INVALID_ENTITY",
  DUPLICATE_ALIAS: "DUPLICATE_ALIAS",
  UNRESOLVED_CANDIDATE: "UNRESOLVED_CANDIDATE",
});

export const COMPETITIVE_INTEGRITY_CUSTOMER_READY_REQUIREMENTS = Object.freeze([
  "NO_CROSS_MARKET_CORE_PEERS",
  "NO_NON_HOTEL_CORE_PEERS",
  "AUTONOMOUS_CORE_PEER_CAP_RESPECTED",
  "NO_HARDCODED_FOREIGN_CONVENTION_PROMPTS",
  "COMPETITIVE_CLASSES_SEPARATED_IN_PRESENTATION",
  "EXCLUSION_LEDGER_PRESERVED",
]);

/**
 * Audit durable registry + prompt bank + CORE peer set for one property.
 * @returns {{ ok: boolean, failures: string[], warnings: string[], metrics: object }}
 */
export function assessCompetitiveEntityIntegrity(propertyId, { cwd = process.cwd() } = {}) {
  const failures = [];
  const warnings = [];
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    return {
      ok: false,
      contract: ADP_UNIVERSAL_COMPETITIVE_ENTITY_INTEGRITY_V1,
      propertyId,
      failures: ["MISSING_PROPERTY_PROFILE"],
      warnings: [],
      metrics: {},
    };
  }

  const hotels = getCanonicalHotelsForProperty(propertyId) || [];
  const peers = hotels.filter((h) => h && !h.subject);
  const coreEligible = peers.filter((h) => h.coreEligible !== false);
  const coreIds = coreIdsForIntent(TRAVELER_INTENTS.LEISURE, profile);

  // Cross-market / non-hotel in CORE-eligible set
  let crossMarketCore = 0;
  let nonHotelCore = 0;
  for (const h of peers.filter((p) => p.coreEligible === true || (p.coreEligible !== false && coreIds.includes(p.entityId)))) {
    const name = h.canonical || "";
    if (isCrossMarketLeak(name, profile)) {
      crossMarketCore += 1;
      failures.push(`CROSS_MARKET_CORE_PEER:${name}`);
    }
    if (isNonHotelEntity(name)) {
      nonHotelCore += 1;
      failures.push(`NON_HOTEL_CORE_PEER:${name}`);
    }
  }

  // Autonomous / greenfield registries must respect CORE peer cap when coreEligible is explicit
  const hasExplicitCoreFlags = peers.some((h) => typeof h.coreEligible === "boolean");
  if (hasExplicitCoreFlags && coreEligible.filter((h) => h.coreEligible === true).length > AUTONOMOUS_CORE_PEER_CAP) {
    failures.push(
      `AUTONOMOUS_CORE_PEER_CAP_EXCEEDED:${coreEligible.filter((h) => h.coreEligible === true).length}>${AUTONOMOUS_CORE_PEER_CAP}`
    );
  }
  if (!hasExplicitCoreFlags && peers.length > AUTONOMOUS_CORE_PEER_CAP + 5) {
    warnings.push(
      `LEGACY_REGISTRY_WITHOUT_CORE_ELIGIBLE_FLAGS:${peers.length}_peers — force-resynthesize recommended`
    );
  }

  // Convention prompt bank must not hardcode foreign cities
  const bank = buildIndependentReplacementBank(profile);
  const subjectCity = String(profile.city || profile.market || "").toLowerCase();
  const foreignConvention = bank.filter((b) => {
    const q = String(b.query || "");
    if (!/convention|exhibition|trade show/i.test(q)) return false;
    if (/guadalajara/i.test(q) && !/guadalajara/i.test(subjectCity)) return true;
    if (/cancun|cancún/i.test(q) && !/cancun|cancún/i.test(subjectCity)) return true;
    return false;
  });
  if (foreignConvention.length) {
    failures.push(`HARDCODED_FOREIGN_CONVENTION_PROMPTS:${foreignConvention.length}`);
  }

  // Staged territory CORE dump — enforce against autonomous/greenfield registries.
  // Curated static CORE sets (e.g. Waterstone) may legitimately exceed the autonomous cap.
  const stagedDir = join(cwd, "data/ai-demand-positioning/staged", propertyId);
  let stagedCoreCount = null;
  const durableRegPath = join(
    cwd,
    "data/ai-demand-positioning/competitor-entity-registries",
    propertyId,
    "registry-v1.json"
  );
  const isAutonomousOrGreenfield =
    Boolean(profile.greenfield) || existsSync(durableRegPath) || hasExplicitCoreFlags;
  if (existsSync(join(stagedDir, "manifest.json"))) {
    try {
      const man = JSON.parse(readFileSync(join(stagedDir, "manifest.json"), "utf8"));
      const ver = man.latestStagedVersionId || man.latestVersionId || man.versionId;
      const fp = ver ? join(stagedDir, `report-${ver}.json`) : null;
      if (fp && existsSync(fp)) {
        const staged = JSON.parse(readFileSync(fp, "utf8"));
        const payload = staged.payload || staged;
        const leisure = payload?.competitiveRankingByTerritory?.byTerritory?.leisure;
        stagedCoreCount = leisure?.coreIds?.length ?? null;
        const allowedCore = hasExplicitCoreFlags
          ? peers.filter((h) => h.coreEligible === true).length
          : AUTONOMOUS_CORE_PEER_CAP;
        if (
          isAutonomousOrGreenfield &&
          stagedCoreCount != null &&
          stagedCoreCount > Math.max(allowedCore, AUTONOMOUS_CORE_PEER_CAP) + 3
        ) {
          failures.push(`STAGED_TERRITORY_CORE_INFLATED:${stagedCoreCount}`);
        } else if (stagedCoreCount != null && stagedCoreCount > 60) {
          warnings.push(`STAGED_TERRITORY_CORE_LARGE:${stagedCoreCount}`);
        }
      }
    } catch {
      warnings.push("STAGED_MANIFEST_UNREADABLE");
    }
  }

  return {
    ok: failures.length === 0,
    contract: ADP_UNIVERSAL_COMPETITIVE_ENTITY_INTEGRITY_V1,
    propertyId,
    failures,
    warnings,
    metrics: {
      registryPeerCount: peers.length,
      coreEligibleTrueCount: peers.filter((h) => h.coreEligible === true).length,
      coreIdsLeisureCount: coreIds.length,
      crossMarketCore,
      nonHotelCore,
      foreignConventionPrompts: foreignConvention.length,
      stagedCoreCount,
      autonomousCorePeerCap: AUTONOMOUS_CORE_PEER_CAP,
    },
  };
}

export function describeCompetitiveEntityIntegrityStandard() {
  return {
    contract: ADP_UNIVERSAL_COMPETITIVE_ENTITY_INTEGRITY_V1,
    entityClasses: COMPETITIVE_ENTITY_CLASSES,
    customerReadyRequirements: COMPETITIVE_INTEGRITY_CUSTOMER_READY_REQUIREMENTS,
    autonomousCorePeerCap: AUTONOMOUS_CORE_PEER_CAP,
    presentationContract: {
      primaryDisplay: "TOP_10_OBSERVED_PLUS_CORE_ELIGIBLE_PEERS",
      certifiedSet: "coreEligible registry peers",
      corePeerCapMeaning:
        "AUTONOMOUS_CORE_PEER_CAP is a documented evidence-frequency selection for territory CORE rankings — not the total verified competitor population; remaining peers stay OBSERVED_ALTERNATIVE_NOT_IN_CORE",
      organicMentions: "OBSERVED_ALTERNATIVE — expandable, not dumped as CORE",
      rejected: "exclusion ledger / audit only",
    },
  };
}
