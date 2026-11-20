/**
 * Factory run configuration + fingerprint for deterministic run identity.
 */

import crypto from "node:crypto";
import fs from "node:fs";

export const HIF_CONFIG_V1 = "hotel-intelligence-factory-config-v1";

export const DEFAULT_CONFIG = Object.freeze({
  schemaVersion: "4.0.0-factory",
  budgetUsd: 25,
  /** Research is always global; this only prioritizes HPC enrichment effort. */
  researchCoverage: "GLOBAL",
  geographicPriority: "PRIORITY_HPC_ENRICHMENT",
  hpcEnrichmentRegions: ["AMERICAS", "EUROPE", "CARIBBEAN"],
  providers: {
    secApiIo: true,
    exa: true,
    freeEdgar: true,
  },
  recurring: {
    enabled: false,
    useSameEngine: true,
    dedupeProviderFingerprints: true,
    note: "Future schedules must call the same CLI/engine — no second pipeline",
  },
  research: {
    batchSize: 100,
    maxBatches: 5,
    futureYearMin: 2027,
    futureYearMax: 2040,
  },
  promotion: {
    pilotMaxCreates: 50,
    requireManifestSha256: true,
    unpublishedDefault: true,
    queueHotelExplorerEnrichment: true,
  },
  publication: {
    defaultState: "CUSTOMER_BLOCKED",
    authorizeSeparately: true,
  },
});

export function loadFactoryConfig(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    return { ...DEFAULT_CONFIG, source: "DEFAULT" };
  }
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return {
    ...DEFAULT_CONFIG,
    ...raw,
    research: { ...DEFAULT_CONFIG.research, ...(raw.research || {}) },
    promotion: { ...DEFAULT_CONFIG.promotion, ...(raw.promotion || {}) },
    providers: { ...DEFAULT_CONFIG.providers, ...(raw.providers || {}) },
    publication: { ...DEFAULT_CONFIG.publication, ...(raw.publication || {}) },
    source: filePath,
  };
}

export function configFingerprint(config) {
  const stable = {
    schemaVersion: config.schemaVersion,
    budgetUsd: config.budgetUsd,
    geographicPriority: config.geographicPriority,
    providers: config.providers,
    research: config.research,
    promotion: {
      pilotMaxCreates: config.promotion?.pilotMaxCreates,
      requireManifestSha256: config.promotion?.requireManifestSha256,
    },
    publication: config.publication,
  };
  return crypto.createHash("sha256").update(JSON.stringify(stable)).digest("hex").slice(0, 12);
}

export function buildRunId(mode, config, now = new Date()) {
  const day = now.toISOString().slice(0, 10).replace(/-/g, "");
  const fp = configFingerprint(config);
  const modeSlug = String(mode || "research").toLowerCase().replace(/_/g, "");
  return `hif_${modeSlug}_${day}_${fp}`;
}

export default { DEFAULT_CONFIG, loadFactoryConfig, configFingerprint, buildRunId };
