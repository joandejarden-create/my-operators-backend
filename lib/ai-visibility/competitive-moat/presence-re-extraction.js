/**
 * Historical Presence re-extraction from stored responses — no provider calls.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  collectStoredResponses,
  commonCohortKey,
  matchBrandInText,
  DATASET_NAMESPACE,
  DEFAULT_RESPONSE_DIRS,
} from "./presence-corpus.js";
import {
  loadApprovedInternalAdditionsConfig,
  verifyApprovedInternalAdditions,
} from "./approved-internal-additions.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");
const ACTIVE_UNIVERSE_REPORT = path.join(
  ROOT,
  "reports",
  "brand-explorer-active-universe-source-of-truth.json"
);

export const RE_EXTRACTION_VERSION = "presence_re_extraction_v1";

function loadInventory() {
  if (!fs.existsSync(ACTIVE_UNIVERSE_REPORT)) return [];
  return JSON.parse(fs.readFileSync(ACTIVE_UNIVERSE_REPORT, "utf8")).inventory || [];
}

export function reExtractPresenceForApprovedAdditions(opts = {}) {
  const config = opts.config || loadApprovedInternalAdditionsConfig();
  const inventory = opts.inventory || loadInventory();
  const verification = verifyApprovedInternalAdditions(inventory, config);
  if (!verification.ok) {
    return {
      ok: false,
      error: "identity_verification_failed",
      verification,
      providerCalls: 0,
    };
  }

  const responses = collectStoredResponses(opts.responseDirs || DEFAULT_RESPONSE_DIRS);
  const brandResults = [];

  for (const add of config.additions || []) {
    const aliases = add.aliases || [add.brandName];
    let resolvedMentions = 0;
    let ambiguousMentions = 0;
    const observations = [];
    const dates = [];

    for (const resp of responses) {
      const { matched, ambiguous } = matchBrandInText(resp.text, aliases);
      if (ambiguous) ambiguousMentions += 1;
      if (matched) {
        resolvedMentions += 1;
        if (resp.timestamp) dates.push(resp.timestamp);
        observations.push({
          observationId: `reext_${add.brandId}_${resp.responseId}`,
          timestamp: resp.timestamp,
          preservedFromStoredResponse: true,
          synthetic: false,
          brandId: add.brandId,
          brandName: add.brandName,
          presence: 1,
          responseId: resp.responseId,
          waveId: resp.waveId,
          runId: resp.runId,
          provider: resp.provider,
          model: resp.model,
          promptId: resp.promptId,
          promptVersion: resp.promptVersion,
          language: resp.language,
          geography: resp.geography,
          intentTerritory: resp.intentTerritory,
          commonCohortKey: commonCohortKey(resp),
          datasetNamespace: DATASET_NAMESPACE,
        });
      }
    }

    dates.sort();
    const seriesAvailable =
      observations.length >= 3 ? "YES" : observations.length > 0 ? "PARTIAL" : "NO";

    brandResults.push({
      brand: add.brandName,
      brandId: add.brandId,
      parent: add.canonicalParent,
      responsesScanned: responses.length,
      resolvedMentions,
      ambiguousMentions,
      presenceObservations: observations.length,
      earliestRealObservationDate: dates[0] || null,
      latestRealObservationDate: dates[dates.length - 1] || null,
      historicalSeriesAvailable: seriesAvailable,
      presenceRate: responses.length ? resolvedMentions / responses.length : null,
      observations,
    });
  }

  return {
    ok: true,
    providerCalls: 0,
    spend: 0,
    reExtractionVersion: RE_EXTRACTION_VERSION,
    responsesScanned: responses.length,
    verification,
    brands: brandResults,
  };
}

export function buildPresenceObservationIndex(opts = {}) {
  const reext = reExtractPresenceForApprovedAdditions(opts);
  const index = new Map();

  if (reext.ok) {
    for (const b of reext.brands) {
      index.set(b.brandId, b.observations);
    }
  }

  const responses = collectStoredResponses(opts.responseDirs || DEFAULT_RESPONSE_DIRS);
  const extraBrands = opts.peerBrandAliases || [];
  for (const peer of extraBrands) {
    if (index.has(peer.brandId)) continue;
    const obs = [];
    for (const resp of responses) {
      const { matched } = matchBrandInText(resp.text, peer.aliases || [peer.brandName]);
      if (matched) {
        obs.push({
          observationId: `reext_${peer.brandId}_${resp.responseId}`,
          timestamp: resp.timestamp,
          preservedFromStoredResponse: true,
          synthetic: false,
          brandId: peer.brandId,
          presence: 1,
          responseId: resp.responseId,
          provider: resp.provider,
          promptId: resp.promptId,
          language: resp.language,
          geography: resp.geography,
          commonCohortKey: commonCohortKey(resp),
          datasetNamespace: DATASET_NAMESPACE,
        });
      }
    }
    index.set(peer.brandId, obs);
  }

  return { index, reext, responsesScanned: responses.length };
}
