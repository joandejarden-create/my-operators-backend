/**
 * ADP_BPP_DEPLOYABLE_PACK_COMPLETE
 * ADP_NEW_PROPERTY_BPP_END_TO_END_PUBLICATION_GATE
 *
 * Every published property with READY_POPULATED_FULL or READY_POPULATED_RANK_ONLY
 * must exist in the deployable BPP customer pack. Prevents Casas-style silent miss.
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { listPublishedPropertyIds } from "../published-snapshot.js";
import { loadPropertyProfile } from "../data-model.js";
import { getBrandPortfolioPeerSet } from "../brand-portfolio/brand-portfolio-peer-set-v1.js";
import { classifyBppEligibility } from "../brand-portfolio/bpp-client-readiness-doctrine-v1.js";
import {
  BPP_CUSTOMER_STATE,
  classifyBppCustomerState,
} from "../brand-portfolio/adp-bpp-customer-state-v1.js";
import {
  BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE,
  BPP_CUSTOMER_PUBLISHED_PACK,
} from "../brand-portfolio/bpp-publication-meta-v1.js";

export const ADP_BPP_DEPLOYABLE_PACK_COMPLETE = "ADP_BPP_DEPLOYABLE_PACK_COMPLETE";
export const ADP_NEW_PROPERTY_BPP_END_TO_END_PUBLICATION_GATE =
  "ADP_NEW_PROPERTY_BPP_END_TO_END_PUBLICATION_GATE";

function readDeployablePack() {
  for (const rel of [BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE, BPP_CUSTOMER_PUBLISHED_PACK]) {
    const p = join(process.cwd(), rel);
    if (!existsSync(p)) continue;
    try {
      return { path: rel, pack: JSON.parse(readFileSync(p, "utf8")) };
    } catch {
      /* continue */
    }
  }
  return { path: null, pack: null };
}

function expectedAnalyticalState(propertyId, profile) {
  const eligibility = classifyBppEligibility(propertyId, profile);
  const peerSet = getBrandPortfolioPeerSet(propertyId);
  if (eligibility.independent) {
    return classifyBppCustomerState({
      affiliated: false,
      lensApplicable: false,
      affiliationResolved: true,
      peerCount: 0,
      payloadReady: false,
    });
  }
  if (!eligibility.mappingPresent) {
    return classifyBppCustomerState({
      affiliated: eligibility.affiliated,
      affiliationResolved: false,
      peerCount: 0,
      payloadReady: false,
    });
  }
  if (!peerSet) {
    return classifyBppCustomerState({
      affiliated: true,
      affiliationResolved: true,
      peerCount: 0,
      exceptionSuppressed: true,
      payloadReady: false,
    });
  }
  const a = peerSet.adequacy || {};
  return classifyBppCustomerState({
    affiliated: true,
    affiliationResolved: true,
    peerCount: peerSet.peerCountExcludingSubject,
    canRank: a.canRank === true,
    canBenchmark: a.canBenchmark === true,
    canIndex: a.canIndex === true,
    payloadReady: a.canRank === true,
    exceptionSuppressed: a.canRank !== true,
  });
}

export function auditBppDeployablePackCompleteness() {
  const publishedIds = listPublishedPropertyIds().sort();
  const { path: packPath, pack } = readDeployablePack();
  const packIds = Object.keys(pack?.payloads || {}).sort();
  const missingReady = [];
  const rows = [];

  for (const propertyId of publishedIds) {
    const profile = loadPropertyProfile(propertyId);
    const analytical = expectedAnalyticalState(propertyId, profile || { propertyId });
    const ready =
      analytical.customerState === BPP_CUSTOMER_STATE.BPP_READY_POPULATED_FULL ||
      analytical.customerState === BPP_CUSTOMER_STATE.BPP_READY_POPULATED_RANK_ONLY;
    const inPack = Boolean(pack?.payloads?.[propertyId]);
    const packReady = pack?.payloads?.[propertyId]?.status === "READY";
    if (ready && (!inPack || !packReady)) {
      missingReady.push({
        propertyId,
        analyticalState: analytical.customerState,
        inPack,
        packStatus: pack?.payloads?.[propertyId]?.status || null,
      });
    }
    rows.push({
      propertyId,
      analyticalState: analytical.customerState,
      readyExpected: ready,
      inPack,
      packStatus: pack?.payloads?.[propertyId]?.status || null,
      pass: !ready || (inPack && packReady),
    });
  }

  return {
    gate: ADP_BPP_DEPLOYABLE_PACK_COMPLETE,
    pass: missingReady.length === 0 && Boolean(pack),
    packPath,
    publishedCount: publishedIds.length,
    bppPackEntries: packIds.length,
    expectedReadyEntries: rows.filter((r) => r.readyExpected).length,
    missingEntries: missingReady,
    rows,
  };
}

export function assertNewPropertyBppEndToEndPublication({
  propertyId,
  publishedSnapshotExists,
  bppAnalyticalState,
  bppPackEntryExists,
  resolverState,
  rendererState,
} = {}) {
  const ready =
    bppAnalyticalState === BPP_CUSTOMER_STATE.BPP_READY_POPULATED_FULL ||
    bppAnalyticalState === BPP_CUSTOMER_STATE.BPP_READY_POPULATED_RANK_ONLY;
  const pass =
    publishedSnapshotExists === true &&
    Boolean(bppAnalyticalState) &&
    (!ready || bppPackEntryExists === true) &&
    resolverState === bppAnalyticalState &&
    rendererState === bppAnalyticalState;
  return {
    gate: ADP_NEW_PROPERTY_BPP_END_TO_END_PUBLICATION_GATE,
    propertyId: propertyId || null,
    pass,
    ready,
    checks: {
      publishedSnapshotExists,
      bppAnalyticalState,
      bppPackEntryExists,
      resolverState,
      rendererState,
    },
  };
}
