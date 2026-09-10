#!/usr/bin/env node
/**
 * npm run test:adp-bpp-analytical-customer-parity
 *
 * Asserts analytical BPP state === published pack state === resolved customer payload.
 * Golden Casas: BPP_READY_POPULATED_RANK_ONLY must NEVER render as NOT SHOWN.
 */

import assert from "assert";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { listPublishedPropertyIds } from "../lib/ai-demand-positioning/published-snapshot.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { getBrandPortfolioPeerSet } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-peer-set-v1.js";
import { resolveBrandPortfolioPosition } from "../api/ai-demand-positioning.js";
import { isBrandPortfolioCustomerReady } from "../lib/ai-demand-positioning/brand-portfolio/build-brand-portfolio-position-payload-v1.js";
import {
  BPP_CUSTOMER_STATE,
  evaluateBppRankOnlyRenderContract,
  assertBppSectionVisibilityNotCoupledToBenchmark,
  ADP_BPP_ANALYTICAL_PUBLISHED_RENDERED_STATE_PARITY,
  ADP_BPP_RANK_ONLY_RENDER_CONTRACT,
  BPP_DISCLOSURE_REASON,
} from "../lib/ai-demand-positioning/brand-portfolio/adp-bpp-customer-state-v1.js";
import {
  BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE,
} from "../lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js";
import {
  resolveBppPopulationAttempt,
  evaluateBppClientReadinessGate,
  BPP_CLIENT_READY_CLASS,
} from "../lib/ai-demand-positioning/brand-portfolio/bpp-client-readiness-doctrine-v1.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const PACK = join(process.cwd(), BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE);

function classifyRendered(bpp) {
  if (!bpp) return { state: null, ready: false, suppression: true };
  if (bpp.bppCustomerState) {
    return {
      state: bpp.bppCustomerState,
      ready: bpp.status === "READY",
      suppression: bpp.status !== "READY",
    };
  }
  if (bpp.status === "READY" && (bpp.kpis || []).length) {
    const adequacy = bpp.peerSet?.adequacy;
    const rankOnly = adequacy === "RANK_ONLY_SUPPRESS_BENCHMARK_INDEX";
    return {
      state: rankOnly
        ? BPP_CUSTOMER_STATE.BPP_READY_POPULATED_RANK_ONLY
        : BPP_CUSTOMER_STATE.BPP_READY_POPULATED_FULL,
      ready: true,
      suppression: false,
    };
  }
  if (bpp.status === "EXCEPTION_SUPPRESSED") {
    return {
      state: BPP_CUSTOMER_STATE.BPP_EXCEPTION_SUPPRESSED,
      ready: false,
      suppression: true,
    };
  }
  return { state: BPP_CUSTOMER_STATE.BPP_EXCEPTION_SUPPRESSED, ready: false, suppression: true };
}

function main() {
  assert.ok(existsSync(PACK), "deployable BPP pack required");
  const pack = JSON.parse(readFileSync(PACK, "utf8"));
  assert.equal(pack.customerPublished, true);

  // RANK_ONLY render contract fixture
  const rankOnlyContract = evaluateBppRankOnlyRenderContract({
    peerCount: 4,
    rankEligible: true,
    benchmarkEligible: false,
    sectionVisible: true,
    peersVisible: true,
    rankMetricsVisible: true,
    benchmarkNumeric: false,
    indexNumeric: false,
    suppressionCard: false,
  });
  assert.equal(rankOnlyContract.pass, true, ADP_BPP_RANK_ONLY_RENDER_CONTRACT);

  // Casas golden regression
  const casasId = "adp_casas_del_xvi";
  const casasProfile = loadPropertyProfile(casasId);
  assert.ok(casasProfile, "Casas profile");
  const casasPeer = getBrandPortfolioPeerSet(casasId);
  assert.ok(casasPeer, "Casas peer set");
  assert.equal(casasPeer.peerCountExcludingSubject, 4);
  assert.equal(casasPeer.adequacy.canRank, true);
  assert.equal(casasPeer.adequacy.canBenchmark, false);

  const casasPack = pack.payloads?.[casasId];
  assert.ok(casasPack, "Casas must be in customer published BPP pack");
  assert.equal(casasPack.status, "READY");
  assert.ok((casasPack.kpis || []).length >= 4);
  assert.ok((casasPack.ranking?.rows || []).length >= 5);
  assert.equal(
    casasPack.bppCustomerState || casasPack.customerReadyClass,
    BPP_CUSTOMER_STATE.BPP_READY_POPULATED_RANK_ONLY
  );

  const casasResolved = resolveBrandPortfolioPosition(casasId, casasProfile, { query: {} });
  assert.equal(casasResolved.status, "READY");
  assert.equal(isBrandPortfolioCustomerReady(casasResolved), true);
  const casasRendered = classifyRendered(casasResolved);
  assert.equal(casasRendered.ready, true);
  assert.equal(casasRendered.suppression, false);
  assert.equal(casasRendered.state, BPP_CUSTOMER_STATE.BPP_READY_POPULATED_RANK_ONLY);

  const forbiddenCopy =
    "Brand & Portfolio benchmarking is not shown for this period because the governed affiliation and peer path are not yet resolved for display.";
  const casasBlob = JSON.stringify(casasResolved);
  assert.ok(!casasBlob.includes(forbiddenCopy), "Casas must not use affiliation-unresolved copy");
  assert.ok(!/Brand & Portfolio not shown/i.test(casasBlob), "Casas must not say NOT SHOWN");

  const peerNames = (casasPack.peerSet?.included || []).map((p) => p.peerHotel);
  for (const name of [
    "Kimpton Las Mercedes",
    "InterContinental Real Santo Domingo",
    "Holiday Inn Santo Domingo",
    "Crowne Plaza Santo Domingo",
  ]) {
    assert.ok(peerNames.includes(name), `peer ${name}`);
  }

  const kpiIds = (casasPack.kpis || []).map((k) => k.id);
  assert.ok(kpiIds.includes("portfolioRank"));
  assert.ok(!kpiIds.includes("portfolioBenchmark"));
  assert.ok(!kpiIds.includes("portfolioPresenceIndex"));

  const attempt = resolveBppPopulationAttempt({
    propertyId: casasId,
    profile: casasProfile,
    bppPayload: casasResolved,
  });
  assert.ok(
    attempt.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED_RANK_ONLY ||
      attempt.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED_FULL ||
      attempt.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED
  );
  assert.equal(evaluateBppClientReadinessGate(attempt).pass, true);

  // Full affiliated universe: no false suppressions
  const falseSuppressions = [];
  for (const propertyId of listPublishedPropertyIds().sort()) {
    const profile = loadPropertyProfile(propertyId);
    const peerSet = getBrandPortfolioPeerSet(propertyId);
    if (!peerSet || peerSet.adequacy?.canRank !== true) continue;
    const resolved = resolveBrandPortfolioPosition(propertyId, profile, { query: {} });
    const rendered = classifyRendered(resolved);
    if (rendered.suppression || !rendered.ready) {
      falseSuppressions.push({
        propertyId,
        status: resolved?.status,
        state: rendered.state,
      });
    }
    const vis = assertBppSectionVisibilityNotCoupledToBenchmark({
      peerCount: peerSet.peerCountExcludingSubject,
      benchmarkEligible: peerSet.adequacy.canBenchmark === true,
      sectionVisible: resolved?.sectionVisible !== false,
      customerState: rendered.state,
    });
    assert.equal(vis.pass, true, `${propertyId} visibility coupled to benchmark`);
  }
  assert.equal(falseSuppressions.length, 0, JSON.stringify(falseSuppressions));

  // UI must not use affiliation-unresolved copy when lens resolved
  const ui = readFileSync(UI, "utf8");
  assert.ok(/data-bpp-customer-state/.test(ui) || /POPULATED_RANK_ONLY/.test(ui));
  assert.ok(/benchmarkLimitation/.test(ui));
  assert.ok(/lens && bpp.lens.label/.test(ui));

  // Disclosure reason parity constant present
  assert.ok(BPP_DISCLOSURE_REASON.PEER_SET_INSUFFICIENT_FOR_BENCHMARK);

  console.log(
    JSON.stringify(
      {
        ok: true,
        gate: ADP_BPP_ANALYTICAL_PUBLISHED_RENDERED_STATE_PARITY,
        casasState: casasRendered.state,
        casasRank: (casasPack.kpis || []).find((k) => k.id === "portfolioRank")?.value,
        casasPeers: peerNames,
        falseSuppressions: 0,
        rankOnlyContract: rankOnlyContract.pass,
      },
      null,
      2
    )
  );
}

main();
