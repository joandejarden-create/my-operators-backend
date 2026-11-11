#!/usr/bin/env node
/**
 * Regression: customer external reports must not block on Provider Presence
 * ↔ Consideration grain mismatch after serve-time repair.
 *
 * Covers the Hilton Times Square defect (eligible ≤53 / sumMismatch) and
 * asserts all 38 durable packages pass client-equivalent grain checks after enrich.
 */
import assert from "node:assert/strict";
import { readFileSync, readdirSync, existsSync } from "fs";
import { join } from "path";
import { enrichPayloadOptionalMetrics } from "../lib/ai-demand-positioning/published-read-service.js";
import {
  auditProviderPresenceGrain,
  repairProviderPresenceGrain,
} from "../lib/ai-demand-positioning/metrics/adp-provider-presence-grain-lock-v1.js";
import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";

const CURRENT = "data/adp-durable-published/current";
const SNAP = "data/adp-durable-published/snapshots";

function loadDurablePayload(propertyId) {
  const man = JSON.parse(
    readFileSync(join(CURRENT, propertyId, "manifest.json"), "utf8")
  );
  const path = join(SNAP, propertyId, man.reportFile);
  assert.ok(existsSync(path), `missing report for ${propertyId}`);
  const wrap = JSON.parse(readFileSync(path, "utf8"));
  return { man, payload: wrap.payload || wrap, wrap };
}

function clientWouldBlock(payload) {
  const em = payload?.executiveMetrics || {};
  const frozen =
    Array.isArray(em.considerationRate?.eligibleScenarioIds) &&
    em.considerationRate.eligibleScenarioIds.length > 0
      ? em.considerationRate.eligibleScenarioIds.length
      : 0;
  const spEligible =
    frozen > 0
      ? frozen
      : Number(em.scenarioPresence?.eligibleScenarios);
  const cr = em.considerationRate || {};
  const methId = cr.methodologyId || em.scenarioPresence?.methodologyId || "";
  const independentGrain =
    /INDEPENDENT_UNPROMPTED/i.test(String(methId)) ||
    (spEligible === 64 && Number(cr.comparableObservations) === 256);
  if (!independentGrain || !Array.isArray(payload?.evidence?.providers)) {
    return false;
  }
  const execPresent = Number(cr.presentObservations);
  const execComparable = Number(cr.comparableObservations);
  let mentionSum = 0;
  let comparableSum = 0;
  const badProv = payload.evidence.providers.filter((row) => {
    const c = Number(row.comparable != null ? row.comparable : row.total);
    const sched = Number(
      row.scenariosScheduled != null ? row.scenariosScheduled : row.scheduled
    );
    const m = Number(row.mentioned || 0);
    if (Number.isFinite(m)) mentionSum += m;
    if (Number.isFinite(c)) comparableSum += c;
    return (
      c === 78 ||
      sched === 78 ||
      (spEligible > 0 && Number.isFinite(c) && c > spEligible) ||
      (spEligible > 0 && Number.isFinite(sched) && sched > spEligible)
    );
  });
  const sumMismatch =
    (Number.isFinite(execPresent) && mentionSum !== execPresent) ||
    (Number.isFinite(execComparable) && comparableSum !== execComparable);
  return Boolean(badProv.length || sumMismatch);
}

// --- Hilton package is defective before repair ---
{
  const { payload, man } = loadDurablePayload("adp_hilton_times_square");
  const before = auditProviderPresenceGrain(payload);
  assert.equal(before.pass, false, "Hilton durable package must fail grain audit (regression fixture)");
  assert.equal(
    payload.executiveMetrics.scenarioPresence.eligibleScenarios,
    53,
    "Hilton SP eligible stays 53 in raw package"
  );
  assert.equal(
    payload.executiveMetrics.considerationRate.eligibleScenarioIds.length,
    47,
    "Hilton Consideration frozen ids = 47"
  );
  assert.equal(clientWouldBlock(payload), true, "raw Hilton package would client-block");

  const period = loadPeriod(man.latestPeriodId);
  const profile = loadPropertyProfile("adp_hilton_times_square");
  const repaired = repairProviderPresenceGrain("adp_hilton_times_square", payload, {
    period,
    propertyProfile: profile,
  });
  assert.equal(repaired.repaired, true);
  assert.equal(
    repaired.reason,
    "rebuilt_providers_from_frozen_consideration_eligible_ids"
  );
  assert.equal(repaired.auditAfter.pass, true, "Hilton repair must pass grain audit");
  assert.equal(
    repaired.payload.executiveMetrics.scenarioPresence.eligibleScenarios,
    47,
    "repair aligns SP eligible to frozen Consideration ids"
  );
  assert.equal(
    clientWouldBlock(repaired.payload),
    false,
    "repaired Hilton must not client-block"
  );
  console.log("PASS hilton frozen-id repair");
}

// --- All 38 after enrichPayloadOptionalMetrics ---
{
  const hotels = readdirSync(CURRENT).filter((d) => d.startsWith("adp_"));
  assert.equal(hotels.length, 38, "expected 38 durable current hotels");
  const fails = [];
  for (const pid of hotels) {
    const { payload } = loadDurablePayload(pid);
    const enriched = enrichPayloadOptionalMetrics(pid, structuredClone(payload));
    const audit = auditProviderPresenceGrain(enriched);
    const blocked = clientWouldBlock(enriched);
    const grainBlocked = enriched?._adpProviderPresenceGrainBlocked?.blocked === true;
    if (!audit.pass || blocked || grainBlocked) {
      fails.push({
        propertyId: pid,
        auditPass: audit.pass,
        clientBlocked: blocked,
        grainBlocked,
        mentionSum: audit.mentionSum,
        execPresent:
          enriched?.executiveMetrics?.considerationRate?.presentObservations ?? null,
        comparableSum: audit.comparableSum,
        execComparable:
          enriched?.executiveMetrics?.considerationRate?.comparableObservations ?? null,
      });
    }
  }
  assert.deepEqual(fails, [], `enrich must yield 38/38 renderable; fails=${JSON.stringify(fails)}`);
  console.log("PASS 38/38 enrich grain + client block checks");
}

console.log("OK test-adp-external-report-provider-presence-grain-v1");
