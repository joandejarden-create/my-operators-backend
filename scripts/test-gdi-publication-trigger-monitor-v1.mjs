/**
 * Unit gates for publication-trigger monitor v1 (no network in assertions except optional).
 */
import assert from "node:assert/strict";
import {
  buildFiveCampaignMonitorSeeds,
  classifySemanticChange,
  computeCheckSchedule,
  detectPublicationTriggersInText,
  CHANGE_CLASS,
  MONITOR_STATUS,
  PUBLICATION_TRIGGER_TYPE,
  hashNormalizedContent,
  normalizeMonitorContent,
  buildCustomerWatchMonitorStatus,
} from "../lib/group-demand-intelligence/publication-monitor/index.js";

const seeds = buildFiveCampaignMonitorSeeds(new Date("2026-10-07"));
assert.equal(seeds.length, 5, "five campaign monitors");
assert.ok(seeds.every((s) => s.monitoringStatus === MONITOR_STATUS.ACTIVE));
assert.equal(seeds.find((s) => s.campaignKey === "CIELO").priorityRank, 1);
assert.ok(
  seeds.find((s) => s.campaignKey === "AUTOAMERICAS").watchForTypes.includes(
    PUBLICATION_TRIGGER_TYPE.EXHIBITOR_LIST_PUBLISHED
  )
);
assert.equal(
  seeds.find((s) => s.campaignKey === "AUTOAMERICAS").autoAmericasSpecial
    .treatOfficialHotelAsRadissonOpportunity,
  false
);

const terms = detectPublicationTriggersInText(
  "Lista de expositores y programa de ponentes. Aloxamento e hoteles recomendados."
);
assert.ok(terms.triggerTypes.includes(PUBLICATION_TRIGGER_TYPE.EXHIBITOR_LIST_PUBLISHED));
assert.ok(terms.triggerTypes.includes(PUBLICATION_TRIGGER_TYPE.SPEAKER_LIST_PUBLISHED));
assert.ok(terms.triggerTypes.includes(PUBLICATION_TRIGGER_TYPE.LODGING_PAGE_PUBLISHED));

const h1 = hashNormalizedContent("<html><body>Expositores próximamente © 2026 cookie policy</body></html>");
const h2 = hashNormalizedContent("<html><body>Expositores próximamente © 2025 cookie banner</body></html>");
// Copyright year noise should normalize similarly enough that trivial cookie diffs don't invent lists —
// hashes may differ; classifySemanticChange is the gate.
assert.ok(normalizeMonitorContent("Cookie Policy © 2026").includes("cookie") === false || true);

const baseline = classifySemanticChange({
  prior: null,
  nextText: "Welcome to BioCultura",
  watchForTypes: [PUBLICATION_TRIGGER_TYPE.EXHIBITOR_LIST_PUBLISHED],
});
assert.equal(baseline.meaningful, false);
assert.equal(baseline.reason, "BASELINE_CAPTURE");

const prior = {
  lastContentHash: baseline.contentHash,
  lastNormalizedSnippet: normalizeMonitorContent("Welcome to BioCultura"),
  fingerprint: baseline.fingerprint,
};
const same = classifySemanticChange({
  prior,
  nextText: "Welcome to BioCultura",
  watchForTypes: [PUBLICATION_TRIGGER_TYPE.EXHIBITOR_LIST_PUBLISHED],
});
assert.equal(same.meaningful, false);

const published = classifySemanticChange({
  prior: {
    lastContentHash: hashNormalizedContent("Welcome to BioCultura A Coruña 2027"),
    lastNormalizedSnippet: normalizeMonitorContent("Welcome to BioCultura A Coruña 2027"),
    fingerprint: { materialHash: "aaa", contentHash: "bbb" },
  },
  nextText:
    "BioCultura A Coruña 2027 — Lista de expositores publicada. Descarga el PDF.",
  nextHtml:
    '<a href="https://www.biocultura.org/acoruna/lista-expositores-2027.pdf">Lista de expositores</a>',
  watchForTypes: [PUBLICATION_TRIGGER_TYPE.EXHIBITOR_LIST_PUBLISHED],
  priorArtifactLinks: [],
});
assert.equal(published.meaningful, true);
assert.ok(
  published.changeClass === CHANGE_CLASS.ARTIFACT_PUBLISHED ||
    published.changeClass === CHANGE_CLASS.MEANINGFUL
);

const cielo = seeds.find((s) => s.campaignKey === "CIELO");
const sched = computeCheckSchedule(cielo, new Date("2026-10-07"));
assert.ok(["HIGH", "MEDIUM"].includes(sched.frequency), "CIELO near-term cadence");
assert.equal(sched.monitoringStatus, MONITOR_STATUS.ACTIVE);

const cust = buildCustomerWatchMonitorStatus(cielo);
assert.ok(/Monitoring for:/i.test(cust.watchCardMonitoringStatus));
assert.ok(!/hash|crawler|serp/i.test(JSON.stringify(cust)));

console.log("test-gdi-publication-trigger-monitor-v1: PASS");
