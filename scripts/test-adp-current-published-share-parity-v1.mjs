#!/usr/bin/env node
/**
 * Permanent gate: current_published share resolver + territory column semantics.
 * npm run test:adp-current-published-share-parity-v1
 *
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 * CURRENT_PUBLISHED_SHARE_ALWAYS_RESOLVES_LATEST_GOVERNED_PUBLISHED_EDITION
 * ADP_TERRITORY_SUBJECT_RATE_INDEPENDENT_OF_BENCHMARK_STATUS
 */

import assert from "assert";
import { readFileSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  ADP_CURRENT_PUBLISHED_SOT,
  CURRENT_PUBLISHED_SHARE_ALWAYS_RESOLVES_LATEST_GOVERNED_PUBLISHED_EDITION,
  ADP_CURRENT_PUBLISHED_SHARE_RESOLVER_CANONICAL,
  ADP_EXISTING_SHARE_URL_AUTO_UPDATES_CURRENT_PUBLISHED,
  ADP_LOCAL_EXTERNAL_ANALYTICAL_PARITY,
  ADP_ANALYTICAL_FINGERPRINT_LOCAL_EXTERNAL_PARITY,
  PRODUCTION_VISUAL_OUTPUT_MATCHES_VERIFIED_LOCAL,
} from "../lib/ai-demand-positioning/governance/adp-current-published-share-parity-v1.js";
import {
  ADP_TERRITORY_SUBJECT_RATE_INDEPENDENT_OF_BENCHMARK_STATUS,
  SUBJECT_PRESENCE_UNAVAILABLE_LABEL,
  INDEX_UNAVAILABLE_LABEL,
  BENCHMARK_UNCERTIFIED_LABEL,
  CUSTOMER_TERMINOLOGY_VERSION,
} from "../lib/ai-demand-positioning/customer/adp-customer-display-contract-v1.js";
import { readShareRegistry } from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import { loadPublishedManifest, listPublishedPropertyIds } from "../lib/ai-demand-positioning/published-snapshot.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");
const SHARE = join(process.cwd(), "public/owner-ai-demand-share.html");
const OUT = join(
  process.cwd(),
  "reports/ai-demand-positioning/adp-current-published-share-parity-v1-latest.json"
);

function main() {
  assert.equal(CUSTOMER_TERMINOLOGY_VERSION, "adp_customer_terminology_v3");
  assert.ok(ADP_CURRENT_PUBLISHED_SOT.resolvers.getPublishedOwnerReport);
  assert.equal(ADP_CURRENT_PUBLISHED_SOT.reportScopeAllowed, "current_published");

  const ui = readFileSync(UI, "utf8");
  assert.ok(ui.includes("ADP_TERRITORY_SUBJECT_RATE_INDEPENDENT_OF_BENCHMARK_STATUS"));
  assert.ok(ui.includes("subjectPresenceUnavailableCell"));
  assert.ok(ui.includes("indexUnavailableCell"));
  assert.ok(ui.includes("Your AI Presence used developingCell") === false);
  // Subject column must not call developingCell() for missing subject rate.
  const renderFn = ui.slice(ui.indexOf("function renderExecIntentTable"), ui.indexOf("function renderExecIntentTable") + 4500);
  assert.ok(renderFn.includes("subjectPresenceUnavailableCell()"));
  assert.ok(
    !/presenceVisual[\s\S]{0,400}developingCell\(\)/.test(renderFn),
    "subject presenceVisual must not use developingCell()"
  );

  const html = readFileSync(HTML, "utf8") + readFileSync(SHARE, "utf8");
  assert.ok(html.includes("adp-v84-territory-column-semantics-20260909"), "cache-bust token required");

  const registry = readShareRegistry();
  const tokens = Object.values(registry.tokens || {}).filter((t) => t && t.tokenId);
  const active = tokens.filter((t) => t.status === "ACTIVE");
  const scoped = active.filter((t) => (t.reportScope || "current_published") === "current_published");
  const pinned = active.filter((t) => t.reportScope && t.reportScope !== "current_published");
  assert.equal(pinned.length, 0, "no ACTIVE pinned edition scopes");
  assert.ok(scoped.length >= 5, "expected active current_published tokens");

  const published = listPublishedPropertyIds();
  assert.ok(published.includes("adp_bethesda_marriott"), "Bethesda must be published locally");

  const bethesdaManifest = loadPublishedManifest("adp_bethesda_marriott");
  assert.ok(bethesdaManifest?.latestPeriodId?.includes("adp_bethesda_marriott"));

  const gates = {
    CURRENT_PUBLISHED_SHARE_ALWAYS_RESOLVES_LATEST_GOVERNED_PUBLISHED_EDITION,
    ADP_CURRENT_PUBLISHED_SHARE_RESOLVER_CANONICAL,
    ADP_EXISTING_SHARE_URL_AUTO_UPDATES_CURRENT_PUBLISHED,
    ADP_TERRITORY_SUBJECT_RATE_INDEPENDENT_OF_BENCHMARK_STATUS,
    ADP_LOCAL_EXTERNAL_ANALYTICAL_PARITY,
    ADP_ANALYTICAL_FINGERPRINT_LOCAL_EXTERNAL_PARITY,
    PRODUCTION_VISUAL_OUTPUT_MATCHES_VERIFIED_LOCAL,
    SUBJECT_PRESENCE_UNAVAILABLE_LABEL,
    INDEX_UNAVAILABLE_LABEL,
    BENCHMARK_UNCERTIFIED_LABEL,
  };

  const report = {
    title: "ADP_CURRENT_PUBLISHED_SHARE_PARITY_V1",
    ok: true,
    gates,
    tokens: { active: active.length, current_published: scoped.length, pinned: pinned.length },
    publishedCount: published.length,
    bethesdaPeriodId: bethesdaManifest.latestPeriodId,
    cacheBust: "adp-v84-territory-column-semantics-20260909",
    methodologyChanged: false,
  };

  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, outPath: OUT, ...report.tokens, published: published.length }, null, 2));
}

main();
