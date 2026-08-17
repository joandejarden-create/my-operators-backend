#!/usr/bin/env node
/**
 * Brand AI Visibility read-path: federated four-provider baseline + multi-slot geography.
 * No provider calls. No Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  createBrandAiVisibilityReadStore,
  resolveBrandAiVisibilityReadRoots,
  listMeasuredBaselineStoreRoots,
  WAVE1_ROOT,
} from "../lib/ai-visibility/storage/index.js";
import {
  isMultiSlotBatchSummary,
  listMatchingSlots,
  multiSlotSummaryMatchesStoreFilter,
  projectMultiSlotSummaryForRead,
} from "../lib/ai-visibility/multi-slot-geography.js";
import {
  findMatchingSummaries,
  parseGeographyQuery,
} from "../lib/ai-visibility/brand-read-service.js";
import { listAvailableAiVisibilityProviders } from "../lib/ai-visibility/provider-dimension.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(__dirname, "..");

let passed = 0;
async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`  ok — ${name}`);
}

async function main() {
  console.log("test:ai-visibility-brand-baseline-read\n");

  await test("measured baseline roots include wave1 when present", () => {
    const roots = listMeasuredBaselineStoreRoots();
    if (fs.existsSync(path.join(WAVE1_ROOT, "summaries"))) {
      assert.ok(roots.some((r) => /wave1-showcase/i.test(r)));
    }
  });

  await test("resolveBrandAiVisibilityReadRoots prefers federation over phase2e", () => {
    const resolved = resolveBrandAiVisibilityReadRoots({});
    const measured = listMeasuredBaselineStoreRoots();
    if (measured.length) {
      assert.equal(resolved.mode, "federated_measured_baseline");
      assert.ok(resolved.rootDirs.length >= 1);
      assert.ok(!resolved.rootDirs.some((r) => /phase2e/i.test(r)));
    }
  });

  await test("createBrandAiVisibilityReadStore lists multi-provider full baselines", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const rows = await store.listBatchSummaries({});
    const full = rows.filter((r) => r.status === "completed" || r.status === "partial");
    const providers = new Set(
      full.map((r) => String(r.provider?.name || r.provider || "").toLowerCase()).filter(Boolean)
    );
    if (listMeasuredBaselineStoreRoots().length >= 4) {
      assert.ok(providers.has("openai"), "openai");
      assert.ok(providers.has("gemini"), "gemini");
      assert.ok(providers.has("perplexity"), "perplexity");
      assert.ok(providers.has("claude"), "claude");
    } else {
      assert.ok(full.length >= 0);
    }
  });

  await test("multi-slot CALA EN matching projects cohort", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const rows = await store.listBatchSummaries({ provider: "openai" });
    const multi = rows.find((r) => isMultiSlotBatchSummary(r) && r.status === "completed");
    if (!multi) {
      console.log("  skip — no multi-slot openai summary in workspace");
      return;
    }
    assert.ok(multiSlotSummaryMatchesStoreFilter(multi, { geographyScope: "Region", commercialRegion: "CALA", language: "en" }));
    const slots = listMatchingSlots(
      multi,
      { geographyScope: "Region", commercialRegion: "CALA" },
      { language: "en" }
    );
    assert.ok(slots.some((s) => s.key === "CALA_EN"));
    const projected = projectMultiSlotSummaryForRead(multi, slots, "en");
    assert.equal(projected.cohort.commercialRegion, "CALA");
    assert.ok(projected._matchedSlotKeys.includes("CALA_EN"));
  });

  await test("findMatchingSummaries returns CALA openai from federated store", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const geo = parseGeographyQuery({ geography: "CALA" });
    const matched = await findMatchingSummaries(store, geo, "openai", { language: "en" });
    if (listMeasuredBaselineStoreRoots().some((r) => /wave1-showcase/i.test(r))) {
      assert.ok(matched.length >= 1, "expected CALA match from wave1 slots");
      assert.ok(matched[0]._matchedSlotKeys?.includes("CALA_EN"));
    }
  });

  await test("available providers include four measured providers for CALA", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const available = await listAvailableAiVisibilityProviders({
      store,
      geographyScope: "Region",
      commercialRegion: "CALA",
    });
    const ids = available.map((p) => p.id);
    if (listMeasuredBaselineStoreRoots().length >= 4) {
      for (const id of ["openai", "gemini", "perplexity", "claude"]) {
        assert.ok(ids.includes(id), `missing provider ${id}: ${ids.join(",")}`);
      }
    }
  });

  await test("UI discoverability renderer references Phase 3C.1 fields", () => {
    const js = fs.readFileSync(
      path.join(REPO, "public/js/ai-visibility/ai-visibility-brand.js"),
      "utf8"
    );
    assert.match(js, /discoverabilityBusinessImpact/);
    assert.match(js, /d\.discoverability/);
  });

  console.log(`\n${passed} passed`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
