#!/usr/bin/env node
/**
 * Offline unit tests for Phase 2D inventory + generator mapping (no Airtable / HTTP).
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildProductionTemplateInventory,
} from "../lib/brand-explorer/be-phase2d-template-inventory-v1.js";
import {
  listApplicableRequiredSlots,
  generateEnhancedCandidateFromEvidence,
  diagnoseResearchRequiredGaps,
  candidateToAtelierBlocks,
  evaluateEnhancedQuality,
} from "../lib/brand-explorer/be-phase2d-curio-quality-recovery-v1.js";
import { describeResearchProcess, GAP_CATEGORIES } from "../lib/brand-explorer/be-phase2d-research-process-v1.js";
import {
  loadBrandExplorerAtelierRenderer,
  renderBrandExplorerHtmlForTest,
} from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

const inventory = buildProductionTemplateInventory();
assert.ok(inventory.reconciliation.unionSlotKeys >= 119, "inventory must not shrink below Phase 2C slot count");
assert.ok(inventory.reconciliation.applicableRequiredSlots > 0);
assert.equal(inventory.slots.some((s) => s.slotKey === "Guest Psychographics Description" && s.applicableRequired === false), true);

const { slots } = listApplicableRequiredSlots(inventory);
assert.ok(slots.length >= inventory.applicableSlotKeys.length);

const processDoc = describeResearchProcess();
assert.ok(processDoc.stages.length >= 8);
assert.ok(GAP_CATEGORIES.GENERATOR_MAPPING_FAILED);

const fakePack = {
  evidence: [
    { id: "identity.brand_name", kind: "identity", claim: "Curio Collection by Hilton", confidence: "high" },
    { id: "identity.parent", kind: "identity", claim: "Hilton Worldwide", confidence: "high" },
    { id: "basics.positioning", kind: "positioning", claim: "Curio Collection is a soft brand for distinctive hotels.", confidence: "high" },
    { id: "basics.year_launched", kind: "snapshot", claim: "2014", confidence: "high" },
    { id: "basics.chain_scale", kind: "identity", claim: "Upscale", confidence: "high" },
    { id: "loyalty.program", kind: "loyalty", claim: "Hilton Honors", confidence: "high" },
    { id: "loyalty.honors_tiers", kind: "loyalty", claim: "Member; Silver; Gold; Diamond", confidence: "high" },
    { id: "competitive.soft_brand_peers", kind: "competitive", claim: "Autograph; Tribute; Tapestry", confidence: "medium" },
    { id: "economics.diligence_path", kind: "economics", claim: "Confirm FDD fees with counsel.", confidence: "medium" },
    { id: "economics.fee_categories", kind: "economics", claim: "Royalty; marketing; loyalty fees — confirm in FDD", confidence: "medium" },
    { id: "standards.diligence", kind: "standards", claim: "Confirm design review and QA in brand standards.", confidence: "medium" },
    { id: "official.image.1", kind: "imagery", claim: "https://example.com/curio1.jpg", confidence: "medium" },
    { id: "metric.open_hotels", kind: "hpc", claim: "HPC Open hotels matched=20", confidence: "medium" },
  ],
  hpc: { certState: "LIVE_UNVERIFIED", certified: false, metrics: { totalOpenHotels: 20, totalPipelineHotels: 2, totalPipelineKeys: 100 } },
  paidResearchUsed: false,
};

const candidate = generateEnhancedCandidateFromEvidence(fakePack, { inventory });
assert.ok(candidate.rows.length >= 100, `expected broad slot coverage, got ${candidate.rows.length}`);
assert.equal(candidate.usedPresentationAsInput, false);

const scenario1 = candidate.rows.find((r) => r.slotKey === "overview.scenario.1");
assert.ok(scenario1 && scenario1.disposition !== "RESEARCH_REQUIRED", "scenario.1 must map from positioning evidence");

const opModel1 = candidate.rows.find((r) => r.slotKey === "operations.model.1");
assert.ok(opModel1 && opModel1.disposition !== "RESEARCH_REQUIRED", "operations.model.1 must map");

const lifecycle1 = candidate.rows.find((r) => r.slotKey === "valueOwners.lifecycle.1");
assert.ok(lifecycle1 && lifecycle1.body.length > 20);

const gallery1 = candidate.rows.find((r) => r.slotKey === "materials.gallery.1");
assert.ok(gallery1 && gallery1.disposition !== "RESEARCH_REQUIRED");

const phase2cStub = {
  rows: [
    { slotKey: "overview.scenario.1", disposition: "RESEARCH_REQUIRED", body: "" },
    { slotKey: "operations.model.1", disposition: "RESEARCH_REQUIRED", body: "" },
    { slotKey: "materials.gallery.1", disposition: "RESEARCH_REQUIRED", body: "" },
  ],
};
const diagnosis = diagnoseResearchRequiredGaps(candidate, fakePack, phase2cStub);
assert.ok(diagnosis.phase2cOriginal72.total === 3);
assert.ok(diagnosis.phase2cOriginal72.byCategory.GENERATOR_MAPPING_FAILED || diagnosis.phase2cOriginal72.byCategory.EXTRACTION_FAILED);

const quality = evaluateEnhancedQuality(candidate, fakePack, inventory);
assert.ok(quality.scores.applicableFieldCompleteness > 0.5, `completeness too low: ${quality.scores.applicableFieldCompleteness}`);

const blocks = candidateToAtelierBlocks(candidate);
assert.ok(blocks.length > 0);
assert.ok(blocks.every((b) => b.slotKey));

// Real atelier renderer mount (VM) — not standalone HTML
const atelierSrc = readFileSync(join(ROOT, "public/js/brand-explorer-atelier-from-api.js"), "utf8");
assert.match(atelierSrc, /beCandidateOverlay/);
assert.match(atelierSrc, /isCandidateOverlayRequest/);

const overlayPath = join(ROOT, "public/brand-explorer-phase2d-preview/curio-candidate.json");
let atelierValidation = { status: "SKIPPED_NO_OVERLAY_YET" };
if (existsSync(overlayPath)) {
  const overlay = JSON.parse(readFileSync(overlayPath, "utf8"));
  assert.equal(typeof loadBrandExplorerAtelierRenderer().buildPanelsHtmlForTest, "function");
  assert.match(atelierSrc, /isCandidateOverlayRequest/);
  const brand = {
    id: "receQkxgjlezsc1xg",
    name: "Curio Collection by Hilton",
    slug: "curio-collection",
    brandExplorer: overlay.brandExplorer,
    brandExplorerDisplayState: "external_owner_ready",
  };
  const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true, internalPreview: true });
  assert.ok(typeof html === "string" && html.length > 500, "atelier panels HTML too short");
  assert.match(html, /atelier-overview|Overview|oe-section/i);
  assert.match(html, /Curio|Hilton|Honors|soft-?brand/i);
  const tabIds = [
    "atelier-overview",
    "atelier-value-owners",
    "atelier-ops",
    "atelier-standards-owner",
    "atelier-commercial",
    "atelier-economics",
    "atelier-loyalty",
    "atelier-footprint",
    "atelier-materials",
    "atelier-insight",
  ];
  const presentTabs = tabIds.filter((id) => html.includes(`id="${id}"`) || html.includes(`data-panel="${id}"`) || html.includes(id));
  atelierValidation = {
    status: "PASS_ATELIER_VM_MOUNT",
    htmlChars: html.length,
    overlayBlocks: overlay.brandExplorer?.blocks?.length || 0,
    tabsDetected: presentTabs.length,
    note: "Mounted candidate blocks through BrandExplorerAtelierFromApi.buildPanelsHtmlForTest (real renderer). Live localhost browser still recommended when BE node_modules/server available.",
  };
  assert.ok(presentTabs.length >= 5, `expected multiple atelier tab markers, got ${presentTabs.join(",")}`);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      inventoryApplicable: inventory.reconciliation.applicableRequiredSlots,
      candidateRows: candidate.rows.length,
      applicableCompleteness: quality.scores.applicableFieldCompleteness,
      researchRequired: quality.counts.applicableResearchRequired,
      atelierValidation,
    },
    null,
    2,
  ),
);
