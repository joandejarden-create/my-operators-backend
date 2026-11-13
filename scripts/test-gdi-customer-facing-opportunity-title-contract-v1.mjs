#!/usr/bin/env node
/**
 * Permanent GDI customer-facing opportunity title contract V1.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_CUSTOMER_FACING_OPPORTUNITY_TITLE_CONTRACT_V1,
  applyCustomerFacingOpportunityTitleContract,
  customerFacingOpportunityTitle,
  isCustomerFacingTitleContractIdempotent,
  normalizeCustomerFacingOpportunityTitle,
} from "../lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js";
import { accountLevelDisplayTitle } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { sanitizeOpportunityTitle } from "../lib/group-demand-intelligence/opportunity-title-quality-v1.js";
import { resolveCanonicalCustomerFacingTitle } from "../lib/group-demand-intelligence/demand-segments/universal-opportunity-identity-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

const FOUNDER_EXAMPLES = [
  {
    before: "AMWA 2027 Interim Meeting — Sep 11–12",
    after: "AMWA 2027 Interim Meeting",
  },
  {
    before: "PDA TRI Bethesda Training — December 2026 Lodging",
    after: "PDA TRI Bethesda Training",
  },
  {
    before: "ASAE Annual Meeting & Exposition 2029 — Washington, DC (Early Watch)",
    after: "ASAE Annual Meeting & Exposition 2029",
  },
];

check("contract_version", () => {
  assert.match(GDI_CUSTOMER_FACING_OPPORTUNITY_TITLE_CONTRACT_V1, /title-contract-v1/);
});

check("founder_examples", () => {
  for (const ex of FOUNDER_EXAMPLES) {
    assert.equal(
      customerFacingOpportunityTitle(ex.before),
      ex.after,
      ex.before
    );
  }
});

check("preserves_official_name_tokens", () => {
  assert.equal(
    customerFacingOpportunityTitle("PDA TRI Bethesda Training"),
    "PDA TRI Bethesda Training"
  );
  assert.equal(
    customerFacingOpportunityTitle("Bethesda Soccer Club Fall Tournament 2026"),
    "Bethesda Soccer Club Fall Tournament 2026"
  );
});

check("idempotent", () => {
  for (const ex of FOUNDER_EXAMPLES) {
    assert.equal(isCustomerFacingTitleContractIdempotent(ex.before), true);
    assert.equal(
      customerFacingOpportunityTitle(ex.after),
      ex.after
    );
  }
});

check("multilingual_location_suffix", () => {
  assert.equal(
    customerFacingOpportunityTitle(
      "Congreso Nacional de Médicos 2027 — Ciudad de México (Early Watch)"
    ),
    "Congreso Nacional de Médicos 2027"
  );
});

check("pipeline_surfaces_agree", () => {
  for (const ex of FOUNDER_EXAMPLES) {
    const row = { title: ex.before, displayTitle: ex.before };
    assert.equal(sanitizeOpportunityTitle(ex.before), ex.after);
    assert.equal(accountLevelDisplayTitle(row), ex.after);
    assert.equal(resolveCanonicalCustomerFacingTitle(row), ex.after);
    const applied = applyCustomerFacingOpportunityTitleContract(row);
    assert.equal(applied.displayTitle, ex.after);
    assert.ok(applied.researchTitlePreserved || applied.originalSourceTitle);
  }
});

check("preserves_original_source_title", () => {
  const { originalSourceTitle, displayTitle, changed } =
    normalizeCustomerFacingOpportunityTitle(
      "AMWA 2027 Interim Meeting — Sep 11–12"
    );
  assert.equal(displayTitle, "AMWA 2027 Interim Meeting");
  assert.equal(originalSourceTitle, "AMWA 2027 Interim Meeting — Sep 11–12");
  assert.equal(changed, true);
});

check("module_wired_in_dto_and_quality", () => {
  const dto = fs.readFileSync(
    path.join(ROOT, "lib/group-demand-intelligence/opportunity-list-dto.js"),
    "utf8"
  );
  const quality = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/opportunity-title-quality-v1.js"
    ),
    "utf8"
  );
  const identity = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/demand-segments/universal-opportunity-identity-v1.js"
    ),
    "utf8"
  );
  const upsert = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/airtable-opportunity-store.js"
    ),
    "utf8"
  );
  const repo = fs.readFileSync(
    path.join(ROOT, "lib/group-demand-intelligence/repository.js"),
    "utf8"
  );
  const hunter = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/nextgen-discovery/hunter-base-v1.js"
    ),
    "utf8"
  );
  const csv = fs.readFileSync(
    path.join(ROOT, "lib/group-demand-intelligence/customer-csv-export.js"),
    "utf8"
  );
  assert.match(dto, /customerFacingOpportunityTitle/);
  assert.match(quality, /customerFacingOpportunityTitle/);
  assert.match(identity, /customerFacingOpportunityTitle/);
  assert.match(upsert, /applyCustomerFacingOpportunityTitleContract/);
  assert.match(repo, /applyCustomerFacingOpportunityTitleContract/);
  assert.match(hunter, /normalizeCustomerFacingOpportunityTitle/);
  assert.match(csv, /resolveCanonicalCustomerFacingTitle/);
});

check("all_caps_brand_preserved", () => {
  assert.equal(customerFacingOpportunityTitle("ADAPTATUR"), "ADAPTATUR");
  assert.equal(
    customerFacingOpportunityTitle("BioCultura A Coruña 2027"),
    "BioCultura A Coruña 2027"
  );
});

check("demand_thesis_suffix_stripped", () => {
  assert.equal(
    customerFacingOpportunityTitle(
      "47th Annual Potomac Memorial Tournament 2027 — Stay-to-Play Weekend Demand"
    ),
    "47th Annual Potomac Memorial Tournament 2027"
  );
  assert.equal(
    customerFacingOpportunityTitle("Matthews — Scaling Corporate Account"),
    "Matthews"
  );
});

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll customer-facing opportunity title contract checks passed");
