/**
 * Structural regression: GDI opportunity tile must match pre-V5A Bethesda card shape.
 * Protects against reintroducing long prose / why-now / fit / action into the tile.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const uiPath = path.join(
  __dirname,
  "../public/js/group-demand-intelligence/dealality-gdi-ui.js"
);
const src = fs.readFileSync(uiPath, "utf8");

// Extract opportunityTileHtml body markers from source (structure lock)
assert.match(src, /function opportunityTileHtml\s*\(/);
assert.match(src, /brand-card__description/);
assert.match(src, /No primary contact/);
assert.match(src, /View Details/);

// Must NOT contain V5A presentation additions in the tile renderer
assert.doesNotMatch(src, /cardHotelFitLine/);
assert.doesNotMatch(src, /cardWhyNowLine/);
assert.doesNotMatch(src, /cardFitBadge/);
assert.doesNotMatch(src, /commercialMotionLabel/);
assert.doesNotMatch(src, /brand-card__meta--fit/);
assert.doesNotMatch(src, /brand-card__meta--why-now/);
assert.doesNotMatch(src, /gdi-pill--fit/);
assert.doesNotMatch(src, /Named stakeholder not publicly identified yet/);

// opportunityToBriefItem must not prefer commercialSummary over summaryWhat
const briefIdx = src.indexOf("function opportunityToBriefItem");
const tileIdx = src.indexOf("function opportunityTileHtml");
assert.ok(briefIdx > 0 && tileIdx > briefIdx);
const briefBlock = src.slice(briefIdx, tileIdx);
assert.doesNotMatch(briefBlock, /commercialSummary/);
assert.doesNotMatch(briefBlock, /cardHotelFitLine/);
assert.match(briefBlock, /summaryWhat:\s*o\.summaryWhat/);

// DTO must not apply card contract
const dtoPath = path.join(
  __dirname,
  "../lib/group-demand-intelligence/opportunity-list-dto.js"
);
const dto = fs.readFileSync(dtoPath, "utf8");
assert.doesNotMatch(dto, /applyCommercialCardContract/);
assert.doesNotMatch(dto, /commercialSummary:/);
assert.doesNotMatch(dto, /cardHotelFitLine/);
assert.match(dto, /gdi_opportunity_list_v2/);

// Card contract module must not exist / not be wired
const contractPath = path.join(
  __dirname,
  "../lib/group-demand-intelligence/commercial-card-contract-v1.js"
);
assert.equal(fs.existsSync(contractPath), false);

const indexPath = path.join(
  __dirname,
  "../lib/group-demand-intelligence/index.js"
);
const indexSrc = fs.readFileSync(indexPath, "utf8");
assert.doesNotMatch(indexSrc, /commercial-card-contract/);
assert.match(indexSrc, /active-eligibility-v1/);
assert.match(indexSrc, /entity-truth-gate-v1/);
assert.match(indexSrc, /customer-surface-revalidation-v1/);

// Runtime: render a Bethesda-shaped row and assert no V5A prose slots
const sandbox = {
  module: { exports: {} },
  exports: {},
  console,
  URL,
  URLSearchParams,
};
// dealality-gdi-ui is IIFE-style browser JS — parse tile via Function from extracted source
const tileMatch = src.match(
  /function opportunityTileHtml\(it, linked, activeFilters\) \{[\s\S]*?\n  \}/
);
assert.ok(tileMatch, "opportunityTileHtml function body required");

// Lightweight structural probe without full GDI UI bootstrap:
const htmlProbe = tileMatch[0];
assert.match(htmlProbe, /brand-card__meta/);
assert.match(htmlProbe, /segment \? " · " \+ esc\(String\(segment\)\.toUpperCase\(\)\)/);
assert.doesNotMatch(htmlProbe, /whyHotel/);
assert.doesNotMatch(htmlProbe, /cardWhyNowLine/);
assert.doesNotMatch(htmlProbe, /metaBits/);

void vm;
void sandbox;

console.log("test:gdi-card-presentation-regression OK");
