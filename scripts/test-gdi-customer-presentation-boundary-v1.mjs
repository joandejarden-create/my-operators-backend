#!/usr/bin/env node
/**
 * Boundary regressions: structured assessment → sanitize → DTO/drawer parity.
 * Proves the generic shared path with Bethesda (third hotel) — no hotel-specific exception.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import {
  sanitizeOpportunityForCustomerPresentation,
  reconcileHotelFitScoreForDisplay,
  isAssessmentStatusProse,
  scrubProviderBrandNames,
} from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixturePath = path.join(
  ROOT,
  "fixtures/gdi-quality/representative-opportunities-v1.json"
);
const uiPath = path.join(
  ROOT,
  "public/js/group-demand-intelligence/dealality-gdi-ui.js"
);

const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const bethesda = (fixture.opportunities || []).find(
  (o) => o.id === "fixture_bethesda_assoc_withheld_v1"
);
assert.ok(bethesda, "Bethesda third-hotel fixture required");
assert.equal(bethesda.hotelId, "recLuxvwwxID7U2B8");
assert.equal(fixture.hotels[bethesda.hotelId]?.role, "regression");

// --- Sanitize boundaries ---
assert.equal(
  reconcileHotelFitScoreForDisplay(bethesda),
  null,
  "withheld must clear stale flat 61"
);

const sanitized = sanitizeOpportunityForCustomerPresentation(bethesda);
assert.equal(sanitized.hotelFitScore, null);
assert.ok(
  !isAssessmentStatusProse(sanitized.hotelOpportunityThesis || ""),
  "assessment prose must not remain as thesis"
);
assert.ok(
  !/Surfe/i.test(sanitized.whoPrimaryReason || ""),
  "Surfe scrubbed from whoPrimaryReason"
);
assert.ok(
  !/Surfe/i.test(sanitized.primaryContact?.whyThisContact || ""),
  "Surfe scrubbed from contact rationale"
);
assert.ok(
  !("provider" in (sanitized.primaryContact || {})) ||
    sanitized.primaryContact.provider == null
);
assert.equal(
  scrubProviderBrandNames("Surfe only if public path fails"),
  "Paid enrichment only if public path fails"
);

// List DTO parity with sanitize (same withheld + scrub rules)
const listDto = toOpportunityListDto(bethesda);
assert.equal(listDto.hotelFitScore, null);
assert.ok(!isAssessmentStatusProse(listDto.hotelOpportunityThesis || ""));
assert.ok(
  !/Surfe/i.test(listDto.primaryContact?.relevanceEvidence || ""),
  "list contact evidence scrubbed"
);

// Preserve richer assessment structure through list projection
assert.ok(listDto.hotelFitDimensionAssessments?.geographyFit);
assert.equal(listDto.hotelFitAssessedAt, "2026-10-15");
assert.equal(listDto.hotelFitAssessmentVersion, "gdi-hotel-fit-assessment-v2");
assert.equal(
  listDto.primaryContact?.emailOwnership,
  "person_specific",
  "person channel ownership preserved"
);

// --- Drawer renderer on Bethesda (shared UI, no hotel exception) ---
const code = fs.readFileSync(uiPath, "utf8");
const sandbox = { window: {}, console };
sandbox.window = sandbox;
vm.runInNewContext(code, sandbox);
const UI = sandbox.DealalityGdiUi;
assert.ok(UI?.hotelFitAssessmentSectionHtml);
assert.ok(UI?.whyThisHotelOpeningHtml);

const drawerOpp = { ...sanitized };
const html = UI.hotelFitAssessmentSectionHtml(drawerOpp, {});
assert.equal((html.match(/Overall Hotel Fit/g) || []).length, 1);
assert.ok(/Withheld/i.test(html));
assert.ok(!/>\s*61\s*</.test(html));
assert.ok(/Needs verification/.test(html));
assert.ok(/Not applicable/.test(html));
assert.ok(/44 \/ 100/.test(html));
assert.ok(!/Surfe|Webhound|requestId|providerCost/i.test(html));

const opening = UI.whyThisHotelOpeningHtml(drawerOpp);
assert.ok(/Bethesda Metro|meeting inventory/i.test(opening));
assert.ok(!/Dimension-level hotel-fit/i.test(opening));
assert.ok(/Sourcing status/i.test(opening));

// Opportunity-type coverage: same shared presenter for PE / LMC / scaling fixtures
for (const id of [
  "fixture_lmc_no_rooms_v1",
  "fixture_pe_venue_v1",
  "fixture_scaling_named_v1",
]) {
  const row = (fixture.opportunities || []).find((o) => o.id === id);
  assert.ok(row, id);
  const cleaned = sanitizeOpportunityForCustomerPresentation(row);
  const section = UI.hotelFitAssessmentSectionHtml(cleaned, {});
  assert.equal(
    (section.match(/Overall Hotel Fit/g) || []).length,
    1,
    `${id} single overall tile`
  );
}

console.log("test-gdi-customer-presentation-boundary-v1: PASS");
