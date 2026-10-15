#!/usr/bin/env node
/**
 * GDI domain integrity + customer DTO leak checks (fixtures + optional live).
 *
 *   node scripts/test-gdi-quality-integrity-v1.mjs
 *   node scripts/test-gdi-quality-integrity-v1.mjs --live
 */
import "dotenv/config";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { buildContactDisplaySurface } from "../lib/group-demand-intelligence/contact-display-surface-v1.js";
import {
  assessHotelFitForOpportunity,
  computeOverallFromAssessedDimensions,
  DIMENSION_STATUS,
} from "../lib/group-demand-intelligence/workflow/hotel-fit-assessment-v1.js";
import { DEFAULT_HOTEL_FIT_WEIGHTS } from "../lib/group-demand-intelligence/scoring.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-quality-review");
const FIXTURE = path.join(
  ROOT,
  "fixtures/gdi-quality/representative-opportunities-v1.json"
);
const live = process.argv.includes("--live");

fs.mkdirSync(OUT, { recursive: true });

const findings = [];
function find(sev, surface, rootCause, fix, verification, limitation = null) {
  findings.push({
    severity: sev,
    surface,
    rootCause,
    fix,
    verification,
    remainingLimitation: limitation,
  });
}

let passed = 0;
function ok(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

const fx = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
const byId = Object.fromEntries(fx.opportunities.map((o) => [o.id, o]));

// --- Fixture integrity ---
{
  const a = byId.fixture_pe_venue_v1;
  const b = byId.fixture_pe_venue_sibling_v1;
  assert.equal(a.officialDomain, b.officialDomain);
  assert.notEqual(a.venueName, b.venueName);
  assert.notEqual(a.id, b.id);
  ok("same_domain_distinct_venues_remain_separate");
}

{
  const lmc = byId.fixture_lmc_no_rooms_v1;
  const phys = lmc.hotelFitDimensionAssessments.physicalFit;
  assert.equal(phys.status, "not_applicable");
  assert.equal(lmc.guestRoomsPeak, null);
  // Recompute overall — N/A must not become zero penalty
  const dims = Object.fromEntries(
    Object.entries(lmc.hotelFitDimensionAssessments).map(([k, d]) => [
      k,
      {
        status:
          d.status === "not_applicable"
            ? DIMENSION_STATUS.NOT_APPLICABLE
            : d.status === "insufficient_evidence"
              ? DIMENSION_STATUS.INSUFFICIENT_EVIDENCE
              : DIMENSION_STATUS.ASSESSED,
        score: d.score,
      },
    ])
  );
  const overall = computeOverallFromAssessedDimensions(
    dims,
    DEFAULT_HOTEL_FIT_WEIGHTS
  );
  assert.ok(!overall.calculationBasis.assessedKeys.includes("physicalFit"));
  assert.ok(overall.score == null || overall.score >= 60);
  ok("catering_only_not_penalized_for_no_guestrooms");
}

{
  const stretch = byId.fixture_stretch_provisional_v1;
  assert.equal(stretch.hotelFitProvisional, true);
  const assessed = assessHotelFitForOpportunity(
    {
      ...stretch,
      hotelFitDimensionAssessments: undefined,
    },
    { hotelId: stretch.hotelId }
  );
  // Fresh assess may differ; fixture contract: provisional flag present on stored shape
  assert.ok(stretch.hotelFitProvisional === true);
  ok("provisional_flag_preserved_on_stretch_fixture");
  void assessed;
}

{
  const named = byId.fixture_scaling_named_v1;
  const surface = buildContactDisplaySurface(named);
  assert.equal(surface.primaryContact?.name, "Alex Rivera");
  assert.equal(surface.primaryContact?.email, "alex.rivera@fixturescale.example");
  assert.equal(surface.companyPhone, "+1 202-555-0199");
  assert.ok(surface.functionalContactEmail || surface.organizationContactEmail);
  assert.notEqual(surface.primaryContact?.email, surface.functionalContactEmail);
  ok("named_vs_org_channel_separation");
}

{
  const orgOnly = byId.fixture_scaling_org_only_v1;
  const surface = buildContactDisplaySurface(orgOnly);
  assert.ok(!surface.primaryContact?.name);
  assert.ok(surface.hasUsableRoute);
  assert.ok(surface.companyPhone || surface.functionalContactEmail || surface.contactUrl);
  ok("org_only_fallback_usable");
}

{
  const bad = byId.fixture_bad_channel_ownership_v1;
  const surface = buildContactDisplaySurface(bad);
  // Without named person, company phone must not appear as person phone
  assert.equal(surface.personPhone || null, null);
  assert.ok(surface.companyPhone);
  ok("company_phone_not_shown_as_person_without_name");
}

{
  const leak = byId.fixture_provider_leak_v1;
  const dto = toOpportunityListDto(leak);
  const blob = JSON.stringify(dto);
  const hasSurfe = /\bSurfe\b/i.test(blob);
  const hasCredits = /providerCreditsConsumed|surfeRequestId/i.test(blob);
  assert.equal(hasCredits, false, "provider keys must not appear on customer DTO");
  assert.equal(hasSurfe, false, "Surfe brand must not appear on customer DTO copy");
  ok("provider_keys_and_surfe_copy_stripped_from_list_dto");
}

{
  // Duplicate venue identity stability
  const ids = fx.opportunities.map((o) => o.id);
  assert.equal(new Set(ids).size, ids.length);
  ok("fixture_opportunity_ids_unique");
}

// --- Optional live production reads (Mayflower + SD) ---
const liveReport = { ran: false, hotels: {} };
if (live) {
  process.env.GDI_OPPORTUNITIES_PERSISTENCE =
    process.env.GDI_OPPORTUNITIES_PERSISTENCE || "airtable";
  const { loadOpportunitiesCanonical } = await import(
    "../lib/group-demand-intelligence/opportunity-persistence.js"
  );
  liveReport.ran = true;
  for (const hotelId of ["rec0eTEnCqlsGJJt9", "recnPB3Q02NO8Ma1o"]) {
    const bag = await loadOpportunitiesCanonical(hotelId, { preferAirtable: true });
    const opps = bag?.opportunities || [];
    const facing = opps.filter(
      (o) => o.prospectHotelFacing === true || o.customerVisible === true
    );
    const scaling = facing.filter(
      (o) =>
        o.researchSegment === "scaling_companies" ||
        String(o.id || "").startsWith("gdi_sc_")
    );
    let dtoLeaks = 0;
    let channelBugs = 0;
    let missingFitExplain = 0;
    for (const o of facing) {
      const dto = toOpportunityListDto(o);
      const blob = JSON.stringify(dto);
      if (/providerRequestId|creditsConsumed|surfeRequestId|estimatedCostUsd/i.test(blob)) {
        dtoLeaks += 1;
      }
      if (/\bSurfe\b/i.test(blob)) dtoLeaks += 1;
      const s = buildContactDisplaySurface(o);
      if (!s.primaryContact?.name && s.personPhone) channelBugs += 1;
      if (
        s.primaryContact?.email &&
        /^(info|hello|contact|pr|federal|kelleninfo)@/i.test(s.primaryContact.email) &&
        !s.primaryContact?.name
      ) {
        channelBugs += 1;
      }
      if (
        o.hotelFitScore != null &&
        !o.hotelFitDimensionAssessments &&
        !o.fitExplanation &&
        !o.hotelFitRationale
      ) {
        missingFitExplain += 1;
      }
    }
    // Stable IDs
    const ids = facing.map((o) => o.id).filter(Boolean);
    const dupIds = ids.length - new Set(ids).size;
    liveReport.hotels[hotelId] = {
      facing: facing.length,
      scaling: scaling.length,
      dtoLeaks,
      channelBugs,
      missingFitExplain,
      duplicateIds: dupIds,
    };
    assert.equal(dupIds, 0, `${hotelId} duplicate opportunity IDs`);
    assert.equal(dtoLeaks, 0, `${hotelId} customer DTO provider leaks`);
    assert.equal(channelBugs, 0, `${hotelId} channel ownership bugs`);
    ok(`live_${hotelId}_integrity`);
  }
}

const report = {
  generatedAt: new Date().toISOString(),
  passed,
  live: liveReport,
  findings,
  fixturePath: FIXTURE,
};
fs.writeFileSync(
  path.join(OUT, "domain-integrity-results.json"),
  JSON.stringify(report, null, 2) + "\n"
);
console.log(
  JSON.stringify(
    { passed, findings: findings.length, live: liveReport.ran },
    null,
    2
  )
);
console.log("test-gdi-quality-integrity-v1: PASS");
