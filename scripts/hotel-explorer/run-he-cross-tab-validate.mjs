#!/usr/bin/env node
/**
 * Hotel Explorer Factory — Audit/Validate/Full/Refresh cross-tab consistency.
 *
 * Always writes:
 *   reports/hotel-explorer/HE-CROSS-TAB-CONSISTENCY-<hotelId>.json
 *   reports/hotel-explorer/HE-CROSS-TAB-CONSISTENCY-<hotelId>.md
 *
 * Usage:
 *   node scripts/hotel-explorer/run-he-cross-tab-validate.mjs --hotelId=recLuxvwwxID7U2B8 --mode=Validate
 *   node scripts/hotel-explorer/run-he-cross-tab-validate.mjs --fixture
 *   node scripts/hotel-explorer/run-he-cross-tab-validate.mjs --hotelId=rec… --skip-browser
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { validateBethesdaGoldenParityFixture } from "../../lib/hotel-intelligence/he-cross-tab-parity-v1.js";
import { runCrossTabFactoryGates } from "../../lib/hotel-intelligence/he-factory/he-cross-tab-factory-gates-v1.js";
import {
  buildCrossTabConsistencyReport,
  writeCrossTabConsistencyArtifacts,
  HE_CONSISTENCY_RESULT,
} from "../../lib/hotel-intelligence/he-factory/he-cross-tab-consistency-report-v1.js";
import { buildSharedHotelViewModel } from "../../lib/hotel-intelligence/he-shared-hotel-view-model-v1.js";
import { buildOwnerPortfolioOperatorView } from "../../lib/hotel-intelligence/ownership/owner-portfolio-operator-view-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS = path.join(ROOT, "reports/hotel-explorer");
const args = process.argv.slice(2);
const fixtureOnly = args.includes("--fixture");
const skipBrowser = args.includes("--skip-browser") || !args.includes("--browser");
const hotelIdArg = args.find((a) => a.startsWith("--hotelId="));
const modeArg = args.find((a) => a.startsWith("--mode="));
const hotelId = hotelIdArg ? hotelIdArg.split("=")[1] : "recLuxvwwxID7U2B8";
const mode = modeArg ? modeArg.split("=")[1] : "Validate";

function buildBethesdaFixtureReport({ browserEnabled = false } = {}) {
  const hpcHotel = {
    id: "recLuxvwwxID7U2B8",
    name: "Bethesda Marriott",
    rooms: 407,
    brand: "Marriott Hotels",
    market: "Bethesda / Montgomery County",
    submarket: "Pooks Hill / Rockville Pike",
    managementCompany: "Marriott International",
    chainScale: "Upper Upscale",
    status: "Open",
    city: "Bethesda",
    country: "United States",
  };
  const ownership = {
    hotel: { ...hpcHotel, id: "recLuxvwwxID7U2B8" },
    report: {
      ownership_and_control: {
        property_company: {
          name: "Marbeth Acquisition",
          verification_state: "Validated",
        },
        economic_owner: {
          name: "Mullen Real Estate Capital",
          founder_classification: "Proposed",
        },
        operator: { name: "Marriott International" },
      },
      corporate_contacts: { company_email: "info@mullencap.com" },
    },
    organization: {
      owned_controlled_hotels: [
        {
          hotel_name: "Bethesda Marriott",
          airtable_record_id: "recLuxvwwxID7U2B8",
          focus_hotel: true,
          rooms: 407,
          rooms_known: true,
          brand_display: "Marriott Hotels",
          market_display: "Bethesda / Montgomery County",
          operator: "Marriott International",
          status: "Open",
          economic_owner_verified: true,
          include_as_owned: true,
        },
      ],
    },
  };
  const shared = buildSharedHotelViewModel({
    hpcHotel,
    ownership,
    hotelId: "recLuxvwwxID7U2B8",
  });
  const portfolioView = buildOwnerPortfolioOperatorView(
    ownership.organization.owned_controlled_hotels,
    {
      ownerDisplayName: "Mullen Real Estate Capital",
      sharedHotelView: shared,
      focusHotelId: "recLuxvwwxID7U2B8",
    }
  );
  const submarketSnapshot = {
    ok: true,
    selectedHotel: hpcHotel,
    hotels: [hpcHotel],
    coverage: {
      partial: true,
      disclosed: true,
      marketShareFabricated: false,
      researchNeeded: true,
    },
    cohorts: {
      matchedHpcInventory: {
        id: "matched_hpc_inventory",
        hotels: [hpcHotel],
      },
      geographicSubmarketInventory: {
        id: "geographic_submarket_inventory",
        hotels: [hpcHotel],
      },
      competitiveSet: {
        id: "property_relevant_competitive_set",
        hotels: [hpcHotel],
        researchNeeded: true,
      },
    },
  };

  return buildCrossTabConsistencyReport({
    hotelId: "recLuxvwwxID7U2B8",
    mode: "Fixture",
    hpcHotel,
    hpcRecordId: "recLuxvwwxID7U2B8",
    ownership,
    portfolioView,
    submarketSnapshot,
    browserEnabled,
    // When browser enabled without renders → NOT_TESTED/FAIL per policy; fixture defaults skip.
    renderedByTab: browserEnabled
      ? {
          hotel: { rooms: 407, brand: "Marriott Hotels" },
          organization: { portfolioFocusRooms: 407, rooms: 407 },
          submarket: { submarketCohortId: "matched_hpc_inventory", rooms: 407 },
        }
      : null,
    shared,
  });
}

async function main() {
  fs.mkdirSync(REPORTS, { recursive: true });

  if (fixtureOnly) {
    const parity = validateBethesdaGoldenParityFixture();
    fs.writeFileSync(
      path.join(REPORTS, "HE-CROSS-TAB-PARITY-BETHESDA-FIXTURE-V1.json"),
      JSON.stringify(parity, null, 2)
    );

    const report = buildBethesdaFixtureReport({ browserEnabled: false });
    const artifacts = writeCrossTabConsistencyArtifacts(report, {
      reportsDir: REPORTS,
      hotelId: "recLuxvwwxID7U2B8",
    });

    // Prove SkipBrowser never yields browser PASS
    const browserSkippedRows = (report.attributes || []).flatMap((a) =>
      (a.tabResults || []).filter((t) => t.browserResult === HE_CONSISTENCY_RESULT.NOT_TESTED)
    );
    const illicitBrowserPass = (report.attributes || []).some((a) =>
      (a.tabResults || []).some(
        (t) => !report.browserEnabled && t.browserResult === HE_CONSISTENCY_RESULT.PASS
      )
    );

    // Prove Proposed→Validated classification drift fails (expected Proposed, API Validated).
    const driftHotel = {
      id: "recLuxvwwxID7U2B8",
      name: "Bethesda Marriott",
      rooms: 407,
      brand: "Marriott Hotels",
      market: "Bethesda / Montgomery County",
      managementCompany: "Marriott International",
      status: "Open",
    };
    const driftOwnership = {
      hotel: {
        ...driftHotel,
        // Explicit ownership-tab leak of Validated while founder/shared remain Proposed.
        economic_owner_classification: "Validated",
      },
      report: {
        ownership_and_control: {
          property_company: { name: "Marbeth Acquisition" },
          economic_owner: {
            name: "Mullen Real Estate Capital",
            verification_state: "Validated",
          },
          operator: { name: "Marriott International" },
        },
      },
      organization: {
        owned_controlled_hotels: [
          {
            airtable_record_id: "recLuxvwwxID7U2B8",
            focus_hotel: true,
            rooms: 407,
            rooms_known: true,
            brand_display: "Marriott Hotels",
            market_display: "Bethesda / Montgomery County",
            operator: "Marriott International",
            economic_owner_verified: true,
            include_as_owned: true,
          },
        ],
      },
    };
    const driftShared = {
      ...buildSharedHotelViewModel({
        hpcHotel: driftHotel,
        ownership: driftOwnership,
        hotelId: "recLuxvwwxID7U2B8",
      }),
      economicOwner: "Mullen Real Estate Capital",
      economicOwnerClassification: "Proposed",
    };
    const driftPortfolio = buildOwnerPortfolioOperatorView(
      driftOwnership.organization.owned_controlled_hotels,
      {
        sharedHotelView: driftShared,
        focusHotelId: "recLuxvwwxID7U2B8",
      }
    );
    const driftReport = buildCrossTabConsistencyReport({
      hotelId: "recLuxvwwxID7U2B8",
      mode: "FixtureDrift",
      hpcHotel: driftHotel,
      ownership: driftOwnership,
      portfolioView: driftPortfolio,
      submarketSnapshot: {
        ok: true,
        selectedHotel: driftHotel,
        coverage: { disclosed: true, marketShareFabricated: false, partial: true },
        cohorts: {
          matchedHpcInventory: { id: "matched_hpc_inventory", hotels: [driftHotel] },
          geographicSubmarketInventory: {
            id: "geographic_submarket_inventory",
            hotels: [driftHotel],
          },
          competitiveSet: {
            id: "property_relevant_competitive_set",
            hotels: [driftHotel],
          },
        },
      },
      browserEnabled: false,
      shared: driftShared,
    });
    const classAttr = driftReport.attributes.find(
      (a) => a.attributeId === "economicOwnerClassification"
    );
    const driftFail = (classAttr?.tabResults || []).some(
      (t) => t.result === HE_CONSISTENCY_RESULT.FAIL
    );

    console.log(
      JSON.stringify(
        {
          mode: "fixture",
          ok: parity.ok && report.ok && !illicitBrowserPass && driftFail,
          parityGates: parity.gates,
          consistencyGates: report.gates,
          e2eCertification: report.e2eCertification,
          browserSkippedNotTestedCount: browserSkippedRows.length,
          illicitBrowserPass,
          classificationDriftFails: driftFail,
          artifacts,
        },
        null,
        2
      )
    );
    process.exit(parity.ok && report.ok && !illicitBrowserPass && driftFail ? 0 : 1);
  }

  const result = await runCrossTabFactoryGates({
    hotelId,
    mode,
    browserEnabled: !skipBrowser,
    reportsDir: REPORTS,
  });

  fs.writeFileSync(
    path.join(REPORTS, `HE-CROSS-TAB-FACTORY-GATES-${hotelId}.json`),
    JSON.stringify(
      {
        ok: result.ok,
        gates: result.gates,
        cohorts: result.cohorts,
        e2eCertification: result.e2eCertification,
        artifacts: result.artifacts,
      },
      null,
      2
    )
  );

  console.log(
    JSON.stringify(
      {
        mode,
        hotelId,
        ok: result.ok,
        gates: result.gates,
        cohorts: result.cohorts,
        e2eCertification: result.e2eCertification,
        conflictCount: result.conflicts?.length || 0,
        artifacts: result.artifacts,
        summary: result.consistencyReport?.summary || null,
      },
      null,
      2
    )
  );
  process.exit(result.ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
