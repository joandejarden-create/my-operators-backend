/**
 * Unit tests — census intake Autopilot apply (mocked Airtable).
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  INTAKE_APPLY_STATUS,
  checkIntakeApplyEnv,
  rededupeIntakeInsertsByIdentityKey,
  runIntakeAutopilotApply,
} from "../lib/independent-census/intake-autopilot-apply.js";
import { writeFileSync, mkdirSync, unlinkSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";

const CONFIRMS = [
  "--confirm-safe-writes",
  "--confirm-write-to-production-census",
  "--confirm-no-brand-explorer-writes",
  "--confirm-no-owner-operator",
  "--confirm-no-date-writes",
  "--confirm-no-recent-momentum",
  "--confirm-no-company-validation",
  "--confirm-webhound-not-production-source",
  "--confirm-intake-inserts",
  "--confirm-no-legacy-hotel-census",
];

function sampleFields(over = {}) {
  return {
    "Property Name": "Hotel Europa",
    "Canonical Property Name": "Hotel Europa",
    "Property Identity Key": "osm_do_node_test_1",
    "Current Brand": "Independent",
    "Affiliation Status": "Independent",
    Country: "Dominican Republic",
    City: "Sosua",
    "Official Property URL": "https://www.hotelplazaeuropa.com",
    "Source URL": "https://www.openstreetmap.org/node/1",
    "Family / Source Family": "independent_open_sources",
    "Source Type": "other",
    "Source Confidence": "High",
    "Identity Confidence": "High",
    "VIC Freeze Hash": "independent_census_dr_osm_2026-08-07",
    "Production Use Status": "Census Only / Not Owner-Facing",
    "Enrichment Status": "Discovered — pending enrichment",
    "Enrichment Priority": "High",
    "Human Review Required": false,
    "Data Eligible": true,
    "Discovery Date": "2026-08-07",
    "Last Reviewed Date": "2026-08-07",
    "Independent Hotel Flag": true,
    ...over,
  };
}

test("rededupe blocks existing identity keys", () => {
  const { writable, blocked } = rededupeIntakeInsertsByIdentityKey(
    [
      {
        source_record_id: "node/1",
        lane: "independent_unaffiliated",
        intake_class: "independent_l1_promote",
        quality_score: 100,
        fields: sampleFields(),
      },
      {
        source_record_id: "node/2",
        lane: "independent_unaffiliated",
        intake_class: "independent_l1_promote",
        quality_score: 100,
        fields: sampleFields({
          "Property Identity Key": "osm_do_node_test_2",
          "Property Name": "Other Hotel",
        }),
      },
    ],
    [{ identityKey: "osm_do_node_test_1" }]
  );
  assert.equal(writable.length, 1);
  assert.equal(blocked.length, 1);
  assert.equal(blocked[0].block_reason, "property_identity_key_already_in_hpc");
});

test("checkIntakeApplyEnv requires flags", () => {
  const bad = checkIntakeApplyEnv({});
  assert.equal(bad.allOk, false);
  const good = checkIntakeApplyEnv({
    ALLOW_CENSUS_AUTOPILOT_APPLY: "1",
    CONFIRM_WRITE_TO_PRODUCTION_CENSUS: "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES: "1",
    CONFIRM_NO_OWNER_OPERATOR_WRITES: "1",
  });
  assert.equal(good.allOk, true);
});

test("runIntakeAutopilotApply dry-run + mocked apply", async () => {
  const dir = join(tmpdir(), `intake-apply-${Date.now()}`);
  mkdirSync(dir, { recursive: true });
  const bundlePath = join(dir, "bundle.json");
  writeFileSync(
    bundlePath,
    JSON.stringify({
      version: "census-intake-autopilot-controlled-v1",
      batch_id: "test-batch",
      cohort: "no_hr",
      approval_bundle_ready: true,
      legacy_hotel_census_used: false,
      write_target: {
        base: "Deal Capture Platform",
        table: "Hotel Property Census",
        table_id: "tbl9aY5ijiuIzzWam",
      },
      inserts: [
        {
          source_record_id: "node/1",
          lane: "independent_unaffiliated",
          intake_class: "independent_l1_promote",
          quality_score: 100,
          fields: sampleFields(),
        },
      ],
    })
  );

  const dry = await runIntakeAutopilotApply({
    argv: ["--approval-bundle", bundlePath, "--cohort", "no_hr"],
    doWrite: false,
    censusRecords: [],
    skipLiveCensusRead: true,
  });
  assert.equal(dry.status, INTAKE_APPLY_STATUS.DRY_RUN);
  assert.equal(dry.writable_after_rededupe, 1);
  assert.equal(dry.airtable_writes, false);

  let createdPayload = null;
  const applied = await runIntakeAutopilotApply({
    argv: [
      "--approval-bundle",
      bundlePath,
      "--cohort",
      "no_hr",
      "--apply",
      "--enable-production-writes",
      ...CONFIRMS,
    ],
    doWrite: true,
    censusRecords: [],
    skipLiveCensusRead: true,
    env: {
      ALLOW_CENSUS_AUTOPILOT_APPLY: "1",
      CONFIRM_WRITE_TO_PRODUCTION_CENSUS: "1",
      CONFIRM_NO_BRAND_EXPLORER_WRITES: "1",
      CONFIRM_NO_OWNER_OPERATOR_WRITES: "1",
    },
    createRecords: async (rows) => {
      createdPayload = rows;
      return {
        created: rows.map((r, i) => ({
          id: `recTEST${i}`,
          fields: r.fields,
        })),
      };
    },
  });
  assert.equal(applied.status, INTAKE_APPLY_STATUS.CLEAN);
  assert.equal(applied.airtable_writes, true);
  assert.equal(applied.created_count, 1);
  assert.ok(createdPayload?.length === 1);

  try {
    unlinkSync(bundlePath);
  } catch {
    // ignore
  }
});
