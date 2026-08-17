/**
 * Unit tests — Peru MINCETUR steward review pack + insert apply (no network).
 */
import test from "node:test";
import assert from "node:assert/strict";
import { mapPeruMinceturRowToCensusCandidate } from "../lib/research-engine-v2/peru-mincetur-open-data-adapter.js";
import {
  PERU_MINCETUR_PLAN_DECISIONS,
  buildPeruMinceturHpcPlan,
} from "../lib/research-engine-v2/peru-mincetur-hpc-match-plan.js";
import {
  PERU_MINCETUR_REQUIRED_APPLY_CONFIRMS,
  PERU_MINCETUR_STEWARD_TIERS,
  buildPeruMinceturStewardReviewPack,
  classifyPeruMinceturStewardTier,
} from "../lib/research-engine-v2/peru-mincetur-steward-review-pack.js";
import {
  PERU_MINCETUR_APPLY_STATUS,
  parsePeruMinceturStewardApplyArgs,
  preparePeruMinceturStewardInsertFields,
  runPeruMinceturStewardInsertApply,
} from "../lib/research-engine-v2/peru-mincetur-steward-insert-apply.js";

const sampleHotel = {
  FECHA_CORTE: "20260806",
  CAMA: "40",
  HABI: "32",
  RUC: "20608180070",
  RAZON_SOCIAL: "Peruvian Spots S.A.C.",
  NOMBRE_COMERCIAL: "AVA SPOTS",
  DEPARTAMENTO: "CUSCO",
  PROVINCIA: "URUBAMBA",
  DISTRITO: "URUBAMBA",
  PAGINA_WEB: "www.avaspots.com",
  CLASE: "Hotel",
  CATEGORIA: "3 Estrellas",
  NRO_CERTIFICADO: "CERT-123",
  REP_LEGAL: "Jane Doe",
  VIA: "Calle",
  DES_VIA: "Principal",
  NUMERO: "10",
  TELEF1: "984000000",
};

function planRowsFromSample() {
  const c = mapPeruMinceturRowToCensusCandidate(sampleHotel);
  const plan = buildPeruMinceturHpcPlan([c], [
    {
      sourceRecordId: "CERT-123",
      proposedIdentityKey: "gov_pe_mincetur_CERT-123",
      recommendedAction: "likely_new_candidate",
    },
  ]);
  return plan.rows;
}

test("Tier A classification for URL+rooms+RUC insert candidate", () => {
  const rows = planRowsFromSample();
  assert.equal(classifyPeruMinceturStewardTier(rows[0]), PERU_MINCETUR_STEWARD_TIERS.A);
});

test("steward pack builds pilot + approval bundle confirms", () => {
  const pack = buildPeruMinceturStewardReviewPack(planRowsFromSample(), { pilotLimit: 10 });
  assert.equal(pack.type, "peru_mincetur_steward_review_pack");
  assert.equal(pack.summary.tier_a, 1);
  assert.equal(pack.proposed_inserts.length, 1);
  assert.equal(pack.approval_bundle.type, "peru_mincetur_steward_insert_approval_bundle");
  for (const flag of PERU_MINCETUR_REQUIRED_APPLY_CONFIRMS) {
    assert.ok(pack.approval_bundle.required_apply_confirms.includes(flag));
  }
  assert.equal(pack.proposed_inserts[0].fields["Owner Name"], undefined);
});

test("prepare insert fields strips Phone / Owner Name and sets Continent", () => {
  const pack = buildPeruMinceturStewardReviewPack(planRowsFromSample());
  const prep = preparePeruMinceturStewardInsertFields(pack.proposed_inserts[0]);
  assert.equal(prep.ok, true);
  assert.equal(prep.fields.Country, "Peru");
  assert.equal(prep.fields.Continent, "South America");
  assert.equal(prep.fields["Owner Name"], undefined);
  assert.equal(prep.fields.Phone, undefined);
  assert.ok(prep.fields["Official Property URL"]);
  assert.equal(prep.ownership_signal.tax_id, "20608180070");
});

test("apply args require all four confirms for live", () => {
  const a = parsePeruMinceturStewardApplyArgs([
    "--pack",
    "x.json",
    "--enable-production-writes",
    "--confirm-peru-mincetur-steward-insert",
    "--confirm-no-owner-operator-writes",
    "--confirm-hotel-property-census-only",
    "--confirm-no-legacy-census-writes",
  ]);
  assert.equal(a.allConfirmsOk, true);
  assert.equal(a.enableProductionWrites, true);
});

test("live without confirms → blocked", async () => {
  const pack = buildPeruMinceturStewardReviewPack(planRowsFromSample());
  const fakeHpc = {
    tableId: "tbl9aY5ijiuIzzWam",
    table: "Hotel Property Census",
    totalLoaded: 0,
    rows: [],
    byIdentityKey: new Map(),
    byCountry: new Map(),
  };
  const result = await runPeruMinceturStewardInsertApply({
    pack,
    args: parsePeruMinceturStewardApplyArgs([
      "--pack",
      "x.json",
      "--enable-production-writes",
      "--pilot-limit",
      "5",
    ]),
    hpc: fakeHpc,
    createRecords: async () => {
      throw new Error("should_not_write");
    },
  });
  assert.equal(result.status, PERU_MINCETUR_APPLY_STATUS.BLOCKED);
  assert.equal(result.blocked_reason, "missing_required_confirms");
  assert.equal(result.airtable_writes, false);
});

test("dry-run with confirms still does not write", async () => {
  const pack = buildPeruMinceturStewardReviewPack(planRowsFromSample());
  let wrote = false;
  const fakeHpc = {
    tableId: "tbl9aY5ijiuIzzWam",
    table: "Hotel Property Census",
    totalLoaded: 0,
    rows: [],
    byIdentityKey: new Map(),
    byCountry: new Map(),
  };
  const result = await runPeruMinceturStewardInsertApply({
    pack,
    args: parsePeruMinceturStewardApplyArgs([
      "--pack",
      "x.json",
      "--confirm-peru-mincetur-steward-insert",
      "--confirm-no-owner-operator-writes",
      "--confirm-hotel-property-census-only",
      "--confirm-no-legacy-census-writes",
    ]),
    hpc: fakeHpc,
    createRecords: async () => {
      wrote = true;
      return { created: [] };
    },
  });
  assert.equal(result.status, PERU_MINCETUR_APPLY_STATUS.DRY_RUN);
  assert.equal(result.airtable_writes, false);
  assert.equal(wrote, false);
  assert.equal(result.summary.writable_after_rededupe, 1);
});

test("live with confirms + inject create writes allowlisted fields only", async () => {
  const pack = buildPeruMinceturStewardReviewPack(planRowsFromSample());
  const fakeHpc = {
    tableId: "tbl9aY5ijiuIzzWam",
    table: "Hotel Property Census",
    totalLoaded: 0,
    rows: [],
    byIdentityKey: new Map(),
    byCountry: new Map(),
  };
  /** @type {object[]} */
  let payloads = [];
  const result = await runPeruMinceturStewardInsertApply({
    pack,
    args: parsePeruMinceturStewardApplyArgs([
      "--pack",
      "x.json",
      "--enable-production-writes",
      "--confirm-peru-mincetur-steward-insert",
      "--confirm-no-owner-operator-writes",
      "--confirm-hotel-property-census-only",
      "--confirm-no-legacy-census-writes",
    ]),
    hpc: fakeHpc,
    createRecords: async (rows) => {
      payloads = rows;
      return { created: rows.map((r, i) => ({ id: `rec${i}`, fields: r.fields })) };
    },
  });
  assert.equal(result.apply_executed, true);
  assert.equal(result.airtable_writes, true);
  assert.equal(payloads.length, 1);
  assert.equal(payloads[0].fields["Owner Name"], undefined);
  assert.equal(payloads[0].fields.Country, "Peru");
  assert.ok(!Object.values(payloads[0].fields).includes("20608180070"));
});
