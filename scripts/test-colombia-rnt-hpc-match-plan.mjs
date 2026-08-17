/**
 * Unit tests — Colombia RNT HPC match + gated plan (no network / no Airtable).
 */
import test from "node:test";
import assert from "node:assert/strict";
import { mapColombiaRntRowToCensusCandidate } from "../lib/research-engine-v2/colombia-rnt-open-data-adapter.js";
import {
  COLOMBIA_RNT_PLAN_DECISIONS,
  buildColombiaRntHpcPlan,
  evaluateColombiaRntInsertGate,
  toColombiaRntHpcMatchInput,
} from "../lib/research-engine-v2/colombia-rnt-hpc-match-plan.js";

const sampleHotel = {
  codigo_rnt: "43",
  estado_rnt: "ACTIVO",
  razon_social_establecimiento: "HOTEL LOS BALCONES",
  departamento: "CAUCA",
  municipio: "POPAYAN",
  nit: "891501824",
  categoria: "ESTABLECIMIENTOS DE ALOJAMIENTO TURÍSTICO",
  sub_categoria: "HOTEL",
  habitaciones: "8",
  ano: "2026",
};

test("toColombiaRntHpcMatchInput maps inventory fields", () => {
  const c = mapColombiaRntRowToCensusCandidate(sampleHotel);
  const m = toColombiaRntHpcMatchInput(c);
  assert.equal(m.rawCountry, "Colombia");
  assert.equal(m.proposedIdentityKey, "gov_co_rnt_43");
  assert.match(m.rawHotelName, /BALCONES/i);
});

test("likely_existing → auto_enrich_only (no insert)", () => {
  const c = mapColombiaRntRowToCensusCandidate(sampleHotel);
  const g = evaluateColombiaRntInsertGate(c, {
    recommendedAction: "likely_existing",
    matchedCensusRecordId: "recABC",
  });
  assert.equal(g.decision, COLOMBIA_RNT_PLAN_DECISIONS.AUTO_ENRICH_ONLY);
  assert.equal(g.production_writable_insert, false);
});

test("likely_new → steward_hold_insert_candidate (never auto_insert)", () => {
  const c = mapColombiaRntRowToCensusCandidate(sampleHotel);
  const g = evaluateColombiaRntInsertGate(c, {
    recommendedAction: "likely_new_candidate",
  });
  assert.equal(g.decision, COLOMBIA_RNT_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE);
  assert.equal(g.production_writable_insert, false);
  assert.equal(g.human_review_required, true);
  assert.ok(g.reasons.includes("no_official_property_url"));
  assert.equal(g.insert_payload_preview.fields["Owner Name"], undefined);
  assert.ok(g.insert_payload_preview.ownership_signal?.tax_id);
});

test("Owner Name in fields → reject", () => {
  const c = mapColombiaRntRowToCensusCandidate(sampleHotel);
  c.fields["Owner Name"] = "Should Not Happen";
  const g = evaluateColombiaRntInsertGate(c, { recommendedAction: "likely_new_candidate" });
  assert.equal(g.decision, COLOMBIA_RNT_PLAN_DECISIONS.REJECT);
});

test("buildColombiaRntHpcPlan tallies decisions", () => {
  const c = mapColombiaRntRowToCensusCandidate(sampleHotel);
  const plan = buildColombiaRntHpcPlan([c], [
    {
      sourceRecordId: "43",
      proposedIdentityKey: "gov_co_rnt_43",
      recommendedAction: "likely_new_candidate",
      matchConfidence: "none",
    },
  ]);
  assert.equal(plan.auto_insert_enabled, false);
  assert.equal(plan.summary.steward_hold_insert_candidates, 1);
  assert.ok(plan.required_future_apply_confirms.includes("--confirm-no-owner-operator-writes"));
});
