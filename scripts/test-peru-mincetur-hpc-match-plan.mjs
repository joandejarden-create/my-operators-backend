/**
 * Unit tests — Peru MINCETUR HPC match + gated plan (no network / no Airtable).
 */
import test from "node:test";
import assert from "node:assert/strict";
import { mapPeruMinceturRowToCensusCandidate } from "../lib/research-engine-v2/peru-mincetur-open-data-adapter.js";
import {
  PERU_MINCETUR_PLAN_DECISIONS,
  buildPeruMinceturHpcPlan,
  evaluatePeruMinceturInsertGate,
  toPeruMinceturHpcMatchInput,
} from "../lib/research-engine-v2/peru-mincetur-hpc-match-plan.js";

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

test("toPeruMinceturHpcMatchInput maps inventory fields", () => {
  const c = mapPeruMinceturRowToCensusCandidate(sampleHotel);
  const m = toPeruMinceturHpcMatchInput(c);
  assert.equal(m.rawCountry, "Peru");
  assert.equal(m.proposedIdentityKey, "gov_pe_mincetur_CERT-123");
  assert.match(m.rawHotelName, /AVA SPOTS/i);
  assert.match(m.rawWebsite, /avaspots/i);
});

test("likely_existing → auto_enrich_only (no insert)", () => {
  const c = mapPeruMinceturRowToCensusCandidate(sampleHotel);
  const g = evaluatePeruMinceturInsertGate(c, {
    recommendedAction: "likely_existing",
    matchedCensusRecordId: "recABC",
  });
  assert.equal(g.decision, PERU_MINCETUR_PLAN_DECISIONS.AUTO_ENRICH_ONLY);
  assert.equal(g.production_writable_insert, false);
});

test("likely_new with PAGINA_WEB → steward_hold_insert_candidate (never auto_insert)", () => {
  const c = mapPeruMinceturRowToCensusCandidate(sampleHotel);
  const g = evaluatePeruMinceturInsertGate(c, {
    recommendedAction: "likely_new_candidate",
  });
  assert.equal(g.decision, PERU_MINCETUR_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE);
  assert.equal(g.production_writable_insert, false);
  assert.equal(g.human_review_required, true);
  assert.ok(g.reasons.includes("official_property_url_from_pagina_web"));
  assert.equal(g.insert_payload_preview.fields["Owner Name"], undefined);
  assert.ok(g.insert_payload_preview.ownership_signal?.tax_id);
});

test("likely_new without website → no_official_property_url reason", () => {
  const c = mapPeruMinceturRowToCensusCandidate({ ...sampleHotel, PAGINA_WEB: "" });
  const g = evaluatePeruMinceturInsertGate(c, {
    recommendedAction: "likely_new_candidate",
  });
  assert.equal(g.decision, PERU_MINCETUR_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE);
  assert.ok(g.reasons.includes("no_official_property_url"));
});

test("Owner Name in fields → reject", () => {
  const c = mapPeruMinceturRowToCensusCandidate(sampleHotel);
  c.fields["Owner Name"] = "Should Not Happen";
  const g = evaluatePeruMinceturInsertGate(c, { recommendedAction: "likely_new_candidate" });
  assert.equal(g.decision, PERU_MINCETUR_PLAN_DECISIONS.REJECT);
});

test("buildPeruMinceturHpcPlan tallies decisions", () => {
  const c = mapPeruMinceturRowToCensusCandidate(sampleHotel);
  const plan = buildPeruMinceturHpcPlan([c], [
    {
      sourceRecordId: "CERT-123",
      proposedIdentityKey: "gov_pe_mincetur_CERT-123",
      recommendedAction: "likely_new_candidate",
      matchConfidence: "none",
    },
  ]);
  assert.equal(plan.auto_insert_enabled, false);
  assert.equal(plan.summary.steward_hold_insert_candidates, 1);
  assert.equal(plan.summary.insert_candidates_with_official_url, 1);
  assert.ok(plan.required_future_apply_confirms.includes("--confirm-no-owner-operator-writes"));
});
