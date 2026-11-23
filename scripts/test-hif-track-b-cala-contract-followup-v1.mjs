#!/usr/bin/env node
import assert from "node:assert/strict";
import { buildCalaContractFollowupDossiers } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-cala-contract-followup-dossiers-v1.js";
import { contractPersistAllowed } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";

const d = buildCalaContractFollowupDossiers();
assert.equal(d.length, 12);
assert.ok(d.some((x) => x.findingId.includes("franchise_10_20yr_gap") && x.persistEligible === false));
assert.ok(d.some((x) => x.hotelName.includes("Westin Monterrey Valle") && x.currentOperator === "Aimbridge Latam"));
assert.ok(d.some((x) => x.hotelName.includes("JW Marriott") && x.currentOperator === "Aimbridge Latam"));
assert.ok(d.some((x) => x.findingId.includes("puebla_hma_nonrenew") && x.milestoneDate === "2025-07-24"));
const vill = d.find((x) => x.findingId.includes("villahermosa_lease"));
assert.equal(vill.agreementType, "LEASE");
assert.equal(vill.commencementDate, "2026-07-01");
const yuc = d.find((x) => x.findingId.includes("yucatan_lease"));
assert.equal(yuc.contractEligible, false);
assert.equal(contractPersistAllowed(yuc), false);
const sant = d.find((x) => x.findingId.includes("santander"));
assert.equal(sant.persistEligible, false);
const ids = new Set(d.map((x) => x.findingId));
assert.equal(ids.size, d.length);
console.log(JSON.stringify({ ok: true, dossiers: d.length, persistEligible: d.filter((x) => x.persistEligible).length }, null, 2));
