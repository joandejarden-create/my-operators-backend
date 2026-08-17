#!/usr/bin/env node
import "../load-env.js";
import { planMarriottCensusEnrichment } from "../lib/hotel-census/plan-marriott-census-enrichment.js";

const plan = await planMarriottCensusEnrichment({ minConfidence: "low" });
const ids = ["recVnsDI03rZXQSm4", "recaiJ11ElqigjE42", "recfNuf3mRidPkKD2", "reckNY4SQHzXjWLBD", "rec1SSmWonlA2X3Wa", "recdq02zLON5fwU3O"];

console.log("Ready:", plan.readyToApply, "Skipped:", plan.skipped.length);
for (const id of ids) {
  const row = plan.planRows.find((r) => r.censusRecordId === id);
  const skip = plan.skipped.find((r) => r.censusRecordId === id);
  console.log("\n", id, row ? { fields: Object.keys(row.applyFields), web: row.websiteSuggested } : skip?.reason || "not in plan/skipped");
}

const marshaRows = plan.planRows.filter((r) => ["SDQAL", "PLSRR", "LIRWI"].includes(r.marshaCode));
console.log("\nPlan rows for SDQAL/PLSRR/LIRWI:", marshaRows.length);
for (const r of marshaRows) console.log(r.marshaCode, r.censusRecordId, r.censusName, Object.keys(r.applyFields));
