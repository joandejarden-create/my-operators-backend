/**
 * test-gdi-maturity-actionable-parity-v1.mjs
 * ACTIONABLE === existing strict Ready gate (100% on production fixture set).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunities } from "../lib/group-demand-intelligence/repository.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  isGdiMaturityActionable,
  assignGdiMaturityState,
  GDI_MATURITY_STATE,
} from "../lib/group-demand-intelligence/gdi-maturity-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const HOTELS = [
  "recLuxvwwxID7U2B8", // Bethesda
  "rece0or38cxo3Fymb", // W Rome
  "recrPQcZg7SFARRb2", // YOTEL
];

const NOW = "2026-10-05";
let compared = 0;
let mismatches = [];

for (const hotelId of HOTELS) {
  const doc = loadOpportunities(hotelId);
  const opps = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
  for (const opp of opps) {
    const oldReady = isGdiCustomerOpportunityReady(opp, { nowDate: NOW });
    const newActionable = isGdiMaturityActionable(opp, { nowDate: NOW });
    compared += 1;
    if (oldReady.ok !== newActionable.ok) {
      mismatches.push({
        hotelId,
        id: opp.id || opp.opportunityId,
        oldReady: oldReady.ok,
        newActionable: newActionable.ok,
        oldFailed: oldReady.failed,
        newFailed: newActionable.failed,
      });
    }
    // When Ready, maturity must be ACTIONABLE
    if (oldReady.ok) {
      const m = assignGdiMaturityState(opp, { nowDate: NOW });
      if (m.gdiMaturityState !== GDI_MATURITY_STATE.ACTIONABLE) {
        mismatches.push({
          hotelId,
          id: opp.id || opp.opportunityId,
          issue: "ready_but_not_actionable_maturity",
          maturity: m.gdiMaturityState,
        });
      }
    }
  }
}

const outDir = path.join(ROOT, "reports/gdi/maturity-v1");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "actionable-parity.json"),
  JSON.stringify({ compared, mismatches, hotels: HOTELS }, null, 2) + "\n",
  "utf8"
);

assert.equal(
  mismatches.length,
  0,
  `ACTIONABLE parity failed: ${mismatches.length} mismatches of ${compared}\n${JSON.stringify(mismatches.slice(0, 5), null, 2)}`
);

console.log(
  `test-gdi-maturity-actionable-parity-v1: PASS (${compared} opportunities, 0 mismatches)`
);
