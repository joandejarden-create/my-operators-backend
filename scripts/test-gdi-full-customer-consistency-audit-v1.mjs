#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runFullCustomerConsistencyAudit,
  CUSTOMER_FACING_FIELD_SOT,
  GDI_FULL_CUSTOMER_CONSISTENCY_AUDIT_V1,
} from "../lib/group-demand-intelligence/customer-consistency/gdi-full-customer-consistency-audit-v1.js";
import { customerFacingOpportunityTitle } from "../lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

check("schema_and_sot_inventory", () => {
  assert.equal(GDI_FULL_CUSTOMER_CONSISTENCY_AUDIT_V1, "gdi-full-customer-consistency-audit-v1");
  assert.ok(CUSTOMER_FACING_FIELD_SOT.length >= 6);
  assert.ok(CUSTOMER_FACING_FIELD_SOT.some((f) => f.field === "displayTitle"));
  assert.ok(CUSTOMER_FACING_FIELD_SOT.some((f) => f.field === "hotelSourcingStatusLabel"));
});

check("founder_title_examples_clean", () => {
  assert.equal(
    customerFacingOpportunityTitle(
      "NADO & DDAA Washington Conference 2027 — Crystal Gateway Marriott Host"
    ),
    "NADO & DDAA Washington Conference 2027"
  );
  assert.equal(
    customerFacingOpportunityTitle(
      "ACVNU Renal Week 2027 — Competitor Room Block at the Bethesdan Hotel"
    ),
    "ACVNU Renal Week 2027"
  );
});

{
  const name = "portfolio_audit_runs_without_throw";
  try {
    const report = await runFullCustomerConsistencyAudit({
      hotelIds: ["recLuxvwwxID7U2B8", "gdi_hotel_waterstone_boca_raton", "recnPB3Q02NO8Ma1o"],
    });
    assert.ok(report.scope.totalOpportunities > 0);
    assert.ok(Array.isArray(report.issues));
    assert.ok(typeof report.score.overall === "number");
    // UI contract: Market Scope present; verification pills not on cards
    assert.equal(
      report.issues.some((i) => i.issueId === "GDI-CCA-UI-MARKET-SCOPE-MISSING"),
      false
    );
    assert.equal(
      report.issues.some((i) => i.issueId === "GDI-CCA-UI-CARD-VERIFICATION-PILLS"),
      false
    );
    console.log(
      "PASS",
      name,
      `(opps=${report.scope.totalOpportunities}, issues=${report.totals.issues}, score=${report.score.overall})`
    );
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

check("module_wired_for_ci", () => {
  const pkg = fs.readFileSync(path.join(ROOT, "package.json"), "utf8");
  assert.match(pkg, /gdi-full-customer-consistency-audit-v1/);
  assert.ok(
    fs.existsSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/customer-consistency/gdi-full-customer-consistency-audit-v1.js"
      )
    )
  );
});

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll full customer consistency audit checks passed");
