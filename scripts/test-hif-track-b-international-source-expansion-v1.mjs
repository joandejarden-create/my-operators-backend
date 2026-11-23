#!/usr/bin/env node
/**
 * Unit checks for Track B international source expansion V1.
 * CUSTOMER_BLOCKED.
 */
import assert from "node:assert/strict";
import {
  yearOnlyExpirationPolicy,
  classifyEvidenceAuthority,
  EVIDENCE_CLASS,
  SOURCE_FAMILIES,
  TRACK_A_SOURCE_PRODUCTIVITY_RANK,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-international-source-registry-v1.js";
import {
  buildInternationalSourceExpansionDossiers,
  CLDT_FRANCHISE_ROWS,
  FIBRA_INN_28,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-international-source-expansion-dossiers-v1.js";
import { contractPersistAllowed } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";

assert.equal(TRACK_A_SOURCE_PRODUCTIVITY_RANK[0], "A_owner_contractual_schedules");
assert.ok(SOURCE_FAMILIES.MX_BMV_EMISNET.spanishTerms.includes("contrato de administración"));
assert.ok(SOURCE_FAMILIES.CMBS_DEBT_CAPITAL.rule.includes("never proof"));

const y = yearOnlyExpirationPolicy(2026);
assert.equal(y.ok, true);
assert.equal(y.expirationDate, null);
assert.equal(y.milestoneDate, null);
assert.equal(y.datePrecision, "YEAR_ONLY");
assert.equal(yearOnlyExpirationPolicy(1800).ok, false);

assert.equal(
  classifyEvidenceAuthority("OWNER_FRANCHISE_HMA_SCHEDULE", true),
  EVIDENCE_CLASS.CORPORATE_CONTRACTUAL_DISCLOSURE,
);
assert.equal(classifyEvidenceAuthority("CMBS_DEBT_CAPITAL", true), EVIDENCE_CLASS.CAPITAL_CONTEXT_NOT_CONVERSION);

const dossiers = buildInternationalSourceExpansionDossiers();
assert.equal(dossiers.length, CLDT_FRANCHISE_ROWS.length + 1 + 5 + FIBRA_INN_28.length);
assert.equal(FIBRA_INN_28.length, 28);
assert.equal(CLDT_FRANCHISE_ROWS.length, 33);

const holtsvilleFran = dossiers.find((d) => d.findingId?.includes("holtsville") && d.agreementType === "FRANCHISE");
assert.ok(holtsvilleFran);
assert.equal(holtsvilleFran.expirationDate, null);
assert.equal(holtsvilleFran.expirationYear, 2026);
assert.equal(contractPersistAllowed(holtsvilleFran), true);

const capital = dossiers.find((d) => d.evidenceClass === EVIDENCE_CLASS.CAPITAL_CONTEXT_NOT_CONVERSION);
assert.ok(capital);
assert.equal(capital.contractEligible, false);
assert.equal(contractPersistAllowed(capital), false);

const fibra = dossiers.filter((d) => d.physicalCountry === "MX");
assert.equal(fibra.length, 28);
assert.ok(fibra.every((d) => d.milestoneDate === "2026-01-15"));
assert.ok(fibra.every((d) => d.priorOperator?.includes("Aimbridge")));
assert.ok(fibra.every((d) => d.currentOperator?.includes("Servicios Hoteleros FINN")));

const findingIds = new Set(dossiers.map((d) => d.findingId));
assert.equal(findingIds.size, dossiers.length, "findingId must be unique");

const marg = dossiers.find((d) => d.findingId?.includes("terminable_202609"));
assert.ok(marg);
assert.equal(marg.milestoneDate, null);
assert.ok(marg.impreciseDates?.some((x) => x.includes("2026-09")));

console.log(
  JSON.stringify(
    {
      ok: true,
      dossiers: dossiers.length,
      cldtFranchise: CLDT_FRANCHISE_ROWS.length,
      fibraInn: FIBRA_INN_28.length,
      capitalBlockedFromContract: true,
      yearOnlyHoltsville: true,
    },
    null,
    2,
  ),
);
