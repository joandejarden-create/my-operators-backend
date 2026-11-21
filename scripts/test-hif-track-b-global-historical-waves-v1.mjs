#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  lackOfNewerFilingProvesCurrent,
  isPortfolioOrArtifactName,
  resolveExplicitAgreementType,
  timingBucket,
  contractPersistAllowed,
  namedInstrumentsInClause,
  executedInstrumentsInClause,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";
import { isTrackAReservedId } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-historical-foundation-v1.js";
import { GLOBAL_WAVE_DOSSIERS, STAGED_PORTFOLIO_FINDINGS } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-wave-dossiers-v1.js";

assert.equal(lackOfNewerFilingProvesCurrent(), false);
assert.equal(isPortfolioOrArtifactName("Carolina Hotel Portfolio Holiday Inn Express"), true);
assert.equal(isPortfolioOrArtifactName("ted Mortgage Loan Starwood Capital Group Hotel Portfolio Hampton Inn"), true);
assert.equal(isPortfolioOrArtifactName("Courtyard Tulsa"), false);
assert.equal(isPortfolioOrArtifactName("Hotel Indigo"), true);

const loan = resolveExplicitAgreementType("The mortgage loan maturity date is June 1, 2030.");
assert.equal(loan.reject, "LOAN_OR_CMBS_MATURITY_NOT_HOTEL_CONTRACT");
assert.equal(loan.resolved, false);

const combined = resolveExplicitAgreementType(
  "Expiration of the franchise agreement or management agreement is listed in one column.",
);
assert.equal(combined.resolved, false);
assert.equal(combined.reason, "COMBINED_OR_AMBIGUOUS_INSTRUMENT");

const mgmt = resolveExplicitAgreementType("We terminated our management agreements for the resort effective December 15, 2023.");
assert.equal(mgmt.agreementType, "MANAGEMENT");
assert.equal(mgmt.resolved, true);

const lease = resolveExplicitAgreementType("FibraHotel celebró un contrato de arrendamiento para el hotel.");
assert.equal(lease.agreementType, "LEASE");

assert.equal(timingBucket("2023-12-15", "2026-11-21").bucket, "HISTORICAL_DATE");
assert.equal(timingBucket("2027-05-09", "2026-11-21").bucket, "WITHIN_24_MONTHS");
assert.equal(timingBucket(null, "2026-11-21").bucket, "UNDATED_OR_IMPRECISE");

assert.equal(isTrackAReservedId("disc_cb1_lakeville_inn_mn_2026"), true);
assert.equal(GLOBAL_WAVE_DOSSIERS.some((d) => isTrackAReservedId(d.discoveryPropertyId)), false);
assert.ok(GLOBAL_WAVE_DOSSIERS.some((d) => d.physicalCountry === "MX" && d.agreementType === "MANAGEMENT"));
assert.ok(GLOBAL_WAVE_DOSSIERS.some((d) => d.physicalCountry === "BR" && d.contractEligible === false));
assert.equal(
  contractPersistAllowed(GLOBAL_WAVE_DOSSIERS.find((d) => d.dateConflict === true)),
  false,
);
assert.ok(STAGED_PORTFOLIO_FINDINGS.every((d) => d.persistEligible === false));
assert.match(
  STAGED_PORTFOLIO_FINDINGS.find((d) => d.physicalCountry === "GB").evidenceSnippet,
  /2005/,
);

const villahermosa =
  "El hotel de Servicio Limitado Villahermosa terminó su contrato de operación y franquicia el 1 de julio de 2026 e iniciará operaciones bajo un esquema de arrendamiento con la marca InHouse.";
assert.deepEqual(namedInstrumentsInClause(villahermosa), ["FRANCHISE", "OPERATING", "LEASE"]);
assert.deepEqual(executedInstrumentsInClause(villahermosa), ["FRANCHISE", "OPERATING"]);
assert.equal(executedInstrumentsInClause(villahermosa).includes("MANAGEMENT"), false);

console.log("test-hif-track-b-global-historical-waves-v1: PASS");
