#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  buildFwdQualifyFindings,
  buildTrackCIdentityHandoff,
  summarizeQualClasses,
  QUAL_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-fwd-qualify-v4.js";

const f = buildFwdQualifyFindings();
assert.ok(f.length >= 8);
assert.equal(f.filter((x) => x.persistEligible).length, 0);
assert.equal(f.filter((x) => x.qualClass === QUAL_CLASS.VERIFIED_OPEN_OPPORTUNITY).length, 0);

const westin = f.find((x) => x.findingId.includes("westin_mty"));
assert.equal(westin.qualClass, QUAL_CLASS.POTENTIAL_DECISION_WINDOW);
assert.equal(westin.expirationDate, null);
assert.ok(westin.answers.evidenceStillMissing.length >= 2);

const jw = f.find((x) => x.findingId.includes("jw_mty"));
assert.equal(jw.preferredCanonicalName, "JW Marriott Monterrey Arboleda");
assert.ok(jw.aliasNames.includes("JW Marriott Monterrey Valle"));

const villa = f.find((x) => x.findingId.includes("villahermosa"));
assert.equal(villa.agreementType, "LEASE");
assert.equal(villa.qualClass, QUAL_CLASS.COMMITTED_NOT_OPEN);
assert.equal(villa.commencementDate, "2026-07-01");

const trackC = buildTrackCIdentityHandoff();
assert.ok(trackC.some((x) => x.issue.includes("ARBOLEDA")));
assert.ok(trackC.some((x) => x.issue.includes("SANTANDER") && /NO HPC CREATE/i.test(x.action)));

const ids = new Set(f.map((x) => x.findingId));
assert.equal(ids.size, f.length);

console.log(JSON.stringify({ ok: true, findings: f.length, byClass: summarizeQualClasses(f), trackC: trackC.length }, null, 2));
