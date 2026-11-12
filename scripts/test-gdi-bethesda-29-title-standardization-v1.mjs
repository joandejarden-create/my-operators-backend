#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_29_STANDARDIZED_TITLES,
  isGenericHistoricalTitle,
  auditBethesda29Titles,
  stageBethesda29TitleStandardization,
} from "../lib/group-demand-intelligence/research/bethesda-29-title-standardization-v1.js";
import { BETHESDA_29_ATTESTED_IDS as ATTESTED } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";
import { loadHotelOpportunityDoc } from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

assert.equal(Object.keys(BETHESDA_29_STANDARDIZED_TITLES).length, 29);
assert.equal(ATTESTED.length, 29);
for (const id of ATTESTED) {
  assert.ok(BETHESDA_29_STANDARDIZED_TITLES[id], `missing title map for ${id}`);
}

assert.equal(
  isGenericHistoricalTitle(
    "Greater Bethesda Chamber of Commerce — Local Catering & Day Meetings",
    "Greater Bethesda Chamber of Commerce"
  ),
  true
);
assert.equal(
  isGenericHistoricalTitle(
    "PDA TRI Bethesda training — December 2026 lodging research",
    "Parenteral Drug Association (PDA)"
  ),
  false
);

const storeBefore = loadHotelOpportunityDoc("recLuxvwwxID7U2B8", { root: ROOT });
assert.equal(storeBefore.count, 59);

const dry = stageBethesda29TitleStandardization({ root: ROOT, dryRun: true });
assert.equal(dry.ok, true);
assert.equal(dry.writeApplied, false);

const applied = stageBethesda29TitleStandardization({ root: ROOT, dryRun: false });
assert.equal(applied.ok, true);

const audit = auditBethesda29Titles({ root: ROOT });
assert.equal(audit.count, 29);
assert.equal(audit.changedCount, 0, "second audit should show titles already standardized");
for (const row of audit.crosswalk) {
  assert.equal(row.afterTitle, BETHESDA_29_STANDARDIZED_TITLES[row.id]);
  assert.equal(
    isGenericHistoricalTitle(row.afterTitle, row.organizationName),
    false,
    `still generic: ${row.id} → ${row.afterTitle}`
  );
}

const storeAfter = loadHotelOpportunityDoc("recLuxvwwxID7U2B8", { root: ROOT });
assert.equal(storeAfter.sha256, storeBefore.sha256, "canonical 59-row store must be untouched");
assert.equal(storeAfter.count, 59);

const union = mergeBethesdaInternalResearchOpportunities(
  "recLuxvwwxID7U2B8",
  storeAfter.opportunities,
  { root: ROOT }
);
assert.equal(union.counts.union, 88);

// Exa three titles present and non-generic.
for (const id of [
  "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
  "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
  "gdi_pep_live_c165c897d233",
]) {
  const u = union.opportunities.find((r) => (r.id || r.opportunityId) === id);
  assert.ok(u);
  assert.equal(u.title, BETHESDA_29_STANDARDIZED_TITLES[id]);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-29-title-standardization-v1",
      titlesMapped: 29,
      storeShaUnchanged: true,
      union: union.counts.union,
    },
    null,
    2
  )
);
