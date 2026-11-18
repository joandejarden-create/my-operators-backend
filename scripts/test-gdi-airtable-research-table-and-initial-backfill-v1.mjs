#!/usr/bin/env node
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildHotelResearchCompositeKey,
  GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
} from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import {
  serializeResearchPayload,
  buildResearchInventoryFields,
  RESEARCH_PAYLOAD_MAX_CHARS,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import {
  buildExact85Allowlist,
  validateInitialBackfillAuthorization,
  YOTEL_GUADALAJARA_85_EXACT_WRITE_SET_AUTHORIZATION,
} from "../lib/group-demand-intelligence/workflow/gdi-yotel-guadalajara-research-backfill-v1.js";
import { buildResearchOpportunityFields } from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-opportunities-schema-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

assert.equal(
  buildHotelResearchCompositeKey("recHotel", "opp1"),
  "recHotel::opp1",
);
assert.equal(buildHotelResearchCompositeKey("", "opp1"), null);

const fields = buildResearchOpportunityFields();
assert.ok(fields.some((f) => f.name === "compositeKey"));
assert.ok(fields.some((f) => f.name === "researchPayloadJson"));
assert.equal(fields[0].name, "compositeKey");
assert.equal(
  GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
  "GDI Hotel Research Opportunities",
);

{
  const ok = serializeResearchPayload({
    id: "x",
    title: "t",
    orsRaw: "drop-me",
  });
  assert.equal(ok.ok, true);
  assert.ok(!ok.json.includes("drop-me"));
}

{
  const huge = { id: "x", blob: "y".repeat(RESEARCH_PAYLOAD_MAX_CHARS) };
  const bad = serializeResearchPayload(huge);
  assert.equal(bad.ok, false);
  assert.equal(bad.code, "RESEARCH_PAYLOAD_TOO_LARGE");
}

{
  const built = buildResearchInventoryFields(
    {
      id: "opp_a",
      organizationName: "Org",
      title: "Title",
      primaryContact: { name: "A", role: "Planner" },
    },
    { hotelId: "recH" },
  );
  assert.equal(built.ok, true);
  assert.equal(built.compositeKey, "recH::opp_a");
  assert.equal(built.fields.hotelId, "recH");
  assert.equal(built.fields.opportunityId, "opp_a");
}

{
  const auth = validateInitialBackfillAuthorization({
    authorizeProductionWrites: true,
    exactWriteSetAuthorization: YOTEL_GUADALAJARA_85_EXACT_WRITE_SET_AUTHORIZATION,
    apply: true,
  });
  assert.equal(auth.ok, true);
  assert.equal(auth.apply, true);
}

assert.equal(
  validateInitialBackfillAuthorization({ apply: true }).ok,
  false,
);

{
  const allow = buildExact85Allowlist(ROOT);
  assert.equal(allow.exactMatch, true, JSON.stringify(allow.errors));
  assert.equal(allow.actualTotal, 85);
  assert.equal(allow.yotelCount, 42);
  assert.equal(allow.guadalajaraCount, 43);
  assert.equal(allow.safeguards.noGdoWrites, true);
  const keys = new Set(allow.entries.map((e) => e.compositeKey));
  assert.equal(keys.size, 85);
}

console.log("test-gdi-airtable-research-table-and-initial-backfill-v1: PASS");
