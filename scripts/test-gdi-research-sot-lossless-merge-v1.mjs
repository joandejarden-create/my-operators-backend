#!/usr/bin/env node
/**
 * Permanent non-regression: GDI research SoT lossless semantic merge V1.
 * Destructive slim-over-fat candidates must fail before any Airtable mutation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
  classifySotPayloadKey,
  SOT_CERTIFICATION_OWNED_KEYS,
} from "../lib/group-demand-intelligence/workflow/gdi-research-sot-field-ownership-v1.js";
import {
  GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
  mergeResearchSotLossless,
  detectSotMergeConflicts,
} from "../lib/group-demand-intelligence/workflow/gdi-research-sot-lossless-merge-v1.js";
import {
  mergeProposedResearchInventoryFields,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import { buildSlimResearchPayload } from "../lib/group-demand-intelligence/workflow/gdi-research-inventory-slim-payload-v1.js";

assert.equal(
  GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
  "gdi-research-sot-field-ownership-v1",
);
assert.equal(
  GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
  "gdi-research-sot-lossless-merge-v1",
);
assert.equal(classifySotPayloadKey("meetingHistory"), "AUTHORITATIVE_RESEARCH");
assert.equal(
  classifySotPayloadKey("hotelSpecificQualification"),
  "CERTIFICATION",
);
assert.equal(
  classifySotPayloadKey("unknownHistoricalBlob"),
  "AUTHORITATIVE_RESEARCH_UNKNOWN",
);
assert.ok(SOT_CERTIFICATION_OWNED_KEYS.includes("hotelSpecificQualification"));

const fatLive = {
  opportunityId: "gdi_opp_nice_2027",
  organizationName: "NIST / NICE",
  commercialDemandType: "BOTH",
  revenueStreamClass: "COMBINED_GUESTROOMS_EVENT_FNB",
  revenueStreamLabel: "Combined guestrooms and meetings/F&B/event revenue",
  summaryWhat: "NIST NICE 18th Annual Conference and Expo, June 7–9, 2027; location TBA.",
  fitExplanation:
    "Prior years ~400–450 attendees at full-service Marriott. Venue still TBA for 2027.",
  meetingHistory: [
    {
      year: 2026,
      city: "Philadelphia, PA",
      venue: "Philadelphia Marriott Downtown",
      attendance: "400+",
      claimKind: "FACT",
    },
    {
      year: 2025,
      city: "Denver, CO",
      venue: "UNKNOWN",
      attendance: "450+",
      claimKind: "FACT",
    },
  ],
  competitors: [
    { name: "Hyatt Regency Bethesda", class: "STR_COMP_SET" },
    { name: "Bethesda North Marriott Hotel & Conference Center", class: "RELEVANT_GROUP_DEMAND_ALTERNATIVE" },
  ],
  evidence: [
    {
      id: "gdi_ev_1",
      field: "event_dates",
      value: "June 7–9, 2027",
      claimKind: "FACT",
      confidence: 80,
      sourceUrl: "https://www.nist.gov/nice-2027",
    },
    {
      id: "gdi_ev_2",
      field: "venue",
      value: "Location to be announced",
      claimKind: "FACT",
      confidence: 80,
      sourceUrl: "https://www.nist.gov/nice-2027",
    },
    {
      id: "gdi_ev_3",
      field: "contact",
      value: "karen.wetzel@nist.gov",
      claimKind: "FACT",
      confidence: 80,
      sourceUrl: "https://www.nist.gov/staff",
    },
  ],
  primaryContact: {
    name: "Karen Wetzel",
    email: "karen.wetzel@nist.gov",
    contactGrade: "A",
    phone: "240-439-0767",
  },
  backupContacts: [
    {
      name: "Susana Barraza",
      email: "susana.barraza@nist.gov",
      contactGrade: "A",
    },
  ],
  contacts: [
    { name: "Karen Wetzel", email: "karen.wetzel@nist.gov", contactGrade: "A" },
    { name: "Susana Barraza", email: "susana.barraza@nist.gov", contactGrade: "A" },
  ],
  contactResearchAudit: {
    pass: "gdi_contact_resolution_v1",
    gradeBefore: "A",
    gradeAfter: "A",
    acceptedPrimary: { name: "Karen Wetzel", contactGrade: "A" },
  },
  likelyCompetitor: "Hyatt Regency Bethesda",
  historicalBlobKeepMe: { nested: true, value: 42 },
};

const slimDestructive = {
  opportunityId: "gdi_opp_nice_2027",
  organizationName: "NICE",
  commercialDemandType: "GUESTROOMS",
  revenueStreamClass: "GUESTROOMS_ONLY",
  whatIsHappening:
    "Exhibitors and attendees create guestroom and networking demand for the annual cycle.",
  summaryWhat: "Annual conference.",
  fitExplanation: "Exhibitors and attendees create guestroom and networking demand.",
  meetingHistory: [],
  competitors: [],
  evidence: [
    { url: "https://www.nist.gov/nice-2027" },
    { url: "https://example.org/other" },
  ],
  primaryContact: { name: "Karen Wetzel", email: "karen.wetzel@nist.gov" },
  contacts: [{ name: "Karen Wetzel", email: "karen.wetzel@nist.gov" }],
  hotelSpecificQualification: {
    finalizedQualified: true,
    status: "PASS",
    schemaVersion: "fqo-v1",
  },
  finalizedQualified: true,
  certificationSchemaVersion: "gdi-fqo-cert-v1",
};

// --- Negative: conflicts detected ---
const conflicts = detectSotMergeConflicts(fatLive, slimDestructive);
assert.ok(
  conflicts.some((c) => c.code === "REVENUE_STREAM_DOWNGRADE"),
  "must detect revenue stream downgrade",
);
assert.ok(
  conflicts.some((c) => c.code === "COMMERCIAL_DEMAND_TYPE_DOWNGRADE"),
  "must detect commercialDemandType downgrade",
);
assert.ok(
  conflicts.some((c) => c.code === "MEETING_HISTORY_ERASURE"),
  "must detect meeting history erasure",
);
assert.ok(
  conflicts.some((c) => c.code === "EVIDENCE_COLLAPSED_TO_STUBS"),
  "must detect evidence collapsed to stubs",
);

// --- Positive: lossless merge preserves SoT + adds cert ---
const merged = mergeResearchSotLossless({
  livePayload: fatLive,
  proposedPayload: slimDestructive,
});
assert.equal(merged.ok, true, `merge failed: ${merged.code} ${JSON.stringify(merged.blockers)}`);
assert.equal(merged.payload.revenueStreamClass, "COMBINED_GUESTROOMS_EVENT_FNB");
assert.equal(merged.payload.commercialDemandType, "BOTH");
assert.equal(merged.payload.meetingHistory.length, 2);
assert.equal(merged.payload.competitors.length, 2);
assert.equal(merged.payload.evidence.length >= 2, true);
assert.ok(
  merged.payload.evidence.some(
    (e) => e.id === "gdi_ev_1" && e.value === "June 7–9, 2027" && e.claimKind === "FACT",
  ),
  "FACT evidence with value must survive",
);
assert.equal(merged.payload.primaryContact.contactGrade, "A");
assert.equal(merged.payload.backupContacts.length, 1);
assert.deepEqual(merged.payload.historicalBlobKeepMe, { nested: true, value: 42 });
assert.ok(merged.payload.hotelSpecificQualification?.finalizedQualified === true);
assert.equal(merged.payload.contactResearchAudit.pass, "gdi_contact_resolution_v1");
assert.ok(
  !String(merged.payload.whatIsHappening || "").includes("Exhibitors and attendees"),
  "must not inject exhibitor template narrative",
);
assert.ok(merged.payload.summaryWhat.includes("NIST NICE 18th"));
assert.ok(merged.length > 2000, `merged payload too thin: ${merged.length}`);
assert.equal(merged.storageLimitCompatible, true);

// --- Negative via store merge: slim-over-fat must not delete unknowns ---
const storeMerge = mergeProposedResearchInventoryFields(
  {
    [F.opportunityId]: "gdi_opp_nice_2027",
    [F.hotelId]: "recLuxvwwxID7U2B8",
    [F.organizationName]: "NICE",
    [F.researchPayloadJson]: JSON.stringify(slimDestructive),
  },
  {
    [F.opportunityId]: "gdi_opp_nice_2027",
    [F.hotelId]: "recLuxvwwxID7U2B8",
    [F.organizationName]: "NIST / NICE",
    [F.researchPayloadJson]: JSON.stringify(fatLive),
  },
);
assert.equal(storeMerge.ok, true, `store merge failed: ${storeMerge.code}`);
const after = JSON.parse(storeMerge.fields[F.researchPayloadJson]);
assert.equal(after.revenueStreamClass, "COMBINED_GUESTROOMS_EVENT_FNB");
assert.deepEqual(after.historicalBlobKeepMe, { nested: true, value: 42 });
assert.equal(after.meetingHistory.length, 2);
assert.ok(after.hotelSpecificQualification);

// --- Slim builder alone must not be treated as SoT replacement size ---
const slimOnly = buildSlimResearchPayload(slimDestructive);
assert.equal(slimOnly.ok, true);
assert.ok(
  slimOnly.length < merged.length,
  "slim package must remain shorter than lossless merged SoT",
);

// --- NICE live fixture regression when snapshot present ---
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const nicePath = path.join(
  ROOT,
  "reports/gdi-validated-airtable-canary-prewrite-safety-v1/04b-ORIGINAL_researchPayloadJson.json",
);
const nicePropPath = path.join(
  ROOT,
  "reports/gdi-validated-airtable-canary-prewrite-safety-v1/05b-PROPOSED_researchPayloadJson.json",
);
if (fs.existsSync(nicePath) && fs.existsSync(nicePropPath)) {
  const niceLive = JSON.parse(fs.readFileSync(nicePath, "utf8"));
  const niceProp = JSON.parse(fs.readFileSync(nicePropPath, "utf8"));
  const niceMerge = mergeResearchSotLossless({
    livePayload: niceLive,
    proposedPayload: niceProp,
  });
  assert.equal(niceMerge.ok, true, `NICE merge failed: ${niceMerge.code}`);
  assert.equal(niceMerge.payload.commercialDemandType, "BOTH");
  assert.equal(
    niceMerge.payload.revenueStreamClass,
    "COMBINED_GUESTROOMS_EVENT_FNB",
  );
  assert.ok(
    Array.isArray(niceMerge.payload.meetingHistory) &&
      niceMerge.payload.meetingHistory.length >= 2,
  );
  assert.ok(
    Array.isArray(niceMerge.payload.competitors) &&
      niceMerge.payload.competitors.length >= 5,
  );
  assert.ok(
    Array.isArray(niceMerge.payload.evidence) &&
      niceMerge.payload.evidence.length >= 9,
  );
  assert.ok(niceMerge.payload.contactResearchAudit);
  const liveChars = JSON.stringify(niceLive).length;
  assert.ok(
    niceMerge.length >= liveChars * 0.9,
    `NICE unexplained loss: live=${liveChars} merged=${niceMerge.length}`,
  );
  // Reject the previously proposed ~7k replacement as authoritative.
  assert.ok(JSON.stringify(niceProp).length < 15000);
  assert.ok(niceMerge.length > 30000);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      ownership: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
      merge: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
      mergedChars: merged.length,
      classification: merged.classification,
    },
    null,
    2,
  ),
);
