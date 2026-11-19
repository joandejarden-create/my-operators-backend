#!/usr/bin/env node
/**
 * Bethesda Marriott — five Overview field provenance audit regression.
 * Free / local (+ optional live brand-presence when HE :8091 is up).
 */
import assert from "node:assert/strict";
import { formatHotelPropertyCensusAsBrandPresenceHotel } from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";
import { attachOverviewFieldProvenance } from "../lib/hotel-intelligence/he-overview-field-provenance-v1.js";
import {
  applyOverviewEnrichmentToHotel,
  buildHotelExplorerOverviewEnrichment,
} from "../lib/hotel-intelligence/hotel-explorer-overview-enrichment-v1.js";

const BETH = "recLuxvwwxID7U2B8";

const hpcFields = {
  "Property Name": "Bethesda Marriott",
  "Current Brand": "Marriott Hotels",
  "Brand Family": "Marriott International",
  "Parent Company": "",
  "Hotel Class / Segment": "Upper Upscale",
  "Management Model": "Management Agreement",
  "Operator Type": "Brand-Managed",
  "Asset Context": "Suburban",
  "Meeting Space Flag": true,
  "Rooms / Keys": 407,
  "Operator / Management Company": "Marriott International (Managed)",
};

const formatted = formatHotelPropertyCensusAsBrandPresenceHotel({
  id: BETH,
  fields: hpcFields,
});

assert.equal(formatted.parentCompany, "Marriott International");
assert.equal(formatted.brandFamily, "Marriott International");
assert.equal(formatted.chainScale, "Upper Upscale");
assert.equal(formatted.operationType, "Management Agreement");
assert.equal(formatted.locationType, "Suburban");
assert.equal(formatted.totalMeetingSpace, null);

const enrichment = await buildHotelExplorerOverviewEnrichment(BETH, {
  hotelDto: { ...formatted, parentCompany: null },
  audience: "internal",
  skipLiveHpc: true,
  skipHiAirtable: true,
});
assert.equal(enrichment.meetingSpace?.totalSqFt, 18719);
assert.match(String(enrichment.meetingSpace?.sourceUrl || ""), /marriott\.com/);

let hotel = applyOverviewEnrichmentToHotel({ ...formatted, parentCompany: null }, enrichment);
assert.equal(hotel.totalMeetingSpace, 18719);
assert.equal(hotel.meetingSpaceProvenance?.factKind, "VERIFIED_FACT");

hotel = attachOverviewFieldProvenance(hotel, {
  hpcFields,
  owningTableKind: "HPC",
});

assert.equal(hotel.parentCompany, "Marriott International");
assert.equal(hotel.fieldProvenance.parentCompany.class, "VERIFIED_HPC");
assert.equal(hotel.fieldProvenance.parentCompany.semantic, "brand_parent_company");
assert.equal(hotel.fieldProvenance.parentCompany.hpcField, "Brand Family");
assert.equal(hotel.fieldProvenance.chainScale.class, "VERIFIED_HPC");
assert.equal(hotel.fieldProvenance.chainScale.hpcField, "Hotel Class / Segment");
assert.equal(hotel.fieldProvenance.operationType.class, "VERIFIED_HPC");
assert.equal(hotel.fieldProvenance.operationType.hpcField, "Management Model");
assert.equal(hotel.fieldProvenance.locationType.class, "VERIFIED_HPC");
assert.equal(hotel.fieldProvenance.locationType.hpcField, "Asset Context");
assert.equal(hotel.fieldProvenance.totalMeetingSpace.class, "VERIFIED_DERIVED");
assert.equal(hotel.fieldProvenance.totalMeetingSpace.unit, "sq_ft");
assert.equal(hotel.fieldProvenance.totalMeetingSpace.metric, "total_meeting_space");

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-he-beth-five-field-provenance-v1",
      dispositions: {
        parentCompany: "VERIFIED_HPC (Brand Family → brand parent)",
        chainScale: "VERIFIED_HPC (Hotel Class / Segment)",
        operationType: "VERIFIED_HPC (Management Model)",
        locationType: "VERIFIED_HPC (Asset Context)",
        totalMeetingSpace: "VERIFIED_DERIVED (ADP/HI + Marriott events URL)",
      },
    },
    null,
    2,
  ),
);

// Optional live probe
const base = process.env.HE_GATE1_BASE_URL || "http://127.0.0.1:8091";
try {
  const res = await fetch(`${base}/api/brand-presence/hotel/${BETH}`);
  if (res.ok) {
    const json = await res.json();
    const h = json.hotel || {};
    assert.equal(h.censusSourceTable, "Hotel Property Census");
    assert.equal(h.propertyIdentity?.owningTableKind, "HPC");
    assert.equal(h.parentCompany, "Marriott International");
    assert.equal(h.fieldProvenance?.parentCompany?.semantic, "brand_parent_company");
    assert.equal(h.fieldProvenance?.chainScale?.class, "VERIFIED_HPC");
    assert.equal(h.fieldProvenance?.operationType?.class, "VERIFIED_HPC");
    assert.equal(h.fieldProvenance?.locationType?.class, "VERIFIED_HPC");
    assert.equal(h.fieldProvenance?.totalMeetingSpace?.class, "VERIFIED_DERIVED");
    console.log("OK — live brand-presence provenance on", base);
  } else {
    console.log("SKIP live probe — HTTP", res.status);
  }
} catch (err) {
  console.log("SKIP live probe —", err?.message || err);
}
