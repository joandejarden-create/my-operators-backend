#!/usr/bin/env node
import "dotenv/config";

const base =
  process.env.GDI_PROD_BASE ||
  "https://my-operators-backend-production.up.railway.app";

const pairs = [
  ["recnPB3Q02NO8Ma1o", "gdi_opp_prospect_mv8p5ddk_c3ef825a"],
  ["recnPB3Q02NO8Ma1o", "gdi_opp_prospect_mv8p5ddl_acb56fba"],
  ["rec0eTEnCqlsGJJt9", "gdi_pe_1e87cdb00b80fc5f"],
  ["rec0eTEnCqlsGJJt9", "gdi_pe_b4de27f6743c03c5"],
];

for (const [hotelId, oid] of pairs) {
  const url = `${base}/api/group-demand-intelligence/hotels/${hotelId}/opportunities/${oid}`;
  try {
    const res = await fetch(url);
    const body = await res.json();
    const o = body.opportunity || {};
    console.log(
      JSON.stringify({
        status: res.status,
        oid,
        layout: o.detailLayout,
        type: o.opportunityType,
        peVenueId: o.peVenueId,
        fitId: o.hotelVenueFitId,
        signalId: o.peSignalId,
        venue: o.venueName || o.organizationName,
        catchment: o.lodgingCatchmentFitLabel || o.lodgingCatchmentFit,
        partner: o.partnerStatusLabel || o.partnerStatus,
        contact: o.contactGradeLabel || o.contactGrade,
      })
    );
  } catch (err) {
    console.log(JSON.stringify({ oid, error: String(err.message || err) }));
  }
}
