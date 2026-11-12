#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  applyBethesdaCommercialEventFamilyPresentation,
  buildBethesdaCommercialEventFamilyCrosswalk,
  FAMILY_ROLE,
  BETHESDA_88_COMMERCIAL_EVENT_FAMILY_V1,
} from "../lib/group-demand-intelligence/research/bethesda-88-commercial-event-family-v1.js";
import { loadHotelOpportunityDoc } from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  mergeContactAntiClobber,
  extractContactSnapshot,
  COVERAGE_BUCKET,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-quality-completion-v1.js";

assert.equal(typeof BETHESDA_88_COMMERCIAL_EVENT_FAMILY_V1, "string");

const store = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
const union = mergeBethesdaInternalResearchOpportunities(
  "recLuxvwwxID7U2B8",
  store.opportunities
);
assert.equal(union.counts.union, 88);

const family = applyBethesdaCommercialEventFamilyPresentation(union.opportunities, {
  hideLinkedFromList: true,
});
assert.equal(family.counts.rawResearchIds, 88);
assert.ok(family.counts.uniqueCommercialOpportunities < 88);
assert.ok(family.counts.linkedResearchHidden >= 4);
assert.ok(family.counts.quarantinedHidden >= 1);
assert.equal(family.opportunities.length, family.counts.listVisible);

// CTN siblings collapsed
const listIds = new Set(family.opportunities.map((o) => o.id));
assert.equal(listIds.has("gdi_opp_series_ctn_annual_conference_2027"), true);
assert.equal(listIds.has("gdi_opp_series_new_in_the_library_august_2026"), false);
assert.equal(
  listIds.has("gdi_opp_series_march_15_17_bethesda_md_ctn_dissemination_2027"),
  false
);
assert.equal(
  listIds.has("gdi_opp_series_if_prompted_enter_meeting_id_250_658_877__2027"),
  false
);

// NCI RNA collapsed to one primary
assert.equal(listIds.has("gdi_opp_2027_nci_rna_biology_symposium_7"), true);
assert.equal(
  listIds.has("gdi_opp_series_request_a_conference_registration_website_2026"),
  false
);
assert.equal(
  listIds.has("gdi_opp_series_nci_rna_biology_symposium_ncif_conference_2027"),
  false
);

// AMWA years remain distinct
assert.equal(listIds.has("gdi_opp_amwa_2027_annual"), true);
assert.equal(listIds.has("gdi_opp_amwa_2028_annual"), true);
assert.equal(listIds.has("gdi_opp_amwa_2027_interim"), true);

const crosswalk = buildBethesdaCommercialEventFamilyCrosswalk(union.opportunities);
assert.ok(crosswalk.some((f) => f.familyId === "ctn_annual_conference_bethesda_2027"));
assert.ok(crosswalk.every((f) => f.independentDemandCase === false));

// Contact field consistency: Arlington + SBPO
const arlington = store.opportunities.find(
  (o) => o.id === "gdi_opp_arlington_spring_tournament_2027"
);
const arlingtonMerged = mergeContactAntiClobber(
  arlington,
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID.gdi_opp_arlington_spring_tournament_2027
);
const arlingtonSnap = extractContactSnapshot(arlingtonMerged);
assert.equal(arlingtonSnap.email, "lizzy@arlingtonsoccer.com");
assert.equal(arlingtonSnap.hasDirectEmail, true);
assert.ok(
  /tournaments@arlingtonsoccer\.com/i.test(
    arlingtonSnap.orgEmail || arlingtonSnap.orgRoute || ""
  )
);
assert.equal(arlingtonSnap.coverage, COVERAGE_BUCKET.A);
assert.notEqual(
  (arlingtonSnap.email || "").toLowerCase(),
  (arlingtonSnap.orgEmail || arlingtonSnap.orgRoute || "").toLowerCase()
);

const sbpo = store.opportunities.find((o) => o.id === "gdi_opp_nih_sbpo_vos_pattern");
const sbpoMerged = mergeContactAntiClobber(
  sbpo,
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID.gdi_opp_nih_sbpo_vos_pattern
);
const sbpoSnap = extractContactSnapshot(sbpoMerged);
assert.equal(sbpoSnap.email, "scarbora@od.nih.gov");
assert.ok(/nihsmallbusiness@/i.test(sbpoSnap.orgEmail || sbpoSnap.orgRoute || ""));
assert.equal(sbpoSnap.hasDirectEmail, true);

// Quandra / scudderq retained
const ctn = store.opportunities.find(
  (o) => o.id === "gdi_opp_series_ctn_annual_conference_2027"
);
const ctnMerged = mergeContactAntiClobber(
  ctn,
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID.gdi_opp_series_ctn_annual_conference_2027
);
assert.equal(extractContactSnapshot(ctnMerged).email, "scudderq@nida.nih.gov");

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-88-commercial-event-family-v1",
      raw: 88,
      uniqueCommercial: family.counts.uniqueCommercialOpportunities,
      listVisible: family.counts.listVisible,
      linkedHidden: family.counts.linkedResearchHidden,
      quarantined: family.counts.quarantinedHidden,
      arlingtonDirect: arlingtonSnap.email,
      sbpoDirect: sbpoSnap.email,
      roles: Object.values(FAMILY_ROLE),
    },
    null,
    2
  )
);
