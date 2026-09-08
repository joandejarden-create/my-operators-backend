#!/usr/bin/env node
/**
 * Smoke: Mexico Explorer Research Center shows Full HI + follow-ups.
 */

import assert from "node:assert/strict";
import { clearDossierRegistryCache } from "../lib/hotel-intelligence/dossier/index.js";
import { buildResearchCenterPayload } from "../lib/hotel-intelligence/research/center-payload.js";
import {
  SHERATON_GDL_AIRTABLE_ID,
  REAL_INN_CANCUN_AIRTABLE_ID,
} from "../lib/hotel-intelligence/ownership/golden-demo/mexico-explorer-demo-cohort.js";

clearDossierRegistryCache();

for (const hotelId of [SHERATON_GDL_AIRTABLE_ID, REAL_INN_CANCUN_AIRTABLE_ID]) {
  const p = buildResearchCenterPayload({ hotel_id: hotelId });
  assert.equal(p.mode, "AFTER_FULL_INVESTIGATION", hotelId);
  assert.ok(p.latest_investigation, `${hotelId} latest`);
  assert.equal(p.latest_investigation.template_id, "FULL_HOTEL_INTELLIGENCE");
  assert.ok(p.latest_investigation.report_id, `${hotelId} report_id`);
  assert.ok((p.archive || []).length >= 1, `${hotelId} archive`);
  assert.ok((p.recommended_follow_up || []).length >= 1, `${hotelId} follow-ups`);
  assert.ok((p.research_more || []).length >= 1, `${hotelId} research_more`);
  console.log("ok", hotelId, {
    follow_ups: p.recommended_follow_up.map((r) => r.card_title),
    report_id: p.latest_investigation.report_id,
  });
}

console.log("mexico-explorer-research-center-smoke: PASS");
