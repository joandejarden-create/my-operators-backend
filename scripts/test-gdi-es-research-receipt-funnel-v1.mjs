#!/usr/bin/env node
/**
 * Executive Summary Research Receipt funnel + productivity presentation gates.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildGdiExecutiveSummaryV2,
  stripResearchIntelligencePerspective,
} from "../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v2.js";
import { resolveResearchFunnelCohort } from "../lib/group-demand-intelligence/executive-summary/gdi-research-funnel-cohort-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const hotelId = "recnPB3Q02NO8Ma1o";

const store = createGdiPublicationPackageStore({});
const ptr = store.readPointer(hotelId, "STAGED");
assert.ok(ptr?.publicationVersion);
const loaded = store.loadPackage(hotelId, ptr.publicationVersion);
assert.equal(loaded.ok, true);
const opps = loaded.package.opportunities;
assert.equal(opps.length, 26);

const cohort = resolveResearchFunnelCohort({ hotelId, repoRoot: root });
assert.equal(cohort.available, true);
assert.equal(cohort.stages.potentialOpportunitiesIdentified, 161);
assert.equal(cohort.stages.opportunitiesResearched, 121);
assert.equal(cohort.omitValidated, true);
assert.equal(cohort.omitScreenedOut, true);

const es = buildGdiExecutiveSummaryV2({
  hotelId,
  hotelName: "Wyndham San Diego Bayside",
  opportunities: opps,
  founderPreview: true,
  customerAuthorizedInventory: false,
  dataState: "FOUNDER_PREVIEW",
  publicationMeta: {
    publicationVersion: ptr.publicationVersion,
    createdAt: loaded.package.createdAt,
  },
  includeResearchIntelligence: true,
});

const receipt = es.sections.researchReceipt;
assert.ok(receipt.researchFunnel);
assert.ok(Array.isArray(receipt.researchFunnel.stages));
const funnelKeys = receipt.researchFunnel.stages.map((s) => s.key);
assert.deepEqual(funnelKeys, [
  "potentialOpportunitiesIdentified",
  "opportunitiesResearched",
  "qualifiedOpportunitiesDelivered",
]);
assert.equal(
  receipt.researchFunnel.stages.find(
    (s) => s.key === "potentialOpportunitiesIdentified",
  ).value,
  161,
);
assert.equal(
  receipt.researchFunnel.stages.find((s) => s.key === "opportunitiesResearched")
    .value,
  121,
);
assert.equal(
  receipt.researchFunnel.stages.find(
    (s) => s.key === "qualifiedOpportunitiesDelivered",
  ).value,
  26,
);
assert.equal(
  receipt.researchFunnel.stages.some((s) => s.key === "opportunitiesValidated"),
  false,
  "validated stage must be omitted when not independently measurable",
);

const supporting = receipt.supportingMetrics || [];
assert.ok(supporting.some((m) => m.key === "evidenceSourcesReviewed"));
assert.ok(supporting.some((m) => m.key === "commercialContactsIdentified"));
assert.ok(supporting.some((m) => m.key === "verifiedContactChannels"));
assert.equal(
  supporting.some((m) => m.key === "opportunitiesScreenedOut"),
  false,
  "screened-out omitted without disposition ledger",
);

const contacts = supporting.find((m) => m.key === "commercialContactsIdentified");
const channels = supporting.find((m) => m.key === "verifiedContactChannels");
assert.ok(contacts.value >= 26);
assert.notEqual(channels.value, 26, "channels must not equal opportunity count");
assert.ok(channels.value > contacts.value || channels.value >= 12);

const prod = receipt.researchProductivity;
assert.ok(prod);
const hours = prod.metrics.find((m) => m.key === "estimatedResearchHoursSaved");
const days = prod.metrics.find((m) => m.key === "equivalentWorkingDaysSaved");
assert.ok(hours);
assert.ok(days);
assert.equal(hours.displayUnit, "Hours");
assert.equal(days.displayUnit, "Days");
assert.equal(hours.customerEstimateLabel, "Estimated");
assert.ok(hours.value < 119);
assert.ok(hours.value > 40);
assert.equal(days.value, Math.round((hours.value / 8) * 2) / 2);

const assessment = es.sections.executiveOverview.executiveAssessment;
assert.ok(assessment.primaryCards.length === 3);
assert.ok(assessment.optionalCards.some((c) => c.key === "competitiveCapture"));
assert.ok(assessment.primaryCards.every((c) => c.icon));

const stripped = stripResearchIntelligencePerspective(es);
assert.equal(stripped.sections.researchIntelligence, undefined);
assert.ok(stripped.sections.researchReceipt.researchFunnel);

const tabJs = fs.readFileSync(
  path.join(root, "public/js/group-demand-intelligence/executive-summary-tab.js"),
  "utf8",
);
assert.match(tabJs, /gdi-es-info-panel/);
assert.match(tabJs, /gdi-es-kpi__unit/);
assert.match(tabJs, /Research Funnel/);
assert.match(tabJs, /gdi-es-columns/);
assert.doesNotMatch(tabJs, /note = "Provisional estimate"/);
assert.match(tabJs, /Estimated/);

const css = fs.readFileSync(
  path.join(root, "public/css/group-demand-intelligence.css"),
  "utf8",
);
assert.match(css, /data-card-count="4"/);
assert.match(css, /gdi-es-info-panel--fixed/);

console.log(
  JSON.stringify(
    {
      ok: true,
      funnel: funnelKeys,
      discovered: 161,
      researched: 121,
      qualified: 26,
      hoursSaved: hours.value,
      daysSaved: days.value,
      contacts: contacts.value,
      channels: channels.value,
    },
    null,
    2,
  ),
);
