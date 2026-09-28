#!/usr/bin/env node
/**
 * Remove V2 noise promotions that failed stricter entity quality (titles/nav artifacts).
 */
import "../load-env.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { passesStructuredEntityQualityGate } from "../lib/group-demand-intelligence/hidden-demand/structured-quality-gate.js";
import { isPlausibleOrganizationName } from "../lib/group-demand-intelligence/hidden-demand/quality-gate.js";

const HOTELS = ["rec35fExUxCClpOP6", "recG66DQJKP2c0UNh"];
const APPLY = process.argv.includes("--apply");

const JUNK_TITLE_RE =
  /exhibitor application|how to manage|kansas city|exhibitor success webinar|exhibitor lists|global exhibitor database|master copy|corporate sponsor program|elderly housing|community housing|housing committee|housing[- ]tilman|housing joseph|travel conference|^tilman lukas/i;

async function main() {
  for (const id of HOTELS) {
    const doc = await loadOpportunitiesCanonical(id);
    const before = doc.opportunities.length;
    const kept = [];
    const removed = [];
    for (const opp of doc.opportunities || []) {
      const org = opp.organizationName || String(opp.title || "").split("—")[0].trim();
      const isV2Noise =
        JUNK_TITLE_RE.test(opp.title || "") ||
        JUNK_TITLE_RE.test(org) ||
        ((opp.gdiVersion === "gdi_hidden_demand_v2" ||
          /EXHIBITOR BLOCK|VENDOR BLOCK|LEADERSHIP MEETING/i.test(opp.title || "")) &&
          !isPlausibleOrganizationName(org));
      const gate = passesStructuredEntityQualityGate({
        entityName: org,
        sourceType: opp.sourceType || "PROGRAM_PDF",
        participationRole: "EXHIBITOR",
        futureTiming: true,
      });
      if (isV2Noise || (opp.opportunityType === "HIDDEN_DEMAND" && !gate.ok)) {
        removed.push(opp.title);
        continue;
      }
      kept.push(opp);
    }
    console.log(JSON.stringify({ hotelId: id, before, after: kept.length, removed }, null, 2));
    if (APPLY) {
      await saveOpportunitiesCanonical(id, { ...doc, opportunities: kept });
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
