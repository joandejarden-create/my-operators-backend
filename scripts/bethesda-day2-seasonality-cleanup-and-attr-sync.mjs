#!/usr/bin/env node
/**
 * Bethesda Day-2: deactivate non-Bethesda seasonality junk, set need-period posture,
 * then regenerate ADP Attributes. Never touches Oct-1 baseline artifacts.
 *
 *   node scripts/bethesda-day2-seasonality-cleanup-and-attr-sync.mjs --dry-run
 *   node scripts/bethesda-day2-seasonality-cleanup-and-attr-sync.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  HI_TABLES,
  MAP_SEASONALITY as SP,
} from "../lib/hotel-intelligence/schema/hotel-intelligence-schema-v1.js";
import { CANONICAL_INTELLIGENCE_BASE_ID } from "../lib/decision-outcomes/airtable-base.js";
import {
  upsertDomainStatus,
  loadDomainStatusLedger,
} from "../lib/hotel-intelligence/onboarding/domain-status-store.js";
import { HI_DOMAIN, HI_DOMAIN_STATUS, OVERALL_HI_STATUS, summarizeOverallHiStatus } from "../lib/hotel-intelligence/onboarding/domain-status-v1.js";
import { buildAdpHotelAttributes } from "../lib/hotel-intelligence/adp-attributes/build-adp-hotel-attributes.js";
import { syncHotelAdpAttributesToAirtable } from "../lib/hotel-intelligence/adp-attributes/airtable-store.js";
import { isHotelIntelligenceComplete } from "../lib/hotel-intelligence/onboarding/hi-completeness-gate.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/bethesda-pilot/day2/2026-10-02");
const HOTEL = "recLuxvwwxID7U2B8";
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;

const BAD_SEASONALITY_IDS = ["recgch42A3gwmQyQu", "recv8BORC7IlWT4bK"];

fs.mkdirSync(OUT, { recursive: true });

function getBase() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
  const baseId =
    process.env.ADP_AIRTABLE_BASE_ID ||
    process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
    CANONICAL_INTELLIGENCE_BASE_ID;
  if (!token) throw new Error("missing_airtable_token");
  if (baseId === "appvtnDurnMSjINP6") throw new Error("FORBIDDEN_LEGACY_BASE");
  return new Airtable({ apiKey: token }).base(baseId);
}

async function main() {
  const base = getBase();
  const table = base(HI_TABLES.seasonalityNeedPeriods);
  const actions = [];

  for (const id of BAD_SEASONALITY_IDS) {
    const rec = await table.find(id);
    const url = rec.fields?.[SP.sourceUrl] || "";
    const label = rec.fields?.[SP.seasonLabel] || "";
    const active = rec.fields?.[SP.active];
    const reason =
      /anaheim|disneyland|facebook\.com\/2ndhand|milnerton/i.test(url + " " + label)
        ? "NON_BETHESDA_DESTINATION_SEASONALITY_REJECTED"
        : "SEASONALITY_SOURCE_QUALITY_REJECTED";
    const priorDesc = String(rec.fields?.[SP.description] || "").trim();
    const payload = {
      [SP.active]: false,
      [SP.description]: [
        priorDesc,
        `Day-2 ${new Date().toISOString().slice(0, 10)}: deactivated — ${reason}. Not hotel-supplied need period. Bethesda market seasonality remains researchable; hotel need periods = NOT_PROVIDED.`,
      ]
        .filter(Boolean)
        .join("\n"),
    };
    actions.push({
      id,
      label,
      url,
      wasActive: active,
      reason,
      fields: payload,
    });
    if (APPLY) {
      await table.update([{ id, fields: payload }]);
    }
  }

  // Domain ledger: seasonality researched but public rows rejected; need periods not provided
  const ledgerBefore = loadDomainStatusLedger(HOTEL);
  const seasonalityEntry = {
    domainStatus: HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING,
    lastResearchedAt: new Date().toISOString(),
    researchRunId: `hi_day2_seasonality_cleanup_${Date.now()}`,
    evidenceCount: ledgerBefore.domains?.SEASONALITY_NEED_PERIODS?.evidenceCount || 0,
    rowCount: 0,
    sourceCoverage: ["PUBLIC_REJECTED_NON_MARKET", "HOTEL_NEED_PERIOD_NOT_PROVIDED"],
    notes:
      "Day-2: deactivated non-Bethesda public seasonality rows. Market seasonality research ceiling reached without reliable Bethesda-specific public seasonality. Hotel-supplied need periods = NOT_PROVIDED (not blocking). HI remains complete via PUBLIC_DATA_CEILING.",
    blocker: null,
    hotelNeedPeriodStatus: "NOT_PROVIDED",
    researchDepth: "PUBLIC_DATA_CEILING",
  };

  if (APPLY) {
    upsertDomainStatus(HOTEL, HI_DOMAIN.SEASONALITY_NEED_PERIODS, seasonalityEntry);
  }

  // Regenerate ADP attributes from cleaned HI
  const packet = await buildAdpHotelAttributes(HOTEL);
  const sync = await syncHotelAdpAttributesToAirtable(packet, { dryRun: DRY });
  const completeness = await isHotelIntelligenceComplete(HOTEL);

  const report = {
    mode: APPLY ? "APPLY" : "DRY_RUN",
    hotelId: HOTEL,
    seasonalityDeactivations: actions,
    seasonalityDomainAfter: seasonalityEntry,
    hotelNeedPeriodStatus: "NOT_PROVIDED",
    hotelNeedPeriodRequiredForProduction: false,
    attributeSync: {
      ok: sync.ok !== false,
      createCount: sync.createCount,
      updateCount: sync.updateCount,
      deactivateCount: sync.deactivateCount,
      error: sync.error || null,
      proposedCount: packet.attributes?.length || 0,
      missingCritical: packet.missingCritical || [],
    },
    completeness: {
      complete: completeness.complete,
      overallStatus: completeness.overallStatus,
      coverage: completeness.coverage,
      blockingDomains: completeness.blockingDomains,
    },
    generatedAt: new Date().toISOString(),
  };

  fs.writeFileSync(
    path.join(OUT, "SEASONALITY_CLEANUP_AND_ATTR_SYNC.json"),
    JSON.stringify(report, null, 2)
  );
  console.log(JSON.stringify(report, null, 2));
  if (!completeness.complete) process.exitCode = 3;
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
