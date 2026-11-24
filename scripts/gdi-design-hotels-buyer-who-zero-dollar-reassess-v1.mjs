#!/usr/bin/env node
/**
 * $0 Design Hotels reassessment — lineage reuse + junk rejection on saved evidence.
 * Does NOT rewrite STAGED pointers. Writes a versioned plan only.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  indexPriorNamedContactsByCluster,
  applyContactLineageReuse,
  buildDemandClusterKey,
} from "../lib/group-demand-intelligence/workflow/gdi-opportunity-contact-lineage-reuse-v1.js";
import { isPlausibleBuyerPersonName } from "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js";
import {
  evaluateContactCompletenessForFinalization,
  evaluateVerifiedCommercialRoute,
} from "../lib/group-demand-intelligence/presentation/gdi-verified-commercial-route-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SERVE = process.env.GDI_SERVE_ROOT || ROOT;
const OUT = path.join(ROOT, "reports/gdi-buyer-who-e2e-recovery-v1");

const HOTELS = [
  { hotelId: "rece0NdV9FCnaFlVS", hotelName: "Condesa DF" },
  { hotelId: "recaokE8psIbuVeuH", hotelName: "LaLuna Boutique Hotels & Villas" },
  { hotelId: "rec1t35e3sPerzxYu", hotelName: "Memmo Príncipe Real" },
];

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadInventory(hotelId) {
  const p = path.join(
    SERVE,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) return [];
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  return j.opportunities || j || [];
}

function loadV3Staged(hotelId) {
  const base = path.join(
    SERVE,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
  );
  if (!fs.existsSync(base)) return null;
  const dir = fs
    .readdirSync(base, { withFileTypes: true })
    .find((d) => d.isDirectory() && d.name.includes("gdi_dh_v3_staged"));
  if (!dir) return null;
  const pkg = JSON.parse(
    fs.readFileSync(path.join(base, dir.name, "package.json"), "utf8"),
  );
  return { version: dir.name, opportunities: pkg.opportunities || [] };
}

function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const hotels = [];
  let recoveredTotal = 0;
  let stillNeedsLiveWho = 0;
  let strictNamed = 0;
  let orgRouteRejected = 0;

  for (const hotel of HOTELS) {
    const inventory = loadInventory(hotel.hotelId);
    const priorIndex = indexPriorNamedContactsByCluster(inventory);
    const staged = loadV3Staged(hotel.hotelId);
    const rows = [];
    let recovered = 0;
    let needsLive = 0;
    let named = 0;

    for (const opp of staged?.opportunities || []) {
      const id = text(opp.id || opp.opportunityId);
      const inv = inventory.find(
        (o) => text(o.opportunityId || o.id) === id,
      );
      const base = inv || {
        id,
        organizationName: opp.organizationName,
        title: opp.title,
        primaryContact: opp.primaryContact,
      };
      const beforeName = text(
        base.primaryContact?.name || opp.primaryContact?.name || opp.tileContactPreview?.name,
      );
      let working = { ...base };
      let reuse = { reused: false };
      if (!isPlausibleBuyerPersonName(beforeName)) {
        reuse = applyContactLineageReuse(working, priorIndex);
        working = reuse.opportunity;
      }
      // Strip junk
      if (
        working.primaryContact?.name &&
        !isPlausibleBuyerPersonName(working.primaryContact.name)
      ) {
        working.primaryContact = { ...working.primaryContact, name: null };
      }
      const afterName = text(working.primaryContact?.name);
      const recoveredHere =
        !isPlausibleBuyerPersonName(beforeName) &&
        isPlausibleBuyerPersonName(afterName);
      if (recoveredHere) recovered += 1;

      const fin = evaluateContactCompletenessForFinalization(working, {
        looksLikePersonName: isPlausibleBuyerPersonName,
      });
      const routeText = [
        opp.recommendedNextStep,
        opp.recommendedAction,
        working.approvedCommercialRoute,
      ]
        .map(text)
        .join(" ");
      const hadOrgRoute = /Official .+ organizing\s*\/\s*lodging\s*channel/i.test(
        routeText,
      );
      if (hadOrgRoute && !fin.complete) orgRouteRejected += 1;
      if (fin.complete && fin.mode === "NAMED_PERSON") named += 1;
      else if (!fin.complete) needsLive += 1;

      rows.push({
        opportunityId: id,
        organizationName: working.organizationName || opp.organizationName,
        title: working.title || opp.title,
        beforeName: isPlausibleBuyerPersonName(beforeName) ? beforeName : null,
        afterName: isPlausibleBuyerPersonName(afterName) ? afterName : null,
        lineageReused: reuse.reused === true,
        lineageReason: reuse.reason || null,
        demandClusterKey: buildDemandClusterKey(working),
        finalizationComplete: fin.complete === true,
        finalizationMode: fin.mode,
        needsLiveBuyerWho: !fin.complete,
        hadOrgRouteLanguage: hadOrgRoute,
      });
    }

    recoveredTotal += recovered;
    stillNeedsLiveWho += needsLive;
    strictNamed += named;
    hotels.push({
      ...hotel,
      stagedVersion: staged?.version || null,
      stagedCount: staged?.opportunities?.length || 0,
      inventoryCount: inventory.length,
      priorNamedClusters: priorIndex.size,
      recoveredAt0Usd: recovered,
      strictNamedFinalizationReady: named,
      needsLiveBuyerWho: needsLive,
      rows,
    });
  }

  const plan = {
    contract: "gdi-design-hotels-buyer-who-zero-dollar-reassess-v1",
    at: new Date().toISOString(),
    paidCalls: 0,
    note: "Plan only — does not rewrite STAGED pointers or CURRENT",
    totals: {
      recoveredAt0Usd: recoveredTotal,
      strictNamedFinalizationReady: strictNamed,
      needsLiveBuyerWho: stillNeedsLiveWho,
      orgRouteLanguageWithoutNamedCompletion: orgRouteRejected,
    },
    proposedCorrectedStagingVersionPrefix: "gdi_dh_buyer_who_recovery_",
    hotels,
    proposedIncrementalExaBudgetUsd: {
      note: "Only if founder authorizes live buyer-WHO on remaining gaps",
      remainingGaps: stillNeedsLiveWho,
      suggestedPerOpportunityUsd: 0.15,
      suggestedCeilingUsd: Math.min(
        40,
        Math.ceil(stillNeedsLiveWho * 0.15 * 10) / 10,
      ),
    },
  };

  fs.writeFileSync(
    path.join(OUT, "ZERO_DOLLAR_REASSESS_PLAN.json"),
    `${JSON.stringify(plan, null, 2)}\n`,
  );
  console.log(
    JSON.stringify(
      {
        ok: true,
        out: path.join(OUT, "ZERO_DOLLAR_REASSESS_PLAN.json"),
        totals: plan.totals,
        proposedExaCeilingUsd: plan.proposedIncrementalExaBudgetUsd.suggestedCeilingUsd,
      },
      null,
      2,
    ),
  );
}

main();
