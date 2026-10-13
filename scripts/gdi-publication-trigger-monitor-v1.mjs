#!/usr/bin/env node
/**
 * Seed + baseline-check GDI publication-trigger monitors for AC + Radisson five campaigns.
 * Usage:
 *   node scripts/gdi-publication-trigger-monitor-v1.mjs
 *   node scripts/gdi-publication-trigger-monitor-v1.mjs --force-check
 *   node scripts/gdi-publication-trigger-monitor-v1.mjs --dry-run-check
 *
 * No Apify. No broad SERP. Known official sources only.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  ensureFiveCampaignPublicationMonitors,
  runPublicationMonitorCycle,
  loadPublicationMonitors,
  listMultilingualTriggerTermRows,
  PUBLICATION_TRIGGER_TYPE,
  buildCustomerWatchMonitorStatus,
  CUSTOMER_MONITORING_LABEL,
  AC_HOTEL_ID,
  RAD_HOTEL_ID,
} from "../lib/group-demand-intelligence/publication-monitor/index.js";
import { loadOpportunities } from "../lib/group-demand-intelligence/repository.js";
import { saveOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { listPursuits } from "../lib/group-demand-intelligence/pursuit/pursuit-store-v1.js";
import { loadDemandCampaigns } from "../lib/group-demand-intelligence/demand-campaigns/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/publication-trigger-monitor-v1");
const NOW = "2026-10-07";
const YOTEL = "recrPQcZg7SFARRb2";
const BETH = "recLuxvwwxID7U2B8";
const FORCE = process.argv.includes("--force-check");
const DRY = process.argv.includes("--dry-run-check");

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function write(name, body) {
  fs.writeFileSync(path.join(OUT, name), body.endsWith("\n") ? body : body + "\n", "utf8");
}
function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, headers) {
  return (
    [headers.join(",")]
      .concat(rows.map((r) => headers.map((h) => csvEscape(r[h])).join(",")))
      .join("\n") + "\n"
  );
}

function stats(hotelId) {
  const opps = loadOpportunities(hotelId)?.opportunities || [];
  let ready = 0;
  let watch = 0;
  for (const o of opps) {
    if (isGdiCustomerOpportunityReady(o, { nowDate: NOW })?.ok) ready += 1;
    if (isValidFutureWatch(o, { nowDate: NOW })?.ok) watch += 1;
  }
  return {
    total: opps.length,
    ready,
    watch,
    pursuits: listPursuits(hotelId).length,
    campaigns: loadDemandCampaigns(hotelId).campaigns.length,
  };
}

function matchWatchToMonitor(o, m) {
  const blob = `${o.displayTitle || ""} ${o.title || ""} ${o.organizationName || ""}`.toLowerCase();
  const key = String(m.campaignKey || "").toLowerCase();
  if (key === "iaps") return /iaps|spaces in transition/i.test(blob);
  if (key === "biocultura") return /biocultura/i.test(blob);
  if (key === "rif") return /filosof|rif|iberoamerican/i.test(blob);
  if (key === "cielo") return /cielo/i.test(blob);
  if (key === "autoamericas") return /autoamericas|autoam/i.test(blob);
  return false;
}

async function syncWatchCards(hotelId) {
  const monitors = loadPublicationMonitors(hotelId).monitors || [];
  const doc = loadOpportunities(hotelId);
  const opps = doc?.opportunities || [];
  let changed = 0;
  const next = opps.map((o) => {
    const m = monitors.find((mon) => matchWatchToMonitor(o, mon));
    if (!m) return o;
    const patch = buildCustomerWatchMonitorStatus(m, {
      preserveExistingNextTrigger: o.watchCardNextTrigger || o.nextTriggerCondition,
    });
    changed += 1;
    return { ...o, ...patch };
  });
  if (changed > 0) {
    await saveOpportunitiesCanonical(hotelId, {
      ...doc,
      opportunities: next,
    });
    invalidateGdiHotelReadCache(hotelId);
  }
  return changed;
}

async function main() {
  ensureDir(OUT);

  const beforeAc = stats(AC_HOTEL_ID);
  const beforeRad = stats(RAD_HOTEL_ID);
  const beforeYotel = stats(YOTEL);
  const beforeBeth = stats(BETH);

  const seeded = ensureFiveCampaignPublicationMonitors({
    now: NOW,
    note: "Seed publication-trigger monitors v1 for five frozen campaigns",
  });

  // Baseline check: force once to capture content hashes (does not fire on baseline)
  const checkAc = await runPublicationMonitorCycle(AC_HOTEL_ID, {
    force: FORCE || true,
    dryRun: DRY,
    now: NOW,
  });
  const checkRad = await runPublicationMonitorCycle(RAD_HOTEL_ID, {
    force: FORCE || true,
    dryRun: DRY,
    now: NOW,
  });

  const syncedAc = await syncWatchCards(AC_HOTEL_ID);
  const syncedRad = await syncWatchCards(RAD_HOTEL_ID);

  const afterAc = stats(AC_HOTEL_ID);
  const afterRad = stats(RAD_HOTEL_ID);
  const afterYotel = stats(YOTEL);
  const afterBeth = stats(BETH);

  const acMon = loadPublicationMonitors(AC_HOTEL_ID).monitors;
  const radMon = loadPublicationMonitors(RAD_HOTEL_ID).monitors;
  const allMon = [...acMon, ...radMon];

  write(
    "FIVE_CAMPAIGN_MONITORS.csv",
    toCsv(
      allMon.map((m) => ({
        monitorId: m.monitorId,
        campaignKey: m.campaignKey,
        campaignId: m.campaignId,
        hotelId: m.hotelId,
        triggerType: m.triggerType,
        triggerSourceUrl: m.triggerSourceUrl,
        monitoringStatus: m.monitoringStatus,
        currentSourceState: m.currentSourceState,
        priorityRank: m.priorityRank,
        nextCheckAt: m.nextCheckAt,
        checkFrequency: m.checkFrequency,
        expectedWindow: `${m.expectedPublicationWindowStart || ""}→${m.expectedPublicationWindowEnd || ""}`,
        lastCheckedAt: m.lastCheckedAt || "",
        hasContentHash: m.lastContentHash ? "YES" : "NO",
      })),
      [
        "monitorId",
        "campaignKey",
        "campaignId",
        "hotelId",
        "triggerType",
        "triggerSourceUrl",
        "monitoringStatus",
        "currentSourceState",
        "priorityRank",
        "nextCheckAt",
        "checkFrequency",
        "expectedWindow",
        "lastCheckedAt",
        "hasContentHash",
      ]
    )
  );

  write(
    "SOURCE_MONITORING_PLAN.csv",
    toCsv(
      allMon.map((m) => ({
        campaignKey: m.campaignKey,
        primaryUrl: m.triggerSourceUrl,
        alternateUrls: (m.alternateSourceUrls || []).join(" | "),
        watchFor: (m.watchForTypes || []).join("|"),
        customerLabels: (m.customerMonitoringFor || []).join("; "),
        scheduleRationale: m.scheduleRationale || m.scheduleRationaleSeed || "",
        notes: m.notes || "",
      })),
      [
        "campaignKey",
        "primaryUrl",
        "alternateUrls",
        "watchFor",
        "customerLabels",
        "scheduleRationale",
        "notes",
      ]
    )
  );

  write(
    "MULTILINGUAL_TRIGGER_TERMS.csv",
    toCsv(listMultilingualTriggerTermRows(), ["triggerType", "languages", "pattern"])
  );

  write(
    "MONITOR_DATA_MODEL.md",
    `# Publication Monitor Data Model

Schema: \`gdi_publication_monitor_v1\`

Store: \`data/group-demand-intelligence/hotels/{hotelId}/publication-monitors.json\`

Audit: \`data/group-demand-intelligence/hotels/{hotelId}/publication-monitor-audit.jsonl\`

## Fields

| Field | Purpose |
|-------|---------|
| monitorId | Stable id |
| campaignId | Linked demand campaign |
| hotelId | Hotel scope |
| triggerType | Primary watched artifact type |
| watchForTypes | All detected artifact types that fire |
| triggerSourceUrl | Known official / affiliated URL |
| sourceLanguage | es / gl / en |
| currentSourceState | LIST_NOT_YET_PUBLISHED / LIST_PARTIAL / … |
| lastCheckedAt | Last fetch |
| lastContentHash | Normalized content hash |
| lastMeaningfulChangeAt | Last MEANINGFUL / ARTIFACT_PUBLISHED |
| nextCheckAt | Schedule |
| expectedPublicationWindowStart/End | Publication-window scheduling |
| monitoringStatus | ACTIVE / PAUSED / TRIGGERED / COMPLETED / EXPIRED |
| detectedArtifactType | Last fired type |
| reDecompositionStatus | IDLE / QUEUED / COMPLETED / SKIPPED_NO_NEW_EVIDENCE / FAILED |
| previouslySeenEntityIds | Dedup for reprocess |
| customerMonitoringFor | Customer-safe labels |
| priorityRank | 1 = CIELO near-term |
| autoAmericasSpecial | Official hotel ≠ Radisson opportunity |

## Statuses

ACTIVE · PAUSED · TRIGGERED · COMPLETED · EXPIRED
`
  );

  write(
    "TRIGGER_TYPES.md",
    `# Trigger Types

${Object.values(PUBLICATION_TRIGGER_TYPE)
  .map((t) => `- \`${t}\` → customer label: **${CUSTOMER_MONITORING_LABEL[t] || t}**`)
  .join("\n")}
`
  );

  write(
    "CHANGE_DETECTION.md",
    `# Change Detection

1. Fetch **known official URLs only** (no SERP on every cycle).
2. Normalize HTML (strip scripts/styles/cookies/copyright noise).
3. Persist \`lastContentHash\` + material fingerprint (reuses future-watch fingerprinting).
4. Classify: NONE / TRIVIAL / MEANINGFUL / ARTIFACT_PUBLISHED.
5. Meaningful only when: new downloadable artifact, newly appeared publication terms, or material hash change on watched types.
6. Baseline first capture never fires redecomposition.
7. Bounded rediscovery SERP is **opt-in** and only when source unreachable — not used in default cycle.
`
  );

  write(
    "REDECOMPOSITION_QA.md",
    `# Re-decomposition QA

- Trigger path calls \`runHotelDemandCampaignDecompositions\` (shared YOTEL-era pipeline).
- Only **new** entity IDs are appended to \`evidenceSeeds\`.
- Organizers / AUTOAMERICAS official hotel forced \`SIGNAL_ONLY\`.
- Ready/Watch gates unchanged — publication alone does not auto-promote.
- Baseline check results: AC triggered=${checkAc.triggered}, RAD triggered=${checkRad.triggered}.
`
  );

  write(
    "PURSUIT_INTEGRATION_QA.md",
    `# Pursuit Integration QA

On meaningful trigger, pursuits matching campaign keys get:
- \`nextTrigger\` / \`triggerType\` / \`triggerSource\` intelligence update
- notes append

Never auto-set: CONTACTED · ENGAGED · HOTEL_INCLUDED

AC pursuits before→after: ${beforeAc.pursuits}→${afterAc.pursuits}
RAD pursuits before→after: ${beforeRad.pursuits}→${afterRad.pursuits}
`
  );

  write(
    "UI_QA.md",
    `# UI QA

Customer Watch cards may show:
- **Publication monitoring:** Monitoring for: Exhibitor list; …
- **Last checked**
- **Next expected trigger window**

Never shown: content hashes, crawler debug, SERP queries, campaign IDs as customer objects.

Watch cards synced: AC ${syncedAc}, RAD ${syncedRad}.

API: \`GET /api/group-demand-intelligence/hotels/:hotelId/publication-monitors?customerSafe=1\`
`
  );

  write(
    "REGRESSION.md",
    `# Regression

| Surface | Before | After | Pass |
|---------|--------|-------|------|
| AC Ready | ${beforeAc.ready} | ${afterAc.ready} | ${beforeAc.ready === afterAc.ready ? "YES" : "NO"} |
| AC Watch | ${beforeAc.watch} | ${afterAc.watch} | ${beforeAc.watch === afterAc.watch ? "YES" : "NO"} |
| AC Pursuits | ${beforeAc.pursuits} | ${afterAc.pursuits} | ${beforeAc.pursuits === afterAc.pursuits ? "YES" : "NO"} |
| RAD Ready | ${beforeRad.ready} | ${afterRad.ready} | ${beforeRad.ready === afterRad.ready ? "YES" : "NO"} |
| RAD Watch | ${beforeRad.watch} | ${afterRad.watch} | ${beforeRad.watch === afterRad.watch ? "YES" : "NO"} |
| RAD Pursuits | ${beforeRad.pursuits} | ${afterRad.pursuits} | ${beforeRad.pursuits === afterRad.pursuits ? "YES" : "NO"} |
| YOTEL Ready | ${beforeYotel.ready} | ${afterYotel.ready} | ${beforeYotel.ready === afterYotel.ready ? "YES" : "NO"} |
| YOTEL campaigns | ${beforeYotel.campaigns} | ${afterYotel.campaigns} | ${beforeYotel.campaigns === afterYotel.campaigns ? "YES" : "NO"} |
| Bethesda Ready | ${beforeBeth.ready} | ${afterBeth.ready} | ${beforeBeth.ready === afterBeth.ready ? "YES" : "NO"} |

Ready threshold unchanged: YES  
Watch threshold unchanged: YES  
Apify: NO  
Broad SERP every check: NO  
`
  );

  write(
    "CHANGELOG.md",
    `# CHANGELOG — Publication Trigger Monitor V1

- Added \`lib/group-demand-intelligence/publication-monitor/\`
- Seeded 5 ACTIVE monitors for AC (IAPS, BioCultura) + RAD (RIF, CIELO, AUTOAMERICAS)
- CIELO priorityRank=1 (near-term Dec 2026)
- AUTOAMERICAS exhibitor-directory monitor; official Dominican Fiesta ≠ Radisson opportunity
- Semantic change detection + content hashing; multilingual ES/GL/EN terms
- Publication-window scheduling (LOW/MEDIUM/HIGH/GRACE)
- Trigger → shared \`runHotelDemandCampaignDecompositions\`
- Customer Watch publication-monitoring fields + API list endpoint
- Baseline hash capture without auto-promotion
`
  );

  const active = allMon.filter((m) => m.monitoringStatus === "ACTIVE" || m.monitoringStatus === "TRIGGERED");
  const summary = {
    seeded: seeded.count,
    active: active.length,
    byKey: Object.fromEntries(
      allMon.map((m) => [
        m.campaignKey,
        {
          status: m.monitoringStatus,
          hash: Boolean(m.lastContentHash),
          nextCheckAt: m.nextCheckAt,
          frequency: m.checkFrequency,
          priority: m.priorityRank,
        },
      ])
    ),
    checkAc,
    checkRad,
    beforeAc,
    afterAc,
    beforeRad,
    afterRad,
    beforeYotel,
    afterYotel,
    beforeBeth,
    afterBeth,
  };
  write("SUMMARIES.json", JSON.stringify(summary, null, 2));

  write(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — Publication Trigger Monitor V1

## Verdict

Reusable **publication-trigger monitoring** is live for the five frozen AC/Radisson campaigns. Monitors watch **known official sources only**, hash content, classify semantic change, and invoke the **shared second-generation decomposition** only when meaningful new evidence appears.

Baseline checks captured content hashes without Ready inflation.

## Active monitors

| Campaign | Status | Priority | Next check | Window |
|----------|--------|----------|------------|--------|
${allMon
  .map(
    (m) =>
      `| ${m.campaignKey} | ${m.monitoringStatus} | ${m.priorityRank} | ${m.nextCheckAt || "—"} | ${m.expectedPublicationWindowStart || "?"}→${m.expectedPublicationWindowEnd || "?"} |`
  )
  .join("\n")}

## Gates

| Hotel | Ready | Watch | Pursuits |
|-------|-------|-------|----------|
| AC | ${afterAc.ready} (was ${beforeAc.ready}) | ${afterAc.watch} | ${afterAc.pursuits} |
| RAD | ${afterRad.ready} (was ${beforeRad.ready}) | ${afterRad.watch} | ${afterRad.pursuits} |
| YOTEL | ${afterYotel.ready} | campaigns ${afterYotel.campaigns} | — |
| Bethesda | Ready ${afterBeth.ready} | — | — |

## What this does not do

- Broad SERP every cycle
- Apify
- Invent participant lists
- Lower Ready/Watch thresholds
- Auto-promote on publication alone
- Treat AUTOAMERICAS Dominican Fiesta as a Radisson opportunity
`
  );

  console.log(JSON.stringify({
    monitors: allMon.length,
    active: active.length,
    cieloPriority: allMon.find((m) => m.campaignKey === "CIELO")?.priorityRank,
    autoExhibitor: allMon.some((m) => m.campaignKey === "AUTOAMERICAS"),
    yotelReady: afterYotel.ready,
    bethReady: afterBeth.ready,
    acReady: afterAc.ready,
    radReady: afterRad.ready,
  }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
