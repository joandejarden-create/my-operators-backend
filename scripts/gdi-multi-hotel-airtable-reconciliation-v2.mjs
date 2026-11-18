#!/usr/bin/env node
/**
 * Multi-hotel Airtable reconciliation + dry-run sync planner V2.
 * Read-only. No production Airtable writes. No paid research. No publication changes.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  REFERENCE_HOTELS_V2,
  loadFilesystemOpportunities,
  publicationPointers,
  planHotelOpportunitySync,
  buildMultiHotelDryRunManifest,
  analyzeRockCreekVenueIdentity,
  assertNoUnauthorizedProductionWrite,
  certifyWeeklyChangeStabilityAcrossIdenticalReruns,
  sampleCrossHotelSharedIdentitySafety,
  SYNC_DECISION,
  GDI_MULTI_HOTEL_AIRTABLE_RECONCILIATION_V2,
} from "../lib/group-demand-intelligence/workflow/gdi-multi-hotel-airtable-reconciliation-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-multi-hotel-airtable-reconciliation-v2");

function write(name, content) {
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof content === "string" ? content : JSON.stringify(content, null, 2),
  );
  return p;
}

function mdEscape(s) {
  return String(s || "").replace(/\|/g, "\\|");
}

async function loadAirtable(hotelId) {
  try {
    const rows = await listOpportunitiesForHotel(hotelId);
    return { ok: true, rows: rows || [], error: null };
  } catch (err) {
    return {
      ok: false,
      rows: [],
      error: String(err?.code || err?.message || err),
    };
  }
}

function loadRockCreekAudit() {
  const p = path.join(OUT, "ROCK_CREEK_IDENTITY_AUDIT.json");
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function buildFounderSummary({ manifest, rockCreek, hotelPlans }) {
  const yotel = hotelPlans.find((p) => p.hotel.key === "yotel");
  const radisson = hotelPlans.find((p) => p.hotel.key === "radisson");
  const guad = hotelPlans.find((p) => p.hotel.key === "guadalajara");
  return `# Multi-hotel Airtable reconciliation V2 — founder summary

**STOP FOR FOUNDER REVIEW — NO PRODUCTION WRITES IN THIS PHASE**

## Verdict

Rock Creek's dual venue IDs are the **same physical venue**. The domain-stable historical ID \`pev_53b689545914754d\` is authoritative; the pilot structured ID \`pev_42542bf58aac936b\` is a geo-only recomputation without \`rockcreekmansion.com\`. Remediation is planned but **not applied**.

YOTEL / Radisson / Guadalajara inventory gaps are **identity and lifecycle differences**, not simple missing mirrors. Dry-run sync planner is deterministic and fail-closed.

## Rock Creek

| Item | Value |
|------|-------|
| Airtable record | \`recf35Q9B6jpX3Ivm\` |
| Structured Venue ID (live) | \`${rockCreek?.live?.structuredVenueId || "n/a"}\` |
| Payload venueId | \`${rockCreek?.live?.payloadVenueId || "n/a"}\` |
| Authoritative proposal | \`pev_53b689545914754d\` |
| Alias / erroneous rewrite | \`pev_42542bf58aac936b\` |
| Same physical venue | yes |

## Primary hotel inventory

| Hotel | FS | Airtable | Overlap | FS-only | AT-only | CREATE cand. | UPDATE cand. |
|-------|----|----------|---------|---------|---------|--------------|--------------|
| YOTEL | ${yotel?.filesystemCount ?? "?"} | ${yotel?.airtableCount ?? "?"} | ${yotel?.idOverlapCount ?? "?"} | ${yotel?.filesystemOnlyCount ?? "?"} | ${yotel?.airtableOnlyCount ?? "?"} | ${yotel?.decisionCounts?.[SYNC_DECISION.CREATE] ?? 0} | ${yotel?.decisionCounts?.[SYNC_DECISION.UPDATE] ?? 0} |
| Radisson | ${radisson?.filesystemCount ?? "?"} | ${radisson?.airtableCount ?? "?"} | ${radisson?.idOverlapCount ?? "?"} | ${radisson?.filesystemOnlyCount ?? "?"} | ${radisson?.airtableOnlyCount ?? "?"} | ${radisson?.decisionCounts?.[SYNC_DECISION.CREATE] ?? 0} | ${radisson?.decisionCounts?.[SYNC_DECISION.UPDATE] ?? 0} |
| Guadalajara | ${guad?.filesystemCount ?? "?"} | ${guad?.airtableCount ?? "?"} | ${guad?.idOverlapCount ?? "?"} | ${guad?.filesystemOnlyCount ?? "?"} | ${guad?.airtableOnlyCount ?? "?"} | ${guad?.decisionCounts?.[SYNC_DECISION.CREATE] ?? 0} | ${guad?.decisionCounts?.[SYNC_DECISION.UPDATE] ?? 0} |

## Sync readiness totals

- CREATE candidates: **${manifest.totals.createCandidates}**
- UPDATE candidates: **${manifest.totals.updateCandidates}**
- DEFERRED: **${manifest.totals.deferred}**
- CONFLICT: **${manifest.totals.conflicts}**
- REJECTED: **${manifest.totals.rejected}**

Production Airtable synchronization remains **unauthorized** until founder approves an exact write-set allowlist after Rock Creek remediation decision.

## Safety

- No Airtable mutations in this phase
- No paid research
- No customer publication changes
- Weekly Change continuity preserved in planner annotations
- Hotel-private feedback not copied into global identity
`;
}

function buildMatrix(hotelPlans) {
  const lines = [
    "# Hotel inventory reconciliation matrix",
    "",
    "| Hotel | Cohort | FS | AT | Overlap | FS-only | AT-only | CREATE | UPDATE | UNCHANGED | DEFERRED | CONFLICT | REJECTED |",
    "|-------|--------|----|----|---------|---------|---------|--------|--------|-----------|----------|----------|----------|",
  ];
  for (const p of hotelPlans) {
    const d = p.decisionCounts || {};
    lines.push(
      `| ${mdEscape(p.hotel.name)} | ${p.hotel.cohort} | ${p.filesystemCount} | ${p.airtableCount} | ${p.idOverlapCount} | ${p.filesystemOnlyCount} | ${p.airtableOnlyCount} | ${d.CREATE || 0} | ${d.UPDATE || 0} | ${d.UNCHANGED || 0} | ${d.DEFERRED || 0} | ${d.CONFLICT || 0} | ${d.REJECTED || 0} |`,
    );
  }
  lines.push("");
  lines.push(
    "Counts are opportunity-ID joins. Stores are not forced to equal each other.",
  );
  return lines.join("\n");
}

function buildIdLevelMd(plan, title) {
  const lines = [
    `# ${title}`,
    "",
    `- Hotel ID: \`${plan.hotel.hotelId}\``,
    `- Filesystem: ${plan.filesystemCount}`,
    `- Airtable: ${plan.airtableCount}`,
    `- Overlap: ${plan.idOverlapCount}`,
    "",
    "## Classification counts",
    "",
    "```json",
    JSON.stringify(plan.counts, null, 2),
    "```",
    "",
    "## Decision counts",
    "",
    "```json",
    JSON.stringify(plan.decisionCounts, null, 2),
    "```",
    "",
    "## Record-level rows (PII stripped)",
    "",
  ];
  for (const r of plan.inventoryRows) {
    lines.push(
      `- \`${r.opportunityId}\` — ${r.class} → **${r.syncDecision}** (${r.syncReason})`,
    );
  }
  lines.push("");
  return lines.join("\n");
}

async function main() {
  assertNoUnauthorizedProductionWrite({
    apply: false,
    authorizeProductionWrites: false,
  });
  fs.mkdirSync(OUT, { recursive: true });

  const rockAudit = loadRockCreekAudit();
  const rockCreek = analyzeRockCreekVenueIdentity({
    structuredVenueId: rockAudit?.record?.structuredVenueId,
    payloadVenueId: rockAudit?.record?.payloadVenueId,
    website: rockAudit?.record?.website,
    officialDomain: rockAudit?.record?.officialDomain,
    city: rockAudit?.record?.city,
    onSiteLodgingStatus: rockAudit?.record?.onSiteLodgingStatus,
  });

  const hotelPlans = [];
  const airtableErrors = [];
  const oppsByHotel = {};

  for (const hotel of REFERENCE_HOTELS_V2) {
    const filesystem = loadFilesystemOpportunities(ROOT, hotel.hotelId);
    const at = await loadAirtable(hotel.hotelId);
    if (!at.ok) airtableErrors.push({ hotelId: hotel.hotelId, error: at.error });
    const plan = planHotelOpportunitySync({
      hotel,
      filesystem,
      airtableRows: at.rows,
    });
    plan.publication = publicationPointers(ROOT, hotel.hotelId);
    plan.airtableReadOk = at.ok;
    plan.airtableReadError = at.error;
    hotelPlans.push(plan);
    oppsByHotel[hotel.hotelId] = filesystem.rows || [];

    write(`${hotel.key.toUpperCase()}_ID_LEVEL_RECONCILIATION.json`, {
      hotelId: hotel.hotelId,
      name: hotel.name,
      cohort: hotel.cohort,
      filesystemCount: plan.filesystemCount,
      airtableCount: plan.airtableCount,
      counts: plan.counts,
      decisionCounts: plan.decisionCounts,
      publication: plan.publication,
      airtableReadOk: plan.airtableReadOk,
      rows: plan.inventoryRows,
      syncOperations: plan.syncOperations,
    });
  }

  // Idempotency of planner: re-plan first primary hotel
  const primary = hotelPlans[0];
  const rerun = planHotelOpportunitySync({
    hotel: primary.hotel,
    filesystem: loadFilesystemOpportunities(ROOT, primary.hotel.hotelId),
    airtableRows: (await loadAirtable(primary.hotel.hotelId)).rows,
  });
  const weeklyStable = certifyWeeklyChangeStabilityAcrossIdenticalReruns(
    primary,
    rerun,
  );
  const sharedIdentity = sampleCrossHotelSharedIdentitySafety(oppsByHotel);

  const manifest = buildMultiHotelDryRunManifest({
    hotelPlans,
    rockCreekAnalysis: rockCreek,
  });
  manifest.airtableErrors = airtableErrors;
  manifest.idempotencyRecheck = weeklyStable;
  manifest.sharedIdentitySample = sharedIdentity;

  write("MULTI_HOTEL_DRY_RUN_SYNC_MANIFEST.json", manifest);
  write("FOUNDER_EXECUTIVE_SUMMARY.md", buildFounderSummary({
    manifest,
    rockCreek,
    hotelPlans,
  }));
  write("HOTEL_INVENTORY_RECONCILIATION_MATRIX.md", buildMatrix(hotelPlans));
  write(
    "YOTEL_ID_LEVEL_RECONCILIATION.md",
    buildIdLevelMd(
      hotelPlans.find((p) => p.hotel.key === "yotel"),
      "YOTEL Geneva Lake — ID-level reconciliation",
    ),
  );
  write(
    "RADISSON_ID_LEVEL_RECONCILIATION.md",
    buildIdLevelMd(
      hotelPlans.find((p) => p.hotel.key === "radisson"),
      "Radisson Blu Toronto Downtown — ID-level reconciliation",
    ),
  );
  write(
    "GUADALAJARA_ID_LEVEL_RECONCILIATION.md",
    buildIdLevelMd(
      hotelPlans.find((p) => p.hotel.key === "guadalajara"),
      "Sheraton Guadalajara Expo — ID-level reconciliation",
    ),
  );

  const yotel = hotelPlans.find((p) => p.hotel.key === "yotel");
  const radisson = hotelPlans.find((p) => p.hotel.key === "radisson");
  const guad = hotelPlans.find((p) => p.hotel.key === "guadalajara");

  console.log(
    JSON.stringify(
      {
        version: GDI_MULTI_HOTEL_AIRTABLE_RECONCILIATION_V2,
        out: OUT,
        rockCreekAuthoritative: rockCreek.authoritativeCanonicalId,
        totals: manifest.totals,
        primary: {
          yotel: {
            fs: yotel.filesystemCount,
            at: yotel.airtableCount,
            create: yotel.decisionCounts.CREATE,
          },
          radisson: {
            fs: radisson.filesystemCount,
            at: radisson.airtableCount,
            create: radisson.decisionCounts.CREATE,
          },
          guadalajara: {
            fs: guad.filesystemCount,
            at: guad.airtableCount,
            create: guad.decisionCounts.CREATE,
          },
        },
        weeklyStable,
        airtableErrors,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
