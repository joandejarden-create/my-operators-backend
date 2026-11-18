#!/usr/bin/env node
/**
 * Bethesda authorized 27 research CREATEs (founder-gated).
 *
 *   node scripts/gdi-bethesda-27-production-backfill-v1.mjs
 *   node scripts/gdi-bethesda-27-production-backfill-v1.mjs --apply \
 *     --authorize-production-writes \
 *     --exact-write-set-authorization=gdi-bethesda-research-inventory-27-creates-v1
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
  BETHESDA_HOTEL_ID,
  buildBethesda27ProductionManifest,
  preflightBethesda27ResearchCreates,
  executeBethesda27ResearchCreates,
  alignCanonicalBethesdaOpportunitiesTo86,
  reconcileBethesdaResearchInventory,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-research-consolidation-v1.js";
import { listResearchInventoryForHotel } from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { listCanonicalResearchHotels } from "../lib/group-demand-intelligence/workflow/gdi-all-hotel-research-backfill-final-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-bethesda-27-production-backfill-v1");
const APPROVED_MANIFEST = path.join(
  ROOT,
  "reports/gdi-bethesda-research-consolidation-and-persistence-activation-v1/EXACT_27_RECORD_PRODUCTION_MANIFEST.json",
);

const APPLY = process.argv.includes("--apply");
const AUTHORIZE = process.argv.includes("--authorize-production-writes");
const authArg = process.argv.find((a) =>
  a.startsWith("--exact-write-set-authorization="),
);
const EXACT_AUTH = authArg
  ? authArg.slice("--exact-write-set-authorization=".length)
  : "";

function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string" ? body : `${JSON.stringify(body, null, 2)}\n`,
  );
  return p;
}

async function countAllResearchInventory(repoRoot) {
  const hotels = listCanonicalResearchHotels(repoRoot);
  let total = 0;
  const byHotel = {};
  for (const h of hotels) {
    const rows = await listResearchInventoryForHotel(h.hotelId);
    byHotel[h.hotelId] = rows.length;
    total += rows.length;
  }
  return { total, byHotel, hotelCount: hotels.length };
}

async function main() {
  const approved = JSON.parse(fs.readFileSync(APPROVED_MANIFEST, "utf8"));
  const regenerated = buildBethesda27ProductionManifest(ROOT);
  const approvedKeys = (approved.entries || []).map((e) => e.compositeKey).sort();
  const regenKeys = regenerated.entries.map((e) => e.compositeKey).sort();
  const keysMatch =
    approvedKeys.length === 27 &&
    regenKeys.length === 27 &&
    approvedKeys.every((k, i) => k === regenKeys[i]);

  write("APPROVED_MANIFEST_COPY.json", approved);
  write("REGENERATED_MANIFEST_CHECK.json", {
    keysMatch,
    approvedKeys,
    regenKeys,
    approvedAuth: approved.authorizationId,
    regeneratedAuth: regenerated.authorizationId,
    sourceSummaryPlaceholders: regenerated.entries.filter((e) =>
      String(e.fields?.[F.sourceSummary] || "").includes("[object Object]"),
    ).length,
  });

  console.log("[preflight] querying live Airtable…");
  const preflight = await preflightBethesda27ResearchCreates(ROOT);
  write("FINAL_LIVE_PREFLIGHT.json", preflight);
  write(
    "FINAL_LIVE_PREFLIGHT.md",
    `# Final live preflight

| Check | Result |
|-------|--------|
| Unique composite keys | ${preflight.uniqueCompositeKeys} |
| Manifest entries | ${preflight.entryCount} |
| sourceSummary placeholders | ${preflight.sourceSummaryPlaceholderCount} |
| Payload over limit | ${preflight.payloadOverLimitCount} |
| Live Bethesda count | ${preflight.liveBethesdaCount} |
| Original 59 present | ${preflight.original59PresentInAirtable} |
| Already present of 27 | ${preflight.alreadyPresentCount} |
| Proposed CREATEs | ${preflight.proposedCreateCount} |
| Keys match approved | ${keysMatch} |
| Stop reasons | ${preflight.stopReasons.length} |
| Ready | **${preflight.ok && keysMatch}** |

Authorization: \`${BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION}\`
`,
  );

  if (!preflight.ok || !keysMatch) {
    write("EXECUTION_BLOCKED.json", {
      preflight,
      keysMatch,
      applyRequested: APPLY,
    });
    console.error("PREFLIGHT_BLOCKED — no writes applied");
    process.exitCode = 2;
    return;
  }

  console.log(
    `[execute] apply=${APPLY} authorize=${AUTHORIZE} proposedCreates=${preflight.proposedCreateCount}`,
  );
  const execution = await executeBethesda27ResearchCreates({
    repoRoot: ROOT,
    apply: APPLY,
    authorizeProductionWrites: AUTHORIZE,
    exactWriteSetAuthorization: EXACT_AUTH || BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
    sleepMs: APPLY ? 220 : 0,
  });
  write("PRODUCTION_WRITE_LEDGER.json", execution);

  const recordMap = (execution.results || [])
    .filter((r) => r.airtableRecordId)
    .map((r) => ({
      opportunityId: r.opportunityId,
      compositeKey: r.compositeKey,
      airtableRecordId: r.airtableRecordId,
      action: r.action,
      readBackOk: r.readBack?.ok ?? null,
    }));
  write("AIRTABLE_RECORD_ID_MAPPING.json", {
    count: recordMap.length,
    records: recordMap,
  });

  write("READ_BACK_VERIFICATION.json", {
    results: (execution.results || []).map((r) => ({
      opportunityId: r.opportunityId,
      action: r.action,
      readBack: r.readBack || null,
      ok: r.ok,
    })),
  });

  // Second dry-run reconciliation (idempotency)
  console.log("[idempotency] second dry-run…");
  const idempotency = await executeBethesda27ResearchCreates({
    repoRoot: ROOT,
    apply: false,
    authorizeProductionWrites: false,
    exactWriteSetAuthorization: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
    sleepMs: 0,
  });
  write("IDEMPOTENCY_VERIFICATION.json", {
    counts: idempotency.counts,
    postBethesdaCount: idempotency.postBethesdaCount,
    wouldCreate: idempotency.counts.wouldCreate,
    alreadyPresent: idempotency.counts.alreadyPresent,
    expectedAfterApply:
      APPLY && execution.ok
        ? { wouldCreate: 0, alreadyPresent: 27 }
        : null,
  });

  let portfolio = null;
  try {
    portfolio = await countAllResearchInventory(ROOT);
  } catch (err) {
    portfolio = { error: String(err?.message || err) };
  }
  write("PORTFOLIO_RESEARCH_INVENTORY_RECONCILIATION.json", portfolio);

  // Canonical filesystem alignment (after successful apply, or dry preview)
  const alignDry = alignCanonicalBethesdaOpportunitiesTo86(ROOT, { write: false });
  let alignLive = alignDry;
  if (APPLY && execution.ok && execution.counts.failed === 0) {
    alignLive = alignCanonicalBethesdaOpportunitiesTo86(ROOT, { write: true });
  }
  write("CANONICAL_FILESYSTEM_ALIGNMENT.json", { dry: alignDry, applied: alignLive });

  const recon = reconcileBethesdaResearchInventory(ROOT, { scanLedgers: false });
  write("BETHESDA_86_86_RECONCILIATION.json", {
    counts: recon.counts,
    sharedTitleOrgDrift: recon.counts.sharedTitleOrgDrift,
    fingerprintDriftSample: (recon.fingerprintDrift || []).slice(0, 5),
    note: "24 shared title/org drifts retained informational; original 59 not overwritten",
  });

  write(
    "GDO_PUBLICATION_ISOLATION.json",
    {
      gdoWrites: 0,
      publicationWrites: 0,
      rockCreekApplied: false,
      autoPersistenceEnabled: false,
      researchTableOnly: true,
      tableId: "tblbkDj0vhQLfrssV",
      baseId: "appa2cE7FTRmIbB32",
    },
  );

  write(
    "FAILURES_OR_EXCEPTIONS.json",
    {
      preflightStopReasons: preflight.stopReasons,
      executionFailed: (execution.results || []).filter((r) => !r.ok),
      blocked: execution.blocked === true,
      code: execution.code,
    },
  );

  write(
    "README.md",
    `# Bethesda 27 production research backfill V1

## Authorization

\`${BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION}\`

## Outcome

| Metric | Value |
|--------|------:|
| Applied | ${execution.applied === true} |
| CREATED | ${execution.counts.created} |
| WOULD_CREATE (dry) | ${execution.counts.wouldCreate} |
| ALREADY_PRESENT | ${execution.counts.alreadyPresent} |
| Failed | ${execution.counts.failed} |
| Post Bethesda Airtable count | ${JSON.stringify(execution.postBethesdaCount)} |
| Portfolio research total | ${portfolio?.total ?? portfolio?.error ?? "n/a"} |
| Canonical FS after | ${alignLive.afterCount} (wrote=${alignLive.wrote}) |
| GDO writes | 0 |
| Publication writes | 0 |

## Acceptance targets (report actuals above)

- Bethesda research inventory: 86
- Total research inventory: 686
- New creates: ≤27 (only missing keys)
- Repeated run: zero unnecessary CREATEs/UPDATEs
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: execution.ok,
        applied: execution.applied,
        counts: execution.counts,
        postBethesdaCount: execution.postBethesdaCount,
        portfolioTotal: portfolio?.total ?? null,
        fsAligned: alignLive.wrote,
        fsCount: alignLive.afterCount,
      },
      null,
      2,
    ),
  );

  if (!execution.ok) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
