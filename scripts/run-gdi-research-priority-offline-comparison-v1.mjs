#!/usr/bin/env node
/**
 * Offline research-priority comparison on frozen inventories ($0).
 * Does not change production selection policy. Does not call paid providers.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  inventoryRowToSelectionCandidate,
  compareResearchPriorityPolicies,
} from "../lib/group-demand-intelligence/research/gdi-research-priority-evaluation-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function loadOpportunities(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  return Array.isArray(raw) ? raw : raw.opportunities || raw.rows || [];
}

const hotels = [
  {
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    maxCandidates: 8,
  },
  {
    hotelId: "recH9h3McgMNFQEla",
    hotelName: "Radisson Blu Toronto Downtown",
    maxCandidates: 8,
  },
];

const report = {
  version: "gdi-research-priority-offline-comparison-v1",
  at: new Date().toISOString(),
  paidCalls: 0,
  airtableWrites: 0,
  productionPolicyChanged: false,
  hotels: [],
};

for (const h of hotels) {
  const rows = loadOpportunities(h.hotelId);
  const candidates = rows.map((r) =>
    inventoryRowToSelectionCandidate(r, {
      hotelId: h.hotelId,
      hotelName: h.hotelName,
    }),
  );
  const comparison = compareResearchPriorityPolicies({
    candidates,
    maxCandidates: h.maxCandidates,
    fairMinPerSegment: 1,
    hotelProfile: { city: h.hotelName.includes("Bethesda") ? "Bethesda" : "Toronto" },
  });
  report.hotels.push({
    hotelId: h.hotelId,
    hotelName: h.hotelName,
    inventoryCount: rows.length,
    comparison,
  });
}

const outDir = path.join(
  ROOT,
  "reports/gdi-research-priority-offline-comparison-v1",
);
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "COMPARISON.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);

const bethesda = report.hotels[0]?.comparison;
const md = `# GDI research-priority offline comparison V1

Generated: ${report.at}

## Safety
- Paid calls: 0
- Airtable writes: 0
- Production selection policy changed: **false**

## Structural finding
${bethesda?.structuralFinding?.note || ""}

## Bethesda Marriott (frozen inventory)

| Metric | Value |
| --- | ---: |
| Inventory rows | ${report.hotels[0]?.inventoryCount} |
| Cap | ${bethesda?.maxCandidates} |
| Current selected | ${bethesda?.policies?.currentProductionFairFloorThenScore?.selectedCount} |
| Proposed pure-score selected | ${bethesda?.policies?.proposedPureScoreEval?.selectedCount} |
| Overlap current∩proposed | ${bethesda?.overlap?.currentVsProposed} |
| Overlooked by current (in proposed top) | ${bethesda?.valuableOverlookedByCurrentCap?.length} |

### Segments selected under current policy
\`\`\`
${JSON.stringify(bethesda?.policies?.currentProductionFairFloorThenScore?.segmentSelected || {}, null, 2)}
\`\`\`

### Examples overlooked by current cap (proposed score)
${(bethesda?.valuableOverlookedByCurrentCap || [])
  .slice(0, 8)
  .map(
    (r) =>
      `- ${r.organizationName || r.id} · ${r.segmentId} · proposed=${r.proposedScore} existing=${r.existingScore}`,
  )
  .join("\n") || "- (none)"}

## Recommendation
${bethesda?.recommendation || ""}

**Founder gate:** do not enable proposed weights in production until review.
`;

fs.writeFileSync(path.join(outDir, "FOUNDER_COMPARISON.md"), md);
console.log(JSON.stringify({ ok: true, outDir, hotels: report.hotels.length }, null, 2));
