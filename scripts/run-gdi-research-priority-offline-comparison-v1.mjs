#!/usr/bin/env node
/**
 * Offline research-priority comparison on frozen inventories ($0).
 * Includes founder Option 2 fairness/commercial arms.
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
  founderOption2Selected: true,
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
    hotelProfile: {
      city: h.hotelName.includes("Bethesda") ? "Bethesda" : "Toronto",
    },
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
const q = bethesda?.qualityByPolicy || {};
const arms =
  bethesda?.policies?.option2BoundedFairnessThenCommercialEval || {};
const rot = bethesda?.fairRotationSimulation?.fair40_commercial60;

function qualityRow(label, quality) {
  if (!quality) return `| ${label} | — | — | — | — | — |`;
  return `| ${label} | ${quality.selectedCount} | ${quality.segmentDiversity} | ${quality.meanProposedScore} | ${quality.withEvidenceCount} | ${quality.withContactNameCount} |`;
}

const md = `# GDI research-priority offline comparison V1 (Option 2)

Generated: ${report.at}

## Safety
- Paid calls: 0
- Airtable writes: 0
- Production selection policy changed: **false**
- Founder Option 2 selected for evaluation: **true**
- Production scoring weights: **unchanged**

## Structural finding
${bethesda?.structuralFinding?.note || ""}

## Coverage distinction
- Shallow discovery coverage ≠ deep-research coverage
- ${bethesda?.coverageDistinction?.note || ""}

## Edition dedupe (Bethesda)
- Inventory rows: ${report.hotels[0]?.inventoryCount}
- After org+edition dedupe: ${bethesda?.afterEditionDedupeCount}
- Dropped same-edition duplicates: ${bethesda?.droppedSameEditionDuplicates}
- Distinct future editions of the same org are **not** collapsed

## Policy quality comparison (Bethesda, cap=8)

| Policy | Selected | Segment diversity | Mean score | With evidence | With contact name |
| --- | ---: | ---: | ---: | ---: | ---: |
${qualityRow("Current fair-floor", q.currentFairFloor)}
${qualityRow("Pure commercial", q.pureCommercial)}
${qualityRow("Fair 25% / Commercial 75%", q.fair25_commercial75)}
${qualityRow("Fair 40% / Commercial 60%", q.fair40_commercial60)}
${qualityRow("Fair 50% / Commercial 50%", q.fair50_commercial50)}

### Option 2 arm slot budgets
${Object.entries(arms)
  .map(
    ([id, arm]) =>
      `- **${id}**: fairSlots=${arm.fairSlots}, commercialSlots=${arm.commercialSlots}, diversity=${arm.quality?.segmentDiversity}`,
  )
  .join("\n") || "- (none)"}

### Fair rotation (fair40 / 4 cycles)
- Late segments recovered across cycles: **${rot?.lateSegmentsRecovered}**
- Starved after 4 cycles: ${(rot?.starvedAfterCycles || []).join(", ") || "(none)"}

### Overlap
- Current ∩ pure commercial: ${bethesda?.overlap?.currentVsProposed}
- Option2 40/60 ∩ current: ${bethesda?.overlap?.option2_40_vs_current}
- Option2 40/60 ∩ pure: ${bethesda?.overlap?.option2_40_vs_pure}

### Examples overlooked by current fair-floor cap (pure commercial top)
${(bethesda?.valuableOverlookedByCurrentCap || [])
  .slice(0, 8)
  .map(
    (r) =>
      `- ${r.organizationName || r.id} · ${r.segmentId} · proposed=${r.proposedScore} existing=${r.existingScore}`,
  )
  .join("\n") || "- (none)"}

## Recommendation
${bethesda?.recommendation || ""}

**Founder gate:** keep production weights unchanged until this Option 2 evaluation is reviewed. Do not enable a new paid production selection policy from this report alone.
`;

fs.writeFileSync(path.join(outDir, "FOUNDER_COMPARISON.md"), md);
console.log(
  JSON.stringify(
    {
      ok: true,
      outDir,
      hotels: report.hotels.length,
      productionPolicyChanged: false,
    },
    null,
    2,
  ),
);
