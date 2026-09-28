/**
 * Persist V5A customer-surface revalidation for Hilton + Renaissance Times Square.
 * Dry-run by default; pass --apply to write.
 *
 * Usage:
 *   node scripts/gdi-v5a-customer-surface-revalidate.mjs
 *   node scripts/gdi-v5a-customer-surface-revalidate.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import {
  revalidateCustomerSurfacePopulation,
  classifyCustomerSurfaceOpportunity,
} from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";
import {
  applyCommercialCardContract,
  scoreCardCompleteness,
} from "../lib/group-demand-intelligence/commercial-card-contract-v1.js";
import { businessDateYmd } from "../lib/group-demand-intelligence/active-eligibility-v1.js";

const APPLY = process.argv.includes("--apply");
const NOW = businessDateYmd();
const HOTELS = [
  { hotelId: "rec35fExUxCClpOP6", label: "Hilton New York Times Square" },
  { hotelId: "recG66DQJKP2c0UNh", label: "Renaissance New York Times Square" },
  { hotelId: "recLuxvwwxID7U2B8", label: "Bethesda Marriott (regression)" },
];

const DEFECT_KEYS = [
  "CDA Annual Meeting",
  "NYC Hotel Week",
  "Interested in Exhibiting",
  "Powered by A2Z Events",
  "Tech Conference Venues",
  "How To Manage The Paper",
  "Interiors from Spain",
  "FRANCHISE Solutions Group",
  "Vanguard Industrial Corp",
];

const outDir = path.join(process.cwd(), "reports", "gdi-v5a-customer-surface");
fs.mkdirSync(outDir, { recursive: true });

const report = {
  generatedAt: new Date().toISOString(),
  businessDate: NOW,
  apply: APPLY,
  hotels: {},
};

for (const hotel of HOTELS) {
  const doc = await loadOpportunitiesCanonical(hotel.hotelId);
  const all = doc.opportunities || [];
  const beforeActive = filterCustomerFacingOpportunities(
    filterSalespersonView(all),
    { nowDate: NOW }
  ).length;

  // Before counts for Hilton/Renaissance use pre-filter snapshot of prior visibility
  // Re-compute "before" as rows that were salesperson-visible and not test,
  // ignoring the new gate for the BEFORE number when disposition not yet applied:
  const beforeLegacy = all.filter(
    (o) =>
      o &&
      o.priority !== "DISQUALIFIED" &&
      o.isTestData !== true &&
      o.customerVisible !== false
  ).length;

  const result = revalidateCustomerSurfacePopulation(all, { nowDate: NOW });
  const afterActive = result.opportunities.filter(
    (o) => o.customerActiveEligible === true && o.priority !== "DISQUALIFIED"
  );

  const cardScores = afterActive.map((o) => {
    const enriched = applyCommercialCardContract(o);
    return { id: o.id, title: o.title, ...scoreCardCompleteness(enriched) };
  });

  const defectExamples = {};
  for (const key of DEFECT_KEYS) {
    const row = result.rows.find((r) => String(r.title || "").includes(key));
    if (row) {
      defectExamples[key] = {
        disposition: row.disposition,
        entityClass: row.entityClass,
        activeDateClass: row.activeDateClass,
        reasons: row.reasons,
        title: row.title,
      };
    }
  }

  // Also classify any defect that was already removed from bag
  for (const key of DEFECT_KEYS) {
    if (defectExamples[key]) continue;
    const raw = all.find((o) => String(o.title || "").includes(key));
    if (!raw) continue;
    const cls = classifyCustomerSurfaceOpportunity(raw, { nowDate: NOW });
    defectExamples[key] = {
      disposition: cls.disposition,
      entityClass: cls.entityClass,
      activeDateClass: cls.activeDateClass,
      reasons: cls.reasons,
      title: raw.title,
    };
  }

  report.hotels[hotel.hotelId] = {
    label: hotel.label,
    beforeLegacyCustomerVisible: beforeLegacy,
    beforeActiveWithNewGate: beforeActive,
    afterActive: afterActive.length,
    tally: result.tally,
    defectExamples,
    cardCompleteness: {
      retained: afterActive.length,
      complete: cardScores.filter((c) => c.complete).length,
      avgScore:
        cardScores.length === 0
          ? 0
          : Number(
              (
                cardScores.reduce((s, c) => s + c.score, 0) / cardScores.length
              ).toFixed(2)
            ),
      samples: cardScores.slice(0, 8),
    },
    retainedTitles: afterActive.map((o) => o.title),
  };

  if (APPLY && hotel.hotelId !== "recLuxvwwxID7U2B8") {
    await saveOpportunitiesCanonical(hotel.hotelId, {
      ...doc,
      opportunities: result.opportunities,
      updatedAt: new Date().toISOString(),
      runId: doc.runId || null,
      v5aCustomerSurfaceRevalidatedAt: new Date().toISOString(),
      v5aBusinessDate: NOW,
    });
  } else if (APPLY && hotel.hotelId === "recLuxvwwxID7U2B8") {
    // Bethesda: apply card contract enrichment only for KEEP_ACTIVE; still stamp dispositions
    await saveOpportunitiesCanonical(hotel.hotelId, {
      ...doc,
      opportunities: result.opportunities,
      updatedAt: new Date().toISOString(),
      runId: doc.runId || null,
      v5aCustomerSurfaceRevalidatedAt: new Date().toISOString(),
      v5aBusinessDate: NOW,
    });
  }
}

const outPath = path.join(outDir, `revalidation-${NOW}${APPLY ? "-applied" : "-dry"}.json`);
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ ok: true, apply: APPLY, businessDate: NOW, outPath, summary: Object.fromEntries(
  Object.entries(report.hotels).map(([id, h]) => [
    id,
    {
      label: h.label,
      beforeLegacy: h.beforeLegacyCustomerVisible,
      afterActive: h.afterActive,
      tally: h.tally,
    },
  ])
) }, null, 2));
