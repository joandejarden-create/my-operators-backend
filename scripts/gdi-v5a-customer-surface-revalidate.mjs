/**
 * Persist V5A customer-surface revalidation (eligibility only).
 * Dry-run by default; pass --apply to write.
 * Does NOT stamp card-presentation fields — presentation is owned by the UI tile.
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
    retainedTitles: afterActive.map((o) => o.title),
  };

  if (APPLY) {
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

const outPath = path.join(
  outDir,
  `revalidation-${NOW}${APPLY ? "-applied" : "-dry"}.json`
);
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      ok: true,
      apply: APPLY,
      businessDate: NOW,
      outPath,
      summary: Object.fromEntries(
        Object.entries(report.hotels).map(([id, h]) => [
          id,
          {
            label: h.label,
            beforeLegacy: h.beforeLegacyCustomerVisible,
            afterActive: h.afterActive,
            tally: h.tally,
          },
        ])
      ),
    },
    null,
    2
  )
);
