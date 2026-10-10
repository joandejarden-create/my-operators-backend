/**
 * GDI Maturity Funnel V1 — backfill classifier (DRY RUN by default).
 *
 *   node scripts/gdi-maturity-backfill-v1.mjs
 *   node scripts/gdi-maturity-backfill-v1.mjs --hotels=recLuxvwwxID7U2B8,rece0or38cxo3Fymb,recrPQcZg7SFARRb2
 *
 * Does NOT write opportunities unless --apply is passed (Phase 1 audit = dry-run only).
 */

import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadOpportunities,
  listRegisteredHotels,
} from "../lib/group-demand-intelligence/repository.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isCustomerFacingOpportunity } from "../lib/group-demand-intelligence/customer-visibility.js";
import { classifyAccountQuality } from "../lib/group-demand-intelligence/account-quality-taxonomy-v1.js";
import {
  assignGdiMaturityState,
  GDI_MATURITY_STATE,
  mapLegacyFieldsToSuggestedMaturity,
} from "../lib/group-demand-intelligence/gdi-maturity-v1.js";
import { isGeneratorOnlyCustomerRecord } from "../lib/group-demand-intelligence/customer-visibility.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports/gdi/maturity-v1");

const APPLY = process.argv.includes("--apply");
const hotelArg = process.argv.find((a) => a.startsWith("--hotels="));
const HOTEL_FILTER = hotelArg
  ? hotelArg
      .slice("--hotels=".length)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
  : null;

const FOCUS = [
  "recLuxvwwxID7U2B8", // Bethesda
  "rece0or38cxo3Fymb", // W Rome
  "recrPQcZg7SFARRb2", // YOTEL Geneva
];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function bump(map, key) {
  const k = String(key || "UNKNOWN");
  map[k] = (map[k] || 0) + 1;
}

async function main() {
  if (APPLY) {
    console.error(
      "Phase 1 backfill is DRY-RUN only. Refusing --apply. Re-run without --apply."
    );
    process.exit(2);
  }

  fs.mkdirSync(OUT_DIR, { recursive: true });

  let hotelIds = HOTEL_FILTER;
  if (!hotelIds) {
    try {
      const configured = listRegisteredHotels() || [];
      hotelIds = configured
        .map((h) => (typeof h === "string" ? h : h.hotelId || h.id))
        .filter(Boolean);
    } catch {
      hotelIds = [];
    }
    if (!hotelIds.length) hotelIds = [...FOCUS];
    // Always include focus hotels
    for (const id of FOCUS) {
      if (!hotelIds.includes(id)) hotelIds.push(id);
    }
  }

  const rows = [];
  const byHotel = {};
  const byMaturity = {};
  const byAccountQuality = {};
  const visibilityChange = { newlyVisible: 0, newlyHidden: 0, unchanged: 0 };
  const qualifiedRows = [];

  for (const hotelId of hotelIds) {
    let doc;
    try {
      doc = loadOpportunities(hotelId);
    } catch (err) {
      console.warn(`skip ${hotelId}: ${err.message}`);
      continue;
    }
    const opps = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
    byHotel[hotelId] = {
      total: opps.length,
      SIGNAL: 0,
      CANDIDATE: 0,
      QUALIFIED: 0,
      ACTIONABLE: 0,
      wasReady: 0,
      wasCustomerVisible: 0,
    };

    for (const opp of opps) {
      const ready = isGdiCustomerOpportunityReady(opp, { nowDate: "2026-10-05" });
      const aq = classifyAccountQuality(opp);
      const gen = isGeneratorOnlyCustomerRecord(opp);
      const visBefore = isCustomerFacingOpportunity(opp, {
        nowDate: "2026-10-05",
        env: { ...process.env, GDI_QUALIFIED_CUSTOMER_VISIBILITY_V1: "0" },
      });
      const maturity = assignGdiMaturityState(opp, { nowDate: "2026-10-05" });
      const stamped = {
        ...opp,
        gdiMaturityState: maturity.gdiMaturityState,
        gdiMaturityReason: maturity.gdiMaturityReason,
      };
      // Visibility with QUALIFIED flag imagined ON (audit only — not production default)
      const visIfQualifiedOn = isCustomerFacingOpportunity(stamped, {
        nowDate: "2026-10-05",
        env: {
          ...process.env,
          GDI_MATURITY_FUNNEL_V1: "1",
          GDI_QUALIFIED_CUSTOMER_VISIBILITY_V1: "1",
        },
      });

      if (ready.ok) byHotel[hotelId].wasReady += 1;
      if (visBefore) byHotel[hotelId].wasCustomerVisible += 1;
      byHotel[hotelId][maturity.gdiMaturityState] =
        (byHotel[hotelId][maturity.gdiMaturityState] || 0) + 1;
      bump(byMaturity, maturity.gdiMaturityState);
      bump(byAccountQuality, aq.class);

      let visDelta = "unchanged";
      if (!visBefore && visIfQualifiedOn) {
        visDelta = "would_become_visible_if_qualified_flag_on";
        visibilityChange.newlyVisible += 1;
      } else if (visBefore && !visIfQualifiedOn) {
        visDelta = "would_hide";
        visibilityChange.newlyHidden += 1;
      } else {
        visibilityChange.unchanged += 1;
      }

      const row = {
        hotelId,
        opportunityId: opp.id || opp.opportunityId || "",
        title: opp.title || "",
        organizationName: opp.organizationName || opp.company || "",
        oldReady: ready.ok,
        oldReadyState: ready.state,
        accountQualityClass: aq.class,
        generatorOnly: gen,
        childAdmissionClass: opp.childAdmissionClass || "",
        funnelStage: opp.funnelStage || "",
        customerFacingState: opp.customerFacingState || "",
        bookingWindowStatus: opp.bookingWindowStatus || "",
        priority: opp.priority || "",
        currentCustomerVisible: visBefore,
        suggestedLegacyMaturity: mapLegacyFieldsToSuggestedMaturity(opp),
        gdiMaturityState: maturity.gdiMaturityState,
        gdiMaturityReason: maturity.gdiMaturityReason,
        visibilityIfQualifiedFlagOn: visIfQualifiedOn,
        visibilityDelta: visDelta,
        travelingCohortType: opp.travelingCohortType || "",
        lodgingControlHypothesis: opp.lodgingControlHypothesis || "",
      };
      rows.push(row);

      if (maturity.gdiMaturityState === GDI_MATURITY_STATE.QUALIFIED) {
        qualifiedRows.push(row);
      }
    }
  }

  // CSV
  const headers = [
    "hotelId",
    "opportunityId",
    "organizationName",
    "title",
    "oldReady",
    "oldReadyState",
    "accountQualityClass",
    "generatorOnly",
    "childAdmissionClass",
    "funnelStage",
    "customerFacingState",
    "bookingWindowStatus",
    "priority",
    "currentCustomerVisible",
    "suggestedLegacyMaturity",
    "gdiMaturityState",
    "gdiMaturityReason",
    "visibilityIfQualifiedFlagOn",
    "visibilityDelta",
    "travelingCohortType",
    "lodgingControlHypothesis",
  ];
  const csvLines = [
    headers.join(","),
    ...rows.map((r) => headers.map((h) => csvEscape(r[h])).join(",")),
  ];
  const csvPath = path.join(OUT_DIR, "backfill-dry-run.csv");
  fs.writeFileSync(csvPath, csvLines.join("\n") + "\n", "utf8");

  // Markdown
  const focusLines = FOCUS.map((id) => {
    const h = byHotel[id] || { total: 0 };
    return `| ${id} | ${h.total || 0} | ${h.wasReady || 0} | ${h.wasCustomerVisible || 0} | ${h.SIGNAL || 0} | ${h.CANDIDATE || 0} | ${h.QUALIFIED || 0} | ${h.ACTIONABLE || 0} |`;
  }).join("\n");

  const qualifiedSection =
    qualifiedRows.length === 0
      ? "_No rows classified as QUALIFIED under Phase 1 evaluator (expected — strict cohort/lodging/account requirements)._\n"
      : qualifiedRows
          .slice(0, 50)
          .map(
            (r) =>
              `- **${r.organizationName || r.title}** (\`${r.opportunityId}\`) @ ${r.hotelId}\n  - reason: ${r.gdiMaturityReason}\n  - account: ${r.accountQualityClass}\n  - cohort: ${r.travelingCohortType || "—"} · lodging: ${r.lodgingControlHypothesis || "—"}`
          )
          .join("\n") + "\n";

  const md = `# GDI Maturity Funnel V1 — Backfill Dry-Run

**Generated:** ${new Date().toISOString()}
**Mode:** DRY RUN (no writes)
**Gate bypass:** NO
**Speculative accounts created:** NO

## Focus hotels (Bethesda / W Rome / YOTEL)

| Hotel | Total | Old Ready | Old Visible | SIGNAL | CANDIDATE | QUALIFIED | ACTIONABLE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
${focusLines}

## Global maturity counts

\`\`\`json
${JSON.stringify(byMaturity, null, 2)}
\`\`\`

## Account quality distribution (classified)

\`\`\`json
${JSON.stringify(byAccountQuality, null, 2)}
\`\`\`

## Visibility delta if \`GDI_QUALIFIED_CUSTOMER_VISIBILITY_V1=1\`

\`\`\`json
${JSON.stringify(visibilityChange, null, 2)}
\`\`\`

**Recommendation:** Keep \`GDI_QUALIFIED_CUSTOMER_VISIBILITY_V1\` **OFF** until Phase 2 review if \`newlyVisible\` includes questionable rows.

## Rows that became QUALIFIED

${qualifiedSection}

## Legacy field mapping (compat)

| Legacy signal | Suggested maturity (non-authoritative) |
| --- | --- |
| Ready / CONTACT_NOW / HIGH_PRIORITY | ACTIONABLE |
| QUALIFY_NOW / QUALIFIED | QUALIFIED (suggested only) |
| WATCH / RESEARCH_FURTHER / FUTURE_WATCH | CANDIDATE |
| DEMAND_GENERATOR / DISQUALIFIED | SIGNAL |

**Authoritative:** \`gdiMaturityState\` from \`assignGdiMaturityState()\`.
**ACTIONABLE** ≡ \`isGdiCustomerOpportunityReady()\` (unchanged).

## Artifacts

- \`${path.relative(ROOT, csvPath)}\`
- \`${path.relative(ROOT, path.join(OUT_DIR, "backfill-dry-run.md"))}\`
`;

  const mdPath = path.join(OUT_DIR, "backfill-dry-run.md");
  fs.writeFileSync(mdPath, md, "utf8");

  const summary = {
    hotels: Object.keys(byHotel).length,
    rows: rows.length,
    byMaturity,
    byHotel: Object.fromEntries(
      FOCUS.map((id) => [id, byHotel[id] || null])
    ),
    qualifiedCount: qualifiedRows.length,
    visibilityChange,
    csvPath,
    mdPath,
  };
  fs.writeFileSync(
    path.join(OUT_DIR, "backfill-dry-run-summary.json"),
    JSON.stringify(summary, null, 2) + "\n",
    "utf8"
  );

  console.log(JSON.stringify(summary, null, 2));
  console.log(`\nWrote ${mdPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
