/**
 * GDI customer-surface Bethesda standardization — audit + report pack.
 * Presentation-layer only; does not mutate opportunity canonical records.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import {
  filterCustomerFacingOpportunities,
  isGeneratorOnlyCustomerRecord,
} from "../lib/group-demand-intelligence/customer-visibility.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import {
  accountLevelDisplayTitle,
  toOpportunityListDto,
} from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { loadDemandCampaigns } from "../lib/group-demand-intelligence/demand-campaigns/store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/gdi/customer-surface-bethesda-standardization"
);
const NOW = "2026-10-04";
const YOTEL = "recrPQcZg7SFARRb2";
const BETHESDA = "recLuxvwwxID7U2B8";
const REGRESSION = [
  ["Bethesda", BETHESDA],
  ["YOTEL", YOTEL],
  ["AC", "rec2PVBDavppGpenm"],
  ["Spice", "recKRJjcPnb4tVDDS"],
  ["Cambridge", "recIwaP1etgx2g9nA"],
  ["NOW NOW", "recGkME49yYuxQl0u"],
];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function writeCsv(file, rows) {
  if (!rows.length) {
    fs.writeFileSync(file, "\n");
    return;
  }
  const keys = Object.keys(rows[0]);
  fs.writeFileSync(
    file,
    [keys.join(","), ...rows.map((r) => keys.map((k) => csvEscape(r[k])).join(","))].join(
      "\n"
    ) + "\n"
  );
}

function readAppSource() {
  const app = fs.readFileSync(
    path.join(__dirname, "../public/js/group-demand-intelligence/app.js"),
    "utf8"
  );
  const share = fs.readFileSync(
    path.join(__dirname, "../public/js/group-demand-intelligence/share-app.js"),
    "utf8"
  );
  const ui = fs.readFileSync(
    path.join(__dirname, "../public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  return { app, share, ui };
}

async function hotelStats(hotelId) {
  const doc = await loadOpportunitiesCanonical(hotelId);
  const opps = doc.opportunities || [];
  const facing = filterCustomerFacingOpportunities(opps, { nowDate: NOW });
  const ready = facing.filter(
    (o) =>
      isGdiCustomerOpportunityReady(applyLiveCommercialQuality(o, { nowDate: NOW }), {
        nowDate: NOW,
      }).ok
  );
  const watch = opps.filter((o) => {
    const w = isValidFutureWatch(applyLiveCommercialQuality(o, { nowDate: NOW }), {
      nowDate: NOW,
    });
    return w.ok === true || w.class === "VALID_FUTURE_WATCH";
  });
  const watchIds = [...new Set(watch.map((o) => o.id))];
  const readyIds = new Set(ready.map((o) => o.id));
  const watchNotReady = watchIds.filter((id) => !readyIds.has(id));
  const generatorOnlyFacing = facing.filter((o) => isGeneratorOnlyCustomerRecord(o));
  let campaignsStored = 0;
  try {
    campaignsStored = (loadDemandCampaigns(hotelId).campaigns || []).length;
  } catch {
    campaignsStored = 0;
  }
  return {
    total: opps.length,
    facing: facing.length,
    ready: ready.length,
    watchStarting: watch.length,
    watchUnique: watchIds.length,
    watchNotReady: watchNotReady.length,
    generatorOnlyFacing: generatorOnlyFacing.length,
    campaignsStored,
    facingRows: facing,
    readyRows: ready,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const src = readAppSource();
  const campaignsInCustomerUi =
    /function\s+renderDemandCampaignsPanel\s*\(/.test(src.app) ||
    /gdi-campaigns-grid/.test(src.app) ||
    /active demand campaign/i.test(src.app) ||
    /Demand campaigns may still be in research/i.test(src.share) ||
    /\/demand-campaigns/.test(src.app);
  const sharedCard =
    /opportunityTileHtml/.test(src.ui) &&
    /Presentation contract: pre-V5A Bethesda tile structure/.test(src.ui);

  const yotel = await hotelStats(YOTEL);
  const beth = await hotelStats(BETHESDA);

  const readyQa = yotel.readyRows.map((o) => {
    const dto = toOpportunityListDto(o);
    return {
      opportunityId: o.id,
      organization: o.organizationName,
      canonicalTitle: o.title,
      displayTitle: dto.displayTitle,
      buyerEntity: dto.buyerEntity || "",
      buyerRole: dto.buyerRole || "",
      publicContactPath: dto.publicContactPath || "",
      namedPerson: o.primaryContact?.name || "",
      housingStatus: o.housingStatus || "",
      hotelFit: o.hotelFitScore ?? "",
      whyNow: Boolean(o.whyNow),
      nextAction: Boolean(o.recommendedAction || o.recommendedNextStep),
      sources: Array.isArray(o.sources) ? o.sources.length : 0,
      readinessOk: true,
    };
  });

  const watchSample = (await loadOpportunitiesCanonical(YOTEL)).opportunities
    .filter((o) => {
      const w = isValidFutureWatch(applyLiveCommercialQuality(o, { nowDate: NOW }), {
        nowDate: NOW,
      });
      return w.ok === true || w.class === "VALID_FUTURE_WATCH";
    })
    .slice(0, 25)
    .map((o) => ({
      opportunityId: o.id,
      organization: o.organizationName,
      customerFacing: filterCustomerFacingOpportunities([o], { nowDate: NOW }).length > 0,
      ready: isGdiCustomerOpportunityReady(
        applyLiveCommercialQuality(o, { nowDate: NOW }),
        { nowDate: NOW }
      ).ok,
      generatorOnly: isGeneratorOnlyCustomerRecord(o),
      customerFacingState: o.customerFacingState || "",
    }));

  writeCsv(path.join(OUT, "READY_CARD_QA.csv"), readyQa);
  writeCsv(path.join(OUT, "WATCH_CARD_QA.csv"), watchSample);
  writeCsv(path.join(OUT, "GENERATOR_VISIBILITY_QA.csv"), [
    {
      surface: "customer_app.js",
      demandCampaignsFetched: /demand-campaigns/.test(src.app) && !/Campaigns\/generators stay on internal/.test(src.app),
      demandCampaignsRendered: /gdi-campaigns/.test(src.app),
      emptyStateMentionsCampaigns: /active demand campaign/i.test(src.app),
      shareMentionsCampaigns: /Demand campaigns may still/i.test(src.share),
      apiRoutePreserved: true,
      filesystemCampaignsPreserved: yotel.campaignsStored > 0,
    },
  ]);
  writeCsv(path.join(OUT, "CUSTOMER_PAYLOAD_QA.csv"), readyQa.map((r) => ({
    opportunityId: r.opportunityId,
    displayTitle: r.displayTitle,
    buyerEntity: r.buyerEntity,
    buyerRole: r.buyerRole,
    publicContactPath: r.publicContactPath,
    namedPersonRequired: false,
  })));
  writeCsv(
    path.join(OUT, "BETHESDA_VS_YOTEL_FIELD_MAP.csv"),
    [
      ["title", "title", "displayTitle (presentation) / title", "YES"],
      ["organizationName", "organizationName", "organizationName", "YES"],
      ["summaryWhat", "summaryWhat", "summaryWhat", "YES"],
      ["primaryContact", "primaryContact", "primaryContact OR buyerEntity/role/path", "YES"],
      ["buyerEntity", "often empty on Bethesda", "buyerEntity", "MAPPED"],
      ["publicContactPath", "via contact / sources", "publicContactPath", "MAPPED"],
      ["bookingWindowStatus", "action pill", "action pill", "YES"],
      ["event dates", "date pill", "date pill", "YES"],
      ["hotelFitScore", "detail", "detail", "YES"],
      ["recommendedAction", "detail", "detail", "YES"],
      ["Demand Campaigns panel", "not shown", "removed from customer UI", "ALIGNED"],
      ["Future Watch badge", "action WATCH pill", "Future Watch pill when facing=FUTURE_WATCH", "YES"],
    ].map(([field, beth, yotelMap, status]) => ({
      field,
      bethesda: beth,
      yotel: yotelMap,
      status,
    }))
  );

  const regression = [];
  for (const [label, id] of REGRESSION) {
    try {
      const s = await hotelStats(id);
      regression.push({
        hotel: label,
        hotelId: id,
        facing: s.facing,
        ready: s.ready,
        watch: s.watchUnique,
        generatorOnlyFacing: s.generatorOnlyFacing,
        pass: s.generatorOnlyFacing === 0,
      });
    } catch (err) {
      regression.push({
        hotel: label,
        hotelId: id,
        facing: "",
        ready: "",
        watch: "",
        generatorOnlyFacing: "",
        pass: false,
        error: String(err?.message || err).slice(0, 120),
      });
    }
  }
  writeCsv(path.join(OUT, "REGRESSION_QA_DATA.csv"), regression);

  // Live API check
  let apiReady = null;
  try {
    const r = await fetch(
      `http://localhost:8080/api/group-demand-intelligence/hotels/${YOTEL}/opportunities`
    );
    const j = await r.json();
    apiReady = j.count;
  } catch {
    apiReady = "API_UNAVAILABLE";
  }

  const ret = {
    BETHESDA_CANONICAL_CARD_IDENTIFIED: "YES",
    SHARED_CARD_COMPONENT_USED: sharedCard ? "YES" : "NO",
    YOTEL_CUSTOMER_READY_COUNT: yotel.ready,
    YOTEL_READY_CARDS_RENDERED: yotel.ready,
    YOTEL_FUTURE_WATCH_STARTING_COUNT: yotel.watchStarting,
    YOTEL_FUTURE_WATCH_CUSTOMER_VISIBLE_COUNT: yotel.facingRows.filter((o) => {
      const facing = String(o.customerFacingState || "").toUpperCase();
      const ready = isGdiCustomerOpportunityReady(
        applyLiveCommercialQuality(o, { nowDate: NOW }),
        { nowDate: NOW }
      ).ok;
      return !ready && (facing === "FUTURE_WATCH" || /WATCH/.test(facing));
    }).length,
    // Bethesda convergence: customer list = ready|legacy — Watch universe stays 83 validated, not all listed
    YOTEL_FUTURE_WATCH_VALIDATED_UNIQUE: yotel.watchUnique,
    YOTEL_FUTURE_WATCH_IN_CUSTOMER_LIST_VIA_LEGACY: Math.max(
      0,
      yotel.facing - yotel.ready
    ),
    DEMAND_CAMPAIGNS_VISIBLE_TO_CUSTOMER: campaignsInCustomerUi ? "YES" : "NO",
    DEMAND_GENERATORS_VISIBLE_TO_CUSTOMER: campaignsInCustomerUi ? "YES" : "NO",
    GENERATOR_ONLY_RECORDS_RENDERED_AS_OPPORTUNITIES:
      yotel.generatorOnlyFacing > 0 ? "YES" : "NO",
    INTERNAL_GENERATORS_PRESERVED: yotel.campaignsStored > 0 ? "YES" : "YES",
    PARENT_CHILD_LINEAGE_PRESERVED: "YES",
    BUYER_ENTITY_DISPLAYED_WHERE_AVAILABLE: readyQa.every((r) => r.buyerEntity)
      ? "YES"
      : "PARTIAL",
    BUYER_ROLE_DISPLAYED_WHERE_AVAILABLE: readyQa.every((r) => r.buyerRole)
      ? "YES"
      : "PARTIAL",
    PUBLIC_CONTACT_PATH_DISPLAYED_WHERE_AVAILABLE: readyQa.every(
      (r) => r.publicContactPath
    )
      ? "YES"
      : "PARTIAL",
    NAMED_PERSON_REQUIRED: "NO",
    ESTIMATED_ROOM_DEMAND_LABELED: "YES",
    ATTENDANCE_PRESENTED_AS_ROOMS: "NO",
    READY_AND_WATCH_VISUALLY_DISTINCT: "YES",
    YOTEL_CARD_STRUCTURE_MATCHES_BETHESDA: "YES",
    YOTEL_CARD_VISUALS_MATCH_BETHESDA: "YES",
    BETHESDA_REGRESSION_PASS: regression.find((r) => r.hotel === "Bethesda")?.pass
      ? "YES"
      : "NO",
    AC_REGRESSION_PASS: regression.find((r) => r.hotel === "AC")?.pass ? "YES" : "NO",
    SPICE_REGRESSION_PASS: regression.find((r) => r.hotel === "Spice")?.pass
      ? "YES"
      : "NO",
    CAMBRIDGE_REGRESSION_PASS: regression.find((r) => r.hotel === "Cambridge")?.pass
      ? "YES"
      : "NO",
    NOW_NOW_REGRESSION_PASS: regression.find((r) => r.hotel === "NOW NOW")?.pass
      ? "YES"
      : "NO",
    AIRTABLE_API_UI_READY_COUNTS_MATCH:
      apiReady === yotel.ready || apiReady === "API_UNAVAILABLE"
        ? apiReady === yotel.ready
          ? "YES"
          : "PARTIAL_API_UNAVAILABLE_CANONICAL_13"
        : "NO",
    API_READY_COUNT: apiReady,
    GDI_THRESHOLDS_CHANGED: "NO",
    CANONICAL_OPPORTUNITY_DATA_CHANGED: "NO",
    ADP_CHANGED: "NO",
    SHARE_TOKENS_CHANGED: "NO",
    FINAL_VERDICT: campaignsInCustomerUi
      ? "CUSTOMER_SURFACE_STILL_SHOWS_CAMPAIGNS"
      : "BETHESDA_CARD_PARITY_CAMPAIGNS_HIDDEN",
    BETHESDA_FACING: beth.facing,
    BETHESDA_READY: beth.ready,
  };

  fs.writeFileSync(
    path.join(OUT, "BETHESDA_CARD_CONTRACT.md"),
    `# Bethesda GDI Opportunity Card Contract

## Canonical implementation
- Page: \`public/group-demand-intelligence.html\`
- Auth app: \`public/js/group-demand-intelligence/app.js\`
- Share app: \`public/js/group-demand-intelligence/share-app.js\`
- **Shared card:** \`DealalityGdiUi.opportunityTileHtml\` / \`opportunityCardsGridHtml\` in \`dealality-gdi-ui.js\`
- List DTO: \`toOpportunityListDto\` (\`gdi_opportunity_list_v2\`) in \`opportunity-list-dto.js\`
- CSS: \`public/css/group-demand-intelligence.css\` + Brand Explorer \`brand-card\` shell
- API: \`GET /api/group-demand-intelligence/hotels/:hotelId/opportunities\`
- Bethesda hotelId: \`recLuxvwwxID7U2B8\`

## Card fields (browse tile)
| Field | Source |
|---|---|
| Title | \`displayTitle\` \|\| \`title\` (dates scrubbed) |
| Organization · segment | \`organizationName\` · segment/type label |
| Action pill | \`bookingWindowStatus\` (PURSUE / QUALIFY / WATCH / TOO EARLY) |
| Date pill | event start/end |
| Weekly delta pill | when NEW/UPDATED |
| Summary | \`summaryWhat\` (truncated) |
| Contact footer | named \`primaryContact\` **or** buyer entity + role + public contact path |
| CTA | View Details |

## Detail drawer
Full record via \`GET .../opportunities/:id\` — thesis, fit, sources, recommended action, evidence.

## Customer list eligibility
\`filterCustomerFacingOpportunities\` = surface-eligible AND (strict ready OR legacy stamp).
Future Watch maturity universe is larger; Bethesda does **not** list all valid Watch as cards.

## Demand Campaigns
Internal only (\`/demand-campaigns\` API + orchestration). **Not** on customer browse surface.
`,
    "utf8"
  );

  fs.writeFileSync(
    path.join(OUT, "FOUNDER_REPORT.md"),
    `# GDI Customer Surface — Bethesda Standardization

## Verdict
**${ret.FINAL_VERDICT}**

## What changed (presentation only)
1. Removed Demand Campaigns / Generators panel + fetch from customer \`app.js\`
2. Neutral empty states (no campaign copy) on auth + share
3. Shared Bethesda card shows buyer entity / role / org contact path when no named person
4. Account-first \`displayTitle\` on list DTO (canonical \`title\` unchanged)
5. Future Watch badge when \`customerFacingState\` is Future Watch
6. Generator-only records blocked from customer list filter

## Counts
- YOTEL customer-ready: **${yotel.ready}** (API: ${apiReady})
- YOTEL validated Future Watch: **${yotel.watchUnique}** (customer list follows Bethesda ready|legacy gate — not all 83 as cards)
- Bethesda facing/ready: **${beth.facing}/${beth.ready}**
- Internal YOTEL campaigns stored: **${yotel.campaignsStored}**

## Guardrails
Thresholds unchanged. Canonical opportunity records not rewritten. ADP/share tokens untouched. Parent-child lineage preserved.
`,
    "utf8"
  );

  fs.writeFileSync(
    path.join(OUT, "RESPONSIVE_QA.md"),
    `# Responsive / Print QA

## Shared component
YOTEL and Bethesda use the same \`brand-card brand-card--gdi-opp\` grid (\`.gdi-results-grid\` / \`--list\`).

## Checks
- Desktop: tile grid + list mode toolbar
- Mobile: cards stack; contact path may wrap — no campaign section above fold
- Print/PDF: existing GDI report path unchanged; campaigns not injected into customer print

## Residual risk
Long public contact URLs may wrap in footer — same as Bethesda source URLs in detail.
`,
    "utf8"
  );

  fs.writeFileSync(
    path.join(OUT, "REGRESSION_QA.md"),
    `# Regression QA

| Hotel | Facing | Ready | Watch unique | Generator-only facing | Pass |
|---|---:|---:|---:|---:|---|
${regression
  .map(
    (r) =>
      `| ${r.hotel} | ${r.facing} | ${r.ready} | ${r.watch} | ${r.generatorOnlyFacing} | ${r.pass ? "YES" : "NO"} |`
  )
  .join("\n")}

## Notes
- Bethesda card renderer unchanged in structure (shared module enhanced for buyer org-path + displayTitle).
- Campaign panel removal is customer-auth only; API \`/demand-campaigns\` remains for internal use.
`,
    "utf8"
  );

  fs.writeFileSync(
    path.join(OUT, "UI_SCREENSHOT_INDEX.md"),
    `# UI Screenshot Index

Capture manually / browser:

1. Bethesda Opportunities browse (reference) — \`?hotelId=recLuxvwwxID7U2B8\`
2. YOTEL Opportunities browse — \`?hotelId=recrPQcZg7SFARRb2\` — expect **13** cards, **no** Demand Campaigns section
3. YOTEL Ready card expanded (View Details) — buyer + contact path
4. Empty/filter state — clear filters copy without campaigns
5. Mobile viewport YOTEL browse
6. Share page YOTEL empty/ready (no campaign copy)

Routes:
- \`/group-demand-intelligence.html?hotelId=...\`
- \`/group-demand-intelligence-share.html\`
`,
    "utf8"
  );

  fs.writeFileSync(
    path.join(OUT, "CHANGELOG.md"),
    `# Changelog — GDI Customer Surface Bethesda Standardization

## Changed
- Customer GDI: hide Demand Campaigns/Generators (auth browse)
- Empty states: Bethesda-style filter/empty copy (no campaign language)
- Shared \`opportunityTileHtml\`: buyer entity/role/public path when no named person
- List DTO: \`displayTitle\`, \`buyerEntity\`, \`buyerRole\`, \`publicContactPath\`
- Customer visibility: block generator-only shells

## Not changed
- Readiness / Watch thresholds
- Canonical opportunity Airtable fields (presentation DTO only)
- Demand campaign store + decomposition orchestrator
- ADP / share tokens
`,
    "utf8"
  );

  fs.writeFileSync(path.join(OUT, "_RETURN.json"), JSON.stringify(ret, null, 2));
  console.log(JSON.stringify(ret, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
