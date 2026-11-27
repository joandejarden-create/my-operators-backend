#!/usr/bin/env node
/**
 * Multi-hotel optional-column readiness report for GDI List View.
 *   node scripts/report-gdi-list-optional-columns-readiness-v1.mjs
 */
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports", "gdi-list-optional-columns-hydration-v1");

function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

function loadModules() {
  const sandbox = {
    console,
    sessionStorage: {
      _m: Object.create(null),
      getItem(k) {
        return Object.prototype.hasOwnProperty.call(this._m, k) ? this._m[k] : null;
      },
      setItem(k, v) {
        this._m[k] = String(v);
      },
      removeItem(k) {
        delete this._m[k];
      },
    },
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  sandbox.document = {
    createElement() {
      return {
        style: {},
        setAttribute() {},
        addEventListener() {},
        querySelector() {
          return null;
        },
      };
    },
    body: { appendChild() {} },
    addEventListener() {},
  };
  vm.runInNewContext(read("public/js/group-demand-intelligence/dealality-gdi-ui.js"), sandbox);
  vm.runInNewContext(
    read("public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js"),
    sandbox,
  );
  return sandbox;
}

function getJson(url) {
  return new Promise((resolve, reject) => {
    http
      .get(url, (res) => {
        let data = "";
        res.on("data", (c) => {
          data += c;
        });
        res.on("end", () => {
          try {
            resolve(JSON.parse(data));
          } catch (err) {
            reject(err);
          }
        });
      })
      .on("error", reject);
  });
}

const hotels = [
  { id: "recnPB3Q02NO8Ma1o", name: "Wyndham San Diego Bayside" },
  { id: "recLuxvwwxID7U2B8", name: "Bethesda Marriott" },
];

const { DealalityGdiCommercialListTable: Table, DealalityGdiUi: UI } = loadModules();
const results = [];

for (const hotel of hotels) {
  const payload = await getJson(
    `http://127.0.0.1:8080/api/group-demand-intelligence/hotels/${hotel.id}/opportunities?preview=staged`,
  );
  const ops = payload.opportunities || [];
  const assess = Table.assessAllOptionalColumns(ops, UI);
  const offered = Table.availableOptionalColumnIds(ops, UI);
  results.push({ hotel, count: ops.length, assess, offered });
}

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "column-readiness.json"), JSON.stringify(results, null, 2));

let md = `# GDI List View — Optional Column Readiness Assessment

Date: 2026-11-27  
Server: http://localhost:8080 (ws-gdi cumulative)  
Cache: gdi-competitive-filter-layout-v1  
Contract: DEALALITY_ADDITIVE_NON_REGRESSION_V1 — List presentation/mapping only

## Cross-hotel decisions

| Column | Canonical data source | Mapping corrections | Decision |
|---|---|---|---|
| Sales Opportunity Type | opportunityType* / salesOpportunityType* | Redundant with Demand Type | **CONSOLIDATED** — removed from Columns; Sales Opportunities filters kept |
| Market Scope | inventoryPopulations / hotelMarketScopeEligible / competitiveMarketScopeEligible / marketScope | Was empty because renderer ignored filter taxonomy | **RETAIN_HYDRATED** when Competitive or scope varies; else **HIDDEN_FILTER_SUFFICES** |
| Contact Readiness | contactActionabilityTier / Label | None | **RETAIN_HYDRATED** |
| Opportunity Status | hotelSourcingStatusLabel → commercialStagePillLabel | Was reading missing status fields | **RETAIN_HYDRATED** |
| Need Period Alignment | needDateIndicator.displayLabel / tileNeedDatePreview / needDateMatchV1.timingOverlapLabel | Was ignoring structured indicator objects | **RETAIN_HYDRATED** when present |
| Estimated Room Nights | estimatedRoomNights / peakRooms / roomNights | No values in audited STAGED packages | **HIDDEN_NO_DATA** |
| Weekly Change | weeklyDeltaStateOf / isNewThisWeek | No weekly delta on audited STAGED packages | **HIDDEN_NO_DATA** |
| Last Verified | lastVerifiedAt / evidenceAsOf | No per-opportunity verification timestamps | **HIDDEN_NO_DATA** |

`;

for (const row of results) {
  md += `## ${row.hotel.name} (\`${row.hotel.id}\`) — ${row.count} opportunities\n\n`;
  md += `| Column | Canonical source | Populated | Coverage % | Decision |\n|---|---|---:|---:|---|\n`;
  md += `| Sales Opportunity Type | duplicates Demand Type | — | — | CONSOLIDATED |\n`;
  for (const a of row.assess) {
    md += `| ${a.label} | ${(a.canonicalSource || "").replace(/\|/g, "/")} | ${a.populated}/${a.total} | ${a.coveragePct} | ${a.decision} |\n`;
  }
  const offeredLabels = row.offered.map((id) => {
    const col = Table.OPTIONAL_COLUMNS.find((c) => c.id === id);
    return col ? col.label : id;
  });
  md += `\n**Columns menu offers:** ${offeredLabels.join(", ") || "(none)"}\n\n`;
}

md += `## Validation

- \`npm run test:gdi-list-optional-columns-hydration-v1\`
- \`npm run test:gdi-list-view-contact-actions-v1\`
- \`npm run test:gdi-sortable-commercial-list-view-v1\`
- Browser List View Columns menu on Wyndham + Bethesda (:8080)

No paid research. No enrichment. No fabricated values.
`;

fs.writeFileSync(path.join(OUT, "COLUMN_READINESS_ASSESSMENT.md"), md);
console.log(md);
console.log("WROTE", OUT);
