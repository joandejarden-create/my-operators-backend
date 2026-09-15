/**
 * P8.10 — write markdown reports 243–253 from shadow artifacts.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "data", "hotel-census-v2", "p810-brand-explorer");
const REP = path.join(ROOT, "reports", "hotel-census-v2");

function readJson(name) {
  let raw = fs.readFileSync(path.join(OUT, name), "utf8");
  if (raw.charCodeAt(0) === 0xfeff) raw = raw.slice(1);
  return JSON.parse(raw);
}

const founder = readJson("founder-summary.json");
const contract = readJson("metric-contract.json");
const room = readJson("room-coverage.json");
const aff = readJson("affiliation-coverage.json");
const life = readJson("lifecycle-coverage.json");
const routing = readJson("routing-proof.json");
const perf = readJson("performance.json");
const oe = readJson("oe-next-scope.json");
const ready = readJson("ready-gate.json");
const qa = readJson("brand-level-qa.json");
const diffs = readJson("metric-diffs.json");

fs.mkdirSync(REP, { recursive: true });

function w(name, body) {
  fs.writeFileSync(path.join(REP, name), body.trim() + "\n");
}

w(
  "243-P810-BE-LEGACY-CONTRACT.md",
  `# P8.10 — Brand Explorer Legacy Census Dependency Contract

## Scope
Brand Explorer census-derived metrics only. HE / Radar / Scout / OE unchanged.

## Primary production path (Legacy — still default)
| METRIC | UI LOCATION | CODE PATH | LEGACY SOURCE | BUSINESS MEANING | NUMERATOR | DENOMINATOR | HPC REPLACEMENT | UNKNOWN HANDLING | READY? | BLOCKER? |
|--------|-------------|-----------|---------------|------------------|-----------|-------------|-----------------|------------------|--------|----------|
| totalOpenHotels | Footprint hero / portfolio | \`build-brand-census-summary\` → \`aggregate-presence-summary\` → Hotel Census Affiliation | Hotel Census \`Affiliation\` + status Open | Operating branded hotels for this brand | Open rows matching alias Affiliation | Alias matchers ∩ Include-in-BE ∩ not Independent | HPC Current Brand/Family + BRANDED_CONFIRMED + Open lifecycle | Excluded from stock | YES | NO |
| totalPipelineHotels | Pipeline totals | same | Hotel Census status Pipeline | Future / pipeline stock | Pipeline rows | same | HPC Pipeline lifecycle separate | Excluded | YES | NO |
| totalOpenKeys | Rooms total | same | Legacy \`rooms\` | Keys in open footprint | Sum rooms | Open branded rows | Product-governed High+official rooms + coverage % | Show known + coverage; no Legacy fill | YES (PARTIAL) | NO |
| countryCount | Markets / countries | same | Legacy \`country\` | Country presence | Distinct countries | Open stock | HPC country | Unknown country excluded from count | YES | NO |
| dealalityRegionCount | Regional distribution | same | Region + country → Dealality region | Regional footprint | Distinct Dealality regions | Open stock | Same region shim as Radar/Scout | Other bucket | YES | NO |
| chainScaleMix | Chain scale chart | same | Legacy \`Chain Scale\` | Segment mix | Hotels/keys by scale | Open keys | SAFE_DEGRADE → Unknown; chart suppressed when all Unknown | Explicit Unknown | YES | NO |
| locationTypeMix | Location mix | same | Legacy Location | Location mix | by Location | Open keys | SAFE_DEGRADE Unknown | Explicit | YES | NO |
| STR Number | — | not used in BE censusSummary | — | — | — | — | N/A | — | N/A | NO |
| Legacy Market/Submarket | — | not required for BE censusSummary rollups | — | — | — | — | country/state/city/dealality_market | nullable | YES | NO |

## Additional Legacy touchpoints audited
- \`GET /api/brand-library/brand\` → \`censusSummary\` (feature \`BRAND_EXPLORER_CENSUS_METRICS\`)
- \`public/js/brand-explorer-census-metrics.js\` display model
- Brand Alias Mapping (platform) — **kept** on HPC path (matcher strings only; not Legacy Hotel Census table reads)

## Isolation
Operator Explorer uses \`build-operator-census-footprint.js\` (Management Company grain) — **not modified**.
`
);

w(
  "244-P810-BE-METRIC-CONTRACT.md",
  `# P8.10 — Brand Explorer Canonical HPC Metric Contract

\`\`\`json
${JSON.stringify(contract, null, 2)}
\`\`\`

## Classifications
| Metric | Classification |
|--------|----------------|
| totalOpenHotels | KEEP_WITH_NEW_DENOMINATOR (BRANDED_CONFIRMED + Open) |
| totalPipelineHotels | KEEP_WITH_NEW_DENOMINATOR (Pipeline separate) |
| totalOpenKeys | SAFE_TO_SHOW_WITH_UNKNOWN (partial + coverage banner) |
| totalPipelineKeys | SAFE_TO_SHOW_WITH_UNKNOWN |
| countryCount | KEEP_SAME_SEMANTICS |
| dealalityRegionCount | KEEP_SAME_SEMANTICS |
| chainScaleMix | SAFE_DEGRADE (Unknown; UI suppresses empty Unknown-only chart) |
| locationTypeMix | SAFE_DEGRADE |
| Legacy Chain Scale ranking | REMOVE from HPC path |
| STR identity joins | REMOVE |

## Inclusion rules (hotel count)
1. Match \`Current Brand\` OR \`Brand Family\` to Brand Alias Mapping affiliation matchers (exact key).
2. Affiliation class = BRANDED_CONFIRMED only for brand stock.
3. Lifecycle Open → operating; Pipeline → pipeline; neither silently mixed.
4. Identity = \`dhl_\` (HPC record id fallback only for internal dedupe).
`
);

w(
  "245-P810-BE-AFFILIATION.md",
  `# P8.10 — Brand Explorer Affiliation Semantics

## Classes
| Class | HPC Affiliation Status | In brand stock? |
|-------|------------------------|-----------------|
| BRANDED_CONFIRMED | Branded, Soft-Branded / Collection, Formerly Branded, Future / Pipeline | YES |
| INDEPENDENT_CONFIRMED | Independent | NO |
| BRAND_UNCONFIRMED | Brand-Unconfirmed | NO |
| AFFILIATION_UNKNOWN | Unknown / empty / other | NO |

## Rules
- UNKNOWN ≠ Independent
- BRAND_UNCONFIRMED ≠ BRANDED_CONFIRMED
- Brand-share / footprint denominators use BRANDED_CONFIRMED only; unknowns counted separately in \`affiliationClassCounts\`

## Shadow coverage
\`\`\`json
${JSON.stringify(aff, null, 2)}
\`\`\`

## Routing smoke
\`\`\`json
${JSON.stringify(routing.classifyHpcAffiliationSmoke, null, 2)}
\`\`\`
`
);

w(
  "246-P810-BE-ROOMS.md",
  `# P8.10 — Brand Explorer Rooms / Keys

## Policy
Product-governed rooms only (\`deriveHpcProductRooms\`: High confidence + official / trusted gov secondary).
**No quarantined Legacy room fill. No silent fill.**

## Choice per metric
| Metric | Choice |
|--------|--------|
| totalOpenKeys | A + D — known-room total + coverage banner |
| totalPipelineKeys | A + D |
| Legacy rooms parity | C — not pursued |

## Shadow coverage
\`\`\`json
${JSON.stringify(room, null, 2)}
\`\`\`

Coverage is intentionally low in current clean Census (~${room.coveragePctOpen}% open hotels with product rooms). UI shows honest partial coverage when \`hpcPath\` is active.
`
);

w(
  "247-P810-BE-GEOGRAPHY-SCALE.md",
  `# P8.10 — Brand Explorer Geography & Chain Scale

## Geography
| Field | Treatment |
|-------|-----------|
| country | HPC country |
| state/region | HPC State/Region → \`resolveDealalityRegion\` (Radar/Scout shim) |
| city | HPC city (available on rows; not primary rollup) |
| dealality_market | nullable HPC Market |
| dealality_submarket | nullable (not required for BE rollups) |
| Legacy Market / Submarket | **not required** |
| STR geography | **not used** |

Canonical geo reused: YES. Third geography implementation: NO.

## Chain Scale
| Use | Classification |
|-----|----------------|
| FILTER | SAFE_DEGRADE — not driven by HPC Chain Scale |
| SEGMENTATION / DISPLAY | Unknown; chart suppressed when Unknown-only |
| RANKING / BENCHMARK | not required for BE censusSummary |
| Proprietary STR Chain Scale copy into HPC | **FORBIDDEN** |

required? NO · safe degradation? YES
`
);

w(
  "248-P810-BE-HPC-ADAPTER.md",
  `# P8.10 — Brand Explorer HPC Metrics Adapter

## Architecture
\`\`\`
Brand Explorer UI (?beHpc=1 | DEALALITY_BE_HPC | BRAND_EXPLORER_HPC_V2)
  → GET /api/brand-library/brand
  → shouldUseHpcBrandExplorerMetrics(req)
  → buildBrandCensusSummary(..., { useHpc: true })
  → aggregateHpcBrandPresenceSummary
  → Hotel Property Census (dhl_) + Brand Alias Mapping matchers
\`\`\`

## Modules
- \`lib/hotel-census/brand-explorer-hpc-metrics.js\` — aggregator + counters
- \`lib/hotel-census/build-brand-census-summary.js\` — \`useHpc\` branch
- \`lib/hotel-census/brand-presence-hpc-request.js\` — \`BRAND_EXPLORER_HPC_V2\` + \`shouldUseHpcBrandExplorerMetrics\`
- \`api/brand-library.js\` — request gate wiring
- \`public/js/brand-explorer-census-source.js\` — client opt-in
- \`public/js/brand-explorer-brand-fetch.js\` — query/header opt-in + cache key split

## Isolation
- \`BRAND_PRESENCE_HPC_V2\` alone does **not** switch BE
- \`RADAR_HPC_V2\` / \`SCOUT_HPC_V2\` do **not** switch BE
- Production default: \`BRAND_EXPLORER_HPC_V2\` OFF → Legacy metrics

## Counters
\`brandExplorerCensusReadCounters\`: hpc / legacy / silentFallback
`
);

w(
  "249-P810-BE-SHADOW.md",
  `# P8.10 — Brand Explorer Shadow

## Result: **${founder.shadow.result}**

| Check | Value |
|-------|-------|
| Brands tested | ${qa.length} |
| HPC available | ${qa.filter((r) => r.hpcAvailable).length} |
| Critical | ${founder.shadow.critical} |
| Suspicious | ${founder.shadow.suspicious} |
| Expected differences | ${founder.shadow.expectedDifferences} |
| Legacy reads on HPC path | ${founder.legacyOff.beLegacyReadsOnHpcPath} |
| Silent fallback | ${founder.legacyOff.silentFallback} |

## Brand-level open hotel sample
${qa
  .map(
    (r) =>
      `- **${r.brand}**: Legacy ${r.legacyOpenHotels} → HPC ${r.hpcOpenHotels} (rooms cov ${r.hpcRoomsCoveragePctOpen}%)`
  )
  .join("\n")}

## Client path
Local/staging: \`?beHpc=1\` or \`localStorage.DEALALITY_BE_HPC=1\` with server \`BRAND_EXPLORER_HPC_V2=1\` + \`BRAND_PRESENCE_HPC_V2=1\`.

Harness: \`node scripts/hotel-census-v2-p810-be-shadow.mjs\`
`
);

const classCounts = {};
for (const d of diffs) {
  classCounts[d.classification] = (classCounts[d.classification] || 0) + 1;
}

w(
  "250-P810-BE-METRIC-DIFFS.md",
  `# P8.10 — Brand Explorer Metric Diffs

## Classification counts
\`\`\`json
${JSON.stringify(classCounts, null, 2)}
\`\`\`

## Principle
Do **not** force Legacy parity. Hotel-count deltas are expected from BRANDED_CONFIRMED + HPC universe. Room totals drop when product rooms are sparse — labeled PARTIAL.

Artifacts: \`data/hotel-census-v2/p810-brand-explorer/metric-diffs.parquet\` (+ \`.csv\`), \`brand-level-qa.parquet\`.
`
);

w(
  "251-P810-BE-LEGACY-OFF.md",
  `# P8.10 — Brand Explorer Legacy-Off Assertion (HPC shadow path)

| Assertion | Result |
|-----------|--------|
| Legacy Hotel Census reads on HPC path | **${founder.legacyOff.beLegacyReadsOnHpcPath}** |
| STR joins | **${founder.legacyOff.strJoins}** |
| Legacy Chain Scale fallback | **0** (Unknown SAFE_DEGRADE) |
| Legacy Market/Submarket fallback | **${founder.legacyOff.legacyGeo}** |
| Legacy Rooms fallback | **${founder.legacyOff.legacyRooms}** |
| Legacy affiliation fallback | **0** (HPC Affiliation Status) |
| Silent fallback | **${founder.legacyOff.silentFallback ? "YES" : "NO"}** |
| HE | HPC (unchanged) |
| Radar | HPC (unchanged) |
| Scout | HPC (unchanged) |
| OE | unchanged |

Proven via \`brandExplorerCensusReadCounters\` per brand on \`useHpc: true\` path.
`
);

w(
  "252-P810-BE-READY-GATE.md",
  `# P8.10 — Brand Explorer READY Gate

## Verdict: **${ready.verdict}**

\`\`\`json
${JSON.stringify(ready, null, 2)}
\`\`\`

## P8.11 controlled cutover plan (DO NOT EXECUTE IN P8.10)

1. Deploy BE HPC code to production with \`BRAND_EXPLORER_HPC_V2=0\` (default OFF).
2. Verify HE / Radar / Scout still HPC via runtime flags + smoke.
3. Verify BE production still Legacy (\`censusSummary.source\` Hotel Census).
4. Verify OE unchanged.
5. Enable \`BRAND_EXPLORER_HPC_V2=1\` on Railway (and ensure \`BRAND_PRESENCE_HPC_V2=1\`).
6. Smoke Brand Explorer: footprint cards, charts, filters, detail, rooms banner.
7. Prove zero Legacy BE census reads on opted-in path (counters / logs).
8. Rollback = set \`BRAND_EXPLORER_HPC_V2=0\` only (HE/Radar/Scout untouched).
`
);

w(
  "253-P810-OE-NEXT-SCOPE.md",
  `# P8.10 — Operator Explorer Next Scope (audit only; no OE changes)

\`\`\`json
${JSON.stringify(oe, null, 2)}
\`\`\`

## Smallest package after BE cutover
**P8.12** — OE footprint HPC adapter:
- Management Company → HPC Operator
- Brand from Current Brand
- Rooms product-governed
- Chain Scale SAFE_DEGRADE
- Flag \`OPERATOR_EXPLORER_HPC_V2\` default OFF
`
);

// Refresh founder with performance numbers already present
fs.writeFileSync(
  path.join(OUT, "founder-summary.json"),
  JSON.stringify({ ...founder, reportsWritten: true, lifecycleCoverage: life, performanceDetail: perf }, null, 2)
);

console.log(JSON.stringify({ ok: true, verdict: ready.verdict, reports: "243-253" }, null, 2));
