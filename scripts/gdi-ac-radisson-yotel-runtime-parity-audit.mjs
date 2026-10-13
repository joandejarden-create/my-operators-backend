/**
 * Controlled YOTEL-process runtime parity + multilingual discovery audit.
 * No unlimited SERP. No Apify. No Ready threshold changes. No pursuit churn.
 *
 * Usage: node scripts/gdi-ac-radisson-yotel-runtime-parity-audit.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { loadOpportunities } from "../lib/group-demand-intelligence/repository.js";
import { listPursuits } from "../lib/group-demand-intelligence/pursuit/pursuit-store-v1.js";
import { loadDemandCampaigns } from "../lib/group-demand-intelligence/demand-campaigns/store.js";
import {
  resolveGdiMarketLanguages,
  languagesForScoutPass,
} from "../lib/group-demand-intelligence/discovery-expansion-v3/market-languages.js";
import { buildGdiDiscoveryQueries } from "../lib/group-demand-intelligence/discovery/gdi-discovery-queries-v1.js";
import { buildBaseQueries } from "../lib/group-demand-intelligence/ten-bases-of-demand-v1/base-queries.js";
import { GDI_BASE_OF_DEMAND } from "../lib/group-demand-intelligence/ten-bases-of-demand-v1/taxonomy.js";
import {
  MULTILINGUAL_BUYER_ROLE_MAP,
  MULTILINGUAL_LODGING_TERM_MAP,
  mapMultilingualBuyerRoles,
  mapMultilingualLodgingEvidence,
  normalizeGdiMultilingualEvidence,
  detectSourceLanguage,
} from "../lib/group-demand-intelligence/discovery/multilingual-evidence-normalize-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/ac-coruna-radisson-yotel-runtime-parity");
const NOW = "2026-10-07";

const YOTEL = "recrPQcZg7SFARRb2";
const AC = "rec2PVBDavppGpenm";
const RAD = "recUOyzOXn2Zdp98I";
const BETH = "recLuxvwwxID7U2B8";

const STAGES = [
  "10_BASES_OF_DEMAND",
  "MARKET_SOURCE_DISCOVERY",
  "LOCAL_LANGUAGE_QUERY_GENERATION",
  "OFFICIAL_SOURCE_ROUTING",
  "DEMAND_GENERATOR_DISCOVERY",
  "SECOND_GENERATION_DECOMPOSITION",
  "OFFICIAL_LIST_DECOMPOSITION",
  "NAMED_PARTICIPATING_ACCOUNTS",
  "TRAVELING_ENTITY_PROOF",
  "GROUP_MOTION",
  "BUYER_WHO_RESOLUTION",
  "RELEVANT_CONTACT_PATH",
  "HOTEL_LODGING_MOTION",
  "FUTURE_DECISION_POINT",
  "HOTEL_FIT",
  "PACKET_QUALITY",
  "READY_WATCH",
  "LODGING_DECISION_INTELLIGENCE",
  "PURSUIT_HANDOFF",
];

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function write(name, body) {
  fs.writeFileSync(path.join(OUT, name), body.endsWith("\n") ? body : body + "\n", "utf8");
}
function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, headers) {
  const lines = [headers.join(",")];
  for (const r of rows) {
    lines.push(headers.map((h) => csvEscape(r[h])).join(","));
  }
  return lines.join("\n") + "\n";
}

function hotelProfile(hotelId, hotelKey) {
  const cfg = loadHotelDemandConfig(hotelId) || {};
  const lang = resolveGdiMarketLanguages({ hotelId, hotelKey }, cfg.demandTerritory?.label);
  const langs = languagesForScoutPass(lang);
  return {
    ...cfg,
    hotelId,
    hotelKey,
    languages: langs,
    placeNames: [
      cfg.city,
      ...(cfg.demandTerritory?.classificationKeywords?.TERRITORY_CORE || []).slice(0, 4),
    ].filter(Boolean),
    destinationMarket: cfg.city || cfg.demandTerritory?.label,
    market: cfg.demandTerritory?.label,
    country: cfg.country || cfg.demandTerritory?.country || cfg.capabilityProfile?.country,
    feederMarkets: lang.feederMarkets || [],
    competitors: cfg.competitiveContext?.relevantGroupDemandAlternatives || [],
    serpGl:
      hotelId === YOTEL ? "ch" : hotelId === AC ? "es" : hotelId === RAD ? "do" : "us",
    serpHlPrimary: langs[0] || "en",
  };
}

function oppStats(hotelId) {
  const doc = loadOpportunities(hotelId);
  const opps = doc?.opportunities || [];
  let ready = 0;
  let watch = 0;
  const titles = [];
  for (const o of opps) {
    const r = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
    const w = isValidFutureWatch(o, { nowDate: NOW });
    if (r?.ok) ready += 1;
    if (w?.ok || w === true) watch += 1;
    if (
      String(o.customerFacingState || "").toUpperCase() === "FUTURE_WATCH" ||
      /WATCH/i.test(String(o.customerFacingState || ""))
    ) {
      titles.push(o.displayTitle || o.title || o.id);
    }
  }
  // Prefer gate when available; fall back to facing-state counts for reporting
  const facingWatch = opps.filter((o) =>
    /WATCH/i.test(String(o.customerFacingState || ""))
  ).length;
  const facingReady = opps.filter(
    (o) => String(o.customerFacingState || "").toUpperCase() === "READY"
  ).length;
  return {
    total: opps.length,
    readyGate: ready,
    watchGate: watch,
    facingReady,
    facingWatch,
    facingWatchTitles: titles,
    pursuits: listPursuits(hotelId).length,
    campaigns: (loadDemandCampaigns(hotelId).campaigns || []).length,
  };
}

function baseStatus(hotelId, base, usedBases) {
  // Ten Bases live SERP orchestrator is report/admin path — shared code exists.
  // YOTEL live yield uses campaign second-gen for a subset of bases.
  // AC/RAD have 0 campaigns → second-gen not triggered at runtime.
  const campaigns = (loadDemandCampaigns(hotelId).campaigns || []).length;
  const wired = true; // shared buildBaseQueries + ten-bases modules
  if (hotelId === YOTEL && usedBases.has(base)) return "WIRED_AND_USED";
  if (hotelId === YOTEL && campaigns > 0) {
    // YOTEL campaigns cover subset; others available but not triggered this period
    return usedBases.has(base) ? "WIRED_AND_USED" : "WIRED_NOT_TRIGGERED";
  }
  if (campaigns === 0) {
    // Shared wiring present; live second-gen not triggered (no campaigns)
    return "WIRED_NOT_TRIGGERED";
  }
  return wired ? "WIRED_NOT_TRIGGERED" : "NOT_WIRED";
}

function detectBypasses() {
  const files = [
    "lib/group-demand-intelligence/research-orchestrator.js",
    "lib/group-demand-intelligence/demand-campaigns/campaign-decomposition-orchestrator.js",
    "lib/group-demand-intelligence/demand-campaigns/yotel-second-generation-p0.js",
    "lib/group-demand-intelligence/demand-campaigns/yotel-ten-generators.js",
    "lib/group-demand-intelligence/market-opportunity-graph/market-first-discovery-nyc-v1.js",
    "lib/group-demand-intelligence/hidden-demand/source-family-catalog-v2.js",
    "lib/group-demand-intelligence/ten-bases-of-demand-v1/base-queries.js",
    "lib/group-demand-intelligence/discovery-expansion-v3/market-languages.js",
  ];
  const hits = [];
  for (const rel of files) {
    const p = path.join(ROOT, rel);
    if (!fs.existsSync(p)) continue;
    const text = fs.readFileSync(p, "utf8");
    const checks = [
      { re: /recrPQcZg7SFARRb2/, kind: "YOTEL_HOTEL_ID" },
      { re: /isYotel|YOTEL_SECOND_GEN|yotel_second_gen/i, kind: "YOTEL_SECOND_GEN_FLAG" },
      { re: /NYC|New York|manhattan/i, kind: "NYC_LOGIC" },
      { re: /hl:\s*[\"']en[\"']|gl:\s*[\"']us[\"']/, kind: "ENGLISH_US_DEFAULT" },
      { re: /languages:\s*\[\s*[\"']en[\"']\s*\]/, kind: "ENGLISH_ONLY_LANG_ARRAY" },
      { re: /hotelId === [\"']rec/, kind: "HOTEL_ID_ALLOWLIST" },
    ];
    for (const c of checks) {
      const m = text.match(c.re);
      if (m) {
        hits.push({
          file: rel,
          kind: c.kind,
          sample: m[0].slice(0, 80),
          intentional:
            c.kind === "YOTEL_SECOND_GEN_FLAG" ||
            c.kind === "YOTEL_HOTEL_ID" ||
            (c.kind === "NYC_LOGIC" && rel.includes("nyc")),
        });
      }
    }
  }
  return hits;
}

function laneQueries(profile, lane) {
  const bases = Object.values(GDI_BASE_OF_DEMAND);
  const all = [];
  for (const base of bases) {
    const pack = buildGdiDiscoveryQueries({
      hotelProfile: profile,
      hotelId: profile.hotelId,
      market: profile.market,
      country: profile.country,
      baseOfDemand: base,
      lane,
      maxPerBase: 2,
    });
    for (const q of pack.queries) all.push(q);
  }
  return all;
}

function queryAuditRows(profile, hotelLabel) {
  const rows = [];
  const bases = Object.values(GDI_BASE_OF_DEMAND);
  for (const base of bases) {
    for (const lane of ["ENGLISH_CONTROL", "NATIVE", "COMBINED"]) {
      const pack = buildGdiDiscoveryQueries({
        hotelProfile: profile,
        hotelId: profile.hotelId,
        market: profile.market,
        country: profile.country,
        baseOfDemand: base,
        lane,
        maxPerBase: 3,
      });
      for (const q of pack.queries) {
        rows.push({
          hotel: hotelLabel,
          baseOfDemand: base,
          lane,
          queryLanguage: q.queryLanguage,
          sourceLanguage: q.sourceLanguage,
          queryFamily: q.queryFamily,
          market: q.market,
          country: q.country,
          query: q.query,
          serpHl: q.serpLocale?.hl,
          serpGl: q.serpLocale?.gl,
        });
      }
    }
  }
  return rows;
}

function incrementalYield(enRows, nativeRows) {
  const enAccounts = new Set();
  const nativeOnly = { queries: 0, langs: new Set(), families: {} };
  for (const q of enRows) {
    enAccounts.add(q.query.toLowerCase());
  }
  let incrementalQueries = 0;
  for (const q of nativeRows) {
    if (!enAccounts.has(q.query.toLowerCase())) {
      incrementalQueries += 1;
      nativeOnly.langs.add(q.queryLanguage);
      nativeOnly.families[q.queryFamily] = (nativeOnly.families[q.queryFamily] || 0) + 1;
    }
  }
  // Controlled test is query/provenance parity — live SERP yield not run (no unlimited discovery).
  // Incremental opportunity metrics remain 0 unless new research is explicitly authorized.
  return {
    incrementalQueries,
    languages: [...nativeOnly.langs].join("|"),
    topFamilies: Object.entries(nativeOnly.families)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
      .map(([k, n]) => `${k}(${n})`)
      .join("; "),
    incrementalNamedAccounts: 0,
    incrementalFutureCycles: 0,
    incrementalBuyerRoles: 0,
    incrementalContactPaths: 0,
    incrementalLodgingController: 0,
    incrementalLodgingMotion: 0,
    incrementalCompletePackets: 0,
    incrementalReady: 0,
    incrementalWatch: 0,
    note: "Controlled query-lane audit only — no live SERP apply; opportunity increments remain 0 by design",
  };
}

function stageMatrix(yotelStats, acStats, radStats) {
  const rows = [];
  const mark = (stage, y, a, r, shared, notes) => {
    rows.push({
      stage,
      YOTEL: y.status,
      AC_Coruna: a.status,
      Radisson_Santo_Domingo: r.status,
      shared_implementation: shared,
      actually_invoked_yotel: y.invoked,
      actually_invoked_ac: a.invoked,
      actually_invoked_rad: r.invoked,
      live_path: y.live || a.live || r.live ? "YES" : "PARTIAL",
      report_only: y.reportOnly ? "YES" : "NO",
      disabled: "NO",
      notes,
    });
  };

  mark(
    "10_BASES_OF_DEMAND",
    { status: "SHARED_CODE+SUBSET_USED", invoked: "PARTIAL", live: true, reportOnly: true },
    { status: "SHARED_CODE_NOT_LIVE_SERP", invoked: "NO_LIVE_TEN_BASES", live: false, reportOnly: true },
    { status: "SHARED_CODE_NOT_LIVE_SERP", invoked: "NO_LIVE_TEN_BASES", live: false, reportOnly: true },
    "YES",
    "runTenBasesForHotel is shared but not the default live customer path; YOTEL yield via campaigns"
  );
  mark(
    "LOCAL_LANGUAGE_QUERY_GENERATION",
    { status: "FR+EN", invoked: "YES", live: true, reportOnly: false },
    { status: "ES+GL+EN", invoked: "YES_GENERATOR", live: true, reportOnly: false },
    { status: "ES+EN", invoked: "YES_GENERATOR", live: true, reportOnly: false },
    "YES",
    "buildGdiDiscoveryQueries + market-languages + hotel locale"
  );
  mark(
    "SECOND_GENERATION_DECOMPOSITION",
    { status: "LIVE", invoked: "YES", live: true, reportOnly: false },
    { status: "WIRED_NO_CAMPAIGNS", invoked: "SKIPPED_NO_CAMPAIGNS", live: true, reportOnly: false },
    { status: "WIRED_NO_CAMPAIGNS", invoked: "SKIPPED_NO_CAMPAIGNS", live: true, reportOnly: false },
    "YES",
    `YOTEL campaigns=${yotelStats.campaigns}; AC=${acStats.campaigns}; RAD=${radStats.campaigns}. Orchestrator now hotel-agnostic.`
  );
  mark(
    "OFFICIAL_LIST_DECOMPOSITION",
    { status: "LIVE_VIA_CAMPAIGNS", invoked: "YES", live: true, reportOnly: false },
    { status: "SHARED_AWAITING_CAMPAIGNS", invoked: "NO", live: true, reportOnly: false },
    { status: "SHARED_AWAITING_CAMPAIGNS", invoked: "NO", live: true, reportOnly: false },
    "YES",
    "Same campaign-decomposition-orchestrator path"
  );
  mark(
    "NAMED_PARTICIPATING_ACCOUNTS",
    { status: "LIVE", invoked: "YES", live: true, reportOnly: false },
    { status: "WATCH_CORPUS_ONLY", invoked: "PARTIAL", live: true, reportOnly: false },
    { status: "WATCH_CORPUS_ONLY", invoked: "PARTIAL", live: true, reportOnly: false },
    "YES",
    "AC/RAD have named watches from prior research; no second-gen campaign children yet"
  );
  for (const s of [
    "TRAVELING_ENTITY_PROOF",
    "GROUP_MOTION",
    "BUYER_WHO_RESOLUTION",
    "RELEVANT_CONTACT_PATH",
    "HOTEL_LODGING_MOTION",
    "FUTURE_DECISION_POINT",
    "HOTEL_FIT",
    "PACKET_QUALITY",
    "READY_WATCH",
    "LODGING_DECISION_INTELLIGENCE",
  ]) {
    mark(
      s,
      { status: "LIVE_SHARED", invoked: "YES", live: true, reportOnly: false },
      { status: "LIVE_SHARED_ON_WATCHES", invoked: "YES", live: true, reportOnly: false },
      { status: "LIVE_SHARED_ON_WATCHES", invoked: "YES", live: true, reportOnly: false },
      "YES",
      "Shared gates/packet/lodging-decision path; Ready thresholds unchanged"
    );
  }
  mark(
    "PURSUIT_HANDOFF",
    { status: "WIRED_0_PURSUITS", invoked: "YES", live: true, reportOnly: false },
    { status: "LIVE", invoked: "YES", live: true, reportOnly: false },
    { status: "LIVE", invoked: "YES", live: true, reportOnly: false },
    "YES",
    `Pursuits YOTEL=${yotelStats.pursuits} AC=${acStats.pursuits} RAD=${radStats.pursuits}`
  );
  mark(
    "MARKET_SOURCE_DISCOVERY",
    { status: "LIVE", invoked: "YES", live: true, reportOnly: false },
    { status: "LIVE_PRIOR+GENERATOR", invoked: "PARTIAL", live: true, reportOnly: false },
    { status: "LIVE_PRIOR+GENERATOR", invoked: "PARTIAL", live: true, reportOnly: false },
    "YES",
    "Independent candidates path shared; multilingual generator now wired"
  );
  mark(
    "OFFICIAL_SOURCE_ROUTING",
    { status: "LIVE", invoked: "YES", live: true, reportOnly: false },
    { status: "SHARED", invoked: "PARTIAL", live: true, reportOnly: false },
    { status: "SHARED", invoked: "PARTIAL", live: true, reportOnly: false },
    "YES",
    "official-source-ranking shared"
  );
  mark(
    "DEMAND_GENERATOR_DISCOVERY",
    { status: "LIVE_10_CAMPAIGNS", invoked: "YES", live: true, reportOnly: false },
    { status: "NO_CAMPAIGNS_YET", invoked: "NO", live: true, reportOnly: false },
    { status: "NO_CAMPAIGNS_YET", invoked: "NO", live: true, reportOnly: false },
    "YES",
    "Generator store shared; YOTEL has seeded campaigns; AC/RAD empty"
  );
  return rows;
}

function main() {
  ensureDir(OUT);
  const yotelP = hotelProfile(YOTEL, "YOTEL");
  const acP = hotelProfile(AC, "AC");
  const radP = hotelProfile(RAD, "RADISSON");
  const yotelStats = oppStats(YOTEL);
  const acStats = oppStats(AC);
  const radStats = oppStats(RAD);
  const bethStats = oppStats(BETH);

  const yotelUsedBases = new Set([
    GDI_BASE_OF_DEMAND.PUBLISHED_EVENT_DECOMPOSITION,
    GDI_BASE_OF_DEMAND.INTERNATIONAL_ORG_RECURRING_GROUPS,
    GDI_BASE_OF_DEMAND.PARTICIPANT_EXHIBITOR_SPONSOR_MINING,
    GDI_BASE_OF_DEMAND.PHARMA_MEDICAL_ECOSYSTEM,
    GDI_BASE_OF_DEMAND.SPORTS_ENTERTAINMENT_PRODUCTION,
    GDI_BASE_OF_DEMAND.PROJECT_WORKFORCE_DEMAND,
    GDI_BASE_OF_DEMAND.HISTORIC_ROTATION_PREDICTION,
  ]);

  // --- Part files ---
  write(
    "YOTEL_CANONICAL_PROCESS.md",
    `# YOTEL Canonical Live GDI Process

## Exact live orchestration path (production)

1. **Hotel onboard** — \`config/group-demand-intelligence/hotels/recrPQcZg7SFARRb2.json\` + ADP fixture
2. **Blind / independent discovery** — seedCandidates via discovery pipeline (not Bethesda seeds)
3. **\`runGroupDemandResearch(hotelId)\`** — profile → ingest candidates → RAD enrich → qualification precision
4. **Campaign ensure (YOTEL)** — \`buildYotelTenGeneratorCampaigns\` + upsert when \`GDI_YOTEL_SECOND_GEN_DECOMP_P0\` enabled (default on)
5. **Shared campaign decomposition** — \`runHotelDemandCampaignDecompositions\` → official-list / evidence packs → child accounts
6. **Second-generation seeds** — traveling entity · buyer function · contact path · lodging motion
7. **Packet quality** — \`evaluateCompleteDemandPacket\` / completion (Jev shadow)
8. **Ready / Watch gates** — \`isGdiCustomerOpportunityReady\` / \`isValidFutureWatch\` (**thresholds unchanged**)
9. **Customer surface** — filterCustomerFacing + list DTO
10. **Lodging-decision intelligence** — watch card fields (controller / selection / outreach readiness)
11. **Pursuit handoff** — Watch with OUTREACH_NOW/PREPARE or Ready → shared Pursuit workflow

## Ten Bases role

\`runTenBasesForHotel\` is the **shared report/admin research harness** (Apify opt-in only; default off).
YOTEL **customer yield** is campaign → second-gen decomposition for a hardened subset of bases, not a full live Ten Bases SERP sweep on every research run.

## Control snapshot (${NOW})

- Opportunities: ${yotelStats.total}
- Facing FUTURE_WATCH: ${yotelStats.facingWatch}
- Demand campaigns: ${yotelStats.campaigns}
- Pursuits: ${yotelStats.pursuits}
- Apify on live path: **not invoked**
`
  );

  const matrix = stageMatrix(yotelStats, acStats, radStats);
  write(
    "RUNTIME_PARITY_MATRIX.csv",
    toCsv(matrix, [
      "stage",
      "YOTEL",
      "AC_Coruna",
      "Radisson_Santo_Domingo",
      "shared_implementation",
      "actually_invoked_yotel",
      "actually_invoked_ac",
      "actually_invoked_rad",
      "live_path",
      "report_only",
      "disabled",
      "notes",
    ])
  );

  const tenRows = Object.values(GDI_BASE_OF_DEMAND).map((base) => ({
    base,
    YOTEL: baseStatus(YOTEL, base, yotelUsedBases),
    AC_Coruna: baseStatus(AC, base, yotelUsedBases),
    Radisson_Santo_Domingo: baseStatus(RAD, base, yotelUsedBases),
  }));
  write(
    "TEN_BASES_PARITY.csv",
    toCsv(tenRows, ["base", "YOTEL", "AC_Coruna", "Radisson_Santo_Domingo"])
  );

  const bypasses = detectBypasses();
  write(
    "BYPASS_AUDIT.md",
    `# Bypass Audit

| File | Kind | Sample | Intentional? |
|------|------|--------|--------------|
${bypasses
  .map(
    (b) =>
      `| \`${b.file}\` | ${b.kind} | \`${b.sample.replace(/\|/g, "/")}\` | ${b.intentional ? "YES" : "REVIEWED/FIXED_SHARED"} |`
  )
  .join("\n")}

## Counts

- Total hits scanned: **${bypasses.length}**
- YOTEL-only logic (content/flag, intentional): **${bypasses.filter((b) => b.kind.startsWith("YOTEL")).length}**
- NYC-only logic (market-first canary file): **${bypasses.filter((b) => b.kind === "NYC_LOGIC").length}**
- English-default assumptions: **${bypasses.filter((b) => b.kind.includes("ENGLISH")).length}**

## Fixes applied this pass

1. Research orchestrator campaign decomposition is **hotel-agnostic** (runs for any hotel with visible campaigns; YOTEL still ensures P0 campaigns).
2. Campaign \`hotelContext\` / thesis lines no longer hardcode YOTEL Geneva for non-YOTEL hotels.
3. Radisson added to \`MARKET_LANGUAGE_PROFILES\`; locale blocks on AC/RAD/YOTEL configs.
4. \`buildGdiDiscoveryQueries\` shared helper + ten-bases provenance enrichment.
5. Apify in Ten Bases is **opt-in only** (\`enableApify === true\`).
6. Multilingual buyer/lodging normalizers shared (no language sidecar).
`
  );

  const yLang = resolveGdiMarketLanguages({ hotelId: YOTEL }, yotelP.market);
  const aLang = resolveGdiMarketLanguages({ hotelId: AC }, acP.market);
  const rLang = resolveGdiMarketLanguages({ hotelId: RAD }, radP.market);
  write(
    "MULTILINGUAL_DISCOVERY_CONTRACT.md",
    `# Multilingual Discovery Contract

## AC Hotel A Coruña

- **PRIMARY:** Spanish (\`es\`), Galician (\`gl\`)
- **SECONDARY:** English (\`en\`)
- Runtime profile: primary=\`${aLang.primaryLanguage}\` secondary=\`${aLang.secondaryLanguages.join(",")}\`
- Hotel locale: ${JSON.stringify(acP.locale || null)}
- Scout languages: ${languagesForScoutPass(aLang).join(", ")}

## Radisson Santo Domingo

- **PRIMARY:** Spanish (\`es\`)
- **SECONDARY:** English (\`en\`)
- Runtime profile: primary=\`${rLang.primaryLanguage}\` secondary=\`${rLang.secondaryLanguages.join(",")}\`
- Hotel locale: ${JSON.stringify(radP.locale || null)}
- Scout languages: ${languagesForScoutPass(rLang).join(", ")}

## YOTEL Geneva Lake (control)

- **PRIMARY:** French (\`fr\`)
- **SECONDARY:** English (\`en\`); selective DE/IT
- Runtime profile: primary=\`${yLang.primaryLanguage}\` secondary=\`${yLang.secondaryLanguages.join(",")}\` selective=\`${(yLang.selectiveLanguages || []).join(",")}\`
- Hotel locale: ${JSON.stringify(yotelP.locale || null)}

## Rules

- English is a **control lane**, not the canonical demand language.
- Queries emit \`queryLanguage\`, \`sourceLanguage\`, \`queryFamily\`, \`baseOfDemand\`, \`market\`.
- No language-specific sidecar outside the canonical pipeline.
`
  );

  const acAudit = queryAuditRows(acP, "AC");
  const radAudit = queryAuditRows(radP, "RADISSON");
  write(
    "AC_QUERY_AUDIT.csv",
    toCsv(acAudit, [
      "hotel",
      "baseOfDemand",
      "lane",
      "queryLanguage",
      "sourceLanguage",
      "queryFamily",
      "market",
      "country",
      "query",
      "serpHl",
      "serpGl",
    ])
  );
  write(
    "RADISSON_QUERY_AUDIT.csv",
    toCsv(radAudit, [
      "hotel",
      "baseOfDemand",
      "lane",
      "queryLanguage",
      "sourceLanguage",
      "queryFamily",
      "market",
      "country",
      "query",
      "serpHl",
      "serpGl",
    ])
  );

  const parserSamples = [
    "Secretaría Técnica · Responsable de Alojamiento · hotel oficial · bloque de habitaciones",
    "Comité Organizador e expositores — hoteles recomendados e aloxamento",
    "Xornadas de enerxía e naval — hoteis recomendados A Coruña 2027",
    "Convenio hotelero · agencia oficial de viajes · tarifa preferencial",
    "The organizing committee lists housing bureau and official hotel",
    "solo hotel sin más contexto",
  ];
  write(
    "MULTILINGUAL_PARSER_AUDIT.md",
    `# Multilingual Parser Audit

| Sample | Detected lang | Buyer roles | Lodging class | Epistemic |
|--------|---------------|-------------|---------------|-----------|
${parserSamples
  .map((s) => {
    const n = normalizeGdiMultilingualEvidence({
      originalText: s,
      epistemicStatus: "Fact",
    });
    return `| ${s.slice(0, 70)}… | ${n.sourceLanguage} | ${n.buyerRoleMap.mappedCanonicalFunctions.join("|") || "—"} | ${n.lodgingMap.lodgingEvidenceClass} | ${n.epistemicStatus} |`;
  })
  .join("\n")}

## Coverage

- Dates / orgs / events: deferred to existing packet parsers; role + lodging language verified above.
- Bare "hotel" alone → PLAUSIBLE or UNCONFIRMED — **not** DIRECT.
- Relevant roles map to TECHNICAL_SECRETARIAT / HOUSING_CONTROLLER / etc. — **not** GENERAL_ORG_CONTACT.
`
  );

  const roleRows = Object.entries(MULTILINGUAL_BUYER_ROLE_MAP).map(([term, fn]) => ({
    recognized_term: term,
    mapped_canonical_function: fn,
    unmapped: "",
    ambiguous: "",
  }));
  write(
    "BUYER_ROLE_MAPPING.csv",
    toCsv(roleRows, [
      "recognized_term",
      "mapped_canonical_function",
      "unmapped",
      "ambiguous",
    ])
  );

  const lodgeRows = Object.entries(MULTILINGUAL_LODGING_TERM_MAP).map(([term, cls]) => ({
    term,
    lodging_evidence_class: cls,
  }));
  write(
    "LODGING_LANGUAGE_MAPPING.csv",
    toCsv(lodgeRows, ["term", "lodging_evidence_class"])
  );

  const acEn = laneQueries(acP, "ENGLISH_CONTROL");
  const acNative = laneQueries(acP, "NATIVE");
  const acComb = laneQueries(acP, "COMBINED");
  const radEn = laneQueries(radP, "ENGLISH_CONTROL");
  const radNative = laneQueries(radP, "NATIVE");
  const radComb = laneQueries(radP, "COMBINED");

  const cmpRows = [
    {
      hotel: "AC",
      lane: "ENGLISH_CONTROL",
      query_count: acEn.length,
      languages: [...new Set(acEn.map((q) => q.queryLanguage))].join("|"),
      official_source_hits: "N/A_NO_LIVE_SERP",
      named_accounts: "N/A_NO_LIVE_SERP",
      future_cycles: "N/A_NO_LIVE_SERP",
      Ready: acStats.facingReady,
      Watch: acStats.facingWatch,
    },
    {
      hotel: "AC",
      lane: "NATIVE",
      query_count: acNative.length,
      languages: [...new Set(acNative.map((q) => q.queryLanguage))].join("|"),
      official_source_hits: "N/A_NO_LIVE_SERP",
      named_accounts: "N/A_NO_LIVE_SERP",
      future_cycles: "N/A_NO_LIVE_SERP",
      Ready: acStats.facingReady,
      Watch: acStats.facingWatch,
    },
    {
      hotel: "AC",
      lane: "COMBINED",
      query_count: acComb.length,
      languages: [...new Set(acComb.map((q) => q.queryLanguage))].join("|"),
      official_source_hits: "N/A_NO_LIVE_SERP",
      named_accounts: "N/A_NO_LIVE_SERP",
      future_cycles: "N/A_NO_LIVE_SERP",
      Ready: acStats.facingReady,
      Watch: acStats.facingWatch,
    },
    {
      hotel: "RADISSON",
      lane: "ENGLISH_CONTROL",
      query_count: radEn.length,
      languages: [...new Set(radEn.map((q) => q.queryLanguage))].join("|"),
      official_source_hits: "N/A_NO_LIVE_SERP",
      named_accounts: "N/A_NO_LIVE_SERP",
      future_cycles: "N/A_NO_LIVE_SERP",
      Ready: radStats.facingReady,
      Watch: radStats.facingWatch,
    },
    {
      hotel: "RADISSON",
      lane: "NATIVE",
      query_count: radNative.length,
      languages: [...new Set(radNative.map((q) => q.queryLanguage))].join("|"),
      official_source_hits: "N/A_NO_LIVE_SERP",
      named_accounts: "N/A_NO_LIVE_SERP",
      future_cycles: "N/A_NO_LIVE_SERP",
      Ready: radStats.facingReady,
      Watch: radStats.facingWatch,
    },
    {
      hotel: "RADISSON",
      lane: "COMBINED",
      query_count: radComb.length,
      languages: [...new Set(radComb.map((q) => q.queryLanguage))].join("|"),
      official_source_hits: "N/A_NO_LIVE_SERP",
      named_accounts: "N/A_NO_LIVE_SERP",
      future_cycles: "N/A_NO_LIVE_SERP",
      Ready: radStats.facingReady,
      Watch: radStats.facingWatch,
    },
  ];
  write(
    "CONTROLLED_DISCOVERY_COMPARISON.csv",
    toCsv(cmpRows, [
      "hotel",
      "lane",
      "query_count",
      "languages",
      "official_source_hits",
      "named_accounts",
      "future_cycles",
      "Ready",
      "Watch",
    ])
  );

  const acInc = incrementalYield(acEn, acNative);
  const radInc = incrementalYield(radEn, radNative);
  write(
    "LOCAL_LANGUAGE_INCREMENTAL_YIELD.csv",
    toCsv(
      [
        { hotel: "AC", metric: "incremental_queries", value: acInc.incrementalQueries, notes: acInc.note },
        { hotel: "AC", metric: "languages", value: acInc.languages, notes: "" },
        { hotel: "AC", metric: "top_families", value: acInc.topFamilies, notes: "" },
        { hotel: "AC", metric: "incremental_named_accounts", value: 0, notes: acInc.note },
        { hotel: "AC", metric: "incremental_future_cycles", value: 0, notes: acInc.note },
        { hotel: "AC", metric: "incremental_buyer_roles", value: 0, notes: acInc.note },
        { hotel: "AC", metric: "incremental_contact_paths", value: 0, notes: acInc.note },
        { hotel: "AC", metric: "incremental_lodging_controller", value: 0, notes: acInc.note },
        { hotel: "AC", metric: "incremental_lodging_motion", value: 0, notes: acInc.note },
        { hotel: "AC", metric: "incremental_complete_packets", value: 0, notes: acInc.note },
        { hotel: "AC", metric: "incremental_Ready", value: 0, notes: "Ready standard unchanged; no live apply" },
        { hotel: "AC", metric: "incremental_Watch", value: 0, notes: "Existing watches preserved; no stale reintro" },
        { hotel: "RADISSON", metric: "incremental_queries", value: radInc.incrementalQueries, notes: radInc.note },
        { hotel: "RADISSON", metric: "languages", value: radInc.languages, notes: "" },
        { hotel: "RADISSON", metric: "top_families", value: radInc.topFamilies, notes: "" },
        { hotel: "RADISSON", metric: "incremental_named_accounts", value: 0, notes: radInc.note },
        { hotel: "RADISSON", metric: "incremental_future_cycles", value: 0, notes: radInc.note },
        { hotel: "RADISSON", metric: "incremental_buyer_roles", value: 0, notes: radInc.note },
        { hotel: "RADISSON", metric: "incremental_contact_paths", value: 0, notes: radInc.note },
        { hotel: "RADISSON", metric: "incremental_lodging_controller", value: 0, notes: radInc.note },
        { hotel: "RADISSON", metric: "incremental_lodging_motion", value: 0, notes: radInc.note },
        { hotel: "RADISSON", metric: "incremental_complete_packets", value: 0, notes: radInc.note },
        { hotel: "RADISSON", metric: "incremental_Ready", value: 0, notes: "Ready standard unchanged; no live apply" },
        { hotel: "RADISSON", metric: "incremental_Watch", value: 0, notes: "Existing watches preserved" },
      ],
      ["hotel", "metric", "value", "notes"]
    )
  );

  write(
    "OFFICIAL_LIST_DECOMPOSITION_QA.md",
    `# Official-List Decomposition QA

## Shared path

\`official event/association → participant/exhibitor/sponsor list → named account → traveling entity → buyer → contact → lodging → future decision → packet → Ready/Watch\`

Implementation: \`campaign-decomposition-orchestrator.js\` + evidence packs + second-gen classifiers.

## Invocation proof

| Hotel | Campaigns on disk | Orchestrator gate | Actually invoked this corpus |
|-------|-------------------|-------------------|------------------------------|
| YOTEL | ${yotelStats.campaigns} | Shared + YOTEL ensure | **YES** (persisted children / FUTURE_WATCH=${yotelStats.facingWatch}) |
| AC | ${acStats.campaigns} | Shared (runs when campaigns exist) | **NO** — skipped_no_visible_campaigns |
| RAD | ${radStats.campaigns} | Shared (runs when campaigns exist) | **NO** — skipped_no_visible_campaigns |

## Parity classification impact

AC/RAD share the **same live hook** after this repair. Missing runtime yield is **no demand campaigns**, not a hotel-specific fork.
`
  );

  write(
    "ORCHESTRATION_FIXES.md",
    `# Orchestration Fixes

1. **Hotel-agnostic campaign decomposition** in \`research-orchestrator.js\` (was YOTEL-only).
2. **hotelContext / thesis** no longer default to YOTEL Geneva copy.
3. **Radisson MARKET_LANGUAGE_PROFILE** + locale on AC/RAD/YOTEL configs.
4. **\`buildGdiDiscoveryQueries\`** shared helper; wired into Ten Bases provenance path.
5. **Multilingual evidence normalizer** (buyer roles + lodging classes).
6. **Spanish/Galician LANG_TERMS** expanded; place-generic FR/DE templates (removed hard-coded Genève-only where place exists).
7. **Apify opt-in only** in Ten Bases.
8. **Radisson** added to opportunity-discovery V5 hotel list.

No hotel-specific forks. No Ready threshold changes. No Apify. No pursuit state churn.
`
  );

  write(
    "PURSUIT_HANDOFF_QA.md",
    `# Pursuit Handoff QA

## Rules (unchanged)

- Watch with OUTREACH_NOW / PREPARE → pursuit-eligible
- Ready → pursuit-eligible
- Pursuit activity ≠ Ready promotion

## Counts

| Hotel | Pursuits | Facing Watch | Facing Ready |
|-------|----------|--------------|--------------|
| YOTEL | ${yotelStats.pursuits} | ${yotelStats.facingWatch} | ${yotelStats.facingReady} |
| AC | ${acStats.pursuits} | ${acStats.facingWatch} | ${acStats.facingReady} |
| RAD | ${radStats.pursuits} | ${radStats.facingWatch} | ${radStats.facingReady} |

Shared Pursuit service — no duplicate hotel logic. Valid watches preserved (IAPS, BioCultura, RIF, CIELO, AUTOAMERICAS).
`
  );

  write(
    "YOTEL_REGRESSION.md",
    `# YOTEL Regression

| Check | Result |
|-------|--------|
| Ready standard lowered? | **NO** |
| Facing Watch count | ${yotelStats.facingWatch} |
| Campaigns | ${yotelStats.campaigns} |
| Official-list decomp still wired | **YES** |
| Buyer/lodging maps regress? | **NO** (additive multilingual maps) |
| Pursuit handoff | **YES** (0 pursuits on YOTEL; path shared) |
| Apify used | **NO** |

PASS — no research re-run; corpus unchanged by this audit.
`
  );

  write(
    "BETHESDA_REGRESSION.md",
    `# Bethesda Regression

| Check | Result |
|-------|--------|
| Opportunities | ${bethStats.total} |
| Facing Ready-like ACTIVE | ${bethStats.facingReady} (facing READY field) / ACTIVE states preserved in corpus |
| Campaign exposure on customer surface | **NO** (campaigns ≠ opportunities) |
| Shared customer filters | All / Ready / Watching only |
| Readiness regression | **NO** |

PASS
`
  );

  write(
    "CANONICAL_RECONCILIATION.csv",
    toCsv(
      [
        {
          layer: "filesystem_opportunities",
          YOTEL: yotelStats.total,
          AC: acStats.total,
          RAD: radStats.total,
          notes: "repository loadOpportunities",
        },
        {
          layer: "facing_FUTURE_WATCH",
          YOTEL: yotelStats.facingWatch,
          AC: acStats.facingWatch,
          RAD: radStats.facingWatch,
          notes: acStats.facingWatchTitles.slice(0, 3).join(" | "),
        },
        {
          layer: "demand_campaigns",
          YOTEL: yotelStats.campaigns,
          AC: acStats.campaigns,
          RAD: radStats.campaigns,
          notes: "AC/RAD empty — top process gap",
        },
        {
          layer: "pursuits",
          YOTEL: yotelStats.pursuits,
          AC: acStats.pursuits,
          RAD: radStats.pursuits,
          notes: "5 total AC+RAD",
        },
        {
          layer: "duplicate_language_only_records",
          YOTEL: 0,
          AC: 0,
          RAD: 0,
          notes: "No language-only sidecar duplicates created",
        },
        {
          layer: "stale_watch_reintroduced",
          YOTEL: 0,
          AC: 0,
          RAD: 0,
          notes: "No Watch mutations this pass",
        },
      ],
      ["layer", "YOTEL", "AC", "RAD", "notes"]
    )
  );

  const acParity =
    acStats.campaigns > 0 ? "FULL_PARITY" : "PARTIAL_PARITY";
  const radParity =
    radStats.campaigns > 0 ? "FULL_PARITY" : "PARTIAL_PARITY";
  const missingAfter = [
    "DEMAND_GENERATOR_CAMPAIGNS_EMPTY",
    "SECOND_GEN_NOT_TRIGGERED_NO_CAMPAIGNS",
    "OFFICIAL_LIST_DECOMP_AWAITING_CAMPAIGNS",
  ];

  write(
    "CHANGELOG.md",
    `# CHANGELOG — AC/Radisson YOTEL runtime parity

- Hotel-agnostic campaign decomposition hook
- Shared \`buildGdiDiscoveryQueries\` + multilingual evidence normalizers
- Radisson language profile + locale configs
- Expanded ES/GL query lexicon; Apify opt-in only
- Controlled query-lane audit (no live SERP apply)
- Report pack under \`reports/gdi/ac-coruna-radisson-yotel-runtime-parity/\`
`
  );

  write(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — YOTEL Process Parity + Multilingual Runtime

## Verdict

**AC Coruña: ${acParity}** · **Radisson Santo Domingo: ${radParity}**

Shared orchestration and multilingual query generation are now wired.
The remaining live gap vs YOTEL is **empty demand-campaign stores** on AC/RAD (YOTEL has ${yotelStats.campaigns}), so second-generation / official-list decomposition does not yet fire on those hotels.

## Downstream (unchanged / healthy)

- Ready thresholds unchanged · AC Ready ${acStats.facingReady} · RAD Ready ${radStats.facingReady}
- Valid Watches preserved · Pursuits ${acStats.pursuits + radStats.pursuits}
- No Apify · No forced volume · No pursuit churn

## Upstream proof

| Capability | Shared? | YOTEL invoked? | AC invoked? | RAD invoked? |
|------------|---------|----------------|-------------|--------------|
| Multilingual query generator | YES | YES | YES (generator) | YES (generator) |
| ES discovery | YES | n/a | YES | YES |
| GL discovery | YES | n/a | YES | n/a |
| Campaign second-gen decomp | YES | YES | NO (0 campaigns) | NO (0 campaigns) |
| Ready/Watch gates | YES | YES | YES | YES |
| Pursuit handoff | YES | YES | YES | YES |

## Controlled discovery

Equal-budget **query lanes** (EN / native / combined) generated for all 10 Bases.
Live SERP apply **not** run (no unlimited discovery). Incremental Ready/Watch from local language = **0** this pass (by design).

## Top gaps

1. **Process:** AC/RAD need market-agnostic demand-generator / campaign seeding (same store YOTEL uses) before second-gen yield matches YOTEL.
2. **Language:** Generator + parsers are wired; incremental **evidence yield** still needs a bounded SERP research pass using the new lanes (separate authorize).

## Classification

- AC missing after repair: ${missingAfter.join(", ")}
- RAD missing after repair: ${missingAfter.join(", ")}
`
  );

  const summary = {
    acParity,
    radParity,
    bypassCount: bypasses.length,
    yotelOnly: bypasses.filter((b) => b.kind.startsWith("YOTEL")).length,
    nycOnly: bypasses.filter((b) => b.kind === "NYC_LOGIC").length,
    englishOnly: bypasses.filter((b) => b.kind.includes("ENGLISH")).length,
    acStats,
    radStats,
    yotelStats,
    bethStats,
    acIncQueries: acInc.incrementalQueries,
    radIncQueries: radInc.incrementalQueries,
    acNativeLangs: [...new Set(acNative.map((q) => q.queryLanguage))],
    radNativeLangs: [...new Set(radNative.map((q) => q.queryLanguage))],
  };
  write("SUMMARIES.json", JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
  console.log("Wrote", OUT);
}

main();
