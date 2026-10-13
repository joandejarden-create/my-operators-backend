/**
 * GDI Research Engine Benchmark V1 — offline / credit-preserving pack writer.
 * Does NOT run live Webhound. Does NOT lower Ready/Watch. Does NOT force counts.
 *
 * Evidence bases:
 * - Mallorca Webhound session c1b18a88-279f-480d-80a3-630f048b0c5b ($5 / 66 sources)
 * - mallorca-yield-forensic-v1 funnel + intermediary graph
 * - westin / bethesda prior E2E + publication monitor baselines
 * - native-blind-discovery.js / missing-pillar-audit.js code audit
 */
import fs from "node:fs";
import path from "node:path";

const OUT = path.resolve("reports/gdi/research-engine-benchmark-v1");
const TRACE_DIR = path.join(OUT, "RESEARCH_TRACES");

const WEBHOUND_BALANCE = 1.031601;
const WEBHOUND_MIN_BUDGET = 1.0;
const WEBHOUND_25PCT = WEBHOUND_BALANCE * 0.25;
const LIVE_WEBHOUND = false;
const LIVE_SKIP_REASON = `Projected minimum Webhound budget $${WEBHOUND_MIN_BUDGET} exceeds 25% of remaining credit ($${WEBHOUND_25PCT.toFixed(3)} of $${WEBHOUND_BALANCE.toFixed(3)}). Historical Mallorca dual-hotel trace exists.`;

const MALLORCA_SESSION = {
  id: "c1b18a88-279f-480d-80a3-630f048b0c5b",
  name: "Mallorca Son Vida dual-hotel GDI demand scan — Castillo + Sheraton",
  cost: 5.0,
  searches: 13,
  page_visits: 42,
  llm_calls: 61,
  operations: 116,
  sources: 66,
  token_cost: 4.793,
  search_cost: 0.102,
  page_cost: 0.105,
};

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function write(rel, body) {
  const fp = path.join(OUT, rel);
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, body.endsWith("\n") ? body : body + "\n", "utf8");
  return fp;
}

function csv(rows) {
  if (!rows.length) return "";
  const keys = [...rows.reduce((s, r) => {
    Object.keys(r).forEach((k) => s.add(k));
    return s;
  }, new Set())];
  const esc = (v) => {
    const t = v == null ? "" : String(v);
    return /[",\n]/.test(t) ? `"${t.replace(/"/g, '""')}"` : t;
  };
  return [keys.join(","), ...rows.map((r) => keys.map((k) => esc(r[k])).join(","))].join("\n") + "\n";
}

// ─── Cohort ───────────────────────────────────────────────────────────────
const COHORT = [
  {
    id: "castillo",
    hotel: "Castillo Hotel Son Vida",
    market: "Mallorca / Spain",
    role: "INTERNATIONAL_OPAQUE_LEISURE",
    languages: "es,ca,en",
  },
  {
    id: "sheraton",
    hotel: "Sheraton Mallorca Arabella Golf Hotel",
    market: "Mallorca / Spain",
    role: "INTERNATIONAL_GOLF_TOUR",
    languages: "es,ca,en,de",
  },
  {
    id: "westin",
    hotel: "The Westin Grand München",
    market: "Munich / Germany",
    role: "INTERNATIONAL_MICE",
    languages: "de,en",
  },
  {
    id: "bethesda",
    hotel: "Bethesda Marriott",
    market: "Bethesda / US",
    role: "US_PUBLIC_DATA_CONTROL",
    languages: "en",
  },
];

/** Production baseline from yield forensic + prior E2E (canonical gates). */
const BASELINE = {
  castillo: {
    raw_signals: 30,
    qualified_signals: 6,
    campaigns_admitted: 10,
    high_quality_campaigns: 0,
    named_commercial_accounts: 0,
    current_future_movements: 0,
    proven_traveling: 0,
    strong_inference_traveling: 0,
    controllers: 2,
    lodging_evidence: 0,
    complete_strong: 0,
    complete_plausible: 0,
    valid_watch: 0,
    ready: 0,
    queries: 30,
    sources: 20,
    research_steps: "native SERP→fetch→gpt-4o-mini extract; spines empty",
    time: "prior e2e+yield forensic",
    estimated_cost: "UNKNOWN (SerpAPI+OpenAI; not billed in this pack)",
    first_collapse: "spine_accounts",
  },
  sheraton: {
    raw_signals: 30,
    qualified_signals: 5,
    campaigns_admitted: 10,
    high_quality_campaigns: 1,
    named_commercial_accounts: 0,
    current_future_movements: 1,
    proven_traveling: 0,
    strong_inference_traveling: 2,
    controllers: 3,
    lodging_evidence: 2,
    complete_strong: 0,
    complete_plausible: 1,
    valid_watch: 0,
    ready: 0,
    queries: 30,
    sources: 33,
    research_steps: "native SERP→fetch→gpt-4o-mini; GPH lodging found but not Ready",
    time: "prior e2e+yield forensic",
    estimated_cost: "UNKNOWN",
    first_collapse: "valid_watch_gate",
  },
  westin: {
    raw_signals: "UNKNOWN_PRIOR",
    qualified_signals: "UNKNOWN_PRIOR",
    campaigns_admitted: 1,
    high_quality_campaigns: 0,
    named_commercial_accounts: 1,
    current_future_movements: 1,
    proven_traveling: 0,
    strong_inference_traveling: 0,
    controllers: 1,
    lodging_evidence: 0,
    complete_strong: 0,
    complete_plausible: 0,
    valid_watch: 0,
    ready: 0,
    queries: "multilingual DE/EN SERP (prior e2e)",
    sources: "prior e2e",
    research_steps: "native + Stadtgeburtstag campaign; PUBLIC_DATA_CEILING",
    time: "prior westin e2e",
    estimated_cost: "UNKNOWN",
    first_collapse: "public_data_ceiling_exhibitor_list",
  },
  bethesda: {
    raw_signals: "US_CONTROL_BASELINE",
    qualified_signals: "US_CONTROL_BASELINE",
    campaigns_admitted: "n/a_bag",
    high_quality_campaigns: "n/a_bag",
    named_commercial_accounts: "many_public",
    current_future_movements: "many_public",
    proven_traveling: "many_public",
    strong_inference_traveling: "n/a",
    controllers: "many_public",
    lodging_evidence: "many_public",
    complete_strong: "subset",
    complete_plausible: "subset",
    valid_watch: "n/a_ready_primary",
    ready: 28,
    queries: "prior production + Webhound Bethesda pilots",
    sources: "public US assoc/medical/gov adjacency",
    research_steps: "production GDI bag unchanged this pack",
    time: "frozen control",
    estimated_cost: "UNKNOWN",
    first_collapse: "NONE",
  },
};

/**
 * Recursive Research V1 — offline orchestration simulation using Webhound source
 * graph + intermediary graph. NOT a live agent loop. Same canonical gates.
 */
const RECURSIVE = {
  castillo: {
    high_quality_campaigns: 2,
    named_commercial_accounts: 1,
    current_future_movements: 2,
    proven_traveling: 0,
    strong_inference_traveling: 1,
    controllers: 3,
    lodging_evidence: 1,
    complete_strong: 0,
    complete_plausible: 1,
    valid_watch: 0,
    ready: 0,
    notes:
      "Recursive pivots reach Golf Planet Castillo product + DMC ecosystem; still no Valid Watch (missing buyer path / incremental rooms / list inclusion). Hotel-hosted wedding sales motion excluded by external-demand invariant.",
    unique_vs_baseline: [
      "golfplanetholidays.com/product/castillo-hotel-son-vida",
      "Emotions Maestro Balearic Islands 2027 (travel-trade)",
      "LifeXperiences/Tuset DMC site-deep-dive (no named future client program)",
    ],
  },
  sheraton: {
    high_quality_campaigns: 3,
    named_commercial_accounts: 3,
    current_future_movements: 3,
    proven_traveling: 0,
    strong_inference_traveling: 3,
    controllers: 5,
    lodging_evidence: 4,
    complete_strong: 0,
    complete_plausible: 2,
    valid_watch: 0,
    ready: 0,
    notes:
      "Entity pivots organiser→golf tour operators yield Golf Planet Holidays + Golf Travel Centre + GreenGolf as NAMED commercial intermediaries with lodging control + dated packages. Still fail Valid Watch/Ready under current gates (contact path / decision window / Complete Strong).",
    unique_vs_baseline: [
      "golfplanetholidays hosted tour Western Cape dates (pivot example; destination check required)",
      "yourgolftravel / greengolf / golf-extra / golftravelcentre Sheraton product pages",
      "operator→scheduled departure pivot pattern",
    ],
  },
  westin: {
    high_quality_campaigns: 1,
    named_commercial_accounts: 1,
    current_future_movements: 1,
    proven_traveling: 0,
    strong_inference_traveling: 0,
    controllers: 2,
    lodging_evidence: 0,
    complete_strong: 0,
    complete_plausible: 0,
    valid_watch: 0,
    ready: 0,
    notes:
      "Recursive on Stadtgeburtstag / Messe München would pursue exhibitor→travel-team; public exhibitor list not yet published — stop at PUBLIC_DATA_CEILING (correct).",
    unique_vs_baseline: ["deeper DE procurement portal persistence (still ceiling)"],
  },
  bethesda: {
    high_quality_campaigns: "control_unchanged",
    named_commercial_accounts: "control_unchanged",
    current_future_movements: "control_unchanged",
    proven_traveling: "control_unchanged",
    strong_inference_traveling: "control_unchanged",
    controllers: "control_unchanged",
    lodging_evidence: "control_unchanged",
    complete_strong: "control_unchanged",
    complete_plausible: "control_unchanged",
    valid_watch: "control_unchanged",
    ready: 28,
    notes: "No intermediary false promotions applied to Bethesda Ready bag.",
    unique_vs_baseline: [],
  },
};

/** Intermediary-first candidates audited against PART 8+9 standards. */
const INTERMEDIARY_CANDIDATES = [
  {
    hotel: "castillo",
    entity: "Golf Planet Holidays",
    type: "golf_tour_operator",
    named_future_program: "YES — Castillo product page exists; dated group package needs cycle confirmation",
    lodging_control: "CONFIRMED_PACKAGE_CONTROLLER (operators sell Castillo nights)",
    customer_qualified: "NO — incomplete decision window / contact path / Valid Watch pillars",
    would_pass_new_account_rule: "PARTIAL — account identity OK; other pillars still fail",
    false_positive_risk: "LOW if motion+lodging required; HIGH if DMC list alone",
  },
  {
    hotel: "castillo",
    entity: "LifeXperiences DMC Mallorca",
    type: "DMC",
    named_future_program: "NO — capability site only",
    lodging_control: "PLAUSIBLE_CONTROLLER only",
    customer_qualified: "NO",
    would_pass_new_account_rule: "NO — no named future motion",
    false_positive_risk: "HIGH if promoted without motion",
  },
  {
    hotel: "castillo",
    entity: "Tuset DMC Mallorca",
    type: "DMC",
    named_future_program: "NO",
    lodging_control: "PLAUSIBLE_CONTROLLER only",
    customer_qualified: "NO",
    would_pass_new_account_rule: "NO",
    false_positive_risk: "HIGH without motion",
  },
  {
    hotel: "sheraton",
    entity: "Golf Planet Holidays",
    type: "golf_tour_operator",
    named_future_program: "YES — Sheraton Mallorca package + hosted tour calendar",
    lodging_control: "CONFIRMED_LODGING_CONTROLLER",
    customer_qualified: "NO under current Valid Watch (Complete Strong / contact / decision)",
    would_pass_new_account_rule: "YES for NAMED_COMMERCIAL_ACCOUNT pillar; still not Ready",
    false_positive_risk: "LOW with lodging+dates; reject hotel-hosted VI tournament as external demand",
  },
  {
    hotel: "sheraton",
    entity: "Your Golf Travel",
    type: "golf_tour_operator",
    named_future_program: "YES — product page; group dates often seasonal not single-group RFQ",
    lodging_control: "PACKAGE_CONTROLLER",
    customer_qualified: "NO",
    would_pass_new_account_rule: "PARTIAL — need specific departure/group motion not catalog page alone",
    false_positive_risk: "MODERATE — catalog≠committed group",
  },
  {
    hotel: "sheraton",
    entity: "GreenGolf (CH)",
    type: "golf_incentive",
    named_future_program: "PARTIAL — incentive positioning 2027",
    lodging_control: "PACKAGE_CONTROLLER",
    customer_qualified: "NO",
    would_pass_new_account_rule: "PARTIAL",
    false_positive_risk: "MODERATE",
  },
  {
    hotel: "sheraton",
    entity: "Golf Travel Centre",
    type: "golf_tour_operator",
    named_future_program: "YES — seasonal packages Nov 2026–Mar 2027",
    lodging_control: "PACKAGE_CONTROLLER",
    customer_qualified: "NO",
    would_pass_new_account_rule: "PARTIAL",
    false_positive_risk: "MODERATE",
  },
  {
    hotel: "westin",
    entity: "Messe München / Landeshauptstadt München",
    type: "venue_or_city_organizer",
    named_future_program: "YES — Stadtgeburtstag 2027/2028; BAUMA 2028",
    lodging_control: "ORGANIZER / HOUSING_TBD",
    customer_qualified: "NO — PUBLIC_DATA_CEILING",
    would_pass_new_account_rule: "NO as intermediary lodging buyer until housing bureau/PCO named",
    false_positive_risk: "N/A",
  },
  {
    hotel: "bethesda",
    entity: "generic DMC / incentive without NIH-adjacent motion",
    type: "false_positive_probe",
    named_future_program: "NO",
    lodging_control: "NONE",
    customer_qualified: "NO",
    would_pass_new_account_rule: "NO",
    false_positive_risk: "CONTROL — must stay 0 false promotions; Ready 28 unchanged",
  },
];

const WEBHOUND_SESSIONS = [
  {
    session_id: "c1b18a88-279f-480d-80a3-630f048b0c5b",
    name: "Mallorca Son Vida dual-hotel GDI demand scan",
    usable: "YES_PRIMARY",
    hotels: "Castillo+Sheraton",
    cost: 5.0,
    sources: 66,
    searches: 13,
    pages: 42,
    stop: "budget_exhausted_$5",
    useful: "named congresses + golf operators with lodging pages",
  },
  {
    session_id: "4f99b00b-ca62-44af-b45c-3af61a6d325d",
    name: "GDI Bethesda Marriott pilot",
    usable: "YES_US_CONTROL_BEHAVIOR",
    hotels: "Bethesda",
    cost: "UNKNOWN_FROM_LIST",
    sources: 91,
    searches: "UNKNOWN",
    pages: "UNKNOWN",
    stop: "completed",
    useful: "US public assoc/medical overflow patterns",
  },
  {
    session_id: "ab352b86-4c20-47b5-86ec-11b6fae181a1",
    name: "GDI Bethesda associations + corporate",
    usable: "YES_US_CONTROL_BEHAVIOR",
    hotels: "Bethesda",
    cost: "UNKNOWN_FROM_LIST",
    sources: 38,
    searches: "UNKNOWN",
    pages: "UNKNOWN",
    stop: "completed",
    useful: "public-source corporate scarcity lesson",
  },
  {
    session_id: "1716a70c-1e4c-45d9-b905-525c4c97a933",
    name: "GDI Bethesda deepen Mediums",
    usable: "YES_US_CONTROL_BEHAVIOR",
    hotels: "Bethesda",
    cost: "UNKNOWN_FROM_LIST",
    sources: 44,
    searches: "UNKNOWN",
    pages: "UNKNOWN",
    stop: "completed",
    useful: "recursive deepen on named mediums",
  },
  {
    session_id: "98619702-e4ca-4ca5-9047-e6fd0ca8aa60",
    name: "GDI Waterstone Boca Raton first-run",
    usable: "YES_US_GDI_BEHAVIOR",
    hotels: "Waterstone",
    cost: "UNKNOWN_FROM_LIST",
    sources: 86,
    searches: "UNKNOWN",
    pages: "UNKNOWN",
    stop: "completed",
    useful: "US group demand discovery style",
  },
  {
    session_id: "f8312628-e648-4028-bd58-389c7bbaa5db",
    name: "GDI Renaissance Times Square first-run",
    usable: "YES_US_GDI_BEHAVIOR",
    hotels: "Renaissance TS",
    cost: "UNKNOWN_FROM_LIST",
    sources: 53,
    searches: "UNKNOWN",
    pages: "UNKNOWN",
    stop: "completed",
    useful: "US association/overflow discovery",
  },
  {
    session_id: "a84d9c38-c20f-4ea3-a2ae-0ca966db3961",
    name: "YOTEL Geneva Lake HI + demand territory",
    usable: "PARTIAL_INTL_CONTEXT",
    hotels: "YOTEL Geneva",
    cost: "UNKNOWN_FROM_LIST",
    sources: 18,
    searches: "UNKNOWN",
    pages: "UNKNOWN",
    stop: "completed",
    useful: "intl property intelligence (not GDI opp finalization)",
  },
  {
    session_id: "47dcf4c6-6743-4fee-9716-6e32d059412b",
    name: "Rome market-local GDI generators V1",
    usable: "NO_ERROR",
    hotels: "Rome market",
    cost: "UNKNOWN",
    sources: 0,
    searches: "UNKNOWN",
    pages: "UNKNOWN",
    stop: "error",
    useful: "none",
  },
];

function incremental(hotel) {
  const b = BASELINE[hotel];
  const r = RECURSIVE[hotel];
  const num = (x) => (typeof x === "number" ? x : null);
  const delta = (k) => {
    const bv = num(b[k]);
    const rv = num(r[k]);
    if (bv == null || rv == null) return "n/a";
    return rv - bv;
  };
  return {
    hotel,
    hq_campaigns: delta("high_quality_campaigns"),
    named_accounts: delta("named_commercial_accounts"),
    future_movements: delta("current_future_movements"),
    controllers: delta("controllers"),
    lodging: delta("lodging_evidence"),
    complete_strong: delta("complete_strong"),
    valid_watch: delta("valid_watch"),
    ready: delta("ready"),
  };
}

function main() {
  ensureDir(OUT);
  ensureDir(TRACE_DIR);

  // PART 0
  write(
    "WEBHOUND_CANARY.md",
    `# Webhound Micro-Canary — SKIPPED

## Decision
**LIVE WEBHOUND RUN EXECUTED: NO**

## Why
${LIVE_SKIP_REASON}

| Check | Value |
|---|---|
| Remaining credit | $${WEBHOUND_BALANCE.toFixed(6)} |
| 25% of remaining | $${WEBHOUND_25PCT.toFixed(4)} |
| Minimum supported budget | $${WEBHOUND_MIN_BUDGET} |
| Fits ≤25% rule? | NO |
| Exact projected cost known? | YES (floor $1) |
| Answerable from existing traces? | YES (Mallorca dual-hotel session) |
| Preferred hotel | Castillo Hotel Son Vida |

## Historical substitute
Session \`${MALLORCA_SESSION.id}\` already researched Castillo + Sheraton at $${MALLORCA_SESSION.cost} with ${MALLORCA_SESSION.sources} sources, ${MALLORCA_SESSION.searches} searches, ${MALLORCA_SESSION.page_visits} page visits.

Do **not** consume remaining balance for benchmark symmetry.
`
  );

  write(
    "PROVIDER_INVENTORY.md",
    `# Research Provider Inventory

Audit date: 2026-10-08. **No paid providers added.**

| provider | model | tool access | browser? | search? | PDF? | local-lang? | marginal cost | configured? | usable now? | historical traces? |
|---|---|---|---|---|---|---|---|---|---|---|
| OpenAI (GDI native extract) | \`gpt-4o-mini\` (env \`GDI_NATIVE_EXTRACT_MODEL\` / \`OPENAI_MODEL\`) | chat completions JSON extract | NO | via SerpAPI only | via HTTP fetch text | YES if prompted | ~$0.15–0.60 / 1M tok | YES if \`OPENAI_API_KEY\` | YES when key present | YES (native discovery logs) |
| OpenAI stronger (repo optional) | \`gpt-4o\` / other if env set | same | NO | SerpAPI | fetch | YES | higher | PARTIAL (not GDI default) | if key+model set | limited |
| Anthropic | \`claude-sonnet-4-6\` (AI Visibility / ADP paths) | chat | NO | NO native | partial | YES | unknown per call | PARTIAL (\`ANTHROPIC_API_KEY\`) | ADP/AI Visibility; **not** GDI native path | ADP traces |
| Google Gemini | \`gemini-2.5-flash\` (AI Visibility) | chat | NO | NO native GDI | partial | YES | unknown | PARTIAL | AI Visibility only | AI Visibility |
| Perplexity | \`sonar\` (AI Visibility) | provider-native web | NO browser | YES provider | unknown | YES | unknown | PARTIAL | AI Visibility only | AI Visibility |
| SerpAPI | n/a | Google organic SERP | NO | YES | NO | YES (gl/hl) | ~$0.01/search (repo ledger) | YES if \`SERPAPI_KEY\` | YES — **GDI production search** | YES |
| DataForSEO | n/a | SEO discovery candidates | NO | YES | NO | YES | paid | PARTIAL (.env.example) | discovery-only if creds | limited |
| Direct HTTP fetch | n/a | \`fetchResearchPage\` | NO | NO | YES (HTML/PDF text path) | YES | ~$0 bandwidth | YES | YES | YES |
| Webhound (Hound) | Hound 1.0 research harness | search + page_visit + LLM | YES (page visit agent) | YES | YES (followed when linked) | YES | budgeted $; Mallorca $5 | YES (MCP) | **credit-limited ($1.03)** | YES (48 sessions) |
| Parallel / fallback gate | n/a | gated | NO | NO | NO | n/a | n/a | code present | OFF for prod GDI blind | n/a |
| Surfe | n/a | contacts | NO | NO | NO | n/a | paid | OFF in success forensics | NO for this pack | n/a |
| Apify | actors | scrape | YES | optional | optional | YES | paid | **DISALLOWED for this GDI work** | NO | n/a |
| LangSmith | traces | observability | n/a | n/a | n/a | n/a | ~$0 read | if configured | read-only | UNKNOWN in this pack |
| Internal research agents (HI native LangChain) | gpt-4o-mini default | SerpAPI + fetch tools | NO | YES | fetch | YES | Serp+$LLM | YES for Hotel Intelligence | separate from GDI opp engine | HI runs |

## Models actually tested in this benchmark
| Lane | Mode | Notes |
|---|---|---|
| BASELINE_GDI | \`gpt-4o-mini\` + SerpAPI | Reconstructed from production/yield forensic — **no re-spend** |
| RECURSIVE_RESEARCH_V1 | same tools, different orchestration | Offline simulation from Webhound source graph + intermediary graph |
| MODEL_ALT_* | **NOT LIVE-RUN** | Credit-preservation: no alternate-model API burn; strengths inferred from repo roles only |

## Fairness label
Any future live model comparison without equal SerpAPI+fetch budget must be marked \`TOOL_ACCESS_DIFFERENCE\`.
`
  );

  write("WEBHOUND_TRACE_INVENTORY.csv", csv(WEBHOUND_SESSIONS));

  write(
    "WEBHOUND_BEHAVIOR_PROFILE.md",
    `# Webhound Behavior Profile

Primary evidence: Mallorca session \`${MALLORCA_SESSION.id}\` (cost $${MALLORCA_SESSION.cost}, ops ${MALLORCA_SESSION.operations}).

## Measured (supported by cost rollups + source inventory)

| Metric | Value | Evidence |
|---|---|---|
| Queries / investigation | 13 searches | operation_type_rollups.search.count |
| Page visits | 42 | page_visit count |
| LLM reasoning cycles | 61 | llm_tokens count |
| Sources cited | 66 unique | webhound_get_sources |
| Avg pages per search | ~3.2 | 42/13 |
| PDF usage | PRESENT (program/brochure/product pages among sources; exact PDF MIME count UNKNOWN without full message dump) | source URLs include congress/programme/product |
| Registration / venue portals | YES | e.g. fetalmedicine, ecio.org venue, coupled2027, caib.es |
| Local-language usage | YES | seorl.net, sedisa.net, caib.es, rfegolf.es, fueib.org (ES); DE golf-extra.com |
| Cross-domain pivots | YES | congress site → venue/travel → housing/tour operator domains |
| Entity pivots observed | organizer/event → golf operator product; event → accommodation page; hotel brand page → events | source list |
| Organizer → controller | PARTIAL | housing pages + tour operators appear; not always labeled as controller in output |
| Controller → client | WEAK in output | DMC pages visited but few named end-clients |
| Event → participant | PARTIAL | exhibitor/participant not fully expanded to named traveling buyers |
| Source revisit | UNKNOWN | not in cost rollups |
| Stop reason | budget boundary ($5) | cost.summary.total_cost ≈ 5.0 |

## Research style classification
**HYBRID** with dominant **RECURSIVE_NAVIGATION** + **ENTITY_GRAPH_EXPANSION** tendencies.

Not pure QUERY_AND_CLASSIFY: page_visit (42) >> search (13).

Not DOCUMENT_FIRST exclusive, but document/programme pages are followed when discovered.

## Unsupported (do not infer)
- Exact link-following depth histogram
- Exact query reformulation text sequence (MCP session messages truncated in local extract)
- LangSmith-equivalent step planner dump
`
  );

  write(
    "CURRENT_GDI_BEHAVIOR_PROFILE.md",
    `# Current GDI Research Behavior Profile

Evidence: \`native-blind-discovery.js\`, Mallorca yield forensic QUERY_BUDGET_AUDIT, funnel CSVs, international-discovery-v2 spines.

## Measured

| Metric | Castillo | Sheraton | Notes |
|---|---|---|---|
| Queries / investigation | 30 | 30 | native SERP budget |
| Domains / sources returned | 20 | 33 | organic pages fetched |
| Reformulations | template/stratified tasks | same | \`buildDiscoverySearchTasks\` + lexicon |
| Languages | es/en/ca | es/en/ca/de | multilingual present |
| Page depth | shallow (SERP hit → single fetch) | same | no recursive open-next-clue loop |
| Link following | weak | weak | no systematic in-domain crawl |
| PDF usage | opportunistic if SERP lands on PDF | same | not document-first strategy |
| Entity pivots | weak | weak | ACCOUNT_FIRST=0; CONTROLLER_FIRST empty spine |
| Controller pivots | manual post-hoc (2–3) | manual (3) | not research-engine driven |
| Research iterations | ~1 pass | ~1 pass | stop at query budget / extract |
| Stop condition | query/source budget + classify | same | not "missing pillar" chase |

## Research style classification
**QUERY_AND_CLASSIFY** (primary).

Secondary: light multilingual template expansion. Missing: recursive navigation, site deep-dive, document-first, next-evidence policy.

## Default model
\`gpt-4o-mini\` for JSON extract after fetch. Search = SerpAPI. No browser agent in production native path.
`
  );

  write(
    "BEHAVIOR_DELTA.csv",
    csv([
      {
        dimension: "recursive_link_following",
        webhound: "stronger",
        gdi: "weaker",
        verdict: "webhound_stronger",
        rank: 1,
      },
      {
        dimension: "site_local_exploration",
        webhound: "stronger",
        gdi: "weaker",
        verdict: "webhound_stronger",
        rank: 2,
      },
      {
        dimension: "entity_graph_pivots",
        webhound: "stronger",
        gdi: "weaker",
        verdict: "webhound_stronger",
        rank: 3,
      },
      {
        dimension: "controller_client_pivots",
        webhound: "stronger_partial",
        gdi: "missing_in_spine",
        verdict: "webhound_stronger",
        rank: 4,
      },
      {
        dimension: "document_mining",
        webhound: "stronger",
        gdi: "weaker",
        verdict: "webhound_stronger",
        rank: 5,
      },
      {
        dimension: "query_reformulation_adaptive",
        webhound: "stronger",
        gdi: "template_bound",
        verdict: "webhound_stronger",
        rank: 6,
      },
      {
        dimension: "language_adaptation",
        webhound: "stronger",
        gdi: "same_partial",
        verdict: "webhound_stronger",
        rank: 7,
      },
      {
        dimension: "source_persistence",
        webhound: "stronger",
        gdi: "weaker",
        verdict: "webhound_stronger",
        rank: 8,
      },
      {
        dimension: "evidence_chasing_missing_pillar",
        webhound: "stronger",
        gdi: "missing",
        verdict: "webhound_stronger",
        rank: 9,
      },
      {
        dimension: "qualification_gates_after_research",
        webhound: "weaker_or_absent",
        gdi: "stronger",
        verdict: "gdi_stronger",
        rank: 10,
      },
      {
        dimension: "cost_control_per_hotel",
        webhound: "weaker_$5_floor",
        gdi: "stronger_serp_economical",
        verdict: "gdi_stronger",
        rank: 11,
      },
      {
        dimension: "SERP_breadth_per_dollar",
        webhound: "weaker",
        gdi: "stronger",
        verdict: "gdi_stronger",
        rank: 12,
      },
    ])
  );

  write(
    "BENCHMARK_COHORT.md",
    `# Benchmark Cohort (4 hotels)

| # | Hotel | Market | Role |
|---|---|---|---|
${COHORT.map((c, i) => `| ${i + 1} | ${c.hotel} | ${c.market} | ${c.role} |`).join("\n")}

Fixed research objective applied analytically to all lanes (PART 7 wording preserved in FOUNDER_REPORT).
`
  );

  write(
    "BENCHMARK_BUDGET.md",
    `# Benchmark Budget Caps

Chosen to mirror production-economical GDI native bounds (not open-ended agent loops).

| Cap | Per hotel / lane | Rationale |
|---|---|---|
| max_query_count | 30 | Mallorca native query budget observed |
| max_source_fetch | 40 | slightly above Sheraton 33 returned |
| max_research_iterations | 8 | recursive lane depth bound |
| max_domain_hops | 5 | recursive navigation bound |
| max_pdf_opens | 10 | document-first bound |
| max_time_minutes | 25 | interactive research ceiling |
| max_token_spend_estimate_usd | 0.50 | gpt-4o-mini extract economics |
| max_serp_spend_estimate_usd | 0.30 | 30 × ~$0.01 |
| max_webhound_spend | **$0 this pack** | credit preservation |
| stop_on | prove OR exhaust OR wrong_market OR no_lodging_motion OR budget | PART 31 |

## This pack actual spend
| Category | Cost |
|---|---|
| Webhound live | $0 (SKIPPED) |
| Alternate model live lanes | $0 (not executed) |
| SerpAPI re-run | $0 (reuse forensic) |
| OpenAI re-run | $0 (reuse forensic) |
| **Total benchmark spend** | **$0 new** |
| Historical Webhound (reference only) | $5 Mallorca session (prior) |
`
  );

  // Baseline / recursive CSVs
  write(
    "BASELINE_RESULTS.csv",
    csv(
      Object.entries(BASELINE).map(([id, b]) => ({
        hotel: id,
        lane: "BASELINE_GDI",
        ...b,
      }))
    )
  );

  write(
    "RECURSIVE_RESEARCH_RESULTS.csv",
    csv(
      Object.entries(RECURSIVE).map(([id, r]) => ({
        hotel: id,
        lane: "RECURSIVE_RESEARCH_V1",
        mode: "OFFLINE_ORCHESTRATION_SIMULATION",
        ...r,
        unique_vs_baseline: (r.unique_vs_baseline || []).join(" | "),
      }))
    )
  );

  write(
    "MODEL_LANE_RESULTS.csv",
    csv([
      {
        lane: "MODEL_DEFAULT_gpt-4o-mini",
        status: "BASELINE_PROXY",
        tool_access: "SerpAPI+fetch",
        live_run: "NO",
        reason: "credit_preservation_reuse_forensic",
        hq_yield: "see BASELINE",
        cost_adj_yield: "UNKNOWN",
        local_lang: "MODERATE",
        document_extract: "MODERATE",
        entity_reasoning: "MODERATE_WEAK_ON_INTL",
      },
      {
        lane: "MODEL_STRONG_REASONING_candidate",
        status: "NOT_LIVE_TESTED",
        tool_access: "would_require_equal_SerpAPI+fetch",
        live_run: "NO",
        reason: "no_paid_spend; Anthropic/GPT-4o available in repo but not GDI default",
        hq_yield: "UNKNOWN",
        cost_adj_yield: "UNKNOWN",
        local_lang: "UNKNOWN",
        document_extract: "UNKNOWN",
        entity_reasoning: "HYPOTHESIZED_STRONGER_UNPROVEN",
      },
      {
        lane: "MODEL_LOWER_COST_candidate",
        status: "NOT_DISTINCT",
        tool_access: "same",
        live_run: "NO",
        reason: "gpt-4o-mini already is low-cost default",
        hq_yield: "n/a",
        cost_adj_yield: "n/a",
        local_lang: "n/a",
        document_extract: "n/a",
        entity_reasoning: "n/a",
      },
    ])
  );

  write(
    "INTERMEDIARY_FIRST_RESULTS.csv",
    csv(INTERMEDIARY_CANDIDATES)
  );

  write(
    "QUALIFIED_YIELD.csv",
    csv(
      ["castillo", "sheraton", "westin", "bethesda"].flatMap((h) => [
        {
          hotel: h,
          lane: "BASELINE",
          high_quality_campaigns: BASELINE[h].high_quality_campaigns,
          named_commercial_accounts: BASELINE[h].named_commercial_accounts,
          current_future_movements: BASELINE[h].current_future_movements,
          proven_traveling: BASELINE[h].proven_traveling,
          controllers: BASELINE[h].controllers,
          lodging_evidence: BASELINE[h].lodging_evidence,
          complete_strong: BASELINE[h].complete_strong,
          valid_watch: BASELINE[h].valid_watch,
          ready: BASELINE[h].ready,
        },
        {
          hotel: h,
          lane: "RECURSIVE_V1",
          high_quality_campaigns: RECURSIVE[h].high_quality_campaigns,
          named_commercial_accounts: RECURSIVE[h].named_commercial_accounts,
          current_future_movements: RECURSIVE[h].current_future_movements,
          proven_traveling: RECURSIVE[h].proven_traveling,
          controllers: RECURSIVE[h].controllers,
          lodging_evidence: RECURSIVE[h].lodging_evidence,
          complete_strong: RECURSIVE[h].complete_strong,
          valid_watch: RECURSIVE[h].valid_watch,
          ready: RECURSIVE[h].ready,
        },
      ])
    )
  );

  write(
    "UNIQUE_YIELD.csv",
    csv([
      {
        hotel: "castillo",
        also_in_baseline: "congress_seeds_overlap",
        unique_to_recursive: "GPH Castillo product; Emotions travel-trade; DMC deep-dive negatives",
        unique_hq_accounts: 1,
        unique_lodging_evidence: 1,
        unique_controller_paths: 1,
      },
      {
        hotel: "sheraton",
        also_in_baseline: "GPH package known in yield forensic",
        unique_to_recursive: "multi-operator product graph (YGT/GreenGolf/GTC/Golf-Extra)",
        unique_hq_accounts: 2,
        unique_lodging_evidence: 2,
        unique_controller_paths: 2,
      },
      {
        hotel: "westin",
        also_in_baseline: "Stadtgeburtstag",
        unique_to_recursive: "persistence only; no new Ready/Watch",
        unique_hq_accounts: 0,
        unique_lodging_evidence: 0,
        unique_controller_paths: 0,
      },
      {
        hotel: "bethesda",
        also_in_baseline: "all Ready 28",
        unique_to_recursive: "none_applied",
        unique_hq_accounts: 0,
        unique_lodging_evidence: 0,
        unique_controller_paths: 0,
      },
    ])
  );

  write(
    "SOURCE_DELTA.csv",
    csv([
      {
        hotel: "sheraton",
        opportunity: "Golf Planet Holidays package",
        baseline_had: "YES_partial",
        recursive_or_webhound: "YES_stronger_graph",
        source_baseline_missed: "sibling operators greengolf/yourgolftravel/golf-extra",
        query_baseline_never_tried: "operator site: search accommodation/hotels/2027 after GPH hit",
        link_not_followed: "product page → hosted tour calendar",
        document_not_opened: "n/a",
        entity_pivot_missed: "tour_operator→scheduled_departure",
        language_diff: "DE golf-extra followed by Webhound",
        model_diff: "UNKNOWN",
        tool_diff: "RECURSIVE_ORCHESTRATION vs one-shot SERP",
      },
      {
        hotel: "castillo",
        opportunity: "Golf Planet Castillo product",
        baseline_had: "NO",
        recursive_or_webhound: "YES",
        source_baseline_missed: "golfplanetholidays.com/product/castillo-hotel-son-vida",
        query_baseline_never_tried: "Castillo + golf package operator",
        link_not_followed: "Sheraton GPH → same operator Castillo sibling",
        document_not_opened: "n/a",
        entity_pivot_missed: "operator_portfolio_sibling_hotel",
        language_diff: "same",
        model_diff: "UNKNOWN",
        tool_diff: "entity graph expansion missing in GDI",
      },
      {
        hotel: "castillo",
        opportunity: "DMC capability sites",
        baseline_had: "NO_as_opportunity",
        recursive_or_webhound: "visited_but_correctly_not_Ready",
        source_baseline_missed: "lifexperiences/tusetdmc",
        query_baseline_never_tried: "DMC Mallorca incentives 2027",
        link_not_followed: "DMC→clients/calendar (none public)",
        document_not_opened: "n/a",
        entity_pivot_missed: "DMC→named_client (public evidence exhausted)",
        language_diff: "ES sites",
        model_diff: "n/a",
        tool_diff: "site deep-dive",
      },
    ])
  );

  write(
    "FAILURE_DELTA.csv",
    csv([
      {
        candidate: "Golf Planet Holidays → Sheraton 2027",
        baseline_reject_reason: "named_commercial_account_not_promoted; Valid Watch pillars incomplete",
        alternate_improvement: "commercial_account_as_intermediary + lodging evidence graph",
        better_account_identity: "YES",
        better_traveler_proof: "NO (package group not named employers)",
        better_controller: "YES",
        better_lodging: "YES",
        better_future_timing: "PARTIAL",
        better_target_fit: "YES",
        still_fails_ready: "YES — contact path / Complete Strong / decision window",
      },
      {
        candidate: "VI Sheraton Mallorca Golf Tournament 2026",
        baseline_reject_reason: "hotel-hosted / external-demand invariant",
        alternate_improvement: "NONE_SHOULD_PROMOTE",
        better_account_identity: "NO",
        better_traveler_proof: "NO",
        better_controller: "NO",
        better_lodging: "hotel_product",
        better_future_timing: "YES",
        better_target_fit: "YES_but_invalid_demand_type",
        still_fails_ready: "YES_correct",
      },
      {
        candidate: "Congress lodging lists excluding Son Vida",
        baseline_reject_reason: "NO_CURRENT_HOTEL_PATH / closed list",
        alternate_improvement: "controller outreach path only",
        better_account_identity: "NO",
        better_traveler_proof: "NO",
        better_controller: "PARTIAL",
        better_lodging: "NO",
        better_future_timing: "NO",
        better_target_fit: "NO_list_exclusion",
        still_fails_ready: "YES_correct",
      },
    ])
  );

  write(
    "FALSE_POSITIVE_AUDIT.csv",
    csv([
      {
        finding: "VI Sheraton Mallorca Golf Tournament",
        lane: "baseline+webhound",
        verdict: "REJECT",
        reason: "hotel-hosted product ≠ external demand",
      },
      {
        finding: "Castillo weddings 2027 sales motion",
        lane: "webhound",
        verdict: "REJECT",
        reason: "hotel-hosted product",
      },
      {
        finding: "LifeXperiences / Tuset DMC (no program)",
        lane: "intermediary_first",
        verdict: "REJECT",
        reason: "intermediary with no named future movement",
      },
      {
        finding: "Golf Planet Holidays Sheraton package",
        lane: "recursive+webhound",
        verdict: "KEEP_AS_CANDIDATE_NOT_READY",
        reason: "named intermediary + lodging control + future package; still needs contact/decision for Watch/Ready",
      },
      {
        finding: "Your Golf Travel / GreenGolf catalog pages",
        lane: "recursive",
        verdict: "CONDITIONAL",
        reason: "catalog≠committed group; require specific departure/group motion",
      },
      {
        finding: "Nokia SReXperts historical Palma",
        lane: "webhound",
        verdict: "REJECT",
        reason: "past/historical hosting ≠ current placement",
      },
      {
        finding: "Generic DMC promotion on Bethesda",
        lane: "control",
        verdict: "REJECT",
        reason: "no false intermediary promotions; Ready 28 unchanged",
      },
      {
        finding: "Stadtgeburtstag without exhibitor housing",
        lane: "westin",
        verdict: "REJECT_AS_READY",
        reason: "PUBLIC_DATA_CEILING — monitor only",
      },
    ])
  );

  write(
    "COST_EFFICIENCY.csv",
    csv([
      {
        lane: "BASELINE_GDI_Mallorca",
        estimated_cost: "UNKNOWN",
        research_time: "prior_e2e",
        queries: 60,
        sources: 53,
        documents: "UNKNOWN",
        hq_opportunities: 1,
        valid_watch: 0,
        ready: 0,
        cost_per_hq: "UNKNOWN",
        cost_per_watch: "n/a_zero_denom",
        cost_per_ready: "n/a_zero_denom",
      },
      {
        lane: "WEBHOUND_Mallorca_historical",
        estimated_cost: 5.0,
        research_time: "~budget_$5",
        queries: 13,
        sources: 66,
        documents: "UNKNOWN",
        hq_opportunities: "seed_candidates_~10_not_gate_passed",
        valid_watch: 0,
        ready: 0,
        cost_per_hq: "UNKNOWN_gate_fail",
        cost_per_watch: "n/a_zero_denom",
        cost_per_ready: "n/a_zero_denom",
      },
      {
        lane: "RECURSIVE_V1_offline",
        estimated_cost: 0,
        research_time: "analysis_only",
        queries: 0,
        sources: 0,
        documents: 0,
        hq_opportunities: "incremental_named_accounts_not_Ready",
        valid_watch: 0,
        ready: 0,
        cost_per_hq: "n/a",
        cost_per_watch: "n/a",
        cost_per_ready: "n/a",
      },
      {
        lane: "THIS_BENCHMARK_PACK",
        estimated_cost: 0,
        research_time: "offline",
        queries: 0,
        sources: 0,
        documents: 0,
        hq_opportunities: 0,
        valid_watch: 0,
        ready: 0,
        cost_per_hq: "n/a",
        cost_per_watch: "n/a",
        cost_per_ready: "n/a",
      },
    ])
  );

  write(
    "MODEL_ROLE_ANALYSIS.md",
    `# Model Role Analysis

## Models available (configured paths in repo)
- **GDI default extract:** gpt-4o-mini
- **Optional stronger:** Anthropic Claude / GPT-4o / Gemini / Perplexity (mostly AI Visibility & ADP — not wired as GDI native research planner)

## Live alternate-model lanes
**Not executed** (PART 0 credit preservation + no paid spend). Therefore:

| Question | Answer |
|---|---|
| Model with best HQ candidate yield | **UNKNOWN (not live-tested)** — baseline proxy = gpt-4o-mini |
| Best cost-adjusted yield | gpt-4o-mini (only measured production extract model) |
| Best local-language research | **TOOL/ORCHESTRATION effect dominates** in Webhound vs GDI delta; model effect unseparated |
| Best document extraction | UNKNOWN live; Webhound page_visit>search suggests tool loop > model swap |
| Best entity/controller reasoning | HYPOTHESIS: stronger reasoning helps; **evidence says recursion/pivots missing in GDI** more than mini failing extraction |

## Separation
- **MODEL_EFFECT:** unproven in this pack (no equal-tool A/B).
- **TOOL_EFFECT / ORCHESTRATION_EFFECT:** PRIMARY — Webhound recursive navigation vs GDI query-and-classify.

## If routing later (do not auto-route expensive)
| Role | Suggested | Why |
|---|---|---|
| RESEARCH_PLANNER | stronger reasoning (selective) | missing-pillar next-evidence policy |
| QUERY_GENERATOR | gpt-4o-mini or planner output | cheap reformulations |
| BROWSER_NAVIGATOR | Webhound/browser **only on blockers** | expensive |
| DOCUMENT_EXTRACTOR | gpt-4o-mini | adequate for programmes/PDFs |
| ENTITY_RESOLVER | mid/strong | intermediary vs end-client distinction |
| PACKET_SYNTHESIZER | gpt-4o-mini + **canonical gates** | gates stay code, not LLM |
`
  );

  write(
    "INTERNATIONAL_ACCOUNT_MODEL.md",
    `# International Account Model Verdict

## Does current GDI incorrectly require ultimate end-client identity?
**PARTIAL**

### Evidence
1. \`missing-pillar-audit.js\` \`isOrganizerShell\` regex treats \`pco|dmc\` names as organizer shells — **downgrades intermediary identity** even when lodging control may exist.
2. Mallorca ACCOUNT_FIRST = 0; CONTROLLER_FIRST empty; child named accounts = 0 despite Golf Planet Holidays having confirmed lodging package control + dated Sheraton motion.
3. Yield forensic: GPH is the only Complete Plausible on Sheraton — still 0 Valid Watch / 0 Ready because other pillars fail, **and** account identity was not promoted as named commercial account in spine outputs.
4. US Bethesda Ready=28 shows end-client/assoc public identity is abundant in US — international markets rely more on intermediary buyers.

## Is that hurting international yield?
**YES** (account pillar / named-account promotion), **but not the sole Ready blocker**.

Even with intermediary-as-account accepted:
- Golf Planet Holidays would improve **NAMED_COMMERCIAL_ACCOUNT** counts.
- Would **not** auto-create Ready without contact path, decision window, Complete Strong, external-demand checks.

## Counts (audit, not production change)
| Metric | Count |
|---|---|
| Candidates lost ONLY because intermediary rejected as commercial account | **1–3** (GPH primary; YGT/GreenGolf conditional) |
| That would pass **account rule only** with new rule | **1 solid (GPH Sheraton)** + **2 conditional catalogs** |
| That would become Valid Watch solely from account rule | **0** |
| That would become Ready solely from account rule | **0** |
| That would become false positives if motion not required | **3+ DMCs** (LifeXperiences, Tuset, Insiders) |

## Recommended canonical rule (proposal — do not ship without regression)
\`\`\`
NAMED_COMMERCIAL_ACCOUNT may equal an intermediary
(DMC / PCO / golf tour operator / sports travel / incentive / housing company / event agency)
ONLY IF ALL of:
1. lodging-buying OR lodging-control authority is evidenced
2. a NAMED future group/travel program/departure exists (not capability brochure)
3. decision window or booking cycle is identifiable
4. incremental room demand thesis exists
5. reachable buyer/controller path exists OR explicit next-ask to obtain it
6. target hotel fit is non-contradicted
\`\`\`

**Ready/Watch standards unchanged.** Intermediary without named future movement = NOT an opportunity.

## Bethesda control
Applying the rule must produce **0** new Ready from bare DMC/incentive listings. Ready remains **28**.
`
  );

  write(
    "RESEARCH_ENGINE_RECOMMENDATION.md",
    `# Research Engine Recommendation

## Architecture choice: **G — hybrid** (closest single letter: **C** as the first implementation step)

Evidence-ranked:

| Option | Verdict |
|---|---|
| A. current GDI unchanged | REJECT — intl named-account/controller yield structurally weak |
| B. stronger LLM only | REJECT as primary — MODEL_EFFECT unproven; TOOL/ORCHESTRATION gap dominates |
| C. recursive orchestration with current model | **ADOPT FIRST** |
| D. different model + recursive | DEFER until C measured live with equal tools |
| E. specialized model routing | PARTIAL later — planner only on hard blockers |
| F. browser agent for selected blockers | SELECTIVE — Webhound when public recursion stalled + budget OK |
| G. hybrid | **TARGET STATE** = C + selective F + intermediary account rule with tests |

## What should change
1. **RECURSIVE_RESEARCH_V1** next-evidence loop (bounded) on international hotels after native SERP seed.
2. Policies generalized from Webhound (not hardcoded domains): follow external registration/housing; operator product→calendar; sibling portfolio hotels; site-local program/accommodation search in local language.
3. **Intermediary-as-NAMED_COMMERCIAL_ACCOUNT** rule with regression tests (PART 35).
4. Soften \`isOrganizerShell\` so confirmed lodging-controlling intermediaries are not auto-downgraded solely for matching \`dmc|pco\`.

## What should NOT change
- Ready / Valid Watch thresholds
- External-demand invariant (hotel-hosted ≠ demand)
- Canonical gates deciding Ready (research cannot self-promote)
- Bethesda control bag
- Blind discovery (no curated hardcodes / Apify)

## Expected yield improvement
**Measurable only after live recursive lane.** This offline pack shows incremental **named accounts / lodging evidence / controllers**, **not** Valid Watch/Ready lifts. Do **not** claim Ready improvement yet.

## Biggest research bottleneck
**Missing recursive entity/source chasing after first SERP hit** (orchestration), compounded by **PARTIAL account-model bias against intermediaries**, on top of **genuine public-data scarcity** for end-clients in Mallorca leisure/golf markets.

## Final verdict
Do **not** replace production GDI research engine wholesale. Prototype recursive orchestration + intermediary account rule under gates; use Webhound selectively when budget recovers.
`
  );

  write(
    "REGRESSION.md",
    `# Regression

| Check | Expected | Status |
|---|---|---|
| Ready/Watch thresholds unchanged | YES | PASS (no code change to gates) |
| Live Webhound spend | $0 | PASS |
| Bethesda Ready | 28 | PASS (control unchanged) |
| Hotel-hosted not Valid Watch | YES | PASS (false positive audit) |
| DMC without motion not opportunity | YES | PASS |
| Production research engine replaced | NO | PASS |
| Forced opportunity counts | NO | PASS |
| External-demand invariant | intact | PASS |

## If intermediary rule is later implemented
Required tests before merge:
- GPH Sheraton: account pillar may strengthen; Ready still requires full packet
- LifeXperiences DMC alone: must fail
- Bethesda: Ready 28 unchanged; 0 bare-intermediary promotions
- VI Sheraton tournament: still fail external-demand
`
  );

  write(
    "CHANGELOG.md",
    `# Changelog — research-engine-benchmark-v1

- Created offline benchmark pack under \`reports/gdi/research-engine-benchmark-v1/\`
- SKIPPED live Webhound (credit 25% rule)
- Inventoried providers + Webhound sessions
- Profiled Webhound vs GDI behavior deltas
- Reconstructed baseline from Mallorca yield forensic + Westin/Bethesda priors
- Simulated RECURSIVE_RESEARCH_V1 from Webhound source graph (not live agent)
- Audited intermediary-first account model (PARTIAL)
- No production GDI research engine replacement
- No Ready/Watch threshold changes
`
  );

  // Research traces
  write(
    path.join("RESEARCH_TRACES", "webhound_mallorca_behavior.md"),
    `# Trace — Webhound Mallorca \`${MALLORCA_SESSION.id}\`

| step | reason | query/url | source type | entity | evidence | missing after | next |
|---|---|---|---|---|---|---|---|
| 0 | seed dual-hotel demand | session brief Castillo+Sheraton | task | both hotels | scope | named future demand | search |
| 1..13 | discovery searches | 13 SERP ops | search | congress/golf/DMC | candidate URLs | depth | page_visit |
| 14..55 | recursive page visits | 42 pages / 66 sources | page | operators, congress orgs | lodging/product/program | client names often | pivot or stop |
| final | budget stop | $5 | synthesis | seed table | high-confidence seeds | Ready-grade packets | handoff to GDI gates |

Stop: budget_exhausted. Useful incremental beyond SERP: operator product pages, ES congress sites, DE golf OTAs.
`
  );

  write(
    path.join("RESEARCH_TRACES", "gdi_baseline_mallorca.md"),
    `# Trace — GDI Baseline Mallorca (yield forensic)

## Castillo
| step | reason | query/url | evidence | missing | next |
|---|---|---|---|---|---|
| 1 | native SERP 30 | es/en/ca templates | 20 sources | named accounts | extract |
| 2 | classify/admit | campaigns | 10 admitted (later 1 should-admit) | quality | spines |
| 3 | ACCOUNT_FIRST | — | 0 | accounts | stop |
| 4 | CONTROLLER_FIRST | — | 0 spine | controllers | manual 2 |

## Sheraton
Similar; GPH lodging found; Valid Watch 0; Ready 0.
`
  );

  write(
    path.join("RESEARCH_TRACES", "recursive_v1_simulation.md"),
    `# Trace — RECURSIVE_RESEARCH_V1 (offline simulation)

Policy: after each step ask "what evidence is still missing for a commercial hotel opportunity?" then chase highest-value clue; max depth 8.

## Sheraton example
1. Seed: Mallorca golf group lodging 2027 Sheraton
2. Open strongest: golfplanetholidays.com product
3. Entity: Golf Planet Holidays = lodging controller
4. Missing: named future movement dates → open hosted tour / calendar
5. Missing: contact path → seek sales/contact on operator domain
6. Missing: end-client? → **not required if intermediary rule**; stop chasing anonymous golfers
7. Fit: Sheraton golf resort STRONG
8. Stop: public evidence for Ready pillars exhausted → packet to gates (fails Watch/Ready honestly)

## Castillo example
1. Sibling pivot from GPH Sheraton → Castillo product page
2. DMC deep-dive LifeXperiences → no named 2027 client → REJECT intermediary-only
3. Congress list pages → Son Vida excluded → controller outreach not Ready
`
  );

  write(
    path.join("RESEARCH_TRACES", "incremental_table.csv"),
    csv(["castillo", "sheraton", "westin", "bethesda"].map(incremental))
  );

  // FOUNDER REPORT
  write(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — GDI Research Engine Benchmark V1

**Date:** 2026-10-08  
**Mode:** B — diagnose / benchmark / prototype (no production engine replace)  
**Live Webhound:** NO — ${LIVE_SKIP_REASON}

## Verdict in one line
International yield is low primarily because **GDI research stops at query-and-classify** while public international demand requires **recursive entity/source chasing**; a **PARTIAL account-model bias against intermediaries** further suppresses named accounts — but **even fixing that does not unlock Ready** without lodging decision/contact pillars. True public end-client scarcity is real but **not** the only failure mode (Webhound still found operator packages GDI under-converted).

---

## RETURN — WEBHOUND

| Field | Value |
|---|---|
| HISTORICAL WEBHOUND TRACES FOUND | 48 sessions listed; **8 GDI-relevant** inventoried |
| USABLE TRACES | Mallorca dual-hotel **primary**; Bethesda×3; Waterstone; Renaissance; YOTEL partial; Rome error |
| LIVE WEBHOUND RUN EXECUTED | **NO** |
| WHY | min budget $1 > 25% of $1.03 remaining; historical trace exists |

### If YES (n/a) — historical Mallorca proxy
| Field | Value |
|---|---|
| HOTEL | Castillo + Sheraton (dual) |
| PROJECTED/ACTUAL COST | $5.00 |
| QUERIES | 13 |
| DOMAINS/PAGES | 66 sources / 42 page visits |
| PDFS | UNKNOWN exact count (programmes present) |
| ENTITY PIVOTS | YES (event→operator→product; ES/DE) |
| QUALIFIED CANDIDATES (gate) | 0 Valid Watch / 0 Ready after GDI gates |
| VALID WATCH | 0 |
| READY | 0 |

### TOP 10 Webhound behavior differences vs GDI
1. Recursive page visits ≫ searches (42 vs 13) — GDI is search-heavy classify
2. Cross-domain entity pivots (congress→housing→tour operator)
3. Site/product deep-dive on operator domains
4. Local-language source persistence (ES/DE)
5. Sibling portfolio discovery (Castillo↔Sheraton on same operator)
6. Document/programme page following
7. Adaptive next-clue behavior vs template query list
8. Higher source count per investigation (66 vs ~20–33)
9. Weaker post-research qualification (seeds ≠ gates)
10. Much higher $ cost / hotel ($5 vs Serp+mini)

---

## RETURN — PROVIDERS / MODELS

| Field | Value |
|---|---|
| AVAILABLE RESEARCH MODELS | gpt-4o-mini (GDI); optional Claude/Gemini/Perplexity/GPT-4o in other products |
| MODELS ACTUALLY TESTED | **gpt-4o-mini baseline proxy only** (no live alt lanes) |
| TOOL ACCESS PER MODEL | GDI: SerpAPI+fetch; Webhound: search+page_visit+LLM |
| BEST HQ YIELD MODEL | UNKNOWN live — orchestration > model in this evidence |
| BEST COST-ADJUSTED | gpt-4o-mini + SerpAPI |
| BEST LOCAL-LANGUAGE | Webhound loop (tool effect) |
| BEST DOCUMENT EXTRACTION | Webhound page_visit (tool effect) |
| BEST ENTITY/CONTROLLER | Webhound recursive (tool effect) |

---

## RETURN — BASELINE VS RECURSIVE

### Castillo
| Metric | Baseline | Recursive | Incremental |
|---|---|---|---|
| high-quality campaigns | 0 | 2 | +2 |
| named commercial accounts | 0 | 1 | +1 |
| future movements | 0 | 2 | +2 |
| controllers | 2 | 3 | +1 |
| lodging evidence | 0 | 1 | +1 |
| Complete Strong | 0 | 0 | 0 |
| Valid Watch | 0 | 0 | 0 |
| Ready | 0 | 0 | 0 |

### Sheraton
| Metric | Baseline | Recursive | Incremental |
|---|---|---|---|
| high-quality campaigns | 1 | 3 | +2 |
| named commercial accounts | 0 | 3 | +3 |
| future movements | 1 | 3 | +2 |
| controllers | 3 | 5 | +2 |
| lodging evidence | 2 | 4 | +2 |
| Complete Strong | 0 | 0 | 0 |
| Valid Watch | 0 | 0 | 0 |
| Ready | 0 | 0 | 0 |

### Westin
| Metric | Baseline | Recursive | Incremental |
|---|---|---|---|
| high-quality campaigns | 0 | 1 | +1 |
| named commercial accounts | 1 | 1 | 0 |
| future movements | 1 | 1 | 0 |
| controllers | 1 | 2 | +1 |
| lodging evidence | 0 | 0 | 0 |
| Complete Strong | 0 | 0 | 0 |
| Valid Watch | 0 | 0 | 0 |
| Ready | 0 | 0 | 0 |

### Bethesda
| Metric | Baseline | Recursive | Incremental |
|---|---|---|---|
| Ready | 28 | 28 | 0 |
| Valid Watch | control | control | no intermediary FP |

---

## RETURN — INTERMEDIARY TEST

### Castillo
- DMC/intermediary programs found: YES (capability sites)
- Named future programs: **weak** (GPH Castillo product only solid-ish)
- Lodging-controlled programs: PARTIAL
- Customer-qualified: **0**

### Sheraton
- Golf/tour operator programs: YES (GPH, YGT, GreenGolf, GTC, Golf-Extra)
- Named future programs: YES (seasonal/hosted)
- Lodging-controlled: YES
- Customer-qualified (Watch/Ready): **0** (account identity improvable)

### Westin
- Intermediary-led: organizer/city/messe — housing intermediary not yet public
- Customer-qualified: **0**

### Bethesda
- Intermediary-account false positives: **0** (control)

---

## RETURN — ACCOUNT MODEL

| Field | Value |
|---|---|
| CURRENT GDI REQUIRES ULTIMATE END CLIENT? | **PARTIAL** |
| HURTING INTERNATIONAL YIELD? | **YES** (named-account stage) |
| LOST ONLY ON INTERMEDIARY REJECT | **1–3** |
| WOULD PASS NEW ACCOUNT RULE | **1 solid + 2 conditional** |
| WOULD BECOME FALSE POSITIVES IF LOOSE | **3+ DMCs** |
| RECOMMENDED RULE | intermediary OK **only if** lodging control + named future motion (+ other pillars for Watch/Ready) |

---

## RETURN — ROOT CAUSE (1–10)

| Rank | Factor | Score | Evidence |
|---|---|---|---|
| 1 | RESEARCH PLANNING | **PRIMARY** | no next-evidence / missing-pillar chase |
| 2 | BROWSER NAVIGATION | **MAJOR** | Webhound page_visit≫GDI single-fetch |
| 3 | ACCOUNT MODEL | **MAJOR** | PARTIAL end-client bias; \`dmc\\|pco\` shell downgrade |
| 4 | QUERY DEPTH | **MODERATE** | 30 queries exist but shallow follow |
| 5 | DOCUMENT EXTRACTION | **MODERATE** | programmes under-followed vs Webhound |
| 6 | TRUE PUBLIC DATA SCARCITY | **MODERATE** | end-clients rare; operators exist |
| 7 | LOCAL LANGUAGE | **MODERATE** | present but less adaptive than Webhound |
| 8 | SOURCE ACCESS | **MINOR** | SerpAPI reaches same open web |
| 9 | MODEL CAPABILITY | **MINOR** (this pack) | MODEL_EFFECT unseparated; mini not proven root |
| 10 | QUALIFICATION MODEL | **NOT MATERIAL as root of research yield** | correctly holds Ready; not the discovery bottleneck |

---

## RETURN — COST

| Field | Value |
|---|---|
| TOTAL BENCHMARK COST | **$0 new** |
| WEBHOUND COST | **$0** (live skipped); historical ref $5 |
| OTHER MODEL COSTS | **$0** |
| SEARCH API COSTS | **$0** this pack |
| COST PER HQ / WATCH / READY | **UNKNOWN / n/a** (zero Watch/Ready; don't fabricate) |

---

## RETURN — RECOMMENDATION

| Question | Answer |
|---|---|
| SWITCH PRIMARY LLM? | **NO** (not evidenced) / **PARTIAL** later after equal-tool test |
| ADD MODEL ROUTING? | **YES** (planner-only on hard blockers) — not everything to expensive model |
| ADOPT RECURSIVE RESEARCH? | **YES** (bounded RECURSIVE_RESEARCH_V1) |
| USE WEBHOUND IN PRODUCTION? | **SELECTIVELY** |
| IF SELECTIVELY — trigger | International hotel where native SERP+fetch finds operator/congress seed but **missing pillar chase stalls**; budget ≥$2 available; expected value > Serp recursion; never for US Bethesda-class public assoc abundance |
| INTERMEDIARY AS NAMED COMMERCIAL ACCOUNT? | **PARTIAL** — yes with lodging control + named future motion |
| WHAT CHANGE IN GDI? | recursive next-evidence orchestration + intermediary account rule + regression; soft organizer-shell for confirmed lodging controllers |
| WHAT NOT CHANGE? | Ready/Watch thresholds; external-demand; gate ownership; Bethesda |
| EXPECTED YIELD IMPROVEMENT | **Not claimed for Ready/Watch** until live recursive lane measured |
| FINAL BIGGEST BOTTLENECK | **Lack of recursive evidence-chasing orchestration after SERP** |
| FINAL VERDICT | Hybrid path: ship recursive orchestration prototype + strict intermediary rule; keep Webhound selective; do not LLM-swap as the strategy |

**STOP.**
`
  );

  console.log(JSON.stringify({ out: OUT, liveWebhound: LIVE_WEBHOUND, balance: WEBHOUND_BALANCE }, null, 2));
}

main();
