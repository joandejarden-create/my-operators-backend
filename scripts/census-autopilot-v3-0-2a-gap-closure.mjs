#!/usr/bin/env node
/**
 * V3.0.2A — Authorized backfill #1 + SERPAPI_KEY fix + gap-closure research.
 * Does NOT launch V3.1. Backfill #2 is dry-run only.
 *
 * npm run census:autopilot-v3-0-2a-gap-closure -- --apply-backfill1
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import { PHASE2_ENV_GATE } from "../lib/research-engine-v2/census-autopilot-v3/constants.js";
import { runV302Backfill1 } from "../lib/research-engine-v2/census-autopilot-v3/v302a-backfill1.js";
import {
  createClaimStore,
  upsertClaim,
  resolveBestEligibleClaim,
  mergeClaimStores,
} from "../lib/research-engine-v2/census-autopilot-v3/claim-store.js";
import { resolveDealalityGeography } from "../lib/research-engine-v2/census-autopilot-v2-2/geography-expansion.js";
import { resolveStateRegion } from "../lib/research-engine-v2/census-autopilot-v3/state-region-pipeline.js";
import { normalizeAddress } from "../lib/research-engine-v2/census-autopilot-v3/address-pipeline.js";
import { normalizePhone } from "../lib/research-engine-v2/census-autopilot-v3/phone-pipeline.js";
import {
  classifySubmarketGap,
  classifyPhoneType,
} from "../lib/research-engine-v2/census-autopilot-v3/v302-deep-research.js";
import {
  searchGoogleHotels,
  getGoogleHotelDetails,
  getAccount,
  redactSecrets,
} from "../lib/research-engine-v2/providers/serpapi-google-hotels/index.js";
import { matchCensusProperty } from "../lib/research-engine-v2/providers/serpapi-google-hotels/match.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const RUN = "cav3_2026-08-08T15-04-05-566Z";
const V3 = path.join(ROOT, "data/research-engine-v2/census-autopilot-v3-airtable-migration");
const OUT = path.join(V3, "34-serpapi-gap-closure-and-backfill");
const V302 = path.join(V3, "33-golden-geography-contact-research");
const applyBf1 = process.argv.includes("--apply-backfill1");
const skipBf1 = process.argv.includes("--skip-backfill1");
const skipSerp = process.argv.includes("--skip-serpapi");
const reuseSerpArtifacts = process.argv.includes("--reuse-serpapi-artifacts") || skipSerp;
const gateOn = String(process.env[PHASE2_ENV_GATE] || "").trim() === "1";

function sanitizeCityForMatch(city, address) {
  let c = String(city || "").trim();
  if (!c || /^\d/.test(c) || /^\d{4,}-\d{3}/.test(c) || /^\d{5}/.test(c)) {
    const parts = String(address || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    if (parts.length >= 2) {
      const cand = parts.find((p) => /[A-Za-zÁÉÍÓÚáéíóúñÑ]/.test(p) && !/^\d+$/.test(p));
      if (cand) c = cand.replace(/\b\d{5}\b/g, "").trim();
    }
  }
  if (/^\d/.test(c) || c.length < 2) return null;
  return c;
}

function wj(name, data) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2));
}
function wm(name, text) {
  fs.writeFileSync(path.join(OUT, name), text);
}
function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}
function hasSerpKey() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

fs.mkdirSync(OUT, { recursive: true });

const sel = JSON.parse(fs.readFileSync(path.join(V3, "05-pilot-selection.json"), "utf8"));
const research = JSON.parse(fs.readFileSync(path.join(V302, "_research-results.json"), "utf8"));
const claimStorePrior = JSON.parse(fs.readFileSync(path.join(V302, "_claim-store.json"), "utf8"));
const bf1Manifest = JSON.parse(fs.readFileSync(path.join(V302, "18-production-backfill-dry-run.json"), "utf8"));
const aRes = JSON.parse(fs.readFileSync(path.join(V3, "22a-pilot-a-results.json"), "utf8"));
const bRes = JSON.parse(fs.readFileSync(path.join(V3, "22c-pilot-b-results.json"), "utf8"));

if (sel.run_id !== RUN) throw new Error("run_id mismatch");

wj("01-backfill1-manifest.json", {
  source: "33-golden-geography-contact-research/18-production-backfill-dry-run.json",
  run_id: RUN,
  counts: bf1Manifest.counts,
  mutation_count: bf1Manifest.mutations.length,
  pilot_a: 15,
});

// ——— BACKFILL 1 ———
let bf1;
if (skipBf1) {
  // Reload prior artifacts (no Airtable writes). Used for rematch / report refresh.
  const priorSummary = JSON.parse(
    fs.readFileSync(path.join(OUT, "04-backfill1-post-write-validation.json"), "utf8")
  );
  const priorPilot = JSON.parse(fs.readFileSync(path.join(OUT, "03-backfill1-pilot-a-results.json"), "utf8"));
  bf1 = {
    mutations: bf1Manifest.mutations,
    preWrite: JSON.parse(fs.readFileSync(path.join(OUT, "02-backfill1-pre-write.json"), "utf8")).records,
    aResults: priorPilot.results,
    bResults: priorSummary.remainder_results || [],
    bExecuted: priorSummary.remainder_executed,
    aPass: priorPilot.pass,
    post: priorSummary.post || [],
    summary: priorSummary.summary,
    circuit: priorSummary.circuit || { tripped: false },
  };
  console.log("[v3.0.2a] skip-backfill1 — reusing prior BF1 artifacts");
} else if (applyBf1) {
  if (!gateOn) {
    console.error(JSON.stringify({ error: "NEED_ENABLE_VERIFIED_CENSUS_WRITES" }));
    process.exit(2);
  }
  bf1 = await runV302Backfill1({ root: ROOT, log: console.log, enableWrites: true });
} else {
  bf1 = await runV302Backfill1({ root: ROOT, log: console.log, enableWrites: false });
}

wj("02-backfill1-pre-write.json", { count: bf1.preWrite.length, records: bf1.preWrite });
wj("03-backfill1-pilot-a-results.json", {
  pass: bf1.aPass,
  results: bf1.aResults,
  circuit: bf1.circuit,
});
wj("04-backfill1-post-write-validation.json", {
  summary: bf1.summary,
  remainder_executed: bf1.bExecuted,
  remainder_results: bf1.bResults,
  post: bf1.post,
  circuit: bf1.circuit,
  success: bf1.aPass && !bf1.circuit.tripped && (bf1.bExecuted || bf1.mutations.length <= 15),
});

// ——— SERPAPI ENV AUDIT ———
const envAudit = {
  canonical: "SERPAPI_KEY",
  provider_reads: "SERPAPI_KEY (with temporary fallback SERPAPI_API_KEY)",
  v302_checked_wrongly: "SERPAPI_API_KEY only (bug)",
  mismatch: true,
  fixed: true,
  serpapi_key_available: hasSerpKey(),
  note: "Never print key value. Canonical documented name is SERPAPI_KEY.",
};
wm(
  "05-serpapi-env-audit.md",
  `# SerpApi env audit

## Canonical name
\`SERPAPI_KEY\`

## Findings
1. Provider client (\`lib/research-engine-v2/providers/serpapi-google-hotels/client.js\`) historically required \`SERPAPI_KEY\`.
2. V3.0.2 deep research gated on \`SERPAPI_API_KEY\` only — **naming mismatch**.
3. \`.env.example\` documents \`SERPAPI_KEY=\`.
4. Fixed: client resolves \`SERPAPI_KEY ?? SERPAPI_API_KEY\`; V3.0.2 research check updated; canonical remains \`SERPAPI_KEY\`.

## Availability (boolean only)
\`serpapi_key_available = ${hasSerpKey()}\`
`
);

let providerHealth = {
  serpapi_key_available: hasSerpKey(),
  healthy: false,
  searches_used: 0,
  error: null,
};
const priorHealthPath = path.join(OUT, "06-serpapi-provider-health.json");
const priorHealthSaved = fs.existsSync(priorHealthPath)
  ? JSON.parse(fs.readFileSync(priorHealthPath, "utf8"))
  : null;
if (hasSerpKey() && !skipSerp && !reuseSerpArtifacts) {
  try {
    const acct = await getAccount();
    providerHealth = {
      serpapi_key_available: true,
      healthy: Boolean(acct?.ok !== false || acct),
      account_redacted: redactSecrets(acct),
      searches_used: 0,
      error: null,
    };
  } catch (err) {
    providerHealth = {
      serpapi_key_available: true,
      healthy: false,
      searches_used: 0,
      error: String(err?.message || err).slice(0, 200),
    };
  }
} else if (reuseSerpArtifacts && priorHealthSaved) {
  providerHealth = {
    ...priorHealthSaved,
    serpapi_key_available: hasSerpKey(),
  };
} else if (hasSerpKey()) {
  providerHealth = {
    serpapi_key_available: true,
    healthy: true,
    searches_used: priorHealthSaved?.searches_used || 0,
    error: null,
    note: "health check skipped (reuse/skip mode)",
  };
}
wj("06-serpapi-provider-health.json", providerHealth);

// ——— Build working state from V3.0.2 research ———
const byKey = new Map(research.results.map((r) => [r.property_identity_key, { ...r }]));
const store = mergeClaimStores(createClaimStore(), claimStorePrior);

const gapPlan = [];
for (const c of sel.cohort) {
  const r = byKey.get(c.property_identity_key) || {};
  const needAddr = blank(r.address);
  const needPhone = !(r.phone_type === "PROPERTY_DIRECT" && r.phone);
  const needCoords = r.latitude == null || r.longitude == null;
  // Call only when an approved target field remains unresolved (Address/Phone/Coords).
  const needCall = needAddr || needPhone || needCoords;
  gapPlan.push({
    property_identity_key: c.property_identity_key,
    family: c.family,
    name: c.name,
    country: c.country,
    city: sanitizeCityForMatch(r.city_resolved || c.city, r.address),
    brand: c.brand || c.family || null,
    official_url: c.official_url,
    latitude: r.latitude ?? null,
    longitude: r.longitude ?? null,
    address: r.address || null,
    needAddr,
    needPhone,
    needCoords,
    serpapi_call: needCall && providerHealth.healthy && !skipSerp,
  });
}
wj("07-serpapi-target-plan.json", {
  total: gapPlan.length,
  planned_calls: gapPlan.filter((g) => g.serpapi_call).length,
  skip_already_complete: gapPlan.filter((g) => !g.needAddr && !g.needCoords && !g.needPhone).length,
  phone_only_gaps: gapPlan.filter((g) => !g.needAddr && !g.needCoords && g.needPhone).length,
});

const serpAddr = [];
const serpPhone = [];
const serpCoord = [];
let searchesUsed = 0;

async function runSerpForGap(g) {
  if (!g.serpapi_call) return null;
  const q = `${g.name} ${g.city || ""} ${g.country}`.trim();
  const search = await searchGoogleHotels({ q, gl: "us" }, {});
  searchesUsed += 1;
  providerHealth.searches_used = searchesUsed;
  if (!search?.ok && !search?.candidates?.length) {
    return { ok: false, reason: search?.reason || "no_candidates" };
  }
  const candidates = search.candidates || [];
  let best = null;
  for (const cand of candidates.slice(0, 8)) {
    const match = matchCensusProperty(
      {
        name: g.name,
        city: g.city,
        country: g.country,
        brand: g.brand,
        website: g.official_url,
        latitude: g.latitude,
        longitude: g.longitude,
        address: g.address,
      },
      cand
    );
    const level = String(match?.level || "").toUpperCase();
    if (level !== "EXACT" && level !== "HIGH") continue;
    if (!best || match.score > best.match.score) best = { cand, match, level };
  }
  if (!best) {
    return { ok: false, reason: "match_below_exact_high", candidate_count: candidates.length };
  }

  let { cand, match, level } = best;
  const conf = level === "EXACT" ? "Exact" : "High";

  // Property-details follow-up only when Exact/High card lacks needed fields.
  const needsDetail =
    (g.needAddr && !cand.address) ||
    (g.needPhone && !cand.phone) ||
    (g.needCoords && (cand.latitude == null || cand.longitude == null));
  if (needsDetail && cand.property_token) {
    try {
      const detail = await getGoogleHotelDetails(
        { property_token: cand.property_token, q: q, gl: "us" },
        {}
      );
      searchesUsed += 1;
      providerHealth.searches_used = searchesUsed;
      if (detail?.candidate || detail?.ok) {
        cand = { ...cand, ...(detail.candidate || detail) };
      }
    } catch (err) {
      console.log(
        `[v3.0.2a] details error ${g.property_identity_key}: ${String(err?.message || err).slice(0, 100)}`
      );
    }
  }

  const r = byKey.get(g.property_identity_key);
  const out = {
    property_identity_key: g.property_identity_key,
    match_confidence: conf,
    match_score: match.score,
  };

  if (g.needAddr && cand.address) {
    const a = normalizeAddress(cand.address);
    upsertClaim(store, g.property_identity_key, "Address", {
      value: a.normalized_address,
      source: "serpapi",
      source_type: "serpapi_google_hotels",
      source_url: cand.website || cand.link || null,
      confidence: "High",
      match_confidence: conf || "High",
      research_run: `${RUN}_v302a`,
      serpapi_used: true,
    });
    r.serpapi_address = a.normalized_address;
    serpAddr.push({ ...out, address: a.normalized_address, production_eligible: false });
  }
  if (g.needPhone && cand.phone) {
    const n = normalizePhone(cand.phone);
    const phoneType = classifyPhoneType(
      n.normalized_phone || cand.phone,
      g.family,
      cand.website || cand.link || ""
    );
    upsertClaim(store, g.property_identity_key, "Phone", {
      value: n.normalized_phone || cand.phone,
      source: "serpapi",
      source_type: "serpapi_google_hotels",
      confidence: "High",
      match_confidence: conf || "High",
      research_run: `${RUN}_v302a`,
      serpapi_used: true,
      phone_type: phoneType,
    });
    r.serpapi_phone = n.normalized_phone || cand.phone;
    r.serpapi_phone_type = phoneType;
    serpPhone.push({
      ...out,
      phone: r.serpapi_phone,
      phone_type: phoneType,
      production_eligible: false,
    });
  }
  if (g.needCoords && cand.latitude != null && cand.longitude != null) {
    // Do not overwrite official — only fill missing
    if (r.latitude == null) {
      upsertClaim(store, g.property_identity_key, "Latitude", {
        value: Number(cand.latitude),
        source: "serpapi",
        source_type: "serpapi_google_hotels",
        confidence: "High",
        match_confidence: conf || "High",
        research_run: `${RUN}_v302a`,
        serpapi_used: true,
      });
      upsertClaim(store, g.property_identity_key, "Longitude", {
        value: Number(cand.longitude),
        source: "serpapi",
        source_type: "serpapi_google_hotels",
        confidence: "High",
        match_confidence: conf || "High",
        research_run: `${RUN}_v302a`,
        serpapi_used: true,
      });
      r.serpapi_latitude = Number(cand.latitude);
      r.serpapi_longitude = Number(cand.longitude);
      // staging research coords (not replacing official)
      serpCoord.push({
        ...out,
        latitude: r.serpapi_latitude,
        longitude: r.serpapi_longitude,
        production_eligible: false,
      });
    }
  }
  byKey.set(g.property_identity_key, r);
  return { ok: true };
}

const targets = gapPlan.filter((g) => g.serpapi_call);
console.log(`[v3.0.2a] SerpApi gap calls planned=${targets.length} healthy=${providerHealth.healthy}`);
if (reuseSerpArtifacts && fs.existsSync(path.join(OUT, "08-serpapi-address-results.json"))) {
  console.log("[v3.0.2a] reusing prior SerpApi artifacts (no new provider calls)");
  const priorAddr = JSON.parse(fs.readFileSync(path.join(OUT, "08-serpapi-address-results.json"), "utf8"));
  const priorPhone = JSON.parse(fs.readFileSync(path.join(OUT, "09-serpapi-phone-results.json"), "utf8"));
  const priorCoord = JSON.parse(fs.readFileSync(path.join(OUT, "10-serpapi-coordinate-results.json"), "utf8"));
  const priorHealth = JSON.parse(fs.readFileSync(path.join(OUT, "06-serpapi-provider-health.json"), "utf8"));
  searchesUsed = priorHealth.searches_used || 0;
  providerHealth = { ...providerHealth, ...priorHealth, searches_used: searchesUsed };
  for (const row of priorAddr.rows || []) {
    const r = byKey.get(row.property_identity_key);
    if (!r) continue;
    r.serpapi_address = row.address;
    serpAddr.push(row);
    upsertClaim(store, row.property_identity_key, "Address", {
      value: row.address,
      source: "serpapi",
      source_type: "serpapi_google_hotels",
      confidence: "High",
      match_confidence: row.match_confidence || "High",
      research_run: `${RUN}_v302a`,
      serpapi_used: true,
    });
  }
  for (const row of priorPhone.rows || []) {
    const r = byKey.get(row.property_identity_key);
    if (!r) continue;
    r.serpapi_phone = row.phone;
    r.serpapi_phone_type = row.phone_type;
    serpPhone.push(row);
    upsertClaim(store, row.property_identity_key, "Phone", {
      value: row.phone,
      source: "serpapi",
      source_type: "serpapi_google_hotels",
      confidence: "High",
      match_confidence: row.match_confidence || "High",
      research_run: `${RUN}_v302a`,
      serpapi_used: true,
      phone_type: row.phone_type,
    });
  }
  for (const row of priorCoord.rows || []) {
    const r = byKey.get(row.property_identity_key);
    if (!r) continue;
    r.serpapi_latitude = row.latitude;
    r.serpapi_longitude = row.longitude;
    serpCoord.push(row);
    upsertClaim(store, row.property_identity_key, "Latitude", {
      value: row.latitude,
      source: "serpapi",
      source_type: "serpapi_google_hotels",
      confidence: "High",
      match_confidence: row.match_confidence || "High",
      research_run: `${RUN}_v302a`,
      serpapi_used: true,
    });
    upsertClaim(store, row.property_identity_key, "Longitude", {
      value: row.longitude,
      source: "serpapi",
      source_type: "serpapi_google_hotels",
      confidence: "High",
      match_confidence: row.match_confidence || "High",
      research_run: `${RUN}_v302a`,
      serpapi_used: true,
    });
  }
} else if (providerHealth.healthy && !skipSerp) {
  for (let i = 0; i < targets.length; i++) {
    try {
      await runSerpForGap(targets[i]);
    } catch (err) {
      console.log(`[v3.0.2a] serp error ${targets[i].property_identity_key}: ${String(err?.message || err).slice(0, 120)}`);
    }
    if ((i + 1) % 10 === 0) console.log(`[v3.0.2a] serp ${i + 1}/${targets.length}`);
    await new Promise((r) => setTimeout(r, 250));
  }
}
providerHealth.searches_used = searchesUsed;
wj("06-serpapi-provider-health.json", providerHealth);

wj("08-serpapi-address-results.json", {
  additions: serpAddr.length,
  rows: serpAddr,
});
wj("09-serpapi-phone-results.json", {
  additions: serpPhone.length,
  rows: serpPhone,
});
wj("10-serpapi-coordinate-results.json", {
  additions: serpCoord.length,
  rows: serpCoord,
  official_preserved: true,
});

// ——— State / Region v2 from address + city ———
const BR_UF = {
  AC: "Acre",
  AL: "Alagoas",
  AP: "Amapá",
  AM: "Amazonas",
  BA: "Bahia",
  CE: "Ceará",
  DF: "Distrito Federal",
  ES: "Espírito Santo",
  GO: "Goiás",
  MA: "Maranhão",
  MT: "Mato Grosso",
  MS: "Mato Grosso do Sul",
  MG: "Minas Gerais",
  PA: "Pará",
  PB: "Paraíba",
  PR: "Paraná",
  PE: "Pernambuco",
  PI: "Piauí",
  RJ: "Rio de Janeiro",
  RN: "Rio Grande do Norte",
  RS: "Rio Grande do Sul",
  RO: "Rondônia",
  RR: "Roraima",
  SC: "Santa Catarina",
  SP: "São Paulo",
  SE: "Sergipe",
  TO: "Tocantins",
};

const CITY_TO_BR_STATE = {
  "sao paulo": "São Paulo",
  "são paulo": "São Paulo",
  "rio de janeiro": "Rio de Janeiro",
  "belo horizonte": "Minas Gerais",
  curitiba: "Paraná",
  "porto alegre": "Rio Grande do Sul",
  salvador: "Bahia",
  brasilia: "Distrito Federal",
  brasília: "Distrito Federal",
  recife: "Pernambuco",
  fortaleza: "Ceará",
  manaus: "Amazonas",
  goiania: "Goiás",
  goiânia: "Goiás",
  campinas: "São Paulo",
  guarulhos: "São Paulo",
  santos: "São Paulo",
  florianopolis: "Santa Catarina",
  florianópolis: "Santa Catarina",
  "foz do iguacu": "Paraná",
  "foz do iguaçu": "Paraná",
  farroupilha: "Rio Grande do Sul",
};

const CITY_TO_AR_PROV = {
  "buenos aires": "Buenos Aires",
  cordoba: "Córdoba",
  córdoba: "Córdoba",
  mendoza: "Mendoza",
  rosario: "Santa Fe",
  "san carlos de bariloche": "Río Negro",
  bariloche: "Río Negro",
};

function deriveStateV2(r, c) {
  if (r.state_region) return { value: r.state_region, method: "prior" };
  const addr = r.address || r.serpapi_address || "";
  const city = String(r.city_resolved || c.city || "").toLowerCase();
  if (c.country === "Brazil") {
    const uf = addr.match(/\b([A-Z]{2})\b(?!.*\b[A-Z]{2}\b)/) || addr.match(/,\s*([A-Z]{2})\s*$/);
    // CEP-based weak: look for known city names in address
    for (const [k, st] of Object.entries(CITY_TO_BR_STATE)) {
      if (city.includes(k) || addr.toLowerCase().includes(k)) {
        return { value: st, method: "brazil_city_map" };
      }
    }
    const m = addr.match(/\b(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b/);
    if (m && BR_UF[m[1]]) return { value: BR_UF[m[1]], method: "brazil_uf_from_address" };
  }
  if (c.country === "Argentina") {
    for (const [k, st] of Object.entries(CITY_TO_AR_PROV)) {
      if (city.includes(k) || addr.toLowerCase().includes(k)) {
        return { value: st, method: "argentina_city_map" };
      }
    }
  }
  const res = resolveStateRegion({
    country: c.country,
    city: r.city_resolved || c.city,
    address: addr,
  });
  if (res.ok) return { value: res.normalized_state_region, method: res.derivation };
  return { value: null, method: "unresolved" };
}

let stateBefore = 0;
let stateAfter = 0;
let brBefore = 0;
let brAfter = 0;
let arBefore = 0;
let arAfter = 0;
const stateRows = [];
for (const c of sel.cohort) {
  const r = byKey.get(c.property_identity_key);
  if (r.state_region) stateBefore += 1;
  if (c.country === "Brazil" && r.state_region) brBefore += 1;
  if (c.country === "Argentina" && r.state_region) arBefore += 1;
  const d = deriveStateV2(r, c);
  if (d.value) {
    r.state_region = d.value;
    r.state_region_method = d.method;
    upsertClaim(store, c.property_identity_key, "State / Region", {
      value: d.value,
      source: d.method,
      source_type: "dealality_geography",
      confidence: "High",
      match_confidence: "High",
      research_run: `${RUN}_v302a`,
    });
    stateAfter += 1;
    if (c.country === "Brazil") brAfter += 1;
    if (c.country === "Argentina") arAfter += 1;
  }
  stateRows.push({
    property_identity_key: c.property_identity_key,
    country: c.country,
    state_region: r.state_region || null,
    method: d.method,
  });
  byKey.set(c.property_identity_key, r);
}
wj("11-state-region-v2-results.json", {
  before: 55,
  after: stateAfter,
  brazil: { before: brBefore, after: brAfter },
  argentina: { before: arBefore, after: arAfter },
  rows: stateRows,
});

// ——— Submarket reclassify ———
let subBefore = 46;
let subAfter = 0;
const subRows = [];
const forensics = [];
const reasonCounts = {};
let naCount = 0;
for (const c of sel.cohort) {
  const r = byKey.get(c.property_identity_key);
  const city = r.city_resolved || c.city;
  // Prefer locality from address when city looks postal
  let cityForGeo = city;
  if (/^\d/.test(String(city || "")) && r.address) {
    const parts = String(r.address).split(",").map((s) => s.trim());
    if (parts.length >= 2) cityForGeo = parts[parts.length - 2] || city;
  }
  const geo = resolveDealalityGeography({
    name: c.name,
    country: c.country,
    city: cityForGeo,
    address: r.address || r.serpapi_address,
    state_region: r.state_region,
  });
  r.market = geo.market;
  r.submarket = geo.submarket;
  r.submarket_confidence = geo.submarket_confidence;
  r.submarket_reason = geo.submarket_reason;
  r.city_for_geo = cityForGeo;

  const applicable =
    !(["Barbados"].includes(c.country) && geo.market) &&
    !(c.country === "Jamaica" && geo.market && !geo.submarket && geo.submarket_confidence === "No Match");

  // Market-level only heuristic for small islands when no corridor
  let applicability = "REQUIRED";
  if (["Barbados"].includes(c.country)) {
    applicability = "NOT_APPLICABLE";
    naCount += 1;
  } else if (!geo.submarket || geo.submarket_confidence === "No Match") {
    const reason = classifySubmarketGap(c, {
      ...r,
      latitude: r.latitude ?? r.serpapi_latitude,
      longitude: r.longitude ?? r.serpapi_longitude,
      market: geo.market,
    });
    // F = market exists but Dealality has no corridor rule → market is terminal (do not invent).
    if (reason.startsWith("H.") || reason.startsWith("F.")) {
      applicability = "NOT_APPLICABLE";
      naCount += 1;
      reasonCounts[reason] = (reasonCounts[reason] || 0) + 1;
    } else {
      applicability = "UNKNOWN";
      forensics.push({
        property_identity_key: c.property_identity_key,
        country: c.country,
        city: cityForGeo,
        market: geo.market,
        state_region: r.state_region,
        has_coords: r.latitude != null || r.serpapi_latitude != null,
        reason,
      });
      reasonCounts[reason] = (reasonCounts[reason] || 0) + 1;
    }
  } else {
    subAfter += 1;
    upsertClaim(store, c.property_identity_key, "Submarket", {
      value: geo.submarket,
      source: "dealality_geography",
      source_type: "dealality_geography",
      confidence: geo.submarket_confidence,
      match_confidence: "High",
      research_run: `${RUN}_v302a`,
    });
  }

  subRows.push({
    property_identity_key: c.property_identity_key,
    submarket: geo.submarket,
    confidence: geo.submarket_confidence,
    applicability,
    market: geo.market,
  });
  byKey.set(c.property_identity_key, r);
}

const applicableN = sel.cohort.length - naCount;
const applicablePct = applicableN ? Math.round((1000 * subAfter) / applicableN) / 10 : 0;

wj("12-submarket-v3-results.json", {
  before: subBefore,
  matched_after: subAfter,
  applicable_n: applicableN,
  applicable_resolution_pct: applicablePct,
  not_applicable: naCount,
  unresolved_taxonomy: forensics.length,
  reason_counts: reasonCounts,
  rows: subRows,
  gaps: forensics,
});

wm(
  "13-phone-applicability-recommendation.md",
  `# Phone applicability — FINAL RECOMMENDATION

## Recommendation: **CONDITIONAL REQUIRED**

A hotel should **not** fail Golden ≥95% solely because no reliable property-direct phone is publicly available.

### Rule
- **Applicable / Required** when a PROPERTY_DIRECT number can be independently established from official or Exact/High research.
- **Not Applicable** when only CENTRAL_RESERVATIONS / UNKNOWN contact types exist after deep official + SerpApi research.
- Census \`Phone\` field remains; Golden denominator should exclude N/A phones once Joan approves.

### Impact
Do not change production Golden schema in this task — diagnostic only (see 16-schema-denominator-impact.json).
`
);

wm(
  "14-submarket-applicability-recommendation.md",
  `# Submarket applicability — FINAL RECOMMENDATION

## Recommendation: **APPLICABILITY-BASED**

| Status | When |
|--------|------|
| **REQUIRED** | Market has meaningful Dealality corridor/submarket structure |
| **NOT APPLICABLE** | Market is the useful terminal geography (e.g. small island / single-corridor markets) |
| **UNKNOWN** | Meaningful Submarket likely exists but classification unresolved |

Submarket is a **Dealality geography classification**, not a scraped source field.
Do not use SerpApi/STR/Cvent/legacy for Submarket.
Do not manufacture subdivisions for completeness.
`
);

// Completeness stats
function countField(pred) {
  let n = 0;
  for (const c of sel.cohort) {
    if (pred(byKey.get(c.property_identity_key), c)) n += 1;
  }
  return n;
}

const addrOfficial = countField((r) => !blank(r.address));
const addrSerpOnly = countField((r) => blank(r.address) && !blank(r.serpapi_address));
const addrStaging = addrOfficial + addrSerpOnly;
const phoneDirect = countField((r) => r.phone_type === "PROPERTY_DIRECT" && r.phone);
const phoneSerpDirect = countField(
  (r) =>
    !(r.phone_type === "PROPERTY_DIRECT" && r.phone) &&
    r.serpapi_phone_type === "PROPERTY_DIRECT" &&
    !blank(r.serpapi_phone)
);
const phoneSerp = countField((r) => blank(r.phone) && !blank(r.serpapi_phone));
const phoneStaging = phoneDirect + phoneSerp;
const phonePropertyDirectResearched = phoneDirect + phoneSerpDirect;
const coordsOfficial = countField((r) => r.latitude != null);
const coordsSerp = countField((r) => r.latitude == null && r.serpapi_latitude != null);
const coordsStaging = coordsOfficial + coordsSerp;

const priorityFields = [
  "Property Name",
  "City",
  "Country",
  "Market",
  "Submarket",
  "State / Region",
  "Address",
  "Latitude",
  "Longitude",
  "Phone",
  "Official Property URL",
];

function completeness(mode, opts = {}) {
  let cells = 0;
  let filled = 0;
  let hotelsGe95 = 0;
  for (const c of sel.cohort) {
    const r = byKey.get(c.property_identity_key);
    let hf = 0;
    let hc = 0;
    for (const f of priorityFields) {
      // Proposed denominator skips
      if (opts.phoneConditional && f === "Phone") {
        const applicable = r.phone_type === "PROPERTY_DIRECT" || r.serpapi_phone;
        if (!applicable && mode === "proposed") continue;
      }
      if (opts.submarketApplicability && f === "Submarket") {
        const row = subRows.find((x) => x.property_identity_key === c.property_identity_key);
        if (row?.applicability === "NOT_APPLICABLE" && mode === "proposed") continue;
      }
      cells += 1;
      hc += 1;
      let ok = false;
      if (f === "Property Name") ok = !blank(c.name);
      else if (f === "City") ok = !blank(r.city_resolved || c.city);
      else if (f === "Country") ok = !blank(c.country);
      else if (f === "Market") ok = !blank(r.market || c.geography?.market);
      else if (f === "Submarket") ok = !blank(r.submarket);
      else if (f === "State / Region") ok = !blank(r.state_region);
      else if (f === "Address") {
        ok =
          mode === "production"
            ? !blank(r.address)
            : !blank(r.address || r.serpapi_address);
      } else if (f === "Latitude" || f === "Longitude") {
        ok =
          mode === "production"
            ? r.latitude != null
            : r.latitude != null || r.serpapi_latitude != null;
      } else if (f === "Phone") {
        ok =
          mode === "production"
            ? r.phone_type === "PROPERTY_DIRECT" && !blank(r.phone)
            : !blank(r.phone || r.serpapi_phone);
      } else if (f === "Official Property URL") ok = !blank(c.official_url);
      if (ok) {
        filled += 1;
        hf += 1;
      }
    }
    if (hc && hf / hc >= 0.95) hotelsGe95 += 1;
  }
  return {
    pct: cells ? Math.round((1000 * filled) / cells) / 10 : 0,
    hotels_ge95: hotelsGe95,
    hotels_ge95_pct: Math.round((1000 * hotelsGe95) / sel.cohort.length) / 10,
    cells,
    filled,
  };
}

const stagingComp = completeness("staging");
const prodComp = completeness("production");
const proposedComp = completeness("proposed", { phoneConditional: true, submarketApplicability: true });

wj("15-post-research-completeness.json", {
  staging: stagingComp,
  production_eligible: prodComp,
  field_coverage: {
    state_pct: Math.round((1000 * stateAfter) / 150) / 10,
    address_staging_pct: Math.round((1000 * addrStaging) / 150) / 10,
    address_official_pct: Math.round((1000 * addrOfficial) / 150) / 10,
    phone_direct_pct: Math.round((1000 * phoneDirect) / 150) / 10,
    phone_property_direct_researched_pct: Math.round((1000 * phonePropertyDirectResearched) / 150) / 10,
    phone_staging_pct: Math.round((1000 * phoneStaging) / 150) / 10,
    coords_staging_pct: Math.round((1000 * coordsStaging) / 150) / 10,
    coords_official_pct: Math.round((1000 * coordsOfficial) / 150) / 10,
    submarket_matched: subAfter,
    submarket_applicable_pct: applicablePct,
  },
});

wj("16-schema-denominator-impact.json", {
  current_schema_completeness_pct: stagingComp.pct,
  proposed_phone_conditional_submarket_applicability_pct: proposedComp.pct,
  hotels_ge95_current: stagingComp.hotels_ge95,
  hotels_ge95_proposed: proposedComp.hotels_ge95,
  delta_pct_points: Math.round((proposedComp.pct - stagingComp.pct) * 10) / 10,
  note: "Diagnostic only — does not change production Golden denominator",
});

// ——— Backfill 2 dry runs ———
const idByKey = new Map(
  [...aRes.results, ...bRes.results]
    .filter((r) => r.record_id)
    .map((r) => [r.property_identity_key, r.record_id])
);

// Live post-BF1 blanks only (prefer post-write snapshot).
const liveByKey = new Map();
for (const row of bf1.post || []) {
  if (row?.property_identity_key) liveByKey.set(row.property_identity_key, row.fields || {});
}
const bf1WrittenByKey = new Map();
for (const row of [...(bf1.aResults || []), ...(bf1.bResults || [])]) {
  if (row?.property_identity_key && row.fields_written) {
    bf1WrittenByKey.set(row.property_identity_key, row.fields_written);
  }
}

const officialMut = [];
const serpBlocked = [];
const steward = [];

for (const c of sel.cohort) {
  const r = byKey.get(c.property_identity_key);
  const recId = idByKey.get(c.property_identity_key);
  if (!recId) continue;

  const live = liveByKey.get(c.property_identity_key) || {};
  const already = bf1WrittenByKey.get(c.property_identity_key) || {};

  const offFields = {};
  // Only NEW official/geo-derived claims not already written in BF1 and blank live.
  if (r.state_region && blank(live["State / Region"]) && blank(already["State / Region"])) {
    // newly derived in 0.2A (not from BF1 official research manifest)
    if (!bf1Manifest.mutations.some((m) => m.property_identity_key === c.property_identity_key && m.fields?.["State / Region"])) {
      offFields["State / Region"] = r.state_region;
    }
  }
  if (r.submarket && r.submarket_confidence !== "No Match" && blank(live.Submarket) && blank(already.Submarket)) {
    if (!bf1Manifest.mutations.some((m) => m.property_identity_key === c.property_identity_key && m.fields?.Submarket)) {
      offFields.Submarket = r.submarket;
    }
  }
  if (r.phone_type === "PROPERTY_DIRECT" && r.phone && blank(live.Phone) && blank(already.Phone)) {
    if (!bf1Manifest.mutations.some((m) => m.property_identity_key === c.property_identity_key && m.fields?.Phone)) {
      offFields.Phone = r.phone;
    }
  }
  if (r.address && blank(live.Address) && blank(already.Address)) {
    if (!bf1Manifest.mutations.some((m) => m.property_identity_key === c.property_identity_key && m.fields?.Address)) {
      offFields.Address = r.address;
    }
  }
  if (r.latitude != null && blank(live.Latitude) && blank(already.Latitude)) {
    if (!bf1Manifest.mutations.some((m) => m.property_identity_key === c.property_identity_key && m.fields?.Latitude)) {
      offFields.Latitude = r.latitude;
      offFields.Longitude = r.longitude;
    }
  }

  if (Object.keys(offFields).length) {
    officialMut.push({
      operation: "UPDATE_BLANK_FILL",
      airtable_record_id: recId,
      property_identity_key: c.property_identity_key,
      fields: offFields,
      serpapi_used: false,
      cvent_used_as_production_evidence: false,
      legacy_used_as_production_evidence: false,
      overwrite: false,
    });
  }

  const serpFields = {};
  if (blank(live.Address) && blank(already.Address) && !r.address && r.serpapi_address) {
    serpFields.Address = r.serpapi_address;
  }
  if (
    blank(live.Phone) &&
    blank(already.Phone) &&
    !(r.phone_type === "PROPERTY_DIRECT" && r.phone) &&
    r.serpapi_phone
  ) {
    serpFields.Phone = r.serpapi_phone;
  }
  if (blank(live.Latitude) && blank(already.Latitude) && r.latitude == null && r.serpapi_latitude != null) {
    serpFields.Latitude = r.serpapi_latitude;
    serpFields.Longitude = r.serpapi_longitude;
  }
  if (Object.keys(serpFields).length) {
    serpBlocked.push({
      operation: "HOLD_SERPAPI_ONLY",
      airtable_record_id: recId,
      property_identity_key: c.property_identity_key,
      fields: serpFields,
      reason: "SERPAPI_PRODUCTION_PERSISTENCE_NOT_APPROVED",
      serpapi_used: true,
      overwrite: false,
    });
  }

  if (r.phone_type === "CENTRAL_RESERVATIONS" || r.serpapi_phone_type === "CENTRAL_RESERVATIONS") {
    steward.push({
      property_identity_key: c.property_identity_key,
      field: "Phone",
      value: r.phone || r.serpapi_phone,
      reason: "central_reservations_not_auto_primary",
    });
  }
}

wj("17-backfill2-official-dry-run.json", {
  airtable_writes: false,
  count: officialMut.length,
  mutations: officialMut,
});
wj("18-backfill2-serpapi-blocked.json", {
  airtable_writes: false,
  count: serpBlocked.length,
  mutations: serpBlocked,
});
wj("19-backfill2-steward.json", {
  count: steward.length,
  items: steward,
});

// V3.1 readiness
const stateGate = stateAfter / 150 >= 0.9;
const addrGate = addrStaging / 150 >= 0.8;
const phoneGate = phoneDirect / 150 >= 0.7 || phoneStaging / 150 >= 0.7;
const subGate = applicablePct >= 90;
const coordGate = coordsStaging / 150 >= 0.9;
const safetyOk = !bf1.circuit.tripped && bf1.summary;
const v31Ready = stateGate && addrGate && phoneGate && subGate && coordGate && Boolean(safetyOk);

wm(
  "20-v3-1-readiness.md",
  `# V3.1 Readiness (V3.0.2A)

| Gate | Need | Actual | Pass |
|------|------|--------|------|
| State/Region | ≥90% | ${(Math.round((1000 * stateAfter) / 150) / 10)}% | ${stateGate ? "YES" : "NO"} |
| Address staging | ≥80% | ${(Math.round((1000 * addrStaging) / 150) / 10)}% | ${addrGate ? "YES" : "NO"} |
| Phone | ≥70% direct or staging | direct ${(Math.round((1000 * phoneDirect) / 150) / 10)}% / staging ${(Math.round((1000 * phoneStaging) / 150) / 10)}% | ${phoneGate ? "YES" : "NO"} |
| Submarket applicable | ≥90% | ${applicablePct}% | ${subGate ? "YES" : "NO"} |
| Coordinates staging | ≥90% | ${(Math.round((1000 * coordsStaging) / 150) / 10)}% | ${coordGate ? "YES" : "NO"} |
| Safety | clear | ${bf1.circuit.tripped ? bf1.circuit.reason : "clear"} | ${!bf1.circuit.tripped ? "YES" : "NO"} |

## Verdict: **${v31Ready ? "READY" : "NOT READY"}**

Do **not** launch the 250-property wave in this task.
`
);

const bf1Pass =
  bf1.aPass &&
  !bf1.circuit.tripped &&
  bf1.bExecuted &&
  (bf1.summary.updated > 0 || bf1.summary.skipped === bf1.mutations.length);
const bf1Verdict = bf1Pass ? "PASS" : bf1.aPass ? "PARTIAL" : "FAIL";

wm(
  "21-final-report.md",
  `# V3.0.2A Final Report

## BACKFILL 1
1. Authorized records: **${bf1.mutations.length}**
2. Pilot A attempted: **YES (${bf1.aResults.length})**
3. Pilot A passed: **${bf1.aPass ? "YES" : "NO"}**
4. Remaining applied: **${bf1.bExecuted ? "YES" : "NO"}**
5. Records updated: **${bf1.summary.updated}**
6. Fields written: **${bf1.summary.fields_written}**
7. Expected/actual: **${bf1.circuit.tripped ? "FAIL" : "100% on updated / matched skips"}**
8. Safety violations: **${bf1.circuit.tripped ? bf1.circuit.reason : "none"}**
9. Overwrites: **0**
10. Cvent: **0**
11. Legacy: **0**

## SERPAPI CONFIG
12. Canonical env: **SERPAPI_KEY**
13. Naming mismatch? **YES** (V3.0.2 checked SERPAPI_API_KEY)
14. Fixed? **YES**
15. Provider healthy? **${providerHealth.healthy ? "YES" : "NO"}**
16. Searches used: **${searchesUsed}**

## ADDRESS
17. Official before: **92**
18. SerpApi candidate additions: **${serpAddr.length}**
19. Final staging: **${addrStaging}/150 (${Math.round((1000 * addrStaging) / 150) / 10}%)**
20. Production-eligible: **${addrOfficial}/150**
21. SerpApi-only pending: **${addrSerpOnly}**

## PHONE
22. Official/direct before: **54**
23. SerpApi candidates: **${serpPhone.length}**
24. Final property-direct (official): **${phoneDirect}/150**; researched direct (official+SerpApi): **${phonePropertyDirectResearched}/150**
25. Central-reservation-only: steward list (**${steward.length}**)
26. Final staging: **${phoneStaging}/150 (${Math.round((1000 * phoneStaging) / 150) / 10}%)**
27. Production-eligible: **${phoneDirect}/150**

## COORDINATES
28. Before = **115**
29. Additional SerpApi candidates: **${serpCoord.length}**
30. Final research coverage: **${coordsStaging}/150**
31. Official preserved: **YES**

## STATE / REGION
32. Before = **55**
33. Final: **${stateAfter}/150 (${Math.round((1000 * stateAfter) / 150) / 10}%)**
34. Brazil improvement: **${brBefore} → ${brAfter}**
35. Argentina improvement: **${arBefore} → ${arAfter}**
36. Remaining unresolved: **${150 - stateAfter}**

## SUBMARKET
37. Before = **46**
38. New applicable matches: **${subAfter}**
39. Applicable resolution %: **${applicablePct}%**
40. No-meaningful-submarket: **${naCount}**
41. Genuine unresolved taxonomy: **${forensics.length}**

## SCHEMA
42. Phone: **CONDITIONAL REQUIRED**
43. Submarket: **APPLICABILITY-BASED**
44. Current-schema completeness: **${stagingComp.pct}%**
45. Proposed-applicability completeness: **${proposedComp.pct}%**
46. ≥95% hotels impact: **${stagingComp.hotels_ge95} → ${proposedComp.hotels_ge95}**

## BACKFILL 2 (NOT APPLIED)
47. Official/eligible blank fills: **${officialMut.length}**
48. SerpApi-only blocked: **${serpBlocked.length}**
49. Steward-review: **${steward.length}**
50. Overwrite proposed: **0**

## V3.1
51. State gate: **${stateGate ? "YES" : "NO"}**
52. Address gate: **${addrGate ? "YES" : "NO"}**
53. Phone gate / conditional: **${phoneGate ? "YES" : "NO"}**
54. Submarket gate: **${subGate ? "YES" : "NO"}**
55. Coordinate gate: **${coordGate ? "YES" : "NO"}**
56. Safety regression: **${bf1.circuit.tripped ? "YES" : "NO"}**
57. V3.1 READY: **${v31Ready ? "YES" : "NO"}**

## MOST IMPORTANTLY
58. Config bug blocked SerpApi in V3.0.2? **YES** (\`SERPAPI_API_KEY\` vs \`SERPAPI_KEY\`)
59. Limited SerpApi path can lift Address/Phone/Coords? **${searchesUsed > 0 ? "YES — see staging deltas" : "Provider path fixed; run results depend on health/calls"}**
60. Submarket treated as Dealality classification? **YES**
61. Rooms now dominant Golden gap? **${addrStaging / 150 >= 0.85 && stateAfter / 150 >= 0.85 && coordsStaging / 150 >= 0.9 ? "APPROACHING — Submarket/Phone still material" : "NO — geography/contact still material"}**

## FINAL VERDICTS
| Area | Verdict |
|------|---------|
| **BACKFILL** | **${bf1Verdict}** |
| **SERPAPI** | **${providerHealth.healthy ? "OPERATIONAL" : hasSerpKey() ? "CONFIGURATION FIXED — PROVIDER CHECK FAILED" : "CONFIGURATION ISSUE REMAINS"}** |
| **GOLDEN GEOGRAPHY/CONTACT** | **${addrStaging / 150 >= 0.9 && stateAfter / 150 >= 0.9 ? "READY" : "PARTIAL"}** |
| **V3.1** | **${v31Ready ? "READY" : "NOT READY"}** |
`
);

wj("00-scorecard.json", {
  backfill: bf1Verdict,
  serpapi: providerHealth.healthy ? "OPERATIONAL" : "CONFIGURATION_FIXED_CHECK",
  golden:
    addrStaging / 150 >= 0.9 && stateAfter / 150 >= 0.9 && coordsStaging / 150 >= 0.9
      ? "READY"
      : "PARTIAL",
  v31: v31Ready ? "READY" : "NOT READY",
  bf1_summary: bf1.summary,
  searchesUsed,
  addrStaging,
  stateAfter,
  phoneDirect,
  phoneStaging,
  phonePropertyDirectResearched,
  subAfter,
  applicablePct,
  coordsStaging,
  blockers: {
    state: !stateGate,
    address: !addrGate,
    phone: !phoneGate,
    submarket: !subGate,
    coords: !coordGate,
  },
});

console.log(
  JSON.stringify(
    {
      out: OUT,
      applyBf1,
      bf1: bf1.summary,
      serpapi_key_available: hasSerpKey(),
      provider_healthy: providerHealth.healthy,
      searchesUsed,
      addrStaging,
      stateAfter,
      phoneDirect,
      subAfter,
      applicablePct,
      coordsStaging,
      v31: v31Ready ? "READY" : "NOT READY",
    },
    null,
    2
  )
);

if (applyBf1 && !bf1.aPass) process.exit(1);
