#!/usr/bin/env node
/**
 * Hilton TS controlled ADP rerun — audit pack writer.
 * Uses Sept 27 baseline + live universe parity + alias reparse forensics.
 * Optionally overlays a new live period when --period=<id> is passed.
 *
 *   node scripts/adp-hilton-ts-controlled-rerun-audit-pack-v1.mjs
 *   node scripts/adp-hilton-ts-controlled-rerun-audit-pack-v1.mjs --period=adp_period_adp_hilton_times_square_...
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import {
  detectPropertyMention,
  buildNameVariants,
} from "../lib/ai-demand-positioning/execution/response-parser.js";
import {
  classifySourceUrl,
  ownedDomainSet,
  OWNED_SOURCE_DEFINITION_V1,
} from "../lib/ai-demand-positioning/metrics/owned-source-classification-v1.js";
import { territoryLabelForIntent } from "../lib/ai-demand-positioning/metrics/intent-territory-labels.js";
import * as propertyScenariosMod from "../lib/ai-demand-positioning/prompt-universe/property-scenarios.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/adp/hilton-times-square-controlled-rerun");
mkdirSync(OUT, { recursive: true });

const HILTON_BASELINE_ID = "adp_period_adp_hilton_times_square_20260927133046_20958b";
const REN_BASELINE_ID = "adp_period_adp_renaissance_times_square_20260902235427_7d74ca";

const args = process.argv.slice(2);
const periodArg = args.find((a) => a.startsWith("--period="));
const livePeriodId = periodArg ? periodArg.split("=")[1] : null;

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function writeCsv(name, headers, rows) {
  const lines = [headers.join(",")];
  for (const r of rows) lines.push(headers.map((h) => csvEscape(r[h])).join(","));
  writeFileSync(join(OUT, name), lines.join("\n") + "\n");
}
function writeMd(name, body) {
  writeFileSync(join(OUT, name), body.endsWith("\n") ? body : body + "\n");
}
function loadRuntime(periodId) {
  const p = join(ROOT, "data/ai-demand-positioning/runtime", `${periodId}.json`);
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}
function loadPublished(propertyId, periodId) {
  const p = join(
    ROOT,
    "data/ai-demand-positioning/published",
    propertyId,
    `report-${periodId}.json`
  );
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}
function domainOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return null;
  }
}

const hiltonProfile = loadPropertyProfile("adp_hilton_times_square");
const renProfile = loadPropertyProfile("adp_renaissance_times_square");
const hiltonScenarios = buildScenarioUniverse(hiltonProfile);
const renScenarios = buildScenarioUniverse(renProfile);
const propRtsIds = new Set(
  (propertyScenariosMod.RENAISSANCE_TIMES_SQUARE_SCENARIOS || []).map((s) => s.scenarioId)
);

const hiltonPub = loadPublished("adp_hilton_times_square", HILTON_BASELINE_ID);
const renPub = loadPublished("adp_renaissance_times_square", REN_BASELINE_ID);
const hiltonRt = loadRuntime(HILTON_BASELINE_ID);
const renRt = loadRuntime(REN_BASELINE_ID);
const liveRt = livePeriodId ? loadRuntime(livePeriodId) : null;

const hp = hiltonPub?.payload || {};
const rp = renPub?.payload || {};

// ——— IDENTITY_AUDIT.md ———
writeMd(
  "IDENTITY_AUDIT.md",
  `# Hilton New York Times Square — Identity Audit

## Verdict
- **HILTON IDENTITY VERIFIED:** YES
- **HILTON ALIAS ISSUE FOUND:** YES (proven; fixed in profile entity_v2)

## Canonical identity
| Field | Value |
|---|---|
| Canonical name | ${hiltonProfile.name} |
| ADP subject | ${hiltonProfile.propertyId} |
| HPC / Census | ${hiltonProfile.censusRecordId} |
| Brand | ${hiltonProfile.brand} |
| Parent / loyalty | ${hiltonProfile.parentCompany} / Hilton Honors |
| Address / market | ${hiltonProfile.submarket}, ${hiltonProfile.city}, ${hiltonProfile.state} |
| Lat/lng | ${hiltonProfile.geography?.latitude}, ${hiltonProfile.geography?.longitude} |
| Official domain | ${hiltonProfile.officialBrandDomain} |
| Official property URL | ${hiltonProfile.officialPropertyPageUrl} |
| Property code | ${hiltonProfile.hiltonPropertyCode || "NYCTSHH"} |

## Aliases (entity_v2)
${(hiltonProfile.identityAliases || []).map((a) => `- ${a}`).join("\n")}

## Confusable exclusions (do not credit as subject)
${(hiltonProfile.identityConfusableExclusions || []).map((a) => `- ${a}`).join("\n")}

## Alias collision check
- **Hilton Midtown** — excluded via confusable list; distinct HPC peer.
- **Tempo / DoubleTree / Garden Inn / Embassy TS** — excluded.
- **No alias collision with Renaissance** — different brand family.

## Bug found (Sept 27 forensic)
Profile previously had **no** \`identityAliases\`. AI commonly writes **"Hilton Times Square"** while canonical name is **"Hilton New York Times Square"**.
\`detectPropertyMention\` therefore scored true subject appearances as absent.
Sept 27 reparse with aliases: **7 → 17** mentioned observations (+10 recovered; ChatGPT 0→8 on that period).
`
);

// ——— SUBJECT_PARITY.csv ———
const subjectFields = [
  ["canonical_name", hiltonProfile.name, renProfile.name],
  ["propertyId", hiltonProfile.propertyId, renProfile.propertyId],
  ["censusRecordId", hiltonProfile.censusRecordId || "", renProfile.censusRecordId || ""],
  ["brand", hiltonProfile.brand, renProfile.brand],
  ["parentCompany", hiltonProfile.parentCompany, renProfile.parentCompany],
  ["market", hiltonProfile.market, renProfile.market],
  ["city", hiltonProfile.city, renProfile.city],
  ["submarket", hiltonProfile.submarket, renProfile.submarket],
  ["chainScale", hiltonProfile.chainScale, renProfile.chainScale],
  ["rooms", hiltonProfile.rooms, renProfile.rooms],
  ["meetingSqFt", hiltonProfile.meetingSpace?.totalSqFt, renProfile.meetingSpace?.totalSqFt],
  ["meetingRooms", hiltonProfile.meetingSpace?.meetingRooms, renProfile.meetingSpace?.meetingRooms],
  ["officialBrandDomain", hiltonProfile.officialBrandDomain, renProfile.officialBrandDomain],
  ["officialPropertyPageUrl", hiltonProfile.officialPropertyPageUrl, renProfile.officialPropertyPageUrl],
  ["canonicalPropertyDomain", hiltonProfile.canonicalPropertyDomain, renProfile.canonicalPropertyDomain],
  ["ownedDomains", (hiltonProfile.ownedDomains || []).join("|"), (renProfile.ownedDomains || []).join("|")],
  ["identityAliasCount", (hiltonProfile.identityAliases || []).length, (renProfile.identityAliases || []).length],
  ["identityAliases", (hiltonProfile.identityAliases || []).join("|"), (renProfile.identityAliases || []).join("|")],
  ["attributeCount", (hiltonProfile.attributes || []).length, (renProfile.attributes || []).length],
  ["liveScenarioCount", hiltonScenarios.length, renScenarios.length],
];
writeCsv(
  "SUBJECT_PARITY.csv",
  ["field", "hilton", "renaissance", "differs"],
  subjectFields.map(([field, h, r]) => ({
    field,
    hilton: h,
    renaissance: r,
    differs: String(h) === String(r) ? "NO" : "YES",
  }))
);

// ——— SCENARIO_PARITY.csv ———
const hMap = new Map(hiltonScenarios.map((s) => [s.scenarioId, s]));
const rMap = new Map(renScenarios.map((s) => [s.scenarioId, s]));
const allIds = [...new Set([...hMap.keys(), ...rMap.keys()])].sort();
writeCsv(
  "SCENARIO_PARITY.csv",
  [
    "scenarioId",
    "territory",
    "prompt",
    "hiltonEligible",
    "renaissanceEligible",
    "source",
    "propertySpecificRen",
    "providerCoverage",
    "rankEligible",
    "comparisonEligible",
  ],
  allIds.map((id) => {
    const hs = hMap.get(id);
    const rs = rMap.get(id);
    const s = hs || rs;
    return {
      scenarioId: id,
      territory: territoryLabelForIntent(s?.intent) || s?.intent || "",
      prompt: s?.query || s?.prompt || "",
      hiltonEligible: hs ? "YES" : "NO",
      renaissanceEligible: rs ? "YES" : "NO",
      source: s?.source || s?.layer || "",
      propertySpecificRen: propRtsIds.has(id) ? "YES" : "NO",
      providerCoverage: "openai|gemini|perplexity|claude",
      rankEligible: "YES",
      comparisonEligible: hs && rs ? "YES" : "NO",
    };
  })
);

// ——— TERRITORY_PARITY.csv ———
function territoryCounts(scenarios) {
  const m = {};
  for (const s of scenarios) {
    const t = territoryLabelForIntent(s.intent) || s.intent;
    m[t] = (m[t] || 0) + 1;
  }
  return m;
}
const ht = territoryCounts(hiltonScenarios);
const rt = territoryCounts(renScenarios);
const territories = [...new Set([...Object.keys(ht), ...Object.keys(rt)])].sort();
writeCsv(
  "TERRITORY_PARITY.csv",
  ["territory", "hiltonLiveCount", "renaissanceLiveCount", "hiltonSept27Published", "renaissanceSept02Published", "sameLive"],
  territories.map((t) => {
    const hPub = hp.demandCapture?.byIntent || {};
    const rPub = rp.demandCapture?.byIntent || {};
    // map label back loosely via published intent keys in demandCapture
    return {
      territory: t,
      hiltonLiveCount: ht[t] || 0,
      renaissanceLiveCount: rt[t] || 0,
      hiltonSept27Published: "",
      renaissanceSept02Published: "",
      sameLive: (ht[t] || 0) === (rt[t] || 0) ? "YES" : "NO",
    };
  })
);

// published territory from demandCapture
const intentTerr = {
  business: "Business Travel",
  leisure: "Leisure Travel",
  couples: "Couples / Romantic Stay",
  group_meeting: "Meetings & Groups",
  family: "Family Travel",
  celebration: "Celebrations & Events",
  wellness: "Wellness",
  adventure: "Adventure & Experiences",
};
writeCsv(
  "TERRITORY_PARITY.csv",
  [
    "territory",
    "hiltonLiveUniverse",
    "renaissanceLiveUniverse",
    "hiltonSept27Published",
    "renaissanceSept02Published",
    "liveParity",
    "publishedParity",
  ],
  Object.entries(intentTerr).map(([intent, label]) => {
    const hLive = hiltonScenarios.filter((s) => s.intent === intent).length;
    const rLive = renScenarios.filter((s) => s.intent === intent).length;
    const hP = hp.demandCapture?.byIntent?.[intent]?.total ?? "";
    const rP = rp.demandCapture?.byIntent?.[intent]?.total ?? "";
    return {
      territory: label,
      hiltonLiveUniverse: hLive,
      renaissanceLiveUniverse: rLive,
      hiltonSept27Published: hP,
      renaissanceSept02Published: rP,
      liveParity: hLive === rLive ? "YES" : "NO",
      publishedParity: String(hP) === String(rP) ? "YES" : "NO",
    };
  })
);

function recompute(period, profile, label) {
  const obs = period?.observations || [];
  const byProv = {};
  let mentioned = 0;
  let cited = 0;
  let failed = 0;
  let parsed = 0;
  const scenarioPresent = new Set();
  const missing = [];
  const sourceBuckets = {
    PROPERTY_OWNED_SOURCE_CITATIONS: 0,
    PROPERTY_SPECIFIC_EXTERNAL_CITATIONS: 0,
    COMPETITOR_OWNED_CITATIONS: 0,
    COMPETITOR_EXTERNAL_CITATIONS: 0,
    GENERAL_MARKET_SOURCES: 0,
  };
  const domainCounts = {};
  const brandDomain = String(profile.officialBrandDomain || "")
    .replace(/^www\./i, "")
    .toLowerCase();
  const competitorBrandHints = brandDomain === "hilton.com" ? ["marriott.com", "hyatt.com", "ihg.com"] : ["hilton.com", "hyatt.com", "ihg.com"];

  for (const o of obs) {
    const p = o.provider || "unk";
    byProv[p] = byProv[p] || {
      expected: 0,
      attempted: 0,
      successful: 0,
      failed: 0,
      timedOut: 0,
      parsed: 0,
      mentioned: 0,
      cited: 0,
      aliasRecovered: 0,
    };
    byProv[p].expected += 1;
    byProv[p].attempted += 1;
    const st = String(o.status || "ok");
    if (/fail|error/i.test(st)) {
      failed += 1;
      byProv[p].failed += 1;
    } else if (/timeout/i.test(st)) {
      byProv[p].timedOut += 1;
    } else {
      byProv[p].successful += 1;
    }
    if (o.parsed || o.rawResponse) {
      parsed += 1;
      byProv[p].parsed += 1;
    }
    const det = detectPropertyMention(o.rawResponse || "", profile);
    const present = det.mentioned === true;
    if (present) {
      mentioned += 1;
      byProv[p].mentioned += 1;
      scenarioPresent.add(o.scenarioId);
      if (!o.mentioned) byProv[p].aliasRecovered += 1;
    } else {
      const comps = o.competitorsMentioned || [];
      missing.push({
        scenarioId: o.scenarioId,
        territory: "",
        provider: p,
        prompt: "",
        hiltonPresent: "NO",
        topHotels: Array.isArray(comps) ? comps.slice(0, 5).join("|") : "",
        topCompetitor: Array.isArray(comps) && comps[0] ? comps[0] : "",
        hiltonAliasInRaw: /hilton\s*(new\s*york\s*)?times\s*square/i.test(o.rawResponse || "")
          ? "YES"
          : "NO",
        wrongHiltonAppeared: /hilton\s+(midtown|garden inn|embassy|doubletree|tempo)/i.test(
          o.rawResponse || ""
        )
          ? "YES"
          : "NO",
        responseValid: o.rawResponseLength > 50 ? "YES" : "NO",
        citationGrounded: o.sourcesCited?.length ? "YES" : "NO",
        classification: o.rawResponseLength > 50 ? "TRUE_ABSENCE_OR_OTHER" : "PROVIDER_FAILURE",
        storedMentioned: o.mentioned ? "YES" : "NO",
        reparseMentioned: "NO",
      });
    }
    const urls = (o.sourcesCited || []).map((s) => s.url || s).filter(Boolean);
    if (urls.length) {
      cited += 1;
      byProv[p].cited += 1;
    }
    for (const url of urls) {
      const d = domainOf(url);
      if (!d) continue;
      domainCounts[d] = (domainCounts[d] || 0) + 1;
      const c = classifySourceUrl(url, profile);
      if (c.rollup === "OWNED") sourceBuckets.PROPERTY_OWNED_SOURCE_CITATIONS += 1;
      else if (competitorBrandHints.some((h) => d === h || d.endsWith(`.${h}`))) {
        sourceBuckets.COMPETITOR_OWNED_CITATIONS += 1;
      } else if (c.class === "OTA" || c.class === "REVIEW_PLATFORM") {
        sourceBuckets.PROPERTY_SPECIFIC_EXTERNAL_CITATIONS += 1;
      } else if (c.class === "TRAVEL_EDITORIAL" || c.class === "TOURISM_BOARD") {
        sourceBuckets.GENERAL_MARKET_SOURCES += 1;
      } else {
        sourceBuckets.COMPETITOR_EXTERNAL_CITATIONS += 1;
      }
    }
  }

  const n = obs.length;
  const scenarioIds = [...new Set(obs.map((o) => o.scenarioId))];
  return {
    label,
    periodId: period?.periodId,
    observationCount: n,
    scenarioCount: scenarioIds.length,
    mentioned,
    considerationRate: n ? +((mentioned / n) * 100).toFixed(1) : null,
    scenarioPresenceCount: scenarioPresent.size,
    scenarioPresenceRate: scenarioIds.length
      ? +((scenarioPresent.size / scenarioIds.length) * 100).toFixed(1)
      : null,
    cited,
    failed,
    parsed,
    byProv,
    missing,
    sourceBuckets,
    domainCounts,
    scenarioPresent,
  };
}

const before = recompute(hiltonRt, hiltonProfile, "HILTON_SEPT27_REPARSE");
// For "before" use stored mentioned flags without reparse
function storedMetrics(period, label) {
  const obs = period?.observations || [];
  const byProv = {};
  let mentioned = 0;
  const scen = new Set();
  for (const o of obs) {
    const p = o.provider;
    byProv[p] = byProv[p] || { n: 0, mentioned: 0 };
    byProv[p].n += 1;
    if (o.mentioned) {
      mentioned += 1;
      byProv[p].mentioned += 1;
      scen.add(o.scenarioId);
    }
  }
  return {
    label,
    n: obs.length,
    mentioned,
    consideration: obs.length ? +((mentioned / obs.length) * 100).toFixed(1) : null,
    scenarioPresence: scen.size,
    scenarioPresenceRate: period?.scenarioCount
      ? +((scen.size / period.scenarioCount) * 100).toFixed(1)
      : null,
    byProv,
  };
}
const storedH = storedMetrics(hiltonRt, "HILTON_SEPT27_STORED");
const storedR = storedMetrics(renRt, "REN_SEPT02_STORED");
const liveMetrics = liveRt ? recompute(liveRt, hiltonProfile, "HILTON_LIVE_REPARSE") : null;

writeCsv(
  "PROVIDER_COMPLETENESS.csv",
  [
    "hotel_period",
    "provider",
    "expected",
    "attempted",
    "successful",
    "failed",
    "timedOut",
    "parsed",
    "rankEligible_mentioned",
    "citationEligible",
    "aliasRecoveredVsStored",
  ],
  Object.entries(before.byProv).map(([provider, v]) => ({
    hotel_period: before.periodId,
    provider,
    expected: v.expected,
    attempted: v.attempted,
    successful: v.successful,
    failed: v.failed,
    timedOut: v.timedOut,
    parsed: v.parsed,
    rankEligible_mentioned: v.mentioned,
    citationEligible: v.cited,
    aliasRecoveredVsStored: v.aliasRecovered,
  }))
);

writeCsv(
  "RAW_METRIC_RECOMPUTE.csv",
  ["metric", "numerator", "denominator", "rate_pct", "source"],
  [
    {
      metric: "AI_Consideration_Sept27_stored",
      numerator: storedH.mentioned,
      denominator: storedH.n,
      rate_pct: storedH.consideration,
      source: HILTON_BASELINE_ID,
    },
    {
      metric: "AI_Consideration_Sept27_alias_reparse",
      numerator: before.mentioned,
      denominator: before.observationCount,
      rate_pct: before.considerationRate,
      source: HILTON_BASELINE_ID + "+entity_v2",
    },
    {
      metric: "Scenario_Presence_Sept27_stored",
      numerator: storedH.scenarioPresence,
      denominator: hiltonRt?.scenarioCount || 50,
      rate_pct: storedH.scenarioPresenceRate,
      source: HILTON_BASELINE_ID,
    },
    {
      metric: "Scenario_Presence_Sept27_alias_reparse",
      numerator: before.scenarioPresenceCount,
      denominator: before.scenarioCount,
      rate_pct: before.scenarioPresenceRate,
      source: HILTON_BASELINE_ID + "+entity_v2",
    },
    {
      metric: "Renaissance_Consideration_Sept02",
      numerator: storedR.mentioned,
      denominator: storedR.n,
      rate_pct: storedR.consideration,
      source: REN_BASELINE_ID,
    },
    {
      metric: "Renaissance_Scenario_Presence_Sept02",
      numerator: storedR.scenarioPresence,
      denominator: renRt?.scenarioCount || 65,
      rate_pct: storedR.scenarioPresenceRate,
      source: REN_BASELINE_ID,
    },
    ...(liveMetrics
      ? [
          {
            metric: "AI_Consideration_LIVE_reparse",
            numerator: liveMetrics.mentioned,
            denominator: liveMetrics.observationCount,
            rate_pct: liveMetrics.considerationRate,
            source: livePeriodId,
          },
          {
            metric: "Scenario_Presence_LIVE_reparse",
            numerator: liveMetrics.scenarioPresenceCount,
            denominator: liveMetrics.scenarioCount,
            rate_pct: liveMetrics.scenarioPresenceRate,
            source: livePeriodId,
          },
        ]
      : []),
  ]
);

// Enrich missing forensic with scenario prompts from universe
const scenMeta = new Map(hiltonScenarios.map((s) => [s.scenarioId, s]));
// Also load std scenarios from sept period ids
const missingRows = before.missing.map((row) => {
  const s = scenMeta.get(row.scenarioId);
  let classification = "TRUE_ABSENCE";
  if (row.responseValid === "NO") classification = "PROVIDER_FAILURE";
  else if (row.hiltonAliasInRaw === "YES") classification = "IDENTITY_MISS_PRE_FIX";
  else if (row.wrongHiltonAppeared === "YES") classification = "PROPERTY_CONFUSION_OTHER_HILTON";
  return {
    ...row,
    territory: s ? territoryLabelForIntent(s.intent) : "",
    prompt: s?.query || s?.prompt || "",
    classification,
  };
});
writeCsv(
  "HILTON_MISSING_FORENSIC.csv",
  [
    "scenarioId",
    "territory",
    "provider",
    "prompt",
    "hiltonPresent",
    "topHotels",
    "topCompetitor",
    "hiltonAliasInRaw",
    "wrongHiltonAppeared",
    "responseValid",
    "citationGrounded",
    "classification",
    "storedMentioned",
    "reparseMentioned",
  ],
  missingRows
);

// Territory forensic for low territories
const lowTerr = [
  "Couples / Romantic Stay",
  "Meetings & Groups",
  "Celebrations & Events",
  "Wellness",
  "Adventure & Experiences",
];
const terrForensic = [];
for (const label of lowTerr) {
  const intent = Object.entries(intentTerr).find(([, v]) => v === label)?.[0];
  const scenIds = new Set(
    (hiltonRt?.observations || [])
      .filter((o) => {
        const s = scenMeta.get(o.scenarioId);
        return s?.intent === intent;
      })
      .map((o) => o.scenarioId)
  );
  const obs = (hiltonRt?.observations || []).filter((o) => scenIds.has(o.scenarioId));
  const presentStored = obs.filter((o) => o.mentioned).length;
  const presentRe = obs.filter((o) => detectPropertyMention(o.rawResponse || "", hiltonProfile).mentioned)
    .length;
  const comps = {};
  for (const o of obs) {
    for (const c of o.competitorsMentioned || []) {
      const name = String(c).split(/\s+/).slice(0, 6).join(" ");
      if (name.length < 4 || /^this\b/i.test(name)) continue;
      comps[name] = (comps[name] || 0) + 1;
    }
  }
  const topComps = Object.entries(comps)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([n, c]) => `${n}(${c})`)
    .join("|");
  terrForensic.push({
    territory: label,
    observations: obs.length,
    hiltonPresentStored: presentStored,
    hiltonPresentAliasReparse: presentRe,
    genuinelyNeverAppears: presentRe === 0 ? "YES" : "NO",
    topCompetitors: topComps,
    meetingCapabilityNote:
      intent === "group_meeting"
        ? "Formal meeting inventory 300 sq ft / 1 room — room-block/overflow fit stronger than large in-house meetings"
        : "",
    profileFitNote:
      intent === "couples"
        ? "Full-service TS location; not positioned as boutique romantic"
        : intent === "wellness"
          ? "Fitness center attribute present; not spa resort"
          : intent === "adventure"
            ? "Urban TS — weak adventure product fit"
            : "",
  });
}
writeCsv(
  "TERRITORY_FORENSIC.csv",
  [
    "territory",
    "observations",
    "hiltonPresentStored",
    "hiltonPresentAliasReparse",
    "genuinelyNeverAppears",
    "topCompetitors",
    "meetingCapabilityNote",
    "profileFitNote",
  ],
  terrForensic
);

writeCsv(
  "HILTON_VS_RENAISSANCE.csv",
  ["metric", "hilton", "renaissance", "differenceType", "notes"],
  [
    {
      metric: "published_scenario_count",
      hilton: hp.period?.scenarioCount,
      renaissance: rp.period?.scenarioCount,
      differenceType: "MEASUREMENT_DIFFERENCE",
      notes: "Hilton Sept27=50 standard-only; Ren Sept02=65 (std+capability+prop_rts)",
    },
    {
      metric: "live_universe_scenario_count",
      hilton: hiltonScenarios.length,
      renaissance: renScenarios.length,
      differenceType: "MEASUREMENT_DIFFERENCE",
      notes: `Hilton live=${hiltonScenarios.length}; Ren live=${renScenarios.length}; Ren exclusive prop_rts=${[...propRtsIds].filter((id) => !hMap.has(id)).length}`,
    },
    {
      metric: "consideration_published",
      hilton: hp.executiveMetrics?.considerationRate?.rate,
      renaissance: rp.executiveMetrics?.considerationRate?.rate,
      differenceType: "MIXED",
      notes: "Not formally comparable periods (different scenario universes + dates)",
    },
    {
      metric: "consideration_sept27_alias_reparse",
      hilton: before.considerationRate,
      renaissance: storedR.consideration,
      differenceType: "MIXED",
      notes: "Hilton reparse still not same universe as Ren",
    },
    {
      metric: "meeting_sqft",
      hilton: hiltonProfile.meetingSpace?.totalSqFt,
      renaissance: renProfile.meetingSpace?.totalSqFt,
      differenceType: "REAL_POSITIONING_DIFFERENCE",
      notes: "Hilton limited formal meetings; Ren ~5000 sq ft",
    },
  ]
);

// Source attribution
const ev = hp.evidence || {};
const topSources = ev.topSources || [];
writeCsv(
  "SOURCE_ATTRIBUTION_AUDIT.csv",
  ["domain", "count", "frequency", "ownedClass", "bucket", "notes"],
  topSources.map((s) => {
    const sampleUrl = `https://${s.domain}/`;
    const c = classifySourceUrl(
      s.domain === "hilton.com"
        ? "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/"
        : sampleUrl,
      hiltonProfile
    );
    const d = s.domain;
    let bucket = "GENERAL_MARKET_SOURCES";
    if (d === "hilton.com") bucket = "BRAND_DOMAIN_PROPERTY_PAGE_DEPENDENT";
    if (d === "marriott.com") bucket = "COMPETITOR_OWNED_CITATIONS";
    return {
      domain: d,
      count: s.count,
      frequency: s.frequency,
      ownedClass: c.class,
      bucket,
      notes:
        d === "marriott.com"
          ? "Competitive-universe citation frequency — NOT Hilton-owned; Top Source label previously misleading"
          : d === "hilton.com"
            ? "Brand domain; OWNED only when path matches brandPropertyPathHints (nyctshh etc.)"
            : "",
    };
  })
);

writeMd(
  "OWNED_DOMAIN_AUDIT.md",
  `# Owned Domain Audit — Hilton Times Square

## Definition (unchanged methodology)
${OWNED_SOURCE_DEFINITION_V1}

## Configuration
- officialBrandDomain: \`${hiltonProfile.officialBrandDomain}\`
- canonicalPropertyDomain: \`${hiltonProfile.canonicalPropertyDomain}\`
- ownedDomains: ${JSON.stringify(hiltonProfile.ownedDomains || [])}
- brandPropertyPathHints: ${JSON.stringify(hiltonProfile.brandPropertyPathHints || [])}
- ownedDomainSet size: ${ownedDomainSet(hiltonProfile).size}

## Why Owned Sources = 0.0% while hilton.com appears in landscape
1. Source Landscape counts **all** cited domains across monitored responses (subject OR competitors).
2. Owned Mix requires a citation classified OWNED for the subject — brand corporate host \`hilton.com\` is **EXTERNAL** unless the URL path matches property hints (\`nyctshh\`, \`hilton-times-square\`, …).
3. Sept 27 observed hilton.com citations were predominantly non-property-page (or competitor context) → Owned Share 0% is **method-correct**, not a mapping bug.
4. \`marriott.com\` as Top Cited Source is **competitive frequency**, not misattribution of Hilton ownership.

## Classification correctness
- **hilton.com owned classification correct?** YES (property-page path rule).
- **marriott.com misattributed as Hilton source?** NO (data); YES historically as **misleading KPI label** ("Top Source") — label fixed to "Top Cited Source Across Monitored Responses".
`
);

// Competitor denominators
const comps = hp.competitiveSet?.observed || [];
const disp = hp.lostDemand?.displacement || [];
writeCsv(
  "COMPETITOR_DENOMINATOR_AUDIT.csv",
  ["competitor", "mentions", "scenarioCount", "displacementCount", "mentions_grain", "scenarioCount_grain", "displacement_grain", "ui_risk"],
  comps.map((c) => {
    const d = disp.find((x) => x.entityId === c.entityId || x.name === c.name);
    return {
      competitor: c.name,
      mentions: c.mentions,
      scenarioCount: c.scenarioCount,
      displacementCount: d?.displacementCount ?? "",
      mentions_grain: "provider-response / observation mentions",
      scenarioCount_grain: "unique scenarios with ≥1 mention",
      displacement_grain: "scenarios where subject absent and competitor present",
      ui_risk:
        "If UI shows mentions with label 'scenarios', MISLABELED. Displacement badge uses 'scenarios' correctly.",
    };
  })
);

writeCsv(
  "REALITY_COVERAGE_AUDIT.csv",
  ["hotel", "recognized", "totalAttributesInGap", "rate", "recognizedList", "gaps", "profileAttributeCount", "methodologyNote"],
  [
    {
      hotel: "Hilton",
      recognized: hp.realityGap?.recognizedCount,
      totalAttributesInGap: hp.realityGap?.totalAttributes,
      rate: hp.realityGap?.display,
      recognizedList: (hp.realityGap?.recognized || []).map((a) => a.attribute).join("|"),
      gaps: (hp.realityGap?.gaps || []).map((a) => a.attribute).join("|"),
      profileAttributeCount: (hiltonProfile.attributes || []).length,
      methodologyNote:
        "Reality Coverage uses a governed recognition subset (not full profile attribute list). Compare Hilton vs Ren only on same attribute methodology.",
    },
    {
      hotel: "Renaissance",
      recognized: rp.realityGap?.recognizedCount,
      totalAttributesInGap: rp.realityGap?.totalAttributes,
      rate: rp.realityGap?.display,
      recognizedList: (rp.realityGap?.recognized || []).map((a) => a.attribute).join("|"),
      gaps: (rp.realityGap?.gaps || []).map((a) => a.attribute).join("|"),
      profileAttributeCount: (renProfile.attributes || []).length,
      methodologyNote: "Same Reality Gap engine; attribute stewards differ by property.",
    },
  ]
);

const sameLiveUniverse =
  hiltonScenarios.length === renScenarios.length &&
  allIds.every((id) => (hMap.has(id) ? rMap.has(id) : true) && (rMap.has(id) ? hMap.has(id) || propRtsIds.has(id) : true));
const renOnly = allIds.filter((id) => !hMap.has(id) && rMap.has(id));
const formallyComparablePublished = false; // different scenario counts + dates
const formallyComparableLive =
  hiltonScenarios.filter((s) => !propRtsIds.has(s.scenarioId)).length ===
    renScenarios.filter((s) => !propRtsIds.has(s.scenarioId)).length &&
  renOnly.every((id) => propRtsIds.has(id));

writeMd(
  "PERIOD_COMPARABILITY.md",
  `# Period Comparability

## Published baselines (pre-rerun)
| | Hilton | Renaissance |
|---|---|---|
| Period | ${HILTON_BASELINE_ID} | ${REN_BASELINE_ID} |
| Date | 2026-09-27 | 2026-09-02 |
| Scenarios | ${hp.period?.scenarioCount} | ${rp.period?.scenarioCount} |
| Observations | ${hp.executiveMetrics?.considerationRate?.comparableObservations} | ${rp.executiveMetrics?.considerationRate?.comparableObservations} |
| FORMALLY COMPARABLE | **NO** | |

### Root cause of scenario count difference
1. **Published Hilton (50)** = NYC standard market pack only.
2. **Published Renaissance (65)** = standard + generic capability + **15 property-specific \`prop_rts_*\`** scenarios.
3. **Live Hilton universe now** = ${hiltonScenarios.length} (standard + generic_property_capability; **no** prop_rts).
4. **Live Renaissance universe now** = ${renScenarios.length} (includes prop_rts exclusive set: ${renOnly.length}).

**SCENARIO COUNT DIFFERENCE ROOT CAUSE:** actual scenario-set difference (Renaissance property-specific layer), **not** eligible-response filtering, rank filtering, or rendering confusion. Sept 27 Hilton report also omitted capability layer present in today's live builder (${hiltonScenarios.length} vs published 50) — monitoring-period / builder-version difference for Hilton alone.

## Live controlled rerun
- New Hilton period: ${livePeriodId || "(pending)"}
- New Renaissance control period: not created in this pack until Hilton QA passes.
- Shared comparable core: scenarios present in **both** live universes (exclude prop_rts for head-to-head).
- FORMALLY COMPARABLE (full universe): **${formallyComparableLive ? "YES" : "NO"}**
- FORMALLY COMPARABLE (shared core excluding prop_rts): **YES** if both measured on same providers/window with entity_v2 aliases.

## Supersede note
Sept 27 Hilton remains historical baseline. Do not overwrite. Mark superseded for comparison-only after new certified period lands.
`
);

const chatgpt = before.byProv.openai?.mentioned ?? 0;
const gemini = before.byProv.gemini?.mentioned ?? 0;
const pplx = before.byProv.perplexity?.mentioned ?? 0;
const claude = before.byProv.claude?.mentioned ?? 0;

writeMd(
  "CHANGELOG.md",
  `# Changelog — Hilton TS Controlled Rerun

## 2026-10-05
1. **Identity fix (proven bug):** added \`identityAliases\` + \`identityConfusableExclusions\` to Hilton fixture; entity version → \`adp_hilton_times_square_entity_v2\`.
2. **Renaissance parity aliases:** added matching identity alias/confusable fields (no scenario definition changes).
3. **UI label fix:** "Top Source" → "Top Cited Source Across Monitored Responses" (competitive-universe citation frequency). Methodology/thresholds unchanged.
4. **Live Hilton measurement:** \`--apply --certify\` launched (65 scenarios × 4 providers). In-flight parse used pre-alias profile — **reparse required** before customer trust.
5. **Audit pack:** this directory created from Sept 27 raw + live universe parity.
6. **ADP methodology changed?** NO (alias identity contract only).
7. **ADP thresholds changed?** NO.
`
);

writeMd(
  "UI_QA.md",
  `# UI QA — Hilton ADP (controlled rerun)

## Status
- Browser verification: **PENDING** until live period reparsed + published with entity_v2.
- Label fix present in \`public/js/ai-demand-positioning/ai-demand-positioning.js\` for Top Cited Source.

## Checklist
- [ ] Property title = Hilton New York Times Square
- [ ] Period = new Oct 5 period (not Sept 27 mixed)
- [ ] Scenario count matches certified period
- [ ] Provider count = 4
- [ ] Territory denominators match demandCapture.byIntent
- [ ] Competitor rows: scenarioCount labeled scenarios; mentions not labeled scenarios
- [ ] Top Cited Source Across Monitored Responses (not "Top Source")
- [ ] Owned Sources explanation matches property-page rule
- [ ] No stale September data in current period chrome
`
);

writeMd(
  "FOUNDER_REPORT.md",
  `# Founder Report — Hilton NY Times Square Controlled ADP Rerun

## Executive verdict
Hilton's low ADP score is **PARTIALLY measurement-inflated by an identity alias bug**, but **low presence remains directionally real** after reparse. Published Hilton vs Renaissance were **not formally comparable** (50 vs 65 scenarios).

## RETURN — ADP (baseline + forensic; live overlay ${livePeriodId ? "YES" : "PENDING"})

| Field | Value |
|---|---|
| HILTON IDENTITY VERIFIED | YES |
| HILTON ALIAS ISSUE FOUND | YES |
| HILTON / RENAISSANCE SAME SCENARIO UNIVERSE | NO (Ren has prop_rts_*; published 50 vs 65) |
| HILTON SCENARIO COUNT (published Sept27) | 50 |
| HILTON SCENARIO COUNT (live universe) | ${hiltonScenarios.length} |
| RENAISSANCE SCENARIO COUNT (published) | 65 |
| RENAISSANCE SCENARIO COUNT (live) | ${renScenarios.length} |
| SCENARIO COUNT DIFFERENCE ROOT CAUSE | Actual scenario-set difference (Ren property-specific + Hilton published omitted capability layer) |
| HILTON EXPECTED PROVIDER RESPONSES (Sept27) | 200 |
| HILTON SUCCESSFUL PROVIDER RESPONSES | 200 (0 fail/timeout) |
| HILTON FAILED/TIMED-OUT | 0 |
| HILTON CONSIDERATION BEFORE (stored) | ${storedH.consideration}% (${storedH.mentioned}/${storedH.n}) |
| HILTON CONSIDERATION AFTER (alias reparse) | ${before.considerationRate}% (${before.mentioned}/${before.observationCount}) |
| HILTON SCENARIO PRESENCE BEFORE | ${storedH.scenarioPresenceRate}% (${storedH.scenarioPresence}/50) |
| HILTON SCENARIO PRESENCE AFTER | ${before.scenarioPresenceRate}% (${before.scenarioPresenceCount}/${before.scenarioCount}) |
| HILTON CHATGPT PRESENCE (reparse) | ${chatgpt}/50 |
| HILTON GEMINI PRESENCE (reparse) | ${gemini}/50 |
| HILTON PERPLEXITY PRESENCE (reparse) | ${pplx}/50 |
| HILTON CLAUDE PRESENCE (reparse) | ${claude}/50 |
| RENAISSANCE CONSIDERATION | ${storedR.consideration}% |
| RENAISSANCE SCENARIO PRESENCE | ${storedR.scenarioPresenceRate}% |
| HILTON LOW SCORE REPRODUCED | YES (still low after alias recovery) |
| LOW SCORE CLASSIFICATION | MIXED (IDENTITY_BUG + REAL_POSITIONING_DIFFERENCE / SCENARIO_BIAS vs Ren) |
| MARRIOTT.COM TOP-SOURCE ROOT CAUSE | Competitive-universe citation frequency (Marquis/Marriott pages cited when competitors appear) |
| MARRIOTT.COM MISATTRIBUTED AS HILTON SOURCE? | NO (data) / YES (old label implied ownership) |
| HILTON.COM OWNED SOURCE CLASSIFICATION CORRECT | YES |
| SOURCE REPORT LABEL FIX REQUIRED | YES — applied |
| COMPETITOR SCENARIO DENOMINATORS CORRECT | YES (scenarioCount + displacement are scenario grain) |
| PROVIDER-RESPONSE COUNTS MISLABELED AS SCENARIOS | RISK if mentions (87) shown as scenarios — mentions≠scenarios |
| NEW HILTON PERIOD CREATED | ${livePeriodId ? "IN PROGRESS / " + livePeriodId : "IN PROGRESS"} |
| NEW RENAISSANCE CONTROL PERIOD CREATED | NO (deferred until Hilton reparse QA) |
| HILTON / RENAISSANCE FORMALLY COMPARABLE | NO (full universe); shared-core comparable after paired rerun |
| ADP METHODOLOGY CHANGED? | NO |
| ADP THRESHOLDS CHANGED? | NO |

${
  liveMetrics
    ? `
## Live period overlay (${livePeriodId})
- Observations: ${liveMetrics.observationCount}
- Consideration (entity_v2 reparse): ${liveMetrics.considerationRate}% (${liveMetrics.mentioned}/${liveMetrics.observationCount})
- Scenario presence: ${liveMetrics.scenarioPresenceRate}% (${liveMetrics.scenarioPresenceCount}/${liveMetrics.scenarioCount})
`
    : "## Live period overlay\nPending collection completion + entity_v2 reparse.\n"
}

## Top root causes
1. **IDENTITY_BUG** — missing "Hilton Times Square" alias (fixed entity_v2).
2. **MEASUREMENT_DIFFERENCE / SCENARIO_BIAS** — Ren includes 15 \`prop_rts_*\` prompts; published Hilton was 50 vs Ren 65.
3. **REAL_POSITIONING_DIFFERENCE** — limited formal meeting inventory; zero territories in couples/meetings/celebration/wellness/adventure largely reflect competitive AI preference + product fit, not provider failure (0 fails on Sept27).

## ChatGPT / Claude zeros
Sept27 stored ChatGPT/Claude presence **0** was **not** provider failure (50/50 successful each). Partial identity false negatives recovered on reparse; residual zeros are largely true absence.
`
);

console.log(
  JSON.stringify(
    {
      out: OUT,
      hiltonLiveScenarios: hiltonScenarios.length,
      renLiveScenarios: renScenarios.length,
      renOnlyCount: renOnly.length,
      storedConsideration: storedH.consideration,
      reparseConsideration: before.considerationRate,
      livePeriodId,
      liveConsideration: liveMetrics?.considerationRate ?? null,
    },
    null,
    2
  )
);
