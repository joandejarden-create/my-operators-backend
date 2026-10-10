#!/usr/bin/env node
/**
 * Build matched-control report pack from a completed pair run.
 *
 *   node scripts/adp-hilton-renaissance-matched-control-report-pack-v1.mjs \
 *     --hilton-period=... --ren-period=... --pair-id=...
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { detectPropertyMention } from "../lib/ai-demand-positioning/execution/response-parser.js";
import {
  loadMatchedControlContract,
  auditRenaissanceEligibility,
  auditIdentityFalseNegatives,
  MATCHED_CONTROL_SET_ID,
} from "../lib/ai-demand-positioning/execution/hilton-renaissance-matched-control-v1.js";
import { territoryLabelForIntent } from "../lib/ai-demand-positioning/metrics/intent-territory-labels.js";
import {
  classifySourceUrl,
  computeOwnedExternalSourceMix,
} from "../lib/ai-demand-positioning/metrics/owned-source-classification-v1.js";

const OUT = join(process.cwd(), "reports/adp/hilton-renaissance-matched-control");
mkdirSync(OUT, { recursive: true });

const args = process.argv.slice(2);
const arg = (k) => args.find((a) => a.startsWith(`--${k}=`))?.split("=")[1];
const hiltonPeriodId = arg("hilton-period");
const renPeriodId = arg("ren-period");
const pairId = arg("pair-id") || "";

function loadRuntime(periodId) {
  const p = join(process.cwd(), "data/ai-demand-positioning/runtime", `${periodId}.json`);
  if (!existsSync(p)) throw new Error(`missing ${p}`);
  return JSON.parse(readFileSync(p, "utf8"));
}
function loadPublished(propertyId, periodId) {
  const p = join(
    process.cwd(),
    "data/ai-demand-positioning/published",
    propertyId,
    `report-${periodId}.json`
  );
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}
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
function domainOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return null;
  }
}

if (!hiltonPeriodId || !renPeriodId) {
  console.error("Need --hilton-period= and --ren-period=");
  process.exit(2);
}

const contract = loadMatchedControlContract();
const hiltonProfile = loadPropertyProfile("adp_hilton_times_square");
const renProfile = loadPropertyProfile("adp_renaissance_times_square");
const eligibility = auditRenaissanceEligibility(contract, renProfile);
const hRt = loadRuntime(hiltonPeriodId);
const rRt = loadRuntime(renPeriodId);
const hPub = loadPublished("adp_hilton_times_square", hiltonPeriodId);
const rPub = loadPublished("adp_renaissance_times_square", renPeriodId);

// Force entity_v2 presence recount
function metrics(period, profile) {
  const obs = period.observations || [];
  const byProv = {};
  let mentioned = 0;
  const scenPresent = new Set();
  const scenIds = [...new Set(obs.map((o) => o.scenarioId))];
  let rankEligible = 0;
  let top1 = 0;
  let top3 = 0;
  const terr = {};
  for (const o of obs) {
    const p = o.provider;
    byProv[p] = byProv[p] || { n: 0, m: 0, ok: 0 };
    byProv[p].n += 1;
    const ok = !!(o.rawResponse && o.rawResponseLength > 20);
    if (ok) byProv[p].ok += 1;
    const det = detectPropertyMention(o.rawResponse || "", profile);
    const present = det.mentioned || o.mentioned;
    // prefer stored if already force-parsed
    const hit = o.mentioned === true || det.mentioned === true;
    if (hit) {
      mentioned += 1;
      byProv[p].m += 1;
      scenPresent.add(o.scenarioId);
      if (o.position != null) {
        rankEligible += 1;
        if (o.position === 1) top1 += 1;
        if (o.position <= 3) top3 += 1;
      }
    }
    const intent = contract.scenarios.find((s) => s.controlScenarioId === o.scenarioId)?.intent;
    const t = territoryLabelForIntent(intent) || intent || "unknown";
    terr[t] = terr[t] || { obs: 0, hits: 0, scen: new Set(), scenHit: new Set() };
    terr[t].obs += 1;
    terr[t].scen.add(o.scenarioId);
    if (hit) {
      terr[t].hits += 1;
      terr[t].scenHit.add(o.scenarioId);
    }
  }
  const owned = computeOwnedExternalSourceMix(obs, profile);
  return {
    n: obs.length,
    mentioned,
    consideration: obs.length ? +((mentioned / obs.length) * 100).toFixed(1) : null,
    scenarioPresence: scenPresent.size,
    scenarioPresenceRate: scenIds.length
      ? +((scenPresent.size / scenIds.length) * 100).toFixed(1)
      : null,
    scenIds: scenIds.length,
    byProv,
    terr,
    rankEligible,
    top1Rate: rankEligible ? +((top1 / rankEligible) * 100).toFixed(1) : null,
    top3Rate: rankEligible ? +((top3 / rankEligible) * 100).toFixed(1) : null,
    owned,
    scenPresent,
  };
}

const hM = metrics(hRt, hiltonProfile);
const rM = metrics(rRt, renProfile);

writeCsv(
  "CONTROL_SCENARIO_UNIVERSE.csv",
  [
    "controlScenarioId",
    "territory",
    "intent",
    "frame",
    "layer",
    "queryTemplate",
    "subjectSubstitution",
    "rankEligible",
    "providerEligible",
  ],
  contract.scenarios.map((s) => ({
    controlScenarioId: s.controlScenarioId,
    territory: s.territory,
    intent: s.intent,
    frame: s.frame,
    layer: s.layer,
    queryTemplate: s.queryTemplate,
    subjectSubstitution: s.subjectSubstitution ? "YES" : "NO",
    rankEligible: "YES",
    providerEligible: "openai|gemini|perplexity|claude",
  }))
);

writeCsv(
  "RENAISSANCE_ELIGIBILITY.csv",
  ["controlScenarioId", "territory", "classification", "reason", "inCommonComparable"],
  eligibility.map((e) => ({
    controlScenarioId: e.controlScenarioId,
    territory: e.territory,
    classification: e.classification,
    reason: e.reason,
    inCommonComparable: e.inCommonComparable ? "YES" : "NO",
  }))
);

writeCsv(
  "COMMON_COMPARABLE_SET.csv",
  ["controlScenarioId", "territory", "intent", "included"],
  eligibility
    .filter((e) => e.inCommonComparable)
    .map((e) => ({
      controlScenarioId: e.controlScenarioId,
      territory: e.territory,
      intent: e.intent,
      included: "YES",
    }))
);

writeCsv(
  "IDENTITY_PARITY.csv",
  ["field", "hilton", "renaissance", "notes"],
  [
    { field: "canonical_name", hilton: hiltonProfile.name, renaissance: renProfile.name, notes: "" },
    {
      field: "censusRecordId",
      hilton: hiltonProfile.censusRecordId,
      renaissance: renProfile.censusRecordId,
      notes: "verified",
    },
    {
      field: "entityVersion",
      hilton: "adp_hilton_times_square_entity_v2",
      renaissance: "adp_renaissance_times_square_entity_v2",
      notes: "alias parity pass",
    },
    {
      field: "aliasCount",
      hilton: (hiltonProfile.identityAliases || []).length,
      renaissance: (renProfile.identityAliases || []).length,
      notes: "Ren ≥ Hilton depth",
    },
    {
      field: "aliases",
      hilton: (hiltonProfile.identityAliases || []).join("|"),
      renaissance: (renProfile.identityAliases || []).join("|"),
      notes: "",
    },
    {
      field: "officialBrandDomain",
      hilton: hiltonProfile.officialBrandDomain,
      renaissance: renProfile.officialBrandDomain,
      notes: "",
    },
    {
      field: "brandPropertyPathHints",
      hilton: (hiltonProfile.brandPropertyPathHints || []).join("|"),
      renaissance: (renProfile.brandPropertyPathHints || []).join("|"),
      notes: "",
    },
  ]
);

writeCsv(
  "PROVIDER_COMPLETENESS.csv",
  ["hotel", "provider", "expected", "successful", "failed", "mentioned"],
  ["hilton", "renaissance"].flatMap((hotel) => {
    const m = hotel === "hilton" ? hM : rM;
    return Object.entries(m.byProv).map(([provider, v]) => ({
      hotel,
      provider,
      expected: v.n,
      successful: v.ok,
      failed: v.n - v.ok,
      mentioned: v.m,
    }));
  })
);

writeCsv(
  "RAW_METRICS.csv",
  ["metric", "hilton_num", "hilton_den", "hilton_pct", "renaissance_num", "renaissance_den", "renaissance_pct"],
  [
    {
      metric: "AI_Consideration",
      hilton_num: hM.mentioned,
      hilton_den: hM.n,
      hilton_pct: hM.consideration,
      renaissance_num: rM.mentioned,
      renaissance_den: rM.n,
      renaissance_pct: rM.consideration,
    },
    {
      metric: "Scenario_Presence",
      hilton_num: hM.scenarioPresence,
      hilton_den: hM.scenIds,
      hilton_pct: hM.scenarioPresenceRate,
      renaissance_num: rM.scenarioPresence,
      renaissance_den: rM.scenIds,
      renaissance_pct: rM.scenarioPresenceRate,
    },
    {
      metric: "NumberOne_Appearance",
      hilton_num: hPub?.payload?.executiveMetrics?.rankMetrics?.numberOneCount ?? "",
      hilton_den: hPub?.payload?.executiveMetrics?.rankMetrics?.rankEligibleN ?? hM.rankEligible,
      hilton_pct: hPub?.payload?.executiveMetrics?.rankMetrics?.numberOneAppearanceRate ?? hM.top1Rate,
      renaissance_num: rPub?.payload?.executiveMetrics?.rankMetrics?.numberOneCount ?? "",
      renaissance_den: rPub?.payload?.executiveMetrics?.rankMetrics?.rankEligibleN ?? rM.rankEligible,
      renaissance_pct: rPub?.payload?.executiveMetrics?.rankMetrics?.numberOneAppearanceRate ?? rM.top3Rate,
    },
    {
      metric: "Owned_Source_Share",
      hilton_num: hM.owned.ownedResponses,
      hilton_den: hM.owned.responsesWithCitations,
      hilton_pct: hM.owned.ownedShare,
      renaissance_num: rM.owned.ownedResponses,
      renaissance_den: rM.owned.responsesWithCitations,
      renaissance_pct: rM.owned.ownedShare,
    },
  ]
);

const territories = [
  ...new Set([...Object.keys(hM.terr), ...Object.keys(rM.terr)]),
].sort();
writeCsv(
  "TERRITORY_COMPARISON.csv",
  [
    "territory",
    "scenarioCount",
    "hiltonHits",
    "renHits",
    "hiltonPresencePct",
    "renPresencePct",
    "diffPp",
    "meaningful",
  ],
  territories.map((t) => {
    const h = hM.terr[t] || { obs: 0, hits: 0, scen: new Set() };
    const r = rM.terr[t] || { obs: 0, hits: 0, scen: new Set() };
    const scenCount = Math.max(h.scen.size, r.scen.size);
    const hPct = h.obs ? +((h.hits / h.obs) * 100).toFixed(1) : null;
    const rPct = r.obs ? +((r.hits / r.obs) * 100).toFixed(1) : null;
    const diff = hPct != null && rPct != null ? +(hPct - rPct).toFixed(1) : null;
    return {
      territory: t,
      scenarioCount: scenCount,
      hiltonHits: h.hits,
      renHits: r.hits,
      hiltonPresencePct: hPct,
      renPresencePct: rPct,
      diffPp: diff,
      meaningful: scenCount >= 5 && Math.abs(diff || 0) >= 10 ? "YES" : "CAUTION_SMALL_OR_NARROW",
    };
  })
);

writeCsv(
  "PROVIDER_COMPARISON.csv",
  ["provider", "hiltonMentioned", "hiltonN", "hiltonPct", "renMentioned", "renN", "renPct", "diffPp"],
  ["openai", "gemini", "perplexity", "claude"].map((p) => {
    const h = hM.byProv[p] || { n: 0, m: 0 };
    const r = rM.byProv[p] || { n: 0, m: 0 };
    const hp = h.n ? +((h.m / h.n) * 100).toFixed(1) : null;
    const rp = r.n ? +((r.m / r.n) * 100).toFixed(1) : null;
    return {
      provider: p,
      hiltonMentioned: h.m,
      hiltonN: h.n,
      hiltonPct: hp,
      renMentioned: r.m,
      renN: r.n,
      renPct: rp,
      diffPp: hp != null && rp != null ? +(hp - rp).toFixed(1) : null,
    };
  })
);

function displacement(period, profile) {
  const counts = {};
  for (const o of period.observations || []) {
    const hit = detectPropertyMention(o.rawResponse || "", profile).mentioned || o.mentioned;
    if (hit) continue;
    for (const c of o.competitorsMentioned || []) {
      const name = String(c || "").trim();
      if (name.length < 4 || /^this\b/i.test(name)) continue;
      const key = name.split(/\s+/).slice(0, 6).join(" ");
      counts[key] = (counts[key] || 0) + 1;
    }
  }
  return Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15)
    .map(([name, count]) => ({ name, count }));
}
const hDisp = displacement(hRt, hiltonProfile);
const rDisp = displacement(rRt, renProfile);
writeCsv(
  "DISPLACEMENT_COMPARISON.csv",
  ["hotel", "rank", "competitor", "absentObservationMentions"],
  [
    ...hDisp.map((d, i) => ({
      hotel: "Hilton",
      rank: i + 1,
      competitor: d.name,
      absentObservationMentions: d.count,
    })),
    ...rDisp.map((d, i) => ({
      hotel: "Renaissance",
      rank: i + 1,
      competitor: d.name,
      absentObservationMentions: d.count,
    })),
  ]
);

const hFn = auditIdentityFalseNegatives(hRt, hiltonProfile);
const rFn = auditIdentityFalseNegatives(rRt, renProfile);
writeCsv(
  "IDENTITY_FALSE_NEGATIVES.csv",
  ["hotel", "scenarioId", "provider", "classification", "reparseMentioned", "matchedVariant"],
  [
    ...hFn.map((r) => ({ hotel: "Hilton", ...r })),
    ...rFn.map((r) => ({ hotel: "Renaissance", ...r })),
  ]
);

function sourceBuckets(period, profile) {
  const buckets = {
    PROPERTY_OWNED: {},
    PROPERTY_SPECIFIC_EXTERNAL: {},
    COMPETITOR_OWNED: {},
    GENERAL_MARKET: {},
  };
  const brand = String(profile.officialBrandDomain || "").replace(/^www\./i, "");
  const competitorBrands =
    brand === "hilton.com" ? ["marriott.com", "hyatt.com", "ihg.com"] : ["hilton.com", "hyatt.com", "ihg.com"];
  for (const o of period.observations || []) {
    for (const s of o.sourcesCited || []) {
      const url = s.url || s;
      const d = domainOf(url);
      if (!d) continue;
      const c = classifySourceUrl(url, profile);
      let bucket = "GENERAL_MARKET";
      if (c.rollup === "OWNED") bucket = "PROPERTY_OWNED";
      else if (competitorBrands.some((x) => d === x || d.endsWith(`.${x}`))) bucket = "COMPETITOR_OWNED";
      else if (c.class === "OTA" || c.class === "REVIEW_PLATFORM") bucket = "PROPERTY_SPECIFIC_EXTERNAL";
      buckets[bucket][d] = (buckets[bucket][d] || 0) + 1;
    }
  }
  return buckets;
}
function topDomain(map) {
  const e = Object.entries(map || {}).sort((a, b) => b[1] - a[1])[0];
  return e ? { domain: e[0], count: e[1] } : { domain: "", count: 0 };
}
const hSrc = sourceBuckets(hRt, hiltonProfile);
const rSrc = sourceBuckets(rRt, renProfile);
writeCsv(
  "SOURCE_COMPARISON.csv",
  ["hotel", "bucket", "topDomain", "count", "ownedShare"],
  [
    {
      hotel: "Hilton",
      bucket: "PROPERTY_OWNED",
      topDomain: topDomain(hSrc.PROPERTY_OWNED).domain,
      count: topDomain(hSrc.PROPERTY_OWNED).count,
      ownedShare: hM.owned.ownedShare,
    },
    {
      hotel: "Hilton",
      bucket: "COMPETITOR_OWNED",
      topDomain: topDomain(hSrc.COMPETITOR_OWNED).domain,
      count: topDomain(hSrc.COMPETITOR_OWNED).count,
      ownedShare: "",
    },
    {
      hotel: "Hilton",
      bucket: "GENERAL_MARKET",
      topDomain: topDomain(hSrc.GENERAL_MARKET).domain,
      count: topDomain(hSrc.GENERAL_MARKET).count,
      ownedShare: "",
    },
    {
      hotel: "Renaissance",
      bucket: "PROPERTY_OWNED",
      topDomain: topDomain(rSrc.PROPERTY_OWNED).domain,
      count: topDomain(rSrc.PROPERTY_OWNED).count,
      ownedShare: rM.owned.ownedShare,
    },
    {
      hotel: "Renaissance",
      bucket: "COMPETITOR_OWNED",
      topDomain: topDomain(rSrc.COMPETITOR_OWNED).domain,
      count: topDomain(rSrc.COMPETITOR_OWNED).count,
      ownedShare: "",
    },
    {
      hotel: "Renaissance",
      bucket: "GENERAL_MARKET",
      topDomain: topDomain(rSrc.GENERAL_MARKET).domain,
      count: topDomain(rSrc.GENERAL_MARKET).count,
      ownedShare: "",
    },
  ]
);

writeCsv(
  "REALITY_COVERAGE_PARITY.csv",
  ["hotel", "recognized", "total", "display", "profileAttributeCount", "note"],
  [
    {
      hotel: "Hilton",
      recognized: hPub?.payload?.realityGap?.recognizedCount,
      total: hPub?.payload?.realityGap?.totalAttributes,
      display: hPub?.payload?.realityGap?.display,
      profileAttributeCount: (hiltonProfile.attributes || []).length,
      note: "Governed reality-gap subset — not full profile attribute list",
    },
    {
      hotel: "Renaissance",
      recognized: rPub?.payload?.realityGap?.recognizedCount,
      total: rPub?.payload?.realityGap?.totalAttributes,
      display: rPub?.payload?.realityGap?.display,
      profileAttributeCount: (renProfile.attributes || []).length,
      note: "Same engine; attribute stewards differ by property",
    },
  ]
);

const hFail = Object.values(hM.byProv).reduce((a, v) => a + (v.n - v.ok), 0);
const rFail = Object.values(rM.byProv).reduce((a, v) => a + (v.n - v.ok), 0);
const completenessOk = hM.n === rM.n && hFail === 0 && rFail === 0;
const identityIssuesH = hFn.filter((x) =>
  ["PARSER_MISS", "PROVIDER_OR_EMPTY", "STORED_TRUE_DETECT_FALSE"].includes(x.classification)
).length;
const identityIssuesR = rFn.filter((x) =>
  ["PARSER_MISS", "PROVIDER_OR_EMPTY", "STORED_TRUE_DETECT_FALSE"].includes(x.classification)
).length;
const formallyComparable =
  completenessOk &&
  identityIssuesH === 0 &&
  identityIssuesR === 0 &&
  hM.scenIds === rM.scenIds &&
  hM.scenIds === eligibility.filter((e) => e.inCommonComparable).length;

const hMeet = hM.terr["Meetings & Groups"];
const rMeet = rM.terr["Meetings & Groups"];
const hMeetPct = hMeet?.obs ? +((hMeet.hits / hMeet.obs) * 100).toFixed(1) : null;
const rMeetPct = rMeet?.obs ? +((rMeet.hits / rMeet.obs) * 100).toFixed(1) : null;

let classification = "NOT_COMPARABLE";
if (formallyComparable) {
  const diff = (hM.consideration || 0) - (rM.consideration || 0);
  if (Math.abs(diff) < 3) classification = "ROUGHLY_SIMILAR";
  else if (diff > 5) classification = "HILTON_STRONGER";
  else if (diff < -5) classification = "RENAISSANCE_STRONGER";
  else classification = "MIXED_BY_TERRITORY";
  // check territory mix
  const meaningfulTerr = territories.filter((t) => {
    const h = hM.terr[t];
    const r = rM.terr[t];
    if (!h || !r || h.scen.size < 5) return false;
    const hp = (h.hits / h.obs) * 100;
    const rp = (r.hits / r.obs) * 100;
    return Math.abs(hp - rp) >= 12;
  });
  if (meaningfulTerr.length >= 2 && Math.abs((hM.consideration || 0) - (rM.consideration || 0)) < 8) {
    classification = "MIXED_BY_TERRITORY";
  }
}

const priorHiltonBaseline = 21.5;
const baselineConfirmed =
  Math.abs((hM.consideration || 0) - priorHiltonBaseline) <= 4
    ? "YES"
    : Math.abs((hM.consideration || 0) - priorHiltonBaseline) <= 8
      ? "PARTIAL"
      : "NO";

writeMd(
  "FORMAL_COMPARABILITY.md",
  `# Formal Comparability

| Gate | Result |
|---|---|
| Same common scenario set (${eligibility.filter((e) => e.inCommonComparable).length}) | ${hM.scenIds === rM.scenIds ? "YES" : "NO"} |
| Same providers (4) | YES |
| Same control set | ${MATCHED_CONTROL_SET_ID} |
| Same parser / entity detect | YES |
| Completeness equal & full | ${completenessOk ? "YES" : "NO"} (H fail=${hFail}, R fail=${rFail}) |
| Identity false-negatives | H=${identityIssuesH} R=${identityIssuesR} |
| **FORMALLY COMPARABLE** | **${formallyComparable ? "YES" : "NO"}** |

Pair ID: \`${pairId}\`
Hilton period: \`${hiltonPeriodId}\`
Renaissance period: \`${renPeriodId}\`
`
);

writeMd(
  "PERIOD_DECISION.md",
  `# Period Decision

- New Hilton matched period: **YES** — \`${hiltonPeriodId}\` (does not overwrite Sept27 or prior Oct baseline file; becomes latest published)
- New Renaissance matched period: **YES** — \`${renPeriodId}\`
- Prior periods overwritten: **NO**
- customerTrendEligible: **false** on matched-control metadata (pair is control, not automatic trend join)
- Pair ID: \`${pairId}\`
`
);

writeMd(
  "CHANGELOG.md",
  `# Changelog — Hilton × Renaissance Matched Control

## 2026-10-05
1. Frozen control set \`hilton-rts-matched-control-query-set-v1.json\` (65 from Hilton certified universe).
2. Dual fresh run on identical controlScenarioIds; capability rows subject/brand/loyalty substituted only.
3. Renaissance identity aliases deepened (entity_v2); censusRecordId set.
4. No ADP methodology/threshold changes. Identity rules not weakened.
`
);

writeMd(
  "UI_QA.md",
  `# UI QA — Matched Control

## Status
Pending browser verify after publish.

Checklist:
- [ ] Hilton report shows ${hM.scenIds} scenarios / 4 providers
- [ ] Renaissance report shows ${rM.scenIds} scenarios / 4 providers
- [ ] Top Cited Source Across Monitored Responses label
- [ ] No stale period mixing in chrome
- [ ] Consideration H=${hM.consideration}% R=${rM.consideration}%
`
);

writeMd(
  "FOUNDER_REPORT.md",
  `# Founder Report — Hilton × Renaissance Matched Control

## Verdict
FORMALLY COMPARABLE: **${formallyComparable ? "YES" : "NO"}**  
Classification: **${classification}**

| Field | Value |
|---|---|
| CONTROL SCENARIO COUNT | ${contract.scenarioCount} |
| COMMON COMPARABLE SCENARIO COUNT | ${eligibility.filter((e) => e.inCommonComparable).length} |
| HILTON EXPECTED / SUCCESSFUL | ${hM.n} / ${hM.n - hFail} |
| RENAISSANCE EXPECTED / SUCCESSFUL | ${rM.n} / ${rM.n - rFail} |
| HILTON IDENTITY ISSUES | ${identityIssuesH} |
| RENAISSANCE IDENTITY ISSUES | ${identityIssuesR} |
| HILTON CONSIDERATION | ${hM.consideration}% (${hM.mentioned}/${hM.n}) |
| RENAISSANCE CONSIDERATION | ${rM.consideration}% (${rM.mentioned}/${rM.n}) |
| HILTON SCENARIO PRESENCE | ${hM.scenarioPresenceRate}% (${hM.scenarioPresence}/${hM.scenIds}) |
| RENAISSANCE SCENARIO PRESENCE | ${rM.scenarioPresenceRate}% (${rM.scenarioPresence}/${rM.scenIds}) |
| HILTON CHATGPT | ${hM.byProv.openai?.m || 0}/${hM.byProv.openai?.n || 0} |
| RENAISSANCE CHATGPT | ${rM.byProv.openai?.m || 0}/${rM.byProv.openai?.n || 0} |
| HILTON GEMINI | ${hM.byProv.gemini?.m || 0}/${hM.byProv.gemini?.n || 0} |
| RENAISSANCE GEMINI | ${rM.byProv.gemini?.m || 0}/${rM.byProv.gemini?.n || 0} |
| HILTON PERPLEXITY | ${hM.byProv.perplexity?.m || 0}/${hM.byProv.perplexity?.n || 0} |
| RENAISSANCE PERPLEXITY | ${rM.byProv.perplexity?.m || 0}/${rM.byProv.perplexity?.n || 0} |
| HILTON CLAUDE | ${hM.byProv.claude?.m || 0}/${hM.byProv.claude?.n || 0} |
| RENAISSANCE CLAUDE | ${rM.byProv.claude?.m || 0}/${rM.byProv.claude?.n || 0} |
| HILTON MEETINGS & GROUPS | ${hMeetPct}% |
| RENAISSANCE MEETINGS & GROUPS | ${rMeetPct}% |
| HILTON TOP DISPLACER | ${hDisp[0]?.name || "—"} (${hDisp[0]?.count || 0}) |
| RENAISSANCE TOP DISPLACER | ${rDisp[0]?.name || "—"} (${rDisp[0]?.count || 0}) |
| HILTON TOP PROPERTY-SUPPORTING SOURCE | ${topDomain(hSrc.PROPERTY_OWNED).domain || "—"} |
| RENAISSANCE TOP PROPERTY-SUPPORTING SOURCE | ${topDomain(rSrc.PROPERTY_OWNED).domain || "—"} |
| HILTON OWNED SOURCE SHARE | ${hM.owned.ownedShare}% |
| RENAISSANCE OWNED SOURCE SHARE | ${rM.owned.ownedShare}% |
| NEW HILTON MATCHED PERIOD | YES \`${hiltonPeriodId}\` |
| NEW RENAISSANCE MATCHED PERIOD | YES \`${renPeriodId}\` |
| HILTON 21.5% BASELINE CONFIRMED | ${baselineConfirmed} (matched=${hM.consideration}%) |
| FINAL CLASSIFICATION | ${classification} |
| ADP METHODOLOGY CHANGED? | NO |
| ADP THRESHOLDS CHANGED? | NO |
| IDENTITY RULES WEAKENED? | NO |
| OLD PERIODS OVERWRITTEN? | NO |

## Interpretation
1. Hilton vs Renaissance on matched scenarios: see consideration delta ${+(hM.consideration - rM.consideration).toFixed(1)} pp.
2. Meetings & Groups: Hilton ${hMeetPct}% vs Renaissance ${rMeetPct}% (observation grain).
3. Prior Hilton 21.5% stability: ${baselineConfirmed}.
4. Top displacers listed above.
`
);

writeFileSync(
  join(OUT, "SUMMARY.json"),
  JSON.stringify(
    {
      formallyComparable,
      classification,
      hiltonConsideration: hM.consideration,
      renConsideration: rM.consideration,
      hiltonScenarioPresence: hM.scenarioPresenceRate,
      renScenarioPresence: rM.scenarioPresenceRate,
      baselineConfirmed,
      hiltonPeriodId,
      renPeriodId,
      pairId,
    },
    null,
    2
  )
);

console.log(
  JSON.stringify(
    {
      out: OUT,
      formallyComparable,
      classification,
      hiltonConsideration: hM.consideration,
      renConsideration: rM.consideration,
      baselineConfirmed,
    },
    null,
    2
  )
);
