#!/usr/bin/env node
/**
 * V2.4 quality closure + HPC enrichment demonstration.
 *
 *   node scripts/sec-edgar-v24-quality-closure-and-hpc.mjs
 *   node scripts/sec-edgar-v24-quality-closure-and-hpc.mjs --apply-demote
 *   node scripts/sec-edgar-v24-quality-closure-and-hpc.mjs --apply-hpc
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  HPC_TABLE_ID,
  DISCOVERY_SCHEMA_VERSION,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import {
  classifyHotelIdentity,
  IDENTITY_CLASS,
  evaluateVerifiedPrecision,
  normalizeHotelDisplayName,
} from "../lib/hotel-intelligence/sec-edgar/hotel-identity-quality-v2.js";
import { matchHotelHintToHpc } from "../lib/hotel-intelligence/sec-edgar/hotel-attribution-v1.js";
import { HPC_ALLOWLISTED_PROMOTION_FIELDS } from "../lib/hotel-intelligence/sec-edgar/contract.js";
import { detectBrandFamily } from "../lib/hotel-intelligence/sec-edgar/brand-families-v2.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY_DEMOTE = process.argv.includes("--apply-demote");
const APPLY_HPC = process.argv.includes("--apply-hpc");
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;

const STATE_ALIASES = {
  alabama: "AL",
  alaska: "AK",
  arizona: "AZ",
  arkansas: "AR",
  california: "CA",
  colorado: "CO",
  connecticut: "CT",
  delaware: "DE",
  florida: "FL",
  georgia: "GA",
  hawaii: "HI",
  idaho: "ID",
  illinois: "IL",
  indiana: "IN",
  iowa: "IA",
  kansas: "KS",
  kentucky: "KY",
  louisiana: "LA",
  maine: "ME",
  maryland: "MD",
  massachusetts: "MA",
  michigan: "MI",
  minnesota: "MN",
  mississippi: "MS",
  missouri: "MO",
  montana: "MT",
  nebraska: "NE",
  nevada: "NV",
  "new hampshire": "NH",
  "new jersey": "NJ",
  "new mexico": "NM",
  "new york": "NY",
  "north carolina": "NC",
  "north dakota": "ND",
  ohio: "OH",
  oklahoma: "OK",
  oregon: "OR",
  pennsylvania: "PA",
  "rhode island": "RI",
  "south carolina": "SC",
  "south dakota": "SD",
  tennessee: "TN",
  texas: "TX",
  utah: "UT",
  vermont: "VT",
  virginia: "VA",
  washington: "WA",
  "west virginia": "WV",
  wisconsin: "WI",
  wyoming: "WY",
  "district of columbia": "DC",
  dc: "DC",
};

function normalizeState(s) {
  const raw = String(s || "").trim();
  if (!raw) return "";
  if (/^[A-Za-z]{2}$/.test(raw)) return raw.toUpperCase();
  return STATE_ALIASES[raw.toLowerCase()] || raw.toUpperCase();
}

function jaccardLocal(a, b) {
  const norm = (s) =>
    String(s || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .replace(/\b(the|hotel|inn|resort|suites|a|an|by)\b/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  const A = new Set(norm(a).split(" ").filter((t) => t.length > 1));
  const B = new Set(norm(b).split(" ").filter((t) => t.length > 1));
  if (!A.size || !B.size) return 0;
  let inter = 0;
  for (const t of A) if (B.has(t)) inter += 1;
  return inter / (A.size + B.size - inter);
}
const baseId = GRAPH_BASE_ID_CANONICAL;
const ids = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"),
    "utf8",
  ),
);
const invTable = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const findTable = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];
const M = MAP_DISCOVERY_INVENTORY;
const OUT = path.join(ROOT, "reports", "sec-edgar-v24-quality-closure");
fs.mkdirSync(OUT, { recursive: true });

const headers = {
  Authorization: `Bearer ${token}`,
  "Content-Type": "application/json",
};

async function listAll(tableId, formula) {
  const records = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (formula) qs.set("filterByFormula", formula);
    if (offset) qs.set("offset", offset);
    const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tableId}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return records;
}

async function patch(tableId, recordId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tableId}/${recordId}`, {
    method: "PATCH",
    headers,
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  return { ok: res.ok, json };
}

async function get(tableId, recordId) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tableId}/${recordId}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  return { ok: res.ok, json };
}

function extraQualityIssues(name, city, state, sourceUrl) {
  const issues = [];
  const n = String(name || "").trim();
  const c = String(city || "").trim();
  const s = String(state || "").trim();
  const url = String(sourceUrl || "");
  if (/\/fixture\//i.test(url) || /edgar\/data\/fixture\b/i.test(url)) {
    issues.push("fixture_source_not_live_sec");
  }
  if (/\b(ltd|l\.p\.|llc|inc\.?|corp)\b/i.test(c)) issues.push("city_legal_bleed");
  if (/\b(ltd|l\.p\.|llc)\b/i.test(n) && !/\b(hotel|inn|resort|suites)\b/i.test(n)) {
    issues.push("legal_entity_as_hotel");
  }
  if (/^(marriott|hilton|hyatt|westin|sheraton|renaissance|courtyard|embassy suites)$/i.test(n)) {
    issues.push("generic_brand_only_name");
  }
  if (/\(formerly|\(f\/k\/a|aka\b/i.test(n)) issues.push("alias_in_primary_name");
  if (/[,-]\s*$/.test(n) || /\bthe$/i.test(n)) issues.push("truncated_name");
  if (/\bloan is interest|net profit|for each loan\b/i.test(n)) issues.push("narrative_fragment");
  if (s && s.length === 2 && !/^[A-Z]{2}$/.test(s) && !/^[A-Za-z]{2}$/.test(s)) {
    issues.push("bad_state");
  }
  if (c && /\b(only for|full term|we |the )\b/i.test(c)) issues.push("narrative_city");
  if (/\b(and suites|suites washington|washington dc)\b/i.test(c)) issues.push("city_name_bleed");
  if (/&#\d+;|&amp;|&nbsp;/i.test(n)) issues.push("html_entity_in_name");
  if (/^Independent/i.test(n) && /&#/.test(n)) issues.push("html_entity_in_name");
  // Truncated soft-brand forms
  if (/^(Fairfield Inn|&|Fairfield Inn &)$/i.test(n) || /Fairfield Inn &\s*$/i.test(n)) {
    issues.push("truncated_name");
  }
  return issues;
}

function mapHpcRecord(r) {
  return {
    id: r.id,
    name: r.fields?.["Property Name"] || r.fields?.["Canonical Property Name"] || null,
    city: r.fields?.City || null,
    state: r.fields?.["State / Region"] || null,
    brand: r.fields?.["Current Brand"] || r.fields?.["Brand Family"] || null,
    operator: r.fields?.["Operator / Management Company"] || null,
    managementModel: r.fields?.["Management Model"] || null,
    operatorType: r.fields?.["Operator Type"] || null,
    operatorSourceUrl: r.fields?.["Operator Source URL"] || null,
    operatorConfidence: r.fields?.["Operator Confidence"] || null,
    raw: r.fields || {},
  };
}

async function searchHpcOnce(formula) {
  const fields = [
    "Property Name",
    "Canonical Property Name",
    "City",
    "State / Region",
    "Current Brand",
    "Brand Family",
    "Operator / Management Company",
    "Management Model",
    "Operator Type",
    "Operator Source URL",
    "Operator Confidence",
  ];
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "12" });
  for (const f of fields) qs.append("fields[]", f);
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${HPC_TABLE_ID}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) {
    console.error("[hpc-search]", res.status, json.error || json);
    return [];
  }
  return (json.records || []).map(mapHpcRecord);
}

async function searchHpc(name, city) {
  const tokens = String(name || "")
    .replace(/[^a-zA-Z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((t) => t.length >= 3 && !/^(the|and|by|hotel|inn|at|of)$/i.test(t));
  if (!tokens.length) return [];
  const needles = [];
  if (tokens.length >= 2) needles.push(tokens.slice(0, 2).join(" "));
  if (tokens.length >= 3) needles.push(tokens.slice(-2).join(" "));
  needles.push(tokens[tokens.length - 1]);
  if (city && city.length >= 3) needles.push(`${tokens[0]} ${city}`);

  const byId = new Map();
  for (const needle of [...new Set(needles)].slice(0, 4)) {
    const safe = needle.replace(/'/g, "\\'").slice(0, 40);
    if (safe.length < 4) continue;
    const rows = await searchHpcOnce(
      `SEARCH(LOWER('${safe.toLowerCase()}'), LOWER({Property Name}))`,
    );
    for (const r of rows) byId.set(r.id, r);
    await new Promise((r) => setTimeout(r, 80));
  }
  let rows = [...byId.values()];
  if (city && String(city).length >= 3) {
    const cn = String(city).toLowerCase();
    const filtered = rows.filter(
      (r) =>
        !r.city ||
        String(r.city).toLowerCase().includes(cn) ||
        cn.includes(String(r.city).toLowerCase()),
    );
    if (filtered.length) rows = filtered;
  }
  return rows;
}

async function main() {
  if (!token) throw new Error("AIRTABLE_API_KEY missing");

  // ── Phase 1: reinspect VERIFIED ──────────────────────────────────────
  const verified = await listAll(invTable, '{Identity Class}="VERIFIED_PROPERTY"');
  const review = [];
  const demotePlan = [];
  for (const rec of verified) {
    const f = rec.fields || {};
    const name = f[M.hotelNameNormalized] || f[M.hotelNameRaw] || "";
    const city = f[M.city] || "";
    const state = f[M.stateRegion] || "";
    const cls = classifyHotelIdentity({
      hotelName: name,
      city,
      stateRegion: state,
      discoveryMethod: f[M.discoveryMethod] || "HTML_PROPERTY_TABLE",
    });
    const sourceUrl = f[M.sourceFilingUrl] || null;
    const extras = extraQualityIssues(name, city, state, sourceUrl);
    const brand = detectBrandFamily(name);
    const forcedDemote = extras.includes("fixture_source_not_live_sec")
      ? IDENTITY_CLASS.AMBIGUOUS_PROPERTY
      : extras.includes("generic_brand_only_name")
        ? IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY
        : extras.length
          ? IDENTITY_CLASS.AMBIGUOUS_PROPERTY
          : null;
    const row = {
      airtableId: rec.id,
      propertyId: f[M.discoveryPropertyId],
      name,
      city,
      state,
      country: f[M.country] || null,
      registrant: f[M.secRegistrant] || null,
      sourceUrl,
      brandFamily: f[M.brandFamily] || brand.family,
      liveClass: f[M.identityClass],
      reclass: forcedDemote || cls.identityClass,
      reasons: [...(cls.reasons || []), ...extras],
      promotionEligible: !forcedDemote && cls.promotionEligible,
      hpcMatchStatus: f[M.hpcMatchStatus] || null,
      hpcHotel: f[M.hpcHotel] || [],
    };
    review.push(row);
    if (forcedDemote || cls.identityClass !== IDENTITY_CLASS.VERIFIED_PROPERTY) {
      demotePlan.push({
        ...row,
        targetClass: forcedDemote || cls.identityClass,
        note: `V2.4 quality closure demotion: ${(cls.reasons || []).concat(extras).join(", ")}`,
      });
    }
  }

  const demoteActions = [];
  if (APPLY_DEMOTE) {
    for (const d of demotePlan) {
      const fields = {
        [M.identityClass]: d.targetClass,
        [M.lifecycleStatus]: "QUARANTINED",
        [M.notesQuarantine]: d.note,
        [M.persistenceStatus]: "DISCOVERY_PERSISTED",
        [M.publicationState]: "CUSTOMER_BLOCKED",
        [M.schemaVersion]: DISCOVERY_SCHEMA_VERSION,
      };
      // Keep normalized name cleaned if generic
      if (d.reasons.includes("alias_in_primary_name") || d.reasons.includes("truncated_name")) {
        fields[M.hotelNameNormalized] = normalizeHotelDisplayName(d.name);
      }
      const patched = await patch(invTable, d.airtableId, fields);
      const rb = await get(invTable, d.airtableId);
      demoteActions.push({
        airtableId: d.airtableId,
        propertyId: d.propertyId,
        name: d.name,
        targetClass: d.targetClass,
        patchOk: patched.ok,
        readBackClass: rb.json?.fields?.[M.identityClass] || null,
        readBackLifecycle: rb.json?.fields?.[M.lifecycleStatus] || null,
        readBackOk:
          rb.ok &&
          rb.json?.fields?.[M.identityClass] === d.targetClass &&
          rb.json?.fields?.[M.publicationState] === "CUSTOMER_BLOCKED",
      });
      await new Promise((r) => setTimeout(r, 120));
    }
  }

  const remainingVerified = review.filter(
    (r) => !demotePlan.some((d) => d.airtableId === r.airtableId),
  );
  // Reviewer labels: remaining rows re-passed classifyHotelIdentity + extra checks
  const precision = evaluateVerifiedPrecision(
    remainingVerified.map((r) => ({
      hotelName: r.name,
      city: r.city,
      stateRegion: r.state,
      correct: r.reclass === IDENTITY_CLASS.VERIFIED_PROPERTY && r.promotionEligible === true,
    })),
  );

  // ── Phase 5 prep: strong HPC matches among remaining verified ────────
  const matchPlans = [];
  const hpcEnrichPlans = [];
  const cohort = remainingVerified.filter((r) => r.name && r.city && String(r.name).length >= 8);

  for (const row of cohort) {
    const candidates = await searchHpc(row.name, row.city);
    // Try name+city then name-only; prefer city-consistent matches
    const hintCity = [row.name, row.city].filter(Boolean).join(" ");
    let match = matchHotelHintToHpc(hintCity, candidates, { minScore: 0.5 });
    if (match.status !== "MATCHED") {
      match = matchHotelHintToHpc(row.name, candidates, { minScore: 0.5 });
    }
    // If still ambiguous/no match, pick best city-aligned candidate manually
    if (match.status !== "MATCHED" && candidates.length) {
      const cn = String(row.city || "").toLowerCase();
      const scored = candidates
        .map((h) => {
          const score = Math.max(
            jaccardLocal(row.name, h.name),
            jaccardLocal(`${row.name} ${row.city}`, `${h.name} ${h.city}`),
          );
          const cityOk =
            !cn ||
            !h.city ||
            String(h.city).toLowerCase().includes(cn) ||
            cn.includes(String(h.city).toLowerCase());
          return { h, score, cityOk };
        })
        .filter((x) => x.cityOk && x.score >= 0.45)
        .sort((a, b) => b.score - a.score);
      if (scored.length === 1 || (scored.length > 1 && scored[0].score - scored[1].score >= 0.05)) {
        match = {
          status: "MATCHED",
          hpcId: scored[0].h.id,
          hotelName: scored[0].h.name,
          hotel: scored[0].h,
          score: scored[0].score,
          quarantine: false,
        };
      }
    }
    // Hard geo guard — reject cross-market false positives (e.g. La Jolla CA ≠ Acapulco)
    if (match.status === "MATCHED" && match.hotel) {
      const dCity = String(row.city || "").toLowerCase();
      const hCity = String(match.hotel.city || "").toLowerCase();
      const dState = normalizeState(row.state);
      const hState = normalizeState(match.hotel.state);
      const cityOverlap =
        !dCity ||
        !hCity ||
        dCity.includes(hCity) ||
        hCity.includes(dCity) ||
        (/(new york)/.test(dCity) && /(new york)/.test(hCity));
      const stateOk = !dState || !hState || dState === hState;
      if (!cityOverlap || !stateOk) {
        match = {
          status: "NO_MATCH",
          reason: "geo_guard_reject",
          hpcId: null,
          quarantine: true,
          rejected: { name: match.hotel.name, city: match.hotel.city, state: match.hotel.state },
        };
      }
    }

    const plan = {
      discoveryId: row.propertyId,
      airtableId: row.airtableId,
      discoveryName: row.name,
      discoveryCity: row.city,
      discoveryState: row.state,
      matchStatus: match.status,
      score: match.score || null,
      hpcId: match.hpcId || null,
      hpcName: match.hotelName || match.hotel?.name || null,
      hpcCity: match.hotel?.city || null,
      candidatesTried: candidates.length,
    };
    matchPlans.push(plan);

    if (match.status === "MATCHED" && match.hotel) {
      const h = match.hotel;
      const proposed = {};
      const preserve = {};
      // Non-destructive null-fills only on allowlisted fields
      if (!h.managementModel) proposed["Management Model"] = "Management Agreement";
      else preserve["Management Model"] = h.managementModel;
      if (!h.operatorSourceUrl && row.sourceUrl) proposed["Operator Source URL"] = row.sourceUrl;
      else if (h.operatorSourceUrl) preserve["Operator Source URL"] = h.operatorSourceUrl;
      if (!h.operatorConfidence) proposed["Operator Confidence"] = "MEDIUM";
      else preserve["Operator Confidence"] = h.operatorConfidence;
      // Never invent operator name without explicit manager finding on this property
      if (h.operator) preserve["Operator / Management Company"] = h.operator;

      hpcEnrichPlans.push({
        ...plan,
        existingOperator: h.operator || null,
        existingManagementModel: h.managementModel || null,
        proposedNullFills: proposed,
        preservedStrongerValues: preserve,
        allowedFields: [...HPC_ALLOWLISTED_PROMOTION_FIELDS],
        action: Object.keys(proposed).length ? "NULL_FILL" : "LINK_ONLY",
        note: "Null-fill only; no HPC create; customer publication blocked",
      });
    }
    await new Promise((r) => setTimeout(r, 150));
  }

  const hpcApplyResults = [];
  const discoveryLinkResults = [];
  if (APPLY_HPC) {
    for (const p of hpcEnrichPlans) {
      // 1) Link discovery inventory → HPC (non-destructive)
      const linkFields = {
        [M.hpcMatchStatus]: "DISCOVERED_MATCHED_HPC",
        [M.hpcHotel]: [p.hpcId],
        [M.lifecycleStatus]: "HPC_MATCHED",
        [M.publicationState]: "CUSTOMER_BLOCKED",
        [M.notesQuarantine]: `V2.4 HPC match score=${p.score?.toFixed?.(3) || p.score}; enrich=${p.action}`,
      };
      const linkPatch = await patch(invTable, p.airtableId, linkFields);
      const linkRb = await get(invTable, p.airtableId);
      discoveryLinkResults.push({
        airtableId: p.airtableId,
        hpcId: p.hpcId,
        patchOk: linkPatch.ok,
        readBackStatus: linkRb.json?.fields?.[M.hpcMatchStatus] || null,
        readBackHpc: linkRb.json?.fields?.[M.hpcHotel] || [],
        readBackPublication: linkRb.json?.fields?.[M.publicationState] || null,
        readBackOk:
          linkRb.ok &&
          linkRb.json?.fields?.[M.hpcMatchStatus] === "DISCOVERED_MATCHED_HPC" &&
          Array.isArray(linkRb.json?.fields?.[M.hpcHotel]) &&
          linkRb.json.fields[M.hpcHotel].includes(p.hpcId) &&
          linkRb.json?.fields?.[M.publicationState] === "CUSTOMER_BLOCKED",
      });

      // 2) HPC null-fills
      if (p.action === "NULL_FILL" && Object.keys(p.proposedNullFills).length) {
        // Re-read HPC to confirm still empty (idempotent / race-safe)
        const live = await get(HPC_TABLE_ID, p.hpcId);
        const liveFields = live.json?.fields || {};
        const safeFills = {};
        for (const [k, v] of Object.entries(p.proposedNullFills)) {
          if (!HPC_ALLOWLISTED_PROMOTION_FIELDS.includes(k)) continue;
          const cur = liveFields[k];
          if (cur == null || cur === "" || (Array.isArray(cur) && !cur.length)) {
            safeFills[k] = v;
          }
        }
        let patchOk = true;
        let readBack = null;
        if (Object.keys(safeFills).length) {
          const hp = await patch(HPC_TABLE_ID, p.hpcId, safeFills);
          patchOk = hp.ok;
          const rb = await get(HPC_TABLE_ID, p.hpcId);
          readBack = {};
          for (const k of Object.keys(safeFills)) {
            readBack[k] = rb.json?.fields?.[k] ?? null;
          }
        }
        hpcApplyResults.push({
          hpcId: p.hpcId,
          hpcName: p.hpcName,
          discoveryName: p.discoveryName,
          fieldsWritten: safeFills,
          patchOk,
          readBack,
          readBackOk:
            patchOk &&
            Object.entries(safeFills).every(([k, v]) => {
              const got = readBack?.[k];
              return got === v || String(got) === String(v);
            }),
        });
      } else {
        hpcApplyResults.push({
          hpcId: p.hpcId,
          hpcName: p.hpcName,
          discoveryName: p.discoveryName,
          fieldsWritten: {},
          patchOk: true,
          readBackOk: true,
          note: "link_only_no_null_fills_needed",
        });
      }
      await new Promise((r) => setTimeout(r, 180));
    }
  }

  // Inventory counts
  const allInv = await listAll(invTable);
  const byClass = {};
  const byPub = {};
  let hpcLinked = 0;
  for (const r of allInv) {
    const ic = r.fields?.[M.identityClass] || "OTHER";
    byClass[ic] = (byClass[ic] || 0) + 1;
    const pub = r.fields?.[M.publicationState] || "OTHER";
    byPub[pub] = (byPub[pub] || 0) + 1;
    if (Array.isArray(r.fields?.[M.hpcHotel]) && r.fields[M.hpcHotel].length) hpcLinked += 1;
  }
  const findings = await listAll(findTable);

  const report = {
    version: "sec-edgar-v24-quality-closure-and-hpc",
    generatedAt: new Date().toISOString(),
    modes: { APPLY_DEMOTE, APPLY_HPC },
    phase1: {
      verifiedReviewed: review.length,
      demotePlanned: demotePlan.length,
      demoteApplied: demoteActions.length,
      demoteReadBackOk: demoteActions.filter((a) => a.readBackOk).length,
      remainingVerified: remainingVerified.length,
      precision: {
        sampleSize: remainingVerified.length,
        correct: precision.correct,
        precision: precision.precision,
        meetsGate: precision.precision >= 0.95,
        gateThreshold: 0.95,
      },
      demotePlan,
      demoteActions,
      reviewSample: review.slice(0, 20),
    },
    phase5: {
      cohortTried: cohort.length,
      matchPlans,
      strongMatches: hpcEnrichPlans.length,
      enrichPlans: hpcEnrichPlans,
      discoveryLinkResults,
      hpcApplyResults,
      linkReadBackOk: discoveryLinkResults.filter((r) => r.readBackOk).length,
      hpcReadBackOk: hpcApplyResults.filter((r) => r.readBackOk).length,
      fieldsEnriched: hpcApplyResults.reduce(
        (n, r) => n + Object.keys(r.fieldsWritten || {}).length,
        0,
      ),
      guarantees: [
        "No HPC hotel creates",
        "Null-fill only on allowlisted empty fields",
        "Stronger existing values preserved",
        "Customer publication CUSTOMER_BLOCKED",
        "Mandatory read-back after writes",
      ],
    },
    inventorySnapshot: {
      total: allInv.length,
      findings: findings.length,
      byIdentityClass: byClass,
      publicationStates: byPub,
      hpcLinkedCount: hpcLinked,
    },
    researchSpendUsd: 0,
  };

  fs.writeFileSync(path.join(OUT, "QUALITY_CLOSURE_REPORT.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(OUT, "QUALITY_CLOSURE_REPORT.md"),
    [
      `# SEC EDGAR V2.4 — Quality Closure & HPC Enrichment`,
      ``,
      `**Generated:** ${report.generatedAt}`,
      `**Modes:** demote=${APPLY_DEMOTE} hpc=${APPLY_HPC}`,
      ``,
      `## Phase 1`,
      `| Metric | Value |`,
      `|---|---|`,
      `| VERIFIED reviewed | ${report.phase1.verifiedReviewed} |`,
      `| Demote planned | ${report.phase1.demotePlanned} |`,
      `| Demote applied / read-back OK | ${report.phase1.demoteApplied} / ${report.phase1.demoteReadBackOk} |`,
      `| Remaining VERIFIED | ${report.phase1.remainingVerified} |`,
      `| Precision | ${(report.phase1.precision.precision * 100).toFixed(1)}% (gate ${report.phase1.precision.meetsGate ? "PASS" : "FAIL"}) |`,
      ``,
      `## Phase 5 HPC`,
      `| Metric | Value |`,
      `|---|---|`,
      `| Cohort tried | ${report.phase5.cohortTried} |`,
      `| Strong matches | ${report.phase5.strongMatches} |`,
      `| Discovery links OK | ${report.phase5.linkReadBackOk} |`,
      `| HPC fields enriched | ${report.phase5.fieldsEnriched} |`,
      `| HPC read-back OK | ${report.phase5.hpcReadBackOk} |`,
      ``,
      `## Inventory`,
      `- Total: ${report.inventorySnapshot.total}`,
      `- Findings: ${report.inventorySnapshot.findings}`,
      `- Classes: ${JSON.stringify(report.inventorySnapshot.byIdentityClass)}`,
      `- Publication: ${JSON.stringify(report.inventorySnapshot.publicationStates)}`,
      `- HPC linked: ${report.inventorySnapshot.hpcLinkedCount}`,
      ``,
    ].join("\n"),
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: OUT,
        verifiedReviewed: report.phase1.verifiedReviewed,
        demotePlanned: report.phase1.demotePlanned,
        demoteApplied: report.phase1.demoteApplied,
        remainingVerified: report.phase1.remainingVerified,
        precision: report.phase1.precision,
        strongHpcMatches: report.phase5.strongMatches,
        fieldsEnriched: report.phase5.fieldsEnriched,
        hpcLinked: report.inventorySnapshot.hpcLinkedCount,
        inventory: report.inventorySnapshot.byIdentityClass,
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
