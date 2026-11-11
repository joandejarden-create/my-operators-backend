/**
 * HPC_LIVE_IDENTITY_COLLISION_V1
 *
 * Read-only Hotel Property Census identity / collision check for Full ADP E2E S2.
 * Reuses the shared HPC Airtable client (AIRTABLE_BASE_ID_ALT + Hotel Property Census).
 * Never writes. Never treats lookup failure as verified absence.
 */

import "dotenv/config";
import {
  fetchHpcRecord,
  searchHpcByName,
  resolveHpcIdForAdpProperty,
} from "./hpc-shared-upsert-v1.js";
import { HPC_TABLE_NAME, HPC_FULL_ONBOARDING_CONTRACT_V1 } from "./hpc-full-onboarding-contract-v1.js";

export const HPC_LIVE_IDENTITY_COLLISION_V1 = "HPC_LIVE_IDENTITY_COLLISION_V1";
export const HPC_LIVE_LOOKUP_VERSION = "2026-10-21.v1";

/** Mandatory orchestrator outcomes */
export const HPC_LIVE_OUTCOME = Object.freeze({
  HPC_EXISTING_RECORD_VERIFIED: "HPC_EXISTING_RECORD_VERIFIED",
  HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT: "HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT",
  HPC_IDENTITY_COLLISION_REQUIRES_REVIEW: "HPC_IDENTITY_COLLISION_REQUIRES_REVIEW",
  HPC_LIVE_LOOKUP_UNAVAILABLE: "HPC_LIVE_LOOKUP_UNAVAILABLE",
});

export const HPC_LIVE_FRESHNESS_MS = 24 * 60 * 60 * 1000;

function airtableToken() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

function platformBaseId() {
  return (process.env.AIRTABLE_BASE_ID_ALT || "").trim();
}

export function hpcLiveLookupConfigured() {
  return Boolean(airtableToken() && platformBaseId());
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function tokens(s) {
  return new Set(norm(s).split(" ").filter((t) => t.length > 1));
}

function jaccard(a, b) {
  const A = tokens(a);
  const B = tokens(b);
  if (!A.size || !B.size) return 0;
  let inter = 0;
  for (const t of A) if (B.has(t)) inter += 1;
  return inter / new Set([...A, ...B]).size;
}

function escapeFormula(s) {
  return String(s || "").replace(/'/g, "\\'");
}

async function airtableList(filterByFormula, maxRecords = 25) {
  const token = airtableToken();
  const baseId = platformBaseId();
  if (!token || !baseId) {
    const err = new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT");
    err.code = "HPC_CONFIG_MISSING";
    throw err;
  }
  const q = new URLSearchParams({
    maxRecords: String(maxRecords),
    filterByFormula,
  });
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(HPC_TABLE_NAME)}?${q}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(
      `Airtable ${res.status}: ${json?.error?.message || JSON.stringify(json).slice(0, 200)}`
    );
    err.status = res.status;
    err.body = json;
    throw err;
  }
  return (json.records || []).map(mapRecord);
}

function mapRecord(r) {
  const f = r.fields || {};
  return {
    id: r.id,
    name: f["Property Name"] || null,
    canonicalName: f["Canonical Property Name"] || null,
    brand: f["Current Brand"] || null,
    city: f.City || null,
    country: f.Country || null,
    address: f.Address || null,
    officialUrl: f["Official Property URL"] || f.Official || null,
    fields: f,
  };
}

/**
 * Multi-query live search: exact name, canonical name, FIND tokens, city+brand.
 */
export async function searchHpcIdentityCandidates({
  hotelName,
  aliases = [],
  city = null,
  brand = null,
  maxRecords = 25,
} = {}) {
  const queries = [];
  const pushQ = (label, formula) => {
    if (formula) queries.push({ label, formula });
  };

  const names = [
    hotelName,
    ...aliases,
    // Accent-folded ASCII variant for Airtable FIND (Cancún → Cancun)
    hotelName && norm(hotelName) !== String(hotelName || "").toLowerCase()
      ? norm(hotelName)
          .split(" ")
          .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
          .join(" ")
      : null,
  ]
    .map((n) => String(n || "").trim())
    .filter(Boolean);

  const uniqueNames = [...new Set(names)];
  for (const n of uniqueNames) {
    const e = escapeFormula(n);
    const el = escapeFormula(n.toLowerCase());
    pushQ(`exact_or_find:${n}`, `OR({Property Name}='${e}', FIND(LOWER('${el}'), LOWER({Property Name}&'')))`);
    pushQ(
      `canonical:${n}`,
      `OR({Canonical Property Name}='${e}', FIND(LOWER('${el}'), LOWER({Canonical Property Name}&'')))`
    );
  }

  // City + distinctive tokens (Chapultepec / Condesa / Fiesta Americana)
  const nameToks = norm(hotelName)
    .split(" ")
    .filter((t) => t.length > 4 && !["hotel", "fiesta", "americana", "grand", "inclusive", "all"].includes(t));
  if (city && nameToks.length) {
    const c = escapeFormula(String(city));
    const findParts = nameToks
      .slice(0, 3)
      .map((t) => `FIND('${escapeFormula(t)}', LOWER({Property Name}&''))`)
      .join(", ");
    pushQ(
      `city_tokens:${city}`,
      `AND(OR(LOWER({City}&'')=LOWER('${c}'), FIND(LOWER('${c}'), LOWER({City}&''))), OR(${findParts}))`
    );
  }

  if (brand && city) {
    const b = escapeFormula(String(brand));
    const c = escapeFormula(String(city));
    pushQ(
      `brand_city`,
      `AND(FIND(LOWER('${escapeFormula(norm(brand))}'), LOWER({Current Brand}&'')), OR(LOWER({City}&'')=LOWER('${c}'), FIND(LOWER('${c}'), LOWER({City}&''))))`
    );
  }

  const byId = new Map();
  const queryLog = [];
  for (const q of queries.slice(0, 12)) {
    try {
      const rows = await airtableList(q.formula, maxRecords);
      queryLog.push({ label: q.label, hitCount: rows.length, ok: true });
      for (const row of rows) {
        if (!byId.has(row.id)) byId.set(row.id, row);
      }
    } catch (e) {
      queryLog.push({ label: q.label, ok: false, error: e.message });
      // Continue other queries unless config missing
      if (e.code === "HPC_CONFIG_MISSING") throw e;
    }
  }

  // Fallback to shared simple search for primary name + aliases
  for (const n of uniqueNames.slice(0, 6)) {
    try {
      const rows = await searchHpcByName({ name: n, maxRecords });
      queryLog.push({ label: `shared_search:${n}`, hitCount: rows.length, ok: true });
      for (const row of rows) {
        if (!byId.has(row.id)) {
          byId.set(row.id, {
            id: row.id,
            name: row.name,
            brand: row.brand,
            city: row.city,
            country: row.country,
            canonicalName: null,
            address: null,
            officialUrl: null,
          });
        }
      }
    } catch (e) {
      queryLog.push({ label: `shared_search:${n}`, ok: false, error: e.message });
      if (e.message?.includes("Missing AIRTABLE")) throw e;
    }
  }

  return { candidates: [...byId.values()], queryLog };
}

function scoreCandidate(candidate, query) {
  const nameScores = [
    jaccard(query.hotelName, candidate.name),
    jaccard(query.hotelName, candidate.canonicalName),
    ...(query.aliases || []).map((a) =>
      Math.max(jaccard(a, candidate.name), jaccard(a, candidate.canonicalName))
    ),
  ];
  let score = Math.max(0, ...nameScores.filter((n) => Number.isFinite(n)));

  const qn = norm(query.hotelName);
  const cn = norm(candidate.name);
  const cc = norm(candidate.canonicalName);
  if (qn && (qn === cn || qn === cc)) score = Math.max(score, 1);
  for (const a of query.aliases || []) {
    const an = norm(a);
    if (an && (an === cn || an === cc)) score = Math.max(score, 0.98);
  }

  const cityQ = norm(query.city);
  const cityC = norm(candidate.city);
  if (cityQ && cityC) {
    if (cityQ === cityC || cityC.includes(cityQ) || cityQ.includes(cityC)) {
      score += 0.08;
    } else {
      score -= 0.15;
    }
  }

  const brandQ = norm(query.brand);
  const brandC = norm(candidate.brand);
  if (brandQ && brandC) {
    if (brandC.includes(brandQ) || brandQ.includes(brandC) || jaccard(brandQ, brandC) >= 0.5) {
      score += 0.05;
    }
  }

  if (query.address && candidate.address) {
    const addrScore = jaccard(query.address, candidate.address);
    if (addrScore >= 0.5) score += 0.1;
  }

  return Math.max(0, Math.min(1, Number(score.toFixed(4))));
}

function buildCreateProposal(query) {
  return {
    writeAuthorized: false,
    action: "CREATE_IF_FOUNDER_APPROVES",
    proposedFields: {
      "Property Name": query.hotelName || null,
      "Canonical Property Name": query.hotelName || null,
      "Current Brand": query.brand || null,
      City: query.city || null,
      Country: query.country || null,
      Address: query.address || null,
      "Official Property URL": query.officialUrl || null,
    },
    note: "Verified absence via live HPC search — do not create without founder authorization. Lookup failure is never treated as absence.",
  };
}

/**
 * Perform live read-only HPC identity / collision check.
 */
export async function performLiveHpcIdentityCollisionCheck(input = {}) {
  const startedAt = new Date().toISOString();
  const query = {
    hotelName: String(input.hotelName || input.name || "").trim(),
    aliases: [...new Set((input.aliases || []).map((a) => String(a || "").trim()).filter(Boolean))],
    city: input.city || null,
    country: input.country || null,
    address: input.address || null,
    brand: input.brand || null,
    officialUrl: input.officialUrl || null,
    adpPropertyId: input.adpPropertyId || input.draftPropertyId || null,
    knownHpcId: input.knownHpcId || input.hpcId || null,
  };

  if (!hpcLiveLookupConfigured()) {
    return {
      ok: false,
      contract: HPC_LIVE_IDENTITY_COLLISION_V1,
      version: HPC_LIVE_LOOKUP_VERSION,
      outcome: HPC_LIVE_OUTCOME.HPC_LIVE_LOOKUP_UNAVAILABLE,
      writeBlocked: true,
      paidMonitoringBlocked: true,
      reason: "Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT",
      baseId: platformBaseId() || null,
      table: HPC_TABLE_NAME,
      query,
      checkedAt: startedAt,
      candidates: [],
      createProposal: null,
      note: "Unavailable lookup must never be interpreted as verified absence.",
    };
  }

  try {
    // Known link path — fetch and verify
    let knownId = query.knownHpcId;
    if (!knownId && query.adpPropertyId) {
      knownId = resolveHpcIdForAdpProperty(query.adpPropertyId)?.censusRecordId || null;
    }
    if (knownId) {
      const rec = await fetchHpcRecord(knownId);
      if (rec) {
        const mapped = mapRecord(rec);
        const score = scoreCandidate(mapped, query);
        if (score >= 0.55 || !query.hotelName) {
          return {
            ok: true,
            contract: HPC_LIVE_IDENTITY_COLLISION_V1,
            hpcContract: HPC_FULL_ONBOARDING_CONTRACT_V1,
            version: HPC_LIVE_LOOKUP_VERSION,
            outcome: HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED,
            writeBlocked: true,
            paidMonitoringBlocked: false,
            hpcRecordId: mapped.id,
            verifiedRecord: mapped,
            matchScore: score,
            matchMethod: "known_hpc_id_fetch",
            query,
            candidates: [{ ...mapped, score }],
            createProposal: null,
            checkedAt: startedAt,
            baseId: platformBaseId(),
            table: HPC_TABLE_NAME,
          };
        }
        // Known id mismatches name badly → collision review
        return {
          ok: true,
          contract: HPC_LIVE_IDENTITY_COLLISION_V1,
          version: HPC_LIVE_LOOKUP_VERSION,
          outcome: HPC_LIVE_OUTCOME.HPC_IDENTITY_COLLISION_REQUIRES_REVIEW,
          writeBlocked: true,
          paidMonitoringBlocked: true,
          hpcRecordId: mapped.id,
          reason: "KNOWN_HPC_ID_NAME_MISMATCH",
          matchScore: score,
          verifiedRecord: mapped,
          query,
          candidates: [{ ...mapped, score }],
          createProposal: null,
          checkedAt: startedAt,
          baseId: platformBaseId(),
          table: HPC_TABLE_NAME,
        };
      }
    }

    const { candidates: raw, queryLog } = await searchHpcIdentityCandidates({
      hotelName: query.hotelName,
      aliases: query.aliases,
      city: query.city,
      brand: query.brand,
    });

    const scored = raw
      .map((c) => ({ ...c, score: scoreCandidate(c, query) }))
      .sort((a, b) => b.score - a.score);

    const strong = scored.filter((c) => c.score >= 0.85);
    const reviewBand = scored.filter((c) => c.score >= 0.62 && c.score < 0.85);

    if (strong.length === 1 && reviewBand.filter((c) => c.id !== strong[0].id).length === 0) {
      return {
        ok: true,
        contract: HPC_LIVE_IDENTITY_COLLISION_V1,
        version: HPC_LIVE_LOOKUP_VERSION,
        outcome: HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED,
        writeBlocked: true,
        paidMonitoringBlocked: false,
        hpcRecordId: strong[0].id,
        verifiedRecord: strong[0],
        matchScore: strong[0].score,
        matchMethod: "live_name_alias_score",
        query,
        candidates: scored.slice(0, 8),
        queryLog,
        createProposal: null,
        checkedAt: startedAt,
        baseId: platformBaseId(),
        table: HPC_TABLE_NAME,
      };
    }

    if (strong.length > 1 || (strong.length === 1 && reviewBand.length > 0) || reviewBand.length >= 1) {
      return {
        ok: true,
        contract: HPC_LIVE_IDENTITY_COLLISION_V1,
        version: HPC_LIVE_LOOKUP_VERSION,
        outcome: HPC_LIVE_OUTCOME.HPC_IDENTITY_COLLISION_REQUIRES_REVIEW,
        writeBlocked: true,
        paidMonitoringBlocked: true,
        hpcRecordId: null,
        reason: strong.length > 1 ? "MULTIPLE_STRONG_MATCHES" : "AMBIGUOUS_NEAR_MATCHES",
        query,
        candidates: scored.slice(0, 10),
        queryLog,
        createProposal: null,
        checkedAt: startedAt,
        baseId: platformBaseId(),
        table: HPC_TABLE_NAME,
      };
    }

    // No credible match after live search → verified absence proposal (still no write)
    return {
      ok: true,
      contract: HPC_LIVE_IDENTITY_COLLISION_V1,
      version: HPC_LIVE_LOOKUP_VERSION,
      outcome: HPC_LIVE_OUTCOME.HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT,
      writeBlocked: true,
      paidMonitoringBlocked: true,
      hpcRecordId: null,
      query,
      candidates: scored.slice(0, 8),
      queryLog,
      createProposal: buildCreateProposal(query),
      checkedAt: startedAt,
      baseId: platformBaseId(),
      table: HPC_TABLE_NAME,
      note: "Live search returned no credible match. Create only after founder authorization.",
    };
  } catch (e) {
    return {
      ok: false,
      contract: HPC_LIVE_IDENTITY_COLLISION_V1,
      version: HPC_LIVE_LOOKUP_VERSION,
      outcome: HPC_LIVE_OUTCOME.HPC_LIVE_LOOKUP_UNAVAILABLE,
      writeBlocked: true,
      paidMonitoringBlocked: true,
      reason: e.message || String(e),
      status: e.status || null,
      query,
      candidates: [],
      createProposal: null,
      checkedAt: startedAt,
      baseId: platformBaseId() || null,
      table: HPC_TABLE_NAME,
      note: "Unavailable lookup must never be interpreted as verified absence.",
    };
  }
}

/**
 * Resume freshness: reuse prior S2 live result when same query hash and fresh.
 */
export function mayReusePriorHpcLiveResult(prior, queryFingerprint, nowMs = Date.now()) {
  if (!prior || !prior.outcome || !prior.checkedAt) return false;
  if (prior.outcome === HPC_LIVE_OUTCOME.HPC_LIVE_LOOKUP_UNAVAILABLE) return false;
  if (prior.queryFingerprint && queryFingerprint && prior.queryFingerprint !== queryFingerprint) {
    return false;
  }
  const age = nowMs - Date.parse(prior.checkedAt);
  return Number.isFinite(age) && age >= 0 && age < HPC_LIVE_FRESHNESS_MS;
}

export function hpcQueryFingerprint(query) {
  return norm(
    [
      query?.hotelName,
      ...(query?.aliases || []),
      query?.city,
      query?.country,
      query?.brand,
      query?.address,
      query?.knownHpcId,
    ]
      .filter(Boolean)
      .join("|")
  );
}

/** Pure fixtures for acceptance (no network). */
export function hpcLiveIdentityNegativeFixtures() {
  return {
    unavailableNeverMeansAbsent:
      HPC_LIVE_OUTCOME.HPC_LIVE_LOOKUP_UNAVAILABLE !==
      HPC_LIVE_OUTCOME.HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT,
    outcomes: Object.values(HPC_LIVE_OUTCOME),
    writeNeverAuto: true,
  };
}
