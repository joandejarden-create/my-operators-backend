#!/usr/bin/env node
/**
 * Cvent Phase 4 — targeted remediation work queue + verification (safe writes).
 *
 * Default: dry-run (no Airtable writes). Pass --apply to write provenance-only
 * patches for P0 Choice rooms after independent verification.
 *
 * VERIFY BEFORE REPLACE. No bulk deletes / mass overwrites.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PHASE03 = path.join(
  ROOT,
  "reports/data-intelligence/cvent-sourcing-policy-v1/phase-0-3"
);
const OUT = path.join(
  ROOT,
  "reports/data-intelligence/cvent-sourcing-policy-v1/phase-4-remediation"
);

/** Minimal RFC4180-ish CSV parse (quoted fields). */
function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let i = 0;
  let inQuotes = false;
  while (i < text.length) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"' && text[i + 1] === '"') {
        field += '"';
        i += 2;
        continue;
      }
      if (ch === '"') {
        inQuotes = false;
        i += 1;
        continue;
      }
      field += ch;
      i += 1;
      continue;
    }
    if (ch === '"') {
      inQuotes = true;
      i += 1;
      continue;
    }
    if (ch === ",") {
      row.push(field);
      field = "";
      i += 1;
      continue;
    }
    if (ch === "\n" || ch === "\r") {
      if (ch === "\r" && text[i + 1] === "\n") i += 1;
      row.push(field);
      if (row.some((c) => c !== "")) rows.push(row);
      row = [];
      field = "";
      i += 1;
      continue;
    }
    field += ch;
    i += 1;
  }
  if (field.length || row.length) {
    row.push(field);
    if (row.some((c) => c !== "")) rows.push(row);
  }
  if (!rows.length) return [];
  const headers = rows[0];
  return rows.slice(1).map((cells) => {
    const obj = {};
    headers.forEach((h, idx) => {
      obj[h] = cells[idx] ?? "";
    });
    return obj;
  });
}

const POLICY_VERSION = "source-policy-v1";
const APPLY = process.argv.includes("--apply");
const TODAY = new Date().toISOString().slice(0, 10);

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

/**
 * Curated independent evidence (Choice.com bot-blocked; use Tier A press + Tier B/C directories).
 * Cvent remains DISCOVERY_ONLY and never closes verification alone.
 */
const P0_TARGETS = [
  {
    recordId: "recabSgALHHvys0In",
    hotelId: "ind_choice_mx_mx092",
    hotelName: "Comfort Inn & Suites Irapuato",
    field: "Rooms / Keys",
    cventValue: 110,
    cventUrl:
      "https://www.cvent.com/venues/irapuato/hotel/comfort-inn-irapuato/venue-f264d80b-e323-4365-842d-c91a18430d72",
    brandUrl:
      "https://www.choicehotels.com/mexico/irapuato/comfort-inn-hotels/mx092",
    brandCode: "MX092",
    independentEvidence: [
      {
        tier: "A",
        url: "https://www.am.com.mx/guanajuato/2019/07/03/llega-nuevo-hotel-irapuato-485467.html",
        rooms: 110,
        method: "official_choice_mexico_dg_quoted_in_press",
        note: "Periódico AM 2019-07-03: Choice Hotels México DG states 110 habitaciones at Comfort Inn Irapuato opening",
      },
      {
        tier: "C",
        url: "https://www.businesstravelnews.com/Hotels/Irapuato-Mexico/Comfort-Inn-Irapuato-p53832334",
        rooms: 110,
        method: "directory_total_rooms",
        note: "Business Travel News directory: Total Rooms 110; GDS CI MX092",
      },
    ],
  },
  {
    recordId: "rec8OtmFD9eqKORYs",
    hotelId: "ind_choice_mx_mx226",
    hotelName: "Comfort Inn Queretaro Tecnologico",
    field: "Rooms / Keys",
    cventValue: 41,
    cventUrl:
      "https://www.cvent.com/venues/es-ES/queretaro/hotel/comfort-inn-queretaro-tecnologico/venue-cd252652-75b1-454d-9360-bd48fb9000b1",
    brandUrl:
      "https://www.choicehotels.com/mexico/queretaro/comfort-inn-hotels/mx226",
    brandCode: "MX226",
    independentEvidence: [
      {
        tier: "C",
        url: "https://www.travelweekly.com/Hotels/Queretaro-Mexico/Comfort-Inn-Queretaro-Tecnologico-p59333873",
        rooms: 36,
        method: "directory_rooms",
        note: "Travel Weekly directory lists Rooms: 36 (conflicts with Cvent 41)",
      },
      {
        tier: "C",
        url: "https://www.travala.com/hotel/comfort-inn-queretaro-tecnologico-1734040",
        rooms: 36,
        method: "ota_guestroom_count",
        note: "Travala listing: 36 guestrooms (conflicts with Cvent 41)",
      },
    ],
  },
];

function ensureOut() {
  fs.mkdirSync(OUT, { recursive: true });
}

function loadInventory() {
  const csvPath = path.join(PHASE03, "REMEDIATION_INVENTORY.csv");
  const text = fs.readFileSync(csvPath, "utf8");
  return parseCsv(text);
}

function yn(v) {
  return String(v || "").trim().toUpperCase() === "YES";
}

function toWorkRow(r, i) {
  return {
    queueId: `q_${String(i + 1).padStart(3, "0")}`,
    recordId: r.notes?.match(/rec[A-Za-z0-9]{14}/)?.[0] || "",
    hotelId: r.hotel || "",
    hotelName: r.hotel || "",
    system: r.system,
    field: r.field,
    currentValue: "",
    currentSourceSet: r.source_combination,
    sourceRole: r.class === "C" ? "DISCOVERY_ONLY_CANONICAL_EXPOSURE" : "MIXED_OR_HISTORICAL",
    customerVisible: yn(r.customer_visible),
    scoringImpact: yn(r.scoring_impact),
    severity: r.severity,
    verificationStatus: "PENDING",
    inventoryClass: r.class,
    notes: r.notes,
    market: r.market,
  };
}

/** Extract room count hints from Choice / official HTML. */
function extractRoomsFromHtml(html, brandCode) {
  const out = { rooms: null, methods: [], snippets: [] };
  if (!html) return out;
  const patterns = [
    { re: /(\d{1,4})\s*(?:guest\s*)?rooms?\b/gi, label: "rooms_phrase" },
    { re: /"numberOfRooms"\s*:\s*(\d{1,4})/gi, label: "json_ld_numberOfRooms" },
    { re: /"roomCount"\s*:\s*(\d{1,4})/gi, label: "json_roomCount" },
    { re: /totalRooms["']?\s*[:=]\s*["']?(\d{1,4})/gi, label: "totalRooms" },
    {
      re: new RegExp(
        `${brandCode}[^\\d]{0,80}(\\d{1,4})\\s*(?:guest\\s*)?rooms?`,
        "gi"
      ),
      label: "brand_code_nearby",
    },
  ];
  for (const { re, label } of patterns) {
    let m;
    while ((m = re.exec(html)) !== null) {
      const n = Number(m[1]);
      if (n >= 5 && n <= 2000) {
        out.methods.push(label);
        out.snippets.push(m[0].slice(0, 120));
        if (out.rooms == null) out.rooms = n;
        else if (out.rooms !== n && !out.conflicts) {
          out.conflicts = [out.rooms, n];
        }
      }
    }
  }
  return out;
}

async function fetchText(url, { timeoutMs = 20000 } = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: {
        "user-agent":
          "DealalityCventPhase4/1.0 (+https://dealality.com; independent verification)",
        accept: "text/html,application/xhtml+xml,application/json",
      },
      redirect: "follow",
    });
    const text = await res.text();
    return { ok: res.ok, status: res.status, url: res.url, text };
  } catch (err) {
    return { ok: false, status: 0, url, text: "", error: String(err?.message || err) };
  } finally {
    clearTimeout(t);
  }
}

function resolveCensusTarget() {
  const token = resolvePat();
  const base = resolveTargetBase();
  const baseId = base.target_base_id;
  if (!token || !baseId) {
    throw new Error("missing_airtable_pat_or_platform_base");
  }
  assertProductionCensusWriteTarget({
    baseId,
    tableId: CENSUS_TABLE_ID,
  });
  return { token, baseId, tableId: CENSUS_TABLE_ID };
}

async function airtableGetRecord(recordId, identityKey) {
  let target;
  try {
    target = resolveCensusTarget();
  } catch (err) {
    return { ok: false, error: String(err?.message || err) };
  }
  if (recordId) {
    const url = `https://api.airtable.com/v0/${target.baseId}/${target.tableId}/${recordId}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${target.token}` },
    });
    const json = await res.json().catch(() => ({}));
    if (res.ok) return { ok: true, record: json, target };
  }
  if (identityKey) {
    const params = new URLSearchParams();
    params.set("filterByFormula", `{Property Identity Key}='${identityKey}'`);
    params.set("maxRecords", "1");
    const url = `https://api.airtable.com/v0/${target.baseId}/${target.tableId}?${params}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${target.token}` },
    });
    const json = await res.json().catch(() => ({}));
    if (res.ok && json.records?.[0]) {
      return { ok: true, record: json.records[0], target };
    }
    return { ok: false, status: res.status, error: json?.error || "identity_key_not_found" };
  }
  return { ok: false, error: "missing_record_identity" };
}

async function airtablePatchRecord(recordId, fields) {
  let target;
  try {
    target = resolveCensusTarget();
  } catch (err) {
    return { ok: false, error: String(err?.message || err) };
  }
  const url = `https://api.airtable.com/v0/${target.baseId}/${target.tableId}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${target.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    return { ok: false, status: res.status, error: json?.error || json };
  }
  return { ok: true, record: json };
}

function appendStewardNote(existing, addition) {
  const prev = String(existing || "").trim();
  if (prev.includes("cvent_phase4_remediation")) return prev;
  return [prev, addition].filter(Boolean).join("\n").slice(0, 90000);
}

function classifyMixedRow(r) {
  const sys = r.system;
  const src = r.source_combination;
  const cls = r.class;
  // Prior shell aggregates
  if (sys === "HPC_SHELL" && src === "cvent_plus_hbx") {
    return {
      class: "B",
      reason: "Cvent + HBX multi-source shell; provenance normalize only; no research",
      researchNow: false,
    };
  }
  if (sys === "HPC_SHELL" && src === "cvent_only") {
    return {
      class: "D",
      reason: "Discovery shell Not Field Source; historical non-canonical",
      researchNow: false,
    };
  }
  if (sys === "LOCAL_CVENT_CACHE" || cls === "D") {
    return {
      class: "D",
      reason: "Historical / non-customer / cache",
      researchNow: false,
    };
  }
  if (sys === "CENSUS_ROOMS_FILL" && cls === "C") {
    return {
      class: "D_P0",
      reason: "Handled under P0 cvent-only canonical",
      researchNow: false,
    };
  }
  // ADP fixtures with cvent venue URLs in evidence — mixed but not live scoring SoT
  if (sys === "ADP_FIXTURE") {
    if (yn(r.customer_visible) && src.includes("cvent_venue")) {
      return {
        class: "C",
        reason: "Customer-facing evidence artifact may surface Cvent venue URL — P2 surface hide/flag",
        researchNow: true,
      };
    }
    if (src.includes("cvent_mention") && !src.includes("cvent_venue")) {
      return {
        class: "D",
        reason: "Mention-only in fixture notes; not fact SoT",
        researchNow: false,
      };
    }
    return {
      class: "B",
      reason: "Seed/fixture provenance with Cvent URL alongside other sources; normalize display",
      researchNow: false,
    };
  }
  // HI / GDI report artifacts flagged scoring_impact
  if ((sys === "HI_REPORT" || sys === "GDI_REPORT") && yn(r.scoring_impact)) {
    return {
      class: "C",
      reason: "Report artifact marked scoring_impact — verify whether live scoring still consumes Cvent venue facts",
      researchNow: true,
    };
  }
  if (sys === "HI_REPORT" || sys === "GDI_REPORT") {
    return {
      class: "B",
      reason: "Historical research ledger with Cvent venue URL; not active customer SoT",
      researchNow: false,
    };
  }
  return {
    class: "B",
    reason: "Default mixed — provenance review only",
    researchNow: false,
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function writeCsv(file, rows, columns) {
  const lines = [columns.join(",")];
  for (const r of rows) {
    lines.push(columns.map((c) => csvEscape(r[c])).join(","));
  }
  fs.writeFileSync(file, lines.join("\n") + "\n", "utf8");
}

async function verifyP0(target, liveFields) {
  const cost = { fetches: 0, methods: [] };
  const liveRooms = liveFields?.["Rooms / Keys"] ?? null;
  const liveSourceUrl = liveFields?.["Rooms Source URL"] || "";
  const liveConf = liveFields?.["Rooms Confidence"] || "";

  // Attempt Choice brand page (often Akamai-blocked)
  cost.fetches += 1;
  const brand = await fetchText(target.brandUrl, { timeoutMs: 15000 });
  cost.methods.push("brand_html_fetch");
  const extracted = extractRoomsFromHtml(brand.text, target.brandCode);

  // Re-fetch / confirm curated Tier A evidence where URL is press
  const indie = Array.isArray(target.independentEvidence)
    ? target.independentEvidence
    : [];
  const confirmedIndie = [];
  for (const ev of indie.filter((e) => e.tier === "A")) {
    cost.fetches += 1;
    cost.methods.push("tier_a_evidence_fetch");
    const res = await fetchText(ev.url, { timeoutMs: 20000 });
    const ok =
      res.ok &&
      (res.text.includes(String(ev.rooms)) ||
        res.text.includes("110 habitaciones") ||
        res.text.includes(`${ev.rooms} `));
    confirmedIndie.push({ ...ev, fetchOk: res.ok, status: res.status, contentConfirmed: ok });
  }
  for (const ev of indie.filter((e) => e.tier !== "A")) {
    confirmedIndie.push({ ...ev, fetchOk: null, status: null, contentConfirmed: null });
  }

  let verifiedRooms = null;
  let verifiedUrl = null;
  let method = null;
  let classification = "UNVERIFIED_KEEP_FLAGGED";
  let status = "UNVERIFIED_KEEP_FLAGGED";

  if (brand.ok && extracted.rooms != null && !extracted.conflicts) {
    verifiedRooms = extracted.rooms;
    verifiedUrl = brand.url || target.brandUrl;
    method = extracted.methods[0];
  } else {
    const tierA = confirmedIndie.filter(
      (e) => e.tier === "A" && e.rooms != null && e.contentConfirmed !== false
    );
    const agreed = new Set(tierA.map((e) => Number(e.rooms)));
    if (tierA.length && agreed.size === 1) {
      verifiedRooms = [...agreed][0];
      verifiedUrl = tierA[0].url;
      method = tierA[0].method;
    }
  }

  const indieRooms = indie.map((e) => Number(e.rooms)).filter((n) => Number.isFinite(n));
  const indieUnique = [...new Set(indieRooms)];
  const conflictsWithCvent =
    indieUnique.length > 0 && !indieUnique.every((n) => n === Number(target.cventValue));
  const indieInternalConflict = indieUnique.length > 1;

  if (verifiedRooms != null) {
    const liveNum =
      liveRooms === "" || liveRooms == null ? null : Number(liveRooms);
    if (liveNum != null && Number.isFinite(liveNum) && liveNum !== Number(verifiedRooms)) {
      classification = "VERIFIED_WITH_CORRECTION";
    } else {
      classification = "VERIFIED_INDEPENDENTLY";
    }
    status = classification;
  } else if (indieInternalConflict || conflictsWithCvent) {
    // Do not guess among 36 vs 41 — remove from canonical/scoring use; keep historical value
    classification = "REMOVE_FROM_CURRENT_CANONICAL_USE";
    status = "REMOVE_FROM_CURRENT_CANONICAL_USE";
  } else if (/cvent\.com/i.test(String(liveSourceUrl)) || liveSourceUrl === "") {
    classification = "REMOVE_FROM_CURRENT_CANONICAL_USE";
    status = "REMOVE_FROM_CURRENT_CANONICAL_USE";
  }

  return {
    ...target,
    liveRooms,
    liveSourceUrl,
    liveConf,
    brandFetchOk: brand.ok,
    brandStatus: brand.status,
    brandFinalUrl: brand.url,
    brandError: brand.error || null,
    extracted,
    confirmedIndie,
    verifiedRooms,
    verifiedUrl,
    verificationMethod: method,
    classification,
    status,
    cost,
    removeFromCanonicalUse: classification === "REMOVE_FROM_CURRENT_CANONICAL_USE",
    indieInternalConflict,
    conflictsWithCvent,
  };
}

function buildP0Patch(verification, liveFields) {
  const note = [
    `cvent_phase4_remediation ${POLICY_VERSION} ${TODAY}`,
    `class=${verification.classification}`,
    `old_rooms=${verification.liveRooms}`,
    `cvent_claim=${verification.cventValue}`,
    `verified_rooms=${verification.verifiedRooms ?? "n/a"}`,
    `verified_source=${verification.verifiedUrl || "n/a"}`,
    `method=${verification.verificationMethod || "n/a"}`,
    "cvent_role=DISCOVERY_ONLY",
  ].join(" | ");

  if (verification.classification === "VERIFIED_INDEPENDENTLY") {
    return {
      "Rooms / Keys": verification.verifiedRooms,
      "Rooms Confidence": "High",
      "Rooms Source URL": verification.verifiedUrl,
      "Rooms Source Type": "trusted_secondary_source",
      "Last Reviewed Date": TODAY,
      "Notes for Steward": appendStewardNote(liveFields?.["Notes for Steward"], note),
    };
  }
  if (verification.classification === "VERIFIED_WITH_CORRECTION") {
    return {
      "Rooms / Keys": verification.verifiedRooms,
      "Rooms Confidence": "High",
      "Rooms Source URL": verification.verifiedUrl,
      "Rooms Source Type": "trusted_secondary_source",
      "Last Reviewed Date": TODAY,
      "Notes for Steward": appendStewardNote(
        liveFields?.["Notes for Steward"],
        note + " | supersedes_cvent_claim"
      ),
    };
  }
  // REMOVE / UNVERIFIED: do not zero rooms; flag provenance only + steward_review
  const patch = {
    "Rooms Confidence": "Low",
    "Rooms Source Type": "steward_review",
    "Last Reviewed Date": TODAY,
    "Notes for Steward": appendStewardNote(
      liveFields?.["Notes for Steward"],
      note +
        " | needsSourceReview=true | sourceRole=DISCOVERY_ONLY | usedInScoring=false | customerDisplayAllowed=false_until_verified | value_retained_not_deleted"
    ),
  };
  // If Rooms Source URL is still Cvent, keep URL for provenance history but mark via notes
  return patch;
}

function analyzeScoringArtifact(fileRel) {
  const abs = path.join(ROOT, fileRel.replace(/\\/g, "/"));
  if (!fs.existsSync(abs)) {
    return {
      file: fileRel,
      exists: false,
      liveScoringConsumer: false,
      disposition: "NEEDS_MANUAL_REVIEW",
    };
  }
  const text = fs.readFileSync(abs, "utf8");
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* text report */
  }
  const cventVenue =
    (text.match(/cvent\.com\/venues\//gi) || []).length +
    (text.match(/www\.cvent\.com\/venues\//gi) || []).length;
  const cventEvent =
    (text.match(/cvent\.com\/event\//gi) || []).length +
    (text.match(/web\.cvent\.com/gi) || []).length;
  // These are offline report artifacts under reports/ — not live Airtable scoring stores.
  const liveScoringConsumer = false;
  let disposition = "SAFE_CONFIRMED";
  let reason =
    "Historical report artifact only; Phase 0–3 gates block new Cvent venue scoring. Not a live Airtable scoring store.";
  if (cventVenue > 0 && /apply|POSITIVE_CONTROL|HOTEL_MATCHES|GDI_DISCOVERY|AC_RESULT|LEDGER|EVIDENCE_PACK|cvent-probe/i.test(fileRel)) {
    disposition = "BLOCK_FROM_SCORING";
    reason =
      "Contains Cvent venue URLs in research/apply artifacts. Treat as DISCOVERY_ONLY; do not feed verified GDI/ADP scoring. No live score recompute in Phase 4.";
  }
  if (cventEvent > 0 && cventVenue === 0) {
    disposition = "SAFE_CONFIRMED";
    reason = "Cvent event-platform evidence only — allowed under event policy.";
  }
  return {
    file: fileRel,
    exists: true,
    cventVenueUrlHits: cventVenue,
    cventEventUrlHits: cventEvent,
    liveScoringConsumer,
    disposition,
    reason,
    topKeys: json && typeof json === "object" ? Object.keys(json).slice(0, 12) : [],
  };
}

function analyzeCustomerSurface(fileRel) {
  const abs = path.join(ROOT, fileRel.replace(/\\/g, "/"));
  if (!fs.existsSync(abs)) {
    return {
      file: fileRel,
      exists: false,
      disposition: "HIDE_PENDING_VERIFICATION",
    };
  }
  const text = fs.readFileSync(abs, "utf8");
  const hotelVenueHits = (
    text.match(/cvent\.com\/venues\/[^"'\\\s]*\/hotel\//gi) || []
  ).length;
  const venueResultsHits = (
    text.match(/cvent\.com\/venues\/results\//gi) || []
  ).length;
  const venueHits = (text.match(/cvent\.com\/venues\//gi) || []).length;
  const mentionOnly = /cvent/i.test(text) && venueHits === 0;
  let disposition = "DISPLAY_SAFE";
  let reason = "No Cvent venue hotel URL in artifact.";
  let surface = "fixture_or_report";
  if (hotelVenueHits > 0) {
    disposition = "HIDE_PENDING_VERIFICATION";
    reason =
      "Cvent hotel/venue property URL in evidence. Discovery citation only — not authoritative hotel fact. Phase 2 ADP guard blocks Cvent-only attribute activation.";
    surface = fileRel.includes("published")
      ? "ADP_published_evidence"
      : "ADP_seed_or_report";
  } else if (venueResultsHits > 0 || venueHits > 0) {
    disposition = "DISPLAY_SAFE";
    reason =
      "Cvent venues/results or non-hotel venue path — market/event discovery citation only; not a hotel Rooms/Keys or meeting SoT claim. Phase 2 ADP guard still blocks Cvent-only attribute activation.";
    surface = fileRel.includes("published")
      ? "ADP_published_evidence"
      : "ADP_seed_or_report";
  } else if (mentionOnly) {
    disposition = "DISPLAY_SAFE";
    reason = "Cvent mention without venue/hotel fact claim as SoT.";
  }
  return {
    file: fileRel,
    exists: true,
    venueHits,
    disposition,
    reason,
    surface,
  };
}

async function main() {
  ensureOut();
  const inventory = loadInventory();
  const workQueue = inventory.map(toWorkRow);

  fs.writeFileSync(
    path.join(OUT, "WORK_QUEUE.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        policyVersion: POLICY_VERSION,
        apply: APPLY,
        count: workQueue.length,
        rows: workQueue,
      },
      null,
      2
    ),
    "utf8"
  );

  // --- P0 ---
  const p0Rows = [];
  const evidence = { p0: [], p1: [], p2: [], cost: { fetches: 0, airtableReads: 0, airtableWrites: 0 } };
  for (const t of P0_TARGETS) {
    const live = await airtableGetRecord(t.recordId, t.hotelId);
    evidence.cost.airtableReads += 1;
    const fields = live.ok ? live.record.fields || {} : {};
    const verification = await verifyP0(t, fields);
    evidence.cost.fetches += verification.cost.fetches;

    const patch = buildP0Patch(verification, fields);
    let writeResult = { ok: false, skipped: true, reason: "dry_run" };
    if (APPLY) {
      writeResult = await airtablePatchRecord(t.recordId, patch);
      if (writeResult.ok) evidence.cost.airtableWrites += 1;
    }

    const row = {
      recordId: t.recordId,
      hotelId: t.hotelId,
      hotelName: t.hotelName,
      system: "HPC",
      field: "Rooms / Keys",
      currentValue: verification.liveRooms,
      currentSourceUrl: verification.liveSourceUrl,
      cventClaim: t.cventValue,
      verifiedValue: verification.verifiedRooms ?? "",
      verifiedSource: verification.verifiedUrl || "",
      classification: verification.classification,
      verificationStatus: verification.status,
      customerVisible: "YES",
      scoringImpact: "YES",
      airtableReadOk: live.ok,
      writeApplied: Boolean(writeResult.ok),
      writeMode: APPLY ? "apply" : "dry_run",
      writeError: writeResult.error ? JSON.stringify(writeResult.error) : "",
      changeReason: "cvent_phase4_independent_verification",
      policyVersion: POLICY_VERSION,
      verificationDate: TODAY,
      verificationMethod: verification.verificationMethod || "",
      brandFetchStatus: verification.brandStatus,
      extractMethods: (verification.extracted.methods || []).join("|"),
      extractSnippets: (verification.extracted.snippets || []).slice(0, 3).join(" || "),
    };
    p0Rows.push(row);
    evidence.p0.push({
      target: t,
      live: live.ok
        ? {
            rooms: fields["Rooms / Keys"],
            roomsSourceUrl: fields["Rooms Source URL"],
            roomsConfidence: fields["Rooms Confidence"],
            roomsSourceType: fields["Rooms Source Type"],
            productionUseStatus: fields["Production Use Status"],
          }
        : { error: live.error || live },
      verification,
      proposedPatch: patch,
      writeResult,
    });
  }

  writeCsv(
    path.join(OUT, "P0_CVENT_ONLY_CANONICAL.csv"),
    p0Rows,
    [
      "recordId",
      "hotelId",
      "hotelName",
      "system",
      "field",
      "currentValue",
      "currentSourceUrl",
      "cventClaim",
      "verifiedValue",
      "verifiedSource",
      "classification",
      "verificationStatus",
      "customerVisible",
      "scoringImpact",
      "writeApplied",
      "writeMode",
      "policyVersion",
      "verificationDate",
      "verificationMethod",
      "extractMethods",
      "extractSnippets",
      "writeError",
    ]
  );

  // --- P1 scoring-impact ---
  const scoringRows = inventory.filter((r) => yn(r.scoring_impact));
  const p1Rows = [];
  for (const r of scoringRows) {
    const fileRel = String(r.notes || "").includes("reports\\") || String(r.notes || "").includes("reports/")
      ? String(r.notes).trim()
      : "";
    let analysis;
    if (r.system === "CENSUS_ROOMS_FILL") {
      const p0 = p0Rows.find((x) => x.hotelId === r.hotel);
      analysis = {
        file: r.hotel,
        disposition:
          p0?.classification === "VERIFIED_INDEPENDENTLY" ||
          p0?.classification === "VERIFIED_WITH_CORRECTION"
            ? p0.classification === "VERIFIED_WITH_CORRECTION"
              ? "SAFE_CORRECTED"
              : "SAFE_CONFIRMED"
            : "BLOCK_FROM_SCORING",
        reason: p0
          ? `Linked P0 ${p0.classification}; rooms scoring must use verified brand provenance only`
          : "P0 missing",
        liveScoringConsumer: true,
        influences: "Census Rooms/Keys → ADP room capacity / GDI room-capacity fit if consumed",
      };
    } else if (fileRel) {
      analysis = analyzeScoringArtifact(fileRel);
      analysis.influences =
        r.system === "GDI_REPORT"
          ? "Historical GDI discovery/match ledger (not live opportunity score store)"
          : "Historical HI evidence-depth research artifact";
    } else {
      analysis = {
        disposition: "NEEDS_MANUAL_REVIEW",
        reason: "No file path",
        influences: "unknown",
      };
    }
    evidence.p1.push(analysis);
    p1Rows.push({
      hotelId: r.hotel,
      system: r.system,
      field: r.field,
      source_combination: r.source_combination,
      inventoryClass: r.class,
      disposition: analysis.disposition,
      influences: analysis.influences || "",
      reason: analysis.reason || "",
      cventVenueUrlHits: analysis.cventVenueUrlHits ?? "",
      liveScoringConsumer: analysis.liveScoringConsumer ? "YES" : "NO",
      notes: r.notes,
    });
  }
  writeCsv(
    path.join(OUT, "P1_SCORING_IMPACT.csv"),
    p1Rows,
    [
      "hotelId",
      "system",
      "field",
      "source_combination",
      "inventoryClass",
      "disposition",
      "influences",
      "reason",
      "cventVenueUrlHits",
      "liveScoringConsumer",
      "notes",
    ]
  );

  // --- P2 customer-visible ---
  const custRows = inventory.filter((r) => yn(r.customer_visible));
  const p2Rows = [];
  for (const r of custRows) {
    let analysis;
    if (r.system === "CENSUS_ROOMS_FILL") {
      const p0 = p0Rows.find((x) => x.hotelId === r.hotel);
      const ok =
        p0?.classification === "VERIFIED_INDEPENDENTLY" ||
        p0?.classification === "VERIFIED_WITH_CORRECTION";
      analysis = {
        surface: "Hotel Property Census / Rooms Keys (may feed client profiles)",
        disposition: ok
          ? p0.classification === "VERIFIED_WITH_CORRECTION"
            ? "DISPLAY_CORRECTED"
            : "DISPLAY_SAFE"
          : "HIDE_PENDING_VERIFICATION",
        reason: ok
          ? `Independently verified (${p0.classification}); live Production Use Status is Census Only / Not Owner-Facing; provenance upgraded off Cvent`
          : "Still Cvent-only/unverified — suppress as verified customer truth; value retained; Production Use Status already Census Only / Not Owner-Facing",
      };
    } else {
      const fileRel = String(r.notes || "").trim();
      analysis = analyzeCustomerSurface(fileRel);
    }
    evidence.p2.push({ hotel: r.hotel, ...analysis });
    p2Rows.push({
      hotelId: r.hotel,
      system: r.system,
      field: r.field,
      surface: analysis.surface || "",
      disposition: analysis.disposition,
      reason: analysis.reason || "",
      venueHits: analysis.venueHits ?? "",
      notes: r.notes,
    });
  }
  writeCsv(
    path.join(OUT, "P2_CUSTOMER_VISIBLE.csv"),
    p2Rows,
    [
      "hotelId",
      "system",
      "field",
      "surface",
      "disposition",
      "reason",
      "venueHits",
      "notes",
    ]
  );

  // --- Mixed provenance classification for the 179 mixed set (144 shells + 35 inventory B) ---
  // Do NOT fold P0 Class C, cache, or cvent-only shells into the 179 mixed tallies.
  const mixedRows = [];
  let classA = 0;
  let classB = 0;
  let classC = 0;
  let classD = 0;
  // 144 prior Cvent+HBX shells → B (normalize only; no blanket research)
  classB += 144;
  mixedRows.push({
    hotelId: "(aggregate n=144)",
    system: "HPC_SHELL",
    field: "Discovery Source / Shell Identity",
    mixedClass: "B",
    researchNow: "NO",
    reason: "Cvent + HBX multi-source shell; provenance normalize only",
    notes: "Prior DR/CR/PA shell audit",
  });
  for (const r of inventory) {
    if (r.system === "HPC_SHELL") continue;
    if (r.system === "LOCAL_CVENT_CACHE") continue;
    if (r.system === "CENSUS_ROOMS_FILL") continue; // P0, not mixed-179
    if (r.class !== "B" && !(r.system === "HI_REPORT" || r.system === "GDI_REPORT" || r.system === "ADP_FIXTURE")) {
      continue;
    }
    // Only the Phase-0-3 inventory rows that contributed to mixed=179 (class B rows)
    if (r.class !== "B") continue;
    const c = classifyMixedRow(r);
    // Re-bucket B inventory into A/B/C/D for Phase 5 research gating
    let mixedClass = c.class;
    if (mixedClass === "D_P0") continue;
    // Scoring-impact report artifacts → C (research whether live scoring consumes)
    if (yn(r.scoring_impact)) mixedClass = "C";
    // Customer-visible ADP venue hotel pages → C; venues/results stay B
    if (yn(r.customer_visible) && String(r.notes || "").includes("cvent_venue")) {
      const notesPath = String(r.notes || "");
      let hotelVenue = false;
      try {
        const abs = path.join(ROOT, notesPath.replace(/\\/g, "/"));
        if (fs.existsSync(abs)) {
          hotelVenue = /cvent\.com\/venues\/[^"'\\\s]*\/hotel\//i.test(
            fs.readFileSync(abs, "utf8")
          );
        }
      } catch {
        /* ignore */
      }
      mixedClass = hotelVenue ? "C" : "B";
    }
    mixedRows.push({
      hotelId: r.hotel,
      system: r.system,
      field: r.field,
      mixedClass,
      researchNow: mixedClass === "C" || mixedClass === "D" ? "YES" : "NO",
      reason: c.reason,
      notes: r.notes,
    });
    if (mixedClass === "A") classA += 1;
    else if (mixedClass === "B") classB += 1;
    else if (mixedClass === "C") classC += 1;
    else if (mixedClass === "D") classD += 1;
  }
  writeCsv(
    path.join(OUT, "MIXED_PROVENANCE_CLASSIFICATION.csv"),
    mixedRows,
    ["hotelId", "system", "field", "mixedClass", "researchNow", "reason", "notes"]
  );

  fs.writeFileSync(
    path.join(OUT, "VERIFICATION_EVIDENCE.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        policyVersion: POLICY_VERSION,
        apply: APPLY,
        evidence,
        mixedClassCounts: { A: classA, B: classB, C: classC, D: classD },
      },
      null,
      2
    ),
    "utf8"
  );

  // Summary counts for founder return
  const summary = {
    p0Processed: p0Rows.length,
    p0VerifiedIndependently: p0Rows.filter((r) => r.classification === "VERIFIED_INDEPENDENTLY").length,
    p0Corrected: p0Rows.filter((r) => r.classification === "VERIFIED_WITH_CORRECTION").length,
    p0StillUnverified: p0Rows.filter((r) =>
      ["UNVERIFIED_KEEP_FLAGGED", "REMOVE_FROM_CURRENT_CANONICAL_USE"].includes(r.classification)
    ).length,
    p1Processed: p1Rows.length,
    p1SafeConfirmed: p1Rows.filter((r) => r.disposition === "SAFE_CONFIRMED").length,
    p1Corrected: p1Rows.filter((r) => r.disposition === "SAFE_CORRECTED").length,
    p1Blocked: p1Rows.filter((r) => r.disposition === "BLOCK_FROM_SCORING").length,
    p1Manual: p1Rows.filter((r) => r.disposition === "NEEDS_MANUAL_REVIEW").length,
    p2Processed: p2Rows.length,
    p2DisplaySafe: p2Rows.filter((r) => r.disposition === "DISPLAY_SAFE").length,
    p2DisplayCorrected: p2Rows.filter((r) => r.disposition === "DISPLAY_CORRECTED").length,
    p2Hidden: p2Rows.filter((r) => r.disposition === "HIDE_PENDING_VERIFICATION").length,
    mixedA: classA,
    mixedB: classB,
    mixedC: classC,
    mixedD: classD,
    newExternalFetches: evidence.cost.fetches,
    airtableReads: evidence.cost.airtableReads,
    airtableWrites: evidence.cost.airtableWrites,
    apply: APPLY,
  };

  fs.writeFileSync(
    path.join(OUT, "SUMMARY.json"),
    JSON.stringify(summary, null, 2),
    "utf8"
  );

  console.log(JSON.stringify({ ok: true, out: OUT, summary }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
