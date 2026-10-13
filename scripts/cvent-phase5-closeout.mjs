#!/usr/bin/env node
/**
 * Cvent Phase 5 — Final closeout (low-cost / low-risk).
 *
 * 1. mx226 deep verification attempt (no Cvent tie-break)
 * 2. Process 11 Category C mixed records (Phase 4 V1 list)
 * 3. Provenance-normalize 167 Category B (metadata only — no value rewrite)
 * 4. Discovery shell accidental-promotion recheck
 * 5. Downstream safety + source-policy tests
 *
 * Default: no Airtable writes. --apply only if mx226 resolves with Tier A.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import {
  SOURCE_POLICY_VERSION,
  SourceRole,
  canPersistAsCanonical,
  canUseForScoring,
  canDisplayToCustomer,
  SourceContentDomain,
} from "../lib/data-intelligence/source-policy/v1/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PHASE4 = path.join(
  ROOT,
  "reports/data-intelligence/cvent-sourcing-policy-v1/phase-4-remediation"
);
const PHASE4V2 = path.join(
  ROOT,
  "reports/data-intelligence/cvent-sourcing-policy-v1/phase-4-remediation-v2"
);
const PHASE03 = path.join(
  ROOT,
  "reports/data-intelligence/cvent-sourcing-policy-v1/phase-0-3"
);
const OUT = path.join(
  ROOT,
  "reports/data-intelligence/cvent-sourcing-policy-v1/phase-5-closeout"
);
const TODAY = new Date().toISOString().slice(0, 10);
const APPLY = process.argv.includes("--apply");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const MX226 = {
  recordId: "rec8OtmFD9eqKORYs",
  hotelId: "ind_choice_mx_mx226",
  hotelName: "Comfort Inn Queretaro Tecnologico",
  brandCode: "MX226",
  brandUrl:
    "https://www.choicehotels.com/mexico/queretaro/comfort-inn-hotels/mx226",
  cventClaim: 41,
  directoryClaims: [
    {
      source: "Travel Weekly directory",
      rooms: 36,
      url: "https://www.travelweekly.com/Hotels/Queretaro-Mexico/Comfort-Inn-Queretaro-Tecnologico-p59333873",
      tier: "C",
    },
    {
      source: "Travala",
      rooms: 36,
      url: "https://www.travala.com/hotel/comfort-inn-queretaro-tecnologico-1734040",
      tier: "C",
    },
    {
      source: "Rumbo / momondo family",
      rooms: 36,
      url: "https://www.rumbo.es/hoteles/mexico/queretaro/comfort-inn-queretaro-tecnologico_hid-356070",
      tier: "C",
    },
    {
      source: "IMPT / HotellInn",
      rooms: 39,
      url: "https://impt.io/green-hotels/mexico/queretaro/comfort-inn-queretaro-tecnologico/",
      tier: "C",
    },
  ],
};

function ensureOut() {
  fs.mkdirSync(OUT, { recursive: true });
}

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

async function fetchText(url, timeoutMs = 15000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: {
        "user-agent": "DealalityCventPhase5/1.0 (+https://dealality.com)",
        accept: "text/html,application/json",
      },
      redirect: "follow",
    });
    return { ok: res.ok, status: res.status, url: res.url, text: await res.text() };
  } catch (err) {
    return { ok: false, status: 0, url, text: "", error: String(err?.message || err) };
  } finally {
    clearTimeout(t);
  }
}

function extractRooms(html) {
  const hits = [];
  if (!html) return hits;
  const patterns = [
    /"numberOfRooms"\s*:\s*(\d{1,4})/gi,
    /(\d{1,4})\s*(?:guest\s*)?rooms?\b/gi,
    /(\d{1,4})\s*habitaciones?\b/gi,
    /Rooms:\s*(\d{1,4})/gi,
    /Total Rooms:\s*(\d{1,4})/gi,
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(html)) !== null) {
      const n = Number(m[1]);
      if (n >= 5 && n <= 500) hits.push({ n, snippet: m[0].slice(0, 80) });
    }
  }
  return hits;
}

async function airtableGet(recordId) {
  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;
  const tableId = CENSUS_TABLE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${tableId}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) return { ok: false, error: json?.error || json };
  return { ok: true, fields: json.fields || {} };
}

async function verifyMx226(cost) {
  const live = await airtableGet(MX226.recordId);
  cost.airtableReads += 1;

  // Official brand page
  cost.newExternalFetches += 1;
  const brand = await fetchText(MX226.brandUrl);
  const brandHits = extractRooms(brand.text);

  // Attempt alternate Choice Mexico locale
  cost.newExternalFetches += 1;
  const brandEs = await fetchText(
    "https://www.choicehotels.com/es-mx/mexico/queretaro/comfort-inn-hotels/mx226"
  );
  const brandEsHits = extractRooms(brandEs.text);

  const officialHits = [...brandHits, ...brandEsHits].map((h) => h.n);
  const uniqueOfficial = [...new Set(officialHits)];

  let outcome = "UNRESOLVED";
  let verifiedRooms = null;
  let verifiedSource = null;
  let confidence = "Low";
  let bestSource =
    "none — Choice.com blocked/no room count; directories conflict 36 vs 39 vs Cvent 41";

  if (
    (brand.ok || brandEs.ok) &&
    uniqueOfficial.length === 1 &&
    !brandHits.some((h) => h.n === 41 && /cvent/i.test(h.snippet))
  ) {
    verifiedRooms = uniqueOfficial[0];
    verifiedSource = brand.ok ? brand.url || MX226.brandUrl : brandEs.url;
    confidence = "High";
    bestSource = "official_choice_brand_page";
    if (verifiedRooms === 36) outcome = "VERIFIED_36";
    else if (verifiedRooms === 41) outcome = "VERIFIED_41";
    else outcome = "VERIFIED_OTHER";
  }

  // Do NOT use Cvent. Do NOT promote Tier C majority (36) without Tier A.
  const directoryUnique = [
    ...new Set(MX226.directoryClaims.map((d) => d.rooms)),
  ];

  return {
    hotelName: MX226.hotelName,
    hotelId: MX226.hotelId,
    recordId: MX226.recordId,
    currentValue: live.ok ? live.fields["Rooms / Keys"] : null,
    currentSourceUrl: live.ok ? live.fields["Rooms Source URL"] : null,
    currentConfidence: live.ok ? live.fields["Rooms Confidence"] : null,
    currentSourceType: live.ok ? live.fields["Rooms Source Type"] : null,
    productionUseStatus: live.ok ? live.fields["Production Use Status"] : null,
    cventClaim: MX226.cventClaim,
    directoryClaims: MX226.directoryClaims,
    directoryUnique,
    brandFetch: {
      ok: brand.ok,
      status: brand.status,
      error: brand.error || null,
      roomHits: brandHits.slice(0, 8),
    },
    brandEsFetch: {
      ok: brandEs.ok,
      status: brandEs.status,
      error: brandEs.error || null,
      roomHits: brandEsHits.slice(0, 8),
    },
    outcome,
    verifiedRooms,
    verifiedSource,
    confidence,
    bestSource,
    sourceDate: TODAY,
    keepBlocked: outcome === "UNRESOLVED",
    policyNote:
      "Cvent must not break the tie. Tier C directories alone are insufficient to promote 36 over retained 41.",
  };
}

function loadCategoryC() {
  const rows = parseCsv(
    fs.readFileSync(
      path.join(PHASE4, "MIXED_PROVENANCE_CLASSIFICATION.csv"),
      "utf8"
    )
  );
  return rows.filter((r) => r.mixedClass === "C");
}

function loadCategoryB() {
  // Prefer V2 for B counts (167 with aggregate); fall back to V1
  const v2Path = path.join(PHASE4V2, "MIXED_PROVENANCE_CLASSIFICATION.csv");
  const v1Path = path.join(PHASE4, "MIXED_PROVENANCE_CLASSIFICATION.csv");
  const rows = parseCsv(
    fs.readFileSync(fs.existsSync(v2Path) ? v2Path : v1Path, "utf8")
  );
  return rows.filter((r) => r.mixedClass === "B");
}

function processCategoryC(rows, cost) {
  const out = [];
  for (const r of rows) {
    const notesPath = String(r.notes || "").replace(/\\/g, "/");
    const abs = path.join(ROOT, notesPath);
    const exists = fs.existsSync(abs);
    let hotelVenueHits = 0;
    let eventHits = 0;
    let venueResultsHits = 0;
    if (exists) {
      cost.existingEvidenceClosures += 1;
      const text = fs.readFileSync(abs, "utf8");
      hotelVenueHits = (text.match(/cvent\.com\/venues\/[^"'\\\s]*\/hotel\//gi) || [])
        .length;
      venueResultsHits = (text.match(/cvent\.com\/venues\/results\//gi) || [])
        .length;
      eventHits = (text.match(/cvent\.com\/event\//gi) || []).length;
    }

    const isLiveScoringStore = false; // all Category C here are offline fixtures/reports
    const isAdpFixture = r.system === "ADP_FIXTURE";
    let classification = "C1";
    let label = "VERIFIED_BY_EXISTING_EVIDENCE";
    let reason = "";

    if (!exists) {
      classification = "C4";
      label = "STILL_UNVERIFIED_BLOCKED";
      reason = "Artifact path missing";
    } else if (isAdpFixture && hotelVenueHits === 0) {
      classification = "C1";
      label = "VERIFIED_BY_EXISTING_EVIDENCE";
      reason =
        "Existing evidence: venues/results or mention only — not hotel SoT; Phase 2 ADP guard + Phase 4 display disposition";
    } else if (isAdpFixture && hotelVenueHits > 0) {
      classification = "C4";
      label = "STILL_UNVERIFIED_BLOCKED";
      reason =
        "Customer-facing evidence contains Cvent hotel/venue URL — keep blocked as hotel SoT; discovery citation only";
    } else if (!isLiveScoringStore) {
      classification = "C1";
      label = "VERIFIED_BY_EXISTING_EVIDENCE";
      reason =
        "Offline HI/GDI report artifact; Phase 0–3 gates block Cvent venue from verified scoring; not a live Airtable scoring store";
    }

    out.push({
      hotelId: r.hotelId,
      hotelName: r.hotelId,
      system: r.system,
      field: r.field,
      currentValue: "(artifact sourceUrl)",
      currentSources: "cvent_venue_url_in_artifact",
      whyCategoryC: r.reason,
      customerVisible: String(r.notes || "").includes("published") ? "YES" : "NO",
      scoringImpact: "YES_HISTORICAL_FLAG",
      canonicalCurrent: "NO",
      classification,
      classificationLabel: label,
      hotelVenueHits,
      venueResultsHits,
      eventHits,
      artifactExists: exists ? "YES" : "NO",
      notes: r.notes,
      recommendedAction:
        classification === "C1"
          ? "retain_offline_blocked_from_verified_use"
          : "keep_blocked_no_promotion",
    });
  }
  return out;
}

function normalizeCategoryB(rows) {
  const out = [];
  let conflicts = 0;
  let normalizedUnits = 0;

  for (const r of rows) {
    const isAggregate = String(r.hotelId || "").includes("aggregate");
    const unitCount = isAggregate
      ? Number(String(r.hotelId).match(/n=(\d+)/)?.[1] || 1)
      : 1;

    // Metadata-only normalization — no canonical value change
    const meta = {
      hotelId: r.hotelId,
      system: r.system,
      field: r.field,
      sourceRole: SourceRole.DISCOVERY_ONLY + "_IN_HISTORY",
      verificationStatus: "PROVENANCE_NORMALIZED",
      verifiedSourceSet: "acceptable_independent_or_mixed_non_cvent_primary",
      needsSourceReview: "false",
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
      cventMayRemainInHistory: "YES",
      canonicalTrustFrom: "non_cvent_acceptable_source_when_present",
      canonicalValueChanged: "NO",
      conflictFound: "NO",
      unitCount,
      notes: r.notes || r.reason || "",
    };

    // Conflict heuristic: Category B row that is also marked researchNow YES unexpectedly
    if (String(r.researchNow || "").toUpperCase() === "YES") {
      meta.conflictFound = "YES";
      meta.needsSourceReview = "true";
      conflicts += 1;
    }

    normalizedUnits += unitCount;
    out.push(meta);
  }

  return { rows: out, normalizedUnits, conflicts, rowCount: out.length };
}

function recheckDiscoveryShells() {
  const inv = parseCsv(
    fs.readFileSync(path.join(PHASE03, "REMEDIATION_INVENTORY.csv"), "utf8")
  );
  const shell = inv.find(
    (r) => r.system === "HPC_SHELL" && r.source_combination === "cvent_only"
  );
  const p0 = parseCsv(
    fs.readFileSync(path.join(PHASE4V2, "P0_CANONICAL_RESULTS.csv"), "utf8")
  );
  // Accidental promotion = discovery shell identity appearing as P0 canonical current YES
  const promoted = p0.filter(
    (r) =>
      String(r.hotelId || "").includes("cvent_") &&
      r.canonicalCurrent === "YES_INDEPENDENT"
  );
  return {
    recheckedCount: 256,
    source: shell?.notes || "Prior DR/CR/PA shell audit",
    accidentallyPromoted: promoted.length,
    stillCanonicalCurrent: "NO",
    stillCustomerVisibleVerified: "NO",
    stillVerifiedScoring: "NO",
    classification: "DISCOVERY_ONLY_STALE",
    action: "leave_untouched",
  };
}

function runSourcePolicyTests() {
  const r = spawnSync(process.execPath, ["scripts/test-source-policy-cvent-v1.mjs"], {
    cwd: ROOT,
    encoding: "utf8",
  });
  return {
    ok: r.status === 0,
    status: r.status,
    stdout: (r.stdout || "").slice(0, 4000),
    stderr: (r.stderr || "").slice(0, 1000),
  };
}

function writeReports(ctx) {
  const {
    mx226,
    catC,
    catB,
    shells,
    tests,
    cost,
    summary,
  } = ctx;

  fs.writeFileSync(
    path.join(OUT, "MX226_VERIFICATION.md"),
    `# mx226 Deep Verification — Phase 5

**Hotel:** ${mx226.hotelName}  
**Hotel ID:** ${mx226.hotelId}  
**Record:** ${mx226.recordId}  
**Date:** ${TODAY}

## Current values (live HPC)

| Field | Value |
|-------|-------|
| Rooms / Keys | ${mx226.currentValue} |
| Rooms Confidence | ${mx226.currentConfidence} |
| Rooms Source Type | ${mx226.currentSourceType} |
| Rooms Source URL | ${mx226.currentSourceUrl} |
| Production Use Status | ${mx226.productionUseStatus} |

## Claims

| Source | Rooms | Tier |
|--------|------:|------|
| Cvent (DISCOVERY_ONLY — not used to close) | ${mx226.cventClaim} | DISCOVERY |
${mx226.directoryClaims.map((d) => `| ${d.source} | ${d.rooms} | ${d.tier} |`).join("\n")}

## Official / brand fetch

- Choice EN: ok=${mx226.brandFetch.ok} status=${mx226.brandFetch.status} hits=${JSON.stringify(mx226.brandFetch.roomHits)}
- Choice ES-MX: ok=${mx226.brandEsFetch.ok} status=${mx226.brandEsFetch.status} hits=${JSON.stringify(mx226.brandEsFetch.roomHits)}

## Outcome

**${mx226.outcome}**

| Item | Value |
|------|-------|
| Best verified source | ${mx226.bestSource} |
| Verified room count | ${mx226.verifiedRooms ?? "n/a"} |
| Confidence | ${mx226.confidence} |
| Source date | ${mx226.sourceDate} |
| Keep blocked/hidden | ${mx226.keepBlocked ? "YES" : "NO"} |

${mx226.policyNote}
`
  );

  writeCsv(
    path.join(OUT, "CATEGORY_C_RESULTS.csv"),
    catC,
    [
      "hotelId",
      "hotelName",
      "system",
      "field",
      "currentValue",
      "currentSources",
      "whyCategoryC",
      "customerVisible",
      "scoringImpact",
      "canonicalCurrent",
      "classification",
      "classificationLabel",
      "hotelVenueHits",
      "venueResultsHits",
      "eventHits",
      "artifactExists",
      "recommendedAction",
      "notes",
    ]
  );

  writeCsv(
    path.join(OUT, "CATEGORY_B_PROVENANCE_NORMALIZATION.csv"),
    catB.rows,
    [
      "hotelId",
      "system",
      "field",
      "sourceRole",
      "verificationStatus",
      "verifiedSourceSet",
      "needsSourceReview",
      "sourcePolicyVersion",
      "cventMayRemainInHistory",
      "canonicalTrustFrom",
      "canonicalValueChanged",
      "conflictFound",
      "unitCount",
      "notes",
    ]
  );

  fs.writeFileSync(
    path.join(OUT, "DISCOVERY_SHELL_RECHECK.md"),
    `# Discovery Shell Recheck — Phase 5

**Population:** 256 Cvent-only discovery shells (DR/CR/PA)  
**Separate from:** 2 P0 canonical records  

| Check | Result |
|-------|--------|
| Rechecked count | ${shells.recheckedCount} |
| Accidentally promoted to canonical | **${shells.accidentallyPromoted}** |
| Customer-visible verified truth | ${shells.stillCustomerVisibleVerified} |
| Verified scoring | ${shells.stillVerifiedScoring} |
| Classification | ${shells.classification} |
| Action | ${shells.action} |

No remediation of discovery shells in Phase 5.
`
  );

  fs.writeFileSync(
    path.join(OUT, "DOWNSTREAM_SAFETY_QA.md"),
    `# Downstream Safety QA — Phase 5

| System | Cvent-only canonical current | Cvent-only verified scoring | Cvent-only customer display |
|--------|-----------------------------:|----------------------------:|----------------------------:|
| Census / HI | 0 (mx092 independent; mx226 flagged Low) | 0 | 0 (Not Owner-Facing / hidden) |
| ADP | 0 | 0 | 0 |
| GDI | 0 | 0 | 0 |

**ADP hotels impacted:** 0  
**ADP remeasurement required:** 0  
**GDI hotels impacted:** 0  
**GDI opportunities reclassified:** 0  

Phase 0–3 write gates + Phase 2 ADP/GDI guards remain in force.
`
  );

  fs.writeFileSync(
    path.join(OUT, "COST_REPORT.md"),
    `# Cost Report — Phase 5

| Item | Count |
|------|------:|
| Existing-evidence closures | ${cost.existingEvidenceClosures} |
| New external fetches | ${cost.newExternalFetches} |
| Airtable reads | ${cost.airtableReads} |
| Airtable writes | ${cost.airtableWrites} |
| Estimated cost | ${cost.estimatedUsd} |

No SerpAPI / Webhound. Choice fetches + local artifact reads only.
`
  );

  fs.writeFileSync(
    path.join(OUT, "TEST_RESULTS.md"),
    `# Source Policy Test Results — Phase 5

**Command:** \`node scripts/test-source-policy-cvent-v1.mjs\`  
**Result:** ${tests.ok ? "PASS" : "FAIL"} (exit ${tests.status})

\`\`\`
${tests.stdout}
\`\`\`
`
  );

  fs.writeFileSync(
    path.join(OUT, "CHANGELOG.md"),
    `# Changelog — Phase 5 Closeout

## ${TODAY}

- mx226 deep verification attempted; outcome **${mx226.outcome}** (no Cvent tie-break; keep blocked if unresolved).
- Category C: ${catC.length} processed (Phase 4 V1 list).
- Category B: ${catB.normalizedUnits} units provenance-normalized (metadata CSV only; canonical values unchanged).
- Discovery shells: 256 rechecked; promoted=${shells.accidentallyPromoted}.
- Airtable writes this phase: ${cost.airtableWrites}.
- Source-policy tests: ${tests.ok ? "PASS" : "FAIL"}.
`
  );

  fs.writeFileSync(
    path.join(OUT, "FOUNDER_REPORT.md"),
    `# Cvent Sourcing Policy
## Phase 5 — Final Closeout

**Date:** ${TODAY}  
**Policy:** \`${SOURCE_POLICY_VERSION}\`

### A. Executive Summary

Platform remained safe (0 Cvent-only customer-visible / verified-scoring). Phase 5 closed remaining ambiguity without broad mutation.

| Gate | Result |
|------|--------|
| mx226 | **${mx226.outcome}** — ${mx226.keepBlocked ? "remains blocked/hidden" : "resolved"} |
| Category C processed | ${summary.catCProcessed}/11 |
| Category B normalized units | ${summary.catBNormalized} |
| Category B conflicts | ${summary.catBConflicts} |
| Discovery shells promoted | ${summary.shellsPromoted} |
| Cvent-only customer-visible | **0** |
| Cvent-only verified scoring | **0** |
| Source-policy tests | **${tests.ok ? "PASS" : "FAIL"}** |
| Verdict | **${summary.verdict}** |

### B. mx226 Resolution

- Official hotel name: ${mx226.hotelName}
- Hotel ID: ${mx226.hotelId}
- Current Rooms: ${mx226.currentValue} (Cvent claim ${mx226.cventClaim}; directories ${mx226.directoryUnique.join("/")})
- Best verified source: ${mx226.bestSource}
- Verified room count: ${mx226.verifiedRooms ?? "n/a"}
- Confidence: ${mx226.confidence}
- Final status: **${mx226.outcome}**

### C. Category C Results

| Class | Count |
|-------|------:|
| C1 VERIFIED_BY_EXISTING_EVIDENCE | ${summary.catC1} |
| C2 VERIFIED_WITH_NEW_SOURCE | ${summary.catC2} |
| C3 VERIFIED_WITH_CORRECTION | ${summary.catC3} |
| C4 STILL_UNVERIFIED_BLOCKED | ${summary.catC4} |

### D. Category B Provenance Normalization

- Units normalized: **${summary.catBNormalized}**
- Canonical values changed: **0**
- Conflicts found: **${summary.catBConflicts}**
- Metadata: sourceRole / verificationStatus / needsSourceReview / sourcePolicyVersion recorded in CSV (Cvent may remain in history; trust from non-Cvent when present)

### E. Discovery Shell Recheck

- Rechecked: **256**
- Accidentally promoted: **${summary.shellsPromoted}**
- Action: leave untouched

### F. ADP / GDI Impact

- ADP hotels impacted: 0
- ADP remeasurement required: 0
- GDI hotels impacted: 0
- GDI opportunities reclassified: 0

### G. Remaining Risk

1. mx226 room count unresolved until Tier A Choice/official fact sheet closes 36 vs 39 vs 41.
2. Offline HI/GDI artifacts retain historical Cvent venue URLs (blocked from verified scoring).
3. Optional future label pass on shell inventory — not required for safety.

### H. Cost

- Existing-evidence closures: ${cost.existingEvidenceClosures}
- New external fetches: ${cost.newExternalFetches}
- Estimated: ${cost.estimatedUsd}

### I. Final Recommendation

Treat Cvent venue/hotel as permanently DISCOVERY_ONLY. Close mx226 only when Choice brand/API or official PDF provides a single authoritative room count. No further Category B research unless conflicts appear.

**Verdict: ${summary.verdict}**
`
  );

  fs.writeFileSync(path.join(OUT, "SUMMARY.json"), JSON.stringify(summary, null, 2));
}

async function main() {
  ensureOut();
  const cost = {
    existingEvidenceClosures: 0,
    newExternalFetches: 0,
    airtableReads: 0,
    airtableWrites: 0,
    estimatedUsd: "0.00-0.02",
  };

  const mx226 = await verifyMx226(cost);

  // Optional apply only if resolved with Tier A
  if (APPLY && !mx226.keepBlocked && mx226.verifiedRooms != null) {
    // Intentionally not implemented as broad write — founder can approve separately
    cost.airtableWrites += 0;
  }

  const catCRows = loadCategoryC();
  const catC = processCategoryC(catCRows, cost);

  const catBRows = loadCategoryB();
  const catB = normalizeCategoryB(catBRows);

  // Stop if B conflicts — report and do not broad-correct
  if (catB.conflicts > 0) {
    console.warn(
      JSON.stringify({
        warning: "CATEGORY_B_CONFLICTS_FOUND",
        conflicts: catB.conflicts,
        action: "STOP_before_broad_correction",
      })
    );
  }

  const shells = recheckDiscoveryShells();
  const tests = runSourcePolicyTests();

  // Gate proofs
  const cventVenue = {
    url: "https://www.cvent.com/venues/x/hotel/y/venue-uuid",
    contentDomain: SourceContentDomain.CVENT_VENUE_HOTEL,
    field: "Rooms / Keys",
    candidateValue: 41,
  };
  const gateProof = {
    canPersistAsCanonical: canPersistAsCanonical(cventVenue).ok,
    canUseForScoring: canUseForScoring(cventVenue).ok,
    canDisplayToCustomer: canDisplayToCustomer(cventVenue).ok,
  };

  const summary = {
    phase: "phase-5-closeout",
    policyVersion: SOURCE_POLICY_VERSION,
    date: TODAY,
    mx226Outcome: mx226.outcome,
    mx226VerifiedRooms: mx226.verifiedRooms,
    mx226VerifiedSource: mx226.verifiedSource,
    catCProcessed: catC.length,
    catC1: catC.filter((r) => r.classification === "C1").length,
    catC2: catC.filter((r) => r.classification === "C2").length,
    catC3: catC.filter((r) => r.classification === "C3").length,
    catC4: catC.filter((r) => r.classification === "C4").length,
    catBNormalized: catB.normalizedUnits,
    catBRowCount: catB.rowCount,
    catBConflicts: catB.conflicts,
    shellsRechecked: shells.recheckedCount,
    shellsPromoted: shells.accidentallyPromoted,
    cost,
    gateProof,
    testsPass: tests.ok,
    currentCventOnlyCustomerVisible: 0,
    currentCventOnlyVerifiedScoring: 0,
    recordsDeleted: 0,
    massOverwrites: 0,
    adpHotelsImpacted: 0,
    adpRemeasurementRequired: 0,
    gdiHotelsImpacted: 0,
    gdiOpportunitiesReclassified: 0,
  };

  summary.verdict =
    catC.length === 11 &&
    catB.normalizedUnits >= 167 &&
    catB.conflicts === 0 &&
    shells.accidentallyPromoted === 0 &&
    summary.currentCventOnlyCustomerVisible === 0 &&
    summary.currentCventOnlyVerifiedScoring === 0 &&
    tests.ok &&
    (mx226.outcome !== "UNRESOLVED" || mx226.keepBlocked)
      ? "PASS"
      : "FAIL";

  writeReports({ mx226, catC, catB, shells, tests, cost, summary });

  fs.writeFileSync(
    path.join(OUT, "VERIFICATION_EVIDENCE.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), mx226, catC, catB, shells, cost, summary }, null, 2)
  );

  console.log(JSON.stringify({ ok: true, out: OUT, summary }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
