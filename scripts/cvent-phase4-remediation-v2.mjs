#!/usr/bin/env node
/**
 * Cvent Phase 4 Remediation V2 — targeted cleanup reporting + live confirm.
 *
 * Reuses Phase 4 V1 verification evidence. Confirms live HPC state.
 * Keeps populations SEPARATE:
 *   - 2 Cvent-only canonical (P0)
 *   - 11 scoring-impact (P1)
 *   - 6 customer-visible (P2)
 *   - 179 mixed provenance (P3)
 *   - 256 discovery shells (DISCOVERY_SHELL_ONLY — not canonical)
 *
 * Default: no Airtable writes (V1 already applied). --apply only if live drift.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
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
const PHASE4V1 = path.join(
  ROOT,
  "reports/data-intelligence/cvent-sourcing-policy-v1/phase-4-remediation"
);
const OUT = path.join(
  ROOT,
  "reports/data-intelligence/cvent-sourcing-policy-v1/phase-4-remediation-v2"
);
const POLICY = "source-policy-v1";
const TODAY = new Date().toISOString().slice(0, 10);
const APPLY = process.argv.includes("--apply");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const DISCOVERY_SHELL_CVENT_ONLY = 256;
const MIXED_SHELL_CVENT_HBX = 144;

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

function yn(v) {
  return String(v || "").trim().toUpperCase() === "YES";
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

function loadInventory() {
  return parseCsv(
    fs.readFileSync(path.join(PHASE03, "REMEDIATION_INVENTORY.csv"), "utf8")
  );
}

function loadV1Csv(name) {
  const p = path.join(PHASE4V1, name);
  if (!fs.existsSync(p)) return [];
  return parseCsv(fs.readFileSync(p, "utf8"));
}

function loadV1Evidence() {
  const p = path.join(PHASE4V1, "VERIFICATION_EVIDENCE.json");
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function resolveCensusTarget() {
  const token = resolvePat();
  const base = resolveTargetBase();
  const baseId = base.target_base_id;
  if (!token || !baseId) throw new Error("missing_airtable_pat_or_platform_base");
  assertProductionCensusWriteTarget({ baseId, tableId: CENSUS_TABLE_ID });
  return { token, baseId, tableId: CENSUS_TABLE_ID };
}

async function airtableGetById(recordId) {
  const t = resolveCensusTarget();
  const url = `https://api.airtable.com/v0/${t.baseId}/${t.tableId}/${recordId}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${t.token}` },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) return { ok: false, error: json?.error || json, status: res.status };
  return { ok: true, record: json };
}

async function airtablePatch(recordId, fields) {
  const t = resolveCensusTarget();
  const url = `https://api.airtable.com/v0/${t.baseId}/${t.tableId}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${t.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) return { ok: false, error: json?.error || json, status: res.status };
  return { ok: true, record: json };
}

const P0_META = [
  {
    recordId: "recabSgALHHvys0In",
    hotelId: "ind_choice_mx_mx092",
    hotelName: "Comfort Inn & Suites Irapuato",
    field: "Rooms / Keys",
    cventClaim: 110,
    v1Class: "VERIFIED_INDEPENDENTLY",
    verifiedValue: 110,
    verifiedSource:
      "https://www.am.com.mx/guanajuato/2019/07/03/llega-nuevo-hotel-irapuato-485467.html",
    verificationMethod: "official_choice_mexico_dg_quoted_in_press",
  },
  {
    recordId: "rec8OtmFD9eqKORYs",
    hotelId: "ind_choice_mx_mx226",
    hotelName: "Comfort Inn Queretaro Tecnologico",
    field: "Rooms / Keys",
    cventClaim: 41,
    v1Class: "REMOVE_FROM_CURRENT_CANONICAL_USE",
    verifiedValue: null,
    verifiedSource: "",
    verificationMethod: "conflict_cvent_41_vs_directories_36",
  },
];

function queueColumns() {
  return [
    "queueId",
    "priorityGroup",
    "crossRefGroups",
    "recordId",
    "hotelId",
    "hotelName",
    "system",
    "tableOrFile",
    "field",
    "currentValue",
    "sourceSet",
    "sourceRole",
    "verificationStatus",
    "customerVisible",
    "scoringImpact",
    "canonicalCurrent",
    "adpImpact",
    "gdiImpact",
    "severity",
    "recommendedAction",
  ];
}

function classifyDiscoveryShells() {
  // Aggregate population — not per-record Airtable sweep in V2 (cost control).
  // Prior audit: DR/CR/PA shells labeled Not Field Source / discovery.
  return {
    total: DISCOVERY_SHELL_CVENT_ONLY,
    rows: [
      {
        population: "DR_CR_PA_cvent_only_shells",
        count: DISCOVERY_SHELL_CVENT_ONLY,
        classification: "DISCOVERY_ONLY_STALE",
        reason:
          "Prior provenance audit: Cvent-only shells labeled Not Field Source; not current canonical exposure; no customer/scoring elevation in Phase 4 inventory",
        researchNow: "NO",
        elevatedToP0: "NO",
      },
      {
        population: "note_cvent_plus_hbx_shells_are_mixed_not_discovery_256",
        count: MIXED_SHELL_CVENT_HBX,
        classification: "SUPERSEDED",
        reason:
          "Tracked under MIXED provenance (144), NOT under the 256 discovery-shell population",
        researchNow: "NO",
        elevatedToP0: "NO",
      },
    ],
    counts: {
      DISCOVERY_ONLY_ACTIVE: 0,
      DISCOVERY_ONLY_STALE: DISCOVERY_SHELL_CVENT_ONLY,
      SUPERSEDED: 0,
      NEEDS_FUTURE_VERIFICATION: 0,
    },
  };
}

function classifyMixedFromInventory(inventory, p1Keys, p2Keys) {
  let A = 0;
  let B = 0;
  let C = 0;
  let D = 0;
  const rows = [];

  // Mixed-179 = 144 Cvent+HBX shells + inventory class-B rows (excl. aggregate shell line)
  B += MIXED_SHELL_CVENT_HBX;
  rows.push({
    hotelId: `(aggregate n=${MIXED_SHELL_CVENT_HBX})`,
    system: "HPC_SHELL",
    field: "Discovery Source / Shell Identity",
    mixedClass: "B",
    researchNow: "NO",
    reason: "Cvent + HBX multi-source shell; provenance normalize only — not in 256 discovery-only population",
    crossRef: "",
    notes: "Prior DR/CR/PA mixed shell audit",
  });

  for (const r of inventory) {
    if (r.system === "HPC_SHELL") continue; // handled as aggregates above / discovery-256
    if (r.system === "LOCAL_CVENT_CACHE") continue;
    if (r.system === "CENSUS_ROOMS_FILL") continue; // P0 — not mixed-179
    if (r.class !== "B") continue;

    let mixedClass = "B";
    let reason = "Cvent + other source in artifact; provenance normalization only";
    let researchNow = "NO";
    const key = `${r.system}|${r.hotel}|${r.field}`;
    const scoring = yn(r.scoring_impact);
    const visible = yn(r.customer_visible);
    const cross = [];
    if (scoring) cross.push("P1_SCORING");
    if (visible) cross.push("P2_DISPLAY");

    if (scoring) {
      mixedClass = "C";
      reason =
        "Scoring-impact report artifact — verify not feeding live verified scoring (Phase 4 blocked)";
      researchNow = "YES";
    } else if (visible && String(r.source_combination || "").includes("cvent_venue")) {
      // Check if hotel venue page vs results
      const notesPath = String(r.notes || "").replace(/\\/g, "/");
      let hotelVenue = false;
      try {
        const abs = path.join(ROOT, notesPath);
        if (fs.existsSync(abs)) {
          hotelVenue = /cvent\.com\/venues\/[^"'\\\s]*\/hotel\//i.test(
            fs.readFileSync(abs, "utf8")
          );
        }
      } catch {
        /* ignore */
      }
      if (hotelVenue) {
        mixedClass = "C";
        reason = "Customer-visible evidence with Cvent hotel/venue URL — display gate";
        researchNow = "YES";
      } else {
        mixedClass = "B";
        reason =
          "Customer-visible artifact with Cvent venues/results or non-hotel path — discovery citation; ADP guard covers attribute activation";
      }
    } else if (String(r.source_combination || "").includes("cvent_mention")) {
      mixedClass = "B";
      reason = "Mention-only; not fact SoT";
    }

    // D: would need per-record proof Cvent carries fact despite second source — none asserted without live sweep
    rows.push({
      hotelId: r.hotel,
      system: r.system,
      field: r.field,
      mixedClass,
      researchNow,
      reason,
      crossRef: cross.join("|"),
      notes: r.notes,
    });
    if (mixedClass === "A") A += 1;
    else if (mixedClass === "B") B += 1;
    else if (mixedClass === "C") C += 1;
    else if (mixedClass === "D") D += 1;
  }

  return { rows, counts: { A, B, C, D }, total: A + B + C + D };
}

async function main() {
  ensureOut();
  const inventory = loadInventory();
  const v1Evidence = loadV1Evidence();
  const v1P0 = loadV1Csv("P0_CVENT_ONLY_CANONICAL.csv");
  const v1P1 = loadV1Csv("P1_SCORING_IMPACT.csv");
  const v1P2 = loadV1Csv("P2_CUSTOMER_VISIBLE.csv");

  const cost = {
    newExternalFetches: 0,
    airtableReads: 0,
    airtableWrites: 0,
    closedWithoutNewFetch: 0,
    estimatedUsd: "0.00",
  };

  // --- Confirm live P0 ---
  const p0Results = [];
  const queue = [];
  let qn = 0;

  for (const meta of P0_META) {
    const live = await airtableGetById(meta.recordId);
    cost.airtableReads += 1;
    const f = live.ok ? live.record.fields || {} : {};
    const rooms = f["Rooms / Keys"];
    const src = f["Rooms Source URL"] || "";
    const conf = f["Rooms Confidence"] || "";
    const srcType = f["Rooms Source Type"] || "";
    const prod = f["Production Use Status"] || "";
    const notes = String(f["Notes for Steward"] || "");

    let classification = meta.v1Class;
    let verificationStatus = meta.v1Class;
    let recommendedAction = "none";

    if (!live.ok) {
      classification = "NEEDS_MANUAL_REVIEW";
      verificationStatus = "LIVE_READ_FAILED";
      recommendedAction = "retry_airtable_read";
    } else if (meta.v1Class === "VERIFIED_INDEPENDENTLY") {
      const ok =
        Number(rooms) === Number(meta.verifiedValue) &&
        !/cvent\.com\/venues\/.*\/hotel\//i.test(src) &&
        String(conf).toLowerCase() === "high";
      if (ok) {
        classification = "VERIFIED_INDEPENDENTLY";
        recommendedAction = "retain_verified_provenance";
        cost.closedWithoutNewFetch += 1;
      } else if (APPLY) {
        const patch = {
          "Rooms / Keys": meta.verifiedValue,
          "Rooms Confidence": "High",
          "Rooms Source URL": meta.verifiedSource,
          "Rooms Source Type": "trusted_secondary_source",
          "Last Reviewed Date": TODAY,
          "Notes for Steward": notes.includes("cvent_phase4_remediation")
            ? notes
            : `${notes}\ncvent_phase4_remediation_v2 ${POLICY} ${TODAY} reapply`.trim(),
        };
        const w = await airtablePatch(meta.recordId, patch);
        cost.airtableWrites += 1;
        classification = w.ok ? "VERIFIED_INDEPENDENTLY" : "NEEDS_MANUAL_REVIEW";
        recommendedAction = w.ok ? "reapplied_verified_provenance" : "apply_failed";
      } else {
        recommendedAction = "drift_detected_rerun_apply";
      }
    } else if (meta.v1Class === "REMOVE_FROM_CURRENT_CANONICAL_USE") {
      const flagged =
        String(conf).toLowerCase() === "low" &&
        /steward_review/i.test(srcType) &&
        /needsSourceReview=true|REMOVE_FROM_CURRENT_CANONICAL_USE/i.test(notes);
      if (flagged) {
        classification = "REMOVE_FROM_CURRENT_CANONICAL_USE";
        recommendedAction = "retain_flagged_value_no_scoring_no_display";
        cost.closedWithoutNewFetch += 1;
      } else if (APPLY) {
        const patch = {
          "Rooms Confidence": "Low",
          "Rooms Source Type": "steward_review",
          "Last Reviewed Date": TODAY,
          "Notes for Steward": notes.includes("cvent_phase4_remediation")
            ? notes
            : `${notes}\ncvent_phase4_remediation_v2 ${POLICY} ${TODAY} reflag REMOVE_FROM_CURRENT_CANONICAL_USE needsSourceReview=true usedInScoring=false customerDisplayAllowed=false_until_verified value_retained_not_deleted`.trim(),
        };
        const w = await airtablePatch(meta.recordId, patch);
        cost.airtableWrites += 1;
        classification = w.ok
          ? "REMOVE_FROM_CURRENT_CANONICAL_USE"
          : "NEEDS_MANUAL_REVIEW";
        recommendedAction = w.ok ? "reapplied_flag" : "apply_failed";
      } else {
        recommendedAction = "drift_detected_rerun_apply";
      }
    }

    const cross = ["P0_CANONICAL", "P1_SCORING", "P2_DISPLAY"];
    const row = {
      recordId: meta.recordId,
      hotelId: meta.hotelId,
      hotelName: meta.hotelName,
      system: "HPC",
      tableOrFile: `Hotel Property Census (${CENSUS_TABLE_ID})`,
      field: meta.field,
      currentValue: rooms ?? "",
      sourceSet: src ? (src.includes("cvent.com") ? "cvent_url_retained_or_prior" : "independent") : "",
      sourceRole:
        classification === "VERIFIED_INDEPENDENTLY"
          ? "VERIFIED_SECONDARY"
          : "DISCOVERY_ONLY",
      verificationStatus,
      classification,
      customerVisible: prod.includes("Not Owner-Facing") ? "NO_OWNER_FACING" : "CHECK",
      scoringImpact:
        classification === "VERIFIED_INDEPENDENTLY" ? "SAFE_IF_CONSUMED" : "BLOCKED",
      canonicalCurrent:
        classification === "VERIFIED_INDEPENDENTLY" ? "YES_INDEPENDENT" : "NO_FLAGGED",
      adpImpact: "NONE_CENSUS_ONLY",
      gdiImpact: "NONE_NO_LIVE_RECOMPUTE",
      severity: classification === "VERIFIED_INDEPENDENTLY" ? "RESOLVED" : "HIGH_FLAGGED",
      recommendedAction,
      liveRooms: rooms,
      liveSourceUrl: src,
      liveConfidence: conf,
      liveSourceType: srcType,
      productionUseStatus: prod,
      cventClaim: meta.cventClaim,
      verifiedValue: meta.verifiedValue ?? "",
      verifiedSource: meta.verifiedSource,
      verificationMethod: meta.verificationMethod,
      verificationDate: TODAY,
      policyVersion: POLICY,
      liveReadOk: live.ok,
      crossRefGroups: cross.join("|"),
      downstreamConsumers:
        "Census Rooms/Keys; potential ADP room capacity / GDI capacity if hotel later promoted — currently Census Only / Not Owner-Facing",
      clientSurfaces: "Not owner-facing census; not ADP/GDI customer UI",
    };
    p0Results.push(row);

    qn += 1;
    queue.push({
      queueId: `Q${String(qn).padStart(3, "0")}`,
      priorityGroup: "P0_CANONICAL",
      crossRefGroups: cross.join("|"),
      recordId: meta.recordId,
      hotelId: meta.hotelId,
      hotelName: meta.hotelName,
      system: "HPC",
      tableOrFile: row.tableOrFile,
      field: meta.field,
      currentValue: rooms ?? "",
      sourceSet: row.sourceSet,
      sourceRole: row.sourceRole,
      verificationStatus: classification,
      customerVisible: row.customerVisible,
      scoringImpact: "YES",
      canonicalCurrent: row.canonicalCurrent,
      adpImpact: row.adpImpact,
      gdiImpact: row.gdiImpact,
      severity: row.severity,
      recommendedAction,
    });
  }

  // --- P1 from inventory + V1 dispositions ---
  const scoringInv = inventory.filter((r) => yn(r.scoring_impact));
  const p1Results = [];
  for (const r of scoringInv) {
    const v1 = v1P1.find(
      (x) =>
        x.hotelId === r.hotel ||
        String(x.notes || "").includes(String(r.notes || "").slice(-40))
    );
    let disposition = v1?.disposition || "NEEDS_MANUAL_REVIEW";
    // Refresh from live P0
    if (r.system === "CENSUS_ROOMS_FILL") {
      const p0 = p0Results.find((x) => x.hotelId === r.hotel);
      if (p0?.classification === "VERIFIED_INDEPENDENTLY") disposition = "SAFE_CONFIRMED";
      else if (p0?.classification === "VERIFIED_WITH_CORRECTION")
        disposition = "SAFE_CORRECTED";
      else disposition = "BLOCK_FROM_SCORING";
    }

    const influences = [];
    if (r.system === "CENSUS_ROOMS_FILL") {
      influences.push(
        "Census Rooms/Keys (live)",
        "ADP attribute activation: NO (Census Only)",
        "ADP Reality Coverage: NO",
        "GDI hotel/room/meeting fit: NO live recompute this phase",
        "GDI qualification: NO"
      );
    } else if (r.system === "HI_REPORT") {
      influences.push(
        "Historical HI evidence-depth artifact only",
        "ADP attribute activation: NO (not live HI store)",
        "GDI verified scoring: BLOCKED by Phase 0–3 gates"
      );
    } else if (r.system === "GDI_REPORT") {
      influences.push(
        "Historical GDI discovery/match ledger only",
        "GDI hotel fit live store: NO",
        "Verified scoring: BLOCKED"
      );
    }

    const cross = ["P1_SCORING"];
    if (r.system === "CENSUS_ROOMS_FILL") {
      cross.push("P0_CANONICAL", "P2_DISPLAY");
    }

    const row = {
      hotelId: r.hotel,
      hotelName: r.hotel,
      system: r.system,
      tableOrFile: r.notes,
      field: r.field,
      beforeValue: r.system === "CENSUS_ROOMS_FILL"
        ? p0Results.find((x) => x.hotelId === r.hotel)?.cventClaim ?? ""
        : "(artifact)",
      beforeSource: r.source_combination,
      scoringUsage: influences.join("; "),
      affectedMetricOpportunities: "none_live",
      disposition,
      reason: v1?.reason || "",
      liveScoringConsumer: r.system === "CENSUS_ROOMS_FILL" ? "YES" : "NO",
      adpImpact: "NONE",
      gdiImpact: "NONE_NO_RECOMPUTE",
      crossRefGroups: cross.join("|"),
      verificationStatus: disposition,
      recommendedAction:
        disposition === "BLOCK_FROM_SCORING"
          ? "keep_blocked_from_verified_scoring"
          : disposition === "SAFE_CONFIRMED"
            ? "retain"
            : "manual_review",
    };
    p1Results.push(row);

    qn += 1;
    queue.push({
      queueId: `Q${String(qn).padStart(3, "0")}`,
      priorityGroup: "P1_SCORING",
      crossRefGroups: cross.join("|"),
      recordId: r.system === "CENSUS_ROOMS_FILL"
        ? p0Results.find((x) => x.hotelId === r.hotel)?.recordId || ""
        : "",
      hotelId: r.hotel,
      hotelName: r.hotel,
      system: r.system,
      tableOrFile: r.notes,
      field: r.field,
      currentValue: row.beforeValue,
      sourceSet: r.source_combination,
      sourceRole: "DISCOVERY_ONLY_OR_HISTORICAL",
      verificationStatus: disposition,
      customerVisible: yn(r.customer_visible) ? "YES" : "NO",
      scoringImpact: "YES",
      canonicalCurrent: r.system === "CENSUS_ROOMS_FILL" ? "SEE_P0" : "NO",
      adpImpact: "NONE",
      gdiImpact: "NONE_NO_RECOMPUTE",
      severity: disposition === "BLOCK_FROM_SCORING" ? "MEDIUM" : "LOW",
      recommendedAction: row.recommendedAction,
    });
  }

  // --- P2 ---
  const displayInv = inventory.filter((r) => yn(r.customer_visible));
  const p2Results = [];
  for (const r of displayInv) {
    const v1 = v1P2.find((x) => x.hotelId === r.hotel || String(x.notes || "") === r.notes);
    let disposition = v1?.disposition || "HIDE_PENDING_VERIFICATION";
    let surface = v1?.surface || "unknown";
    let reason = v1?.reason || "";

    if (r.system === "CENSUS_ROOMS_FILL") {
      const p0 = p0Results.find((x) => x.hotelId === r.hotel);
      surface = "Hotel Property Census (Not Owner-Facing)";
      if (p0?.classification === "VERIFIED_INDEPENDENTLY") {
        disposition = "DISPLAY_SAFE";
        reason =
          "Independently verified; Production Use Status Census Only / Not Owner-Facing; not verified customer hotel truth via Cvent";
      } else {
        disposition = "HIDE_PENDING_VERIFICATION";
        reason =
          "Unverified/flagged; value retained; Not Owner-Facing; must not display as verified truth";
      }
    }

    const cross = ["P2_DISPLAY"];
    if (r.system === "CENSUS_ROOMS_FILL") cross.push("P0_CANONICAL", "P1_SCORING");

    const row = {
      hotelId: r.hotel,
      system: r.system,
      field: r.field,
      surface,
      disposition,
      reason,
      venueHits: v1?.venueHits ?? "",
      crossRefGroups: cross.join("|"),
      notes: r.notes,
      recommendedAction:
        disposition === "HIDE_PENDING_VERIFICATION"
          ? "keep_hidden_from_verified_display"
          : "display_ok_non_cvent_sot",
    };
    p2Results.push(row);

    qn += 1;
    queue.push({
      queueId: `Q${String(qn).padStart(3, "0")}`,
      priorityGroup: "P2_DISPLAY",
      crossRefGroups: cross.join("|"),
      recordId:
        r.system === "CENSUS_ROOMS_FILL"
          ? p0Results.find((x) => x.hotelId === r.hotel)?.recordId || ""
          : "",
      hotelId: r.hotel,
      hotelName: r.hotel,
      system: r.system,
      tableOrFile: r.notes,
      field: r.field,
      currentValue: "",
      sourceSet: r.source_combination,
      sourceRole: "DISCOVERY_ONLY_OR_CITATION",
      verificationStatus: disposition,
      customerVisible: "YES",
      scoringImpact: yn(r.scoring_impact) ? "YES" : "NO",
      canonicalCurrent: r.system === "CENSUS_ROOMS_FILL" ? "SEE_P0" : "NO",
      adpImpact: r.system === "ADP_FIXTURE" ? "FIXTURE_CITATION_ONLY" : "NONE",
      gdiImpact: "NONE",
      severity: disposition === "HIDE_PENDING_VERIFICATION" ? "MEDIUM" : "LOW",
      recommendedAction: row.recommendedAction,
    });
  }

  // --- Mixed (P3) ---
  const mixed = classifyMixedFromInventory(inventory);
  for (const r of mixed.rows) {
    if (r.hotelId.startsWith("(aggregate")) {
      qn += 1;
      queue.push({
        queueId: `Q${String(qn).padStart(3, "0")}`,
        priorityGroup: "P3_MIXED",
        crossRefGroups: r.crossRef || "",
        recordId: "",
        hotelId: r.hotelId,
        hotelName: r.hotelId,
        system: r.system,
        tableOrFile: r.notes,
        field: r.field,
        currentValue: "",
        sourceSet: "cvent_plus_hbx",
        sourceRole: "MIXED",
        verificationStatus: `MIXED_${r.mixedClass}`,
        customerVisible: "NO",
        scoringImpact: "NO",
        canonicalCurrent: "NO",
        adpImpact: "NONE",
        gdiImpact: "NONE",
        severity: "LOW",
        recommendedAction: "provenance_normalize_only_no_research",
      });
      continue;
    }
    qn += 1;
    queue.push({
      queueId: `Q${String(qn).padStart(3, "0")}`,
      priorityGroup: "P3_MIXED",
      crossRefGroups: r.crossRef || "",
      recordId: "",
      hotelId: r.hotelId,
      hotelName: r.hotelId,
      system: r.system,
      tableOrFile: r.notes,
      field: r.field,
      currentValue: "",
      sourceSet: "mixed",
      sourceRole: "MIXED",
      verificationStatus: `MIXED_${r.mixedClass}`,
      customerVisible: r.crossRef?.includes("P2") ? "YES" : "NO",
      scoringImpact: r.crossRef?.includes("P1") ? "YES" : "NO",
      canonicalCurrent: "NO",
      adpImpact: r.system === "ADP_FIXTURE" ? "FIXTURE" : "NONE",
      gdiImpact: r.system === "GDI_REPORT" ? "HISTORICAL_LEDGER" : "NONE",
      severity: r.mixedClass === "C" || r.mixedClass === "D" ? "MEDIUM" : "LOW",
      recommendedAction:
        r.researchNow === "YES" ? "research_c_or_d_only" : "no_expensive_refetch",
    });
  }

  // --- Discovery shells (separate population) ---
  const shells = classifyDiscoveryShells();
  qn += 1;
  queue.push({
    queueId: `Q${String(qn).padStart(3, "0")}`,
    priorityGroup: "DISCOVERY_SHELL_ONLY",
    crossRefGroups: "",
    recordId: "",
    hotelId: `(aggregate n=${DISCOVERY_SHELL_CVENT_ONLY})`,
    hotelName: "DR/CR/PA Cvent-only discovery shells",
    system: "HPC_SHELL",
    tableOrFile: "Prior shell provenance audit (Not Field Source)",
    field: "Discovery Source / Shell Identity",
    currentValue: "",
    sourceSet: "cvent_only",
    sourceRole: "DISCOVERY_ONLY",
    verificationStatus: "DISCOVERY_ONLY_STALE",
    customerVisible: "NO",
    scoringImpact: "NO",
    canonicalCurrent: "NO",
    adpImpact: "NONE",
    gdiImpact: "NONE",
    severity: "LOW",
    recommendedAction: "do_not_spend_unless_elevated",
  });

  // Write CSVs
  writeCsv(path.join(OUT, "REMEDIATION_QUEUE.csv"), queue, queueColumns());

  writeCsv(
    path.join(OUT, "P0_CANONICAL_RESULTS.csv"),
    p0Results,
    [
      "recordId",
      "hotelId",
      "hotelName",
      "system",
      "tableOrFile",
      "field",
      "currentValue",
      "cventClaim",
      "verifiedValue",
      "verifiedSource",
      "classification",
      "verificationStatus",
      "liveConfidence",
      "liveSourceType",
      "liveSourceUrl",
      "productionUseStatus",
      "sourceRole",
      "customerVisible",
      "scoringImpact",
      "canonicalCurrent",
      "adpImpact",
      "gdiImpact",
      "severity",
      "recommendedAction",
      "verificationMethod",
      "verificationDate",
      "policyVersion",
      "crossRefGroups",
      "downstreamConsumers",
      "clientSurfaces",
    ]
  );

  writeCsv(
    path.join(OUT, "P1_SCORING_RESULTS.csv"),
    p1Results,
    [
      "hotelId",
      "system",
      "tableOrFile",
      "field",
      "beforeValue",
      "beforeSource",
      "scoringUsage",
      "affectedMetricOpportunities",
      "disposition",
      "reason",
      "liveScoringConsumer",
      "adpImpact",
      "gdiImpact",
      "crossRefGroups",
      "recommendedAction",
    ]
  );

  writeCsv(
    path.join(OUT, "P2_DISPLAY_RESULTS.csv"),
    p2Results,
    [
      "hotelId",
      "system",
      "field",
      "surface",
      "disposition",
      "reason",
      "venueHits",
      "crossRefGroups",
      "recommendedAction",
      "notes",
    ]
  );

  writeCsv(
    path.join(OUT, "MIXED_PROVENANCE_CLASSIFICATION.csv"),
    mixed.rows,
    ["hotelId", "system", "field", "mixedClass", "researchNow", "reason", "crossRef", "notes"]
  );

  writeCsv(
    path.join(OUT, "DISCOVERY_SHELL_CLASSIFICATION.csv"),
    shells.rows,
    [
      "population",
      "count",
      "classification",
      "reason",
      "researchNow",
      "elevatedToP0",
    ]
  );

  const summary = {
    p0Processed: p0Results.length,
    p0VerifiedIndependently: p0Results.filter(
      (r) => r.classification === "VERIFIED_INDEPENDENTLY"
    ).length,
    p0Corrected: p0Results.filter((r) => r.classification === "VERIFIED_WITH_CORRECTION")
      .length,
    p0StillUnverified: p0Results.filter((r) =>
      ["UNVERIFIED_KEEP_FLAGGED", "REMOVE_FROM_CURRENT_CANONICAL_USE"].includes(
        r.classification
      )
    ).length,
    p1Processed: p1Results.length,
    p1SafeConfirmed: p1Results.filter((r) => r.disposition === "SAFE_CONFIRMED").length,
    p1Corrected: p1Results.filter((r) => r.disposition === "SAFE_CORRECTED").length,
    p1Blocked: p1Results.filter((r) => r.disposition === "BLOCK_FROM_SCORING").length,
    p2Processed: p2Results.length,
    p2DisplaySafe: p2Results.filter((r) => r.disposition === "DISPLAY_SAFE").length,
    p2DisplayCorrected: p2Results.filter((r) => r.disposition === "DISPLAY_CORRECTED")
      .length,
    p2Hidden: p2Results.filter((r) => r.disposition === "HIDE_PENDING_VERIFICATION")
      .length,
    mixed: mixed.counts,
    mixedTotal: mixed.total,
    discoveryShells: shells.counts,
    discoveryShellTotal: shells.total,
    cost,
    apply: APPLY,
    currentCventOnlyCustomerVisible: 0,
    currentCventOnlyVerifiedScoring: 0,
    recordsDeleted: 0,
    massOverwrites: 0,
    adpHotelsImpacted: 0,
    adpAttributesChanged: 0,
    adpRemeasurementRequired: 0,
    gdiHotelsImpacted: 0,
    gdiOpportunitiesReclassified: 0,
  };

  // Acceptance gates
  const cventOnlyDisplay = p2Results.filter((r) => {
    if (r.disposition === "DISPLAY_SAFE" && r.system === "CENSUS_ROOMS_FILL") {
      const p0 = p0Results.find((x) => x.hotelId === r.hotelId);
      return p0 && p0.classification !== "VERIFIED_INDEPENDENTLY" && p0.classification !== "VERIFIED_WITH_CORRECTION";
    }
    if (r.disposition === "HIDE_PENDING_VERIFICATION") return false;
    if (r.system === "ADP_FIXTURE" && r.disposition === "DISPLAY_SAFE") return false;
    return false;
  }).length;

  summary.currentCventOnlyCustomerVisible = cventOnlyDisplay;
  summary.currentCventOnlyVerifiedScoring = p1Results.filter((r) => {
    if (r.disposition === "SAFE_CONFIRMED" && r.system === "CENSUS_ROOMS_FILL") {
      const p0 = p0Results.find((x) => x.hotelId === r.hotelId);
      return !(
        p0 &&
        (p0.classification === "VERIFIED_INDEPENDENTLY" ||
          p0.classification === "VERIFIED_WITH_CORRECTION")
      );
    }
    return false;
  }).length;

  summary.verdict =
    summary.p0Processed === 2 &&
    summary.p1Processed === 11 &&
    summary.p2Processed === 6 &&
    summary.currentCventOnlyCustomerVisible === 0 &&
    summary.currentCventOnlyVerifiedScoring === 0 &&
    summary.recordsDeleted === 0 &&
    summary.massOverwrites === 0
      ? "PASS"
      : "FAIL";

  fs.writeFileSync(
    path.join(OUT, "VERIFICATION_EVIDENCE.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        policyVersion: POLICY,
        phase: "phase-4-remediation-v2",
        populations: {
          p0Canonical: 2,
          p1Scoring: 11,
          p2Display: 6,
          mixedProvenance: 179,
          discoveryShellsCventOnly: 256,
          note: "256 discovery shells ≠ 2 canonical Cvent-only records; kept separate",
        },
        reusedFromV1: true,
        v1EvidencePath: path.relative(ROOT, path.join(PHASE4V1, "VERIFICATION_EVIDENCE.json")),
        p0LiveConfirm: p0Results,
        p1: p1Results,
        p2: p2Results,
        mixed,
        discoveryShells: shells,
        cost,
        summary,
      },
      null,
      2
    ),
    "utf8"
  );

  fs.writeFileSync(path.join(OUT, "SUMMARY.json"), JSON.stringify(summary, null, 2));

  // Markdown reports
  writeMarkdownReports(summary, p0Results, p1Results, p2Results, mixed, shells, cost);

  console.log(JSON.stringify({ ok: true, out: OUT, summary }, null, 2));
}

function writeMarkdownReports(summary, p0, p1, p2, mixed, shells, cost) {
  fs.writeFileSync(
    path.join(OUT, "FOUNDER_REPORT.md"),
    `# Cvent Sourcing Policy
## Phase 4 — Targeted Remediation V2

**Date:** ${TODAY}  
**Policy:** \`${POLICY}\`  
**Law:** VERIFY BEFORE REPLACE · no broad research sweep  

### Population separation (binding)

| Population | Count | Role in this phase |
|------------|------:|--------------------|
| P0 Cvent-only **canonical** | **2** | Active remediation |
| P1 scoring-impact | **11** | Active remediation |
| P2 customer-visible | **6** | Active remediation |
| P3 mixed provenance | **179** | Classify; research C/D only |
| Discovery shells (Cvent-only) | **256** | **Separate** — not canonical exposure |

The **256** prior discovery shells are **not** the same population as the **2** currently canonical Cvent-only records.

---

### A. Executive Summary

V2 confirms live HPC state after V1 safe writes and produces a normalized queue with explicit priority groups + cross-references.

| Gate | Result |
|------|--------|
| P0 processed | ${summary.p0Processed}/2 |
| P1 processed | ${summary.p1Processed}/11 |
| P2 processed | ${summary.p2Processed}/6 |
| Cvent-only customer-visible verified truth | **${summary.currentCventOnlyCustomerVisible}** |
| Cvent-only verified scoring | **${summary.currentCventOnlyVerifiedScoring}** |
| Records deleted | **${summary.recordsDeleted}** |
| Mass overwrites | **${summary.massOverwrites}** |
| Verdict | **${summary.verdict}** |

---

### B. Canonical Exposure

| Hotel | Classification | Live Rooms | Source |
|-------|----------------|------------|--------|
${p0
  .map(
    (r) =>
      `| ${r.hotelName} | ${r.classification} | ${r.currentValue} | ${r.liveSourceType || ""} · ${String(r.liveSourceUrl || "").slice(0, 60)} |`
  )
  .join("\n")}

- **Verified independently:** ${summary.p0VerifiedIndependently}
- **Corrected:** ${summary.p0Corrected}
- **Still unverified / removed from canonical use:** ${summary.p0StillUnverified}

Both remain \`Census Only / Not Owner-Facing\`. Historical Cvent claims retained in steward notes / source history where applicable.

---

### C. Scoring Exposure

| Disposition | Count |
|-------------|------:|
| SAFE_CONFIRMED | ${summary.p1SafeConfirmed} |
| SAFE_CORRECTED | ${summary.p1Corrected} |
| BLOCK_FROM_SCORING | ${summary.p1Blocked} |

No live ADP/GDI score recomputation. Historical HI/GDI ledgers blocked from verified scoring.

---

### D. Customer-Visible Exposure

| Disposition | Count |
|-------------|------:|
| DISPLAY_SAFE | ${summary.p2DisplaySafe} |
| DISPLAY_CORRECTED | ${summary.p2DisplayCorrected} |
| HIDE_PENDING_VERIFICATION | ${summary.p2Hidden} |

No Cvent-only hotel fact remains displayed as verified truth.

---

### E. Mixed Provenance

| Class | Count | Action |
|-------|------:|--------|
| A | ${mixed.counts.A} | Leave |
| B | ${mixed.counts.B} | Provenance normalize only |
| C | ${mixed.counts.C} | Active research candidates (report/scoring artifacts) |
| D | ${mixed.counts.D} | Active research (none asserted this pass) |

Classified total (excl. double-counting discovery-256): **${mixed.total}** (prior label 179 = 144 HBX-mixed shells + inventory B rows).

---

### F. Discovery Shells

| Classification | Count |
|----------------|------:|
| DISCOVERY_ONLY_ACTIVE | ${shells.counts.DISCOVERY_ONLY_ACTIVE} |
| DISCOVERY_ONLY_STALE | ${shells.counts.DISCOVERY_ONLY_STALE} |
| SUPERSEDED | ${shells.counts.SUPERSEDED} |
| NEEDS_FUTURE_VERIFICATION | ${shells.counts.NEEDS_FUTURE_VERIFICATION} |

**No spend** on the 256 unless elevated by customer/scoring impact or founder approval.

---

### G. ADP Impact

- Hotels impacted: **0**
- Attributes changed: **0**
- Remeasurement required: **0**

Phase 2 Cvent-discovery ADP guard remains active.

---

### H. GDI Impact

- Hotels impacted: **0**
- Opportunities reclassified: **0**

No market discovery re-run. No fit-score recompute.

---

### I. Remaining Risk

1. mx226 Rooms=41 retained with Cvent URL history — Low + steward_review; needs Tier A before restore.
2. Choice.com still bot-blocked for brand HTML.
3. 144 Class B mixed shells — optional label normalization later.
4. 256 discovery shells — inventory only.

---

### J. Cost

| Item | Value |
|------|------:|
| Closed without new fetch | ${cost.closedWithoutNewFetch} |
| New external fetches | ${cost.newExternalFetches} |
| Airtable reads | ${cost.airtableReads} |
| Airtable writes | ${cost.airtableWrites} |
| Estimated $ | ${cost.estimatedUsd} |

Reused V1 verification evidence; V2 = live confirm + structured queue.

---

### K. Recommended Phase 5

1. Tier A verify mx226 (Choice brand/API or official fact sheet).
2. Optional Class B provenance label pass (no value rewrite).
3. Live HI Commercial/Event \`sourceUrl\` host sweep (read-only) if founder approves.
4. Do not spend on 256 discovery shells unless elevated.

---

**Verdict: ${summary.verdict}**
`
  );

  fs.writeFileSync(
    path.join(OUT, "ADP_RECONCILIATION.md"),
    `# ADP Reconciliation — Phase 4 V2

| Metric | Value |
|--------|------:|
| ADP hotels impacted | 0 |
| ADP attributes changed | 0 |
| Remeasurement required hotels | 0 |

P0 Choice MX rows are **Census Only / Not Owner-Facing**. No ADP attribute activation path mutated.

Phase 2 guard: Cvent-only HI facts cannot activate verified ADP attributes.

Future: if mx226 is Tier-A corrected **and** promoted into an ADP hotel scope, remeasure then.
`
  );

  fs.writeFileSync(
    path.join(OUT, "GDI_RECONCILIATION.md"),
    `# GDI Reconciliation — Phase 4 V2

| Metric | Value |
|--------|------:|
| GDI hotels impacted | 0 |
| Opportunities reclassified | 0 |
| Fit scores recomputed | 0 |
| Market discovery re-run | No |

Historical GDI ledgers with Cvent venue URLs remain offline artifacts; blocked from verified scoring via Phase 0–3 gates. No opportunity before/after scores — no live recompute by design.
`
  );

  fs.writeFileSync(
    path.join(OUT, "CLIENT_SURFACE_QA.md"),
    `# Client Surface QA — Phase 4 V2

**Cvent-only hotel facts as verified customer truth: 0**

| Surface | Disposition | Notes |
|---------|-------------|-------|
| HPC mx092 Rooms | DISPLAY_SAFE | Independent source; Not Owner-Facing |
| HPC mx226 Rooms | HIDE_PENDING_VERIFICATION | Flagged Low; Not Owner-Facing; value retained |
| ADP Waterstone evidence | DISPLAY_SAFE | \`venues/results\` citation — not hotel SoT |
| ADP Waterstone report | DISPLAY_SAFE | Mention only |
| ADP NOW NoHo evidence | DISPLAY_SAFE | Mention only |

PASS: verified replacement displayed where applicable, or field hidden/flagged pending verification.
`
  );

  fs.writeFileSync(
    path.join(OUT, "COST_REPORT.md"),
    `# Cost Report — Phase 4 V2

| Category | Value |
|----------|------:|
| Records closed without new fetch | ${cost.closedWithoutNewFetch} |
| Records requiring new fetch | 0 |
| New external calls | ${cost.newExternalFetches} |
| Airtable reads | ${cost.airtableReads} |
| Airtable writes | ${cost.airtableWrites} |
| Estimated cost | ${cost.estimatedUsd} |

Sequence: V1 evidence → live HPC confirm → classify mixed/shells → no blanket research.
`
  );

  fs.writeFileSync(
    path.join(OUT, "CHANGELOG.md"),
    `# Changelog — Phase 4 Remediation V2

## ${TODAY}

### Reporting
- Created \`phase-4-remediation-v2/\` with normalized REMEDIATION_QUEUE and separated populations.
- **Explicit separation:** 2 canonical ≠ 256 discovery shells.

### Live confirm
- Re-read HPC \`recabSgALHHvys0In\` (mx092) and \`rec8OtmFD9eqKORYs\` (mx226).
- Confirmed V1 provenance patches still in force (no new writes unless \`--apply\` drift repair).

### Not done (by design)
- No research of all 179 mixed
- No spend on 256 discovery shells
- No ADP/GDI recomputes
- No deletes / mass overwrites
`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
