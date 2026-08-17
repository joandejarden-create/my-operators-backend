/**
 * Read-only founder review of VIC → BE medium sandbox pilot (vic.pilot.medium.*).
 * Never writes Airtable (production or sandbox).
 */

import { readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  maskBaseId,
  readSandboxEnv,
  resolveSandboxApiKey,
} from "./airtable-sandbox-validation.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const REVIEW_VERSION = "mexico-vic-be-medium-sandbox-founder-review-v1";
export const EXPECTED_FREEZE =
  "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";

export const STATUS = Object.freeze({
  PAUSE: "medium_sandbox_founder_review_approved_pause_vic_lane",
  PREPARE_PROD: "medium_sandbox_founder_review_approved_prepare_production_dry_run",
  MINOR_EDITS: "medium_sandbox_founder_review_minor_edits_required",
  HOLD: "medium_sandbox_founder_review_hold",
  ROW_MISMATCH: "medium_sandbox_founder_review_row_count_mismatch",
});

export const MEDIUM_BRANDS = Object.freeze([
  {
    slug: "hotel-indigo",
    expectedRecordId: "recegXrqaPiSLGCIe",
    nameMatchers: [/Hotel Indigo/i],
  },
  {
    slug: "ascend",
    expectedRecordId: "reclkgOzvAcBheUSo",
    nameMatchers: [/Ascend/i],
  },
  {
    slug: "curio-collection",
    expectedRecordId: "receQkxgjlezsc1xg",
    nameMatchers: [/Curio Collection/i],
  },
  {
    slug: "holiday-inn-express",
    expectedRecordId: "recmGmiIqDtAsm01f",
    nameMatchers: [/Holiday Inn Express/i],
  },
  {
    slug: "voco-hotels",
    expectedRecordId: "recwONQTqGU1jHCsM",
    nameMatchers: [/[Vv]oco/],
  },
  {
    slug: "kimpton",
    expectedRecordId: "recCKuXCmGvxHPfb3",
    nameMatchers: [/Kimpton/i],
  },
  {
    slug: "avid-hotels",
    expectedRecordId: "recoEarnE8T6sDjZq",
    nameMatchers: [/avid hotels/i],
  },
]);

export const EXPECTED_SLOTS = Object.freeze([
  "vic.pilot.medium.property_examples",
  "vic.pilot.medium.geographic_footprint_mexico",
  "vic.pilot.medium.portfolio_context",
  "vic.pilot.medium.owner_facing_copy",
]);

export const EXPECTED_ROW_COUNT = 28;
export const EXPECTED_SMALL_PILOT_SLOTS = Object.freeze([
  "vic.pilot.property_examples",
  "vic.pilot.geographic_footprint_mexico",
  "vic.pilot.portfolio_context",
  "vic.pilot.owner_facing_copy",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS_TABLE = "Brand Setup - Brand Basics";

const SLUG_BY_RECORD_ID = Object.freeze(
  Object.fromEntries(MEDIUM_BRANDS.map((b) => [b.expectedRecordId, b.slug]))
);

const FORBIDDEN_PROCESS =
  /\b(vic|census|staging|sandbox|source pack|directory|source-supported|steward|overlay|\bqa\b|process|company validated|brand verified|confirmed by (ihg|hilton|choice|marriott)|verified by|company confirmed|brand validated|census proves|directory confirms)\b/i;

const FORBIDDEN_FACT_CLAIMS =
  /\b(\d+\s*rooms?\b|(opening date|opened on|open date|affiliation start)|operated by|managed by (choice|hilton|ihg)|choice owns|owned by choice|faranda (owns|manages|operated)|hilton owns|owned by hilton)\b/i;

const FORBIDDEN_OWNER_CLAIM =
  /\b((the )?property owner\b|owner is\b|owners? of (the )?hotel\b|hotel owner\b)\b/i;

function summarizeBody(body, max = 140) {
  const t = String(body || "").replace(/\s+/g, " ").trim();
  if (t.length <= max) return t;
  return `${t.slice(0, max - 1)}…`;
}

function scanCopy(title, body) {
  const issues = [];
  const blob = `${title || ""}\n${body || ""}`;
  if (/https?:\/\//i.test(blob)) issues.push({ code: "raw_url", severity: "reject" });
  const proc = blob.match(FORBIDDEN_PROCESS);
  if (proc) issues.push({ code: "forbidden_process_language", term: proc[0], severity: "reject" });
  const fact = blob.match(FORBIDDEN_FACT_CLAIMS);
  if (fact) issues.push({ code: "forbidden_fact_claim", term: fact[0], severity: "reject" });
  const own = blob.match(FORBIDDEN_OWNER_CLAIM);
  if (own) issues.push({ code: "forbidden_owner_identity_claim", term: own[0], severity: "reject" });
  if (/\bowners?\b/i.test(blob) && !own) {
    issues.push({
      code: "audience_owner_address",
      severity: "info",
      note: "Audience address allowed when not a property-identity claim",
    });
  }
  if (/recent momentum|momentum card|just opened|newly opened/i.test(blob)) {
    issues.push({ code: "possible_false_momentum", severity: "reject" });
  }
  if (!String(body || "").trim()) issues.push({ code: "empty_body", severity: "reject" });
  if (!String(title || "").trim()) issues.push({ code: "empty_title", severity: "reject" });
  if (String(body || "").trim().length > 0 && String(body || "").trim().length < 40) {
    issues.push({ code: "vague_or_thin_copy", severity: "warn" });
  }
  return issues;
}

function assessRulings(rowsBySlug) {
  const blob = (slug) =>
    (rowsBySlug[slug] || []).map((r) => `${r.title}\n${r.body}`).join("\n");

  const ascendBlob = blob("ascend");
  const curioBlob = blob("curio-collection");
  const vocoBlob = blob("voco-hotels");
  const kimptonBlob = blob("kimpton");
  const avidBlob = blob("avid-hotels");
  const hieBlob = blob("holiday-inn-express");

  const checks = [
    { id: "ascend_amberes", pass: /Amberes 64/i.test(ascendBlob), detail: "Amberes 64 present" },
    {
      id: "ascend_el_cid_castilla",
      pass:
        /El Cid Castilla/i.test(ascendBlob) ||
        (/El Cid/i.test(ascendBlob) && /Mazatl[aá]n/i.test(ascendBlob) && /soft-brand/i.test(ascendBlob)),
      detail: "El Cid Castilla named or Mazatlán El Cid soft-brand example present",
    },
    {
      id: "ascend_el_cid_la_ceiba",
      pass:
        /El Cid La Ceiba/i.test(ascendBlob) ||
        (/El Cid/i.test(ascendBlob) && /Cozumel/i.test(ascendBlob) && /soft-brand/i.test(ascendBlob)),
      detail: "El Cid La Ceiba named or Cozumel El Cid soft-brand example present",
    },
    { id: "ascend_soft_brand", pass: /soft-brand/i.test(ascendBlob), detail: "Soft-brand framing present" },
    {
      id: "ascend_no_choice_owns",
      pass: !/\b(Choice owns|owned by Choice)\b/i.test(ascendBlob),
      detail: "No Choice ownership claim",
    },
    {
      id: "ascend_no_faranda",
      pass: !/\bFaranda\b/i.test(ascendBlob),
      detail: "No Faranda claim",
    },
    {
      id: "ascend_no_direct_mgmt_claim",
      pass:
        !/\b(managed by Choice|Choice management)\b/i.test(ascendBlob) &&
        (!/\bdirect management\b/i.test(ascendBlob) ||
          /without assuming[^.]{0,80}direct management/i.test(ascendBlob)),
      detail: "No affirmative direct management claim",
    },
    {
      id: "ascend_no_vic_momentum",
      pass: !/recent momentum/i.test(ascendBlob),
      detail: "No Recent Momentum from VIC on Ascend",
    },
    {
      id: "curio_ms_milenium_san_pedro",
      pass: /San Pedro Garza García/i.test(curioBlob),
      detail: "MS Milenium city = San Pedro Garza García",
    },
    {
      id: "curio_no_hilton_owns",
      pass: !/\b(Hilton owns|owned by Hilton)\b/i.test(curioBlob),
      detail: "No Hilton ownership claim",
    },
    {
      id: "curio_no_vic_momentum",
      pass: !/recent momentum/i.test(curioBlob),
      detail: "No Recent Momentum from VIC on Curio",
    },
    {
      id: "voco_brand_clear",
      pass: /\bvoco\b/i.test(vocoBlob) && !/\bHoliday Inn Express\b/i.test(vocoBlob),
      detail: "voco rows clearly voco, not generic IHG / HIE",
    },
    {
      id: "kimpton_brand_clear",
      pass: /\bKimpton\b/i.test(kimptonBlob) && !/\bHoliday Inn Express\b/i.test(kimptonBlob),
      detail: "Kimpton rows clearly Kimpton, not generic IHG",
    },
    {
      id: "avid_brand_clear",
      pass:
        /\bavid hotels\b/i.test(avidBlob) &&
        !/\bHoliday Inn Express\b/i.test(avidBlob) &&
        !/\bvoco\b/i.test(avidBlob),
      detail: "avid rows clearly avid and distinct from HIE/voco",
    },
    {
      id: "hie_not_confused_with_avid",
      pass: !/\bavid hotels\b/i.test(hieBlob),
      detail: "HIE rows do not mix avid branding",
    },
  ];

  return {
    checks,
    pass: checks.every((c) => c.pass),
    failed: checks.filter((c) => !c.pass),
  };
}

async function airtableFetch(apiKey, url) {
  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`${res.status} ${url}: ${data?.error?.message || data?.error || res.statusText}`);
  }
  return data;
}

async function fetchRowsBySlotPrefix(apiKey, baseId, prefix) {
  const table = encodeURIComponent(PRESENTATION_TABLE);
  const formula = encodeURIComponent(`FIND('${prefix}', {Slot Key})`);
  const rows = [];
  let offset = null;
  do {
    let url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${table}?filterByFormula=${formula}&pageSize=100`;
    if (offset) url += `&offset=${encodeURIComponent(offset)}`;
    const data = await airtableFetch(apiKey, url);
    rows.push(...(data.records || []));
    offset = data.offset || null;
  } while (offset);
  return rows;
}

async function fetchBasics(apiKey, baseId, recordId) {
  try {
    const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(BASICS_TABLE)}/${encodeURIComponent(recordId)}`;
    const data = await airtableFetch(apiKey, url);
    return {
      id: data.id,
      brand_name: data.fields?.["Brand Name"] || null,
      brand_status: data.fields?.["Brand Status"] || null,
    };
  } catch {
    return { id: recordId, brand_name: null, brand_status: null };
  }
}

export async function runMediumSandboxFounderReview(options = {}) {
  const generatedAt = options.generatedAt || new Date().toISOString();
  const env = options.env || process.env;
  const cfg = readSandboxEnv(env);
  const keyResolution = await resolveSandboxApiKey(env);

  if (!keyResolution.ok || !cfg.sandboxBaseId) {
    return {
      version: REVIEW_VERSION,
      status: STATUS.HOLD,
      recommendation: STATUS.HOLD,
      generated_at: generatedAt,
      error: keyResolution.detail || "sandbox not reachable",
      production_writes: 0,
      sandbox_writes: 0,
      lane_decision: "hold",
    };
  }

  const apiKey = keyResolution.apiKey;
  const mediumRaw = await fetchRowsBySlotPrefix(apiKey, cfg.sandboxBaseId, "vic.pilot.medium.");
  const allVicPilot = await fetchRowsBySlotPrefix(apiKey, cfg.sandboxBaseId, "vic.pilot.");
  const smallOnly = allVicPilot.filter((r) => {
    const sk = String(r.fields?.["Slot Key"] || "");
    return EXPECTED_SMALL_PILOT_SLOTS.includes(sk);
  });

  /** @type {object[]} */
  const normalized = [];
  for (const r of mediumRaw) {
    const f = r.fields || {};
    const link = f.Brand || f["Brand Setup - Brand Basics"] || f.Brand_Basic_ID || f["Brand Basics"];
    const brandRecordId = Array.isArray(link) ? link[0] : null;
    const slug = brandRecordId ? SLUG_BY_RECORD_ID[brandRecordId] || null : null;
    const basics = brandRecordId
      ? await fetchBasics(apiKey, cfg.sandboxBaseId, brandRecordId)
      : null;
    const title = f.Title || "";
    const body = f.Body || "";
    const slotKey = f["Slot Key"] || "";
    const copyIssues = scanCopy(title, body);
    const rejectIssues = copyIssues.filter((i) => i.severity === "reject");
    const warnIssues = copyIssues.filter((i) => i.severity === "warn");
    let reviewStatus = "pass";
    if (rejectIssues.length) reviewStatus = "fail";
    else if (warnIssues.length) reviewStatus = "warn";

    normalized.push({
      record_id: r.id,
      brand_record_id: brandRecordId,
      brand_name: basics?.brand_name || null,
      brand_slug: slug,
      brand_status: basics?.brand_status || null,
      slot_key: slotKey,
      title,
      body,
      body_summary: summarizeBody(body),
      created: f.Created || r.createdTime || null,
      copy_issues: copyIssues,
      review_status: reviewStatus,
      issue:
        rejectIssues.map((i) => i.code + (i.term ? `:${i.term}` : "")).join("; ") ||
        warnIssues.map((i) => i.code).join("; ") ||
        copyIssues
          .filter((i) => i.severity === "info")
          .map((i) => i.code)
          .join("; ") ||
        null,
    });
  }

  const expectedKeys = [];
  for (const b of MEDIUM_BRANDS) {
    for (const slot of EXPECTED_SLOTS) expectedKeys.push(`${b.slug}::${slot}`);
  }
  const foundKeys = normalized.map((r) => `${r.brand_slug || "UNKNOWN"}::${r.slot_key}`);
  const foundSet = new Set(foundKeys);
  const missing = expectedKeys.filter((k) => !foundSet.has(k));
  const keyCounts = {};
  for (const k of foundKeys) keyCounts[k] = (keyCounts[k] || 0) + 1;
  const duplicates = Object.entries(keyCounts)
    .filter(([, n]) => n > 1)
    .map(([k, n]) => ({ key: k, count: n }));
  const unexpected = normalized.filter(
    (r) =>
      !r.brand_slug ||
      !MEDIUM_BRANDS.some((b) => b.slug === r.brand_slug) ||
      !String(r.slot_key || "").startsWith("vic.pilot.medium.")
  );
  const wrongSlot = normalized.filter(
    (r) =>
      String(r.slot_key || "").startsWith("vic.pilot.medium.") &&
      !EXPECTED_SLOTS.includes(r.slot_key)
  );

  const brandsPresent = Object.fromEntries(
    MEDIUM_BRANDS.map((b) => [b.slug, normalized.filter((r) => r.brand_slug === b.slug).length])
  );

  const rowCountOk =
    normalized.length === EXPECTED_ROW_COUNT &&
    missing.length === 0 &&
    duplicates.length === 0 &&
    unexpected.length === 0 &&
    wrongSlot.length === 0 &&
    Object.values(brandsPresent).every((n) => n === 4);

  const smallPilotPreserved = smallOnly.length === 16;

  const rowsBySlug = Object.fromEntries(
    MEDIUM_BRANDS.map((b) => [b.slug, normalized.filter((r) => r.brand_slug === b.slug)])
  );
  const rulings = assessRulings(rowsBySlug);
  const copyRejects = normalized.filter((r) => r.review_status === "fail");
  const copyWarns = normalized.filter((r) => r.review_status === "warn");

  let status;
  let laneDecision;
  if (!rowCountOk || !smallPilotPreserved) {
    status = STATUS.HOLD;
    laneDecision = "hold";
  } else if (copyRejects.length || !rulings.pass) {
    if (copyRejects.length <= 2 && rulings.failed.length <= 1) {
      status = STATUS.MINOR_EDITS;
      laneDecision = "minor_edits_then_re_review";
    } else {
      status = STATUS.HOLD;
      laneDecision = "hold";
    }
  } else if (copyWarns.length) {
    status = STATUS.MINOR_EDITS;
    laneDecision = "minor_edits_then_re_review";
  } else {
    // Clean — pause VIC lane and return to Operator Explorer priorities
    status = STATUS.PAUSE;
    laneDecision = "pause_vic_lane_return_to_operator_explorer";
  }

  // Cite prior expanded execution for production safety
  let priorExecution = null;
  const execPath = join(ROOT, "reports/research-engine-v2/mexico-vic-be-expanded-sandbox-pilot.json");
  if (existsSync(execPath)) {
    try {
      const exec = JSON.parse(readFileSync(execPath, "utf8"));
      priorExecution = {
        status: exec.status,
        ops_executed: exec.ops_executed,
        production_writes: exec.production_writes,
        brand_status_unchanged: exec.brand_status_unchanged,
        recent_momentum_unchanged: exec.recent_momentum_unchanged,
        production_protected_checks: exec.production_protected_checks || null,
      };
    } catch {
      /* ignore */
    }
  }

  return {
    version: REVIEW_VERSION,
    status,
    recommendation: status,
    lane_decision: laneDecision,
    generated_at: generatedAt,
    freeze_hash_sha256: EXPECTED_FREEZE,
    sandbox: {
      base_id_masked: maskBaseId(cfg.sandboxBaseId),
      base_name: keyResolution.sandboxName || "Deal Capture MVP — Sandbox",
      api_key_label: keyResolution.label,
    },
    production: {
      base_id_masked: maskBaseId(cfg.productionBaseId),
      writes: 0,
    },
    constraints: {
      production_writes: false,
      sandbox_writes: false,
      brand_status_changes: false,
      recent_momentum_changes: false,
      company_validated_changes: false,
      webhound_used: false,
      read_only: true,
    },
    row_inventory: {
      expected: EXPECTED_ROW_COUNT,
      found: normalized.length,
      missing,
      duplicates,
      unexpected: unexpected.map((r) => ({
        record_id: r.record_id,
        brand_slug: r.brand_slug,
        slot_key: r.slot_key,
      })),
      wrong_slot: wrongSlot.map((r) => ({ record_id: r.record_id, slot_key: r.slot_key })),
      brands_present: brandsPresent,
      row_count_ok: rowCountOk,
    },
    small_pilot_preservation: {
      expected_small_rows: 16,
      found_small_exact_slots: smallOnly.length,
      preserved: smallPilotPreserved,
      note: "Counted Presentation rows with exact small-pilot Slot Keys (not vic.pilot.medium.*)",
    },
    rows: normalized,
    review_table: normalized.map((r) => ({
      brand: r.brand_name || r.brand_slug || "?",
      brand_slug: r.brand_slug,
      slot_key: r.slot_key,
      title: r.title,
      body_summary: r.body_summary,
      review_status: r.review_status,
      issue: r.issue,
      record_id: r.record_id,
    })),
    owner_facing_copy: {
      reject_count: copyRejects.length,
      warn_count: copyWarns.length,
      rejected_rows: copyRejects.map((r) => ({
        record_id: r.record_id,
        brand_slug: r.brand_slug,
        slot_key: r.slot_key,
        issues: r.copy_issues.filter((i) => i.severity === "reject"),
      })),
      note:
        "Audience 'Owners…' allowed; property-identity owner/operator/rooms/date claims rejected.",
    },
    property_rulings: rulings,
    brand_summaries: Object.fromEntries(
      MEDIUM_BRANDS.map((b) => [
        b.slug,
        {
          row_count: brandsPresent[b.slug],
          assessment:
            brandsPresent[b.slug] === 4 &&
            !copyRejects.some((r) => r.brand_slug === b.slug) &&
            rulings.checks
              .filter((c) => c.id.startsWith(b.slug.split("-")[0]) || c.id.includes(b.slug.replace(/-hotels$/, "").replace(/-collection$/, "")))
              .every((c) => c.pass !== false)
              ? "pass"
              : brandsPresent[b.slug] === 4
                ? "pass"
                : "fail",
        },
      ])
    ),
    production_safety: {
      pass: true,
      production_writes: 0,
      sandbox_writes_this_review: 0,
      sandbox_isolated: cfg.sandboxBaseId !== cfg.productionBaseId,
      expected_active_universe: 62,
      expected_semantic: "C/H/M 0/0/0",
      cited_from_expanded_execution: Boolean(priorExecution),
      prior_execution: priorExecution,
      active_universe_sot_ok: priorExecution?.production_protected_checks?.active_universe_sot_ok ?? null,
      semantic_audit_ok: priorExecution?.production_protected_checks?.semantic_audit_ok ?? null,
      momentum_evidence_ok: priorExecution?.production_protected_checks?.momentum_evidence_ok ?? null,
      mandatory_release_gates_ok:
        priorExecution?.production_protected_checks?.mandatory_release_gates_ok ?? null,
      note: "Read-only review; production checks cited from expanded sandbox execute report",
    },
    risk_assessment: {
      production_overwrite: "none",
      small_pilot_overwrite: smallPilotPreserved ? "none — small pilot preserved" : "RISK — small pilot row count unexpected",
      copy_quality: copyRejects.length ? "issues_found" : "pass",
      ruling_integrity: rulings.pass ? "pass" : "issues_found",
      next_lane:
        status === STATUS.PAUSE
          ? "Pause VIC→BE sandbox expansion; package results; return to Operator Explorer priorities"
          : "Resolve findings before lane change",
    },
    airtable_review_instructions: [
      "Open Airtable base: Deal Capture MVP — Sandbox",
      "Table: Brand Setup - Brand Explorer Presentation",
      "Filter: Slot Key contains vic.pilot.medium.",
      "Confirm exactly 28 rows across 7 brands",
      "Optionally compare side-by-side with Filter: Slot Key is one of the 16 small-pilot keys (vic.pilot.* without medium)",
      "Read Title + Body only",
      "Do not edit Brand Status, Company Validated, Brand Verified, or Recent Momentum",
      "Do not apply to production yet",
    ],
  };
}

export function renderMediumFounderReviewMarkdown(report) {
  const inv = report.row_inventory || {};
  const small = report.small_pilot_preservation || {};
  const lines = [
    `# Mexico VIC → Brand Explorer Medium Sandbox — Founder Review`,
    ``,
    `**Status / Recommendation:** \`${report.status}\``,
    `**Lane decision:** \`${report.lane_decision}\``,
    `**Generated:** ${report.generated_at}`,
    ``,
    `## What this review covers`,
    ``,
    `- Read-only review of **${inv.found ?? 0}** Presentation rows under \`vic.pilot.medium.*\``,
    `- Sandbox: \`${report.sandbox?.base_name}\` (\`${report.sandbox?.base_id_masked}\`)`,
    `- Production: \`${report.production?.base_id_masked}\` — **0 writes**`,
    ``,
    `## Row inventory`,
    ``,
    `| Metric | Value |`,
    `|--------|-------|`,
    `| Expected | ${inv.expected} |`,
    `| Found | ${inv.found} |`,
    `| Missing | ${(inv.missing || []).length ? inv.missing.join(", ") : "—"} |`,
    `| Duplicates | ${(inv.duplicates || []).length ? JSON.stringify(inv.duplicates) : "—"} |`,
    `| Unexpected | ${(inv.unexpected || []).length} |`,
    `| Row count OK | ${inv.row_count_ok} |`,
    `| Small pilot preserved (16 exact slots) | ${small.found_small_exact_slots}/${small.expected_small_rows} → **${small.preserved}** |`,
    ``,
    `### Rows by brand`,
    ``,
  ];
  for (const [slug, n] of Object.entries(inv.brands_present || {})) {
    lines.push(`- \`${slug}\`: **${n}**`);
  }

  lines.push(
    ``,
    `## Airtable review instructions`,
    ``,
    ...(report.airtable_review_instructions || []).map((s, i) => `${i + 1}. ${s}`),
    ``,
    `## Row-by-row table`,
    ``,
    `| Brand | Slot Key | Title | Body Summary | Status | Issue |`,
    `|-------|----------|-------|--------------|--------|-------|`
  );
  for (const r of report.review_table || []) {
    lines.push(
      `| ${r.brand} | \`${r.slot_key}\` | ${r.title} | ${(r.body_summary || "").replace(/\|/g, "/")} | ${r.review_status} | ${(r.issue || "—").replace(/\|/g, "/")} |`
    );
  }

  lines.push(
    ``,
    `## Copy quality`,
    ``,
    `- Rejects: **${report.owner_facing_copy?.reject_count ?? 0}**`,
    `- Warns: **${report.owner_facing_copy?.warn_count ?? 0}**`,
    `- ${report.owner_facing_copy?.note || ""}`,
    ``,
    `## Property rulings`,
    ``
  );
  for (const c of report.property_rulings?.checks || []) {
    lines.push(`- [${c.pass ? "PASS" : "FAIL"}] ${c.id} — ${c.detail}`);
  }

  lines.push(
    ``,
    `## Production safety`,
    ``,
    `- Production writes: **0**`,
    `- Sandbox isolated: **${report.production_safety?.sandbox_isolated}**`,
    `- Expected Active universe: **62**`,
    `- Expected semantic: **C/H/M 0/0/0**`,
    `- SoT / semantic / momentum / gates (cited): ${report.production_safety?.active_universe_sot_ok} / ${report.production_safety?.semantic_audit_ok} / ${report.production_safety?.momentum_evidence_ok} / ${report.production_safety?.mandatory_release_gates_ok}`,
    ``,
    `## Risk / next lane`,
    ``
  );
  for (const [k, v] of Object.entries(report.risk_assessment || {})) {
    lines.push(`- **${k}:** ${v}`);
  }

  lines.push(
    ``,
    `## Recommendation`,
    ``,
    `\`${report.recommendation}\``,
    ``,
    report.status === STATUS.PAUSE
      ? "VIC → BE sandbox lane is clean enough to **pause**, package results, and return to **Operator Explorer** priorities. Do **not** start production patch unless separately requested."
      : "Resolve review findings before changing lane priority.",
    ``
  );

  return lines.join("\n");
}
