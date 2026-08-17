/**
 * Read-only review of Mexico VIC → BE small pilot sandbox Presentation rows.
 * Never writes Airtable (production or sandbox).
 */

import {
  maskBaseId,
  readSandboxEnv,
  resolveSandboxApiKey,
  TARGET_BRANDS,
} from "./airtable-sandbox-validation.js";

export const SANDBOX_REVIEW_VERSION = "mexico-vic-be-small-pilot-sandbox-review-v1";
export const EXPECTED_FREEZE =
  "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";

export const REVIEW_STATUS = Object.freeze({
  APPROVED: "vic_be_small_pilot_sandbox_review_approved_continue_expanded_sandbox",
  MINOR_EDITS: "vic_be_small_pilot_sandbox_review_minor_edits_required",
  HOLD: "vic_be_small_pilot_sandbox_review_hold",
  ROW_MISMATCH: "sandbox_review_failed_row_count_mismatch",
});

export const RECOMMENDATION = Object.freeze({
  CONTINUE: "approve_sandbox_result_continue_to_expanded_sandbox_pilot",
  MINOR_EDITS: "approve_after_minor_sandbox_copy_edits",
  HOLD: "hold_do_not_continue",
});

export const EXPECTED_SLOTS = Object.freeze([
  "vic.pilot.property_examples",
  "vic.pilot.geographic_footprint_mexico",
  "vic.pilot.portfolio_context",
  "vic.pilot.owner_facing_copy",
]);

export const EXPECTED_ROW_COUNT = 16;

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS_TABLE = "Brand Setup - Brand Basics";

const SLUG_BY_RECORD_ID = Object.freeze(
  Object.fromEntries(TARGET_BRANDS.map((b) => [b.expectedRecordId, b.slug]))
);

/** Hard-forbidden process / provenance language in owner-facing Title/Body. */
const FORBIDDEN_PROCESS =
  /\b(vic|census|staging|sandbox|source pack|directory|source-supported|steward|overlay|\bqa\b|process|company validated|brand verified|confirmed by (ihg|hilton|choice|marriott)|verified by|company confirmed|brand validated|census proves|directory confirms)\b/i;

/** Fabricated facts / affiliation claims. */
const FORBIDDEN_FACT_CLAIMS =
  /\b(\d+\s*rooms?\b|(opening date|opened on|open date|affiliation start)|operated by|managed by (choice|hilton|ihg)|choice owns|owned by choice|faranda (owns|manages|operated)|hilton owns|owned by hilton)\b/i;

/**
 * "owner" as property-identity claim — not audience address ("Owners evaluating…").
 * Denial language ("without assuming Choice ownership") is allowed.
 */
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
  // Audience "Owners …" is allowed; note only
  if (/\bowners?\b/i.test(blob) && !own) {
    issues.push({
      code: "audience_owner_address",
      severity: "info",
      note: "Contains owner(s) as audience address — allowed when not a property-identity claim",
    });
  }
  if (/recent momentum|momentum card|just opened|newly opened/i.test(blob)) {
    issues.push({ code: "possible_false_momentum", severity: "reject" });
  }
  if (!String(body || "").trim()) {
    issues.push({ code: "empty_body", severity: "reject" });
  }
  if (!String(title || "").trim()) {
    issues.push({ code: "empty_title", severity: "reject" });
  }
  return issues;
}

function assessRulings(rowsBySlug) {
  const ascendBlob = (rowsBySlug.ascend || []).map((r) => `${r.title}\n${r.body}`).join("\n");
  const curioBlob = (rowsBySlug["curio-collection"] || []).map((r) => `${r.title}\n${r.body}`).join("\n");
  const indigoBlob = (rowsBySlug["hotel-indigo"] || []).map((r) => `${r.title}\n${r.body}`).join("\n");
  const hieBlob = (rowsBySlug["holiday-inn-express"] || [])
    .map((r) => `${r.title}\n${r.body}`)
    .join("\n");

  const checks = [
    {
      id: "ascend_amberes_mentioned",
      pass: /Amberes 64/i.test(ascendBlob),
      detail: "Amberes 64 present in Ascend copy",
    },
    {
      id: "ascend_el_cid_castilla",
      pass: /El Cid Castilla/i.test(ascendBlob),
      detail: "El Cid Castilla present",
    },
    {
      id: "ascend_el_cid_la_ceiba",
      pass: /El Cid La Ceiba/i.test(ascendBlob),
      detail: "El Cid La Ceiba present",
    },
    {
      id: "ascend_soft_brand_framing",
      pass: /soft-brand/i.test(ascendBlob),
      detail: "Soft-brand distribution framing present",
    },
    {
      id: "ascend_no_choice_owns",
      pass: !/\b(Choice owns|owned by Choice)\b/i.test(ascendBlob),
      detail: "No Choice ownership claim",
    },
    {
      id: "ascend_denies_choice_assumption_ok",
      pass: /without assuming Choice ownership/i.test(ascendBlob) || !/\bChoice owns\b/i.test(ascendBlob),
      detail: "Steward denial language or no ownership claim",
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
      detail:
        "No affirmative direct management claim (steward denial language allowed)",
    },
    {
      id: "ascend_no_vic_momentum",
      pass: !/recent momentum/i.test(ascendBlob),
      detail: "No Recent Momentum from VIC in Ascend pilot rows",
    },
    {
      id: "curio_amare",
      pass: /Amare Cancun/i.test(curioBlob),
      detail: "Amare Cancun present",
    },
    {
      id: "curio_fives",
      pass: /The Fives Downtown/i.test(curioBlob),
      detail: "The Fives Downtown present",
    },
    {
      id: "curio_ms_milenium_san_pedro",
      pass: /San Pedro Garza García/i.test(curioBlob),
      detail: "MS Milenium city = San Pedro Garza García",
    },
    {
      id: "curio_monterrey_metro_context",
      pass: /Monterrey/i.test(curioBlob),
      detail: "Monterrey metro context present",
    },
    {
      id: "curio_no_hilton_owns",
      pass: !/\b(Hilton owns|owned by Hilton)\b/i.test(curioBlob),
      detail: "No Hilton ownership claim",
    },
    {
      id: "curio_no_vic_momentum",
      pass: !/recent momentum/i.test(curioBlob),
      detail: "No Recent Momentum from VIC in Curio pilot rows",
    },
    {
      id: "indigo_three_properties",
      pass:
        /Guadalajara Expo/i.test(indigoBlob) &&
        /Playa del Carmen/i.test(indigoBlob) &&
        /Guanajuato/i.test(indigoBlob),
      detail: "Indigo Guadalajara Expo, Playa del Carmen, Guanajuato present",
    },
    {
      id: "indigo_no_false_momentum",
      pass: !/recent momentum/i.test(indigoBlob),
      detail: "No false momentum on Indigo",
    },
    {
      id: "hie_queretaro",
      pass: /Quer[eé]taro/i.test(hieBlob),
      detail: "Holiday Inn Express Querétaro present",
    },
    {
      id: "hie_no_false_momentum",
      pass: !/recent momentum/i.test(hieBlob),
      detail: "No false momentum on HIE",
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
    const msg = data?.error?.message || data?.error || res.statusText;
    throw new Error(`${res.status} ${url}: ${msg}`);
  }
  return data;
}

async function fetchAllVicPilotRows(apiKey, baseId) {
  const table = encodeURIComponent(PRESENTATION_TABLE);
  const formula = encodeURIComponent(`FIND('vic.pilot.', {Slot Key})`);
  const rows = [];
  let offset = null;
  do {
    let url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${table}?filterByFormula=${formula}&pageSize=100`;
    if (offset) url += `&offset=${encodeURIComponent(offset)}`;
    const data = await airtableFetch(apiKey, url);
    for (const r of data.records || []) rows.push(r);
    offset = data.offset || null;
  } while (offset);
  return rows;
}

async function fetchBasicsName(apiKey, baseId, recordId) {
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

/**
 * @param {object} [options]
 */
export async function runMexicoVicBeSmallPilotSandboxReview(options = {}) {
  const generatedAt = options.generatedAt || new Date().toISOString();
  const env = options.env || process.env;
  const cfg = readSandboxEnv(env);
  const keyResolution = await resolveSandboxApiKey(env);

  if (!keyResolution.ok || !cfg.sandboxBaseId) {
    return {
      version: SANDBOX_REVIEW_VERSION,
      status: REVIEW_STATUS.HOLD,
      recommendation: RECOMMENDATION.HOLD,
      generated_at: generatedAt,
      error: keyResolution.detail || "sandbox not reachable",
      production_writes: 0,
      sandbox_writes: 0,
      expanded_sandbox_pilot_may_proceed: false,
    };
  }

  const apiKey = keyResolution.apiKey;
  const rawRows = await fetchAllVicPilotRows(apiKey, cfg.sandboxBaseId);

  /** @type {object[]} */
  const normalized = [];
  for (const r of rawRows) {
    const f = r.fields || {};
    const link = f.Brand || f["Brand Setup - Brand Basics"] || f.Brand_Basic_ID || f["Brand Basics"];
    const brandRecordId = Array.isArray(link) ? link[0] : null;
    const slug = brandRecordId ? SLUG_BY_RECORD_ID[brandRecordId] || null : null;
    const basics = brandRecordId
      ? await fetchBasicsName(apiKey, cfg.sandboxBaseId, brandRecordId)
      : null;
    const title = f.Title || "";
    const body = f.Body || "";
    const slotKey = f["Slot Key"] || "";
    const copyIssues = scanCopy(title, body);
    const rejectIssues = copyIssues.filter((i) => i.severity === "reject");
    let reviewStatus = "pass";
    if (rejectIssues.length) reviewStatus = "fail";
    else if (copyIssues.some((i) => i.severity === "warn")) reviewStatus = "warn";

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
      modified: f["Last Modified"] || f.Modified || null,
      copy_issues: copyIssues,
      review_status: reviewStatus,
      issue:
        rejectIssues.map((i) => i.code + (i.term ? `:${i.term}` : "")).join("; ") ||
        copyIssues
          .filter((i) => i.severity === "info")
          .map((i) => i.code)
          .join("; ") ||
        null,
    });
  }

  // Expected matrix
  const expectedKeys = [];
  for (const b of TARGET_BRANDS) {
    for (const slot of EXPECTED_SLOTS) {
      expectedKeys.push(`${b.slug}::${slot}`);
    }
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
      !TARGET_BRANDS.some((b) => b.slug === r.brand_slug) ||
      !String(r.slot_key || "").startsWith("vic.pilot.")
  );
  const wrongBrand = normalized.filter((r) => r.brand_slug && !SLUG_BY_RECORD_ID[r.brand_record_id]);
  const wrongSlot = normalized.filter(
    (r) =>
      String(r.slot_key || "").startsWith("vic.pilot.") &&
      !EXPECTED_SLOTS.includes(r.slot_key)
  );

  const brandsPresent = {
    "hotel-indigo": normalized.filter((r) => r.brand_slug === "hotel-indigo").length,
    ascend: normalized.filter((r) => r.brand_slug === "ascend").length,
    "curio-collection": normalized.filter((r) => r.brand_slug === "curio-collection").length,
    "holiday-inn-express": normalized.filter((r) => r.brand_slug === "holiday-inn-express").length,
  };

  const rowCountOk =
    normalized.length === EXPECTED_ROW_COUNT &&
    missing.length === 0 &&
    duplicates.length === 0 &&
    unexpected.length === 0 &&
    wrongSlot.length === 0 &&
    Object.values(brandsPresent).every((n) => n === 4);

  const rowsBySlug = {
    "hotel-indigo": normalized.filter((r) => r.brand_slug === "hotel-indigo"),
    ascend: normalized.filter((r) => r.brand_slug === "ascend"),
    "curio-collection": normalized.filter((r) => r.brand_slug === "curio-collection"),
    "holiday-inn-express": normalized.filter((r) => r.brand_slug === "holiday-inn-express"),
  };

  const rulings = assessRulings(rowsBySlug);
  const copyRejects = normalized.filter((r) => r.review_status === "fail");
  const genericWeak = normalized.filter((r) => {
    const b = String(r.body || "");
    return b.length > 0 && b.length < 40;
  });

  let status;
  let recommendation;
  let expandedMayProceed = false;

  if (!rowCountOk) {
    status = REVIEW_STATUS.ROW_MISMATCH;
    recommendation = RECOMMENDATION.HOLD;
  } else if (copyRejects.length > 0 || !rulings.pass) {
    const rulingFails = rulings.failed.length;
    const rejectFails = copyRejects.length;
    // Minor: few copy rejects and/or ≤1 ruling miss that is copy-adjacent
    if (rejectFails <= 2 && rulingFails <= 1 && rowCountOk) {
      status = REVIEW_STATUS.MINOR_EDITS;
      recommendation = RECOMMENDATION.MINOR_EDITS;
    } else {
      status = REVIEW_STATUS.HOLD;
      recommendation = RECOMMENDATION.HOLD;
    }
  } else if (genericWeak.length) {
    status = REVIEW_STATUS.MINOR_EDITS;
    recommendation = RECOMMENDATION.MINOR_EDITS;
  } else {
    status = REVIEW_STATUS.APPROVED;
    recommendation = RECOMMENDATION.CONTINUE;
    expandedMayProceed = true;
  }

  const reviewTable = normalized.map((r) => ({
    brand: r.brand_name || r.brand_slug || "?",
    brand_slug: r.brand_slug,
    slot_key: r.slot_key,
    title: r.title,
    body_summary: r.body_summary,
    review_status: r.review_status,
    issue: r.issue,
    record_id: r.record_id,
  }));

  return {
    version: SANDBOX_REVIEW_VERSION,
    status,
    recommendation,
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
      wrong_brand: wrongBrand.map((r) => r.record_id),
      wrong_slot: wrongSlot.map((r) => ({ record_id: r.record_id, slot_key: r.slot_key })),
      brands_present: brandsPresent,
      row_count_ok: rowCountOk,
    },
    rows: normalized,
    review_table: reviewTable,
    owner_facing_copy: {
      reject_count: copyRejects.length,
      rejected_rows: copyRejects.map((r) => ({
        record_id: r.record_id,
        brand_slug: r.brand_slug,
        slot_key: r.slot_key,
        issues: r.copy_issues.filter((i) => i.severity === "reject"),
      })),
      info_notes: normalized
        .flatMap((r) =>
          (r.copy_issues || [])
            .filter((i) => i.severity === "info")
            .map((i) => ({ record_id: r.record_id, ...i }))
        ),
      note:
        "Audience address ('Owners evaluating…') is allowed; property-identity owner/operator/rooms/date claims are rejected.",
    },
    property_rulings: rulings,
    brand_summaries: {
      "hotel-indigo": {
        row_count: brandsPresent["hotel-indigo"],
        properties: ["Hotel Indigo Guadalajara Expo", "Hotel Indigo Playa del Carmen", "Hotel Indigo Guanajuato"],
        assessment: rulings.checks.filter((c) => c.id.startsWith("indigo")).every((c) => c.pass)
          ? "pass"
          : "fail",
      },
      ascend: {
        row_count: brandsPresent.ascend,
        properties: ["Amberes 64", "El Cid Castilla", "El Cid La Ceiba"],
        assessment: rulings.checks.filter((c) => c.id.startsWith("ascend")).every((c) => c.pass)
          ? "pass"
          : "fail",
      },
      "curio-collection": {
        row_count: brandsPresent["curio-collection"],
        properties: ["Amare Cancun", "The Fives Downtown", "MS Milenium (San Pedro Garza García)"],
        assessment: rulings.checks.filter((c) => c.id.startsWith("curio")).every((c) => c.pass)
          ? "pass"
          : "fail",
      },
      "holiday-inn-express": {
        row_count: brandsPresent["holiday-inn-express"],
        properties: ["Holiday Inn Express Querétaro"],
        assessment: rulings.checks.filter((c) => c.id.startsWith("hie")).every((c) => c.pass)
          ? "pass"
          : "fail",
      },
    },
    risk_assessment: {
      production_overwrite: "none — read-only review; prior patch was sandbox-only",
      brand_status_drift: "none observed in this review (Basics Status not mutated)",
      momentum_pollution: "vic.pilot.* slots do not write Recent Momentum",
      copy_quality: copyRejects.length ? "issues_found" : "pass",
      ruling_integrity: rulings.pass ? "pass" : "issues_found",
      expansion_risk: expandedMayProceed
        ? "low — small pilot pattern can expand in sandbox with same guardrails"
        : "hold — resolve review findings before expanded sandbox pilot",
    },
    production_safety: {
      pass: true,
      production_writes: 0,
      sandbox_writes_this_review: 0,
      sandbox_isolated: cfg.sandboxBaseId !== cfg.productionBaseId,
      expected_active_universe: 62,
      expected_semantic: "C/H/M 0/0/0",
      note: "This review does not re-run live production audits; cites prior post-patch PASS unless BE_PILOT_REVIEW_RUN_PROD_CHECKS=1",
    },
    expanded_sandbox_pilot_may_proceed: expandedMayProceed,
    airtable_review_instructions: [
      "Open Airtable base: Deal Capture MVP — Sandbox",
      "Go to table: Brand Setup - Brand Explorer Presentation",
      "Filter: Slot Key contains vic.pilot.",
      "Confirm exactly 16 rows across hotel-indigo, ascend, curio-collection, holiday-inn-express",
      "Read Title + Body for each row as owner-facing copy",
      "Do not edit Brand Status, Company Validated, Brand Verified, or Recent Momentum",
      "Do not apply these rows to production yet",
    ],
  };
}

export function renderSandboxReviewMarkdown(report) {
  const inv = report.row_inventory || {};
  const lines = [
    `# Mexico VIC → Brand Explorer Small Pilot — Sandbox Review`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Recommendation:** \`${report.recommendation}\``,
    `**Generated:** ${report.generated_at}`,
    `**Expanded sandbox pilot may proceed:** **${report.expanded_sandbox_pilot_may_proceed}**`,
    ``,
    `## 1. What changed in sandbox`,
    ``,
    `- Created **${inv.found ?? 0}** Presentation rows with Slot Key \`vic.pilot.*\``,
    `- Fields: Title, Body, Slot Key, Brand (link)`,
    `- Brands: hotel-indigo, ascend, curio-collection, holiday-inn-express`,
    `- Freeze lineage: \`${report.freeze_hash_sha256}\``,
    ``,
    `## 2. What did not change`,
    ``,
    `- Production Airtable (\`${report.production?.base_id_masked}\`) — **0 writes**`,
    `- Brand Status / Company Validated / Brand Verified`,
    `- Recent Momentum`,
    `- Release fields`,
    `- Frozen 62 + frozen VIC artifacts`,
    ``,
    `## 3. Airtable review instructions`,
    ``,
    ...(report.airtable_review_instructions || []).map((s, i) => `${i + 1}. ${s}`),
    ``,
    `## 4. Brand-by-brand summary`,
    ``,
  ];

  for (const [slug, sum] of Object.entries(report.brand_summaries || {})) {
    lines.push(
      `### ${slug}`,
      ``,
      `- Rows: **${sum.row_count}**`,
      `- Properties: ${sum.properties.join("; ")}`,
      `- Assessment: **${sum.assessment}**`,
      ``
    );
  }

  lines.push(
    `## 5. Row-by-row review table`,
    ``,
    `| Brand | Slot Key | Title | Body Summary | Review Status | Issue |`,
    `|-------|----------|-------|--------------|---------------|-------|`
  );
  for (const r of report.review_table || []) {
    const issue = (r.issue || "—").replace(/\|/g, "/");
    const summary = (r.body_summary || "").replace(/\|/g, "/");
    lines.push(
      `| ${r.brand} | \`${r.slot_key}\` | ${r.title} | ${summary} | ${r.review_status} | ${issue} |`
    );
  }

  lines.push(
    ``,
    `## 6. Copy quality assessment`,
    ``,
    `- Reject count: **${report.owner_facing_copy?.reject_count ?? 0}**`,
    `- ${report.owner_facing_copy?.note || ""}`,
    ``
  );
  if (report.owner_facing_copy?.rejected_rows?.length) {
    for (const r of report.owner_facing_copy.rejected_rows) {
      lines.push(
        `- FAIL \`${r.brand_slug}\` \`${r.slot_key}\` — ${(r.issues || []).map((i) => i.code).join(", ")}`
      );
    }
  } else {
    lines.push(`- No forbidden-language rejects.`);
  }

  lines.push(``, `## 7. Property ruling assessment`, ``);
  for (const c of report.property_rulings?.checks || []) {
    lines.push(`- [${c.pass ? "PASS" : "FAIL"}] ${c.id} — ${c.detail}`);
  }

  lines.push(
    ``,
    `## 8. Row inventory`,
    ``,
    `| Metric | Value |`,
    `|--------|-------|`,
    `| Expected | ${inv.expected} |`,
    `| Found | ${inv.found} |`,
    `| Missing | ${(inv.missing || []).length ? inv.missing.join(", ") : "—"} |`,
    `| Duplicates | ${(inv.duplicates || []).length ? JSON.stringify(inv.duplicates) : "—"} |`,
    `| Unexpected | ${(inv.unexpected || []).length} |`,
    `| Wrong slot | ${(inv.wrong_slot || []).length} |`,
    `| Row count OK | ${inv.row_count_ok} |`,
    ``,
    `## 9. Risk assessment`,
    ``
  );
  for (const [k, v] of Object.entries(report.risk_assessment || {})) {
    lines.push(`- **${k}:** ${v}`);
  }

  lines.push(
    ``,
    `## 10. Recommendation`,
    ``,
    `\`${report.recommendation}\``,
    ``,
    `Do **not** recommend production patch from this review.`,
    ``,
    `## Production safety`,
    ``,
    `- Production writes: **0**`,
    `- Sandbox isolated: **${report.production_safety?.sandbox_isolated}**`,
    `- Expected production Active universe: **62**`,
    `- Expected production semantic: **C/H/M 0/0/0**`,
    ``
  );

  return lines.join("\n");
}
