/**
 * Global Active — High-severity semantic cleanup (Batch 1).
 *
 * Scope: lifecycle/internal language, portfolio-mix prose, weak Recent Momentum
 * semantics, and obvious High-only phrase fixes. Uses the refreshed global
 * semantic audit as source of truth.
 *
 * Forbidden: Brand Status, release, CV, Source Library, Registry, images,
 * Four Points Flex / House of Originals / Morgans / Radisson Collection,
 * broad rewrites, baseline freeze.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { isOwnerFacingPresentationRow } from "./brand-explorer-public-visibility-quality-lock.js";
import { getWave13ActiveIdentityBySlug } from "./brand-explorer-wave13-active-identity-anchors.js";
import {
  EXPECTED_ACTIVE_UNIVERSE_COUNT,
  EXCLUDED_FROM_ACTIVE_SEMANTIC_AUDIT,
} from "./brand-explorer-global-active-semantic-audit.js";

export const GLOBAL_HIGH_SEVERITY_CLEANUP_VERSION =
  "global-high-severity-cleanup-batch1-v1";

export const GLOBAL_HIGH_SEVERITY_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-global-high-severity-cleanup-batch1",
  "--confirm-fresh-global-audit-used",
  "--confirm-high-severity-only",
  "--confirm-targeted-visible-copy-only",
  "--confirm-no-critical-regression",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-image-writes",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-portfolio-mix-structured",
  "--confirm-recent-momentum-semantics-preserved",
  "--confirm-no-internal-lifecycle-language",
  "--confirm-no-gate-weakening",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 320;
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const NEVER_WRITE_BRANDS = new Set(
  EXCLUDED_FROM_ACTIVE_SEMANTIC_AUDIT.map((s) => s.toLowerCase())
);

const BATCH1_CLASSES = new Set([
  "lifecycle_internal_language",
  "portfolio_mix_prose",
  "recent_momentum_weak_semantics",
  "value_scenario_generic_or_repetitive",
]);

const OWNER_VALUE_CLOSING =
  "This brand is strongest when the ownership team can support the physical product, service model, and operating expectations that make the affiliation credible.";

/** Curated sample mixes — illustrative, not disclosed portfolio census. */
const PORTFOLIO_MIX_BY_SLUG = Object.freeze({
  "everhome-suites": {
    title: "Curated sample mix",
    body: [
      "Suburban / Highway corridor: 45%",
      "Secondary / Regional: 25%",
      "Urban fringe / Employment: 20%",
      "Conversion / Adaptive: 10%",
      "Curated sample mix based on brand positioning (illustrative, not a disclosed portfolio census).",
    ].join("\n"),
  },
  "fairmont-hotels-and-resorts": {
    title: "Curated sample mix",
    body: [
      "Urban / Gateway: 40%",
      "Resort / Leisure: 35%",
      "Mixed-use / Landmark: 15%",
      "Secondary destination: 10%",
      "Curated sample mix based on global brand positioning (illustrative, not a disclosed portfolio census).",
    ].join("\n"),
  },
  ibis: {
    title: "Curated sample mix",
    body: [
      "Urban / Gateway: 35%",
      "Airport / Highway: 30%",
      "Secondary / Regional: 25%",
      "Suburban: 10%",
      "Curated sample mix based on global brand positioning (illustrative, not a disclosed portfolio census).",
    ].join("\n"),
  },
  "mama-shelter": {
    title: "Curated sample mix",
    body: [
      "Urban / Lifestyle district: 55%",
      "Gateway / Secondary city: 25%",
      "Resort-adjacent / Leisure: 10%",
      "Other: 10%",
      "Curated sample mix based on brand positioning (illustrative, not a disclosed portfolio census).",
    ].join("\n"),
  },
  mercure: {
    title: "Curated sample mix",
    body: [
      "Urban / Gateway: 40%",
      "Secondary / Regional: 25%",
      "Airport / Highway: 20%",
      "Resort / Leisure: 15%",
      "Curated sample mix based on global brand positioning (illustrative, not a disclosed portfolio census).",
    ].join("\n"),
  },
  novotel: {
    title: "Curated sample mix",
    body: [
      "Urban / Gateway: 40%",
      "Airport / Convention: 25%",
      "Resort / Leisure: 20%",
      "Secondary / Regional: 15%",
      "Curated sample mix based on global brand positioning (illustrative, not a disclosed portfolio census).",
    ].join("\n"),
  },
  pullman: {
    title: "Curated sample mix",
    body: [
      "Urban / Gateway: 45%",
      "Resort / Destination: 25%",
      "Airport / Convention: 15%",
      "Secondary / Mixed-use: 15%",
      "Curated sample mix based on global brand positioning (illustrative, not a disclosed portfolio census).",
    ].join("\n"),
  },
  "so-hotels-and-resorts": {
    title: "Curated sample mix",
    body: [
      "Urban / Lifestyle: 50%",
      "Resort / Destination: 30%",
      "Landmark / Mixed-use: 15%",
      "Secondary: 5%",
      "Curated sample mix based on brand positioning (illustrative, not a disclosed portfolio census).",
    ].join("\n"),
  },
});

const MAMA_SHELTER_MOMENTUM_PATCH = Object.freeze({
  recordId: "recu3NAAx3QbAfjfO",
  // Prefer hide: a stronger Mexico City pipeline card already exists (recTGxtIVd9vfa0Qz).
  hide: true,
  title: "Mama Shelter Mexico City Listed As Accor ALL Pipeline",
  body: [
    "Pipeline · CALA · end of 2026",
    "",
    "Accor ALL lists Mama Shelter Mexico City among upcoming openings—CALA pipeline proof for the lifestyle brand ahead of debut. Treat as pipeline, not operating inventory, until the hotel opens.",
    "",
    "https://all.accor.com/hotel/C4I1/index.en.shtml",
  ].join("\n"),
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function checkFlags(required, argv, apply) {
  const missing = required.filter((f) => !argv.includes(f));
  return { apply: apply === true, ok: apply === true && missing.length === 0, missing, required: [...required] };
}

async function airtablePatch(baseId, apiKey, table, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`PATCH ${recordId} → ${res.status}: ${text.slice(0, 300)}`);
  }
  return res.json();
}

export function classifyHighFailureType(failureType, section) {
  const ft = nz(failureType);
  const sec = nz(section);
  if (
    /confirm_owner|confirm_operator|keep_sibling|do_not_reuse|avoid_borrowing|directory_card|governance|qa|fee_stack/i.test(
      ft
    ) ||
    sec === "Internal Language"
  ) {
    if (/fee_stack/i.test(ft)) return "other_high";
    return "lifecycle_internal_language";
  }
  if (ft === "prose_market_note" || sec === "Portfolio Mix") return "portfolio_mix_prose";
  if (
    /momentum|brand_page_as_momentum|development_page|title_is_source_note|directory_as_momentum/i.test(ft) ||
    sec === "Recent Momentum"
  ) {
    return "recent_momentum_weak_semantics";
  }
  if (/generic_bonvoy|valueOwners\.scenario|scenario_|value.?scenario/i.test(ft) || /Value Creation|Where This Brand/i.test(sec)) {
    return "value_scenario_generic_or_repetitive";
  }
  if (/openings|archetype|generic_brand_reference/i.test(ft) || sec === "Openings") {
    return "openings_property_semantic_weakness";
  }
  return "other_high";
}

export function sanitizeHighLifecycleOwnerFacingText(text) {
  let s = String(text || "");

  // Boilerplate confirm-owner closing (including mangled multi-sentence Fairmont variants)
  s = s.replace(
    /\s*Confirm owner,\s*operator,\s*and brand responsibilities for[\s\S]*?deliverable after affiliation(?:\s+and through ongoing operations)?\.?/gi,
    ` ${OWNER_VALUE_CLOSING}`
  );

  s = s.replace(
    /\s*Confirm owner versus operator reporting responsibilities[^.]*\./gi,
    " Owners should clarify reporting and quality obligations with their operator before affiliation so platform systems stay usable in practice."
  );

  s = s.replace(
    /\s*Confirm owner reporting expectations[^.]*\./gi,
    " Owners should clarify reporting expectations and system participation with their operator before affiliation."
  );

  s = s.replace(
    /\s*Clarify owner,\s*operator,\s*and brand responsibilities[^.]*\./gi,
    ` ${OWNER_VALUE_CLOSING}`
  );

  s = s.replace(
    /\s*Confirm operator capacity[^.]*\./gi,
    " The affiliation works best when the operating team can sustain brand-level quality and guest experience after opening."
  );

  s = s.replace(
    /\s*Confirm operator responsibilities[^.]*\./gi,
    " Align opening timeline, staffing, and milestone approvals with brand development and advisors before go-live."
  );

  s = s.replace(
    /\s*[—-]\s*confirm operator[^.]*\./gi,
    " Owners should validate operating capacity and program economics before treating the example as transferable."
  );

  s = s.replace(/\s*confirm operator[^.]*\./gi, (match) => {
    if (/confirm owner/i.test(match)) return match;
    return " Owners should validate operating capacity for the intended brand experience before locking conversion capital.";
  });

  s = s.replace(
    /\bKeep sibling lines labeled only as family context\.?/gi,
    "Treat adjacent Accor economy brands as separate operating models, not interchangeable proof."
  );

  s = s.replace(/\bkeep sibling\b/gi, "keep adjacent brands");
  s = s.replace(/\bdo not reuse\b/gi, "do not mix");
  s = s.replace(/\bavoid borrowing\b/gi, "avoid mixing");
  s = s.replace(/\bfee-?stack\b/gi, "affiliation fee");

  // Residual orphan fragments from mangled confirm sentences
  s = s.replace(/\s*stays deliverable after affiliation(?:\s+and through ongoing operations)?\.?/gi, "");
  s = s.replace(/\s*Confirm owner(?:,|\s+versus|\s+and)[^.]*\./gi, ` ${OWNER_VALUE_CLOSING}`);

  s = s
    .replace(/[ \t]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/\.\s*\./g, ".")
    .replace(/\s+\./g, ".")
    .replace(/^\s+|\s+$/g, "")
    .trim();

  return s;
}

function stillHasLifecycleHighPhrase(text) {
  const blob = String(text || "");
  return (
    /\bconfirm (the )?owner\b/i.test(blob) ||
    /\bconfirm (the )?operator\b/i.test(blob) ||
    /\bkeep sibling\b/i.test(blob) ||
    /\bdo not reuse\b/i.test(blob) ||
    /\bavoid borrowing\b/i.test(blob) ||
    /\bfee-?stack\b/i.test(blob) ||
    /Brand Page Frames|Brand Site Confirms|Development Page Frames|Development Positioning Remains/i.test(blob)
  );
}

function brandNameCandidates(brandSlug, brandName) {
  const names = [];
  const push = (v) => {
    const s = nz(v);
    if (s && !names.includes(s)) names.push(s);
  };
  push(brandName);
  const anchor = getWave13ActiveIdentityBySlug(brandSlug);
  if (anchor) {
    push(anchor.name);
    for (const a of anchor.nameAliases || []) push(a);
  }
  return names;
}

async function fetchPresentationRowById(recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey || !recordId) return null;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  if (!res.ok) return null;
  const rec = await res.json();
  const f = rec.fields || {};
  return {
    recordId: rec.id,
    slotKey: nz(f["Slot Key"]),
    title: nz(f.Title),
    body: nz(f.Body),
    brandName: nz(f["Brand Name"]),
    active: f.Active !== false,
    externalDisplayStatus: nz(f["External Display Status"]),
    sortOrder: f["Sort Order"] ?? 0,
  };
}

async function listOwnerRowsForCleanup(brandSlug, brandName, brandRecordId) {
  const seen = new Map();
  for (const name of brandNameCandidates(brandSlug, brandName)) {
    const live = await listPresentationRowsLight(brandRecordId, name);
    for (const r of live.rows || []) {
      if (r?.recordId && !seen.has(r.recordId)) seen.set(r.recordId, r);
    }
    if (seen.size > 0) break;
  }
  return [...seen.values()].filter(isOwnerFacingPresentationRow);
}

/**
 * Extract all High findings from a refreshed global audit.
 */
export function extractHighSeverityFailuresFromAudit(auditReport) {
  const failures = [];
  const classCounts = {
    lifecycle_internal_language: 0,
    portfolio_mix_prose: 0,
    recent_momentum_weak_semantics: 0,
    value_scenario_generic_or_repetitive: 0,
    openings_property_semantic_weakness: 0,
    other_high: 0,
  };

  for (const b of auditReport.brandResults || []) {
    if (NEVER_WRITE_BRANDS.has(nz(b.brandSlug).toLowerCase())) continue;
    for (const f of b.findings || []) {
      if (String(f.severity).toLowerCase() !== "high") continue;
      const batchClass = classifyHighFailureType(f.failureType, f.section);
      classCounts[batchClass] = (classCounts[batchClass] || 0) + 1;
      const batchIncluded = BATCH1_CLASSES.has(batchClass) || f.failureType === "fee_stack";
      failures.push({
        brand: b.brandName,
        brandSlug: b.brandSlug,
        recordId: b.recordId || null,
        presentationRecordId: f.recordId || null,
        section: f.section,
        field: "Title/Body",
        slotKey: f.slotKey || null,
        currentVisibleCopy: nz(f.currentValue),
        highFailureType: f.failureType,
        batchClass,
        proposedFix:
          f.proposedFix ||
          (batchClass === "portfolio_mix_prose"
            ? "Convert to curated sample percentage mix"
            : batchClass === "recent_momentum_weak_semantics"
              ? "Replace brand-page framing with property/pipeline proof"
              : "Rewrite confirm owner/operator language to owner-facing value"),
        batchIncluded,
        reason: batchIncluded
          ? "Batch 1 focus (A/B/C or obvious phrase High)"
          : "Deferred — outside Batch 1 focus or needs steward judgment",
      });
    }
  }

  // fee_stack is other_high but Batch 1 includes the obvious phrase fix
  for (const f of failures) {
    if (f.highFailureType === "fee_stack") {
      f.batchIncluded = true;
      f.reason = "Batch 1 — obvious High phrase fix (fee-stack → affiliation fee)";
    }
  }

  return {
    version: GLOBAL_HIGH_SEVERITY_CLEANUP_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    auditGeneratedAt: auditReport.generatedAt || null,
    universe: {
      activeCount: auditReport.activeCount,
      expected: EXPECTED_ACTIVE_UNIVERSE_COUNT,
      reconciled: auditReport.universeReconciled,
      critical: auditReport.severityTotals?.critical ?? null,
      high: auditReport.severityTotals?.high ?? failures.length,
      medium: auditReport.severityTotals?.medium ?? null,
    },
    summary: {
      highFindingCount: failures.length,
      brandsWithHigh: [...new Set(failures.map((f) => f.brandSlug))].length,
      classCounts,
      batch1IncludedCount: failures.filter((f) => f.batchIncluded).length,
      deferredCount: failures.filter((f) => !f.batchIncluded).length,
    },
    failures,
  };
}

export function writeHighSeverityFailuresReports(extract) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-global-high-severity-failures.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-global-high-severity-failures.md");
  const planPath = path.join(REPORTS_DIR, "brand-explorer-global-high-severity-batch1-plan.md");

  fs.writeFileSync(jsonPath, `${JSON.stringify(extract, null, 2)}\n`, "utf8");

  const lines = [
    "# Global Active — High-Severity Semantic Failures",
    "",
    `Generated: ${extract.generatedAt}`,
    `Audit generated: ${extract.auditGeneratedAt}`,
    `High findings: **${extract.summary.highFindingCount}**`,
    `Brands with High: **${extract.summary.brandsWithHigh}**`,
    `Batch 1 included: **${extract.summary.batch1IncludedCount}**`,
    `Deferred: **${extract.summary.deferredCount}**`,
    "",
    "## Class counts",
    "",
    ...Object.entries(extract.summary.classCounts).map(([k, v]) => `- **${k}**: ${v}`),
    "",
    "| Brand | Slug | Record ID | Section | Field | Current Visible Copy | High Failure Type | Proposed Fix | Batch Included? | Reason |",
    "|-------|------|-----------|---------|-------|----------------------|-------------------|--------------|-----------------|--------|",
  ];
  for (const f of extract.failures) {
    lines.push(
      `| ${f.brand} | \`${f.brandSlug}\` | ${f.presentationRecordId || f.recordId || "—"} | ${String(f.section || "").replace(/\|/g, "/")} | ${f.field} | ${String(f.currentVisibleCopy || "").replace(/\|/g, "/").replace(/\n/g, " ").slice(0, 90)} | ${f.highFailureType} | ${String(f.proposedFix || "").replace(/\|/g, "/").slice(0, 60)} | ${f.batchIncluded} | ${String(f.reason || "").replace(/\|/g, "/")} |`
    );
  }
  lines.push("");
  fs.writeFileSync(mdPath, lines.join("\n"), "utf8");

  const included = extract.failures.filter((f) => f.batchIncluded);
  const deferred = extract.failures.filter((f) => !f.batchIncluded);
  const plan = [
    "# Global High-Severity Cleanup — Batch 1 Plan",
    "",
    `Audit: ${extract.auditGeneratedAt}`,
    `High total: **${extract.summary.highFindingCount}**`,
    `Batch 1 patches planned for: **${included.length}** findings`,
    `Deferred to Batch 2+: **${deferred.length}**`,
    "",
    "## Batch 1 includes",
    "",
    "- A. lifecycle_internal_language (confirm owner/operator, keep sibling, related)",
    "- B. portfolio_mix_prose → curated sample percentage mixes",
    "- C. recent_momentum_weak_semantics (Mama Shelter brand-page card)",
    "- Obvious fee-stack phrase High",
    "",
    "## Deferred",
    "",
    deferred.length
      ? deferred.map((f) => `- \`${f.brandSlug}\` · ${f.highFailureType} · ${f.slotKey || f.section}`).join("\n")
      : "_None — all High findings classified into Batch 1 focus classes or obvious phrase fixes._",
    "",
    "## Brands in Batch 1",
    "",
    [...new Set(included.map((f) => f.brandSlug))].sort().map((s) => `- \`${s}\``).join("\n"),
    "",
    "## Forbidden writes (must remain true)",
    "",
    "- No Brand Status / release / CV / Source / Registry / image writes",
    "- No Four Points Flex / House of Originals / Morgans / Radisson Collection writes",
    "- No broad profile rewrites; targeted Title/Body only",
    "",
  ];
  fs.writeFileSync(planPath, plan.join("\n"), "utf8");

  return { jsonPath, mdPath, planPath };
}

function buildPatchFromRow(row, brandSlug, fields, meta = {}) {
  return {
    action: "PATCH",
    table: PRESENTATION_TABLE,
    recordId: row.recordId,
    brandSlug,
    slotKey: row.slotKey,
    fields,
    before: { title: nz(row.title).slice(0, 120), body: nz(row.body).slice(0, 180) },
    after: {
      title: (fields.Title != null ? fields.Title : nz(row.title)).slice(0, 120),
      body: (fields.Body != null ? fields.Body : nz(row.body)).slice(0, 180),
    },
    ...meta,
  };
}

async function planPatchesForBrand(brandSlug, brandName, brandRecordId, batchFailures) {
  const ownerRows = await listOwnerRowsForCleanup(brandSlug, brandName, brandRecordId);
  const byId = new Map(ownerRows.map((r) => [r.recordId, r]));
  const patches = [];
  const touched = new Set();

  const ensureRow = async (presentationRecordId) => {
    if (!presentationRecordId) return null;
    if (byId.has(presentationRecordId)) return byId.get(presentationRecordId);
    const fetched = await fetchPresentationRowById(presentationRecordId);
    if (fetched && isOwnerFacingPresentationRow(fetched)) {
      byId.set(fetched.recordId, fetched);
      return fetched;
    }
    return fetched;
  };

  // B. Portfolio mix
  const mixFailure = batchFailures.find((f) => f.batchClass === "portfolio_mix_prose");
  const mixSpec = PORTFOLIO_MIX_BY_SLUG[brandSlug];
  if (mixFailure && mixSpec) {
    const row =
      (await ensureRow(mixFailure.presentationRecordId)) ||
      ownerRows.find((r) => r.slotKey === "footprint.portfolio_mix");
    if (row?.recordId && !touched.has(row.recordId)) {
      touched.add(row.recordId);
      const fields = {};
      if (nz(row.title) !== mixSpec.title) fields.Title = mixSpec.title;
      if (nz(row.body) !== mixSpec.body) fields.Body = mixSpec.body;
      if (Object.keys(fields).length) {
        patches.push(buildPatchFromRow(row, brandSlug, fields, { batchClass: "portfolio_mix_prose" }));
      }
    }
  }

  // C. Mama Shelter momentum — hide brand-page card (duplicate Mexico City pipeline already exists)
  if (brandSlug === "mama-shelter") {
    const momFailure = batchFailures.find((f) => f.batchClass === "recent_momentum_weak_semantics");
    const row =
      (await ensureRow(momFailure?.presentationRecordId || MAMA_SHELTER_MOMENTUM_PATCH.recordId)) ||
      ownerRows.find((r) => /Brand Page Frames/i.test(nz(r.title)));
    if (row?.recordId && !touched.has(row.recordId)) {
      touched.add(row.recordId);
      const fields = MAMA_SHELTER_MOMENTUM_PATCH.hide
        ? { Active: false, "External Display Status": "Do Not Display" }
        : {
            Title: MAMA_SHELTER_MOMENTUM_PATCH.title,
            Body: MAMA_SHELTER_MOMENTUM_PATCH.body,
          };
      patches.push(
        buildPatchFromRow(row, brandSlug, fields, { batchClass: "recent_momentum_weak_semantics" })
      );
    }
  }

  // A (+ fee_stack): lifecycle / internal language on finding rows + residual scan
  const candidates = [];
  for (const f of batchFailures) {
    if (f.batchClass === "portfolio_mix_prose") continue;
    if (f.batchClass === "recent_momentum_weak_semantics") continue;
    const row = await ensureRow(f.presentationRecordId);
    if (row) candidates.push(row);
  }
  for (const r of ownerRows) {
    const text = `${nz(r.title)}\n${nz(r.body)}`;
    if (stillHasLifecycleHighPhrase(text)) candidates.push(r);
  }

  for (const row of candidates) {
    if (!row?.recordId || touched.has(row.recordId)) continue;
    if (row.slotKey === "footprint.portfolio_mix") continue; // handled above
    touched.add(row.recordId);

    const titleBefore = nz(row.title);
    const bodyBefore = nz(row.body);
    let titleAfter = sanitizeHighLifecycleOwnerFacingText(titleBefore);
    let bodyAfter = sanitizeHighLifecycleOwnerFacingText(bodyBefore);

    // Momentum titles that still look like brand-page framing
    if (row.slotKey === "footprint.momentum" && /Brand Page Frames|Brand Site Confirms|Development Page Frames/i.test(titleAfter)) {
      // Only Mama Shelter has a Batch 1 momentum rewrite; hide unsupported brand-page cards otherwise
      if (brandSlug !== "mama-shelter") {
        // leave for Batch 2 unless sanitize already fixed
      }
    }

    const fields = {};
    if (titleAfter !== titleBefore) fields.Title = titleAfter;
    if (bodyAfter !== bodyBefore) fields.Body = bodyAfter;

    if (!Object.keys(fields).length) continue;
    if (stillHasLifecycleHighPhrase(`${fields.Title || titleAfter}\n${fields.Body || bodyAfter}`)) {
      if (fields.Body != null) fields.Body = sanitizeHighLifecycleOwnerFacingText(fields.Body);
      if (fields.Title != null) fields.Title = sanitizeHighLifecycleOwnerFacingText(fields.Title);
    }

    patches.push(
      buildPatchFromRow(
        { ...row, title: titleBefore, body: bodyBefore },
        brandSlug,
        fields,
        { batchClass: "lifecycle_internal_language" }
      )
    );
  }

  return patches;
}

export async function runGlobalHighSeverityCleanup({
  dryRun = true,
  argv = [],
  auditReport = null,
  batch = 1,
} = {}) {
  const apply = argv.includes("--apply") && dryRun === false;
  const flagCheck = checkFlags(GLOBAL_HIGH_SEVERITY_CLEANUP_APPLY_FLAGS, argv, apply);

  if (apply && !flagCheck.ok) {
    return {
      version: GLOBAL_HIGH_SEVERITY_CLEANUP_VERSION,
      generatedAt: new Date().toISOString(),
      applyPerformed: false,
      pass: false,
      stopRecommended: true,
      readyStatement: "global_high_severity_cleanup_batch1_blocked_missing_flags",
      missingFlags: flagCheck.missing,
    };
  }

  if (!auditReport) {
    return {
      version: GLOBAL_HIGH_SEVERITY_CLEANUP_VERSION,
      generatedAt: new Date().toISOString(),
      applyPerformed: false,
      pass: false,
      stopRecommended: true,
      readyStatement: "global_high_severity_cleanup_batch1_blocked_missing_fresh_audit",
      error: "Pass refreshed audit JSON (reports/brand-explorer-global-active-semantic-audit-refresh.json)",
    };
  }

  if ((auditReport.severityTotals?.critical ?? 0) > 0) {
    return {
      version: GLOBAL_HIGH_SEVERITY_CLEANUP_VERSION,
      generatedAt: new Date().toISOString(),
      applyPerformed: false,
      pass: false,
      stopRecommended: true,
      readyStatement: "global_high_severity_cleanup_batch1_blocked_critical_not_clean",
      criticalCount: auditReport.severityTotals.critical,
    };
  }

  const extract = extractHighSeverityFailuresFromAudit(auditReport);
  writeHighSeverityFailuresReports(extract);

  const batchFailures = extract.failures.filter((f) => f.batchIncluded);
  const byBrand = new Map();
  for (const f of batchFailures) {
    if (!byBrand.has(f.brandSlug)) {
      byBrand.set(f.brandSlug, {
        brandSlug: f.brandSlug,
        brandName: f.brand,
        recordId: f.recordId,
        failures: [],
      });
    }
    byBrand.get(f.brandSlug).failures.push(f);
  }

  const brandPlans = [];
  for (const [slug, meta] of byBrand) {
    if (NEVER_WRITE_BRANDS.has(slug.toLowerCase())) {
      brandPlans.push({ ...meta, patches: [], skipped: "excluded_brand" });
      continue;
    }
    if (!meta.recordId) {
      brandPlans.push({ ...meta, patches: [], skipped: "missing_record_id" });
      continue;
    }
    process.stdout.write(`[high-cleanup-b${batch}] plan ${slug}...\n`);
    const patches = await planPatchesForBrand(slug, meta.brandName, meta.recordId, meta.failures);
    brandPlans.push({ ...meta, patches, patchCount: patches.length });
    await sleep(250);
  }

  let applyResult = { applied: 0, errors: [] };
  if (apply) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("Missing AIRTABLE_BASE_ID / AIRTABLE_API_KEY");
    for (const plan of brandPlans) {
      for (const p of plan.patches || []) {
        try {
          await airtablePatch(baseId, apiKey, p.table, p.recordId, p.fields);
          applyResult.applied += 1;
        } catch (err) {
          applyResult.errors.push({
            brandSlug: plan.brandSlug,
            recordId: p.recordId,
            slotKey: p.slotKey,
            error: err?.message || String(err),
          });
        }
        await sleep(WRITE_THROTTLE_MS);
      }
    }
  }

  const totalPatches = brandPlans.reduce((n, b) => n + (b.patches?.length || 0), 0);
  const sectionsPatched = [
    ...new Set(
      brandPlans.flatMap((b) => (b.patches || []).map((p) => p.batchClass || p.slotKey).filter(Boolean))
    ),
  ];

  const report = {
    version: GLOBAL_HIGH_SEVERITY_CLEANUP_VERSION,
    generatedAt: new Date().toISOString(),
    batch,
    dryRun: !apply,
    applyPerformed: apply === true,
    writePerformed: apply === true && applyResult.applied > 0,
    auditGeneratedAt: auditReport.generatedAt,
    highFindingsBefore: extract.summary.highFindingCount,
    batch1FindingCount: batchFailures.length,
    deferredFindingCount: extract.summary.deferredCount,
    classCounts: extract.summary.classCounts,
    brandsPatched: brandPlans.filter((b) => (b.patches || []).length > 0).map((b) => b.brandSlug),
    sectionsPatched,
    patchCount: totalPatches,
    applyResult,
    forbiddenWritesAvoided: [
      "Brand Status",
      "release fields",
      "Active Profile Approved / Ready for Active Profile / Founder Visual Review Pass",
      "Company Validated / Company Validation Date",
      "Source Library status",
      "Registry approval/status",
      "images",
      "four-points-flex-by-sheraton",
      "the-house-of-originals",
      "morgans-originals",
      "radisson-collection",
      "broad profile rewrites",
      "baseline freeze artifacts",
    ],
    brandPlans: brandPlans.map((b) => ({
      brandSlug: b.brandSlug,
      brandName: b.brandName,
      recordId: b.recordId,
      failureCount: (b.failures || []).length,
      patchCount: (b.patches || []).length,
      skipped: b.skipped || null,
      patches: b.patches || [],
    })),
    readyStatement: apply
      ? applyResult.errors.length === 0
        ? "global_high_semantic_cleanup_batch1_complete_batch2_required"
        : "global_high_severity_cleanup_batch1_applied_with_errors"
      : "global_high_severity_cleanup_batch1_dry_run_ready",
    freezeNote:
      "Do not freeze 54 in this task — High Batch 1 only; re-audit after apply; Medium remains for later review.",
  };

  writeGlobalHighSeverityCleanupReports(report);
  return report;
}

export function writeGlobalHighSeverityCleanupReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-global-high-severity-cleanup-batch1.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-global-high-severity-cleanup-batch1.md");
  const byBrandPath = path.join(
    REPORTS_DIR,
    "brand-explorer-global-high-severity-cleanup-batch1-by-brand.md"
  );
  const docsPath = path.join(DOCS_DIR, "brand-explorer-global-high-severity-cleanup-batch1.md");

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    "# Global Active — High-Severity Semantic Cleanup Batch 1",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.applyPerformed ? "APPLY" : "dry-run"}**`,
    `Audit used: ${report.auditGeneratedAt || "—"}`,
    "",
    `Ready: \`${report.readyStatement}\``,
    "",
    "## Counts",
    "",
    `| Item | Count |`,
    `|------|------:|`,
    `| High findings before | ${report.highFindingsBefore ?? 0} |`,
    `| Batch 1 findings in scope | ${report.batch1FindingCount ?? 0} |`,
    `| Deferred | ${report.deferredFindingCount ?? 0} |`,
    `| Patches planned | ${report.patchCount ?? 0} |`,
    `| Patches applied | ${report.applyResult?.applied ?? 0} |`,
    `| Apply errors | ${report.applyResult?.errors?.length ?? 0} |`,
    `| Brands patched | ${(report.brandsPatched || []).length} |`,
    "",
    "## Class counts (from extract)",
    "",
    ...Object.entries(report.classCounts || {}).map(([k, v]) => `- **${k}**: ${v}`),
    "",
    "## Brands patched",
    "",
    ...(report.brandsPatched || []).map((s) => `- \`${s}\``),
    "",
    "## Forbidden writes avoided",
    "",
    ...(report.forbiddenWritesAvoided || []).map((s) => `- ${s}`),
    "",
    report.freezeNote || "",
    "",
  ];

  const byBrandLines = ["# High-Severity Cleanup Batch 1 — By Brand", ""];
  for (const b of report.brandPlans || []) {
    lines.push(`## ${b.brandName} (\`${b.brandSlug}\`)`);
    lines.push(
      `Failures: ${b.failureCount} · Patches: ${b.patchCount}${b.skipped ? ` · skipped=${b.skipped}` : ""}`
    );
    byBrandLines.push(`## ${b.brandName} (\`${b.brandSlug}\`)`);
    byBrandLines.push(`Patches: ${b.patchCount}`);
    for (const p of (b.patches || []).slice(0, 30)) {
      const line = `- \`${p.slotKey}\` ${p.recordId} [${p.batchClass || ""}]: ${(p.before?.title || "").slice(0, 40)} → ${(p.after?.title || "").slice(0, 40)}`;
      lines.push(line);
      byBrandLines.push(line);
    }
    lines.push("");
    byBrandLines.push("");
  }

  const md = lines.join("\n");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");
  fs.writeFileSync(byBrandPath, byBrandLines.join("\n"), "utf8");
  return { jsonPath, mdPath, byBrandPath, docsPath };
}
