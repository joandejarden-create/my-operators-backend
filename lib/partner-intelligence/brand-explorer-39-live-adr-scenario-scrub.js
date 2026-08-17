/**
 * Protected 39 live PVQL re-green — targeted ADR scrub on flagged
 * valueOwners.scenario.* Body rows only.
 *
 * Never writes CV / Source Library / Registry / Brand Status / release / images /
 * non-flagged Presentation rows / Wave 13 / Radisson Collection.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";

export const LIVE_39_ADR_SCENARIO_SCRUB_VERSION = "brand-explorer-39-live-adr-scenario-scrub-v1";

export const APPLY_FLAGS = Object.freeze([
  "--approve-live-39-adr-scenario-scrub",
  "--confirm-targeted-scenario-rows-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-image-writes",
  "--confirm-no-wave13-work",
  "--confirm-no-broad-rewrites",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-forbidden-owner-facing-language",
]);

/** Exact flagged rows from fresh PVQL / quality audit (2026-07-26). */
export const FLAGGED_SCENARIO_ROWS = Object.freeze([
  {
    slug: "ac-hotels-by-marriott",
    slotKey: "valueOwners.scenario.1",
    recordId: "recOuvKY5QCAkPGI1",
  },
  {
    slug: "canopy-by-hilton",
    slotKey: "valueOwners.scenario.1",
    recordId: "recIGLioUN2WUel2o",
  },
  {
    slug: "city-express-by-marriott",
    slotKey: "valueOwners.scenario.1",
    recordId: "reclBLmmOSSWtcQS4",
  },
  {
    slug: "hotel-indigo",
    slotKey: "valueOwners.scenario.1",
    recordId: "recY6oHFUjaKw4Ibs",
  },
  { slug: "kimpton", slotKey: "valueOwners.scenario.1", recordId: "recIQPqSxhrJYXr1t" },
  {
    slug: "moxy-hotels",
    slotKey: "valueOwners.scenario.1",
    recordId: "recfpAQEnidwzTPOZ",
  },
  {
    slug: "dazzler-by-wyndham",
    slotKey: "valueOwners.scenario.2",
    recordId: "recYdFgUuRq7x7arw",
  },
  {
    slug: "even-hotels",
    slotKey: "valueOwners.scenario.2",
    recordId: "rec4N8Qx3qUjJgp7L",
  },
  {
    slug: "comfort-inn-suites",
    slotKey: "valueOwners.scenario.3",
    recordId: "rec0Xzu7aXf1S1thM",
  },
  {
    slug: "courtyard-by-marriott",
    slotKey: "valueOwners.scenario.3",
    recordId: "recK09kuVsNwhcicQ",
  },
  {
    slug: "everhome-suites",
    slotKey: "valueOwners.scenario.3",
    recordId: "recjqUhmrrWoXmd0E",
  },
  {
    slug: "motto-by-hilton",
    slotKey: "valueOwners.scenario.1",
    recordId: "rec2ZSeIZac9XhFXu",
  },
  {
    slug: "motto-by-hilton",
    slotKey: "valueOwners.scenario.2",
    recordId: "recXFCKeosfFJ9fZf",
  },
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const ALLOWED_WRITE_FIELDS = Object.freeze(["Body", "Title", "Case Summary Overview"]);
const NEVER_WRITE = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library status",
  "Registry approval/status",
  "Brand Status",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
  "Image",
]);

/** Phrase-first replacements to preserve scenario meaning. */
const ADR_PHRASE_REPLACEMENTS = Object.freeze([
  { re: /\bcan lift ADR\b/gi, replace: "can lift rate positioning", phrase: "can lift ADR" },
  {
    re: /\brealistic corridor ADR\b/gi,
    replace: "realistic corridor rate support",
    phrase: "realistic corridor ADR",
  },
  {
    re: /\brealistic ADR support\b/gi,
    replace: "realistic rate support",
    phrase: "realistic ADR support",
  },
  {
    re: /\brealistic ADR for the market\b/gi,
    replace: "realistic rate support for the market",
    phrase: "realistic ADR for the market",
  },
  {
    re: /\brealistic ADR rather\b/gi,
    replace: "realistic rate support rather",
    phrase: "realistic ADR rather",
  },
  {
    re: /\brealistic ADR peers\b/gi,
    replace: "realistic rate-positioning peers",
    phrase: "realistic ADR peers",
  },
  {
    re: /\bweekly-stay ADR\b/gi,
    replace: "weekly-stay rate support",
    phrase: "weekly-stay ADR",
  },
  {
    re: /\brealistic urban ADR\b/gi,
    replace: "realistic urban rate support",
    phrase: "realistic urban ADR",
  },
  { re: /\bRevPAR\b/g, replace: "revenue mix", phrase: "RevPAR" },
  { re: /\brevpar\b/gi, replace: "revenue mix", phrase: "revpar" },
  { re: /\bfee stack\b/gi, replace: "affiliation economics", phrase: "fee stack" },
  { re: /\bFDD\b/g, replace: "affiliation disclosure materials", phrase: "FDD" },
  { re: /\bItem 19\b/gi, replace: "affiliation performance disclosures", phrase: "Item 19" },
  { re: /\bLOI\b/g, replace: "preliminary interest letter", phrase: "LOI" },
  // Fallback token scrub last.
  { re: /\bADR\b/g, replace: "rate support", phrase: "ADR" },
  { re: /\badr\b/g, replace: "rate support", phrase: "adr" },
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS = path.join(ROOT, "reports");
const DOCS = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function checkFlags(argv = [], apply = false) {
  const missing = APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: !!apply,
    ok: !apply || missing.length === 0,
    missing,
    required: [...APPLY_FLAGS],
  };
}

function detectAdrPhrase(body) {
  const m = nz(body).match(/\b(?:ADR|adr|RevPAR|revpar|fee stack|FDD|Item 19|LOI)\b/);
  return m ? m[0] : null;
}

function scrubBody(text) {
  let out = nz(text);
  const applied = [];
  if (!out) return { cleaned: out, applied };
  for (const rule of ADR_PHRASE_REPLACEMENTS) {
    const before = out;
    out = out.replace(rule.re, rule.replace);
    if (out !== before) {
      applied.push({ phrase: rule.phrase, replace: rule.replace });
    }
  }
  out = out.replace(/[ \t]{2,}/g, " ").replace(/\s+\./g, ".").trim();
  return { cleaned: out, applied };
}

function remainingForbidden(text) {
  const hits = scanForbiddenLanguage(text).filter((h) =>
    /adr|revpar|fdd|item_?19|loi|fee_?stack|raw_url/i.test(h.id || h.label || "")
  );
  if (/\bfee stack\b/i.test(text)) hits.push({ id: "fee_stack", label: "fee stack" });
  if (/\bADR\b|\badr\b/.test(text)) hits.push({ id: "adr", label: "ADR" });
  return hits;
}

function readJson(name) {
  const p = path.join(REPORTS, name);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Failure extraction from latest PVQL + quality audit + live Body fetch.
 */
export async function extractLiveAdrScenarioFailures() {
  const pvql = readJson("brand-explorer-public-visibility-quality-lock.json");
  const quality = readJson("brand-explorer-24-tab-section-quality-audit.json");
  const universe = await loadActiveUniverse({ includeDetails: false });
  const bySlug = new Map((universe.brands || []).map((b) => [b.slug, b]));

  const failures = [];
  for (const flag of FLAGGED_SCENARIO_ROWS) {
    const brand = bySlug.get(flag.slug);
    if (!brand) {
      failures.push({
        ...flag,
        error: "brand_not_in_active_universe",
        currentBody: null,
        proposedBody: null,
      });
      continue;
    }
    const { rows } = await listPresentationRowsLight(
      brand.recordId,
      brand.brandName || brand.name
    );
    const row =
      (rows || []).find((r) => r.recordId === flag.recordId) ||
      (rows || []).find((r) => r.slotKey === flag.slotKey);
    const currentBody = nz(row?.body);
    const adrPhrase = detectAdrPhrase(currentBody);
    const { cleaned, applied } = scrubBody(currentBody);
    const still = remainingForbidden(cleaned);
    const pvqlBrand = (pvql?.brands || []).find((b) => b.slug === flag.slug);
    const pvqlHit = (pvqlBrand?.gateResults?.forbidden_owner_facing_language?.hits || []).find(
      (h) => h.slotKey === flag.slotKey
    );
    const qaBrand = (quality?.brandResults || []).find((b) => b.slug === flag.slug);
    const qaFinding = (qaBrand?.tabFindings || []).find(
      (f) => f.slotKey === flag.slotKey && /adr|forbidden/i.test(`${f.finding || ""} ${f.status || ""}`)
    );

    failures.push({
      brand: brand.brandName || brand.name || flag.slug,
      slug: flag.slug,
      scenarioRow: flag.slotKey,
      recordId: row?.recordId || flag.recordId,
      field: "Body",
      currentBody,
      adrPhrase,
      proposedReplacement: cleaned,
      replacementRulesApplied: applied,
      remainingForbidden: still.map((h) => h.id || h.label),
      blocked: still.length > 0 || !adrPhrase || cleaned === currentBody,
      blockedReason: !adrPhrase
        ? currentBody
          ? "no_adr_token_in_live_body"
          : "empty_body"
        : still.length
          ? "scrub_left_forbidden_tokens"
          : cleaned === currentBody
            ? "no_change"
            : null,
      pvqlHit: pvqlHit || null,
      qualityFinding: qaFinding
        ? {
            finding: qaFinding.finding,
            severity: qaFinding.severity,
            field: qaFinding.field,
          }
        : null,
      title: nz(row?.title),
    });
  }

  const report = {
    version: `${LIVE_39_ADR_SCENARIO_SCRUB_VERSION}-failures`,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    sourcePvqlGeneratedAt: pvql?.generatedAt || null,
    sourceQualityGeneratedAt: quality?.generatedAt || null,
    sourceQualityDecision: quality?.baselineFreezeDecision || null,
    flaggedRowCount: FLAGGED_SCENARIO_ROWS.length,
    failureCount: failures.filter((f) => f.adrPhrase).length,
    scrubReadyCount: failures.filter((f) => f.adrPhrase && !f.blocked).length,
    failures,
    readyStatement: "failures_extracted_awaiting_targeted_adr_scrub",
  };

  writeFailuresReports(report);
  return report;
}

function writeFailuresReports(report) {
  fs.mkdirSync(REPORTS, { recursive: true });
  const base = "brand-explorer-39-live-adr-scenario-scrub-failures";
  fs.writeFileSync(path.join(REPORTS, `${base}.json`), `${JSON.stringify(report, null, 2)}\n`);
  const lines = [
    `# Brand Explorer 39 — Live ADR Scenario Scrub Failures`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Source PVQL: ${report.sourcePvqlGeneratedAt || "—"}`,
    `Source quality decision: ${report.sourceQualityDecision || "—"}`,
    ``,
    `## Summary`,
    ``,
    `- Flagged rows: **${report.flaggedRowCount}**`,
    `- Rows still containing ADR/forbidden: **${report.failureCount}**`,
    `- Scrub-ready: **${report.scrubReadyCount}**`,
    ``,
    `## Failure table`,
    ``,
    `| Brand | Scenario Row | Record ID | ADR Phrase | Proposed Replacement (excerpt) |`,
    `| --- | --- | --- | --- | --- |`,
    ...report.failures.map((f) => {
      const prop = (f.proposedReplacement || "").slice(0, 120).replace(/\|/g, "/");
      return `| ${f.brand || f.slug} | \`${f.scenarioRow}\` | \`${f.recordId}\` | ${f.adrPhrase || "—"} | ${prop}${(f.proposedReplacement || "").length > 120 ? "…" : ""} |`;
    }),
    ``,
    `## Current Body (full)`,
    ``,
    ...report.failures.flatMap((f) => [
      `### ${f.brand || f.slug} — \`${f.scenarioRow}\``,
      ``,
      `Record: \`${f.recordId}\``,
      ``,
      `**Current:**`,
      ``,
      `> ${f.currentBody || "_(empty)_"}`,
      ``,
      `**Proposed:**`,
      ``,
      `> ${f.proposedReplacement || "_(n/a)_"}`,
      ``,
    ]),
  ];
  fs.writeFileSync(path.join(REPORTS, `${base}.md`), `${lines.join("\n")}\n`);
}

async function patchPresentation({ recordId, fields }) {
  for (const k of Object.keys(fields)) {
    if (NEVER_WRITE.includes(k)) throw new Error(`Refuse forbidden field ${k}`);
    if (!ALLOWED_WRITE_FIELDS.includes(k)) throw new Error(`Refuse non-allowed field ${k}`);
  }
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE credentials required");
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  let lastErr = null;
  for (let attempt = 1; attempt <= 8; attempt++) {
    const res = await fetch(url, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields }),
    });
    const json = await res.json().catch(() => ({}));
    if (res.ok) return json;
    lastErr = new Error(json.error?.message || `PATCH failed ${res.status}`);
    if (!(res.status === 429 || res.status >= 500) || attempt === 8) break;
    await sleep(Math.min(30_000, 800 * 2 ** (attempt - 1)));
  }
  throw lastErr;
}

function renderScrubMarkdown(report) {
  const lines = [
    `# Brand Explorer 39 — Live ADR Scenario Scrub`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Version: ${report.version}`,
    `Apply performed: **${report.applyPerformed}**`,
    `Write performed: **${report.writePerformed}**`,
    ``,
    `## Executive summary`,
    ``,
    `Targeted scrub of ADR in flagged \`valueOwners.scenario.*\` Body rows so the protected 39 Active/Live public-full baseline is live-clean again.`,
    ``,
    `| Metric | Value |`,
    `| --- | --- |`,
    `| Flagged rows | ${report.flaggedRowCount} |`,
    `| Planned Body patches | ${report.plannedPatchCount} |`,
    `| Applied patches | ${report.appliedPatchCount} |`,
    `| Blocked | ${report.blockedPatchCount} |`,
    `| Company Validated writes | false |`,
    `| Source Library writes | false |`,
    `| Registry writes | false |`,
    `| Brand Status writes | false |`,
    `| Release field writes | false |`,
    `| Image writes | false |`,
    `| Wave 13 work | false |`,
    ``,
    `## Patches`,
    ``,
    `| Brand | Slot | Record ID | ADR Phrase | Applied |`,
    `| --- | --- | --- | --- | --- |`,
    ...report.patches.map(
      (p) =>
        `| ${p.brand} | \`${p.slotKey}\` | \`${p.recordId}\` | ${p.adrPhrase || "—"} | ${p.applied ? "yes" : p.blocked ? "blocked" : "planned"} |`
    ),
    ``,
    `## Ready statement`,
    ``,
    `**${report.readyStatement}**`,
    ``,
    `## Guardrails`,
    ``,
    `- Only flagged scenario Body rows`,
    `- No CV / Source Library / Registry / Brand Status / release / image writes`,
    `- No Wave 13 source packs or content generation`,
    `- No Radisson Collection`,
    ``,
  ];
  return `${lines.join("\n")}\n`;
}

function writeScrubReports(report) {
  fs.mkdirSync(REPORTS, { recursive: true });
  fs.mkdirSync(DOCS, { recursive: true });
  const base = "brand-explorer-39-live-adr-scenario-scrub";
  fs.writeFileSync(path.join(REPORTS, `${base}.json`), `${JSON.stringify(report, null, 2)}\n`);
  const md = renderScrubMarkdown(report);
  fs.writeFileSync(path.join(REPORTS, `${base}.md`), md);
  fs.writeFileSync(path.join(DOCS, `${base}.md`), md);
  return {
    jsonPath: path.join(REPORTS, `${base}.json`),
    mdPath: path.join(REPORTS, `${base}.md`),
    docPath: path.join(DOCS, `${base}.md`),
  };
}

/**
 * @param {{ apply?: boolean, argv?: string[], extractOnly?: boolean }} opts
 */
export async function run39LiveAdrScenarioScrub({
  apply = false,
  argv = [],
  extractOnly = false,
} = {}) {
  const failuresReport = await extractLiveAdrScenarioFailures();
  if (extractOnly) {
    return { ...failuresReport, extractOnly: true };
  }

  const flagCheck = checkFlags(argv, apply);
  if (apply && !flagCheck.ok) {
    throw new Error(`Missing apply flags: ${flagCheck.missing.join(", ")}`);
  }

  const allowedIds = new Set(FLAGGED_SCENARIO_ROWS.map((r) => r.recordId));
  const patches = [];
  const blocked = [];

  for (const f of failuresReport.failures) {
    if (!f.adrPhrase) {
      blocked.push({ ...f, reason: f.blockedReason || "no_adr" });
      continue;
    }
    if (f.blocked || remainingForbidden(f.proposedReplacement).length) {
      blocked.push({ ...f, reason: f.blockedReason || "scrub_incomplete" });
      continue;
    }
    if (!allowedIds.has(f.recordId)) {
      throw new Error(`Refuse non-allowlisted recordId ${f.recordId}`);
    }
    if (!/^valueOwners\.scenario\.\d+$/.test(f.scenarioRow)) {
      throw new Error(`Refuse non-scenario slot ${f.scenarioRow}`);
    }
    patches.push({
      brand: f.brand,
      slug: f.slug,
      slotKey: f.scenarioRow,
      recordId: f.recordId,
      table: PRESENTATION_TABLE,
      adrPhrase: f.adrPhrase,
      fields: { Body: f.proposedReplacement },
      before: { Body: f.currentBody },
      after: { Body: f.proposedReplacement },
      replacementRulesApplied: f.replacementRulesApplied,
      applied: false,
      blocked: false,
    });
  }

  let writePerformed = false;
  let appliedPatchCount = 0;
  if (apply) {
    for (const p of patches) {
      if (!allowedIds.has(p.recordId)) throw new Error(`Refuse write outside allowlist ${p.recordId}`);
      if (Object.keys(p.fields).some((k) => k === "Image" || NEVER_WRITE.includes(k))) {
        throw new Error(`Refuse forbidden field write for ${p.slotKey}`);
      }
      await patchPresentation({ recordId: p.recordId, fields: p.fields });
      p.applied = true;
      writePerformed = true;
      appliedPatchCount += 1;
      await sleep(300);
    }
  }

  const allClean =
    blocked.length === 0 &&
    patches.length === FLAGGED_SCENARIO_ROWS.length &&
    (!apply || appliedPatchCount === patches.length);

  const report = {
    version: LIVE_39_ADR_SCENARIO_SCRUB_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    apply,
    applyPerformed: !!apply && writePerformed,
    writePerformed,
    airtableWrites: writePerformed,
    presentationWrites: writePerformed,
    imageWrites: false,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    brandStatusWrites: false,
    releaseFieldWrites: false,
    wave13Work: false,
    radissonCollectionTouched: false,
    flagCheck,
    flaggedRowCount: FLAGGED_SCENARIO_ROWS.length,
    plannedPatchCount: patches.length,
    appliedPatchCount,
    blockedPatchCount: blocked.length,
    patches,
    blocked,
    failuresArtifact: "brand-explorer-39-live-adr-scenario-scrub-failures.json",
    readyStatement: apply
      ? allClean
        ? "live_adr_scenario_scrub_applied_awaiting_fresh_pvql"
        : "live_adr_scenario_scrub_applied_with_blockers"
      : allClean
        ? "live_adr_scenario_scrub_dry_run_ready"
        : "live_adr_scenario_scrub_dry_run_has_blockers",
  };

  const paths = writeScrubReports(report);
  return { ...report, paths };
}
