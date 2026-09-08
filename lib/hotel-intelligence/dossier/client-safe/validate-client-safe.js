/**
 * Mandatory customer-report sanitizer / validator.
 * Runs before web render, PDF generation, and archive publication.
 */

import { CLIENT_SAFE_BLOCK_PATTERNS, INTERNAL_ONLY_DOSSIER_KEYS } from "./block-patterns.js";
import { prepareCustomerProse, prepareCustomerProseDeep } from "./text-normalize.js";

function collectCustomerText(dossier) {
  const chunks = [];
  const push = (v) => {
    if (v == null) return;
    if (typeof v === "string") {
      chunks.push(v);
      return;
    }
    if (Array.isArray(v)) {
      v.forEach(push);
      return;
    }
    if (typeof v === "object") {
      for (const [k, val] of Object.entries(v)) {
        if (INTERNAL_ONLY_DOSSIER_KEYS.includes(k)) continue;
        if (
          (k === "claim_id" || k === "id") &&
          typeof val === "string" &&
          (/^(kf_|oq_|claim_|finding_|co_f|report:)/i.test(val) ||
            /^[0-9a-f]{8}-[0-9a-f]{4}-/i.test(val))
        ) {
          continue;
        }
        push(val);
      }
    }
  };

  push(dossier?.title);
  push(dossier?.hotel_name);
  push(dossier?.executive_summary);
  push(dossier?.key_findings);
  push(dossier?.sections);
  push(dossier?.appendices);
  push(dossier?.open_questions);
  // Bibliography titles/publishers/urls are customer-visible
  for (const s of dossier?.sources || []) {
    push(s.title);
    push(s.publisher);
    push(s.source_type);
    push(s.url);
  }
  for (const p of dossier?.people || []) {
    push(p.name);
    push(p.title);
    push(p.organization);
    push(p.relevance);
    push(p.professional_profile_url);
  }
  return chunks.join("\n");
}

/**
 * @param {object} report — dossier-shaped customer report
 * @param {object} [opts]
 * @returns {{ ok: boolean, errors: string[], warnings: string[], textLength: number }}
 */
export function validateClientSafeReport(report, opts = {}) {
  const errors = [];
  const warnings = [];
  if (!report || typeof report !== "object") {
    return { ok: false, errors: ["missing_report"], warnings, textLength: 0 };
  }

  const text = collectCustomerText(report);
  const patterns = opts.patterns || CLIENT_SAFE_BLOCK_PATTERNS;

  for (const p of patterns) {
    if (p.re.test(text)) {
      const m = text.match(p.re);
      errors.push(`CLIENT_UNSAFE:${p.id}:${m ? m[0].slice(0, 80) : ""}`);
    }
  }

  // Generic Source N in bibliography titles
  for (const s of report.sources || []) {
    if (/^Source\s+\d+$/i.test(String(s.title || ""))) {
      errors.push(`GENERIC_SOURCE_LABEL:${s.number || "?"}`);
    }
    if (!s.url && !opts.allowMissingUrls) {
      warnings.push(`SOURCE_WITHOUT_URL:${s.number || s.title}`);
    }
    if (!s.title || String(s.title).trim().length < 3) {
      errors.push(`SOURCE_TITLE_MISSING:${s.number || "?"}`);
    }
  }

  // Citation integrity
  const maxN = (report.sources || []).length;
  const citeRe = /\[(\d{1,3})\]/g;
  let cm;
  const orphan = new Set();
  while ((cm = citeRe.exec(text)) !== null) {
    const n = Number(cm[1]);
    if (!Number.isFinite(n) || n < 1 || n > maxN) orphan.add(n);
  }
  for (const n of orphan) errors.push(`ORPHAN_CITATION:${n}`);

  // Person profile consistency across people[] and section tables
  const people = report.people || [];
  const byName = new Map();
  for (const p of people) {
    const key = String(p.name || "").toLowerCase();
    if (!key) continue;
    byName.set(key, p.professional_profile_url || null);
  }
  for (const sec of report.sections || []) {
    for (const b of sec.blocks || []) {
      if (b.type !== "table") continue;
      const headers = (b.headers || []).map((h) => String(h).toLowerCase());
      const nameIdx = headers.findIndex((h) => h === "name");
      const profileIdx = headers.findIndex((h) => /profile|linkedin/.test(h));
      if (nameIdx < 0 || profileIdx < 0) continue;
      for (const row of b.rows || []) {
        const name = String(row[nameIdx] || "").toLowerCase();
        const cell = String(row[profileIdx] || "").trim();
        const expected = byName.get(name);
        if (expected && /linkedin\.com\/in\//i.test(expected)) {
          if (cell === "—" || cell === "-" || !cell) {
            errors.push(`PERSON_PROFILE_CROSS_SECTION_MISS:${row[nameIdx]}`);
          } else if (cell !== expected && !cell.includes(expected)) {
            errors.push(`PERSON_PROFILE_CROSS_SECTION_MISMATCH:${row[nameIdx]}`);
          }
        }
      }
    }
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    textLength: text.length,
  };
}

/**
 * Soft sanitize: rewrite known leak phrases on customer-visible surfaces.
 * Prefer fixing adapters; this is the last-line compiler / serve guard.
 */
export function enforceClientSafeCustomerSurfaces(dossier) {
  const clone = structuredClone ? structuredClone(dossier) : JSON.parse(JSON.stringify(dossier));
  clone.executive_summary = prepareCustomerProseDeep(clone.executive_summary);
  clone.key_findings = prepareCustomerProseDeep(clone.key_findings);
  clone.findings = prepareCustomerProseDeep(clone.findings);
  clone.sections = prepareCustomerProseDeep(clone.sections);
  clone.open_questions = prepareCustomerProseDeep(clone.open_questions);
  clone.appendices = prepareCustomerProseDeep(clone.appendices);
  if (clone.methodology_summary) {
    clone.methodology_summary = prepareCustomerProse(clone.methodology_summary);
  }
  if (Array.isArray(clone.research_methods)) {
    clone.research_methods = clone.research_methods.map((m) => prepareCustomerProse(String(m)));
  }
  if (Array.isArray(clone.sources)) {
    clone.sources = clone.sources
      .filter((s) => !/webhound\.ai/i.test(String(s.url || "")) && !/\bwebhound\b/i.test(String(s.title || "")))
      .map((s, i) => ({
        ...s,
        number: i + 1,
        title: prepareCustomerProse(s.title || ""),
        publisher: s.publisher ? prepareCustomerProse(s.publisher) : s.publisher,
        source_type: s.source_type ? prepareCustomerProse(String(s.source_type)) : s.source_type,
        note: s.note ? prepareCustomerProse(s.note) : s.note,
        // Never expose provider engine names on customer bibliography rows
        provider: undefined,
      }));
  }
  // Redact research-engine metadata from customer payload (keep internally if needed via raw fixture)
  delete clone.research_provider;
  delete clone.research_run_id;
  delete clone.research_cost_usd;
  if (clone.raw_artifact_reference) {
    clone.raw_artifact_reference = {
      kind: "compiled",
      immutable: true,
    };
  }
  // Strip accidental customer methodology that still names providers after rewrite
  if (clone.methodology_summary && /\bwebhound\b|\bcensus\b|\bfixture\b/i.test(clone.methodology_summary)) {
    clone.methodology_summary =
      "Full Hotel Intelligence Investigation compiled from government, first-party, press, operator, and professional-profile sources. Findings remain research-grade until independently reviewed.";
  }
  return clone;
}
