/**
 * Packet 2.6C-R4 — Report-type contracts.
 * One publishing system; multiple content contracts.
 * Full HI stays strict. Research Addenda use template-specific contracts.
 */

import {
  DOSSIER_TYPE,
  DOSSIER_TYPE_RESEARCH_ADDENDUM,
} from "./statuses.js";
import { evaluateFullInvestigationQuality } from "./full-investigation-quality.js";
import { validateDossier } from "./schema.js";

function countWords(text) {
  return String(text || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

function sectionWordCount(sec) {
  let n = 0;
  for (const b of sec?.blocks || []) {
    if (b.type === "paragraphs") n += countWords((b.paragraphs || []).join(" "));
    else if (b.type === "list") n += countWords((b.items || []).join(" "));
    else if (b.type === "para" || b.type === "paragraph" || b.type === "heading") {
      n += countWords(b.text);
    } else if (b.type === "finding_callout") {
      n += countWords(`${b.headline || ""} ${b.body || ""} ${b.explanation || ""}`);
    } else if (b.type === "table") {
      const cells = (b.rows || []).flatMap((row) => (Array.isArray(row) ? row : Object.values(row || {})));
      n += countWords([...(b.headers || []), ...cells].join(" "));
    } else n += countWords(JSON.stringify(b));
  }
  return n;
}

/** Generic Research Addendum baseline (all templates). */
export const GENERIC_ADDENDUM_REQUIRED_SECTIONS = Object.freeze([
  "executive_answer",
  "research_question",
  "key_findings",
  "open_questions",
  "sources_evidence",
]);

export const CHANGE_OPPORTUNITY_REQUIRED_SECTIONS = Object.freeze([
  "executive_answer",
  "why_now",
  "recent_change_triggers",
  "asset_product_risk",
  "product_investment_capex",
  "operating_quality",
  "operator_management_stability",
  "what_could_derail",
  "deal_risk_flags",
  "what_looks_stable",
  "what_to_verify_next",
  "open_questions",
  "sources_evidence",
]);

export const DECISION_AUTHORITY_REQUIRED_SECTIONS = Object.freeze([
  "executive_answer",
  "decision_map",
  "people_decision_authority",
  "authority_evidence",
  "professional_profiles",
  "contact_paths",
  "open_questions",
  "sources_evidence",
]);

export const BRAND_OPERATOR_AGREEMENT_REQUIRED_SECTIONS = Object.freeze([
  "executive_answer",
  "current_structure",
  "brand_history",
  "operator_history",
  "agreement_evidence",
  "change_signals",
  "open_questions",
  "sources_evidence",
]);

export const OWNERSHIP_CAPITAL_EVENTS_REQUIRED_SECTIONS = Object.freeze([
  "executive_answer",
  "current_ownership",
  "recent_ownership_events",
  "financing_capital_events",
  "transaction_signals",
  "entity_control_changes",
  "open_questions",
  "sources_evidence",
]);

export const REPOSITIONING_DEVELOPMENT_REQUIRED_SECTIONS = Object.freeze([
  "executive_answer",
  "current_product_position",
  "renovation_capex",
  "development_expansion",
  "repositioning_signals",
  "open_questions",
  "sources_evidence",
]);

export const OWNER_PORTFOLIO_REQUIRED_SECTIONS = Object.freeze([
  "executive_answer",
  "owner_organization",
  "portfolio",
  "owned_controlled_assets",
  "pipeline_development",
  "portfolio_change_signals",
  "open_questions",
  "sources_evidence",
]);

/**
 * Report contract registry.
 */
export const REPORT_CONTRACTS = Object.freeze({
  FULL_HOTEL_INTELLIGENCE_INVESTIGATION: {
    report_type: DOSSIER_TYPE,
    validate: (dossier, opts) => evaluateFullInvestigationQuality(dossier, opts),
  },
  RESEARCH_ADDENDUM: {
    report_type: DOSSIER_TYPE_RESEARCH_ADDENDUM,
    templates: Object.freeze({
      CHANGE_OPPORTUNITY: {
        template_id: "CHANGE_OPPORTUNITY",
        required_sections: CHANGE_OPPORTUNITY_REQUIRED_SECTIONS,
        aliases: {
          operating_quality_management_risk: "operating_quality",
          opportunity_thesis: "why_now",
        },
      },
      DECISION_AUTHORITY: {
        template_id: "DECISION_AUTHORITY",
        required_sections: DECISION_AUTHORITY_REQUIRED_SECTIONS,
      },
      BRAND_OPERATOR_AGREEMENT: {
        template_id: "BRAND_OPERATOR_AGREEMENT",
        required_sections: BRAND_OPERATOR_AGREEMENT_REQUIRED_SECTIONS,
      },
      OWNERSHIP_CAPITAL_EVENTS: {
        template_id: "OWNERSHIP_CAPITAL_EVENTS",
        required_sections: OWNERSHIP_CAPITAL_EVENTS_REQUIRED_SECTIONS,
      },
      REPOSITIONING_DEVELOPMENT: {
        template_id: "REPOSITIONING_DEVELOPMENT",
        required_sections: REPOSITIONING_DEVELOPMENT_REQUIRED_SECTIONS,
      },
      OWNER_PORTFOLIO: {
        template_id: "OWNER_PORTFOLIO",
        required_sections: OWNER_PORTFOLIO_REQUIRED_SECTIONS,
      },
    }),
  },
});

function resolveSection(sections, id, aliases = {}) {
  const direct = sections.find((s) => s && s.id === id);
  if (direct) return direct;
  for (const [alias, target] of Object.entries(aliases)) {
    if (target === id) {
      const hit = sections.find((s) => s && s.id === alias);
      if (hit) return hit;
    }
  }
  // Reverse: if required id is alias of present section
  for (const [alias, target] of Object.entries(aliases)) {
    if (alias === id) {
      const hit = sections.find((s) => s && s.id === target);
      if (hit) return hit;
    }
  }
  return null;
}

/**
 * Multidimensional Research Addendum quality — NOT Full HI word-count.
 */
export function evaluateResearchAddendumQuality(dossier, options = {}) {
  const errors = [];
  const dimensions = {};
  const templateId = String(dossier?.template_id || options.template_id || "").trim();
  const contract =
    REPORT_CONTRACTS.RESEARCH_ADDENDUM.templates[templateId] ||
    {
      template_id: templateId || "UNKNOWN",
      required_sections: GENERIC_ADDENDUM_REQUIRED_SECTIONS,
      aliases: {},
    };

  if (!dossier || typeof dossier !== "object") {
    return { ok: false, errors: ["dossier_missing"], dimensions: {}, wordCount: 0 };
  }

  dimensions.DOSSIER_TYPE_VALID = dossier.dossier_type === DOSSIER_TYPE_RESEARCH_ADDENDUM;
  if (!dimensions.DOSSIER_TYPE_VALID) errors.push("dossier_type_invalid_for_addendum");

  dimensions.TEMPLATE_ID_PRESENT = Boolean(templateId);
  if (!dimensions.TEMPLATE_ID_PRESENT) errors.push("template_id_required");

  dimensions.IDENTITY_PRESENT = Boolean(
    dossier.hotel_name && (dossier.hotel_id || dossier.hotel_airtable_record_id)
  );
  if (!dimensions.IDENTITY_PRESENT) errors.push("identity_incomplete");

  const shape = validateDossier(dossier);
  // Ignore Full-HI-only unknown section noise if any slipped; schema already allows addendum ids.
  for (const e of shape.errors || []) {
    if (e === "dossier_type_invalid") continue; // handled above with addendum-specific message
    errors.push(e);
  }

  const sections = dossier.sections || [];
  const chapterCoverage = {};
  const required = contract.required_sections || GENERIC_ADDENDUM_REQUIRED_SECTIONS;
  const aliases = contract.aliases || {};
  let covered = 0;
  for (const id of required) {
    const sec = resolveSection(sections, id, aliases);
    const words = sec ? sectionWordCount(sec) : 0;
    const nonEmpty = words >= 12;
    const hasRenderableBody = Boolean(
      sec &&
        (sec.blocks || []).some((b) => {
          if (!b) return false;
          if (b.type === "paragraphs") return (b.paragraphs || []).some((p) => String(p || "").trim());
          if (b.type === "para" || b.type === "paragraph") return Boolean(String(b.text || "").trim());
          if (b.type === "list") return (b.items || []).some((p) => String(p || "").trim());
          if (b.type === "finding_callout") return Boolean(String(b.headline || b.body || "").trim());
          if (b.type === "table") return Array.isArray(b.rows) && b.rows.length > 0;
          if (b.type === "heading") return false;
          return true;
        })
    );
    chapterCoverage[id] = { present: Boolean(sec), words, nonEmpty, hasRenderableBody };
    if (!sec) errors.push(`missing_addendum_section:${id}`);
    else if (!nonEmpty || !hasRenderableBody) {
      errors.push(`EMPTY_REQUIRED_SECTION:${id}`);
      errors.push(`thin_addendum_section:${id}:${words}`);
    } else covered += 1;
  }
  dimensions.TEMPLATE_SECTION_COVERAGE = covered / Math.max(required.length, 1);
  dimensions.TEMPLATE_SECTIONS_COMPLETE = covered === required.length;

  const findings = dossier.key_findings || dossier.findings || [];
  const sources = dossier.sources || [];
  const sourceCount = Number(dossier.source_count || sources.length || 0);
  const findingCount = Number(dossier.finding_count || findings.length || 0);

  dimensions.FINDINGS_PRESENT = findingCount >= 1;
  dimensions.SOURCES_PRESENT = sourceCount >= 1;
  if (!dimensions.FINDINGS_PRESENT) errors.push("findings_missing");
  if (!dimensions.SOURCES_PRESENT) errors.push("sources_missing");

  const exec = dossier.executive_summary?.paragraphs || [];
  const execSec = resolveSection(sections, "executive_answer", aliases);
  dimensions.EXECUTIVE_ANSWER_PRESENT =
    exec.length >= 1 || (execSec && sectionWordCount(execSec) >= 40);
  if (!dimensions.EXECUTIVE_ANSWER_PRESENT) errors.push("executive_answer_thin");

  const words = countWords(
    JSON.stringify({
      executive_summary: dossier.executive_summary,
      key_findings: findings,
      sections,
    })
  );
  // Soft depth signal only — never a hard Full-HI 3500 gate.
  dimensions.SUBSTANTIVE_DEPTH_WORDS = words;
  dimensions.DEPTH_CREDIBLE_FOR_SOURCES =
    sourceCount < 8 ? words >= 400 : sourceCount < 20 ? words >= 800 : words >= 1000;
  // Soft warning dimension — do not hard-fail solely on word count for addenda.
  if (!dimensions.DEPTH_CREDIBLE_FOR_SOURCES && options.strictDepth === true) {
    errors.push(`addendum_depth_thin:${words}:sources=${sourceCount}`);
  }

  // Client-safe: forbid provider leakage in customer fields
  const blob = JSON.stringify({
    title: dossier.title,
    executive_summary: dossier.executive_summary,
    key_findings: findings,
    sections,
  });
  const leakPatterns = [
    /\bwebhound\b/i,
    /\$5(\.00)?\b/,
    /\bprovider[_ ]?run[_ ]?id\b/i,
    /\bauto[_-]?promote\b/i,
    /\bclaim[_-]?handoff\b/i,
  ];
  dimensions.CLIENT_SAFE = !leakPatterns.some((re) => re.test(blob));
  if (!dimensions.CLIENT_SAFE) errors.push("client_unsafe_content");

  // R4.2 — location, citation dump, truncated findings, bibliography
  const locLabel = String(
    dossier.hotel_location?.label ||
      [dossier.hotel_location?.city, dossier.hotel_location?.country].filter(Boolean).join(", ") ||
      ""
  );
  const locationPendingLiteral = /location\s+pending/i.test(locLabel);
  // Require a real location for Change & Opportunity (canonical hotel context).
  // Other templates may omit location until hotel seed is attached.
  dimensions.NO_LOCATION_PENDING =
    !locationPendingLiteral &&
    (templateId !== "CHANGE_OPPORTUNITY" || Boolean(locLabel));
  if (!dimensions.NO_LOCATION_PENDING) errors.push("NO_LOCATION_PENDING");

  const execText = (exec || []).join("\n");
  const execSecText = execSec
    ? (execSec.blocks || [])
        .map((b) =>
          b.type === "paragraphs"
            ? (b.paragraphs || []).join("\n")
            : b.text || b.body || ""
        )
        .join("\n")
    : "";
  const leadingCiteDump =
    /^(?:\[\d{1,3}\])+\s/m.test(String(execText).trim()) ||
    /^(?:\[\d{1,3}\])+\s/m.test(String(execSecText).trim()) ||
    /^(?:\[\d{1,3}\])+\n/m.test(String(execText).trim());
  dimensions.NO_CITATION_PREFIX_DUMP = !leadingCiteDump;
  if (!dimensions.NO_CITATION_PREFIX_DUMP) errors.push("NO_CITATION_PREFIX_DUMP");

  const legacyClip = (dossier.key_findings || []).some((f) =>
    /\bBreathless\s+conv$/i.test(String(f.headline || "").trim())
  );
  dimensions.NO_TRUNCATED_FINDINGS = !legacyClip;
  if (legacyClip) errors.push("NO_TRUNCATED_FINDINGS");

  const sourcesSec = resolveSection(sections, "sources_evidence", aliases);
  const bibRows =
    (sourcesSec?.blocks || []).find((b) => b.type === "table" && b.table_kind === "sources")
      ?.rows || [];
  dimensions.REAL_SOURCE_BIBLIOGRAPHY =
    bibRows.length >= 1 || sourceCount >= 1;
  if (bibRows.length < 1 && sourceCount >= 1) {
    errors.push("REAL_SOURCE_BIBLIOGRAPHY_MISSING_TABLE");
  }

  const criticalFail = errors.some((e) =>
    /dossier_missing|dossier_type_invalid|identity_incomplete|missing_addendum_section|EMPTY_REQUIRED_SECTION|findings_missing|sources_missing|executive_answer_thin|client_unsafe|NO_LOCATION_PENDING|NO_CITATION_PREFIX_DUMP|NO_TRUNCATED_FINDINGS|REAL_SOURCE_BIBLIOGRAPHY/.test(
      e
    )
  );

  return {
    ok: errors.length === 0,
    soft_ok: !criticalFail && dimensions.TEMPLATE_SECTIONS_COMPLETE,
    errors,
    dimensions,
    chapterCoverage,
    wordCount: words,
    sourceCount,
    findingCount,
    template_id: templateId,
    report_type: DOSSIER_TYPE_RESEARCH_ADDENDUM,
    contract: contract.template_id,
    thresholds: { hard_min_substantive_words: null, note: "addendum_not_wordcount_gated" },
  };
}

/**
 * Unified entry: dispatch by report / dossier type.
 */
export function validateIntelligenceReport(report, options = {}) {
  if (!report || typeof report !== "object") {
    return { ok: false, errors: ["dossier_missing"], report_type: null };
  }
  const type = String(report.dossier_type || report.report_type || "").toUpperCase();

  if (type === DOSSIER_TYPE || type === "FULL_INVESTIGATION") {
    const q = evaluateFullInvestigationQuality(report, options);
    return { ...q, report_type: DOSSIER_TYPE, dispatched: "FULL_HOTEL_INTELLIGENCE_INVESTIGATION" };
  }

  if (type === DOSSIER_TYPE_RESEARCH_ADDENDUM || type === "RESEARCH_ADDENDUM") {
    const q = evaluateResearchAddendumQuality(report, options);
    return { ...q, report_type: DOSSIER_TYPE_RESEARCH_ADDENDUM, dispatched: "RESEARCH_ADDENDUM" };
  }

  return {
    ok: false,
    errors: [`unknown_report_type:${type || "missing"}`],
    report_type: type || null,
    dispatched: null,
  };
}

/** Customer-safe error surface — never expose engineering codes. */
export function customerReportValidationMessage(result, { founderDebug = false } = {}) {
  if (result?.ok) return null;
  if (founderDebug) {
    return {
      title: "Report validation failed (founder debug)",
      message: "Internal diagnostics available.",
      codes: result?.errors || [],
    };
  }
  return {
    title: "This report is not yet ready",
    message: "Report processing is incomplete. Please try again shortly.",
    codes: [],
  };
}
