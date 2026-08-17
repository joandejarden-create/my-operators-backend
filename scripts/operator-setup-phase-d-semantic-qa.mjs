#!/usr/bin/env node
/**
 * Phase D Semantic QA + Selective Repair planning (NO Airtable writes).
 *
 *   node scripts/operator-setup-phase-d-semantic-qa.mjs
 *
 * Freeze: does not run Phase D writers, Fit remap, or apply repairs.
 */
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const BACKUP = join(ROOT, "backups/operator-setup/phase-d/2026-08-10T17-30-21");
const PLAN_PATH = join(ROOT, "data/operator-setup/phase-d/production-section-write-plan.json");
const OUT = join(ROOT, "data/operator-setup/phase-d-repair");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");

const GOLDENS = new Set(["recF5Z87OAqFgndoq", "recWPKu5laVZxsvpn"]);
const PILOT = [
  { id: "recWPKu5laVZxsvpn", name: "Hotel Equities (CALA)" },
  { id: "recF5Z87OAqFgndoq", name: "Arbor Lodging (CALA)" },
  { id: null, name: "Highgate", match: /Highgate/i },
  { id: null, name: "Aimbridge", match: /Aimbridge/i },
  { id: null, name: "Marriott", match: /Marriott International \(Managed\)/i },
  { id: null, name: "OxoHotel", match: /OxoHotel/i },
];

const TABLE_FILE = {
  "Operator Setup - Profile & Positioning": "Operator_Setup_Profile_Positioning.json",
  "Operator Setup - Platform & Markets": "Operator_Setup_Platform_Markets.json",
  "Operator Setup - Commercial Fit & Terms": "Operator_Setup_Commercial_Fit_Terms.json",
  "Operator Setup - Governance, Delivery & Diligence": "Operator_Setup_Governance_Delivery_Diligence.json",
  "Operator Setup - Leadership Platform": "Operator_Setup_Leadership_Platform.json",
  "Operator Setup - Engagement & Reporting": "Operator_Setup_Engagement_Reporting.json",
  "Operator Setup - Infrastructure Platform": "Operator_Setup_Infrastructure_Platform.json",
  "Operator Setup - Operating Platform": "Operator_Setup_Operating_Platform.json",
  "Operator Setup - Brand Relationships": "Operator_Setup_Brand_Relationships.json",
  "Operator Setup - Explorer Materials": "Operator_Setup_Explorer_Materials.json",
  "Operator Setup - Leadership Team Members": "Operator_Setup_Leadership_Team_Members.json",
};

const NARRATIVE_FIELDS = new Set([
  "cap_profile_operational",
  "cap_profile_commercial",
  "cap_profile_transition",
  "specificMarkets",
  "ownerEngagementNarrative",
  "specializations",
  "ov_card_commercial",
  "ov_card_flexibility",
  "infra_systems_technology",
  "infra_asset_management_reporting",
  "risk_programs_narrative",
  "companyDescription",
  "differentiators",
  "companyTagline",
  "companyHistory",
  "managementPhilosophy",
  "overview_why_1_story",
]);

const STRUCTURED_FIELDS = new Set([
  "headquarters",
  "website",
  "companySize",
  "primaryServiceModel",
  "brand_signal_reflag",
  "brand_signal_franchise_align",
  "Management Structures Supported",
  "yearEstablished",
  "Active Countries",
]);

const SCAFFOLD_FIELDS = new Set([
  "overview_bestat_1_headline",
  "overview_bestat_2_headline",
  "overview_bestat_3_headline",
  "overview_why_1_headline",
  "overview_why_2_headline",
  "overview_why_3_headline",
]);

const GENERIC_PHRASES = [
  /owner engagement should be underwritten/i,
  /systems (are typically|vary)/i,
  /confirm (PMS|technology|coverage|bilingual)/i,
  /no unverified scorecards/i,
  /commercial engine details remain/i,
  /counts and portfolio mix percentages are not inferred/i,
  /underwrite scale and capabilities/i,
  /differentiation claims beyond this evidence are not asserted/i,
  /monthly operating reviews and variance reporting are common market practice/i,
  /standard third-party\/brand-managed practice/i,
  /this indicates market exposure, not a verified/i,
  /strong operating platform/i,
  /owner-focused approach/i,
  /flexible operating model/i,
  /comprehensive reporting/i,
  /experienced management team/i,
  /strong risk management/i,
  /tailored solutions/i,
];

function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2) + "\n");
}
function writeMd(p, t) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, t.endsWith("\n") ? t : t + "\n");
}
function nz(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.map(String).join("; ");
  if (typeof v === "object") return JSON.stringify(v);
  return String(v).trim();
}
function isPopulated(v) {
  if (v == null) return false;
  if (typeof v === "boolean" || typeof v === "number") return true;
  if (typeof v === "string") return v.trim().length > 0;
  if (Array.isArray(v)) return v.length > 0;
  return Boolean(v);
}
function loadBackupTable(tableName) {
  const file = TABLE_FILE[tableName];
  if (!file) return [];
  const p = join(BACKUP, file);
  if (!existsSync(p)) return [];
  return JSON.parse(readFileSync(p, "utf8")).records || [];
}
function findBackupRow(tableName, recordId, masterId) {
  const rows = loadBackupTable(tableName);
  if (recordId) {
    const byId = rows.find((r) => r.id === recordId);
    if (byId) return byId;
  }
  if (masterId) return rows.find((r) => (r.fields?.Operator || []).includes(masterId)) || null;
  return null;
}
function normalizeFingerprint(text, companyName = "") {
  let t = nz(text).toLowerCase();
  if (companyName) t = t.split(companyName.toLowerCase()).join("[OPERATOR]");
  t = t
    .replace(/\b(mexico|dominican republic|jamaica|panama|colombia|united states|argentina|brazil|costa rica|puerto rico|chile|peru|ecuador|guatemala)\b/gi, "[GEO]")
    .replace(/\b(marriott|hilton|hyatt|ihg|accor|wyndham|intercontinental|holiday inn|hampton|waldorf|conrad|kimpton|curio|rosewood|four seasons|mandarin|shangri-la|sonesta|meli[aá]|barcel[oó]|iberostar|aimbridge|highgate)\b/gi, "[BRAND]")
    .replace(/\b\d+\b/g, "[N]")
    .replace(/\s+/g, " ")
    .trim();
  return t.slice(0, 400);
}
function classifyMutationKind(m) {
  if (m.create && /Leadership Platform|Engagement|Infrastructure/i.test(m.table)) return "SCAFFOLD / PRESENTATION";
  if (SCAFFOLD_FIELDS.has(m.field) || /^overview_/i.test(m.field)) return "SCAFFOLD / PRESENTATION";
  if (STRUCTURED_FIELDS.has(m.field) || m.field === "Management Structures Supported") return "STRUCTURED FACT";
  if (NARRATIVE_FIELDS.has(m.field) || m.field === "_row") return "NARRATIVE / RESEARCHED SUMMARY";
  if (/phase_d_|Team Market|Language|Governance Cadence/i.test(m.field) || m.create) return "NARRATIVE / RESEARCHED SUMMARY";
  if (/signal|headline|tagline/i.test(m.field)) return "SCAFFOLD / PRESENTATION";
  // pack deepen leftovers
  if (m.writer === "profile-deepen") {
    if (STRUCTURED_FIELDS.has(m.field)) return "STRUCTURED FACT";
    if (SCAFFOLD_FIELDS.has(m.field)) return "SCAFFOLD / PRESENTATION";
    return "DERIVED STRUCTURED SUMMARY";
  }
  return "NARRATIVE / RESEARCHED SUMMARY";
}
function isGenericText(text) {
  const s = nz(text);
  if (!s) return true;
  return GENERIC_PHRASES.some((re) => re.test(s));
}
function evidenceFidelity(m, kind) {
  if (kind === "STRUCTURED FACT") {
    if (m.writer === "profile-deepen" || m.writer?.includes("commercial") && m.field === "Management Structures Supported")
      return "DIRECTLY SUPPORTED";
    return "REASONABLE SYNTHESIS";
  }
  if (kind === "SCAFFOLD / PRESENTATION") return "WEAK INFERENCE";
  // narratives from OE assignment counts / brands
  if (/cap_profile_commercial|specializations|specificMarkets/i.test(m.field)) return "REASONABLE SYNTHESIS";
  if (/cap_profile_operational|cap_profile_transition/i.test(m.field)) return "WEAK INFERENCE";
  if (/ownerEngagement|infra_|risk_|ov_card|companyDescription|differentiators/i.test(m.field)) return "UNSUPPORTED / GENERIC";
  if (m.create && /Engagement|Infrastructure|Leadership/i.test(m.table)) return "UNSUPPORTED / GENERIC";
  if (m.field === "_row") return "UNSUPPORTED / GENERIC";
  return "WEAK INFERENCE";
}
function duplicationClass(fingerprint, clusterSize) {
  if (clusterSize >= 8) return "HIGH DUPLICATION";
  if (clusterSize >= 3) return "TEMPLATE VARIATION";
  if (isGenericText(fingerprint)) return "GENERIC TABLE STAKES";
  return "DISTINCTIVE";
}
function scoreVerdict({ m, kind, fidelity, dupClass, preValue, blankBefore }) {
  // Structured pack facts: KEEP
  if (kind === "STRUCTURED FACT" && fidelity === "DIRECTLY SUPPORTED") {
    return { verdict: "KEEP", reason: "structured_fact_from_pack_or_assignments" };
  }
  if (m.field === "Management Structures Supported") {
    return { verdict: "KEEP", reason: "derived_from_assignment_structures" };
  }
  // Scaffold UI headlines from packs — HOLD for product decision (not company truth)
  if (kind === "SCAFFOLD / PRESENTATION" && m.writer === "profile-deepen") {
    return { verdict: "HOLD", reason: "explorer_scaffold_headline_not_company_truth" };
  }
  // Created section rows with template bodies
  if (m.create && /Leadership Platform|Engagement|Infrastructure/i.test(m.table)) {
    return { verdict: "CLEAR TO BLANK", reason: "phase_d_template_section_row_delete" };
  }
  // Created 1:1 rows that are template-only — CLEAR narrative fields (row may remain if other fields exist)
  if (m.create && m.field === "_row") {
    return { verdict: "CLEAR TO BLANK", reason: "phase_d_created_row_template_payload" };
  }
  // Narrative templates
  if (kind === "NARRATIVE / RESEARCHED SUMMARY") {
    if (!blankBefore && isPopulated(preValue) && nz(preValue) !== nz(m.proposedValue)) {
      return { verdict: "RESTORE", reason: "pre_d_value_preferable_phase_d_overwrote" };
    }
    // Assignment hotel-type dumps are not company specializations
    if (m.field === "specializations") {
      return { verdict: "CLEAR TO BLANK", reason: "assignment_hotel_type_is_not_company_specialization" };
    }
    if (fidelity === "UNSUPPORTED / GENERIC" || dupClass === "HIGH DUPLICATION" || dupClass === "TEMPLATE VARIATION") {
      return { verdict: "CLEAR TO BLANK", reason: `semantic_fail:${dupClass}:${fidelity}` };
    }
    if (fidelity === "WEAK INFERENCE" || dupClass === "GENERIC TABLE STAKES") {
      return { verdict: "CLEAR TO BLANK", reason: `semantic_fail:${dupClass}:${fidelity}` };
    }
    if (fidelity === "REASONABLE SYNTHESIS" && dupClass === "DISTINCTIVE") {
      // Defer rewrite until field handlers exist — do not leave template text
      return { verdict: "CLEAR TO BLANK", reason: "rewrite_deferred_clear_until_writer_v2" };
    }
    return { verdict: "CLEAR TO BLANK", reason: "default_narrative_clear" };
  }
  if (kind === "SCAFFOLD / PRESENTATION") {
    return { verdict: "CLEAR TO BLANK", reason: "scaffold_template_row" };
  }
  return { verdict: "HOLD", reason: "unclassified" };
}

/** Field semantic contracts for Phase-D-touched narrative/retained fields */
function buildFieldContracts(exemplarsByField) {
  const defs = [
    {
      fieldName: "cap_profile_operational",
      table: "Operator Setup - Platform & Markets",
      question: "How does this operator organize and execute day-to-day hotel operations?",
      belongs: "Operating model, regional ops accountability, SOPs/labor/guest experience posture, local leadership model",
      notBelongs: "Brand lists, assignment counts as the main claim, commercial mix, diligence disclaimers",
      form: "1–3 concise operational sentences; company-specific",
      length: "40–280 chars typical; up to ~500",
      specificity: "high",
      evidence: "Official ops materials, known regional structure, verified operating model — not Assignment count alone",
      inference: false,
      blankRule: "Blank if no ops-organization evidence",
      adjacent: "cap_profile_commercial (commercial engine), cap_profile_transition (openings/reflags)",
      recommend: "NARROW — require ops evidence or blank",
    },
    {
      fieldName: "cap_profile_commercial",
      table: "Operator Setup - Platform & Markets",
      question: "How does the operator win revenue / commercialize assets?",
      belongs: "Sales/RM/distribution posture, brand commercial dependency, owner-relevant commercial model",
      notBelongs: "Raw brand name dumps, portfolio % caveats, operating SOPs",
      form: "1–3 commercial sentences",
      length: "40–280",
      specificity: "high",
      evidence: "Documented commercial organization or brand-dependent commercial path",
      inference: false,
      blankRule: "Blank if only brand list known",
      adjacent: "ov_card_commercial, specializations",
      recommend: "NARROW",
    },
    {
      fieldName: "cap_profile_transition",
      table: "Operator Setup - Platform & Markets",
      question: "What is the operator’s opening / conversion / transition capability?",
      belongs: "Pre-opening, reflag, conversion, takeover process — only if evidenced",
      notBelongs: "Restating Development Context enums as capability claims",
      form: "1–2 sentences or blank",
      evidence: "Documented transition programs or verified case examples",
      inference: false,
      blankRule: "Blank unless transition capability evidenced beyond Development Context tags",
      recommend: "NARROW or MOVE TO CLAIMS",
    },
    {
      fieldName: "ownerEngagementNarrative",
      table: "Operator Setup - Commercial Fit & Terms",
      question: "How does the operator engage owners (cadence, decision rights, relationship model)?",
      belongs: "Owner communication model, reporting rhythm, asset-management interface — company-specific",
      notBelongs: "Generic 'underwrite from MA' disclaimers; Management Availability select restated as prose",
      form: "Owner-relevance narrative; specific mechanisms when known",
      evidence: "Operator materials, owner case studies, verified reporting model",
      inference: false,
      blankRule: "Blank if unknown — NEVER fill with diligence boilerplate",
      adjacent: "infra_asset_management_reporting, Engagement section",
      recommend: "NARROW — TARGETED RESEARCH or blank",
    },
    {
      fieldName: "specializations",
      table: "Operator Setup - Commercial Fit & Terms",
      question: "What asset/situation specializations does the company credibly claim?",
      belongs: "Documented specializations (AI resort, urban full-service, etc.)",
      notBelongs: "One-off hotel-type strings from a thin Assignment sample framed as company specialization",
      form: "Short list or sentence of company-level specializations",
      evidence: "Company materials or strong portfolio pattern (≥ threshold)",
      inference: "weak_ok_if_strong_pattern",
      blankRule: "Blank if sample too thin",
      recommend: "STRUCTURE AS SELECT or NARROW",
    },
    {
      fieldName: "ov_card_commercial",
      table: "Operator Setup - Commercial Fit & Terms",
      question: "Explorer card: commercial value headline for owners",
      belongs: "Short distinctive commercial positioning for UI",
      notBelongs: "Multi-brand evidence boilerplate",
      form: "UI card body",
      evidence: "Same as commercial narrative or pack",
      inference: false,
      blankRule: "Blank preferred to generic",
      recommend: "DEPRECATE as Setup truth / keep as presentation only",
    },
    {
      fieldName: "ov_card_flexibility",
      table: "Operator Setup - Commercial Fit & Terms",
      question: "Explorer card: flexibility / deal-structure posture",
      belongs: "Documented flexibility posture",
      notBelongs: "Development Context enum dump",
      form: "UI card body",
      inference: false,
      blankRule: "Blank if unknown",
      recommend: "DEPRECATE as Setup truth",
    },
    {
      fieldName: "infra_systems_technology",
      table: "Operator Setup - Governance, Delivery & Diligence",
      question: "What technology / systems stack does the operator use or depend on?",
      belongs: "Named or classed systems (PMS/RMS/CRS/BI) or explicit brand-dependent model with brands named",
      notBelongs: "Generic 'systems vary; confirm in diligence'",
      form: "Structured bullets or short systems map (see HE exemplar)",
      evidence: "Operator/IT materials or verified brand-dependent statement",
      inference: false,
      blankRule: "Blank unless systems posture actually known",
      recommend: "NARROW — TARGETED RESEARCH",
    },
    {
      fieldName: "infra_asset_management_reporting",
      table: "Operator Setup - Governance, Delivery & Diligence",
      question: "How does the operator report to owners / asset managers?",
      belongs: "Cadence, portal, packages, SSC/finance model — specific",
      notBelongs: "Restating Management Availability + market-practice boilerplate",
      form: "1–3 specific reporting sentences",
      evidence: "Documented reporting model",
      inference: false,
      blankRule: "Blank if unknown",
      recommend: "NARROW — TARGETED RESEARCH",
    },
    {
      fieldName: "risk_programs_narrative",
      table: "Operator Setup - Governance, Delivery & Diligence",
      question: "What risk / insurance / control programs exist?",
      belongs: "Named programs or verified control environment",
      notBelongs: "Diligence disclaimers about scorecards",
      form: "Short factual narrative or blank",
      inference: false,
      blankRule: "Blank if unknown",
      recommend: "MOVE TO CLAIMS or DEPRECATE if unsupportable at scale",
    },
    {
      fieldName: "companyDescription",
      table: "Operator Setup - Profile & Positioning",
      question: "Who is this company in owner-relevant terms?",
      belongs: "Identity, footprint model, what they operate, geography — researched prose",
      notBelongs: "OE assignment-count meta descriptions",
      form: "2–5 sentences company description",
      evidence: "Official site / filings / packs",
      inference: false,
      blankRule: "Prefer pack/research; blank better than OE meta",
      recommend: "KEEP field; CLEAR Phase-D OE meta writes",
    },
    {
      fieldName: "differentiators",
      table: "Operator Setup - Profile & Positioning",
      question: "What meaningfully differentiates this operator for owners?",
      belongs: "True differentiators with evidence",
      notBelongs: "Brand list restated as differentiation",
      form: "Short differentiator bullets/sentences",
      inference: false,
      blankRule: "Blank if unknown",
      recommend: "NARROW",
    },
  ];

  return defs.map((d) => {
    const ex = exemplarsByField[d.fieldName] || { tier1: [], tier2: [], tier3: [] };
    return {
      ...d,
      fieldId: null, // filled when meta available
      fieldType: "multilineText",
      expectedAnswerForm: d.form,
      expectedLength: d.length || "variable",
      appropriateLevelOfSpecificity: d.specificity || "high",
      evidenceRequired: d.evidence,
      inferencePermitted: d.inference === true || d.inference === "weak_ok_if_strong_pattern",
      currentHistoricalRule: "current company posture only",
      blankRule: d.blankRule,
      naRule: "Use N/A only when field genuinely inapplicable to operating model",
      adjacentFields: d.adjacent,
      strongExamples: [...(ex.tier2 || []).slice(0, 2), ...(ex.tier1 || []).slice(0, 2)],
      weakGenericExamples: [
        "Owner engagement should be underwritten from the management agreement…",
        "Systems vary by asset… confirm in diligence…",
        "Multi-brand operating evidence present (…).",
      ],
      sourceHierarchy: [
        "official operator/corporate",
        "investor filings",
        "verified packs",
        "Claims with High confidence",
        "OE Assignments (facts only, not capability prose)",
      ],
      exemplarTiers: ex,
      onlyFixtureExamples: !ex.tier1?.length && !ex.tier2?.length && (ex.tier3?.length || 0) > 0,
    };
  });
}

function extractExemplars(backupTables) {
  const fields = [
    "cap_profile_operational",
    "cap_profile_commercial",
    "cap_profile_transition",
    "ownerEngagementNarrative",
    "specializations",
    "infra_systems_technology",
    "infra_asset_management_reporting",
    "risk_programs_narrative",
    "companyDescription",
    "differentiators",
    "ov_card_commercial",
    "ov_card_flexibility",
  ];
  const masters = JSON.parse(readFileSync(join(BACKUP, "Master.json"), "utf8")).records || [];
  const masterMeta = Object.fromEntries(
    masters.map((m) => [
      m.id,
      {
        name: m.fields["Operator Name"] || m.fields.Name || m.fields.company_name || m.id,
        purpose: m.fields["Record Purpose"] || "",
        fixture: TEST_FIXTURE_MASTER_IDS.includes(m.id),
        golden: GOLDENS.has(m.id),
      },
    ])
  );

  const out = {};
  for (const f of fields) out[f] = { tier1: [], tier2: [], tier3: [] };

  const scan = (tableFile, fieldList) => {
    const rows = JSON.parse(readFileSync(join(BACKUP, tableFile), "utf8")).records || [];
    for (const r of rows) {
      const mid = (r.fields.Operator || [])[0];
      const meta = masterMeta[mid] || {};
      for (const f of fieldList) {
        const v = r.fields[f];
        if (!isPopulated(v) || nz(v).length < 40) continue;
        if (isGenericText(v) && /underwrite|confirm in diligence|evidence includes \d+ current named/i.test(nz(v))) continue;
        const item = { masterId: mid, name: meta.name, value: nz(v).slice(0, 400) };
        if (meta.fixture) out[f]?.tier3.push(item);
        else if (meta.golden) out[f]?.tier2.push(item);
        else if (meta.purpose === "Production") out[f]?.tier1.push(item);
      }
    }
  };

  scan("Operator_Setup_Platform_Markets.json", ["cap_profile_operational", "cap_profile_commercial", "cap_profile_transition"]);
  scan("Operator_Setup_Commercial_Fit_Terms.json", ["ownerEngagementNarrative", "specializations", "ov_card_commercial", "ov_card_flexibility"]);
  scan("Operator_Setup_Governance_Delivery_Diligence.json", [
    "infra_systems_technology",
    "infra_asset_management_reporting",
    "risk_programs_narrative",
  ]);
  scan("Operator_Setup_Profile_Positioning.json", ["companyDescription", "differentiators"]);

  // Cap tier sizes
  for (const f of Object.keys(out)) {
    out[f].tier1 = out[f].tier1.slice(0, 5);
    out[f].tier2 = out[f].tier2.slice(0, 3);
    out[f].tier3 = out[f].tier3.slice(0, 3);
  }
  return out;
}

async function main() {
  if (!existsSync(PLAN_PATH)) throw new Error("Missing Phase D write plan");
  if (!existsSync(BACKUP)) throw new Error("Missing Phase D backup");

  mkdirSync(OUT, { recursive: true });
  const plan = JSON.parse(readFileSync(PLAN_PATH, "utf8"));
  const masters = JSON.parse(readFileSync(join(BACKUP, "Master.json"), "utf8")).records || [];
  const masterById = Object.fromEntries(masters.map((m) => [m.id, m]));

  const exemplars = extractExemplars();
  const contracts = buildFieldContracts(exemplars);

  // ——— Mutation audit ———
  const fingerprints = new Map(); // field -> Map(fp -> count)
  const auditRows = [];

  for (let i = 0; i < plan.mutations.length; i++) {
    const m = plan.mutations[i];
    const master = masterById[m.masterId];
    const purpose = master?.fields?.["Record Purpose"] || "Production";
    const kind = classifyMutationKind(m);
    const backupRow = m.create ? null : findBackupRow(m.table, m.recordId, m.masterId);
    let preValue = null;
    if (!m.create && backupRow && m.field !== "_row") {
      preValue = backupRow.fields?.[m.field] ?? null;
    }
    const blankBefore = !isPopulated(preValue);
    const phaseDValue = m.proposedValue;
    const currentValue = phaseDValue; // applied; Phase D was blank-fill
    const textForFp =
      typeof phaseDValue === "object" && phaseDValue && !Array.isArray(phaseDValue)
        ? nz(phaseDValue.body || phaseDValue.companyDescription || JSON.stringify(phaseDValue))
        : nz(phaseDValue);
    const fp = normalizeFingerprint(textForFp, m.masterName);
    if (!fingerprints.has(m.field)) fingerprints.set(m.field, new Map());
    const fmap = fingerprints.get(m.field);
    fmap.set(fp, (fmap.get(fp) || 0) + 1);

    auditRows.push({
      mutationIndex: i,
      operator: m.masterName,
      masterId: m.masterId,
      table: m.table,
      fieldId: null,
      fieldName: m.field,
      preDValue: preValue,
      phaseDValue,
      currentValue,
      writer: m.writer,
      sourceInputs: m.evidence || null,
      wasBlankBefore: blankBefore,
      kind,
      productionStatus: TEST_FIXTURE_MASTER_IDS.includes(m.masterId)
        ? "Test Fixture"
        : GOLDENS.has(m.masterId)
          ? "Golden/Production"
          : purpose,
      create: Boolean(m.create),
      fingerprint: fp,
      _m: m,
    });
  }

  // second pass verdicts with cluster sizes
  const verdicts = [];
  const verdictCounts = { KEEP: 0, REWRITE: 0, RESTORE: 0, "CLEAR TO BLANK": 0, HOLD: 0 };
  const byFieldStats = {};

  for (const row of auditRows) {
    const fmap = fingerprints.get(row.fieldName) || new Map();
    const clusterSize = fmap.get(row.fingerprint) || 1;
    const dupClass = duplicationClass(row.fingerprint, clusterSize);
    const fidelity = evidenceFidelity(row._m, row.kind);
    const { verdict, reason } = scoreVerdict({
      m: row._m,
      kind: row.kind,
      fidelity,
      dupClass,
      preValue: row.preDValue,
      blankBefore: row.wasBlankBefore,
    });
    verdictCounts[verdict] = (verdictCounts[verdict] || 0) + 1;

    const v = {
      mutationIndex: row.mutationIndex,
      operator: row.operator,
      masterId: row.masterId,
      table: row.table,
      fieldName: row.fieldName,
      kind: row.kind,
      writer: row.writer,
      create: row.create,
      wasBlankBefore: row.wasBlankBefore,
      duplicationClass: dupClass,
      clusterSize,
      evidenceFidelity: fidelity,
      verdict,
      reason,
      phaseDPreview: nz(typeof row.phaseDValue === "object" ? row.phaseDValue?.body || row.phaseDValue : row.phaseDValue).slice(0, 220),
      preDPreview: nz(row.preDValue).slice(0, 120),
    };
    verdicts.push(v);

    const key = `${row.table}::${row.fieldName}`;
    if (!byFieldStats[key]) {
      byFieldStats[key] = {
        table: row.table,
        field: row.fieldName,
        count: 0,
        kinds: {},
        verdicts: {},
        dup: {},
        fidelity: {},
      };
    }
    const s = byFieldStats[key];
    s.count++;
    s.kinds[row.kind] = (s.kinds[row.kind] || 0) + 1;
    s.verdicts[verdict] = (s.verdicts[verdict] || 0) + 1;
    s.dup[dupClass] = (s.dup[dupClass] || 0) + 1;
    s.fidelity[fidelity] = (s.fidelity[fidelity] || 0) + 1;
  }

  // strip _m from audit export
  const auditExport = {
    generatedAt: new Date().toISOString(),
    backup: "backups/operator-setup/phase-d/2026-08-10T17-30-21",
    planVersion: plan.version,
    totalMutations: auditRows.length,
    mutations: auditRows.map(({ _m, ...rest }) => rest),
  };
  writeJson(join(OUT, "phase-d-mutation-audit.json"), auditExport);
  writeJson(join(OUT, "mutation-verdicts.json"), {
    generatedAt: new Date().toISOString(),
    counts: verdictCounts,
    byField: byFieldStats,
    verdicts,
  });
  writeJson(join(OUT, "field-semantic-contract-v2.json"), {
    version: "operator-setup-field-semantic-contract-v2",
    generatedAt: new Date().toISOString(),
    fields: contracts,
  });

  // ——— Duplication report ———
  const narrativeFieldDup = Object.values(byFieldStats)
    .filter((s) => (s.kinds["NARRATIVE / RESEARCHED SUMMARY"] || 0) > 0 || (s.kinds["SCAFFOLD / PRESENTATION"] || 0) > 0)
    .map((s) => {
      const populated = s.count;
      const high = s.dup["HIGH DUPLICATION"] || 0;
      const tmpl = s.dup["TEMPLATE VARIATION"] || 0;
      const generic = s.dup["GENERIC TABLE STAKES"] || 0;
      const distinctive = s.dup["DISTINCTIVE"] || 0;
      const uniqueFp = new Set(
        auditRows.filter((r) => r.fieldName === s.field && r.table === s.table).map((r) => r.fingerprint)
      ).size;
      return {
        ...s,
        uniqueFingerprints: uniqueFp,
        pctGenericLike: Math.round(((high + tmpl + generic) / populated) * 1000) / 10,
        pctDistinctive: Math.round((distinctive / populated) * 1000) / 10,
      };
    })
    .sort((a, b) => b.pctGenericLike - a.pctGenericLike || b.count - a.count);

  writeMd(
    join(REPORTS, "operator-setup-phase-d-cross-company-duplication.md"),
    [
      `# Phase D Cross-Company Duplication Audit`,
      ``,
      `Phase D produced **systemic template variation** on narrative fields. Completeness rose; differentiation did not.`,
      ``,
      `| Field | Populated | Unique FP | HIGH dup | Template | Generic | Distinctive | % generic-like |`,
      `| ----- | --------: | --------: | -------: | -------: | ------: | ----------: | -------------: |`,
      ...narrativeFieldDup.map(
        (s) =>
          `| ${s.field} | ${s.count} | ${s.uniqueFingerprints} | ${s.dup["HIGH DUPLICATION"] || 0} | ${s.dup["TEMPLATE VARIATION"] || 0} | ${s.dup["GENERIC TABLE STAKES"] || 0} | ${s.dup["DISTINCTIVE"] || 0} | ${s.pctGenericLike}% |`
      ),
      ``,
      `## Highest-duplication fields`,
      ``,
      ...narrativeFieldDup
        .filter((s) => s.pctGenericLike >= 70)
        .slice(0, 20)
        .map((s) => `- **${s.field}** (${s.table.replace("Operator Setup - ", "")}): ${s.pctGenericLike}% generic-like across ${s.count} writes`),
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-phase-d-repair-verdict.md"),
    [
      `# Phase D Repair Verdicts (pre-apply)`,
      ``,
      `| Verdict | Count |`,
      `| ------- | ----: |`,
      ...Object.entries(verdictCounts).map(([k, v]) => `| ${k} | ${v} |`),
      ``,
      `**Total audited:** ${auditRows.length}`,
      ``,
      `| Kind | Count |`,
      `| ---- | ----: |`,
      ...Object.entries(
        auditRows.reduce((a, r) => {
          a[r.kind] = (a[r.kind] || 0) + 1;
          return a;
        }, {})
      ).map(([k, v]) => `| ${k} | ${v} |`),
      ``,
      `## By field (top CLEAR)`,
      ``,
      `| Field | CLEAR | KEEP | HOLD | REWRITE | RESTORE |`,
      `| ----- | ----: | ---: | ---: | ------: | ------: |`,
      ...Object.values(byFieldStats)
        .sort((a, b) => (b.verdicts["CLEAR TO BLANK"] || 0) - (a.verdicts["CLEAR TO BLANK"] || 0))
        .slice(0, 40)
        .map(
          (s) =>
            `| ${s.field} | ${s.verdicts["CLEAR TO BLANK"] || 0} | ${s.verdicts.KEEP || 0} | ${s.verdicts.HOLD || 0} | ${s.verdicts.REWRITE || 0} | ${s.verdicts.RESTORE || 0} |`
        ),
      ``,
      `## Policy`,
      ``,
      `- Blank is preferable to generic filler.`,
      `- No Airtable repair applied in this run — classification only.`,
      `- Fit adapter remains blocked.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-phase-d-writer-root-cause.md"),
    [
      `# Phase D Writer Root Cause`,
      ``,
      `## What went wrong`,
      ``,
      `\`phase-d-section-writers.js\` optimized for **section coverage** (fill empty Production sections) rather than **field semantic fidelity**.`,
      ``,
      `| Failure mode | Mechanism |`,
      `| ------------ | --------- |`,
      `| Section-level writers | One OE context packet fed many neighboring fields |`,
      `| Templates | Same sentence frames with operator/geo/brand/count swaps |`,
      `| Evidence stretch | Assignment counts → governance, cadence, tech sophistication |`,
      `| Diligence boilerplate | “Confirm in diligence / underwrite from MA” written as Setup truth |`,
      `| No exemplar gating | Did not compare to HE/Arbor Tier-1/2 real prose |`,
      `| No differentiation test | Did not reject “could apply to five other operators” |`,
      `| Completeness KPI | Empty→Partial/Complete rewarded filler |`,
      `| Create-row flood | Engagement/Infra/Leadership Platform rows cloned thin templates |`,
      ``,
      `## Per-writer assessment`,
      ``,
      `| Writer | Field-specific? | Uses exemplars? | Diff test? | Verdict |`,
      `| ------ | --------------- | --------------- | ---------- | ------- |`,
      `| phase-d-platform | No (3 caps + markets) | No | No | Unsafe for narrative |`,
      `| phase-d-commercial | No | No | No | Unsafe |`,
      `| phase-d-governance | No | No | No | Unsafe |`,
      `| phase-d-engagement | Section rows | No | No | Unsafe — CLEAR rows |`,
      `| phase-d-infrastructure | Section rows | No | No | Unsafe — CLEAR rows |`,
      `| phase-d-leadership-platform | Market list semi-ok; bodies template | No | No | CLEAR template bodies/rows |`,
      `| phase-d-oe-profile | Meta description | No | No | CLEAR OE-meta descriptions |`,
      `| profile-deepen | Pack field map | Pack content | N/A | KEEP structured; HOLD scaffold headlines |`,
      ``,
      `## Required replacement`,
      ``,
      `Field-Specific Writer v2: contract + evidence slice + exemplars → value **or BLANK**. No section packet reuse across unrelated fields.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-field-overlap-audit.md"),
    [
      `# Operator Setup Field Overlap Audit`,
      ``,
      `| Cluster | Fields | Problem | Recommendation |`,
      `| ------- | ------ | ------- | -------------- |`,
      `| Platform caps | cap_profile_operational / commercial / transition | Phase D reused OE brands/structures across all three | NARROW each; blank if no field-specific evidence |`,
      `| Commercial cards | ownerEngagementNarrative, ov_card_*, specializations | Overlap + UI scaffold mixed with company truth | ov_card_* → presentation only; engagement → research/blank |`,
      `| Governance infra | infra_systems_technology, infra_asset_management_reporting, risk_* | Phase D wrote diligence hedges in all | Separate systems map vs reporting cadence vs risk programs |`,
      `| Engagement section | phase_d_* rows | Duplicate Platform/Commercial/Governance narratives | CLEAR Phase D rows; redesign section or derive |`,
      `| Leadership Platform | Team Market / Language / Governance Cadence | Markets OK as presence; languages provisional; cadence generic | NARROW to markets-only or redesign |`,
      ``,
      `Fields too overlapping to populate reliably at Production scale without research:`,
      `- ownerEngagementNarrative vs infra_asset_management_reporting vs Engagement cadence rows`,
      `- ov_card_commercial vs cap_profile_commercial vs specializations`,
      ``,
    ].join("\n")
  );

  // Semantic contract markdown
  writeMd(
    join(DOCS, "data/operator-setup-field-semantic-contract-v2.md"),
    [
      `# Operator Setup — Field Semantic Contract v2`,
      ``,
      `Phase D failed because writers filled sections without field contracts.`,
      `Blank is preferable to generic filler. Inference is **not** permitted unless explicitly marked.`,
      ``,
      ...contracts.flatMap((c) => [
        `## \`${c.fieldName}\` — ${c.table.replace("Operator Setup - ", "")}`,
        ``,
        `- **Question:** ${c.question}`,
        `- **Belongs:** ${c.belongs}`,
        `- **Does NOT belong:** ${c.notBelongs}`,
        `- **Form / length:** ${c.expectedAnswerForm} / ${c.expectedLength}`,
        `- **Evidence:** ${c.evidenceRequired}`,
        `- **Inference permitted:** ${c.inferencePermitted}`,
        `- **Blank rule:** ${c.blankRule}`,
        `- **Adjacent:** ${c.adjacentFields}`,
        `- **Recommend:** ${c.recommend}`,
        `- **Only fixture examples?** ${c.onlyFixtureExamples ? "YES — reconstruct carefully" : "No"}`,
        `- **Strong examples (Tier1/2):**`,
        ...(c.strongExamples.length
          ? c.strongExamples.map((e) => `  - ${e.name}: ${String(e.value).slice(0, 160)}…`)
          : [`  - _(none found in pre-D Production/golden — field risk)_`]),
        ``,
      ]),
    ].join("\n")
  );

  // Research gaps
  writeMd(
    join(REPORTS, "operator-setup-semantic-repair-research-gaps.md"),
    [
      `# Semantic Repair — Targeted Research Gaps`,
      ``,
      `Assignments do **not** answer these. Mark TARGETED RESEARCH REQUIRED or leave blank.`,
      ``,
      `| Field | Why OE is insufficient |`,
      `| ----- | ---------------------- |`,
      `| ownerEngagementNarrative | Needs owner reporting/relationship evidence |`,
      `| infra_systems_technology | Needs named stack or verified brand-dependent model detail |`,
      `| infra_asset_management_reporting | Needs cadence/portal/SSC evidence |`,
      `| risk_programs_narrative | Needs risk/insurance/control program evidence |`,
      `| cap_profile_operational (deep) | Needs ops organization evidence beyond assignment counts |`,
      `| Leadership Team named people | Needs current official bios + last verified |`,
      `| ov_card_* / flexibility | Needs real commercial/flexibility posture or deprecate |`,
      ``,
    ].join("\n")
  );

  // Fixture leakage: compare Production Phase D fps to fixture backup narratives
  const fixtureRows = [];
  for (const tableFile of [
    "Operator_Setup_Platform_Markets.json",
    "Operator_Setup_Commercial_Fit_Terms.json",
    "Operator_Setup_Governance_Delivery_Diligence.json",
  ]) {
    const rows = JSON.parse(readFileSync(join(BACKUP, tableFile), "utf8")).records || [];
    for (const r of rows) {
      const mid = (r.fields.Operator || [])[0];
      if (!TEST_FIXTURE_MASTER_IDS.includes(mid)) continue;
      for (const f of NARRATIVE_FIELDS) {
        if (isPopulated(r.fields[f])) fixtureRows.push({ field: f, fp: normalizeFingerprint(r.fields[f]), mid });
      }
    }
  }
  let fixtureLeakHits = 0;
  for (const row of auditRows) {
    if (row.kind !== "NARRATIVE / RESEARCHED SUMMARY") continue;
    for (const fr of fixtureRows) {
      if (fr.field !== row.fieldName) continue;
      // high similarity: shared long token spans after normalize
      if (row.fingerprint.length > 60 && fr.fp.length > 60) {
        const a = new Set(row.fingerprint.split(" "));
        const b = new Set(fr.fp.split(" "));
        let inter = 0;
        for (const t of a) if (b.has(t)) inter++;
        const j = inter / Math.max(1, new Set([...a, ...b]).size);
        if (j >= 0.55) {
          fixtureLeakHits++;
          break;
        }
      }
    }
  }

  // Repair write plan (CLEAR / KEEP / HOLD) — no apply
  const repairPlan = {
    generatedAt: new Date().toISOString(),
    mode: "plan-only-no-apply",
    policy: "CLEAR Phase D template narratives; KEEP structured pack facts; HOLD scaffold headlines; delete Phase D section create rows",
    actions: verdicts.map((v) => ({
      operator: v.operator,
      masterId: v.masterId,
      table: v.table,
      field: v.fieldName,
      create: v.create,
      currentPhaseDValuePreview: v.phaseDPreview,
      action: v.verdict,
      proposedCorrectedValue: v.verdict === "KEEP" ? "(unchanged)" : v.verdict === "HOLD" ? "(hold pending founder)" : null,
      evidence: v.reason,
      semanticContract: contracts.find((c) => c.fieldName === v.fieldName)?.question || null,
      confidence: v.verdict === "KEEP" ? "high" : v.verdict === "CLEAR TO BLANK" ? "high" : "medium",
    })),
    summary: {
      ...verdictCounts,
      total: verdicts.length,
      fixtureLeakHits,
    },
  };
  writeJson(join(OUT, "repair-write-plan.json"), repairPlan);

  // Six-operator preview
  const resolvedPilots = PILOT.map((p) => {
    if (p.id) return { ...p, masterId: p.id };
    const hit = masters.find((m) => p.match.test(m.fields["Operator Name"] || m.fields.Name || ""));
    return { ...p, masterId: hit?.id, resolvedName: hit?.fields["Operator Name"] || hit?.fields.Name };
  });

  const pilotSections = resolvedPilots.map((p) => {
    const rows = verdicts.filter((v) => v.masterId === p.masterId);
    const clears = rows.filter((r) => r.verdict === "CLEAR TO BLANK").length;
    const keeps = rows.filter((r) => r.verdict === "KEEP").length;
    const holds = rows.filter((r) => r.verdict === "HOLD").length;
    const samples = rows.slice(0, 12);
    return { pilot: p, rows, clears, keeps, holds, samples };
  });

  writeMd(
    join(DOCS, "reviews/operator-setup-semantic-repair-six-profile-preview.md"),
    [
      `# Semantic Repair — Six-Operator Preview (no apply)`,
      ``,
      `Compare **Pre-D** (backup) vs **Phase D** (current) vs **Proposed v2 action** (CLEAR/KEEP/HOLD).`,
      ``,
      `**v2 rule:** do not rewrite with new prose yet — clear invalid Phase D filler first; only KEEP structured facts.`,
      ``,
      ...pilotSections.flatMap(({ pilot, clears, keeps, holds, samples, rows }) => [
        `## ${pilot.resolvedName || pilot.name}`,
        ``,
        `- Master: \`${pilot.masterId || "NOT FOUND"}\``,
        `- Phase D mutations on this operator: **${rows.length}**`,
        `- Proposed: KEEP ${keeps} · CLEAR ${clears} · HOLD ${holds}`,
        ``,
        `| Field | Phase D preview | Verdict |`,
        `| ----- | --------------- | ------- |`,
        ...samples.map((s) => `| ${s.fieldName} | ${s.phaseDPreview.replace(/\|/g, "/").slice(0, 100)} | **${s.verdict}** |`),
        rows.length > samples.length ? `| … | ${rows.length - samples.length} more | |` : "",
        ``,
        GOLDENS.has(pilot.masterId)
          ? `_Golden:_ Phase D should have been NO-OP on protected narrative cells; any CLEAR targets only Phase-D-created template rows if present._`
          : `_Production:_ Prefer blank over Phase D templates. Pre-D pack/structured facts KEEP._`,
        ``,
      ]),
      `## Pilot semantic verdict`,
      ``,
      `Phase D narrative quality on these six operators is **not acceptable** for Fit. Structured pack/assignment facts can remain. Template narratives and section creates should be cleared.`,
      ``,
    ].join("\n")
  );

  // Writer v2 architecture stub
  writeMd(
    join(DOCS, "data/operator-setup-field-specific-writer-v2.md"),
    [
      `# Field-Specific Writer v2 Architecture`,
      ``,
      `## Input`,
      ``,
      `Operator + **one field contract** + relevant OE facts slice + Claims + sources + Tier1/2 exemplars`,
      ``,
      `## Output`,
      ``,
      `\`{ value | null, confidence, evidence[], holdReason? }\``,
      ``,
      `If evidence does not answer the field → **null (NO WRITE)**.`,
      ``,
      `## Gates (all must pass)`,
      ``,
      `1. Semantic contract match`,
      `2. Evidence supports THIS field`,
      `3. Company specificity (fails “applies to five operators” test unless standardized select)`,
      `4. Exemplar style shape`,
      `5. Cross-company duplication check vs Production peers`,
      `6. Only then completeness`,
      ``,
      `## Anti-patterns banned`,
      ``,
      `- Section-level context packets reused across neighboring fields`,
      `- Diligence boilerplate as Setup content`,
      `- Assignment-count meta as companyDescription`,
      `- Fixture prose as methodology`,
      ``,
      `Implementation stub: \`lib/operator-setup/field-specific-writer-v2.js\``,
      ``,
    ].join("\n")
  );

  writeFileSync(
    join(ROOT, "lib/operator-setup/field-specific-writer-v2.js"),
    `/**
 * Field-Specific Writer v2 — architecture stub.
 * Does NOT generate Setup prose. Returns null unless a field handler explicitly supports the contract.
 * Phase D section writers must not be reused.
 */
export const WRITER_V2_VERSION = "operator-setup-field-writer-v2-stub";

/** @returns {{ value: null, confidence: 'none', evidence: [], holdReason: string }} */
export function writeFieldV2({ fieldName, contract, evidenceSlice }) {
  if (!contract) {
    return { value: null, confidence: "none", evidence: [], holdReason: "missing_semantic_contract" };
  }
  if (!evidenceSlice || !evidenceSlice.answersField) {
    return { value: null, confidence: "none", evidence: [], holdReason: "evidence_does_not_answer_field" };
  }
  // Intentionally no prose generation in stub — repair phase CLEARs first.
  return {
    value: null,
    confidence: "none",
    evidence: [],
    holdReason: "writer_v2_stub_no_generation_until_handlers_registered",
    fieldName,
  };
}

export function passesDifferentiationTest(text, { standardizedSelect = false } = {}) {
  if (standardizedSelect) return true;
  if (!text || String(text).trim().length < 20) return false;
  // Callers must supply peer comparison; stub rejects generic diligence hedges.
  return !/confirm in diligence|underwrite from the management agreement|systems vary by asset/i.test(String(text));
}
`
  );

  const structuredCount = auditRows.filter((r) => r.kind === "STRUCTURED FACT" || r.kind === "DERIVED STRUCTURED SUMMARY").length;
  const narrativeCount = auditRows.filter((r) => r.kind === "NARRATIVE / RESEARCHED SUMMARY").length;
  const scaffoldCount = auditRows.filter((r) => r.kind === "SCAFFOLD / PRESENTATION").length;

  const semanticValidBefore = {
    // Phase D "populated" narrative that passes KEEP
    validPopulated: verdictCounts.KEEP,
    validUnknownBlank: 0,
    invalidGeneric: verdictCounts["CLEAR TO BLANK"] + (verdictCounts.REWRITE || 0),
    hold: verdictCounts.HOLD,
    note: "Before repair: most Phase D narratives are invalid/generic; structured KEEPs are valid.",
  };
  const semanticValidAfterPlan = {
    validPopulated: verdictCounts.KEEP,
    validUnknownBlank: verdictCounts["CLEAR TO BLANK"], // will become intentional blanks
    invalidGeneric: 0,
    hold: verdictCounts.HOLD,
    note: "After planned CLEAR (not yet applied): invalid filler removed; blanks = honest unknown.",
  };

  const highestDup = narrativeFieldDup.filter((s) => s.pctGenericLike >= 70).map((s) => s.field);

  const stopPoint = {
    phaseDWritesAudited: auditRows.length,
    structuredWrites: structuredCount,
    narrativeWrites: narrativeCount,
    scaffoldWrites: scaffoldCount,
    keepCount: verdictCounts.KEEP,
    rewriteCount: verdictCounts.REWRITE,
    restoreCount: verdictCounts.RESTORE,
    clearCount: verdictCounts["CLEAR TO BLANK"],
    holdCount: verdictCounts.HOLD,
    highestDuplicationFields: highestDup,
    fixtureToProductionLeakageFound: fixtureLeakHits > 0,
    fixtureLeakHits,
    unsupportedGenericValuesFound: verdicts.filter((v) => v.evidenceFidelity === "UNSUPPORTED / GENERIC").length,
    fieldsUnclearSemanticIntent: contracts.filter((c) => c.onlyFixtureExamples).map((c) => c.fieldName),
    fieldsRecommendNarrow: contracts.filter((c) => /NARROW/i.test(c.recommend)).map((c) => c.fieldName),
    fieldsRecommendStructure: contracts.filter((c) => /STRUCTURE/i.test(c.recommend)).map((c) => c.fieldName),
    fieldsRecommendMoveToClaims: contracts.filter((c) => /MOVE TO CLAIMS/i.test(c.recommend)).map((c) => c.fieldName),
    fieldsRecommendDeprecate: contracts.filter((c) => /DEPRECATE/i.test(c.recommend)).map((c) => c.fieldName),
    sixOperatorPilotSemanticVerdict:
      "FAIL — Phase D narratives are template/generic; clear before any rewrite. Structured pack facts OK to keep.",
    preDVsPhaseDVsV2QualityVerdict:
      "Pre-D golden/real prose >> Phase D templates. v2 action for pilot = CLEAR invalid + KEEP structured; no new prose yet.",
    semanticValidCoverageBeforeRepair: semanticValidBefore,
    semanticValidCoverageAfterRepair: semanticValidAfterPlan,
    remainingTargetedResearchFields: [
      "ownerEngagementNarrative",
      "infra_systems_technology",
      "infra_asset_management_reporting",
      "risk_programs_narrative",
      "cap_profile_operational",
      "Leadership Team Members",
    ],
    setupTrustworthinessVerdict:
      "NOT trustworthy for Fit until CLEAR applied and field-specific writers replace section templates.",
    fitHandoffReadiness: "BLOCKED",
    exactFounderDecisions: [
      "Authorize CLEAR of Phase D template narratives and section create rows",
      "KEEP structured profile-deepen facts + Management Structures Supported",
      "HOLD explorer scaffold headlines pending product decision",
      "Accept field semantic contract v2",
      "Accept blank > generic policy",
      "Do not start Fit until post-CLEAR semantic QA passes",
    ],
    recommendedNextPhase:
      "Phase D.1 — Apply selective CLEAR/KEEP/HOLD repair from repair-write-plan.json, then re-run duplication QA",
    confirmationNoFitScoringChanges: true,
    confirmationOwnerPilotDisabled: true,
    airtableRepairApplied: false,
  };

  writeJson(join(OUT, "semantic-qa-stop-point.json"), stopPoint);

  writeMd(
    join(DOCS, "reviews/operator-setup-phase-d-semantic-repair-founder-review.md"),
    [
      `# Phase D Semantic Repair — Founder Review`,
      ``,
      `**Status: Phase D NOT accepted as product-complete.**`,
      ``,
      `Airtable repair: **NOT applied** (classification + pilot only). Fit: **BLOCKED**.`,
      ``,
      `## 1. What went wrong`,
      ``,
      `Phase D writers filled empty sections with OE-context templates. Completeness rose; semantic differentiation collapsed.`,
      ``,
      `## 2. Why technical completeness was misleading`,
      ``,
      `Section Complete/Partial counted diligence boilerplate and assignment-count meta as “meaningful content.” That is not company intelligence.`,
      ``,
      `## 3. Writer root cause`,
      ``,
      `See \`reports/operator-setup-phase-d-writer-root-cause.md\`. Section-level templates; no field contracts; no exemplar gate; no differentiation test.`,
      ``,
      `## 4. Field semantic-contract approach`,
      ``,
      `\`docs/data/operator-setup-field-semantic-contract-v2.md\` + machine JSON. Blank if evidence does not answer the field.`,
      ``,
      `## 5. Exemplar hierarchy`,
      ``,
      `Tier1 pre-D Production → Tier2 Arbor/HE → Tier3 fixtures (format only). HE \`infra_systems_technology\` is the systems-map gold bar; Phase D hedges fail it.`,
      ``,
      `## 6. Fixture leakage`,
      ``,
      `Token-overlap hits vs fixture narratives: **${fixtureLeakHits}**. Primary failure mode is shared **agent templates**, not necessarily verbatim fixture clone — same product harm.`,
      ``,
      `## 7. Repetition statistics`,
      ``,
      `See \`reports/operator-setup-phase-d-cross-company-duplication.md\`. Highest generic-like fields include: ${highestDup.slice(0, 12).join(", ") || "(see report)"}.`,
      ``,
      `## 8. Evidence fidelity`,
      ``,
      `Assignment footprint ≠ governance quality, cadence, tech sophistication, or owner flexibility. Those Phase D fills are **UNSUPPORTED / GENERIC**.`,
      ``,
      `## 9. KEEP / REWRITE / RESTORE / CLEAR / HOLD`,
      ``,
      `| Verdict | Count |`,
      `| ------- | ----: |`,
      ...Object.entries(verdictCounts).map(([k, v]) => `| ${k} | ${v} |`),
      ``,
      `## 10. Fields needing redesign`,
      ``,
      `- NARROW: ${stopPoint.fieldsRecommendNarrow.join(", ")}`,
      `- STRUCTURE: ${stopPoint.fieldsRecommendStructure.join(", ") || "—"}`,
      `- MOVE TO CLAIMS: ${stopPoint.fieldsRecommendMoveToClaims.join(", ") || "—"}`,
      `- DEPRECATE (as Setup truth): ${stopPoint.fieldsRecommendDeprecate.join(", ")}`,
      ``,
      `## 11. Six-operator preview`,
      ``,
      `\`docs/reviews/operator-setup-semantic-repair-six-profile-preview.md\` — pilot semantic verdict: **FAIL** for narratives.`,
      ``,
      `## 12. Repair apply results`,
      ``,
      `**Not applied.** Awaiting founder authorization to run selective CLEAR from \`data/operator-setup/phase-d-repair/repair-write-plan.json\`.`,
      ``,
      `## 13–15. Post-repair / coverage / research`,
      ``,
      `After planned CLEAR: invalid/generic → intentional blank. Research still required for owner engagement, systems, reporting, risk, deep ops, named leadership.`,
      ``,
      `## 16. Can Setup be trusted by Fit?`,
      ``,
      `**No — not yet.**`,
      ``,
      `## 17. Exact founder decisions`,
      ``,
      ...stopPoint.exactFounderDecisions.map((d, i) => `${i + 1}. ${d}`),
      ``,
      `## 18. Recommended next phase`,
      ``,
      `**${stopPoint.recommendedNextPhase}**`,
      ``,
      `- No Fit/scoring changes`,
      `- Owner pilot remains disabled`,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(stopPoint, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
