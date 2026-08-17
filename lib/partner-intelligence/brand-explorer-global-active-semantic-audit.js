/**
 * Brand Explorer — global Active/Live semantic QA audit (read-only).
 *
 * Universe: Brand Basics Brand Status Active/Live via loadActiveUniverse().
 * No Airtable writes. Dry-run only.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ACTIVE_UNIVERSE_SOURCE,
  ACTIVE_UNIVERSE_VERSION,
  NON_ACTIVE_STATUS_CONFLICT_PROBES,
  loadActiveUniverse,
  resolveActiveUniverseRecordId,
} from "./brand-explorer-active-universe.js";
import {
  isOwnerFacingPresentationRow,
  scanOwnerFacingForbiddenLanguage,
} from "./brand-explorer-public-visibility-quality-lock.js";
import { evaluateScenarioOwnerValueBar } from "./brand-explorer-scenario-owner-value-bar.js";
import { evaluateValueCreationScenariosBar } from "./brand-explorer-value-creation-scenarios-bar.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { evaluateAiAssistedProfileFootnoteGate } from "./brand-explorer-ai-assisted-footnote.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const GLOBAL_ACTIVE_SEMANTIC_AUDIT_VERSION = "global-active-semantic-audit-v1";
export const EXPECTED_ACTIVE_UNIVERSE_COUNT = 62;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

/** Held / excluded from active semantic audit scope. */
export const EXCLUDED_FROM_ACTIVE_SEMANTIC_AUDIT = Object.freeze([
  "four-points-flex-by-sheraton",
  "the-house-of-originals",
  "morgans-originals",
  "radisson-collection",
]);

const BRAND_FETCH_THROTTLE_MS = 350;

const INTERNAL_LANGUAGE_RES = [
  { id: "visual_diligence", re: /\bvisual diligence\b/i, severity: "critical" },
  { id: "steward_matched", re: /\bsteward-matched\b/i, severity: "critical" },
  { id: "underwriting_lane", re: /\bunderwriting lane\b/i, severity: "critical" },
  { id: "brand_lane_evidence", re: /\bbrand-lane evidence\b/i, severity: "critical" },
  { id: "directory_card", re: /\bDirectory card\b/i, severity: "high" },
  { id: "source_supported", re: /\bsource-supported\b/i, severity: "critical" },
  { id: "source_pack", re: /\bsource pack\b/i, severity: "critical" },
  { id: "factory", re: /\bfactory\b/i, severity: "critical" },
  { id: "stage", re: /\bStage\s*\d/i, severity: "critical" },
  { id: "governance", re: /\bgovernance\b/i, severity: "high" },
  { id: "qa", re: /\bQA gate\b|\bQA checklist\b|\bfactory QA\b/i, severity: "high" },
  { id: "confirm_owner", re: /\bconfirm (the )?owner\b/i, severity: "high" },
  { id: "confirm_operator", re: /\bconfirm (the )?operator\b/i, severity: "high" },
  { id: "keep_sibling", re: /\bkeep sibling\b/i, severity: "high" },
  { id: "do_not_reuse", re: /\bdo not reuse\b/i, severity: "high" },
  { id: "avoid_borrowing", re: /\bavoid borrowing\b/i, severity: "high" },
  { id: "use_labeled_example", re: /\bUse this labeled example\b/i, severity: "critical" },
  { id: "raw_url", re: /https?:\/\/[^\s]+/, severity: "critical" },
  { id: "fdd", re: /\bFDD\b/, severity: "critical" },
  { id: "item_19", re: /\bItem\s*19\b/i, severity: "critical" },
  { id: "loi", re: /\bLOI\b/, severity: "critical" },
  { id: "adr", re: /\bADR\b/, severity: "high" },
  { id: "revpar", re: /\bRevPAR\b/i, severity: "high" },
  { id: "fee_stack", re: /fee-?stack/i, severity: "high" },
];

const MOMENTUM_URL_ALLOWED_SLOTS = new Set(["footprint.momentum", "footprint.openings"]);

const INTERNAL_LANGUAGE_SKIP_SLOTS = new Set([
  "operations.compliance.qa_cadence",
  "operations.compliance.qa_rhythm",
  "footprint.portfolio_mix",
]);

const MOMENTUM_FAKE_TITLE_RES = [
  /Development Page Frames/i,
  /Development Positioning Remains/i,
  /Brand Site Confirms/i,
  /Brand Presence Confirms/i,
  /Guest Brand Site Confirms/i,
  /Brand Page Confirms/i,
  /on Marriott Longer Stays family positioning/i,
  /development page confirms/i,
  /brand site confirms/i,
  /directory card/i,
];

const GENERIC_PROPERTY_TITLE_RE =
  /^(.+?)\s*[-—]\s*(International Reference|CALA|Property Example|Americas Reference)$/i;

const ARCHETYPE_PROPERTY_RE =
  /^(US |North America |Suburban (?!Studios\b)|Airport |Upper-midscale |US longer|employment|kitchenette|competitive sets|secondary)/i;

const BONVOY_REACH_TITLE_RE = /\bBonvoy Reach\b/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function mockRes() {
  return {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
}

async function fetchBrandById(brandId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`Brand fetch failed for ${brandId}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

function mkFinding({
  section,
  severity,
  failureType,
  currentValue = "",
  sourceProblem = "",
  proposedFix = "",
  slotKey = null,
  recordId = null,
  needsSteward = false,
}) {
  return {
    section,
    severity,
    failureType,
    currentValue: String(currentValue || "").slice(0, 300),
    sourceProblem,
    proposedFix,
    slotKey,
    recordId,
    needsSteward,
    pass: false,
  };
}

function classifyMomentumSource(title, body, url = "") {
  const text = `${title}\n${body}\n${url}`;
  if (/hotel-development\.marriott\.com/i.test(url)) return "development_page";
  if (/marriott\.com\/brands\//i.test(url) || /brand site|brand page/i.test(title))
    return "brand_page";
  if (/marriott\.com\/hotels\//i.test(url) || /\.marriott\.com\/en-us\/hotels\//i.test(url))
    return "directory_page";
  if (/press release|announcement|opening|signed|conversion|renovation/i.test(text))
    return "press_announcement";
  if (/skift\.com|hospitalitynet|hotel management/i.test(url)) return "trade_press";
  return "other";
}

function auditRecentMomentum(rows, brandSlug) {
  const cards = (rows || []).filter((r) => r.slotKey === "footprint.momentum");
  const findings = [];
  const cardAudits = [];
  const sourceTypes = [];

  for (const m of cards) {
    const title = nz(m.title);
    const body = nz(m.body);
    const url = nz(m.sourceUrl || m.url || "");
    const sourceType = classifyMomentumSource(title, body, url);
    sourceTypes.push(sourceType);
    const cardFailures = [];

    if (MOMENTUM_FAKE_TITLE_RES.some((re) => re.test(title))) {
      cardFailures.push("title_is_source_note_not_momentum");
      findings.push(
        mkFinding({
          section: "Recent Momentum",
          severity: "critical",
          failureType: "title_is_source_note_not_momentum",
          currentValue: title,
          sourceProblem: "Source-audit title, not opening/announcement",
          proposedFix: "Use actual hotel/project name and event semantics",
          slotKey: m.slotKey,
          recordId: m.recordId,
        })
      );
    }
    if (sourceType === "development_page") {
      cardFailures.push("development_page_as_momentum");
      findings.push(
        mkFinding({
          section: "Recent Momentum",
          severity: "high",
          failureType: "development_page_as_momentum",
          currentValue: title,
          sourceProblem: "Development page used as momentum",
          proposedFix: "Replace with press release or property proof",
          slotKey: m.slotKey,
          recordId: m.recordId,
        })
      );
    }
    if (sourceType === "brand_page") {
      cardFailures.push("brand_page_as_momentum");
      findings.push(
        mkFinding({
          section: "Recent Momentum",
          severity: "high",
          failureType: "brand_page_as_momentum",
          currentValue: title,
          proposedFix: "Use opening/announcement or labeled property proof",
          slotKey: m.slotKey,
          recordId: m.recordId,
        })
      );
    }
    if (sourceType === "directory_page" && !/property proof/i.test(body)) {
      cardFailures.push("directory_as_momentum");
      findings.push(
        mkFinding({
          section: "Recent Momentum",
          severity: "high",
          failureType: "directory_as_momentum_without_proof_label",
          currentValue: title,
          proposedFix: "Label as property proof or replace with announcement",
          slotKey: m.slotKey,
          recordId: m.recordId,
        })
      );
    }
    if (/https?:\/\//i.test(body)) {
      // Trailing source URLs on momentum cards are structural (dated-momentum gate); flag only inline URLs.
      const withoutTrailing = body.replace(/\s+https?:\/\/\S+\s*$/i, "").trim();
      if (/https?:\/\//i.test(withoutTrailing)) {
        findings.push(
          mkFinding({
            section: "Recent Momentum",
            severity: "critical",
            failureType: "raw_url_in_body",
            currentValue: body.slice(0, 120),
            slotKey: m.slotKey,
            recordId: m.recordId,
          })
        );
      }
    }

    cardAudits.push({
      title,
      body: body.slice(0, 200),
      geography: nz(m.caseSummaryTags || ""),
      sourceType,
      sourceUrl: url.slice(0, 200),
      pass: cardFailures.length === 0,
      failures: cardFailures,
    });
  }

  if (cards.length && sourceTypes.every((t) => ["development_page", "brand_page", "directory_page"].includes(t))) {
    findings.push(
      mkFinding({
        section: "Recent Momentum",
        severity: "critical",
        failureType: "all_cards_source_pages_not_momentum",
        currentValue: `${cards.length} cards`,
        proposedFix: "Add at least one opening/announcement or labeled property proof",
      })
    );
  }

  return { pass: findings.length === 0, findings, cards: cardAudits };
}

function auditOpenings(rows, brandName) {
  const cards = (rows || []).filter((r) => r.slotKey === "footprint.openings");
  const findings = [];

  for (const o of cards) {
    const title = nz(o.title);
    const body = nz(o.body);
    const generic = GENERIC_PROPERTY_TITLE_RE.exec(title);
    if (generic && generic[1].trim().toLowerCase() === brandName.toLowerCase()) {
      findings.push(
        mkFinding({
          section: "Openings / Examples / Properties",
          severity: "critical",
          failureType: "generic_brand_reference_title",
          currentValue: title,
          proposedFix: "Use actual hotel name with geography label",
          slotKey: o.slotKey,
          recordId: o.recordId,
        })
      );
    }
    if (ARCHETYPE_PROPERTY_RE.test(title)) {
      findings.push(
        mkFinding({
          section: "Openings / Examples / Properties",
          severity: "critical",
          failureType: "archetype_not_actual_hotel",
          currentValue: title,
          proposedFix: "Replace with verified property name or hide card",
          slotKey: o.slotKey,
          recordId: o.recordId,
          needsSteward: true,
        })
      );
    }
    if (/\bvisual diligence\b/i.test(body) || /\bUse this labeled example\b/i.test(body)) {
      findings.push(
        mkFinding({
          section: "Openings / Examples / Properties",
          severity: "critical",
          failureType: "internal_diligence_language",
          currentValue: body.slice(0, 120),
          slotKey: o.slotKey,
          recordId: o.recordId,
        })
      );
    }
  }

  return { pass: findings.length === 0, findings, cardCount: cards.length };
}

function auditPortfolioMix(rows) {
  const mix = (rows || []).find((r) => r.slotKey === "footprint.portfolio_mix");
  const findings = [];
  if (!mix) {
    return {
      pass: true,
      classification: "missing_or_cleanly_unavailable",
      findings: [],
      body: "",
    };
  }
  const body = nz(mix.body);
  const hasPercent = /%/.test(body);
  const isBullets = /^[-•]\s/m.test(body) || /\n/.test(body);
  const isLongProse = body.length > 200 && !hasPercent;

  let classification = "structured_percentage_mix";
  if (!body) classification = "missing_or_cleanly_unavailable";
  else if (hasPercent && isBullets !== false) classification = "structured_percentage_mix";
  else if (hasPercent) classification = "structured_percentage_mix";
  else if (isLongProse || /peer comparison set|CALA-supported operating|evaluate on operating model|steward-confirmed/i.test(body)) {
    classification = "prose_market_note";
    findings.push(
      mkFinding({
        section: "Portfolio Mix",
        severity: "high",
        failureType: "prose_market_note",
        currentValue: body.slice(0, 200),
        proposedFix: "Convert to structured percentage mix or cleanly unavailable",
        slotKey: mix.slotKey,
        recordId: mix.recordId,
      })
    );
  } else if (!hasPercent) {
    classification = "structured_non_percentage_mix";
  }

  return { pass: findings.length === 0, classification, findings, body: body.slice(0, 300) };
}

function auditInternalLanguage(ownerRows) {
  const findings = [];
  const pvqlHits = scanOwnerFacingForbiddenLanguage(ownerRows);
  for (const h of pvqlHits) {
    if (INTERNAL_LANGUAGE_SKIP_SLOTS.has(nz(h.slotKey))) continue;
    if (MOMENTUM_URL_ALLOWED_SLOTS.has(nz(h.slotKey)) && (h.id || "") === "raw_url") continue;
    findings.push(
      mkFinding({
        section: "Internal Language",
        severity: /raw_url|source_pack|factory|visual_diligence|steward/i.test(h.id || "")
          ? "critical"
          : "high",
        failureType: `forbidden:${h.id || h.label}`,
        currentValue: h.snippet || h.label,
        slotKey: h.slotKey,
        recordId: h.recordId,
      })
    );
  }

  for (const r of ownerRows) {
    const slotKey = nz(r.slotKey);
    if (INTERNAL_LANGUAGE_SKIP_SLOTS.has(slotKey)) continue;
    const text = `${nz(r.title)} ${nz(r.body)}`;
    for (const rule of INTERNAL_LANGUAGE_RES) {
      if (rule.id === "raw_url" && MOMENTUM_URL_ALLOWED_SLOTS.has(slotKey)) continue;
      if (
        rule.id === "stage" &&
        /Stage\s+\d/i.test(text) &&
        (/diligence gaps|Confirm Brand Basics|Source Confidence|image remediation|before Stage/i.test(text) ||
          /^(operations|governance|diligence)\./i.test(slotKey))
      ) {
        continue;
      }
      if (rule.re.test(text) && !findings.some((f) => f.slotKey === r.slotKey && f.failureType === rule.id)) {
        findings.push(
          mkFinding({
            section: "Internal Language",
            severity: rule.severity,
            failureType: rule.id,
            currentValue: text.slice(0, 120),
            slotKey: r.slotKey,
            recordId: r.recordId,
          })
        );
      }
    }
  }

  return { pass: findings.length === 0, findings, hitCount: findings.length };
}

function auditGeographicFootprint(rows) {
  const findings = [];
  const regions = (rows || []).filter((r) => /^footprint\.region\./.test(nz(r.slotKey)));
  const cala = regions.find((r) => r.slotKey === "footprint.region.cala");
  const calaBody = nz(cala?.body);
  if (cala && /CALA unavailable|no verified CALA|do not imply regional presence/i.test(calaBody) && /operating examples|property reference/i.test(calaBody)) {
    findings.push(
      mkFinding({
        section: "Geographic Footprint",
        severity: "medium",
        failureType: "cala_label_without_support",
        currentValue: calaBody.slice(0, 120),
        slotKey: cala.slotKey,
        recordId: cala.recordId,
      })
    );
  }
  if (regions.length < 2) {
    findings.push(
      mkFinding({
        section: "Geographic Footprint",
        severity: "medium",
        failureType: "insufficient_region_cards",
        currentValue: `${regions.length} region cards`,
        proposedFix: "Add region cards or cleanly unavailable handling",
      })
    );
  }
  return { pass: findings.length === 0, findings, regionCount: regions.length };
}

function auditImages(ownerRows, brandSlug) {
  const uniqueness = evaluateImageUniqueness({
    presentationRows: ownerRows,
    brandSlug,
  });
  const roleMatch = evaluateBrandImageRoleMatch({ presentationRows: ownerRows, brandSlug });
  const findings = [];

  if ((uniqueness.scenarioDistinctCount || 0) < 3 && (uniqueness.scenarios || []).length >= 3) {
    findings.push(
      mkFinding({
        section: "Image Uniqueness",
        severity: "high",
        failureType: "scenario_image_duplicates",
        currentValue: `${uniqueness.scenarioDistinctCount}/3 distinct`,
        proposedFix: "Use three visually distinct scenario images",
      })
    );
  }
  if (!uniqueness.pass) {
    for (const dup of uniqueness.duplicateGroups || []) {
      if ((dup.slots || []).length < 2) continue;
      findings.push(
        mkFinding({
          section: "Image Uniqueness",
          severity: dup.slots.some((s) => /^overview\.scenario/.test(s)) ? "high" : "medium",
          failureType: "duplicate_image_group",
          currentValue: dup.duplicateGroupId || dup.groupId,
          sourceProblem: (dup.slots || []).join(", "),
        })
      );
    }
  }
  if (!roleMatch.pass) {
    findings.push(
      mkFinding({
        section: "Image Role Match",
        severity: "medium",
        failureType: "image_role_mismatch",
        currentValue: (roleMatch.failures || roleMatch.issues || []).slice(0, 3).join("; "),
      })
    );
  }

  return {
    pass: findings.length === 0,
    findings,
    uniqueness,
    roleMatch,
  };
}

function classifyBrandBucket(allFindings) {
  const severities = allFindings.map((f) => f.severity);
  if (severities.includes("critical")) return "critical_blocker";
  if (severities.includes("high")) return "remediation_required";
  if (severities.includes("medium")) return "minor_cleanup";
  return "freeze_safe";
}

function sectionPass(findings, section) {
  return !findings.some((f) => f.section === section);
}

/**
 * Audit one Active/Live brand (read-only).
 */
export async function auditBrandGlobalSemantics(slug, { universeRow = null } = {}) {
  const recordId = universeRow?.recordId || resolveActiveUniverseRecordId(slug) || slug;
  const brand = await fetchBrandById(recordId);
  const ownerRows = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
  const momentum = auditRecentMomentum(ownerRows, slug);
  const footnote = evaluateAiAssistedProfileFootnoteGate(brand, "");
  const openings = auditOpenings(ownerRows, brand.name || slug);
  const portfolioMix = auditPortfolioMix(ownerRows);
  const internalLanguage = auditInternalLanguage(ownerRows);
  const geo = auditGeographicFootprint(ownerRows);
  const valueScenarios = evaluateScenarioOwnerValueBar(ownerRows, { brandSlug: slug });
  const valueCreation = evaluateValueCreationScenariosBar(ownerRows, {
    brandSlug: slug,
    brandName: brand.name,
  });
  const images = auditImages(ownerRows, slug);

  const allFindings = [
    ...momentum.findings,
    ...openings.findings,
    ...portfolioMix.findings,
    ...internalLanguage.findings,
    ...geo.findings,
    ...images.findings,
  ];

  for (const f of valueScenarios.failures || []) {
    const sev = /duplicate|forbidden|bonvoy|reference|thin_body/i.test(f) ? "high" : "medium";
    allFindings.push(
      mkFinding({
        section: "Where This Brand Creates the Most Value",
        severity: sev,
        failureType: f,
        proposedFix: "Rewrite scenario to owner-value topic with distinct image",
      })
    );
  }
  for (const s of valueScenarios.scenarios || []) {
    if (BONVOY_REACH_TITLE_RE.test(s.title || "")) {
      allFindings.push(
        mkFinding({
          section: "Where This Brand Creates the Most Value",
          severity: "high",
          failureType: "generic_bonvoy_reach_title",
          currentValue: s.title,
        })
      );
    }
  }

  for (const f of valueCreation.failures || []) {
    allFindings.push(
      mkFinding({
        section: "Value Creation Scenarios",
        severity: /blank|missing|forbidden/i.test(f) ? "high" : "medium",
        failureType: f,
      })
    );
  }

  if (!footnote.pass) {
    for (const f of footnote.failures || []) {
      allFindings.push(
        mkFinding({
          section: "AI-Assisted Profile footnote",
          severity: /company_validated/i.test(f) ? "critical" : "high",
          failureType: f,
        })
      );
    }
  }

  const bucket = classifyBrandBucket(allFindings);
  const severityCounts = allFindings.reduce(
    (acc, f) => {
      acc[f.severity] = (acc[f.severity] || 0) + 1;
      return acc;
    },
    { critical: 0, high: 0, medium: 0, low: 0 }
  );

  return {
    brandSlug: slug,
    brandName: brand.name,
    recordId: brand.id || recordId,
    brandStatus: universeRow?.status || brand.status || null,
    publicFull: brand.shouldRenderFullProfile === true,
    classification: bucket,
    pass: bucket === "freeze_safe",
    severityCounts,
    findings: allFindings,
    sections: {
      recentMomentum: { pass: momentum.pass, ...momentum },
      openings: { pass: openings.pass, ...openings },
      portfolioMix: { pass: portfolioMix.pass, ...portfolioMix },
      valueScenarios: { pass: valueScenarios.pass, failures: valueScenarios.failures },
      valueCreation: { pass: valueCreation.pass, failures: valueCreation.failures },
      geographicFootprint: { pass: geo.pass, ...geo },
      internalLanguage: { pass: internalLanguage.pass, hitCount: internalLanguage.hitCount },
      aiFootnote: { pass: footnote.pass, failures: footnote.failures },
      images: {
        pass: images.pass,
        scenarioDistinct: images.uniqueness.scenarioDistinctCount,
        galleryDistinct: images.uniqueness.galleryDistinctCount,
      },
    },
    sectionPass: {
      recentMomentum: sectionPass(allFindings, "Recent Momentum"),
      openings: sectionPass(allFindings, "Openings / Examples / Properties"),
      portfolioMix: sectionPass(allFindings, "Portfolio Mix"),
      valueScenarios: valueScenarios.pass,
      valueCreation: valueCreation.pass,
      geographicFootprint: geo.pass,
      internalLanguage: internalLanguage.pass,
      aiFootnote: footnote.pass,
      images: images.pass,
    },
    writePerformed: false,
  };
}

/**
 * Run global Active/Live semantic audit (read-only).
 */
export async function runGlobalActiveSemanticAudit({ dryRun = true, brands = null } = {}) {
  if (!dryRun) {
    throw new Error("Global semantic audit is read-only. Use --dry-run only (no writes).");
  }

  const universe = await loadActiveUniverse({ includeDetails: false });
  const excluded = new Set(EXCLUDED_FROM_ACTIVE_SEMANTIC_AUDIT.map((s) => s.toLowerCase()));

  const fullActiveBrands = (universe.brands || []).filter(
    (b) => b.isActiveLive !== false && !excluded.has(nz(b.slug).toLowerCase())
  );

  let activeBrands = fullActiveBrands;

  if (Array.isArray(brands) && brands.length) {
    const allow = new Set(brands.map((s) => s.toLowerCase()));
    activeBrands = fullActiveBrands.filter((b) => allow.has(b.slug.toLowerCase()));
  }

  const bySlug = new Map(activeBrands.map((b) => [b.slug, b]));
  const brandResults = [];

  for (let i = 0; i < activeBrands.length; i++) {
    const row = activeBrands[i];
    process.stdout.write(`[semantic-audit] ${i + 1}/${activeBrands.length} ${row.slug}...\n`);
    try {
      brandResults.push(await auditBrandGlobalSemantics(row.slug, { universeRow: row }));
    } catch (err) {
      brandResults.push({
        brandSlug: row.slug,
        brandName: row.name,
        recordId: row.recordId,
        classification: "critical_blocker",
        pass: false,
        error: err?.message || String(err),
        findings: [
          mkFinding({
            section: "Audit",
            severity: "critical",
            failureType: "audit_fetch_failed",
            currentValue: err?.message || String(err),
          }),
        ],
        severityCounts: { critical: 1, high: 0, medium: 0, low: 0 },
        writePerformed: false,
      });
    }
    if (i < activeBrands.length - 1) await sleep(BRAND_FETCH_THROTTLE_MS);
  }

  const allFindings = brandResults.flatMap((b) => b.findings || []);
  const severityTotals = allFindings.reduce(
    (acc, f) => {
      acc[f.severity] = (acc[f.severity] || 0) + 1;
      return acc;
    },
    { critical: 0, high: 0, medium: 0, low: 0 }
  );

  const bucketCounts = brandResults.reduce((acc, b) => {
    acc[b.classification] = (acc[b.classification] || 0) + 1;
    return acc;
  }, {});

  const sectionFailCounts = {};
  for (const b of brandResults) {
    for (const [sec, ok] of Object.entries(b.sectionPass || {})) {
      if (!sectionFailCounts[sec]) sectionFailCounts[sec] = { pass: 0, fail: 0 };
      if (ok) sectionFailCounts[sec].pass += 1;
      else sectionFailCounts[sec].fail += 1;
    }
  }

  const activeCount = fullActiveBrands.length;
  const auditedCount = brandResults.length;
  const universeReconciled = activeCount === EXPECTED_ACTIVE_UNIVERSE_COUNT;

  const criticalBlockers = brandResults.filter((b) => b.classification === "critical_blocker");
  const remediationRequired = brandResults.filter((b) => b.classification === "remediation_required");
  const minorCleanup = brandResults.filter((b) => b.classification === "minor_cleanup");
  const freezeSafe = brandResults.filter((b) => b.classification === "freeze_safe");

  let freezeDecision = "may_freeze_54_after_review";
  let freezeRationale = "";
  if (!universeReconciled) {
    freezeDecision = "do_not_freeze_universe_count_mismatch";
    freezeRationale = `Active universe count ${activeCount} !== expected ${EXPECTED_ACTIVE_UNIVERSE_COUNT}. Reconcile before freeze.`;
  } else if (criticalBlockers.length > 0) {
    freezeDecision = "do_not_freeze_critical_blockers_present";
    freezeRationale = `${criticalBlockers.length} brand(s) have critical semantic blockers. Remediate before 54 freeze.`;
  } else if (remediationRequired.length > 0) {
    freezeDecision = "do_not_freeze_remediation_required";
    freezeRationale = `${remediationRequired.length} brand(s) require semantic remediation before 54 freeze.`;
  } else if (minorCleanup.length > 0) {
    freezeDecision = "freeze_after_minor_cleanup";
    freezeRationale = `${minorCleanup.length} brand(s) need minor cleanup; no critical blockers. Founder may accept interim freeze with cleanup plan.`;
  } else {
    freezeDecision = "ready_to_freeze_62_semantic_qa_clean";
    freezeRationale = `All ${activeCount} audited Active/Live brands pass semantic QA with no critical/high findings.`;
  }

  const remediationWaves = [];
  if (criticalBlockers.length) {
    remediationWaves.push({
      wave: "critical_blockers",
      brands: criticalBlockers.map((b) => b.brandSlug),
      focus: "wrong-brand evidence, placeholder properties, internal language, fake momentum",
    });
  }
  if (remediationRequired.length) {
    remediationWaves.push({
      wave: "high_severity_remediation",
      brands: remediationRequired.map((b) => b.brandSlug),
      focus: "portfolio mix prose, momentum source notes, duplicate images, thin scenarios",
    });
  }

  const gateHardening = [
    "Add semantic momentum checks: fail brand/dev/directory-only cards without property-proof label",
    "Add openings title check: fail generic [Brand] — International Reference",
    "Add portfolio mix structured-format check on all Active/Live profiles",
    "Extend forbidden-language scan for steward-matched, visual diligence, underwriting lane",
    "Harden image-uniqueness v3 across scenario + openings + gallery",
  ];

  const report = {
    version: GLOBAL_ACTIVE_SEMANTIC_AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE,
    activeUniverseVersion: ACTIVE_UNIVERSE_VERSION,
    expectedActiveCount: EXPECTED_ACTIVE_UNIVERSE_COUNT,
    activeCount,
    auditedCount,
    universeReconciled,
    excludedFromAudit: [...EXCLUDED_FROM_ACTIVE_SEMANTIC_AUDIT],
    excludedStatusConflicts: NON_ACTIVE_STATUS_CONFLICT_PROBES,
    severityTotals,
    bucketCounts,
    sectionFailCounts,
    freezeDecision,
    freezeRationale,
    freezeSafeSlugs: freezeSafe.map((b) => b.brandSlug),
    minorCleanupSlugs: minorCleanup.map((b) => b.brandSlug),
    remediationRequiredSlugs: remediationRequired.map((b) => b.brandSlug),
    criticalBlockerSlugs: criticalBlockers.map((b) => b.brandSlug),
    recommendedRemediationWaves: remediationWaves,
    gateHardeningRecommendations: gateHardening,
    readyStatement: universeReconciled && criticalBlockers.length === 0 && remediationRequired.length === 0
      ? "global_active_semantic_audit_complete_review_freeze_decision"
      : "global_active_semantic_audit_complete_remediation_before_54_freeze",
    brandResults,
  };

  return report;
}

function mdEscape(s) {
  return String(s || "").replace(/\|/g, "/").replace(/\n/g, " ");
}

export function writeGlobalActiveSemanticAuditReports(report, { refresh = false } = {}) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const stem = refresh
    ? "brand-explorer-global-active-semantic-audit-refresh"
    : "brand-explorer-global-active-semantic-audit";
  const jsonPath = path.join(REPORTS_DIR, `${stem}.json`);
  const mdPath = path.join(REPORTS_DIR, `${stem}.md`);
  const criticalPath = path.join(REPORTS_DIR, `${stem}-critical.md`);
  const byBrandPath = path.join(REPORTS_DIR, `${stem}-by-brand.md`);
  const bySectionPath = path.join(REPORTS_DIR, `${stem}-by-section.md`);
  const docsPath = path.join(
    DOCS_DIR,
    refresh
      ? "brand-explorer-global-active-semantic-audit-refresh.md"
      : "brand-explorer-global-active-semantic-audit.md"
  );

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const summaryLines = [
    refresh
      ? "# Brand Explorer — Global Active/Live Semantic QA Audit (Refresh)"
      : "# Brand Explorer — Global Active/Live Semantic QA Audit",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Mode: **read-only / dry-run** (no Airtable writes)${refresh ? " · **fresh refresh**" : ""}`,
    "",
    "## Universe",
    "",
    `- Source: ${report.activeUniverseSource.name}`,
    `- Expected active count: **${report.expectedActiveCount}**`,
    `- Active universe count: **${report.activeCount}**`,
    `- Brands audited this run: **${report.auditedCount}**`,
    `- Universe reconciled: **${report.universeReconciled}**`,
    `- Excluded from audit: ${report.excludedFromAudit.map((s) => `\`${s}\``).join(", ")}`,
    "",
    "## Severity totals",
    "",
    `| Severity | Count |`,
    `|----------|------:|`,
    `| Critical | ${report.severityTotals.critical || 0} |`,
    `| High | ${report.severityTotals.high || 0} |`,
    `| Medium | ${report.severityTotals.medium || 0} |`,
    `| Low | ${report.severityTotals.low || 0} |`,
    "",
    "## Brand buckets",
    "",
    `| Bucket | Count |`,
    `|--------|------:|`,
    `| freeze_safe | ${report.bucketCounts.freeze_safe || 0} |`,
    `| minor_cleanup | ${report.bucketCounts.minor_cleanup || 0} |`,
    `| remediation_required | ${report.bucketCounts.remediation_required || 0} |`,
    `| critical_blocker | ${report.bucketCounts.critical_blocker || 0} |`,
    "",
    "## Section pass/fail",
    "",
    `| Section | Pass | Fail |`,
    `|---------|-----:|-----:|`,
  ];
  for (const [sec, counts] of Object.entries(report.sectionFailCounts || {})) {
    summaryLines.push(`| ${sec} | ${counts.pass} | ${counts.fail} |`);
  }
  summaryLines.push(
    "",
    "## 54 freeze decision",
    "",
    `**${report.freezeDecision}**`,
    "",
    report.freezeRationale,
    "",
    `Ready statement: \`${report.readyStatement}\``,
    "",
    "## Per-brand summary",
    "",
    "| Brand | Slug | Bucket | Critical | High | Medium |",
    "|-------|------|--------|----------:|-----:|-------:|"
  );
  for (const b of report.brandResults) {
    summaryLines.push(
      `| ${mdEscape(b.brandName)} | \`${b.brandSlug}\` | ${b.classification} | ${b.severityCounts?.critical || 0} | ${b.severityCounts?.high || 0} | ${b.severityCounts?.medium || 0} |`
    );
  }
  summaryLines.push(
    "",
    "## Recommended remediation waves",
    "",
    "```json",
    JSON.stringify(report.recommendedRemediationWaves, null, 2),
    "```",
    "",
    "## Gate hardening",
    ""
  );
  for (const g of report.gateHardeningRecommendations || []) {
    summaryLines.push(`- ${g}`);
  }
  summaryLines.push("");

  const md = summaryLines.join("\n");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");

  const criticalLines = [
    "# Global Active Semantic Audit — Critical Findings",
    "",
    `Generated: ${report.generatedAt}`,
    "",
  ];
  for (const b of report.brandResults) {
    const critical = (b.findings || []).filter((f) => f.severity === "critical");
    if (!critical.length) continue;
    criticalLines.push(`## ${b.brandName} (\`${b.brandSlug}\`)`);
    criticalLines.push("");
    for (const f of critical) {
      criticalLines.push(
        `- **${f.section}** · ${f.failureType}: ${mdEscape(f.currentValue)} → ${mdEscape(f.proposedFix)}`
      );
    }
    criticalLines.push("");
  }
  fs.writeFileSync(criticalPath, `${criticalLines.join("\n")}\n`, "utf8");

  const byBrandLines = ["# Global Active Semantic Audit — By Brand", ""];
  for (const b of report.brandResults) {
    byBrandLines.push(`## ${b.brandName} (\`${b.brandSlug}\`)`);
    byBrandLines.push(`Classification: **${b.classification}**`);
    byBrandLines.push("");
    if (!b.findings?.length) {
      byBrandLines.push("_No findings — freeze_safe_");
    } else {
      for (const f of b.findings) {
        byBrandLines.push(
          `- [${f.severity}] ${f.section} · ${f.failureType}: ${mdEscape(f.currentValue)}`
        );
      }
    }
    byBrandLines.push("");
  }
  fs.writeFileSync(byBrandPath, `${byBrandLines.join("\n")}\n`, "utf8");

  const bySectionLines = ["# Global Active Semantic Audit — By Section", ""];
  const sections = [
    "Recent Momentum",
    "Openings / Examples / Properties",
    "Portfolio Mix",
    "Where This Brand Creates the Most Value",
    "Value Creation Scenarios",
    "Geographic Footprint",
    "Internal Language",
    "AI-Assisted Profile footnote",
    "Image Uniqueness",
    "Image Role Match",
  ];
  for (const sec of sections) {
    bySectionLines.push(`## ${sec}`);
    bySectionLines.push("");
    let any = false;
    for (const b of report.brandResults) {
      for (const f of b.findings || []) {
        if (f.section !== sec) continue;
        any = true;
        bySectionLines.push(
          `- \`${b.brandSlug}\` [${f.severity}] ${f.failureType}: ${mdEscape(f.currentValue)}`
        );
      }
    }
    if (!any) bySectionLines.push("_No findings in this section._");
    bySectionLines.push("");
  }
  fs.writeFileSync(bySectionPath, `${bySectionLines.join("\n")}\n`, "utf8");

  const paths = { jsonPath, mdPath, criticalPath, byBrandPath, bySectionPath, docsPath };

  if (refresh) {
    // Also refresh canonical filenames so consumers of the non-refresh path stay current.
    const canonStem = "brand-explorer-global-active-semantic-audit";
    const canon = {
      jsonPath: path.join(REPORTS_DIR, `${canonStem}.json`),
      mdPath: path.join(REPORTS_DIR, `${canonStem}.md`),
      criticalPath: path.join(REPORTS_DIR, `${canonStem}-critical.md`),
      byBrandPath: path.join(REPORTS_DIR, `${canonStem}-by-brand.md`),
      bySectionPath: path.join(REPORTS_DIR, `${canonStem}-by-section.md`),
      docsPath: path.join(DOCS_DIR, `${canonStem}.md`),
    };
    fs.copyFileSync(jsonPath, canon.jsonPath);
    fs.copyFileSync(mdPath, canon.mdPath);
    fs.copyFileSync(criticalPath, canon.criticalPath);
    fs.copyFileSync(byBrandPath, canon.byBrandPath);
    fs.copyFileSync(bySectionPath, canon.bySectionPath);
    fs.copyFileSync(docsPath, canon.docsPath);
    paths.canonical = canon;
  }

  return paths;
}
