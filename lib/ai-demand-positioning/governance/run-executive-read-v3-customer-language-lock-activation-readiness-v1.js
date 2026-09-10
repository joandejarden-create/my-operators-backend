/**
 * ADP Executive Read V3 — customer-language lock + production activation READINESS.
 * Does NOT activate composition. Does NOT rewrite production reports.
 */

import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { join } from "path";
import { runExecutiveReadV3EditorialHardeningPreviewV1 } from "./run-executive-read-v3-editorial-hardening-preview-v1.js";
import {
  ADP_EXECUTIVE_READ_COMPOSITION_V3,
  proposeExecutiveReadCompositionV3,
  COMPOSITION_V3_HISTORICAL_VERSIONING_POLICY,
  COMPOSITION_V3_CROSS_SURFACE_CANONICAL,
  COMPOSITION_V3_FUTURE_PROPERTY_PIPELINE,
} from "./adp-executive-read-composition-v3.js";
import { METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN } from "./adp-methodology-governance-v1.js";

const METHODOLOGY_DEFENSE =
  /\bdoes not,? by itself,? prove\b|\bno causal relationship should be inferred\b|\bcannot establish\b|\bthe model cannot\b|\bno causality\b/i;
const WEAK_BRAND_RHETORIC = /\binventing a weak-brand story\b|\bmanufactur(?:e|ing) a crisis\b/i;

const WATCH_DECISIONS = Object.freeze({
  "Cambridge Beaches Resort & Spa": {
    decision: "REMOVE",
    prior:
      "Continue monitoring recognition of Five Private Coves as a separate proposition-quality signal — secondary to the business/group displacement pattern.",
    reason:
      "Already covered in KEY INSIGHT; would not earn first-90-seconds space beyond Focus Now.",
  },
  "Faranda Collection Bogotá": {
    decision: "REMOVE",
    prior:
      "Choice Hotels distribution recognition remains very low if that membership is still an intentional commercial proposition.",
    reason:
      "Fails Watch materiality: affiliation intentionality not frozen as current/canonical/commercial priority for Exec Read; hedge language is research-note tone; detailed report can retain attribute.",
  },
  "Hotel Caribe by Faranda Grand, a member of Radisson Individuals": {
    decision: "REMOVE",
    prior: "Hilton Cartagena and Bastión Luxury displacement counts in contexts where Caribe remains absent.",
    reason: "Secondary displacers; not first-90-seconds material beside Focus Now on consideration consistency.",
  },
  "Hotel Phillips Kansas City, Curio Collection by Hilton": {
    decision: "REMOVE",
    prior:
      "Fitness Center recognition remains near zero if wellness/fitness remains an intentional offer — secondary to meetings.",
    reason: "Secondary Reality Gap; meetings Focus Now is the executive priority.",
  },
  "JW Marriott Hotel Monterrey Valle": {
    decision: "OMITTED",
    prior: null,
    reason: "No justified Watch (already omitted).",
  },
  "JW Marriott Hotel Santo Domingo": {
    decision: "OMITTED",
    prior: null,
    reason: "No justified Watch (already omitted).",
  },
  "NOW NOW NOHO": {
    decision: "REMOVE",
    prior: "Crosby Street Hotel remains the lead substitute pattern to monitor as entry improves.",
    reason: "Already named in KEY INSIGHT; redundant Watch.",
  },
  "Radisson Hotel Santo Domingo": {
    decision: "REMOVE",
    prior: "Jaragua and El Embajador remain secondary substitutes worth monitoring as JW-overlap needs improve.",
    reason: "JW substitution is the Primary Focus; secondary substitutes do not earn Exec Read space.",
  },
  "Renaissance New York Times Square Hotel": {
    decision: "REMOVE",
    prior:
      "Westin Times Square and Marriott Marquis remain secondary displacers; protect the small business-travel foothold.",
    reason: "Secondary to Times Square entry + Knickerbocker story already in Key Insight / Focus Now.",
  },
  "The St. Regis Cap Cana Resort": {
    decision: "OMITTED",
    prior: null,
    reason: "No justified Watch (already omitted).",
  },
  "The St. Regis Mexico City": {
    decision: "OMITTED",
    prior: null,
    reason: "No justified Watch (already omitted).",
  },
  "Waterstone Resort & Marina": {
    decision: "REMOVE",
    prior:
      "Four Seasons Palm Beach and The Boca Raton remain secondary displacers; wellness territory softness is also worth monitoring.",
    reason: "Eau Palm Beach + waterfront already in Key Insight / Focus Now; secondary peers do not earn Watch.",
  },
  "The Westin Monterrey Valle": {
    decision: "REMOVE",
    prior:
      "Family territory softness relative to other intents — monitor, but do not let it displace the spa priority.",
    reason: "Explicitly secondary to spa priority; fails first-90-seconds test.",
  },
});

const NARROW_COPY_CHANGES = Object.freeze([
  {
    property: "JW Marriott Hotel Santo Domingo",
    section: "WHY_IT_MATTERS",
    reason: "CUSTOMER_LANGUAGE",
    before: "…rather than inventing a weak-brand story.",
    after:
      "That keeps leadership focused on the remaining representation gaps rather than unnecessary broad competitive remediation.",
  },
  {
    property: "The Westin Monterrey Valle",
    section: "WHY_IT_MATTERS",
    reason: "NO_CUSTOMER_FACING_METHODOLOGY_DEFENSE",
    before:
      "It does not, by itself, prove that spa recognition will change Consideration.",
    after:
      "The two signals warrant review together without assuming one is driving the other.",
  },
  {
    property: "Faranda Collection Bogotá",
    section: "WATCH",
    reason: "WATCH_EXECUTIVE_MATERIALITY",
    before: "Choice Hotels distribution recognition…",
    after: "OMITTED",
  },
]);

function wordCount(text) {
  return String(text || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function assessUiReadiness() {
  const cssPath = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.css");
  const jsPath = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
  const htmlPath = join(process.cwd(), "public/owner-ai-demand.html");
  const builderPath = join(
    process.cwd(),
    "lib/ai-demand-positioning/customer/executive-read-v2.js"
  );
  const findings = [];
  if (existsSync(htmlPath)) {
    const html = readFileSync(htmlPath, "utf8");
    if (html.includes('id="adpExecutiveReadNarrative"') && html.includes("<p class=\"adp-executive-read__narrative\"")) {
      findings.push({
        id: "SINGLE_PARAGRAPH_SHELL",
        severity: "BLOCKER",
        note: "Owner/share UI uses a single <p> for narrative; no structured HEADLINE/KEY INSIGHT/FOCUS NOW DOM.",
      });
    }
  }
  if (existsSync(jsPath)) {
    const js = readFileSync(jsPath, "utf8");
    if (js.includes("narrativeEl.textContent") || js.includes("textContent = presentation.narrative")) {
      findings.push({
        id: "FLAT_TEXTCONTENT_RENDER",
        severity: "BLOCKER",
        note: "Renderer assigns flat narrative textContent — V3 sections cannot render with hierarchy/Focus Now emphasis.",
      });
    }
  }
  if (existsSync(builderPath)) {
    const b = readFileSync(builderPath, "utf8");
    if (b.includes("words > 180")) {
      findings.push({
        id: "PRODUCTION_BUILDER_180_WORD_TRUNCATE",
        severity: "BLOCKER",
        note: "executive-read-v2.js truncates narratives above ~180 words; V3 preferred band is 180–280.",
      });
    }
  }
  findings.push({
    id: "NO_ADP_PRINT_PDF_ER_PATH",
    severity: "BLOCKER",
    note: "No dedicated ADP Executive Read print/PDF renderer found; cross-surface parity incomplete.",
  });
  findings.push({
    id: "COMPOSITION_VERSION_NOT_ON_PAYLOAD",
    severity: "BLOCKER",
    note: "Published executiveRead lacks compositionVersion / sections / numericAnchors fields required by V3 contract.",
  });
  findings.push({
    id: "HARDCODED_PREVIEW_COMPOSITIONS",
    severity: "BLOCKER",
    note: "Current V3 texts live in preview governance maps, not a certified-state → insight → compose zero-code path for future hotels (e.g. Bethesda).",
  });
  return {
    cssPath,
    jsPath,
    htmlPath,
    builderPath,
    findings,
    requiredBeforeActivation: [
      "Structured section renderer (HEADLINE / KEY INSIGHT / WHY IT MATTERS / FOCUS NOW / optional WATCH / WHAT TO REVIEW) with Focus Now visual emphasis",
      "Remove or raise 180-word truncate for V3 compositionVersion only",
      "Desktop / tablet / mobile / print visual QA on 200+ word structured ER",
      "Wire owner-payload + published snapshot to compositionVersion=ADP_EXECUTIVE_READ_COMPOSITION_V3",
      "Implement COMPOSITION_V3_FUTURE_PROPERTY_PIPELINE (no hard-coded hotel narratives)",
      "Historical immutability: do not overwrite issued narratives; new compositionVersion on eligible regenerations only",
    ],
  };
}

export function runExecutiveReadV3CustomerLanguageLockActivationReadinessV1() {
  const hardened = runExecutiveReadV3EditorialHardeningPreviewV1();
  const contract = proposeExecutiveReadCompositionV3();
  const ui = assessUiReadiness();

  const hotels = (hardened.sideBySide || []).map((row) => {
    const anchors =
      hardened.NUMERIC_ANCHORS_BY_HOTEL?.find((h) => h.property === row.property)?.anchors || [];
    const sections = hardened.B_CHANGED_SENTENCES_BY_HOTEL // may not have sections
      ? null
      : null;
    return { property: row.property, V3_HARDENED: row.V3_HARDENED, anchors };
  });

  // Prefer structured rows from hardening report fields if present
  const scorecard = (hardened.sideBySide || []).map((row) => {
    const h =
      hardened.B_CHANGED_SENTENCES_BY_HOTEL?.find((x) => x.property === row.property) || null;
    void h;
    const text = row.V3_HARDENED || "";
    const sectionMap = {};
    for (const key of [
      "HEADLINE",
      "KEY INSIGHT",
      "WHY IT MATTERS",
      "FOCUS NOW",
      "WATCH",
      "WHAT TO REVIEW",
    ]) {
      const re = new RegExp(`${key}\\n([\\s\\S]*?)(?=\\n\\n(?:HEADLINE|KEY INSIGHT|WHY IT MATTERS|FOCUS NOW|WATCH|WHAT TO REVIEW)|$)`);
      const m = text.match(re);
      sectionMap[key] = m ? m[1].trim() : null;
    }
    const watchDecision = WATCH_DECISIONS[row.property] || {
      decision: "UNKNOWN",
      reason: "Not listed",
    };
    const hasWatch = Boolean(sectionMap.WATCH);
    const customerLanguagePass =
      !METHODOLOGY_DEFENSE.test(text) && !WEAK_BRAND_RHETORIC.test(text);
    const anchors =
      hardened.NUMERIC_ANCHORS_BY_HOTEL?.find((a) => a.property === row.property)?.anchors || [];
    return {
      property: row.property,
      HEADLINE: sectionMap.HEADLINE,
      KEY_INSIGHT: sectionMap["KEY INSIGHT"],
      WHY_IT_MATTERS: sectionMap["WHY IT MATTERS"],
      FOCUS_NOW: sectionMap["FOCUS NOW"],
      WATCH: hasWatch ? sectionMap.WATCH : "OMITTED",
      WHAT_TO_REVIEW: sectionMap["WHAT TO REVIEW"],
      numericAnchors: anchors.map((a) => ({
        display: a.display,
        section: a.section,
        metricId: a.metricId,
        why: a.why,
        removingWeakensInsight: a.removingWeakensInsight,
        parity: a.displayMatchesResolved,
      })),
      wordCount: wordCount(text),
      customerLanguagePass,
      watchDecision,
      evidenceTrace:
        hardened.G_SECTION_TRACEABILITY?.hotels?.find((x) => x.property === row.property)
          ?.evidenceTrace || null,
    };
  });

  const customerLanguageAllPass = scorecard.every((h) => h.customerLanguagePass);
  const watchAllMaterial = scorecard.every(
    (h) => h.WATCH === "OMITTED" || h.watchDecision.decision === "RETAIN"
  );
  const numericOk =
    hardened.gateDetail?.ADP_EXECUTIVE_SELECTIVE_NUMERIC_ANCHORS &&
    hardened.gateDetail?.ADP_EXECUTIVE_NO_KPI_ENUMERATION &&
    hardened.gateDetail?.ADP_EXECUTIVE_NUMERIC_REFERENCE_PARITY;

  // Cross-hotel template test (lightweight)
  const openings = scorecard.map((h) => String(h.HEADLINE || "").toLowerCase().slice(0, 48));
  const openingDupes = openings.filter((o, i) => o && openings.indexOf(o) === i && openings.lastIndexOf(o) !== i);
  const spVsConsCount = scorecard.filter((h) =>
    /scenario presence is .+ but ai consideration/i.test(h.KEY_INSIGHT || "")
  ).length;
  const crossHotel = {
    duplicateHeadlinePrefixes: openingDupes.length,
    hotelsUsingScenarioVsConsiderationContrast: spVsConsCount,
    note: "Contrast pattern allowed when it IS the insight; fail only if every hotel uses it as the whole story.",
    pass:
      openingDupes.length === 0 &&
      spVsConsCount < scorecard.length &&
      scorecard.every((h) => h.FOCUS_NOW && h.WHAT_TO_REVIEW && h.FOCUS_NOW !== h.WHAT_TO_REVIEW),
  };

  const activationBlockers = [
    ...ui.findings.filter((f) => f.severity === "BLOCKER").map((f) => f.id),
    "COMPOSITION_V3_NOT_WIRED_TO_OWNER_PAYLOAD",
    "FUTURE_PROPERTY_ZERO_CODE_COMPOSER_NOT_IMPLEMENTED",
    "HISTORICAL_IMMUTABILITY_ENFORCEMENT_NOT_IMPLEMENTED",
    "FOUNDER_SIX_PACK_VISUAL_QA_NOT_RUN",
  ];

  // Future zero-code path status
  const zeroCode = {
    required: true,
    implemented: false,
    pipeline: COMPOSITION_V3_FUTURE_PROPERTY_PIPELINE,
    currentState: "PREVIEW_HARDCODED_PER_PROPERTY_MAP",
    bethesdaCompatibleWhenActivated: false,
    gate: "ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ZERO_CODE_PATH",
    pass: false,
  };

  const productionActivationReady =
    customerLanguageAllPass &&
    watchAllMaterial &&
    numericOk &&
    crossHotel.pass &&
    activationBlockers.length === 0;

  const html = `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<title>ADP Exec Read V3 — Customer Language Lock + Activation Readiness</title>
<style>
body{font-family:Georgia,serif;max-width:1100px;margin:24px auto;padding:0 16px;background:#f7f5f0;color:#1a1a1a}
.banner{background:${productionActivationReady ? "#1f3d2b" : "#5c1f1f"};color:#fff;padding:14px 16px;border-radius:6px}
.card{background:#fff;border:1px solid #ddd;border-radius:6px;padding:12px;margin:12px 0}
.focus{border-left:4px solid #1f3d2b;background:#eef5f0;padding:8px 10px;margin:8px 0}
.meta{color:#555;font-size:.9rem}
.tag{display:inline-block;background:#eee;padding:1px 6px;border-radius:3px;font-size:.75rem;margin-right:4px}
</style></head><body>
<div class="banner"><strong>PRODUCTION_ACTIVATION_READY = ${productionActivationReady ? "YES" : "NO"}</strong><br/>
Do not activate. LIVE_PROVIDER_CALLS=0. Methodology unchanged. Composition V3 proposed only.</div>
<h1>Customer-language lock + activation readiness</h1>
<p class="meta">Contract: ${escapeHtml(ADP_EXECUTIVE_READ_COMPOSITION_V3)} · Hotels: ${scorecard.length}</p>
<h2>Activation blockers</h2>
<ul>${activationBlockers.map((b) => `<li>${escapeHtml(b)}</li>`).join("")}</ul>
${scorecard
  .map(
    (h) => `<div class="card">
  <h2>${escapeHtml(h.property)}</h2>
  <p class="meta">${h.wordCount} words · Watch: ${escapeHtml(h.WATCH === "OMITTED" ? "OMITTED" : "PRESENT")} · Customer language: ${h.customerLanguagePass ? "PASS" : "FAIL"}</p>
  <p><strong>HEADLINE</strong><br/>${escapeHtml(h.HEADLINE)}</p>
  <p><strong>KEY INSIGHT</strong><br/>${escapeHtml(h.KEY_INSIGHT)}</p>
  <p><strong>WHY IT MATTERS</strong><br/>${escapeHtml(h.WHY_IT_MATTERS)}</p>
  <div class="focus"><strong>FOCUS NOW</strong><br/>${escapeHtml(h.FOCUS_NOW)}</div>
  <p><strong>WATCH</strong><br/>${escapeHtml(h.WATCH)}</p>
  <p><strong>WHAT TO REVIEW</strong><br/>${escapeHtml(h.WHAT_TO_REVIEW)}</p>
  <p><strong>Numeric anchors</strong><br/>${h.numericAnchors.map((a) => `${escapeHtml(a.display)} (${escapeHtml(a.section)})`).join(" · ") || "—"}</p>
</div>`
  )
  .join("\n")}
</body></html>`;

  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const jsonPath = join(
    outDir,
    "adp-executive-read-v3-customer-language-lock-activation-readiness-v1-latest.json"
  );
  const htmlPath = join(
    outDir,
    "adp-executive-read-v3-customer-language-lock-activation-readiness-v1-latest.html"
  );
  const stamped = join(
    outDir,
    `adp-executive-read-v3-customer-language-lock-activation-readiness-v1-${stamp}.json`
  );

  const report = {
    version: "adp_executive_read_v3_customer_language_lock_activation_readiness_v1",
    auditedAt: new Date().toISOString(),
    LIVE_PROVIDER_CALLS: 0,
    methodologyChanged: false,
    compositionActivated: false,
    productionSummariesRewritten: false,
    doctrine: [METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN],
    hardStop: true,

    A_FINAL_CUSTOMER_LANGUAGE_VERDICT: customerLanguageAllPass
      ? "CUSTOMER_LANGUAGE_LOCK_PASS"
      : "CUSTOMER_LANGUAGE_LOCK_FAIL",
    B_FINAL_NARROW_COPY_CHANGES: NARROW_COPY_CHANGES,
    C_WATCH_RETAIN_REMOVE: Object.entries(WATCH_DECISIONS).map(([property, d]) => ({
      property,
      ...d,
      finalWatch: scorecard.find((h) => h.property === property)?.WATCH || "OMITTED",
    })),
    D_FINAL_13_PROPERTY_V3_COMPOSITION_SCORECARD: scorecard,
    E_NUMERIC_ANCHOR_FINAL_RESULT: {
      pass: Boolean(numericOk),
      hotels: scorecard.map((h) => ({
        property: h.property,
        anchors: h.numericAnchors,
        count: h.numericAnchors.length,
        inPreferredBand: h.numericAnchors.length >= 1 && h.numericAnchors.length <= 3,
      })),
    },
    F_REQUIRED_PRODUCTION_UI_CHANGES: ui,
    G_CROSS_SURFACE_SOURCE_OF_TRUTH_PLAN: COMPOSITION_V3_CROSS_SURFACE_CANONICAL,
    H_HISTORICAL_VERSIONING_POLICY: COMPOSITION_V3_HISTORICAL_VERSIONING_POLICY,
    I_ADP_EXECUTIVE_READ_COMPOSITION_V3: contract,
    J_FUTURE_PROPERTY_ZERO_CODE_RESULT: zeroCode,
    K_ACTIVATION_PLAN: {
      phase1: "Activate V3 composition code behind controlled feature/version flag (compositionVersion).",
      phase2: "Regenerate eligible non-distributed reports as new immutable versions where governance permits.",
      phase3: "Production visual QA + analytical parity (owner + share + print).",
      phase4: "Founder reviews Cambridge, Phillips, JW Santo Domingo, NOHO, Waterstone, Westin Monterrey Valle.",
      phase5: "If six PASS, activate full governed current universe.",
      phase6: "Make V3 default for future Existing Hotel ADP periods via zero-code pipeline.",
      executedInThisTask: false,
    },
    L_ACTIVATION_BLOCKERS: activationBlockers.length ? activationBlockers : "NONE",
    M_PRODUCTION_ACTIVATION_READY: productionActivationReady ? "YES" : "NO",
    N_METHODOLOGY_CHANGED: "NO",
    crossHotelTemplateTest: crossHotel,
    watchMaterialityPass: watchAllMaterial,
    customerLanguagePass: customerLanguageAllPass,
    hardeningGateDetail: hardened.gateDetail,
    jsonPath,
    htmlPath,
  };

  writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  writeFileSync(stamped, JSON.stringify(report, null, 2));
  writeFileSync(htmlPath, html);
  return report;
}
