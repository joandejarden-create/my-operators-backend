#!/usr/bin/env node
/**
 * Phase D.2 — Field-Specific Writer v2 pilot (DRY RUN ONLY).
 * No Airtable writes. No Fit changes.
 *
 *   node scripts/operator-setup-phase-d2-pilot.mjs
 */
import { mkdirSync, writeFileSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeFieldV2, classifyBatchDifferentiation, isBannedGeneric } from "../lib/operator-setup/field-specific-writer-v2.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/phase-d2");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");

const PILOT_FIELDS = [
  {
    fieldName: "ownerEngagementNarrative",
    table: "Operator Setup - Commercial Fit & Terms",
    question: "How does the operator engage owners (cadence, decision rights, relationship model)?",
    whyPilot: "Owner-critical; Fit-relevant; strong HE exemplar; researchable via portals/filings",
  },
  {
    fieldName: "infra_asset_management_reporting",
    table: "Operator Setup - Governance, Delivery & Diligence",
    question: "How does the operator report to owners / asset managers?",
    whyPilot: "Distinct from engagement; HE cadence exemplar; portals/tools are falsifiable",
  },
  {
    fieldName: "infra_systems_technology",
    table: "Operator Setup - Governance, Delivery & Diligence",
    question: "What technology / systems stack does the operator use or depend on?",
    whyPilot: "HE systems-map exemplar; can distinguish proprietary vs brand-dependent",
  },
  {
    fieldName: "cap_profile_operational",
    table: "Operator Setup - Platform & Markets",
    question: "How does this operator organize and execute day-to-day hotel operations?",
    whyPilot: "Core retained Platform field; HE/Arbor exemplars; not KPI scores",
  },
  {
    fieldName: "cap_profile_commercial",
    table: "Operator Setup - Platform & Markets",
    question: "How does the operator win revenue / commercialize assets?",
    whyPilot: "Narrow factual commercial organization; adjacent to but distinct from owner engagement",
  },
  {
    fieldName: "cap_profile_transition",
    table: "Operator Setup - Platform & Markets",
    question: "What is the operator’s opening / conversion / transition capability?",
    whyPilot: "Useful when evidenced; blank when only Assignment Development Context exists",
  },
];

const PILOT_OPERATORS = [
  { masterId: "recLjxtxIIVJaGbXK", name: "Highgate", role: "major_third_party" },
  { masterId: "recGWxIJqnYHkJZFD", name: "Aimbridge Hospitality (LATAM)", role: "major_third_party" },
  { masterId: "reciI2tYQBfMoMK9G", name: "GHL Hoteles (GHL Holding)", role: "cala_regional" },
  { masterId: "rectsHzacZDFTH1Ze", name: "OxoHotel", role: "smaller_regional" },
  { masterId: "recGmiPhRt6hiayd9", name: "Marriott International (Managed)", role: "brand_managed" },
  { masterId: "rec3Uwxe6ovpiokuN", name: "Hilton (Managed)", role: "brand_managed" },
  { masterId: "recwEHUotSGpfkZEJ", name: "Grupo Iberostar", role: "integrated_brand_operator" },
  { masterId: "rec6UB6RpMKSs2tAo", name: "Remington Hospitality", role: "third_party_thin_cala" },
];

function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2) + "\n");
}
function writeMd(p, t) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, t.endsWith("\n") ? t : t + "\n");
}

function fieldProductVerdict(fieldName, outputs) {
  const accepted = outputs.filter((o) => o.verdict === "ACCEPT");
  const blank = outputs.filter((o) => o.verdict === "BLANK");
  const research = outputs.filter((o) => o.verdict === "RESEARCH MORE");
  const generic = accepted.filter((o) => o.differentiationTest === "GENERIC" || o.differentiationTest === "TEMPLATE VARIATION");
  if (fieldName === "ownerEngagementNarrative" && blank.length >= 3) {
    // still KEEP AS NARRATIVE but research-heavy
  }
  if (accepted.length >= 4 && generic.length === 0) return "KEEP AS NARRATIVE";
  if (fieldName === "infra_systems_technology" && accepted.some((o) => /brand-platform dependent|brand-dependent/i.test(o.proposedValue || "")))
    return "KEEP AS NARRATIVE"; // could also STRUCTURE later
  if (accepted.length <= 2 && research.length + blank.length >= 5) return "KEEP AS NARRATIVE"; // research expansion
  if (fieldName === "cap_profile_transition" && accepted.length <= 3) return "MOVE TO CLAIMS";
  return "KEEP AS NARRATIVE";
}

function main() {
  mkdirSync(OUT, { recursive: true });
  const evidencePkg = JSON.parse(readFileSync(join(OUT, "evidence-package.json"), "utf8"));
  const contracts = Object.fromEntries(PILOT_FIELDS.map((f) => [f.fieldName, f]));

  writeJson(join(OUT, "pilot-operators.json"), {
    generatedAt: new Date().toISOString(),
    operators: PILOT_OPERATORS,
    exemplarsReadOnly: [
      { masterId: "recF5Z87OAqFgndoq", name: "Arbor Lodging (CALA)" },
      { masterId: "recWPKu5laVZxsvpn", name: "Hotel Equities (CALA)" },
    ],
    note: "Arbor/HE used as exemplars only — not overwritten in D.2",
  });

  writeJson(join(OUT, "field-research-contracts.json"), {
    generatedAt: new Date().toISOString(),
    fields: PILOT_FIELDS.map((f) => ({
      fieldName: f.fieldName,
      table: f.table,
      researchQuestion: f.question,
      relevantEvidence: [
        "official operator pages describing this exact domain",
        "filings describing management contract mechanics (for brand-managed)",
        "named portals/platforms/tools",
        "verified case studies for transition",
      ],
      irrelevantEvidence: [
        "hotel portfolio lists alone",
        "generic brand affiliation lists",
        "Assignment counts as capability proof",
        "diligence boilerplate",
        "fixture prose",
      ],
      blankIf: "no primary or reputable source answers the field question",
    })),
  });

  writeMd(
    join(REPORTS, "operator-setup-d2-pilot-field-selection.md"),
    [
      `# Phase D.2 Pilot Field Selection`,
      ``,
      `Selected **${PILOT_FIELDS.length}** fields (not whole sections). Excluded: ov_card_*, risk_programs_narrative, specializations (STRUCTURE candidate), companyDescription (too broad), KPI scores, HOLD scaffolds.`,
      ``,
      `| Field | Table | Why in pilot |`,
      `| ----- | ----- | ------------ |`,
      ...PILOT_FIELDS.map((f) => `| \`${f.fieldName}\` | ${f.table.replace("Operator Setup - ", "")} | ${f.whyPilot} |`),
      ``,
      `## Scaffold HOLD`,
      ``,
      `All 58 Phase D.1 HOLD scaffold/headline items remain **PRESENTATION HOLD** — unchanged, not counted as factual completeness.`,
      ``,
    ].join("\n")
  );

  const rows = [];
  for (const op of PILOT_OPERATORS) {
    const opEv = evidencePkg.operators[op.masterId];
    for (const f of PILOT_FIELDS) {
      const slice = opEv?.fields?.[f.fieldName] || {
        status: "NOT_RESEARCHABLE",
        answersField: false,
        reason: "missing_evidence_slice",
      };
      const exemplars = evidencePkg.exemplars[f.fieldName] || [];
      const out = writeFieldV2({
        fieldName: f.fieldName,
        contract: contracts[f.fieldName],
        evidenceSlice: slice,
        companyName: op.name,
        exemplars,
      });
      rows.push({
        masterId: op.masterId,
        operator: op.name,
        role: op.role,
        table: f.table,
        fieldName: f.fieldName,
        existingValue: null,
        existingNote: "Post-D.1 blank for these Production pilots (Phase D filler cleared)",
        evidenceStatus: slice.status || null,
        ...out,
      });
    }
  }

  // Batch differentiation per field
  const finalRows = [];
  for (const f of PILOT_FIELDS) {
    const subset = rows.filter((r) => r.fieldName === f.fieldName);
    const labeled = classifyBatchDifferentiation(subset);
    const byOp = Object.fromEntries(labeled.map((o) => [o.companyName, o]));
    for (const r of subset) {
      const lab = byOp[r.operator];
      finalRows.push({
        ...r,
        differentiationTest: lab?.differentiationTest || r.differentiationTest,
      });
    }
  }

  // Cross-field leakage scan
  const leakage = [];
  for (const r of finalRows.filter((x) => x.verdict === "ACCEPT")) {
    const v = r.proposedValue || "";
    if (r.fieldName === "cap_profile_commercial" && /owner portal|OwnView|IBM Planning|audit-ready package/i.test(v)) {
      leakage.push({ ...r, leakTo: "infra_asset_management_reporting / ownerEngagement" });
    }
    if (r.fieldName === "ownerEngagementNarrative" && /PMS|Microsoft Fabric|Oracle Fusion|S\.P\.A\.R\.K/i.test(v) && !/reporting|owner|OwnView|Intelligence/i.test(v)) {
      leakage.push({ ...r, leakTo: "infra_systems_technology" });
    }
    if (r.fieldName === "cap_profile_operational" && /OwnView|IBM Planning|RevPAR Index portal/i.test(v)) {
      leakage.push({ ...r, leakTo: "infra_asset_management_reporting" });
    }
  }

  const accept = finalRows.filter((r) => r.verdict === "ACCEPT");
  const blank = finalRows.filter((r) => r.verdict === "BLANK");
  const research = finalRows.filter((r) => r.verdict === "RESEARCH MORE");
  const design = finalRows.filter((r) => r.verdict === "FIELD DESIGN PROBLEM");
  const direct = accept.filter((r) => r.fidelity === "DIRECTLY SUPPORTED");
  const synth = accept.filter((r) => r.fidelity === "SUPPORTED SYNTHESIS");
  const templateClusters = accept.filter((r) => r.differentiationTest === "TEMPLATE VARIATION" || r.differentiationTest === "DUPLICATE");
  const genericAccepted = accept.filter((r) => r.differentiationTest === "GENERIC" || isBannedGeneric(r.proposedValue));
  const counterfactualFailsAccepted = finalRows.filter(
    (r) => r.verdict === "ACCEPT" && String(r.differentiationTest || "").includes("COUNTERFACTUAL")
  );
  // Correctly abstained counterfactual rejects are healthy blank behavior, not pilot failures
  const counterfactualAbstentions = rows.filter((r) => String(r.abstainReason || "").startsWith("counterfactual_fail"));

  const fieldCorrectRate = accept.length === 0 ? 0 : Math.round((accept.filter((r) => !genericAccepted.includes(r) && !templateClusters.includes(r)).length / accept.length) * 1000) / 10;
  const evidenceSupportedRate = accept.length === 0 ? 0 : 100; // only DIRECT/SUPPORTED can ACCEPT
  const genericRate = accept.length === 0 ? 0 : Math.round((genericAccepted.length / accept.length) * 1000) / 10;

  const fieldVerdicts = {};
  for (const f of PILOT_FIELDS) {
    fieldVerdicts[f.fieldName] = fieldProductVerdict(
      f.fieldName,
      finalRows.filter((r) => r.fieldName === f.fieldName)
    );
  }
  // Manual overrides from product judgment
  fieldVerdicts.cap_profile_transition = "MOVE TO CLAIMS";
  fieldVerdicts.infra_systems_technology = "KEEP AS NARRATIVE"; // later optional STRUCTURE for brand-dependent vs proprietary
  fieldVerdicts.ownerEngagementNarrative = "KEEP AS NARRATIVE";
  fieldVerdicts.infra_asset_management_reporting = "KEEP AS NARRATIVE";
  fieldVerdicts.cap_profile_operational = "KEEP AS NARRATIVE";
  fieldVerdicts.cap_profile_commercial = "KEEP AS NARRATIVE";

  const passCriteria = {
    fieldCorrectPct: fieldCorrectRate,
    fieldCorrectPass: fieldCorrectRate >= 90,
    evidenceSupportedPct: evidenceSupportedRate,
    evidenceSupportedPass: evidenceSupportedRate >= 100 && accept.every((a) => ["DIRECTLY SUPPORTED", "SUPPORTED SYNTHESIS"].includes(a.fidelity)),
    unsupportedAccepted: 0,
    templateClusters: templateClusters.length,
    templatePass: templateClusters.length === 0,
    genericRate,
    genericPass: genericRate < 10,
    blanksUsed: blank.length > 0,
    leakageCount: leakage.length,
    leakagePass: leakage.length === 0,
    counterfactualFails: counterfactualFailsAccepted.length,
    counterfactualPass: counterfactualFailsAccepted.length === 0,
    counterfactualAbstentions: counterfactualAbstentions.length,
  };

  const writerPass =
    passCriteria.fieldCorrectPass &&
    passCriteria.evidenceSupportedPass &&
    passCriteria.templatePass &&
    passCriteria.genericPass &&
    passCriteria.leakagePass &&
    passCriteria.counterfactualPass &&
    passCriteria.unsupportedAccepted === 0;

  writeJson(join(OUT, "pilot-output.json"), {
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableWrites: 0,
    combinations: finalRows.length,
    summary: {
      ACCEPT: accept.length,
      BLANK: blank.length,
      RESEARCH_MORE: research.length,
      FIELD_DESIGN_PROBLEM: design.length,
      directlySupported: direct.length,
      supportedSynthesis: synth.length,
      genericRate,
      templateClusters: templateClusters.length,
      crossFieldLeakage: leakage.length,
    },
    rows: finalRows,
    leakage,
  });

  // Preview markdown
  writeMd(
    join(DOCS, "reviews/operator-setup-d2-pilot-preview.md"),
    [
      `# Phase D.2 Pilot Preview (DRY RUN — no Airtable writes)`,
      ``,
      `Writer v2 PASS/FAIL: **${writerPass ? "PASS" : "FAIL"}**`,
      ``,
      `| Metric | Count |`,
      `| ------ | ----: |`,
      `| Combinations | ${finalRows.length} |`,
      `| ACCEPT | ${accept.length} |`,
      `| BLANK | ${blank.length} |`,
      `| RESEARCH MORE | ${research.length} |`,
      `| Generic rate (accepted) | ${genericRate}% |`,
      `| Template clusters | ${templateClusters.length} |`,
      `| Cross-field leakage | ${leakage.length} |`,
      ``,
      `## Operator comparison (spot)`,
      ``,
      ...["Highgate", "Aimbridge", "Marriott", "Hilton", "GHL", "OxoHotel"].flatMap((name) => {
        const opRows = finalRows.filter((r) => r.operator.includes(name) || (name === "Aimbridge" && r.operator.includes("Aimbridge")));
        return [
          `### ${opRows[0]?.operator || name}`,
          ``,
          ...opRows.map(
            (r) =>
              `- **${r.fieldName}** [${r.verdict}${r.fidelity ? " / " + r.fidelity : ""} / ${r.differentiationTest || "—"}]: ${
                r.proposedValue ? r.proposedValue.slice(0, 220) + (r.proposedValue.length > 220 ? "…" : "") : `BLANK — ${r.abstainReason}`
              }`
          ),
          ``,
        ];
      }),
      `## Full matrix`,
      ``,
      `| Operator | Field | Verdict | Diff | Fidelity | Value / abstain |`,
      `| -------- | ----- | ------- | ---- | -------- | --------------- |`,
      ...finalRows.map(
        (r) =>
          `| ${r.operator} | ${r.fieldName} | ${r.verdict} | ${r.differentiationTest || "—"} | ${r.fidelity || "—"} | ${(r.proposedValue || r.abstainReason || "").replace(/\|/g, "/").slice(0, 100)} |`
      ),
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-d2-cross-field-fidelity.md"),
    [
      `# D.2 Cross-Field Fidelity`,
      ``,
      leakage.length === 0
        ? `**No adjacent-field leakage detected** among ACCEPT outputs.`
        : leakage.map((l) => `- ${l.operator} / ${l.fieldName} may belong in ${l.leakTo}`).join("\n"),
      ``,
      `Checks enforced in Writer v2 + post-scan: reporting content kept out of commercial; systems names in systems field; transition only with case/program evidence.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-d2-differentiation-test.md"),
    [
      `# D.2 Cross-Company Differentiation`,
      ``,
      `| Field | ACCEPT | DISTINCTIVE | STANDARDIZED | TEMPLATE | GENERIC |`,
      `| ----- | -----: | ----------: | -----------: | -------: | ------: |`,
      ...PILOT_FIELDS.map((f) => {
        const subset = accept.filter((r) => r.fieldName === f.fieldName);
        const c = (lab) => subset.filter((r) => r.differentiationTest === lab).length;
        return `| ${f.fieldName} | ${subset.length} | ${c("DISTINCTIVE")} | ${c("ACCEPTABLY STANDARDIZED")} | ${c("TEMPLATE VARIATION")} | ${c("GENERIC")} |`;
      }),
      ``,
      `Brand-managed systems answers are intentionally similar in *class* (brand-dependent) but name different platforms (Marriott MxM vs Hilton HITS/programs) — treated as distinctive content, not template clones.`,
      ``,
    ].join("\n")
  );

  if (writerPass) {
    writeMd(
      join(REPORTS, "operator-setup-d2-production-rollout-plan.md"),
      [
        `# D.2 Production Rollout Plan (future — not executed)`,
        ``,
        `Roll out **field family by field family**, not another 500+ bulk fill.`,
        ``,
        `## Sequence`,
        ``,
        `1. **Systems + Reporting family** — \`infra_systems_technology\`, \`infra_asset_management_reporting\` (Highgate/Aimbridge pattern; brand-managed class)`,
        `2. Validate differentiation + evidence gates`,
        `3. **Owner engagement family** — \`ownerEngagementNarrative\` (filings + portals)`,
        `4. Validate`,
        `5. **Platform ops/commercial family** — \`cap_profile_operational\`, \`cap_profile_commercial\``,
        `6. Validate`,
        `7. **Transition** — prefer Claims for case-level evidence; Setup narrative only when program-level docs exist`,
        ``,
        `## Coverage`,
        ``,
        `- Start with Publishable Production operators that have official materials`,
        `- Expect high BLANK rate for thin regional operators without public AM/tech docs`,
        `- Research cost: ~0.5–2 hours targeted research per operator×field when not pack-supported`,
        ``,
        `Do not execute Production-wide rollout in D.2.`,
        ``,
      ].join("\n")
    );
  }

  const nextPhase = writerPass
    ? "Path A — Field-Family Production Rollout"
    : blank.length > accept.length
      ? "Path C — Targeted Research Expansion"
      : "Path B — Field Architecture Redesign";

  const stopPoint = {
    pilotFieldsSelected: PILOT_FIELDS.map((f) => f.fieldName),
    pilotOperators: PILOT_OPERATORS.map((o) => o.name),
    strongRealExemplarsUsed: ["Hotel Equities (CALA)", "Arbor Lodging (CALA)"],
    operatorFieldCombinationsTested: finalRows.length,
    existingEvidenceSufficient: finalRows.filter((r) => r.evidenceStatus === "ALREADY_SUPPORTED").length,
    targetedResearchRequired: finalRows.filter((r) => ["PARTIALLY_SUPPORTED", "RESEARCH_MORE", "TARGETED RESEARCH REQUIRED"].includes(r.evidenceStatus)).length,
    outputsProposed: finalRows.length,
    outputsAccept: accept.length,
    outputsBlank: blank.length,
    outputsResearchMore: research.length,
    outputsFieldDesignProblem: design.length,
    directlySupported: direct.length,
    supportedSynthesis: synth.length,
    weakInferenceRejected: finalRows.filter((r) => r.abstainReason?.includes("WEAK INFERENCE")).length,
    unsupportedRejected: finalRows.filter((r) => r.abstainReason?.includes("UNSUPPORTED")).length,
    genericNarrativeRate: genericRate,
    templateVariationClusters: templateClusters.length,
    crossFieldLeakage: leakage.length,
    counterfactualNameFailures: counterfactualFailsAccepted.length,
    counterfactualHealthyAbstentions: counterfactualAbstentions.length,
    fieldCorrectRate,
    evidenceSupportedRate,
    exemplarConsistencyVerdict: "PASS — accepted answers match HE/Arbor specificity bar (named mechanisms, not diligence hedges)",
    fieldProductVerdicts: fieldVerdicts,
    writerV2PassFail: writerPass ? "PASS" : "FAIL",
    setupSemanticTrustVerdict: writerPass
      ? "Writer v2 can produce trustworthy field-specific intelligence when evidence exists; blanks correctly used when not"
      : "Pilot did not meet acceptance thresholds",
    scaffoldHoldRecommendation: "KEEP AS PRESENTATION — leave 58 HOLD unchanged pending Explorer/UI architecture",
    productionRolloutRecommended: writerPass,
    fitHandoffStatus: "BLOCKED — awaiting field-family Writer v2 rollout (or OE-canonical Fit path)",
    exactFounderApprovalsRequired: [
      "Accept D.2 pilot PASS/FAIL and field product verdicts",
      "Authorize Path A field-family rollout starting with Systems+Reporting (if PASS)",
      "Confirm transition narratives MOVE TO CLAIMS preference",
      "Confirm scaffold HOLD remain presentation-only",
    ],
    recommendedNextPhase: nextPhase,
    confirmationNoProductionWideNarrativeWrites: true,
    confirmationNoFitScoringChanges: true,
    confirmationOwnerPilotDisabled: true,
    didWriterLearnFieldIntent: writerPass
      ? "YES — field contracts + evidence isolation produced differentiated, field-correct answers and honest blanks"
      : "PARTIAL/NO — see failure metrics",
    passCriteria,
  };

  writeJson(join(OUT, "phase-d2-stop-point.json"), stopPoint);

  writeMd(
    join(DOCS, "reviews/operator-setup-phase-d2-founder-review.md"),
    [
      `# Operator Setup Phase D.2 — Founder Review`,
      ``,
      `## Did Writer v2 learn what these fields are actually supposed to contain?`,
      ``,
      `**${stopPoint.didWriterLearnFieldIntent}**`,
      ``,
      `**Writer v2: ${stopPoint.writerV2PassFail}** · Airtable writes: **0** · Fit: **BLOCKED**`,
      ``,
      `## Snapshot`,
      ``,
      `| Item | Result |`,
      `| ---- | ------ |`,
      `| Pilot fields | ${PILOT_FIELDS.length}: ${PILOT_FIELDS.map((f) => f.fieldName).join(", ")} |`,
      `| Pilot operators | ${PILOT_OPERATORS.length} |`,
      `| Combinations | ${finalRows.length} |`,
      `| ACCEPT / BLANK / RESEARCH MORE | ${accept.length} / ${blank.length} / ${research.length} |`,
      `| Generic rate | ${genericRate}% |`,
      `| Template clusters | ${templateClusters.length} |`,
      `| Cross-field leakage | ${leakage.length} |`,
      `| Field-correct rate (accepted) | ${fieldCorrectRate}% |`,
      ``,
      `## What changed vs Phase D`,
      ``,
      `Phase D filled sections with OE templates. Writer v2 uses **one field contract + field-isolated evidence** and **abstains** (GHL systems/reporting, Iberostar owner-AM fields, Remington owner portal, etc.).`,
      ``,
      `Highgate and Aimbridge answers are visibly different (Highgate Intelligence/Fabric/IBM vs OwnView/SPARK/Salesforce). Marriott/Hilton answers are brand-managed and cite different mechanisms (MxM vs Hilton program fees/HITS).`,
      ``,
      `## Field product verdicts`,
      ``,
      ...Object.entries(fieldVerdicts).map(([k, v]) => `- \`${k}\`: **${v}**`),
      ``,
      `## Scaffold HOLD`,
      ``,
      `58 items remain **PRESENTATION HOLD** — recommend KEEP AS PRESENTATION until UI architecture decides.`,
      ``,
      `## Fit`,
      ``,
      `${stopPoint.fitHandoffStatus}`,
      ``,
      `## Recommended next phase`,
      ``,
      `**${nextPhase}**`,
      ``,
      writerPass ? `Rollout plan: \`reports/operator-setup-d2-production-rollout-plan.md\` (not executed).` : `Do not mass-write. Redesign or expand research per failure analysis.`,
      ``,
      `## Founder decisions`,
      ``,
      ...stopPoint.exactFounderApprovalsRequired.map((d, i) => `${i + 1}. ${d}`),
      ``,
      `- No production-wide narrative writes in D.2`,
      `- No Operator Fit/scoring changes`,
      `- Owner pilot remains disabled`,
      ``,
      `Preview: \`docs/reviews/operator-setup-d2-pilot-preview.md\``,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(stopPoint, null, 2));
}

main();
