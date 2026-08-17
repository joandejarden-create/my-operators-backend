/**
 * Tribute Portfolio by Marriott — source-backed package APPLY PLAN (dry-run default).
 *
 * Turns the package dry-run into a concrete, staged apply plan:
 *   register sources → steward (approve for explorer/extraction) → extract Pending
 *   facts → fact stewardship → governance readiness → publish (Company Materials).
 *
 * Reuses the existing factory: package report, brand-source-auto-resolver,
 * profile-governance publish-readiness taxonomy, and the Source Library field map.
 *
 * Writes NOTHING to Airtable. Produces validated registration payloads, proposed
 * facts, held/human-review facts, Brand Setup completion DRAFTS (report only),
 * asset/PR gaps, and the exact apply command sequence. Does not overwrite Brand
 * Setup content/hero/logo, does not set Company Validated / Company Validation Date.
 *
 * @see docs/data-intelligence/tribute-portfolio-brand-package-pilot.md
 */
import { buildTributePortfolioBrandPackageReport, TRIBUTE_RECORD_ID, BRAND_NAME, COMPANY_FOLDER } from "./tribute-portfolio-brand-package.js";
import { SOURCE_ROLE } from "./brand-source-auto-resolver.js";
import {
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  mapSourceToProfileSourceType,
  classifySourceBasisBucket,
} from "./profile-governance-publish-readiness.js";

export const APPLY_PLAN_VERSION = "1";
export const REPORT_JSON_NAME = "tribute-portfolio-package-apply-plan.json";
export const REPORT_MD_NAME = "tribute-portfolio-package-apply-plan.md";

/** Registration recipe per role: PI Source Type + Origin + capture --type + default quality. */
const ROLE_REGISTRATION = {
  [SOURCE_ROLE.CONSUMER_PAGE]: { sourceType: "Website Capture", sourceOrigin: "Public Web", captureType: "website-capture", quality: "Medium" },
  [SOURCE_ROLE.DEVELOPMENT_PAGE]: { sourceType: "Website Capture", sourceOrigin: "Public Web", captureType: "website-capture", quality: "Medium" },
  [SOURCE_ROLE.LOCAL_PDF]: { sourceType: "FDD", sourceOrigin: "FDD Library", captureType: "fdd", quality: "High" },
  [SOURCE_ROLE.PRESS_PAGE]: { sourceType: "Press Release", sourceOrigin: "Public Web", captureType: "press", quality: "Medium" },
  [SOURCE_ROLE.PR_OPENING]: { sourceType: "Press Release", sourceOrigin: "Public Web", captureType: "press", quality: "Medium" },
};

/**
 * Facts whose keys must stay held / human-review / internal (FDD legal, fees,
 * Item 19). Never auto-approve, never publish externally without human review.
 */
export const HELD_FACT_KEYS = new Set([
  "be.economics.royaltyPct",
  "be.economics.initialFranchiseFee",
]);

export const HELD_FACT_AREAS = [
  { area: "FDD Item 19 financial performance", reason: "Sensitive; internal-only, human review; no public trust-label display." },
  { area: "Franchise fees / royalty / marketing fund", reason: "Economic terms — human review; do not publish externally as company-validated." },
  { area: "Legal / franchise obligations", reason: "Legal terms — human review; no legal-advice statements; likely internal-only." },
];

/**
 * Target facts (validated registry keys) with expected source role + handling.
 * approvable=true means a source-backed stewardship step may recommend approval;
 * held=true means keep Pending / internal until explicit human review.
 */
export const TARGET_FACTS = [
  { key: "be.identity.brandName", role: "any", extractionType: "Directly Stated", approvable: true, held: false, note: "Brand name — direct." },
  { key: "be.identity.parentCompany", role: SOURCE_ROLE.LOCAL_PDF, extractionType: "Directly Stated", approvable: true, held: false, note: "FDD / brand page — Marriott International." },
  { key: "be.positioning.summary", role: SOURCE_ROLE.CONSUMER_PAGE, extractionType: "Directly Stated", approvable: true, held: false, note: "Consumer page + FDD framing." },
  { key: "be.positioning.guestPromise", role: SOURCE_ROLE.CONSUMER_PAGE, extractionType: "Directly Stated", approvable: true, held: false, note: "Consumer + Bonvoy." },
  { key: "be.positioning.tagline", role: SOURCE_ROLE.CONSUMER_PAGE, extractionType: "Needs Confirmation", approvable: true, held: false, note: "Approve only if source-stated verbatim." },
  { key: "be.overview.developmentModel", role: SOURCE_ROLE.LOCAL_PDF, extractionType: "Directly Stated", approvable: true, held: false, note: "FDD franchise/conversion model (non-fee)." },
  { key: "be.overview.whyValue", role: SOURCE_ROLE.DEVELOPMENT_PAGE, extractionType: "Inferred", approvable: false, held: false, note: "Owner value prop — AI-draft, human review before approve." },
  { key: "be.overview.typicalUseCase", role: SOURCE_ROLE.CONSUMER_PAGE, extractionType: "Inferred", approvable: false, held: false, note: "Conversion / adaptive-reuse fit — AI-draft, review." },
  { key: "be.loyalty.programName", role: SOURCE_ROLE.CONSUMER_PAGE, extractionType: "Directly Stated", approvable: true, held: false, note: "Marriott Bonvoy." },
  { key: "be.footprint.geoIntro", role: SOURCE_ROLE.CONSUMER_PAGE, extractionType: "Needs Confirmation", approvable: false, held: false, note: "Regional relevance — only if source-backed." },
  { key: "be.economics.royaltyPct", role: SOURCE_ROLE.LOCAL_PDF, extractionType: "Directly Stated", approvable: false, held: true, note: "FDD fee — HELD internal / human review." },
  { key: "be.economics.initialFranchiseFee", role: SOURCE_ROLE.LOCAL_PDF, extractionType: "Directly Stated", approvable: false, held: true, note: "FDD fee — HELD internal / human review." },
];

/** Dealality-authored owner guidance (not source facts; report-only drafts). */
const OWNER_GUIDANCE_DRAFTS = [
  { label: "Owner / developer considerations", type: "ai_drafted_company_materials", note: "Draft from FDD + development context; Pending review; never implies Marriott endorsement." },
  { label: "Questions owners should ask", type: "dealality_authored_guidance", note: "Dealality-drafted diligence prompts (conversion terms, PIP scope, Bonvoy economics, territory)." },
  { label: "Conversion / adaptive-reuse fit", type: "ai_drafted_review", note: "Tribute is conversion-friendly soft brand; confirm against FDD/company materials before display." },
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

/* ------------------------------------------------------------------ */
/* Source registration plan                                            */
/* ------------------------------------------------------------------ */

function buildRegistrationPayload(entry) {
  const recipe = ROLE_REGISTRATION[entry.role] || ROLE_REGISTRATION[SOURCE_ROLE.CONSUMER_PAGE];
  const isLocal = entry.origin === "local";
  const title = entry.title || `${BRAND_NAME} — ${entry.role}`;

  const payload = {
    [MAP_PARTNER_SOURCE.profileType]: "Brand",
    [MAP_PARTNER_SOURCE.brand]: [TRIBUTE_RECORD_ID],
    [MAP_PARTNER_SOURCE.sourceTitle]: title,
    [MAP_PARTNER_SOURCE.sourceType]: recipe.sourceType,
    [MAP_PARTNER_SOURCE.sourceOrigin]: recipe.sourceOrigin,
    [MAP_PARTNER_SOURCE.status]: "Found",
    [MAP_PARTNER_SOURCE.visibility]: "Public",
    [MAP_PARTNER_SOURCE.verifiedSource]: "Yes",
    [MAP_PARTNER_SOURCE.sourceQuality]: recipe.quality,
    [MAP_PARTNER_SOURCE.approvedForExtraction]: "No",
    [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
    [MAP_PARTNER_SOURCE.notes]:
      "Tribute Portfolio full-package pilot — company-controlled Marriott source. Stewardship decides extraction/explorer approval. Do not imply Marriott validated.",
  };
  if (isLocal) {
    payload[MAP_PARTNER_SOURCE.localFilePath] = entry.localFilePath;
    payload[MAP_PARTNER_SOURCE.fileType] = /\.pdf$/i.test(entry.localFilePath || "") ? "PDF" : "HTML";
  } else {
    payload[MAP_PARTNER_SOURCE.sourceUrl] = entry.sourceUrl;
  }

  const validationErrors = validatePayload(payload);
  const profileSourceType = mapSourceToProfileSourceType({ sourceType: recipe.sourceType, sourceOrigin: recipe.sourceOrigin });
  const basisBucket = classifySourceBasisBucket({ sourceType: recipe.sourceType, sourceOrigin: recipe.sourceOrigin });

  return {
    role: entry.role,
    origin: entry.origin,
    title,
    sourceUrl: entry.sourceUrl || null,
    localFilePath: entry.localFilePath || null,
    piSourceType: recipe.sourceType,
    sourceOrigin: recipe.sourceOrigin,
    profileSourceType,
    companyControlled: basisBucket === "company",
    payload,
    valid: validationErrors.length === 0,
    validationErrors,
    registrationMethod: isLocal ? "local_file_register" : "download_register",
    registerCommand: isLocal
      ? `Register local file via createPartnerSource (Local File Path="${entry.localFilePath}") — see register-radisson-blu-pdf-sources.mjs pattern (dry-run first)`
      : `npm run partner-reference:download -- --url "${entry.sourceUrl}" --company "${COMPANY_FOLDER}" --brand "${BRAND_NAME}" --brand-id ${TRIBUTE_RECORD_ID} --type ${recipe.captureType} --title "${title}" --apply --register`,
  };
}

function validatePayload(payload) {
  const errors = [];
  const checks = [
    ["profileType", MAP_PARTNER_SOURCE.profileType],
    ["sourceOrigin", MAP_PARTNER_SOURCE.sourceOrigin],
    ["visibility", MAP_PARTNER_SOURCE.visibility],
    ["verifiedSource", MAP_PARTNER_SOURCE.verifiedSource],
    ["sourceQuality", MAP_PARTNER_SOURCE.sourceQuality],
    ["status", MAP_PARTNER_SOURCE.status],
    ["approvedForExtraction", MAP_PARTNER_SOURCE.approvedForExtraction],
    ["approvedForExplorerUse", MAP_PARTNER_SOURCE.approvedForExplorerUse],
  ];
  for (const [selectKey, col] of checks) {
    const allowed = VAL_PARTNER_SOURCE_SELECTS[selectKey];
    const value = payload[col];
    if (allowed && value != null && !allowed.includes(value)) {
      errors.push(`invalid_${selectKey}:${value}`);
    }
  }
  if (!payload[MAP_PARTNER_SOURCE.sourceUrl] && !payload[MAP_PARTNER_SOURCE.localFilePath]) {
    errors.push("missing_url_and_local_path");
  }
  if (!Array.isArray(payload[MAP_PARTNER_SOURCE.brand]) || !payload[MAP_PARTNER_SOURCE.brand].length) {
    errors.push("missing_brand_link");
  }
  return errors;
}

export function buildSourceRegistrationPlan(packageReport) {
  const proposed = packageReport.proposedSourcePackage?.proposed || [];
  const registerable = proposed.filter(
    (p) =>
      p.registrationRecommendation === "register_website_capture" ||
      p.registrationRecommendation === "register_local_document"
  );

  const entries = registerable.map((p) =>
    buildRegistrationPayload({
      role: p.role,
      origin: p.origin,
      title: p.title,
      sourceUrl: p.sourceUrl,
      localFilePath: p.localFilePath,
    })
  );

  const provenanceOnly = proposed
    .filter((p) => p.registrationRecommendation === "provenance_only_js_shell" || p.registrationRecommendation === "hold_unreachable")
    .map((p) => ({ role: p.role, ref: p.sourceUrl || p.localFilePath, reason: p.registrationRecommendation }));

  return {
    readyToRegister: entries,
    readyToRegisterCount: entries.length,
    allValid: entries.every((e) => e.valid),
    allCompanyControlled: entries.every((e) => e.companyControlled),
    provenanceOnly,
    localReady: entries.filter((e) => e.origin === "local"),
    webReady: entries.filter((e) => e.origin === "web"),
  };
}

/* ------------------------------------------------------------------ */
/* Extraction eligibility + proposed facts                             */
/* ------------------------------------------------------------------ */

export function buildExtractionPlan(packageReport, registrationPlan) {
  const rolesReady = new Set(registrationPlan.readyToRegister.map((e) => e.role));
  const extractionEligibleSources = registrationPlan.readyToRegister
    .filter((e) => e.role !== SOURCE_ROLE.PR_OPENING && e.role !== SOURCE_ROLE.PRESS_PAGE && e.role !== SOURCE_ROLE.IMAGE_ASSET)
    .map((e) => ({ role: e.role, ref: e.sourceUrl || e.localFilePath, extractionEligible: true }));

  const proposedFacts = TARGET_FACTS.map((f) => {
    const roleAvailable = f.role === "any" || rolesReady.has(f.role) || rolesReady.has(SOURCE_ROLE.LOCAL_PDF);
    const held = f.held || HELD_FACT_KEYS.has(f.key);
    return {
      fieldKey: f.key,
      expectedSourceRole: f.role,
      extractionType: f.extractionType,
      createAs: "Pending",
      sourceAvailable: roleAvailable,
      approvableBySourceBackedStewardship: f.approvable && !held && roleAvailable,
      held,
      publicVisibility: held ? "Internal Only" : "Public",
      note: f.note,
    };
  });

  return {
    extractionEligibleSources,
    proposedFacts,
    proposedFactCount: proposedFacts.length,
    proposedApprovable: proposedFacts.filter((f) => f.approvableBySourceBackedStewardship).length,
    heldFacts: proposedFacts.filter((f) => f.held),
    heldFactAreas: HELD_FACT_AREAS,
    note: "Extraction apply creates Pending facts only. No auto-approval. FDD fee/Item 19/legal facts stay Internal Only / human-review.",
  };
}

/* ------------------------------------------------------------------ */
/* Brand Setup completion draft (report-only)                          */
/* ------------------------------------------------------------------ */

export function buildBrandSetupCompletionDraft(packageReport) {
  const plan = packageReport.brandSetupCompletionPlan || {};
  return {
    doNotOverwrite: [
      ...(plan.alreadyPopulated || []).map((r) => r.field),
      "Logo (present — preserve)",
      "Explorer Hero (Mock/Demo — do not replace in this step)",
      "Explorer Hero Data Source / Verification",
    ],
    sourceBackable: (plan.alreadyPopulated || []).map((r) => ({
      field: r.field,
      factKey: r.factKey,
      action: "back existing content with source-derived Pending fact (staged; no direct write)",
    })),
    aiDraftedEnhancements: [
      ...(plan.aiDraftable || []).map((r) => ({ label: r.label, factKey: r.factKey, action: "AI-draft, Pending review" })),
      ...OWNER_GUIDANCE_DRAFTS,
    ],
    humanReviewOnly: plan.humanReview || [],
    keepBlank: plan.keepBlank || [],
    stagingOutputOnly: true,
    note: "Completion drafts are report/staging output only — no Brand Setup writes. Existing populated fields preserved.",
  };
}

/* ------------------------------------------------------------------ */
/* Governance readiness path                                           */
/* ------------------------------------------------------------------ */

export function buildGovernanceReadinessPath(packageReport, registrationPlan, extractionPlan) {
  const g = packageReport.governanceRecommendation || {};
  const strongCompanyMaterials =
    registrationPlan.allCompanyControlled &&
    registrationPlan.readyToRegisterCount >= 2 &&
    registrationPlan.readyToRegister.some((e) => e.role === SOURCE_ROLE.LOCAL_PDF || e.role === SOURCE_ROLE.CONSUMER_PAGE);

  return {
    currentReadiness: "blocked_no_sources_registered_yet",
    targetPosture: strongCompanyMaterials ? "company_materials" : "source_informed_fallback",
    expectedGovernance: g.expectedGovernance || null,
    gateSequence: [
      { step: 1, gate: "sources_registered", status: "pending", detail: `${registrationPlan.readyToRegisterCount} company-controlled sources` },
      { step: 2, gate: "sources_approved_for_explorer_and_extraction", status: "pending", detail: "stewardship approves Website Capture + FDD (extraction Yes when readable)" },
      { step: 3, gate: "facts_extracted_pending", status: "pending", detail: `${extractionPlan.proposedFactCount} Pending facts; FDD fee/Item 19 Internal Only` },
      { step: 4, gate: "facts_approved_source_backed", status: "pending", detail: `${extractionPlan.proposedApprovable} approvable; held ${extractionPlan.heldFacts.length}` },
      { step: 5, gate: "readiness_clean_min_3_approved_facts", status: "pending", detail: "≥3 approved publish-scope facts incl. identity + substantive" },
      { step: 6, gate: "governance_publish", status: "pending", detail: "Company Published / Platform Display Allowed / Show Trust Label / AI-Assisted Profile / Company Materials" },
    ],
    doNotPublishUntil: "readiness clean (≥3 approved facts + ≥1 approved Explorer source, no downgrade/conflict)",
    companyValidated: "false / unchanged (never set)",
    companyValidationDate: "unchanged (never set)",
  };
}

/* ------------------------------------------------------------------ */
/* Apply command sequence                                              */
/* ------------------------------------------------------------------ */

function buildApplyCommandSequence(registrationPlan) {
  const cmds = ["# 1. Review this plan (dry-run). No Airtable writes yet."];
  cmds.push("# 2. Register sources (after review):");
  for (const e of registrationPlan.webReady) cmds.push(`   ${e.registerCommand}`);
  for (const e of registrationPlan.localReady) cmds.push(`   ${e.registerCommand}`);
  cmds.push("# 3. Re-run the package + apply-plan dry-run to auto-resolve registered source IDs:");
  cmds.push("   npm run tribute-portfolio-brand-package -- --dry-run");
  cmds.push("   npm run tribute-portfolio-package-apply-plan -- --dry-run");
  cmds.push("# 4. Steward sources → extract (Pending) → fact stewardship → governance publish (each dry-run BEFORE apply).");
  cmds.push("#    Use the existing stewardship/extraction/governance scripts; auto-resolver supplies the allowlist (no manual IDs).");
  return cmds;
}

/* ------------------------------------------------------------------ */
/* Report                                                              */
/* ------------------------------------------------------------------ */

export async function buildTributePortfolioApplyPlanReport({ probeUrls = true, packageReport = null } = {}) {
  const report = packageReport || (await buildTributePortfolioBrandPackageReport({ probeUrls }));

  const registrationPlan = buildSourceRegistrationPlan(report);
  const extractionPlan = buildExtractionPlan(report, registrationPlan);
  const completionDraft = buildBrandSetupCompletionDraft(report);
  const governanceReadiness = buildGovernanceReadinessPath(report, registrationPlan, extractionPlan);
  const applyCommands = buildApplyCommandSequence(registrationPlan);

  return {
    applyPlanVersion: APPLY_PLAN_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry_run",
    airtableModified: false,
    brand: report.brand,
    profileCompleteness: report.profileCompleteness,
    partnerIntelligence: report.partnerIntelligence,
    sourceRegistrationPlan: registrationPlan,
    extractionPlan,
    brandSetupCompletionDraft: completionDraft,
    assetGaps: {
      note: "Asset governance is a FUTURE module — no image downloads/overwrites in this step; Mock/Demo hero preserved.",
      needed: [
        "Official logo confirmation (existing Brand Setup logo present — confirm authoritative)",
        "Hero image candidate (replace Mock/Demo hero — future asset module)",
        "Property images",
        "Room / public-space / lifestyle images",
        "PR / recent-opening imagery",
      ],
      assetGovernanceModule: "does_not_exist_yet",
    },
    prRecentOpeningGaps: {
      newsroom: "news.marriott.com is a JS shell (near-zero readable text) — provenance only; not extraction-eligible.",
      recommendedNextStep: "Rendered snapshot / manual capture of Tribute openings for a future PR/openings source; do not rely on live newsroom for extraction.",
    },
    governanceReadinessPath: governanceReadiness,
    applyCommandSequence: applyCommands,
    nextApplyCommand: registrationPlan.webReady[0]?.registerCommand || null,
    doesNotDo: [
      "Write to Airtable / register / extract / approve / publish in dry-run",
      "Overwrite Brand Setup content, hero, image, or logo fields",
      "Auto-approve facts (source-backed stewardship recommends; human approves)",
      "Publish FDD fee / Item 19 / legal detail externally without human review",
      "Set Company Validated or Company Validation Date",
      "Imply Marriott validated the profile",
      "Download images or change UI/scoring/BAS/OAS/OCS/Deal Readiness/schema",
    ],
  };
}

/* ------------------------------------------------------------------ */
/* Markdown                                                            */
/* ------------------------------------------------------------------ */

export function buildTributePortfolioApplyPlanMarkdown(report) {
  const rp = report.sourceRegistrationPlan;
  const ep = report.extractionPlan;
  const cd = report.brandSetupCompletionDraft;
  const gp = report.governanceReadinessPath;
  const lines = [
    "# Tribute Portfolio — Source-Backed Package Apply Plan",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `Brand: ${report.brand.name} \`${report.brand.recordId}\` · Completeness: **${report.profileCompleteness.category}** · PI sources: ${report.partnerIntelligence.existingSourceCount} · facts: ${report.partnerIntelligence.existingFactCount}`,
    "",
    "## 1. Source registration plan",
    "",
    `- Ready to register: **${rp.readyToRegisterCount}** · all valid: **${rp.allValid}** · all company-controlled: **${rp.allCompanyControlled}**`,
    "",
    "| Role | Origin | PI Source Type | Profile type | Valid | Ref |",
    "|------|--------|----------------|--------------|-------|-----|",
    ...rp.readyToRegister.map(
      (e) => `| ${e.role} | ${e.origin} | ${e.piSourceType} | ${e.profileSourceType} | ${e.valid ? "yes" : "NO"} | ${(e.sourceUrl || e.localFilePath || "").slice(0, 70)} |`
    ),
    "",
    "**Provenance-only (not registered for extraction):**",
    ...(rp.provenanceOnly.length ? rp.provenanceOnly.map((p) => `- ${p.role} (${p.reason}) — ${p.ref}`) : ["- none"]),
    "",
    "## 2. Ready to register (web / local)",
    "",
    `- Web: ${rp.webReady.length} · Local: ${rp.localReady.length}`,
    ...rp.readyToRegister.map((e) => `- [${e.origin}] ${e.title}`),
    "",
    "## 3-4. Extraction eligibility + proposed facts",
    "",
    `- Extraction-eligible sources: ${report.extractionPlan.extractionEligibleSources.length}`,
    `- Proposed facts: **${ep.proposedFactCount}** (Pending) · approvable by source-backed stewardship: **${ep.proposedApprovable}** · held: **${ep.heldFacts.length}**`,
    "",
    "| Fact key | Source role | Extraction | Create as | Approvable | Held | Visibility |",
    "|----------|-------------|------------|-----------|------------|------|------------|",
    ...ep.proposedFacts.map(
      (f) => `| \`${f.fieldKey}\` | ${f.expectedSourceRole} | ${f.extractionType} | ${f.createAs} | ${f.approvableBySourceBackedStewardship ? "yes" : "no"} | ${f.held ? "**HELD**" : "no"} | ${f.publicVisibility} |`
    ),
    "",
    "## 6. Human-review / held facts (FDD)",
    "",
    ...ep.heldFactAreas.map((h) => `- **${h.area}** — ${h.reason}`),
    "",
    "## 7. Brand Setup completion draft (staging output only)",
    "",
    `**Source-backable existing fields (${cd.sourceBackable.length}):**`,
    ...cd.sourceBackable.map((r) => `- ${r.field} → \`${r.factKey || "n/a"}\``),
    "",
    "**AI-drafted enhancements (Pending review):**",
    ...cd.aiDraftedEnhancements.map((r) => `- ${r.label}${r.factKey ? ` → \`${r.factKey}\`` : ""} (${r.type || r.action})`),
    "",
    "**Human-review only:**",
    ...cd.humanReviewOnly.map((r) => `- ${r.field} — ${r.note}`),
    "",
    "## 8. Fields that should NOT be overwritten",
    "",
    ...cd.doNotOverwrite.map((f) => `- ${f}`),
    "",
    "## 9. Asset / image gaps",
    "",
    `- Asset governance module: **${report.assetGaps.assetGovernanceModule}**`,
    ...report.assetGaps.needed.map((a) => `- ${a}`),
    `- ${report.assetGaps.note}`,
    "",
    "## 10. PR / recent-opening gaps",
    "",
    `- ${report.prRecentOpeningGaps.newsroom}`,
    `- Next: ${report.prRecentOpeningGaps.recommendedNextStep}`,
    "",
    "## 11. Governance readiness path",
    "",
    `- Current: **${gp.currentReadiness}** → target **${gp.targetPosture}**`,
    ...gp.gateSequence.map((s) => `  ${s.step}. ${s.gate} — ${s.detail}`),
    `- Do not publish until: ${gp.doNotPublishUntil}`,
    `- Company Validated: ${gp.companyValidated} · Company Validation Date: ${gp.companyValidationDate}`,
    "",
    "## 12. Exact apply command sequence",
    "",
    "```bash",
    ...report.applyCommandSequence,
    "```",
    "",
    "## 13. Airtable modified",
    "",
    `- **${report.airtableModified ? "yes" : "no"}** — dry-run planner only.`,
    "",
    "## Does not do",
    "",
    ...report.doesNotDo.map((d) => `- ${d}`),
    "",
  ];
  return lines.join("\n");
}
