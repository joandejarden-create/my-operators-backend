/**
 * Brand Explorer Evidence-Required Slot Readiness Plan v22.
 *
 * Read-only plan for post-v20B/v21B Tribute Portfolio presentation gaps.
 * Classifies evidence-required, media-required, and unresolved slots — no Airtable writes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import { HELD_FACT_KEYS } from "./tribute-portfolio-package-apply-plan.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME } from "./tribute-portfolio-brand-package.js";

export const PLAN_VERSION = "22";
export const REPORT_JSON_NAME = "brand-explorer-evidence-required-slot-readiness-plan.json";
export const REPORT_MD_NAME = "brand-explorer-evidence-required-slot-readiness-plan.md";
export const DOC_MD_NAME = "brand-explorer-evidence-required-slot-readiness-plan-v22.md";

const DEFAULT_BRAND_ID = TRIBUTE_RECORD_ID;

const READINESS = {
  APPROVED_EXTERNAL: "existing_approved_evidence_available",
  INTERNAL_ONLY: "existing_evidence_internal_only",
  HUMAN_REVIEW: "existing_evidence_needs_human_review",
  NEW_CAPTURE: "needs_new_source_capture",
  ASSET_REVIEW: "needs_asset_review",
  REMAIN_BLANK: "should_remain_blank",
  NOT_APPLICABLE: "not_applicable_to_tribute",
};

const V23_BATCH = {
  A: "v23A_source_backed_safe_writer",
  B: "v23B_human_review_evidence_writer",
  C: "v23C_media_asset_work",
  D: "v23D_remain_blank_not_applicable",
};

const SOURCE_IDS = {
  consumer: "recF0qS9JIZjM3qza",
  brandPage: "recNvITV5HzuQburM",
  development: "recSLu3N7s84rIKS6",
  developmentHome: "recZmeduOoM1PZEpT",
  fdd: "recjVfKnl9q18MO5w",
  bonvoy: "recu6AFRZBBBNiCQn",
};

const TARGET_SLOT_PATTERNS = [
  /^economics\./i,
  /^loyalty\.(earn|redeem|elite|proof)$/i,
  /^loyalty\.kpi\./i,
  /^overview\.proof\./i,
  /^overview\.proof_operator$/i,
  /^footprint\.momentum$/i,
  /^footprint\.openings$/i,
  /^materials\.caseStudy$/i,
  /^materials\.gallery\.3$/i,
  /^standards\.last_reviewed$/i,
  /^standards\.requirement$/i,
  /^overview\.portfolio_context$/i,
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-slot-completion-remaining-plan.md",
  "reports/brand-explorer-slot-completion-remaining-plan.json",
  "reports/brand-explorer-slot-standard-manifest.md",
  "reports/brand-explorer-slot-standard-manifest.json",
  "reports/brand-explorer-presentation-slot-coverage-audit.md",
  "reports/brand-explorer-remaining-editorial-slot-completion-writer.md",
  "reports/brand-explorer-slot-completion-writer.md",
  "reports/tribute-portfolio-package-pipeline.md",
  "reports/tribute-portfolio-package-pipeline.json",
  "reports/brand-explorer-visual-qa-verification.md",
  "reports/tribute-portfolio-targeted-extract.json",
  "reports/tribute-visual-asset-slot-review.json",
  "api/brand-library.js",
  "docs/brand-explorer-presentation-slots.md",
  "docs/data-intelligence/BRAND_PROFILE_DATA_MODEL.md",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return DEFAULT_BRAND_ID;
  }
  return nz(raw);
}

function readJson(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

function isTargetSlot(slotKey) {
  return TARGET_SLOT_PATTERNS.some((re) => re.test(slotKey));
}

function tabFromSlot(slotKey) {
  if (/^economics\./i.test(slotKey)) return "Economics";
  if (/^loyalty\./i.test(slotKey)) return "Loyalty Program";
  if (/^overview\./i.test(slotKey) || /^hero\./i.test(slotKey)) return "Overview";
  if (/^footprint\./i.test(slotKey)) return "Footprint & Growth";
  if (/^materials\./i.test(slotKey)) return "Brand Materials";
  if (/^standards\./i.test(slotKey)) return "Owner Considerations";
  return "Unknown";
}

function evidenceTypeForSlot(slotKey) {
  const key = nz(slotKey);
  if (/^economics\./i.test(key)) {
    return "FDD / franchise economics — fee stack, term, lifecycle cash rhythm, negotiability, legal themes (internal review required)";
  }
  if (/^loyalty\.kpi\./i.test(key)) {
    return "Approved numeric loyalty facts — member scale, hotel participation, markets, loyalty mix";
  }
  if (key === "loyalty.proof") {
    return "Source-backed loyalty proof headline — program scale or owner-benefit theme without performance guarantees";
  }
  if (/^loyalty\.(earn|redeem|elite)$/i.test(key)) {
    return "Marriott Bonvoy program mechanics — earn/redeem/elite rules stated on approved Bonvoy source";
  }
  if (/^overview\.proof\./i.test(key) || key === "overview.proof_operator") {
    return "Property-level proof narrative — asset, market, situation, brand-fit with verifiable source";
  }
  if (key === "footprint.momentum") {
    return "Dated opening/PR items — property name, date, verifiable Marriott or hotel URL";
  }
  if (key === "footprint.openings") {
    return "Recent-opening cards — dated property narrative with source URL";
  }
  if (key === "materials.caseStudy") {
    return "Case-study package — property overview, owner objective, brand relevance, optional image/URL";
  }
  if (key === "materials.gallery.3") {
    return "Approved visual asset — Explorer Image Gallery slot 3 with rights/usage review";
  }
  if (key === "standards.last_reviewed") {
    return "Governance metadata — Last Reviewed Date / FDD vintage / stewardship review timestamp";
  }
  if (key === "standards.requirement") {
    return "Brand standards requirement rows — source-backed owner planning considerations";
  }
  if (key === "overview.portfolio_context") {
    return "Portfolio ladder positioning — CHI-style tier map (not applicable to Marriott Tribute)";
  }
  return "Approved source-backed evidence";
}

function factKeysForSlot(slotKey) {
  const key = nz(slotKey);
  const map = {
    "loyalty.earn": ["be.loyalty.earnMechanics", "be.loyalty.programName"],
    "loyalty.redeem": ["be.loyalty.redeemMechanics", "be.loyalty.programName"],
    "loyalty.elite": ["be.loyalty.eliteTiers", "be.loyalty.programName"],
    "loyalty.proof": ["be.loyalty.memberCount", "be.loyalty.roomContributionPct", "be.loyalty.programName"],
    "loyalty.kpi.hotels": ["be.loyalty.participatingHotels", "be.footprint.globalHotels"],
    "loyalty.kpi.markets": ["be.footprint.globalMarkets", "be.loyalty.participatingMarkets"],
    "loyalty.kpi.members": ["be.loyalty.memberCount"],
    "loyalty.kpi.mix": ["be.loyalty.roomContributionPct", "be.loyalty.enterpriseBookingPct"],
    "standards.last_reviewed": ["be.meta.lastReviewedDate"],
    "standards.requirement": ["be.standards.requirements"],
    "materials.caseStudy": ["be.materials.caseStudy"],
    "footprint.momentum": ["be.footprint.recentOpenings"],
    "overview.proof_operator": ["be.proof.operator"],
  };
  if (map[key]) return map[key];
  if (/^overview\.proof\.\d+$/i.test(key)) return [`be.proof.property${key.split(".").pop()}`];
  if (/^economics\./i.test(key)) {
    return [
      "be.economics.royaltyPct",
      "be.economics.initialFranchiseFee",
      "be.economics.feeStack",
      "be.economics.termRenewal",
      "be.overview.developmentModel",
    ];
  }
  return [];
}

function isApprovedFact(fact) {
  const status = nz(fact?.humanReviewStatus);
  return status === "Approved" || status === "Edited";
}

function isHeldOrInternalFact(fact) {
  if (HELD_FACT_KEYS.has(nz(fact?.fieldName))) return true;
  if (nz(fact?.publicVisibility) === "Internal Only") return true;
  if (/HOLD|internal|Item\s*19|FDD economics/i.test(nz(fact?.reviewerNotes))) return true;
  return false;
}

function isPlaceholderFact(fact) {
  return nz(fact?.dataGap) === "Yes" || /data\s*gap|placeholder/i.test(nz(fact?.reviewerNotes));
}

function matchFacts(facts, fieldKeys) {
  const keys = new Set(fieldKeys.map((k) => nz(k).toLowerCase()));
  return facts.filter((f) => keys.has(nz(f.fieldName).toLowerCase()));
}

function sourceById(sources, id) {
  return sources.find((s) => s.id === id) || null;
}

function approvedExplorerSources(sources) {
  return sources.filter(
    (s) =>
      (nz(s.status) === "Approved" || nz(s.status) === "Extracted") &&
      nz(s.approvedForExplorerUse) === "Yes"
  );
}

function classifySlot(slotKey, context) {
  const { facts, sources, manifestRow, visualReview, remainingPlan } = context;
  const key = nz(slotKey);

  if (key === "overview.portfolio_context") {
    return buildClassification({
      slotKey: key,
      readiness: READINESS.REMAIN_BLANK,
      v23Batch: V23_BATCH.D,
      evidenceType: evidenceTypeForSlot(key),
      existingEvidence: "Intentionally blank for Tribute — Marriott portfolio ladder not CHI-style.",
      sourceRecordIds: [],
      factRecordIds: [],
      evidenceApproved: false,
      externalDisplaySafe: false,
      fddEconomicsInternalOnly: false,
      proposedNextAction: "Leave blank; do not force CHI portfolio-context ladder for Tribute.",
    });
  }

  if (key === "footprint.openings" || manifestRow?.classification === "not_applicable_to_tribute") {
    return buildClassification({
      slotKey: key,
      readiness: READINESS.NOT_APPLICABLE,
      v23Batch: V23_BATCH.D,
      evidenceType: evidenceTypeForSlot(key),
      existingEvidence: "Manifest marks footprint.openings not applicable until dated Tribute opening package exists.",
      sourceRecordIds: [],
      factRecordIds: [],
      evidenceApproved: false,
      externalDisplaySafe: false,
      fddEconomicsInternalOnly: false,
      proposedNextAction: "Remain not applicable; capture dated PR/opening sources before reconsidering.",
    });
  }

  if (key === "footprint.momentum") {
    return buildClassification({
      slotKey: key,
      readiness: READINESS.REMAIN_BLANK,
      v23Batch: V23_BATCH.D,
      evidenceType: evidenceTypeForSlot(key),
      existingEvidence:
        "No dated, source-backed Tribute opening timeline. Marriott newsroom URL is JS-shell / provenance-only.",
      sourceRecordIds: [],
      factRecordIds: matchFacts(facts, factKeysForSlot(key)).map((f) => f.id),
      evidenceApproved: false,
      externalDisplaySafe: false,
      fddEconomicsInternalOnly: false,
      proposedNextAction:
        "Remain blank until dated property-opening PR or hotel-page captures are stewarded (not JS-shell newsroom).",
      evidenceGaps: ["Dated opening items with property name, month/year, and verifiable URL"],
    });
  }

  if (/^economics\./i.test(key)) {
    const fdd = sourceById(sources, SOURCE_IDS.fdd);
    const econFacts = matchFacts(facts, factKeysForSlot(key));
    const heldFacts = econFacts.filter(isHeldOrInternalFact);
    return buildClassification({
      slotKey: key,
      readiness: READINESS.INTERNAL_ONLY,
      v23Batch: V23_BATCH.D,
      evidenceType: evidenceTypeForSlot(key),
      existingEvidence: fdd
        ? `2026 Tribute FDD registered (${fdd.id}) — economics/fees/Item 19 held at stewardship.`
        : "FDD source missing.",
      sourceRecordIds: fdd ? [fdd.id] : [],
      factRecordIds: heldFacts.map((f) => f.id),
      evidenceApproved: heldFacts.some(isApprovedFact),
      externalDisplaySafe: false,
      fddEconomicsInternalOnly: true,
      proposedNextAction:
        "Keep internal-only. Human review required before any external economics copy; do not invent fees, KPIs, or Item 19.",
      evidenceGaps: ["Approved external-display economics facts with explicit human sign-off"],
    });
  }

  if (key === "materials.gallery.3") {
    const competing = (visualReview?.competingBySlot || []).find((x) => x.explorerSection === key);
    const candidates = competing?.candidates || [];
    const approvedLocked = candidates.find((c) => c.approvedLocked);
    return buildClassification({
      slotKey: key,
      readiness: READINESS.ASSET_REVIEW,
      v23Batch: V23_BATCH.C,
      evidenceType: evidenceTypeForSlot(key),
      existingEvidence: candidates.length
        ? `${candidates.length} registry candidate(s); primary scoring candidate ${competing?.primary?.recordId || "n/a"}.`
        : "No approved Explorer visual asset.",
      sourceRecordIds: [],
      factRecordIds: [],
      evidenceApproved: Boolean(approvedLocked),
      externalDisplaySafe: false,
      fddEconomicsInternalOnly: false,
      proposedNextAction:
        "Run visual asset stewardship — pick Ermita v2 candidate or alternate, approve rights/usage, then promote via explorer-media-promotion-writer.",
      evidenceGaps: ["Approved Brand Asset Registry row with Explorer slot approval for materials.gallery.3"],
      assetCandidates: candidates.map((c) => ({
        recordId: c.recordId,
        assetName: c.assetName,
        qualityScore: c.qualityScore,
        approvedLocked: Boolean(c.approvedLocked),
      })),
    });
  }

  if (key === "materials.caseStudy") {
    return buildClassification({
      slotKey: key,
      readiness: READINESS.NEW_CAPTURE,
      v23Batch: V23_BATCH.D,
      evidenceType: evidenceTypeForSlot(key),
      existingEvidence: "Property visual discovery exists but no stewarded case-study narrative package.",
      sourceRecordIds: [SOURCE_IDS.consumer, SOURCE_IDS.brandPage].filter((id) => sourceById(sources, id)),
      factRecordIds: [],
      evidenceApproved: false,
      externalDisplaySafe: false,
      fddEconomicsInternalOnly: false,
      proposedNextAction:
        "Capture property-level case study (situation, owner objective, brand relevance) from approved hotel/PR source — do not invent.",
      evidenceGaps: [
        "Approved property narrative with verifiable asset and URL",
        "Optional case-study image after asset review",
      ],
    });
  }

  if (/^overview\.proof\./i.test(key) || key === "overview.proof_operator") {
    return buildClassification({
      slotKey: key,
      readiness: READINESS.NEW_CAPTURE,
      v23Batch: V23_BATCH.D,
      evidenceType: evidenceTypeForSlot(key),
      existingEvidence: "No approved Tribute property proof narratives.",
      sourceRecordIds: approvedExplorerSources(sources).map((s) => s.id),
      factRecordIds: matchFacts(facts, factKeysForSlot(key)).map((f) => f.id),
      evidenceApproved: false,
      externalDisplaySafe: false,
      fddEconomicsInternalOnly: false,
      proposedNextAction:
        "Register and extract property-level proof sources (hotel page or PR) before any overview.proof writer.",
      evidenceGaps: ["Property proof fact(s) with approved external visibility"],
    });
  }

  if (/^loyalty\.kpi\./i.test(key) || key === "loyalty.proof") {
    const related = matchFacts(facts, factKeysForSlot(key));
    const placeholders = related.filter(isPlaceholderFact);
    return buildClassification({
      slotKey: key,
      readiness: READINESS.NEW_CAPTURE,
      v23Batch: V23_BATCH.D,
      evidenceType: evidenceTypeForSlot(key),
      existingEvidence:
        key === "loyalty.proof"
          ? "No approved loyalty performance proof; Item 19 / mix metrics not cleared for external display."
          : "Numeric loyalty KPI facts not approved — prior placeholders rejected/superseded.",
      sourceRecordIds: [SOURCE_IDS.bonvoy, SOURCE_IDS.consumer].filter((id) => sourceById(sources, id)),
      factRecordIds: related.map((f) => f.id),
      evidenceApproved: related.some((f) => isApprovedFact(f) && !isHeldOrInternalFact(f)),
      externalDisplaySafe: false,
      fddEconomicsInternalOnly: key === "loyalty.proof",
      proposedNextAction:
        "Extract and steward numeric Bonvoy facts from approved sources; loyalty.proof must not cite FDD Item 19 externally.",
      evidenceGaps: ["Approved numeric fact with evidence excerpt and external-display sign-off"],
      rejectedPlaceholderFactIds: placeholders.map((f) => f.id),
    });
  }

  if (/^loyalty\.(earn|redeem|elite)$/i.test(key)) {
    const bonvoy = sourceById(sources, SOURCE_IDS.bonvoy);
    const programFact = facts.find((f) => nz(f.fieldName) === "be.loyalty.programName" && isApprovedFact(f));
    const mechanicFieldKeys = factKeysForSlot(key).filter((k) => k !== "be.loyalty.programName");
    const mechanicFacts = matchFacts(facts, mechanicFieldKeys).filter((f) => !isPlaceholderFact(f));
    const hasApprovedMechanics = mechanicFacts.some((f) => isApprovedFact(f) && !isHeldOrInternalFact(f));
    return buildClassification({
      slotKey: key,
      readiness: hasApprovedMechanics ? READINESS.APPROVED_EXTERNAL : READINESS.HUMAN_REVIEW,
      v23Batch: hasApprovedMechanics ? V23_BATCH.A : V23_BATCH.B,
      evidenceType: evidenceTypeForSlot(key),
      existingEvidence: bonvoy
        ? `Approved Bonvoy source (${bonvoy.id}) readable; program name fact ${programFact?.id || "n/a"} approved; slot mechanics not yet approved.`
        : "Bonvoy source missing.",
      sourceRecordIds: [SOURCE_IDS.bonvoy, SOURCE_IDS.consumer].filter((id) => sourceById(sources, id)),
      factRecordIds: [...new Set([...mechanicFacts, ...(programFact ? [programFact] : [])].map((f) => f.id))],
      evidenceApproved: hasApprovedMechanics,
      externalDisplaySafe: hasApprovedMechanics,
      fddEconomicsInternalOnly: false,
      proposedNextAction: hasApprovedMechanics
        ? "Eligible for v23A only after founder review package confirms verbatim Bonvoy mechanics."
        : "Targeted-extract Bonvoy mechanics into Pending facts; human review before any v23B writer.",
      evidenceGaps: hasApprovedMechanics ? [] : ["Approved earn/redeem/elite mechanic facts tied to Bonvoy excerpt"],
    });
  }

  if (key === "standards.last_reviewed") {
    const metaFact = facts.find((f) => nz(f.fieldName) === "be.meta.lastReviewedDate");
    const fdd = sourceById(sources, SOURCE_IDS.fdd);
    const governanceReady = Boolean(fdd) && !isPlaceholderFact(metaFact);
    return buildClassification({
      slotKey: key,
      readiness: READINESS.HUMAN_REVIEW,
      v23Batch: V23_BATCH.B,
      evidenceType: evidenceTypeForSlot(key),
      existingEvidence: fdd
        ? "2026 FDD vintage + stewardship metadata path; no approved last-reviewed fact yet."
        : "Governance metadata incomplete.",
      sourceRecordIds: fdd ? [fdd.id] : [],
      factRecordIds: metaFact ? [metaFact.id] : [],
      evidenceApproved: metaFact ? isApprovedFact(metaFact) : false,
      externalDisplaySafe: false,
      fddEconomicsInternalOnly: false,
      proposedNextAction:
        "Map to governance Last Reviewed Date after stewardship approves be.meta.lastReviewedDate — do not imply Marriott validation.",
      evidenceGaps: ["Approved be.meta.lastReviewedDate or explicit founder governance date"],
    });
  }

  if (key === "standards.requirement") {
    const fdd = sourceById(sources, SOURCE_IDS.fdd);
    return buildClassification({
      slotKey: key,
      readiness: READINESS.HUMAN_REVIEW,
      v23Batch: V23_BATCH.B,
      evidenceType: evidenceTypeForSlot(key),
      existingEvidence: fdd
        ? "FDD + development captures contain standards themes — not yet extracted into approved requirement rows."
        : "No standards requirement facts.",
      sourceRecordIds: [SOURCE_IDS.fdd, SOURCE_IDS.development, SOURCE_IDS.brandPage].filter((id) =>
        sourceById(sources, id)
      ),
      factRecordIds: matchFacts(facts, factKeysForSlot(key)).map((f) => f.id),
      evidenceApproved: false,
      externalDisplaySafe: false,
      fddEconomicsInternalOnly: true,
      proposedNextAction:
        "Extract standards requirement bullets from FDD/development sources into Pending facts; founder review before v23B writer.",
      evidenceGaps: ["Approved standards requirement facts with source excerpts"],
    });
  }

  const grouped = remainingPlan?.remainingSlotsGrouped || {};
  const bucket = Object.entries(grouped).find(([, slots]) => (slots || []).includes(key))?.[0] || "unknown";
  return buildClassification({
    slotKey: key,
    readiness: READINESS.NEW_CAPTURE,
    v23Batch: V23_BATCH.D,
    evidenceType: evidenceTypeForSlot(key),
    existingEvidence: `Unclassified evidence slot (remaining-plan bucket: ${bucket}).`,
    sourceRecordIds: approvedExplorerSources(sources).map((s) => s.id),
    factRecordIds: [],
    evidenceApproved: false,
    externalDisplaySafe: false,
    fddEconomicsInternalOnly: false,
    proposedNextAction: "Re-assess with stewarded sources before any writer.",
    evidenceGaps: ["Approved evidence for slot"],
  });
}

function buildClassification(fields) {
  return {
    slotKey: fields.slotKey,
    tab: tabFromSlot(fields.slotKey),
    readinessClassification: fields.readiness,
    recommendedV23Batch: fields.v23Batch,
    requiredEvidenceType: fields.evidenceType,
    existingEvidenceFound: fields.existingEvidence,
    sourceRecordIds: fields.sourceRecordIds || [],
    factRecordIds: fields.factRecordIds || [],
    evidenceApproved: Boolean(fields.evidenceApproved),
    externalDisplaySafe: Boolean(fields.externalDisplaySafe),
    fddEconomicsInternalOnly: Boolean(fields.fddEconomicsInternalOnly),
    proposedNextAction: fields.proposedNextAction,
    evidenceGaps: fields.evidenceGaps || [],
    assetCandidates: fields.assetCandidates || [],
    rejectedPlaceholderFactIds: fields.rejectedPlaceholderFactIds || [],
  };
}

function projectCoverageScore(currentScore, missingTargetCount, additionalFilledCount) {
  if (!missingTargetCount || additionalFilledCount <= 0) return currentScore;
  const headroom = Math.max(0, 100 - currentScore);
  const uplift = Math.round((additionalFilledCount / missingTargetCount) * headroom);
  return Math.min(100, currentScore + uplift);
}

function projectManifestScore(manifestRows, extraPresentSlotKeys) {
  const present = new Set(
    manifestRows.filter((r) => r.tributeHasSlot).map((r) => r.slotKey).concat(extraPresentSlotKeys)
  );
  const requiredRows = manifestRows.filter((r) =>
    ["core_required", "tab_required", "soft_brand_required", "candidate_for_tribute_completion"].includes(
      nz(r.classification)
    )
  );
  const optionalRows = manifestRows.filter((r) =>
    ["common_optional", "media_optional", "source_material", "collection_brand_required"].includes(
      nz(r.classification)
    )
  );
  const reqTotal = requiredRows.length || 1;
  const reqHas = requiredRows.filter((r) => present.has(r.slotKey)).length;
  const optTotal = optionalRows.length || 1;
  const optHas = optionalRows.filter((r) => present.has(r.slotKey)).length;
  const evidencePenalty = manifestRows.filter(
    (r) => (r.classification || "") === "requires_source_evidence" && !present.has(r.slotKey)
  ).length;
  const base = (reqHas / reqTotal) * 80 + (optHas / optTotal) * 20;
  const penalty = Math.min(15, evidencePenalty * 0.4);
  return Math.max(0, Math.round(base - penalty));
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function presentationSlotKeys(brand) {
  const blocks = brand?.brandExplorer?.blocks || brand?.brandExplorerPresentation || [];
  const keys = new Set();
  for (const row of Array.isArray(blocks) ? blocks : []) {
    const slotKey = nz(row.slotKey || row.slot_key);
    if (slotKey) keys.add(slotKey);
  }
  return keys;
}

export async function buildBrandExplorerEvidenceRequiredSlotReadinessPlanReport(options = {}) {
  const brandRecordId = normalizeBrandInput(options.brandIdOrName);
  const coverage = readJson("reports/brand-explorer-presentation-slot-coverage-audit.json");
  const remainingPlan = readJson("reports/brand-explorer-slot-completion-remaining-plan.json");
  const manifest = readJson("reports/brand-explorer-slot-standard-manifest.json");
  const pipeline = readJson("reports/tribute-portfolio-package-pipeline.json");
  const visualReview = readJson("reports/tribute-visual-asset-slot-review.json");
  const v20b = readJson("reports/brand-explorer-slot-completion-writer.json");
  const v21b = readJson("reports/brand-explorer-remaining-editorial-slot-completion-writer.json");

  const [brand, liveState] = await Promise.all([
    fetchBrandApiShape(brandRecordId),
    fetchLiveState(brandRecordId),
  ]);
  if (!brand) throw new Error(`Unable to read brand: ${brandRecordId}`);

  const manifestBySlot = new Map((manifest?.slotStandardManifestRows || []).map((r) => [r.slotKey, r]));
  const missing = (coverage?.tributeSlotKeysMissing || []).filter(isTargetSlot);
  const presentKeys = presentationSlotKeys(brand);

  const slotReadiness = missing.map((slotKey) =>
    classifySlot(slotKey, {
      facts: liveState.facts || [],
      sources: liveState.sources || [],
      manifestRow: manifestBySlot.get(slotKey),
      visualReview,
      remainingPlan,
    })
  );

  const byReadiness = {};
  for (const row of slotReadiness) {
    if (!byReadiness[row.readinessClassification]) byReadiness[row.readinessClassification] = [];
    byReadiness[row.readinessClassification].push(row.slotKey);
  }

  const v23A = slotReadiness.filter((r) => r.recommendedV23Batch === V23_BATCH.A).map((r) => r.slotKey);
  const v23B = slotReadiness.filter((r) => r.recommendedV23Batch === V23_BATCH.B).map((r) => r.slotKey);
  const v23C = slotReadiness.filter((r) => r.recommendedV23Batch === V23_BATCH.C).map((r) => r.slotKey);
  const v23D = slotReadiness.filter((r) => r.recommendedV23Batch === V23_BATCH.D).map((r) => r.slotKey);

  const approvedFacts = (liveState.facts || []).filter(isApprovedFact);
  const heldFacts = (liveState.facts || []).filter(isHeldOrInternalFact);
  const pendingFacts = (liveState.facts || []).filter(
    (f) => nz(f.humanReviewStatus) === "Pending" && !isPlaceholderFact(f)
  );

  const sourceIdsUsed = [...new Set(slotReadiness.flatMap((r) => r.sourceRecordIds))];
  const factIdsUsed = [...new Set(slotReadiness.flatMap((r) => r.factRecordIds))];

  const currentPresent = coverage?.tributeSlotKeysPresent?.length || presentKeys.size;
  const currentCoverageScore = coverage?.slotCoverageScore ?? 61;
  const currentManifestScore = manifest?.revisedRealisticTributeCompletionScore ?? null;
  const missingTargetCount = slotReadiness.length || 1;
  const evidenceSupportedSlots = slotReadiness.filter(
    (r) =>
      r.readinessClassification === READINESS.APPROVED_EXTERNAL ||
      r.readinessClassification === READINESS.HUMAN_REVIEW
  );

  const projectedCoverageAfterV23A = projectCoverageScore(currentCoverageScore, missingTargetCount, v23A.length);
  const projectedCoverageAllEvidence = projectCoverageScore(
    currentCoverageScore,
    missingTargetCount,
    evidenceSupportedSlots.length
  );

  const projectedManifestAfterV23A = manifest?.slotStandardManifestRows
    ? projectManifestScore(manifest.slotStandardManifestRows, v23A)
    : null;
  const projectedManifestAllEvidence = manifest?.slotStandardManifestRows
    ? projectManifestScore(
        manifest.slotStandardManifestRows,
        evidenceSupportedSlots.map((r) => r.slotKey)
      )
    : null;

  const tributeCompletedComparableAfterV23A =
    projectedCoverageAfterV23A >= 85 &&
    (byReadiness[READINESS.NEW_CAPTURE] || []).length <= 2 &&
    v23A.length > 0;

  const visualQa = readJson("reports/brand-explorer-visual-qa-verification.json");
  const visualQaPass =
    visualQa?.overallResult === "pass" ||
    visualQa?.pass === true ||
    /pass/i.test(nz(visualQa?.summary));

  return {
    planVersion: PLAN_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    brand: {
      recordId: brandRecordId,
      name: nz(brand.name) || BRAND_NAME,
    },
    postV21BState: {
      slotCoverageScore: currentCoverageScore,
      manifestScore: currentManifestScore,
      displayParityScore: readJson("reports/brand-explorer-display-parity-audit.json")?.displayParityScore ?? null,
      visualQaPass,
      v20BIdempotent:
        v20b?.reconciliation?.matchedCount != null
          ? `${v20b.reconciliation.matchedCount} matched / ${v20b.reconciliation.wouldUpdateCount ?? 0} updates`
          : null,
      v21BIdempotent:
        v21b?.reconciliation?.matchedCount != null
          ? `${v21b.reconciliation.matchedCount} matched / ${v21b.reconciliation.wouldUpdateCount ?? 0} updates`
          : null,
      tributeSlotKeysPresent: currentPresent,
      tributeSlotKeysMissing: coverage?.tributeSlotKeysMissing?.length || 0,
    },
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-evidence-required-slot-readiness-plan.js",
      "scripts/brand-explorer-evidence-required-slot-readiness-plan.mjs",
      "docs/data-intelligence/brand-explorer-evidence-required-slot-readiness-plan-v22.md",
      "reports/brand-explorer-evidence-required-slot-readiness-plan.md",
      "reports/brand-explorer-evidence-required-slot-readiness-plan.json",
      "package.json",
    ],
    v22ReadinessPlanExists: true,
    remainingEvidenceMediaManualSlotsReviewed: slotReadiness.length,
    slotReadinessRows: slotReadiness,
    slotsByReadinessClassification: byReadiness,
    slotsWithApprovedExternalDisplayEvidence: byReadiness[READINESS.APPROVED_EXTERNAL] || [],
    slotsWithInternalOnlyEvidence: byReadiness[READINESS.INTERNAL_ONLY] || [],
    slotsNeedingHumanReview: byReadiness[READINESS.HUMAN_REVIEW] || [],
    slotsNeedingNewSourceCapture: byReadiness[READINESS.NEW_CAPTURE] || [],
    slotsNeedingApprovedMedia: byReadiness[READINESS.ASSET_REVIEW] || [],
    slotsThatShouldRemainBlank: byReadiness[READINESS.REMAIN_BLANK] || [],
    slotsNotApplicableToTribute: byReadiness[READINESS.NOT_APPLICABLE] || [],
    existingSourceRecordsUsed: sourceIdsUsed.map((id) => {
      const src = sourceById(liveState.sources || [], id);
      return {
        id,
        title: nz(src?.title || src?.sourceTitle),
        sourceType: nz(src?.sourceType),
        approvedForExplorerUse: nz(src?.approvedForExplorerUse),
        approvedForExtraction: nz(src?.approvedForExtraction),
      };
    }),
    existingFactRecordsUsed: factIdsUsed.map((id) => {
      const fact = (liveState.facts || []).find((f) => f.id === id);
      return {
        id,
        fieldName: nz(fact?.fieldName),
        humanReviewStatus: nz(fact?.humanReviewStatus),
        publicVisibility: nz(fact?.publicVisibility),
        approved: fact ? isApprovedFact(fact) : false,
      };
    }),
    liveIntelligenceSummary: {
      approvedSources: approvedExplorerSources(liveState.sources || []).length,
      totalSources: (liveState.sources || []).length,
      approvedFacts: approvedFacts.length,
      pendingFacts: pendingFacts.length,
      heldInternalFacts: heldFacts.length,
      pipelineStage: nz(pipeline?.currentStage),
    },
    evidenceGapsBySlot: slotReadiness
      .filter((r) => r.evidenceGaps.length)
      .map((r) => ({ slotKey: r.slotKey, gaps: r.evidenceGaps })),
    recommendedV23Batches: {
      v23A_source_backed_safe_writer: {
        batch: V23_BATCH.A,
        slotKeys: v23A,
        count: v23A.length,
        note: "Only slots with approved external-display evidence. Build review package before any writer.",
      },
      v23B_human_review_evidence_writer: {
        batch: V23_BATCH.B,
        slotKeys: v23B,
        count: v23B.length,
        note: "Bonvoy mechanics, standards rows, governance last-reviewed — extract + founder review first.",
      },
      v23C_media_asset_work: {
        batch: V23_BATCH.C,
        slotKeys: v23C,
        count: v23C.length,
        note: "Approve and promote materials.gallery.3 visual asset.",
      },
      v23D_remain_blank_not_applicable: {
        batch: V23_BATCH.D,
        slotKeys: v23D,
        count: v23D.length,
        note: "Economics internal-only, proof/case-study gaps, momentum blank, not-applicable openings.",
      },
    },
    scoreProjection: {
      currentSlotCoverageScore: currentCoverageScore,
      currentManifestScore: currentManifestScore,
      projectedSlotCoverageAfterV23A: projectedCoverageAfterV23A,
      projectedManifestScoreAfterV23A: projectedManifestAfterV23A,
      projectedSlotCoverageAllEvidenceSupported: projectedCoverageAllEvidence,
      projectedManifestScoreAllEvidenceSupported: projectedManifestAllEvidence,
      tributeCompletedBrandComparableAfterV23A: tributeCompletedComparableAfterV23A,
      tributeCompletedBrandComparableAfterAllEvidenceSafeWork:
        projectedCoverageAllEvidence >= 85 &&
        (byReadiness[READINESS.NEW_CAPTURE] || []).length <= 5,
    },
    exactNextCommand:
      v23A.length > 0
        ? "npm run brand-explorer-evidence-required-slot-readiness-plan -- --brand tribute-portfolio --dry-run"
        : "npm run tribute-portfolio-targeted-extract -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerEvidenceRequiredSlotReadinessPlanMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Evidence-Required Slot Readiness Plan v22");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}** (read-only — no Airtable writes)`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- Slots reviewed: **${report.remainingEvidenceMediaManualSlotsReviewed}**`);
  lines.push("");
  lines.push("## Post-v21B baseline");
  lines.push(`- Slot coverage: **${report.postV21BState.slotCoverageScore}/100**`);
  lines.push(`- Manifest score: **${report.postV21BState.manifestScore ?? "n/a"}/100**`);
  lines.push(`- Present slot keys: **${report.postV21BState.tributeSlotKeysPresent}**`);
  lines.push("");
  lines.push("## Readiness summary");
  lines.push(`- Approved external-display evidence: **${report.slotsWithApprovedExternalDisplayEvidence.length}**`);
  lines.push(`- Internal-only evidence: **${report.slotsWithInternalOnlyEvidence.length}**`);
  lines.push(`- Needs human review: **${report.slotsNeedingHumanReview.length}**`);
  lines.push(`- Needs new source capture: **${report.slotsNeedingNewSourceCapture.length}**`);
  lines.push(`- Needs media/assets: **${report.slotsNeedingApprovedMedia.length}**`);
  lines.push(`- Should remain blank: **${report.slotsThatShouldRemainBlank.length}**`);
  lines.push(`- Not applicable to Tribute: **${report.slotsNotApplicableToTribute.length}**`);
  lines.push("");
  lines.push("## Recommended v23 batches");
  for (const [key, batch] of Object.entries(report.recommendedV23Batches)) {
    lines.push(`### ${key} (${batch.count})`);
    lines.push(batch.note);
    if (batch.slotKeys.length) {
      batch.slotKeys.forEach((slotKey) => lines.push(`- ${slotKey}`));
    } else {
      lines.push("- *(none)*");
    }
    lines.push("");
  }
  lines.push("## Score projection");
  lines.push(`- After v23A: coverage **${report.scoreProjection.projectedSlotCoverageAfterV23A}/100**, manifest **${report.scoreProjection.projectedManifestScoreAfterV23A ?? "n/a"}/100**`);
  lines.push(
    `- All evidence-supported: coverage **${report.scoreProjection.projectedSlotCoverageAllEvidenceSupported}/100**, manifest **${report.scoreProjection.projectedManifestScoreAllEvidenceSupported ?? "n/a"}/100**`
  );
  lines.push(
    `- Completed-brand comparable after v23A: **${report.scoreProjection.tributeCompletedBrandComparableAfterV23A ? "yes" : "no"}**`
  );
  lines.push(
    `- Completed-brand comparable after all evidence-safe work: **${report.scoreProjection.tributeCompletedBrandComparableAfterAllEvidenceSafeWork ? "yes" : "no"}**`
  );
  lines.push("");
  lines.push("## Slot detail");
  lines.push("| Slot | Tab | Classification | v23 batch | External safe | FDD internal | Next action |");
  lines.push("|------|-----|----------------|-----------|---------------|--------------|-------------|");
  for (const row of report.slotReadinessRows) {
    lines.push(
      `| ${row.slotKey} | ${row.tab} | ${row.readinessClassification} | ${row.recommendedV23Batch} | ${row.externalDisplaySafe ? "yes" : "no"} | ${row.fddEconomicsInternalOnly ? "yes" : "no"} | ${row.proposedNextAction.replace(/\|/g, "/")} |`
    );
  }
  lines.push("");
  lines.push(`## Exact next command`);
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}
