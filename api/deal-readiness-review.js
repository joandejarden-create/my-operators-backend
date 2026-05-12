/**
 * Deal Readiness Review — deterministic scoring from merged deal fields (no LLM required).
 * Uses the same required field list as Deal Setup (REQUIRED_DEAL_SETUP_FIELDS), except the
 * Lease Structure block is omitted when Preferred Deal Structure is not Lease / Flexible/Open.
 * POST /api/ai/deal-readiness-review loads the deal via fetchDealWithMergedLinkedRecords.
 */

import { fetchDealWithMergedLinkedRecords, REQUIRED_DEAL_SETUP_FIELDS, isFieldFilled } from "./my-deals.js";
import {
  DEALS_TABLE,
  DEAL_READINESS_SCORE_AIRTABLE_FIELD,
  DEAL_READINESS_STAGE_AIRTABLE_FIELD,
  DEAL_READINESS_SUMMARY_AIRTABLE_FIELD,
  DEAL_READINESS_LAST_REVIEWED_AIRTABLE_FIELD,
  LEASE_STRUCTURE_FORM_FIELDS,
  isLeaseStructureDealApplicableFromMergedFields,
} from "./schemas/deal-setup-fields.js";
import { READINESS_TAB_ORDER, readinessTabForField } from "./deal-readiness-field-tabs.js";

const LEASE_STRUCTURE_FIELD_SET = new Set(LEASE_STRUCTURE_FORM_FIELDS);

/** Same as REQUIRED_DEAL_SETUP_FIELDS except lease-structure keys when the lease tab is not applicable. */
function requiredFieldNamesForReadiness(fields) {
  if (isLeaseStructureDealApplicableFromMergedFields(fields)) return REQUIRED_DEAL_SETUP_FIELDS;
  return REQUIRED_DEAL_SETUP_FIELDS.filter((f) => !LEASE_STRUCTURE_FIELD_SET.has(f));
}
const READINESS_ALTERNATE_KEYS = {
  "Are you open to lesser-known or emerging brands with favorable terms?": [
    "Are you open to considering other brands with favorable terms?",
  ],
  "Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site?": [
    "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?",
  ],
  /** Market–Performance column label in some bases; merged GET uses form key after MP_TABLE_TO_FORM. */
  "Regulatory or Permitting Issues Description": ["Regulatory or Permitting Issues Text"],
};

function getFieldValueForReadiness(fields, canonicalKey) {
  const extras = READINESS_ALTERNATE_KEYS[canonicalKey] || [];
  for (const k of [canonicalKey, ...extras]) {
    if (fields[k] !== undefined && fields[k] !== null) return fields[k];
  }
  return undefined;
}

function isFilledForReadiness(fields, canonicalKey) {
  const v = getFieldValueForReadiness(fields, canonicalKey);
  return isFieldFilled(v);
}

function primaryDemandDriversSelected(fields) {
  const raw = getFieldValueForReadiness(fields, "Primary Demand Drivers");
  if (Array.isArray(raw)) {
    return raw.map((x) => (typeof x === "string" ? x : (x && x.name) || "").trim()).filter(Boolean);
  }
  if (typeof raw === "string" && raw.trim() !== "") {
    return raw
      .split(/\s*,\s*/)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [];
}

/**
 * Required fields for readiness (see requiredFieldNamesForReadiness), but respects Deal Setup conditionals:
 * e.g. regulatory description is cleared in the UI when issues = "No"; do not count as missing then.
 */
function isReadinessRequirementMet(fields, fname) {
  if (fname === "Regulatory or Permitting Issues Description") {
    const issue = String(getFieldValueForReadiness(fields, "Regulatory or Permitting Issues?") ?? "")
      .trim()
      .toLowerCase();
    if (issue === "no") return true;
    return isFilledForReadiness(fields, fname);
  }
  if (fname === "Primary Demand Drivers Other") {
    const drivers = primaryDemandDriversSelected(fields);
    if (!drivers.some((d) => d === "Other")) return true;
    return isFilledForReadiness(fields, fname);
  }
  return isFilledForReadiness(fields, fname);
}

/**
 * True only for obvious non-answers in free-text strings.
 * Do not use short length: selects save values like "No" or "Yes"; numbers may serialize as short strings.
 * Arrays (multi-select) and numbers are handled in buildReadinessFromFields and never pass here.
 */
function isWeakText(val) {
  if (val == null) return true;
  if (typeof val !== "string") return false;
  const s = val.trim();
  if (s === "") return true;
  const low = s.toLowerCase();
  if (["tbd", "n/a", "na", "none", "unknown", "—", "-", "pending", "todo", "lorem ipsum"].includes(low)) return true;
  return false;
}

function rowForMissing(field) {
  const tab = readinessTabForField(field);
  return {
    field,
    highlightField: field,
    label: field,
    section: tab,
    relatedTab: tab,
  };
}

function rowForWeak(field) {
  const tab = readinessTabForField(field);
  return {
    field,
    highlightField: field,
    label: field + " (placeholder-style text)",
    section: tab,
    relatedTab: tab,
  };
}

function deriveStage(score, blockingCount) {
  if (blockingCount > 0 || score < 60) return "Discovery";
  if (score < 75) return "Shaping";
  if (score < 90) return "Ready for External Review";
  return "Ready";
}

function buildTabScores(fields) {
  const byTab = {};
  for (const tab of READINESS_TAB_ORDER) {
    byTab[tab] = { filled: 0, total: 0 };
  }
  const reqNames = requiredFieldNamesForReadiness(fields);
  for (const fname of reqNames) {
    const tab = readinessTabForField(fname);
    if (!byTab[tab]) byTab[tab] = { filled: 0, total: 0 };
    byTab[tab].total += 1;
    if (isReadinessRequirementMet(fields, fname)) byTab[tab].filled += 1;
  }
  const sectionScores = {};
  const sectionScoresLabeled = [];
  for (const tab of READINESS_TAB_ORDER) {
    const cell = byTab[tab];
    let pct = null;
    if (cell && cell.total > 0) {
      pct = Math.round((100 * cell.filled) / cell.total);
    }
    sectionScores[tab] = pct;
    sectionScoresLabeled.push({ id: tab, label: tab, score: pct });
  }
  return { sectionScores, sectionScoresLabeled };
}

function buildReadinessFromFields(fields) {
  const reqNames = requiredFieldNamesForReadiness(fields);
  const missingReq = reqNames.filter((f) => !isReadinessRequirementMet(fields, f));
  const weakFields = reqNames.filter((f) => {
    if (!isReadinessRequirementMet(fields, f)) return false;
    const v = getFieldValueForReadiness(fields, f);
    if (typeof v === "number" && Number.isFinite(v)) return false;
    if (Array.isArray(v)) return false;
    if (typeof v === "object" && v !== null) return false;
    if (typeof v !== "string") return false;
    return isWeakText(v);
  });

  const nReq = reqNames.length;
  const baseScore = Math.max(
    0,
    Math.min(
      100,
      Math.round(100 - (missingReq.length / Math.max(1, nReq)) * 72 - weakFields.length * 2)
    )
  );

  const missingInformation = missingReq.map(rowForMissing);
  const weakInformation = weakFields.map(rowForWeak);
  const blockingIssues = [];

  const { sectionScores, sectionScoresLabeled } = buildTabScores(fields);
  const readinessStage = deriveStage(baseScore, blockingIssues.length);

  const priorityActions = missingReq.slice(0, 8).map((f) => ({
    label: `Complete “${f}”`,
    reason: "Required on Deal Setup for a complete intake and reliable outreach packaging.",
    relatedField: f,
    relatedTab: readinessTabForField(f),
    severity: "high",
  }));

  const humanReadableSummary =
    `Readiness is based on your saved Deal Setup fields (${nReq} required fields; no AI narrative in this environment). ` +
    `${missingReq.length} required field(s) missing` +
    (weakFields.length
      ? `, ${weakFields.length} text field(s) use placeholder-style answers (each subtracts 2 from the headline score).`
      : ".") +
    ` Dropdown and multi-select answers are not scored as “weak.” ` +
    `Tab percentages count required fields filled vs empty; the headline score adds the small placeholder penalty when applicable. ` +
    `Re-run this review after saving Deal Setup (the modal saves the new score to your deal record when the run finishes).`;

  return {
    success: true,
    dealReadinessScore: baseScore,
    readinessStage,
    missingInformation,
    weakInformation,
    blockingIssues,
    sectionScores,
    sectionScoresLabeled,
    tabScores: sectionScores,
    tabScoresLabeled: sectionScoresLabeled,
    humanReadableSummary,
    workflowRecommendation: {
      label: "Unified review",
      explanation: "Complete missing and weak fields on Deal Setup before external outreach.",
      allowedNextActions: ["Edit deal and highlight gaps", "Save, then re-run readiness"],
    },
    scoreImprovementPlan: {
      targetScoreLabel: "Target: 90+ with zero blocking package issues",
      priorityActions,
    },
    ai: null,
  };
}

export function getDealReadinessMeta(req, res) {
  res.json({
    success: true,
    dealFields: {
      score: DEAL_READINESS_SCORE_AIRTABLE_FIELD,
      stage: DEAL_READINESS_STAGE_AIRTABLE_FIELD,
    },
  });
}

export async function postDealReadinessReview(req, res) {
  try {
    const dealId = req.body && typeof req.body.dealId === "string" ? req.body.dealId.trim() : "";
    if (!dealId || !dealId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid dealId (Airtable record id) is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, dealId);
    if (!full) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }

    const mergedFields = full.deal.fields || {};
    const payload = buildReadinessFromFields(mergedFields);
    res.json({
      ...payload,
      normalized: full.normalized,
      deal: { id: full.deal.id, fields: mergedFields },
      sourceFields: mergedFields,
    });
  } catch (err) {
    console.error("postDealReadinessReview:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

export async function postDealReadinessSave(req, res) {
  try {
    const dealId = req.body && typeof req.body.dealId === "string" ? req.body.dealId.trim() : "";
    const review = req.body && req.body.review && typeof req.body.review === "object" ? req.body.review : null;
    if (!dealId || !dealId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid dealId is required" });
    }
    if (!review) {
      return res.status(400).json({ success: false, error: "review payload is required" });
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const scNum = Number(review.dealReadinessScore);
    const savedAt = new Date().toISOString();
    const airtableFields = {
      [DEAL_READINESS_SCORE_AIRTABLE_FIELD]: Number.isFinite(scNum) ? scNum : 0,
      [DEAL_READINESS_STAGE_AIRTABLE_FIELD]: String(review.readinessStage != null ? review.readinessStage : "").trim(),
    };
    if (DEAL_READINESS_LAST_REVIEWED_AIRTABLE_FIELD) {
      airtableFields[DEAL_READINESS_LAST_REVIEWED_AIRTABLE_FIELD] = savedAt;
    }
    if (DEAL_READINESS_SUMMARY_AIRTABLE_FIELD && review.humanReadableSummary != null) {
      const text = String(review.humanReadableSummary).trim();
      if (text) airtableFields[DEAL_READINESS_SUMMARY_AIRTABLE_FIELD] = text.slice(0, 8000);
    }
    const missCol = process.env.DEAL_READINESS_MISSING_COUNT_FIELD || "";
    if (missCol && Array.isArray(review.missingInformation)) {
      airtableFields[missCol] = review.missingInformation.length;
    }
    const blockCol = process.env.DEAL_READINESS_BLOCKING_COUNT_FIELD || "";
    if (blockCol && Array.isArray(review.blockingIssues)) {
      airtableFields[blockCol] = review.blockingIssues.length;
    }

    const tableEnc = encodeURIComponent(DEALS_TABLE);
    const patchRes = await fetch(`https://api.airtable.com/v0/${baseId}/${tableEnc}/${encodeURIComponent(dealId)}`, {
      method: "PATCH",
      headers: {
        Authorization: "Bearer " + apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: airtableFields }),
    });
    const body = await patchRes.json().catch(() => ({}));
    if (!patchRes.ok || body.error) {
      const msg = body.error?.message || body.error || `Airtable error (${patchRes.status})`;
      return res.status(patchRes.status >= 400 ? patchRes.status : 502).json({
        success: false,
        error: msg,
        hint:
          "Ensure your Deals table has columns for score and stage (defaults: Deal Readiness Score, Deal Readiness Stage), or set DEAL_READINESS_SCORE_FIELD / DEAL_READINESS_STAGE_FIELD in .env. Optional: Deal Readiness Last Reviewed, or set DEAL_READINESS_LAST_REVIEWED_FIELD=0 to skip that column on save.",
      });
    }

    res.json({
      success: true,
      savedAt,
      dealReadinessScore: airtableFields[DEAL_READINESS_SCORE_AIRTABLE_FIELD],
      readinessStage: airtableFields[DEAL_READINESS_STAGE_AIRTABLE_FIELD],
      dealReadinessMissingCount: Array.isArray(review.missingInformation) ? review.missingInformation.length : null,
      dealReadinessBlockingCount: Array.isArray(review.blockingIssues) ? review.blockingIssues.length : null,
    });
  } catch (err) {
    console.error("postDealReadinessSave:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}
