/**
 * Publish rules + Operator Explorer prefill overlay.
 */
import {
  MAP_PARTNER_FACT,
  MAP_PARTNER_PUBLISHED,
  PARTNER_INTELLIGENCE_FLAGS,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  PARTNER_INTELLIGENCE_REGISTRY_VERSION,
  getRegistryField,
} from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import {
  listPublishedFieldsForOperator,
  findPublishedRowByFieldName,
  upsertPublishedField,
} from "./airtable-facts.js";
import { getPartnerSourceById, fetchLinkedPrimaryName } from "./airtable-source.js";
import { getPartnerFactById } from "./airtable-facts.js";
import { getPartnerIntelligenceConfig } from "./airtable-source.js";
import { PARTNER_INTELLIGENCE_LINKS } from "../../api/lib/partner-intelligence-field-map.js";

const QUALITY_RANK = { Low: 1, Medium: 2, High: 3 };

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

export function isPublishOverlayEnabled() {
  return PARTNER_INTELLIGENCE_FLAGS.publishOverlay || PARTNER_INTELLIGENCE_FLAGS.publishEnabled;
}

/**
 * @param {object} fact — normalized fact
 * @param {object} source — normalized source
 */
export function validatePublishEligibility(fact, source) {
  const failures = [];
  const status = nz(fact.humanReviewStatus);
  if (status !== "Approved" && status !== "Edited") {
    failures.push("humanReviewStatus must be Approved or Edited.");
  }
  const approvedValue = nz(fact.approvedValue) || nz(fact.extractedValue);
  if (!approvedValue) failures.push("approvedValue is required.");
  if (fact.dataGap === "Yes" && approvedValue === "Not confirmed in available sources.") {
    /* allowed — intentional gap publish */
  }
  const srcQuality = nz(source?.sourceQuality || fact.sourceQuality);
  if ((QUALITY_RANK[srcQuality] || 0) < QUALITY_RANK.Medium) {
    failures.push("source quality must be Medium or High.");
  }
  if (nz(source?.status) === "Stale") failures.push("source is Stale.");
  if (nz(source?.approvedForExplorerUse) === "No") {
    failures.push("source Approved for Explorer Use must be Yes.");
  }
  const visibility = nz(fact.publicVisibility) || "Public";
  if (visibility === "Restricted") failures.push("restricted facts cannot publish to public Explorer overlay.");
  return { ok: failures.length === 0, failures, approvedValue, visibility, srcQuality };
}

/**
 * Merge published Operator Explorer values into operator detail prefill.
 * @param {object} prefill
 * @param {string} operatorId
 */
export async function applyPartnerIntelligenceOperatorOverlay(prefill, operatorId) {
  if (!isPublishOverlayEnabled() || !operatorId || !prefill) {
    return { applied: 0, fields: [] };
  }

  const published = await listPublishedFieldsForOperator(operatorId);
  const applied = [];

  for (const row of published) {
    const reg = getRegistryField(row.fieldName, "Operator Explorer");
    if (!reg) continue;
    if (row.publicVisibility === "Internal Only") continue;

    if (reg.prefillKey) {
      let val = row.approvedValue;
      if (reg.valueType === "json" && typeof val === "string") {
        try {
          val = JSON.parse(val);
        } catch (_) {
          /* keep string; reviewer may fix on publish */
        }
      }
      prefill[reg.prefillKey] = val;
    } else if (reg.responsePath && reg.responsePath.startsWith("prefill.")) {
      prefill[reg.responsePath.slice("prefill.".length)] = row.approvedValue;
    } else {
      continue;
    }
    applied.push({
      fieldKey: row.fieldName,
      prefillKey: reg.prefillKey,
      approvedValue: row.approvedValue,
      publishRecordId: row.id,
    });
  }

  if (applied.length) {
    prefill.partnerIntelligenceOverlay = {
      version: PARTNER_INTELLIGENCE_REGISTRY_VERSION,
      appliedCount: applied.length,
      fields: applied.map((a) => a.fieldKey),
    };
  }

  return { applied: applied.length, fields: applied };
}

/**
 * Publish one approved fact → Published Explorer Fields row.
 */
export async function publishApprovedFact(factId) {
  if (!PARTNER_INTELLIGENCE_FLAGS.publishEnabled && !PARTNER_INTELLIGENCE_FLAGS.publishOverlay) {
    throw new Error("Publish is disabled (set PARTNER_INTELLIGENCE_PUBLISH_ENABLED=1).");
  }

  const fact = await getPartnerFactById(factId);
  if (!fact) throw new Error("Fact not found.");

  const source = fact.sourceRecordId ? await getPartnerSourceById(fact.sourceRecordId) : null;
  const eligibility = validatePublishEligibility(fact, source);
  if (!eligibility.ok) {
    const err = new Error("Publish validation failed: " + eligibility.failures.join(" "));
    err.validationFailures = eligibility.failures;
    throw err;
  }

  const { baseId, apiKey } = getPartnerIntelligenceConfig();
  const operatorId = fact.operatorId;
  if (!operatorId) throw new Error("Fact has no operator link.");

  const operatorLinkName = await fetchLinkedPrimaryName(
    baseId,
    apiKey,
    PARTNER_INTELLIGENCE_LINKS.operatorMaster,
    operatorId,
    ["company_name", "Company Name"]
  );
  if (!operatorLinkName) throw new Error("Could not resolve operator link name.");

  const reg = getRegistryField(fact.fieldName, "Operator Explorer");
  const existing = await findPublishedRowByFieldName(operatorLinkName, fact.fieldName);

  const today = new Date().toISOString().slice(0, 10);
  const fields = {
    "Source Title": `${reg?.displayLabel || fact.fieldName} — published`,
    [MAP_PARTNER_PUBLISHED.profileType]: fact.profileType || "Operator",
    [MAP_PARTNER_PUBLISHED.operator]: [operatorId],
    [MAP_PARTNER_PUBLISHED.supportingFacts]: [factId],
    [MAP_PARTNER_PUBLISHED.primarySource]: source ? [source.id] : [],
    [MAP_PARTNER_PUBLISHED.explorerType]: fact.explorerType || "Operator Explorer",
    [MAP_PARTNER_PUBLISHED.explorerSection]: fact.explorerSection,
    [MAP_PARTNER_PUBLISHED.fieldName]: fact.fieldName,
    [MAP_PARTNER_PUBLISHED.approvedValue]: eligibility.approvedValue,
    [MAP_PARTNER_PUBLISHED.displayLabel]: reg?.displayLabel || fact.explorerSection,
    [MAP_PARTNER_PUBLISHED.publicVisibility]: eligibility.visibility,
    [MAP_PARTNER_PUBLISHED.overallSourceConfidence]: eligibility.srcQuality || fact.confidenceLevel,
    [MAP_PARTNER_PUBLISHED.lastReviewedDate]: today,
    [MAP_PARTNER_PUBLISHED.publishStatus]: "Published",
    [MAP_PARTNER_PUBLISHED.publishedAt]: today,
    [MAP_PARTNER_PUBLISHED.stale]: false,
    [MAP_PARTNER_PUBLISHED.dataGap]: fact.dataGap === "Yes",
    [MAP_PARTNER_PUBLISHED.registryVersion]: PARTNER_INTELLIGENCE_REGISTRY_VERSION,
  };

  const published = await upsertPublishedField(fields, existing?.id);
  return { fact, source, published, validation: eligibility };
}

export { MAP_PARTNER_FACT, MAP_PARTNER_PUBLISHED };
