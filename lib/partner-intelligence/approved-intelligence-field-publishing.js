/**
 * Approved Intelligence → Platform Field Publishing v1 — read-only audit + classification.
 * @see docs/data-intelligence/approved-intelligence-platform-field-publishing-v1.md
 */
import { readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  PARTNER_INTELLIGENCE_TABLES,
  PARTNER_INTELLIGENCE_LINKS,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  getRegistryField,
  BRAND_EXPLORER_FIELDS,
  OPERATOR_EXPLORER_FIELDS,
} from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import { MAP_PROFILE_GOVERNANCE_AIRTABLE } from "../profile-governance/profile-governance-fields.js";
import {
  extractProfileGovernanceRaw,
  normalizeProfileGovernance,
} from "../profile-governance/normalize-profile-governance.js";
import { buildPackageFromRecords } from "./stewardship-package.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..", "..");

export const AUDIT_VERSION = "v1";
export const REPORT_JSON_NAME = "approved-intelligence-field-publishing-audit.json";
export const REPORT_MD_NAME = "approved-intelligence-field-publishing-audit.md";

export function auditReportFileNames(targetRecId) {
  return {
    perEntityJson: `approved-intelligence-field-publishing-audit-${targetRecId}.json`,
    perEntityMd: `approved-intelligence-field-publishing-audit-${targetRecId}.md`,
    latestJson: REPORT_JSON_NAME,
    latestMd: REPORT_MD_NAME,
  };
}

export const PUBLISH_MODES = {
  evidenceOnly: "evidence_only",
  suggestedUpdate: "suggested_field_update",
  controlledPublishCandidate: "controlled_publish_candidate",
  blocked: "blocked",
};

export const APPLY_FLAGS = ["--apply", "--publish-apply", "--approve-stewardship", "--write"];

/** Never write from PI fact publish path */
export const BLOCKED_DESTINATION_FIELDS = new Set([
  MAP_PROFILE_GOVERNANCE_AIRTABLE.companyValidated,
  MAP_PROFILE_GOVERNANCE_AIRTABLE.companyValidationDate,
  MAP_PROFILE_GOVERNANCE_AIRTABLE.validationStatus,
  MAP_PROFILE_GOVERNANCE_AIRTABLE.usagePermission,
  MAP_PROFILE_GOVERNANCE_AIRTABLE.externalDisplayStatus,
  MAP_PROFILE_GOVERNANCE_AIRTABLE.confidenceLevel,
  MAP_PROFILE_GOVERNANCE_AIRTABLE.lastReviewedDate,
  MAP_PROFILE_GOVERNANCE_AIRTABLE.evidenceNotes,
  "Company Reviewed",
  "submission_status",
  "readyForInvestorPublication",
]);

/** Field keys that must not auto-infer scoring / fit conclusions */
export const INFERENCE_RISK_FIELD_PREFIXES = [
  "op.dealFit.",
  "op.meta.",
  "be.fit.",
];

/** v1 default: remain evidence until staging workflow ships */
export const V1_EVIDENCE_ONLY_KEYS = new Set([
  "op.ownerValueProposition",
  "op.operatingModel",
  "op.capabilities.managementServices",
  "op.portfolio.scale",
  "op.events.miceCapability",
]);

/** Per-key publishing policy overrides */
export const FIELD_PUBLISHING_POLICIES = {
  "op.snapshot.companyName": {
    overwritePolicy: "blank_only",
    stagingPreferred: true,
    identityField: true,
    notes: "Suggest only when destination blank; never overwrite curated identity.",
  },
  "op.snapshot.companyDescription": {
    overwritePolicy: "blank_or_staging",
    stagingPreferred: true,
    notes: "Profile summary staging; suggested update when live field populated.",
  },
  "op.markets.regionsSupported": {
    overwritePolicy: "blank_or_staging",
    stagingPreferred: true,
    notes: "Regional presence text; validate against select options if normalized later.",
  },
  "op.brand.familiesOperated": {
    overwritePolicy: "blank_or_staging",
    stagingPreferred: true,
    multipleSelectRisk: true,
    notes: "multipleSelects destination — requires option validation before controlled publish.",
  },
  "op.platform.offeredServices": {
    overwritePolicy: "blank_or_staging",
    stagingPreferred: true,
    multipleSelectRisk: true,
    notes: "multipleSelects — map tokens carefully; prefer suggested review.",
  },
  "op.snapshot.primaryServiceModel": {
    overwritePolicy: "blank_only",
    stagingPreferred: true,
    singleSelectRisk: true,
    notes: "singleSelect — only controlled publish when blank and option exists.",
  },
  "be.identity.brandName": {
    overwritePolicy: "blank_only",
    identityField: true,
    stagingPreferred: true,
  },
  "be.positioning.summary": {
    overwritePolicy: "blank_or_staging",
    stagingPreferred: true,
  },
};

let _operatorDestCache = null;

function loadOperatorDestinationMap() {
  if (_operatorDestCache) return _operatorDestCache;
  const path = join(ROOT, "api/lib/operator-setup-new-base-build-sheet-rows.json");
  const raw = JSON.parse(readFileSync(path, "utf8"));
  const rows = raw.rows || raw;
  const byPrefill = {};
  for (const row of rows) {
    if (!row.form_name || !row.airtable_field_name || !row.table_name) continue;
    byPrefill[row.form_name] = {
      destinationTable: row.table_name,
      destinationField: row.airtable_field_name,
      fieldType: row.airtable_type || "unknown",
      prefillKey: row.form_name,
    };
  }
  _operatorDestCache = byPrefill;
  return byPrefill;
}

/**
 * Resolve product destination for a registry field.
 */
export function resolveDestination(registry, entityType) {
  if (!registry) return null;

  if (entityType === "operator" && registry.prefillKey) {
    const map = loadOperatorDestinationMap();
    const dest = map[registry.prefillKey];
    if (dest) {
      return {
        ...dest,
        consumptionPath: registry.responsePath || `prefill.${registry.prefillKey}`,
        explorerTab: registry.explorerTab,
        explorerSection: registry.explorerSection,
      };
    }
  }

  if (entityType === "brand") {
    if (registry.brandSetupBasicsField) {
      return {
        destinationTable: PARTNER_INTELLIGENCE_LINKS.brandBasics,
        destinationField: registry.brandSetupBasicsField,
        fieldType: "text",
        consumptionPath: registry.responsePath || registry.brandSetupBasicsField,
        presentationSlotKey: registry.slotKey || null,
        explorerTab: registry.explorerTab,
        explorerSection: registry.explorerSection,
      };
    }
    if (registry.slotKey) {
      return {
        destinationTable: "Brand Setup - Brand Explorer Presentation",
        destinationField: "body",
        fieldType: "longText",
        presentationSlotKey: registry.slotKey,
        consumptionPath: `presentation.${registry.slotKey}`,
        explorerTab: registry.explorerTab,
        explorerSection: registry.explorerSection,
        stagingOnly: true,
      };
    }
  }

  return {
    destinationTable: null,
    destinationField: null,
    fieldType: null,
    consumptionPath: registry.responsePath || null,
    explorerTab: registry.explorerTab,
    explorerSection: registry.explorerSection,
    stagingOnly: true,
    notes: "No mapped Setup column — evidence or Published overlay only",
  };
}

export function rejectFieldPublishingApplyFlags(argv = process.argv) {
  for (const flag of APPLY_FLAGS) {
    if (argv.includes(flag)) {
      return {
        rejected: true,
        flag,
        message:
          "[approved-intelligence-field-publishing-audit] Write/apply mode is disabled in v1. Audit reports only.",
      };
    }
  }
  return { rejected: false };
}

function nz(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.map((x) => String(x).trim()).filter(Boolean).join(", ");
  return String(v).trim();
}

function isApprovedFact(fact) {
  const status = nz(fact.humanReviewStatus);
  return status === "Approved" || status === "Edited";
}

function isApprovedSource(source) {
  return source && nz(source.approvedForExplorerUse) === "Yes";
}

function isGovernanceDisplayAllowed(governance) {
  const usage = governance?.usagePermission || governance?.live?.usagePermission;
  const ext = governance?.externalDisplayStatus || governance?.live?.externalDisplayStatus;
  if (usage === "Do Not Use" || usage === "Internal Only") return false;
  if (ext === "Do Not Display" || ext === "Internal Only") return false;
  return true;
}

function isDestinationPopulated(liveValue) {
  const v = nz(liveValue);
  return v.length > 0;
}

function hasInferenceRisk(fieldKey) {
  return INFERENCE_RISK_FIELD_PREFIXES.some((p) => fieldKey.startsWith(p));
}

/**
 * Classify one fact → destination mapping.
 */
export function classifyFactMapping({
  fact,
  source,
  entityType,
  registry,
  destination,
  liveValue,
  governance = {},
}) {
  const fieldKey = fact?.fieldName || fact?.fieldKey;
  const blockers = [];
  const policy = FIELD_PUBLISHING_POLICIES[fieldKey] || {};

  if (!fieldKey) {
    return modeResult(PUBLISH_MODES.blocked, ["missing_field_key"]);
  }

  if (!isApprovedFact(fact)) {
    return modeResult(PUBLISH_MODES.blocked, [`fact_status_${nz(fact?.humanReviewStatus) || "unknown"}`]);
  }

  if (!source || !isApprovedSource(source)) {
    return modeResult(PUBLISH_MODES.blocked, ["source_not_explorer_approved"]);
  }

  if (hasInferenceRisk(fieldKey)) {
    return modeResult(PUBLISH_MODES.blocked, ["deal_fit_or_scoring_inference_risk"]);
  }

  if (V1_EVIDENCE_ONLY_KEYS.has(fieldKey)) {
    return modeResult(PUBLISH_MODES.evidenceOnly, ["v1_evidence_only_registry"]);
  }

  if (!registry || registry.publishScope === false) {
    return modeResult(PUBLISH_MODES.blocked, ["unsupported_or_out_of_publish_scope"]);
  }

  if (destination?.destinationField && BLOCKED_DESTINATION_FIELDS.has(destination.destinationField)) {
    return modeResult(PUBLISH_MODES.blocked, ["destination_governance_field"]);
  }

  if (!destination?.destinationTable || !destination?.destinationField) {
    return modeResult(PUBLISH_MODES.evidenceOnly, ["no_product_destination_mapped"]);
  }

  if (!isGovernanceDisplayAllowed(governance)) {
    return modeResult(PUBLISH_MODES.blocked, ["governance_display_not_allowed"]);
  }

  const populated = isDestinationPopulated(liveValue);
  const overwritePolicy = policy.overwritePolicy || "blank_or_staging";

  if (policy.identityField && populated) {
    return modeResult(PUBLISH_MODES.suggestedUpdate, ["identity_field_populated_no_overwrite"], {
      proposedValue: nz(fact.approvedValue) || nz(fact.extractedValue),
    });
  }

  if (populated && overwritePolicy !== "allow") {
    return modeResult(PUBLISH_MODES.suggestedUpdate, ["destination_field_populated"], {
      proposedValue: nz(fact.approvedValue) || nz(fact.extractedValue),
      liveValue: nz(liveValue),
    });
  }

  if (policy.multipleSelectRisk || policy.singleSelectRisk) {
    return modeResult(PUBLISH_MODES.suggestedUpdate, ["select_option_validation_required"], {
      proposedValue: nz(fact.approvedValue) || nz(fact.extractedValue),
    });
  }

  if (policy.stagingPreferred || destination.stagingOnly) {
    return modeResult(PUBLISH_MODES.controlledPublishCandidate, ["staging_preferred_blank_destination"], {
      proposedValue: nz(fact.approvedValue) || nz(fact.extractedValue),
    });
  }

  if (!populated) {
    return modeResult(PUBLISH_MODES.controlledPublishCandidate, ["blank_destination_eligible"], {
      proposedValue: nz(fact.approvedValue) || nz(fact.extractedValue),
    });
  }

  return modeResult(PUBLISH_MODES.suggestedUpdate, ["default_human_review"], {
    proposedValue: nz(fact.approvedValue) || nz(fact.extractedValue),
    liveValue: nz(liveValue),
  });
}

function modeResult(mode, blockers = [], extra = {}) {
  return { mode, blockers, ...extra };
}

/**
 * Read live destination value from operator bundle child row.
 */
export function readOperatorLiveValue(bundle, destination) {
  if (!bundle || !destination?.destinationField) return "";
  const table = destination.destinationTable;
  const field = destination.destinationField;
  let row = null;
  if (table.includes("Profile & Positioning")) row = bundle.profile;
  else if (table.includes("Platform & Markets")) row = bundle.platform;
  else if (table.includes("Governance")) row = bundle.governance;
  else if (table.includes("Commercial")) row = bundle.commercial;
  else if (table.includes("Brand & Relationships")) row = bundle.brandRelationships?.[0];
  else if (table.includes("Master")) row = bundle.master;
  if (!row?.fields) return "";
  return row.fields[field];
}

/**
 * Read live brand destination from Brand Basics (+ optional presentation).
 */
export function readBrandLiveValue(brandRecord, destination, presentationRows = []) {
  if (!destination) return "";
  if (destination.destinationTable === PARTNER_INTELLIGENCE_LINKS.brandBasics && brandRecord?.fields) {
    return brandRecord.fields[destination.destinationField];
  }
  if (destination.presentationSlotKey && presentationRows.length) {
    const slot = presentationRows.find(
      (r) => nz(r.fields?.slotKey || r.fields?.["Slot Key"]) === destination.presentationSlotKey
    );
    if (slot?.fields) return slot.fields.body || slot.fields.Body;
  }
  return "";
}

/**
 * Build audit for one entity.
 */
export function buildFieldPublishingAudit({
  entityType,
  targetRecId,
  entityName,
  facts,
  sources,
  targetProfile,
  operatorBundle = null,
  brandPresentationRows = [],
  governanceRaw = null,
}) {
  const explorerType = entityType === "brand" ? "Brand Explorer" : "Operator Explorer";
  const sourceById = new Map((sources || []).map((s) => [s.id, s]));
  const governance = governanceRaw
    ? {
        live: governanceRaw,
        normalized: normalizeProfileGovernance(governanceRaw, {
          entityType,
          sourceTable:
            entityType === "brand"
              ? PARTNER_INTELLIGENCE_LINKS.brandBasics
              : PARTNER_INTELLIGENCE_LINKS.operatorMaster,
        }),
      }
    : {};

  const approvedFacts = (facts || []).filter(isApprovedFact);
  const mappings = [];

  for (const fact of approvedFacts) {
    const fieldKey = fact.fieldName;
    const registry = getRegistryField(fieldKey, explorerType);
    const destination = resolveDestination(registry, entityType);
    const source = fact.sourceRecordId ? sourceById.get(fact.sourceRecordId) : null;
    const liveValue =
      entityType === "operator"
        ? readOperatorLiveValue(operatorBundle, destination)
        : readBrandLiveValue(targetProfile, destination, brandPresentationRows);

    const classification = classifyFactMapping({
      fact,
      source,
      entityType,
      registry,
      destination,
      liveValue,
      governance,
    });

    mappings.push({
      factId: fact.id,
      fieldKey,
      displayLabel: registry?.displayLabel || fieldKey,
      approvedValue: nz(fact.approvedValue) || nz(fact.extractedValue),
      humanReviewStatus: fact.humanReviewStatus,
      sourceId: fact.sourceRecordId,
      sourceApproved: isApprovedSource(source),
      destinationTable: destination?.destinationTable || null,
      destinationField: destination?.destinationField || null,
      destinationFieldType: destination?.fieldType || null,
      consumptionPath: destination?.consumptionPath || registry?.responsePath || null,
      presentationSlotKey: destination?.presentationSlotKey || registry?.slotKey || null,
      liveValue: nz(liveValue) || null,
      liveValuePopulated: isDestinationPopulated(liveValue),
      publishMode: classification.mode,
      blockers: classification.blockers,
      proposedValue: classification.proposedValue || null,
      policy: FIELD_PUBLISHING_POLICIES[fieldKey] || null,
      registrySupported: Boolean(registry),
      notes: destination?.notes || null,
    });
  }

  const pendingFacts = (facts || []).filter((f) => !isApprovedFact(f));

  const summary = {
    totalFacts: facts?.length || 0,
    approvedFacts: approvedFacts.length,
    pendingOrRejectedFacts: pendingFacts.length,
    evidenceOnly: mappings.filter((m) => m.publishMode === PUBLISH_MODES.evidenceOnly).length,
    suggestedUpdate: mappings.filter((m) => m.publishMode === PUBLISH_MODES.suggestedUpdate).length,
    controlledPublishCandidate: mappings.filter(
      (m) => m.publishMode === PUBLISH_MODES.controlledPublishCandidate
    ).length,
    blocked: mappings.filter((m) => m.publishMode === PUBLISH_MODES.blocked).length,
  };

  return {
    auditVersion: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "audit",
    entityType,
    targetRecId,
    entityName,
    governance: {
      validationStatus: governanceRaw?.validationStatus || null,
      externalDisplayStatus: governanceRaw?.externalDisplayStatus || null,
      companyValidated: Boolean(governanceRaw?.companyValidated),
      displayAllowed: isGovernanceDisplayAllowed({ live: governanceRaw }),
    },
    stagingTable: PARTNER_INTELLIGENCE_TABLES.publishedFields,
    summary,
    mappings,
    excludedFacts: pendingFacts.map((f) => ({
      factId: f.id,
      fieldKey: f.fieldName,
      humanReviewStatus: f.humanReviewStatus,
      reason: "not_approved",
    })),
    safety: {
      readOnly: true,
      applyEnabled: false,
      neverWrites: [
        ...BLOCKED_DESTINATION_FIELDS,
        "Company Validated",
        "Company Validation Date",
        "scoring fields (BAS/OAS/OCS)",
        "Deal Readiness outputs",
      ],
    },
  };
}

export function buildFieldPublishingAuditMarkdown(audit) {
  const lines = [
    "# Approved Intelligence → Platform Field Publishing Audit",
    "",
    `Generated: ${audit.generatedAt}`,
    `Audit: **${audit.auditVersion}** (read-only)`,
    `Entity: ${audit.entityType} — **${audit.entityName}** (\`${audit.targetRecId}\`)`,
    "",
    "## Governance snapshot",
    "",
    `- Validation Status: ${audit.governance.validationStatus || "—"}`,
    `- External Display: ${audit.governance.externalDisplayStatus || "—"}`,
    `- Company Validated: ${audit.governance.companyValidated ? "yes" : "no"}`,
    `- Display allowed for field publish: **${audit.governance.displayAllowed ? "yes" : "no"}**`,
    "",
    "## Summary",
    "",
    `| Metric | Count |`,
    `|--------|------:|`,
    `| Approved facts audited | ${audit.summary.approvedFacts} |`,
    `| Evidence only | ${audit.summary.evidenceOnly} |`,
    `| Suggested field update | ${audit.summary.suggestedUpdate} |`,
    `| Controlled publish candidate | ${audit.summary.controlledPublishCandidate} |`,
    `| Blocked | ${audit.summary.blocked} |`,
    `| Pending/rejected (excluded) | ${audit.summary.pendingOrRejectedFacts} |`,
    "",
    "## Mappings",
    "",
    "| Field key | Approved value | Destination | Live value | Mode | Blockers |",
    "|-----------|----------------|-------------|------------|------|----------|",
  ];

  for (const m of audit.mappings) {
    const dest = m.destinationTable
      ? `${m.destinationTable} → \`${m.destinationField}\``
      : "—";
    const blockers = (m.blockers || []).join("; ") || "—";
    const val = (m.approvedValue || "").slice(0, 60);
    const live = (m.liveValue || "—").slice(0, 40);
    lines.push(
      `| \`${m.fieldKey}\` | ${val} | ${dest} | ${live} | **${m.publishMode}** | ${blockers} |`
    );
  }

  lines.push("", "## By publish mode", "");

  for (const mode of Object.values(PUBLISH_MODES)) {
    const group = audit.mappings.filter((m) => m.publishMode === mode);
    if (!group.length) continue;
    lines.push(`### ${mode}`, "");
    for (const m of group) {
      lines.push(`- **${m.fieldKey}** (${m.displayLabel})`);
      if (m.proposedValue) lines.push(`  - Proposed: ${m.proposedValue.slice(0, 120)}`);
      if (m.blockers?.length) lines.push(`  - Blockers: ${m.blockers.join(", ")}`);
    }
    lines.push("");
  }

  if (audit.excludedFacts?.length) {
    lines.push("## Excluded facts (not approved)", "");
    for (const f of audit.excludedFacts) {
      lines.push(`- \`${f.fieldKey}\` — ${f.humanReviewStatus}`);
    }
    lines.push("");
  }

  lines.push("## Safety", "");
  lines.push("- Read-only audit — no Airtable writes in v1");
  lines.push("- No apply mode — controlled publish is design-only");
  for (const item of audit.safety.neverWrites) {
    lines.push(`- Never writes: ${item}`);
  }
  lines.push("");

  return lines.join("\n");
}

/** List all registry keys for destination audit reference */
export function listRegistryFieldKeys(entityType) {
  const pool = entityType === "brand" ? BRAND_EXPLORER_FIELDS : OPERATOR_EXPLORER_FIELDS;
  return pool.map((f) => f.fieldKey);
}

export { buildPackageFromRecords, getRegistryField };
