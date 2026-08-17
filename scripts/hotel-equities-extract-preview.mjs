#!/usr/bin/env node
/**
 * Read-only Hotel Equities extraction preview — no Airtable writes.
 * @see docs/data-intelligence/hotel-equities-extraction-plan.md
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { getPartnerSourceById } from "../lib/partner-intelligence/airtable-source.js";
import { loadSourceDocumentText } from "../lib/partner-intelligence/extract-source-text.js";
import { classifySourceDocument } from "../lib/partner-intelligence/classify-source-document.js";
import { enrichDocumentTextForExtraction } from "../lib/partner-intelligence/parse-deck-pdf-text.js";
import { listOperatorFieldsForExtraction } from "../api/lib/partner-intelligence-explorer-field-registry.js";
import { extractOperatorFactsWithLlm } from "../lib/partner-intelligence/llm-extract-operator-facts.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

export const HE_OPERATOR_ID = "recWPKu5laVZxsvpn";
export const ALLOWLISTED_SOURCE_IDS = [
  "rectG9wdsAeL7u0FG",
  "rec9FSzLhaLPcPvtv",
  "recy1oDTNe7kyQGbE",
];

export const TARGET_FACT_KEYS = [
  "op.snapshot.companyName",
  "op.snapshot.companyDescription",
  "op.snapshot.parentCompany",
  "op.platform.offeredServices",
  "op.capabilities.managementServices",
  "op.geography.regions",
  "op.brandRelationships",
  "op.ownerValueProposition",
  "op.operatingModel",
];

function fieldsForSourceRole(registryFields, sourceRole) {
  return registryFields.filter((f) => {
    const roles = f.sourceRoles || ["any"];
    return roles.includes(sourceRole) || roles.includes("any");
  });
}

async function previewSource(sourceId, registryFields, targetKeySet) {
  const source = await getPartnerSourceById(sourceId);
  if (!source) return { sourceId, error: "source_not_found" };
  if (source.operatorId !== HE_OPERATOR_ID) {
    return { sourceId, error: "operator_mismatch", operatorId: source.operatorId };
  }

  const classification = classifySourceDocument(source);
  const doc = await loadSourceDocumentText(source);
  const enriched = enrichDocumentTextForExtraction(doc.text, classification, source);
  const applicableFields = fieldsForSourceRole(registryFields, classification.role).filter((f) =>
    targetKeySet.has(f.fieldKey)
  );

  const sourceMeta = {
    sourceTitle: source.sourceTitle,
    sourceUrl: source.sourceUrl,
    sourceType: source.sourceType,
    localFilePath: source.localFilePath,
    metaDescription: doc.metaDescription,
    sourceRole: classification.role,
    pageSectionAnchor: source.localFilePath || source.sourceUrl || source.sourceTitle,
  };

  const { usedLlm, extractor, facts } = await extractOperatorFactsWithLlm(
    enriched.text,
    applicableFields,
    sourceMeta
  );

  return {
    sourceId,
    sourceTitle: source.sourceTitle,
    sourceUrl: source.sourceUrl,
    localFilePath: source.localFilePath,
    status: source.status,
    approvedForExtraction: source.approvedForExtraction,
    approvedForExplorerUse: source.approvedForExplorerUse,
    classification,
    documentKind: doc.kind,
    textLength: enriched.text.length,
    usedLlm,
    extractor,
    targetFieldCount: applicableFields.length,
    facts: facts.map((f) => ({
      fieldKey: f.fieldKey,
      extractedValue: String(f.extractedValue || "").slice(0, 500),
      evidenceText: String(f.evidenceText || "").slice(0, 300),
      extractionType: f.extractionType,
      confidenceLevel: f.confidenceLevel,
      dataGap: f.dataGap,
    })),
  };
}

export async function buildHotelEquitiesExtractionPreview() {
  const registryFields = listOperatorFieldsForExtraction();
  const targetKeySet = new Set(TARGET_FACT_KEYS);
  const missingKeys = TARGET_FACT_KEYS.filter(
    (k) => !registryFields.some((f) => f.fieldKey === k)
  );

  const sourcePreviews = [];
  for (const id of ALLOWLISTED_SOURCE_IDS) {
    sourcePreviews.push(await previewSource(id, registryFields, targetKeySet));
  }

  const allFacts = sourcePreviews.flatMap((s) => s.facts || []);
  const byKey = {};
  for (const key of TARGET_FACT_KEYS) {
    byKey[key] = allFacts.filter((f) => f.fieldKey === key);
  }

  return {
    generatedAt: new Date().toISOString(),
    mode: "read-only-preview",
    operatorId: HE_OPERATOR_ID,
    allowlistedSourceIds: ALLOWLISTED_SOURCE_IDS,
    targetFactKeys: TARGET_FACT_KEYS,
    registryMissingKeys: missingKeys,
    llmEnabled: process.env.PARTNER_INTELLIGENCE_LLM_EXTRACTION_ENABLED === "1",
    sourcePreviews,
    mergedByTargetKey: byKey,
    summary: {
      sourcesPreviewed: sourcePreviews.length,
      sourcesWithErrors: sourcePreviews.filter((s) => s.error).length,
      totalFactCandidates: allFacts.length,
      keysWithCandidates: TARGET_FACT_KEYS.filter((k) => (byKey[k] || []).length > 0).length,
    },
  };
}

async function main() {
  const report = await buildHotelEquitiesExtractionPreview();
  const jsonPath = join(ROOT, "reports", "hotel-equities-extraction-preview.json");
  const mdPath = join(ROOT, "reports", "hotel-equities-extraction-preview.md");
  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const lines = [
    "# Hotel Equities Extraction Preview (read-only)",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Operator: \`${report.operatorId}\``,
    "",
    "## Summary",
    "",
    `- Sources previewed: ${report.summary.sourcesPreviewed}`,
    `- Fact candidates (target keys): ${report.summary.totalFactCandidates}`,
    `- Target keys with ≥1 candidate: ${report.summary.keysWithCandidates} / ${report.targetFactKeys.length}`,
    `- LLM enabled: ${report.llmEnabled}`,
    "",
  ];
  if (report.registryMissingKeys.length) {
    lines.push(`**Registry missing keys:** ${report.registryMissingKeys.join(", ")}`, "");
  }
  for (const sp of report.sourcePreviews) {
    lines.push(`## ${sp.sourceTitle || sp.sourceId}`, "");
    if (sp.error) {
      lines.push(`Error: ${sp.error}`, "");
      continue;
    }
    lines.push(
      `- Document: ${sp.documentKind} (${sp.textLength} chars)`,
      `- Classifier role: ${sp.classification.role}`,
      `- Extractor: ${sp.extractor} (LLM: ${sp.usedLlm})`,
      `- Candidates: ${(sp.facts || []).length}`,
      ""
    );
    for (const f of sp.facts || []) {
      lines.push(`### \`${f.fieldKey}\``, "", `- Value: ${f.extractedValue}`, `- Evidence: ${f.evidenceText}`, "");
    }
  }
  writeFileSync(mdPath, lines.join("\n"));
  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  console.log(
    `[he-extract-preview] sources=${report.summary.sourcesPreviewed} candidates=${report.summary.totalFactCandidates} keys=${report.summary.keysWithCandidates}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
