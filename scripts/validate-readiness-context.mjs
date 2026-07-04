/**
 * Context-aware readiness validation report (read-only).
 * Usage: node scripts/validate-readiness-context.mjs
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { fetchDealWithMergedLinkedRecords } from "../api/my-deals.js";
import { buildReadinessFromFields } from "../api/deal-readiness-review.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = join(__dirname, "output", "readiness-context-validation.json");

const VALIDATION_TARGETS = [
  { label: "Xavier (conversion/repositioning, gaps)", nameMatch: /xavier/i },
  { label: "Copley (conversion/reflag, mostly complete)", nameMatch: /copley/i },
  { label: "Amsterdam Airport (new-build proxy)", nameMatch: /amsterdam airport/i },
  {
    label: "Franchise-only candidate",
    pick: (deals) =>
      deals.find((d) => /^franchise$/i.test(String(d.dealStructure || "").trim())) ||
      deals.find((d) => /franchise only/i.test(String(d.dealStructure || ""))),
  },
  { label: "Lease-oriented candidate", nameMatch: /./, dealStructureHint: /^lease$/i },
];

async function listDeals(baseId, apiKey) {
  const table = encodeURIComponent(process.env.AIRTABLE_DEALS_TABLE || "Deals");
  const url = `https://api.airtable.com/v0/${baseId}/${table}?pageSize=100`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error?.message || res.statusText);
  return (data.records || []).map((r) => ({
    id: r.id,
    name: r.fields?.["Property Name"] || r.fields?.Name || r.id,
    dealStructure: r.fields?.["Preferred Deal Structure"] || "",
  }));
}

function pickTarget(deals, spec) {
  if (spec.pick) return spec.pick(deals);
  if (spec.dealStructureHint) {
    const byStruct = deals.filter((d) =>
      spec.dealStructureHint.test(String(d.dealStructure || "").trim())
    );
    if (byStruct.length) return byStruct[0];
  }
  return deals.find((d) => spec.nameMatch && spec.nameMatch.test(String(d.name || "")));
}

function summarizeResult(label, dealId, name, result) {
  return {
    label,
    dealId,
    name,
    readinessContext: result.readinessContext,
    dealReadinessScore: result.dealReadinessScore,
    readinessStage: result.readinessStage,
    weightedCompletionScore: result.weightedCompletionScore,
    appliedScoreCaps: (result.appliedScoreCaps || []).map((c) => ({
      reason: c.reason,
      maxScore: c.maxScore,
    })),
    contextExcludedFields: (result.contextExcludedFields || []).map((x) => x.field),
    contextTooEarlyFields: (result.contextTooEarlyFields || []).map((x) => x.field),
    contextConditionalActivated: (result.contextConditionalFields || [])
      .filter((x) => x.activated)
      .map((x) => x.field),
    contextConditionalInactive: (result.contextConditionalFields || [])
      .filter((x) => !x.activated)
      .map((x) => x.field),
    contextAdjustedRequiredFieldCount: result.contextAdjustedRequiredFieldCount,
    blocking: result.gapSeverityCounts?.blocking,
    limiting: result.gapSeverityCounts?.limiting,
    missingN: result.missingRequiredCount,
    draftValidationCapApplied: result.draftValidationCapApplied || false,
    computedReadinessScore: result.computedReadinessScore,
  };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY");
    process.exit(1);
  }

  const deals = await listDeals(baseId, apiKey);
  const report = { generatedAt: new Date().toISOString(), cases: [] };

  for (const spec of VALIDATION_TARGETS) {
    const deal = pickTarget(deals, spec);
    if (!deal) {
      report.cases.push({ label: spec.label, error: "No matching deal found" });
      continue;
    }
    const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, deal.id);
    if (!full) {
      report.cases.push({ label: spec.label, dealId: deal.id, error: "Fetch failed" });
      continue;
    }
    const result = buildReadinessFromFields(full.deal.fields || {});
    report.cases.push(summarizeResult(spec.label, deal.id, deal.name, result));
  }

  // Amsterdam incomplete proxy: same deal with simulated blank new-build anchors (in-memory only)
  const ams = deals.find((d) => /amsterdam airport/i.test(String(d.name || "")));
  if (ams) {
    const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, ams.id);
    const fields = { ...(full.deal.fields || {}) };
    fields["Stage of Development"] = "";
    fields["Current Form of Site Control"] = "";
    fields["Total Project Cost Range"] = "";
    fields["Project Type"] = fields["Project Type"] || "New Build";
    const sim = buildReadinessFromFields(fields);
    report.cases.push({
      ...summarizeResult(
        "Amsterdam incomplete new-build (simulated gaps, in-memory)",
        ams.id,
        ams.name + " [simulated]",
        sim
      ),
      note: "In-memory simulation only; not saved to Airtable",
    });
  }

  mkdirSync(dirname(OUT), { recursive: true });
  writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
