#!/usr/bin/env node
/**
 * Brand Explorer 62 — Webhound claim validation (read-only).
 *
 *   npm run brand-explorer-62-webhound-claim-validation-readonly -- --dry-run
 *   npm run brand-explorer-62-webhound-claim-validation-readonly -- --dry-run --extract-only
 *   npm run brand-explorer-62-webhound-claim-validation-readonly -- --dry-run --merge-webhound path/to/rows.json
 */
import "../load-env.js";
import fs from "node:fs";
import {
  VALIDATION_VERSION,
  extractActive62Claims,
  selectWebhoundClaimPack,
  mergeWebhoundDatasetRows,
  assembleValidationReport,
  writeClaimValidationArtifacts,
} from "../lib/partner-intelligence/brand-explorer-62-webhound-claim-validation.js";

function readArgValue(argv, flag) {
  const i = argv.indexOf(flag);
  if (i < 0) return null;
  return argv[i + 1] || null;
}

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes("--apply")) {
    console.error("Refusing --apply. Claim validation is read-only.");
    process.exit(2);
  }
  if (!argv.includes("--dry-run")) {
    console.error("Require --dry-run (read-only; no writes).");
    process.exit(2);
  }

  const extractOnly = argv.includes("--extract-only");
  // P1 public tabs + P2 momentum + P3 property + P4 parent — larger than v1 parent/property pack.
  const maxClaims = Number(readArgValue(argv, "--max-pack") || 186);
  const mergePath = readArgValue(argv, "--merge-webhound");
  const maxBrandsRaw = readArgValue(argv, "--max-brands");
  const maxBrands = maxBrandsRaw ? Number(maxBrandsRaw) : null;
  const priorReportPath =
    readArgValue(argv, "--prior-report") ||
    "reports/brand-explorer/brand-explorer-62-webhound-claim-validation-readonly.json";
  let priorWebhound = null;
  if (fs.existsSync(priorReportPath)) {
    try {
      const prior = JSON.parse(fs.readFileSync(priorReportPath, "utf8"));
      priorWebhound = prior.webhound || null;
    } catch (err) {
      console.warn(`[warn] could not read prior report: ${err.message}`);
    }
  }

  console.log(`[${VALIDATION_VERSION}] extract Active-62 claims (read-only)`);
  const extracted = await extractActive62Claims({ maxBrands });
  const pack = selectWebhoundClaimPack(extracted.claims, { maxClaims });
  console.log(
    `Extracted claims=${extracted.claims.length} factual=${extracted.claims.filter((c) => c.factuality === "factual_candidate").length} pack=${pack.length}`
  );

  let webhoundMeta = {
    role: "research_sidecar_not_sot",
    packPrepared: true,
    packSize: pack.length,
    sessionId: priorWebhound?.sessionId || null,
    url: priorWebhound?.url || null,
    budgetUsd: priorWebhound?.budgetUsd ?? null,
    spendUsd: priorWebhound?.spendUsd ?? null,
    datasetRowsMerged: 0,
    note: extractOnly
      ? "Extract-only: start Webhound dataset externally with the claim pack CSV/JSON."
      : "Awaiting Webhound merge via --merge-webhound or MCP assembly.",
  };

  if (mergePath) {
    const raw = JSON.parse(fs.readFileSync(mergePath, "utf8"));
    const rows = Array.isArray(raw) ? raw : raw.rows || raw.records || raw.claims || [];
    const merged = mergeWebhoundDatasetRows(extracted.claims, rows);
    webhoundMeta = {
      ...webhoundMeta,
      ...(raw.webhoundMeta || {}),
      datasetRowsMerged: merged.matched,
      mergePath,
      note: "Merged Webhound dataset rows into claim inventory.",
    };
    console.log(`Merged Webhound rows matched=${merged.matched}`);
  }

  const report = assembleValidationReport({
    frozen: extracted.frozen,
    liveUniverseCount: extracted.liveUniverseCount,
    brandsChecked: extracted.brandsChecked,
    claims: extracted.claims,
    webhoundPack: pack,
    webhoundMeta,
  });

  const paths = writeClaimValidationArtifacts(report, { packClaims: pack });
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docsPath}`);
  console.log(`Wrote pack ${paths.packCsvPath}`);
  console.log(`Status: ${report.status}`);
  console.log(`Summary: ${JSON.stringify(report.summary)}`);
  console.log(
    `Writes: airtable=${report.airtableWrites} be=${report.brandExplorerWrites} setup=${report.brandSetupWrites} census=${report.censusWrites}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
