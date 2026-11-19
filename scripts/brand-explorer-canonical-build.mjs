#!/usr/bin/env node
/**
 * Canonical build entry. Materialization is disabled.
 * This command validates the pipeline and refuses every Airtable write.
 */
import { instantiateContentLedger } from "../lib/brand-explorer/canonical/content-item-ledger.js";
import { runBrandExplorerDoctor } from "../lib/brand-explorer/canonical/doctor.js";

if (process.argv.includes("--apply")) {
  console.error("Canonical build refuses --apply. No Brand Basics or Presentation writes.");
  process.exit(2);
}

const doctor = runBrandExplorerDoctor();
const ledger = instantiateContentLedger();
console.log(JSON.stringify({
  pipeline: [
    "IDENTITY",
    "FOUNDATION",
    "PRODUCT_SHELL",
    "ITEM_APPLICABILITY",
    "RESEARCH",
    "ATOMIC_CONTENT",
    "COLLECTION_ENUMERATION",
    "EVIDENCE_FACT_GRAPH",
    "MATERIALIZATION_DISABLED",
    "IMAGE_PROPERTY_EVIDENCE",
    "RENDER",
    "AUDITS",
    "MASTER_RELEASE_CONTRACT_PASS",
    "READY_FOR_FOUNDER_REVIEW",
  ],
  productionBuildEnabled: doctor.productionBuildEnabled,
  ledgerDenominator: ledger.denominator,
  baseCanonicalItems: ledger.baseCanonicalItems,
  writes: 0,
  umusic: "not_created",
}, null, 2));
process.exit(doctor.pass ? 0 : 1);
