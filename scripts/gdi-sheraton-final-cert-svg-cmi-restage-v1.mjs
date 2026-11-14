#!/usr/bin/env node
/**
 * Restage Sheraton for final certification: fix AMHPAC/APIEJ revenueStreamIcons
 * that were incorrectly stored as revenueStreamClass objects.
 * Preserves actionability v2 archive. Zero paid spend.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  computePublicationInventoryCounts,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { attachSalesOpportunityFilterFields } from "../lib/group-demand-intelligence/sales-opportunity-quick-filters-v1.js";
import { annotateInventoryPopulations } from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { annotateCanonicalCmiContract } from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import { GDI_FULL_E2E_HOTEL_TEMPLATE_V1 } from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import {
  resolveOpportunityRevenueStreamIcons,
  GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1,
} from "../lib/group-demand-intelligence/opportunity-revenue-stream-icons-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recCAGqkERSXgiku4";
const SOURCE_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261114_commercial_actionability_gate_v2";
const SOURCE_FP =
  "690c90dc250d7ae1c119d92954a899aca76f7db85077c44a5f7eaa395defe5b8";
const V3_FP =
  "2c9bb143887af8cae49050ff67fbb4f414e05f4eea924b3326489522b09065ef";

const store = createGdiPublicationPackageStore({
  root: path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  ),
});
const loaded = store.loadPackage(HOTEL_ID, SOURCE_VERSION);
if (!loaded.ok || loaded.package.contentFingerprintSha256 !== SOURCE_FP) {
  console.error("Source actionability v2 mismatch");
  process.exit(3);
}

const FIX_IDS = new Set([
  "cmi_sheraton_amhpac_rsa_20270303",
  "cmi_sheraton_apiej_20271013",
]);

const fixed = (loaded.package.opportunities || []).map((o) => {
  let next = { ...o };
  if (FIX_IDS.has(o.id)) {
    // Icons were wrongly stored in revenueStreamClass; restore taxonomy.
    const misplaced =
      o.revenueStreamClass && typeof o.revenueStreamClass === "object"
        ? o.revenueStreamClass
        : null;
    next.revenueStreamClass = "COMBINED_GUESTROOMS_EVENT_FNB";
    const resolved = resolveOpportunityRevenueStreamIcons({
      ...next,
      revenueStreamClass: "COMBINED_GUESTROOMS_EVENT_FNB",
      revenueStreamIcons: null,
      whatIsHappening:
        next.whatIsHappening ||
        "Competitor-committed congress/symposium with guestroom and meeting demand.",
      potentialHotelBusiness:
        next.potentialHotelBusiness ||
        "Guestrooms and meetings/catering at competitor venue — CMI future cycle.",
    });
    next.revenueStreamClass = resolved.revenueStreamClass;
    next.revenueStreamIcons = resolved.icons;
    next.revenueStreamIconsVersion = GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1;
    next.revenueStreamIconsAria = resolved.aria;
    next._svgFixNote = {
      priorMisplacedClassObject: Boolean(misplaced),
      restoredClass: next.revenueStreamClass,
      restoredIcons: next.revenueStreamIcons,
    };
  }
  return attachSalesOpportunityFilterFields(annotateCanonicalCmiContract(next));
});

const annotated = annotateInventoryPopulations(fixed, { hotelId: HOTEL_ID });
const publicationVersion = `gdi_pub_${HOTEL_ID}_20261114_final_certification_v1`;
const built = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: "Sheraton Guadalajara Expo",
  publicationVersion,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  opportunities: annotated,
  templateVersion: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  notes: {
    purpose:
      "Final certification restage — restore frozen revenueStreamIcons on AMHPAC/APIEJ CMI rows",
    preservesActionabilityV2: SOURCE_FP,
    paidSpendUsd: 0,
  },
});
const pkg = built.package || built;
const integrity = verifyPublicationPackageIntegrity(pkg);
store.writePackage(pkg);
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  supersedes: {
    publicationVersion: SOURCE_VERSION,
    contentFingerprintSha256: SOURCE_FP,
    preservesV3: {
      version: "gdi_pub_recCAGqkERSXgiku4_20261114_paid_full_e2e_discovery_v3",
      fingerprint: V3_FP,
    },
    preservesActionabilityV2: {
      version: SOURCE_VERSION,
      fingerprint: SOURCE_FP,
    },
  },
});

const after = store.loadPackage(HOTEL_ID, SOURCE_VERSION);
console.log(
  JSON.stringify(
    {
      ok: integrity?.ok !== false,
      publicationVersion: pkg.publicationVersion,
      fingerprint: pkg.contentFingerprintSha256,
      inventoryCounts: computePublicationInventoryCounts(pkg.opportunities || []),
      sourceV2Unchanged: after.package?.contentFingerprintSha256 === SOURCE_FP,
      fixedIds: [...FIX_IDS],
    },
    null,
    2,
  ),
);
