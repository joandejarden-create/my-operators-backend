#!/usr/bin/env node
/**
 * Build a GDI Customer Publication Package from the local approved presentation.
 *
 * Usage:
 *   node scripts/gdi-build-customer-publication-package-v1.mjs --hotel recLuxvwwxID7U2B8
 *   node scripts/gdi-build-customer-publication-package-v1.mjs --hotel recLuxvwwxID7U2B8 --stage
 *
 * Does NOT set CUSTOMER_PUBLISHED / customer-visible pointer without
 * --publish (requires FOUNDER_APPROVED approvalStatus + fingerprint receipt).
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  runPublicationQualityGate,
  comparePublicationParity,
  GDI_PUBLICATION_LIFECYCLE_STATE,
  dedupeTileClassificationSourcing,
  remediateDuplicateOrgAssociationSentence,
  stripEmptyParentheses,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}
function hasFlag(name) {
  return process.argv.includes(name);
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function applyPackagePresentationRemediation(dto) {
  const next = { ...dto };
  const org = next.organizationName || next.displayCompanyName || "";
  if (next.whatIsHappening) {
    next.whatIsHappening = remediateDuplicateOrgAssociationSentence(
      next.whatIsHappening,
      org,
    );
  }
  for (const k of [
    "whatIsHappening",
    "summary",
    "cardWhyNowLine",
    "displayTitle",
    "title",
    "whyActNow",
    "whyThisHotel",
    "recommendedNextStep",
  ]) {
    if (typeof next[k] === "string") next[k] = stripEmptyParentheses(next[k]);
  }
  if (next.tileClassificationLine || next.hotelSourcingStatusLabel) {
    next.tileClassificationLine = dedupeTileClassificationSourcing(
      next.tileClassificationLine,
      next.hotelSourcingStatusLabel,
    );
  }
  return next;
}

async function loadApprovedShareDtos(hotelId) {
  const doc = await loadOpportunitiesCanonical(hotelId, { bypassCache: true });
  let ops = doc.opportunities || [];

  if (hotelId === "recLuxvwwxID7U2B8") {
    const { selectBethesdaExternalShareOpportunities } = await import(
      "../lib/group-demand-intelligence/bethesda-share-inventory-v1.js"
    );
    const selected = selectBethesdaExternalShareOpportunities(ops, {
      hotelId,
      forceApproved: true,
    });
    ops = selected.opportunities;
  } else {
    const { filterCustomerFacingOpportunities } = await import(
      "../lib/group-demand-intelligence/customer-visibility.js"
    );
    const { annotateInventoryPopulations } = await import(
      "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js"
    );
    ops = annotateInventoryPopulations(
      filterCustomerFacingOpportunities(ops, { hotelId }),
      { hotelId },
    );
  }

  // Freeze Hotel Need-Date Match into the package (same attach path as live list).
  // Timing overlap must never be reinterpreted as an open room-block after approval.
  let withNeedDates = ops;
  try {
    const { attachHotelNeedDateTilePreviews } = await import(
      "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js"
    );
    const { listRegisteredHotels } = await import(
      "../lib/group-demand-intelligence/repository.js"
    );
    const hotels = listRegisteredHotels();
    const hotelMeta =
      hotels.find((h) => h.hotelId === hotelId) || { hotelId, name: hotelId };
    withNeedDates = await attachHotelNeedDateTilePreviews(hotelId, ops, {
      hotelName: hotelMeta.name || hotelMeta.hotelName || null,
    });
  } catch (err) {
    console.warn(
      "[gdi-build-publication-package] need-date attach failed:",
      err?.message || err,
    );
  }

  return withNeedDates.map((o) => {
    const projected = projectOpportunityForPresentation(o, {
      hotelId,
      surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
      view: "detail",
    });
    return applyPackagePresentationRemediation(projected.dto);
  });
}

async function main() {
  const hotelId = text(arg("--hotel"));
  if (!hotelId) {
    console.error("Usage: --hotel <hotelId>");
    process.exit(2);
  }
  const hotelName = text(arg("--hotel-name")) || null;
  const publicationVersion =
    text(arg("--version")) ||
    `gdi_pub_${hotelId}_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}_candidate`;

  const opportunities = await loadApprovedShareDtos(hotelId);
  const pkg = buildCustomerPublicationPackage({
    hotelId,
    hotelName,
    opportunities,
    publicationVersion,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
    approvalStatus: "CANDIDATE",
    applicationCompatibilityVersion: text(arg("--app-compat")) || "gdi-customer-app-compat-v1",
    notes: "Candidate package — not customer-published until founder fingerprint approval",
  });

  const integrity = verifyPublicationPackageIntegrity(pkg);
  const quality = runPublicationQualityGate({ opportunities: pkg.opportunities });
  // Self-parity (package vs itself) must pass inventory/content/fingerprint;
  // quality may still fail and block certification.
  const selfParity = comparePublicationParity(pkg, {
    opportunities: pkg.opportunities,
    sortOrder: pkg.sortOrder,
    contentFingerprintSha256: pkg.contentFingerprintSha256,
  });

  const reportDir = path.join(
    ROOT,
    "reports/gdi-publication-parity-certification-v1",
  );
  fs.mkdirSync(reportDir, { recursive: true });
  const store = createGdiPublicationPackageStore();
  const written = store.writePackage(pkg);

  const summary = {
    ok: integrity.ok && written.ok,
    hotelId,
    publicationId: pkg.publicationId,
    publicationVersion: pkg.publicationVersion,
    contentFingerprintSha256: pkg.contentFingerprintSha256,
    inventoryCounts: pkg.inventoryCounts,
    integrity,
    quality: {
      ok: quality.ok,
      blockingCount: quality.blockingCount,
      findingCount: quality.findingCount,
      blockingSample: quality.blocking.slice(0, 25),
    },
    selfParity: {
      ok: selfParity.ok,
      publicationReady: selfParity.publicationReady,
      fingerprintMatch: selfParity.fingerprintMatch,
      inventoryOk: selfParity.inventory.ok,
      contentOk: selfParity.content.ok,
      qualityApprovedOk: selfParity.quality.approvedOk,
    },
    storePath: written.path || null,
    staged: false,
    published: false,
    founderApprovalRequired: true,
  };

  if (hasFlag("--stage") && integrity.ok) {
    const staged = store.stageForProduction({
      ...pkg,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
      gates: {
        integrity: { ok: integrity.ok },
        quality: { ok: quality.ok, blockingCount: quality.blockingCount },
      },
    });
    summary.staged = staged.ok === true;
    summary.stageResult = staged;
  }

  if (hasFlag("--publish")) {
    summary.published = false;
    summary.publishBlocked = {
      reason: "FOUNDER_FINGERPRINT_APPROVAL_REQUIRED",
      note: "Refusing --publish without explicit founder approval receipt matching fingerprint",
    };
  }

  fs.writeFileSync(
    path.join(reportDir, "05-PUBLICATION-PACKAGE-MANIFEST.json"),
    `${JSON.stringify({ package: {
      schema: pkg.schema,
      hotelId: pkg.hotelId,
      hotelName: pkg.hotelName,
      publicationId: pkg.publicationId,
      publicationVersion: pkg.publicationVersion,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      opportunityCount: pkg.opportunityCount,
      inventoryCounts: pkg.inventoryCounts,
      inventoryIds: pkg.inventoryIds,
      sortOrder: pkg.sortOrder,
      lifecycleState: pkg.lifecycleState,
      approvalStatus: pkg.approvalStatus,
      presentationSchemaVersion: pkg.presentationSchemaVersion,
      applicationCompatibilityVersion: pkg.applicationCompatibilityVersion,
      createdAt: pkg.createdAt,
    }, buildSummary: summary }, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(reportDir, "06-QUALITY-GATE-RESULTS.json"),
    `${JSON.stringify(quality, null, 2)}\n`,
  );

  console.log(JSON.stringify(summary, null, 2));
  if (!integrity.ok || !written.ok) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
