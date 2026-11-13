#!/usr/bin/env node
/**
 * Compare staged/approved GDI publication package against a candidate serve
 * (local DTOs or live production share). Writes certification report JSON.
 *
 * Usage:
 *   node scripts/gdi-publication-parity-certify-v1.mjs --hotel recLuxvwwxID7U2B8 --production
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublicationPackageStore,
  comparePublicationParity,
  comparePublicationInventory,
  comparePublicationContent,
  compareRenderedCustomerText,
  runPublicationQualityGate,
} from "../lib/group-demand-intelligence/publication-package/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-publication-parity-certification-v1");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}
function hasFlag(name) {
  return process.argv.includes(name);
}
function text(v) {
  return String(v == null ? "" : v).trim();
}

async function loadProductionShare(hotelId, shareToken) {
  const url = `https://my-operators-backend-production.up.railway.app/api/group-demand-intelligence/share/hotels/${hotelId}/opportunities?view=full`;
  const res = await fetch(url, {
    headers: { "X-GDI-Share-Capability": shareToken },
  });
  const api = await res.json();
  return {
    httpOk: res.ok,
    status: res.status,
    url,
    opportunities: api.opportunities || [],
    count: api.count ?? (api.opportunities || []).length,
    publication: api.publication || null,
    raw: api,
  };
}

async function main() {
  const hotelId = text(arg("--hotel"));
  if (!hotelId) {
    console.error("Usage: --hotel <id> [--production]");
    process.exit(2);
  }
  const store = createGdiPublicationPackageStore();
  const staged = store.readPointer(hotelId, "STAGED");
  const current = store.readPointer(hotelId, "CURRENT");
  const version =
    text(arg("--version")) ||
    staged?.publicationVersion ||
    current?.publicationVersion;
  if (!version) {
    console.error("No staged/current publication version found");
    process.exit(1);
  }
  const loaded = store.loadPackage(hotelId, version);
  if (!loaded.ok) {
    console.error(loaded);
    process.exit(1);
  }
  const pkg = loaded.package;

  let candidate;
  let productionMeta = null;
  if (hasFlag("--production")) {
    const shareToken =
      text(arg("--share-token")) ||
      text(process.env.GDI_BETHESDA_SHARE_TOKEN) ||
      "gdishare.v1.eyJ2IjoxLCJ0aWQiOiJnZGlzaHRfNDdjMjVkNzRjNzkyMTYwMjFmYjM2MTUwIiwiaG90ZWxJZCI6InJlY0x1eHZ3d3hJRDdVMkI4Iiwic3VyZmFjZXMiOlsiYnJpZWYiLCJvcHBvcnR1bml0aWVzIiwib3Bwb3J0dW5pdHlfZGV0YWlsIiwic3VtbWFyeSJdLCJtb2RlIjoicmVhZF9vbmx5IiwiaWF0IjoxNzg5MjE0OTMyLCJleHAiOjE3OTg2NzUyMDB9.DcmVDLtAQD_XkAvgI8mn6ir8YREmAwJ0uG3VOQXY-gc";
    productionMeta = await loadProductionShare(hotelId, shareToken);
    candidate = {
      opportunities: productionMeta.opportunities,
      sortOrder: productionMeta.opportunities.map((o) => o.id),
      contentFingerprintSha256:
        productionMeta.publication?.contentFingerprintSha256 || null,
    };
  } else {
    candidate = {
      opportunities: pkg.opportunities,
      sortOrder: pkg.sortOrder,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
    };
  }

  const inventory = comparePublicationInventory(pkg, candidate);
  const content = comparePublicationContent(pkg, candidate);
  const rendered = compareRenderedCustomerText(pkg, candidate);
  const parity = comparePublicationParity(pkg, candidate);
  const prodQuality = runPublicationQualityGate({
    opportunities: candidate.opportunities,
  });

  const namedIds = [
    "gdi_pe_781f12393f8117e7",
    "gdi_opp_bethesda_premier_cup_2026",
    "gdi_opp_potomac_memorial_2027",
    "gdi_opp_acvnu_renal_week_20270412",
  ];
  const named = namedIds.map((id) => {
    const a = pkg.opportunities.find((o) => o.id === id);
    const c = candidate.opportunities.find((o) => o.id === id);
    const slim = (o) =>
      o
        ? {
            displayTitle: o.displayTitle,
            salesActionPillLabel: o.salesActionPillLabel,
            salesActionKey: o.salesActionKey,
            whatIsHappening: String(o.whatIsHappening || "").slice(0, 240),
            hotelSourcingStatusLabel: o.hotelSourcingStatusLabel,
            tileClassificationLine: o.tileClassificationLine,
            recommendedNextStep: String(o.recommendedNextStep || "").slice(0, 200),
          }
        : null;
    return { id, approved: slim(a), candidate: slim(c) };
  });

  fs.mkdirSync(OUT, { recursive: true });

  const sot = {
    schema: "gdi-publication-source-of-truth-analysis-v1",
    hotelId,
    analyzedAt: new Date().toISOString(),
    divergenceRootCauses: [
      {
        id: "INDEPENDENT_PRESENTATION_RECOMPUTE",
        detail:
          "Local preview and production share each re-run projectOpportunityForPresentation / sales-action resolver over live inventory instead of serving one frozen approved package. Example: Potomac Memorial raw BUILD_ACCOUNT, local projected WIN_ROOM_BLOCK, production QUALIFY_OPPORTUNITY.",
      },
      {
        id: "SUMMARY_ORG_EQUALS_TITLE",
        detail:
          "opportunity-summary-v1 emitted Org is associated with Org when organizationName equals event/title (Woman's Club of Bethesda).",
      },
      {
        id: "TILE_LINE_REPEATS_SOURCING_BADGE",
        detail:
          "tileClassificationLine included hotelSourcingStatusLabel while the UI also renders the sourcing badge, so customers saw the same status twice.",
      },
      {
        id: "NO_IMMUTABLE_PUBLICATION_FINGERPRINT",
        detail:
          "Prior Bethesda 67 recovery locked inventory selection but did not freeze customer-facing DTO content under a content SHA-256 publication package.",
      },
      {
        id: "AIRTABLE_LIVE_REREAD",
        detail:
          "Share API loadOppDoc + presentation pipeline can drift from founder-reviewed local projection after research refreshes or resolver changes.",
      },
    ],
    dataFlow: [
      "Research discovery",
      "Research qualification",
      "Canonical opportunity store / Airtable",
      "Commercial presentation transformations",
      "Semantic remediation",
      "Customer DTO",
      "Local preview",
      "Publication approval",
      "Production storage",
      "Customer-share API",
      "External HTML rendering",
    ],
    divergencePoints: [
      "Different inventory selection (customerFacing vs approved 67 selector)",
      "Legacy Airtable records without package freeze",
      "Backend version drift of sales-action resolver",
      "Presentation sanitizer version drift",
      "Missing commercial remediation modules on one deploy",
      "Re-sort / re-filter after approval",
      "Share-token resolving against live inventory rather than package version",
    ],
    candidatePackage: {
      publicationId: pkg.publicationId,
      publicationVersion: pkg.publicationVersion,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      inventoryCounts: pkg.inventoryCounts,
      lifecycleState: pkg.lifecycleState,
      approvalStatus: pkg.approvalStatus,
      staged: Boolean(staged),
      customerPublished: false,
    },
    namedDefectExamples: named,
  };

  fs.writeFileSync(
    path.join(OUT, "01-SOURCE-OF-TRUTH-ANALYSIS.json"),
    `${JSON.stringify(sot, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(OUT, "02-OPPORTUNITY-PARITY.json"),
    `${JSON.stringify(
      {
        inventory,
        approvedCounts: inventory.approvedCounts,
        productionCounts: inventory.candidateCounts,
      },
      null,
      2,
    )}\n`,
  );
  fs.writeFileSync(
    path.join(OUT, "03-CONTENT-DIFFERENCES.json"),
    `${JSON.stringify(
      {
        fieldDifferenceCount: content.fieldDifferenceCount,
        comparedOpportunityCount: content.comparedOpportunityCount,
        differences: content.differences.slice(0, 250),
        truncated: content.differences.length > 250,
        totalDifferences: content.differences.length,
      },
      null,
      2,
    )}\n`,
  );
  fs.writeFileSync(
    path.join(OUT, "04-PRESENTATION-DIFFERENCES.json"),
    `${JSON.stringify(
      {
        renderedTextDifferenceCount: rendered.renderedTextDifferenceCount,
        differences: rendered.differences.slice(0, 120),
        totalDifferences: rendered.differences.length,
        candidateQuality: {
          ok: prodQuality.ok,
          blockingCount: prodQuality.blockingCount,
          findingCount: prodQuality.findingCount,
          blockingSample: prodQuality.blocking.slice(0, 40),
        },
      },
      null,
      2,
    )}\n`,
  );
  fs.writeFileSync(
    path.join(OUT, "07-PRODUCTION-VERIFICATION.json"),
    `${JSON.stringify(
      {
        verifiedAt: new Date().toISOString(),
        mode: hasFlag("--production") ? "production_share" : "self",
        endpoint: productionMeta?.url || null,
        httpOk: productionMeta?.httpOk ?? null,
        shareTokenPreserved: hasFlag("--production"),
        productionOpportunityCount: productionMeta?.count ?? null,
        productionPublicationMeta: productionMeta?.publication || null,
        packageServed: Boolean(productionMeta?.publication?.publicationId),
        note: hasFlag("--production")
          ? "Production still on legacy share path unless CUSTOMER_PUBLISHED pointer is active. Candidate package is staged locally only — not customer-published."
          : "Self comparison",
        parity,
        publicationReady: false,
        blockers: [
          "Quality gate blocking on candidate package",
          "Founder fingerprint approval not granted",
          "Production CUSTOMER_PUBLISHED package not activated",
          ...(parity.ok
            ? []
            : ["Content/rendered/inventory differences vs candidate serve"]),
        ],
      },
      null,
      2,
    )}\n`,
  );

  console.log(
    JSON.stringify(
      {
        hotelId,
        publicationVersion: pkg.publicationVersion,
        contentFingerprintSha256: pkg.contentFingerprintSha256,
        inventoryOk: inventory.ok,
        approved: inventory.approvedCounts,
        candidate: inventory.candidateCounts,
        contentDiffs: content.fieldDifferenceCount,
        renderedDiffs: rendered.renderedTextDifferenceCount,
        parityOk: parity.ok,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
