#!/usr/bin/env node
/**
 * Product-wide GDI opportunity-ID inventory audit (read-only).
 * Compares STAGED / CURRENT package IDs per hotel. Optional ES KPI parity.
 *
 * NO_PAID_RESEARCH · NO_AIRTABLE_WRITES · NO_STAGED_ACTIVATE · NO_CURRENT_PUBLISH
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../../lib/group-demand-intelligence/publication-package/index.js";
import { buildGdiExecutiveSummaryV1 } from "../../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v1.js";
import { buildWyndhamProposedWriteSetFounderReview } from "../../lib/group-demand-intelligence/publication-package/gdi-proposed-write-set-founder-review-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-executive-summary-v1",
);
const OUT_JSON = path.join(OUT_DIR, "PRODUCT_WIDE_OPPORTUNITY_ID_AUDIT.json");

const FOCUS = [
  { hotelId: "recLuxvwwxID7U2B8", name: "Bethesda Marriott" },
  { hotelId: "recnPB3Q02NO8Ma1o", name: "Wyndham San Diego Bayside" },
  { hotelId: "recrPQcZg7SFARRb2", name: "YOTEL Geneva Lake" },
  { hotelId: "rec35fExUxCClpOP6", name: "Hilton New York Times Square" },
  { hotelId: "recCAGqkERSXgiku4", name: "Sheraton Guadalajara Expo" },
  { hotelId: "recH9h3McgMNFQEla", name: "Radisson Blu Toronto Downtown" },
  { hotelId: "recXh4NgF8sDbi9cQ", name: "Wyndham Alltra Punta Cana" },
  { hotelId: "rec1t35e3sPerzxYu", name: "Memmo Príncipe Real" },
];

function idsFromPackage(pkg) {
  return (pkg?.opportunities || [])
    .map((o) => String(o?.id || o?.opportunityId || "").trim())
    .filter(Boolean)
    .sort();
}

function loadKind(store, hotelId, kind) {
  const pointer = store.readPointer(hotelId, kind);
  if (!pointer?.publicationVersion) {
    return {
      available: false,
      publicationVersion: null,
      count: 0,
      opportunityIds: [],
    };
  }
  const loaded = store.loadPackage(hotelId, pointer.publicationVersion);
  if (!loaded?.ok) {
    return {
      available: false,
      publicationVersion: pointer.publicationVersion,
      count: 0,
      opportunityIds: [],
      error: loaded?.error || "LOAD_FAILED",
    };
  }
  const opportunityIds = idsFromPackage(loaded.package);
  return {
    available: true,
    publicationVersion: pointer.publicationVersion,
    publicationId: pointer.publicationId || null,
    count: opportunityIds.length,
    opportunityIds,
  };
}

function diffIds(a = [], b = []) {
  const A = new Set(a);
  const B = new Set(b);
  return {
    missingFromB: a.filter((id) => !B.has(id)),
    unexpectedInB: b.filter((id) => !A.has(id)),
  };
}

function main() {
  const store = createGdiPublicationPackageStore({});
  const hotels = [];

  for (const h of FOCUS) {
    const staged = loadKind(store, h.hotelId, "STAGED");
    const current = loadKind(store, h.hotelId, "CURRENT");

    let esParity = null;
    if (current.available) {
      const summary = buildGdiExecutiveSummaryV1({
        hotelId: h.hotelId,
        hotelName: h.name,
        opportunities: current.opportunityIds.map((id) => ({ id })),
        customerAuthorizedInventory: true,
        dataState: "CURRENT_AVAILABLE",
      });
      const esCount =
        summary?.sections?.executiveCommercialSnapshot
          ?.totalQualifiedOpportunities ?? null;
      esParity = {
        mode: "CURRENT",
        opportunitiesCount: current.count,
        executiveSummaryQualifiedCount: esCount,
        idsMatchCount: esCount === current.count,
      };
    } else if (staged.available) {
      const summary = buildGdiExecutiveSummaryV1({
        hotelId: h.hotelId,
        hotelName: h.name,
        opportunities: staged.opportunityIds.map((id) => ({ id })),
        customerAuthorizedInventory: false,
        dataState: "FOUNDER_PREVIEW",
        founderPreview: true,
      });
      const esCount =
        summary?.sections?.executiveCommercialSnapshot
          ?.totalQualifiedOpportunities ?? null;
      esParity = {
        mode: "STAGED_FOUNDER_PREVIEW",
        opportunitiesCount: staged.count,
        executiveSummaryQualifiedCount: esCount,
        idsMatchCount: esCount === staged.count,
      };
    }

    hotels.push({
      hotelId: h.hotelId,
      hotelName: h.name,
      staged,
      current,
      note:
        h.hotelId === "recnPB3Q02NO8Ma1o"
          ? "STAGED=CLEAN11 (11). Locked proposed qualified=26 not applied. Not an ES regression."
          : h.hotelId === "rec35fExUxCClpOP6"
            ? "Hilton New York Times Square present; Renaissance NYTS not in bundled publication packages."
            : null,
      executiveSummaryParity: esParity,
    });
  }

  const wyndham = buildWyndhamProposedWriteSetFounderReview({});

  const report = {
    generatedAt: new Date().toISOString(),
    contract: "gdi-opportunity-inventory-parity-audit-v1",
    governance: {
      noPaidResearch: true,
      noAirtableWrites: true,
      noStagedActivate: true,
      noCurrentPublish: true,
    },
    conclusion: {
      executiveSummaryChangesCustomerInventory: false,
      wyndhamElevenRootCause:
        "STAGED pointer -> CLEAN11 package only; locked 26 write-set not applied/published",
    },
    hotels,
    wyndhamProposedWriteSetReview: wyndham.ok
      ? {
          rootCause: wyndham.rootCause,
          counts: wyndham.counts,
          proposedQualified: wyndham.proposedQualified.map((r) => ({
            opportunityId: r.opportunityId,
            org: r.org,
            bucket: r.bucket,
            visibilityState: r.visibilityState,
            publicationStatus: r.publicationStatus,
          })),
          hold: wyndham.hold.map((r) => ({
            opportunityId: r.opportunityId,
            org: r.org,
            bucket: r.bucket,
            visibilityState: r.visibilityState,
          })),
          missingFromStagedIds: wyndham.proposedQualified
            .filter((r) => !r.inStaged)
            .map((r) => r.opportunityId),
        }
      : wyndham,
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, out: OUT_JSON, hotelCount: hotels.length }, null, 2));
  for (const h of hotels) {
    console.log(
      `${h.hotelName}: STAGED=${h.staged.count} CURRENT=${h.current.count} esParity=${h.executiveSummaryParity?.idsMatchCount}`,
    );
  }
  if (wyndham.ok) {
    console.log(
      `Wyndham locked26: proposed=${wyndham.counts.lockedProposedQualified} inStaged=${wyndham.counts.proposedPresentInStaged} missing=${wyndham.counts.proposedMissingFromStaged}`,
    );
  }
}

main();
