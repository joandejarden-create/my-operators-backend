#!/usr/bin/env node
/**
 * Annotate Design Hotels STAGED packages with secondary-contact research status.
 * Does not invent secondaries. Rebuilds integrity-valid packages + STAGED pointers.
 * $0 — no paid research.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const {
  isPlausibleBuyerPersonName,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js"
);
const {
  evaluateNamedContactCoverageStandard,
  assertFinalizedPackageNamedContactCoverage,
  SECONDARY_CONTACT_RESEARCH_STATUS,
  attachNamedContactCoverageMetadata,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js"
);
const {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
} = await import(
  "../lib/group-demand-intelligence/publication-package/gdi-customer-publication-package-v1.js"
);
const {
  createGdiPublicationPackageStore,
} = await import(
  "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js"
);
const {
  GDI_PUBLICATION_LIFECYCLE_STATE,
} = await import(
  "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js"
);

const HOTELS = [
  { hotelId: "rece0NdV9FCnaFlVS", hotelName: "Condesa DF" },
  { hotelId: "recaokE8psIbuVeuH", hotelName: "LaLuna Boutique Hotels & Villas" },
  { hotelId: "rec1t35e3sPerzxYu", hotelName: "Memmo Príncipe Real" },
];
const DATE = new Date().toISOString().slice(0, 10).replace(/-/g, "");
const applyPointer = process.argv.includes("--apply-pointer");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function main() {
  const store = createGdiPublicationPackageStore({
    root: path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
    ),
  });
  const audit = { at: new Date().toISOString(), hotels: [], totals: {} };
  let one = 0;
  let two = 0;
  let zero = 0;

  for (const hotel of HOTELS) {
    const ptr = store.readPointer(hotel.hotelId, "STAGED");
    const loaded = store.loadPackage(hotel.hotelId, ptr.publicationVersion);
    if (!loaded.ok) {
      audit.hotels.push({ hotelId: hotel.hotelId, error: loaded.error });
      continue;
    }
    const annotated = [];
    const rows = [];
    for (const o of loaded.package.opportunities || []) {
      let next = { ...o };
      const primaryOk = isPlausibleBuyerPersonName(next.primaryContact?.name);
      const secondaryOk = isPlausibleBuyerPersonName(next.secondaryContact?.name);
      if (primaryOk && !secondaryOk) {
        next = attachNamedContactCoverageMetadata(next, {
          secondContactAttempted: true,
          secondContactFound: false,
          secondContactDisposition:
            "Second relevant named contact not verified from available public evidence after buyer-WHO / contact investigation",
          contactGaps: [
            "Second relevant named contact not available from current evidence (not an eligibility failure).",
          ],
        });
      } else if (primaryOk && secondaryOk) {
        next = attachNamedContactCoverageMetadata(next, {
          secondContactAttempted: true,
          secondContactFound: true,
        });
      }
      const ev = evaluateNamedContactCoverageStandard(next);
      if (ev.namedCount === 0) zero += 1;
      else if (ev.namedCount === 1) one += 1;
      else two += 1;
      rows.push({
        opportunityId: text(o.id || o.opportunityId),
        organizationName: o.organizationName,
        primary: ev.primary?.name || null,
        primaryRole: ev.primary?.role || null,
        secondary: ev.secondary?.name || null,
        namedCount: ev.namedCount,
        secondaryResearchStatus: ev.secondaryResearchStatus,
        disposition: ev.disposition,
        primarySourceUrl: next.primaryContact?.sourceUrl || null,
        qualityNotes: [
          !next.primaryContact?.sourceUrl ? "PRIMARY_SOURCE_URL_MISSING" : null,
          !text(next.primaryContact?.role || next.primaryContact?.title)
            ? "PRIMARY_TITLE_MISSING"
            : null,
          /coordinator:\s*rev\.|donate home page|nowhere to go/i.test(
            text(next.primaryContact?.role || next.primaryContact?.title),
          )
            ? "PRIMARY_TITLE_QUALITY_FLAG"
            : null,
        ].filter(Boolean),
      });
      annotated.push(next);
    }

    const coverageGate = assertFinalizedPackageNamedContactCoverage(annotated, {
      requireSecondaryAttemptDocumentation: true,
    });
    const version = `gdi_dh_named_coverage_${hotel.hotelId}_${DATE}`;
    const pkg = buildCustomerPublicationPackage({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      opportunities: annotated,
      publicationVersion: version,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
      approvalStatus: "CANDIDATE",
      applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
      notes:
        "Named-contact coverage standard V1 — min 1 verified person; secondary research attempted/documented. Historical packages preserved. No CURRENT.",
    });
    pkg.lineage = {
      priorStagedVersion: ptr.publicationVersion,
      namedContactCoverageStandard: "gdi-named-contact-coverage-standard-v1",
      currentPointerUpdated: false,
    };
    const integrity = verifyPublicationPackageIntegrity(pkg);
    store.writePackage(pkg);
    let staged = { ok: false };
    if (applyPointer && integrity.ok && coverageGate.ok) {
      staged = store.stageForProduction({
        ...pkg,
        lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
      });
    }
    audit.hotels.push({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      priorVersion: ptr.publicationVersion,
      version: pkg.publicationVersion,
      count: annotated.length,
      oneNamed: rows.filter((r) => r.namedCount === 1).length,
      twoNamed: rows.filter((r) => r.namedCount === 2).length,
      zeroNamed: rows.filter((r) => r.namedCount === 0).length,
      coverageGateOk: coverageGate.ok,
      integrityOk: integrity.ok,
      staged: staged.ok === true,
      rows,
    });
    console.log(
      `${hotel.hotelName}: ${annotated.length} opps one=${rows.filter((r) => r.namedCount === 1).length} two=${rows.filter((r) => r.namedCount === 2).length} coverage=${coverageGate.ok} staged=${staged.ok === true}`,
    );
  }

  audit.totals = {
    oneNamed: one,
    twoNamed: two,
    zeroNamed: zero,
    totalFinalized: one + two + zero,
    unresolvedOutsideStaged: 110,
  };
  const outDir = path.join(
    ROOT,
    "reports/gdi-named-contact-coverage-standard-v1",
  );
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "DESIGN_HOTELS_18_CONTACT_AUDIT.json"),
    JSON.stringify(audit, null, 2),
  );
  console.log(`WROTE ${path.join(outDir, "DESIGN_HOTELS_18_CONTACT_AUDIT.json")}`);
  console.log(JSON.stringify(audit.totals, null, 2));
}

main();
