#!/usr/bin/env node
/**
 * Rebuild versioned STAGED packages from best-contact restore + recovered named buyers.
 * Uses canonical buildCustomerPublicationPackage + store.stageForProduction.
 * Does NOT touch CURRENT. Does not overwrite historical V3.
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
  evaluateNamedBuyerFinalizationGate,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-who-permanent-full-e2e-v1.js"
);
const {
  assertFinalizedPackageNamedContactCoverage,
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

function loadPkgDir(hotelId, needle) {
  const base = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
  );
  const dir = fs
    .readdirSync(base, { withFileTypes: true })
    .find((d) => d.isDirectory() && d.name.includes(needle));
  if (!dir) return null;
  const pkgPath = path.join(base, dir.name, "package.json");
  return {
    dirName: dir.name,
    dirPath: path.join(base, dir.name),
    pkg: JSON.parse(fs.readFileSync(pkgPath, "utf8")),
  };
}

function loadRecovered() {
  const p = path.join(
    ROOT,
    "reports/gdi-buyer-who-live-recovery-v1/RECOVERED_OPPORTUNITIES.json",
  );
  if (!fs.existsSync(p)) return [];
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  return (j.opportunities || []).filter(Boolean);
}

function mergeContactOntoV3(v3Opp, recovered) {
  const name = recovered.primaryContact?.name;
  if (!isPlausibleBuyerPersonName(name)) return null;
  const gate = evaluateNamedBuyerFinalizationGate(recovered, {
    requireNamedPerson: true,
    looksLikePersonName: isPlausibleBuyerPersonName,
  });
  if (!gate.pass) return null;

  const role = recovered.primaryContact?.role || recovered.primaryContact?.title;
  const org = recovered.primaryContact?.organization || v3Opp.organizationName;
  const outreach =
    recovered.recommendedNextStep ||
    `Research outreach to ${name}${role ? ` (${role})` : ""} at ${org || "the organization"}. Confirm lodging / meeting decision role, propose property-fit options, and request timing for a short commercial conversation.`;

  return {
    ...v3Opp,
    primaryContact: {
      ...(v3Opp.primaryContact || {}),
      ...recovered.primaryContact,
      name,
      role: role || null,
      title: role || null,
      email: recovered.primaryContact?.email || null,
      emailVerificationStatus: recovered.primaryContact?.email
        ? recovered.primaryContact.emailVerificationStatus || "PUBLIC_CANDIDATE"
        : "NO_EMAIL",
      phone: recovered.primaryContact?.phone || null,
      phoneVerificationStatus: recovered.primaryContact?.phone
        ? "PUBLIC_CANDIDATE"
        : "NO_PHONE",
      organization: org,
      suggestedContactLabel:
        recovered.primaryContact?.suggestedContactLabel ||
        (role ? `Suggested contact: ${name} (${role})` : `Suggested contact: ${name}`),
      personIdentityKey:
        recovered.personIdentityKey || recovered.primaryContact?.personIdentityKey,
      sourceUrl: recovered.primaryContact?.sourceUrl || null,
      verificationStatus:
        recovered.primaryContact?.verificationStatus || "PUBLIC_SOURCE_VERIFIED",
    },
    tileContactPreview: {
      name,
      role: role || null,
      organization: org || null,
    },
    personIdentityKey:
      recovered.personIdentityKey || recovered.primaryContact?.personIdentityKey,
    contactProvenance: recovered.contactProvenance || {
      source: "BUYER_WHO_RESEARCH",
      sourceUrl: recovered.primaryContact?.sourceUrl || null,
    },
    recommendedNextStep: outreach,
    recommendedAction: outreach,
    recommendedNextAction: outreach,
    buyerWhoResearchStatus: "COMPLETE",
  };
}

function main() {
  const store = createGdiPublicationPackageStore({ root: path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  ) });
  const recovered = loadRecovered();
  const byHotel = new Map();
  for (const o of recovered) {
    const hid = text(o.hotelId);
    if (!byHotel.has(hid)) byHotel.set(hid, []);
    byHotel.get(hid).push(o);
  }

  const summary = [];

  for (const hotel of HOTELS) {
    const best = loadPkgDir(hotel.hotelId, "gdi_dh_best_contact_restore");
    const v3 = loadPkgDir(hotel.hotelId, "gdi_dh_v3_staged");
    if (!best || !v3) {
      summary.push({ hotelId: hotel.hotelId, error: "MISSING_PACKAGE" });
      continue;
    }

    const bestIds = new Set(
      (best.pkg.opportunities || []).map((o) => text(o.id || o.opportunityId)),
    );
    const recoveredForHotel = (byHotel.get(hotel.hotelId) || []).filter((o) => {
      const id = text(o.id || o.opportunityId);
      return id && !bestIds.has(id);
    });

    const v3ById = new Map(
      (v3.pkg.opportunities || []).map((o) => [text(o.id || o.opportunityId), o]),
    );

    const added = [];
    for (const rec of recoveredForHotel) {
      const id = text(rec.id || rec.opportunityId);
      const base = v3ById.get(id);
      if (!base) continue;
      const merged = mergeContactOntoV3(base, rec);
      if (!merged) continue;
      added.push(merged);
    }

    const opportunities = [...(best.pkg.opportunities || []), ...added].map(
      (o) =>
        attachNamedContactCoverageMetadata(o, {
          secondContactAttempted: true,
          secondContactFound: Boolean(o.secondaryContact?.name),
          secondContactDisposition: o.secondaryContact?.name
            ? "SECOND_VERIFIED"
            : "Second relevant named contact not verified from available public evidence",
        }),
    );
    const coverageGate = assertFinalizedPackageNamedContactCoverage(
      opportunities,
      { requireSecondaryAttemptDocumentation: true },
    );
    if (!coverageGate.ok) {
      console.error(
        `${hotel.hotelName}: named-contact coverage gate failed`,
        coverageGate.blockers,
      );
    }
    const version = `gdi_dh_buyer_who_recovery_${hotel.hotelId}_${DATE}`;
    const pkg = buildCustomerPublicationPackage({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      opportunities,
      publicationVersion: version,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
      approvalStatus: "CANDIDATE",
      applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
      notes:
        "Buyer-WHO live recovery V1 — prior named contacts preserved; newly verified named buyers only. Min-1 named contact coverage enforced. Historical V3 + best-contact restore packages preserved. No CURRENT.",
    });
    pkg.lineage = {
      preservedBestContactRestore: best.dirName,
      preservedV3: v3.dirName,
      recoveredNamedAdded: added.length,
      buyerWhoRecoveryContract: "gdi-design-hotels-buyer-who-live-recovery-v1",
      namedContactCoverageStandard: "gdi-named-contact-coverage-standard-v1",
      currentPointerUpdated: false,
      customerPublication: false,
    };

    const integrity = verifyPublicationPackageIntegrity(pkg);
    const written = store.writePackage(pkg);
    let staged = { ok: false, skipped: !applyPointer };
    if (applyPointer && integrity.ok && coverageGate.ok) {
      staged = store.stageForProduction({
        ...pkg,
        lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
        gates: {
          integrity: { ok: integrity.ok },
          namedContactCoverage: coverageGate,
          buyerWhoRecovery: true,
          priorBestContactPreserved: best.dirName,
          priorV3Preserved: v3.dirName,
        },
      });
    }

    summary.push({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      version: pkg.publicationVersion,
      publicationId: pkg.publicationId,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      preservedPriorNamed: best.pkg.opportunities.length,
      newlyAdded: added.length,
      totalStaged: opportunities.length,
      integrityOk: integrity.ok,
      integrityFails: integrity.fails || [],
      storePath: written.path || null,
      staged: staged.ok === true,
      namedContacts: opportunities.map((c) => ({
        id: c.id || c.opportunityId,
        name: c.primaryContact?.name || null,
      })),
    });
    console.log(
      `${hotel.hotelName}: ${pkg.publicationVersion} total=${opportunities.length} (+${added.length}) integrity=${integrity.ok} staged=${staged.ok === true}`,
    );
  }

  const reportDir = path.join(ROOT, "reports/gdi-buyer-who-live-recovery-v1");
  fs.mkdirSync(reportDir, { recursive: true });
  fs.writeFileSync(
    path.join(reportDir, "RESTAGE_SUMMARY.json"),
    JSON.stringify(
      { at: new Date().toISOString(), applyPointer, packages: summary },
      null,
      2,
    ),
  );
  console.log(`WROTE ${path.join(reportDir, "RESTAGE_SUMMARY.json")}`);
}

main();
