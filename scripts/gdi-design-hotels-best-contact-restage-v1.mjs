#!/usr/bin/env node
/**
 * Build corrected Design Hotels STAGED *candidate* packages that restore
 * best-suggested-contact finalization (named person required by default).
 *
 * - Preserves historical v3 STAGED package directories on disk
 * - Writes a new publicationVersion
 * - Optionally updates STAGED pointer with --apply-staged-pointer
 * - $0 — no paid research / Airtable / CURRENT mutation
 *
 * Usage (from a tree that has the v3 packages + hotel inventories):
 *   node scripts/gdi-design-hotels-best-contact-restage-v1.mjs --root <serving-tree>
 *   node scripts/gdi-design-hotels-best-contact-restage-v1.mjs --root <serving-tree> --apply-staged-pointer
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  runPublicationQualityGate,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import {
  evaluateVerifiedCommercialRoute,
  evaluateContactCompletenessForFinalization,
} from "../lib/group-demand-intelligence/presentation/gdi-verified-commercial-route-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_ROOT = path.resolve(__dirname, "..");

function arg(flag, fallback = null) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] || fallback : fallback;
}
function has(flag) {
  return process.argv.includes(flag);
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

const HOTELS = [
  {
    hotelId: "rece0NdV9FCnaFlVS",
    hotelName: "Condesa DF",
    priorVersion: "gdi_dh_v3_staged_rece0NdV9FCnaFlVS_20261124",
  },
  {
    hotelId: "recaokE8psIbuVeuH",
    hotelName: "LaLuna Boutique Hotels & Villas",
    priorVersion: "gdi_dh_v3_staged_recaokE8psIbuVeuH_20261124",
  },
  {
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
    priorVersion: "gdi_dh_v3_staged_rec1t35e3sPerzxYu_20261124",
  },
];

function plausiblePerson(name) {
  const n = text(name);
  if (!n || !looksLikePersonName(n)) return false;
  if (
    /organization contact desk|member\s*(select|log)|website feedback|pennsylvania|place brand|belgium|survey software|amsterdam|netherlands|mexico city|george grenada|conference|congress|exhibition|software|committee|buyer not yet|contact research/i.test(
      n,
    )
  ) {
    return false;
  }
  return true;
}

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function loadInventory(root, hotelId) {
  const p = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) return [];
  return loadJson(p).opportunities || [];
}

function loadPriorPackage(root, hotel) {
  const p = path.join(
    root,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotel.hotelId,
    hotel.priorVersion,
    "package.json",
  );
  return loadJson(p);
}

function enrichFromInventory(stagedOpp, inv) {
  if (!inv) return stagedOpp;
  const next = { ...stagedOpp };
  if (
    plausiblePerson(inv.primaryContact?.name) &&
    !plausiblePerson(next.primaryContact?.name)
  ) {
    next.primaryContact = {
      ...inv.primaryContact,
      customerFacingNamed: true,
    };
  }
  if (
    Array.isArray(inv.namedStakeholders) &&
    inv.namedStakeholders.some((n) => plausiblePerson(n?.name)) &&
    !(next.namedStakeholders || []).some((n) => plausiblePerson(n?.name))
  ) {
    next.namedStakeholders = inv.namedStakeholders;
  }
  if (inv.hotelOpportunityThesis && !text(next.hotelOpportunityThesis)) {
    next.hotelOpportunityThesis = inv.hotelOpportunityThesis;
  }
  if (inv.recommendedNextStep && text(next.recommendedNextStep).length < 20) {
    next.recommendedNextStep = inv.recommendedNextStep;
  }
  // Strip generic org-route completion stamps from restored candidate
  const routeEval = evaluateVerifiedCommercialRoute({
    label: inv.approvedCommercialRoute || next.approvedCommercialRoute,
    url: inv.commercialContactRoute || next.commercialContactRoute,
  });
  if (!routeEval.ok) {
    delete next.approvedCommercialRoute;
    delete next.commercialContactRoute;
    delete next.commercialRouteMeta;
  } else {
    next.approvedCommercialRoute =
      inv.approvedCommercialRoute || next.approvedCommercialRoute;
    next.commercialContactRoute =
      inv.commercialContactRoute || next.commercialContactRoute;
  }
  return next;
}

function ensureBestContactAction(opp) {
  const name = text(opp.primaryContact?.name);
  const role = text(
    opp.primaryContact?.role ||
      opp.primaryContact?.title ||
      opp.primaryContact?.professionalTitle,
  );
  const org = text(opp.organizationName || opp.primaryContact?.organization);
  if (plausiblePerson(name) && text(opp.recommendedNextStep).length < 20) {
    opp.recommendedNextStep = `Outreach to ${name}${
      role ? ` (${role})` : ""
    }${org ? ` at ${org}` : ""} — confirm lodging / meeting decision role and propose property-fit options for this demand.`;
  }
  return opp;
}

async function main() {
  const root = path.resolve(arg("--root", DEFAULT_ROOT));
  const allowValidRoute = has("--allow-verified-org-route");
  const applyPointer = has("--apply-staged-pointer");
  const day = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  const outReport = path.join(
    root,
    "reports/gdi-best-suggested-contact-restore-v1",
  );
  fs.mkdirSync(outReport, { recursive: true });

  // Prefer modules from this script's tree; data/packages from --root.
  const store = createGdiPublicationPackageStore({
    root: path.join(
      root,
      "lib/group-demand-intelligence/bundled-publication-packages",
    ),
  });

  const hotels = [];
  for (const hotel of HOTELS) {
    const prior = loadPriorPackage(root, hotel);
    const inventory = loadInventory(root, hotel.hotelId);
    const invById = new Map(
      inventory.map((o) => [text(o.opportunityId || o.id), o]),
    );

    const preservedPriorPath = path.join(
      root,
      "lib/group-demand-intelligence/bundled-publication-packages",
      hotel.hotelId,
      hotel.priorVersion,
      "package.json",
    );
    if (!fs.existsSync(preservedPriorPath)) {
      throw new Error(`HISTORICAL_STAGED_MISSING:${preservedPriorPath}`);
    }

    const candidates = [];
    const excluded = [];
    for (const stagedOpp of prior.opportunities || []) {
      const id = text(stagedOpp.id || stagedOpp.opportunityId);
      let opp = enrichFromInventory(stagedOpp, invById.get(id));
      // Rebuild a research-shaped object for gate evaluation
      const gateInput = {
        ...opp,
        primaryContact: opp.primaryContact || invById.get(id)?.primaryContact,
        organizationName:
          opp.organizationName || invById.get(id)?.organizationName,
        approvedCommercialRoute:
          opp.approvedCommercialRoute ||
          invById.get(id)?.approvedCommercialRoute,
        commercialContactRoute:
          opp.commercialContactRoute ||
          invById.get(id)?.commercialContactRoute,
      };
      const completeness = evaluateContactCompletenessForFinalization(
        gateInput,
        { looksLikePersonName: plausiblePerson },
      );
      const namedOk = completeness.mode === "NAMED_PERSON";
      const routeOk =
        allowValidRoute && completeness.mode === "VERIFIED_ORG_ROUTE";
      if (!namedOk && !routeOk) {
        excluded.push({
          opportunityId: id,
          title: text(opp.title),
          reason: completeness.mode,
          routeCode: completeness.route?.code || null,
        });
        continue;
      }
      opp = ensureBestContactAction({
        ...opp,
        primaryContact: gateInput.primaryContact,
        salesReady: false,
        customerPublicationReady: false,
        bestSuggestedContactRestored: true,
        contactCompletenessMode: completeness.mode,
      });
      const projected = projectOpportunityForPresentation(opp, {
        hotelId: hotel.hotelId,
        surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
        view: "detail",
      });
      candidates.push(projected.dto);
    }

    const publicationVersion = `gdi_dh_best_contact_restore_${hotel.hotelId}_${day}`;
    if (!candidates.length) {
      hotels.push({
        hotelId: hotel.hotelId,
        hotelName: hotel.hotelName,
        priorVersion: hotel.priorVersion,
        priorCount: (prior.opportunities || []).length,
        finalizedCount: 0,
        excludedCount: excluded.length,
        excludedSample: excluded.slice(0, 25),
        staged: false,
        note: "No named-person (or verified-route) rows passed restored gate",
      });
      continue;
    }

    const pkg = buildCustomerPublicationPackage({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      opportunities: candidates,
      publicationVersion,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
      approvalStatus: "CANDIDATE",
      applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
      notes:
        "Best-suggested-contact restore V1 — named person required (optional verified org route with --allow-verified-org-route). Historical v3 STAGED package preserved.",
    });
    const integrity = verifyPublicationPackageIntegrity(pkg);
    const quality = runPublicationQualityGate({
      opportunities: pkg.opportunities,
    });
    const written = store.writePackage(pkg);
    let staged = { ok: false, skipped: !applyPointer };
    if (applyPointer && integrity.ok) {
      staged = store.stageForProduction({
        ...pkg,
        lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
        gates: {
          integrity: { ok: integrity.ok },
          quality: { ok: quality.ok, blockingCount: quality.blockingCount },
          bestSuggestedContactRestore: true,
          priorVersionPreserved: hotel.priorVersion,
        },
      });
    }

    hotels.push({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      priorVersion: hotel.priorVersion,
      priorCount: (prior.opportunities || []).length,
      priorPackagePreserved: true,
      priorPackagePath: preservedPriorPath,
      finalizedCount: candidates.length,
      excludedCount: excluded.length,
      excludedSample: excluded.slice(0, 25),
      publicationVersion: pkg.publicationVersion,
      publicationId: pkg.publicationId,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      integrityOk: integrity.ok,
      qualityOk: quality.ok,
      storePath: written.path || null,
      staged: staged.ok === true,
      namedContacts: candidates.map((c) => ({
        id: c.id || c.opportunityId,
        name: c.primaryContact?.name || c.contactName || null,
        title: c.title,
      })),
    });
  }

  const summary = {
    contract: "gdi-design-hotels-best-contact-restage-v1",
    at: new Date().toISOString(),
    root,
    allowValidRoute,
    applyPointer,
    hotels,
    totals: {
      priorStagedPreserved: hotels.reduce((s, h) => s + (h.priorCount || 0), 0),
      finalizedNamedRestore: hotels.reduce(
        (s, h) => s + (h.finalizedCount || 0),
        0,
      ),
      excluded: hotels.reduce((s, h) => s + (h.excludedCount || 0), 0),
      stagedHotels: hotels.filter((h) => h.staged).length,
    },
    safety: {
      paidResearch: false,
      airtable: false,
      currentPublication: false,
      historicalV3PackagesPreserved: true,
    },
  };
  fs.writeFileSync(
    path.join(outReport, "RESTAGE_SUMMARY.json"),
    `${JSON.stringify(summary, null, 2)}\n`,
  );
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
