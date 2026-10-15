/**
 * Corporate (scaling / LMC) package → canonical Airtable persist.
 *
 * Accepts in-memory packages from dispositions — no handwritten per-hotel
 * packages and no temporary research-packages.json swap required.
 */

import { loadHotelConfig } from "./hotel-segment-research-plan-v1.js";
import { buildScalingCompanyDemandGeneratorPackage } from "./scaling-companies-research-v1.js";
import { buildLocalMeetingsCateringDemandGeneratorPackage } from "./local-meetings-catering-research-v1.js";
import {
  upsertDemandGenerator,
  upsertDemandProgram,
  upsertHotelGeneratorFit,
} from "../demand-generators/airtable-stores.js";
import { isDgAirtableConfigured } from "../demand-generators/airtable-client.js";
import {
  upsertOpportunity,
  findOpportunityRecordById,
  airtableRecordToOpportunity,
} from "../airtable-opportunity-store.js";
import { invalidateGdiHotelReadCache } from "../read-cache.js";

export const CORPORATE_PACKAGE_PERSIST_VERSION =
  "gdi-corporate-package-persist-v1";

async function verifyOpp(opportunityId, hotelId) {
  invalidateGdiHotelReadCache(hotelId);
  const rec = await findOpportunityRecordById(opportunityId);
  if (!rec) return { ok: false, reason: "missing" };
  const raw = airtableRecordToOpportunity(rec);
  return {
    ok: Boolean(raw?.id || raw?.opportunityId),
    title: raw?.title || null,
    opportunityType: raw?.opportunityType || null,
    detailLayout: raw?.detailLayout || null,
    prospectHotelFacing: raw?.prospectHotelFacing ?? null,
    customerVisible: raw?.customerVisible ?? null,
    researchSegment: raw?.researchSegment ?? null,
    contactEmail: raw?.contactEmail || null,
    primaryContactName: raw?.primaryContact?.name || raw?.primaryContactName || null,
    functionalContactEmail: raw?.functionalContactEmail || null,
  };
}

/**
 * Persist one scaling or LMC package through DG + fit + opportunity writers.
 */
export async function persistCorporatePackage(pkg, kind, { dryRun = true } = {}) {
  const hotelId = pkg.hotelId;
  const hotelConfig = loadHotelConfig(hotelId) || { hotelId };
  const built =
    kind === "scaling" || kind === "scaling_companies"
      ? buildScalingCompanyDemandGeneratorPackage(pkg, { hotelId, hotelConfig })
      : buildLocalMeetingsCateringDemandGeneratorPackage(pkg, {
          hotelId,
          hotelConfig,
        });

  if (!built.ok) {
    return {
      ok: false,
      kind,
      organizationName: pkg.organizationName,
      hotelId,
      failReasons: built.qualification?.failReasons || [],
      warnings: built.qualification?.warnings || [],
      dryRun,
    };
  }

  const report = {
    ok: true,
    kind,
    organizationName: built.opportunity.organizationName,
    hotelId,
    opportunityId: built.opportunity.id,
    publish: built.opportunity.prospectHotelFacing === true,
    dryRun,
    steps: {},
  };

  if (dryRun) {
    report.steps.preview = {
      title: built.opportunity.title,
      contactEmail: built.opportunity.contactEmail,
      dgId: built.demandGenerator?.demandGeneratorId,
    };
    return report;
  }

  if (isDgAirtableConfigured()) {
    try {
      const dg = await upsertDemandGenerator(built.demandGenerator, {
        dryRun: false,
      });
      report.steps.demandGenerator = {
        action: dg.action,
        recordId: dg.recordId,
      };
      const prog = await upsertDemandProgram(built.program, {
        dryRun: false,
        generatorRecordId: dg.recordId,
      });
      report.steps.program = { action: prog.action, recordId: prog.recordId };
      const fit = await upsertHotelGeneratorFit(built.hotelGeneratorFit, {
        dryRun: false,
        generatorRecordId: dg.recordId,
      });
      report.steps.hotelGeneratorFit = {
        action: fit.action,
        recordId: fit.recordId,
      };
    } catch (err) {
      report.steps.demandGenerator = {
        error: err?.message || String(err),
      };
    }
  } else {
    report.steps.demandGenerator = { skipped: "dg_airtable_not_configured" };
  }

  const write = await upsertOpportunity(built.opportunity, {
    hotelId,
    runId: built.opportunity.researchVersion || CORPORATE_PACKAGE_PERSIST_VERSION,
  });
  invalidateGdiHotelReadCache(hotelId);
  try {
    const { appendHotelActivityEvent, ACTIVITY_KIND } = await import(
      "../hotel-activity-history-v1.js"
    );
    appendHotelActivityEvent(hotelId, {
      kind: ACTIVITY_KIND.CANONICAL_PUBLICATION,
      status: "completed",
      at: new Date().toISOString(),
      runId:
        built.opportunity.researchVersion || CORPORATE_PACKAGE_PERSIST_VERSION,
      source: "corporate-package-persist-v1",
      provenance: "live_write",
      note: built.opportunity.id || null,
    });
  } catch (actErr) {
    console.warn(
      "[corporate-persist] activity ledger:",
      actErr?.message || actErr
    );
  }
  report.steps.opportunity = {
    created: Boolean(write?.created),
    updated: Boolean(write?.updated || !write?.created),
    recordId: write?.recordId || write?.id || null,
  };
  report.verification = await verifyOpp(built.opportunity.id, hotelId);
  return report;
}

/**
 * Build packages from paid-MS disposition rows (auto — not handwritten).
 */
export function packagesFromDispositions(dispositions = []) {
  const publishable = dispositions.filter(
    (d) =>
      (d.disposition === "qualified_publishable" ||
        d.disposition === "contact_resolved") &&
      d.package
  );
  return {
    version: CORPORATE_PACKAGE_PERSIST_VERSION,
    generatedAt: new Date().toISOString(),
    scalingCompanies: publishable
      .filter((d) => d.segment === "scaling_companies")
      .map((d) => ({ ...d.package, _fromTaskId: d.taskId })),
    localMeetingsCatering: publishable
      .filter((d) => d.segment === "local_meetings_catering")
      .map((d) => ({ ...d.package, _fromTaskId: d.taskId })),
    privateEvents: publishable
      .filter((d) => d.segment === "weddings_social_private_events")
      .map((d) => ({ ...d.package, _fromTaskId: d.taskId })),
  };
}

/**
 * Persist a package document (scaling + LMC arrays) without file swap.
 */
export async function persistCorporatePackageDocument(
  doc = {},
  { dryRun = true, hotelIds = null } = {}
) {
  const filter = hotelIds ? new Set(hotelIds) : null;
  const scaling = (doc.scalingCompanies || []).filter(
    (p) => !filter || filter.has(p.hotelId)
  );
  const lmc = (doc.localMeetingsCatering || []).filter(
    (p) => !filter || filter.has(p.hotelId)
  );
  const results = [];
  for (const p of scaling) {
    results.push(await persistCorporatePackage(p, "scaling", { dryRun }));
  }
  for (const p of lmc) {
    results.push(
      await persistCorporatePackage(p, "local_meetings_catering", { dryRun })
    );
  }
  return {
    version: CORPORATE_PACKAGE_PERSIST_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    count: results.length,
    published: results.filter((r) => r.publish && r.ok).length,
    failed: results.filter((r) => !r.ok).length,
    results,
  };
}

/**
 * Apply buyer-contact patched opportunity through upsertOpportunity
 * (no direct field-repair patch script — uses same writer as other paths).
 */
export async function persistBuyerContactPatchedOpportunity(
  patchedOpp,
  { dryRun = true, hotelId = null } = {}
) {
  const hid = hotelId || patchedOpp.hotelId;
  if (!patchedOpp?.id && !patchedOpp?.opportunityId) {
    return { ok: false, reason: "missing_opportunity_id" };
  }
  const opportunityId = patchedOpp.id || patchedOpp.opportunityId;
  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      opportunityId,
      preview: {
        primaryContactName: patchedOpp.primaryContact?.name || null,
        primaryContactEmail: patchedOpp.primaryContactEmail || null,
        functionalContactEmail: patchedOpp.functionalContactEmail || null,
        namedContactResearchStatus: patchedOpp.namedContactResearchStatus,
        contactChannelSeparation: patchedOpp.contactChannelSeparation || null,
      },
    };
  }
  // Strip internal-only bags before Airtable; normalize display surface
  const { _internalContactProvenance, ...safeRaw } = patchedOpp;
  let safe = safeRaw;
  try {
    const { applyContactDisplaySurfaceToOpportunity, buildContactDisplaySurface } =
      await import("../contact-display-surface-v1.js");
    const surface = buildContactDisplaySurface(safeRaw);
    safe = applyContactDisplaySurfaceToOpportunity(safeRaw, surface);
    if (surface.companyPhone) {
      safe.companyPhone = surface.companyPhone;
      safe.companyPhoneLabel = surface.companyPhoneLabel;
    }
  } catch {
    safe = safeRaw;
  }
  const write = await upsertOpportunity(
    { ...safe, id: opportunityId },
    { hotelId: hid, runId: "gdi_buyer_contact_enrichment_v1" }
  );
  invalidateGdiHotelReadCache(hid);
  return {
    ok: true,
    dryRun: false,
    opportunityId,
    steps: {
      opportunity: {
        created: Boolean(write?.created),
        updated: Boolean(write?.updated || !write?.created),
        recordId: write?.recordId || write?.id || null,
      },
    },
    verification: await verifyOpp(opportunityId, hid),
  };
}
