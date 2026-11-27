/**
 * Persist permitted Findymail-verified professional emails onto existing
 * Group Demand Opportunities Airtable fields (no new tables/fields).
 *
 * Writes:
 *  - primaryContactEmail (typed)
 *  - opportunityPayloadJson.primaryContact.email (+ provenance)
 *
 * Surfe results must never call this module.
 */
import {
  isGdiOpportunityAirtableConfigured,
  upsertOpportunitiesBatch,
} from "../airtable-opportunity-store.js";

export const GDI_FINDYMAIL_AIRTABLE_CONTACT_PERSIST_V1 =
  "gdi-findymail-airtable-contact-persist-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Build an opportunity patch for Airtable upsert after Findymail email find.
 */
export function buildFindymailAirtableContactPatch(opportunity = {}, enrichment = {}) {
  const email = text(enrichment.person?.email || enrichment.channel?.value);
  if (!email || !email.includes("@")) {
    return { ok: false, reason: "NO_EMAIL" };
  }
  if (text(enrichment.channel?.source) === "SURFE") {
    return { ok: false, reason: "SURFE_PERSISTENCE_FORBIDDEN" };
  }
  const person = {
    ...(opportunity.primaryContact || {}),
    ...(enrichment.person || {}),
    email,
    emailOwnership: "person_specific",
    emailVerificationStatus:
      enrichment.person?.emailVerificationStatus || "FINDYMAIL_PROVIDER_RETURNED",
    emailCustomerVerified: false,
  };
  return {
    ok: true,
    opportunity: {
      ...opportunity,
      primaryContact: person,
      primaryContactEmail: email,
      contactResearchStage: {
        ...(opportunity.contactResearchStage || {}),
        findymailUsed: true,
        findymailPersistedAt: new Date().toISOString(),
        findymailPermanentComponent: true,
      },
    },
  };
}

/**
 * Dry-run or apply Airtable persistence for Findymail email results.
 */
export async function persistFindymailContactsToAirtable(
  patches = [],
  opts = {},
) {
  const rows = [];
  for (const p of patches) {
    const built = buildFindymailAirtableContactPatch(p.opportunity, p.enrichment);
    if (!built.ok) {
      rows.push({ opportunityId: p.opportunity?.id, skipped: true, reason: built.reason });
      continue;
    }
    rows.push({
      opportunityId: built.opportunity.id || built.opportunity.opportunityId,
      skipped: false,
      email: built.opportunity.primaryContact?.email,
      opportunity: built.opportunity,
    });
  }

  if (opts.dryRun !== false && opts.apply !== true) {
    return {
      version: GDI_FINDYMAIL_AIRTABLE_CONTACT_PERSIST_V1,
      dryRun: true,
      configured: isGdiOpportunityAirtableConfigured(),
      wouldWrite: rows.filter((r) => !r.skipped).length,
      rows,
    };
  }

  if (!isGdiOpportunityAirtableConfigured()) {
    const err = new Error("GDI Airtable opportunities not configured");
    err.code = "GDI_AIRTABLE_NOT_CONFIGURED";
    throw err;
  }

  const toWrite = rows.filter((r) => !r.skipped).map((r) => r.opportunity);
  const hotelId = text(opts.hotelId || toWrite[0]?.hotelId);
  if (!hotelId) {
    const err = new Error("hotelId required for Findymail Airtable persist");
    err.code = "MISSING_HOTEL_ID";
    throw err;
  }
  const result = await upsertOpportunitiesBatch(hotelId, toWrite, {
    ...(opts.meta || {}),
    researchVersion: "findymail-stage4-persist-v1",
    runId: opts.runId || `findymail_${Date.now()}`,
  });
  return {
    version: GDI_FINDYMAIL_AIRTABLE_CONTACT_PERSIST_V1,
    dryRun: false,
    written: toWrite.length,
    result,
    rows,
  };
}
