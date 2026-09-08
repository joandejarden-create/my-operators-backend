/**
 * Packet 2.8B-2 — Ownership surface: owner_get / owner_portfolio / related entities.
 * Read-only. No Census writes. No Webhound.
 */

import {
  ensureGoldenOwnerPortfoliosMaterialized,
  getOwnerPortfolioProfile,
  getOwnerControlGraph,
  resolveOwnerEntityId,
  resolveOwnerForHotel,
  listMaterializedOwnerIds,
  getOrganizationEvidenceCorpus,
  resolveOwnerAnchor,
  explainEntityAssociation,
  HOTEL_TO_OWNER,
} from "./owner-control/index.js";
import { buildHotelOwnershipReport } from "./golden-demo/gsf-cohort.js";

export const OWNERSHIP_SURFACE_VERSION = "ownership-surface-v1";

export const OWNERSHIP_TOOL_STATUS = Object.freeze({
  hotel_get_ownership: "COMPLETE",
  hotel_ownership_research: "PARTIAL",
  owner_get: "COMPLETE",
  owner_portfolio: "COMPLETE",
  owner_related_entities: "COMPLETE",
  owner_hotels: "COMPLETE",
  owner_people: "PARTIAL",
  owner_sources: "PARTIAL",
});

export const OWNERSHIP_WRITE_GUARANTEES = Object.freeze({
  owner_name_auto_writes: 0,
  census_owner_mutations: 0,
  webhound_calls: 0,
  airtable_ownership_writes: 0,
  external_spend_usd: 0,
});

function withGuarantees(payload) {
  return {
    ...payload,
    tool_status: OWNERSHIP_TOOL_STATUS,
    write_guarantees: { ...OWNERSHIP_WRITE_GUARANTEES },
    surface_version: OWNERSHIP_SURFACE_VERSION,
  };
}

export function createOwnershipSurface() {
  ensureGoldenOwnerPortfoliosMaterialized();

  function ownerGet(input = {}) {
    const ownerId = resolveOwnerEntityId(
      input.owner_id || input.entity_id || input.slug || input.ownerEntityId
    );
    if (!ownerId) {
      return withGuarantees({ ok: false, error: "owner_id_required" });
    }
    const profile = getOwnerPortfolioProfile(ownerId);
    if (!profile) {
      return withGuarantees({ ok: false, error: "owner_not_found", owner_entity_id: ownerId });
    }
    return withGuarantees({
      ok: true,
      owner: {
        entity_id: profile.owner_entity_id,
        display_name: profile.owner_display_name,
        legal_name: profile.owner_legal_name,
        completeness: profile.completeness,
        customer_label: profile.customer_label,
        markets: profile.markets,
        brands: profile.brands,
        metrics: profile.metrics,
        last_researched_at: profile.last_researched_at,
      },
      corpus: getOrganizationEvidenceCorpus(ownerId),
    });
  }

  function ownerPortfolio(input = {}) {
    const ownerId = resolveOwnerEntityId(
      input.owner_id || input.entity_id || input.slug || input.ownerEntityId
    );
    if (!ownerId) {
      return withGuarantees({ ok: false, error: "owner_id_required" });
    }
    const profile = getOwnerPortfolioProfile(ownerId);
    if (!profile) {
      return withGuarantees({ ok: false, error: "owner_portfolio_not_found", owner_entity_id: ownerId });
    }
    const focusHotel = String(input.focus_hotel_id || input.hotel_id || "").trim() || null;
    return withGuarantees({
      ok: true,
      status: "PARTIAL",
      semantics:
        "Organization-level known portfolio relationships with attribution paths. Not a claim of complete ownership census.",
      profile,
      focus_hotel_id: focusHotel,
      buckets: {
        owned_controlled: profile.buckets?.OWNED_CONTROLLED || [],
        jv_partial: profile.buckets?.JV_PARTIAL || [],
        sponsored: profile.buckets?.SPONSORED_ECONOMIC_INTEREST || [],
        operated_managed: profile.buckets?.OPERATED_MANAGED || [],
        historical: profile.buckets?.HISTORICAL_DISPOSED || [],
        pipeline: profile.buckets?.PIPELINE_ANNOUNCED || [],
      },
    });
  }

  function ownerRelatedEntities(input = {}) {
    const ownerId = resolveOwnerEntityId(input.owner_id || input.entity_id || input.slug);
    if (!ownerId) return withGuarantees({ ok: false, error: "owner_id_required" });
    const profile = getOwnerPortfolioProfile(ownerId);
    const graph = getOwnerControlGraph(ownerId);
    if (!profile) return withGuarantees({ ok: false, error: "owner_not_found" });
    return withGuarantees({
      ok: true,
      owner_entity_id: ownerId,
      related_entities: profile.related_entities || [],
      explain: input.entity_id && graph ? explainEntityAssociation(graph, input.entity_id) : null,
    });
  }

  function ownerHotels(input = {}) {
    const port = ownerPortfolio(input);
    if (!port.ok) return port;
    const bucket = String(input.bucket || "all").toUpperCase();
    let hotels = port.profile.hotel_relationships || [];
    if (bucket !== "ALL") {
      hotels = hotels.filter((h) => String(h.bucket || "").toUpperCase() === bucket);
    }
    return withGuarantees({
      ok: true,
      owner_entity_id: port.profile.owner_entity_id,
      hotels,
      count: hotels.length,
    });
  }

  function ownerPeople(input = {}) {
    const ownerId = resolveOwnerEntityId(input.owner_id || input.entity_id || input.slug);
    const profile = getOwnerPortfolioProfile(ownerId);
    if (!profile) return withGuarantees({ ok: false, error: "owner_not_found" });
    return withGuarantees({
      ok: true,
      owner_entity_id: ownerId,
      people: profile.people || [],
      status: "PARTIAL",
    });
  }

  function ownerSources(input = {}) {
    const ownerId = resolveOwnerEntityId(input.owner_id || input.entity_id || input.slug);
    const profile = getOwnerPortfolioProfile(ownerId);
    if (!profile) return withGuarantees({ ok: false, error: "owner_not_found" });
    return withGuarantees({
      ok: true,
      owner_entity_id: ownerId,
      sources: profile.sources || [],
      corpus: getOrganizationEvidenceCorpus(ownerId),
    });
  }

  function hotelOwnerAnchor(hotelId) {
    const report = buildHotelOwnershipReport(hotelId);
    if (!report?.ok && !report?.report) {
      // Still try static map
      const mapped = resolveOwnerForHotel(hotelId);
      if (mapped) {
        return withGuarantees({
          ok: true,
          hotel_id: hotelId,
          anchor: {
            ok: true,
            hotel_id: hotelId,
            primary_owner_entity_id: mapped.owner_entity_id,
            owner_display_name: mapped.owner_display_name,
            owner_role: "ECONOMIC_OWNER",
            confidence: "HIGH",
          },
          owner_explorer_path: `/owner-explorer.html?owner=${encodeURIComponent(mapped.owner_entity_id)}&focusHotel=${encodeURIComponent(hotelId)}`,
        });
      }
      return withGuarantees({ ok: false, error: "hotel_ownership_unavailable", hotel_id: hotelId });
    }
    const ownershipPayload = report.report ? report : { report, ...report };
    const anchor = resolveOwnerAnchor({
      airtable_record_id: hotelId,
      hotel_id: hotelId,
      ...ownershipPayload,
      ownership_and_control: ownershipPayload.report?.ownership_and_control,
      ownership_chain: ownershipPayload.report?.ownership_chain,
    });
    // Prefer golden owner map when present
    const mappedOwner = HOTEL_TO_OWNER[hotelId];
    if (mappedOwner) {
      anchor.ok = true;
      anchor.primary_owner_entity_id = mappedOwner;
      const profile = getOwnerPortfolioProfile(mappedOwner);
      if (profile) anchor.owner_display_name = profile.owner_display_name;
    }
    return withGuarantees({
      ok: Boolean(anchor.ok && anchor.primary_owner_entity_id),
      hotel_id: hotelId,
      anchor,
      owner_explorer_path: anchor.primary_owner_entity_id
        ? `/owner-explorer.html?owner=${encodeURIComponent(anchor.primary_owner_entity_id)}&focusHotel=${encodeURIComponent(hotelId)}`
        : null,
    });
  }

  function listOwners() {
    const ids = listMaterializedOwnerIds();
    return withGuarantees({
      ok: true,
      owners: ids.map((id) => {
        const p = getOwnerPortfolioProfile(id);
        return {
          owner_entity_id: id,
          display_name: p?.owner_display_name || id,
          completeness: p?.completeness,
          metrics: p?.metrics,
        };
      }),
    });
  }

  function meta() {
    return withGuarantees({
      ok: true,
      version: OWNERSHIP_SURFACE_VERSION,
      tool_status: { ...OWNERSHIP_TOOL_STATUS },
    });
  }

  return {
    ownerGet,
    ownerPortfolio,
    ownerRelatedEntities,
    ownerHotels,
    ownerPeople,
    ownerSources,
    hotelOwnerAnchor,
    listOwners,
    meta,
  };
}

export function getDefaultOwnershipSurface() {
  return createOwnershipSurface();
}
