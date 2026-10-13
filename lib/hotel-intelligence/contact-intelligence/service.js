/**
 * Contact Intelligence V1 — canonical service (Census + Hotel/Owner Explorer).
 * Read-mostly. Preserves ownership write protections. Paid enrichment off by default.
 */

import { getDefaultOwnershipSurface } from "../ownership/ownership-surface-v1.js";
import {
  CONTACT_INTELLIGENCE_VERSION,
  CONTACT_WRITE_GUARANTEES,
  UNRESOLVED_REASON,
  REFRESH_STATUS,
} from "./vocabulary.js";
import { createContactStore } from "./store.js";
import { publishContactPackage } from "./publication-rules.js";
import { isPaidEnrichmentEnabled, assertPaidEnrichmentAllowed } from "./policy.js";
import { runNativeContactResearch } from "./native-research.js";
import { applyOwnerRouteReuse, buildOrganizationContactRoute } from "./owner-reuse.js";
import { createHotelContactPackage } from "./contact-record.js";
import {
  CONTACT_SLICE_TEN_VERSION,
  CONTACT_SLICE_TEN_HOTELS,
  buildSliceTenSeedPackage,
  listSliceTenPackages,
  getOwnerRouteForSlice,
} from "./slice-ten.js";

function withGuarantees(payload) {
  return {
    ...payload,
    surface_version: CONTACT_INTELLIGENCE_VERSION,
    write_guarantees: { ...CONTACT_WRITE_GUARANTEES },
    paid_enrichment_enabled: isPaidEnrichmentEnabled(),
  };
}

export function createContactIntelligenceService(opts = {}) {
  const store = opts.store || createContactStore(opts);
  const ownership = opts.ownershipSurface || getDefaultOwnershipSurface();
  const audienceDefault = opts.audience || "public";

  function meta() {
    return withGuarantees({
      ok: true,
      version: CONTACT_INTELLIGENCE_VERSION,
      slice_ten_version: CONTACT_SLICE_TEN_VERSION,
      slice_ten_count: CONTACT_SLICE_TEN_HOTELS.length,
      dimensions: [
        "attribution",
        "deliverability",
        "role_currency",
        "property_relevance",
        "freshness",
        "usage_rights",
      ],
      unresolved_reason_codes: Object.values(UNRESOLVED_REASON),
    });
  }

  function resolveSeed(hotelId) {
    const stored = store.getHotelPackage(hotelId);
    if (stored) return stored;
    return buildSliceTenSeedPackage(hotelId);
  }

  /**
   * Portfolio reuse only when stored/owner route matches CURRENT ownership anchor.
   * Ownership change removes previous owner's current hotel path.
   * If ownership surface is unresolved, keep evidenced/seeded owner (do not invent; do not wipe).
   */
  function applyCurrentOwnerReuse(pkg, anchor) {
    const currentOwnerId = anchor?.ok ? anchor?.anchor?.primary_owner_entity_id || null : null;

    if (!currentOwnerId) {
      if (pkg.owner_entity_id) {
        return {
          ...pkg,
          portfolio_reuse: pkg.portfolio_reuse || {
            reused: false,
            reason: "ownership_surface_unresolved_seed_owner_retained",
          },
        };
      }
      return {
        ...pkg,
        owner_entity_id: null,
        owner_display_name: null,
        organization_contact_route: { channels: [] },
        portfolio_reuse: { reused: false, reason: "owner_unresolved" },
        unresolved_reasons: [
          ...new Set([...(pkg.unresolved_reasons || []), UNRESOLVED_REASON.OWNER_UNRESOLVED]),
        ],
      };
    }

    if (pkg.owner_entity_id && pkg.owner_entity_id !== currentOwnerId) {
      pkg = {
        ...pkg,
        owner_entity_id: currentOwnerId,
        owner_display_name: anchor.anchor.owner_display_name || null,
        organization_contact_route: { channels: [] },
        people: (pkg.people || []).filter(
          (p) => !p.organization_entity_id || p.organization_entity_id === currentOwnerId
        ),
        portfolio_reuse: {
          reused: false,
          reason: "ownership_changed_previous_owner_path_removed",
          previous_owner_entity_id: pkg.owner_entity_id,
          current_owner_entity_id: currentOwnerId,
        },
      };
    }

    const ownerRoute =
      store.getOwnerRoute(currentOwnerId) ||
      getOwnerRouteForSlice(currentOwnerId) ||
      buildOrganizationContactRoute({
        owner_entity_id: currentOwnerId,
        owner_display_name:
          pkg.owner_display_name || anchor?.anchor?.owner_display_name || null,
        channels: pkg.organization_contact_route?.channels || [],
      });

    if (ownerRoute?.channels?.length) {
      store.putOwnerRoute(ownerRoute);
      pkg = applyOwnerRouteReuse(
        {
          ...pkg,
          owner_entity_id: currentOwnerId,
          owner_display_name: ownerRoute.owner_display_name || pkg.owner_display_name,
        },
        ownerRoute
      );
    } else {
      pkg = {
        ...pkg,
        owner_entity_id: currentOwnerId,
        owner_display_name: anchor.anchor.owner_display_name || pkg.owner_display_name,
      };
    }
    return pkg;
  }

  function filterPeopleForPublication(people = []) {
    return (people || []).filter((p) => {
      if (p.former_affiliation === true) return false;
      if (String(p.affiliation_status || "").toUpperCase() === "FORMER") return false;
      return true;
    });
  }

  function hotelGet(input = {}) {
    const hotelId = String(input.hotel_id || input.hotelId || "").trim();
    if (!hotelId) return withGuarantees({ ok: false, error: "hotel_id_required" });

    const audience = input.audience || audienceDefault;
    const seed = resolveSeed(hotelId);
    const anchor = ownership.hotelOwnerAnchor(hotelId);

    let pkg =
      seed ||
      createHotelContactPackage({
        hotel_id: hotelId,
        unresolved_reasons: [UNRESOLVED_REASON.NO_HOTEL_CONTACT, UNRESOLVED_REASON.OWNER_UNRESOLVED],
        refresh_status: REFRESH_STATUS.NEVER,
      });

    pkg = applyCurrentOwnerReuse(pkg, anchor);
    pkg = {
      ...pkg,
      people: filterPeopleForPublication(pkg.people),
    };

    if (!pkg.organization_contact_route?.channels?.length) {
      if (!pkg.unresolved_reasons.includes(UNRESOLVED_REASON.NO_ORG_ROUTE)) {
        pkg = {
          ...pkg,
          unresolved_reasons: [...pkg.unresolved_reasons, UNRESOLVED_REASON.NO_ORG_ROUTE],
        };
      }
    }

    const published = publishContactPackage(pkg, { audience });

    return withGuarantees({
      ok: true,
      hotel_id: hotelId,
      package: published,
      owner_anchor: anchor?.ok
        ? {
            owner_entity_id: anchor.anchor.primary_owner_entity_id,
            owner_display_name: anchor.anchor.owner_display_name,
            owner_explorer_path: anchor.owner_explorer_path,
          }
        : null,
      unresolved_reasons: published.unresolved_reasons || [],
      refresh_status: published.refresh_status,
    });
  }

  function ownerGet(input = {}) {
    const ownerId = String(input.owner_id || input.ownerId || "").trim();
    if (!ownerId) return withGuarantees({ ok: false, error: "owner_id_required" });
    const audience = input.audience || audienceDefault;

    const route =
      store.getOwnerRoute(ownerId) ||
      getOwnerRouteForSlice(ownerId) ||
      null;

    const peopleResult = ownership.ownerPeople({ owner_id: ownerId });
    const hotels = ownership.ownerHotels({ owner_id: ownerId });

    const hotelIds = CONTACT_SLICE_TEN_HOTELS.filter((h) => h.owner_entity_id === ownerId).map(
      (h) => h.hotel_id
    );

    const pkg = publishContactPackage(
      createHotelContactPackage({
        hotel_id: null,
        owner_entity_id: ownerId,
        owner_display_name:
          route?.owner_display_name || peopleResult?.owner_entity_id || ownerId,
        organization_contact_route: route || { channels: [] },
        people: (peopleResult?.people || []).slice(0, 8),
        channels: route?.channels || [],
        unresolved_reasons: route?.channels?.length ? [] : [UNRESOLVED_REASON.NO_ORG_ROUTE],
        portfolio_reuse: {
          reusable: true,
          hotel_ids_in_slice: hotelIds,
          hotel_relationship_count: hotels?.count ?? null,
        },
        refresh_status: route ? REFRESH_STATUS.COMPLETE : REFRESH_STATUS.NEVER,
      }),
      { audience }
    );

    return withGuarantees({
      ok: true,
      owner_entity_id: ownerId,
      package: pkg,
      slice_hotel_ids: hotelIds,
    });
  }

  function sliceTen(input = {}) {
    const audience = input.audience || audienceDefault;
    const rows = listSliceTenPackages().map((seed) => {
      const resolved = hotelGet({ hotel_id: seed.hotel_id, audience });
      return {
        hotel_id: seed.hotel_id,
        hotel_name: seed.hotel_name,
        owner_entity_id: seed.owner_entity_id,
        owner_display_name: seed.owner_display_name,
        refresh_status: resolved.package?.refresh_status,
        unresolved_reasons: resolved.unresolved_reasons || [],
        hotel_channel_count: resolved.package?.hotel_contact?.channels?.length || 0,
        org_channel_count: resolved.package?.organization_contact_route?.channels?.length || 0,
        person_count: resolved.package?.people?.length || 0,
        portfolio_reuse: resolved.package?.portfolio_reuse || null,
        ok: resolved.ok,
      };
    });
    return withGuarantees({
      ok: true,
      slice_version: CONTACT_SLICE_TEN_VERSION,
      count: rows.length,
      hotels: rows,
    });
  }

  function refreshHotel(input = {}) {
    const hotelId = String(input.hotel_id || "").trim();
    if (!hotelId) return withGuarantees({ ok: false, error: "hotel_id_required" });

    const wantPaid = input.paid === true || input.provider_strategy === "PAID";
    if (wantPaid) {
      try {
        assertPaidEnrichmentAllowed(opts.env || process.env);
      } catch (err) {
        return withGuarantees({
          ok: false,
          error: err.code || "paid_enrichment_disabled",
          customer_safe: err.customer_safe,
          unresolved_reasons: [UNRESOLVED_REASON.PAID_ENRICHMENT_DISABLED],
        });
      }
    }

    const enq = store.enqueueJob({
      hotel_id: hotelId,
      template_id: "CONTACT_REFRESH",
      provider_strategy: wantPaid ? "PAID" : "NATIVE",
      paid: wantPaid,
      idempotency_token: input.idempotency_token || `contact_refresh:${hotelId}:native`,
    });

    if (enq.deduped) {
      const existingPkg = store.getHotelPackage(hotelId);
      return withGuarantees({
        ok: true,
        deduped: true,
        dedupe_reason: enq.reason,
        job: enq.job,
        package: existingPkg
          ? publishContactPackage(existingPkg, { audience: input.audience || audienceDefault })
          : null,
        refresh_status: enq.job.status === "RUNNING" ? REFRESH_STATUS.RUNNING : existingPkg?.refresh_status || REFRESH_STATUS.COMPLETE,
        unresolved_reasons: existingPkg?.unresolved_reasons || [],
      });
    }

    store.updateJob(enq.job.job_id, { status: "RUNNING", started_at: new Date().toISOString() });

    const seed = resolveSeed(hotelId);
    const anchor = ownership.hotelOwnerAnchor(hotelId);
    const people = ownership.ownerPeople({
      owner_id: anchor?.anchor?.primary_owner_entity_id,
    });

    const native = runNativeContactResearch({
      hotel_id: hotelId,
      seedPackage: seed,
      ownershipAnchor: anchor,
      ownerPeople: people?.people || [],
      hotelPhone: seed?.hotel_contact?.channels?.find((c) => c.kind === "HOTEL_PHONE")?.value,
      hotelWebsite: seed?.hotel_contact?.channels?.find((c) => c.kind === "HOTEL_WEBSITE")?.value,
      env: opts.env || process.env,
    });

    const priorVerifiedAt = seed?.last_successfully_verified_at || null;

    if (!native.ok) {
      store.updateJob(enq.job.job_id, {
        status: "FAILED",
        completed_at: new Date().toISOString(),
        result_summary: {
          refresh_status: REFRESH_STATUS.FAILED,
          unresolved_reasons: native.unresolved_reasons,
          elapsed_ms: native.elapsed_ms,
          paid_used: false,
        },
        error: native.error || "native_failed",
      });
      // Failed refresh must NOT advance successful verification dates.
      const failedPkg = {
        ...(seed || {}),
        hotel_id: hotelId,
        refresh_status: REFRESH_STATUS.FAILED,
        last_refresh_attempt_at: new Date().toISOString(),
        last_successfully_verified_at: priorVerifiedAt,
        last_refreshed_at: seed?.last_refreshed_at || null,
        unresolved_reasons: [
          ...new Set([...(seed?.unresolved_reasons || []), ...(native.unresolved_reasons || [])]),
        ],
        provenance: {
          ...(seed?.provenance || {}),
          last_failed_refresh: native.provenance || { summary: "refresh_failed" },
        },
      };
      store.putHotelPackage(failedPkg);
      return withGuarantees({
        ok: false,
        error: native.error || "native_failed",
        package: publishContactPackage(failedPkg, { audience: input.audience || audienceDefault }),
        refresh_status: REFRESH_STATUS.FAILED,
        unresolved_reasons: failedPkg.unresolved_reasons,
        last_successfully_verified_at: priorVerifiedAt,
      });
    }

    let pkg = createHotelContactPackage({
      hotel_id: hotelId,
      hotel_name: seed?.hotel_name || null,
      owner_entity_id:
        seed?.owner_entity_id || anchor?.anchor?.primary_owner_entity_id || null,
      owner_display_name:
        seed?.owner_display_name || anchor?.anchor?.owner_display_name || null,
      hotel_contact: native.hotel_contact,
      organization_contact_route: native.organization_contact_route,
      people: filterPeopleForPublication(native.people),
      channels: native.channels,
      refresh_status: native.refresh_status,
      last_refreshed_at: new Date().toISOString(),
      unresolved_reasons: native.unresolved_reasons,
      provenance: native.provenance,
      verification_history: seed?.verification_history || [],
    });
    pkg.last_successfully_verified_at = new Date().toISOString();
    pkg.last_refresh_attempt_at = pkg.last_refreshed_at;

    const ownerId = pkg.owner_entity_id;
    if (ownerId) {
      const route =
        store.getOwnerRoute(ownerId) ||
        getOwnerRouteForSlice(ownerId) ||
        native.organization_contact_route;
      if (route?.channels?.length) {
        const fullRoute = buildOrganizationContactRoute({
          owner_entity_id: ownerId,
          owner_display_name: pkg.owner_display_name,
          channels: route.channels,
        });
        store.putOwnerRoute(fullRoute);
        pkg = applyOwnerRouteReuse(pkg, fullRoute);
      }
    }

    const saved = store.putHotelPackage(pkg);
    store.updateJob(enq.job.job_id, {
      status: "COMPLETE",
      completed_at: new Date().toISOString(),
      result_summary: {
        refresh_status: native.refresh_status,
        unresolved_reasons: native.unresolved_reasons,
        elapsed_ms: native.elapsed_ms,
        paid_used: false,
      },
      error: null,
    });

    const published = publishContactPackage(saved, {
      audience: input.audience || audienceDefault,
    });

    return withGuarantees({
      ok: true,
      deduped: enq.deduped,
      job: store.listJobs({ hotel_id: hotelId }).find((j) => j.job_id === enq.job.job_id),
      package: published,
      refresh_status: published.refresh_status,
      unresolved_reasons: published.unresolved_reasons,
    });
  }

  /**
   * Ingest a live discovery result into the canonical store (single write path).
   */
  function ingestLiveDiscovery(result, { audience = audienceDefault } = {}) {
    if (!result?.hotel_id) throw new Error("hotel_id_required");
    const existing = store.getHotelPackage(result.hotel_id);
    const pkg = createHotelContactPackage({
      hotel_id: result.hotel_id,
      hotel_name: result.hotel_name,
      owner_entity_id: result.owner_entity_id,
      owner_display_name: result.owner_display_name,
      hotel_contact: result.hotel_contact,
      organization_contact_route: result.organization_contact_route,
      people: filterPeopleForPublication(result.people || []),
      channels: [
        ...(result.hotel_contact?.channels || []),
        ...(result.organization_contact_route?.channels || []),
      ],
      refresh_status: result.ok ? REFRESH_STATUS.COMPLETE : REFRESH_STATUS.PARTIAL,
      last_refreshed_at: result.ok ? new Date().toISOString() : existing?.last_refreshed_at || null,
      unresolved_reasons: result.unresolved_reasons || [],
      provenance: {
        method: "live_native_discovery_v1.1",
        summary: "Live native discovery ingest",
        cost: result.cost,
        sources: result.sources,
        inferred_email_candidates: result.inferred_email_candidates || [],
        flags: result.flags,
        blockers: result.blockers,
      },
      portfolio_reuse: result.portfolio_reuse || null,
      verification_history: existing?.verification_history || [],
    });
    pkg.last_successfully_verified_at = result.ok
      ? new Date().toISOString()
      : existing?.last_successfully_verified_at || null;
    pkg.last_refresh_attempt_at = new Date().toISOString();
    pkg.inferred_email_candidates = result.inferred_email_candidates || [];
    pkg.organization_website_only = result.organization_website_only || [];
    pkg.contact_source_date = result.contact_source_date || null;
    pkg.role_check_date = result.role_check_date || null;

    if (result.owner_entity_id && result.organization_contact_route?.channels?.length) {
      store.putOwnerRoute(
        buildOrganizationContactRoute({
          owner_entity_id: result.owner_entity_id,
          owner_display_name: result.owner_display_name,
          channels: result.organization_contact_route.channels,
        })
      );
    }

    const saved = store.putHotelPackage(pkg);
    return withGuarantees({
      ok: true,
      package: publishContactPackage(saved, { audience }),
    });
  }

  return {
    meta,
    hotelGet,
    ownerGet,
    sliceTen,
    refreshHotel,
    ingestLiveDiscovery,
    store,
  };
}

let _default = null;
export function getDefaultContactIntelligenceService(opts = {}) {
  if (!_default || opts.forceNew) {
    _default = createContactIntelligenceService(opts);
  }
  return _default;
}
