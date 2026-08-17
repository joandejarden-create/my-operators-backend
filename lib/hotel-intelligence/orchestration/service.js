/**
 * Hotel Intelligence orchestration — MCP tools call into this layer.
 * Default: no Airtable writes. Stage-only ingest.
 */

import { createLocalStore } from "../local-store.js";
import { createExternalIdRegistry } from "../external-ids.js";
import { createEvidenceStore } from "../evidence-store.js";
import { createReviewQueue, enqueueFromResolveResult } from "../review-queue.js";
import { createBatchJobStore, BATCH_STATUS } from "../batch-jobs.js";
import { createProviderRegistry } from "../providers/registry.js";
import { resolveHotelIdentity, MATCH_STATUS } from "../identity-resolve.js";
import { findNearbyHotels } from "../nearby.js";
import { preferCanonicalValue, scoreFieldConfidence } from "../confidence.js";
import { toMvpHotelSummary, createEmptyCanonicalHotel } from "../canonical-hotel.js";
import { censusRecordToCanonical } from "../providers/census-read.js";
import { MAP_CENSUS_FIELDS, MAP_PROVIDER_IDS } from "../map_hotel_intelligence_fields.js";
import { researchHotelRoomCount } from "../room-count-research/index.js";
import { ISSUE_TYPES } from "../review-queue.js";

export const SERVICE_VERSION = "hotel-intelligence-service-v1";

function airtableWritesEnabled(env = process.env) {
  return String(env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES || "0").trim() === "1";
}

function candidateToEvidenceFields(candidate) {
  const pairs = [
    ["official_name", candidate.name],
    ["address_line_1", candidate.address],
    ["city", candidate.city],
    ["country", candidate.country],
    ["latitude", candidate.latitude],
    ["longitude", candidate.longitude],
    ["room_count", candidate.room_count],
    ["brand_name", candidate.brand_name],
    ["parent_company_name", candidate.parent_company_name],
    ["website", candidate.website],
    ["phone", candidate.phone],
  ];
  return pairs.filter(([, v]) => v != null && v !== "");
}

/**
 * @param {object} [opts]
 */
export function createHotelIntelligenceService(opts = {}) {
  const store = opts.store || createLocalStore(opts);
  const idRegistry = opts.idRegistry || createExternalIdRegistry(store);
  const evidence = opts.evidence || createEvidenceStore(store);
  const reviewQueue = opts.reviewQueue || createReviewQueue(store);
  const batches = opts.batches || createBatchJobStore(store);
  const providers =
    opts.providers ||
    createProviderRegistry({
      ...opts,
      store,
      idRegistry,
      records: opts.censusRecords,
    });

  async function loadCensusRecords() {
    if (Array.isArray(opts.censusRecords)) return opts.censusRecords;
    return providers.census.loadRecords();
  }

  async function hotelSearch(input = {}) {
    const providerFailures = [];
    const hotels = [];

    const censusResult = await providers.census.searchHotels(input);
    if (censusResult.provider_status.status === "ok") {
      hotels.push(
        ...censusResult.hotels.map((h) => ({
          ...h,
          source_provider: MAP_PROVIDER_IDS.census,
        }))
      );
    } else {
      providerFailures.push(censusResult.provider_status);
    }

    const wantHotelbeds =
      !input.providers ||
      input.providers.includes("hotelbeds") ||
      input.providers.includes(MAP_PROVIDER_IDS.hotelbeds);

    if (wantHotelbeds && (input.country_code || input.hotel_codes || input.name)) {
      try {
        const hbx = await providers.hotelbeds.searchHotels(input);
        if (hbx.provider_status.status === "ok") {
          hotels.push(
            ...hbx.hotels.map((h) => ({
              ...h,
              source_provider: MAP_PROVIDER_IDS.hotelbeds,
            }))
          );
        } else {
          providerFailures.push(hbx.provider_status);
        }
      } catch (err) {
        providerFailures.push({
          provider: "hotelbeds",
          status: "unavailable",
          retryable: true,
          message: String(err?.message || err).slice(0, 120),
        });
      }
    }

    // GIATA Drive: targeted discovery only (zero/near-zero or explicit opt-in)
    const wantGiata =
      input.providers?.includes("giata_drive") ||
      input.providers?.includes(MAP_PROVIDER_IDS.giata_drive) ||
      input.giata_discovery === true;
    if (
      wantGiata &&
      providers.giata_drive &&
      (input.country_code || input.countryCode)
    ) {
      try {
        const g = await providers.giata_drive.searchHotels({
          countryCode: input.country_code || input.countryCode,
          limit: input.limit || 25,
          fetch_details: input.fetch_details !== false,
          after: input.after,
        });
        if (g.provider_status.status === "ok") {
          hotels.push(
            ...g.hotels.map((h) => ({
              ...h,
              source_provider: MAP_PROVIDER_IDS.giata_drive,
            }))
          );
        } else {
          providerFailures.push(g.provider_status);
        }
      } catch (err) {
        providerFailures.push({
          provider: "giata_drive",
          status: "unavailable",
          retryable: true,
          message: String(err?.message || err).slice(0, 120),
        });
      }
    }

    return {
      ok: true,
      count: hotels.length,
      hotels,
      provider_failures: providerFailures,
      version: SERVICE_VERSION,
    };
  }

  async function hotelGet(input = {}) {
    const hotelId = String(input.hotel_id || "").trim();
    if (!hotelId) {
      return { ok: false, error: "hotel_id_required", hotel: null };
    }

    const staged = store.readStagedHotels();
    if (staged.hotels?.[hotelId]) {
      const hotel = staged.hotels[hotelId];
      return {
        ok: true,
        hotel: toMvpHotelSummary(hotel),
        canonical: hotel,
        evidence_summary: evidence.summarizeHotel(hotelId),
        staged: true,
      };
    }

    let canonical = await providers.census.getHotelByHotelId(hotelId);
    if (!canonical) {
      return {
        ok: false,
        error: "hotel_not_found",
        hotel: null,
        evidence_summary: evidence.summarizeHotel(hotelId),
      };
    }
    return {
      ok: true,
      hotel: toMvpHotelSummary(canonical),
      canonical,
      evidence_summary: evidence.summarizeHotel(hotelId),
      staged: false,
    };
  }

  async function hotelResolve(input = {}) {
    const records = await loadCensusRecords();
    const result = resolveHotelIdentity(input, records, { idRegistry, store });
    if (result.review_required || result.match_status === MATCH_STATUS.AMBIGUOUS) {
      enqueueFromResolveResult(reviewQueue, result, {
        sources: Object.keys(input.external_ids || {}).length
          ? Object.keys(input.external_ids)
          : ["input"],
      });
    }
    return { ok: true, ...result };
  }

  async function hotelEnrich(input = {}) {
    const hotelId = String(input.hotel_id || "").trim();
    const fields = Array.isArray(input.fields) ? input.fields : [];
    const providerIds = Array.isArray(input.providers)
      ? input.providers
      : ["giata_drive", "hotelbeds", "stayingapi", "serpapi"];

    const fields_requested = fields.length
      ? fields
      : [
          "room_count",
          "website",
          "phone",
          "brand_name",
          "address_line_1",
          "latitude",
          "longitude",
        ];
    const fields_found = [];
    const fields_updated = [];
    const conflicts = [];
    const provider_failures = [];

    if (!hotelId) {
      return {
        ok: false,
        error: "hotel_id_required",
        fields_requested,
        fields_found,
        fields_updated,
        conflicts,
        provider_failures,
        review_required: false,
      };
    }

    const mapping = idRegistry.getByHotelId(hotelId);
    let hbxCode = null;
    let stayingListing = null;
    let giataId = null;
    for (const e of mapping?.external_ids || []) {
      if (e.provider === "hotelbeds") hbxCode = e.external_id;
      if (e.provider === "stayingapi" || e.provider === "booking_com") {
        stayingListing = e;
      }
      if (e.provider === "giata_drive" || e.provider === "giata") {
        giataId = e.external_id;
      }
    }
    if (!hbxCode && mapping?.airtable_record_id) {
      const records = await loadCensusRecords();
      const rec = records.find((r) => r.id === mapping.airtable_record_id);
      hbxCode = rec?.fields?.[MAP_CENSUS_FIELDS.hbxHotelCode] || null;
    }

    function stageCandidate(cand, source) {
      // Never stage room_count from providers that mark Rooms/Keys NOT_SUPPORTED
      for (const [field, value] of candidateToEvidenceFields(cand)) {
        if (!fields_requested.includes(field)) continue;
        if (
          (source === "stayingapi" ||
            source === "serpapi" ||
            source === "giata_drive") &&
          field === "room_count"
        ) {
          continue;
        }
        if (value == null || value === "") continue;
        fields_found.push(field);
        const ev = evidence.addEvidence({
          hotel_id: hotelId,
          field,
          value,
          source,
          source_record_id: cand.external_id,
        });
        fields_updated.push({
          field,
          value,
          confidence: ev.confidence,
          staged: true,
          airtable_written: false,
          provider: source,
        });
      }
      // Link external IDs when present
      if (cand.external_id && source === "stayingapi") {
        try {
          idRegistry.linkExternalId(hotelId, "stayingapi", String(cand.external_id));
        } catch {
          /* ignore */
        }
      }
      if (cand.external_id && source === "serpapi") {
        try {
          idRegistry.linkExternalId(
            hotelId,
            "serpapi",
            String(cand.external_id),
            { external_url: cand.raw_safe?.google_property_url || null }
          );
        } catch {
          /* ignore */
        }
      }
      if (cand.raw_safe?.serpapi_property_token && source === "serpapi") {
        try {
          idRegistry.linkExternalId(
            hotelId,
            "serpapi_property_token",
            String(cand.raw_safe.serpapi_property_token)
          );
        } catch {
          /* ignore */
        }
      }
      if (cand.external_id && source === "giata_drive") {
        try {
          idRegistry.linkExternalId(hotelId, "giata_drive", String(cand.external_id));
        } catch {
          /* ignore */
        }
      }
      if (cand.raw_safe?.booking_com_id) {
        try {
          idRegistry.linkExternalId(
            hotelId,
            "booking_com",
            String(cand.raw_safe.booking_com_id)
          );
        } catch {
          /* ignore */
        }
      }
    }

    for (const pid of providerIds) {
      if (pid === "hotelbeds" || pid === MAP_PROVIDER_IDS.hotelbeds) {
        try {
          if (!hbxCode) {
            provider_failures.push({
              provider: "hotelbeds",
              status: "not_found",
              retryable: false,
              message: "no_hotelbeds_external_id_linked",
            });
          } else {
            const got = await providers.hotelbeds.getHotel(hbxCode);
            if (got.provider_status.status !== "ok") {
              provider_failures.push(got.provider_status);
              // Do not abort — other providers may still run
            } else if (got.hotel) {
              stageCandidate(got.hotel, "hotelbeds");
            }
          }
        } catch (err) {
          provider_failures.push({
            provider: "hotelbeds",
            status: "unavailable",
            retryable: true,
            message: String(err?.message || err).slice(0, 120),
          });
        }
        continue;
      }

      if (pid === "stayingapi" || pid === MAP_PROVIDER_IDS.stayingapi) {
        try {
          let cand = null;
          if (stayingListing?.external_id && stayingListing.provider === "booking_com") {
            const got = await providers.stayingapi.getHotel(stayingListing.external_id, {
              platform: "booking",
              platform_listing_id: stayingListing.external_id,
              hotel_id: hotelId,
            });
            if (got.provider_status.status !== "ok") {
              provider_failures.push(got.provider_status);
            } else {
              cand = got.hotel;
            }
          } else if (input.location || input.name || input.city) {
            const searched = await providers.stayingapi.searchHotels({
              location: input.location,
              name: input.name,
              city: input.city,
              country: input.country,
              limit: input.limit || 5,
              hotel_id: hotelId,
            });
            if (searched.provider_status.status !== "ok") {
              provider_failures.push(searched.provider_status);
            } else {
              cand = searched.hotels?.[0] || null;
              if (!cand) {
                provider_failures.push({
                  provider: "stayingapi",
                  status: "not_found",
                  retryable: false,
                  message: "zero_search_results",
                });
              }
            }
          } else {
            provider_failures.push({
              provider: "stayingapi",
              status: "not_found",
              retryable: false,
              message: "stayingapi_requires_listing_id_or_location_query",
            });
          }
          if (cand) stageCandidate(cand, "stayingapi");
        } catch (err) {
          provider_failures.push({
            provider: "stayingapi",
            status: "unavailable",
            retryable: true,
            message: String(err?.message || err).slice(0, 120),
          });
        }
        continue;
      }

      if (pid === "serpapi" || pid === MAP_PROVIDER_IDS.serpapi) {
        try {
          let cand = null;
          const tokenExt = (mapping?.external_ids || []).find(
            (e) =>
              e.provider === "serpapi" || e.provider === "serpapi_property_token"
          );
          if (tokenExt?.external_id && input.prefer_serpapi_token) {
            const got = await providers.serpapi.getHotel(tokenExt.external_id, {
              property_token: tokenExt.external_id,
              name: input.name,
              country: input.country,
              hotel_id: hotelId,
            });
            if (got.provider_status.status !== "ok") {
              provider_failures.push(got.provider_status);
            } else {
              cand = got.hotel;
            }
          } else if (input.location || input.name || input.city || input.q) {
            const searched = await providers.serpapi.searchHotels({
              location: input.location,
              name: input.name,
              city: input.city,
              country: input.country,
              q: input.q,
              hotel_id: hotelId,
            });
            if (searched.provider_status.status !== "ok") {
              provider_failures.push(searched.provider_status);
            } else {
              cand = searched.hotels?.[0] || null;
              if (!cand) {
                provider_failures.push({
                  provider: "serpapi",
                  status: "not_found",
                  retryable: false,
                  message: "zero_search_results",
                });
              }
            }
          } else {
            provider_failures.push({
              provider: "serpapi",
              status: "not_found",
              retryable: false,
              message: "serpapi_requires_property_token_or_name_query",
            });
          }
          if (cand) stageCandidate(cand, "serpapi");
        } catch (err) {
          provider_failures.push({
            provider: "serpapi",
            status: "unavailable",
            retryable: true,
            message: String(err?.message || err).slice(0, 120),
          });
        }
        continue;
      }

      if (pid === "giata_drive" || pid === MAP_PROVIDER_IDS.giata_drive) {
        try {
          if (!providers.giata_drive) {
            provider_failures.push({
              provider: "giata_drive",
              status: "unavailable",
              retryable: false,
              message: "provider_not_registered",
            });
          } else {
            let cand = null;
            const knownId =
              giataId ||
              input.giata_id ||
              input.giataId ||
              null;
            if (knownId) {
              const got = await providers.giata_drive.getHotel(String(knownId), {
                hotel_id: hotelId,
              });
              if (got.provider_status.status !== "ok") {
                provider_failures.push(got.provider_status);
              } else {
                cand = got.hotel;
              }
            } else if (
              input.force_giata_search ||
              input.country_code ||
              input.countryCode
            ) {
              // Targeted country search only — not general bulk discovery
              const searched = await providers.giata_drive.searchHotels({
                countryCode: input.country_code || input.countryCode,
                name: input.name,
                city: input.city,
                limit: input.limit || 5,
                fetch_details: true,
              });
              if (searched.provider_status.status !== "ok") {
                provider_failures.push(searched.provider_status);
              } else {
                cand = searched.hotels?.[0] || null;
                if (!cand) {
                  provider_failures.push({
                    provider: "giata_drive",
                    status: "not_found",
                    retryable: false,
                    message: "zero_search_results",
                  });
                }
              }
            } else {
              provider_failures.push({
                provider: "giata_drive",
                status: "not_found",
                retryable: false,
                message: "giata_drive_requires_giata_id_or_targeted_country",
              });
            }
            if (cand) stageCandidate(cand, "giata_drive");
          }
        } catch (err) {
          provider_failures.push({
            provider: "giata_drive",
            status: "unavailable",
            retryable: true,
            message: String(err?.message || err).slice(0, 120),
          });
        }
      }
    }

    const summary = evidence.summarizeHotel(hotelId);
    conflicts.push(...summary.conflicts);
    if (conflicts.length) {
      enqueueFromResolveResult(
        reviewQueue,
        { hotel_id: hotelId, match_status: "probable", review_required: true, match_score: 0.7 },
        { hotel_id: hotelId, conflicts }
      );
    }

    return {
      ok: true,
      fields_requested,
      fields_found: [...new Set(fields_found)],
      fields_updated,
      conflicts,
      provider_failures,
      review_required: conflicts.length > 0,
      airtable_writes_enabled: airtableWritesEnabled(opts.env || process.env),
      note: "Canonical census not mutated; evidence staged locally",
    };
  }

  async function hotelNearby(input = {}) {
    const records = await loadCensusRecords();
    return findNearbyHotels(records, input, { idRegistry, store });
  }

  async function hotelSources(input = {}) {
    const hotelId = String(input.hotel_id || "").trim();
    if (!hotelId) return { ok: false, error: "hotel_id_required" };
    const items = evidence.listForHotel(hotelId, input.field || null);
    const summary = evidence.summarizeHotel(hotelId);
    return {
      ok: true,
      hotel_id: hotelId,
      evidence: items,
      conflicts: summary.conflicts,
      fields: summary.fields,
    };
  }

  async function hotelReviewQueue(input = {}) {
    return {
      ok: true,
      items: reviewQueue.list(input || {}),
      version: reviewQueue.version,
    };
  }

  /**
   * Evidence-backed TOTAL PROPERTY ROOM COUNT research for one hotel.
   * Research-only — never writes Airtable / census.
   */
  async function hotelRoomCountResearch(input = {}) {
    const hotelId = String(input.hotel_id || "").trim() || null;
    let hotel = {
      hotel_id: hotelId,
      hotel_name: input.hotel_name || input.name || null,
      city: input.city || null,
      country: input.country || null,
      brand: input.brand || null,
      website: input.website || null,
      latitude: input.latitude ?? null,
      longitude: input.longitude ?? null,
      identity_confidence: input.identity_confidence ?? 0.9,
    };

    // Fill from staged/census mapping when hotel_id provided
    if (hotelId) {
      const mapping = idRegistry.getByHotelId(hotelId);
      const staged = store.readStagedHotels()?.hotels?.[hotelId];
      if (staged) {
        hotel = {
          ...hotel,
          hotel_name:
            hotel.hotel_name || staged.official_name || staged.display_name || null,
          city: hotel.city || staged.city || null,
          country: hotel.country || staged.country || null,
          brand: hotel.brand || staged.brand_name || null,
          website: hotel.website || staged.website || null,
          latitude: hotel.latitude ?? staged.latitude ?? null,
          longitude: hotel.longitude ?? staged.longitude ?? null,
        };
      }
      if (!hotel.hotel_name && mapping?.airtable_record_id) {
        try {
          const records = await loadCensusRecords();
          const rec = records.find((r) => r.id === mapping.airtable_record_id);
          const f = rec?.fields || {};
          hotel.hotel_name =
            hotel.hotel_name ||
            f[MAP_CENSUS_FIELDS.officialName] ||
            f[MAP_CENSUS_FIELDS.propertyName] ||
            null;
          hotel.city = hotel.city || f[MAP_CENSUS_FIELDS.city] || null;
          hotel.country = hotel.country || f[MAP_CENSUS_FIELDS.country] || null;
          hotel.brand = hotel.brand || f[MAP_CENSUS_FIELDS.brandName] || null;
          hotel.website = hotel.website || f[MAP_CENSUS_FIELDS.website] || null;
          hotel.latitude =
            hotel.latitude ??
            (f[MAP_CENSUS_FIELDS.latitude] != null
              ? Number(f[MAP_CENSUS_FIELDS.latitude])
              : null);
          hotel.longitude =
            hotel.longitude ??
            (f[MAP_CENSUS_FIELDS.longitude] != null
              ? Number(f[MAP_CENSUS_FIELDS.longitude])
              : null);
        } catch {
          /* census optional */
        }
      }
    }

    if (!hotel.hotel_name && !hotel.website) {
      return {
        ok: false,
        error: "hotel_name_or_website_required",
        candidate_room_count: null,
        confidence: 0,
        supporting_sources: [],
        supporting_quotes: [],
        review_required: true,
        research_status: "NO_EVIDENCE",
        airtable_written: false,
      };
    }

    const result = await researchHotelRoomCount(hotel, {
      evidence,
      env: opts.env || process.env,
      maxSearches: input.max_searches,
      maxPageFetches: input.max_page_fetches,
      allowSerpapi: input.allow_serpapi,
    });

    if (result.review_required && hotelId) {
      try {
        reviewQueue.enqueue({
          hotel_id: hotelId,
          issue_type: result.conflicts?.length
            ? ISSUE_TYPES.ROOM_COUNT_CONFLICT
            : ISSUE_TYPES.MISSING_ROOM_COUNT,
          candidate_value: result.candidate_room_count,
          confidence: result.confidence,
          sources: ["room_count_research"],
          recommended_action: "manual_review",
        });
      } catch {
        /* review queue optional */
      }
    }

    return result;
  }

  /**
   * Stage-only census ingest. Never partial-corrupts production census.
   */
  async function hotelCensusIngest(input = {}) {
    const records = Array.isArray(input.records)
      ? input.records
      : input.record
        ? [input.record]
        : [];
    if (!records.length) {
      return { ok: false, error: "records_required", results: [] };
    }

    const wantEnrich = input.enrich !== false;
    const job = batches.create(records, { batch_id: input.batch_id });
    job.status = BATCH_STATUS.RUNNING;
    batches.save(job);

    const censusRecords = await loadCensusRecords();
    const results = [];
    let pausedQuota = false;

    for (const row of job.records) {
      if (pausedQuota) {
        row.status = BATCH_STATUS.PENDING;
        results.push({
          index: row.index,
          status: BATCH_STATUS.PENDING,
          note: "paused_due_to_provider_quota",
        });
        continue;
      }

      try {
        row.status = BATCH_STATUS.RUNNING;
        const resolved = resolveHotelIdentity(row.input, censusRecords, {
          idRegistry,
          store,
        });

        let hotelId = resolved.hotel_id;
        if (
          !hotelId &&
          resolved.match_status !== MATCH_STATUS.AMBIGUOUS &&
          resolved.match_status !== MATCH_STATUS.INSUFFICIENT
        ) {
          hotelId = idRegistry.createStagedHotelId();
          resolved.hotel_id = hotelId;
        }

        if (resolved.match_status === MATCH_STATUS.AMBIGUOUS) {
          enqueueFromResolveResult(reviewQueue, resolved);
          row.status = BATCH_STATUS.REVIEW_REQUIRED;
          row.hotel_id = null;
          row.result = {
            match_status: resolved.match_status,
            match_score: resolved.match_score,
            review_required: true,
            candidate_matches: resolved.candidate_matches,
          };
          results.push({
            index: row.index,
            status: row.status,
            hotel_id: null,
            match_status: resolved.match_status,
            review_required: true,
            airtable_written: false,
          });
          continue;
        }

        // Build staged canonical recommendation
        const hotel = createEmptyCanonicalHotel({
          hotel_id: hotelId,
          identity: {
            official_name: row.input.name || null,
            display_name: row.input.name || null,
          },
          location: {
            address_line_1: row.input.address || null,
            city: row.input.city || null,
            country: row.input.country || null,
            latitude: row.input.latitude ?? null,
            longitude: row.input.longitude ?? null,
          },
          brand: {
            brand_name: row.input.brand || null,
          },
          digital: {
            website: row.input.website || null,
            phone: row.input.phone || null,
          },
          linkages: {
            airtable_record_id:
              resolved.candidate_matches?.[0]?.airtable_record_id || null,
            property_identity_key: null,
            external_ids: row.input.external_ids || [],
          },
        });

        // Seed evidence from input
        for (const [field, value] of candidateToEvidenceFields({
          name: row.input.name,
          address: row.input.address,
          city: row.input.city,
          country: row.input.country,
          latitude: row.input.latitude,
          longitude: row.input.longitude,
          room_count: row.input.room_count,
          brand_name: row.input.brand,
          website: row.input.website,
          phone: row.input.phone,
        })) {
          evidence.addEvidence({
            hotel_id: hotelId,
            field,
            value,
            source: "manual",
            completeness: 1,
          });
        }

        if (
          wantEnrich &&
          row.input.external_ids?.hotelbeds &&
          !pausedQuota
        ) {
          const enrichResult = await hotelEnrich({
            hotel_id: hotelId,
            providers: ["hotelbeds"],
          });
          if (
            enrichResult.provider_failures?.some(
              (p) => p.status === "quota_exhausted"
            )
          ) {
            pausedQuota = true;
            batches.markPausedQuota(job, "TEST_DAILY_QUOTA_EXHAUSTED");
          }
        }

        if (resolved.review_required || resolved.match_status === MATCH_STATUS.AMBIGUOUS) {
          enqueueFromResolveResult(reviewQueue, resolved);
          row.status = BATCH_STATUS.REVIEW_REQUIRED;
        } else {
          row.status = BATCH_STATUS.ENRICHED;
        }

        const staged = store.readStagedHotels();
        staged.hotels[hotelId] = hotel;
        store.writeStagedHotels(staged);

        row.hotel_id = hotelId;
        row.result = {
          match_status: resolved.match_status,
          match_score: resolved.match_score,
          review_required: resolved.review_required,
          hotel: toMvpHotelSummary(hotel),
        };
        results.push({
          index: row.index,
          status: row.status,
          hotel_id: hotelId,
          match_status: resolved.match_status,
          review_required: resolved.review_required,
          airtable_written: false,
        });
      } catch (err) {
        row.status = BATCH_STATUS.FAILED;
        row.error = String(err?.message || err).slice(0, 200);
        results.push({
          index: row.index,
          status: BATCH_STATUS.FAILED,
          error: row.error,
          airtable_written: false,
        });
      }
    }

    const allDone = job.records.every(
      (r) =>
        r.status === BATCH_STATUS.ENRICHED ||
        r.status === BATCH_STATUS.REVIEW_REQUIRED ||
        r.status === BATCH_STATUS.FAILED ||
        r.status === BATCH_STATUS.COMPLETED
    );
    if (pausedQuota) {
      job.status = BATCH_STATUS.PAUSED_QUOTA;
    } else if (allDone) {
      job.status = BATCH_STATUS.COMPLETED;
      for (const r of job.records) {
        if (r.status === BATCH_STATUS.ENRICHED) r.status = BATCH_STATUS.COMPLETED;
      }
    } else {
      job.status = BATCH_STATUS.RUNNING;
    }
    batches.save(job);

    return {
      ok: true,
      batch_id: job.batch_id,
      batch_status: job.status,
      results,
      airtable_writes_enabled: airtableWritesEnabled(opts.env || process.env),
      airtable_writes_made: 0,
      note: "Stage-only: production Hotel Property Census not mutated",
    };
  }

  return {
    version: SERVICE_VERSION,
    store,
    idRegistry,
    evidence,
    reviewQueue,
    batches,
    providers,
    hotelSearch,
    hotelGet,
    hotelResolve,
    hotelEnrich,
    hotelNearby,
    hotelSources,
    hotelReviewQueue,
    hotelRoomCountResearch,
    hotelCensusIngest,
    airtableWritesEnabled: () => airtableWritesEnabled(opts.env || process.env),
  };
}

export {
  toMvpHotelSummary,
  censusRecordToCanonical,
  preferCanonicalValue,
  scoreFieldConfidence,
};
