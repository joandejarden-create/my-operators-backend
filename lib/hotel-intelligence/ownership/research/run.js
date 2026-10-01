/**
 * Ownership research orchestrator — Stages A → B → C, stage-only (no Census writes).
 */

import { ISSUE_TYPES } from "../../review-queue.js";
import { createResearchRun } from "../schemas.js";
import { queryHotelOwnership } from "../queries.js";
import { runStageA, stageCandidates as stageACandidates } from "./stage-a-existing.js";
import {
  runStageB,
  stageBCandidates,
} from "./stage-b-deterministic.js";
import {
  runStageC,
  stageCCandidates,
} from "./stage-c-search.js";
import {
  runStageCountry,
  stageCountryCandidates,
} from "./stage-country-authoritative.js";
import {
  runStageEconomicOwner,
  stageEconomicCandidates,
} from "./stage-economic-owner.js";
import { enrichCorporateGraphWithGleif } from "./gleif-corporate-enrichment.js";
import {
  runBrazilCorporateStage,
  stageBrazilCorporateResults,
} from "./brazil-cnpj-corporate-stage.js";
import { runFranchiseDisclosureStage } from "./stage-franchise-disclosure.js";
import {
  classifyOwnershipStructure,
  buildStructureSignalsFromResearch,
} from "../ownership-structure.js";
import { aggregateMethodPerformance } from "../discovery-methods.js";
import { evaluateWebhoundEscalationEligibility } from "./webhound-escalation.js";
import { emitNativeResearchObservations } from "./emit-observations.js";
import {
  createResearchDossier,
  provenanceStepsFromBrazilNotes,
} from "../research-dossier.js";
import {
  decideStop,
  detectOwnedByConflict,
  STOP_REASONS,
} from "./stop.js";

export const OWNERSHIP_RESEARCH_VERSION = "ownership-research-run-v4";

/**
 * @param {object} input
 */
export async function researchHotelOwnership(input = {}) {
  const t0 = Date.now();
  const repo = input.ownershipRepository;
  const reviewQueue = input.reviewQueue;
  const env = input.env || process.env;
  const hotelId = String(input.hotel_id || "").trim();

  if (!repo) {
    return { ok: false, error: "ownership_repository_required" };
  }
  if (!hotelId) {
    return {
      ok: true,
      hotel_id: null,
      run_id: null,
      stop_reason: "hotel_id_required",
      relationships_staged: [],
      evidence_staged: [],
      entities_staged: [],
      review_items: [],
      status_summary: emptyStatus(),
    };
  }

  let hotel = input.hotel || null;
  let censusRecord = input.censusRecord || null;

  if (input.hotelGet) {
    const got = await input.hotelGet({ hotel_id: hotelId });
    if (got?.ok && got.canonical) {
      hotel = {
        hotel_id: hotelId,
        name: got.hotel?.official_name || got.hotel?.display_name,
        hotel_name: got.hotel?.official_name || got.hotel?.display_name,
        website: got.hotel?.website || got.canonical?.digital?.website,
        brand_name: got.hotel?.brand_name,
        city: got.hotel?.city || got.canonical?.location?.city,
        country: got.hotel?.country || got.canonical?.location?.country,
        ...hotel,
      };
    }
  }

  if (!censusRecord && Array.isArray(input.censusRecords)) {
    censusRecord =
      input.censusRecords.find((r) => {
        const map = input.idRegistry;
        // best-effort: match by name later; optional airtable id on hotel
        return false;
      }) || null;
  }

  // Attach census fields from hotelGet path when providers expose them
  if (!censusRecord && hotel?.census_fields) {
    censusRecord = { fields: hotel.census_fields };
  }

  const run = await repo.createResearchRun(
    createResearchRun({
      hotel_id: hotelId,
      status: "running",
    })
  );

  const stagesCompleted = [];
  /** @type {object[]} */
  let allReview = [];
  /** @type {object[]} */
  let allRels = [];
  /** @type {object[]} */
  let allEv = [];
  const metrics = {
    searches: 0,
    pages_fetched: 0,
    evidence_count: 0,
    cost_usd: null,
    latency_ms: null,
  };

  const ctx = {
    hotelId,
    hotel: hotel || { hotel_id: hotelId },
    censusRecord,
    ownershipRepository: repo,
    env,
    runId: run.run_id,
    maxSearches: input.max_searches,
    maxPageFetches: input.max_page_fetches,
    allowSerpapi: input.allow_serpapi,
  };

  /** @type {object[]} */
  const methodAttempts = [];
  /** @type {object|null} */
  let brazilCorporateResult = null;
  /** @type {object|null} */
  let franchiseDisclosureResult = null;

  // --- Stage A ---
  const a = await runStageA(ctx);
  stagesCompleted.push("A");
  /** @type {object[]} */
  let resolvedEntities = [];
  if (a.candidates?.length) {
    const staged = await stageACandidates(repo, hotelId, a.candidates, run.run_id);
    allRels.push(...staged.relationships);
    allEv.push(...staged.evidence);
    allReview.push(...staged.review);
    resolvedEntities.push(...(staged.entities || []));
  }
  let stop = decideStop({
    existing_strong: a.stop,
    force_stop_reason: a.stop ? a.stop_reason : null,
  });

  // --- Stage Country (authoritative adapters) ---
  /** @type {object[]} */
  let registeredCompanyEntities = [];
  if (!stop.stop) {
    const country = await runStageCountry(ctx);
    stagesCompleted.push("country");
    metrics.country_adapter = country.metrics || null;
    if (country.candidates?.length) {
      const staged = await stageCountryCandidates(
        repo,
        hotelId,
        country.candidates,
        run.run_id
      );
      allRels.push(...staged.relationships);
      allEv.push(...staged.evidence);
      allReview.push(...staged.review);
      resolvedEntities.push(...(staged.entities || []));
      // Registered companies from country stage (no OWNED_BY required)
      registeredCompanyEntities = (country.candidates || [])
        .filter((c) => !c.ambiguity && c.entity && !c.supported_relationship)
        .map((c) => c.entity);
      if (!registeredCompanyEntities.length) {
        registeredCompanyEntities = (staged.entities || []).filter(Boolean);
      }
    }

    // Brazil corporate lane (P1.6) — seed validation, branch→matrix, QSA, related entities
    const isBrazil = /brazil|brasil/i.test(String(hotel?.country || ""));
    if (isBrazil) {
      brazilCorporateResult = await runBrazilCorporateStage(ctx, {
        registeredEntities: registeredCompanyEntities,
        cadasturCandidates: (country.candidates || []).map((c) => ({
          entity_candidate: c.entity_candidate,
        })),
        explicitSeeds: buildExplicitBrazilSeeds(hotel, input),
      });
      stagesCompleted.push("brazil_corporate");
      metrics.brazil_corporate = brazilCorporateResult.metrics || null;
      methodAttempts.push(...(brazilCorporateResult.method_attempts || []));
      if (brazilCorporateResult.entities?.length) {
        const stagedBr = await stageBrazilCorporateResults(
          repo,
          hotelId,
          brazilCorporateResult,
          run.run_id
        );
        allReview.push(...stagedBr.review);
        resolvedEntities.push(...stagedBr.entities);
        registeredCompanyEntities = [
          ...registeredCompanyEntities,
          ...(brazilCorporateResult.operating_entity
            ? [brazilCorporateResult.operating_entity]
            : []),
        ];
      }

      franchiseDisclosureResult = await runFranchiseDisclosureStage(ctx, {
        operatingEntity: brazilCorporateResult.operating_entity,
      });
      stagesCompleted.push("franchise_disclosure");
      metrics.franchise_disclosure = franchiseDisclosureResult.metrics || null;
      if (franchiseDisclosureResult.method_attempt) {
        methodAttempts.push(franchiseDisclosureResult.method_attempt);
      }
      allRels.push(...(franchiseDisclosureResult.relationships || []));
      allEv.push(...(franchiseDisclosureResult.evidence || []));
      metrics.searches += franchiseDisclosureResult.metrics?.searches || 0;
      metrics.pages_fetched += franchiseDisclosureResult.metrics?.pages_fetched || 0;
    }

    // GLEIF corporate graph on registered company candidates (P1.5)
    if (registeredCompanyEntities.length || resolvedEntities.length) {
      const gleifPool = [
        ...registeredCompanyEntities,
        ...resolvedEntities,
      ].filter(Boolean);
      const gleif = await enrichCorporateGraphWithGleif(
        repo,
        hotelId,
        gleifPool,
        run.run_id,
        env
      );
      allEv.push(...gleif.evidence);
      allReview.push(...gleif.review);
      metrics.gleif = gleif.metrics;
      metrics.gleif_entity_relationships = gleif.entity_relationships?.length || 0;
    }

    // Stage Economic Owner — entity-driven sponsor/control research (P1.5B)
    const economic = await runStageEconomicOwner(ctx, {
      registeredEntities: registeredCompanyEntities,
    });
    stagesCompleted.push("economic");
    metrics.economic = economic.metrics || null;
    if (economic.candidates?.length) {
      const staged = await stageEconomicCandidates(
        repo,
        hotelId,
        economic.candidates,
        run.run_id
      );
      allRels.push(...staged.relationships);
      allEv.push(...staged.evidence);
      allReview.push(...staged.review);
      resolvedEntities.push(...(staged.entities || []));
    }

    // Skip Stage C search budget if we already have VERIFIED/HIGH OWNED_BY
    const afterCountry = await repo.listRelationshipsForHotel(hotelId, {
      currentOnly: true,
    });
    const strongOwned = afterCountry.find(
      (r) =>
        r.relationship_type === "OWNED_BY" &&
        (r.verification_status === "verified" ||
          r.verification_status === "high")
    );
    if (strongOwned) {
      stop = decideStop({
        stage_b_high: true,
        force_stop_reason: "country_or_existing_high_owned_by",
      });
    }
  }

  if (!stop.stop) {
    const b = await runStageB(ctx);
    stagesCompleted.push("B");
    if (b.candidates?.length) {
      const staged = await stageBCandidates(repo, hotelId, b.candidates, run.run_id);
      allRels.push(...staged.relationships);
      allEv.push(...staged.evidence);
      allReview.push(...staged.review);
    }
    stop = decideStop({
      stage_b_high: b.stop,
      force_stop_reason: b.stop ? b.stop_reason : null,
    });

    if (!stop.stop) {
      const c = await runStageC({
        ...ctx,
        registeredEntities: registeredCompanyEntities,
      });
      stagesCompleted.push("C");
      metrics.searches += c.metrics?.searches || 0;
      metrics.pages_fetched += c.metrics?.pages_fetched || 0;
      if (c.candidates?.length) {
        const staged = await stageCCandidates(
          repo,
          hotelId,
          c.candidates,
          run.run_id
        );
        allRels.push(...staged.relationships);
        allEv.push(...staged.evidence);
        allReview.push(...staged.review);
      }
      const current = await repo.listRelationshipsForHotel(hotelId, {
        currentOnly: true,
      });
      const conflict = detectOwnedByConflict(current);
      if (conflict.conflict) {
        allReview.push({
          issue_type: ISSUE_TYPES.OWNERSHIP_CONFLICT,
          summary: `Conflicting OWNED_BY entities: ${conflict.entity_ids.join(",")}`,
        });
        for (const rel of current.filter((r) => r.relationship_type === "OWNED_BY")) {
          if (conflict.entity_ids.includes(rel.object_entity_id)) {
            await repo.upsertRelationship({
              ...rel,
              verification_status: "conflict",
            });
          }
        }
        stop = decideStop({ conflict: true });
      } else {
        stop = decideStop({
          stage_c_done: true,
          has_owned_by: current.some((r) => r.relationship_type === "OWNED_BY"),
        });
      }
    }
  }

  // Enqueue reviews
  if (reviewQueue) {
    for (const item of allReview) {
      reviewQueue.enqueue({
        hotel_id: hotelId,
        issue_type: item.issue_type,
        severity: item.issue_type === ISSUE_TYPES.OWNERSHIP_CONFLICT ? "high" : "medium",
        recommended_action: "manual_review",
        candidate_value: item.summary,
      });
    }
  }

  metrics.evidence_count = allEv.length;
  metrics.latency_ms = Date.now() - t0;
  metrics.method_performance = aggregateMethodPerformance(methodAttempts);
  metrics.method_attempts = methodAttempts;

  const structureSignals = buildStructureSignalsFromResearch({
    brazil_corporate: brazilCorporateResult,
    franchise_disclosure: franchiseDisclosureResult,
    review_notes: allReview.map((r) => r.summary),
  });
  const ownershipStructure = classifyOwnershipStructure(structureSignals);
  metrics.ownership_structure = ownershipStructure;

  const observations = emitNativeResearchObservations(ctx, {
    brazil: brazilCorporateResult,
    franchise: franchiseDisclosureResult,
    structure: ownershipStructure,
    review: allReview,
  });
  const observationIds = [];
  for (const obs of observations) {
    const saved = await repo.addObservation(obs);
    observationIds.push(saved.observation_id);
  }
  metrics.observations_count = observationIds.length;

  const unresolved = [
    ...allReview.map((r) => r.summary).filter(Boolean),
    ...(brazilCorporateResult?.notes || []).filter((n) =>
      /no_validated_seed|rejected|unresolved/i.test(String(n))
    ),
  ].slice(0, 12);

  const dossier = await repo.upsertResearchDossier(
    createResearchDossier({
      hotel_id: hotelId,
      research_run_id: run.run_id,
      seed: brazilCorporateResult?.primary_seed
        ? {
            cnpj: brazilCorporateResult.primary_seed.cnpj,
            status: brazilCorporateResult.primary_seed.validation?.status,
            source: brazilCorporateResult.primary_seed.source,
          }
        : null,
      observation_ids: observationIds,
      candidate_relationship_ids: allRels
        .filter((r) => ["needs_review", "probable"].includes(r.verification_status))
        .map((r) => r.relationship_id),
      validated_relationship_ids: allRels
        .filter((r) => ["verified", "high"].includes(r.verification_status))
        .map((r) => r.relationship_id),
      unresolved_questions: unresolved,
      research_methods: [...new Set(methodAttempts.map((a) => a.method).filter(Boolean))],
      provenance_steps: provenanceStepsFromBrazilNotes(brazilCorporateResult?.notes || []),
      cost_usd: metrics.cost_usd,
      latency_ms: Date.now() - t0,
      source_provider: "dealality_native",
      product_truth: false,
    })
  );

  const escalation = evaluateWebhoundEscalationEligibility({
    env,
    unresolved_economic_owner: !finalRelsPrecheckHasEconomic(allRels),
    has_validated_company_seed: Boolean(brazilCorporateResult?.primary_seed),
    research_exhausted: stop.reason === STOP_REASONS.EXHAUSTED,
  });
  metrics.webhound_escalation = escalation;

  const finalRels = await repo.listRelationshipsForHotel(hotelId, {
    currentOnly: true,
  });
  const ownershipView = await queryHotelOwnership(repo, { hotel_id: hotelId }, {
    hotelExists: true,
  });

  await repo.updateResearchRun({
    ...run,
    status: "completed",
    stages_completed: stagesCompleted,
    stop_reason: stop.reason || STOP_REASONS.EXHAUSTED,
    metrics,
    completed_at: new Date().toISOString(),
  });

  return {
    ok: true,
    hotel_id: hotelId,
    run_id: run.run_id,
    stop_reason: stop.reason || STOP_REASONS.EXHAUSTED,
    stages_completed: stagesCompleted,
    relationships_staged: allRels,
    evidence_staged: allEv,
    entities_staged: [],
    review_items: allReview,
    status_summary: {
      propco: ownershipView.verification.propco,
      parent: ownershipView.verification.parent,
      sponsor: ownershipView.verification.ultimate_sponsor,
      operator: ownershipView.verification.operator,
      developer: ownershipView.verification.developer,
    },
    metrics,
    ownership_structure: ownershipStructure,
    webhound_escalation: escalation,
    observations_staged: observations,
    dossier_id: dossier.dossier_id,
    note: "Stage-only: Hotel Property Census Owner Name not written",
    airtable_writes_made: 0,
    relationships_current: finalRels,
  };
}

function emptyStatus() {
  return {
    propco: "unknown",
    parent: "unknown",
    sponsor: "unknown",
    operator: "unknown",
    developer: "unknown",
  };
}

function finalRelsPrecheckHasEconomic(rels) {
  return (rels || []).some(
    (r) =>
      ["SPONSORED_BY", "CONTROLLED_BY"].includes(r.relationship_type) &&
      ["verified", "high", "probable"].includes(String(r.verification_status))
  );
}

function buildExplicitBrazilSeeds(hotel, input) {
  const seeds = [];
  const fromHotel = hotel?.ownership_seeds || hotel?.ownership_seed;
  if (Array.isArray(fromHotel)) {
    for (const s of fromHotel) {
      if (s?.cnpj) seeds.push(s);
    }
  } else if (fromHotel?.cnpj) {
    seeds.push(fromHotel);
  }
  if (Array.isArray(input?.ownership_seeds)) {
    seeds.push(...input.ownership_seeds.filter((s) => s?.cnpj));
  }
  return seeds;
}
