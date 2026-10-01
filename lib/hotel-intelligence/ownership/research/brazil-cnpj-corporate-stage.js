/**
 * Brazil corporate research stage — branch→matrix, QSA, related entities (P1.6 / A′-BR-01).
 */

import {
  fetchCnpjRegistry,
  normalizeCnpjDigits,
  formatCnpj,
} from "./brazil-cnpj-client.js";
import { validateCnpjSeed } from "./brazil-cnpj-seed-validation.js";
import {
  createOwnershipRelationship,
  createRelationshipEvidence,
} from "../schemas.js";
import { resolveOrCreateEntity } from "./entity-resolution.js";
import { scoreOwnershipEvidence } from "../confidence-map.js";
import { createMethodAttempt } from "../discovery-methods.js";
import { discoverHotelCnpjSeed } from "./brazil-hotel-cnpj-discovery.js";

export const BRAZIL_CORPORATE_STAGE_VERSION = "ownership-brazil-corporate-stage-v1";

function partnerEntityType(partner) {
  if (partner.is_pessoa_juridica) return "company";
  return "individual";
}

function partnerIdentifiers(qsaPartner) {
  const doc = String(qsaPartner.documento || "").replace(/\D/g, "");
  if (doc.length === 14) return [{ kind: "cnpj", value: doc, country: "Brazil" }];
  if (doc.length === 11) return [{ kind: "tax_id", value: doc, country: "Brazil" }];
  return [];
}

function matrixEntityIdForCnpj(entities, cnpj) {
  const hit = entities.find((e) =>
    e.identifiers?.some((i) => i.kind === "cnpj" && i.value === cnpj)
  );
  return hit?.entity_id || null;
}

/**
 * @param {object} ctx
 * @param {{ registeredEntities?: object[], cadasturCandidates?: object[] }} [extra]
 */
export async function runBrazilCorporateStage(ctx, extra = {}) {
  const { hotel, ownershipRepository: repo, env } = ctx;
  const started = Date.now();
  const notes = [];
  const methodAttempts = [];
  const metrics = {
    seeds_in: 0,
    seeds_validated: 0,
    seeds_rejected: 0,
    cnpj_lookups: 0,
    matrix_resolved: 0,
    qsa_partners: 0,
    individual_controllers: 0,
    sole_individual_partner: false,
    related_entities: 0,
    parent_candidates: 0,
    propco_candidates: 0,
    branch_relationships: 0,
    control_relationships: 0,
  };

  const country = String(hotel?.country || "").trim();
  if (!/brazil|brasil/i.test(country)) {
    return {
      stage: "brazil_corporate",
      skipped: true,
      candidates: [],
      entities: [],
      entity_relationships: [],
      metrics,
      notes: ["not_brazil"],
      method_attempts: methodAttempts,
    };
  }

  /** @type {object[]} */
  let seedQueue = [];

  for (const raw of extra.explicitSeeds || []) {
    const cnpj = normalizeCnpjDigits(raw.cnpj);
    if (!cnpj) continue;
    metrics.seeds_in += 1;
    const reg = await fetchCnpjRegistry(cnpj, { env });
    metrics.cnpj_lookups += 1;
    if (!reg.ok || !reg.record) continue;
    const validation = validateCnpjSeed(
      hotel,
      {
        legal_name: raw.legal_name || reg.record.razao_social,
        cnpj,
        seed_source: raw.seed_source || "tourism_registry_identity",
      },
      reg.record
    );
    methodAttempts.push(
      createMethodAttempt({
        method: "cnpj_validation",
        success: validation.status === "VALIDATED" || validation.status === "PROBABLE",
        candidate_count: 1,
        notes: [`explicit_seed:${validation.status}`],
        country: "Brazil",
      })
    );
    if (validation.reject_from_ownership_research) {
      metrics.seeds_rejected += 1;
      notes.push(`explicit_seed_rejected:${formatCnpj(cnpj)}:${validation.status}`);
      continue;
    }
    metrics.seeds_validated += 1;
    seedQueue.push({
      cnpj,
      registry: reg.record,
      validation,
      source: raw.seed_source || "tourism_registry_identity",
    });
  }

  for (const ent of extra.registeredEntities || []) {
    const cnpj = ent?.identifiers?.find((i) => i.kind === "cnpj")?.value;
    if (!cnpj) continue;
    metrics.seeds_in += 1;
    const reg = await fetchCnpjRegistry(cnpj, { env });
    metrics.cnpj_lookups += 1;
    if (!reg.ok || !reg.record) {
      notes.push(`seed_lookup_failed:${cnpj}`);
      continue;
    }
    const validation = validateCnpjSeed(
      hotel,
      {
        legal_name: ent.legal_name,
        city: reg.record.municipio,
        uf: reg.record.uf,
        cnpj,
        seed_source: "tourism_registry_identity",
      },
      reg.record
    );
    methodAttempts.push(
      createMethodAttempt({
        method: "cnpj_validation",
        success: validation.status === "VALIDATED" || validation.status === "PROBABLE",
        candidate_count: 1,
        notes: [`status:${validation.status}`, ...validation.reasons],
        country: "Brazil",
      })
    );
    if (validation.reject_from_ownership_research) {
      metrics.seeds_rejected += 1;
      notes.push(`seed_rejected:${formatCnpj(cnpj)}:${validation.status}`);
      continue;
    }
    metrics.seeds_validated += 1;
    seedQueue.push({
      cnpj: normalizeCnpjDigits(cnpj),
      registry: reg.record,
      validation,
      source: "tourism_registry_identity",
    });
  }

  for (const cand of extra.cadasturCandidates || []) {
    const id = cand?.entity_candidate?.identifiers?.find((i) => i.kind === "cnpj")?.value;
    if (!id) continue;
    if (seedQueue.some((s) => s.cnpj === normalizeCnpjDigits(id))) continue;
    metrics.seeds_in += 1;
    const reg = await fetchCnpjRegistry(id, { env });
    metrics.cnpj_lookups += 1;
    if (!reg.ok) continue;
    const validation = validateCnpjSeed(
      hotel,
      {
        legal_name: cand.entity_candidate.legal_name,
        cnpj: id,
        seed_source: "tourism_registry_identity",
      },
      reg.record
    );
    if (validation.reject_from_ownership_research) {
      metrics.seeds_rejected += 1;
      notes.push(`cadastur_seed_rejected:${formatCnpj(id)}`);
      continue;
    }
    metrics.seeds_validated += 1;
    seedQueue.push({
      cnpj: normalizeCnpjDigits(id),
      registry: reg.record,
      validation,
      source: "tourism_registry_identity",
    });
  }

  if (!seedQueue.length) {
    const disc = await discoverHotelCnpjSeed(ctx);
    methodAttempts.push(disc.method_attempt);
    notes.push(...(disc.notes || []));
    for (const s of disc.seeds || []) {
      seedQueue.push({
        cnpj: s.cnpj,
        registry: s.registry,
        validation: s.validation,
        source: "hotel_name_to_company",
      });
      metrics.seeds_validated += 1;
    }
  }

  if (!seedQueue.length) {
    notes.push("brazil_corporate_no_validated_seed");
    return {
      stage: "brazil_corporate",
      skipped: false,
      candidates: [],
      entities: [],
      entity_relationships: [],
      metrics,
      notes,
      method_attempts: methodAttempts,
      latency_ms: Date.now() - started,
    };
  }

  seedQueue.sort(
    (a, b) => (b.validation?.composite_score || 0) - (a.validation?.composite_score || 0)
  );
  const primary = seedQueue[0];
  const branchRecord = primary.registry;
  let matrixRecord = branchRecord;

  if (branchRecord.is_filial && branchRecord.matriz_cnpj) {
    const mat = await fetchCnpjRegistry(branchRecord.matriz_cnpj, { env });
    metrics.cnpj_lookups += 1;
    if (mat.ok && mat.record) {
      matrixRecord = mat.record;
      metrics.matrix_resolved += 1;
      notes.push(
        `branch_to_matrix:${formatCnpj(branchRecord.cnpj)}→${formatCnpj(matrixRecord.cnpj)}`
      );
      methodAttempts.push(
        createMethodAttempt({
          method: "branch_to_matrix",
          success: true,
          candidate_count: 1,
          country: "Brazil",
        })
      );
    }
  } else {
    methodAttempts.push(
      createMethodAttempt({
        method: "branch_to_matrix",
        success: true,
        candidate_count: 1,
        notes: ["already_matriz"],
        country: "Brazil",
      })
    );
  }

  const operatingResolved = await resolveOrCreateEntity(repo, {
    legal_name: branchRecord.razao_social,
    display_name: branchRecord.nome_fantasia || branchRecord.razao_social,
    jurisdiction: "BR",
    country: "Brazil",
    entity_type: "company",
    identifiers: [{ kind: "cnpj", value: branchRecord.cnpj, country: "Brazil" }],
    notes: `seed_validation:${primary.validation.status};source:${primary.source}`,
  });

  const entities = [];
  const entityRelationships = [];
  const candidates = [];

  if (operatingResolved.entity) entities.push(operatingResolved.entity);

  let matrixEntityId = operatingResolved.entity?.entity_id || null;

  if (operatingResolved.entity && matrixRecord.cnpj !== branchRecord.cnpj) {
    const matrixResolved = await resolveOrCreateEntity(repo, {
      legal_name: matrixRecord.razao_social,
      display_name: matrixRecord.nome_fantasia || matrixRecord.razao_social,
      jurisdiction: "BR",
      country: "Brazil",
      entity_type: "company",
      identifiers: [{ kind: "cnpj", value: matrixRecord.cnpj, country: "Brazil" }],
    });
    if (matrixResolved.entity) {
      entities.push(matrixResolved.entity);
      matrixEntityId = matrixResolved.entity.entity_id;
      metrics.branch_relationships += 1;
      const rel = await repo.upsertRelationship(
        createOwnershipRelationship({
          subject_entity_id: operatingResolved.entity.entity_id,
          relationship_type: "CONTROLLED_BY",
          object_entity_id: matrixResolved.entity.entity_id,
          confidence: 0.92,
          verification_status: "high",
          research_run_id: ctx.runId || null,
        })
      );
      entityRelationships.push(rel);
      await repo.addEvidence(
        createRelationshipEvidence({
          relationship_id: rel.relationship_id,
          source: "brasilapi_cnpj",
          source_type: "government",
          source_url: `https://brasilapi.com.br/api/cnpj/v1/${matrixRecord.cnpj}`,
          extracted_claim: `Filial ${formatCnpj(branchRecord.cnpj)} → matriz ${formatCnpj(matrixRecord.cnpj)}`,
          confidence: 0.92,
          source_authority: 0.95,
          extraction_method: "branch_to_matrix",
          researcher: BRAZIL_CORPORATE_STAGE_VERSION,
          notes: "corporate_structure_not_hotel_ownership",
        })
      );
    }
  }

  const qsa = matrixRecord.qsa || [];
  metrics.qsa_partners = qsa.length;
  const companyPartnerCnps = [];
  const controlSubject =
    matrixEntityId || operatingResolved.entity?.entity_id || null;

  for (const p of qsa) {
    if (!p.nome || !controlSubject) continue;
    const et = partnerEntityType(p);
    if (et === "individual") metrics.individual_controllers += 1;

    const resolved = await resolveOrCreateEntity(repo, {
      legal_name: p.nome,
      display_name: p.nome,
      jurisdiction: "BR",
      country: "Brazil",
      entity_type: et,
      identifiers: partnerIdentifiers(p),
    });
    if (!resolved.entity) continue;
    entities.push(resolved.entity);

    const scored = scoreOwnershipEvidence("government", {
      relationshipType: "CONTROLLED_BY",
      relationshipExplicitness: 0.88,
      entityName: p.nome,
      propertyIdentityMatch: 0.5,
      pageBacked: true,
      completeness: 0.8,
    });
    const rel = await repo.upsertRelationship(
      createOwnershipRelationship({
        subject_entity_id: controlSubject,
        relationship_type: "CONTROLLED_BY",
        object_entity_id: resolved.entity.entity_id,
        confidence: Math.min(scored.confidence, et === "individual" ? 0.84 : 0.88),
        verification_status: et === "individual" ? "probable" : scored.verification_status,
        research_run_id: ctx.runId || null,
      })
    );
    entityRelationships.push(rel);
    metrics.control_relationships += 1;
    await repo.addEvidence(
      createRelationshipEvidence({
        relationship_id: rel.relationship_id,
        source: "brasilapi_qsa",
        source_type: "government",
        extracted_claim: `QSA ${p.qualificacao || "sócio"}: ${p.nome}`,
        confidence: rel.confidence,
        source_authority: 0.93,
        extraction_method: "qsa_partner_climb",
        researcher: BRAZIL_CORPORATE_STAGE_VERSION,
        notes: "entity_control_not_hotel_owned_by",
      })
    );

    const doc = String(p.documento || "").replace(/\D/g, "");
    if (doc.length === 14) companyPartnerCnps.push(doc);
  }

  if (metrics.individual_controllers === 1 && metrics.qsa_partners === 1) {
    metrics.sole_individual_partner = true;
  }

  methodAttempts.push(
    createMethodAttempt({
      method: "qsa_partner_climb",
      success: metrics.qsa_partners > 0,
      candidate_count: metrics.qsa_partners,
      relationship_count: metrics.control_relationships,
      country: "Brazil",
    })
  );

  const seenRelated = new Set();
  for (const cnpj of companyPartnerCnps.slice(0, 2)) {
    if (seenRelated.has(cnpj)) continue;
    seenRelated.add(cnpj);
    const reg = await fetchCnpjRegistry(cnpj, { env });
    metrics.cnpj_lookups += 1;
    if (!reg.ok || !reg.record) continue;
    const cnae = String(reg.record.cnae_fiscal || "").slice(0, 4);
    const isPropCoHint =
      cnae === "6810" ||
      /im[oó]veis|incorpora/i.test(reg.record.cnae_fiscal_descricao || "");
    const resolved = await resolveOrCreateEntity(repo, {
      legal_name: reg.record.razao_social,
      display_name: reg.record.nome_fantasia || reg.record.razao_social,
      jurisdiction: "BR",
      country: "Brazil",
      entity_type: "company",
      identifiers: [{ kind: "cnpj", value: reg.record.cnpj, country: "Brazil" }],
      notes: isPropCoHint ? "related_entity_propco_candidate" : "related_entity_qsa_overlap",
    });
    if (resolved.entity) {
      entities.push(resolved.entity);
      metrics.related_entities += 1;
      if (isPropCoHint) metrics.propco_candidates += 1;
      candidates.push({
        kind: "related_entity_candidate",
        entity: resolved.entity,
        propco_hint: isPropCoHint,
        discovery_method: "related_entity_discovery",
        note: "partner_overlap_not_hotel_ownership_proof",
      });
    }
  }

  if (metrics.related_entities) {
    methodAttempts.push(
      createMethodAttempt({
        method: "related_entity_discovery",
        success: true,
        candidate_count: metrics.related_entities,
        country: "Brazil",
      })
    );
  }

  if (metrics.individual_controllers >= 2 && metrics.qsa_partners <= 4) {
    notes.push("family_qsa_pattern_detected");
  }

  return {
    stage: "brazil_corporate",
    skipped: false,
    primary_seed: primary,
    operating_entity: operatingResolved.entity,
    matrix_record: matrixRecord,
    candidates,
    entities,
    entity_relationships: entityRelationships,
    metrics,
    notes,
    method_attempts: methodAttempts,
    latency_ms: Date.now() - started,
  };
}

export async function stageBrazilCorporateResults(_repo, _hotelId, stageResult, _runId) {
  const staged = { entities: [], entity_relationships: [], review: [], candidates: [] };
  for (const e of stageResult.entities || []) {
    if (e?.entity_id) staged.entities.push(e);
  }
  staged.entity_relationships = stageResult.entity_relationships || [];
  staged.candidates = stageResult.candidates || [];

  if (stageResult.primary_seed?.validation?.status === "AMBIGUOUS") {
    staged.review.push({
      issue_type: "weak_ownership_candidate",
      summary: `CNPJ seed AMBIGUOUS: ${stageResult.primary_seed.registry?.razao_social || "unknown"}`,
    });
  }

  for (const c of stageResult.candidates || []) {
    if (c.propco_hint) {
      staged.review.push({
        issue_type: "weak_ownership_candidate",
        summary: `PropCo candidate (not auto OWNED_BY): ${c.entity?.legal_name}`,
      });
    }
  }

  return staged;
}
