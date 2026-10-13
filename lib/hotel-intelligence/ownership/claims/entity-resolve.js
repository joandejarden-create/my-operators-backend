/**
 * Packet 2.5B — Entity resolution hardening.
 * FALSE MERGES ARE WORSE THAN UNRESOLVED ENTITIES.
 * Reuses Packet 2.5 API; upgrades matching to signal-based decisions.
 */

import { normalizeEntityName } from "../ids.js";
import { createEvidenceAlias } from "./schemas.js";
import { ENTITY_SCOPES } from "./claim-types.js";
import {
  scoreEntityMatch,
  MATCH_DECISIONS,
  ALIAS_STATES,
  RELATION_KINDS,
  normalizeOrgCompareKey,
} from "./er-match-signals.js";
import {
  mayPropagateEntityAcrossHotels,
  classifyEntityScope,
  CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD,
} from "../../research-methods/native/cross-property-entity-guard.js";

export const CLAIM_ENTITY_RESOLVE_VERSION = "claim-entity-resolve-v1.5b";

const NEGATIVE_ANTI = 1.0;

/** Hard anti-merge pairs (normalized substrings). */
export const ANTI_MERGE_PAIRS = Object.freeze([
  ["krystal grand puerto vallarta", "krystal resort puerto vallarta"],
  ["krystal grand", "krystal resort"],
  ["ihvsf", "grupo hotelero santa fe"],
  ["inmobiliaria en hoteleria vallarta santa fe", "grupo hotelero santa fe"],
  ["chable maroma", "chable yucatan"],
  ["chable maroma", "grupo chable"],
  ["chable yucatan", "grupo chable"],
  ["nakheel hotels", "one only palmilla"],
  ["walton street capital mexico", "hilton"],
  ["hyatt", "grupo hotelero santa fe"],
  ["mhkl", "grupo hotelero santa fe"],
  ["fideicomiso mahekal", "grupo hotelero santa fe"],
  ["mhkl", "fideicomiso mahekal"],
  ["mahekal holding", "fideicomiso mahekal"],
  ["breathless", "krystal grand puerto vallarta"],
]);

/**
 * @param {object} registry
 */
export function createEntityResolver(registry = {}) {
  const hotels = indexEntities(registry.hotels || [], "hotel");
  const orgs = indexEntities(registry.organizations || [], "organization");
  const people = indexEntities(registry.people || [], "person");

  return {
    version: CLAIM_ENTITY_RESOLVE_VERSION,
    resolveHotel(raw, context = {}) {
      return resolveAgainst(hotels, raw, { ...context, kind: "hotel" });
    },
    resolveOrganization(raw, context = {}) {
      return resolveAgainst(orgs, raw, { ...context, kind: "organization" });
    },
    resolvePerson(raw, context = {}) {
      return resolveAgainst(people, raw, { ...context, kind: "person" });
    },
    resolveMention(raw, context = {}) {
      const name = String(raw || "").trim();
      if (!name) return unresolved(name, "empty");

      // Announced project / brand names are not hotels unless prefer hotel+exact
      if (/^breathless\b/i.test(name) && context.prefer !== "brand") {
        const brand = resolveAgainst(orgs, "Breathless", {
          ...context,
          kind: "organization",
        });
        if (brand.decision === "MATCHED_HIGH" || brand.decision === "MATCHED_PROBABLE") {
          return {
            ...brand,
            notes: [...(brand.notes || []), "brand_project_not_hotel_identity"],
          };
        }
        return unresolved(name, "announced_project_not_hotel", "BRAND_HOTEL_CONFUSION");
      }

      if (looksLikeHotel(name) || context.prefer === "hotel") {
        const h = resolveAgainst(hotels, name, { ...context, kind: "hotel" });
        if (h.resolved) return h;
        if (h.decision === "AMBIGUOUS") return h;
      }
      if (looksLikePerson(name)) {
        const p = resolveAgainst(people, name, { ...context, kind: "person" });
        if (p.resolved) return p;
      }
      const o = resolveAgainst(orgs, name, { ...context, kind: "organization" });
      if (o.resolved) return o;
      if (o.decision === "AMBIGUOUS") return o;
      const h2 = resolveAgainst(hotels, name, { ...context, kind: "hotel" });
      if (h2.resolved) return h2;
      return unresolved(name, "no_confident_match");
    },
    classifyRelation(a, b) {
      return classifyOrgRelation(a, b);
    },
  };
}

/**
 * Apply ER to accepted claims (Packet 2.5A gate already applied upstream).
 */
export function resolveClaimsEntities(claims, resolver, opts = {}) {
  return (claims || []).map((claim) => {
    const hotelCtx = claim.hotel_context_id || opts.hotel_context_id || null;
    const hotelName = opts.hotel_name || claim.subject_raw;
    let subject = null;
    let object = null;
    let hotelMatch = null;

    const baseCtx = {
      hotel_context_id: hotelCtx,
      hotel_name: hotelName,
      claim_type: claim.claim_type,
      country: "Mexico",
    };

    if (claim.claim_type.startsWith("PERSON_")) {
      subject = resolver.resolvePerson(
        claim.subject_raw || claim.subject_entity_candidate,
        baseCtx
      );
      object = resolver.resolveOrganization(
        claim.object_raw || claim.object_entity_candidate,
        baseCtx
      );
    } else if (
      claim.claim_type.startsWith("HOTEL_") ||
      claim.claim_type.startsWith("PROPERTY_")
    ) {
      hotelMatch = resolver.resolveHotel(claim.subject_raw || hotelName, baseCtx);
      object = resolver.resolveMention(
        claim.object_raw || claim.object_entity_candidate,
        {
          ...baseCtx,
          prefer:
            claim.claim_type === "PROPERTY_IS_ADJACENT_TO" ||
            claim.claim_type === "PROPERTY_ALIAS"
              ? "hotel"
              : claim.claim_type.startsWith("HOTEL_")
                ? "brand"
                : "organization",
        }
      );
      subject = hotelMatch;
    } else {
      object = resolver.resolveOrganization(
        claim.object_raw || claim.object_entity_candidate,
        baseCtx
      );
      if (!object.resolved && object.decision !== "AMBIGUOUS") {
        object = resolver.resolveMention(
          claim.object_raw || claim.object_entity_candidate,
          baseCtx
        );
      }
      subject = resolver.resolveMention(
        claim.subject_raw || claim.subject_entity_candidate,
        baseCtx
      );
      hotelMatch = hotelCtx
        ? resolver.resolveHotel(null, baseCtx)
        : resolver.resolveHotel(claim.subject_raw, baseCtx);
    }

    // Cross-property PropCo guard
    object = applyCrossPropertyGuard(object, hotelName, hotelCtx, claim);
    subject = applyCrossPropertyGuard(subject, hotelName, hotelCtx, claim);

    if (
      subject?.resolved &&
      object?.resolved &&
      subject.entity?.kind === object.entity?.kind &&
      wouldFalseMerge(subject.entity, object.entity)
    ) {
      return {
        ...claim,
        canonical_subject_id: subject.entity_id,
        canonical_object_id: null,
        entity_resolution_confidence: Math.min(subject.confidence || 0.5, 0.4),
        verification_status: "NEEDS_REVIEW",
        subject_resolution: subject,
        object_resolution: {
          ...object,
          resolved: false,
          decision: "REJECTED_MATCH",
          error_code: "FALSE_MERGE",
        },
        derivation_notes: [
          ...(claim.derivation_notes || []),
          "anti_merge_blocked_object",
        ],
      };
    }

    let hotelContextId = hotelCtx;
    if (hotelMatch?.resolved) hotelContextId = hotelMatch.entity_id;

    if (
      claim.claim_type === "PROPERTY_IS_ADJACENT_TO" &&
      hotelMatch?.resolved &&
      object?.resolved &&
      hotelMatch.entity_id === object.entity_id
    ) {
      return {
        ...claim,
        canonical_subject_id: hotelMatch.entity_id,
        canonical_object_id: null,
        hotel_context_id: hotelMatch.entity_id,
        entity_resolution_confidence: 0.3,
        verification_status: "NEEDS_REVIEW",
        derivation_notes: [
          ...(claim.derivation_notes || []),
          "ADJACENT_PROPERTY_collision_blocked",
        ],
        hotel_resolution: hotelMatch,
        object_resolution: object,
      };
    }

    // Only MATCHED_HIGH auto-binds for critical ownership types
    const criticalOwnership = [
      "ENTITY_IS_OWNER_OF_PROPERTY",
      "ENTITY_HOLDS_TITLE",
      "ENTITY_OWNS_PERCENT_OF_ENTITY",
    ].includes(claim.claim_type);

    const bindObject =
      object?.resolved &&
      (!criticalOwnership ||
        object.decision === "MATCHED_HIGH" ||
        object.decision === "MATCHED_PROBABLE");

    const bindSubject =
      subject?.resolved &&
      (subject.decision === "MATCHED_HIGH" ||
        subject.decision === "MATCHED_PROBABLE" ||
        !criticalOwnership);

    const erConf = averageConf([
      bindSubject ? subject?.confidence : null,
      bindObject ? object?.confidence : null,
      hotelMatch?.confidence,
    ]);

    return {
      ...claim,
      canonical_subject_id: bindSubject ? subject.entity_id : null,
      canonical_object_id: bindObject ? object.entity_id : null,
      hotel_context_id: hotelContextId,
      entity_resolution_confidence: erConf,
      verification_status:
        bindSubject || bindObject ? "ENTITY_RESOLVED" : claim.verification_status || "EXTRACTED",
      subject_resolution: subject,
      object_resolution: object,
      hotel_resolution: hotelMatch,
      match_provenance: {
        subject_decision: subject?.decision || null,
        object_decision: object?.decision || null,
        subject_signals: subject?.signals || [],
        object_signals: object?.signals || [],
        subject_negatives: subject?.negatives || [],
        object_negatives: object?.negatives || [],
      },
    };
  });
}

function applyCrossPropertyGuard(resolution, hotelName, hotelCtx, claim) {
  if (!resolution?.resolved || !resolution.entity) return resolution;
  const ent = resolution.entity;
  // Guard applies to property-specific organizations / vehicles — not hotel nodes themselves
  if (ent.kind === "hotel" || ent.kind === "person") return resolution;
  if (ent.scope !== "PROPERTY_SPECIFIC") return resolution;

  // Registry allowlist: explicit hotel binding
  if (Array.isArray(ent.allowed_hotel_ids) && ent.allowed_hotel_ids.length) {
    if (hotelCtx && ent.allowed_hotel_ids.includes(hotelCtx)) {
      return resolution;
    }
    if (hotelCtx && !ent.allowed_hotel_ids.includes(hotelCtx)) {
      return {
        ...resolution,
        resolved: false,
        entity_id: null,
        decision: "REJECTED_MATCH",
        confidence: 0,
        error_code: "SPV_PORTFOLIO_BLEED",
        notes: [
          ...(resolution.notes || []),
          CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD,
          "propco_not_allowed_for_hotel_context",
        ],
      };
    }
  }

  const gate = mayPropagateEntityAcrossHotels({
    entityName: ent.name || ent.legal_name,
    targetHotel: {
      name: hotelName,
      city: claim?.city || ent.city || "",
    },
    evidenceText: claim?.evidence_excerpt || "",
    relationshipKind:
      ent.entity_subtype === "propco" || ent.entity_subtype === "trust"
        ? "propco"
        : "other",
  });
  if (gate && gate.allowed === false) {
    return {
      ...resolution,
      resolved: false,
      entity_id: null,
      decision: "REJECTED_MATCH",
      confidence: 0,
      error_code: "SPV_PORTFOLIO_BLEED",
      notes: [
        ...(resolution.notes || []),
        CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD,
        gate.reason,
      ],
    };
  }
  return resolution;
}

export function aliasesFromRegistry(registry = {}) {
  const out = [];
  for (const e of [
    ...(registry.hotels || []),
    ...(registry.organizations || []),
    ...(registry.people || []),
  ]) {
    for (const a of e.aliases || []) {
      const alias = a.alias || a;
      const state = a.state || "VERIFIED_ALIAS";
      if (state === "REJECTED_ALIAS") continue;
      out.push(
        createEvidenceAlias({
          entity_id: e.id,
          alias,
          alias_type: a.alias_type || "OTHER",
          effective_from: a.effective_from || null,
          effective_to: a.effective_to || null,
          source_id: a.source_id || "registry",
          confidence: a.confidence != null ? a.confidence : state === "VERIFIED_ALIAS" ? 0.95 : 0.6,
          entity_scope: e.scope || "UNKNOWN_SCOPE",
        })
      );
    }
  }
  return out;
}

/**
 * Propose merge candidates (non-destructive).
 */
export function proposeMergeCandidates(registry = {}) {
  const orgs = registry.organizations || [];
  const merges = [];
  for (let i = 0; i < orgs.length; i += 1) {
    for (let j = i + 1; j < orgs.length; j += 1) {
      const a = orgs[i];
      const b = orgs[j];
      if (wouldFalseMerge({ ...a, kind: "organization" }, { ...b, kind: "organization" })) {
        continue;
      }
      const ka = normalizeOrgCompareKey(a.name);
      const kb = normalizeOrgCompareKey(b.name);
      if (ka && kb && ka === kb) {
        merges.push({
          schema_version: "entity-merge-candidate-v1",
          entity_a: a.id,
          entity_b: b.id,
          relation: "SAME_ENTITY",
          confidence: 0.9,
          review_state: "needs_review",
          supporting_evidence: ["normalized_legal_name_equality"],
          conflicting_evidence: [],
        });
      }
    }
  }
  return merges;
}

function indexEntities(list, kind) {
  const byNorm = new Map();
  const byId = new Map();
  for (const e of list) {
    const rec = {
      ...e,
      kind,
      scope: e.scope || "UNKNOWN_SCOPE",
      _norms: new Set(
        [
          e.name,
          e.legal_name,
          e.display_name,
          ...(e.aliases || []).map((a) => a.alias || a),
        ]
          .map((x) => normalizeEntityName(x))
          .filter(Boolean)
      ),
    };
    byId.set(e.id, rec);
    for (const n of rec._norms) {
      if (!byNorm.has(n)) byNorm.set(n, []);
      byNorm.get(n).push(rec);
    }
  }
  return { byNorm, byId, kind };
}

function resolveAgainst(index, raw, context = {}) {
  if (context.hotel_context_id && index.byId.has(context.hotel_context_id) && !raw) {
    const e = index.byId.get(context.hotel_context_id);
    return resolved(e, 0.99, "MATCHED_HIGH", [{ code: "HOTEL_CONTEXT_ID", weight: 1 }], [], "hotel_context_id");
  }

  const name = String(raw || "").trim();
  if (!name) return unresolved(name, "empty");

  const scored = [];
  for (const ent of index.byId.values()) {
    let result = scoreEntityMatch(name, ent, { ...context, kind: index.kind });

    // Anti-merge: raw mention identity vs candidate identity
    if (
      wouldFalseMerge(
        { id: "__raw__", name, kind: index.kind, scope: "UNKNOWN_SCOPE" },
        ent
      )
    ) {
      result = {
        ...result,
        score: result.score - NEGATIVE_ANTI,
        negatives: [
          ...result.negatives,
          { code: "ANTI_MERGE_PAIR", weight: NEGATIVE_ANTI },
        ],
        decision: "REJECTED_MATCH",
      };
    }

    // Anti-merge vs hotel context entity of same kind
    if (context.hotel_context_id && index.kind === "hotel") {
      const ctxEnt = index.byId.get(context.hotel_context_id);
      if (ctxEnt && ent.id !== ctxEnt.id && wouldFalseMerge(ctxEnt, ent)) {
        result = {
          ...result,
          score: result.score - 1,
          negatives: [
            ...result.negatives,
            { code: "ADJACENT_PROPERTY", weight: 0.95 },
          ],
          decision:
            result.decision === "MATCHED_HIGH" ? "AMBIGUOUS" : "REJECTED_MATCH",
        };
      }
    }

    // Scope: ticker/abbrev on PropCo blocked already in scorer
    if (result.decision === "REJECTED_MATCH" && result.score <= 0) continue;
    scored.push({ ent, ...result });
  }

  scored.sort((a, b) => b.score - a.score);
  const top = scored[0];
  if (!top || top.score < 0.35) {
    return unresolved(name, "NEW_ENTITY_CANDIDATE", "UNKNOWN_ENTITY");
  }

  // Ambiguous if top two close
  if (
    scored[1] &&
    scored[1].score >= 0.45 &&
    Math.abs(top.score - scored[1].score) < 0.15 &&
    top.ent.id !== scored[1].ent.id
  ) {
    // Prefer exact/alias over weak
    if (
      !top.signals.some((s) =>
        ["EXACT_NAME", "VERIFIED_ALIAS", "TICKER_MATCH", "FORMER_NAME"].includes(s.code)
      )
    ) {
      return {
        resolved: false,
        entity_id: null,
        entity: null,
        confidence: top.score,
        decision: "AMBIGUOUS",
        method: "ambiguous_candidates",
        error_code: "SIMILAR_NAME_COLLISION",
        signals: top.signals,
        negatives: top.negatives,
        candidates: scored.slice(0, 3).map((s) => ({
          id: s.ent.id,
          name: s.ent.name,
          score: s.score,
          decision: s.decision,
        })),
        raw: name,
      };
    }
  }

  if (top.decision === "AMBIGUOUS" || top.decision === "REJECTED_MATCH") {
    return {
      resolved: false,
      entity_id: null,
      entity: null,
      confidence: top.score,
      decision: top.decision,
      method: top.decision.toLowerCase(),
      error_code:
        top.decision === "REJECTED_MATCH" ? "REJECTED_MATCH" : "AMBIGUOUS",
      signals: top.signals,
      negatives: top.negatives,
      raw: name,
    };
  }

  if (top.decision === "NEW_ENTITY_CANDIDATE") {
    return unresolved(name, "NEW_ENTITY_CANDIDATE", "UNKNOWN_ENTITY");
  }

  // MATCHED_HIGH or MATCHED_PROBABLE
  const conf =
    top.decision === "MATCHED_HIGH"
      ? Math.min(0.99, 0.85 + top.score * 0.1)
      : Math.min(0.84, 0.55 + top.score * 0.2);

  return resolved(
    top.ent,
    conf,
    top.decision,
    top.signals,
    top.negatives,
    "signal_score"
  );
}

export function wouldFalseMerge(a, b) {
  if (!a || !b) return false;
  if (a.id && b.id && a.id === b.id) return false;
  const na = normalizeEntityName(a.name || a.legal_name || "");
  const nb = normalizeEntityName(b.name || b.legal_name || "");
  for (const [x, y] of ANTI_MERGE_PAIRS) {
    if ((na.includes(x) && nb.includes(y)) || (na.includes(y) && nb.includes(x))) {
      return true;
    }
  }
  if (a.kind === "organization" && b.kind === "organization") {
    if (
      (a.scope === "PROPERTY_SPECIFIC" && b.scope === "ORGANIZATION_LEVEL") ||
      (b.scope === "PROPERTY_SPECIFIC" && a.scope === "ORGANIZATION_LEVEL")
    ) {
      return true;
    }
    if (
      (a.scope === "PROPERTY_SPECIFIC" && b.scope === "PORTFOLIO_LEVEL") ||
      (b.scope === "PROPERTY_SPECIFIC" && a.scope === "PORTFOLIO_LEVEL")
    ) {
      return true;
    }
    // PropCo ≠ trust / fideicomiso even at same property
    if (
      (a.entity_subtype === "propco" && b.entity_subtype === "trust") ||
      (a.entity_subtype === "trust" && b.entity_subtype === "propco")
    ) {
      return true;
    }
    if (a.parent_id && a.parent_id === b.id) return true; // related, not same
    if (b.parent_id && b.parent_id === a.id) return true;
  }
  if (a.kind === "hotel" && b.kind === "hotel") {
    // Krystal Grand ≠ Krystal Resort (same brand family, adjacent assets)
    if (/krystal/.test(na) && /krystal/.test(nb)) {
      if (
        (/grand/.test(na) && /resort/.test(nb) && !/grand/.test(nb)) ||
        (/resort/.test(na) && /grand/.test(nb) && !/resort/.test(nb))
      ) {
        return true;
      }
    }
    // Distinct Chablé properties (Maroma ≠ Yucatán), not self-match on same name
    if (/chable/.test(na) && /chable/.test(nb)) {
      const aMaroma = /\bmaroma\b/.test(na);
      const bMaroma = /\bmaroma\b/.test(nb);
      const aYuc = /\byucatan\b/.test(na);
      const bYuc = /\byucatan\b/.test(nb);
      if ((aMaroma && bYuc) || (aYuc && bMaroma)) return true;
    }
  }
  return false;
}

function classifyOrgRelation(a, b) {
  if (!a || !b) return "UNKNOWN_RELATION";
  if (a.id === b.id) return "SAME_ENTITY";
  if (wouldFalseMerge(a, b)) {
    if (a.parent_id === b.id) return "SUBSIDIARY_OF";
    if (b.parent_id === a.id) return "PARENT_OF";
    if (
      a.scope === "PROPERTY_SPECIFIC" &&
      b.scope === "ORGANIZATION_LEVEL"
    ) {
      return "SUBSIDIARY_OF";
    }
    return "UNKNOWN_RELATION";
  }
  if (a.parent_id === b.id) return "SUBSIDIARY_OF";
  if (b.parent_id === a.id) return "PARENT_OF";
  return "UNKNOWN_RELATION";
}

function resolved(entity, confidence, decision, signals, negatives, method) {
  return {
    resolved: decision === "MATCHED_HIGH" || decision === "MATCHED_PROBABLE",
    entity_id: entity.id,
    entity,
    confidence,
    decision,
    method,
    signals,
    negatives,
    error_code: null,
    notes: [],
  };
}

function unresolved(raw, reason, errorCode = null) {
  const decision =
    reason === "NEW_ENTITY_CANDIDATE" ? "NEW_ENTITY_CANDIDATE" : "AMBIGUOUS";
  return {
    resolved: false,
    entity_id: null,
    entity: null,
    confidence: 0,
    decision,
    method: reason,
    raw,
    error_code: errorCode,
    signals: [],
    negatives: [],
    notes: [],
  };
}

function averageConf(vals) {
  const nums = vals.filter((v) => v != null && !Number.isNaN(Number(v))).map(Number);
  if (!nums.length) return null;
  return Math.round((nums.reduce((a, b) => a + b, 0) / nums.length) * 1000) / 1000;
}

function looksLikeHotel(name) {
  return /\b(hotel|resort|grand|inn|suites|palace|hacienda|one\s*&\s*only|waldorf|chabl|mahekal)\b/i.test(
    name
  );
}

function looksLikePerson(name) {
  const parts = String(name || "").trim().split(/\s+/);
  return (
    parts.length >= 2 &&
    parts.length <= 4 &&
    parts.every((p) => /^[A-ZÁÉÍÓÚÑ][a-záéíóúñ'’\-]+$/.test(p))
  );
}

export {
  ENTITY_SCOPES,
  MATCH_DECISIONS,
  ALIAS_STATES,
  RELATION_KINDS,
  classifyEntityScope,
  CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD,
};
