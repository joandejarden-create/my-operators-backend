/**
 * Franchisor / public-company financial disclosure lane (P1.6 / A′-BR-03).
 */

import { serpapiSearch } from "../../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../room-count-research/fetch.js";
import { isFetchEligibleUrl } from "../../room-count-research/trust.js";
import {
  createOwnershipRelationship,
  createRelationshipEvidence,
} from "../schemas.js";
import { resolveOrCreateEntity } from "./entity-resolution.js";
import { scoreOwnershipEvidence } from "../confidence-map.js";
import { isPlausibleLegalEntityName } from "./entity-name-guard.js";
import { createMethodAttempt } from "../discovery-methods.js";

export const FRANCHISE_DISCLOSURE_STAGE_VERSION = "ownership-franchise-disclosure-v1";

const BRAND_DOMAINS = Object.freeze({
  accor: ["accor.com", "group.accor.com", "hotelariaaccor.com.br"],
  marriott: ["marriott.com", "marriottinternational.com"],
  hilton: ["hilton.com", "hiltonworldwide.com"],
  ihg: ["ihg.com"],
  hyatt: ["hyatt.com"],
  choice: ["choicehotels.com", "atlanticahotels.com.br"],
  wyndham: ["wyndhamhotels.com"],
  melia: ["melia.com"],
});

const LEASE_PATTERNS = [
  /leased\s+from\s+([A-ZÁÉÍÓÚÑÃÕÂÊÔÀ][\wÁÉÍÓÚÑáéíóúñãõâêôàü&.'’\- ]{3,80})/gi,
  /lessor[:\s]+([A-ZÁÉÍÓÚÑÃÕÂÊÔÀ][\wÁÉÍÓÚÑáéíóúñãõâêôàü&.'’\- ]{3,80})/gi,
  /loca[dç][aã]o\s+(?:de|para)\s+([A-ZÁÉÍÓÚÑÃÕÂÊÔÀ][\wÁÉÍÓÚÑáéíóúñãõâêôàü&.'’\- ]{3,80})/gi,
  /arrendad[oa]\s+(?:de|por|a)\s+([A-ZÁÉÍÓÚÑÃÕÂÊÔÀ][\wÁÉÍÓÚÑáéíóúñãõâêôàü&.'’\- ]{3,80})/gi,
];

function inferBrandKey(hotel) {
  const brand = String(hotel?.brand_name || hotel?.brand || "").toLowerCase();
  const name = String(hotel?.hotel_name || hotel?.name || "").toLowerCase();
  const blob = `${brand} ${name}`;
  if (/accor|ibis|novotel|mercure|pullman|sofitel/.test(blob)) return "accor";
  if (/marriott|sheraton|westin|ritz|courtyard|aloft/.test(blob)) return "marriott";
  if (/hilton|doubletree|embassy|hampton|waldorf/.test(blob)) return "hilton";
  if (/ihg|intercontinental|holiday inn|crowne|kimpton/.test(blob)) return "ihg";
  if (/hyatt|andaz|park hyatt/.test(blob)) return "hyatt";
  if (/choice|sleep inn|comfort suites|quality inn|clarion/.test(blob)) return "choice";
  if (/wyndham|ramada|trademark|la quinta/.test(blob)) return "wyndham";
  if (/meli[aá]|sol melia/.test(blob)) return "melia";
  return null;
}

export function buildFranchiseDisclosureQueries(hotel, brandKey) {
  const hotelName = String(hotel?.hotel_name || hotel?.name || "").trim();
  const city = String(hotel?.city || "").trim();
  const loc = city ? `"${hotelName}" ${city}` : `"${hotelName}"`;
  const queries = [];

  if (brandKey === "accor") {
    queries.push(
      `${loc} site:accor.com OR site:group.accor.com lease hotel`,
      `"Hotelaria Accor Brasil" lease ${hotelName}`,
      `${loc} "Odebrecht" OR "lease" financial statements`
    );
  } else if (brandKey) {
    const domains = BRAND_DOMAINS[brandKey] || [];
    queries.push(
      `${loc} site:${domains[0] || `${brandKey}.com`} lease OR owned OR property`,
      `${loc} ${brandKey} annual report hotel property`
    );
  } else {
    queries.push(`${loc} annual report lease hotel property`, `${loc} 10-K hotel lease lessor`);
  }

  return queries.slice(0, 3);
}

export function extractLeaseDisclosureClaims(text, hotelName) {
  const out = [];
  const body = String(text || "");
  if (!body.trim()) return out;

  const hotelTokens = String(hotelName || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(" ")
    .filter((t) => t.length > 3);
  const bodyLower = body.toLowerCase();

  for (const re of LEASE_PATTERNS) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(body)) !== null) {
      const lessor = String(m[1] || "")
        .replace(/\s+(?:under|pursuant|for|in|at|on).*$/i, "")
        .trim()
        .slice(0, 120);
      if (!lessor || lessor.length < 4) continue;
      if (!isPlausibleLegalEntityName(lessor)) continue;

      if (hotelTokens.length >= 2) {
        const near = bodyLower.slice(
          Math.max(0, m.index - 300),
          m.index + m[0].length + 300
        );
        const hits = hotelTokens.filter((t) => near.includes(t)).length;
        if (hits < Math.min(2, hotelTokens.length)) continue;
      }

      out.push({
        lessor,
        claim_kind: "lease_disclosure",
        quote: String(m[0]).slice(0, 200),
      });
    }
  }

  const seen = new Set();
  return out.filter((c) => {
    const k = c.lessor.toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

export async function runFranchiseDisclosureStage(ctx, extra = {}) {
  const { hotelId, hotel, ownershipRepository: repo, env } = ctx;
  const started = Date.now();
  const notes = [];
  const metrics = {
    searches: 0,
    pages_fetched: 0,
    lease_disclosures: 0,
    owned_by_candidates: 0,
  };
  const candidates = [];
  const relationships = [];
  const evidence = [];

  const allowSerpapi =
    String(env?.OWNERSHIP_RESEARCH_USE_SERPAPI || "1").trim() !== "0" &&
    Boolean(String(env?.SERPAPI_KEY || env?.SERPAPI_API_KEY || "").trim());

  if (!allowSerpapi) {
    notes.push("franchise_disclosure_skipped:serpapi_disabled");
    return {
      stage: "franchise_disclosure",
      candidates,
      relationships,
      evidence,
      metrics,
      notes,
      method_attempt: createMethodAttempt({
        method: "franchisor_financial_disclosure",
        success: false,
        notes: ["serpapi_disabled"],
      }),
    };
  }

  const brandKey = inferBrandKey(hotel);
  const queries = buildFranchiseDisclosureQueries(hotel, brandKey);
  const hotelName = String(hotel?.hotel_name || hotel?.name || "").trim();
  const seenUrls = new Set();
  let fetches = 0;
  const maxFetches = 3;

  for (const q of queries) {
    metrics.searches += 1;
    let serp;
    try {
      serp = await serpapiSearch(
        { engine: "google", q, num: 6, hl: "en", gl: "us" },
        { timeoutMs: 55000, env }
      );
    } catch (err) {
      notes.push(`franchise_serp_error:${String(err?.message || err).slice(0, 50)}`);
      continue;
    }

    for (const row of serp?.organic_results || serp?.results || []) {
      const url = row?.link || row?.url;
      if (!url || seenUrls.has(url) || !isFetchEligibleUrl(url)) continue;
      if (fetches >= maxFetches) break;
      seenUrls.add(url);
      fetches += 1;
      metrics.pages_fetched += 1;

      let page;
      try {
        page = await fetchResearchPage(url, { timeoutMs: 35000, env });
      } catch {
        continue;
      }
      const text = htmlToSearchableText(page?.html || page?.body || "");
      const claims = extractLeaseDisclosureClaims(text, hotelName);
      for (const claim of claims) {
        metrics.lease_disclosures += 1;
        const resolved = await resolveOrCreateEntity(repo, {
          legal_name: claim.lessor,
          display_name: claim.lessor,
          jurisdiction: null,
          entity_type: "company",
        });
        if (!resolved.entity) continue;

        const scored = scoreOwnershipEvidence("securities_filing", {
          relationshipType: "OWNED_BY",
          relationshipExplicitness: 0.82,
          entityName: claim.lessor,
          propertyIdentityMatch: 0.7,
          pageBacked: true,
          completeness: 0.75,
        });

        let verification = scored.verification_status;
        let confidence = scored.confidence;
        if (confidence >= 0.88) {
          verification = "high";
          confidence = Math.min(confidence, 0.9);
        } else {
          verification = "probable";
        }

        const rel = await repo.upsertRelationship(
          createOwnershipRelationship({
            subject_hotel_id: hotelId,
            relationship_type: "OWNED_BY",
            object_entity_id: resolved.entity.entity_id,
            confidence,
            verification_status: verification,
            research_run_id: ctx.runId || null,
          })
        );
        relationships.push(rel);
        metrics.owned_by_candidates += 1;

        const ev = await repo.addEvidence(
          createRelationshipEvidence({
            relationship_id: rel.relationship_id,
            source: "franchise_financial_disclosure",
            source_type: "securities_filing",
            source_url: url,
            extracted_claim: `${claim.quote} [lease/lessor disclosure — verify PropCo vs lessee structure]`,
            confidence,
            source_authority: 0.85,
            extraction_method: "franchisor_financial_disclosure",
            researcher: FRANCHISE_DISCLOSURE_STAGE_VERSION,
            notes: "lessor_may_be_propco; distinguish from operator/franchisee",
          })
        );
        evidence.push(ev);
        candidates.push({
          entity: resolved.entity,
          claim,
          url,
          discovery_method: "franchisor_financial_disclosure",
        });
      }
    }
  }

  return {
    stage: "franchise_disclosure",
    candidates,
    relationships,
    evidence,
    metrics,
    notes,
    brand_key: brandKey,
    method_attempt: createMethodAttempt({
      method: "franchisor_financial_disclosure",
      success: metrics.lease_disclosures > 0,
      candidate_count: metrics.lease_disclosures,
      relationship_count: relationships.length,
      verified_high_count: relationships.filter((r) =>
        ["verified", "high"].includes(String(r.verification_status))
      ).length,
      searches: metrics.searches,
      pages_fetched: metrics.pages_fetched,
      latency_ms: Date.now() - started,
      notes,
    }),
  };
}
