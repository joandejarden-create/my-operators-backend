/**
 * Owner → publicly evidenced hotel portfolio → Census match (P1.7 Strategy B).
 * Bounded reconstruction. No CoStar True Owner / licensed GTM fields.
 * Does not auto-create edges from AMBIGUOUS census matches.
 */

import { FORBIDDEN_EVIDENCE_SOURCES } from "../ontology.js";
import {
  createOwnershipRelationship,
  createRelationshipEvidence,
} from "../schemas.js";
import { resolveOrCreateEntity, addEntityAliasIfDistinct } from "../research/entity-resolution.js";
import { scoreOwnershipEvidence } from "../confidence-map.js";
import { createMethodAttempt } from "../discovery-methods.js";
import { createIntelligenceObservation } from "../../intelligence-observation.js";
import {
  extractPortfolioAssetsFromHtml,
  PORTFOLIO_PAGE_PATHS,
} from "./extract-assets.js";
import { matchPortfolioAssetToCensus, PORTFOLIO_MATCH_STATUS } from "./census-match.js";
import { FIELD_SEMANTICS } from "../research/source-semantics.js";

export const OWNER_PORTFOLIO_RECONSTRUCT_VERSION = "ownership-portfolio-reconstruct-v1";

function assertPublicSource(source) {
  const s = String(source || "").toLowerCase();
  if (FORBIDDEN_EVIDENCE_SOURCES.includes(s) || /costar/.test(s)) {
    throw new Error("costar_firewall:portfolio_reconstruction");
  }
}

async function fetchHtml(url, opts = {}) {
  const timeoutMs = Number(opts.timeoutMs || 15000);
  const fetchImpl = opts.fetchImpl || fetch;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetchImpl(url, {
      signal: controller.signal,
      headers: {
        Accept: "text/html",
        "User-Agent": "DealalityOwnershipResearch/1.7",
      },
      redirect: "follow",
    });
    if (!res.ok) return { ok: false, status: res.status, html: "" };
    const html = await res.text();
    return { ok: true, status: res.status, html };
  } catch (err) {
    return { ok: false, error: String(err?.message || err).slice(0, 120), html: "" };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Optional Playwright render for JS-heavy first-party portfolio pages.
 * Existing Playwright only — not Browser Use.
 */
async function fetchHtmlViaPlaywright(url, opts = {}) {
  const timeoutMs = Number(opts.timeoutMs || 45000);
  try {
    const { chromium } = await import("playwright");
    const browser = await chromium.launch({ headless: true });
    try {
      const page = await browser.newPage({
        userAgent: "DealalityOwnershipResearch/1.7",
      });
      page.setDefaultTimeout(timeoutMs);
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs });
      await page.waitForTimeout(2500);
      const html = await page.content();
      return { ok: true, status: 200, html, via: "playwright" };
    } finally {
      await browser.close();
    }
  } catch (err) {
    return { ok: false, error: String(err?.message || err).slice(0, 120), html: "", via: "playwright" };
  }
}

function portfolioUrlsForOwner(owner, opts = {}) {
  const urls = [];
  for (const u of owner.portfolio_urls || []) {
    if (u) urls.push(u);
  }
  const website = String(owner.website || "").trim();
  const env = opts.env || process.env;
  if (
    website &&
    String(env.OWNERSHIP_PORTFOLIO_URLS_ONLY || "0").trim() !== "1"
  ) {
    let origin = website;
    if (!/^https?:\/\//i.test(origin)) origin = `https://${origin}`;
    try {
      const base = new URL(origin);
      const root = `${base.protocol}//${base.host}`;
      for (const path of PORTFOLIO_PAGE_PATHS) {
        urls.push(path ? `${root}${path}` : root);
      }
    } catch {
      /* ignore */
    }
  } else if (website && !urls.length) {
    urls.push(website);
  }
  return [...new Set(urls)].slice(0, Number(opts.maxUrls || 8));
}

/**
 * @param {object} input
 */
export async function reconstructOwnerPortfolio(input = {}) {
  const started = Date.now();
  const owner = input.owner || {};
  const repo = input.ownershipRepository;
  const censusRecords = input.censusRecords || [];
  const env = input.env || process.env;
  assertPublicSource(owner.source || "first_party");

  const notes = [];
  const observations = [];
  const assetsDiscovered = [];
  const matches = [];
  const relationships = [];
  const evidence = [];
  const entities = [];

  if (!repo) {
    return { ok: false, error: "ownership_repository_required" };
  }
  if (!owner.legal_name && !owner.display_name) {
    return { ok: false, error: "owner_name_required" };
  }

  const resolvedOwner = await resolveOrCreateEntity(repo, {
    legal_name: owner.legal_name || owner.display_name,
    display_name: owner.display_name || owner.legal_name,
    entity_type: owner.entity_type || "company",
    jurisdiction: owner.jurisdiction || owner.country || null,
    identifiers: owner.identifiers || [],
    lei: owner.lei || null,
  });
  if (resolvedOwner.ambiguity || !resolvedOwner.entity) {
    return {
      ok: false,
      error: "owner_entity_ambiguous",
      notes: resolvedOwner.notes,
    };
  }
  const ownerEntity = resolvedOwner.entity;
  entities.push(ownerEntity);
  if (owner.display_name) {
    await addEntityAliasIfDistinct(
      repo,
      ownerEntity.entity_id,
      owner.display_name,
      "owner_portfolio"
    );
  }

  const htmlByUrl = input.htmlByUrl || {};
  const urls = portfolioUrlsForOwner(owner, {
    env,
    maxUrls: Number(input.maxUrls || 4),
  });
  let pagesFetched = 0;

  for (const url of urls) {
    let html = htmlByUrl[url];
    if (!html) {
      let fetched = await fetchHtml(url, {
        fetchImpl: input.fetchImpl,
        timeoutMs: input.timeoutMs,
      });
      const playwrightOn =
        String(env?.OWNERSHIP_PORTFOLIO_PLAYWRIGHT || "0").trim() === "1";
      if (
        playwrightOn &&
        (!fetched.ok || (fetched.html || "").replace(/<[^>]+>/g, " ").trim().length < 800)
      ) {
        const rendered = await fetchHtmlViaPlaywright(url, { timeoutMs: input.timeoutMs });
        if (rendered.ok) {
          fetched = rendered;
          notes.push(`portfolio_playwright_render:${url}`);
        }
      }
      if (!fetched.ok) {
        notes.push(`portfolio_fetch_failed:${url}:${fetched.status || fetched.error}`);
        continue;
      }
      html = fetched.html;
    }
    pagesFetched += 1;
    const extracted = extractPortfolioAssetsFromHtml(html, {
      pageUrl: url,
      relationshipPrior: owner.relationship_prior || null,
      pageTextPrior: owner.legal_name,
    });
    notes.push(`portfolio_page:${url}:assets=${extracted.assets.length}`);
    for (const asset of extracted.assets) {
      assetsDiscovered.push({ ...asset, source_url: asset.source_url || url });
    }
  }

  const seenAsset = new Set();
  const uniqueAssets = [];
  for (const a of assetsDiscovered) {
    const k = String(a.name || "").toLowerCase();
    if (!k || seenAsset.has(k)) continue;
    seenAsset.add(k);
    uniqueAssets.push(a);
  }

  let exactStrong = 0;
  let probable = 0;
  let ambiguous = 0;
  let notFound = 0;
  let edgesAdded = 0;
  let falseMatchReview = 0;

  const sem = FIELD_SEMANTICS["owner_portfolio.asset_list"];

  for (const asset of uniqueAssets) {
    const matched = matchPortfolioAssetToCensus(asset, censusRecords, {
      idRegistry: input.idRegistry,
      store: input.store,
    });
    matches.push({ asset, match: matched });

    if (matched.match_status === PORTFOLIO_MATCH_STATUS.EXACT) exactStrong += 1;
    else if (matched.match_status === PORTFOLIO_MATCH_STATUS.STRONG) exactStrong += 1;
    else if (matched.match_status === PORTFOLIO_MATCH_STATUS.PROBABLE) probable += 1;
    else if (matched.match_status === PORTFOLIO_MATCH_STATUS.AMBIGUOUS) {
      ambiguous += 1;
      falseMatchReview += 1;
    } else notFound += 1;

    const cls = asset.classification || {};
    const findingBits = [];
    if (cls.temporal_status && cls.temporal_status !== "current") {
      findingBits.push(`temporal:${cls.temporal_status}`);
    }
    if (cls.relationship_type === "INVESTED_IN_BY") {
      findingBits.push("described as investment");
    }
    if (cls.relationship_type === "LEASED_FROM" || cls.reasons?.includes("lease_language")) {
      findingBits.push("described as leased");
    }
    if (cls.relationship_type === "JV_WITH") {
      findingBits.push("JV language");
    }

    if (cls.observation_only || !matched.auto_edge_eligible) {
      observations.push(
        createIntelligenceObservation({
          subject_type: matched.hotel_id ? "hotel" : "entity",
          subject_id: matched.hotel_id || ownerEntity.entity_id,
          observation_type:
            cls.temporal_status === "sold" ? "disposition" : "portfolio_inclusion",
          intelligence_class: "intelligence_observation",
          finding: `${owner.display_name || owner.legal_name} portfolio lists "${asset.name}" as ${cls.relationship_type || "unspecified"}${findingBits.length ? ` (${findingBits.join("; ")})` : ""}`,
          source_url: asset.source_url,
          source_provider: "owner_first_party",
          verification_status: "probable",
          confidence: 0.55,
          research_method: "owner_portfolio",
          product_truth: false,
          intended_relationship_type: cls.relationship_type,
          related_names: [asset.name, owner.legal_name].filter(Boolean),
          historical: cls.temporal_status === "sold" || cls.is_current === false,
        })
      );
      continue;
    }

    const relType = cls.relationship_type;
    if (!relType || relType === "INVESTED_IN_BY" || relType === "LEASED_FROM") {
      observations.push(
        createIntelligenceObservation({
          subject_type: "hotel",
          subject_id: matched.hotel_id,
          observation_type: relType === "LEASED_FROM" ? "unconfirmed_lease" : "investment",
          intelligence_class: "canonical_relationship_candidate",
          finding: `Portfolio listing "${asset.name}" classified ${relType} — reserved / not auto-edged`,
          source_url: asset.source_url,
          source_provider: "owner_first_party",
          research_method: "owner_portfolio",
          intended_relationship_type: relType,
          product_truth: false,
        })
      );
      continue;
    }

    const scored = scoreOwnershipEvidence("first_party", {
      relationshipType: relType,
      relationshipExplicitness: relType === "OWNED_BY" ? 0.78 : 0.82,
      entityName: owner.legal_name || owner.display_name,
      propertyIdentityMatch:
        matched.match_status === PORTFOLIO_MATCH_STATUS.EXACT ? 0.95 : 0.82,
      temporalRelevance: cls.is_current ? 0.9 : 0.3,
      corroborationCount: 0,
      pageBacked: true,
    });

    const rel = createOwnershipRelationship({
      subject_hotel_id: matched.hotel_id,
      relationship_type: relType,
      object_entity_id: ownerEntity.entity_id,
      confidence: scored.confidence,
      verification_status:
        scored.verification_status === "verified" ? "high" : scored.verification_status,
      is_current: cls.is_current !== false,
      research_run_id: input.research_run_id || null,
    });
    const ev = createRelationshipEvidence({
      relationship_id: rel.relationship_id,
      source: "owner_first_party",
      source_url: asset.source_url,
      source_type: "first_party",
      extracted_claim: `${owner.legal_name} first-party portfolio lists ${asset.name} (${relType})`,
      source_authority: scored.source_authority ?? 0.88,
      confidence: scored.confidence,
      extraction_method: "owner_portfolio",
      notes: sem?.notes || "First-party portfolio listing — classify owned vs managed vs developed",
    });

    await repo.upsertRelationship(rel);
    await repo.addEvidence(ev);
    relationships.push(rel);
    evidence.push(ev);
    edgesAdded += 1;
  }

  for (const obs of observations) {
    if (typeof repo.addObservation === "function") {
      await repo.addObservation(obs);
    }
  }

  const matchedToCensus = exactStrong + probable;
  const incrementalHotels = edgesAdded;
  const hotelsResolvedPerOwner = incrementalHotels;

  const methodAttempt = createMethodAttempt({
    method: "owner_portfolio",
    success: uniqueAssets.length > 0,
    candidate_count: uniqueAssets.length,
    relationship_count: edgesAdded,
    verified_high_count: relationships.filter((r) =>
      ["verified", "high"].includes(r.verification_status)
    ).length,
    pages_fetched: pagesFetched,
    latency_ms: Date.now() - started,
    country: owner.country || null,
    notes,
  });

  return {
    ok: true,
    version: OWNER_PORTFOLIO_RECONSTRUCT_VERSION,
    owner_entity: ownerEntity,
    assets_discovered: uniqueAssets.length,
    assets: uniqueAssets,
    census_matches: {
      exact_or_strong: exactStrong,
      probable,
      ambiguous,
      not_found: notFound,
      matched_to_census: matchedToCensus,
    },
    matches,
    relationships,
    evidence,
    observations,
    entities,
    metrics: {
      hotels_resolved_per_owner: hotelsResolvedPerOwner,
      incremental_hotels_resolved: incrementalHotels,
      false_match_review_count: falseMatchReview,
      pages_fetched: pagesFetched,
      latency_ms: Date.now() - started,
      cost_usd: 0,
    },
    method_attempt: methodAttempt,
    notes,
    env_note: env?.OWNERSHIP_WEBHOUND_AUTHORIZED,
  };
}
