/**
 * Brand Explorer brand target resolver v28C.
 *
 * Resolves CLI/API brand inputs to Brand Setup - Brand Basics record IDs for
 * active registry brands, v28B expansion backlog slugs, and live lookups.
 *
 * @see docs/data-intelligence/brand-explorer-complete-build-orchestrator.md
 */
import { fetchAllRecordsRest } from "../../api/lib/operator-setup-new-base-read.js";
import { PARTNER_INTELLIGENCE_LINKS } from "../../api/lib/partner-intelligence-field-map.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import {
  EXPANSION_BACKLOG_SEEDS,
  slugifyBrandName,
} from "./brand-explorer-expansion-backlog-planner.js";

export const RESOLVER_VERSION = "v28C";

const BRAND_BASICS_TABLE = PARTNER_INTELLIGENCE_LINKS.brandBasics;
const RECORD_ID_RE = /^rec[a-zA-Z0-9]{10,}$/;

let cachedContext = null;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function fieldStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (Array.isArray(v) && v.length) return fieldStr(v[0]);
  if (typeof v === "object" && v.name) return nz(v.name);
  return nz(v);
}

export class AmbiguousBrandResolutionError extends Error {
  constructor(inputTarget, candidates = []) {
    super(`Ambiguous brand target: ${inputTarget}`);
    this.name = "AmbiguousBrandResolutionError";
    this.inputTarget = inputTarget;
    this.candidates = candidates;
  }
}

function toResolvedTarget({
  inputTarget,
  resolvedBrandName,
  resolvedRecordId,
  resolvedSlug,
  resolutionSource,
  parentCompany = null,
  parentFamily = null,
  wave = null,
  ambiguous = false,
  error = null,
  suggestedMatches = [],
}) {
  return {
    slug: resolvedSlug,
    recordId: resolvedRecordId,
    name: resolvedBrandName,
    resolution: {
      inputTarget,
      resolvedBrandName,
      resolvedRecordId,
      resolvedSlug,
      resolutionSource,
      parentCompany: parentCompany || null,
      parentFamily: parentFamily || null,
      wave: wave ?? null,
      ambiguous,
      error,
      suggestedMatches,
    },
  };
}

function activeTargetToResolved(active, resolutionSource, liveEntry = null) {
  return toResolvedTarget({
    inputTarget: active.slug,
    resolvedBrandName: active.name,
    resolvedRecordId: active.recordId,
    resolvedSlug: active.slug,
    resolutionSource,
    parentCompany: liveEntry?.parentCompany || null,
  });
}

function liveEntryToResolved(inputTarget, entry, resolutionSource, extra = {}) {
  return toResolvedTarget({
    inputTarget,
    resolvedBrandName: entry.name,
    resolvedRecordId: entry.recordId,
    resolvedSlug: entry.slug || slugifyBrandName(entry.name),
    resolutionSource,
    parentCompany: entry.parentCompany || null,
    ...extra,
  });
}

function pickUnique(inputTarget, candidates, resolutionSource) {
  const unique = dedupeCandidates(candidates);
  if (unique.length === 1) {
    return liveEntryToResolved(inputTarget, unique[0], resolutionSource);
  }
  if (unique.length > 1) {
    throw new AmbiguousBrandResolutionError(inputTarget, unique);
  }
  return null;
}

function dedupeCandidates(candidates) {
  const seen = new Set();
  const out = [];
  for (const c of candidates) {
    if (!c?.recordId || seen.has(c.recordId)) continue;
    seen.add(c.recordId);
    out.push(c);
  }
  return out;
}

function closestMatches(input, live, limit = 5) {
  const needle = slugifyBrandName(input);
  const normNeedle = needle.replace(/-/g, "");
  const scored = live.allEntries
    .map((entry) => {
      const slug = entry.slug || slugifyBrandName(entry.name);
      const slugScore = slug === needle ? 100 : slug.includes(needle) || needle.includes(slug) ? 70 : 0;
      const nameScore = entry.name.toLowerCase().includes(input.toLowerCase()) ? 60 : 0;
      const normSlug = slug.replace(/-/g, "");
      const normScore =
        normSlug === normNeedle ? 90 : normSlug.includes(normNeedle) || normNeedle.includes(normSlug) ? 50 : 0;
      return { entry, score: Math.max(slugScore, nameScore, normScore) };
    })
    .filter((row) => row.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit)
    .map((row) => ({
      recordId: row.entry.recordId,
      name: row.entry.name,
      slug: row.entry.slug,
      parentCompany: row.entry.parentCompany || null,
      score: row.score,
    }));
  return scored;
}

async function loadBrandBasicsIndex() {
  const records = await fetchAllRecordsRest(BRAND_BASICS_TABLE);
  const byRecordId = new Map();
  const byExactNameLower = new Map();
  const bySlug = new Map();
  const allEntries = [];

  for (const rec of records) {
    const name = fieldStr(rec.fields?.["Brand Name"] || rec.fields?.brand_name);
    if (!name) continue;
    const slug = slugifyBrandName(name);
    const parentCompany = fieldStr(rec.fields?.["Parent Company"]);
    const entry = { recordId: rec.id, name, slug, parentCompany };
    allEntries.push(entry);
    byRecordId.set(rec.id, entry);

    const exactKey = name.toLowerCase();
    if (!byExactNameLower.has(exactKey)) byExactNameLower.set(exactKey, []);
    byExactNameLower.get(exactKey).push(entry);

    if (!bySlug.has(slug)) bySlug.set(slug, []);
    bySlug.get(slug).push(entry);
  }

  return { byRecordId, byExactNameLower, bySlug, allEntries };
}

function buildExpansionIndex(live) {
  const bySlug = new Map();
  const byBrandNameLower = new Map();

  for (const seed of EXPANSION_BACKLOG_SEEDS) {
    const proposedSlug = slugifyBrandName(seed.brandName);
    const exactMatches = live.byExactNameLower.get(seed.brandName.toLowerCase()) || [];
    const slugMatches = live.bySlug.get(proposedSlug) || [];
    const candidates = dedupeCandidates([...exactMatches, ...slugMatches]);
    const match = candidates[0] || null;

    const row = {
      brandName: seed.brandName,
      proposedSlug,
      recordId: match?.recordId || null,
      parentFamily: seed.parentFamily,
      wave: seed.likelyFactoryWave,
      parentCompany: match?.parentCompany || null,
      liveName: match?.name || null,
    };
    bySlug.set(proposedSlug, row);
    byBrandNameLower.set(seed.brandName.toLowerCase(), row);
  }

  return { bySlug, byBrandNameLower };
}

export function clearBrandTargetResolverCache() {
  cachedContext = null;
}

export async function getBrandTargetResolverContext(options = {}) {
  if (cachedContext && !options.refresh) return cachedContext;
  const live = await loadBrandBasicsIndex();
  const expansion = buildExpansionIndex(live);
  cachedContext = { live, expansion };
  return cachedContext;
}

export async function resolveBrandTarget(input, context = null) {
  const ctx = context || (await getBrandTargetResolverContext());
  const raw = nz(input);
  if (!raw) {
    return toResolvedTarget({
      inputTarget: raw,
      resolvedBrandName: "",
      resolvedRecordId: "",
      resolvedSlug: "",
      resolutionSource: null,
      error: "empty_brand_target",
    });
  }

  const normalized = raw.toLowerCase();
  const inputSlug = slugifyBrandName(raw);

  try {
    if (RECORD_ID_RE.test(raw)) {
      const live = ctx.live.byRecordId.get(raw);
      if (live) {
        return liveEntryToResolved(raw, live, "record_id");
      }
      return toResolvedTarget({
        inputTarget: raw,
        resolvedBrandName: raw,
        resolvedRecordId: raw,
        resolvedSlug: inputSlug || raw,
        resolutionSource: "record_id",
      });
    }

    const byActiveSlug = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === normalized || b.slug === inputSlug);
    if (byActiveSlug) {
      return activeTargetToResolved(byActiveSlug, "active_registry", ctx.live.byRecordId.get(byActiveSlug.recordId));
    }

    const byActiveId = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.recordId === raw);
    if (byActiveId) {
      return activeTargetToResolved(byActiveId, "active_registry", ctx.live.byRecordId.get(byActiveId.recordId));
    }

    const byActiveName = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.name.toLowerCase() === normalized);
    if (byActiveName) {
      return activeTargetToResolved(byActiveName, "active_registry", ctx.live.byRecordId.get(byActiveName.recordId));
    }

    const expansionByInputSlug = ctx.expansion.bySlug.get(inputSlug);
    if (expansionByInputSlug?.recordId) {
      const live = ctx.live.byRecordId.get(expansionByInputSlug.recordId);
      return toResolvedTarget({
        inputTarget: raw,
        resolvedBrandName: live?.name || expansionByInputSlug.liveName || expansionByInputSlug.brandName,
        resolvedRecordId: expansionByInputSlug.recordId,
        resolvedSlug: expansionByInputSlug.proposedSlug,
        resolutionSource: "expansion_backlog",
        parentCompany: expansionByInputSlug.parentCompany || live?.parentCompany || null,
        parentFamily: expansionByInputSlug.parentFamily,
        wave: expansionByInputSlug.wave,
      });
    }

    const exactLive = pickUnique(raw, ctx.live.byExactNameLower.get(normalized) || [], "exact_name");
    if (exactLive) return exactLive;

    const slugLive = pickUnique(raw, ctx.live.bySlug.get(inputSlug) || [], "live_brand_basics");
    if (slugLive) return slugLive;

    const expansion =
      ctx.expansion.bySlug.get(inputSlug) ||
      ctx.expansion.byBrandNameLower.get(normalized) ||
      null;
    if (expansion?.recordId) {
      const live = ctx.live.byRecordId.get(expansion.recordId);
      return toResolvedTarget({
        inputTarget: raw,
        resolvedBrandName: live?.name || expansion.liveName || expansion.brandName,
        resolvedRecordId: expansion.recordId,
        resolvedSlug: expansion.proposedSlug,
        resolutionSource: "expansion_backlog",
        parentCompany: expansion.parentCompany || live?.parentCompany || null,
        parentFamily: expansion.parentFamily,
        wave: expansion.wave,
      });
    }

    if (expansion && !expansion.recordId) {
      const fallback = pickUnique(
        raw,
        dedupeCandidates([
          ...(ctx.live.byExactNameLower.get(expansion.brandName.toLowerCase()) || []),
          ...(ctx.live.bySlug.get(expansion.proposedSlug) || []),
        ]),
        "expansion_backlog"
      );
      if (fallback) {
        return {
          ...fallback,
          resolution: {
            ...fallback.resolution,
            parentFamily: expansion.parentFamily,
            wave: expansion.wave,
          },
        };
      }
    }

    const fuzzyCandidates = dedupeCandidates([
      ...(ctx.live.bySlug.get(inputSlug) || []),
      ...ctx.live.allEntries.filter((entry) => slugifyBrandName(entry.name) === inputSlug),
      ...ctx.live.allEntries.filter((entry) => entry.name.toLowerCase().includes(normalized)),
    ]);
    const fuzzy = pickUnique(raw, fuzzyCandidates, "live_brand_basics");
    if (fuzzy) return fuzzy;
  } catch (err) {
    if (err instanceof AmbiguousBrandResolutionError) {
      return toResolvedTarget({
        inputTarget: raw,
        resolvedBrandName: "",
        resolvedRecordId: "",
        resolvedSlug: inputSlug,
        resolutionSource: "live_brand_basics",
        ambiguous: true,
        error: err.message,
        suggestedMatches: err.candidates.map((c) => ({
          recordId: c.recordId,
          name: c.name,
          slug: c.slug,
          parentCompany: c.parentCompany || null,
        })),
      });
    }
    throw err;
  }

  return toResolvedTarget({
    inputTarget: raw,
    resolvedBrandName: raw,
    resolvedRecordId: raw,
    resolvedSlug: inputSlug,
    resolutionSource: null,
    error: `brand_not_found:${raw}`,
    suggestedMatches: closestMatches(raw, ctx.live),
  });
}

export async function resolveOrchestratorBrandTargets(options = {}) {
  if (options.allActive) {
    const ctx = await getBrandTargetResolverContext();
    return ACTIVE_BRAND_AUDIT_TARGETS.map((active) =>
      activeTargetToResolved(active, "active_registry", ctx.live.byRecordId.get(active.recordId))
    );
  }

  const brandList = Array.isArray(options.brands)
    ? options.brands.map((b) => nz(b)).filter(Boolean)
    : nz(options.brands || "")
        .split(",")
        .map((b) => b.trim())
        .filter(Boolean);

  const inputs = brandList.length
    ? brandList
    : [nz(options.brandIdOrName || "tribute-portfolio")].filter(Boolean);

  const ctx = await getBrandTargetResolverContext();
  return Promise.all(inputs.map((input) => resolveBrandTarget(input, ctx)));
}

export function isResolvableBrandTarget(target) {
  return Boolean(
    target?.recordId &&
      RECORD_ID_RE.test(target.recordId) &&
      !target?.resolution?.ambiguous &&
      !target?.resolution?.error
  );
}
