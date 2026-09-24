/**
 * Canonical ADP/GDI Airtable base resolution — fail-closed for production writes.
 *
 * Canonical intelligence base: appa2cE7FTRmIbB32
 * Legacy MVP (forbidden): appvtnDurnMSjINP6
 */

import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
  isLegacyMvpBase,
  assertNotLegacyMvpCanonicalBase,
  getGdiOpportunitiesAirtableBaseId as getGdiBaseIdLoose,
} from "../decision-outcomes/airtable-base.js";

export {
  CANONICAL_INTELLIGENCE_BASE_ID,
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
  isLegacyMvpBase,
  assertNotLegacyMvpCanonicalBase,
};

/**
 * @returns {"SET_CANONICAL"|"SET_NONCANONICAL"|"MISSING"}
 */
export function classifyBaseConfig(baseId) {
  const id = String(baseId || "").trim();
  if (!id) return "MISSING";
  if (id === CANONICAL_INTELLIGENCE_BASE_ID) return "SET_CANONICAL";
  return "SET_NONCANONICAL";
}

/**
 * Resolve ADP Live Published Reports base.
 * Prefer ADP_AIRTABLE_BASE_ID, then intelligence/GDI canonical envs.
 * Fail closed unless a positively canonical base is found (never silent product-base fallback).
 */
export function resolveAdpCanonicalBaseId(env = process.env, opts = {}) {
  const chain = [
    ["ADP_AIRTABLE_BASE_ID", env.ADP_AIRTABLE_BASE_ID],
    ["AIRTABLE_INTELLIGENCE_BASE_ID", env.AIRTABLE_INTELLIGENCE_BASE_ID],
    ["AIRTABLE_GDI_BASE_ID", env.AIRTABLE_GDI_BASE_ID],
  ];
  for (const [source, raw] of chain) {
    const id = String(raw || "").trim();
    if (!id) continue;
    assertNotLegacyMvpCanonicalBase(id, { surface: "adp_published_reports" });
    return { baseId: id, source, status: classifyBaseConfig(id) };
  }

  const fallback = String(env.AIRTABLE_BASE_ID || "").trim();
  const allowFallback =
    opts.allowGenericFallback === true ||
    String(env.ADP_ALLOW_GENERIC_AIRTABLE_BASE_FALLBACK || "").trim() === "1";

  if (fallback === CANONICAL_INTELLIGENCE_BASE_ID) {
    return {
      baseId: fallback,
      source: "AIRTABLE_BASE_ID_verified_canonical",
      status: "SET_CANONICAL",
    };
  }

  if (allowFallback && fallback && !isLegacyMvpBase(fallback)) {
    return {
      baseId: fallback,
      source: "AIRTABLE_BASE_ID_allowed_fallback",
      status: classifyBaseConfig(fallback),
    };
  }

  const err = new Error(
    `ADP_CANONICAL_BASE_MISSING: Set ADP_AIRTABLE_BASE_ID=${CANONICAL_INTELLIGENCE_BASE_ID} (fail-closed; refusing silent fallback to non-canonical AIRTABLE_BASE_ID).`
  );
  err.code = "ADP_CANONICAL_BASE_MISSING";
  err.statusCode = 500;
  throw err;
}

/**
 * Resolve GDI canonical base. Prefer AIRTABLE_GDI_BASE_ID / intelligence envs.
 * Fail closed if only non-canonical AIRTABLE_BASE_ID would be used.
 */
export function resolveGdiCanonicalBaseId(env = process.env, opts = {}) {
  const chain = [
    ["AIRTABLE_GDI_BASE_ID", env.AIRTABLE_GDI_BASE_ID],
    ["GDI_OPPORTUNITIES_AIRTABLE_BASE_ID", env.GDI_OPPORTUNITIES_AIRTABLE_BASE_ID],
    ["AIRTABLE_INTELLIGENCE_BASE_ID", env.AIRTABLE_INTELLIGENCE_BASE_ID],
    ["ADP_AIRTABLE_BASE_ID", env.ADP_AIRTABLE_BASE_ID],
  ];
  for (const [source, raw] of chain) {
    const id = String(raw || "").trim();
    if (!id) continue;
    assertNotLegacyMvpCanonicalBase(id, { surface: "gdi_canonical" });
    return { baseId: id, source, status: classifyBaseConfig(id) };
  }

  const fallback = String(env.AIRTABLE_BASE_ID || "").trim();
  const allowFallback =
    opts.allowGenericFallback === true ||
    String(env.GDI_ALLOW_GENERIC_AIRTABLE_BASE_FALLBACK || "").trim() === "1";

  if (fallback === CANONICAL_INTELLIGENCE_BASE_ID) {
    return {
      baseId: fallback,
      source: "AIRTABLE_BASE_ID_verified_canonical",
      status: "SET_CANONICAL",
    };
  }

  if (allowFallback && fallback && !isLegacyMvpBase(fallback)) {
    assertNotLegacyMvpCanonicalBase(fallback, { surface: "gdi_canonical" });
    return {
      baseId: fallback,
      source: "AIRTABLE_BASE_ID_allowed_fallback",
      status: classifyBaseConfig(fallback),
    };
  }

  // Preserve loose getter for diagnostics only
  const loose = getGdiBaseIdLoose();
  const err = new Error(
    `GDI_CANONICAL_BASE_MISSING: Set AIRTABLE_GDI_BASE_ID=${CANONICAL_INTELLIGENCE_BASE_ID} (fail-closed; refusing silent non-canonical fallback${loose ? ` would_have_been=${loose}` : ""}).`
  );
  err.code = "GDI_CANONICAL_BASE_MISSING";
  err.statusCode = 500;
  throw err;
}

/**
 * Env config report — never returns secret values.
 */
export function reportCanonicalBaseEnv(env = process.env) {
  const keys = [
    "ADP_AIRTABLE_BASE_ID",
    "AIRTABLE_GDI_BASE_ID",
    "GDI_OPPORTUNITIES_AIRTABLE_BASE_ID",
    "AIRTABLE_INTELLIGENCE_BASE_ID",
    "AIRTABLE_BASE_ID",
  ];
  const report = {};
  for (const k of keys) {
    report[k] = classifyBaseConfig(env[k]);
  }
  let adp = { status: "UNKNOWN", error: null };
  let gdi = { status: "UNKNOWN", error: null };
  try {
    adp = { ...resolveAdpCanonicalBaseId(env), error: null };
  } catch (e) {
    adp = { status: "MISSING", error: e.code || e.message, baseId: null, source: null };
  }
  try {
    gdi = { ...resolveGdiCanonicalBaseId(env), error: null };
  } catch (e) {
    gdi = { status: "MISSING", error: e.code || e.message, baseId: null, source: null };
  }
  return {
    canonicalExpected: CANONICAL_INTELLIGENCE_BASE_ID,
    legacyForbidden: LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
    envKeys: report,
    adpResolve: {
      status: adp.status,
      source: adp.source || null,
      ok: adp.status === "SET_CANONICAL",
      error: adp.error || null,
    },
    gdiResolve: {
      status: gdi.status,
      source: gdi.source || null,
      ok: gdi.status === "SET_CANONICAL",
      error: gdi.error || null,
    },
  };
}
