/**
 * Dev/runtime guard: assert Brand AI Visibility Express routes are registered
 * before the generic /api 404. Helps catch stale Node processes after route adds.
 */

export const BRAND_AI_VISIBILITY_EXPECTED_ROUTES = Object.freeze([
  { method: "get", path: "/api/ai-visibility/brand/portfolio" },
  { method: "get", path: "/api/ai-visibility/brand/executive-summary" },
  { method: "get", path: "/api/ai-visibility/brand/:brandId/overview" },
  { method: "get", path: "/api/ai-visibility/brand/:brandId/trend" },
  { method: "get", path: "/api/ai-visibility/brand/:brandId/questions" },
  { method: "get", path: "/api/ai-visibility/brand/:brandId/competitors" },
  { method: "get", path: "/api/ai-visibility/brand/:brandId/sources" },
  { method: "get", path: "/api/ai-visibility/brand/:brandId/benchmark" },
  { method: "get", path: "/api/ai-visibility/brand/:brandId/benchmark/diagnostics" },
  { method: "get", path: "/api/ai-visibility/brand/:brandId/evidence" },
]);

/**
 * Walk Express app router stack and collect registered route signatures.
 * @param {import('express').Express} app
 * @returns {Array<{ method: string, path: string }>}
 */
export function listExpressRouteSignatures(app) {
  const out = [];
  const stack = app?._router?.stack;
  if (!Array.isArray(stack)) return out;
  for (const layer of stack) {
    if (!layer?.route) continue;
    const path = layer.route.path;
    const methods = layer.route.methods || {};
    for (const [method, enabled] of Object.entries(methods)) {
      if (enabled) out.push({ method: String(method).toLowerCase(), path });
    }
  }
  return out;
}

/**
 * @param {import('express').Express} app
 * @param {{ expected?: typeof BRAND_AI_VISIBILITY_EXPECTED_ROUTES, logger?: Console }} [opts]
 */
export function assertBrandAiVisibilityRoutesRegistered(app, opts = {}) {
  const expected = opts.expected || BRAND_AI_VISIBILITY_EXPECTED_ROUTES;
  const logger = opts.logger || console;
  const registered = listExpressRouteSignatures(app);
  const missing = [];
  for (const want of expected) {
    const hit = registered.some(
      (r) => r.method === want.method && r.path === want.path
    );
    if (!hit) missing.push(`${want.method.toUpperCase()} ${want.path}`);
  }

  if (missing.length) {
    const msg =
      `[ai-visibility] ROUTE_REGISTRATION_INCOMPLETE — missing: ${missing.join(", ")}. ` +
      `Restart Node (npm start) so Brand AI Visibility routes load.`;
    logger.error(msg);
    return {
      ok: false,
      missing,
      registeredCount: registered.filter((r) =>
        String(r.path || "").includes("/api/ai-visibility/")
      ).length,
      message: msg,
    };
  }

  const aivCount = registered.filter((r) =>
    String(r.path || "").includes("/api/ai-visibility/")
  ).length;
  logger.log(
    `✅ Brand AI Visibility route registration check: ${expected.length}/${expected.length} expected routes present (${aivCount} ai-visibility signatures on stack)`
  );
  return {
    ok: true,
    missing: [],
    registeredCount: aivCount,
    expectedCount: expected.length,
  };
}
