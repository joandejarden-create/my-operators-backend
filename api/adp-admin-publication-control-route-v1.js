/**
 * AI Admin publication-control read.
 * Uses the existing matrix builder. Does not publish, certify, or write reports.
 */

import { buildAdpAdminPublicationMatrix } from "../lib/ai-demand-positioning/review/adp-admin-publication-control-v1.js";
import { getAdpProductionBaseUrl } from "../lib/ai-demand-positioning/review/adp-production-verification-v1.js";
import { requireAdpReviewCapability } from "../lib/ai-demand-positioning/review/adp-review-capability-v1.js";

function setNoStore(res) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
}

/** GET /api/ai-demand-positioning/review/admin-publication-control */
export function getAdpAdminPublicationControl(req, res) {
  setNoStore(res);
  const cap = requireAdpReviewCapability(req, res);
  if (!cap) return;

  const includeExcluded = String(req.query.includeExcluded || "1") !== "0";
  const matrix = buildAdpAdminPublicationMatrix({ includeExcluded });
  const actor = cap.actor || {};
  return res.json({
    ...matrix,
    actor: { id: actor.id || null, email: actor.email || null },
    productionBaseUrlConfigured: !!getAdpProductionBaseUrl(),
  });
}
