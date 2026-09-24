/**
 * Surfe enrichment webhook — acknowledges delivery; NEVER persists Surfe PII.
 * Contact details remain ephemeral / request-scoped via on-demand reveal only.
 */

import {
  readPendingEnrichment,
  markPendingEnrichmentDone,
  writeAlertContactCache,
} from "../lib/market-alerts-contact/cache.js";
import { logContactOp } from "../lib/market-alerts-contact/safe-log.js";
import { CONTACT_LOOKUP_STATUS } from "../lib/market-alerts-contact/stakeholder-schema.js";
import { updateStakeholderLookupStatus } from "../lib/market-alerts-contact/stakeholder-airtable.js";

function extractBearer(req) {
  const h = req.headers?.authorization || "";
  const m = String(h).match(/^Bearer\s+(.+)$/i);
  return m ? m[1].trim() : null;
}

function validateWebhookSecret(req) {
  const expected = process.env.SURFE_WEBHOOK_SECRET;
  if (!expected || !String(expected).trim()) {
    return process.env.NODE_ENV !== "production";
  }
  const got =
    req.headers["x-surfe-webhook-secret"] ||
    req.headers["x-webhook-secret"] ||
    extractBearer(req);
  return Boolean(got && String(got) === String(expected));
}

function normalizePayload(body = {}) {
  const enrichmentId =
    body.enrichmentID ||
    body.enrichmentId ||
    body.id ||
    body.data?.enrichmentID ||
    body.data?.enrichmentId ||
    null;
  const status = String(body.status || body.data?.status || "").toUpperCase();
  const peopleCount = Array.isArray(body.people)
    ? body.people.length
    : Array.isArray(body.data?.people)
      ? body.data.people.length
      : 0;
  return { enrichmentId, status, peopleCount };
}

/**
 * POST /api/webhooks/surfe-enrichment
 */
export async function postSurfeEnrichmentWebhook(req, res) {
  try {
    if (!validateWebhookSecret(req)) {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }

    const body = req.body;
    if (!body || typeof body !== "object") {
      return res.status(400).json({ ok: false, error: "malformed_payload" });
    }

    const { enrichmentId, status, peopleCount } = normalizePayload(body);
    if (!enrichmentId) {
      return res.status(400).json({ ok: false, error: "missing_enrichment_id" });
    }

    const pending = readPendingEnrichment(enrichmentId);
    if (pending?.status === "COMPLETED") {
      logContactOp("webhook_duplicate", { enrichmentId: String(enrichmentId).slice(0, 24) });
      return res.status(200).json({ ok: true, duplicate: true });
    }

    if (status && status !== "COMPLETED" && status !== "FAILED") {
      return res.status(200).json({ ok: true, acknowledged: true, status });
    }

    if (status === "FAILED") {
      markPendingEnrichmentDone(enrichmentId, {});
      if (pending?.alertId) {
        writeAlertContactCache(pending.alertId, {
          status: "failed",
          enrichmentId,
          contactLookupStatus: CONTACT_LOOKUP_STATUS.FAILED_RETRYABLE,
          contacts: [],
        });
      }
      if (pending?.stakeholderId) {
        await updateStakeholderLookupStatus(
          pending.stakeholderId,
          CONTACT_LOOKUP_STATUS.FAILED_RETRYABLE
        );
      }
      logContactOp("webhook_failed_job", { enrichmentId: String(enrichmentId).slice(0, 24) });
      return res.status(200).json({ ok: true, status: "FAILED", surfePiiPersisted: false });
    }

    // COMPLETED — acknowledge only. Do NOT read or persist people / emails / phones / LinkedIn.
    markPendingEnrichmentDone(enrichmentId, {});
    if (pending?.alertId) {
      writeAlertContactCache(pending.alertId, {
        status: "webhook_acked",
        enrichmentId,
        contactLookupStatus: CONTACT_LOOKUP_STATUS.NOT_REQUESTED,
        contacts: [],
        note: "Surfe webhook acknowledged; contact details were not persisted",
      });
    }
    if (pending?.stakeholderId) {
      await updateStakeholderLookupStatus(
        pending.stakeholderId,
        peopleCount > 0 ? CONTACT_LOOKUP_STATUS.NOT_REQUESTED : CONTACT_LOOKUP_STATUS.MISS
      );
    }

    logContactOp("webhook_completed_no_pii", {
      enrichmentId: String(enrichmentId).slice(0, 24),
      peopleCountReported: peopleCount,
      surfePiiPersisted: false,
      alertId: pending?.alertId || null,
    });

    return res.status(200).json({
      ok: true,
      status: "COMPLETED",
      peopleCountAcknowledged: peopleCount,
      surfePiiPersisted: false,
    });
  } catch (err) {
    logContactOp("webhook_error", { message: String(err?.message || err).slice(0, 80) });
    return res.status(500).json({ ok: false, error: "webhook_processing_failed" });
  }
}
