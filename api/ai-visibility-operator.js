/**
 * Operator AI Intelligence authenticated read API.
 * No provider calls. No Airtable writes. Separate from Brand AI namespace.
 */

import { buildOperatorFoundationSnapshot } from "../lib/ai-visibility/operator-intelligence/index.js";
import {
  buildOperatorCustomerPayload,
  buildOperatorCustomerUniversePayload,
} from "../lib/ai-visibility/operator-intelligence/operator-customer-read-service.js";
import {
  ALL_PROVIDERS_SELECTOR_ID,
  resolveProviderId,
} from "../lib/ai-visibility/provider-dimension.js";

export function getOperatorAiFoundation(req, res) {
  try {
    const snapshot = buildOperatorFoundationSnapshot();
    return res.json({
      ok: true,
      success: true,
      product: "Operator AI Intelligence",
      executiveCards: [],
      recommendationMetrics: null,
      censusReads: 0,
      ...snapshot,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      success: false,
      error: "operator_ai_foundation_error",
      message: err?.message || "foundation_failed",
    });
  }
}

function providerFromQuery(req) {
  const raw = req.query.provider;
  if (raw == null || String(raw).trim() === "") return ALL_PROVIDERS_SELECTOR_ID;
  return resolveProviderId(raw, ALL_PROVIDERS_SELECTOR_ID);
}

export function getOperatorAiCustomerUniverse(req, res) {
  try {
    return res.json(buildOperatorCustomerUniversePayload());
  } catch (err) {
    return res.status(500).json({
      ok: false,
      success: false,
      error: "operator_ai_customer_universe_error",
      message: err?.message || "customer_universe_failed",
    });
  }
}

export function getOperatorAiCustomerPayload(req, res) {
  try {
    const operatorId = String(req.params.operatorId || "").trim();
    if (!operatorId) {
      return res.status(400).json({
        ok: false,
        success: false,
        error: "operator_id_required",
      });
    }
    const payload = buildOperatorCustomerPayload(operatorId, providerFromQuery(req));
    if (!payload.ok) {
      return res.status(404).json(payload);
    }
    return res.json(payload);
  } catch (err) {
    return res.status(500).json({
      ok: false,
      success: false,
      error: "operator_ai_customer_payload_error",
      message: err?.message || "customer_payload_failed",
    });
  }
}
