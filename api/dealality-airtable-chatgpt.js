/**
 * ChatGPT Custom GPT actions — Dealality GTM Airtable wrapper.
 *
 * Auth: Authorization: Bearer <DEALALITY_AIRTABLE_CHATGPT_SECRET>
 *       or header x-dealality-chatgpt-secret
 */
import {
  createRecordsByTableId,
  getRecordById,
  listDealalityTables,
  listRecordsByTableId,
  ServiceError,
  summarizeRecordsByTableId,
  updateRecordByTableId,
  updateRecordsByTableId,
} from "../lib/gtm-owner-target/dealality-airtable-chatgpt-service.js";

const DEV =
  process.env.NODE_ENV !== "production" || process.env.DEBUG_DEALALITY_AIRTABLE_CHATGPT === "true";

function getExpectedSecret() {
  return (
    process.env.DEALALITY_AIRTABLE_CHATGPT_SECRET ||
    process.env.CHATGPT_AIRTABLE_SECRET ||
    ""
  ).trim();
}

export function dealalityChatgptAuth(req, res, next) {
  const expected = getExpectedSecret();
  if (!expected) {
    return res.status(503).json({
      error: "ChatGPT Airtable API is not configured (DEALALITY_AIRTABLE_CHATGPT_SECRET).",
    });
  }
  const bearer = String(req.headers.authorization || "").replace(/^Bearer\s+/i, "").trim();
  const headerSecret = String(req.headers["x-dealality-chatgpt-secret"] || "").trim();
  const provided = bearer || headerSecret;
  if (!provided || provided !== expected) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  return next();
}

async function handleAction(res, fn) {
  try {
    const result = await fn();
    return res.json(result);
  } catch (err) {
    if (err instanceof ServiceError) {
      if (DEV) console.error("[dealality-airtable-chatgpt]", err.message, err.details || "");
      return res.status(err.status).json({
        error: err.message,
        ...(err.details ? { details: err.details } : {}),
      });
    }
    console.error("[dealality-airtable-chatgpt] unexpected error:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
}

export async function postListDealalityTables(req, res) {
  return handleAction(res, () => listDealalityTables());
}

export async function postListRecordsByTableId(req, res) {
  return handleAction(res, () => listRecordsByTableId(req.body || {}));
}

export async function postGetRecordById(req, res) {
  return handleAction(res, () => getRecordById(req.body || {}));
}

export async function postCreateRecordsByTableId(req, res) {
  return handleAction(res, () => createRecordsByTableId(req.body || {}));
}

export async function postUpdateRecordByTableId(req, res) {
  return handleAction(res, () => updateRecordByTableId(req.body || {}));
}

export async function postUpdateRecordsByTableId(req, res) {
  return handleAction(res, () => updateRecordsByTableId(req.body || {}));
}

export async function postSummarizeRecordsByTableId(req, res) {
  return handleAction(res, () => summarizeRecordsByTableId(req.body || {}));
}
