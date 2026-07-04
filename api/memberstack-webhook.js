import {
  parseMemberstackWebhookPayload,
  syncMemberstackMemberToAirtable,
} from "../lib/memberstack/sync-member-to-airtable.js";

/**
 * POST /api/webhooks/memberstack — Memberstack member events → Airtable Users (Zap B replacement).
 *
 * Configure in Memberstack dashboard with URL: https://<host>/api/webhooks/memberstack
 * Optional: MEMBERSTACK_WEBHOOK_SECRET — compared to x-memberstack-secret or x-webhook-secret header.
 */
export default async function memberstackWebhook(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const expected = (process.env.MEMBERSTACK_WEBHOOK_SECRET || "").trim();
    if (expected) {
      const got =
        req.headers["x-memberstack-secret"] ||
        req.headers["x-webhook-secret"] ||
        req.headers["authorization"] ||
        "";
      const token = String(got).replace(/^Bearer\s+/i, "").trim();
      if (token !== expected) {
        return res.status(401).json({ error: "Unauthorized" });
      }
    }

    const parsed = parseMemberstackWebhookPayload(req.body);
    const interesting =
      !parsed.event ||
      parsed.event.includes("member") ||
      parsed.event.includes("update") ||
      parsed.event.includes("create");

    if (!interesting && !parsed.memberstackId && !parsed.email) {
      return res.status(200).json({ ok: true, skipped: true, reason: "unhandled_event" });
    }

    const result = await syncMemberstackMemberToAirtable(parsed);
    return res.status(200).json({ ok: true, airtableRecordId: result.recordId });
  } catch (err) {
    console.error("Memberstack webhook error:", err);
    const status = err.statusCode === 400 ? 400 : 500;
    return res.status(status).json({
      error: "Webhook processing failed",
      details: err.message || String(err),
    });
  }
}
