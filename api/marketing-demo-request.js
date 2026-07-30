import { sendDemoRequestAdminNotification } from "../lib/demo-request-admin-notify.js";
import { upsertMarketingNotifyUser } from "../lib/marketing-notify-upsert.js";

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const ALLOWED_ROLES = new Set(["owner", "brand", "operator", "advisor", "other"]);

/**
 * POST /api/marketing/demo-request
 * Public one-field demo request — always emails ops; upserts notify-only contact.
 * Body: { email: string, role?: string, pageUrl?: string, referrer?: string }
 */
export default async function marketingDemoRequest(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const rawEmail = typeof req.body?.email === "string" ? req.body.email.trim() : "";
    const normalizedEmail = rawEmail.toLowerCase();
    if (!normalizedEmail || !EMAIL_RE.test(normalizedEmail)) {
      return res.status(400).json({ error: "A valid email is required" });
    }

    const rawRole = typeof req.body?.role === "string" ? req.body.role.trim().toLowerCase() : "";
    const role = ALLOWED_ROLES.has(rawRole) ? rawRole : "";
    const pageUrl = typeof req.body?.pageUrl === "string" ? req.body.pageUrl.trim().slice(0, 500) : "";
    const referrer = typeof req.body?.referrer === "string" ? req.body.referrer.trim().slice(0, 500) : "";

    let record = null;
    let created = false;
    try {
      ({ record, created } = await upsertMarketingNotifyUser(normalizedEmail, {
        source: "Old Home — Request a Demo",
        reason: role ? `Demo request (${role})` : "Demo request",
      }));
    } catch (err) {
      // Still notify even if Airtable upsert fails — demo lead is the priority.
      console.error("Demo request upsert failed (continuing to notify):", err.message || err);
    }

    const notify = await sendDemoRequestAdminNotification({
      email: normalizedEmail,
      role: role || null,
      pageUrl: pageUrl || null,
      referrer: referrer || null,
      created,
    });

    if (!notify.sent) {
      return res.status(502).json({
        error: "Could not send demo request",
        details: notify.error || "notify_failed",
      });
    }

    return res.status(200).json({
      ok: true,
      notified: true,
      created,
      id: record?.id || null,
    });
  } catch (err) {
    console.error("Error in marketing-demo-request:", err);
    const message = err.message || "Internal Server Error";
    return res.status(500).json({ error: "Could not submit demo request", details: message });
  }
}
