import { sendSignupAdminNotification } from "../lib/signup-admin-notify.js";
import {
  MARKETING_NOTIFY_USER_TYPE,
  upsertMarketingNotifyUser,
} from "../lib/marketing-notify-upsert.js";

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/**
 * POST /api/marketing/beta-notify
 * Public hero email capture — upsert Users row with User Type "Notify only".
 * Body: { email: string }
 */
export default async function marketingBetaNotify(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const rawEmail = typeof req.body?.email === "string" ? req.body.email.trim() : "";
    const normalizedEmail = rawEmail.toLowerCase();
    if (!normalizedEmail || !EMAIL_RE.test(normalizedEmail)) {
      return res.status(400).json({ error: "A valid email is required" });
    }

    let record;
    let created;
    try {
      ({ record, created } = await upsertMarketingNotifyUser(normalizedEmail));
    } catch (err) {
      if (err.statusCode === 400) {
        return res.status(400).json({ error: err.message || "Bad request" });
      }
      throw err;
    }

    if (created) {
      sendSignupAdminNotification({
        email: normalizedEmail,
        companyType: MARKETING_NOTIFY_USER_TYPE,
        memberstackId: null,
      }).catch((e) => console.error("Marketing notify admin alert:", e));
    }

    return res.status(200).json({
      ok: true,
      id: record.id,
      created,
      userType: MARKETING_NOTIFY_USER_TYPE,
    });
  } catch (err) {
    console.error("Error in marketing-beta-notify:", err);
    const message = err.message || "Internal Server Error";
    const status = err.statusCode === 422 ? 422 : 500;
    return res.status(status).json({ error: "Could not save email", details: message });
  }
}
