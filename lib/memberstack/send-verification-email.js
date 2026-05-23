import axios from "axios";

const BASE = (process.env.MEMBERSTACK_BASE_URL || "https://admin.memberstack.com").replace(/\/$/, "");

/**
 * Admin API member create does not send verification emails (unlike DOM signup).
 * Try known Memberstack email-action paths; non-fatal if none succeed.
 */
export async function sendMemberstackVerificationEmail(memberstackId, email) {
  const key = (process.env.MEMBERSTACK_SECRET_KEY || "").trim();
  if (!key || !memberstackId) {
    return { sent: false, note: "skipped_no_key_or_id" };
  }

  const headers = { "X-API-KEY": key, "Content-Type": "application/json" };
  const attempts = [
    { method: "POST", url: `${BASE}/members/${memberstackId}/send-verification-email`, data: {} },
    { method: "POST", url: `${BASE}/members/${memberstackId}/resend-verification-email`, data: {} },
    { method: "POST", url: `${BASE}/members/${memberstackId}/verification-email`, data: {} },
    { method: "POST", url: `${BASE}/members/send-verification-email`, data: { memberId: memberstackId, email } },
  ];

  for (const attempt of attempts) {
    try {
      const res = await axios({
        method: attempt.method,
        url: attempt.url,
        headers,
        data: attempt.data,
        validateStatus: () => true,
      });
      if (res.status >= 200 && res.status < 300) {
        return { sent: true, note: `ok:${attempt.url.replace(BASE, "")}` };
      }
    } catch {
      /* try next */
    }
  }

  return { sent: false, note: "no_admin_endpoint_succeeded_use_dom_signup" };
}
