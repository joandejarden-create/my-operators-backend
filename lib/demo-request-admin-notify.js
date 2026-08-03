import { getTransport } from "./email.js";

const FROM_NAME = process.env.EMAIL_FROM_NAME || "Dealality";
const FROM_ADDRESS = process.env.EMAIL_FROM || process.env.SMTP_USER || "noreply@dealcapture.co";

/**
 * Notify ops that someone requested a product demo from marketing.
 * Always attempts to send (unlike signup notify which may be gated elsewhere).
 */
export async function sendDemoRequestAdminNotification({
  email,
  role,
  pageUrl,
  referrer,
  created,
}) {
  const to = (process.env.SIGNUP_NOTIFY_EMAIL || process.env.SUPPORT_EMAIL || "").trim();
  if (!to) {
    console.warn("SIGNUP_NOTIFY_EMAIL / SUPPORT_EMAIL not set — skipping demo request notification.");
    return { sent: false, error: "notify_email_not_configured" };
  }

  const transport = getTransport();
  if (!transport) {
    console.warn("SMTP not configured — skipping demo request notification.");
    return { sent: false, error: "smtp_not_configured" };
  }

  const subject = `Dealality demo request: ${email}`;
  const text = [
    "Someone requested a Dealality system demo.",
    "",
    `Email: ${email}`,
    role ? `Role: ${role}` : "Role: (not specified)",
    pageUrl ? `Page: ${pageUrl}` : null,
    referrer ? `Referrer: ${referrer}` : null,
    typeof created === "boolean" ? `New notify contact: ${created ? "yes" : "no (existing)"}` : null,
    "",
    "Reply to their email to schedule the demo.",
  ]
    .filter(Boolean)
    .join("\n");

  try {
    await transport.sendMail({
      from: `${FROM_NAME} <${FROM_ADDRESS}>`,
      to,
      replyTo: email,
      subject,
      text,
    });
    console.log("Demo request admin notification sent to:", to);
    return { sent: true };
  } catch (err) {
    console.error("Demo request admin notification failed:", err.message || err);
    return { sent: false, error: err.message || String(err) };
  }
}
