import { getTransport } from "./email.js";

const FROM_NAME = process.env.EMAIL_FROM_NAME || "Dealality";
const FROM_ADDRESS = process.env.EMAIL_FROM || process.env.SMTP_USER || "noreply@dealcapture.co";

/**
 * Notify ops that a new signup was submitted (replaces Zap-side admin alerts when configured).
 */
export async function sendSignupAdminNotification({ email, firstName, lastName, companyName, companyType, memberstackId }) {
  const to = (process.env.SIGNUP_NOTIFY_EMAIL || process.env.SUPPORT_EMAIL || "").trim();
  if (!to) {
    console.warn("SIGNUP_NOTIFY_EMAIL / SUPPORT_EMAIL not set — skipping admin signup notification.");
    return { sent: false, error: "notify_email_not_configured" };
  }

  const transport = getTransport();
  if (!transport) {
    console.warn("SMTP not configured — skipping admin signup notification.");
    return { sent: false, error: "smtp_not_configured" };
  }

  const name = [firstName, lastName].filter(Boolean).join(" ").trim() || "(no name)";
  const subject = `New Dealality signup: ${email}`;
  const text = [
    "A new membership application was submitted via the Dealality signup page.",
    "",
    `Name: ${name}`,
    `Email: ${email}`,
    companyName ? `Company: ${companyName}` : null,
    companyType ? `User type: ${companyType}` : null,
    memberstackId ? `Memberstack ID: ${memberstackId}` : "Memberstack: not linked",
    "",
    "Next step: review in Memberstack and assign the approved plan when qualified.",
  ]
    .filter(Boolean)
    .join("\n");

  try {
    await transport.sendMail({
      from: `${FROM_NAME} <${FROM_ADDRESS}>`,
      to,
      subject,
      text,
    });
    console.log("Signup admin notification sent to:", to);
    return { sent: true };
  } catch (err) {
    console.error("Signup admin notification failed:", err.message || err);
    return { sent: false, error: err.message || String(err) };
  }
}
