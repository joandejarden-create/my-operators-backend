import nodemailer from "nodemailer";

const FROM_NAME = process.env.EMAIL_FROM_NAME || "Deal Capture™";
const FROM_ADDRESS = process.env.EMAIL_FROM || process.env.SMTP_USER || "noreply@dealcapture.co";
const SUPPORT_EMAIL = process.env.SUPPORT_EMAIL || "support@dealcapture.co";
const CONFIRM_EMAIL_URL = process.env.CONFIRM_EMAIL_URL || "#";
const PLATFORM_OVERVIEW_URL = process.env.PLATFORM_OVERVIEW_URL || "#";
const MEMBERSHIP_CRITERIA_URL = process.env.MEMBERSHIP_CRITERIA_URL || "#";

function getTransport() {
  const host = process.env.SMTP_HOST;
  const port = process.env.SMTP_PORT || 587;
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  if (!host || !user || !pass) return null;
  const portNum = Number(port);
  const isOutlook = /outlook\.com|office365\.com|hotmail\.com|live\.com/i.test(host);
  return nodemailer.createTransport({
    host,
    port: portNum,
    secure: portNum === 465,
    requireTLS: portNum === 587 && isOutlook,
    auth: { user, pass },
  });
}

/**
 * Build HTML and plain text for the post-signup welcome / onboarding email.
 * @param {{ firstName: string, confirmUrl?: string, platformOverviewUrl?: string, membershipCriteriaUrl?: string, supportEmail?: string }} opts
 */
export function getWelcomeEmailContent(opts) {
  const firstName = opts.firstName || "there";
  const confirmUrl = opts.confirmUrl ?? CONFIRM_EMAIL_URL;
  const platformOverviewUrl = opts.platformOverviewUrl ?? PLATFORM_OVERVIEW_URL;
  const membershipCriteriaUrl = opts.membershipCriteriaUrl ?? MEMBERSHIP_CRITERIA_URL;
  const supportEmail = opts.supportEmail ?? SUPPORT_EMAIL;

  const html = `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
  <p>Hi ${firstName},</p>
  <p>Thanks for submitting your information to Deal Capture™ – the confidential hotel deal platform built for owners, operators, and hospitality advisors.</p>
  <p>To begin your onboarding, please confirm your email address by clicking below:</p>
  <p style="margin: 24px 0;"><a href="${confirmUrl}" style="display: inline-block; padding: 12px 24px; background: #1a1a2e; color: #fff; text-decoration: none; border-radius: 6px; font-weight: 600;">Confirm My Email Address</a></p>
  <p><strong>WHAT HAPPENS AFTER YOU CONFIRM:</strong></p>
  <ul>
    <li>Our team will review your submission within 1–2 business days to confirm your eligibility.</li>
    <li>If qualified, you'll receive a personalized link to complete your full profile and access the platform.</li>
    <li>You'll be able to use Deal Capture™ to privately match with best-fit brands, operators, and partners – no cold outreach or public listings.</li>
    <li>If we need more info, we'll reach out directly.</li>
  </ul>
  <p>Deal Capture™ is designed for fast, focused deal-making – connecting hospitality professionals through a curated network where every match is driven by project fit, execution readiness, and long-term value.</p>
  <p>In the meantime, feel free to explore:</p>
  <ul>
    <li><a href="${platformOverviewUrl}">Platform Overview</a></li>
    <li><a href="${membershipCriteriaUrl}">Membership Criteria</a></li>
  </ul>
  <p>Questions? Just reply to this email or contact us at <a href="mailto:${supportEmail}">${supportEmail}</a>.</p>
  <p>At your service,<br><strong>The Deal Capture™ Team</strong></p>
</body>
</html>`;

  const text = `
Hi ${firstName},

Thanks for submitting your information to Deal Capture™ – the confidential hotel deal platform built for owners, operators, and hospitality advisors.

To begin your onboarding, please confirm your email address by clicking below:
${confirmUrl}

WHAT HAPPENS AFTER YOU CONFIRM:
- Our team will review your submission within 1–2 business days to confirm your eligibility.
- If qualified, you'll receive a personalized link to complete your full profile and access the platform.
- You'll be able to use Deal Capture™ to privately match with best-fit brands, operators, and partners – no cold outreach or public listings.
- If we need more info, we'll reach out directly.

Deal Capture™ is designed for fast, focused deal-making – connecting hospitality professionals through a curated network where every match is driven by project fit, execution readiness, and long-term value.

In the meantime, feel free to explore:
- Platform Overview: ${platformOverviewUrl}
- Membership Criteria: ${membershipCriteriaUrl}

Questions? Just reply to this email or contact us at ${supportEmail}.

At your service,
The Deal Capture™ Team
`.trim();

  return { html, text };
}

const SUBJECT = "YOU'RE IN – WHAT HAPPENS NEXT AT DEAL CAPTURE™";

/**
 * Send the post-signup welcome email. No-op if SMTP is not configured.
 * @param {string} to - Recipient email
 * @param {{ firstName?: string, confirmUrl?: string, platformOverviewUrl?: string, membershipCriteriaUrl?: string, supportEmail?: string }} opts
 * @returns {Promise<{ sent: boolean, error?: string }>}
 */
export async function sendWelcomeEmail(to, opts = {}) {
  const transport = getTransport();
  if (!transport) {
    console.warn("SMTP not configured (SMTP_HOST, SMTP_USER, SMTP_PASS). Skipping welcome email to:", to);
    return { sent: false, error: "SMTP not configured" };
  }

  const { html, text } = getWelcomeEmailContent(opts);
  const from = `${FROM_NAME} <${FROM_ADDRESS}>`;

  try {
    await transport.sendMail({
      from,
      to,
      subject: SUBJECT,
      text,
      html,
    });
    console.log("Welcome email sent to:", to);
    return { sent: true };
  } catch (err) {
    const msg = err.response || err.message || String(err);
    console.error("Failed to send welcome email to", to, "—", msg);
    return { sent: false, error: err.message || msg };
  }
}
