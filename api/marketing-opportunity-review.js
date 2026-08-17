import { createHash, randomUUID } from "crypto";
import { getTransport } from "../lib/email.js";
import {
  appendOpportunityReview,
  checkRateLimit,
  sanitizeString,
} from "../lib/marketing-opportunity-review-store.js";

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

export const ALLOWED_DECISION_TYPES = [
  "Brand selection",
  "Operator selection",
  "Conversion or repositioning",
  "New hotel development",
  "Mixed-use hospitality",
  "Branded residences",
  "Franchise versus management structure",
  "Strategic partner outreach",
  "Sale or exit strategy",
  "Other",
];

export const ALLOWED_TIMINGS = [
  "Active now",
  "Within 3 months",
  "Within 6 months",
  "Within 12 months",
  "Early planning",
  "Not yet determined",
];

async function sendOpportunityReviewAdminNotification(payload) {
  const to = (process.env.SIGNUP_NOTIFY_EMAIL || process.env.SUPPORT_EMAIL || "").trim();
  if (!to) {
    console.warn(
      "[marketing-opportunity-review] SIGNUP_NOTIFY_EMAIL / SUPPORT_EMAIL not set — skipping admin email."
    );
    return { sent: false, error: "notify_email_not_configured" };
  }

  const transport = getTransport();
  if (!transport) {
    console.warn(
      "[marketing-opportunity-review] SMTP not configured — skipping admin email."
    );
    return { sent: false, error: "smtp_not_configured" };
  }

  const fromName = process.env.EMAIL_FROM_NAME || "Dealality";
  const fromAddress =
    process.env.EMAIL_FROM || process.env.SMTP_USER || "noreply@dealcapture.co";

  const subject = `Opportunity review request: ${payload.company}`;
  const text = [
    "A confidential hotel opportunity review was submitted (no account required).",
    "",
    `Name: ${payload.fullName}`,
    `Email: ${payload.businessEmail}`,
    `Company: ${payload.company}`,
    `Role: ${payload.role}`,
    payload.projectName ? `Project: ${payload.projectName}` : null,
    `Location: ${payload.location}`,
    `Decision types: ${payload.decisionTypes.join("; ")}`,
    `Timing: ${payload.projectTiming}`,
    payload.preferredContact
      ? `Preferred contact: ${payload.preferredContact}`
      : null,
    "",
    "Description:",
    payload.description,
    "",
    `Submission id: ${payload.id}`,
  ]
    .filter(Boolean)
    .join("\n");

  try {
    await transport.sendMail({
      from: `${fromName} <${fromAddress}>`,
      to,
      subject,
      text,
      replyTo: payload.businessEmail,
    });
    return { sent: true };
  } catch (err) {
    console.error(
      "[marketing-opportunity-review] admin email failed:",
      err.message || err
    );
    return { sent: false, error: err.message || String(err) };
  }
}

function clientIp(req) {
  const xf = req.headers["x-forwarded-for"];
  if (typeof xf === "string" && xf.trim()) return xf.split(",")[0].trim();
  return req.socket?.remoteAddress || "unknown";
}

/**
 * POST /api/marketing/opportunity-review
 * Public, no account. Persists JSONL + optional admin email.
 */
export default async function marketingOpportunityReview(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const body = req.body || {};

    // Honeypot — silent success for bots
    if (sanitizeString(body.website, 200)) {
      return res.status(204).end();
    }

    const ip = clientIp(req);
    const rate = checkRateLimit(ip);
    if (!rate.allowed) {
      console.warn("[marketing-opportunity-review] rate_limited");
      return res.status(429).json({
        error: "Too many submissions. Please try again later.",
      });
    }

    const fullName = sanitizeString(body.fullName, 120);
    const businessEmail = sanitizeString(body.businessEmail, 180).toLowerCase();
    const company = sanitizeString(body.company, 160);
    const role = sanitizeString(body.role, 120);
    const projectName = sanitizeString(body.projectName, 160);
    const location = sanitizeString(body.location, 160);
    const projectTiming = sanitizeString(body.projectTiming, 40);
    const description = sanitizeString(body.description, 4000);
    const preferredContact = sanitizeString(body.preferredContact, 80);
    const privacyAck =
      body.privacyAck === true ||
      body.privacyAck === "true" ||
      body.privacyAck === "on" ||
      body.privacyAck === "1";

    let decisionTypes = body.decisionTypes;
    if (typeof decisionTypes === "string") {
      decisionTypes = decisionTypes.split(",").map((s) => s.trim());
    }
    if (!Array.isArray(decisionTypes)) decisionTypes = [];
    decisionTypes = [
      ...new Set(
        decisionTypes
          .map((t) => sanitizeString(t, 80))
          .filter((t) => ALLOWED_DECISION_TYPES.includes(t))
      ),
    ];

    const failed = [];
    if (!fullName) failed.push("fullName");
    if (!businessEmail || !EMAIL_RE.test(businessEmail)) failed.push("businessEmail");
    if (!company) failed.push("company");
    if (!location) failed.push("location");
    if (!decisionTypes.length) failed.push("decisionTypes");
    if (!ALLOWED_TIMINGS.includes(projectTiming)) failed.push("projectTiming");
    if (!description || description.length < 20) failed.push("description");
    if (!privacyAck) failed.push("privacyAck");

    if (failed.length) {
      console.warn(
        "[marketing-opportunity-review] validation_failed",
        failed.join(",")
      );
      return res.status(400).json({
        error: "Please complete the required fields.",
        failed,
      });
    }

    const id = randomUUID();
    const ipHash = createHash("sha256").update(ip).digest("hex").slice(0, 16);
    const record = {
      id,
      createdAt: new Date().toISOString(),
      fullName,
      businessEmail,
      company,
      role,
      projectName: projectName || null,
      location,
      decisionTypes,
      projectTiming,
      description,
      preferredContact: preferredContact || null,
      privacyAck: true,
      ipHash,
      userAgent: sanitizeString(req.headers["user-agent"] || "", 200),
      source: "opportunity-review",
    };

    appendOpportunityReview(record);

    sendOpportunityReviewAdminNotification(record).catch((err) => {
      console.error(
        "[marketing-opportunity-review] notify async error:",
        err.message || err
      );
    });

    return res.status(200).json({ ok: true, id });
  } catch (err) {
    console.error("[marketing-opportunity-review] error:", err);
    return res.status(500).json({
      error: "Could not submit opportunity for review.",
    });
  }
}
