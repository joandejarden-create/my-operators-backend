import axios from "axios";
import { readLogicalCustomFields } from "./memberstack-custom-fields.js";
import { upsertSignupUserRecord } from "../signup-airtable-upsert.js";

const BASE = (process.env.MEMBERSTACK_BASE_URL || "https://admin.memberstack.com").replace(/\/$/, "");

function memberstackHeaders() {
  const key = (process.env.MEMBERSTACK_SECRET_KEY || "").trim();
  if (!key) return null;
  return { "X-API-KEY": key, "Content-Type": "application/json" };
}

/**
 * Normalize Memberstack webhook / event payloads (shape varies by version).
 */
export function parseMemberstackWebhookPayload(body) {
  const b = body && typeof body === "object" ? body : {};
  const event =
    b.event || b.type || b.name || b.trigger || (b.data && (b.data.event || b.data.type)) || "";
  const data = b.data || b.payload || b.member || b;
  const member = data.member || data;
  const id = member?.id || data?.id || b.memberId || null;
  const email =
    (typeof member?.email === "string" && member.email) ||
    (typeof data?.email === "string" && data.email) ||
    (typeof b.email === "string" && b.email) ||
    null;
  const customFields = member?.customFields || data?.customFields || null;
  const plans = member?.plans || data?.plans || null;
  return {
    event: String(event || "").toLowerCase(),
    memberstackId: id ? String(id) : null,
    email: email ? String(email).trim().toLowerCase() : null,
    customFields: customFields && typeof customFields === "object" ? customFields : null,
    plans: Array.isArray(plans) ? plans : null,
  };
}

async function fetchMemberById(memberstackId) {
  const headers = memberstackHeaders();
  if (!headers || !memberstackId) return null;
  try {
    const res = await axios.get(`${BASE}/members/${memberstackId}`, {
      headers,
      validateStatus: (s) => s === 200,
    });
    return res.data?.data || null;
  } catch {
    return null;
  }
}

function bodyFromCustomFields(cf, email) {
  if (!cf || typeof cf !== "object") {
    return { email };
  }
  const logical = readLogicalCustomFields(cf);
  return {
    email,
    firstName: logical.firstName,
    lastName: logical.lastName,
    companyName: logical.companyName,
    title: cf.title || "",
    phone: logical.phone,
    companyType: logical.companyType,
    reasonToJoin: logical.reasonToJoin,
    howDidYouHear: logical.howDidYouHear,
  };
}

function memberHasApprovedPlan(plans) {
  const approved = (process.env.MEMBERSTACK_APPROVED_PLAN_IDS || process.env.MEMBERSTACK_SIGNUP_FREE_PLAN_ID || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (!approved.length || !Array.isArray(plans)) return false;
  for (const p of plans) {
    const pid = typeof p === "string" ? p : p?.planId || p?.id;
    if (pid && approved.includes(String(pid))) return true;
  }
  return false;
}

/**
 * Sync Memberstack member → Airtable Users (replaces Zap B: Member Updated → Airtable).
 */
export async function syncMemberstackMemberToAirtable(parsed) {
  let { memberstackId, email, customFields, plans } = parsed;

  if (memberstackId && (!email || !customFields)) {
    const full = await fetchMemberById(memberstackId);
    if (full) {
      email = email || (full.email ? String(full.email).trim().toLowerCase() : null);
      customFields = customFields || full.customFields || null;
      plans = plans || full.plans || null;
    }
  }

  if (!email && !memberstackId) {
    const err = new Error("Webhook payload missing member email and id");
    err.statusCode = 400;
    throw err;
  }

  const body = bodyFromCustomFields(customFields, email);
  if (!body.email && email) body.email = email;

  const uniqueWebflowId = memberstackId || "memberstack-webhook";
  const options = {};
  if (memberHasApprovedPlan(plans)) {
    options.statusOnWrite = (process.env.SIGNUP_AIRTABLE_APPROVED_STATUS || "Active").trim();
  }

  const { record } = await upsertSignupUserRecord(body, uniqueWebflowId, options);
  return { recordId: record.id, email: body.email };
}
