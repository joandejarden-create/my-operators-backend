/**
 * Webhound escalation adapter — teacher layer, not production writer.
 * Does NOT auto-write Webhound output into canonical contacts.
 *
 * Teacher patterns exist in:
 * - reports/webhound-teacher-contact-experiment-v1.json
 * - lib/hotel-intelligence/contact-intelligence/investor-document-discovery.js (IR/PDF lane)
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { mayEscalateExternal } from "../../research-methods/escalation-policy.js";
import { INVESTOR_DOC_DISCOVERY_VERSION } from "../investor-document-discovery.js";
import { EMAIL_STATUS, PHONE_TYPE } from "./vocabulary.js";

export const WEBHOUND_ESCALATION_V2 = "webhound-escalation-v2";

const TEACHER_REPORT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../../reports/webhound-teacher-contact-experiment-v1.json"
);

/** Cached teacher pattern registry — reconciles V2 "no patterns" vs prior experiment. */
export function loadTeacherPatternRegistry() {
  try {
    if (!fs.existsSync(TEACHER_REPORT)) {
      return {
        ok: false,
        reason: "teacher_report_missing",
        investor_doc_module: INVESTOR_DOC_DISCOVERY_VERSION,
      };
    }
    const j = JSON.parse(fs.readFileSync(TEACHER_REPORT, "utf8"));
    return {
      ok: true,
      report: "reports/webhound-teacher-contact-experiment-v1.json",
      investor_doc_module: INVESTOR_DOC_DISCOVERY_VERSION,
      baseline_keys: [
        ...Object.keys(j.frozen_baseline?.first_party || {}),
        ...Object.keys(j.frozen_baseline?.datalayer || {}).filter((k) => k !== "others"),
      ],
      webhound_teacher_table_count: (j.webhound_teacher_table || []).length,
      patterns: [
        "investor_ir_pdf_named_email (GSF 1Q26 webcast — reproduced natively)",
        "first_party_about_leadership_extraction (Dovetail CDO/CEO)",
        "provider_directory_corroboration_not_incremental_first_party (Phil Hospod ContactOut)",
      ],
      note: "V2 must not claim zero teacher patterns; escalate via planWebhoundEscalation only with explicit budget.",
    };
  } catch (err) {
    return { ok: false, reason: err.message, investor_doc_module: INVESTOR_DOC_DISCOVERY_VERSION };
  }
}

/**
 * Minimum contact threshold for escalation.
 */
export function needsWebhoundEscalation(resolution = {}) {
  const contacts = resolution.contacts || [];
  const org = resolution.organization_contacts || {};

  const hasPersonEmail = contacts.some(
    (c) =>
      c.full_name &&
      c.email &&
      [EMAIL_STATUS.EXPLICIT_VERIFIED, EMAIL_STATUS.EXPLICIT_UNVERIFIED, EMAIL_STATUS.INFERRED_VERIFIED].includes(
        c.email_status
      )
  );
  const hasUsefulPhone = contacts.some(
    (c) =>
      c.full_name &&
      c.phone &&
      [PHONE_TYPE.DIRECT_MOBILE, PHONE_TYPE.DIRECT_OFFICE, PHONE_TYPE.EXECUTIVE_OFFICE].includes(c.phone_type)
  );
  const hasStrongCorporate =
    Boolean(org.general_email || org.corporate_phone) && Boolean(resolution.canonical_domain);

  return !(hasPersonEmail || hasUsefulPhone || hasStrongCorporate);
}

export function buildWebhoundEscalationRequest({
  owner_entity_id,
  owner_org_name,
  country = null,
  hotel_context_ids = [],
  domain = null,
} = {}) {
  return {
    version: WEBHOUND_ESCALATION_V2,
    owner_entity_id,
    owner_org_name,
    country,
    hotel_context_ids,
    domain,
    ask_for: [
      "owner-side decision maker (not hotel GM / brand employee)",
      "professional email",
      "useful phone",
      "source evidence with URLs",
      "exact research steps taken",
      "queries used",
      "source classes used",
      "entity transitions",
      "unresolved ambiguity",
    ],
    write_policy: {
      auto_write_canonical_contacts: false,
      require_validation_gates: true,
      teacher_success_on_validated_contact: true,
    },
    created_at: new Date().toISOString(),
  };
}

/**
 * Decide whether to escalate. Never auto-invokes spend.
 */
export function planWebhoundEscalation(resolution, options = {}) {
  const allow = options.allow_webhound !== false;
  const budget_allowed = Boolean(options.budget_allowed);
  const needs = needsWebhoundEscalation(resolution);
  const policyOk = mayEscalateExternal({
    native_exhausted: needs,
    case_value_warrants: options.case_value_warrants !== false,
    teaches_reusable_method: true,
    budget_allowed,
  });

  const teacherPatterns = loadTeacherPatternRegistry();
  const should = allow && needs && policyOk;
  return {
    should_escalate: should,
    needs_threshold: needs,
    policy_ok: policyOk,
    allow_webhound: allow,
    teacher_patterns: teacherPatterns,
    request: should
      ? buildWebhoundEscalationRequest({
          owner_entity_id: resolution.owner_entity_id,
          owner_org_name: resolution.owner_org_name,
          country: options.country,
          hotel_context_ids: options.hotel_context_ids,
          domain: resolution.canonical_domain,
        })
      : null,
    note: should
      ? "Escalation planned only — caller must invoke Webhound with explicit budget; results must pass gates before any canonical write."
      : "No escalation",
  };
}

/**
 * Validate Webhound candidate payload before any store write.
 */
export function validateWebhoundCandidates(payload = {}) {
  const contacts = [];
  const rejected = [];
  for (const c of payload.contacts || []) {
    if (!c.full_name || !(c.email || c.phone)) {
      rejected.push({ reason: "incomplete", snapshot: c });
      continue;
    }
    if (!Array.isArray(c.evidence) || !c.evidence.length) {
      rejected.push({ reason: "missing_evidence", snapshot: c });
      continue;
    }
    contacts.push({
      ...c,
      provider: "webhound",
      pending_gate: true,
      auto_written: false,
    });
  }
  return {
    ok: contacts.length > 0,
    contacts,
    rejected,
    TEACHER_SUCCESS: contacts.length > 0,
    note: "Validated candidates only — not written to canonical store by this adapter.",
  };
}
