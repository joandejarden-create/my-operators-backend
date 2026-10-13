/**
 * Recover saved GSF / Dovetail baseline evidence without new paid discovery.
 * Sources: prior person-email experiment, provider evals, Webhound teacher table.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { GSF_OWNER_ENTITY_ID } from "../../ownership/owner-control/compilers/gsf-from-evidence.js";
import { DOVETAIL_ENTITY_ID } from "../../ownership/owner-control/compilers/cambridge-from-evidence.js";
import { EMAIL_STATUS } from "./vocabulary.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../..");

export const BASELINE_RECOVERY_VERSION = "baseline-evidence-recovery-v1";

const OWNER_BY_COMPANY = Object.freeze({
  "grupo hotelero santa fe": GSF_OWNER_ENTITY_ID,
  "dovetail + co": DOVETAIL_ENTITY_ID,
  "dovetail and co": DOVETAIL_ENTITY_ID,
});

const KNOWN_GOLDEN_DOMAINS = Object.freeze({
  [GSF_OWNER_ENTITY_ID]: "gsf-hotels.com",
  [DOVETAIL_ENTITY_ID]: "dovetailandco.com",
});

function readJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  const raw = fs.readFileSync(p, "utf8");
  try {
    return JSON.parse(raw);
  } catch {
    // Some eval reports were appended in-place — parse first complete root object.
    const start = raw.indexOf("{");
    if (start < 0) return null;
    let depth = 0;
    for (let i = start; i < raw.length; i++) {
      const ch = raw[i];
      if (ch === "{") depth++;
      else if (ch === "}") {
        depth--;
        if (depth === 0) {
          try {
            return JSON.parse(raw.slice(start, i + 1));
          } catch {
            return null;
          }
        }
      }
    }
    return null;
  }
}

function companyToOwnerId(company) {
  const key = String(company || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
  return OWNER_BY_COMPANY[key] || null;
}

function mapAttributionToEmailStatus(attribution, providerReported = false) {
  const a = String(attribution || "").toUpperCase();
  if (a === "NAMED_PERSON" || a === "FIRST_PARTY") {
    return EMAIL_STATUS.EXPLICIT_VERIFIED;
  }
  if (providerReported || a === "PROVIDER_ATTRIBUTED") {
    return EMAIL_STATUS.EXPLICIT_UNVERIFIED;
  }
  return EMAIL_STATUS.INFERRED_UNVERIFIED;
}

/**
 * @returns {{ owner_entity_id: string, contacts: object[], organization_contacts: object, sources_loaded: string[], recovered_at: string|null }[]}
 */
export function loadRecoveredBaselineForOwner(ownerEntityId) {
  const out = {
    owner_entity_id: ownerEntityId,
    contacts: [],
    organization_contacts: {
      general_email: null,
      corporate_phone: null,
      website: KNOWN_GOLDEN_DOMAINS[ownerEntityId]
        ? `https://${KNOWN_GOLDEN_DOMAINS[ownerEntityId]}`
        : null,
    },
    sources_loaded: [],
    recovered_at: null,
    recovery_kind: "RECOVERED_BASELINE",
  };

  const personEmail = readJson("reports/contact-intelligence-person-email-gsf-dovetail.json");
  if (personEmail?.table) {
    out.sources_loaded.push("reports/contact-intelligence-person-email-gsf-dovetail.json");
    out.recovered_at = personEmail.generated_at || out.recovered_at;
    for (const row of personEmail.table) {
      const oid = companyToOwnerId(row.company);
      if (oid !== ownerEntityId) continue;
      if (!row.business_email) continue;
      out.contacts.push({
        full_name: row.person,
        title: row.current_title,
        email: row.business_email,
        email_status: mapAttributionToEmailStatus(row.attribution_evidence?.attribution),
        email_method: "investor_document_or_first_party_pdf",
        email_publishable: mapAttributionToEmailStatus(row.attribution_evidence?.attribution) !== EMAIL_STATUS.INFERRED_UNVERIFIED,
        provider: "recovered_baseline",
        last_verified_at: personEmail.generated_at,
        do_not_advance_verification_date: true,
        evidence: row.attribution_evidence
          ? [
              {
                source_url: row.attribution_evidence.source_url,
                excerpt: row.attribution_evidence.excerpt,
                observed_at: personEmail.generated_at,
              },
            ]
          : [],
        recovery_source: "person_email_gsf_dovetail_table",
        leadership_category: row.leadership_category,
        functionally_relevant: row.functionally_relevant,
      });
    }
  }

  const fe = readJson("reports/fullenrich-person-eval-gsf-dovetail.json");
  if (fe?.evaluations?.length) {
    out.sources_loaded.push("reports/fullenrich-person-eval-gsf-dovetail.json#evaluations");
    out.recovered_at = fe.completed_at || out.recovered_at;
    for (const ev of fe.evaluations) {
      const oid =
        ev.target_company === "Grupo Hotelero Santa Fe"
          ? GSF_OWNER_ENTITY_ID
          : ev.target_company === "Dovetail + Co"
            ? DOVETAIL_ENTITY_ID
            : null;
      if (oid !== ownerEntityId || !ev.email) continue;
      const exists = out.contacts.some(
        (c) => String(c.email).toLowerCase() === String(ev.email).toLowerCase()
      );
      if (exists) continue;
      const isProvider = Boolean(ev.provider_verification?.status);
      out.contacts.push({
        full_name: ev.person || ev.returned_name,
        title: ev.confirmed_title || ev.returned_title,
        email: ev.email,
        email_status: isProvider ? EMAIL_STATUS.EXPLICIT_UNVERIFIED : EMAIL_STATUS.EXPLICIT_VERIFIED,
        email_method: "fullenrich_provider_recovered",
        email_publishable: false,
        provider: "fullenrich_recovered",
        provider_email_status: ev.provider_verification?.status || null,
        provider_verification_timestamp: ev.provider_verification?.verification_timestamp ?? null,
        usage_rights: "INTERNAL_ONLY",
        last_verified_at: fe.completed_at,
        do_not_advance_verification_date: true,
        do_not_count_as_new_discovery: true,
        evidence: [
          {
            source_url: "reports/fullenrich-person-eval-gsf-dovetail.json",
            excerpt: ev.provider_verification?.note || `FullEnrich ${ev.outcome}`,
            observed_at: fe.completed_at,
          },
        ],
        recovery_source: "fullenrich_eval_provider",
        role_supported_by_evidence: ev.role_supported_by_evidence,
        affiliation_class: ev.affiliation_class,
      });
    }
  }
  if (fe?.freeze?.subjects) {
    out.sources_loaded.push("reports/fullenrich-person-eval-gsf-dovetail.json");
    out.recovered_at = fe.completed_at || out.recovered_at;
    for (const subj of fe.freeze.subjects) {
      const oid =
        subj.organization === "Grupo Hotelero Santa Fe"
          ? GSF_OWNER_ENTITY_ID
          : subj.organization === "Dovetail + Co"
            ? DOVETAIL_ENTITY_ID
            : null;
      if (oid !== ownerEntityId || !subj.baseline_email) continue;
      const exists = out.contacts.some(
        (c) => String(c.email).toLowerCase() === String(subj.baseline_email).toLowerCase()
      );
      if (exists) continue;
      out.contacts.push({
        full_name: subj.full_name,
        title: subj.confirmed_title,
        email: subj.baseline_email,
        email_status: EMAIL_STATUS.EXPLICIT_VERIFIED,
        email_method: "recovered_baseline_first_party_or_provider",
        email_publishable: true,
        provider: "recovered_baseline",
        last_verified_at: fe.freeze.frozen_at || fe.completed_at,
        do_not_advance_verification_date: true,
        evidence: [{ excerpt: subj.baseline_note, observed_at: fe.freeze.frozen_at }],
        recovery_source: "fullenrich_eval_freeze_baseline",
      });
    }
  }

  const dl = readJson("reports/datalayer-mcp-person-eval-gsf-dovetail.json");
  const dlResults = []
    .concat(dl?.results || [])
    .concat(dl?.retry_results || []);
  if (dlResults.length) {
    out.sources_loaded.push("reports/datalayer-mcp-person-eval-gsf-dovetail.json");
    out.recovered_at = dl.completed_at || out.recovered_at;
    for (const r of dlResults) {
      const subj = dl.subjects?.find((s) => s.id === r.subject_id);
      if (!subj) continue;
      const oid = companyToOwnerId(subj.organization);
      if (oid !== ownerEntityId) continue;
      const email = r.business_email || r.email || r.provider_email;
      if (!email) continue;
      const exists = out.contacts.some((c) => String(c.email).toLowerCase() === String(email).toLowerCase());
      if (exists) continue;
      out.contacts.push({
        full_name: subj.full_name,
        title: subj.confirmed_title,
        email,
        email_status: mapAttributionToEmailStatus("PROVIDER_ATTRIBUTED", true),
        email_method: "provider_attributed_recovered",
        email_publishable: false,
        provider: "datalayer_recovered",
        provider_email_status: r.provider_verification?.email_status || r.provider_email_status || null,
        provider_email_status_note:
          "Preserve DataLayer provider-reported status; not Dealality-verified",
        last_verified_at: r.started_at || dl.completed_at,
        do_not_advance_verification_date: true,
        evidence: [
          {
            excerpt: r.evidence_category || "DataLayer provider-attributed email (recovered eval)",
            observed_at: r.started_at || dl.completed_at,
          },
        ],
        recovery_source: "datalayer_mcp_eval",
        usage_rights: "INTERNAL_ONLY",
      });
    }
  }

  const wh = readJson("reports/webhound-teacher-contact-experiment-v1.json");
  const whBaseline = {
    ...(wh?.frozen_baseline?.first_party || {}),
    ...(wh?.frozen_baseline?.datalayer || {}),
  };
  if (whBaseline.gsf_rodrigo_ancira_fuentes || whBaseline.dovetail_phil_hospod) {
    out.sources_loaded.push("reports/webhound-teacher-contact-experiment-v1.json");
    out.recovered_at = wh.completed_at || out.recovered_at;
    const entries = [
      { key: "gsf_rodrigo_ancira_fuentes", owner: GSF_OWNER_ENTITY_ID },
      { key: "dovetail_phil_hospod", owner: DOVETAIL_ENTITY_ID },
    ];
    for (const { key, owner } of entries) {
      if (owner !== ownerEntityId) continue;
      const b = whBaseline[key];
      if (!b?.email) continue;
      const exists = out.contacts.some((c) => String(c.email).toLowerCase() === String(b.email).toLowerCase());
      if (exists) continue;
      out.contacts.push({
        full_name: key.includes("rodrigo") ? "Rodrigo Ancira Fuentes" : "Phil Hospod",
        email: b.email,
        email_status: EMAIL_STATUS.EXPLICIT_VERIFIED,
        email_method: "webhound_teacher_baseline_corroboration",
        email_publishable: true,
        provider: "recovered_baseline",
        last_verified_at: wh.frozen_baseline?.frozen_at || wh.completed_at,
        do_not_advance_verification_date: true,
        evidence: [{ source_url: b.source, observed_at: wh.created_at }],
        recovery_source: "webhound_teacher_baseline",
      });
    }
  }

  return out;
}

export function getKnownGoldenDomain(ownerEntityId) {
  return KNOWN_GOLDEN_DOMAINS[ownerEntityId] || null;
}

export function listBaselineRecoveryOwnerIds() {
  return [GSF_OWNER_ENTITY_ID, DOVETAIL_ENTITY_ID];
}
