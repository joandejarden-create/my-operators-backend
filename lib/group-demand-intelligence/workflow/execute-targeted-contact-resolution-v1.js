/**
 * Execute targeted contact resolution for coordinator enrich_contacts stage.
 * Uses existing evidence + official-source retrieval first. No Surfe unless authorized.
 * Writes isolated candidate results (not production opportunity writes).
 */

import fs from "node:fs";
import path from "node:path";
import { resolveOpportunityContact } from "../weekly-contact-resolution-v1-2.js";
import { applyContactResolutionPass } from "../contact-resolution-pass.js";
import {
  classifyContactCompletenessGroup,
  evaluateProspectPublicationEligibility,
} from "../prospect-publication-eligibility-v1.js";
import { phoneTypeCustomerLabel } from "../reviewed-prospect-publication-v1.js";
import { isGdiParallelSurfeFallbackEnabled } from "../parallel/feature-flag.js";
import {
  startSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
  normalizeSurfePerson,
  getSurfeApiKeyFromEnv,
} from "../../surfe/client.js";
import {
  acceptSurfeIdentity,
  IDENTITY_DECISION,
} from "../../hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js";
import {
  SURFE_EMAIL_CREDIT_USD_ESTIMATE,
} from "./paid-multisegment-research-v1.js";

export const EXECUTE_TARGETED_CONTACT_VERSION =
  "gdi-execute-targeted-contact-resolution-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function contactSnapshot(opp = {}) {
  const c = opp.primaryContact || {};
  return {
    name: c.name || null,
    email: c.email || opp.functionalContactEmail || null,
    phone: c.phone || null,
    phoneType: c.phoneType || opp.primaryContactPhoneType || null,
    phoneTypeLabel:
      c.phoneTypeLabel ||
      phoneTypeCustomerLabel(c.phoneType || opp.primaryContactPhoneType) ||
      null,
    role: c.role || c.title || null,
    relevanceEvidence: c.relevanceEvidence || c.whyThisContact || null,
    sourceUrl: c.sourceUrl || opp.officialSource || null,
    verificationDate: c.verificationDate || null,
    bookingResponsibilityClass:
      opp.bookingResponsibilityClass || c.relationshipToOpportunity || null,
  };
}

/**
 * @param {object} opts
 * @param {string} opts.hotelId
 * @param {object[]} opts.opportunities
 * @param {object} opts.plan — from buildTargetedContactPlan
 * @param {string} opts.outDir — isolated candidate storage
 * @param {boolean} [opts.authorizeSurfe=false]
 * @param {number} [opts.maxExecute=12]
 */
export async function executeTargetedContactResolution({
  hotelId,
  opportunities = [],
  plan = {},
  outDir,
  authorizeSurfe = false,
  maxExecute = 12,
} = {}) {
  if (!outDir) throw new Error("outDir_required");
  fs.mkdirSync(outDir, { recursive: true });

  const byId = new Map((opportunities || []).map((o) => [o.id, o]));
  const targets = (plan.targets || []).slice(0, maxExecute);
  const results = [];
  let attempted = 0;
  let resolved = 0;
  let unresolved = 0;
  let blocked = 0;

  // First: apply official curated pass across targeted set (local, $0)
  const scoped = targets.map((t) => byId.get(t.opportunityId)).filter(Boolean);
  const officialPass = applyContactResolutionPass(scoped);
  const officialList = Array.isArray(officialPass)
    ? officialPass
    : officialPass.opportunities || officialPass.results || scoped;
  const officialById = new Map(officialList.map((o) => [o.id, o]));

  for (const target of targets) {
    const baseOpp = officialById.get(target.opportunityId) || byId.get(target.opportunityId);
    if (!baseOpp) {
      blocked += 1;
      results.push({
        opportunityId: target.opportunityId,
        status: "blocked",
        reason: "opportunity_not_found_in_store",
        surfeUsed: false,
      });
      continue;
    }

    attempted += 1;
    const before = contactSnapshot(baseOpp);
    let working = { ...baseOpp };
    let status = "unresolved";
    let reason = "no_usable_route_after_official_pass";
    let fetchesUsed = 0;
    let surfeUsed = false;
    let resolutionPath = ["reuse_verified_existing", "official_curated_pass"];

    try {
      // Existing evidence first
      const hasRoute =
        text(before.email) ||
        text(before.phone) ||
        text(working.functionalContactUrl);
      if (hasRoute && (before.name || before.email)) {
        status = "resolved";
        reason = "existing_or_official_curated_route";
        resolutionPath.push("reuse_existing");
      } else {
        // Official-source page resolution (no Surfe)
        const r = await resolveOpportunityContact(working, {
          maxFetches: 4,
          allowSurfe: false,
          authorizeSurfe: false,
        });
        fetchesUsed = r.fetchesUsed || 0;
        working = r.opportunity || working;
        resolutionPath.push("official_source_retrieval");
        const afterProbe = contactSnapshot(working);
        if (afterProbe.email || afterProbe.phone) {
          status = "resolved";
          reason = r.blankReason
            ? `partial_then_route:${r.blankReason}`
            : "official_source_resolution";
        } else {
          status = "unresolved";
          reason = r.blankReason || "official_sources_exhausted";
        }
      }

      if (authorizeSurfe === true && status === "unresolved") {
        const surfeFlag = isGdiParallelSurfeFallbackEnabled(process.env);
        const named =
          text(working.primaryContact?.name) ||
          text(before.name) ||
          text(target.personName);
        const domain =
          text(working.officialDomain) ||
          text(working.organizationDomain) ||
          text(target.companyDomain);
        const orgName =
          text(working.organizationName) || text(working.company);
        if (!surfeFlag) {
          blocked += 1;
          status = "blocked";
          reason = "surfe_flag_off_set_GDI_PARALLEL_SURFE_FALLBACK_ENABLED";
          resolutionPath.push("surfe_flag_blocked");
        } else if (!named || named.split(/\s+/).length < 2) {
          status = "unresolved";
          reason = "surfe_skipped_no_named_person";
          resolutionPath.push("surfe_requires_named_person");
        } else if (!getSurfeApiKeyFromEnv()) {
          blocked += 1;
          status = "blocked";
          reason = "surfe_key_missing";
          resolutionPath.push("surfe_key_missing");
        } else {
          const parts = named.split(/\s+/);
          const firstName = parts[0];
          const lastName = parts.slice(1).join(" ");
          const start = await startSurfePeopleEnrichment({
            people: [
              {
                firstName,
                lastName,
                companyName: orgName || undefined,
                companyDomain: domain || undefined,
                externalID: `gdi_contact_${working.id}`.slice(0, 64),
              },
            ],
            include: { email: true, mobile: false, linkedInUrl: true },
            enrichmentOptions: { acceptedEmailType: "professional" },
          });
          if (!start.ok) {
            blocked += 1;
            status = "blocked";
            reason = `surfe_start_failed:${start.error || start.http_status}`;
            resolutionPath.push("surfe_start_failed");
          } else {
            const enrichmentId =
              start.payload?.enrichmentID ||
              start.payload?.enrichmentId ||
              start.payload?.id ||
              null;
            const done = await pollSurfePeopleEnrichment(enrichmentId, {
              maxWaitMs: 180000,
            });
            surfeUsed = true;
            resolutionPath.push("surfe_enrichment");
            const person = normalizeSurfePerson(
              (done.payload?.people || [])[0] || {}
            );
            const surfeEmail =
              person.emails?.find((e) => e.validation_status === "VALID")
                ?.email ||
              person.emails?.[0]?.email ||
              null;
            const identity = acceptSurfeIdentity({
              expectedFullName: named,
              expectedOrganization: orgName,
              expectedDomain: domain || null,
              expectedTitle: before.role || null,
              returnedFullName: person.full_name,
              returnedOrganization: person.company_name,
              returnedDomain: person.company_domain,
              returnedTitle: person.job_title,
              returnedEmail: surfeEmail,
              returnedLinkedInUrl: person.linkedin_url,
            });
            const identityOk =
              identity?.decision === IDENTITY_DECISION.ACCEPTED ||
              identity?.decision ===
                IDENTITY_DECISION.ACCEPTED_WITH_LIMITED_EVIDENCE ||
              identity?.decision === IDENTITY_DECISION.CORROBORATION_ONLY;
            if (identityOk && surfeEmail) {
              working = {
                ...working,
                primaryContact: {
                  ...(working.primaryContact || {}),
                  name: named,
                  email: surfeEmail,
                  role: person.job_title || before.role || null,
                  sourceUrl: person.linkedin_url || before.sourceUrl || null,
                  verificationDate: new Date().toISOString().slice(0, 10),
                  surfeEnriched: true,
                  provider: "surfe",
                  estimatedCostUsd: SURFE_EMAIL_CREDIT_USD_ESTIMATE,
                },
                functionalContactEmail: surfeEmail,
              };
              status = "resolved";
              reason = "surfe_email_accepted_after_identity_gate";
            } else {
              status = "unresolved";
              reason = identityOk
                ? "surfe_no_email"
                : `surfe_identity_${identity?.decision || "rejected"}`;
            }
          }
        }
      }
    } catch (err) {
      status = "blocked";
      reason = err?.message || "resolution_error";
      blocked += 1;
    }

    const after = contactSnapshot(working);
    const group = classifyContactCompletenessGroup(working);
    const elig = evaluateProspectPublicationEligibility(working);

    if (status === "resolved") resolved += 1;
    else if (status === "unresolved") unresolved += 1;

    const candidate = {
      hotelId,
      opportunityId: working.id,
      organizationName: working.organizationName || working.company || null,
      title: working.title || null,
      status,
      reason,
      resolutionPath,
      fetchesUsed,
      surfeUsed,
      before,
      after,
      contactCompletenessGroup: group,
      eligibility: elig.eligibility,
      hotelFacing: elig.hotelFacing === true,
      enrichmentQueue: elig.enrichmentQueue === true,
      fieldSources: {
        email: after.email
          ? {
              value: after.email,
              sourceUrl: after.sourceUrl,
              verificationDate: after.verificationDate || new Date().toISOString().slice(0, 10),
            }
          : null,
        phone: after.phone
          ? {
              value: after.phone,
              phoneType: after.phoneType,
              phoneTypeLabel: after.phoneTypeLabel,
              sourceUrl: after.sourceUrl,
              verificationDate: after.verificationDate || new Date().toISOString().slice(0, 10),
            }
          : null,
        responsibilityClass: after.bookingResponsibilityClass || null,
      },
      // Candidate storage only — not a production write
      candidateOnly: true,
      updatedOpportunityPatch: {
        primaryContact: working.primaryContact || null,
        functionalContactEmail: working.functionalContactEmail || null,
        contactCompletenessGroup: group,
        bookingResponsibilityClass: working.bookingResponsibilityClass || null,
      },
    };

    results.push(candidate);
    fs.writeFileSync(
      path.join(outDir, `${working.id}.json`),
      JSON.stringify(candidate, null, 2) + "\n"
    );
  }

  const summary = {
    version: EXECUTE_TARGETED_CONTACT_VERSION,
    hotelId,
    attempted,
    resolved,
    unresolved,
    blocked,
    targetCount: targets.length,
    executedAt: new Date().toISOString(),
    surfe: authorizeSurfe
      ? results.some((r) => r.surfeUsed)
        ? "invoked"
        : "authorized_no_eligible_named_person_or_flag"
      : "not_authorized",
    note: "Isolated candidate storage only — no production opportunity writes unless a later publish stage applies accepted patches",
  };

  fs.writeFileSync(
    path.join(outDir, "execution-summary.json"),
    JSON.stringify({ ...summary, results }, null, 2) + "\n"
  );

  return { ...summary, results, officialPassCounts: officialPass.counts || null };
}
