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
        blocked += 1;
        status = "blocked";
        reason = "surfe_authorized_but_not_invoked_this_pass";
        resolutionPath.push("surfe_deferred");
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
    surfe: authorizeSurfe ? "authorized_not_invoked" : "not_authorized",
    note: "Isolated candidate storage only — no production opportunity writes; Surfe not called",
  };

  fs.writeFileSync(
    path.join(outDir, "execution-summary.json"),
    JSON.stringify({ ...summary, results }, null, 2) + "\n"
  );

  return { ...summary, results, officialPassCounts: officialPass.counts || null };
}
