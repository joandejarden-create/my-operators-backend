#!/usr/bin/env node
/**
 * Focused tests for buyer contact enrichment contract + provenance + leak checks.
 */
import assert from "node:assert/strict";
import {
  assessContactResearchAxes,
  RESPONSIBILITY_CLASS,
  NAMED_RESEARCH_STATUS,
  PUBLICATION_CONTACT_STATUS,
  CHANNEL_OWNERSHIP,
  CONTACT_DEPTH_OUTCOME,
  targetRolesForOpportunity,
} from "../lib/group-demand-intelligence/workflow/buyer-contact-enrichment-contract-v1.js";
import {
  classifyPersonResponsibility as classifyResp,
  buildBuyerContactEnrichmentQueue,
} from "../lib/group-demand-intelligence/workflow/buyer-contact-enrichment-v1.js";
import { buildContactDisplaySurface } from "../lib/group-demand-intelligence/contact-display-surface-v1.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildFieldProvenanceRecord,
  mergeFieldProvenance,
  sanitizeContactForCustomerDisplay,
  assertNoProviderMetadataLeak,
  FIELD_KINDS,
} from "../lib/group-demand-intelligence/workflow/contact-field-provenance-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { estimateSurfeUsd } from "../lib/group-demand-intelligence/workflow/surfe-usage-ledger-v1.js";
import { hasUsableContactRoute } from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import { preservePublicationMetadata } from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";

// A. Functional contact qualifies; named enrichment pending
{
  const axes = assessContactResearchAxes({
    functionalContactEmail: "associatedirector@amwa-doc.org",
    organizationContactUrl: "https://amwa-doc.org/contact/",
  });
  assert.equal(axes.publicationContactStatus, PUBLICATION_CONTACT_STATUS.USABLE_ROUTE);
  assert.equal(axes.namedResearchStatus, NAMED_RESEARCH_STATUS.NOT_ATTEMPTED);
  assert.equal(axes.enrichmentQueue, true);
  assert.match(axes.customerMessageWhenNamedPending, /not yet been established/i);
}

// B. President is not automatic booking decision-maker
{
  const r = classifyResp(
    { title: "President", relevanceEvidence: "LinkedIn title only" },
    { researchSegment: "associations_government", title: "AMWA Annual Meeting" }
  );
  assert.equal(r.class, RESPONSIBILITY_CLASS.REFERRAL_ENTRY);
  assert.equal(r.leadOnly, true);
}

// C. Meetings staff can be confirmed relevant
{
  const r = classifyResp(
    {
      title: "Meetings Manager",
      relevanceEvidence: "Listed on AMWA meetings staff page as housing/venue contact",
    },
    { researchSegment: "associations_government", title: "AMWA Annual Meeting" }
  );
  assert.equal(r.class, RESPONSIBILITY_CLASS.CONFIRMED_RELEVANT);
}

// D. Field provenance: Surfe corroboration preserves original source
{
  const official = buildFieldProvenanceRecord({
    fieldKind: FIELD_KINDS.EMAIL,
    value: "associatedirector@amwa-doc.org",
    sourceProvider: "official_page",
    sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/",
  });
  const surfeSame = buildFieldProvenanceRecord({
    fieldKind: FIELD_KINDS.EMAIL,
    value: "associatedirector@amwa-doc.org",
    sourceProvider: "surfe",
    providerRequestId: "enrich_test",
  });
  const merged = mergeFieldProvenance(official, surfeSame);
  assert.equal(merged.originalSourceProvider, "official_page");
  assert.ok((merged.corroborations || []).length >= 1);
}

// E. Official phone + Surfe email retain different sources
{
  const bag = {
    email: buildFieldProvenanceRecord({
      fieldKind: FIELD_KINDS.EMAIL,
      value: "jane@amwa-doc.org",
      sourceProvider: "surfe",
      providerRequestId: "e1",
    }),
    phone: buildFieldProvenanceRecord({
      fieldKind: FIELD_KINDS.PHONE,
      value: "301-555-0100",
      sourceProvider: "official_page",
      phoneType: "DEPARTMENT",
    }),
  };
  assert.equal(bag.email.originalSourceProvider, "surfe");
  assert.equal(bag.phone.originalSourceProvider, "official_page");
}

// F. Customer sanitize strips Surfe metadata; unknown phone not labeled Direct
{
  const safe = sanitizeContactForCustomerDisplay({
    name: "Jane Doe",
    email: "jane@amwa-doc.org",
    phone: "301-555-0100",
    phoneType: "UNKNOWN",
    surfeEnriched: true,
    provider: "surfe",
    providerRequestId: "secret",
    estimatedCostUsd: 0.25,
  });
  assert.equal(safe.email, "jane@amwa-doc.org");
  assert.equal(safe.phoneTypeLabel, "Phone");
  assert.equal(safe.surfeEnriched, undefined);
  assert.equal(safe.provider, undefined);
  assert.equal(safe.estimatedCostUsd, undefined);
}

// G. List DTO leak check
{
  const dto = toOpportunityListDto({
    id: "gdi_test",
    title: "Test",
    organizationName: "AMWA",
    primaryContact: {
      name: "Jane Doe",
      email: "jane@x.org",
      surfeEnriched: true,
      provider: "surfe",
      providerRequestId: "leak",
      estimatedCostUsd: 0.25,
    },
  });
  const leak = assertNoProviderMetadataLeak(dto);
  assert.equal(leak.ok, true, JSON.stringify(leak.leaks));
  assert.equal(dto.primaryContact.email, "jane@x.org");
  assert.equal(dto.primaryContact.provider, undefined);
}

// H. Surfe cost estimate known
{
  const est = estimateSurfeUsd({ email: true });
  assert.equal(est.basisKnown, true);
  assert.ok(est.estimatedUsd > 0);
}

// I. Association use-case roles
{
  const roles = targetRolesForOpportunity({
    researchSegment: "associations_government",
    title: "AMWA Annual Meeting 2027",
  });
  assert.equal(roles.bucket, "ASSOCIATION_MEETINGS");
  assert.ok(roles.roles.includes("housing_lead"));
}

// J. Organization inbox does NOT suppress named-person research queue
{
  const axes = assessContactResearchAxes({
    functionalContactEmail: "info@planner.com",
    organizationContactUrl: "https://planner.com/contact",
    namedContactResearchAttempted: false,
  });
  assert.equal(axes.publicationContactStatus, PUBLICATION_CONTACT_STATUS.USABLE_ROUTE);
  assert.equal(axes.namedResearchStatus, NAMED_RESEARCH_STATUS.NOT_ATTEMPTED);
  assert.equal(axes.enrichmentQueue, true);
  assert.ok(
    axes.queueReasons.includes("functional_route_only_named_enrichment_pending") ||
      axes.queueReasons.includes("named_person_research_not_yet_attempted")
  );
}

// K. Exhausted named research retains org fallback outcome (≠ not researched)
{
  const axes = assessContactResearchAxes({
    functionalContactEmail: "info@org.org",
    publicContactPath: "https://org.org/contact",
    namedContactResearchAttempted: true,
    namedContactResearchStatus: NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK,
    contactDepthResearch: {
      outcome: "organization_fallback_after_bounded_person_research",
    },
  });
  assert.equal(
    axes.namedResearchStatus,
    NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK
  );
  assert.match(axes.customerMessageWhenNamedPending, /after research/i);
}

// L. Functional form URL (query-param contact) can satisfy basic route;
//    homepage alone does not automatically qualify
{
  const formOk = hasUsableContactRoute({
    publicContactPath: "https://whfdc.org/general/?type=CONTACT",
  });
  assert.equal(formOk.ok, true);
  const home = hasUsableContactRoute({
    publicContactPath: "https://example.org/",
  });
  assert.equal(home.ok, false);
}

// N. Enrichment preservePublicationMetadata keeps visibility + feedback
{
  const base = {
    id: "gdi_test_preserve",
    customerVisible: true,
    prospectHotelFacing: true,
    prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
    hotelFeedbackNote: "customer said contact was helpful",
    functionalContactEmail: "info@org.org",
  };
  const patched = preservePublicationMetadata(base, {
    ...base,
    primaryContact: { name: "Jane Doe", email: "jane@org.org" },
  });
  assert.equal(patched.customerVisible, true);
  assert.equal(patched.prospectHotelFacing, true);
  assert.equal(patched.prospectPublicationBasis, "REVIEWED_PACKAGE_V1");
  assert.equal(patched.hotelFeedbackNote, "customer said contact was helpful");
}

// M. Planner owner is not automatic referral-only
{
  const r = classifyResp(
    {
      title: "Owner / Principal",
      relevanceEvidence: "Official about page lists founder of planning firm",
    },
    {
      title: "Professional planners & intermediaries with recurring event business",
      researchSegment: "local_meetings_catering",
    }
  );
  assert.notEqual(r.class, RESPONSIBILITY_CLASS.REFERRAL_ENTRY);
}

// O. Current researched named contact with person channel is not re-queued
{
  const complete = {
    id: "gdi_done",
    contactName: "Jane Doe",
    primaryContact: {
      name: "Jane Doe",
      email: "jane.doe@amwa-doc.org",
      emailOwnership: CHANNEL_OWNERSHIP.PERSON_SPECIFIC,
    },
    contactResponsibilityClass: RESPONSIBILITY_CLASS.CONFIRMED_RELEVANT,
    namedContactResearchAttempted: true,
    namedContactResearchCompletedAt: "2026-10-01T00:00:00Z",
  };
  const axes = assessContactResearchAxes(complete);
  assert.equal(axes.namedResearchStatus, NAMED_RESEARCH_STATUS.COMPLETE);
  assert.equal(axes.enrichmentQueue, false);
  assert.equal(
    axes.contactDepthOutcome,
    CONTACT_DEPTH_OUTCOME.NAMED_WITH_PERSON_CHANNEL
  );
  const q = buildBuyerContactEnrichmentQueue({
    hotelId: "h1",
    opportunities: [complete],
    maxItems: 12,
  });
  assert.equal((q.items || []).length, 0);
}

// P. Named person + org inbox only still queues for person-channel work
{
  const axes = assessContactResearchAxes({
    contactName: "André Wells",
    primaryContact: {
      name: "André Wells",
      email: "info@eventsbyandrewells.com",
      emailOwnership: CHANNEL_OWNERSHIP.ORGANIZATION_DEPARTMENT,
    },
    functionalContactEmail: "info@eventsbyandrewells.com",
    contactResponsibilityClass: RESPONSIBILITY_CLASS.CONFIRMED_RELEVANT,
    namedContactResearchAttempted: true,
  });
  assert.equal(
    axes.namedResearchStatus,
    NAMED_RESEARCH_STATUS.PERSON_ESTABLISHED_CHANNEL_PENDING
  );
  assert.equal(axes.enrichmentQueue, true);
  assert.ok(
    axes.queueReasons.includes(
      "org_inbox_present_person_specific_channel_still_required"
    ) ||
      axes.queueReasons.includes(
        "named_person_established_person_specific_channel_still_pending"
      )
  );
}

// Q. Weekly paid Surfe remains off without explicit authorization (source lock)
{
  const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
  const weekly = fs.readFileSync(
    path.join(
      root,
      "lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js"
    ),
    "utf8"
  );
  // Surfe stays opt-in via destructured authorizeSurfe (default false) — never hard-true.
  assert.match(weekly, /authorizeSurfe:\s*authorizeSurfe\s*===\s*true/);
  assert.match(weekly, /authorizeSurfe\s*=\s*false/);
  assert.match(weekly, /execute_public_contact_depth/);
  assert.doesNotMatch(weekly, /authorizeSurfe:\s*true\b/);
}

// R. Organization fallback must not invent a person from org-local email (WHF)
{
  const s = buildContactDisplaySurface({
    organizationName: "Women in Housing & Finance, Inc.",
    contactEmail: "whf@whfdc.org",
    functionalContactEmail: "whf@whfdc.org",
    publicContactPath: "https://whfdc.org/general/?type=CONTACT",
    companyPhone: "703-683-4742",
    namedContactResearchStatus: NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK,
    primaryContact: {
      name: null,
      email: null,
      organizationContactRoute: "whf@whfdc.org",
    },
  });
  assert.equal(s.hasNamedPerson, false);
  assert.equal(s.primaryContact, null);
  assert.equal(s.organizationContactEmail, "whf@whfdc.org");
  assert.equal(s.namedBuyerEnrichmentComplete, false);
  const dto = toOpportunityListDto({
    id: "gdi_test_whf",
    title: "WHF",
    organizationName: "WHF",
    contactEmail: "whf@whfdc.org",
    functionalContactEmail: "whf@whfdc.org",
    namedContactResearchStatus: NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK,
    publicContactPath: "https://whfdc.org/general/?type=CONTACT",
  });
  assert.equal(dto.primaryContact, null);
  assert.equal(dto.organizationContactEmail, "whf@whfdc.org");
}

// S. Named person + shared org inbox: keep person, demote shared inbox to org route
{
  const s = buildContactDisplaySurface({
    primaryContactName: "Sin Bosier",
    primaryContact: {
      name: "Sin Bosier",
      role: "President & CEO",
      email: null,
      relevanceEvidence:
        "enrichment provider returned sin@clubvipsd.com — quarantined as employer/domain conflict vs Gaslamp",
    },
    contactEmail: "gaslampevent@gmail.com",
    functionalContactEmail: "gaslampevent@gmail.com",
  });
  assert.equal(s.primaryContact.name, "Sin Bosier");
  assert.equal(s.primaryContact.email, null);
  assert.equal(s.organizationContactEmail, "gaslampevent@gmail.com");
  assert.equal(s.namedBuyerEnrichmentComplete, false);
  assert.doesNotMatch(s.primaryContact.relevanceEvidence || "", /clubvipsd/i);
}

console.log("test-gdi-buyer-contact-enrichment-v1: PASS");
