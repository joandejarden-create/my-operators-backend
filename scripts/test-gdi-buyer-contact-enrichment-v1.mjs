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
  targetRolesForOpportunity,
} from "../lib/group-demand-intelligence/workflow/buyer-contact-enrichment-contract-v1.js";
import { classifyPersonResponsibility as classifyResp } from "../lib/group-demand-intelligence/workflow/buyer-contact-enrichment-v1.js";
import {
  buildFieldProvenanceRecord,
  mergeFieldProvenance,
  sanitizeContactForCustomerDisplay,
  assertNoProviderMetadataLeak,
  FIELD_KINDS,
} from "../lib/group-demand-intelligence/workflow/contact-field-provenance-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { estimateSurfeUsd } from "../lib/group-demand-intelligence/workflow/surfe-usage-ledger-v1.js";

// A. Functional contact qualifies; named enrichment pending
{
  const axes = assessContactResearchAxes({
    functionalContactEmail: "associatedirector@amwa-doc.org",
    organizationContactUrl: "https://amwa-doc.org/contact/",
  });
  assert.equal(axes.publicationContactStatus, PUBLICATION_CONTACT_STATUS.USABLE_ROUTE);
  assert.equal(axes.namedResearchStatus, NAMED_RESEARCH_STATUS.PENDING);
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

console.log("test-gdi-buyer-contact-enrichment-v1: PASS");
