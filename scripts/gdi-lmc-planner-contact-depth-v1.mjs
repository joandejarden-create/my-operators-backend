#!/usr/bin/env node
/**
 * Contact-depth pass for the 13 Mayflower + San Diego LMC/planner accounts.
 * Uses shared buyer_contact_enrichment (no second pipeline).
 *
 * Usage:
 *   node scripts/gdi-lmc-planner-contact-depth-v1.mjs --dry-run
 *   node scripts/gdi-lmc-planner-contact-depth-v1.mjs --apply
 *   node scripts/gdi-lmc-planner-contact-depth-v1.mjs --apply --authorize-surfe
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import {
  enrichBuyerContactForOpportunity,
  writeBuyerEnrichmentArtifacts,
} from "../lib/group-demand-intelligence/workflow/buyer-contact-enrichment-v1.js";
import { persistBuyerContactPatchedOpportunity } from "../lib/group-demand-intelligence/workflow/corporate-package-persist-v1.js";
import {
  evaluateProspectPublicationEligibility,
  preservePublicationMetadata,
} from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import { remainingSharedBudgetUsd } from "../lib/group-demand-intelligence/workflow/surfe-usage-ledger-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-lmc-planner-contact-depth");
const apply = process.argv.includes("--apply");
const authorizeSurfe = process.argv.includes("--authorize-surfe");
const dryRun = !apply;

const TARGET_IDS = Object.freeze({
  rec0eTEnCqlsGJJt9: [
    "gdi_lmc_9a30c08278a92953", // A. Dominick
    "gdi_lmc_ceab66971a48a9f8", // N Street Village
    "gdi_lmc_5b4d51be7726617c", // CSI DMC
    "gdi_lmc_860cde704de88d7c", // Andre Wells
    "gdi_lmc_00cd663a9f282697", // Stratus
    "gdi_lmc_d670fde177ad418b", // Evoke
    "gdi_lmc_82918469d12cfbc1", // WHF
    "gdi_lmc_8bf1156a3b6bc8dc", // DCBIA
  ],
  recnPB3Q02NO8Ma1o: [
    "gdi_lmc_fa0049bb82e08bde", // Cohera
    "gdi_lmc_c8d0bd468ce1cc64", // SD Police Foundation
    "gdi_lmc_97fb727376abb83b", // Gaslamp
    "gdi_lmc_94d719381b8dca86", // Kiwanis
    "gdi_lmc_9d0050cfbdb5925c", // Thomas Bui
  ],
});

/** Public-source lead people — Surfe only when person established + channel pending. */
const LEADS = {
  gdi_lmc_9a30c08278a92953: {
    name: "Aimee Dominick",
    title: "Owner / Principal",
    profileUrl: "https://adominick.com/about",
    domain: "adominick.com",
    personEmail: "aimee@adominick.com",
    relevanceEvidence:
      "Official About page: Aimee founded A. Dominick Events (2005); LinkedIn lists Owner. Planner owner/principal is the venue-sourcing target for this account.",
    sourcesChecked: [
      "https://adominick.com/about",
      "https://adominick.com/contact",
      "https://linkedin.com/in/aimeedominick",
    ],
    discoveryMethod: "official_staff_about_page",
    sourceProvider: "official_page",
  },
  gdi_lmc_ceab66971a48a9f8: {
    name: "Makenzie Delmotte",
    title: "Senior Events Manager",
    profileUrl: "https://www.nstreetvillage.org/sponsor-an-event/",
    domain: "nstreetvillage.org",
    personEmail: "mdelmotte@nstreetvillage.org",
    relevanceEvidence:
      "N Street Village Sponsor an Event page lists Makenzie Delmotte, Senior Events Manager, as the events sponsorship contact (mdelmotte@nstreetvillage.org).",
    sourcesChecked: [
      "https://www.nstreetvillage.org/sponsor-an-event/",
      "https://www.nstreetvillage.org/contact-us/",
      "https://mayor.dc.gov/release/mayor-bowser-public-calendar-thursday-june-18-2015",
    ],
    discoveryMethod: "official_events_page",
    sourceProvider: "official_page",
  },
  gdi_lmc_5b4d51be7726617c: {
    name: "Danielle Kambic",
    title: "Director of Sales, Washington DC",
    profileUrl:
      "https://members.admei.org/find-a-dmc/Details/csi-dmc-washington-dc-701531",
    domain: "csi-dmc.com",
    relevanceEvidence:
      "ADMEI DMC directory lists Danielle Kambic as Director of Sales, Washington DC for CSI DMC (now operating under Cohera). Destination sales lead for planner/DMC venue sourcing — not claimed as confirmed hotel buyer.",
    sourcesChecked: [
      "https://members.admei.org/find-a-dmc/Details/csi-dmc-washington-dc-701531",
      "https://www.csi-dmc.com/contact",
      "https://washington.org/meetings/find-dc-listings/csi-washington-dc",
    ],
    discoveryMethod: "industry_directory_staff_listing",
    sourceProvider: "admei_directory",
  },
  gdi_lmc_860cde704de88d7c: {
    name: "André Wells",
    title: "Founder / CEO",
    profileUrl: "https://eventsbyandrewells.com/about/",
    domain: "eventsbyandrewells.com",
    relevanceEvidence:
      "Official About page: André Wells is Founder and CEO of Events by André Wells. Planner owner/principal is the venue-sourcing target.",
    sourcesChecked: [
      "https://eventsbyandrewells.com/about/",
      "https://eventsbyandrewells.com/contact/",
    ],
    discoveryMethod: "official_about_page",
    sourceProvider: "official_page",
  },
  gdi_lmc_00cd663a9f282697: {
    name: "Roger J. Whyte II",
    title: "Founder & CEO / Principal",
    profileUrl: "https://stratusfirm.com/about/meet-the-team/",
    domain: "stratusfirm.com",
    relevanceEvidence:
      "Stratus Firm Meet the Team lists Roger J. Whyte II as Founder & CEO; press confirms founder/principal. Planner owner/principal target for venue sourcing.",
    sourcesChecked: [
      "https://stratusfirm.com/about/meet-the-team/",
      "https://stratusfirm.com/contact-us/",
      "https://washington.org/meetings/find-dc-listings/stratus-firm",
    ],
    discoveryMethod: "official_team_page",
    sourceProvider: "official_page",
  },
  gdi_lmc_d670fde177ad418b: {
    name: "Jodi Moraru",
    title: "Founder",
    profileUrl: "https://evokedc.com/about",
    domain: "evokedc.com",
    relevanceEvidence:
      "Evoke official About page identifies Jodi Moraru as founder leading the DC event planning/design firm. Planner principal target.",
    sourcesChecked: [
      "https://evokedc.com/about",
      "https://evokedc.com/contact",
    ],
    discoveryMethod: "official_about_page",
    sourceProvider: "official_page",
  },
  gdi_lmc_82918469d12cfbc1: {
    // Org route repair + form; no suitable named events staff with person channel yet
    exhausted: true,
    name: null,
    sourcesChecked: [
      "https://whfdc.org/general/?type=CONTACT",
      "https://whfdc.org/",
      "https://whfdc.org/page/Committees",
    ],
    orgRepair: {
      email: "whf@whfdc.org",
      phone: "703-683-4742",
      url: "https://whfdc.org/general/?type=CONTACT",
      publishIfEligible: true,
    },
  },
  gdi_lmc_8bf1156a3b6bc8dc: {
    name: "Liz DeBarros",
    title: "CEO",
    profileUrl: "https://dcbia.org/staff/",
    domain: "dcbia.org",
    personEmail: "edebarros@dcbia.org",
    confirmedBookingAuthority: false,
    relevanceEvidence:
      "DCBIA staff page lists Liz DeBarros as CEO with email; sponsorship page also lists her for sponsor packages. Senior executive is a referral entry for association awards/banquets — ask to be directed to events/membership staff. Not treated as confirmed purchasing decision-maker.",
    sourcesChecked: [
      "https://dcbia.org/staff/",
      "https://dcbia.org/sponsors/",
      "https://dcbia.org/contact/",
    ],
    discoveryMethod: "official_staff_page",
    sourceProvider: "official_page",
    forceReferral: true,
  },
  gdi_lmc_fa0049bb82e08bde: {
    name: "Sahar Pearose",
    title: "General Manager, San Diego / Orange County / Santa Barbara",
    profileUrl: "https://meetcohera.com/our-team/sahar-pearose/",
    domain: "meetcohera.com",
    personEmail: "sahar.pearose@meetcohera.com",
    personPhone: "310-570-6094",
    relevanceEvidence:
      "Cohera official team page: Sahar Pearose is GM for San Diego (and OC/Santa Barbara) with published business email and phone — destination GM is the regional venue-sourcing lead.",
    sourcesChecked: [
      "https://meetcohera.com/our-team/sahar-pearose/",
      "https://meetcohera.com/contact-us/",
      "https://www.sandiego.org/members/meeting-planners/cohera/1088",
    ],
    discoveryMethod: "official_team_page",
    sourceProvider: "official_page",
  },
  gdi_lmc_c8d0bd468ce1cc64: {
    name: "Cathy Abarca",
    title: "Events / Sponsorship contact (Women in Blue Luncheon)",
    profileUrl:
      "https://sdpolicefoundation.org/wp-content/uploads/2026/01/2026-Women-in-Blue-Sponsorship-Opportunities.pdf",
    domain: "sdpolicefoundation.org",
    personEmail: "cathy@sdpolicefoundation.org",
    personPhone: "619-232-2130",
    relevanceEvidence:
      "2026 Women in Blue Luncheon sponsorship PDF and KPBS event listing name Cathy Abarca for sponsorship/event questions with person email and office extension.",
    sourcesChecked: [
      "https://sdpolicefoundation.org/wp-content/uploads/2026/01/2026-Women-in-Blue-Sponsorship-Opportunities.pdf",
      "https://www.kpbs.org/events/2026/04/02/women-in-blue-luncheon-heart-of-service",
      "https://sdpolicefoundation.org/contact/",
    ],
    discoveryMethod: "official_event_sponsorship_materials",
    sourceProvider: "official_page",
  },
  gdi_lmc_97fb727376abb83b: {
    name: "Sin Bosier",
    title: "President & CEO",
    profileUrl: "https://www.gaslampevent.com/about-us/",
    domain: "gaslampevent.com",
    relevanceEvidence:
      "Gaslamp Event Management About page identifies SIN (Sin Bosier) as President & CEO. Planner owner/principal target; org inbox gaslampevent@gmail.com remains fallback until person-specific email verified.",
    sourcesChecked: [
      "https://www.gaslampevent.com/about-us/",
      "https://www.gaslampevent.com/contact/",
      "https://linkedin.com/in/sinbosier",
    ],
    discoveryMethod: "official_about_page",
    sourceProvider: "official_page",
  },
  gdi_lmc_94d719381b8dca86: {
    name: "Karina Sousa",
    title: "Club Secretary",
    profileUrl: "https://www.kiwanisclubsandiego.org/membership",
    domain: "kiwanisclubsandiego.org",
    personEmail: "karinakiwanis@gmail.com",
    relevanceEvidence:
      "Kiwanis Club of San Diego membership page lists Club Secretary Karina Sousa with email for club questions; Leadership page confirms Secretary role. Potentially relevant for recurring luncheon logistics — not claimed as confirmed venue buyer.",
    sourcesChecked: [
      "https://www.kiwanisclubsandiego.org/membership",
      "https://www.kiwanisclubsandiego.org/Leadership",
      "https://www.kiwanisclubsandiego.org/Contact",
    ],
    discoveryMethod: "official_leadership_page",
    sourceProvider: "official_page",
  },
  gdi_lmc_9d0050cfbdb5925c: {
    name: "Thomas Bui",
    title: "Owner / President",
    profileUrl: "https://thomasbuilifestyle.com/contact/",
    domain: "thomasbuilifestyle.com",
    personEmail: "thomas@thomasbuilifestyle.com",
    personPhone: "(858) 663-8516",
    relevanceEvidence:
      "Official contact page publishes thomas@thomasbuilifestyle.com and phone; LinkedIn company profile lists Thomas Bui as owner/president. Planner principal with person-specific business email.",
    sourcesChecked: [
      "https://thomasbuilifestyle.com/contact/",
      "https://www.linkedin.com/company/thomas-bui-lifestyle",
    ],
    discoveryMethod: "official_contact_page",
    sourceProvider: "official_page",
  },
};

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function applyNStreetHistoricalNarrative(opp) {
  const historical =
    "Documented historical hotel connection: N Street Village’s Annual Empowerment Luncheon was held at The Mayflower Renaissance Hotel on June 18, 2015 (Mayor Bowser public calendar). This supports reactivation / relationship-development outreach. It does not prove current sales-team familiarity, an active contract, open sourcing, or a confirmed new Mayflower event.";
  return preservePublicationMetadata(opp, {
    ...opp,
    hotelRelationshipStatus: "DOCUMENTED_HISTORICAL_HOST",
    hotelRelationshipEvidence: historical,
    hotelFitNarrativeConsistencyNote: historical,
    recommendedAction:
      opp.recommendedAction ||
      "REACTIVATE_RELATIONSHIP — reference documented 2015 Mayflower hosting; confirm current event plans without assuming an open RFP",
    summaryWhyHotel:
      opp.summaryWhyHotel &&
      /mayflower|historical|2015|empowerment/i.test(opp.summaryWhyHotel)
        ? opp.summaryWhyHotel
        : `${historical} ${opp.summaryWhyHotel || ""}`.trim(),
  });
}

function applyWhfOrgRepair(opp, lead) {
  const repair = lead.orgRepair || {};
  const next = preservePublicationMetadata(opp, {
    ...opp,
    functionalContactEmail: repair.email || opp.functionalContactEmail,
    organizationContactEmail: repair.email || opp.organizationContactEmail,
    contactEmail: repair.email || opp.contactEmail,
    contactPhone: repair.phone || opp.contactPhone,
    organizationContactPhone: repair.phone || opp.organizationContactPhone,
    publicContactPath: repair.url || opp.publicContactPath,
    organizationContactUrl: repair.url || opp.organizationContactUrl,
    contactUrl: repair.url || opp.contactUrl,
    functionalContactUrl: repair.url || opp.functionalContactUrl,
    contactMethod:
      opp.contactMethod ||
      "Organization inquiry form + email + phone (WHF Contact Us page)",
  });
  const elig = evaluateProspectPublicationEligibility(next);
  if (repair.publishIfEligible && elig.hotelFacing) {
    return preservePublicationMetadata(next, {
      ...next,
      customerVisible: true,
      prospectHotelFacing: true,
      prospectPublicationBasis:
        next.prospectPublicationBasis || "REVIEWED_PACKAGE_V1",
      prospectPublicationEligibility: elig.eligibility,
      internalHoldReason: null,
      namedContactResearchAttempted: true,
      namedContactResearchStatus: "NAMED_RESEARCH_EXHAUSTED_ORG_FALLBACK",
      contactDepthResearch: {
        attemptedAt: new Date().toISOString(),
        outcome: "organization_fallback_after_bounded_person_research",
        sourcesChecked: lead.sourcesChecked || [],
        unresolvedReason:
          "no_reliable_named_events_staff_with_person_channel_after_bounded_search",
        nextStep: "revisit_staff_directory_on_weekly_refresh",
      },
    });
  }
  return preservePublicationMetadata(next, {
    ...next,
    namedContactResearchAttempted: true,
    namedContactResearchStatus: "NAMED_RESEARCH_EXHAUSTED_ORG_FALLBACK",
    internalHoldReason:
      next.internalHoldReason ||
      elig.reasons?.join("; ") ||
      "eligibility_still_fails_after_contact_route_repair",
  });
}

async function main() {
  ensureDir(OUT);
  ensureDir(path.join(OUT, "results"));
  ensureDir(path.join(OUT, "backups"));

  const remainingBefore = remainingSharedBudgetUsd();
  const rows = [];
  const passResults = [];

  for (const [hotelId, ids] of Object.entries(TARGET_IDS)) {
    const doc = await loadOpportunitiesCanonical(hotelId);
    const backupPath = path.join(
      OUT,
      "backups",
      `${hotelId}-opportunities-${new Date().toISOString().replace(/[:.]/g, "-")}.json`
    );
    fs.writeFileSync(backupPath, JSON.stringify(doc, null, 2) + "\n");

    for (const id of ids) {
      const opp = (doc.opportunities || []).find((o) => o.id === id);
      if (!opp) {
        rows.push({
          hotelId,
          opportunityId: id,
          error: "not_found_in_canonical",
        });
        continue;
      }

      const before = {
        name: opp.primaryContact?.name || opp.contactName || null,
        role: opp.primaryContact?.role || opp.primaryContactRole || null,
        personEmail: opp.primaryContactEmail || opp.primaryContact?.email || null,
        personPhone: opp.primaryContactPhone || opp.primaryContact?.phone || null,
        orgEmail:
          opp.functionalContactEmail ||
          opp.organizationContactEmail ||
          opp.contactEmail ||
          null,
        orgPhone: opp.organizationContactPhone || opp.contactPhone || null,
        url: opp.publicContactPath || opp.contactUrl || null,
        visible: opp.customerVisible,
        facing: opp.prospectHotelFacing,
      };

      const lead = LEADS[id] || {};
      let working = { ...opp };

      // N Street Village — preserve historical Mayflower evidence accurately
      if (id === "gdi_lmc_ceab66971a48a9f8") {
        working = applyNStreetHistoricalNarrative(working);
      }

      // WHF — repair org route from live Contact Us page, then eligibility
      if (id === "gdi_lmc_82918469d12cfbc1") {
        working = applyWhfOrgRepair(working, lead);
      }

      // Seed known person-specific channels from official sources before Surfe
      if (lead.personEmail && !lead.forceReferral) {
        working = {
          ...working,
          primaryContact: {
            ...(working.primaryContact || {}),
            name: lead.name,
            title: lead.title,
            role: lead.title,
            email: lead.personEmail,
            emailOwnership: "person_specific",
            phone: lead.personPhone || working.primaryContact?.phone || null,
            phoneOwnership: lead.personPhone ? "person_specific" : undefined,
            relevanceEvidence: lead.relevanceEvidence,
            sourceUrl: lead.profileUrl,
            verificationDate: new Date().toISOString().slice(0, 10),
          },
          officialDomain: lead.domain || working.officialDomain,
        };
      }

      const leadPerson = lead.exhausted
        ? {
            exhausted: true,
            sourcesChecked: lead.sourcesChecked,
          }
        : lead.name
          ? {
              name: lead.name,
              title: lead.title,
              role: lead.title,
              profileUrl: lead.profileUrl,
              linkedinUrl: lead.profileUrl,
              domain: lead.domain,
              relevanceEvidence: lead.relevanceEvidence,
              sourcesChecked: lead.sourcesChecked,
              discoveryMethod: lead.discoveryMethod,
              sourceProvider: lead.sourceProvider,
              confirmedBookingAuthority: lead.confirmedBookingAuthority === true,
              // Force referral classification for association CEO
              ...(lead.forceReferral
                ? {
                    relevanceEvidence: lead.relevanceEvidence,
                  }
                : {}),
            }
          : null;

      // For forceReferral CEOs, pass title that triggers referral when not planner
      if (lead.forceReferral && leadPerson) {
        leadPerson.title = lead.title || "CEO";
      }

      const result = await enrichBuyerContactForOpportunity(working, {
        hotelId,
        authorizeSurfe:
          authorizeSurfe &&
          Boolean(lead.name) &&
          !lead.personEmail &&
          !lead.exhausted &&
          !lead.forceReferral,
        workflowRunId: `gdi_lmc_contact_depth_${new Date()
          .toISOString()
          .slice(0, 10)}`,
        leadPerson,
        sourcesChecked: lead.sourcesChecked || [],
        markExhausted: Boolean(lead.exhausted),
        persistCanonical: true,
        nextRefreshDate: new Date(
          Date.now() + 30 * 24 * 60 * 60 * 1000
        )
          .toISOString()
          .slice(0, 10),
      });

      // Override referral classification for association CEO
      let patched = result.patchedOpportunity;
      if (lead.forceReferral && patched) {
        patched = preservePublicationMetadata(patched, {
          ...patched,
          contactResponsibilityClass: "REFERRAL_OR_ORG_ENTRY_POINT",
          namedContactResearchStatus: "NAMED_CONTACT_WEAK_RELEVANCE",
          primaryContact: {
            ...(patched.primaryContact || {}),
            name: lead.name,
            title: lead.title,
            email: null,
            emailOwnership: null,
            suggestedContactLabel: `Referral lead: ${lead.name} (${lead.title}) — ask to be directed to the meetings/events contact`,
            relationshipToOpportunity: "REFERRAL_OR_ORG_ENTRY_POINT",
          },
          contactDepthResearch: {
            ...(patched.contactDepthResearch || {}),
            outcome: "named_referral_lead_only",
            unresolvedReason:
              "named_person_is_referral_entry_not_confirmed_buyer",
            nextStep: "identify_meetings_events_or_membership_staff",
          },
          contactChannelSeparation: {
            ...(patched.contactChannelSeparation || {}),
            suggestedPerson: lead.name,
            personSpecificEmail: null,
            organizationRoute:
              patched.functionalContactEmail ||
              patched.organizationContactEmail ||
              patched.publicContactPath,
            askToBeDirectedTo: `Ask ${lead.name} (or the organization route) to direct you to the meetings / events contact`,
          },
        });
        result.patchedOpportunity = patched;
        result.materialImprovement = true;
      }

      // Ensure WHF patched opp carries publish flags when eligible
      if (id === "gdi_lmc_82918469d12cfbc1") {
        patched = applyWhfOrgRepair(
          { ...working, ...patched, primaryContact: patched?.primaryContact },
          lead
        );
        result.patchedOpportunity = patched;
        result.materialImprovement = true;
      }

      if (id === "gdi_lmc_ceab66971a48a9f8" && patched) {
        patched = applyNStreetHistoricalNarrative(patched);
        result.patchedOpportunity = patched;
      }

      passResults.push(result);

      let persist = { ok: true, dryRun: true };
      if (result.patchedOpportunity) {
        persist = await persistBuyerContactPatchedOpportunity(
          result.patchedOpportunity,
          { dryRun, hotelId }
        );
      }

      const afterOpp = result.patchedOpportunity || working;
      const elig = evaluateProspectPublicationEligibility(afterOpp);
      const listDto = toOpportunityListDto(afterOpp);
      const facingProbe = filterCustomerFacingOpportunities([afterOpp]);

      rows.push({
        hotelId,
        opportunityId: id,
        organizationName: opp.organizationName,
        before,
        after: {
          name: afterOpp.primaryContact?.name || null,
          role:
            afterOpp.primaryContact?.title ||
            afterOpp.primaryContact?.role ||
            null,
          responsibilityClass:
            afterOpp.contactResponsibilityClass || null,
          personEmail:
            afterOpp.primaryContactEmail ||
            afterOpp.primaryContact?.email ||
            null,
          personPhone:
            afterOpp.primaryContactPhone ||
            afterOpp.primaryContact?.phone ||
            null,
          orgFallback:
            afterOpp.functionalContactEmail ||
            afterOpp.organizationContactEmail ||
            afterOpp.publicContactPath ||
            afterOpp.contactUrl ||
            null,
          namedResearchStatus: afterOpp.namedContactResearchStatus || null,
          contactDepthOutcome: afterOpp.contactDepthResearch?.outcome || null,
          unresolvedReason:
            afterOpp.contactDepthResearch?.unresolvedReason || null,
        },
        researchPerformed: lead.sourcesChecked || [],
        remainingGap:
          afterOpp.contactDepthResearch?.unresolvedReason || null,
        persist,
        eligibility: {
          hotelFacing: elig.hotelFacing,
          eligibility: elig.eligibility,
          reasons: elig.reasons || elig.blockingReasons || null,
        },
        listDtoOk: Boolean(listDto?.opportunityId || listDto?.id),
        customerFacingFilterOk: (facingProbe || []).some((o) => o.id === id),
        materialImprovement: result.materialImprovement,
        publicationPreserved: result.publicationPreserved,
        surfeStatus: result.surfe?.status || null,
        providerLeakCheck: {
          listHasSurfe: /surfe/i.test(JSON.stringify(listDto || {})),
          listHasCredits: /credit|estimatedCost/i.test(
            JSON.stringify(listDto || {})
          ),
        },
      });

      fs.writeFileSync(
        path.join(OUT, "results", `${id}.json`),
        JSON.stringify(result, null, 2) + "\n"
      );
    }
  }

  writeBuyerEnrichmentArtifacts(
    {
      version: "gdi-lmc-planner-contact-depth-v1",
      hotelId: "mayflower+sd",
      workflowRunId: "gdi_lmc_contact_depth",
      generatedAt: new Date().toISOString(),
      results: passResults,
      remainingSharedBudgetUsd: remainingSharedBudgetUsd(),
    },
    OUT
  );

  const summary = {
    version: "gdi-lmc-planner-contact-depth-v1",
    generatedAt: new Date().toISOString(),
    dryRun,
    authorizeSurfe,
    budget: {
      remainingBeforeUsd: remainingBefore,
      remainingAfterUsd: remainingSharedBudgetUsd(),
      note: "Shared $11 paid-MS envelope; Surfe only when --authorize-surfe and justified",
    },
    counts: {
      accounts: rows.length,
      namedWithPersonChannel: rows.filter(
        (r) =>
          r.after?.contactDepthOutcome ===
          "relevant_named_contact_with_usable_business_channel"
      ).length,
      namedChannelPending: rows.filter(
        (r) =>
          r.after?.contactDepthOutcome ===
          "relevant_named_contact_with_channels_pending"
      ).length,
      referralOnly: rows.filter(
        (r) => r.after?.contactDepthOutcome === "named_referral_lead_only"
      ).length,
      orgFallbackAfterResearch: rows.filter(
        (r) =>
          r.after?.contactDepthOutcome ===
          "organization_fallback_after_bounded_person_research"
      ).length,
      stillOrgRouteOnly: rows.filter((r) => !r.after?.name).length,
      whfPublished: rows.some(
        (r) =>
          r.opportunityId === "gdi_lmc_82918469d12cfbc1" &&
          r.eligibility?.hotelFacing
      ),
    },
    rows,
  };

  fs.writeFileSync(
    path.join(OUT, "account-contact-depth-register.json"),
    JSON.stringify(summary, null, 2) + "\n"
  );

  console.log(
    JSON.stringify(
      {
        dryRun,
        authorizeSurfe,
        accounts: rows.length,
        counts: summary.counts,
        remainingUsd: summary.budget.remainingAfterUsd,
        out: OUT,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
