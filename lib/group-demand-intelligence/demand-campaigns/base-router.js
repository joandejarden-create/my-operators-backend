/**
 * Route a demand campaign to the appropriate Ten Bases decomposer.
 * Rotation / lookalike / corporate-trigger never auto-create confirmed opportunities.
 */

import { GDI_BASE_OF_DEMAND } from "../ten-bases-of-demand-v1/taxonomy.js";

const B = GDI_BASE_OF_DEMAND;

/**
 * @param {object} campaign
 * @returns {{ baseOfDemand: string, secondaryBases: string[], reason: string }}
 */
export function routeCampaignToBaseOfDemand(campaign = {}) {
  const engine = String(campaign.demandEngine || "").toUpperCase();
  const blob = [
    campaign.name,
    campaign.title,
    campaign.organizationName,
    campaign.organizer,
    campaign.venue,
    campaign.fact,
    campaign.verificationNote,
    campaign.hotelFit,
    campaign.nextAction,
  ]
    .map((x) => String(x || ""))
    .join(" ");

  // Sports / production first
  if (
    engine === "SPORTS_ENTERTAINMENT_SOCIAL" ||
    /CHI|concours hippique|rider|team hotel|production|broadcast|gallery|art handler|stand builder|cinema|film festival|altaroma|romaeuropa|fashion/i.test(
      blob
    )
  ) {
    if (/art gen|gallery|installer|art handler|romaeuropa|altaroma|fashion/i.test(blob)) {
      return {
        baseOfDemand: B.SPORTS_ENTERTAINMENT_PRODUCTION,
        secondaryBases: [B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING],
        reason: "arts_production_or_gallery_motion",
      };
    }
    if (
      /CHI|concours|watches.?&.?wonders|sports|rider|team|cinema|film festival|festa del cinema/i.test(
        blob
      )
    ) {
      return {
        baseOfDemand: B.SPORTS_ENTERTAINMENT_PRODUCTION,
        secondaryBases: [B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING],
        reason: "sports_entertainment_production",
      };
    }
  }

  // International org / UN / WHO / ECOSOC / FAO / WFP / IFAD
  if (
    /WHO|World Health|ECOSOC|OCHA|United Nations|ITU|Executive Board|WHA|member.?state|FAO|Food and Agriculture|WFP|World Food Programme|IFAD/i.test(
      blob
    ) ||
    engine === "ASSOCIATION_NGO"
  ) {
    if (
      /WHO|ECOSOC|OCHA|United Nations|WHA|Executive Board|ITU|FAO|WFP|World Food Programme|IFAD|Food and Agriculture/i.test(
        blob
      )
    ) {
      return {
        baseOfDemand: B.INTERNATIONAL_ORG_RECURRING_GROUPS,
        secondaryBases: [B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING, B.PUBLISHED_EVENT_DECOMPOSITION],
        reason: "intl_org_or_un_family",
      };
    }
  }

  // Pharma / medical congress
  if (
    engine === "PHARMA_LIFE_SCIENCES" ||
    /SETAC|pharma|medical|toxicolog|investigator|health forum/i.test(blob)
  ) {
    if (/SETAC|toxicolog|pharma|investigator/i.test(blob)) {
      return {
        baseOfDemand: B.PHARMA_MEDICAL_ECOSYSTEM,
        secondaryBases: [B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING, B.PUBLISHED_EVENT_DECOMPOSITION],
        reason: "pharma_medical_ecosystem",
      };
    }
    if (/health forum|global health/i.test(blob)) {
      return {
        baseOfDemand: B.PUBLISHED_EVENT_DECOMPOSITION,
        secondaryBases: [B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING, B.PHARMA_MEDICAL_ECOSYSTEM],
        reason: "health_forum_published_event",
      };
    }
  }

  // Recurring corporate / executive education
  if (
    engine === "CORPORATE" ||
    engine === "CORPORATE_MEETINGS" ||
    /LUISS|executive education|business school|corporate meeting|sales kickoff/i.test(blob)
  ) {
    if (/LUISS|executive education|business school/i.test(blob) || engine === "CORPORATE") {
      return {
        baseOfDemand: B.RECURRING_CORPORATE_MEETINGS,
        secondaryBases: [B.CORPORATE_TRIGGER_DEMAND],
        reason: "recurring_corporate_or_executive_education",
      };
    }
    if (/watches.?&.?wonders|brand pavilion/i.test(blob)) {
      return {
        baseOfDemand: B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING,
        secondaryBases: [B.PUBLISHED_EVENT_DECOMPOSITION, B.SPORTS_ENTERTAINMENT_PRODUCTION],
        reason: "corporate_exhibitor_sponsor_mining",
      };
    }
  }

  // Default published event → participant mining
  return {
    baseOfDemand: B.PUBLISHED_EVENT_DECOMPOSITION,
    secondaryBases: [B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING],
    reason: "default_published_event_decomposition",
  };
}

export { GDI_BASE_OF_DEMAND };
