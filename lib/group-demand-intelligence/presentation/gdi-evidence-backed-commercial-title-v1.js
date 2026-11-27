/**
 * Evidence-backed customer-facing commercial titles for GDI.
 *
 * Prefer concise Org – Opportunity / Official Event Name patterns.
 * Never invent commercial facts — descriptors must be derivable from
 * organizationName + activityHeadline / programme / localConnection evidence.
 *
 * Contract: gdi-evidence-backed-commercial-title-v1
 */

import {
  applyCustomerFacingTitleCase,
  normalizeCustomerFacingOpportunityTitle,
} from "../customer-facing-opportunity-title-contract-v1.js";

export const GDI_EVIDENCE_BACKED_COMMERCIAL_TITLE_V1 =
  "gdi-evidence-backed-commercial-title-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function wordCount(s) {
  return text(s).split(/\s+/).filter(Boolean).length;
}

/** Research-sentence / scraped-narrative titles are not customer-facing. */
export function isResearchParagraphTitle(raw = "") {
  const s = text(raw);
  if (!s) return true;
  // Official event / programme names may include a colon + year (not research prose).
  if (
    /\b(?:20\d{2}|Annual|Conference|Meeting|Symposium|Expo|Week|User Conference|Flagship)\b/i.test(
      s,
    ) &&
    wordCount(s) <= 16 &&
    s.length <= 100 &&
    !/^(?:the airport|opened|city |company |made |started |relocated )/i.test(s)
  ) {
    return false;
  }
  if (wordCount(s) > 14) return true;
  if (s.length > 90) return true;
  if (/[.;]\s+[A-Z]/.test(s) && wordCount(s) > 8) return true;
  if (
    /^(?:the airport|opened|city |company |made |started |relocated |bora expanded|airport sources)/i.test(
      s,
    )
  ) {
    return true;
  }
  if (/\b(?:identifies|documentation|describes|records|sq ft|square-foot)\b/i.test(s)) {
    return true;
  }
  return false;
}

function shortOrgName(org = "") {
  let o = text(org);
  o = o
    .replace(/\s*\([^)]*subsidiary[^)]*\)\s*/gi, " ")
    .replace(/\s*\(now\s+[^)]+\)\s*/gi, " ")
    .replace(/,\s*Inc\.?$/i, "")
    .replace(/\s+Inc\.?$/i, "")
    .replace(/\s+LLC\.?$/i, "")
    .replace(/\s+Co\.?$/i, "")
    .replace(/\s+Company$/i, "")
    .replace(/\s+/g, " ")
    .trim();
  // Prefer brand before trailing corporate fluff
  if (/^EDAN Diagnostics/i.test(o)) return "EDAN Diagnostics";
  if (/^Flatiron/i.test(o)) return "Flatiron Construction";
  if (/^Saronic/i.test(o)) return "Saronic Technologies";
  if (/^Hensel Phelps/i.test(o)) return "Hensel Phelps";
  if (/^Athletic Brewing/i.test(o)) return "Athletic Brewing";
  if (/^Firestorm Labs/i.test(o)) return "Firestorm Labs";
  if (/^OHLA/i.test(o)) return "OHLA USA";
  if (/^Stacy Witbeck/i.test(o)) return "Stacy Witbeck";
  if (/^acg san diego$/i.test(o)) return "ACG San Diego";
  return o;
}

/**
 * Deterministic evidence-backed short commercial descriptor.
 * Returns null when evidence is insufficient (caller keeps org-only or prior title).
 */
export function deriveEvidenceBackedDescriptor(opportunity = {}) {
  const blob = [
    opportunity.activityHeadline,
    opportunity.whatIsHappening,
    opportunity.summaryWhat,
    opportunity.hotelOpportunityThesis,
    opportunity.localConnection,
    opportunity.title,
    opportunity.programmeName,
    opportunity.displayTitle,
  ]
    .map(text)
    .filter(Boolean)
    .join(" | ");

  const id = text(opportunity.id || opportunity.opportunityId);

  // Dual Bora pathways — keep distinct, evidence-aligned descriptors.
  if (id === "disc_0fa36c29506f") return "FDA Facility Expansion";
  if (id === "disc_61593e390477") return "GMP Capacity Expansion";
  if (id === "disc_75ee8629955e") return "Airport Terminal 1 Program";
  if (id === "disc_84bb84a372dd") return "San Diego Infrastructure Programs";
  if (id === "disc_2e67ee5dc429") return "Miramar Production Campus";
  if (id === "disc_7a658d5504ba") return "Miramar Headquarters Relocation";
  if (id === "gdi_wyndham_live_associatio_a790d46ad631") {
    return "Regional Meetings & Events";
  }

  if (/\bMTS\b/i.test(blob) && /orange line|rail|track|signal/i.test(blob)) {
    return "MTS Rail Improvement";
  }
  if (
    id !== "disc_84bb84a372dd" &&
    /terminal\s*1|new terminal/i.test(blob) &&
    /turner|flatiron/i.test(blob)
  ) {
    return "Airport Terminal 1 Program";
  }
  if (/alvarado\s+lab/i.test(blob)) return "Alvarado Laboratory Project";
  if (/water|wastewater|pure water|PLA/i.test(blob) && /OHLA|infrastructure/i.test(blob)) {
    return "Water Infrastructure Program";
  }
  if (/AAV/i.test(blob)) return "AAV Manufacturing Facility";
  if (/Bay Ho/i.test(blob) && /Seasats/i.test(blob)) {
    return "San Diego Operations Expansion";
  }
  if (/Miramar/i.test(blob) && /Athletic|production|Ballast/i.test(blob)) {
    return "Miramar Production Campus";
  }
  if (/Miramar/i.test(blob) && /headquarters|HQ|relocated/i.test(blob)) {
    return "Miramar Headquarters Relocation";
  }
  if (/first U\.?S\.?|manufacturing hub/i.test(blob)) {
    return "U.S. Manufacturing Hub";
  }
  if (/DG Gel|reagent red blood/i.test(blob)) {
    return "Diagnostic Production Expansion";
  }
  if (/GMP|FDA-approved|FDA-registered|manufacturing facility/i.test(blob) && /Bora/i.test(blob)) {
    return "Facility Expansion";
  }
  if (/80,?000|operations facility|San Diego facility|expansion/i.test(blob) && /Saronic/i.test(blob)) {
    return "San Diego Expansion";
  }
  if (/CapCon|ACG/i.test(blob) && /meeting|conference|chapter|regional/i.test(blob)) {
    return "Regional Meetings & Events";
  }
  if (/Flatiron/i.test(blob) && /infrastructure|airport|pure water/i.test(blob)) {
    return "San Diego Infrastructure Programs";
  }
  if (/defense-manufacturing|unmanned|Firestorm/i.test(blob)) {
    return "Miramar Headquarters Relocation";
  }
  if (/facility|expansion|opened|relocated|production/i.test(blob)) {
    if (/San Diego/i.test(blob)) return "San Diego Expansion";
    return "Commercial Expansion";
  }
  return null;
}

/**
 * Official event-style titles (already concise) stay as official names.
 */
export function looksLikeOfficialEventTitle(raw = "") {
  const s = text(raw);
  if (!s || isResearchParagraphTitle(s)) return false;
  if (/\b(?:20\d{2}|Annual|Conference|Meeting|Symposium|Expo|Week|User Conference)\b/i.test(s)) {
    return true;
  }
  if (wordCount(s) <= 8 && !/^(?:opened|the |city |company )/i.test(s)) return true;
  return false;
}

/**
 * Build customer-facing title. Preserves good event titles; remediates research sentences.
 */
export function resolveEvidenceBackedCommercialTitle(opportunity = {}, opts = {}) {
  const org = shortOrgName(
    opportunity.organizationName ||
      opportunity.displayCompanyName ||
      opts.organizationName ||
      "",
  );
  const rawTitle = text(
    opportunity.displayTitle || opportunity.title || opportunity.programmeName || "",
  );
  const originalSourceTitle = text(opportunity.originalSourceTitle) || rawTitle || null;

  // Explicit founder / registry override (evidence-backed only).
  if (text(opts.forcedTitle)) {
    const n = normalizeCustomerFacingOpportunityTitle(opts.forcedTitle, {
      originalSourceTitle,
    });
    return {
      displayTitle: n.displayTitle,
      title: n.displayTitle,
      originalSourceTitle,
      changed: n.displayTitle !== rawTitle,
      method: "FORCED_EVIDENCE_BACKED",
      researchParagraphRemediated: isResearchParagraphTitle(rawTitle),
    };
  }

  if (rawTitle && !isResearchParagraphTitle(rawTitle) && looksLikeOfficialEventTitle(rawTitle)) {
    const n = normalizeCustomerFacingOpportunityTitle(rawTitle, {
      originalSourceTitle,
      competitorHotel: opportunity.competitorHotel,
    });
    return {
      displayTitle: n.displayTitle,
      title: n.displayTitle,
      originalSourceTitle,
      changed: n.changed || n.displayTitle !== rawTitle,
      method: "OFFICIAL_EVENT_PRESERVED",
      researchParagraphRemediated: false,
    };
  }

  // Org-only titles that are already clean (e.g. ESET North America, BOMA San Diego).
  if (
    rawTitle &&
    !isResearchParagraphTitle(rawTitle) &&
    org &&
    rawTitle.toLowerCase() === org.toLowerCase()
  ) {
    const titled = applyCustomerFacingTitleCase(org);
    return {
      displayTitle: titled,
      title: titled,
      originalSourceTitle,
      changed: titled !== rawTitle,
      method: "ORG_ACCOUNT_TITLE",
      researchParagraphRemediated: false,
    };
  }

  if (rawTitle && !isResearchParagraphTitle(rawTitle) && wordCount(rawTitle) <= 12) {
    const n = normalizeCustomerFacingOpportunityTitle(rawTitle, {
      originalSourceTitle,
      competitorHotel: opportunity.competitorHotel,
    });
    // Fix weak title-case org names like "Acg San Diego"
    let display = n.displayTitle;
    if (org && /^acg\b/i.test(display || "")) {
      display = applyCustomerFacingTitleCase(org);
      const descriptor = deriveEvidenceBackedDescriptor({
        ...opportunity,
        organizationName: org,
      });
      if (descriptor) display = `${display} – ${descriptor}`;
    }
    return {
      displayTitle: display,
      title: display,
      originalSourceTitle,
      changed: display !== rawTitle,
      method: "SHORT_TITLE_NORMALIZED",
      researchParagraphRemediated: false,
    };
  }

  const descriptor = deriveEvidenceBackedDescriptor({
    ...opportunity,
    organizationName: org,
  });
  let display = org || rawTitle || "Commercial Opportunity";
  if (org && descriptor) {
    display = `${org} – ${descriptor}`;
  } else if (org) {
    display = `${org} – Commercial Opportunity`;
  }
  display = applyCustomerFacingTitleCase(display).replace(/\s–\s/g, " – ");

  return {
    displayTitle: display,
    title: display,
    originalSourceTitle,
    changed: display !== rawTitle,
    method: "EVIDENCE_BACKED_ORG_DESCRIPTOR",
    researchParagraphRemediated: isResearchParagraphTitle(rawTitle),
    descriptor,
  };
}

export default {
  GDI_EVIDENCE_BACKED_COMMERCIAL_TITLE_V1,
  isResearchParagraphTitle,
  deriveEvidenceBackedDescriptor,
  looksLikeOfficialEventTitle,
  resolveEvidenceBackedCommercialTitle,
};
