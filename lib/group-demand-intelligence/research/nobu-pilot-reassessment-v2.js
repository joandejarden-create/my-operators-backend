/**
 * Offline reassessment of Nobu professional_planners_dmc pilot candidates
 * under hotel-specific qualification V2. Does not change customer-facing
 * classifications or production publication eligibility.
 */

import fs from "node:fs";
import { qualifyHotelSpecificProspectV2, classifyAccommodationEvidenceRole } from "./hotel-specific-qualification-v2.js";
import { SEGMENT_PILOT_HOTEL_ID, SEGMENT_PILOT_HOTEL_NAME } from "./segment-pilot-professional-planners-dmc-nobu-v1.js";

export const NOBU_PILOT_REASSESSMENT_VERSION = "gdi-nobu-pilot-reassessment-v2";

const DOSSIERS_DIR = "reports/gdi-segment-pilot-professional-planners-dmc-nobu-v1/dossiers";
const CERT_RESULT = "reports/gdi-segment-pilot-professional-planners-dmc-nobu-v1/CERTIFICATION_RESULT.json";
const CANONICAL_OPPS = "data/group-demand-intelligence/hotels/recC2kbCaS48jFjFF/opportunities.json";

function readJson(rel) {
  if (!fs.existsSync(rel)) return null;
  return JSON.parse(fs.readFileSync(rel, "utf8"));
}

function loadDossierClaims(slug) {
  const path = `${DOSSIERS_DIR}/INTERNAL_RESEARCH_DOSSIER_${slug}.json`;
  const doc = readJson(path);
  const dossier = doc?.dossier || doc;
  const ledger = dossier?.evidenceLedger || [];
  return {
    path,
    organizationName: dossier?.organization?.name || slug,
    priorPathway: dossier?.organization?.pathway || null,
    priorDisposition: dossier?.researchDisposition || null,
    acceptedClaims: ledger.map((item) => ({
      statement: item.statement,
      url: item.url,
      topic: item.topic,
      accepted: true,
    })),
    pageTexts: ledger.map((item) => item.statement || ""),
  };
}

function loadOpp(name) {
  const doc = readJson(CANONICAL_OPPS);
  const ops = doc?.opportunities || [];
  return ops.find((o) => String(o.organizationName || o.title || "").trim() === name) || {
    organizationName: name,
  };
}

function reassessOne({ name, slug, notes = "" }) {
  const loaded = slug ? loadDossierClaims(slug) : {
    organizationName: name,
    acceptedClaims: [],
    pageTexts: [],
    priorPathway: null,
    path: null,
  };
  const opp = loadOpp(name);
  const contact = opp.contactEmail || null;
  const claims = loaded.acceptedClaims?.length
    ? loaded.acceptedClaims
    : [
      ...(opp.summaryWhyHotel ? [{ statement: opp.summaryWhyHotel, topic: "organisation", accepted: true }] : []),
      ...(contact ? [{ statement: `Contact: ${contact}`, topic: "contact", accepted: true }] : []),
    ];
  const pages = loaded.pageTexts?.length ? loaded.pageTexts : claims.map((c) => c.statement);

  const accommodation = classifyAccommodationEvidenceRole({
    acceptedClaims: claims,
    pageTexts: pages,
    organizationName: name,
  });
  const qualification = qualifyHotelSpecificProspectV2({
    opportunity: {
      ...opp,
      organizationName: name,
      contactEmail: contact,
      evidenceQuote: pages.join(" "),
      targetHotel: SEGMENT_PILOT_HOTEL_NAME,
    },
    acceptedClaims: claims,
    pageTexts: pages,
    targetHotel: SEGMENT_PILOT_HOTEL_NAME,
    usableContactRoute: contact,
  });

  const priorUseful = /london dmc|miss jones/i.test(name);
  const stillUseful = qualification.internalAccountDevelopment === true;
  const downgraded = priorUseful && !stillUseful;

  return {
    organizationName: name,
    dossierPath: loaded.path,
    priorPilotAssessment: priorUseful ? "USEFUL_ACCOUNT_LED" : "NOT_USEFUL_OR_NOT_INVESTIGATED_AS_USEFUL",
    revisedAssessment: stillUseful ? "INTERNAL_ACCOUNT_DEVELOPMENT" : "DOWNGRADED_OR_INSUFFICIENT",
    downgraded,
    downgradeReason: qualification.downgradeReason || (stillUseful ? null : accommodation.reason),
    accommodationEvidenceRole: accommodation,
    qualification: {
      pathway: qualification.pathway,
      commercialStage: qualification.commercialStage,
      salesReadiness: qualification.salesReadiness,
      internalAccountDevelopment: qualification.internalAccountDevelopment,
      nextSalesAction: qualification.nextSalesAction,
      usableContactRoute: qualification.usableContactRoute,
    },
    evidenceNotes: notes,
    customerFacingClassificationChanged: false,
    productionPublicationEligibilityChanged: false,
  };
}

/**
 * Reassess pilot set using existing dossiers + corpus only (no live fetch).
 */
export function reassessNobuPilotCandidatesV2() {
  const london = reassessOne({
    name: "London DMC",
    slug: "london_dmc",
    notes: "Homepage evidence includes hotel and restaurant bookings and incentive programmes for delegates — supports hotel-sourcing mechanism. Contact info@londondmc.com. Organizer-claimed only; no independent publisher corroboration in pilot. Nobu not named — do not invent relationship.",
  });

  const missJones = reassessOne({
    name: "Miss Jones Group",
    slug: "miss_jones_group",
    notes: "Nobu Portman Square page is a venue listing (Free Venue Finding, Maximum Guests, Bedrooms as amenity). Bedroom count does not establish guestroom purchasing control. Downgrade from useful ACCOUNT_LED under V2.",
  });

  const spectra = reassessOne({
    name: "Spectra DMC",
    slug: "spectra_dmc",
    notes: "Contact present; retrieved pilot pages did not evidence overnight accommodation sourcing mechanism under V2.",
  });

  const twoB = reassessOne({
    name: "2B UK DMC",
    slug: "2b_uk_dmc",
    notes: "Contact present; retrieved pilot pages did not evidence overnight accommodation sourcing mechanism under V2.",
  });

  const headbox = reassessOne({
    name: "HeadBox",
    slug: null,
    notes: "Exact-property ballroom listing is venue-sourcing; not investigated as useful in pilot. V2 treats venue listings without guestroom control as insufficient for overnight account development.",
  });

  const hireSpace = reassessOne({
    name: "Hire Space",
    slug: null,
    notes: "Bookable event venue listing; residential use was plausible not evidenced. V2: venue listing alone insufficient.",
  });

  const cert = readJson(CERT_RESULT);
  const results = [london, missJones, spectra, twoB, headbox, hireSpace];

  return {
    version: NOBU_PILOT_REASSESSMENT_VERSION,
    hotelId: SEGMENT_PILOT_HOTEL_ID,
    hotelName: SEGMENT_PILOT_HOTEL_NAME,
    segmentId: "professional_planners_dmc",
    offline: true,
    liveCalls: 0,
    spent: false,
    priorPilotUseful: ["London DMC", "Miss Jones Group"],
    revisedUsefulInternalAccountDevelopment: results.filter((r) => r.qualification.internalAccountDevelopment).map((r) => r.organizationName),
    downgraded: results.filter((r) => r.downgraded).map((r) => ({
      organizationName: r.organizationName,
      reason: r.downgradeReason,
    })),
    candidates: results,
    priorCertificationPreserved: Boolean(cert),
    priorCertificationSpentUsd: cert?.technical?.actualSpentUsd ?? null,
    customerFacingClassificationChanged: false,
    productionPublicationEligibilityChanged: false,
    airtableWrites: 0,
    deploy: false,
  };
}
