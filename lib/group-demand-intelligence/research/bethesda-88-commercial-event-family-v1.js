/**
 * Bethesda 88 — commercial event-family grouping (presentation only).
 *
 * Collapses multiple research-source records that describe one underlying
 * bookable demand into one primary list card + linked research IDs.
 * Never deletes opportunities. Never writes opportunities.json.
 * Never reduces the raw 88-ID research inventory.
 */

export const BETHESDA_88_COMMERCIAL_EVENT_FAMILY_V1 =
  "gdi-bethesda-88-commercial-event-family-v1";

export const FAMILY_ROLE = Object.freeze({
  PRIMARY: "PRIMARY",
  LINKED_RESEARCH: "LINKED_RESEARCH",
  QUARANTINED: "QUARANTINED",
  STANDALONE: "STANDALONE",
});

/**
 * Governed families — same underlying commercial activity.
 * AMWA 2027 annual / 2028 annual / interim remain distinct.
 */
export const BETHESDA_COMMERCIAL_EVENT_FAMILIES_V1 = Object.freeze([
  {
    familyId: "ctn_annual_conference_bethesda_2027",
    underlyingEvent:
      "NIDA CTN Annual Scientific Meeting / Dissemination — March 15–17 2027, NIH campus Bethesda (Natcher)",
    dates: "2027-03-15/2027-03-17",
    venue: "NIH Natcher Building / Bethesda MD (attendees arrange own lodging)",
    organizer: "NIDA Clinical Trials Network (CCTN)",
    hotelFit:
      "Bethesda lodging corridor credible; campus event — guestroom demand inferred, not a confirmed hotel block",
    independentDemandCase: false,
    primaryId: "gdi_opp_series_ctn_annual_conference_2027",
    linkedIds: [
      "gdi_opp_series_new_in_the_library_august_2026",
      "gdi_opp_series_march_15_17_bethesda_md_ctn_dissemination_2027",
    ],
    quarantinedIds: [
      "gdi_opp_series_if_prompted_enter_meeting_id_250_658_877__2027",
    ],
    note:
      "CTN Annual Conference 2027 and March 15–17 Dissemination describe the same Bethesda campus meeting cycle. Source-invalid Zoom/meeting-ID row stays quarantined.",
  },
  {
    familyId: "nci_rna_biology_symposium_2027",
    underlyingEvent: "2027 NCI RNA Biology Symposium (NCI / Frederick / CCR Conferences)",
    dates: "2027-04-15/2027-04-16",
    venue: "NIH / NCI symposium cycle; accommodations list includes Bethesda Marriott",
    organizer: "NCI RNA Biology Initiative / CCR Conferences",
    hotelFit:
      "Bethesda Marriott appears on official accommodations list — lodging-adjacent; named lodging owner not confirmed",
    independentDemandCase: false,
    primaryId: "gdi_opp_2027_nci_rna_biology_symposium_7",
    linkedIds: [
      "gdi_opp_series_request_a_conference_registration_website_2026",
      "gdi_opp_series_nci_rna_biology_symposium_ncif_conference_2027",
    ],
    quarantinedIds: [],
    note:
      "Three research-source rows for one 2027 symposium. Present one primary card; retain siblings as linked research.",
  },
  {
    familyId: "us_ife_conference_2027",
    underlyingEvent: "2027 U.S. IFE Conference — Bethesda North Marriott host corridor",
    dates: "2027",
    venue: "Bethesda North Marriott (competitor / overflow path for Bethesda Marriott)",
    organizer: "U.S. IFE",
    hotelFit:
      "Same 2027 IFE cycle; two research IDs describe one conference — present one primary card",
    independentDemandCase: false,
    primaryId: "gdi_s4912_512deebd0963",
    linkedIds: ["gdi_beth_v16_evt_3c8228774517"],
    quarantinedIds: [],
    note:
      "Canonical + historical IFE rows are one commercial activity. Keep Liz Conrow contact on primary.",
  },
  {
    familyId: "greater_bethesda_chamber_local_meetings",
    underlyingEvent:
      "Greater Bethesda Chamber — Off the Clock / membership receptions / local meetings",
    dates: "recurring",
    venue: "Greater Bethesda / Montgomery County local programs",
    organizer: "Greater Bethesda Chamber of Commerce",
    hotelFit:
      "One chamber account with local catering / reception demand — collapse duplicate research cards",
    independentDemandCase: false,
    primaryId: "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
    linkedIds: ["gdi_e2d_770e3b44ad97c1ae"],
    quarantinedIds: [],
    note:
      "Two IDs for the same chamber commercial relationship. Primary retains Clarissa Harris.",
  },
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function indexFamilies() {
  const byId = new Map();
  for (const fam of BETHESDA_COMMERCIAL_EVENT_FAMILIES_V1) {
    byId.set(fam.primaryId, { ...fam, role: FAMILY_ROLE.PRIMARY });
    for (const id of fam.linkedIds || []) {
      byId.set(id, { ...fam, role: FAMILY_ROLE.LINKED_RESEARCH });
    }
    for (const id of fam.quarantinedIds || []) {
      byId.set(id, { ...fam, role: FAMILY_ROLE.QUARANTINED });
    }
  }
  return byId;
}

const FAMILY_BY_ID = indexFamilies();

export function classifyBethesdaCommercialFamilyRole(opportunity = {}) {
  const id = text(opportunity.id || opportunity.opportunityId);
  const titleSource = text(opportunity.titleSourceStatus);
  if (titleSource === "TITLE_SOURCE_INVALID" || /TITLE_SOURCE_INVALID/i.test(text(opportunity.title))) {
    const fam = FAMILY_BY_ID.get(id);
    return {
      familyId: fam?.familyId || null,
      role: FAMILY_ROLE.QUARANTINED,
      reason: "TITLE_SOURCE_INVALID",
      primaryId: fam?.primaryId || null,
      listVisible: false,
      countsAsQualifiedOpportunity: false,
    };
  }
  const fam = FAMILY_BY_ID.get(id);
  if (!fam) {
    return {
      familyId: null,
      role: FAMILY_ROLE.STANDALONE,
      reason: null,
      primaryId: id || null,
      listVisible: true,
      countsAsQualifiedOpportunity: true,
    };
  }
  if (fam.role === FAMILY_ROLE.PRIMARY) {
    return {
      familyId: fam.familyId,
      role: FAMILY_ROLE.PRIMARY,
      reason: null,
      primaryId: fam.primaryId,
      linkedIds: fam.linkedIds || [],
      quarantinedIds: fam.quarantinedIds || [],
      underlyingEvent: fam.underlyingEvent,
      hotelFit: fam.hotelFit,
      listVisible: true,
      countsAsQualifiedOpportunity: true,
    };
  }
  if (fam.role === FAMILY_ROLE.QUARANTINED) {
    return {
      familyId: fam.familyId,
      role: FAMILY_ROLE.QUARANTINED,
      reason: "FAMILY_QUARANTINED",
      primaryId: fam.primaryId,
      listVisible: false,
      countsAsQualifiedOpportunity: false,
    };
  }
  return {
    familyId: fam.familyId,
    role: FAMILY_ROLE.LINKED_RESEARCH,
    reason: "DUPLICATE_SOURCE_RECORD_SAME_UNDERLYING_EVENT",
    primaryId: fam.primaryId,
    underlyingEvent: fam.underlyingEvent,
    listVisible: false,
    countsAsQualifiedOpportunity: false,
  };
}

/**
 * Annotate opportunities with family metadata. Optionally hide linked/quarantined
 * from the actionable list while preserving them for detail lookup.
 */
export function applyBethesdaCommercialEventFamilyPresentation(
  opportunities = [],
  { hideLinkedFromList = true } = {}
) {
  const annotated = (Array.isArray(opportunities) ? opportunities : []).map((o) => {
    const family = classifyBethesdaCommercialFamilyRole(o);
    return {
      ...o,
      commercialEventFamily: family,
      commercialEventFamilyId: family.familyId,
      commercialEventFamilyRole: family.role,
      commercialEventFamilyPrimaryId: family.primaryId,
      listVisible: family.listVisible !== false,
      countsAsQualifiedOpportunity: family.countsAsQualifiedOpportunity !== false,
    };
  });

  const list = hideLinkedFromList
    ? annotated.filter((o) => o.listVisible !== false)
    : annotated;

  const rawCount = annotated.length;
  const uniqueCommercial = annotated.filter(
    (o) => o.countsAsQualifiedOpportunity !== false
  ).length;
  const linkedHidden = annotated.filter(
    (o) => o.commercialEventFamilyRole === FAMILY_ROLE.LINKED_RESEARCH
  ).length;
  const quarantined = annotated.filter(
    (o) => o.commercialEventFamilyRole === FAMILY_ROLE.QUARANTINED
  ).length;

  return {
    version: BETHESDA_88_COMMERCIAL_EVENT_FAMILY_V1,
    opportunities: list,
    allAnnotated: annotated,
    counts: {
      rawResearchIds: rawCount,
      uniqueCommercialOpportunities: uniqueCommercial,
      linkedResearchHidden: linkedHidden,
      quarantinedHidden: quarantined,
      listVisible: list.length,
    },
    families: BETHESDA_COMMERCIAL_EVENT_FAMILIES_V1,
  };
}

export function buildBethesdaCommercialEventFamilyCrosswalk(opportunities = []) {
  const byId = new Map(
    (opportunities || []).map((o) => [text(o.id || o.opportunityId), o])
  );
  return BETHESDA_COMMERCIAL_EVENT_FAMILIES_V1.map((fam) => {
    const members = [fam.primaryId, ...(fam.linkedIds || []), ...(fam.quarantinedIds || [])]
      .map((id) => {
        const row = byId.get(id);
        const role = classifyBethesdaCommercialFamilyRole(row || { id });
        return {
          id,
          role: role.role,
          title: row?.title || null,
          organizationName: row?.organizationName || null,
          presentInInventory: Boolean(row),
          commercialMaturity:
            row?.commercialMaturity || row?.priorityPillLabel || null,
          contactName: row?.primaryContact?.name || row?.primaryContactName || null,
          titleSourceStatus: row?.titleSourceStatus || null,
        };
      });
    return {
      ...fam,
      members,
      independentDemandCase: fam.independentDemandCase === true,
      proposedPrimaryDisplayRecord: fam.primaryId,
      suppressedFromList: [
        ...(fam.linkedIds || []),
        ...(fam.quarantinedIds || []),
      ],
    };
  });
}
