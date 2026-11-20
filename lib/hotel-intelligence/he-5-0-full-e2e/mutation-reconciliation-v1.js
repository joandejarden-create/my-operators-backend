/**
 * HE 5.0 founder pre-apply mutation reconciliation (v1).
 * Resolves identity conflicts; READY / HOLD / REJECT each proposed op.
 * applyAuthorized remains false.
 */

import { APPLY_GATE, HE50_PILOT_VERSION, HE50_BETHESDA_HPC_ID } from "./contract.js";

export const MUTATION_REC = Object.freeze({
  READY: "READY",
  HOLD: "HOLD",
  REJECT: "REJECT",
});

export const CONFLICT_RESOLUTIONS = Object.freeze({
  ac_rooms: {
    resolvedValue: 140,
    confidence: "HIGH",
    discardedAlt: 146,
    disposition: "RESOLVED",
    rationale:
      "Opening press + Cvent + Hotel Business converge on 140 (86 king + 50 queen + 4 suites). 146 was an outdated OTA listing — not staged as confident alternate.",
    sources: [
      "https://www.delcotimes.com/2024/09/27/ac-hotel-by-marriott-at-ellis-preserve-holds-grand-opening/",
      "https://www.cvent.com/venues/newtown-square/hotel/ac-hotel-newtown-square/venue-bbfcb889-1474-4e7c-afda-54dfadd1e929",
      "https://hotelbusiness.com/equus-capital-opens-philadelphia-area-ac-hotel/",
    ],
    operatorNote:
      "Press: Gulph Creek Hotels manages; Equus Capital opened — do not treat Mullen portfolio listing as verified current ownership/operator.",
  },
  secrets_cap_cana: {
    disposition: "VERIFIED_DISTINCT_FROM_DREAMS",
    hpcId: "recd7XAM6tLjzQhaI",
    propertyName: "Secrets Cap Cana Resort & Spa",
    brand: "Secrets (Hyatt Inclusive Collection)",
    lat: 18.4843873,
    lng: -68.3942656,
    dreamsNeighbor: {
      hpcId: "reclS16CMUS6eJgpC",
      propertyName: "Dreams Cap Cana Resort & Spa",
      brand: "Dreams (Hyatt Inclusive Collection)",
      lat: 18.480713,
      lng: -68.39564,
    },
    lowercaseAliasNeighbor: {
      hpcId: "recFP9eslejVXbyuR",
      propertyName: "secrets cap cana resort spa adults all inclusive",
      note: "Likely duplicate/shell of Secrets — steward merge review; do not confuse with Dreams",
    },
    rationale:
      "Live HPC read: exact Secrets name + Secrets brand + coords ≈ external Secrets location; Dreams is a separate HPC record with Dreams brand and distinct coordinates.",
  },
  secrets_papagayo: {
    disposition: "HPC_NATIVE_CREATE_WITH_LEGACY_PROVENANCE",
    legacyHotelCensusProvenanceId: "recyX1wLsHBJYRPw7",
    name: "Secrets Papagayo Costa Rica",
    rooms: 202,
    website:
      "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/costa-rica/papagayo-costa-rica/",
    rules: [
      "HPC CREATE after live collision confirms absence — do not call legacy Hotel Census at runtime",
      "Carry rooms=202 + official website from free authoritative sources",
      "Legacy census id is historical provenance only — never HE canonical hotel id",
      "Owner Website=mullencap on legacy census is a signal, not automatic OWNED_BY graph edge",
    ],
  },
  bethesda_renovation: {
    disposition: "EVENT_LEVEL_DUAL_DATES_NOT_SINGLE_YEAR_CONFLICT",
    events: [
      {
        year: 2015,
        role: "major_renovation_near_complete",
        evidence: "Bethesda Magazine 2015-10-08; Travel Weekly Year Last Renovated 2015; CMBS renovated 2015",
      },
      {
        year: 2017,
        role: "contemporary_guestroom_unveil_press",
        evidence: "EIN/Marketwired 2017-01-23 guestroom + public-space unveil; Cvent lists Renovated 2017",
      },
    ],
    rationale:
      "2015 and 2017 describe different public milestones of the same renovation program — preserve both event facts; do not collapse to one unsupported year.",
  },
  mullen_portfolio: {
    disposition: "PROPOSED_LISTING_NOT_VERIFIED_HOLDING",
    rule: "Public mullencap.com portfolio association ≠ independently verified current legal ownership",
    exceptionsNoted: [
      "Bethesda: economic sponsor staged MEDIUM from portfolio + HQ match + PropCo research — still not UBO",
      "Papagayo census Owner Website points to mullencap.com — stronger signal; still require separate HI edge approval",
      "JW Costa Elena trade press claims owner/operator family — VERIFY_BEFORE_GRAPH",
    ],
  },
});

/** @returns {object[]} reconciled mutation rows */
export function buildMutationReconciliation() {
  return [
    {
      op: 1,
      id: "M-BETH-001",
      hotel: "Bethesda Marriott",
      canonicalRecord: HE50_BETHESDA_HPC_ID,
      target: "appCCUsuGsE1ifoLk / HPC tbl9aY5ijiuIzzWam / Year Built",
      existingValue: "empty (live probe: Year Built absent)",
      proposedValue: 1979,
      evidence: "Travel Weekly + CMBS + Cvent consensus",
      conflictStatus: "NONE",
      duplicateCheck: "PASS — update existing HPC",
      expectedHeDisplay: "Overview Year Built = 1979 after apply + re-read",
      recommendation: MUTATION_REC.READY,
    },
    {
      op: 2,
      id: "M-BETH-002",
      hotel: "Bethesda Marriott",
      canonicalRecord: HE50_BETHESDA_HPC_ID,
      target: "HI Graph Evidence / renovation_events",
      existingValue: null,
      proposedValue: CONFLICT_RESOLUTIONS.bethesda_renovation.events,
      evidence: CONFLICT_RESOLUTIONS.bethesda_renovation.rationale,
      conflictStatus: "RESOLVED_AS_DUAL_EVENTS",
      duplicateCheck: "PASS — evidence CREATE keyed by hotel+claim",
      expectedHeDisplay: "Sources & Evidence shows 2015 major reno + 2017 unveil; no single false Last Renovation year",
      recommendation: MUTATION_REC.READY,
    },
    {
      op: 3,
      id: "M-BETH-003",
      hotel: "Bethesda Marriott",
      canonicalRecord: HE50_BETHESDA_HPC_ID,
      target: "HI Graph Evidence / cmbs_financing_note",
      existingValue: null,
      proposedValue: { loanUsd: 39800000, ratePct: 7.472, maturity: "2029-02-01" },
      evidence: "https://cmbs.news/property/bethesda-marriott-bethesda-md/",
      conflictStatus: "NONE — evidence only, no lender edge",
      duplicateCheck: "PASS",
      expectedHeDisplay: "Sources cite CMBS terms; Ownership does not invent current lender",
      recommendation: MUTATION_REC.READY,
    },
    {
      op: 4,
      id: "M-BETH-REF-STAKEHOLDER",
      hotel: "Bethesda Marriott",
      canonicalRecord: HE50_BETHESDA_HPC_ID,
      target: "HI Orgs + Rel (prior dry-run package)",
      existingValue: "prior staged package",
      proposedValue: "include HE-BETH-STAKEHOLDER-UPSERTS-DRY-RUN-V1.json as dependency",
      evidence: "Phase 3/4 stakeholder research",
      conflictStatus: "NOT_A_STANDALONE_MUTATION",
      duplicateCheck: "N/A — reference",
      expectedHeDisplay: "Ownership/Contacts populate only after that package is separately approved/applied",
      recommendation: MUTATION_REC.HOLD,
      holdReason: "Reference pointer only — execute via prior stakeholder dry-run approval, not this op id",
    },
    {
      op: 5,
      id: "M-NEW-ac_hotel_newtown_square",
      hotel: "AC Hotel Newtown Square",
      canonicalRecord: null,
      target: "HPC CREATE minimum identity",
      existingValue: null,
      proposedValue: {
        name: "AC Hotel Newtown Square",
        address: "200 Squire Drive, Newtown Square, PA 19073",
        rooms: 140,
        roomsConfidence: "HIGH",
        brand: "AC Hotels by Marriott",
        website: "https://www.marriott.com/en-us/hotels/phlqc-ac-hotel-newtown-square/overview/",
        operatorPress: "Gulph Creek Hotels (press — not auto HI edge)",
        mullenAssociation: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
      },
      evidence: CONFLICT_RESOLUTIONS.ac_rooms,
      conflictStatus: "ROOMS_RESOLVED_140",
      duplicateCheck: "PASS — live collision absent",
      expectedHeDisplay: "New Founder Preview deep-link after CREATE; min Overview identity",
      recommendation: MUTATION_REC.READY,
    },
    {
      op: 6,
      id: "M-NEW-carton_house_fairmont",
      hotel: "Carton House, a Fairmont Hotel",
      canonicalRecord: null,
      target: "HPC CREATE minimum identity",
      existingValue: null,
      proposedValue: {
        name: "Carton House, A Fairmont Hotel",
        rooms: 169,
        roomsConfidence: "HIGH",
        country: "Ireland",
        mullenAssociation: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
      },
      evidence: "cartonhouse.com / Accor ALL — 169 bedrooms",
      conflictStatus: "NONE",
      duplicateCheck: "PASS — live collision absent",
      expectedHeDisplay: "Min HPC Overview after CREATE",
      recommendation: MUTATION_REC.READY,
    },
    {
      op: 7,
      id: "M-NEW-hgi_newtown_square_radnor",
      hotel: "Hilton Garden Inn Newtown Square Radnor",
      canonicalRecord: null,
      target: "HPC CREATE minimum identity",
      existingValue: null,
      proposedValue: {
        name: "Hilton Garden Inn Newtown Square Radnor",
        address: "3743 West Chester Pike, Newtown Square, PA 19073",
        rooms: 138,
        roomsConfidence: "HIGH",
        roomsNote: "Autonomous free pass: Cvent + Hilton events + Expedia + Ellis + IMC",
        mullenAssociation: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
      },
      evidence: "Cvent 138 + Hilton events 138 + Expedia 138 + Ellis + IMC",
      conflictStatus: "ROOMS_RESOLVED_138_HIGH",
      duplicateCheck: "PASS — live collision absent",
      expectedHeDisplay: "Min identity with rooms 138",
      recommendation: MUTATION_REC.READY,
    },
    {
      op: 8,
      id: "M-UPD-jw_marriott_costa_elena",
      hotel: "JW Marriott Costa Elena Resort All-Inclusive",
      canonicalRecord: "recq9KWr8UE7z2549",
      target: "HPC UPDATE alias + rooms null-fill only",
      existingValue: "MATCHED_HPC",
      proposedValue: {
        rooms: 415,
        aliases: ["Dreams Las Mareas", "Dreams Las Mareas Costa Rica"],
        website:
          "https://www.marriott.com/en-us/hotels/lircj-jw-marriott-costa-elena-resort-and-spa-all-inclusive/overview/",
      },
      evidence: "Marriott news 2026-10-06 — 415 rooms; former Dreams Las Mareas",
      conflictStatus: "MULLEN_OPERATOR_CLAIM_HELD",
      duplicateCheck: "PASS — update matched HPC; alias not second CREATE",
      expectedHeDisplay: "Rooms/alias on Overview; no unverified OWNED_BY/MANAGED_BY edges",
      recommendation: MUTATION_REC.READY,
      removedFromMutation: ["mullenAssociation as graph edge", "operator=MHM auto edge"],
    },
    {
      op: 9,
      id: "M-UPD-secrets_cap_cana-identity",
      hotel: "Secrets Cap Cana Resort & Spa",
      canonicalRecord: "recd7XAM6tLjzQhaI",
      target: "HPC identity fields",
      existingValue: {
        name: "Secrets Cap Cana Resort & Spa",
        brand: "Secrets (Hyatt Inclusive Collection)",
        lat: 18.4843873,
        lng: -68.3942656,
      },
      proposedValue: "NO_OP — identity already correct and distinct from Dreams reclS16CMUS6eJgpC",
      evidence: CONFLICT_RESOLUTIONS.secrets_cap_cana,
      conflictStatus: "IDENTITY_VERIFIED_NO_WRITE_NEEDED",
      duplicateCheck: "PASS vs Dreams; steward HOLD lowercase alias recFP9eslejVXbyuR separately",
      expectedHeDisplay: "No identity change",
      recommendation: MUTATION_REC.REJECT,
      rejectReason: "Identity confirmation complete — do not rewrite correct name/brand/coords",
    },
    {
      op: 9.1,
      id: "M-UPD-secrets_cap_cana-rooms",
      hotel: "Secrets Cap Cana Resort & Spa",
      canonicalRecord: "recd7XAM6tLjzQhaI",
      target: "HPC Rooms / Keys",
      existingValue: "empty_or_unenriched",
      proposedValue: 457,
      evidence:
        "Official Hyatt Inclusive rooms + meetings pages: 457 suites; census prior hit aligned",
      conflictStatus: "ROOMS_RESOLVED_AUTONOMOUS",
      duplicateCheck: "PASS — update matched Secrets HPC only",
      expectedHeDisplay: "Overview rooms = 457 after apply",
      recommendation: MUTATION_REC.READY,
    },
    {
      op: 10,
      id: "M-NEW-secrets_papagayo",
      hotel: "Secrets Papagayo Costa Rica",
      canonicalRecord: null,
      target: "HPC CREATE minimum identity (HPC-native — no legacy census runtime id)",
      existingValue: null,
      proposedValue: {
        name: "Secrets Papagayo Costa Rica",
        rooms: 202,
        roomsConfidence: "HIGH",
        country: "Costa Rica",
        website:
          "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/costa-rica/papagayo-costa-rica/",
        legacyProvenanceNote: "historical census recyX1wLsHBJYRPw7 — not runtime SoT",
        mullenAssociation: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
      },
      evidence: CONFLICT_RESOLUTIONS.secrets_papagayo,
      conflictStatus: "LEGACY_CENSUS_INDEPENDENCE — CREATE not promote",
      duplicateCheck: "PASS — HPC collision absent; do not call legacy census at apply time",
      expectedHeDisplay: "Deep-link after CREATE; rooms 202",
      recommendation: MUTATION_REC.READY,
    },
    {
      op: 11,
      id: "M-NEW-secrets_st_lucia",
      hotel: "Secrets St. Lucia",
      canonicalRecord: null,
      target: "HPC CREATE minimum identity",
      existingValue: null,
      proposedValue: {
        name: "Secrets St. Lucia Resort & Spa",
        rooms: 355,
        roomsConfidence: "HIGH",
        country: "Saint Lucia",
        mullenAssociation: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
      },
      evidence: "Hyatt Inclusive official rooms page + St Lucia tourism certified operator",
      conflictStatus: "NONE",
      duplicateCheck: "PASS — live collision absent",
      expectedHeDisplay: "Min HPC Overview after CREATE",
      recommendation: MUTATION_REC.READY,
    },
  ];
}

export function summarizeReconciliation(rows = buildMutationReconciliation()) {
  const counts = { READY: 0, HOLD: 0, REJECT: 0 };
  for (const r of rows) counts[r.recommendation] = (counts[r.recommendation] || 0) + 1;
  return {
    version: HE50_PILOT_VERSION,
    applyGate: {
      ...APPLY_GATE,
      stopMessage:
        "STOP — AWAITING SEPARATE FOUNDER AUTHORIZATION FOR PRODUCTION AIRTABLE APPLY.",
    },
    totalOps: rows.length,
    counts,
    readyIds: rows.filter((r) => r.recommendation === MUTATION_REC.READY).map((r) => r.id),
    holdIds: rows.filter((r) => r.recommendation === MUTATION_REC.HOLD).map((r) => r.id),
    rejectIds: rows.filter((r) => r.recommendation === MUTATION_REC.REJECT).map((r) => r.id),
    conflictResolutions: CONFLICT_RESOLUTIONS,
    rows,
  };
}
