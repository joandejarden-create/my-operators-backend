/**
 * Hotel-scoped GDI artifact paths.
 * Charlotte's commercial completion pack is valid only for Charlotte.
 * Other hotels must not inherit that pack.
 */

import path from "node:path";

export const CHARLOTTE_GDI_HPC_ID = "recbRIFhro6tLmSrO";

export const GDI_THREE_HOTEL_COHORT = Object.freeze([
  {
    key: "mayflower",
    hpcId: "rec0eTEnCqlsGJJt9",
    displayName: "The Mayflower Hotel, Washington, DC",
  },
  {
    key: "bethesda",
    hpcId: "recLuxvwwxID7U2B8",
    displayName: "Bethesda Marriott, Bethesda, Maryland",
  },
  {
    key: "wyndham_san_diego",
    hpcId: "recnPB3Q02NO8Ma1o",
    displayName: "Wyndham San Diego Bayside, San Diego, California",
  },
]);

export function resolveGdiHotelArtifactScope(hotelId, repoRoot) {
  const id = String(hotelId || "").trim();
  const hotelData = path.join(
    repoRoot,
    "data/group-demand-intelligence/hotels",
    id
  );
  if (id === CHARLOTTE_GDI_HPC_ID) {
    return {
      hotelId: id,
      hotelData,
      scope: "charlotte_completion_pack",
      commercialDecisionsPath: path.join(
        repoRoot,
        "reports/gdi-charlotte-commercial-and-segment-completion-v1/COMMERCIAL_REVIEW_DECISIONS.json"
      ),
      proposedPayloadsPath: path.join(
        repoRoot,
        "reports/gdi-charlotte-commercial-and-segment-completion-v1/PROPOSED_PUBLICATION_PAYLOADS.json"
      ),
      mirrorSidecarIntoHotelData: true,
    };
  }
  return {
    hotelId: id,
    hotelData,
    scope: "hotel_local_store",
    commercialDecisionsPath: path.join(hotelData, "commercial-review-decisions.json"),
    proposedPayloadsPath: path.join(hotelData, "proposed-publication-payloads.json"),
    opportunitiesPath: path.join(hotelData, "opportunities.json"),
    mirrorSidecarIntoHotelData: false,
  };
}
