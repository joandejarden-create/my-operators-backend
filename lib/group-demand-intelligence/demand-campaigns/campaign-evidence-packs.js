/**
 * Hotel-agnostic campaign evidence pack lookup.
 * Priority: campaign.evidenceSeeds (persisted) → curated YOTEL/W Rome packs.
 * YOTEL second-gen official-list merge when enabled.
 */

import { getYotelCampaignEvidencePack } from "./yotel-campaign-evidence-packs.js";
import { getWRomeCampaignEvidencePack } from "./w-rome-campaign-evidence-packs.js";
import {
  isYotelSecondGenEnabled,
  mergeYotelEvidencePackWithSecondGen,
} from "./yotel-second-generation-p0.js";

/**
 * @param {string} campaignId
 * @param {{ hotelId?: string, skipSecondGen?: boolean, campaign?: object }} [opts]
 */
export function getCampaignEvidencePack(campaignId, opts = {}) {
  const hotelId = String(opts.hotelId || opts.campaign?.hotelId || "").trim();
  const persisted = Array.isArray(opts.campaign?.evidenceSeeds)
    ? opts.campaign.evidenceSeeds.filter((s) => s && s.organizationName)
    : [];
  if (persisted.length) return persisted;

  if (hotelId === "rece0or38cxo3Fymb" || String(campaignId || "").startsWith("wrcamp_")) {
    return getWRomeCampaignEvidencePack(campaignId) || [];
  }
  if (hotelId === "recrPQcZg7SFARRb2" || String(campaignId || "").startsWith("ycamp_")) {
    const base = getYotelCampaignEvidencePack(campaignId) || [];
    if (opts.skipSecondGen === true || !isYotelSecondGenEnabled()) return base;
    return mergeYotelEvidencePackWithSecondGen(campaignId, base);
  }
  // Fallback: try curated packs by campaign id prefix
  const w = getWRomeCampaignEvidencePack(campaignId);
  if (w?.length) return w;
  const yBase = getYotelCampaignEvidencePack(campaignId) || [];
  if (yBase.length) {
    if (opts.skipSecondGen === true || !isYotelSecondGenEnabled()) return yBase;
    return mergeYotelEvidencePackWithSecondGen(campaignId, yBase);
  }
  return [];
}
