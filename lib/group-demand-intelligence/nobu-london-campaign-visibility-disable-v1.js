/**
 * Nobu London campaign-level customer-visibility disable (reversible tombstone).
 *
 * Distinguishes founder campaign rejection from permanent opportunity archiving.
 * Retains research artifacts / shadow rows / spend ledgers; removes customer list
 * resurfacing after merge, restart, or other shadow globs.
 *
 * Zero paid. No Airtable. No production publication.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const NOBU_LONDON_CAMPAIGN_VISIBILITY_DISABLE_V1 =
  "nobu-london-campaign-visibility-disable-v1";

export const NOBU_LONDON_HOTEL_ID = "recC2kbCaS48jFjFF";

export const FOUNDER_REJECTED_SECOND_WAVE_QUALITY =
  "FOUNDER_REJECTED_SECOND_WAVE_QUALITY";

export const NOBU_ASSOC_EVENTS_SECOND_WAVE_CAMPAIGN_ID =
  "nobu_london_assoc_events_second_wave_v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

export const DISABLE_REGISTRY_REL =
  "data/group-demand-intelligence/local-shadow/nobu-london-campaign-visibility-disable-v1.json";

/** Exact 23 second-wave opportunity IDs (founder rejected 2026-10-29). */
export const NOBU_ASSOC_EVENTS_SECOND_WAVE_REJECTED_IDS = Object.freeze([
  "gdi_e2d_7ce96947548a090d",
  "gdi_e2d_56bd25001d5959db",
  "gdi_e2d_4d765aafe7339603",
  "gdi_e2d_4b6b4dd87d94146e",
  "gdi_e2d_229ab982cecc0143",
  "gdi_e2d_8ca8b65ad1b893a0",
  "gdi_e2d_bdbc9963f11b63a9",
  "gdi_e2d_e4a950de4a4b6745",
  "gdi_e2d_1fb7d1a86ff242b2",
  "gdi_e2d_1fb77dbe982aa0f8",
  "gdi_e2d_36c63f85a79b6b58",
  "gdi_e2d_5621972ca83fdb5b",
  "gdi_e2d_280fcbbbd6a859ab",
  "gdi_e2d_8237b9ba5872bb2b",
  "gdi_e2d_79ed0cc863b92974",
  "gdi_e2d_5c671a111057c5c0",
  "gdi_e2d_b7fb128b89c4c707",
  "gdi_e2d_3e3a5de40bbbe35a",
  "gdi_e2d_6ac881ec36ac2cf1",
  "gdi_e2d_0921d39ec65f6a05",
  "gdi_e2d_ff7a9b089d341518",
  "gdi_e2d_5437dac241ad5b38",
  "gdi_e2d_afaa33533ea4ed20",
]);

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

export function disableRegistryPath(root = ROOT) {
  return path.join(root, DISABLE_REGISTRY_REL);
}

export function loadNobuLondonCampaignVisibilityDisable(root = ROOT) {
  const p = disableRegistryPath(root);
  if (!fs.existsSync(p)) {
    return {
      version: NOBU_LONDON_CAMPAIGN_VISIBILITY_DISABLE_V1,
      hotelId: NOBU_LONDON_HOTEL_ID,
      applied: false,
      campaigns: {},
      source: "missing_registry",
    };
  }
  try {
    const doc = JSON.parse(fs.readFileSync(p, "utf8"));
    return {
      version: NOBU_LONDON_CAMPAIGN_VISIBILITY_DISABLE_V1,
      hotelId: NOBU_LONDON_HOTEL_ID,
      applied: doc.applied !== false,
      campaigns:
        doc.campaigns && typeof doc.campaigns === "object" ? doc.campaigns : {},
      updatedAt: doc.updatedAt || null,
      source: "registry",
    };
  } catch {
    return {
      version: NOBU_LONDON_CAMPAIGN_VISIBILITY_DISABLE_V1,
      hotelId: NOBU_LONDON_HOTEL_ID,
      applied: false,
      campaigns: {},
      source: "corrupt_registry",
    };
  }
}

export function disabledOpportunityIdsForHotel(hotelId, root = ROOT) {
  if (String(hotelId || "") !== NOBU_LONDON_HOTEL_ID) return new Set();
  const doc = loadNobuLondonCampaignVisibilityDisable(root);
  if (doc.applied === false) return new Set();
  const ids = new Set();
  for (const camp of Object.values(doc.campaigns || {})) {
    if (!camp || camp.enabled === true) continue;
    if (camp.customerFacingEnabled === true) continue;
    for (const id of camp.opportunityIds || []) {
      if (id) ids.add(String(id));
    }
  }
  return ids;
}

export function isNobuCampaignCustomerFacingDisabled(opp = {}, root = ROOT) {
  if (!opp || typeof opp !== "object") return false;
  if (String(opp.hotelId || "") !== NOBU_LONDON_HOTEL_ID) return false;

  if (
    opp.founderCustomerVisibilityDisable === true ||
    opp.campaignCustomerFacingDisabled === true ||
    text(opp.founderVisibilityDisableReason) === FOUNDER_REJECTED_SECOND_WAVE_QUALITY
  ) {
    return true;
  }

  const disabledIds = disabledOpportunityIdsForHotel(opp.hotelId, root);
  if (disabledIds.has(String(opp.id || ""))) return true;

  if (
    opp.nobuLondonAssocEventsSecondWaveV1 === true ||
    text(opp.secondWaveCampaignId) === NOBU_ASSOC_EVENTS_SECOND_WAVE_CAMPAIGN_ID
  ) {
    const doc = loadNobuLondonCampaignVisibilityDisable(root);
    const camp = doc.campaigns?.[NOBU_ASSOC_EVENTS_SECOND_WAVE_CAMPAIGN_ID];
    if (camp && camp.customerFacingEnabled === false && doc.applied !== false) {
      return true;
    }
  }

  return false;
}

/**
 * Apply tombstone overlay: hide from customer lists; retain research in merge set.
 */
export function applyNobuLondonCampaignVisibilityDisable(
  opportunities = [],
  hotelId = "",
  opts = {},
) {
  const id = String(hotelId || "").trim();
  if (id !== NOBU_LONDON_HOTEL_ID) return opportunities || [];
  const root = opts.root || ROOT;
  const disabledIds = disabledOpportunityIdsForHotel(id, root);
  if (!disabledIds.size && opts.forceCampaignFlags !== true) {
    // Still stamp rows that carry campaign flags even if registry missing.
  }

  return (opportunities || []).map((o) => {
    if (!o?.id) return o;
    const hit =
      disabledIds.has(String(o.id)) ||
      isNobuCampaignCustomerFacingDisabled(o, root);
    if (!hit) return o;
    return {
      ...o,
      customerVisible: false,
      campaignCustomerFacingDisabled: true,
      founderCustomerVisibilityDisable: true,
      founderVisibilityDisableReason:
        o.founderVisibilityDisableReason || FOUNDER_REJECTED_SECOND_WAVE_QUALITY,
      // Not permanent archive — research continuation / forensic retention.
      archived: o.archived === true ? true : false,
      prospectSuppressed: true,
      localResearchRetention: true,
      doesNotAffectProduction: true,
    };
  });
}

/**
 * Shadow document header: skip customer-facing merge when disabled.
 */
export function isShadowDocumentCustomerFacingEnabled(doc = {}) {
  if (!doc || typeof doc !== "object") return true;
  if (doc.customerFacingEnabled === false) return false;
  if (doc.founderCustomerVisibilityDisable === true) return false;
  if (
    text(doc.founderVisibilityDisableReason) === FOUNDER_REJECTED_SECOND_WAVE_QUALITY
  ) {
    return false;
  }
  return true;
}

export function writeNobuAssocEventsSecondWaveDisableRegistry(opts = {}) {
  const root = opts.root || ROOT;
  const p = disableRegistryPath(root);
  const now = new Date().toISOString();
  const doc = {
    version: NOBU_LONDON_CAMPAIGN_VISIBILITY_DISABLE_V1,
    hotelId: NOBU_LONDON_HOTEL_ID,
    applied: true,
    updatedAt: now,
    note:
      "Founder rejected entire Nobu Association/Events second-wave customer staging. Research retained. Not permanent archive.",
    campaigns: {
      [NOBU_ASSOC_EVENTS_SECOND_WAVE_CAMPAIGN_ID]: {
        campaignId: NOBU_ASSOC_EVENTS_SECOND_WAVE_CAMPAIGN_ID,
        customerFacingEnabled: false,
        enabled: false,
        reason: FOUNDER_REJECTED_SECOND_WAVE_QUALITY,
        rejectedAt: now,
        opportunityIds: [...NOBU_ASSOC_EVENTS_SECOND_WAVE_REJECTED_IDS],
        opportunityCount: NOBU_ASSOC_EVENTS_SECOND_WAVE_REJECTED_IDS.length,
        shadowPath:
          "data/group-demand-intelligence/local-shadow/nobu-london-assoc-events-second-wave-opportunities-v1.json",
        researchPreserved: true,
        permanentArchive: false,
      },
    },
  };
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`, "utf8");
  return doc;
}
