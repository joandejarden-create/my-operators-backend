/**
 * Mexico two-hotel local archive registry V1 — durable, reversible, hotel-scoped.
 *
 * Marks listed opportunity IDs as archived for customer-facing inventory while
 * retaining full FS history / detail-by-id / evidence / feedback.
 *
 * Precedence: registry archivedIds win over Airtable merges and FS overlays
 * for customer visibility. Protected keep IDs are never archived by this layer.
 *
 * Does not delete records. Does not write Airtable. Does not spend.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const MEXICO_TWO_HOTEL_LOCAL_ARCHIVE_REGISTRY_V1 =
  "mexico-two-hotel-local-archive-registry-v1";

export const CHAPULTEPEC_HOTEL_ID = "recterKmNPdntCm0v";
export const CONDESA_HOTEL_ID = "recIEcSWuhSCoNLFN";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

export const ARCHIVE_REGISTRY_REL =
  "data/group-demand-intelligence/local-shadow/mexico-two-hotel-local-archive-registry-v1.json";

/** Frozen original 12 Chapultepec Events (re-research cohort). */
export const CHAPULTEPEC_ORIGINAL_12_EVENTS_ARCHIVE_IDS = Object.freeze([
  "gdi_e2d_fce300b1112333a3",
  "gdi_e2d_lasa2027_cdmx_edition_v1",
  "gdi_e2d_5f3e355b856acce2",
  "gdi_e2d_314dda6e7a5556f9",
  "gdi_e2d_c477910ddd64ce09",
  "gdi_e2d_03cf40ffeccc5ae8",
  "gdi_e2d_89b3b5b808b2f977",
  "gdi_e2d_a0504df9a6ab5a67",
  "gdi_e2d_df0df0e8644aa968",
  "gdi_e2d_7effd5e6340d6c75",
  "gdi_e2d_baf89e75f384cc89",
  "gdi_e2d_adb3cac9c5109ba4",
]);

/** Dry-run §B — already hidden; formalize durable archive flags. */
export const CHAPULTEPEC_LEGACY_FORMALIZE_ARCHIVE_IDS = Object.freeze([
  "gdi_sc_6b3637edefaa3bbd",
  "gdi_sc_2d6aef305a6ee2e5",
  "gdi_e2d_dbb9e91857b5890b",
  "gdi_sc_9bde48479442c8d0",
  "gdi_sc_75ab57f980d4206a",
  "gdi_sc_6f70a1455fb65b80",
  "gdi_sc_626c437dd8164f1f",
  "gdi_sc_rc_00d42fd8c8f0",
  "gdi_e2d_cc9e19bda366a97f",
]);

/** Seven new Chapultepec Events from Mexico two-hotel discovery — never archive here. */
export const CHAPULTEPEC_PROTECTED_NEW_EVENTS_IDS = Object.freeze([
  "gdi_ev_07a77baf211f",
  "gdi_ev_7e015538dbf3",
  "gdi_ev_835c7e6fdc87",
  "gdi_ev_9195a56c4ba7",
  "gdi_ev_947cabf4c76b",
  "gdi_ev_b163f94cfa0f",
  "gdi_ev_d2e49660dd36",
]);

/** Eight protected Chapultepec Scaling Companies. */
export const CHAPULTEPEC_PROTECTED_SCALING_IDS = Object.freeze([
  "gdi_sc_stage_e_mendel_shadow",
  "gdi_sc_stage_e_r2_shadow",
  "gdi_sc_stage_e_clip_shadow",
  "gdi_sc_mx_live_maxhub_a91fd65324",
  "gdi_sc_mx_live_netflix_a08f478716",
  "gdi_sc_mx_live_toshiba_global_commerce_solutions_beb1e9df68",
  "gdi_sc_mx_live_salesforce_2e8373bd68",
  "gdi_sc_mx_live_immunotec_d0ff2ad190",
]);

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

export function archiveRegistryPath(root = ROOT) {
  return path.join(root, ARCHIVE_REGISTRY_REL);
}

export function emptyHotelBucket(hotelId) {
  return {
    hotelId,
    archivedIds: [],
    protectedKeepIds: [],
    batches: [],
    updatedAt: null,
  };
}

export function loadMexicoTwoHotelLocalArchiveRegistry(root = ROOT) {
  const p = archiveRegistryPath(root);
  if (!fs.existsSync(p)) {
    return {
      version: MEXICO_TWO_HOTEL_LOCAL_ARCHIVE_REGISTRY_V1,
      archiveApplied: false,
      hotels: {
        [CHAPULTEPEC_HOTEL_ID]: emptyHotelBucket(CHAPULTEPEC_HOTEL_ID),
        [CONDESA_HOTEL_ID]: emptyHotelBucket(CONDESA_HOTEL_ID),
      },
      source: "missing_registry",
    };
  }
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  const hotels = doc.hotels && typeof doc.hotels === "object" ? doc.hotels : {};
  for (const hid of [CHAPULTEPEC_HOTEL_ID, CONDESA_HOTEL_ID]) {
    if (!hotels[hid]) hotels[hid] = emptyHotelBucket(hid);
    hotels[hid].archivedIds = Array.isArray(hotels[hid].archivedIds)
      ? hotels[hid].archivedIds.map(String)
      : [];
    hotels[hid].protectedKeepIds = Array.isArray(hotels[hid].protectedKeepIds)
      ? hotels[hid].protectedKeepIds.map(String)
      : [];
  }
  return {
    ...doc,
    version: doc.version || MEXICO_TWO_HOTEL_LOCAL_ARCHIVE_REGISTRY_V1,
    hotels,
    source: p,
  };
}

export function archivedIdSetForHotel(hotelId, root = ROOT) {
  const doc = loadMexicoTwoHotelLocalArchiveRegistry(root);
  const bucket = doc.hotels?.[String(hotelId || "")] || emptyHotelBucket(hotelId);
  const protectedKeep = new Set([
    ...(bucket.protectedKeepIds || []),
    ...(String(hotelId) === CHAPULTEPEC_HOTEL_ID
      ? [
          ...CHAPULTEPEC_PROTECTED_NEW_EVENTS_IDS,
          ...CHAPULTEPEC_PROTECTED_SCALING_IDS,
        ]
      : []),
  ]);
  return new Set(
    (bucket.archivedIds || []).filter((id) => id && !protectedKeep.has(id)),
  );
}

export function isIdArchivedInMexicoTwoHotelRegistry(hotelId, opportunityId, root = ROOT) {
  return archivedIdSetForHotel(hotelId, root).has(String(opportunityId || ""));
}

/**
 * Overlay: mark registry-archived IDs inactive for customer lists.
 * Never archives protected keep IDs. Never deletes. Detail-by-id still finds rows.
 */
export function applyMexicoTwoHotelLocalArchiveRegistry(
  opportunities = [],
  hotelId = "",
  opts = {},
) {
  const env = opts.env || process.env;
  if (
    String(env.GDI_MEXICO_TWO_HOTEL_LOCAL_ARCHIVE || "1").trim() === "0"
  ) {
    return opportunities;
  }
  const hid = text(hotelId);
  if (!hid) return opportunities;

  const root = opts.root || ROOT;
  const archived = archivedIdSetForHotel(hid, root);
  if (!archived.size) return opportunities;

  const now = opts.nowIso || new Date().toISOString();
  const cohort = MEXICO_TWO_HOTEL_LOCAL_ARCHIVE_REGISTRY_V1;

  return (opportunities || []).map((o) => {
    if (!o?.id) return o;
    const id = String(o.id);
    if (!archived.has(id)) return o;
    return {
      ...o,
      archived: true,
      localActiveInventory: false,
      localInventorySuperseded: true,
      localInventorySupersededAt: o.localInventorySupersededAt || now,
      localInventorySupersedeReason:
        o.localInventorySupersedeReason ||
        "mexico_two_hotel_local_archive_registry_v1",
      localArchiveRegistryCohort: cohort,
      localArchiveRegistryHotelId: hid,
      historicalResearchRetained: true,
      doesNotAffectProduction: true,
      // Customer list must not resurrect via shadow / inventory lanes
      customerVisible: false,
      prospectSuppressed: true,
    };
  });
}

/**
 * Build founder-approved Chapultepec archive apply document (Condesa empty).
 */
export function buildFounderApprovedChapultepecArchiveRegistryDoc(opts = {}) {
  const now = opts.nowIso || new Date().toISOString();
  const archivedIds = [
    ...new Set([
      ...CHAPULTEPEC_ORIGINAL_12_EVENTS_ARCHIVE_IDS,
      ...CHAPULTEPEC_LEGACY_FORMALIZE_ARCHIVE_IDS,
    ]),
  ];
  const protectedKeepIds = [
    ...CHAPULTEPEC_PROTECTED_NEW_EVENTS_IDS,
    ...CHAPULTEPEC_PROTECTED_SCALING_IDS,
  ];
  // Safety: never list a protected ID as archived
  const protectedSet = new Set(protectedKeepIds);
  const safeArchived = archivedIds.filter((id) => !protectedSet.has(id));

  return {
    version: MEXICO_TWO_HOTEL_LOCAL_ARCHIVE_REGISTRY_V1,
    archiveApplied: true,
    appliedAt: now,
    founderApproval:
      opts.founderApproval ||
      "FOUNDER ARCHIVE APPROVAL — Chapultepec original 12 Events + nine hidden legacy (2026-10-29)",
    hotels: {
      [CHAPULTEPEC_HOTEL_ID]: {
        hotelId: CHAPULTEPEC_HOTEL_ID,
        hotelName: "Grand Fiesta Americana Chapultepec",
        archivedIds: safeArchived,
        protectedKeepIds,
        batches: [
          {
            batchId: "chapultepec_original_12_events_reresearch_v1",
            archivedIds: [...CHAPULTEPEC_ORIGINAL_12_EVENTS_ARCHIVE_IDS],
            reason:
              "Original 12 re-researched Events — temporary non-destructive archive; retain for future buyer research / reactivation",
            appliedAt: now,
          },
          {
            batchId: "chapultepec_legacy_formalize_section_b_v1",
            archivedIds: [...CHAPULTEPEC_LEGACY_FORMALIZE_ARCHIVE_IDS],
            reason:
              "Formalize durable archive flags on already-hidden FS legacy records (dry-run §B)",
            appliedAt: now,
          },
        ],
        updatedAt: now,
      },
      [CONDESA_HOTEL_ID]: {
        hotelId: CONDESA_HOTEL_ID,
        hotelName: "Fiesta Americana Condesa Cancún All Inclusive",
        archivedIds: [],
        protectedKeepIds: [],
        batches: [],
        note: "No archival changes — Condesa unchanged including ASUR",
        updatedAt: now,
      },
    },
  };
}

export function writeMexicoTwoHotelLocalArchiveRegistry(doc, root = ROOT) {
  const p = archiveRegistryPath(root);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`, "utf8");
  return p;
}

/**
 * Restore helper: remove IDs from hotel archivedIds (does not delete history).
 */
export function buildRegistryAfterRestore(opportunityIds = [], hotelId = CHAPULTEPEC_HOTEL_ID, root = ROOT) {
  const doc = loadMexicoTwoHotelLocalArchiveRegistry(root);
  const hid = String(hotelId);
  const bucket = doc.hotels?.[hid] || emptyHotelBucket(hid);
  const remove = new Set((opportunityIds || []).map(String));
  const nextArchived = (bucket.archivedIds || []).filter((id) => !remove.has(id));
  const now = new Date().toISOString();
  return {
    ...doc,
    hotels: {
      ...doc.hotels,
      [hid]: {
        ...bucket,
        hotelId: hid,
        archivedIds: nextArchived,
        updatedAt: now,
        batches: [
          ...(bucket.batches || []),
          {
            batchId: `restore_${now}`,
            restoredIds: [...remove],
            appliedAt: now,
          },
        ],
      },
    },
    lastRestoreAt: now,
  };
}
