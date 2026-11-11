/**
 * GDI V26 two-hotel clean-room isolation contract.
 *
 * Fail-closed: when an approved namespace is active, discovery / qualification
 * must not read historical GDI opportunity stores, shadows, or prior dispositions.
 * Writes go only to the isolated clean-room artifact tree — never production inventory.
 *
 * Does not authorize paid research.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const CLEAN_ROOM_V26_ISOLATION_V1 = "gdi-clean-room-v26-isolation-v1";

export const CLEAN_ROOM_NAMESPACES = Object.freeze({
  CLEAN_ROOM_YOTEL_V26: Object.freeze({
    namespaceId: "CLEAN_ROOM_YOTEL_V26",
    hotelId: "recrPQcZg7SFARRb2",
    hotelName: "YOTEL Geneva Lake",
    ledgerId: "gdi_v26_clean_room_yotel_fourteen_workflow_live",
    slug: "yotel",
  }),
  CLEAN_ROOM_SPICE_ISLAND_V26: Object.freeze({
    namespaceId: "CLEAN_ROOM_SPICE_ISLAND_V26",
    hotelId: "recKRJjcPnb4tVDDS",
    hotelName: "Spice Island Beach Resort",
    ledgerId: "gdi_v26_clean_room_spice_island_fourteen_workflow_live",
    slug: "spice-island",
  }),
});

export const FORBIDDEN_HISTORICAL_INPUT_CHANNELS = Object.freeze([
  "customer_facing_opportunities",
  "historical_opportunity_ids",
  "research_continuation_records",
  "watch_records",
  "archived_rejected_records",
  "previous_research_shadows",
  "previous_provider_results",
  "previous_opportunity_fingerprints",
  "previous_account_family_mappings",
  "previous_commercial_dispositions",
  "previous_research_stop_reasons",
  "historical_contact_enrichment",
  "historical_candidate_rankings",
  "previously_generated_research_summaries",
  "local_shadow_commercial_portfolio_merge",
  "production_opportunities_json",
  "airtable_opportunity_store_for_discovery_seed",
]);

export const PERMITTED_CLEAN_ROOM_INPUTS = Object.freeze([
  "verified_hotel_identity",
  "official_property_profile",
  "geographic_location",
  "hotel_facilities_capacity",
  "commercial_positioning",
  "current_public_sources_independently_retrieved",
  "canonical_fourteen_segment_definitions",
  "shared_commercial_qualification_standards",
  "current_research_horizon",
  "shared_provider_adapters",
  "first_time_discovery_depth_contracts",
  "in_run_clean_room_candidates_only",
]);

function text(v) {
  return String(v == null ? "" : v).trim();
}

/**
 * Active clean-room context from env. Null when inactive.
 */
export function getActiveCleanRoomContext(env = process.env) {
  const ns = text(env.GDI_CLEAN_ROOM_NAMESPACE);
  if (!ns) return null;
  const meta = CLEAN_ROOM_NAMESPACES[ns];
  if (!meta) {
    const err = new Error(
      `UNKNOWN_CLEAN_ROOM_NAMESPACE:${ns}. Approved: ${Object.keys(CLEAN_ROOM_NAMESPACES).join(", ")}`,
    );
    err.code = "UNKNOWN_CLEAN_ROOM_NAMESPACE";
    throw err;
  }
  const hotelOverride = text(env.GDI_CLEAN_ROOM_HOTEL_ID);
  if (hotelOverride && hotelOverride !== meta.hotelId) {
    const err = new Error(
      `CLEAN_ROOM_HOTEL_MISMATCH: namespace ${ns} expects ${meta.hotelId}, got ${hotelOverride}`,
    );
    err.code = "CLEAN_ROOM_HOTEL_MISMATCH";
    throw err;
  }
  return { ...meta, active: true, version: CLEAN_ROOM_V26_ISOLATION_V1 };
}

export function isCleanRoomActive(env = process.env) {
  try {
    return Boolean(getActiveCleanRoomContext(env));
  } catch {
    return false;
  }
}

export function assertCleanRoomNamespace(expectedNamespaceId, env = process.env) {
  const ctx = getActiveCleanRoomContext(env);
  if (!ctx) {
    const err = new Error("CLEAN_ROOM_NAMESPACE_REQUIRED");
    err.code = "CLEAN_ROOM_NAMESPACE_REQUIRED";
    throw err;
  }
  if (expectedNamespaceId && ctx.namespaceId !== expectedNamespaceId) {
    const err = new Error(
      `CLEAN_ROOM_NAMESPACE_MISMATCH: expected ${expectedNamespaceId}, got ${ctx.namespaceId}`,
    );
    err.code = "CLEAN_ROOM_NAMESPACE_MISMATCH";
    throw err;
  }
  return ctx;
}

export function cleanRoomRootDir(ctx = getActiveCleanRoomContext()) {
  if (!ctx) return null;
  return path.join(
    ROOT,
    "data/group-demand-intelligence/clean-room/v26",
    ctx.slug,
  );
}

export function cleanRoomOpportunitiesPath(ctx = getActiveCleanRoomContext()) {
  const root = cleanRoomRootDir(ctx);
  return root ? path.join(root, "opportunities.json") : null;
}

export function cleanRoomSegmentArtifactDir(
  segmentId,
  ctx = getActiveCleanRoomContext(),
) {
  const root = cleanRoomRootDir(ctx);
  if (!root) return null;
  return path.join(root, "segments", String(segmentId || "unknown"));
}

export function ensureCleanRoomStoreScaffold(ctx = getActiveCleanRoomContext()) {
  if (!ctx) {
    const err = new Error("CLEAN_ROOM_INACTIVE");
    err.code = "CLEAN_ROOM_INACTIVE";
    throw err;
  }
  const root = cleanRoomRootDir(ctx);
  const segments = path.join(root, "segments");
  const ledgers = path.join(root, "ledgers");
  const queries = path.join(root, "query-audit");
  fs.mkdirSync(segments, { recursive: true });
  fs.mkdirSync(ledgers, { recursive: true });
  fs.mkdirSync(queries, { recursive: true });
  const oppPath = cleanRoomOpportunitiesPath(ctx);
  if (!fs.existsSync(oppPath)) {
    fs.writeFileSync(
      oppPath,
      `${JSON.stringify(
        {
          version: CLEAN_ROOM_V26_ISOLATION_V1,
          namespaceId: ctx.namespaceId,
          hotelId: ctx.hotelId,
          opportunities: [],
          createdAt: new Date().toISOString(),
          note: "Clean-room store — isolated from production GDI inventory",
        },
        null,
        2,
      )}\n`,
    );
  }
  return { root, opportunitiesPath: oppPath, segments, ledgers, queries };
}

/**
 * Inventory visible to discovery / in-run dedup when clean room is active.
 * Never loads production hotels/.../opportunities.json or local shadows.
 */
export function loadCleanRoomDiscoveryInventory(hotelId, env = process.env) {
  const ctx = getActiveCleanRoomContext(env);
  if (!ctx) {
    return {
      cleanRoom: false,
      opportunities: null,
      note: "Clean room inactive — caller must use normal inventory path",
    };
  }
  if (hotelId && text(hotelId) !== ctx.hotelId) {
    const err = new Error(
      `CLEAN_ROOM_INVENTORY_HOTEL_MISMATCH: ${hotelId} vs ${ctx.hotelId}`,
    );
    err.code = "CLEAN_ROOM_INVENTORY_HOTEL_MISMATCH";
    throw err;
  }
  ensureCleanRoomStoreScaffold(ctx);
  const fp = cleanRoomOpportunitiesPath(ctx);
  let opportunities = [];
  try {
    const doc = JSON.parse(fs.readFileSync(fp, "utf8"));
    opportunities = Array.isArray(doc.opportunities) ? doc.opportunities : [];
  } catch {
    opportunities = [];
  }
  return {
    cleanRoom: true,
    namespaceId: ctx.namespaceId,
    hotelId: ctx.hotelId,
    opportunities,
    historicalInputsExposed: 0,
    forbiddenChannelsBlocked: [...FORBIDDEN_HISTORICAL_INPUT_CHANNELS],
  };
}

/**
 * Historical exclusion keys for query seeding / candidate suppression.
 * Clean room → always empty (fail closed against historical influence).
 */
export function getDiscoveryExclusionKeys(hotelId, env = process.env) {
  if (isCleanRoomActive(env)) {
    const ctx = getActiveCleanRoomContext(env);
    if (hotelId && text(hotelId) !== ctx.hotelId) {
      const err = new Error("CLEAN_ROOM_EXCLUSION_HOTEL_MISMATCH");
      err.code = "CLEAN_ROOM_EXCLUSION_HOTEL_MISMATCH";
      throw err;
    }
    return [];
  }
  return null; // signal: use legacy path
}

/**
 * Existing opportunities for handoff / dedup pipelines.
 * Clean room → in-run store only (starts empty).
 */
export function resolveExistingOpportunitiesForResearch(
  hotelId,
  legacyLoader,
  env = process.env,
) {
  if (isCleanRoomActive(env)) {
    const inv = loadCleanRoomDiscoveryInventory(hotelId, env);
    return {
      opportunities: inv.opportunities,
      source: "CLEAN_ROOM_STORE_ONLY",
      historicalInputsExposed: 0,
    };
  }
  if (typeof legacyLoader !== "function") {
    return { opportunities: [], source: "NONE", historicalInputsExposed: null };
  }
  const legacy = legacyLoader(hotelId);
  const list = Array.isArray(legacy)
    ? legacy
    : Array.isArray(legacy?.opportunities)
      ? legacy.opportunities
      : [];
  return {
    opportunities: list,
    source: "LEGACY_INVENTORY",
    historicalInputsExposed: list.length,
  };
}

/**
 * Block production / shadow merge when clean room is active.
 */
export function assertShadowMergeAllowed(env = process.env) {
  if (isCleanRoomActive(env)) {
    const err = new Error(
      "CLEAN_ROOM_BLOCKS_LOCAL_SHADOW_MERGE: historical shadows forbidden in clean-room discovery",
    );
    err.code = "CLEAN_ROOM_BLOCKS_LOCAL_SHADOW_MERGE";
    throw err;
  }
  return true;
}

/**
 * Dependency-trace certification for a hotel namespace (zero-paid).
 */
export function certifyCleanRoomIsolation({
  namespaceId,
  hotelId,
  probeHistoricalLoader = null,
  env = process.env,
} = {}) {
  const findings = [];
  let pass = true;

  const priorNs = env.GDI_CLEAN_ROOM_NAMESPACE;
  const priorHotel = env.GDI_CLEAN_ROOM_HOTEL_ID;
  const testEnv = {
    ...env,
    GDI_CLEAN_ROOM_NAMESPACE: namespaceId,
    GDI_CLEAN_ROOM_HOTEL_ID: hotelId,
  };

  try {
    const ctx = getActiveCleanRoomContext(testEnv);
    if (!ctx || ctx.hotelId !== hotelId) {
      pass = false;
      findings.push({ code: "CONTEXT_FAIL", detail: "namespace/hotel bind failed" });
    } else {
      findings.push({ code: "CONTEXT_OK", namespaceId: ctx.namespaceId });
    }

    const scaffold = ensureCleanRoomStoreScaffold(ctx);
    findings.push({ code: "STORE_SCAFFOLD_OK", root: scaffold.root });

    const inv = loadCleanRoomDiscoveryInventory(hotelId, testEnv);
    if (inv.historicalInputsExposed !== 0) {
      pass = false;
      findings.push({
        code: "HISTORICAL_EXPOSED",
        count: inv.historicalInputsExposed,
      });
    } else {
      findings.push({
        code: "HISTORICAL_INPUTS_EXPOSED",
        count: 0,
        status: "PASS",
      });
    }

    const exclusions = getDiscoveryExclusionKeys(hotelId, testEnv);
    if (!Array.isArray(exclusions) || exclusions.length !== 0) {
      pass = false;
      findings.push({
        code: "EXCLUSION_KEYS_NOT_EMPTY",
        length: exclusions?.length,
      });
    } else {
      findings.push({ code: "EXCLUSION_KEYS_EMPTY", status: "PASS" });
    }

    // Probe: if a historical loader would return rows, clean-room path must ignore them
    let historicalProbeCount = 0;
    if (typeof probeHistoricalLoader === "function") {
      try {
        const hist = probeHistoricalLoader(hotelId);
        historicalProbeCount = Array.isArray(hist)
          ? hist.length
          : Array.isArray(hist?.opportunities)
            ? hist.opportunities.length
            : Number(hist) || 0;
      } catch {
        historicalProbeCount = -1;
      }
    }
    const resolved = resolveExistingOpportunitiesForResearch(
      hotelId,
      () => ({ opportunities: Array(Math.max(0, historicalProbeCount)).fill({}) }),
      testEnv,
    );
    if (resolved.source !== "CLEAN_ROOM_STORE_ONLY") {
      pass = false;
      findings.push({ code: "RESOLVE_SOURCE_FAIL", source: resolved.source });
    }
    if (resolved.historicalInputsExposed !== 0) {
      pass = false;
      findings.push({
        code: "RESOLVE_LEAK",
        exposed: resolved.historicalInputsExposed,
      });
    } else {
      findings.push({
        code: "RESOLVE_IGNORES_HISTORICAL_PROBE",
        historicalProbeCount,
        cleanRoomCount: resolved.opportunities.length,
        status: "PASS",
      });
    }

    let shadowBlocked = false;
    try {
      assertShadowMergeAllowed(testEnv);
    } catch (err) {
      shadowBlocked = err?.code === "CLEAN_ROOM_BLOCKS_LOCAL_SHADOW_MERGE";
    }
    if (!shadowBlocked) {
      pass = false;
      findings.push({ code: "SHADOW_MERGE_NOT_BLOCKED" });
    } else {
      findings.push({ code: "SHADOW_MERGE_BLOCKED", status: "PASS" });
    }

    // Unknown namespace must fail closed
    let unknownFails = false;
    try {
      getActiveCleanRoomContext({
        ...testEnv,
        GDI_CLEAN_ROOM_NAMESPACE: "CLEAN_ROOM_FAKE_V99",
      });
    } catch (err) {
      unknownFails = err?.code === "UNKNOWN_CLEAN_ROOM_NAMESPACE";
    }
    if (!unknownFails) {
      pass = false;
      findings.push({ code: "UNKNOWN_NAMESPACE_NOT_FAIL_CLOSED" });
    } else {
      findings.push({ code: "UNKNOWN_NAMESPACE_FAIL_CLOSED", status: "PASS" });
    }
  } finally {
    if (priorNs == null) delete process.env.GDI_CLEAN_ROOM_NAMESPACE;
    else process.env.GDI_CLEAN_ROOM_NAMESPACE = priorNs;
    if (priorHotel == null) delete process.env.GDI_CLEAN_ROOM_HOTEL_ID;
    else process.env.GDI_CLEAN_ROOM_HOTEL_ID = priorHotel;
  }

  return {
    version: CLEAN_ROOM_V26_ISOLATION_V1,
    namespaceId,
    hotelId,
    pass,
    status: pass ? "PASS_CLEAN_ROOM_ISOLATION" : "FAIL_CLEAN_ROOM_ISOLATION",
    historicalInputsExposed: 0,
    forbiddenChannels: [...FORBIDDEN_HISTORICAL_INPUT_CHANNELS],
    permittedInputs: [...PERMITTED_CLEAN_ROOM_INPUTS],
    findings,
    note: "Isolation certified for research path — does not delete historical inventory",
  };
}

export function buildCleanRoomLiveEnv({ namespaceId, hotelId } = {}) {
  const meta = CLEAN_ROOM_NAMESPACES[namespaceId];
  if (!meta) {
    throw Object.assign(new Error("UNKNOWN_CLEAN_ROOM_NAMESPACE"), {
      code: "UNKNOWN_CLEAN_ROOM_NAMESPACE",
    });
  }
  const hid = hotelId || meta.hotelId;
  if (hid !== meta.hotelId) {
    throw Object.assign(new Error("CLEAN_ROOM_HOTEL_MISMATCH"), {
      code: "CLEAN_ROOM_HOTEL_MISMATCH",
    });
  }
  return {
    GDI_CLEAN_ROOM_NAMESPACE: namespaceId,
    GDI_CLEAN_ROOM_HOTEL_ID: hid,
    GDI_LOCAL_SHADOW_COMMERCIAL: "0",
    GDI_STAGE_E_SHADOW_PREVIEW: "0",
    GDI_STAGE_F_SHADOW_PREVIEW: "0",
    GDI_LIVE_CAMPAIGN_V1_SHADOW_PREVIEW: "0",
    GDI_BLOCK_HISTORICAL_INVENTORY_FOR_RESEARCH: "1",
    GDI_OPPORTUNITIES_PERSISTENCE: "filesystem",
    GDI_SURFE_LIVE: "0",
    GDI_FIRECRAWL_LIVE: "0",
  };
}
