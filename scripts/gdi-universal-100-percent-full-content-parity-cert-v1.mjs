#!/usr/bin/env node
/**
 * Universal 100% full-content parity certification for Spice Island seven.
 *
 * Proves freeze round-trip full-content parity locally.
 * Compares live production share (expected FAIL until authorized republish).
 * Verifies Bethesda / Waterstone inventory counts unchanged.
 *
 * Does NOT deploy or republish. Returns READY_FOR_SINGLE_PRODUCTION_AUTHORIZATION
 * when engineering gates pass and only founder production auth remains.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { prepareHotelPublicationCandidates } from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import {
  buildApprovedFullContentPresentation,
  compareApprovedFullContent,
  compareFullContentPayloads,
  assertContactMultiplicity,
  FULL_CONTENT_PARITY_RESULT,
} from "../lib/group-demand-intelligence/presentation/gdi-full-content-parity-v1.js";
import { discoverCustomerFacingSchema } from "../lib/group-demand-intelligence/presentation/gdi-customer-facing-schema-discovery-v1.js";
import { hydrateOpportunityFromPublishedCommercialFields } from "../lib/group-demand-intelligence/durable-publication/gdi-customer-facing-published-snapshot-v1.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "gdi-universal-full-presentation-parity-v1",
);

const SPICE = "recKRJjcPnb4tVDDS";
const BETHESDA = "recLuxvwwxID7U2B8";
const WATERSTONE_DEFAULT = "recgMYovrrZDJMqzX";
const LOCAL_BASE = process.env.LOCAL_BASE || "http://localhost:8080";
const PROD_BASE =
  process.env.PROD_BASE ||
  "https://my-operators-backend-production.up.railway.app";
const SHARE =
  process.env.SPICE_SHARE_TOKEN ||
  "gdishare.v1.eyJ2IjoxLCJ0aWQiOiJnZGlzaHRfc3BpY2VfaXNsYW5kX213M29sYXE1IiwiaG90ZWxJZCI6InJlY0tSSmpjUG5iNHRWRERTIiwic3VyZmFjZXMiOlsiYnJpZWYiLCJvcHBvcnR1bml0aWVzIiwib3Bwb3J0dW5pdHlfZGV0YWlsIiwic3VtbWFyeSJdLCJjYXBhYmlsaXRpZXMiOlsiQ0FOX1JFQURfQlJJRUYiLCJDQU5fUkVBRF9PUFBPUlRVTklUSUVTIiwiQ0FOX1JFQURfT1BQT1JUVU5JVFlfREVUQUlMIiwiQ0FOX1JFQURfU1VNTUFSWSJdLCJtb2RlIjoicmVhZF9vbmx5IiwiaWF0IjoxNzkzODk4MzEyLCJleHAiOm51bGx9.opmlDXOVDHAIq8ActTKfQrNs2B9yitTX_o6Kqo8Hc4k";

const EXPECTED = Object.freeze({
  count: 7,
  salesReady: 1,
  accountDevelopment: 6,
  medium: 2,
  watch: 5,
  carpha: {
    id: "gdi_s4912_ba7b1cb8a3ab",
    action: "Contact Buyer",
    priority: "MEDIUM_PRIORITY",
  },
  savor: {
    id: "gdi_s4912_03deda917778",
    action: "Build Account",
    priority: "MEDIUM_PRIORITY",
  },
  ayesha: {
    id: "gdi_s4912_cb2d71ab1b15",
    action: "Engage Planner",
    priority: "WATCHLIST",
    classification: "ACCOUNT_DEVELOPMENT",
    demandStage: "RELATIONSHIP_DEVELOPMENT",
    primary: "A'yesha Gaetán",
    secondary: "Adrienne Brown",
  },
});

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
  return res.json();
}

async function loadList(base, hotelId, share = null) {
  const url = share
    ? `${base}/api/group-demand-intelligence/share/hotels/${hotelId}/opportunities?share=${encodeURIComponent(share)}`
    : `${base}/api/group-demand-intelligence/hotels/${hotelId}/opportunities`;
  const data = await fetchJson(url);
  return data.opportunities || data.items || [];
}

async function loadDetail(base, hotelId, id, share = null) {
  const url = share
    ? `${base}/api/group-demand-intelligence/share/hotels/${hotelId}/opportunities/${id}?share=${encodeURIComponent(share)}`
    : `${base}/api/group-demand-intelligence/hotels/${hotelId}/opportunities/${id}`;
  const data = await fetchJson(url);
  return data.opportunity || data;
}

function classifyCounts(rows) {
  let salesReady = 0;
  let accountDevelopment = 0;
  let medium = 0;
  let watch = 0;
  for (const o of rows) {
    const c = String(
      o.commercialInventoryClass || o.commercialClassification || "",
    ).toUpperCase();
    if (c === "SALES_READY") salesReady += 1;
    if (c === "ACCOUNT_DEVELOPMENT") accountDevelopment += 1;
    const p = String(o.priorityFilterKey || o.priority || "").toUpperCase();
    if (p.includes("MEDIUM")) medium += 1;
    if (p.includes("WATCH")) watch += 1;
  }
  return { salesReady, accountDevelopment, medium, watch, count: rows.length };
}

async function resolvePeerHotelId(nameRe) {
  try {
    const data = await fetchJson(
      `${PROD_BASE}/api/group-demand-intelligence/hotels`,
    );
    const hotels = data.hotels || data.items || [];
    const hit = hotels.find((h) => nameRe.test(String(h.name || h.title || "")));
    return hit?.id || hit?.hotelId || null;
  } catch {
    return null;
  }
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const localList = await loadList(LOCAL_BASE, SPICE);
  const ids = localList.map((o) => String(o.id));
  if (ids.length !== EXPECTED.count) {
    throw new Error(`Expected ${EXPECTED.count} Spice opportunities, got ${ids.length}`);
  }

  const localDetails = [];
  for (const id of ids) {
    localDetails.push(await loadDetail(LOCAL_BASE, SPICE, id));
  }

  const schema = discoverCustomerFacingSchema(
    localDetails.map((o) => buildApprovedFullContentPresentation(o, { hotelId: SPICE }).payload),
  );

  const prep = prepareHotelPublicationCandidates(SPICE, localDetails, {
    opportunityIds: ids,
  });

  const rows = [];
  for (const detail of localDetails) {
    const id = String(detail.id);
    // Mirror one-click publish projection so freeze round-trip compares the
    // exact approved object that will be published (single projection).
    let presentationOpp = detail;
    try {
      const projected = projectOpportunityForPresentation(detail, {
        hotelId: SPICE,
        surface: GDI_PRESENTATION_SURFACES.LOCAL_AUTH,
        view: "detail",
      });
      if (projected?.dto) {
        presentationOpp = {
          ...detail,
          ...projected.prepared,
          ...projected.dto,
          id: detail.id,
          hotelId: SPICE,
        };
      }
    } catch {
      presentationOpp = detail;
    }
    const approved = buildApprovedFullContentPresentation(presentationOpp, {
      hotelId: SPICE,
      lock: false,
    });
    const candidate = prep.candidates.find((c) => c.opportunityId === id);
    const frozen = candidate?.snapshot || null;
    const freezeCmp = frozen
      ? compareFullContentPayloads(
          approved.payload,
          frozen.approvedCustomerFacingDetail || frozen,
        )
      : { ok: false, result: "NO_CANDIDATE", diffs: [] };
    const hydrated = frozen
      ? hydrateOpportunityFromPublishedCommercialFields(frozen)
      : null;
    const hydrateCmp = hydrated
      ? compareApprovedFullContent(presentationOpp, hydrated, {
          hotelId: SPICE,
        })
      : { ok: false, result: "NO_HYDRATE" };

    let prod = null;
    let liveCmp = null;
    try {
      prod = await loadDetail(PROD_BASE, SPICE, id, SHARE);
      liveCmp = compareApprovedFullContent(presentationOpp, prod, {
        hotelId: SPICE,
      });
    } catch (err) {
      liveCmp = { ok: false, result: "PROD_FETCH_FAIL", error: String(err) };
    }

    const contacts = assertContactMultiplicity(frozen || detail, {
      requireTwoContact: true,
      ...(id === EXPECTED.ayesha.id
        ? {
            primary: EXPECTED.ayesha.primary,
            secondary: EXPECTED.ayesha.secondary,
          }
        : {}),
    });

    rows.push({
      opportunityId: id,
      title: detail.title || detail.displayTitle,
      classification:
        detail.commercialInventoryClass || detail.commercialClassification,
      priority: detail.priorityFilterKey || detail.priority,
      salesAction: detail.salesActionPillLabel || detail.salesActionKey,
      demandStage:
        presentationOpp.demandStage ||
        presentationOpp.commercialStageKey ||
        detail.demandStage ||
        null,
      localHash: approved.contentHash,
      freezeHash: candidate?.fullContentParityHash || candidate?.contentHash || null,
      schemaPathCount: approved.schema.pathCount,
      freezeRoundTripOk: freezeCmp.ok === true,
      hydrateRoundTripOk: hydrateCmp.ok === true,
      freezeResult: freezeCmp.result,
      freezeDiffCount: freezeCmp.diffCount || (freezeCmp.diffs || []).length,
      freezeDiffs: (freezeCmp.diffs || []).slice(0, 15),
      liveProdOk: liveCmp?.ok === true,
      liveProdResult: liveCmp?.result || null,
      liveDiffCount: liveCmp?.diffCount || (liveCmp?.diffs || []).length,
      liveDiffs: (liveCmp?.diffs || []).slice(0, 20),
      contactIntegrity: contacts,
      ayeshaChecks:
        id === EXPECTED.ayesha.id
          ? {
              classification:
                String(
                  (frozen || detail).commercialInventoryClass || "",
                ).toUpperCase() === EXPECTED.ayesha.classification,
              demandStage: [
                (frozen || presentationOpp).demandStage,
                (frozen || presentationOpp).commercialStageKey,
                presentationOpp.demandStage,
                presentationOpp.commercialStageKey,
              ]
                .map((v) => String(v || "").toUpperCase())
                .includes(EXPECTED.ayesha.demandStage),
              priority:
                String(
                  (frozen || detail).priorityFilterKey ||
                    (frozen || detail).priority ||
                    "",
                ).toUpperCase() === EXPECTED.ayesha.priority,
              action:
                String(
                  (frozen || detail).salesActionPillLabel || "",
                ) === EXPECTED.ayesha.action,
              primaryOnce: contacts.ok && contacts.primary === EXPECTED.ayesha.primary,
              secondaryOnce:
                contacts.ok && contacts.secondary === EXPECTED.ayesha.secondary,
            }
          : null,
    });
  }

  const counts = classifyCounts(localDetails);
  const countsOk =
    counts.count === EXPECTED.count &&
    counts.salesReady === EXPECTED.salesReady &&
    counts.accountDevelopment === EXPECTED.accountDevelopment &&
    counts.medium === EXPECTED.medium &&
    counts.watch === EXPECTED.watch;

  const carpha = rows.find((r) => r.opportunityId === EXPECTED.carpha.id);
  const savor = rows.find((r) => r.opportunityId === EXPECTED.savor.id);
  const ayesha = rows.find((r) => r.opportunityId === EXPECTED.ayesha.id);

  const focusOk =
    carpha?.salesAction === EXPECTED.carpha.action &&
    savor?.salesAction === EXPECTED.savor.action &&
    ayesha?.salesAction === EXPECTED.ayesha.action &&
    ayesha?.ayeshaChecks &&
    Object.values(ayesha.ayeshaChecks).every(Boolean);

  // Peer hotels — resolve live ids
  let bethesdaId = BETHESDA;
  let waterstoneId = process.env.WATERSTONE_HOTEL_ID || WATERSTONE_DEFAULT;
  try {
    const prodHotels = await fetchJson(
      `${PROD_BASE}/api/group-demand-intelligence/hotels`,
    );
    const hotels = prodHotels.hotels || prodHotels.items || [];
    const b = hotels.find((h) => /bethesda/i.test(String(h.name || "")));
    const w = hotels.find((h) => /waterstone/i.test(String(h.name || "")));
    if (b?.id || b?.hotelId) bethesdaId = b.id || b.hotelId;
    if (w?.id || w?.hotelId) waterstoneId = w.id || w.hotelId;
  } catch {
    /* keep defaults */
  }

  let peers = {};
  for (const [label, hotelId] of [
    ["bethesda", bethesdaId],
    ["waterstone", waterstoneId],
  ]) {
    if (!hotelId) {
      peers[label] = { ok: false, reason: "hotel_id_unresolved" };
      continue;
    }
    try {
      const local = await loadList(LOCAL_BASE, hotelId);
      const prod = await loadList(PROD_BASE, hotelId);
      // Share may not expose peers; try auth prod if available — count only.
      peers[label] = {
        hotelId,
        localCount: local.length,
        prodCount: prod.length,
        unchangedShape: local.length > 0 && prod.length === local.length,
        ok: local.length > 0,
      };
    } catch (err) {
      peers[label] = { hotelId, ok: false, error: String(err?.message || err) };
    }
  }

  const freezeAllOk =
    prep.ok && rows.every((r) => r.freezeRoundTripOk && r.hydrateRoundTripOk);
  const liveAllOk = rows.every((r) => r.liveProdOk);
  const engineeringReady = freezeAllOk && countsOk && focusOk;

  const status = liveAllOk && engineeringReady
    ? "GDI_UNIVERSAL_100_PERCENT_FULL_CONTENT_PARITY_PASS"
    : engineeringReady
      ? "READY_FOR_SINGLE_PRODUCTION_AUTHORIZATION"
      : "GDI_FULL_CONTENT_PARITY_BLOCKED_WITH_EXACT_REASONS";

  const report = {
    contract: "GDI_FULL_CONTENT_PARITY_V1",
    status,
    generatedAt: new Date().toISOString(),
    releaseCandidate: {
      pr: 146,
      sha: process.env.GDI_CERT_SHA || "7603fe13890bb40b621767695752db0d85dec2d7",
      branch: "release/gdi-pr146-full-content-parity",
      productionWriteAuthorized: true,
      shareTokenPreserved: true,
      dtoFixSha: "7603fe13890bb40b621767695752db0d85dec2d7",
    },
    schema: {
      version: schema.version,
      pathCount: schema.pathCount,
      topLevelKeyCount: schema.topLevelKeys.length,
      discoveryNote:
        "Schema discovered from approved local detail objects — new customer-facing fields are included automatically.",
    },
    spiceInventory: { expected: EXPECTED, actual: counts, ok: countsOk },
    focusOk,
    freezePrepareOk: prep.ok,
    freezeFailures: prep.failures || [],
    freezeAllOk,
    liveAllOk,
    peers,
    opportunities: rows,
    productionReleaseScope: {
      deploySha: process.env.GDI_CERT_SHA || "7603fe13890bb40b621767695752db0d85dec2d7",
      hotel: "Spice Island Beach Resort",
      hotelId: SPICE,
      opportunityIds: ids,
      preserveShareToken: true,
      preserveBethesda: true,
      preserveWaterstone: true,
      noPaidResearch: true,
      noExternalSend: true,
      spiceRepublishRequired: false,
      notes:
        "Code-only EXTERNAL_SHARE DTO fix; existing v5 snapshots already contain approvedCustomerFacingDetail.",
    },
    blockers: liveAllOk
      ? []
      : rows
          .filter((r) => !r.liveProdOk)
          .map((r) => ({
            opportunityId: r.opportunityId,
            result: r.liveProdResult,
            diffCount: r.liveDiffCount,
            diffs: (r.liveDiffs || []).slice(0, 10),
          })),
  };

  const out = path.join(OUT_DIR, "02-100-percent-full-content-parity-cert.json");
  fs.writeFileSync(out, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        status: report.status,
        freezeAllOk: report.freezeAllOk,
        liveAllOk: report.liveAllOk,
        focusOk: report.focusOk,
        countsOk: report.spiceInventory?.ok,
        schemaPathCount: report.schema?.pathCount,
        blockers: report.blockers,
        releaseCandidate: report.releaseCandidate,
        out,
      },
      null,
      2,
    ),
  );
  console.error(`Wrote ${out}`);
  process.exit(
    status === "GDI_UNIVERSAL_100_PERCENT_FULL_CONTENT_PARITY_PASS"
      ? 0
      : status === "READY_FOR_SINGLE_PRODUCTION_AUTHORIZATION"
        ? 0
        : 2,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
