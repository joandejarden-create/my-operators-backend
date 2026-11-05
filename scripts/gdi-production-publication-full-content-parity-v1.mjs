#!/usr/bin/env node
/**
 * Rebuild Spice Island published snapshots with full customer-facing commercial
 * content, validate the publication gate, and emit a machine-readable parity
 * report. Does NOT deploy, rotate tokens, rewrite Airtable, or change
 * classifications.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCustomerFacingPublishedSnapshot,
  validateCustomerFacingPublishedSnapshot,
  hydrateOpportunityFromPublishedCommercialFields,
} from "../lib/group-demand-intelligence/durable-publication/gdi-customer-facing-published-snapshot-v1.js";
import { createGdiPublishedInventoryRegistry } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import { sanitizeOpportunityForShare } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";
import { sanitizeOpportunityForCustomerPresentation } from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";
import { buildEventsDrawerNormalizedModel } from "../lib/group-demand-intelligence/events-drawer-normalized-model-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "recKRJjcPnb4tVDDS";
const REPORT_DIR = path.join(
  ROOT,
  "reports/gdi-production-publication-full-content-parity-v1",
);
const STAGING_ROOT = path.join(
  ROOT,
  "reports/gdi-spice-island-production-activation-v1/staging-published-inventory",
);
const MANIFEST = path.join(
  ROOT,
  "reports/gdi-durable-publication-registry-spice-island-v1/02-spice-island-seven-id-publication-manifest.json",
);

const SOURCE_FILES = [
  "data/group-demand-intelligence/local-shadow/spice-island-v34-selective-promotion-opportunities-v1.json",
  "data/group-demand-intelligence/local-shadow/spice-island-v33-adaptive-continuation-opportunities-v1.json",
  "data/group-demand-intelligence/local-shadow/spice-island-v32-selective-promotion-opportunities-v1.json",
  "data/group-demand-intelligence/local-shadow/spice-island-v30-adaptive-continuation-opportunities-v1.json",
  "data/group-demand-intelligence/local-shadow/spice-island-v28-selective-promotion-opportunities-v1.json",
  "data/group-demand-intelligence/clean-room/v26/spice-island/opportunities.json",
];

const DRAWER_FIELDS = [
  "whatIsHappening",
  "potentialHotelBusiness",
  "whyThisHotel",
  "whyActNow",
  "establishedFacts",
  "uncertainties",
  "recommendedNextStep",
  "qualificationQuestions",
  "primaryContact",
  "secondaryContact",
  "sources",
  "commercialClassification",
];

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function writeJson(file, obj) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
}

function scoreOpportunity(o) {
  if (!o) return -1;
  const cib = o.commercialIntelligenceBrief || {};
  return (
    (o.whatIsHappening || cib.whatIsHappening || cib.companyAndLocalActivity ? 3 : 0) +
    (cib.potentialBusinessForThisHotel || o.potentialHotelBusiness ? 3 : 0) +
    (o.whyThisHotel || cib.whyThisHotel ? 2 : 0) +
    (o.whyNow || o.whyActNow || cib.whyActNow ? 2 : 0) +
    ((o.establishedFacts || cib.establishedFactsAndUncertainties?.facts || []).length ||
      0) +
    ((cib.qualificationQuestions || o.qualificationQuestions || []).length || 0) +
    (o.secondaryContact || cib.whoToContact?.secondary ? 1 : 0) +
    (o.commercialIntelligenceBrief ? 2 : 0)
  );
}

function loadBestSources(ids) {
  const byId = new Map();
  const provenance = {};
  for (const rel of SOURCE_FILES) {
    const abs = path.join(ROOT, rel);
    if (!fs.existsSync(abs)) continue;
    const doc = JSON.parse(fs.readFileSync(abs, "utf8"));
    const list = doc.opportunities || doc;
    if (!Array.isArray(list)) continue;
    for (const o of list) {
      if (!ids.has(o.id)) continue;
      const score = scoreOpportunity(o);
      const prev = byId.get(o.id);
      if (!prev || score >= prev.score) {
        byId.set(o.id, { opportunity: o, score, file: rel });
        provenance[o.id] = { file: rel, score };
      }
    }
  }
  return { byId, provenance };
}

function fieldValue(obj, field) {
  if (!obj) return null;
  if (field === "commercialClassification") {
    return obj.commercialInventoryClass || obj.inventoryLane || null;
  }
  if (field === "primaryContact" || field === "secondaryContact") {
    const c = obj[field];
    return c?.name
      ? {
          name: c.name,
          role: c.role || c.title || null,
          why: c.whyRelevant || c.whyThisContact || null,
        }
      : null;
  }
  if (field === "sources") {
    return Array.isArray(obj.sources)
      ? obj.sources.map((s) => (typeof s === "string" ? s : s?.url)).filter(Boolean)
      : [];
  }
  if (
    field === "establishedFacts" ||
    field === "uncertainties" ||
    field === "qualificationQuestions"
  ) {
    return Array.isArray(obj[field]) ? obj[field] : [];
  }
  return obj[field] ?? null;
}

function comparable(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return JSON.stringify(v);
  if (typeof v === "object") return JSON.stringify(v);
  return String(v).replace(/\s+/g, " ").trim();
}

function analyzeDropStage({ leanSnap, source, enriched, shareDto, field }) {
  const sourceV = comparable(fieldValue(source, field));
  const leanV = comparable(fieldValue(leanSnap, field));
  const enrichedV = comparable(fieldValue(enriched, field));
  const shareV = comparable(fieldValue(shareDto, field));
  if (!sourceV) {
    return {
      stage: "SOURCE_ABSENT",
      note: "Approved source artifact lacked this field",
    };
  }
  if (!leanV && enrichedV) {
    return {
      stage: "PUBLICATION_SNAPSHOT_LEAN",
      note: "Value existed in research/source but was omitted from the durable published snapshot",
    };
  }
  if (enrichedV && !shareV) {
    // Check drawer sections for facts/QQ which may live nested
    const drawer = shareDto?.eventsDrawerModel?.sections;
    if (field === "establishedFacts" && drawer?.facts_and_uncertainties?.facts?.length) {
      return { stage: "NONE", note: "Present in eventsDrawerModel" };
    }
    if (
      field === "qualificationQuestions" &&
      drawer?.recommended_next_step?.qualificationQuestions?.length
    ) {
      return { stage: "NONE", note: "Present in eventsDrawerModel" };
    }
    if (field === "whatIsHappening" && drawer?.what_is_happening?.body) {
      return { stage: "NONE", note: "Present in eventsDrawerModel" };
    }
    if (field === "potentialHotelBusiness" && drawer?.potential_business?.body) {
      return { stage: "NONE", note: "Present in eventsDrawerModel" };
    }
    return {
      stage: "SHARE_SANITIZE_OR_DTO",
      note: "Enriched snapshot had value but share DTO lost it",
    };
  }
  if (sourceV && shareV && sourceV !== shareV) {
    return {
      stage: "TRANSFORMED",
      note: "Value present but differs after customer-facing mapping",
    };
  }
  return { stage: "NONE", note: "Preserved" };
}

function simulateShareFromSnapshot(snap, classification) {
  const hydrated = hydrateOpportunityFromPublishedCommercialFields({
    ...snap,
    hotelId: HOTEL_ID,
    commercialInventoryClass: classification,
    inventoryLane:
      classification === "SALES_READY" ? "SALES_OPPORTUNITY" : "ACCOUNT_DEVELOPMENT",
    customerFacing: true,
    gdiPublishedInventoryRegistry: true,
  });
  const presented = sanitizeOpportunityForCustomerPresentation(hydrated);
  const sanitized = sanitizeOpportunityForShare(presented);
  const eventsDrawer =
    presented.eventsDrawerModel ||
    sanitized.eventsDrawerModel ||
    buildEventsDrawerNormalizedModel(presented, { force: true });
  return {
    ...sanitized,
    eventsDrawerModel: eventsDrawer,
    establishedFacts:
      sanitized.establishedFacts ||
      eventsDrawer?.sections?.facts_and_uncertainties?.facts ||
      null,
    uncertainties:
      sanitized.uncertainties ||
      eventsDrawer?.sections?.facts_and_uncertainties?.uncertainties ||
      null,
    qualificationQuestions:
      sanitized.qualificationQuestions ||
      eventsDrawer?.sections?.recommended_next_step?.qualificationQuestions ||
      null,
    whatIsHappening:
      sanitized.whatIsHappening ||
      eventsDrawer?.sections?.what_is_happening?.body ||
      null,
    potentialHotelBusiness:
      sanitized.potentialHotelBusiness ||
      eventsDrawer?.sections?.potential_business?.body ||
      null,
    whyActNow:
      sanitized.whyActNow ||
      sanitized.whyNow ||
      eventsDrawer?.sections?.why_act_now?.body ||
      null,
  };
}

function main() {
  ensureDir(REPORT_DIR);
  ensureDir(path.join(REPORT_DIR, "enriched-snapshots"));
  ensureDir(path.join(REPORT_DIR, "field-traces"));

  const manifest = JSON.parse(fs.readFileSync(MANIFEST, "utf8"));
  const rows = manifest.rows || [];
  const ids = new Set(rows.map((r) => r.opportunityId));
  const { byId, provenance } = loadBestSources(ids);

  const registry = createGdiPublishedInventoryRegistry({
    root: STAGING_ROOT,
    env: { ...process.env, GDI_PUBLISHED_INVENTORY_ROOT: STAGING_ROOT },
  });

  const missingFieldInventory = [];
  const parityRows = [];
  const sourceMapping = [];
  const gateResults = [];
  const updateResults = [];

  for (const row of rows) {
    const id = row.opportunityId;
    const classification = row.commercialClassification;
    const best = byId.get(id);
    if (!best) {
      missingFieldInventory.push({
        opportunityId: id,
        error: "SOURCE_NOT_FOUND",
      });
      continue;
    }
    const source = best.opportunity;
    const leanDoc = registry.loadSnapshot(
      HOTEL_ID,
      id,
      registry.listPublished(HOTEL_ID).find((r) => r.opportunityId === id)
        ?.publishedVersion || 1,
    );
    const leanSnap = leanDoc?.snapshot || leanDoc || {};

    const enriched = buildCustomerFacingPublishedSnapshot(source, {
      hotelId: HOTEL_ID,
      canonicalTitle: row.canonicalTitle,
      classification,
    });
    // Preserve lean registry identity stamps / evidence already approved.
    enriched.commercialInventoryClass = classification;
    enriched.customerFacing = true;

    const gate = validateCustomerFacingPublishedSnapshot(enriched);
    gateResults.push(gate);
    writeJson(path.join(REPORT_DIR, "enriched-snapshots", `${id}.json`), enriched);

    if (gate.ok) {
      try {
        const updated = registry.updatePublishedOpportunity({
          hotelId: HOTEL_ID,
          opportunityId: id,
          classification,
          snapshot: enriched,
          approval: {
            authorized: true,
            authorizedBy: "founder_parity_rebuild_local",
            authorizedAt: new Date().toISOString(),
            note: "full_content_parity_rebuild_v1",
            authorizationPhrase:
              "FOUNDER_GO_STAGING_SNAPSHOT_FULL_CONTENT_REBUILD_ONLY",
          },
          sourceResearchReference: best.file,
        });
        updateResults.push({
          opportunityId: id,
          ok: true,
          publishedVersion: updated.row.publishedVersion,
        });
      } catch (err) {
        updateResults.push({
          opportunityId: id,
          ok: false,
          error: err.message,
          code: err.code,
          gate: err.gate || null,
        });
      }
    } else {
      updateResults.push({
        opportunityId: id,
        ok: false,
        error: "GATE_FAILED_BEFORE_UPDATE",
        gate,
      });
    }

    const shareDto = simulateShareFromSnapshot(enriched, classification);
    const fieldTrace = [];
    for (const field of DRAWER_FIELDS) {
      const sourceV = fieldValue(source, field);
      const leanV = fieldValue(leanSnap, field);
      const enrichedV = fieldValue(enriched, field);
      const shareV = fieldValue(shareDto, field);
      const drop = analyzeDropStage({
        leanSnap,
        source,
        enriched,
        shareDto,
        field,
      });
      // Recompute drop against lean specifically for inventory
      let exactDrop = drop.stage;
      if (comparable(sourceV) && !comparable(leanV)) {
        exactDrop = "PUBLICATION_SNAPSHOT_LEAN";
      } else if (comparable(enrichedV) && !comparable(shareV) && drop.stage === "SHARE_SANITIZE_OR_DTO") {
        exactDrop = "SHARE_SANITIZE_OR_DTO";
      } else if (comparable(shareV)) {
        exactDrop = "RESTORED_OR_PRESERVED";
      }
      const match =
        comparable(enrichedV) &&
        comparable(shareV) &&
        (comparable(enrichedV) === comparable(shareV) ||
          (typeof enrichedV === "string" &&
            typeof shareV === "string" &&
            comparable(shareV).includes(comparable(enrichedV).slice(0, 80))));
      const rowOut = {
        opportunityId: id,
        field,
        sourceValue:
          typeof sourceV === "string"
            ? sourceV.slice(0, 280)
            : sourceV,
        leanPublishedValue:
          typeof leanV === "string" ? leanV.slice(0, 280) : leanV,
        enrichedPublishedValue:
          typeof enrichedV === "string" ? enrichedV.slice(0, 280) : enrichedV,
        shareValue: typeof shareV === "string" ? shareV.slice(0, 280) : shareV,
        match: Boolean(match),
        dropStageBeforeFix: exactDrop,
        intentionalOmissionReason:
          field === "secondaryContact" && !comparable(sourceV)
            ? "Source had no approved secondary contact"
            : field === "qualificationQuestions" && !comparable(sourceV)
              ? "Source had no qualification questions"
              : null,
      };
      fieldTrace.push(rowOut);
      if (!comparable(leanV) && comparable(sourceV)) {
        missingFieldInventory.push({
          opportunityId: id,
          title: row.canonicalTitle,
          field,
          dropStage: "PUBLICATION_SNAPSHOT_LEAN",
          sourcePreview:
            typeof sourceV === "string"
              ? sourceV.slice(0, 160)
              : Array.isArray(sourceV)
                ? `array:${sourceV.length}`
                : sourceV,
        });
      }
      parityRows.push(rowOut);
    }
    writeJson(path.join(REPORT_DIR, "field-traces", `${id}.json`), {
      opportunityId: id,
      canonicalTitle: row.canonicalTitle,
      classification,
      sourceFile: best.file,
      fields: fieldTrace,
      drawerSections: shareDto.eventsDrawerModel?.sections
        ? Object.fromEntries(
            Object.entries(shareDto.eventsDrawerModel.sections).map(([k, v]) => [
              k,
              {
                body: typeof v?.body === "string" ? v.body.slice(0, 160) : null,
                facts: Array.isArray(v?.facts) ? v.facts.length : 0,
                uncertainties: Array.isArray(v?.uncertainties)
                  ? v.uncertainties.length
                  : 0,
                qualificationQuestions: Array.isArray(v?.qualificationQuestions)
                  ? v.qualificationQuestions.length
                  : 0,
                sources: Array.isArray(v?.sources) ? v.sources.length : 0,
              },
            ]),
          )
        : null,
    });

    sourceMapping.push({
      opportunityId: id,
      canonicalTitle: row.canonicalTitle,
      classification,
      sourceFile: best.file,
      sourceScore: best.score,
      mapping: {
        title: "organizationName | canonicalTitle | eventDisplayName",
        whatIsHappening:
          "whatIsHappening | summaryWhat | commercialIntelligenceBrief.whatIsHappening | companyAndLocalActivity",
        potentialHotelBusiness:
          "potentialHotelBusiness | potentialBusiness | commercialIntelligenceBrief.potentialBusinessForThisHotel",
        whyThisHotel: "whyThisHotel | summaryWhyHotel | commercialIntelligenceBrief.whyThisHotel",
        whyActNow: "whyActNow | whyNow | commercialIntelligenceBrief.whyActNow",
        establishedFacts:
          "establishedFacts | commercialIntelligenceBrief.establishedFactsAndUncertainties.facts",
        uncertainties:
          "uncertainties | commercialIntelligenceBrief.establishedFactsAndUncertainties.uncertainties",
        primaryContact: "primaryContact | commercialIntelligenceBrief.whoToContact.primary",
        secondaryContact:
          "secondaryContact | commercialIntelligenceBrief.whoToContact.secondary",
        recommendedNextStep:
          "recommendedNextStep | recommendedAction | commercialIntelligenceBrief.recommendedNextStep",
        qualificationQuestions:
          "qualificationQuestions | commercialIntelligenceBrief.qualificationQuestions",
        sources: "sources | evidenceUrls",
        commercialClassification: "registry commercialClassification (immutable for this repair)",
      },
    });
  }

  const published = registry.listPublished(HOTEL_ID);
  const classCounts = published.reduce(
    (acc, r) => {
      acc[r.commercialClassification] = (acc[r.commercialClassification] || 0) + 1;
      return acc;
    },
    {},
  );

  // Gate unit checks
  const leanFail = validateCustomerFacingPublishedSnapshot({
    id: "lean_example",
    title: "Hangers Weddings & Events",
    whatIsHappening: "Hangers Weddings & Events",
    whyThisHotel: "fit",
    recommendedNextStep: "call",
    primaryContact: { name: "Khaleen" },
    sources: [{ url: "https://example.com" }],
  });
  const hangersEnriched = gateResults.find(
    (g) => g.opportunityId === "gdi_s4912_b6d9b1929c05",
  );

  writeJson(path.join(REPORT_DIR, "01-root-cause.json"), {
    rootCause:
      "Durable published snapshots were lean (title/why/next/contact/sources only). Production loadOppDoc uses strictPublishedOnly registry merge and production has no local-shadow overlay for Spice Island IDs, so existing research was empty and lean snaps were the sole payload. Share sanitize also dropped eventsDrawerModel (while keeping scalingDrawerModel), and client/events builders did not read commercialIntelligenceBrief.establishedFactsAndUncertainties / top-level qualificationQuestions.",
    dropStages: {
      whatIsHappening: "PUBLICATION_SNAPSHOT_LEAN → share fallback substituted organization title",
      potentialHotelBusiness: "PUBLICATION_SNAPSHOT_LEAN (lived only in CIB on source)",
      whyActNow: "PUBLICATION_SNAPSHOT_LEAN",
      establishedFacts: "PUBLICATION_SNAPSHOT_LEAN (+ CIB nested path not read by share/UI)",
      uncertainties: "PUBLICATION_SNAPSHOT_LEAN",
      secondaryContact: "PUBLICATION_SNAPSHOT_LEAN",
      qualificationQuestions: "PUBLICATION_SNAPSHOT_LEAN (+ share/UI ignored CIB QQ)",
      eventsDrawerModel: "SHARE_SANITIZE_STRIP",
      commercialIntelligenceBrief: "PUBLICATION_SNAPSHOT_LEAN (not stored) + sanitize collapsed incompletely",
    },
    productionResearchDocNote:
      "railway opportunities.json does not contain the seven certified IDs; registry snaps are the only production payload for Spice Island published inventory.",
  });

  writeJson(path.join(REPORT_DIR, "02-missing-field-inventory.json"), {
    hotelId: HOTEL_ID,
    approvedCount: 7,
    researchInventoryNote:
      "Local research dashboard may show 8 including rejected Aventa; published inventory is intentionally 7.",
    missingBeforeRepair: missingFieldInventory,
    countsByField: missingFieldInventory.reduce((acc, r) => {
      acc[r.field] = (acc[r.field] || 0) + 1;
      return acc;
    }, {}),
  });

  writeJson(path.join(REPORT_DIR, "03-source-to-publication-mapping.json"), {
    contract: "gdi-customer-facing-published-snapshot-v1",
    provenance,
    rows: sourceMapping,
  });

  writeJson(path.join(REPORT_DIR, "04-seven-opportunity-parity.json"), {
    hotelId: HOTEL_ID,
    publishedCount: published.length,
    classCounts,
    expected: { total: 7, SALES_READY: 1, ACCOUNT_DEVELOPMENT: 6 },
    classReconcileOk:
      published.length === 7 &&
      classCounts.SALES_READY === 1 &&
      classCounts.ACCOUNT_DEVELOPMENT === 6,
    rows: parityRows,
    matchRate: {
      total: parityRows.length,
      matched: parityRows.filter((r) => r.match).length,
    },
  });

  writeJson(path.join(REPORT_DIR, "05-publication-gate-results.json"), {
    leanSnapshotRejected: {
      ok: leanFail.ok,
      fails: leanFail.fails,
      expectedFail: true,
    },
    hangersEnriched,
    allGateResults: gateResults,
    updateResults,
    allEnrichedPassed: gateResults.every((g) => g.ok),
    allUpdatesOk: updateResults.every((u) => u.ok),
  });

  writeJson(path.join(REPORT_DIR, "06-code-changes.json"), {
    files: [
      "lib/group-demand-intelligence/durable-publication/gdi-customer-facing-published-snapshot-v1.js",
      "lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js",
      "lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js",
      "lib/group-demand-intelligence/share/gdi-customer-facing-opportunity-contract-v1.js",
      "lib/group-demand-intelligence/events-drawer-normalized-model-v1.js",
      "public/js/group-demand-intelligence/dealality-gdi-ui.js",
      "scripts/gdi-production-publication-full-content-parity-v1.mjs",
      "scripts/test-gdi-publication-full-content-contract-v1.mjs",
    ],
  });

  writeJson(path.join(REPORT_DIR, "00-executive-verdict.json"), {
    status: "FOUNDER_GO_REQUIRED_FOR_DEPLOYMENT",
    customerReady: false,
    hotelId: HOTEL_ID,
    hotelName: "Spice Island Beach Resort",
    rootCause:
      "Lean durable snapshots + missing local-shadow on production + share sanitize dropping eventsDrawerModel",
    localRepair: {
      stagingSnapshotsEnriched: updateResults.every((u) => u.ok),
      publicationGate: gateResults.every((g) => g.ok),
      classCounts,
      preservedIds: [...ids],
    },
    requiredBeforeDeploy: [
      "Merge or pin PR #136 so GitHub auto-deploy cannot wipe registry-capable build",
      "Deploy code with full-content snapshot contract + share sanitize drawer retention",
      "Update production durable registry snapshots for the same 7 IDs (volume write / updatePublishedOpportunity) without classification changes",
      "Re-verify logged-out share drawers field-by-field",
    ],
    doNotClaimCustomerReady: true,
  });

  console.log(
    JSON.stringify(
      {
        ok: updateResults.every((u) => u.ok) && gateResults.every((g) => g.ok),
        reportDir: REPORT_DIR,
        classCounts,
        updates: updateResults,
        missingBefore: missingFieldInventory.length,
        matched: parityRows.filter((r) => r.match).length,
        totalParityCells: parityRows.length,
      },
      null,
      2,
    ),
  );
}

main();
