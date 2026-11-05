/**
 * GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1
 *
 * ONE opportunity list for local authenticated GDI and customer publication.
 * Display eligibility === publication eligibility. No dual customer/research UI.
 *
 * Research ledgers remain in backend storage for investigation; they are not
 * a second inventory surface in the product UI.
 *
 * Hotel-agnostic. No hard-coded opportunity-count ceilings.
 * Does not write Airtable, deploy, or mutate production volumes.
 */
import { createHash } from "node:crypto";
import {
  GDI_UNIFIED_COMMERCIAL_INVENTORY_CONTRACT_V1,
  selectCommercialInventoryView,
  getPublishedCustomerOpportunityIds,
  loadCommercialInventoryExclusions,
  isHistoricalOnlyCustomerExclusion,
  COMMERCIAL_INVENTORY_CLASS,
} from "./gdi-unified-commercial-inventory-contract-v1.js";
import {
  COMMERCIAL_PUBLICATION_CLASS,
  createGdiPublishedInventoryRegistry,
} from "../durable-publication/gdi-published-inventory-registry-v1.js";
import { resolveGdiPublishedInventoryRoot } from "../durable-publication/resolve-gdi-published-registry-root-v1.js";
import {
  preparePublicationSnapshot,
  GDI_PUBLICATION_CONTRACT_V1,
} from "../contracts/gdi-publication-contract-v1.js";
import { GDI_PRESENTATION_CONTRACT_V1 } from "../contracts/gdi-presentation-contract-v1.js";

export const GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1 =
  "gdi-canonical-display-eligibility-contract-v1";

export const GDI_PUBLICATION_MANIFEST_SCHEMA_V1 =
  "gdi-publication-manifest-schema-v1";

const CONTENT_HASH_FIELDS = Object.freeze([
  "id",
  "displayTitle",
  "drawerHeaderTitle",
  "title",
  "organizationName",
  "priority",
  "priorityPillLabel",
  "commercialInventoryClass",
  "commercialClassification",
  "salesActivationCategory",
  "salesActionPillLabel",
  "summaryWhat",
  "summaryWhyMatters",
  "whatIsHappening",
  "potentialBusinessForThisHotel",
  "whyThisHotel",
  "whyActNow",
  "establishedFacts",
  "uncertainties",
  "recommendedNextStep",
  "recommendedNextAction",
  "qualificationQuestions",
  "primaryContact",
  "secondaryContact",
  "contacts",
  "sources",
  "supportingEvidence",
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function stableStringify(value) {
  if (value == null) return "null";
  if (typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map((v) => stableStringify(v)).join(",")}]`;
  }
  const keys = Object.keys(value).sort();
  return `{${keys
    .map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`)
    .join(",")}}`;
}

function hashPayload(payload) {
  return createHash("sha256").update(stableStringify(payload)).digest("hex");
}

function pickContentSlice(opp = {}) {
  const slice = {};
  for (const key of CONTENT_HASH_FIELDS) {
    if (opp[key] !== undefined) slice[key] = opp[key];
  }
  // Drawer narrative aliases commonly used by presentation DTOs
  if (opp.eventsOpportunityOverview) {
    slice.eventsOpportunityOverview = {
      drawerHeaderTitle: opp.eventsOpportunityOverview.drawerHeaderTitle,
      whatIsHappening: opp.eventsOpportunityOverview.whatIsHappening,
      potentialBusinessForThisHotel:
        opp.eventsOpportunityOverview.potentialBusinessForThisHotel,
      whyThisHotel: opp.eventsOpportunityOverview.whyThisHotel,
      whyActNow: opp.eventsOpportunityOverview.whyActNow,
    };
  }
  return slice;
}

function loadRegistryClassById(hotelId, opts = {}) {
  const map = new Map();
  const id = text(hotelId);
  if (!id) return map;
  try {
    const registry =
      opts.registry ||
      createGdiPublishedInventoryRegistry({
        env: opts.env || process.env,
        root: opts.root || resolveGdiPublishedInventoryRoot(opts.env || process.env),
      });
    for (const row of registry.listPublished(id)) {
      if (row?.opportunityId) {
        map.set(
          String(row.opportunityId),
          upper(row.commercialClassification || ""),
        );
      }
    }
  } catch {
    /* registry optional for hotels without published set */
  }
  return map;
}

function classCountsFor(rows = [], opts = {}) {
  const registryClass = opts.hotelId
    ? loadRegistryClassById(opts.hotelId, opts)
    : new Map();
  return rows.reduce(
    (acc, o) => {
      const oid = String(o.id || o.opportunityId || "");
      const c = upper(
        o.commercialInventoryClass ||
          o.commercialClassification ||
          o.publicationClass ||
          registryClass.get(oid) ||
          "",
      );
      if (
        c === COMMERCIAL_PUBLICATION_CLASS.SALES_READY ||
        c === COMMERCIAL_INVENTORY_CLASS.SALES_READY
      ) {
        acc.SALES_READY += 1;
      } else if (
        c === COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT ||
        c === COMMERCIAL_INVENTORY_CLASS.ACCOUNT_DEVELOPMENT
      ) {
        acc.ACCOUNT_DEVELOPMENT += 1;
      } else {
        acc.OTHER += 1;
      }
      return acc;
    },
    { SALES_READY: 0, ACCOUNT_DEVELOPMENT: 0, OTHER: 0 },
  );
}

/**
 * Canonical display eligibility — the ONLY user-facing opportunity list.
 * Identical selection used for customer publication preparation.
 */
export function selectDisplayEligibleOpportunities(
  hotelId,
  opportunities = [],
  opts = {},
) {
  const selected = selectCommercialInventoryView(hotelId, opportunities, {
    ...opts,
    inventoryView: "customer",
  });
  const ids = selected.opportunities
    .map((o) => String(o.id || ""))
    .filter(Boolean)
    .sort();
  const classCounts = classCountsFor(selected.opportunities, {
    ...opts,
    hotelId,
  });
  return {
    contractVersion: GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1,
    unifiedContractVersion: GDI_UNIFIED_COMMERCIAL_INVENTORY_CONTRACT_V1,
    hotelId,
    surface: "canonical_display",
    inventoryViewLabel: "Opportunities",
    opportunities: selected.opportunities,
    opportunityIds: ids,
    count: selected.count,
    publishedStrict: selected.publishedStrict === true,
    publishedIds: selected.publishedIds || [],
    classCounts,
    excludedFromDisplay: selected.excludedFromCustomer || [],
  };
}

/**
 * Publication must reuse display eligibility — never recalculate independently.
 */
export function selectPublicationEligibleOpportunities(
  hotelId,
  opportunities = [],
  opts = {},
) {
  const display = selectDisplayEligibleOpportunities(
    hotelId,
    opportunities,
    opts,
  );
  return {
    ...display,
    surface: "customer_publication",
    sameAsDisplayEligibility: true,
  };
}

/**
 * Deterministic publication manifest from the approved local display set.
 */
export function buildPublicationManifest(hotelId, opportunities = [], opts = {}) {
  const display = selectDisplayEligibleOpportunities(
    hotelId,
    opportunities,
    opts,
  );
  const rows = display.opportunities;
  const registryClass = loadRegistryClassById(hotelId, opts);
  const opportunityEntries = rows.map((o) => {
    const pub = preparePublicationSnapshot(o, { hotelId });
    const contentSlice = pickContentSlice(pub.snapshot || o);
    const oid = String(o.id || "");
    return {
      opportunityId: oid,
      publicationOk: pub.ok === true,
      publicationErrors: pub.ok ? [] : pub.errors || [],
      contentHash: hashPayload(contentSlice),
      classification: upper(
        o.commercialInventoryClass ||
          o.commercialClassification ||
          o.publicationClass ||
          registryClass.get(oid) ||
          "",
      ),
      title:
        o.displayTitle ||
        o.drawerHeaderTitle ||
        o.title ||
        o.organizationName ||
        "",
    };
  });
  const opportunityIds = opportunityEntries
    .map((e) => e.opportunityId)
    .filter(Boolean)
    .sort();
  const manifestBody = {
    hotelId: String(hotelId || ""),
    opportunityIds,
    opportunityCount: opportunityIds.length,
    classificationCounts: classCountsFor(rows, { ...opts, hotelId }),
    opportunityContentHashes: Object.fromEntries(
      opportunityEntries.map((e) => [e.opportunityId, e.contentHash]),
    ),
    presentationSchemaVersion: GDI_PRESENTATION_CONTRACT_V1,
    publicationContractVersion: GDI_PUBLICATION_CONTRACT_V1,
    displayEligibilityContractVersion:
      GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1,
    manifestSchemaVersion: GDI_PUBLICATION_MANIFEST_SCHEMA_V1,
  };
  return {
    ok: opportunityEntries.every((e) => e.publicationOk),
    manifest: {
      ...manifestBody,
      manifestHash: hashPayload(manifestBody),
    },
    opportunityEntries,
    display,
  };
}

/**
 * Compare an actual payload (published or share) against an approved manifest.
 * Fails on ID / count / classification / content-hash / required-field drift.
 */
export function assertManifestParity(approvedManifest, actualOpportunities = [], opts = {}) {
  const hotelId = text(opts.hotelId || approvedManifest?.hotelId);
  const rebuilt = buildPublicationManifest(hotelId, actualOpportunities, opts);
  const expected = approvedManifest?.manifest || approvedManifest || {};
  const actual = rebuilt.manifest;
  const diffs = [];

  if (String(expected.hotelId || "") !== String(actual.hotelId || "")) {
    diffs.push({
      kind: "hotelId",
      expected: expected.hotelId,
      actual: actual.hotelId,
    });
  }
  if (Number(expected.opportunityCount) !== Number(actual.opportunityCount)) {
    diffs.push({
      kind: "opportunityCount",
      expected: expected.opportunityCount,
      actual: actual.opportunityCount,
    });
  }
  const expIds = [...(expected.opportunityIds || [])].map(String).sort();
  const actIds = [...(actual.opportunityIds || [])].map(String).sort();
  if (JSON.stringify(expIds) !== JSON.stringify(actIds)) {
    diffs.push({
      kind: "opportunityIds",
      expectedOnly: expIds.filter((id) => !actIds.includes(id)),
      actualOnly: actIds.filter((id) => !expIds.includes(id)),
    });
  }
  const expClass = expected.classificationCounts || {};
  const actClass = actual.classificationCounts || {};
  for (const key of ["SALES_READY", "ACCOUNT_DEVELOPMENT", "OTHER"]) {
    if (Number(expClass[key] || 0) !== Number(actClass[key] || 0)) {
      diffs.push({
        kind: "classificationCounts",
        key,
        expected: expClass[key] || 0,
        actual: actClass[key] || 0,
      });
    }
  }
  const expHashes = expected.opportunityContentHashes || {};
  const actHashes = actual.opportunityContentHashes || {};
  for (const id of expIds) {
    if (expHashes[id] && actHashes[id] && expHashes[id] !== actHashes[id]) {
      diffs.push({
        kind: "contentHash",
        opportunityId: id,
        expected: expHashes[id],
        actual: actHashes[id],
      });
    }
  }
  if (
    expected.presentationSchemaVersion &&
    expected.presentationSchemaVersion !== actual.presentationSchemaVersion
  ) {
    diffs.push({
      kind: "presentationSchemaVersion",
      expected: expected.presentationSchemaVersion,
      actual: actual.presentationSchemaVersion,
    });
  }

  const publicationFailures = rebuilt.opportunityEntries.filter(
    (e) => !e.publicationOk,
  );
  for (const fail of publicationFailures) {
    diffs.push({
      kind: "publicationContentGate",
      opportunityId: fail.opportunityId,
      errors: fail.publicationErrors,
    });
  }

  return {
    ok: diffs.length === 0,
    contractVersion: GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1,
    hotelId,
    diffs,
    expectedManifestHash: expected.manifestHash || null,
    actualManifestHash: actual.manifestHash || null,
    rebuilt,
  };
}

/**
 * Report local display vs published registry without mutating either side.
 */
export function reportLocalPublishedInventoryDiff(hotelId, opportunities = [], opts = {}) {
  const display = selectDisplayEligibleOpportunities(
    hotelId,
    opportunities,
    opts,
  );
  const publishedIds = getPublishedCustomerOpportunityIds(hotelId, opts)
    .map(String)
    .sort();
  const displayIds = display.opportunityIds.slice().sort();
  const localOnly = displayIds.filter((id) => !publishedIds.includes(id));
  const publishedOnly = publishedIds.filter((id) => !displayIds.includes(id));
  const exclusions = loadCommercialInventoryExclusions(hotelId, opts);
  return {
    hotelId: String(hotelId || ""),
    localDisplayCount: displayIds.length,
    publishedRegistryCount: publishedIds.length,
    match:
      localOnly.length === 0 &&
      publishedOnly.length === 0 &&
      displayIds.length === publishedIds.length,
    localOnly,
    publishedOnly,
    classCounts: display.classCounts,
    publishedStrict: display.publishedStrict,
    durableExclusions: exclusions.exclusions || [],
    proposedReconciliation:
      localOnly.length || publishedOnly.length
        ? {
            doNotSilentlyDeleteOrPublish: true,
            localOnlyAction:
              "Founder review — publish only if commercially approved",
            publishedOnlyAction:
              "Founder review — restore local snapshot or unpublish with authorization",
          }
        : null,
  };
}

export function isExcludedFromCanonicalDisplay(opp = {}, opts = {}) {
  return isHistoricalOnlyCustomerExclusion(opp, opts);
}

export {
  getPublishedCustomerOpportunityIds,
  loadCommercialInventoryExclusions,
  COMMERCIAL_INVENTORY_CLASS,
};
