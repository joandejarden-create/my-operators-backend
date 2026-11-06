/**
 * GDI_ONE_CLICK_HOTEL_PUBLISH_V1
 *
 * Permanent atomic hotel publication operation.
 *
 * Local display-eligible opportunities → full-content snapshots →
 * durable registry write → hotel publication pointer advance → verify.
 *
 * No lean publication. No independent share recomputation of approved content.
 * Presentation-only UI changes do not require republish.
 *
 * Does not deploy Railway or write Airtable. Caller controls dryRun vs apply.
 */
import { createHash } from "node:crypto";
import {
  createGdiPublishedInventoryRegistry,
  COMMERCIAL_PUBLICATION_CLASS,
} from "./gdi-published-inventory-registry-v1.js";
import { resolveGdiPublishedInventoryRoot } from "./resolve-gdi-published-registry-root-v1.js";
import {
  preparePublicationSnapshot,
  GDI_PUBLICATION_CONTRACT_V1,
} from "../contracts/gdi-publication-contract-v1.js";
import {
  selectDisplayEligibleOpportunities,
  buildPublicationManifest,
  assertManifestParity,
  GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1,
} from "../commercial-inventory/gdi-canonical-display-eligibility-contract-v1.js";
import {
  GDI_PRESENTATION_CONTRACT_V1,
  GDI_PRESENTATION_SURFACES,
} from "../contracts/gdi-presentation-contract-v1.js";
import { projectOpportunityForPresentation } from "../presentation/gdi-universal-presentation-pipeline-v1.js";

export const GDI_ONE_CLICK_HOTEL_PUBLISH_V1 = "gdi-one-click-hotel-publish-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function nowIso() {
  return new Date().toISOString();
}

function hashJson(value) {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function resolveClassification(opp = {}, registryRow = null) {
  const c = upper(
    opp.commercialInventoryClass ||
      opp.commercialClassification ||
      opp.publicationClass ||
      registryRow?.commercialClassification ||
      "",
  );
  if (c === COMMERCIAL_PUBLICATION_CLASS.SALES_READY) {
    return COMMERCIAL_PUBLICATION_CLASS.SALES_READY;
  }
  if (c === COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT) {
    return COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT;
  }
  return null;
}

/**
 * Freeze customer-facing presentation fields onto the snapshot so share/auth
 * never recomputes classifications or actions after publish.
 */
export function freezePublishedPresentationFields(opportunity = {}, snapshot = {}) {
  const classification =
    resolveClassification(opportunity) ||
    upper(snapshot.commercialInventoryClass || snapshot.commercialClassification) ||
    null;
  const primary = opportunity.primaryContact || snapshot.primaryContact || null;
  const secondary =
    opportunity.secondaryContact || snapshot.secondaryContact || null;
  const quickFilters = Array.isArray(opportunity.salesOpportunityQuickFilterKeys)
    ? opportunity.salesOpportunityQuickFilterKeys
    : Array.isArray(snapshot.salesOpportunityQuickFilterKeys)
      ? snapshot.salesOpportunityQuickFilterKeys
      : null;
  return {
    ...snapshot,
    commercialInventoryClass: classification || snapshot.commercialInventoryClass || null,
    commercialClassification: classification || snapshot.commercialClassification || null,
    publishedPresentationLocked: true,
    salesActionKey:
      opportunity.salesActionKey || snapshot.salesActionKey || null,
    salesActionPillLabel:
      opportunity.salesActionPillLabel || snapshot.salesActionPillLabel || null,
    priorityFilterKey:
      opportunity.priorityFilterKey || snapshot.priorityFilterKey || null,
    // Keep storage priority identical to the approved customer filter key when
    // present so lean production hydration cannot re-label Medium as Watch.
    priority:
      opportunity.priorityFilterKey ||
      opportunity.priority ||
      snapshot.priorityFilterKey ||
      snapshot.priority ||
      null,
    priorityPillLabel:
      opportunity.priorityPillLabel || snapshot.priorityPillLabel || null,
    salesActivationCategory:
      opportunity.salesActivationCategory ||
      snapshot.salesActivationCategory ||
      null,
    salesActivationLabel:
      opportunity.salesActivationLabel || snapshot.salesActivationLabel || null,
    // Standalone production share must not depend on Airtable merge to recover
    // tile contact, verification gates, or Sales Opportunity filter membership.
    hasVerifiedNamedRelevantContact:
      opportunity.hasVerifiedNamedRelevantContact === true ||
      snapshot.hasVerifiedNamedRelevantContact === true ||
      false,
    inventorySalesReady:
      opportunity.inventorySalesReady === true ||
      classification === COMMERCIAL_PUBLICATION_CLASS.SALES_READY ||
      snapshot.inventorySalesReady === true ||
      false,
    accountDevelopmentPublished:
      opportunity.accountDevelopmentPublished === true ||
      classification === COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT ||
      snapshot.accountDevelopmentPublished === true ||
      false,
    tileContactName:
      text(opportunity.tileContactName) ||
      text(primary?.name) ||
      text(snapshot.tileContactName) ||
      null,
    tileContactRoleLabel:
      text(opportunity.tileContactRoleLabel) ||
      text(primary?.role || primary?.title) ||
      text(snapshot.tileContactRoleLabel) ||
      null,
    primaryContact: primary || null,
    secondaryContact: secondary || null,
    salesOpportunityQuickFilterKeys: quickFilters,
    commercialStageKey:
      opportunity.commercialStageKey || snapshot.commercialStageKey || null,
    opportunityTypeFilterCategory:
      opportunity.opportunityTypeFilterCategory ||
      snapshot.opportunityTypeFilterCategory ||
      null,
    opportunityTypeFilterLabel:
      opportunity.opportunityTypeFilterLabel ||
      snapshot.opportunityTypeFilterLabel ||
      null,
  };
}

/**
 * Prepare full-content publish candidates from an approved local opportunity set.
 * Fail closed — any lean/silent-loss gate failure blocks the hotel publish.
 */
export function prepareHotelPublicationCandidates(
  hotelId,
  opportunities = [],
  opts = {},
) {
  const display = selectDisplayEligibleOpportunities(hotelId, opportunities, opts);
  const rows = Array.isArray(opts.opportunityIds)
    ? display.opportunities.filter((o) =>
        opts.opportunityIds.map(String).includes(String(o.id)),
      )
    : display.opportunities;

  if (
    Array.isArray(opts.opportunityIds) &&
    opts.opportunityIds.length &&
    rows.length !== opts.opportunityIds.length
  ) {
    const have = new Set(rows.map((o) => String(o.id)));
    const missing = opts.opportunityIds
      .map(String)
      .filter((id) => !have.has(id));
    return {
      ok: false,
      code: "APPROVED_IDS_NOT_IN_DISPLAY_SET",
      missing,
      display,
      candidates: [],
    };
  }

  const candidates = [];
  const failures = [];
  for (const opp of rows) {
    // Preserve the approved local presentation title. Do not let the snapshot
    // builder collapse event titles into organizationName.
    const approvedTitle =
      text(opp.title) ||
      text(opp.displayTitle) ||
      text(opp.canonicalTitle) ||
      null;
    // Project the same customer classification the local UI shows so freeze
    // captures Contact Buyer / Medium / quick-filter membership, not nulls.
    let presentationOpp = opp;
    try {
      const projected = projectOpportunityForPresentation(opp, {
        hotelId,
        surface: GDI_PRESENTATION_SURFACES.LOCAL_AUTH,
        view: "list",
      });
      if (projected?.dto) {
        presentationOpp = { ...opp, ...projected.dto, id: opp.id, hotelId };
      }
    } catch {
      presentationOpp = opp;
    }
    const prepared = preparePublicationSnapshot(presentationOpp, {
      hotelId,
      classification: resolveClassification(presentationOpp),
      canonicalTitle: approvedTitle,
    });
    if (!prepared.ok || prepared.silentLossBlocked) {
      failures.push({
        opportunityId: String(opp.id || ""),
        fails: prepared.fails || [],
        silentLoss: prepared.silentLoss || [],
      });
      continue;
    }
    const frozen = freezePublishedPresentationFields(
      presentationOpp,
      prepared.snapshot,
    );
    if (!resolveClassification(frozen)) {
      failures.push({
        opportunityId: String(opp.id || ""),
        fails: ["commercialClassification:missing_or_unauthorized"],
        silentLoss: [],
      });
      continue;
    }
    candidates.push({
      opportunityId: String(opp.id),
      classification: resolveClassification(frozen),
      snapshot: frozen,
      contentHash: hashJson(frozen),
    });
  }

  const manifest = buildPublicationManifest(
    hotelId,
    candidates.map((c) => c.snapshot),
    opts,
  );

  return {
    ok: failures.length === 0 && candidates.length > 0 && manifest.ok,
    contractVersion: GDI_ONE_CLICK_HOTEL_PUBLISH_V1,
    publicationContract: GDI_PUBLICATION_CONTRACT_V1,
    displayEligibilityContract: GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1,
    presentationContract: GDI_PRESENTATION_CONTRACT_V1,
    hotelId: String(hotelId || ""),
    display,
    candidates,
    failures,
    manifest,
  };
}

/**
 * Atomic one-click hotel publish.
 *
 * @param {object} input
 * @param {string} input.hotelId
 * @param {object[]} input.opportunities — approved local set (already display-eligible preferred)
 * @param {object} input.approval — founder approval block
 * @param {boolean} [input.dryRun=true]
 * @param {string[]} [input.opportunityIds] — optional exact ID allowlist
 * @param {object} [input.registry]
 * @param {string} [input.root]
 */
export function publishHotelInventoryOneClick(input = {}) {
  const hotelId = text(input.hotelId);
  if (!hotelId) {
    return { ok: false, code: "HOTEL_ID_REQUIRED" };
  }
  const dryRun = input.dryRun !== false; // default safe
  const approval = input.approval || null;
  const prepared = prepareHotelPublicationCandidates(
    hotelId,
    input.opportunities || [],
    {
      opportunityIds: input.opportunityIds,
      env: input.env,
      root: input.root,
      registry: input.registry,
    },
  );
  if (!prepared.ok) {
    return {
      ok: false,
      code: prepared.code || "PUBLICATION_PREPARE_FAILED",
      dryRun,
      prepared,
    };
  }

  const publicationId = `gdi_pub_${hotelId}_${Date.now().toString(36)}_${hashJson(
    prepared.manifest.manifest,
  ).slice(0, 8)}`;

  const plan = {
    hotelId,
    publicationId,
    dryRun,
    opportunityCount: prepared.candidates.length,
    opportunityIds: prepared.candidates.map((c) => c.opportunityId).sort(),
    classificationCounts: prepared.manifest.manifest.classificationCounts,
    manifestHash: prepared.manifest.manifest.manifestHash,
    presentationSchemaVersion: GDI_PRESENTATION_CONTRACT_V1,
  };

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      advanced: false,
      plan,
      prepared,
      message:
        "Dry-run only — durable registry not written. Pass dryRun:false with founder approval to apply.",
    };
  }

  if (!approval || approval.authorized !== true) {
    return {
      ok: false,
      code: "FOUNDER_APPROVAL_REQUIRED",
      dryRun: false,
      plan,
      prepared,
    };
  }

  const registry =
    input.registry ||
    createGdiPublishedInventoryRegistry({
      env: input.env || process.env,
      root: input.root || resolveGdiPublishedInventoryRoot(input.env || process.env),
    });

  if (typeof registry.publishHotelInventoryBatch !== "function") {
    return {
      ok: false,
      code: "REGISTRY_BATCH_PUBLISH_UNAVAILABLE",
      plan,
      prepared,
    };
  }

  const priorPointer =
    registry.loadRegistry()?.hotels?.[hotelId]?.currentPublication || null;
  const pointer = {
    publicationId,
    publishedAt: nowIso(),
    opportunityIds: plan.opportunityIds,
    opportunityCount: plan.opportunityCount,
    classificationCounts: plan.classificationCounts,
    manifestHash: plan.manifestHash,
    presentationSchemaVersion: GDI_PRESENTATION_CONTRACT_V1,
    publicationContractVersion: GDI_PUBLICATION_CONTRACT_V1,
    oneClickPublishVersion: GDI_ONE_CLICK_HOTEL_PUBLISH_V1,
    previousPublicationId: priorPointer?.publicationId || null,
    approval: {
      authorizedBy: approval.authorizedBy || null,
      authorizedAt: approval.authorizedAt || nowIso(),
      authorizationPhrase: approval.authorizationPhrase || null,
    },
  };

  let batch;
  let verify = null;
  try {
    batch = registry.publishHotelInventoryBatch({
      hotelId,
      approval,
      currentPublication: pointer,
      revokeMissingPublishedIds: input.revokeMissingPublishedIds === true,
      revokeReason: input.revokeReason || "one_click_publish_not_in_approved_set",
      items: prepared.candidates.map((c) => ({
        opportunityId: c.opportunityId,
        classification: c.classification,
        snapshot: c.snapshot,
        sourceResearchReference:
          text(input.sourceResearchReference) ||
          `one-click-publish:${publicationId}`,
      })),
      // Verify staged snapshot files before registry/pointer promotion.
      // Failed verify leaves registry unchanged — no rollback crash window.
      verifyBeforeCommit: ({ stagedRows }) => {
        verify = assertManifestParity(prepared.manifest.manifest, stagedRows, {
          hotelId,
        });
        return verify;
      },
    });
  } catch (err) {
    return {
      ok: false,
      code: err.code || "PUBLICATION_BATCH_FAILED",
      message: String(err.message || err),
      dryRun: false,
      plan,
      gate: err.gate || null,
      verify: err.verify || verify,
      stagedOrphanSnapshots: err.stagedWrites || null,
      pointerAdvanced: false,
      previousPublicationId: priorPointer?.publicationId || null,
    };
  }

  return {
    ok: true,
    dryRun: false,
    advanced: true,
    plan,
    pointer,
    writes: batch.writes,
    revoked: batch.revoked || [],
    verify: verify || { ok: true },
    verifiedBeforePointer: batch.verifiedBeforePointer === true,
    pointerAdvanced: true,
    previousPublicationId: priorPointer?.publicationId || null,
  };
}
