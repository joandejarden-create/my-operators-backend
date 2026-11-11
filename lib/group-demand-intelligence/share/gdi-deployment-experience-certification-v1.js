/**
 * Reusable GDI deployment experience certification V1.
 *
 * A deployment MUST FAIL when opportunity IDs match but customer-visible
 * commercial content or presentation differs materially from the authenticated
 * dashboard DTO path.
 */
import { toOpportunityListDto } from "../opportunity-list-dto.js";
import { countSalesOpportunityFacets } from "../sales-opportunity-quick-filters-v1.js";
import {
  buildShareOpportunityListDto,
  diffSharePresentationFields,
  SHARE_CUSTOMER_PRESENTATION_PARITY_V1,
} from "./share-customer-presentation-parity-v1.js";

export const GDI_DEPLOYMENT_EXPERIENCE_CERT_V1 =
  "gdi-deployment-experience-certification-v1";

export const CERT_AXES = Object.freeze({
  DATA_INVENTORY: "A_DATA_INVENTORY_PARITY",
  COMMERCIAL_FIELDS: "B_COMMERCIAL_FIELD_PARITY",
  DRAWER: "C_DRAWER_PARITY",
  VISUAL_COMPONENT: "D_VISUAL_COMPONENT_PARITY",
  SECURITY_CLIENT: "E_SECURITY_CLIENT_ACCESS_PARITY",
  ADP_GDI_COEXIST: "F_ADP_GDI_COEXISTENCE",
});

const SUBSTANTIVE_FIELDS = [
  "title",
  "displayTitle",
  "organizationName",
  "salesActionKey",
  "salesActionPillLabel",
  "salesActivationCategory",
  "salesActivationLabel",
  "salesOpportunityQuickFilterKeys",
  "priority",
  "priorityPillLabel",
  "tileContactName",
  "tileContactRoleLabel",
  "recommendedNextAction",
  "recommendedAction",
  "datePillDisplayLabel",
  "commercialStageKey",
  "commercialStagePillLabel",
];

function normId(v) {
  return String(v || "").trim();
}

/**
 * Compare auth list DTOs vs share list DTOs for the same canonical rows.
 * @param {object[]} opportunities canonical customer-facing opportunities
 * @param {object} opts { hotelId, legacyShareBuilder? }
 */
export function certifySharePresentationParity(opportunities = [], opts = {}) {
  const hotelId = opts.hotelId || null;
  const buildShare =
    typeof opts.shareListBuilder === "function"
      ? opts.shareListBuilder
      : (o) => buildShareOpportunityListDto(o, { hotelId });
  const buildAuth =
    typeof opts.authListBuilder === "function"
      ? opts.authListBuilder
      : (o) => toOpportunityListDto(o, { hotelId });

  const rows = [];
  let substantiveDiffCount = 0;
  let presentationDiffCount = 0;
  const authIds = [];
  const shareIds = [];

  for (const o of opportunities) {
    const auth = buildAuth(o);
    const share = buildShare(o);
    authIds.push(normId(auth.id || o.id));
    shareIds.push(normId(share.id || o.id));
    const diffs = diffSharePresentationFields(auth, share);
    const substantive = diffs.filter((d) =>
      SUBSTANTIVE_FIELDS.includes(d.field) || d.severity === "substantive",
    );
    if (substantive.length) substantiveDiffCount += 1;
    if (diffs.length) presentationDiffCount += 1;
    rows.push({
      id: normId(o.id),
      title: auth.title || o.title || null,
      substantiveDiffs: substantive,
      allDiffs: diffs,
      authSalesKeys: auth.salesOpportunityQuickFilterKeys || [],
      shareSalesKeys: share.salesOpportunityQuickFilterKeys || [],
      authSalesAction: auth.salesActionPillLabel || null,
      shareSalesAction: share.salesActionPillLabel || null,
      authContact: auth.tileContactName || null,
      shareContact: share.tileContactName || null,
    });
  }

  const authFacet = countSalesOpportunityFacets(
    opportunities.map((o) => buildAuth(o)),
  );
  const shareFacet = countSalesOpportunityFacets(
    opportunities.map((o) => buildShare(o)),
  );

  const authIdSet = new Set(authIds.filter(Boolean));
  const shareIdSet = new Set(shareIds.filter(Boolean));
  const missingOnShare = [...authIdSet].filter((id) => !shareIdSet.has(id));
  const extraOnShare = [...shareIdSet].filter((id) => !authIdSet.has(id));

  const axisA =
    missingOnShare.length === 0 &&
    extraOnShare.length === 0 &&
    authIds.length === shareIds.length;
  const axisB = substantiveDiffCount === 0;
  const facetParity =
    JSON.stringify(authFacet) === JSON.stringify(shareFacet) ||
    (authFacet.PURSUE_NOW === shareFacet.PURSUE_NOW &&
      authFacet.FUTURE_GROUP_BUSINESS === shareFacet.FUTURE_GROUP_BUSINESS &&
      authFacet.PARTNERSHIP_OPPORTUNITIES ===
        shareFacet.PARTNERSHIP_OPPORTUNITIES &&
      authFacet.LOCAL_CATERING === shareFacet.LOCAL_CATERING &&
      authFacet[""] === shareFacet[""]);

  const pass = axisA && axisB && facetParity;
  return {
    version: GDI_DEPLOYMENT_EXPERIENCE_CERT_V1,
    sharePresentationParityVersion: SHARE_CUSTOMER_PRESENTATION_PARITY_V1,
    hotelId,
    count: opportunities.length,
    axes: {
      [CERT_AXES.DATA_INVENTORY]: {
        pass: axisA,
        authCount: authIds.length,
        shareCount: shareIds.length,
        missingOnShare,
        extraOnShare,
      },
      [CERT_AXES.COMMERCIAL_FIELDS]: {
        pass: axisB && facetParity,
        substantiveDiffCount,
        presentationDiffCount,
        authFacet,
        shareFacet,
        facetParity,
      },
    },
    rows,
    pass,
    verdict: pass
      ? "PASS_EXTERNAL_EXPERIENCE_PARITY"
      : axisB
        ? "FAIL_VISUAL_OR_DRAWER_PENDING"
        : "FAIL_COMMERCIAL_CONTENT_PARITY",
  };
}

/**
 * Full multi-axis deployment gate. Visual/drawer/security axes accept
 * evidence objects supplied by the caller (browser / API probes).
 */
export function evaluateDeploymentExperienceCertificate(input = {}) {
  const presentation = input.presentationCert || null;
  const drawer = input.drawerEvidence || null;
  const visual = input.visualEvidence || null;
  const security = input.securityEvidence || null;
  const coexist = input.coexistenceEvidence || null;

  const axes = {
    [CERT_AXES.DATA_INVENTORY]: {
      pass: presentation?.axes?.[CERT_AXES.DATA_INVENTORY]?.pass === true,
      detail: presentation?.axes?.[CERT_AXES.DATA_INVENTORY] || null,
    },
    [CERT_AXES.COMMERCIAL_FIELDS]: {
      pass: presentation?.axes?.[CERT_AXES.COMMERCIAL_FIELDS]?.pass === true,
      detail: presentation?.axes?.[CERT_AXES.COMMERCIAL_FIELDS] || null,
    },
    [CERT_AXES.DRAWER]: {
      pass: drawer?.pass === true,
      detail: drawer || null,
      note:
        drawer == null
          ? "Drawer evidence required — ID-only checks are insufficient"
          : null,
    },
    [CERT_AXES.VISUAL_COMPONENT]: {
      pass: visual?.pass === true,
      detail: visual || null,
      note:
        visual == null
          ? "Visual evidence required — API-only tests are insufficient"
          : null,
    },
    [CERT_AXES.SECURITY_CLIENT]: {
      pass: security?.pass === true,
      detail: security || null,
    },
    [CERT_AXES.ADP_GDI_COEXIST]: {
      pass: coexist?.pass === true,
      detail: coexist || null,
    },
  };

  const hardFail =
    !axes[CERT_AXES.DATA_INVENTORY].pass ||
    !axes[CERT_AXES.COMMERCIAL_FIELDS].pass;
  const softPending =
    !axes[CERT_AXES.DRAWER].pass ||
    !axes[CERT_AXES.VISUAL_COMPONENT].pass ||
    !axes[CERT_AXES.SECURITY_CLIENT].pass;

  let verdict = "PASS_EXTERNAL_EXPERIENCE_PARITY_AWAITING_DEPLOYMENT_GO";
  if (hardFail) {
    verdict = !axes[CERT_AXES.COMMERCIAL_FIELDS].pass
      ? "FAIL_COMMERCIAL_CONTENT_PARITY"
      : "FAIL_COMMERCIAL_CONTENT_PARITY";
  } else if (!axes[CERT_AXES.DRAWER].pass && drawer != null) {
    verdict = "FAIL_DRAWER_PARITY";
  } else if (!axes[CERT_AXES.VISUAL_COMPONENT].pass && visual != null) {
    verdict = "FAIL_VISUAL_PARITY";
  } else if (softPending) {
    verdict = "BLOCKED";
  }

  // ID-only never upgrades to CLIENT_READY
  if (
    presentation?.axes?.[CERT_AXES.DATA_INVENTORY]?.pass &&
    !presentation?.axes?.[CERT_AXES.COMMERCIAL_FIELDS]?.pass
  ) {
    verdict = "FAIL_COMMERCIAL_CONTENT_PARITY";
  }

  return {
    version: GDI_DEPLOYMENT_EXPERIENCE_CERT_V1,
    hotelId: input.hotelId || presentation?.hotelId || null,
    axes,
    presentation,
    pass:
      Object.values(axes).every((a) => a.pass === true) &&
      !hardFail,
    verdict,
    clientReady: false,
    note: "CLIENT_READY requires founder GO after production deploy certification",
  };
}
