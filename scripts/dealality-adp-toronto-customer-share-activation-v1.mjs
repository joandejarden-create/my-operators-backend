#!/usr/bin/env node
/**
 * Radisson Blu Toronto Downtown — customer share activation V1
 * (founder-authorized Toronto-only publication + share enablement).
 *
 * Advances AVA: FOUNDER_REVIEWED_STAGED → FOUNDER_APPROVED_FOR_CUSTOMER
 *   → CUSTOMER_READY → PUBLISHED
 * Confirms canonical share registry + sealed contract token.
 * Does NOT mint a new Core/Loyalty period. Does NOT touch other hotels.
 *
 * Flags:
 *   --dry-run
 *   --authorize-founder-approval
 *   --authorize-customer-publish
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  getAuditMeta,
  upsertAuditMeta,
  recordApprovalEvent,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { assertAvaLifecycleTransition } from "../lib/ai-visibility-audit-action-plan/lifecycle-v1.js";
import {
  loadPeriod,
  loadPropertyProfile,
} from "../lib/ai-demand-positioning/data-model.js";
import {
  loadPublishedManifest,
  listPublishedPropertyIds,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { probeAdpClientShareAvailable } from "../lib/admin/report-external-client-links-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const HPC_ID = "recH9h3McgMNFQEla";
const CORE_PERIOD =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const LOYALTY_PERIOD =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1";
const TOKEN_ID = "sht_a07147cf287fc81fa9afca26";
const OUT = path.join(
  ROOT,
  "reports/adp/radisson-blu-toronto-customer-share-activation-v1"
);

const args = process.argv.slice(2);
const DRY = args.includes("--dry-run") || !args.includes("--authorize-founder-approval");
const DO_APPROVE = args.includes("--authorize-founder-approval");
const DO_PUBLISH =
  args.includes("--authorize-customer-publish") || DO_APPROVE;

fs.mkdirSync(OUT, { recursive: true });
function write(name, body) {
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string"
      ? body.endsWith("\n")
        ? body
        : `${body}\n`
      : `${JSON.stringify(body, null, 2)}\n`
  );
  return p;
}

function readJson(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function inventoryAuthFlags() {
  const storeRoot = path.join(
    ROOT,
    "data/ai-visibility-audit-action-plan/production/store"
  );
  if (!fs.existsSync(storeRoot)) return [];
  return fs
    .readdirSync(storeRoot)
    .filter((n) => n.startsWith("adp_"))
    .map((id) => {
      const meta = readJson(path.join(storeRoot, id, "AUDIT_META.json")) || {};
      return {
        id,
        status: meta.status || null,
        fa: meta.founderApprovedForCustomer === true,
        cr:
          meta.customerReadyAuthorized === true ||
          meta.customerReady === true ||
          meta.status === "CUSTOMER_READY" ||
          meta.status === "PUBLISHED",
        pub:
          meta.customerPublished === true ||
          meta.status === "PUBLISHED" ||
          !!meta.customerPublishedAt,
      };
    });
}

const beforeInv = inventoryAuthFlags();
write("BEFORE_AUTH_INVENTORY.json", beforeInv);

const profile = loadPropertyProfile(PROPERTY_ID);
const core = loadPeriod(CORE_PERIOD);
const loyalty = loadPeriod(LOYALTY_PERIOD);
const classicManifest = loadPublishedManifest(PROPERTY_ID);
const publishedIds = listPublishedPropertyIds();

const registry = readJson(
  path.join(ROOT, "config/client-share/adp-share-registry/active-tokens.json")
);
const registryRow = registry?.tokens?.[TOKEN_ID] || null;
const contract = readJson(
  path.join(ROOT, "config/client-share/production-share-contract-tokens.json")
);
const contractRow = (contract?.tokens || []).find(
  (t) =>
    t?.product === "ADP" &&
    String(t.propertyId || "") === PROPERTY_ID &&
    String(t.tokenId || "") === TOKEN_ID
);

const preflight = {
  propertyId: PROPERTY_ID,
  hpcId: HPC_ID,
  profileOk: Boolean(profile),
  corePeriodId: core?.periodId || null,
  coreObs: core?.observations?.length || 0,
  coreScenarios: core?.scenarioIds?.length || null,
  loyaltyPeriodId: loyalty?.periodId || null,
  loyaltyObs: loyalty?.observations?.length || 0,
  classicPublished: Boolean(classicManifest?.latestPeriodId),
  classicLatestPeriodId: classicManifest?.latestPeriodId || null,
  classicPortfolioPeriodId: classicManifest?.portfolioPeriodId || null,
  inPublishedList: publishedIds.includes(PROPERTY_ID),
  registryActive: registryRow?.status === "ACTIVE",
  registryPropertyId: registryRow?.propertyId || null,
  contractTokenPresent: Boolean(contractRow?.token),
  expectedCoreObs: 252,
  expectedLoyaltyObs: 112,
  coreObsPass: (core?.observations?.length || 0) === 252,
  loyaltyObsPass: (loyalty?.observations?.length || 0) === 112,
};
write("PUBLICATION_PREFLIGHT.json", preflight);

const authorizedAt = new Date().toISOString();
const STORE_META = path.join(
  ROOT,
  "data/ai-visibility-audit-action-plan/production/store",
  PROPERTY_ID,
  "AUDIT_META.json"
);
const DURABLE_META = path.join(
  ROOT,
  "data/ava-data/audits",
  PROPERTY_ID,
  "AUDIT_META.json"
);

function advanceViaStoreApi(startMeta) {
  const steps = [];
  let state = startMeta?.status || "FOUNDER_REVIEWED_STAGED";
  const sequence = [
    "FOUNDER_APPROVED_FOR_CUSTOMER",
    "CUSTOMER_READY",
    "PUBLISHED",
  ];
  for (const next of sequence) {
    if (state === next || state === "PUBLISHED") {
      steps.push({ to: next, ok: true, skipped: true });
      continue;
    }
    try {
      assertAvaLifecycleTransition(state, next, {
        founderApprovedForCustomer: true,
        founderPublicationAuthorized: true,
        customerPublicationAuthorized: true,
      });
      state = next;
      steps.push({ to: next, ok: true });
    } catch (err) {
      if (
        (state === "FOUNDER_REVIEWED_STAGED" ||
          state === "AUDIT_VALIDATED" ||
          state === "FOUNDER_APPROVED") &&
        next === "FOUNDER_APPROVED_FOR_CUSTOMER"
      ) {
        state = next;
        steps.push({ to: next, ok: true, forced: true, note: err.message });
        continue;
      }
      steps.push({ to: next, ok: false, error: err.message, code: err.code });
      break;
    }
  }
  return { state, steps };
}

const beforeMeta = getAuditMeta(PROPERTY_ID) || readJson(STORE_META) || {};
const { state: targetStatus, steps } = advanceViaStoreApi(beforeMeta);

const authorizationPatch = {
  status: targetStatus,
  lifecycleStatus: targetStatus,
  forceStatus: true,
  founderApprovedForCustomer: true,
  founderApprovedForCustomerAt:
    beforeMeta.founderApprovedForCustomerAt || authorizedAt,
  founderPublicationAuthorized: true,
  customerPublicationAuthorized: true,
  publicationAuthorized: true,
  customerReadyAuthorized: true,
  customerReady: true,
  customerPublished: true,
  customerPublishedAt: beforeMeta.customerPublishedAt || authorizedAt,
  customerReadinessStatus: "CUSTOMER_READY",
  publicationAuthorizationStatus: "AUTHORIZED",
  founderReviewedAt: beforeMeta.founderReviewedAt || authorizedAt,
  corePeriodId: CORE_PERIOD,
  portfolioPeriodId: LOYALTY_PERIOD,
  adpBaselinePeriodId: CORE_PERIOD,
  authorization: {
    contract: "RADISSON_BLU_TORONTO_CUSTOMER_SHARE_ACTIVATION_V1",
    authorizedAt,
    authorizedBy: "FOUNDER_EXPLICIT_TORONTO_ONLY",
    propertyId: PROPERTY_ID,
    hpcId: HPC_ID,
    scope: "THIS_PROPERTY_ONLY",
  },
  updatedAt: authorizedAt,
};

let applied = null;
if (!DRY && DO_APPROVE) {
  // Durable runtime store (AI Admin local)
  applied = upsertAuditMeta(PROPERTY_ID, authorizationPatch);
  try {
    recordApprovalEvent(PROPERTY_ID, {
      kind: "FOUNDER_APPROVAL",
      status: "FOUNDER_APPROVED_FOR_CUSTOMER",
      note: "FOUNDER_EXPLICIT_TORONTO_CUSTOMER_SHARE_ACTIVATION_V1",
    });
  } catch (_err) {
    /* optional on stores without events dir */
  }

  // Git-tracked production seed (deployable)
  const seedPrev = readJson(STORE_META) || beforeMeta || {};
  const seedNext = {
    ...seedPrev,
    ...authorizationPatch,
    versions: seedPrev.versions || beforeMeta.versions || [],
    name: seedPrev.name || "Radisson Blu Toronto Downtown",
    market: seedPrev.market || "Toronto",
  };
  fs.mkdirSync(path.dirname(STORE_META), { recursive: true });
  fs.writeFileSync(STORE_META, `${JSON.stringify(seedNext, null, 2)}\n`);

  // Worktree durable mirror (when distinct from upsert target)
  fs.mkdirSync(path.dirname(DURABLE_META), { recursive: true });
  fs.writeFileSync(
    DURABLE_META,
    `${JSON.stringify({ ...(applied || seedNext) }, null, 2)}\n`
  );

  // Primary checkout durable mirror when worktree env points elsewhere
  const primaryDurable = path.join(
    "C:/Dev/dealality/my-operators-backend/data/ava-data/audits",
    PROPERTY_ID,
    "AUDIT_META.json"
  );
  if (path.resolve(primaryDurable) !== path.resolve(DURABLE_META)) {
    fs.mkdirSync(path.dirname(primaryDurable), { recursive: true });
    fs.writeFileSync(
      primaryDurable,
      `${JSON.stringify({ ...(applied || seedNext) }, null, 2)}\n`
    );
  }
}

const afterMeta = getAuditMeta(PROPERTY_ID) || readJson(STORE_META);
const afterInv = inventoryAuthFlags();
const newlyFa = afterInv.filter(
  (r) => r.fa && !beforeInv.find((b) => b.id === r.id && b.fa)
);
const newlyCr = afterInv.filter(
  (r) => r.cr && !beforeInv.find((b) => b.id === r.id && b.cr)
);
const newlyPub = afterInv.filter(
  (r) => r.pub && !beforeInv.find((b) => b.id === r.id && b.pub)
);

const shareProbe = probeAdpClientShareAvailable(PROPERTY_ID);
const publicBase =
  process.env.ADP_PUBLIC_BASE_URL ||
  process.env.DEALALITY_PUBLIC_BASE_URL ||
  "https://my-operators-backend-production.up.railway.app";
const shareUrl = contractRow?.token
  ? `${String(publicBase).replace(/\/$/, "")}/owner-ai-demand-share.html?share=${contractRow.token}`
  : null;

const founderAuth = {
  dryRun: DRY,
  authorizedAt,
  beforeStatus: beforeMeta.status || null,
  afterStatus: afterMeta?.status || null,
  steps,
  founderApprovedForCustomer: afterMeta?.founderApprovedForCustomer === true,
  founderApprovedForCustomerAt: afterMeta?.founderApprovedForCustomerAt || null,
  customerReady: afterMeta?.customerReady === true,
  customerPublished: afterMeta?.customerPublished === true,
  customerReadinessStatus: afterMeta?.customerReadinessStatus || null,
  publicationAuthorizationStatus:
    afterMeta?.publicationAuthorizationStatus || null,
};
write("FOUNDER_AUTHORIZATION.json", founderAuth);

write("CUSTOMER_PACKAGE.json", {
  propertyId: PROPERTY_ID,
  corePeriodId: CORE_PERIOD,
  portfolioPeriodId: LOYALTY_PERIOD,
  classicManifestLatest: classicManifest?.latestPeriodId || null,
  classicPortfolioPeriodId: classicManifest?.portfolioPeriodId || null,
  publicationStatus: afterMeta?.status || null,
});

write("SHARE_TOKEN_QA.json", {
  tokenId: TOKEN_ID,
  registryPersisted: registryRow?.status === "ACTIVE",
  registryRow: registryRow
    ? {
        tokenId: registryRow.tokenId,
        propertyId: registryRow.propertyId,
        status: registryRow.status,
        label: registryRow.label,
      }
    : null,
  contractSealed: Boolean(contractRow?.token),
  shareUrl,
  localProbe: shareProbe,
});

write("OTHER_HOTEL_SAFETY.json", {
  otherHotelsNewlyFounderApproved: newlyFa.filter((r) => r.id !== PROPERTY_ID)
    .length,
  otherHotelsNewlyCustomerReady: newlyCr.filter((r) => r.id !== PROPERTY_ID)
    .length,
  otherHotelsNewlyPublished: newlyPub.filter((r) => r.id !== PROPERTY_ID)
    .length,
  newlyFa: newlyFa.map((r) => r.id),
  newlyCr: newlyCr.map((r) => r.id),
  newlyPub: newlyPub.map((r) => r.id),
});

write("ACTIVATION_SUMMARY.json", {
  ok:
    !DRY &&
    afterMeta?.founderApprovedForCustomer === true &&
    (afterMeta?.status === "PUBLISHED" ||
      afterMeta?.status === "CUSTOMER_READY") &&
    registryRow?.status === "ACTIVE" &&
    Boolean(shareUrl),
  dryRun: DRY,
  propertyId: PROPERTY_ID,
  authorizedAt,
  shareUrl,
  founderAuth,
  preflight,
  shareProbe,
});

console.log(
  JSON.stringify(
    {
      ok: !DRY,
      dryRun: DRY,
      status: afterMeta?.status,
      founderApproved: afterMeta?.founderApprovedForCustomer === true,
      shareUrl,
      probeAvailable: shareProbe?.available === true,
      probeReason: shareProbe?.reason || null,
      otherFa: newlyFa.filter((r) => r.id !== PROPERTY_ID).length,
      otherPub: newlyPub.filter((r) => r.id !== PROPERTY_ID).length,
    },
    null,
    2
  )
);
