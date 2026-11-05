import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  buildCanonicalPresentationObject,
  GDI_CANONICAL_OPPORTUNITY_CONTRACT_V1,
} from "../lib/group-demand-intelligence/contracts/gdi-canonical-opportunity-contract-v1.js";
import {
  GDI_PRESENTATION_SURFACES,
  certifyPresentationParity,
} from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import { preparePublicationSnapshot } from "../lib/group-demand-intelligence/contracts/gdi-publication-contract-v1.js";
import { freezePublishedPresentationFields } from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const HANGERS_ID = "gdi_s4912_b6d9b1929c05";
const HOTEL = "recKRJjcPnb4tVDDS";
const OUT = path.join(
  ROOT,
  "reports/gdi-universal-presentation-publication-parity-v1"
);
const snapPath = path.join(
  ROOT,
  "reports/gdi-production-publication-full-content-parity-v1/enriched-snapshots",
  `${HANGERS_ID}.json`
);

function normalizeSources(sources) {
  return (Array.isArray(sources) ? sources : [])
    .map((row) => ({
      url: row?.url || null,
      title: row?.title || row?.name || null,
    }))
    .sort((a, b) => String(a.url || a.title || "").localeCompare(String(b.url || b.title || "")));
}
function hashApproved(obj) {
  const pick = {
    title: obj?.title ?? null,
    whatIsHappening: obj?.whatIsHappening ?? null,
    potentialHotelBusiness: obj?.potentialHotelBusiness || obj?.potentialBusiness || null,
    whyThisHotel: obj?.whyThisHotel ?? null,
    whyActNow: obj?.whyActNow ?? null,
    establishedFacts: obj?.establishedFacts ?? null,
    uncertainties: obj?.uncertainties ?? null,
    primaryContact: obj?.primaryContact?.name ?? null,
    secondaryContact: obj?.secondaryContact?.name ?? null,
    recommendedNextStep: obj?.recommendedNextStep ?? null,
    qualificationQuestions: obj?.qualificationQuestions ?? null,
    sources: normalizeSources(obj?.sources),
    commercialInventoryClass: obj?.commercialInventoryClass ?? null,
  };
  return createHash("sha256").update(JSON.stringify(pick)).digest("hex");
}

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

const raw = JSON.parse(fs.readFileSync(snapPath, "utf8"));
const canonical = buildCanonicalPresentationObject(raw, {
  hotelId: HOTEL,
  lock: true,
});
const pub = preparePublicationSnapshot(raw, {
  hotelId: HOTEL,
  classification: "ACCOUNT_DEVELOPMENT",
  canonicalTitle: "Hangers Weddings & Events",
});
const frozen = freezePublishedPresentationFields(raw, pub.snapshot);

check("hangers_canonical_object", () => {
  assert.equal(canonical.gate.ok, true, canonical.gate.fails.join(","));
  assert.equal(canonical.identity.opportunityId, HANGERS_ID);
  assert.equal(canonical.identity.canonicalTitle, "Hangers Weddings & Events");
  assert.equal(canonical.commercialClassification.inventoryClass, "ACCOUNT_DEVELOPMENT");
  assert.equal(canonical.drawer.primaryContact?.name, "Khaleen Johnson");
  assert.equal(canonical.drawer.secondaryContacts?.[0]?.name, "Aviann Murray");
  assert.ok((canonical.drawer.establishedFacts || []).length >= 5);
  assert.ok((canonical.drawer.uncertainties || []).length >= 3);
  assert.ok((canonical.drawer.qualificationQuestions || []).length >= 5);
  assert.ok((canonical.drawer.supportingEvidence || []).length >= 3);
  assert.ok(String(canonical.drawer.whatIsHappening || "").length > 40);
  assert.ok(String(canonical.drawer.potentialBusinessForThisHotel || "").length > 40);
  assert.ok(String(canonical.drawer.whyThisHotel || "").length > 20);
  assert.ok(String(canonical.drawer.whyActNow || "").length > 20);
  assert.ok(String(canonical.drawer.recommendedNextStep || "").length > 10);
});

check("hangers_publish_freeze_no_rewrite", () => {
  assert.equal(pub.ok, true, pub.fails.join(","));
  assert.equal(frozen.publishedPresentationLocked, true);
  assert.equal(frozen.title, "Hangers Weddings & Events");
  assert.equal(frozen.primaryContact?.name, "Khaleen Johnson");
  assert.equal(frozen.secondaryContact?.name, "Aviann Murray");
});

check("hangers_same_object_auth_share_pipeline", () => {
  const locked = { ...frozen, publishedPresentationLocked: true };
  const auth = projectOpportunityForPresentation(locked, {
    surface: GDI_PRESENTATION_SURFACES.LOCAL_AUTH,
    hotelId: HOTEL,
    view: "detail",
    forceLocked: true,
  });
  const share = projectOpportunityForPresentation(locked, {
    surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
    hotelId: HOTEL,
    view: "detail",
    forceLocked: true,
    shareResolve: { capabilities: ["CAN_READ_OPPORTUNITIES", "CAN_READ_OPPORTUNITY_DETAIL"] },
  });
  assert.equal(auth.locked, true);
  assert.equal(share.locked, true);
  assert.equal(auth.dto.title, "Hangers Weddings & Events");
  assert.equal(share.dto.title, "Hangers Weddings & Events");
  assert.equal(auth.dto.primaryContact?.name, "Khaleen Johnson");
  assert.equal(share.dto.primaryContact?.name, "Khaleen Johnson");
  assert.equal(auth.dto.secondaryContact?.name, "Aviann Murray");
  assert.equal(share.dto.secondaryContact?.name, "Aviann Murray");
  assert.equal(auth.capabilities.canRunResearch, true);
  assert.equal(share.capabilities.canRunResearch, false);
  assert.equal(share.dto.mode, "read_only");

  const aHash = hashApproved(auth.dto);
  const sHash = hashApproved(share.dto);
  assert.equal(aHash, sHash, "approved commercial hash must match auth↔share");

  const cert = certifyPresentationParity(locked, { hotelId: HOTEL });
  assert.equal(cert.ok, true, JSON.stringify(cert.fieldDiffs.slice(0, 8)));
});

check("hangers_no_independent_title_rebuild", () => {
  const locked = { ...frozen, publishedPresentationLocked: true, title: "Hangers Weddings & Events" };
  const auth = projectOpportunityForPresentation(locked, {
    surface: GDI_PRESENTATION_SURFACES.LOCAL_AUTH,
    hotelId: HOTEL,
    view: "detail",
    forceLocked: true,
  });
  assert.equal(auth.dto.title, "Hangers Weddings & Events");
  assert.notEqual(String(auth.dto.whatIsHappening || ""), "Hangers Weddings & Events");
});

const failed = results.filter((r) => !r.ok);
const out = {
  ok: failed.length === 0,
  contract: GDI_CANONICAL_OPPORTUNITY_CONTRACT_V1,
  hangersId: HANGERS_ID,
  hotelId: HOTEL,
  canonicalExample: {
    identity: canonical.identity,
    commercialClassification: canonical.commercialClassification,
    tile: {
      title: canonical.tile.title,
      commercialCategory: canonical.tile.commercialCategory,
      primaryContact: canonical.tile.primaryContact?.name,
      contactRole: canonical.tile.contactRole,
    },
    drawer: {
      primary: canonical.drawer.primaryContact?.name,
      secondary: canonical.drawer.secondaryContacts?.[0]?.name,
      facts: (canonical.drawer.establishedFacts || []).length,
      uncertainties: (canonical.drawer.uncertainties || []).length,
      qq: (canonical.drawer.qualificationQuestions || []).length,
      evidence: (canonical.drawer.supportingEvidence || []).length,
      whatLen: String(canonical.drawer.whatIsHappening || "").length,
      potentialLen: String(canonical.drawer.potentialBusinessForThisHotel || "").length,
    },
  },
  results,
};
fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "13-hangers-exact-parity.json"), JSON.stringify(out, null, 2));
console.log(JSON.stringify(out, null, 2));
process.exit(failed.length ? 1 : 0);
