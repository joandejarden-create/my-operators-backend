#!/usr/bin/env node
import assert from "assert";
import {
  BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
  EXACT_BRAND_DECISION,
  EXACT_BRAND_REJECT_FIXTURES,
  evaluateExactBrandImageIdentity,
  blocksImageCertification,
} from "../lib/brand-explorer/factory-2/be-factory-permanent-exact-brand-image-identity-v1.js";
import {
  IMAGE_V3_CLASS,
  mapV2ClassToV3,
  evaluateGalleryCertLayer,
  GALLERY_CERT_LAYER,
} from "../lib/brand-explorer/factory-2/be-factory-permanent-image-integrity-v3.js";
import { IMAGE_SLOT_CLASS } from "../lib/brand-explorer/factory-2/be-factory-permanent-image-integrity-v2.js";

assert.strictEqual(
  BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
  "BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1"
);

// Waldorf own brand PASS via affinity
{
  const r = evaluateExactBrandImageIdentity({
    targetSlug: "waldorf-astoria",
    imageUrl: "https://www.hilton.com/im/en/MIAAWA/waldorf-astoria-miami-exterior.jpg",
    imageMeta: { propertyName: "Waldorf Astoria Miami", caption: "Waldorf Astoria exterior" },
  });
  assert.strictEqual(r.ok, true, "waldorf self image should pass");
  assert.strictEqual(r.decision, EXACT_BRAND_DECISION.ELIGIBLE);
}

// Founder reject fixtures
for (const fx of EXACT_BRAND_REJECT_FIXTURES) {
  const r = evaluateExactBrandImageIdentity(fx);
  assert.strictEqual(
    r.decision,
    fx.expect,
    `${fx.targetSlug} vs ${fx.propertyBrandSlug || fx.imageUrl} expected ${fx.expect} got ${r.decision}`
  );
  assert.strictEqual(blocksImageCertification(r), true);
}

// Canonical ID match
{
  const r = evaluateExactBrandImageIdentity({
    targetBrandId: "recAAAAAAAAAAAAAA",
    propertyBrandId: "recAAAAAAAAAAAAAA",
    propertyCensusId: "recPROPERTYYYYYYY",
  });
  assert.strictEqual(r.ok, true);
}

// Canonical ID mismatch
{
  const r = evaluateExactBrandImageIdentity({
    targetBrandId: "recWALDORFXXXXXXX",
    propertyBrandId: "recHILTONYYYYYYYY",
  });
  assert.strictEqual(r.ok, false);
  assert.strictEqual(r.decision, EXACT_BRAND_DECISION.REJECT_WRONG_BRAND);
}

// Unverified property HOLD
{
  const r = evaluateExactBrandImageIdentity({
    targetSlug: "unknown-brand-xyz",
    imageUrl: "https://example.com/photo.jpg",
    imageMeta: { propertyName: "Mystery Hotel" },
  });
  assert.strictEqual(r.decision, EXACT_BRAND_DECISION.HOLD_UNVERIFIED_PROPERTY);
}

// Six images present but wrong-brand → not CERTIFIED layer
{
  const slots = Array.from({ length: 6 }, () => ({
    v3Class: IMAGE_V3_CLASS.IMAGE_SIBLING_BRAND,
    rightsOk: true,
    browserOk: true,
  }));
  const layer = evaluateGalleryCertLayer(slots);
  assert.strictEqual(layer, GALLERY_CERT_LAYER.SIX_IMAGES_PRESENT);
}

// Six certified
{
  const slots = Array.from({ length: 6 }, () => ({
    v3Class: IMAGE_V3_CLASS.IMAGE_CERTIFIED,
    rightsOk: true,
    browserOk: true,
  }));
  assert.strictEqual(evaluateGalleryCertLayer(slots), GALLERY_CERT_LAYER.SIX_IMAGES_CERTIFIED);
}

// mapV2 sibling overlay
{
  const exact = evaluateExactBrandImageIdentity({
    targetSlug: "waldorf-astoria",
    propertyBrandSlug: "hilton-hotels-resorts",
  });
  const v3 = mapV2ClassToV3(IMAGE_SLOT_CLASS.VALIDATED, { exactBrand: exact });
  assert.strictEqual(v3, IMAGE_V3_CLASS.IMAGE_SIBLING_BRAND);
}

// Rights missing → HOLD certification (rights blocked)
{
  const r = evaluateExactBrandImageIdentity({
    targetSlug: "aloft-hotels",
    propertyBrandSlug: "aloft-hotels",
    rightsOk: false,
  });
  assert.strictEqual(blocksImageCertification(r), true);
}

// Duplicate slot resolution
{
  const { resolveDuplicateSlotRows } = await import(
    "../lib/brand-explorer/factory-2/be-factory-duplicate-slot-resolution-v1.js"
  );
  const res = resolveDuplicateSlotRows([
    { recordId: "recAAAA", hasAttachment: true, technicallyValid: false },
    { recordId: "recBBBB", hasAttachment: true, technicallyValid: true, exactBrandOk: true },
  ]);
  assert.strictEqual(res.primaryRecordId, "recBBBB");
  assert.strictEqual(res.reviewRequired, true);
}

// HE reuse refuses sibling
{
  const { assertNoSilentBrandSubstitution } = await import(
    "../lib/brand-explorer/factory-2/be-hotel-explorer-verified-image-reuse-v1.js"
  );
  const check = assertNoSilentBrandSubstitution("waldorf-astoria", {
    brandSlug: "hilton-hotels-resorts",
    propertyName: "Hilton Panama",
  });
  assert.strictEqual(check.ok, false);
}

// Six technically valid with one wrong-brand → not CERTIFIED
{
  const slots = [
    ...Array.from({ length: 5 }, () => ({
      v3Class: IMAGE_V3_CLASS.IMAGE_TECHNICALLY_VALID,
      rightsOk: true,
      browserOk: true,
    })),
    { v3Class: IMAGE_V3_CLASS.IMAGE_SIBLING_BRAND, rightsOk: true, browserOk: true },
  ];
  assert.notStrictEqual(evaluateGalleryCertLayer(slots), GALLERY_CERT_LAYER.SIX_IMAGES_CERTIFIED);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
      rejectFixtures: EXACT_BRAND_REJECT_FIXTURES.length,
    },
    null,
    2
  )
);
