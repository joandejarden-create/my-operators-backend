#!/usr/bin/env node
import assert from "assert/strict";
import {
  BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2,
  IMAGE_SLOT_CLASS,
  IMAGE_LIFECYCLE,
  isAirtableSignedCdnUrl,
  classifyLiveAttachment,
  classifyProposedUrlStatus,
  shouldWriteImageOnWriteback,
  attachmentIdentity,
  bumpCoverage,
  emptyCoverageTotals,
} from "../lib/brand-explorer/factory-2/be-factory-permanent-image-integrity-v2.js";
import {
  hashPresentationText,
  hashPresentationImages,
  buildCardinalityAwareWritePlan,
} from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";

assert.equal(BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2, "BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2");
assert.ok(IMAGE_SLOT_CLASS.ATTACHMENT_MISSING);
assert.ok(IMAGE_LIFECYCLE.IMAGE_READBACK_VERIFIED);

assert.equal(isAirtableSignedCdnUrl("https://v5.airtableusercontent.com/v3/u/x"), true);
assert.equal(isAirtableSignedCdnUrl("https://cache.marriott.com/x.jpg"), false);

assert.equal(
  classifyLiveAttachment({ attachment: null }),
  IMAGE_SLOT_CLASS.ATTACHMENT_MISSING
);
assert.equal(
  classifyLiveAttachment({
    attachment: { url: "https://v5.airtableusercontent.com/a" },
    probe: { ok: false, httpStatus: 410 },
  }),
  IMAGE_SLOT_CLASS.SIGNED_URL_EXPIRED
);
assert.equal(
  classifyLiveAttachment({
    attachment: { url: "https://v5.airtableusercontent.com/a" },
    probe: { ok: true, format: "jpeg", decodeOk: true, browserOk: true },
  }),
  IMAGE_SLOT_CLASS.VALIDATED
);

assert.equal(
  classifyProposedUrlStatus("FETCH_FAILED", "http_410"),
  IMAGE_SLOT_CLASS.SIGNED_URL_EXPIRED
);

const preserve = shouldWriteImageOnWriteback({
  currentAttachment: { url: "https://v5.airtableusercontent.com/cur", attachmentId: "att1" },
  proposedImageUrl: "https://cache.marriott.com/x.jpg",
  proposedCertified: false,
});
assert.equal(preserve.writeImage, false);
assert.match(preserve.reason, /preserve_current/);

const fill = shouldWriteImageOnWriteback({
  currentAttachment: null,
  proposedImageUrl: "https://cache.marriott.com/x.jpg",
  proposedTechnicalOk: true,
});
assert.equal(fill.writeImage, true);

const rejectCdn = shouldWriteImageOnWriteback({
  currentAttachment: null,
  proposedImageUrl: "https://v5.airtableusercontent.com/expired",
  proposedCertified: true,
});
assert.equal(rejectCdn.writeImage, false);

const idA = attachmentIdentity({ attachmentId: "att1", filename: "a.jpg", size: 10 });
const idB = attachmentIdentity({
  imageUrl: "https://v5.airtableusercontent.com/different-signed",
  attachmentId: "att1",
  filename: "a.jpg",
  size: 10,
});
assert.equal(idA.fingerprint, idB.fingerprint);

let totals = emptyCoverageTotals();
totals = bumpCoverage(totals, IMAGE_SLOT_CLASS.ATTACHMENT_MISSING);
totals = bumpCoverage(totals, IMAGE_SLOT_CLASS.SIGNED_URL_EXPIRED);
assert.equal(totals.requiredSlots, 2);
assert.equal(totals.attachmentMissing, 1);
assert.equal(totals.signedUrlExpired, 1);

const textHash = hashPresentationText([
  { slotKey: "overview.scenario.1", title: "A", body: "B", sort: 1 },
]);
const textHash2 = hashPresentationText([
  {
    slotKey: "overview.scenario.1",
    title: "A",
    body: "B",
    sort: 1,
    imageUrl: "https://v5.airtableusercontent.com/one",
  },
]);
assert.equal(textHash, textHash2, "text hash ignores image CDN");

const imgHash1 = hashPresentationImages([
  { slotKey: "overview.scenario.1", attachmentId: "att1", filename: "a.jpg" },
]);
const imgHash2 = hashPresentationImages([
  {
    slotKey: "overview.scenario.1",
    attachmentId: "att1",
    filename: "a.jpg",
    imageUrl: "https://v5.airtableusercontent.com/changed-signed",
  },
]);
assert.equal(imgHash1, imgHash2, "image hash ignores signed URL churn");
const imgHashMissing = hashPresentationImages([{ slotKey: "overview.scenario.1" }]);
assert.notEqual(imgHash1, imgHashMissing, "missing image must change image hash");

const plan = buildCardinalityAwareWritePlan({
  brandId: "recTEST",
  brandName: "Test",
  currentBlocks: [
    {
      recordId: "recCurr",
      slotKey: "overview.scenario.1",
      title: "T",
      body: "B",
      sort: 1,
      imageUrl: "https://v5.airtableusercontent.com/current",
      attachmentId: "attCur",
    },
  ],
  proposedBlocks: [
    {
      slotKey: "overview.scenario.1",
      title: "T",
      body: "B",
      sort: 1,
      imageUrl: "https://cache.marriott.com/stale.jpg",
      imageCertified: false,
    },
  ],
});
assert.equal(plan.operations.length, 0, "must not overwrite CURRENT with unverified proposed");

const planFill = buildCardinalityAwareWritePlan({
  brandId: "recTEST",
  brandName: "Test",
  currentBlocks: [
    {
      recordId: "recCurr",
      slotKey: "overview.scenario.1",
      title: "T",
      body: "B",
      sort: 1,
    },
  ],
  proposedBlocks: [
    {
      slotKey: "overview.scenario.1",
      title: "T2",
      body: "B2",
      sort: 1,
      imageUrl: "https://cache.marriott.com/new.jpg",
      imageTechnicalOk: true,
    },
  ],
});
assert.ok(planFill.operations.some((o) => o.op === "UPDATE"));
assert.equal(planFill.operations[0].imageWrite.writeImage, true);
assert.ok(planFill.operations[0].fields.Image);

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2,
      cases: [
        "signed_cdn_detect",
        "classify_expired",
        "preserve_current_on_writeback",
        "text_hash_ignores_cdn",
        "image_hash_identity",
        "missing_image_fails_image_hash",
      ],
    },
    null,
    2
  )
);
