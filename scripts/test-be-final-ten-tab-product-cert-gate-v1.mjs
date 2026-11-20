#!/usr/bin/env node
/**
 * Permanent gate: Final 10-tab product popup certification harness + lesson wiring.
 * Does NOT claim browser PASS by itself — requires CERTIFICATION.json from the
 * Playwright verifier for FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const verify = fs.readFileSync(
  path.join(ROOT, "scripts/verify-be-final-ten-tab-product-popup-cert-v1.mjs"),
  "utf8"
);
const combined = fs.readFileSync(
  path.join(ROOT, "public/brand-explorer-combined.html"),
  "utf8"
);
const lessons = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "configs/brand-explorer-founder-quality-lessons-registry-v1.json"),
    "utf8"
  )
);
const inventory = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "configs/brand-explorer-canonical-subsection-inventory-v1.json"),
    "utf8"
  )
);
const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, "package.json"), "utf8"));

assert.match(verify, /beCombinedPopupAtelierRoot/);
assert.match(verify, /beProfileMode=proposed/);
assert.match(verify, /FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED/);
assert.match(verify, /royal_palm_travel_weekly|royalPalmImage/);
assert.match(verify, /orphanFail|orphan_half_width/);
assert.match(verify, /uppercase_body|upperBodySuspects/);
assert.match(verify, /naPlaceholders/);
assert.match(verify, /Numeric value unavailable/);
assert.match(verify, /brand-explorer-combined\.html/);
assert.match(verify, /publicationRequires/);
assert.match(verify, /BE_LOCAL_PRODUCT_PARITY_GATE/);
assert.ok(
  (inventory.sections || []).length >= 30,
  "canonical subsection inventory must drive subsection matrix"
);

assert.match(
  combined,
  /window\.openBrandEducationPopupForBrand\s*=\s*openBrandEducationPopupForBrand/
);
assert.match(combined, /buildCumulativeProposedBrandAsync/);

const lesson = (lessons.lessons || []).find(
  (l) => l.lessonId === "BE-QL-FINAL-TEN-TAB-PRODUCT-CERT-001"
);
assert.ok(lesson, "founder lesson BE-QL-FINAL-TEN-TAB-PRODUCT-CERT-001 must be registered");
assert.equal(lesson.automatedRegressionTest, "test:be-final-ten-tab-product-cert-gate-v1");

assert.ok(
  pkg.scripts["test:be-final-ten-tab-product-cert-gate-v1"],
  "npm script test:be-final-ten-tab-product-cert-gate-v1 required"
);
assert.ok(
  pkg.scripts["verify:be-final-ten-tab-product-popup-cert-v1"],
  "npm script verify:be-final-ten-tab-product-popup-cert-v1 required"
);

const certPath = path.join(
  ROOT,
  "reports/brand-explorer/final-ten-tab-product-certification-v1/CERTIFICATION.json"
);
if (fs.existsSync(certPath)) {
  const cert = JSON.parse(fs.readFileSync(certPath, "utf8"));
  assert.equal(cert.version, "BE_FINAL_TEN_TAB_PRODUCT_CERTIFICATION_V1");
  assert.ok(cert.classification, "certification classification required");
  if (Array.isArray(cert.publicationRequires)) {
    assert.ok(
      cert.publicationRequires.includes("FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED") &&
        cert.publicationRequires.includes("BE_LOCAL_PRODUCT_PARITY_GATE"),
      "publication must require both product-quality cert and local parity"
    );
  }
  // Soft check: when browser evidence exists, surface classification (do not auto-fail CI on PARTIAL)
  console.log(
    JSON.stringify(
      {
        ok: true,
        lesson: lesson.lessonId,
        browserClassification: cert.classification,
        note:
          cert.classification.combined === "FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED"
            ? "browser evidence FULL certified"
            : "browser evidence present but not FULL — publication blocked",
      },
      null,
      2
    )
  );
} else {
  console.log(
    JSON.stringify(
      {
        ok: true,
        lesson: lesson.lessonId,
        browserClassification: null,
        note: "CERTIFICATION.json absent — run verify:be-final-ten-tab-product-popup-cert-v1 against 8080",
      },
      null,
      2
    )
  );
}
