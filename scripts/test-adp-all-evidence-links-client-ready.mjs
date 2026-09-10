#!/usr/bin/env node
/**
 * Full-universe ADP evidence-link client-readiness gate.
 *
 *   npm run test:adp-all-evidence-links-client-ready
 *
 * Doctrine:
 * EVERY_CLICKABLE_EVIDENCE_LINK_MUST_OPEN_EXACT_SUPPORTING_EVIDENCE
 * EMPTY_WRONG_STALE_OR_SUPPRESSED_EVIDENCE_IS_NOT_CLIENT_READY
 * ADP_NO_INTERNAL_EVIDENCE_DIAGNOSTICS_CUSTOMER_FACING
 * ADP_EVIDENCE_MODAL_TYPE_LABEL_PARITY
 *
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 */

import assert from "assert";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";
import { listPublishedPropertyIds } from "../lib/ai-demand-positioning/published-snapshot.js";
import {
  ADP_CUSTOMER_EVIDENCE_CONTRACT_V1,
  EVIDENCE_LINKED_SURFACES,
  buildEvidenceModalTitle,
  EVIDENCE_TYPE,
  customerCopyContainsInternalDiagnostics,
  auditAllEvidenceLinksClientReady,
} from "../lib/ai-demand-positioning/customer/adp-customer-evidence-contract-v1.js";

const ROOT = process.cwd();
const OUT_DIR = join(ROOT, "reports/ai-demand-positioning");
const OUT = join(OUT_DIR, "adp-all-evidence-links-client-ready-v1.json");
const UI = join(ROOT, "public/js/ai-demand-positioning/ai-demand-positioning.js");
const CSS = join(ROOT, "public/js/ai-demand-positioning/ai-demand-positioning.css");
const OWNER_HTML = join(ROOT, "public/owner-ai-demand.html");
const SHARE_HTML = join(ROOT, "public/owner-ai-demand-share.html");

function assertUiContract() {
  const ui = readFileSync(UI, "utf8");
  const css = readFileSync(CSS, "utf8");
  assert.ok(
    /Displacement Evidence/.test(ui),
    "UI must set Displacement Evidence modal title"
  );
  assert.ok(
    /data-adp-evidence-type=\"COMPETITIVE_DISPLACEMENT\"/.test(ui),
    "Displacement controls must declare evidenceType"
  );
  assert.ok(
    !/identity did not reconcile/.test(ui),
    "No customer-facing identity reconcile diagnostics"
  );
  assert.ok(
    !/analytical integrity check/.test(ui),
    "No customer-facing analytical integrity diagnostics"
  );
  assert.ok(
    !/not client-ready and was not shown/.test(ui),
    "No customer-facing client-ready failure copy"
  );
  assert.ok(
    /Supporting evidence is unavailable for this claim/.test(ui),
    "Customer-safe unavailable copy required"
  );
  assert.ok(
    /Brand & Portfolio Evidence · Positive/.test(ui),
    "BPP modal title contract"
  );
  assert.ok(
    /white-space:\s*pre-wrap/.test(css),
    "ADP_EVIDENCE_SAFE_STRUCTURED_TEXT_RENDER requires pre-wrap"
  );
  assert.ok(
    existsSync(OWNER_HTML) && existsSync(SHARE_HTML),
    "owner + share pages required for local/external parity"
  );
  const owner = readFileSync(OWNER_HTML, "utf8");
  const share = readFileSync(SHARE_HTML, "utf8");
  const ownerToken = (owner.match(/ai-demand-positioning\.js\?v=([^\"]+)/) || [])[1];
  const shareToken = (share.match(/ai-demand-positioning\.js\?v=([^\"]+)/) || [])[1];
  assert.ok(ownerToken && ownerToken === shareToken, "ADP_EVIDENCE_LOCAL_EXTERNAL_PARITY asset token");
}

async function main() {
  assertUiContract();

  assert.equal(
    buildEvidenceModalTitle(EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT, "Business"),
    "Displacement Evidence · Business"
  );
  assert.equal(
    buildEvidenceModalTitle(EVIDENCE_TYPE.POSITIVE_PRESENCE, "Business"),
    "Positive Evidence · Business"
  );
  assert.equal(
    buildEvidenceModalTitle(EVIDENCE_TYPE.MISSING_PRESENCE, "Business"),
    "Missing Evidence · Business"
  );
  assert.equal(
    buildEvidenceModalTitle(EVIDENCE_TYPE.REALITY_GAP, "M Club"),
    "Reality Gap Evidence · M Club"
  );
  assert.ok(
    customerCopyContainsInternalDiagnostics("competitor identity did not reconcile"),
    "diagnostic detector must catch reconcile copy"
  );
  assert.ok(
    !customerCopyContainsInternalDiagnostics("Supporting evidence is unavailable for this claim."),
    "safe copy must pass diagnostic detector"
  );

  const propertyIds = listPublishedPropertyIds().sort();
  assert.ok(propertyIds.length >= 5, "published universe required");

  const audit = await auditAllEvidenceLinksClientReady(propertyIds);
  mkdirSync(OUT_DIR, { recursive: true });
  writeFileSync(OUT, JSON.stringify(audit, null, 2) + "\n");

  console.log(
    JSON.stringify(
      {
        ok: audit.status === "PASS",
        contract: ADP_CUSTOMER_EVIDENCE_CONTRACT_V1,
        propertyCount: audit.propertyCount,
        totalLinks: audit.totalLinks,
        totalFail: audit.totalFail,
        failuresByType: audit.failuresByType,
        emptyLinks: audit.emptyLinks.length,
        wrongStale: audit.wrongStale.length,
        internalDiagnostics: audit.internalDiagnostics.length,
        modalLabelDefects: audit.modalLabelDefects.length,
        surfaces: EVIDENCE_LINKED_SURFACES.map((s) => ({
          id: s.id,
          clickable: s.clickable,
        })),
        outPath: OUT,
        methodologyChanged: false,
      },
      null,
      2
    )
  );

  assert.equal(audit.status, "PASS", `evidence links must be client-ready; fail=${audit.totalFail}`);
  assert.equal(audit.totalFail, 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
