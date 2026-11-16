#!/usr/bin/env node
/**
 * Stage a new Radisson package with Toronto Tempo drawer content remediation.
 * Preserves CURRENT fingerprint 533f2642. No paid research. No Airtable writes.
 *
 *   node scripts/gdi-radisson-tempo-drawer-content-restage-v1.mjs
 *   node scripts/gdi-radisson-tempo-drawer-content-restage-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublicationPackageStore,
  verifyPublicationPackageIntegrity,
  fingerprintPublicationContent,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recH9h3McgMNFQEla";
const SOURCE_VERSION =
  "gdi_pub_recH9h3McgMNFQEla_20261116_segment_classification_remediation_v1";
const NEW_VERSION =
  "gdi_pub_recH9h3McgMNFQEla_20261116_tempo_drawer_commercial_remediation_v1";
const TEMPO_ID = "rx_gdi_s4912_d46e3e911701";
const PRIOR_FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-radisson-drawer-full-e2e-parity-v1",
);
const apply = process.argv.includes("--apply");

const store = createGdiPublicationPackageStore({
  root: path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  ),
});
const loaded = store.loadPackage(HOTEL, SOURCE_VERSION);
if (!loaded.ok) {
  console.error(JSON.stringify(loaded, null, 2));
  process.exit(1);
}

const src = loaded.package;
const pkg = JSON.parse(JSON.stringify(src));
const tempo = (pkg.opportunities || []).find((o) => o.id === TEMPO_ID);
if (!tempo) {
  console.error("TEMPO_NOT_FOUND");
  process.exit(1);
}

const before = {
  whyThisHotel: tempo.whyThisHotel,
  recommendedAction: tempo.recommendedAction,
  recommendedNextStep: tempo.recommendedNextStep,
  primaryContact: tempo.primaryContact,
};

// Concise hotel-fit from already-published hotel facts + existing Tempo thesis.
// Does not invent room blocks, exclusive preferred status, or fixture dates.
tempo.whyThisHotel =
  "Radisson Blu Toronto Downtown (157 rooms, Harbourfront / Queens Quay) is a credible mid-size option for visiting-team overflow, media/production, sponsor, or commercial guest lodging near downtown and Exhibition Place. It is not evidenced as able to house an entire WNBA programme, and Hotel X remains the domestic partner — treat preferred-hotel rights as contested or closed unless Tempo confirms an open non-exclusive segment.";

tempo.whyActNow =
  "2027 ticket/series intent is public, but dated home fixtures and lodging RFPs are not verified. Establish Tempo’s visiting-team / overflow sourcing timeline and whether any non-exclusive lodging segments remain open beside Hotel X — do not claim immediate booking urgency without a confirmed planning milestone.";

if (
  tempo.potentialHotelBusiness &&
  /unquantified/i.test(String(tempo.potentialHotelBusiness))
) {
  tempo.potentialHotelBusiness =
    "Possible visiting-team overflow, traveling league personnel, commercial partner, media/production, or non-exclusive corporate lodging around Tempo home activity — all unquantified and not a confirmed room block or exclusive preferred-hotel award.";
  tempo.potentialBusiness = tempo.potentialHotelBusiness;
}

if (!tempo.primaryContact) tempo.primaryContact = {};
tempo.primaryContact.whyThisContact =
  "Chief Revenue Officer — commercial entry point for partnership and lodging qualification. Confirm who actually controls visiting-team accommodation versus commercial guest travel before treating this as the lodging buyer.";
tempo.primaryContact.relevance = tempo.primaryContact.whyThisContact;
tempo.whoPrimaryReason = tempo.primaryContact.whyThisContact;

// Prefer complete next-step text already present on the opportunity.
if (
  tempo.recommendedNextStep &&
  (!tempo.recommendedAction ||
    String(tempo.recommendedAction).length < String(tempo.recommendedNextStep).length)
) {
  tempo.recommendedAction = tempo.recommendedNextStep;
}

pkg.publicationVersion = NEW_VERSION;
pkg.publicationId = "gdi_pubpkg_recH9h3McgMNFQEla_tempo_drawer_remediation_v1";
pkg.lifecycleState = GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED;
pkg.approvalStatus = "PENDING_FOUNDER_APPROVAL";
pkg.approvedAt = null;
pkg.priorContentFingerprintSha256 = PRIOR_FP;
pkg.createdAt = new Date().toISOString();
pkg.remediationNote =
  "Toronto Tempo drawer commercial remediation from existing package evidence only; CURRENT 533f2642 preserved.";
pkg.founderApprovalReceipt = null;

const fp = fingerprintPublicationContent({
  hotelId: pkg.hotelId,
  publicationVersion: pkg.publicationVersion,
  presentationSchemaVersion: pkg.presentationSchemaVersion,
  applicationCompatibilityVersion: pkg.applicationCompatibilityVersion,
  sortOrder: pkg.sortOrder || (pkg.opportunities || []).map((o) => o.id),
  opportunities: pkg.opportunities || [],
});
pkg.contentFingerprintSha256 = fp.contentFingerprintSha256;
pkg.opportunityCount = fp.opportunityCount;

const integrity = verifyPublicationPackageIntegrity(pkg);
if (!integrity.ok) {
  console.error(JSON.stringify({ ok: false, integrity }, null, 2));
  process.exit(1);
}

let written = { ok: true };
if (apply) {
  written = store.writePackage(pkg);
  if (!written.ok) {
    console.error(JSON.stringify(written, null, 2));
    process.exit(1);
  }
  store.writePointer(HOTEL, "STAGED", {
    publicationId: pkg.publicationId,
    publicationVersion: NEW_VERSION,
    contentFingerprintSha256: pkg.contentFingerprintSha256,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
  });
}
const newFp = pkg.contentFingerprintSha256;

fs.mkdirSync(OUT, { recursive: true });
const result = {
  ok: integrity.ok !== false,
  apply,
  priorFingerprint: PRIOR_FP,
  newFingerprint: newFp,
  publicationVersion: NEW_VERSION,
  currentPreserved: true,
  tempoId: TEMPO_ID,
  before,
  after: {
    whyThisHotel: tempo.whyThisHotel,
    whyActNow: tempo.whyActNow,
    potentialHotelBusiness: tempo.potentialHotelBusiness,
    recommendedNextStep: tempo.recommendedNextStep,
    recommendedAction: tempo.recommendedAction,
    contactRelevance: tempo.primaryContact.whyThisContact,
  },
  next: apply
    ? "Await FOUNDER_APPROVED_FOR_PUBLICATION <newFingerprint> before CURRENT replace"
    : "Re-run with --apply to write STAGED pointer",
};
fs.writeFileSync(
  path.join(OUT, "03b-TEMPO_STAGED_PACKAGE.json"),
  JSON.stringify(result, null, 2) + "\n",
);
console.log(JSON.stringify(result, null, 2));
process.exit(result.ok ? 0 : 1);
