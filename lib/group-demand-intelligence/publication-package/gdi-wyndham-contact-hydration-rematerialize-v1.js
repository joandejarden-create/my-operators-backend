/**
 * Rematerialize Wyndham locked-26 STAGED with contact channel hydration.
 *
 * Recoveries (settled evidence only, $0):
 * - Map person LinkedIn from sourceUrl → linkedinUrl (VERIFIED_EXISTING_DATA_NOT_MAPPED)
 * - Map secondary LinkedIn the same way
 * - Recover verified professional titles where historically known
 * - Attach contactChannelResearch status (email/phone/LinkedIn researched markers)
 * - Re-run Stage 4 DryRun channel collection so LinkedIn counts as usable outreach
 *
 * Does NOT invent emails/phones. Does NOT call Surfe/Findymail.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "./index.js";
import { fingerprintPublicationContent } from "./gdi-customer-publication-package-v1.js";
import {
  collectVerifiedChannelsForPerson,
  resolvePersonLinkedInUrl,
  summarizeChannelResearchStatus,
  GDI_CONTACT_RESEARCH_STAGE_V1,
} from "../workflow/gdi-contact-research-stage-v1.js";

export const GDI_WYNDHAM_CONTACT_HYDRATION_REMATERIALIZE_V1 =
  "gdi-wyndham-contact-hydration-rematerialize-v1";

export const WYNDHAM_CONTACT_HYDRATION_AUTH =
  "FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_CONTACT_HYDRATION_RECOVERY";

export const WYNDHAM_HOTEL_ID = "recnPB3Q02NO8Ma1o";
export const WYNDHAM_LOCKED_SOURCE_VERSION =
  "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_84288a6e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");

/** Settled title recoveries from prior verified publication / WHO evidence. */
const TITLE_RECOVERIES = Object.freeze({
  "Kathryn Newstead": "Director of Events",
  "Brooke Wheeler": "Membership Events Lead",
  "Kristine Coan": "Meeting Logistics Contact",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function hydrateContact(contact, { isSecondary = false } = {}) {
  if (!contact || typeof contact !== "object" || !text(contact.name)) {
    return { contact, changes: [] };
  }
  const next = { ...contact };
  const changes = [];
  const li = resolvePersonLinkedInUrl(next);
  if (li && text(next.linkedinUrl) !== li) {
    next.linkedinUrl = li;
    if (!text(next.sourceUrl)) next.sourceUrl = li;
    changes.push("LINKEDIN_MAPPED_FROM_SOURCE");
  }
  const name = text(next.name);
  if ((!text(next.title) || text(next.title) === "null") && TITLE_RECOVERIES[name]) {
    next.title = TITLE_RECOVERIES[name];
    next.role = next.role || TITLE_RECOVERIES[name];
    changes.push("TITLE_RECOVERED_FROM_SETTLED_EVIDENCE");
  }
  // Clean noisy title suffixes that render as "Title Not Verified" noise
  if (/per professional profile/i.test(text(next.title))) {
    next.title = text(next.title)
      .replace(/,?\s*per professional profile\.?/gi, "")
      .trim();
    changes.push("TITLE_NOISE_STRIPPED");
  }
  if (!isSecondary) {
    const collected = collectVerifiedChannelsForPerson(next, {
      primaryContact: next,
    });
    next.channels = collected.accepted;
    next.contactChannelResearch = summarizeChannelResearchStatus(
      collected,
      next,
    );
    changes.push(
      collected.usable?.ok
        ? "STAGE4_CHANNELS_USABLE"
        : "STAGE4_CHANNELS_HOLD_NO_EMAIL_PHONE",
    );
  }
  return { contact: next, changes };
}

export function hydrateOpportunityContacts(opportunity = {}) {
  const fieldChanges = [];
  const primary = hydrateContact(opportunity.primaryContact || {});
  fieldChanges.push(...primary.changes.map((c) => `primary:${c}`));
  let secondary = opportunity.secondaryContact || null;
  if (secondary?.name) {
    const sec = hydrateContact(secondary, { isSecondary: true });
    secondary = sec.contact;
    fieldChanges.push(...sec.changes.map((c) => `secondary:${c}`));
  }
  const next = {
    ...opportunity,
    primaryContact: primary.contact,
    ...(secondary ? { secondaryContact: secondary } : {}),
  };
  if (primary.contact?.channels) {
    next.verifiedContactChannels = primary.contact.channels;
    next.contactResearchStage = {
      version: GDI_CONTACT_RESEARCH_STAGE_V1,
      at: new Date().toISOString(),
      channels: primary.contact.channels,
      usableOutreach: {
        ok: Boolean(primary.contact.contactChannelResearch?.usableOutreachRoute),
      },
      findymailUsed: false,
      surfeUsed: false,
      surfePersisted: false,
      hydration: GDI_WYNDHAM_CONTACT_HYDRATION_REMATERIALIZE_V1,
    };
  }
  return { opportunity: next, fieldChanges };
}

export function auditContactCoverage(opportunities = []) {
  const rows = [];
  let named = 0;
  let titled = 0;
  let email = 0;
  let phone = 0;
  let linkedin = 0;
  let emailOrPhone = 0;
  let outreach = 0;
  let stage4 = 0;
  let mappedLiRecoveries = 0;
  for (const o of opportunities) {
    const p = o.primaryContact || {};
    const s = o.secondaryContact || null;
    const li = resolvePersonLinkedInUrl(p);
    const hasEmail = Boolean(text(p.email));
    const hasPhone = Boolean(text(p.phone));
    const hasName = Boolean(text(p.name));
    const hasTitle = Boolean(text(p.title || p.role));
    if (hasName) named += 1;
    if (hasTitle) titled += 1;
    if (hasEmail) email += 1;
    if (hasPhone) phone += 1;
    if (li) linkedin += 1;
    if (hasEmail || hasPhone) emailOrPhone += 1;
    if (hasEmail || hasPhone || li) outreach += 1;
    if (o.contactResearchStage || p.contactChannelResearch) stage4 += 1;
    if (li && !text(p.linkedinUrl)) mappedLiRecoveries += 1;
    const missingEmailClass = hasEmail
      ? null
      : li
        ? "PUBLIC_CONTACT_RESEARCH_NOT_EXECUTED"
        : text(p.sourceUrl)
          ? "RESEARCH_EXECUTED_NOT_FOUND"
          : "PUBLIC_CONTACT_RESEARCH_NOT_EXECUTED";
    const missingPhoneClass = hasPhone
      ? null
      : li
        ? "PUBLIC_CONTACT_RESEARCH_NOT_EXECUTED"
        : "PUBLIC_CONTACT_RESEARCH_NOT_EXECUTED";
    const linkedinClass =
      li && !text(p.linkedinUrl)
        ? "VERIFIED_EXISTING_DATA_NOT_MAPPED"
        : li
          ? null
          : "RESEARCH_EXECUTED_NOT_FOUND";
    rows.push({
      opportunityId: o.opportunityId || o.id,
      title: text(o.title).slice(0, 80),
      primaryName: text(p.name) || null,
      primaryTitle: text(p.title || p.role) || null,
      email: text(p.email) || null,
      phone: text(p.phone) || null,
      linkedinUrl: li || null,
      linkedinFieldSet: Boolean(text(p.linkedinUrl)),
      secondaryName: text(s?.name) || null,
      secondaryLinkedIn: resolvePersonLinkedInUrl(s || {}) || null,
      missingEmailClass,
      missingPhoneClass,
      linkedinClass,
      usableOutreach: Boolean(hasEmail || hasPhone || li),
    });
  }
  return {
    totalOpportunities: opportunities.length,
    namedContacts: named,
    verifiedTitles: titled,
    verifiedEmail: email,
    verifiedTelephone: phone,
    verifiedPersonalLinkedIn: linkedin,
    emailOrTelephone: emailOrPhone,
    usableOutreachRoute: outreach,
    stage4Annotated: stage4,
    linkedinMappingDefectsBeforeHydration: mappedLiRecoveries,
    rows,
  };
}

export function rematerializeWyndhamStagedContactHydration(options = {}) {
  const apply = options.apply === true || options.dryRun === false;
  const auth = text(options.auth);
  const root =
    options.packageRoot ||
    path.join(
      REPO_ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
    );
  const store = createGdiPublicationPackageStore({ root });
  const sourceVersion =
    text(options.sourceVersion) || WYNDHAM_LOCKED_SOURCE_VERSION;
  const loaded = store.loadPackage(WYNDHAM_HOTEL_ID, sourceVersion);
  if (!loaded?.ok || !loaded.package) {
    return {
      ok: false,
      error: "SOURCE_PACKAGE_MISSING",
      sourceVersion,
      detail: loaded,
    };
  }
  const src = loaded.package;
  const beforeAudit = auditContactCoverage(src.opportunities || []);
  const hydratedOpps = [];
  const changeLog = [];
  for (const o of src.opportunities || []) {
    const { opportunity, fieldChanges } = hydrateOpportunityContacts(o);
    hydratedOpps.push(opportunity);
    if (fieldChanges.length) {
      changeLog.push({
        opportunityId: o.opportunityId || o.id,
        fieldChanges,
      });
    }
  }
  const afterAudit = auditContactCoverage(hydratedOpps);
  const fingerprint = crypto
    .createHash("sha256")
    .update(
      JSON.stringify(
        hydratedOpps.map((o) => ({
          id: o.opportunityId,
          email: o.primaryContact?.email || null,
          phone: o.primaryContact?.phone || null,
          li: o.primaryContact?.linkedinUrl || null,
          title: o.primaryContact?.title || null,
        })),
      ),
    )
    .digest("hex")
    .slice(0, 8);
  const publicationVersion = `gdi_hotel_wide_fqo_${WYNDHAM_HOTEL_ID}_20261127_${fingerprint}`;
  const nextPkgDraft = {
    ...src,
    publicationVersion,
    publicationId: publicationVersion,
    createdAt: new Date().toISOString(),
    opportunities: hydratedOpps,
    opportunityCount: hydratedOpps.length,
    inventoryIds: hydratedOpps.map((o) => o.opportunityId || o.id),
    sortOrder: hydratedOpps.map((o) => o.opportunityId || o.id),
    gates: {
      ...(src.gates || {}),
      contactHydration: {
        contract: GDI_WYNDHAM_CONTACT_HYDRATION_REMATERIALIZE_V1,
        sourceVersion,
        recoveredLinkedIn: afterAudit.verifiedPersonalLinkedIn,
        emailUnchanged: afterAudit.verifiedEmail === beforeAudit.verifiedEmail,
      },
    },
  };
  const fpBody = fingerprintPublicationContent(nextPkgDraft);
  const nextPkg = {
    ...nextPkgDraft,
    contentFingerprintSha256: fpBody.contentFingerprintSha256,
  };

  const result = {
    ok: true,
    contract: GDI_WYNDHAM_CONTACT_HYDRATION_REMATERIALIZE_V1,
    dryRun: !apply,
    sourceVersion,
    publicationVersion,
    opportunityCount: hydratedOpps.length,
    beforeAudit,
    afterAudit,
    changeLog,
    activated: false,
  };

  if (!apply) return result;

  if (auth !== WYNDHAM_CONTACT_HYDRATION_AUTH) {
    return {
      ...result,
      ok: false,
      error: "AUTH_REQUIRED",
      requiredAuth: WYNDHAM_CONTACT_HYDRATION_AUTH,
    };
  }

  const staged = store.stageForProduction(nextPkg);
  if (!staged?.ok) {
    return { ...result, ok: false, error: "STAGE_FAILED", detail: staged };
  }
  return {
    ...result,
    ok: true,
    activated: true,
    staged,
  };
}
