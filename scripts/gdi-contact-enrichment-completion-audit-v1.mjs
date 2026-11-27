#!/usr/bin/env node
/**
 * Read-only Wyndham (or any hotel) STAGED contact audit → reports/gdi-contact-enrichment-completion-v1/
 * No Findymail/Surfe/Airtable.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { classifyEmailChannel, classifyPhoneChannel } from "../lib/group-demand-intelligence/presentation/gdi-contact-actionability-v1.js";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import { evaluatePersonSpecificOutreachChannel } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = process.env.GDI_AUDIT_HOTEL_ID || "recnPB3Q02NO8Ma1o";
const OUT_DIR = path.join(ROOT, "reports/gdi-contact-enrichment-completion-v1");
const BASELINE_PATH = path.join(
  ROOT,
  "reports/gdi-contact-research-hydration-v1/03-COVERAGE_SCORECARD.json",
);

const GAP = Object.freeze({
  EXISTS_NOT_DISPLAYED: "EXISTS_NOT_DISPLAYED",
  EXISTS_NOT_PERSISTED: "EXISTS_NOT_PERSISTED",
  NOT_RESEARCHED: "NOT_RESEARCHED",
  RESEARCHED_UNVERIFIED: "RESEARCHED_UNVERIFIED",
  NEEDS_IDENTITY_RESOLUTION: "NEEDS_IDENTITY_RESOLUTION",
  NEEDS_TARGETED_ENRICHMENT: "NEEDS_TARGETED_ENRICHMENT",
  GENUINE_UNAVAILABLE: "GENUINE_UNAVAILABLE",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function linkedInUrl(contact = {}) {
  const candidates = [
    contact.linkedinUrl,
    contact.linkedInUrl,
    contact.personSourceUrl,
    contact.sourceUrl,
    contact.profileUrl,
    contact.publicProfileUrl,
  ];
  for (const raw of candidates) {
    const u = text(raw);
    if (/^https?:\/\/([a-z0-9-]+\.)?linkedin\.com\/in\//i.test(u)) {
      return u.replace(/\/$/, "");
    }
  }
  return null;
}

function isIncompleteIdentity(name) {
  const n = text(name);
  if (!n) return { incomplete: true, reason: "MISSING_NAME" };
  const tokens = n.split(/\s+/);
  if (tokens.length === 1 && tokens[0].length <= 2) {
    return { incomplete: true, reason: "INITIALS_ONLY" };
  }
  if (tokens.length >= 2) {
    const last = tokens[tokens.length - 1];
    if (last.length === 1 || /^[A-Z]\.?$/i.test(last)) {
      return { incomplete: true, reason: "TRUNCATED_SURNAME" };
    }
  }
  if (!looksLikePersonName(n)) {
    return { incomplete: true, reason: "NOT_PERSON_SHAPE" };
  }
  return { incomplete: false, reason: null };
}

function channelResearchStatus(contact = {}) {
  const ccr = contact.contactChannelResearch || {};
  return {
    professionalEmail: ccr.professionalEmail || null,
    professionalTelephone: ccr.professionalTelephone || null,
    personalLinkedIn: ccr.personalLinkedIn || null,
    usableOutreachRoute: ccr.usableOutreachRoute === true,
  };
}

function classifyChannelGap(kind, contact, orgContext) {
  const id = isIncompleteIdentity(contact?.name);
  if (id.incomplete) {
    return { gap: GAP.NEEDS_IDENTITY_RESOLUTION, note: id.reason };
  }
  const crs = channelResearchStatus(contact);
  const statusKey =
    kind === "email"
      ? "professionalEmail"
      : kind === "phone"
        ? "professionalTelephone"
        : "personalLinkedIn";
  const raw = kind === "email" ? text(contact?.email) : kind === "phone" ? text(contact?.phone) : linkedInUrl(contact);
  const present = Boolean(raw && (kind !== "email" || raw.includes("@")));

  if (kind === "email") {
    const ch = classifyEmailChannel(contact || {});
    if (ch.verified && ch.email) return { gap: null, note: "VERIFIED" };
    if (present && !ch.verified) {
      return {
        gap:
          crs.professionalEmail === "FOUND"
            ? GAP.RESEARCHED_UNVERIFIED
            : GAP.NEEDS_TARGETED_ENRICHMENT,
        note: ch.status || "EMAIL_PRESENT_UNVERIFIED",
      };
    }
    if (crs.professionalEmail === "FOUND") {
      return { gap: GAP.EXISTS_NOT_DISPLAYED, note: "STAGE4_FOUND_NO_PRIMARY_FIELD" };
    }
    if (crs.professionalEmail === "NOT_FOUND_OR_UNAVAILABLE") {
      return { gap: GAP.GENUINE_UNAVAILABLE, note: "STAGE4_NOT_FOUND" };
    }
    if (crs.professionalEmail === "NOT_YET_RESEARCHED") {
      return { gap: GAP.NOT_RESEARCHED, note: "STAGE4_NOT_YET_RESEARCHED" };
    }
    return { gap: GAP.NEEDS_TARGETED_ENRICHMENT, note: "NO_EMAIL" };
  }

  if (kind === "phone") {
    const ch = classifyPhoneChannel(contact || {});
    if (ch.present && ch.verified) return { gap: null, note: "VERIFIED" };
    if (present) {
      return {
        gap: ch.verified ? null : GAP.RESEARCHED_UNVERIFIED,
        note: ch.status || "PHONE_PRESENT",
      };
    }
    if (crs.professionalTelephone === "FOUND") {
      return { gap: GAP.EXISTS_NOT_DISPLAYED, note: "STAGE4_FOUND_NO_PRIMARY_FIELD" };
    }
    if (crs.professionalTelephone === "NOT_FOUND_OR_UNAVAILABLE") {
      return { gap: GAP.GENUINE_UNAVAILABLE, note: "STAGE4_NOT_FOUND" };
    }
    if (crs.professionalTelephone === "NOT_YET_RESEARCHED") {
      return { gap: GAP.NOT_RESEARCHED, note: "STAGE4_NOT_YET_RESEARCHED" };
    }
    return { gap: GAP.NEEDS_TARGETED_ENRICHMENT, note: "NO_PHONE" };
  }

  // linkedin
  const li = linkedInUrl(contact);
  if (li) return { gap: null, note: "VERIFIED_OR_PRESENT" };
  if (crs.personalLinkedIn === "FOUND") {
    return { gap: GAP.EXISTS_NOT_DISPLAYED, note: "STAGE4_FOUND_NO_LINKEDINURL" };
  }
  if (crs.personalLinkedIn === "NOT_FOUND_OR_UNAVAILABLE") {
    return { gap: GAP.GENUINE_UNAVAILABLE, note: "STAGE4_NOT_FOUND" };
  }
  if (crs.personalLinkedIn === "NOT_YET_RESEARCHED") {
    return { gap: GAP.NOT_RESEARCHED, note: "STAGE4_NOT_YET_RESEARCHED" };
  }
  if (orgContext?.hasOrgRoute) {
    return { gap: GAP.NEEDS_TARGETED_ENRICHMENT, note: "ORG_ROUTE_ONLY" };
  }
  return { gap: GAP.NEEDS_TARGETED_ENRICHMENT, note: "NO_LINKEDIN" };
}

function personKey(name, org) {
  return `${text(name).toLowerCase()}|${text(org).toLowerCase()}`;
}

function loadStagedPackage(hotelId) {
  const stagedPtr = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/bundled-publication-packages",
        hotelId,
        "STAGED.json",
      ),
      "utf8",
    ),
  );
  const pkgPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
    stagedPtr.publicationVersion,
    "package.json",
  );
  const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  return { stagedPtr, pkg, pkgPath };
}

function drawerWhoSection(opp) {
  const scaling = opp.scalingDrawerModel?.sections?.who_to_contact || null;
  const events =
    opp.eventsDrawerModel?.sections?.who_to_contact ||
    opp.sharedTenSectionDrawer?.sections?.who_to_contact ||
    null;
  return scaling || events || null;
}

function findymailEligible(opp) {
  const name = text(opp.primaryContact?.name);
  const org = text(opp.organizationName || opp.displayCompanyName);
  const personOk = looksLikePersonName(name);
  const channel = evaluatePersonSpecificOutreachChannel(opp);
  const channelOk = channel?.ok === true || channel?.usable === true;
  const li = linkedInUrl(opp.primaryContact);
  const eligible =
    personOk && Boolean(org) && !channelOk && !li;
  return {
    eligible,
    reason: eligible
      ? "NAMED_PERSON_AND_ORG_VERIFIED_BUT_OUTREACH_ROUTE_UNRESOLVED"
      : !personOk
        ? "NO_VERIFIED_NAMED_PERSON"
        : channelOk
          ? "USABLE_PUBLIC_ROUTE_EXISTS"
          : li
            ? "LINKEDIN_ROUTE_EXISTS"
            : "ORG_OR_CHANNEL_INCOMPLETE",
    policy: {
      autoInvoke: false,
      persistProviderPiiByDefault: false,
    },
  };
}

function pickPackageContactFields(contact) {
  if (!contact) return null;
  return {
    fullName: text(contact.name || contact.fullName) || null,
    title: text(contact.title || contact.role) || null,
    org: text(contact.organization) || null,
    email: text(contact.email) || null,
    phone: text(contact.phone) || null,
    linkedinUrl: linkedInUrl(contact),
    sourceUrl: text(contact.sourceUrl) || null,
    emailOwnership: contact.emailOwnership || null,
    emailVerificationStatus: contact.emailVerificationStatus || null,
    phoneVerificationStatus: contact.phoneVerificationStatus || null,
    contactChannelResearch: channelResearchStatus(contact),
  };
}

function main() {
  const { stagedPtr, pkg, pkgPath } = loadStagedPackage(HOTEL_ID);
  const baseline = fs.existsSync(BASELINE_PATH)
    ? JSON.parse(fs.readFileSync(BASELINE_PATH, "utf8"))
    : null;

  const opportunities = pkg.opportunities || [];
  const findymailQueuePath = path.join(
    ROOT,
    "reports/gdi-wyndham-sd-bayside-agent-deep-restore-v1/05-FINDYMAIL_ELIGIBILITY_QUEUE.json",
  );
  const findymailQueue = fs.existsSync(findymailQueuePath)
    ? JSON.parse(fs.readFileSync(findymailQueuePath, "utf8"))
    : null;

  const publicRecoverPath = path.join(
    ROOT,
    "reports/gdi-contact-intelligence-v2-findymail-public-optimization/PUBLIC_RECOVERABILITY_EVAL.json",
  );
  const publicRecover = fs.existsSync(publicRecoverPath)
    ? JSON.parse(fs.readFileSync(publicRecoverPath, "utf8"))
    : null;

  const oppRows = [];
  const peopleMap = new Map();

  for (const opp of opportunities) {
    const listDto = toOpportunityListDto(opp);
    const drawerSec = drawerWhoSection(opp);
    const stage4 = opp.contactResearchStage || null;
    const orgRoute = Boolean(
      text(drawerSec?.organizationalRouteUrl) ||
        text(opp.contactUrl) ||
        text(opp.functionalContactEmail),
    );

    const slots = [
      { slot: "primary", contact: opp.primaryContact, list: listDto.primaryContact, drawer: drawerSec?.primaryContact || null },
      { slot: "secondary", contact: opp.secondaryContact, list: listDto.secondaryContact, drawer: drawerSec?.secondaryContact || null },
    ].filter((s) => s.contact && text(s.contact.name));

    const oppContacts = [];
    for (const s of slots) {
      const pkgF = pickPackageContactFields(s.contact);
      const gaps = {
        email: classifyChannelGap("email", s.contact, { hasOrgRoute: orgRoute }),
        phone: classifyChannelGap("phone", s.contact, { hasOrgRoute: orgRoute }),
        linkedin: classifyChannelGap("linkedin", s.contact, { hasOrgRoute: orgRoute }),
      };
      const identity = isIncompleteIdentity(s.contact.name);
      oppContacts.push({
        slot: s.slot,
        package: pkgF,
        listSlimPrimaryContact: s.slot === "primary" ? {
          fullName: s.list?.name || null,
          title: s.list?.title || s.list?.role || null,
          email: s.list?.email || null,
          phone: s.list?.phone || null,
          linkedinUrl: s.list?.linkedinUrl || null,
          sourceUrl: s.list?.sourceUrl || null,
          emailCustomerVerified: s.list?.emailCustomerVerified === true,
          phoneCustomerVerified: s.list?.phoneCustomerVerified === true,
        } : {
          fullName: s.list?.name || null,
          title: s.list?.title || s.list?.role || null,
          email: s.list?.email || null,
          phone: s.list?.phone || null,
          linkedinUrl: s.list?.linkedinUrl || null,
          sourceUrl: s.list?.sourceUrl || null,
        },
        drawerWhoToContactStub: s.drawer
          ? {
              name: text(s.drawer.name) || null,
              role: text(s.drawer.role) || null,
              company: text(s.drawer.company || s.drawer.organization) || null,
              sourceUrl: text(s.drawer.sourceUrl) || null,
              email: text(s.drawer.email) || null,
              phone: text(s.drawer.phone) || null,
              linkedinUrl: linkedInUrl(s.drawer),
            }
          : null,
        incompleteIdentity: identity,
        gaps,
      });

      const pk = personKey(s.contact.name, s.contact.organization || opp.organizationName);
      const prev = peopleMap.get(pk) || {
        personKey: pk,
        fullName: text(s.contact.name),
        organizations: new Set(),
        titles: new Set(),
        opportunityIds: [],
        slots: [],
        surfaces: { package: {}, listDto: {}, drawerStub: {} },
        gaps: { email: [], phone: [], linkedin: [] },
        incompleteIdentity: identity,
        findymailEligible: false,
      };
      prev.organizations.add(text(s.contact.organization || opp.organizationName));
      prev.titles.add(text(s.contact.title || s.contact.role));
      prev.opportunityIds.push(opp.id);
      prev.slots.push({ opportunityId: opp.id, slot: s.slot });
      for (const k of ["email", "phone", "linkedin"]) {
        if (gaps[k].gap) prev.gaps[k].push(gaps[k].gap);
      }
      prev.surfaces.package = { ...prev.surfaces.package, ...pkgF };
      if (s.slot === "primary" && s.list) {
        prev.surfaces.listDto = { ...prev.surfaces.listDto, ...oppContacts.at(-1).listSlimPrimaryContact };
      }
      peopleMap.set(pk, prev);
    }

    const fm = findymailEligible(opp);
    if (fm.eligible && opp.primaryContact?.name) {
      const pk = personKey(opp.primaryContact.name, opp.organizationName);
      const p = peopleMap.get(pk);
      if (p) p.findymailEligible = true;
    }

    oppRows.push({
      opportunityId: opp.id,
      displayTitle: text(opp.displayTitle || opp.title),
      organizationName: text(opp.organizationName),
      primaryContact: pickPackageContactFields(opp.primaryContact),
      secondaryContact: pickPackageContactFields(opp.secondaryContact),
      contacts: oppContacts,
      stage4ContactResearch: stage4
        ? {
            findymailUsed: stage4.findymailUsed === true,
            surfeUsed: stage4.surfeUsed === true,
            surfePersisted: stage4.surfePersisted === true,
            hydration: stage4.hydration || null,
            usableOutreach: stage4.usableOutreach || null,
            channelCount: (stage4.channels || []).length,
          }
        : null,
      findymailEligibilityPrimary: fm,
      drawerWhoToContact: drawerSec
        ? {
            contactPathLabel: text(drawerSec.contactPathLabel) || null,
            organizationalRouteUrl: text(drawerSec.organizationalRouteUrl) || null,
          }
        : null,
    });
  }

  const uniquePeople = [...peopleMap.values()].map((p) => ({
    personKey: p.personKey,
    fullName: p.fullName,
    title: [...p.titles].filter(Boolean).join(" | ") || null,
    org: [...p.organizations].filter(Boolean).join(" | ") || null,
    opportunityCount: p.opportunityIds.length,
    opportunityIds: p.opportunityIds,
    slots: p.slots,
    email: p.surfaces.package.email || p.surfaces.listDto.email || null,
    phone: p.surfaces.package.phone || p.surfaces.listDto.phone || null,
    linkedinUrl: p.surfaces.package.linkedinUrl || p.surfaces.listDto.linkedinUrl || null,
    sourceUrl: p.surfaces.package.sourceUrl || p.surfaces.listDto.sourceUrl || null,
    surfaces: p.surfaces,
    gapSummary: {
      email: summarizeGaps(p.gaps.email),
      phone: summarizeGaps(p.gaps.phone),
      linkedin: summarizeGaps(p.gaps.linkedin),
    },
    incompleteIdentity: p.incompleteIdentity,
    findymailEligible: p.findymailEligible,
  }));

  const counts = computeGapCounts(opportunities, uniquePeople);

  const report = {
    schema: "gdi-contact-enrichment-completion-audit-v1",
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL_ID,
    hotelName: pkg.hotelName || null,
    publicationVersion: stagedPtr.publicationVersion,
    packagePath: pkgPath.replace(/\\/g, "/"),
    baselineBefore: baseline?.before || baseline || null,
    coverageNow: counts.coverage,
    gapCounts: counts.gaps,
    opportunityCount: opportunities.length,
    uniquePersonCount: uniquePeople.length,
    findymail: {
      integrationModulePath:
        "NO_DEDICATED_FINDYMAIL_CLIENT_IN_REPO — eligibility/design only; Surfe client at lib/surfe/client.js; Findymail gated in lib/group-demand-intelligence/workflow/gdi-contact-research-stage-v1.js",
      authEnvVar: "FINDYMAIL_API_KEY (documented in reports/hotel-explorer/HE-FINDYMAIL-IMPLEMENTATION-AUDIT-V1.md; not in .env.example)",
      persistenceAllowed: false,
      persistencePolicy:
        "persistProviderPiiByDefault: false — provider email/phone not auto-persisted to STAGED/Airtable without founder authorization",
      autoInvoke: false,
      authorizationDocs: [
        "reports/gdi-wyndham-sd-bayside-agent-deep-restore-v1/06-FINDYMAIL_ELIGIBILITY_DESIGN.md",
        "docs/ai-build-system/GDI_STAGE4_CONTACT_RESEARCH_COMPLETION_V1.md",
        "reports/hotel-explorer/HE-FINDYMAIL-IMPLEMENTATION-AUDIT-V1.md",
      ],
      creditsDoc:
        "Findymail Basic $49/mo, 1,000 finder + 1,000 verification credits/mo, rollover up to 2× (06-FINDYMAIL_ELIGIBILITY_DESIGN.md)",
      priorEligibilityQueue: findymailQueue,
      publicRecoverabilityEvalSummary: publicRecover?.summary || null,
      eligiblePrimaryContactsNow: oppRows
        .filter((r) => r.findymailEligibilityPrimary.eligible)
        .map((r) => ({
          opportunityId: r.opportunityId,
          displayTitle: r.displayTitle,
          personName: r.primaryContact?.fullName,
          organizationName: r.organizationName,
          reason: r.findymailEligibilityPrimary.reason,
        })),
    },
    identityResolutionNeeded: uniquePeople
      .filter((p) => p.incompleteIdentity.incomplete)
      .map((p) => ({
        personKey: p.personKey,
        fullName: p.fullName,
        reason: p.incompleteIdentity.reason,
        opportunityIds: p.opportunityIds,
      })),
    recommendedNextActionsNoSpend: buildRecommendations(counts, uniquePeople, oppRows),
    opportunities: oppRows,
    uniquePeople,
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(
    path.join(OUT_DIR, "00-CONTACT_AUDIT.json"),
    `${JSON.stringify(report, null, 2)}\n`,
    "utf8",
  );

  const md = buildMarkdownSummary(report);
  fs.writeFileSync(path.join(OUT_DIR, "01-AUDIT_SUMMARY.md"), md, "utf8");
  console.log(`WROTE ${path.join(OUT_DIR, "00-CONTACT_AUDIT.json")}`);
  console.log(`WROTE ${path.join(OUT_DIR, "01-AUDIT_SUMMARY.md")}`);
  console.log(JSON.stringify(counts, null, 2));
}

function summarizeGaps(arr) {
  const out = {};
  for (const g of arr) {
    out[g] = (out[g] || 0) + 1;
  }
  const primary = Object.entries(out).sort((a, b) => b[1] - a[1])[0];
  return primary ? { primary: primary[0], breakdown: out } : { primary: null, breakdown: {} };
}

function hasCustomerEmail(opp, slot = "primary") {
  const c = slot === "primary" ? opp.primaryContact : opp.secondaryContact;
  if (!c) return false;
  return classifyEmailChannel(c).verified === true || Boolean(text(c.email));
}

function hasCustomerPhone(opp, slot = "primary") {
  const c = slot === "primary" ? opp.primaryContact : opp.secondaryContact;
  if (!c) return false;
  const ch = classifyPhoneChannel(c);
  return (ch.present && ch.verified) || Boolean(text(c.phone));
}

function hasLinkedIn(contact) {
  return Boolean(linkedInUrl(contact));
}

function computeGapCounts(opportunities, uniquePeople) {
  const cov = {
    opportunities: opportunities.length,
    primaryNamed: 0,
    primaryEmail: 0,
    primaryPhone: 0,
    primaryLinkedIn: 0,
    primaryEmailOrPhoneOrLinkedIn: 0,
    stage4Annotated: 0,
    uniquePeople: uniquePeople.length,
    peopleWithTitle: uniquePeople.filter((p) => text(p.title)).length,
    peopleWithEmail: uniquePeople.filter((p) => text(p.email)).length,
    peopleWithPhone: uniquePeople.filter((p) => text(p.phone)).length,
    peopleWithLinkedIn: uniquePeople.filter((p) => text(p.linkedinUrl)).length,
  };

  const gaps = {
    opportunityPrimary: { email: 0, phone: 0, linkedin: 0 },
    opportunityAnyContact: { email: 0, phone: 0, linkedin: 0 },
    uniquePerson: { email: 0, phone: 0, linkedin: 0 },
  };

  for (const opp of opportunities) {
    const p = opp.primaryContact;
    if (text(p?.name)) cov.primaryNamed += 1;
    if (hasCustomerEmail(opp, "primary")) cov.primaryEmail += 1;
    else gaps.opportunityPrimary.email += 1;
    if (hasCustomerPhone(opp, "primary")) cov.primaryPhone += 1;
    else gaps.opportunityPrimary.phone += 1;
    if (hasLinkedIn(p)) cov.primaryLinkedIn += 1;
    else gaps.opportunityPrimary.linkedin += 1;
  if (
      hasCustomerEmail(opp, "primary") ||
      hasCustomerPhone(opp, "primary") ||
      hasLinkedIn(p)
    ) {
      cov.primaryEmailOrPhoneOrLinkedIn += 1;
    }
    if (opp.contactResearchStage) cov.stage4Annotated += 1;

    const slots = [opp.primaryContact, opp.secondaryContact].filter((c) => c && text(c.name));
    for (const c of slots) {
      const em = classifyEmailChannel(c);
      if (!em.verified && !text(c.email)) gaps.opportunityAnyContact.email += 1;
      const ph = classifyPhoneChannel(c);
      if (!(ph.present && ph.verified) && !text(c.phone)) gaps.opportunityAnyContact.phone += 1;
      if (!hasLinkedIn(c)) gaps.opportunityAnyContact.linkedin += 1;
    }
  }

  for (const p of uniquePeople) {
    if (!text(p.email)) gaps.uniquePerson.email += 1;
    if (!text(p.phone)) gaps.uniquePerson.phone += 1;
    if (!text(p.linkedinUrl)) gaps.uniquePerson.linkedin += 1;
  }

  return { coverage: cov, gaps };
}

function buildRecommendations(counts, uniquePeople, oppRows) {
  const notResearched = oppRows.filter(
    (r) =>
      r.primaryContact?.contactChannelResearch?.professionalEmail ===
        "NOT_YET_RESEARCHED" ||
      r.primaryContact?.contactChannelResearch?.professionalTelephone ===
        "NOT_YET_RESEARCHED",
  ).length;
  const findymailEligible = oppRows.filter((r) => r.findymailEligibilityPrimary.eligible).length;
  const identity = uniquePeople.filter((p) => p.incompleteIdentity.incomplete);

  return [
    {
      action: "Run Stage 4 public HOW (Exa/public directories) for email/phone gaps",
      scope: `${notResearched} opportunities still marked NOT_YET_RESEARCHED on at least one channel`,
      spend: 0,
      note: "Align with reports/gdi-contact-research-hydration-v1/05-TARGETED_CONTACT_RESEARCH_MANIFEST.json — no Findymail/Surfe auto.",
    },
    {
      action: "Resolve incomplete person identities before enrichment",
      scope: identity.map((p) => p.fullName).join(", ") || "none",
      spend: 0,
    },
    {
      action: "Founder-authorized Findymail batch (if pursued)",
      scope: `${findymailEligible} primary contacts meet strict eligibility (named+org, no usable route, no LinkedIn)`,
      spend: "credits",
      note: "Queue from prior restore (2 rows) may be stale vs current STAGED tip — reconcile against 00-CONTACT_AUDIT.json findymail.eligiblePrimaryContactsNow.",
    },
    {
      action: "Verify list vs package LinkedIn mapping",
      scope: "Any EXISTS_NOT_DISPLAYED linkedin gaps",
      spend: 0,
    },
  ];
}

function buildMarkdownSummary(report) {
  const c = report.gapCounts;
  const cov = report.coverageNow;
  const b = report.baselineBefore;
  return `# Wyndham San Diego Bayside — contact enrichment completion audit

**Hotel:** ${report.hotelName} (\`${report.hotelId}\`)  
**STAGED publication:** \`${report.publicationVersion}\`  
**Generated:** ${report.generatedAt}

## Coverage (vs hydration baseline)

| Metric | Before (baseline) | Now |
|---:|---:|---:|
| Opportunities | ${b?.opportunities ?? "—"} | ${cov.opportunities} |
| Primary named | ${b?.namedOps ?? "—"} | ${cov.primaryNamed} |
| Primary email | ${b?.emailOps ?? "—"} | ${cov.primaryEmail} |
| Primary phone | ${b?.phoneOps ?? "—"} | ${cov.primaryPhone} |
| Primary LinkedIn | ${b?.liOps ?? "—"} | ${cov.primaryLinkedIn} |
| Unique people | ${b?.uniquePeople ?? "—"} | ${cov.uniquePeople} |
| Stage 4 annotated | ${b?.stage4Annotated ?? "—"} | ${cov.stage4Annotated} |

## Gap counts

### Opportunity grain (primary contact)

- Email gaps: **${c.opportunityPrimary.email}**
- Phone gaps: **${c.opportunityPrimary.phone}**
- LinkedIn gaps: **${c.opportunityPrimary.linkedin}**

### Opportunity grain (primary + secondary named contacts)

- Email gaps: **${c.opportunityAnyContact.email}**
- Phone gaps: **${c.opportunityAnyContact.phone}**
- LinkedIn gaps: **${c.opportunityAnyContact.linkedin}**

### Unique person grain (${cov.uniquePeople} people)

- Email gaps: **${c.uniquePerson.email}**
- Phone gaps: **${c.uniquePerson.phone}**
- LinkedIn gaps: **${c.uniquePerson.linkedin}**

## Identity resolution

${report.identityResolutionNeeded.length ? report.identityResolutionNeeded.map((r) => `- **${r.fullName}** (${r.reason}) — ${r.opportunityIds.join(", ")}`).join("\n") : "_None flagged._"}

## Findymail (read-only policy)

- **Client module:** ${report.findymail.integrationModulePath}
- **Auth:** \`${report.findymail.authEnvVar}\`
- **Persistence:** ${report.findymail.persistenceAllowed ? "allowed" : "**not allowed by default**"} (${report.findymail.persistencePolicy})
- **Eligible primaries (current package):** ${report.findymail.eligiblePrimaryContactsNow.length}
${report.findymail.eligiblePrimaryContactsNow.map((r) => `  - ${r.personName} — ${r.displayTitle} (${r.reason})`).join("\n")}

## Recommended next actions (no spend)

${report.recommendedNextActionsNoSpend.map((a) => `1. **${a.action}** — ${a.scope}${a.note ? ` _(${a.note})_` : ""}`).join("\n")}

Full per-opportunity and per-person inventory: \`00-CONTACT_AUDIT.json\`.
`;
}

main();