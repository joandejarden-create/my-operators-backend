#!/usr/bin/env node
/**
 * GDI Gold Standard Certification completion — Wyndham continuation.
 *
 * Completes outstanding operational items after PR #656 PARTIAL:
 *  - Attribution-safe Findymail → Airtable persist (dry-run / --apply)
 *  - alternativeEntryPoints hydration from secondaryContact (drawer-visible)
 *  - Remaining Findymail email gaps (--findymail-live)
 *  - Stage 4 channel completion ledger (email / phone / LinkedIn)
 *  - Airtable read-back + non-publishing reconstruction parity check
 *
 *   node scripts/gdi-gold-standard-cert-completion-v1.mjs
 *   node scripts/gdi-gold-standard-cert-completion-v1.mjs --airtable-dry-run
 *   node scripts/gdi-gold-standard-cert-completion-v1.mjs --airtable-apply --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
 *   node scripts/gdi-gold-standard-cert-completion-v1.mjs --hydrate-alt-entry --apply-staged --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
 *   node scripts/gdi-gold-standard-cert-completion-v1.mjs --findymail-live --credit-hard-cap 15
 *
 * No Surfe. No CURRENT. No UI changes. No unauthorized schema.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const AUTH = "FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127";
const OUT = path.join(
  ROOT,
  "reports/gdi-canonical-full-e2e-gold-standard-certification-v1",
);
const STORE_ROOT = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
);

function flag(n) {
  return process.argv.includes(n);
}
function arg(n, fb = null) {
  const i = process.argv.indexOf(n);
  return i >= 0 ? process.argv[i + 1] || fb : fb;
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(p, `${JSON.stringify(data, null, 2)}\n`);
  return p;
}

function loadEnv() {
  for (const envPath of [
    path.join(ROOT, ".env"),
    path.join(ROOT, "../my-operators-backend/.env"),
    "C:/Dev/dealality/my-operators-backend/.env",
  ]) {
    if (!fs.existsSync(envPath)) continue;
    for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
      const m = line.match(
        /^(FINDYMAIL_API_KEY|AIRTABLE_API_KEY|AIRTABLE_PAT|AIRTABLE_BASE_ID|AIRTABLE_BASE_ID_ALT|AIRTABLE_GDI_BASE_ID)=(.*)$/,
      );
      if (!m) continue;
      if (!process.env[m[1]]) {
        process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
      }
    }
  }
}

function readStagedPointer() {
  return JSON.parse(
    fs.readFileSync(path.join(STORE_ROOT, HOTEL_ID, "STAGED.json"), "utf8"),
  );
}

function loadPackage(version) {
  const p = path.join(STORE_ROOT, HOTEL_ID, version, "package.json");
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function linkedinOf(c = {}) {
  const u = text(c.linkedinUrl || c.sourceUrl);
  return /linkedin\.com\/in\//i.test(u) ? u : "";
}

function score(opportunities) {
  let email = 0;
  let phone = 0;
  let linkedin = 0;
  let secondary = 0;
  let altArr = 0;
  let directBuyer = 0;
  let unspecifiedNoAlt = 0;
  for (const o of opportunities) {
    const c = o.primaryContact || {};
    if (text(c.email)) email += 1;
    if (text(c.phone)) phone += 1;
    if (linkedinOf(c)) linkedin += 1;
    if (text(o.secondaryContact?.name)) secondary += 1;
    const alts = o.alternativeEntryPoints;
    if (Array.isArray(alts) && alts.some((a) => text(a?.name || a?.fullName))) {
      altArr += 1;
    }
    const buyer = text(c.buyerVsEntry);
    if (buyer === "DIRECT_BUYER") directBuyer += 1;
    if (
      (buyer === "UNSPECIFIED" || !buyer) &&
      !text(o.secondaryContact?.name) &&
      !(Array.isArray(alts) && alts.length)
    ) {
      unspecifiedNoAlt += 1;
    }
  }
  return {
    opportunityCount: opportunities.length,
    email,
    phone,
    linkedin,
    secondary,
    altEntryPointsArray: altArr,
    directBuyer,
    unspecifiedWithoutAlt: unspecifiedNoAlt,
  };
}

function buildAltEntryHydration(opportunities) {
  let hydrated = 0;
  const changes = [];
  const next = opportunities.map((o) => {
    const existing = Array.isArray(o.alternativeEntryPoints)
      ? o.alternativeEntryPoints
      : [];
    if (existing.some((a) => text(a?.name || a?.fullName))) return o;
    const s = o.secondaryContact;
    if (!text(s?.name)) return o;
    const entry = {
      name: text(s.name || s.fullName),
      fullName: text(s.fullName || s.name),
      title: text(s.title || s.role || s.professionalTitle),
      role: text(s.role || s.title),
      organization: text(s.organization),
      email: text(s.email) || null,
      phone: text(s.phone) || null,
      linkedinUrl: linkedinOf(s) || null,
      sourceUrl: text(s.sourceUrl) || null,
      whyThisContact: text(s.whyThisContact || s.commercialRelevance) || null,
      commercialClass: s.commercialClass || "commercial_influencer_or_referral",
      contactRelevanceLabel: s.contactRelevanceLabel || "CONTACT_RELEVANT",
      contractingAuthorityStatus:
        s.contractingAuthorityStatus || "CONTRACTING_AUTHORITY_NOT_VERIFIED",
      buyingAuthorityStatus: "NOT_VERIFIED_LODGING_DECISION_MAKER",
      relationshipToOpportunity:
        text(s.relationshipToOpportunity) ||
        "Alternative entry point / buyer influencer",
      recommendedSalesApproach:
        text(s.recommendedSalesApproach) ||
        "Use as referral or operational entry when direct lodging authority is not verified.",
      evidenceUrls: Array.isArray(s.evidenceUrls) ? s.evidenceUrls : [],
    };
    hydrated += 1;
    changes.push({
      opportunityId: o.id,
      secondaryName: entry.name,
      commercialClass: entry.commercialClass,
    });
    return { ...o, alternativeEntryPoints: [entry] };
  });
  return { opportunities: next, hydrated, changes };
}

function stage4ChannelLedger(opportunities) {
  return opportunities.map((o) => {
    const c = o.primaryContact || {};
    const crs = o.contactResearchStage || {};
    const channels = Array.isArray(c.channels) ? c.channels : [];
    const channelType = (t) =>
      channels.find((ch) =>
        String(ch.channelType || ch.type || "")
          .toUpperCase()
          .includes(t),
      );
    const emailCh = channelType("EMAIL");
    const phoneCh = channelType("PHONE") || channelType("TELEPHONE");
    const liCh = channelType("LINKEDIN");
    return {
      opportunityId: o.id,
      name: text(c.name),
      email: {
        value: text(c.email) || null,
        researchStatus: text(c.email)
          ? "VERIFIED_OR_PROVIDER_RETURNED"
          : emailCh?.verificationStatus ||
            crs.emailResearchStatus ||
            "RESEARCH_NOT_DOCUMENTED_OR_GAP",
        findymailUsed: crs.findymailUsed === true,
      },
      phone: {
        value: text(c.phone) || null,
        researchStatus: text(c.phone)
          ? text(c.phoneTypeLabel) || "PRESENT"
          : phoneCh?.verificationStatus ||
            crs.phoneResearchStatus ||
            "RESEARCH_NOT_DOCUMENTED_OR_GAP",
        note: "Findymail is email-only; phone requires public/Exa research",
      },
      linkedin: {
        value: linkedinOf(c) || null,
        researchStatus: linkedinOf(c)
          ? "PERSONAL_PROFILE_PRESENT"
          : liCh?.verificationStatus ||
            crs.linkedinResearchStatus ||
            "RESEARCH_NOT_DOCUMENTED_OR_GAP",
      },
      secondaryVisible: Boolean(text(o.secondaryContact?.name)),
      altEntryPointsCount: Array.isArray(o.alternativeEntryPoints)
        ? o.alternativeEntryPoints.filter((a) => text(a?.name)).length
        : 0,
      buyerVsEntry: text(c.buyerVsEntry) || null,
    };
  });
}

async function main() {
  loadEnv();
  const pointer = readStagedPointer();
  const version =
    pointer.publicationVersion || pointer.publicationId || pointer.version;
  const pkg = loadPackage(version);
  const opportunities = Array.isArray(pkg.opportunities) ? pkg.opportunities : [];
  const before = score(opportunities);

  const {
    emailLikelyMatchesPerson,
    enrichPersonEmailWithFindymail,
    buildFindymailEnrichmentManifest,
    FINDYMAIL_STAGE4_AUTH_TOKEN,
  } = await import(
    pathToFileURL(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js",
      ),
    ).href
  );
  const {
    persistFindymailContactsToAirtable,
    buildFindymailAirtableContactPatch,
  } = await import(
    pathToFileURL(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/persistence/gdi-findymail-airtable-contact-persist-v1.js",
      ),
    ).href
  );
  const {
    isGdiOpportunityAirtableConfigured,
    listOpportunitiesForHotel,
  } = await import(
    pathToFileURL(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/airtable-opportunity-store.js",
      ),
    ).href
  );

  // ——— 1) Airtable write set from STAGED emails (Findymail-attributed + existing) ———
  const patches = [];
  for (const o of opportunities) {
    const c = o.primaryContact || {};
    const email = text(c.email);
    if (!email) continue;
    const name = text(c.name || c.fullName);
    if (!emailLikelyMatchesPerson(email, name)) continue;
    const fromFindymail =
      c.emailVerificationStatus === "FINDYMAIL_PROVIDER_RETURNED" ||
      (Array.isArray(c.channels) &&
        c.channels.some((ch) => /FINDYMAIL/i.test(ch.source || "")));
    patches.push({
      opportunity: {
        ...o,
        hotelId: HOTEL_ID,
        id: o.id || o.opportunityId,
      },
      enrichment: {
        person: { ...c, email },
        channel: {
          value: email,
          source: fromFindymail ? "FINDYMAIL" : "STAGED_EXISTING",
        },
      },
      fromFindymail,
    });
  }

  const airtableDry = await persistFindymailContactsToAirtable(patches, {
    dryRun: true,
    hotelId: HOTEL_ID,
  });
  writeJson("12-AIRTABLE_WRITE_SET_DRY_RUN.json", {
    configured: isGdiOpportunityAirtableConfigured(),
    wouldWrite: airtableDry.wouldWrite,
    findymailAttributed: patches.filter((p) => p.fromFindymail).length,
    rows: airtableDry.rows?.map((r) => ({
      opportunityId: r.opportunityId,
      skipped: r.skipped,
      reason: r.reason || null,
      email: r.email || null,
    })),
  });

  let airtableApply = null;
  if (flag("--airtable-apply")) {
    if (arg("--auth") !== AUTH && arg("--auth") !== FINDYMAIL_STAGE4_AUTH_TOKEN) {
      throw new Error(`--airtable-apply requires --auth ${AUTH}`);
    }
    if (!isGdiOpportunityAirtableConfigured()) {
      throw new Error("Airtable not configured");
    }
    airtableApply = await persistFindymailContactsToAirtable(patches, {
      dryRun: false,
      apply: true,
      hotelId: HOTEL_ID,
      runId: `gold_std_findymail_persist_${Date.now()}`,
    });
    writeJson("13-AIRTABLE_WRITE_APPLY.json", {
      written: airtableApply.written,
      resultSummary: {
        ok: airtableApply.result?.ok ?? null,
        upserted: airtableApply.result?.upserted ?? airtableApply.result?.written ?? null,
        errors: airtableApply.result?.errors?.length ?? 0,
      },
    });
  }

  // ——— 2) Airtable read-back (best effort) ———
  let airtableReadBack = {
    attempted: false,
    configured: isGdiOpportunityAirtableConfigured(),
  };
  if (isGdiOpportunityAirtableConfigured()) {
    try {
      airtableReadBack.attempted = true;
      let records = [];
      if (typeof listOpportunitiesForHotel === "function") {
        records = await listOpportunitiesForHotel(HOTEL_ID);
      } else {
        airtableReadBack.note =
          "listOpportunitiesForHotel not exported — used write-set dry-run only";
      }
      const list = Array.isArray(records)
        ? records
        : records?.opportunities || records?.records || [];
      const byEmail = new Set();
      for (const r of list) {
        const f = r.fields || r;
        const e =
          text(f.primaryContactEmail) ||
          text(r.primaryContactEmail) ||
          text(r.primaryContact?.email);
        if (e) byEmail.add(e.toLowerCase());
      }
      const stagedEmails = patches
        .map((p) => text(p.enrichment.channel.value).toLowerCase())
        .filter(Boolean);
      const missingInAirtable = stagedEmails.filter((e) => !byEmail.has(e));
      airtableReadBack = {
        ...airtableReadBack,
        recordCount: list.length,
        emailsInAirtable: byEmail.size,
        stagedEmailsWithPersonMatch: stagedEmails.length,
        missingInAirtableCount: missingInAirtable.length,
        missingInAirtableSample: missingInAirtable.slice(0, 12),
        parity:
          missingInAirtable.length === 0 && stagedEmails.length > 0
            ? "STAGED_EMAILS_PRESENT_IN_AIRTABLE"
            : "GAPS_REMAIN",
      };
    } catch (err) {
      airtableReadBack.error = String(err.message || err).slice(0, 300);
      airtableReadBack.code = err.code || null;
    }
  }
  writeJson("14-AIRTABLE_READBACK.json", airtableReadBack);

  // ——— 3) Alt-entry hydration from secondaryContact ———
  const alt = buildAltEntryHydration(opportunities);
  writeJson("15-ALT_ENTRY_HYDRATION_PLAN.json", {
    hydrated: alt.hydrated,
    changes: alt.changes,
    note: "Maps existing secondaryContact into alternativeEntryPoints for package/API parity. Does not invent contacts. Drawer already shows secondaryContact.",
  });

  let stagedAfter = version;
  let opportunitiesWorking = opportunities;
  if (flag("--hydrate-alt-entry") && flag("--apply-staged")) {
    if (arg("--auth") !== AUTH) {
      throw new Error(`--apply-staged requires --auth ${AUTH}`);
    }
    const { createGdiPublicationPackageStore } = await import(
      pathToFileURL(
        path.join(
          ROOT,
          "lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js",
        ),
      ).href
    );
    const { fingerprintPublicationContent } = await import(
      pathToFileURL(
        path.join(
          ROOT,
          "lib/group-demand-intelligence/publication-package/gdi-customer-publication-package-v1.js",
        ),
      ).href
    );
    const store = createGdiPublicationPackageStore({ rootDir: STORE_ROOT });
    const shortFp = crypto
      .createHash("sha256")
      .update(JSON.stringify(alt.changes))
      .digest("hex")
      .slice(0, 8);
    const newVersion = `gdi_hotel_wide_fqo_${HOTEL_ID}_20261127_${shortFp}`;
    const nextPkgDraft = {
      ...pkg,
      publicationVersion: newVersion,
      publicationId: newVersion,
      createdAt: new Date().toISOString(),
      opportunities: alt.opportunities,
      opportunityCount: alt.opportunities.length,
      inventoryIds: alt.opportunities.map((o) => o.opportunityId || o.id),
      sortOrder: alt.opportunities.map((o) => o.opportunityId || o.id),
      gates: {
        ...(pkg.gates || {}),
        altEntryHydrationFromSecondary: {
          contract: "gdi-gold-standard-cert-completion-v1",
          sourceVersion: version,
          hydrated: alt.hydrated,
          authorizationId: AUTH,
        },
      },
    };
    const fpBody = fingerprintPublicationContent(nextPkgDraft);
    const nextPkg = {
      ...nextPkgDraft,
      contentFingerprintSha256: fpBody.contentFingerprintSha256,
    };
    const staged = store.stageForProduction(nextPkg);
    if (!staged.ok) {
      throw new Error(`stageForProduction failed: ${JSON.stringify(staged)}`);
    }
    stagedAfter = newVersion;
    opportunitiesWorking = alt.opportunities;
    writeJson("16-ALT_ENTRY_STAGED_APPLIED.json", {
      priorVersion: version,
      newVersion,
      hydrated: alt.hydrated,
      stagedPointer: store.readPointer(HOTEL_ID, "STAGED"),
    });
  } else {
    opportunitiesWorking = alt.opportunities; // plan-only score uses hydrated view
  }

  // ——— 4) Remaining Findymail live ———
  let findymailLive = null;
  if (flag("--findymail-live")) {
    const cap = Number(arg("--credit-hard-cap", "15"));
    const manifest = buildFindymailEnrichmentManifest(opportunitiesWorking, {
      creditHardCap: cap,
      repoRoot: ROOT,
    });
    findymailLive = {
      mode: "LIVE",
      selected: [],
      found: 0,
      notFound: 0,
      skipped: 0,
      creditsDebitedEstimate: 0,
      errors: [],
    };
    const byId = new Map(opportunitiesWorking.map((o) => [o.id, o]));
    for (const row of manifest.selected) {
      const opp = byId.get(row.opportunityIds[0]);
      if (!opp) {
        findymailLive.skipped += 1;
        continue;
      }
      const base = opp.primaryContact || {};
      const samePrimary =
        text(base.name || base.fullName).toLowerCase() ===
        text(row.name).toLowerCase();
      const person = {
        name: row.name,
        fullName: row.name,
        organization: row.organization,
        title: samePrimary ? base.title || base.role : null,
        email: samePrimary ? base.email || null : null,
        phone: samePrimary ? base.phone || null : null,
        linkedinUrl: samePrimary ? base.linkedinUrl || null : null,
      };
      try {
        const enriched = await enrichPersonEmailWithFindymail(person, opp, {
          dryRun: false,
          allowFindymail: true,
          hotelId: HOTEL_ID,
          authorizationId: FINDYMAIL_STAGE4_AUTH_TOKEN,
          repoRoot: ROOT,
          persistFindymailResults: false,
        });
        findymailLive.selected.push({
          personKey: row.personKey,
          opportunityId: opp.id,
          outcome: enriched.outcome,
          email: enriched.person?.email || null,
          domain: row.domain,
          attributionOk: enriched.person?.email
            ? emailLikelyMatchesPerson(enriched.person.email, row.name)
            : null,
        });
        if (enriched.outcome === "FOUND_VERIFICATION_REQUIRED") {
          findymailLive.found += 1;
          findymailLive.creditsDebitedEstimate += 1;
        } else if (enriched.outcome === "RESEARCH_COMPLETED_NOT_FOUND") {
          findymailLive.notFound += 1;
        } else {
          findymailLive.skipped += 1;
        }
      } catch (err) {
        findymailLive.errors.push({
          personKey: row.personKey,
          message: String(err.message || err).slice(0, 200),
          code: err.code || null,
        });
      }
    }
    writeJson("17-FINDYMAIL_REMAINING_LIVE.json", findymailLive);
  }

  // ——— 5) Stage 4 channel ledger + reconstruction note ———
  const ledger = stage4ChannelLedger(opportunitiesWorking);
  writeJson("18-STAGE4_CHANNEL_LEDGER.json", { rows: ledger });

  const after = score(opportunitiesWorking);
  const reconstruction = {
    method: "AIRTABLE_PLUS_APPROVED_DURABLE_STORES_REQUIRED",
    stagedAsSourceProhibited: true,
    priorInsufficientTest:
      "03-RECONSTRUCTION_TEST.json only proved bundled STAGED loadability",
    airtableConfigured: isGdiOpportunityAirtableConfigured(),
    airtableRecordCount: airtableReadBack.recordCount ?? null,
    airtableEmailParity: airtableReadBack.parity || null,
    isolatedRebuildExecuted: false,
    blocker:
      airtableReadBack.parity === "STAGED_EMAILS_PRESENT_IN_AIRTABLE"
        ? "Need isolated package rebuild from Airtable research inventory + GDO without reading STAGED tip as source"
        : "Airtable contact/opportunity coverage incomplete or unread — cannot certify reconstruction yet",
    durableStores: [
      "Group Demand Opportunities (embedded primaryContact*)",
      "GDI Hotel Research Opportunities (researchPayloadJson)",
      "bundled publication packages (immutable STAGED/CURRENT pointers — output only)",
    ],
  };
  writeJson("19-RECONSTRUCTION_FROM_AIRTABLE.json", reconstruction);

  const outstanding = [];
  if (!airtableApply) {
    outstanding.push(
      "Airtable live persist not applied this run — dry-run write set in 12-AIRTABLE_WRITE_SET_DRY_RUN.json; apply with --airtable-apply --auth " +
        AUTH,
    );
  } else if (airtableReadBack.parity !== "STAGED_EMAILS_PRESENT_IN_AIRTABLE") {
    outstanding.push(
      `Airtable read-back parity incomplete: ${airtableReadBack.parity || airtableReadBack.error || "unknown"}`,
    );
  }
  if (!flag("--apply-staged")) {
    outstanding.push(
      `Alt-entry array hydration planned (${alt.hydrated}) but not applied to STAGED — run --hydrate-alt-entry --apply-staged --auth ${AUTH}`,
    );
  }
  if (after.email < after.opportunityCount) {
    outstanding.push(
      `Primary email gaps: ${after.opportunityCount - after.email}/${after.opportunityCount}`,
    );
  }
  if (after.phone < after.opportunityCount) {
    outstanding.push(
      `Primary phone gaps: ${after.opportunityCount - after.phone}/${after.opportunityCount} (public/Exa research required; Findymail email-only)`,
    );
  }
  if (after.linkedin < after.opportunityCount) {
    outstanding.push(
      `Primary LinkedIn gaps: ${after.opportunityCount - after.linkedin}/${after.opportunityCount}`,
    );
  }
  if (after.unspecifiedWithoutAlt > 0) {
    outstanding.push(
      `WHO/HOW unspecified without alt/secondary: ${after.unspecifiedWithoutAlt} — recover settled WHO/HOW or complete mandatory alt-entry research`,
    );
  }
  if (!reconstruction.isolatedRebuildExecuted) {
    outstanding.push(
      "Genuine Airtable→package reconstruction not yet executed in isolated non-publishing context",
    );
  }
  if (!flag("--findymail-live") && after.email < after.opportunityCount) {
    outstanding.push(
      "Remaining Findymail live not run — --findymail-live --credit-hard-cap 15",
    );
  }

  const status =
    outstanding.length === 0
      ? "GDI_CANONICAL_FULL_E2E_GOLD_STANDARD_CERTIFIED — INTEGRATED_ON_8080 — AWAITING_FOUNDER_REVIEW"
      : "PARTIAL";

  const summary = {
    schema: "gdi-gold-standard-cert-completion-v1",
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL_ID,
    stagedBefore: version,
    stagedAfter,
    status,
    before,
    after,
    airtableDryWouldWrite: airtableDry.wouldWrite,
    airtableApplied: Boolean(airtableApply),
    airtableReadBack,
    altEntryHydrated: alt.hydrated,
    findymailLive: findymailLive
      ? {
          found: findymailLive.found,
          notFound: findymailLive.notFound,
          credits: findymailLive.creditsDebitedEstimate,
          errors: findymailLive.errors.length,
        }
      : null,
    reconstruction,
    outstanding,
  };
  writeJson("20-COMPLETION_SUMMARY.json", summary);

  const md = `# GDI Gold Standard Completion — Wyndham

**Status:** \`${status}\`  
**STAGED before:** \`${version}\`  
**STAGED after:** \`${stagedAfter}\`  
**Generated:** ${summary.generatedAt}

## Scorecard
| Metric | Before | After (working set) |
|---|---:|---:|
| Opportunities | ${before.opportunityCount} | ${after.opportunityCount} |
| Primary email | ${before.email} | ${after.email} |
| Primary phone | ${before.phone} | ${after.phone} |
| Primary LinkedIn | ${before.linkedin} | ${after.linkedin} |
| Secondary contacts | ${before.secondary} | ${after.secondary} |
| alternativeEntryPoints[] | ${before.altEntryPointsArray} | ${after.altEntryPointsArray} |

## Airtable
- Configured: **${isGdiOpportunityAirtableConfigured()}**
- Dry-run would write: **${airtableDry.wouldWrite}**
- Applied: **${Boolean(airtableApply)}**
- Read-back: **${airtableReadBack.parity || airtableReadBack.error || "n/a"}**

## Outstanding
${outstanding.map((x) => `- ${x}`).join("\n") || "- None"}

## Commands
\`\`\`powershell
node scripts/gdi-gold-standard-cert-completion-v1.mjs --airtable-dry-run
node scripts/gdi-gold-standard-cert-completion-v1.mjs --airtable-apply --auth ${AUTH}
node scripts/gdi-gold-standard-cert-completion-v1.mjs --hydrate-alt-entry --apply-staged --auth ${AUTH}
node scripts/gdi-gold-standard-cert-completion-v1.mjs --findymail-live --credit-hard-cap 15
\`\`\`
`;
  fs.writeFileSync(path.join(OUT, "20-COMPLETION_FOUNDER.md"), md);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
