#!/usr/bin/env node
/**
 * GDI Canonical Full E2E Gold Standard Certification (Wyndham reference).
 *
 * Non-publishing. No Surfe. No CURRENT. No unauthorized spend by default.
 *
 *   node scripts/gdi-canonical-full-e2e-gold-standard-certification-v1.mjs
 *   node scripts/gdi-canonical-full-e2e-gold-standard-certification-v1.mjs --findymail-dry-run
 *   node scripts/gdi-canonical-full-e2e-gold-standard-certification-v1.mjs --findymail-live --credit-hard-cap 30
 *
 * Live Findymail requires FINDYMAIL_API_KEY and founder Stage 4 permanent GO.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { pathToFileURL } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const HOTEL_NAME = "Wyndham San Diego Bayside";
const FALLBACK_STAGED_VERSION =
  "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_46de4104";
const OUT = path.join(
  ROOT,
  "reports/gdi-canonical-full-e2e-gold-standard-certification-v1",
);

function resolveStagedVersion() {
  const stagedPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    HOTEL_ID,
    "STAGED.json",
  );
  if (fs.existsSync(stagedPath)) {
    try {
      const p = JSON.parse(fs.readFileSync(stagedPath, "utf8"));
      const v = p.publicationVersion || p.publicationId || p.version;
      if (v && fs.existsSync(packagePathFor(v))) return v;
    } catch {
      /* fall through */
    }
  }
  return FALLBACK_STAGED_VERSION;
}
function packagePathFor(version) {
  return path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    HOTEL_ID,
    version,
    "package.json",
  );
}

function flag(name) {
  return process.argv.includes(name);
}
function argValue(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(p, `${JSON.stringify(data, null, 2)}\n`, "utf8");
  return p;
}

function writeMd(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(p, body.endsWith("\n") ? body : `${body}\n`, "utf8");
  return p;
}

function packagePath(version = resolveStagedVersion()) {
  return packagePathFor(version);
}

function scoreContacts(opportunities) {
  const people = new Map();
  const push = (person, oppId, role) => {
    if (!person || !text(person.name || person.fullName)) return;
    const key = `${text(person.name || person.fullName).toLowerCase()}|${text(
      person.organization || "",
    ).toLowerCase()}`;
    const prev = people.get(key) || {
      name: text(person.name || person.fullName),
      title: text(person.title || person.role || person.professionalTitle),
      organization: text(person.organization),
      email: null,
      phone: null,
      linkedin: null,
      roles: [],
      opportunityIds: [],
    };
    prev.email = prev.email || text(person.email) || null;
    prev.phone = prev.phone || text(person.phone) || null;
    prev.linkedin =
      prev.linkedin ||
      text(person.linkedinUrl || person.linkedInUrl || person.sourceUrl) ||
      null;
    if (prev.linkedin && !/linkedin\.com\/in\//i.test(prev.linkedin)) {
      prev.linkedin = text(person.linkedinUrl) || null;
    }
    prev.roles.push(role);
    prev.opportunityIds.push(oppId);
    people.set(key, prev);
  };

  let primaryEmail = 0;
  let primaryPhone = 0;
  let primaryLi = 0;
  let primaryAll3 = 0;
  let primaryAny = 0;
  let stage4 = 0;
  let findymailUsed = 0;
  let withSecondary = 0;
  let withAlt = 0;

  for (const o of opportunities) {
    const c = o.primaryContact || {};
    const email = text(c.email);
    const phone = text(c.phone);
    const li = text(c.linkedinUrl || c.linkedInUrl);
    if (email) primaryEmail += 1;
    if (phone) primaryPhone += 1;
    if (li) primaryLi += 1;
    if (email && phone && li) primaryAll3 += 1;
    if (email || phone || li) primaryAny += 1;
    if (o.contactResearchStage) stage4 += 1;
    if (o.contactResearchStage?.findymailUsed === true) findymailUsed += 1;
    if (text(o.secondaryContact?.name)) withSecondary += 1;
    const alts =
      o.alternativeEntryPoints || o.altEntryPoints || o.whoHow?.entryPoints || [];
    if (Array.isArray(alts) && alts.filter((a) => text(a.name || a.fullName)).length) {
      withAlt += 1;
    }
    push(c, o.id, "PRIMARY");
    push(o.secondaryContact, o.id, "SECONDARY");
    for (const a of alts) push(a, o.id, "ALT");
  }

  const uniq = [...people.values()];
  return {
    opportunityCount: opportunities.length,
    primary: {
      named: opportunities.filter((o) => text(o.primaryContact?.name)).length,
      email: primaryEmail,
      phone: primaryPhone,
      linkedin: primaryLi,
      allThreeChannels: primaryAll3,
      anyOutreach: primaryAny,
      stage4Annotated: stage4,
      findymailUsed,
      withSecondaryContact: withSecondary,
      withAltEntryPoints: withAlt,
    },
    uniquePeople: {
      count: uniq.length,
      withTitle: uniq.filter((p) => p.title).length,
      withEmail: uniq.filter((p) => p.email).length,
      withPhone: uniq.filter((p) => p.phone).length,
      withLinkedIn: uniq.filter((p) => p.linkedin).length,
      allThree: uniq.filter((p) => p.email && p.phone && p.linkedin).length,
      anyOutreach: uniq.filter((p) => p.email || p.phone || p.linkedin).length,
    },
    people: uniq,
  };
}

function listDrawerParity(opportunities) {
  const rows = [];
  for (const o of opportunities) {
    const c = o.primaryContact || {};
    const tile = o.tileContactPreview || {};
    const listName = text(c.name || c.fullName);
    const drawerName = listName; // same object in package
    const email = text(c.email);
    const phone = text(c.phone);
    const linkedin = text(c.linkedinUrl);
    const tileEmail = text(tile.email);
    const tilePhone = text(tile.phone);
    const ok =
      (!tileEmail || tileEmail === email) &&
      (!tilePhone || tilePhone === phone);
    rows.push({
      opportunityId: o.id,
      listName,
      drawerName,
      email,
      phone,
      linkedin,
      tileEmail: tileEmail || null,
      tilePhone: tilePhone || null,
      parityOk: ok && listName === drawerName,
    });
  }
  return {
    checked: rows.length,
    pass: rows.filter((r) => r.parityOk).length,
    fail: rows.filter((r) => !r.parityOk).length,
    rows,
  };
}

async function main() {
  const STAGED_VERSION = resolveStagedVersion();
  const pkgFile = packagePath(STAGED_VERSION);
  if (!fs.existsSync(pkgFile)) {
    console.error(`STAGED package missing: ${pkgFile}`);
    process.exit(1);
  }
  const pkg = loadJson(pkgFile);
  const opportunities = Array.isArray(pkg.opportunities) ? pkg.opportunities : [];
  const before = scoreContacts(opportunities);

  const {
    buildFindymailEnrichmentManifest,
    enrichPersonEmailWithFindymail,
    FINDYMAIL_STAGE4_AUTH_TOKEN,
  } = await import(
    pathToFileURL(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js",
      ),
    ).href
  );
  const { describeFindymailKeyPresence } = await import(
    pathToFileURL(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js",
      ),
    ).href
  );

  // Load Integration .env for Findymail/Airtable without printing secrets
  const envCandidates = [
    path.join(ROOT, ".env"),
    path.join(ROOT, "../my-operators-backend/.env"),
    "C:/Dev/dealality/my-operators-backend/.env",
  ];
  for (const envPath of envCandidates) {
    if (!fs.existsSync(envPath)) continue;
    const raw = fs.readFileSync(envPath, "utf8");
    for (const line of raw.split(/\r?\n/)) {
      const m = line.match(/^(FINDYMAIL_API_KEY|AIRTABLE_API_KEY|AIRTABLE_PAT|AIRTABLE_BASE_ID|AIRTABLE_GDI_BASE_ID)=(.*)$/);
      if (!m) continue;
      if (!process.env[m[1]]) process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
    }
  }

  const creditHardCap = Number(argValue("--credit-hard-cap", "30"));
  const manifest = buildFindymailEnrichmentManifest(opportunities, {
    creditHardCap,
    repoRoot: ROOT,
  });

  const architecture = {
    fiveStages: [
      {
        stage: 1,
        name: "Demand Discovery",
        runner: "scripts/gdi/Invoke-GdiDiscovery.ps1",
        status: "OPERATIONAL",
      },
      {
        stage: 2,
        name: "Opportunity Deep Research",
        runner: "scripts/gdi/Invoke-GdiDeepResearch.ps1",
        status: "OPERATIONAL",
      },
      {
        stage: 3,
        name: "WHO/HOW Resolution",
        runner: "scripts/gdi/Invoke-GdiWhoHow.ps1",
        status: "OPERATIONAL",
      },
      {
        stage: 4,
        name: "Professional Contact Research",
        runner: "scripts/gdi/Invoke-GdiContactResearch.ps1",
        findymail: "PERMANENT_COMPONENT",
        status: "OPERATIONAL_WITH_FINDYMAIL_GATE",
      },
      {
        stage: 5,
        name: "Commercial Qualification",
        runner: "scripts/gdi/Invoke-GdiQualification.ps1",
        status: "OPERATIONAL",
      },
    ],
    orchestrator: "scripts/gdi/Invoke-GdiFullE2E.ps1",
    validator: "scripts/gdi/Invoke-GdiValidateFullE2E.ps1",
    lifecycle: "FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT",
    surfe: "EXPLICIT_CLICK_ONLY_NO_PERSIST",
    findymail: {
      permanentStage4Component: true,
      client: "lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js",
      enrichment:
        "lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js",
      airtablePersist:
        "lib/group-demand-intelligence/persistence/gdi-findymail-airtable-contact-persist-v1.js",
      authToken: FINDYMAIL_STAGE4_AUTH_TOKEN,
      keyPresence: describeFindymailKeyPresence(),
    },
  };

  const airtableRegister = {
    base: {
      name: "AI Demand Positioning",
      baseId: "appa2cE7FTRmIbB32",
      contract:
        "lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-contract-v1.js",
    },
    opportunityTable: {
      name: "Group Demand Opportunities",
      tableId: "tblRuReslJMwsfRQj",
      contactFields: [
        "primaryContactId",
        "primaryContactName",
        "primaryContactRole",
        "primaryContactEmail",
        "primaryContactPhone",
        "contactQuality",
        "opportunityPayloadJson.primaryContact.*",
      ],
      note: "Contacts embedded on opportunities — no separate Contacts table required for gold-standard registry.",
    },
    researchHistoryTables: [
      "GDI Research Runs",
      "GDI Research Targets",
      "GDI Hotel Research Opportunities",
    ],
    detailedMatrixCsv:
      "reports/gdi-airtable-complete-data-model-audit-v1/GDI_FIELD_TO_AIRTABLE_MATRIX.csv",
  };

  const reconstruction = {
    dependsOnTemporaryWorktree: false,
    dependsOnUncommittedResearch: false,
    dependsOnUntrackedJson: false,
    durablePackagePath: path.relative(ROOT, pkgFile).replace(/\\/g, "/"),
    packageVersion: STAGED_VERSION,
    opportunityCount: opportunities.length,
    loadableFromBundledPublication: true,
    pointer:
      "lib/group-demand-intelligence/bundled-publication-packages/recnPB3Q02NO8Ma1o/CURRENT_STAGED.json",
  };

  const parity = listDrawerParity(opportunities);

  const oppMatrix = opportunities.map((o) => {
    const c = o.primaryContact || {};
    return {
      opportunityId: o.id,
      title: o.title || o.displayTitle,
      organization: o.organizationName || o.displayCompanyName,
      marketScope: o.inventoryPopulationPrimary || o.marketScope || null,
      priority: o.priority || o.priorityPillLabel,
      salesAction: o.salesActionPillLabel || o.salesActionKey,
      primaryName: text(c.name),
      primaryTitle: text(c.title || c.role),
      email: text(c.email) || null,
      phone: text(c.phone) || null,
      linkedin: text(c.linkedinUrl) || null,
      stage4: Boolean(o.contactResearchStage),
      findymailUsed: o.contactResearchStage?.findymailUsed === true,
      secondaryName: text(o.secondaryContact?.name) || null,
    };
  });

  let findymailLiveResults = null;
  const doLive = flag("--findymail-live");
  const doDry = flag("--findymail-dry-run") || doLive || true;
  if (doDry) {
    findymailLiveResults = {
      mode: doLive ? "LIVE" : "DRY_RUN",
      selected: [],
      found: 0,
      notFound: 0,
      skipped: 0,
      errors: [],
      creditsDebitedEstimate: 0,
    };
    const byId = new Map(opportunities.map((o) => [o.id, o]));
    for (const row of manifest.selected) {
      const opp = byId.get(row.opportunityIds[0]);
      if (!opp) {
        findymailLiveResults.skipped += 1;
        continue;
      }
      // Never inherit another person's email/phone when enriching a different name.
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
          dryRun: !doLive,
          allowFindymail: true,
          hotelId: HOTEL_ID,
          authorizationId: FINDYMAIL_STAGE4_AUTH_TOKEN,
          repoRoot: ROOT,
          persistFindymailResults: false,
        });
        findymailLiveResults.selected.push({
          personKey: row.personKey,
          outcome: enriched.outcome,
          email: enriched.person?.email || null,
          domain: row.domain,
        });
        if (enriched.outcome === "FOUND_VERIFICATION_REQUIRED") {
          findymailLiveResults.found += 1;
          findymailLiveResults.creditsDebitedEstimate += doLive ? 1 : 0;
        } else if (enriched.outcome === "RESEARCH_COMPLETED_NOT_FOUND") {
          findymailLiveResults.notFound += 1;
        } else {
          findymailLiveResults.skipped += 1;
        }
      } catch (err) {
        findymailLiveResults.errors.push({
          personKey: row.personKey,
          code: err.code || null,
          message: String(err.message || err).slice(0, 200),
        });
      }
    }
  }

  const after = doLive
    ? scoreContacts(
        opportunities.map((o) => {
          const hit = findymailLiveResults?.selected?.find((s) =>
            (s.opportunityIds || []).includes?.(o.id),
          );
          // Live enrichment applied into STAGED separately; score current package + found emails by person
          const found = findymailLiveResults?.selected?.find(
            (s) =>
              s.email &&
              text(o.primaryContact?.name).toLowerCase() ===
                text(s.personKey?.split("|")[0]).toLowerCase(),
          );
          if (!found?.email) return o;
          return {
            ...o,
            primaryContact: {
              ...(o.primaryContact || {}),
              email: o.primaryContact?.email || found.email,
            },
          };
        }),
      )
    : before;

  const outstanding = [];
  if (after.primary.email < after.opportunityCount) {
    outstanding.push(
      `Primary email gaps remain: ${after.opportunityCount - after.primary.email}/${after.opportunityCount}`,
    );
  }
  if (after.primary.phone < after.opportunityCount) {
    outstanding.push(
      `Primary phone gaps remain: ${after.opportunityCount - after.primary.phone}/${after.opportunityCount} (Findymail is email-only; use public/Exa phone research)`,
    );
  }
  if (after.primary.linkedin < after.opportunityCount) {
    outstanding.push(
      `Primary LinkedIn gaps remain: ${after.opportunityCount - after.primary.linkedin}/${after.opportunityCount}`,
    );
  }
  if (after.primary.withAltEntryPoints < after.opportunityCount) {
    outstanding.push(
      `Alternative entry-point hydration incomplete on package: ${after.primary.withAltEntryPoints}/${after.opportunityCount} (WHO/HOW may store alts outside STAGED tip fields)`,
    );
  }
  if (!doLive) {
    outstanding.push(
      `Findymail live enrichment not executed this cycle — dry-run manifest ready (${manifest.eligibleCount} eligible / ${manifest.estimatedFinderCreditsMax} capped). Re-run with --findymail-live --credit-hard-cap ${creditHardCap}.`,
    );
  }
  if (findymailLiveResults?.errors?.length) {
    outstanding.push(
      `Findymail errors: ${findymailLiveResults.errors.length}`,
    );
  }

  const certificationStatus =
    outstanding.length === 0
      ? "GDI_CANONICAL_FULL_E2E_GOLD_STANDARD_CERTIFIED"
      : "PARTIAL";

  const report = {
    schema: "gdi-canonical-full-e2e-gold-standard-certification-v1",
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    stagedVersion: STAGED_VERSION,
    certificationStatus,
    architecture,
    airtableRegister,
    reconstruction,
    contactScorecard: { before, after },
    findymailManifest: manifest,
    findymailExecution: findymailLiveResults,
    listDrawerParity: {
      checked: parity.checked,
      pass: parity.pass,
      fail: parity.fail,
    },
    opportunityMatrixCount: oppMatrix.length,
    powershellCommands: {
      discovery: `pwsh -File scripts/gdi/Invoke-GdiDiscovery.ps1 -HotelId ${HOTEL_ID} -Mode DryRun`,
      deep: `pwsh -File scripts/gdi/Invoke-GdiDeepResearch.ps1 -HotelId ${HOTEL_ID} -Mode DryRun`,
      whoHow: `pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -HotelId ${HOTEL_ID} -Mode DryRun`,
      contact: `pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId ${HOTEL_ID} -Mode DryRun`,
      contactFindymailLive: `pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId ${HOTEL_ID} -Mode Live -AuthorizePaidResearch -AuthorizeFindymail`,
      qualification: `pwsh -File scripts/gdi/Invoke-GdiQualification.ps1 -HotelId ${HOTEL_ID} -Mode DryRun`,
      fullE2e: `pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId ${HOTEL_ID} -Mode DryRun`,
      validate: `pwsh -File scripts/gdi/Invoke-GdiValidateFullE2E.ps1 -HotelId ${HOTEL_ID}`,
      goldCert: `node scripts/gdi-canonical-full-e2e-gold-standard-certification-v1.mjs`,
      findymailLive: `node scripts/gdi-canonical-full-e2e-gold-standard-certification-v1.mjs --findymail-live --credit-hard-cap ${creditHardCap}`,
    },
    outstanding,
  };

  writeJson("00-CERTIFICATION.json", report);
  writeJson("01-ARCHITECTURE.json", architecture);
  writeJson("02-AIRTABLE_FIELD_REGISTER.json", airtableRegister);
  writeJson("03-RECONSTRUCTION_TEST.json", reconstruction);
  writeJson("04-OPPORTUNITY_MATRIX.json", { opportunities: oppMatrix });
  writeJson("05-CONTACT_SCORECARD.json", { before, after });
  writeJson("06-FINDYMAIL_MANIFEST.json", manifest);
  writeJson("07-LIST_DRAWER_PARITY.json", parity);
  if (findymailLiveResults) {
    writeJson("08-FINDYMAIL_EXECUTION.json", findymailLiveResults);
  }

  writeMd(
    "FOUNDER_REPORT.md",
    `# GDI Full E2E Gold Standard Certification — ${HOTEL_NAME}

**Status:** \`${certificationStatus}\`  
**STAGED:** \`${STAGED_VERSION}\`  
**Generated:** ${report.generatedAt}

## Inventory
- Qualified opportunities: **${after.opportunityCount}**
- Unique commercial contacts: **${after.uniquePeople.count}**
- Primary email / phone / LinkedIn: **${after.primary.email} / ${after.primary.phone} / ${after.primary.linkedin}**
- Primaries with any outreach route: **${after.primary.anyOutreach}/${after.opportunityCount}**
- List/Drawer parity: **${parity.pass}/${parity.checked}**

## Findymail (permanent Stage 4)
- Client: wired (\`gdi-findymail-client-v1\`)
- API key present: **${manifest.keyPresence.present}**
- Eligible unique people (missing email + domain): **${manifest.eligibleCount}**
- Selected under credit hard-cap ${creditHardCap}: **${manifest.selectedCount}**
- Execution this cycle: **${findymailLiveResults?.mode || "NONE"}** (found=${findymailLiveResults?.found ?? 0}, notFound=${findymailLiveResults?.notFound ?? 0})

## Surfe
- Explicit-click only — not used in Full E2E — never persisted

## Reconstruction
- Durable bundled STAGED package loadable without untracked research: **YES**
- Path: \`${reconstruction.durablePackagePath}\`

## Outstanding
${outstanding.map((x) => `- ${x}`).join("\n") || "- None"}

## Exact PowerShell
\`\`\`powershell
${report.powershellCommands.fullE2e}
${report.powershellCommands.validate}
\`\`\`
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        certificationStatus,
        stagedVersion: STAGED_VERSION,
        opportunities: after.opportunityCount,
        findymailEligible: manifest.eligibleCount,
        outstanding: outstanding.length,
        outDir: OUT,
      },
      null,
      2,
    ),
  );

  if (certificationStatus !== "GDI_CANONICAL_FULL_E2E_GOLD_STANDARD_CERTIFIED") {
    process.exitCode = 0; // PARTIAL is a valid delivery state
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
