/**
 * Brand Explorer Momentum Announcement Source Upgrade Writer v25C-3F.
 *
 * Upgrades Tribute Portfolio footprint.momentum rows to announcement-quality
 * sources (press releases, newsroom, owner announcements, trade articles).
 * Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-momentum-announcement-source-upgrade-writer-v25C-3F.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import { MOMENTUM_SLOT } from "./brand-explorer-openings-momentum-row-review-package.js";

export const WRITER_VERSION = "25C-3F";
export const REPORT_JSON_NAME = "brand-explorer-momentum-announcement-source-upgrade-writer.json";
export const REPORT_MD_NAME = "brand-explorer-momentum-announcement-source-upgrade-writer.md";
export const DOC_MD_NAME = "brand-explorer-momentum-announcement-source-upgrade-writer-v25C-3F.md";

export const APPLY_FLAG_BATCH = "--approve-brand-explorer-v25C-3F-momentum-announcement-source-upgrade";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-momentum-announcement-copy";
export const APPLY_FLAG_SOURCES = "--confirm-announcement-quality-sources";

const CASA_NIZUC_MARSHA = "CUNAN";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const MIN_ELIGIBLE_ROWS = 3;
const TARGET_ELIGIBLE_ROWS = 6;

const GOVERNANCE_LABELS = [
  "Founder-reviewed announcement copy package",
  "Source-grounded from press/newsroom/trade announcements",
  "Not company-validated",
  "Not Marriott-validated",
];

const INTERNAL_COPY_RE =
  /consumer site|brand site|tribute site|consumer map|metadata|source data|dated listing|official materials|official source|appears on|listed with|carried a dated|brand-site|consumer-site|official Tribute portfolio materials|official Marriott\/Tribute materials/i;

const FORBIDDEN_UI_PATTERNS = [
  INTERNAL_COPY_RE,
  /\bMARSHA\b/i,
  /AI-drafted from official-source metadata/i,
  /Pending founder review/i,
  /Not company-validated/i,
  /Not Marriott-validated/i,
];

const PR_URL_RE =
  /newsroom|press-release|press_release|\/news\/|prnewswire\.com|globenewswire\.com|businesswire\.com|marriott\.pressarea\.com|marriott\.africa-newsroom|media\.choicehotels\.com|ihgplc\.com\/news/i;

const POSITIVE_PR_CLAIM_RE =
  /(?:marriott|tribute).{0,80}(?:press release|newsroom announcement|announced (?:its|the) (?:opening|debut))/i;

/**
 * Founder-reviewed announcement upgrades — sources verified via public press/trade capture (v25C-3F).
 * Grand Hotel Preanger (BDOGP) has no announcement-quality source; row repurposed with Recoleta Grand CALA debut.
 */
export const ANNOUNCEMENT_UPGRADE_PACKAGES = [
  {
    marsha: "LIMTX",
    recordId: "recwinQHDJ9rL02Lw",
    sort: 0,
    dateLine: "Apr 2026",
    propertyName: "Humano, Lima, a Tribute Portfolio Hotel",
    polishedTitle: "Humano Lima Opens As Tribute Portfolio Hotel In Peru",
    polishedSummary:
      "Humano, Lima opened in Miraflores as Tribute Portfolio's debut in Peru—a waterfront urban hotel that extends the collection's South America lifestyle positioning for owners evaluating CALA gateway markets.",
    announcementUrl:
      "https://www.hotel-online.com/news/humano-lima-a-tribute-portfolio-hotel-opens-its-doors-in-miraflores",
    sourceType: "Hotel Opening Announcement",
    sourceClassification: "Hospitality Trade Article",
    linkLabel: "View Article",
    prUrlFound: true,
    discoveryNotes: "Hotel Online opening announcement; corroborated by Travel Daily News and Hotel Management Network.",
  },
  {
    marsha: "MDETX",
    recordId: "recr4swOtCY7nOjui",
    sort: 1,
    dateLine: "Dec 2025",
    propertyName: "Loma, Medellin, a Tribute Portfolio Hotel",
    polishedTitle: "Loma Medellín Joins Tribute Portfolio In Colombia",
    polishedSummary:
      "Marriott International and OxoHotel opened Loma in El Poblado—an urban lifestyle hotel that strengthens Tribute's design-forward presence in Medellín for owners comparing Andean city affiliation options.",
    announcementUrl:
      "https://colombia.ladevi.info/negocios/marriott-international-y-oxohotel-amplian-la-oferta-hotelera-medellin-n94379",
    sourceType: "Official Brand Announcement",
    sourceClassification: "Hospitality Trade Article",
    linkLabel: "View Article",
    prUrlFound: true,
    discoveryNotes: "Ladevi trade coverage with Marriott CALA president quote; corroborated by Semana and La República.",
  },
  {
    marsha: "BGITY",
    recordId: "rec46SixcVS6j9NbC",
    sort: 2,
    dateLine: "Feb 2026",
    propertyName: "Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort",
    polishedTitle: "Crystal Cove Opens As Tribute Portfolio's First All-Inclusive Resort",
    polishedSummary:
      "Crystal Cove opened on Barbados' west coast as the first all-inclusive resort in Tribute Portfolio—an 88-room beachfront debut that expands Marriott's Caribbean character-hotel story for resort-scale owner conversations.",
    announcementUrl:
      "https://www.prnewswire.com/news-releases/crystal-cove-welcomes-a-new-era-of-indie-spirited-island-escapes-as-the-first-tribute-portfolio-allinclusive-resort-302686362.html",
    sourceType: "Marriott Press Release",
    sourceClassification: "Marriott Press Release",
    linkLabel: "View Marriott Announcement",
    prUrlFound: true,
    discoveryNotes: "PRNewswire release with Marriott International CALA president quote.",
  },
  {
    marsha: "SJUTX",
    recordId: "recfvVguASf2SQmEc",
    sort: 3,
    dateLine: "May 2024",
    propertyName: "Hotel Rumbao, a Tribute Portfolio Hotel",
    polishedTitle: "Hotel Rumbao Reopens In Old San Juan Under Tribute Portfolio",
    polishedSummary:
      "Driftwood Capital celebrated the grand opening of Hotel Rumbao after a $21.8M repositioning—the only Tribute Portfolio hotel in Puerto Rico and a reference for heritage urban conversion deals in CALA.",
    announcementUrl:
      "https://www.hotel-online.com/press_releases/release/driftwood-capital-celebrates-rebranding-of-its-245-key-hotel-rumbao-in-historic-old-san-juan-puerto-rico/",
    sourceType: "Owner / Developer Announcement",
    sourceClassification: "Owner / Developer Announcement",
    linkLabel: "View Owner Announcement",
    prUrlFound: true,
    discoveryNotes: "Driftwood Capital owner press release; corroborated by Travel Weekly and News is My Business.",
  },
  {
    marsha: "MILNT",
    recordId: "recgzP6rMkL4VFrsW",
    sort: 5,
    dateLine: "Jun 2026",
    propertyName: "NEMI, Milan, a Tribute Portfolio Hotel",
    polishedTitle: "NEMI Milan Joins Tribute Portfolio Collection",
    polishedSummary:
      "NEMI Hotel Milano joined Tribute Portfolio in Porta Venezia—expanding the brand's Italian urban lifestyle footprint with a 49-room luxury property managed by Opera Hotels.",
    announcementUrl:
      "https://www.journaldespalaces.com/en/pressrelease-78419-italy-tribute-portfolio-hotels-expands-italian-offering-with-nemi-milan.html",
    sourceType: "Official Brand Announcement",
    sourceClassification: "Official Brand Announcement",
    linkLabel: "View Article",
    prUrlFound: true,
    discoveryNotes: "Journal des Palaces press release; corroborated by ITHIC trade coverage.",
  },
];

/** BDOGP lacks announcement-quality source — replace row content with sourced CALA portfolio debut. */
export const MOMENTUM_REPLACEMENT_PACKAGE = {
  replacesMarsha: "BDOGP",
  recordId: "recjmKLQyq2YW0vp2",
  sort: 4,
  replacedPropertyName: "Grand Hotel Preanger, Bandung, a Tribute Portfolio Hotel",
  replacementReason: "No Marriott press release, newsroom item, owner announcement, or credible trade article found for Preanger Tribute affiliation; only consumer listings and generic Marriott openings page.",
  notRecentMomentumReady: true,
  dateLine: "Jun 2025",
  propertyName: "Recoleta Grand, Buenos Aires, a Tribute Portfolio Hotel",
  polishedTitle: "Recoleta Grand Debuts Tribute Portfolio In Buenos Aires",
  polishedSummary:
      "Recoleta Grand marked Tribute Portfolio's Argentina debut in Buenos Aires' Recoleta district—another CALA-relevant expansion point for owners tracking Marriott's independent-character growth in Latin America.",
  announcementUrl:
    "https://www.prnewswire.com/news-releases/tribute-portfolio-debuts-in-buenos-aires-with-the-opening-of-recoleta-grand-buenos-aires-a-tribute-portfolio-hotel-302473568.html",
  sourceType: "Marriott Press Release",
  sourceClassification: "Marriott Press Release",
  linkLabel: "View Marriott Announcement",
  prUrlFound: true,
  discoveryNotes: "PRNewswire Marriott/Tribute Portfolio debut release; replaces weak Preanger consumer-listing row.",
};

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-momentum-editorial-link-repair-writer.md",
  "reports/brand-explorer-momentum-editorial-link-repair-writer.json",
  "reports/brand-explorer-openings-momentum-row-creation-writer.md",
  "reports/brand-explorer-openings-momentum-row-creation-writer.json",
  "reports/brand-explorer-openings-momentum-row-review-package.md",
  "reports/brand-explorer-openings-momentum-row-review-package.json",
  "reports/brand-explorer-openings-momentum-source-capture-completion.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Tribute Brand Explorer Presentation rows",
  "live Tribute Source Library records",
  "live Curio/Kimpton/Radisson/Ascend footprint.momentum rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-momentum-announcement-source-upgrade-writer.js",
  "scripts/brand-explorer-momentum-announcement-source-upgrade-writer.mjs",
  "docs/data-intelligence/brand-explorer-momentum-announcement-source-upgrade-writer-v25C-3F.md",
  "reports/brand-explorer-momentum-announcement-source-upgrade-writer.md",
  "reports/brand-explorer-momentum-announcement-source-upgrade-writer.json",
  "public/js/brand-explorer-atelier-from-api.js",
  "package.json",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function buildMomentumBody(pkg) {
  return normalizeBody([pkg.dateLine, pkg.polishedSummary, pkg.announcementUrl].join("\n\n"));
}

function parseMomentumBody(body) {
  const paras = normalizeBody(body)
    .split(/\n\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
  const date = paras[0] || "";
  let url = "";
  const descParts = [];
  for (let i = 1; i < paras.length; i++) {
    if (/^https?:\/\//i.test(paras[i])) url = paras[i];
    else descParts.push(paras[i]);
  }
  return { date, description: descParts.join("\n\n"), url };
}

function classifySourceUrl(url) {
  const u = nz(url).toLowerCase();
  if (!u) return "Not Suitable";
  if (/prnewswire\.com|globenewswire\.com|businesswire\.com/.test(u)) return "Marriott Press Release";
  if (/marriott\.pressarea\.com|marriott\.africa-newsroom|marriott\.com\/newsroom/.test(u))
    return "Marriott Newsroom";
  if (/hotel-online\.com\/press_releases|hotel-online\.com\/press\//.test(u))
    return "Owner / Developer Announcement";
  if (/journaldespalaces\.com\/en\/pressrelease/.test(u)) return "Official Brand Announcement";
  if (
    /travelweekly|traveldailynews|hotel-online\.com\/news|ladevi|semana\.com|ithic|hotelmanagement-network|travelprnews|breakingtravelnews|hotelnewsresource|newsismybusiness/.test(
      u
    )
  )
    return "Hospitality Trade Article";
  if (u.includes("tribute-portfolio.marriott.com")) return "Generic Consumer Listing";
  if (u.includes("marriott.com") && /\/hotels\//.test(u)) return "Generic Property Page";
  if (u.includes("marriott.com")) return "Generic Consumer Listing";
  return "Other";
}

function isAnnouncementQualitySourceType(type) {
  return ![
    "Generic Property Page",
    "Generic Consumer Listing",
    "Not Suitable",
    "Other",
  ].includes(type);
}

function containsForbiddenUiCopy(text) {
  return FORBIDDEN_UI_PATTERNS.some((re) => re.test(nz(text)));
}

function bodyClaimsPrWithoutUrl(body, url) {
  if (POSITIVE_PR_CLAIM_RE.test(body) && !PR_URL_RE.test(nz(url))) return true;
  return /\bpress release\b/i.test(body) && !PR_URL_RE.test(nz(url));
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function listByFormula(baseId, apiKey, tableName, formula) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    if (formula) params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, tableName)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed ${tableName}: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-momentum-announcement-source-upgrade-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_BATCH} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_SOURCES}`;
}

function packageToPlan(pkg, { isReplacement = false, replacementMeta = null } = {}) {
  const proposedBody = buildMomentumBody(pkg);
  const sourceClass = pkg.sourceClassification || classifySourceUrl(pkg.announcementUrl);
  return {
    marsha: pkg.marsha || replacementMeta?.replacesMarsha,
    recordId: pkg.recordId,
    sort: pkg.sort,
    propertyName: pkg.propertyName,
    polishedTitle: pkg.polishedTitle,
    polishedSummary: pkg.polishedSummary,
    proposedBody,
    announcementUrl: pkg.announcementUrl,
    sourceType: pkg.sourceType,
    sourceClassification: sourceClass,
    linkLabel: pkg.linkLabel,
    prUrlFound: pkg.prUrlFound,
    discoveryNotes: pkg.discoveryNotes,
    recentMomentumEligible: isAnnouncementQualitySourceType(sourceClass) && pkg.prUrlFound,
    isReplacement,
    replacementMeta,
    fields: {
      Title: pkg.polishedTitle,
      Body: proposedBody,
      Active: true,
    },
  };
}

export async function buildBrandExplorerMomentumAnnouncementSourceUpgradeWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  announcementQualitySourcesConfirmed = false,
} = {}) {
  const brandRecordId =
    nz(brandIdOrName).toLowerCase() === "tribute-portfolio" || !nz(brandIdOrName)
      ? TRIBUTE_RECORD_ID
      : nz(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-3F pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );

  const allPresentation = presentationRaw.map((rec) => ({
    recordId: rec.id,
    slotKey: nz(rec.fields?.["Slot Key"]),
    title: nz(rec.fields?.Title),
    body: nz(rec.fields?.Body),
    sortOrder: rec.fields?.["Sort Order"],
    active: rec.fields?.Active,
    imageCount: Array.isArray(rec.fields?.Image) ? rec.fields.Image.length : 0,
  }));

  const liveMomentum = allPresentation.filter((r) => r.slotKey === MOMENTUM_SLOT);
  const openingsSnapshot = allPresentation
    .filter((r) => r.slotKey === "footprint.openings")
    .map((r) => ({ recordId: r.recordId, title: r.title }));
  const loyaltySnapshot = allPresentation
    .filter((r) => r.slotKey.startsWith("loyalty."))
    .map((r) => ({ recordId: r.recordId, slotKey: r.slotKey, title: r.title }));

  const casaNizucInMomentum = liveMomentum.some((r) =>
    /casa nizuc|cunan/i.test(`${r.title} ${r.body}`)
  );

  const upgradePlans = ANNOUNCEMENT_UPGRADE_PACKAGES.map((pkg) => packageToPlan(pkg));
  const replacementPlan = packageToPlan(
    {
      ...MOMENTUM_REPLACEMENT_PACKAGE,
      marsha: MOMENTUM_REPLACEMENT_PACKAGE.replacesMarsha,
      polishedTitle: MOMENTUM_REPLACEMENT_PACKAGE.polishedTitle,
      polishedSummary: MOMENTUM_REPLACEMENT_PACKAGE.polishedSummary,
      announcementUrl: MOMENTUM_REPLACEMENT_PACKAGE.announcementUrl,
    },
    {
      isReplacement: true,
      replacementMeta: {
        replacesMarsha: MOMENTUM_REPLACEMENT_PACKAGE.replacesMarsha,
        replacedPropertyName: MOMENTUM_REPLACEMENT_PACKAGE.replacedPropertyName,
        replacementReason: MOMENTUM_REPLACEMENT_PACKAGE.replacementReason,
      },
    }
  );

  const allPlans = [...upgradePlans, replacementPlan];
  const rowsWouldUpdate = [];
  const rowsWouldCreate = [];
  const rowsWouldHold = [];
  const applyBlockers = [];
  const rowDiagnostics = [];

  if (casaNizucInMomentum) {
    applyBlockers.push("casa_nizuc_in_momentum_blocked");
  }

  for (const plan of allPlans) {
    const live =
      liveMomentum.find((r) => r.recordId === plan.recordId) ||
      liveMomentum.find((r) => Number(r.sortOrder ?? -1) === Number(plan.sort));

    const parsed = live ? parseMomentumBody(live.body) : { date: "", description: "", url: "" };
    const currentSourceType = classifySourceUrl(parsed.url);

    rowDiagnostics.push({
      marsha: plan.marsha,
      recordId: plan.recordId,
      sort: plan.sort,
      propertyName: plan.propertyName,
      isReplacement: plan.isReplacement,
      replacementMeta: plan.replacementMeta || null,
      currentTitle: live?.title || null,
      currentBody: live?.body || null,
      currentSourceUrl: parsed.url,
      currentSourceType,
      proposedTitle: plan.polishedTitle,
      proposedBody: plan.proposedBody,
      proposedSourceUrl: plan.announcementUrl,
      proposedSourceType: plan.sourceClassification,
      proposedLinkLabel: plan.linkLabel,
      announcementSourcesFound: plan.prUrlFound
        ? [{ url: plan.announcementUrl, type: plan.sourceClassification, notes: plan.discoveryNotes }]
        : [],
      prNewsroomUrlFound: plan.prUrlFound,
      recentMomentumEligible: plan.recentMomentumEligible,
      discoveryNotes: plan.discoveryNotes,
    });

    if (!live) {
      rowsWouldCreate.push({ marsha: plan.marsha, sort: plan.sort });
      applyBlockers.push(`missing_momentum_row:${plan.marsha}`);
      continue;
    }

    if (containsForbiddenUiCopy(plan.polishedTitle) || containsForbiddenUiCopy(plan.polishedSummary)) {
      applyBlockers.push(`forbidden_copy:${plan.marsha}`);
    }
    if (INTERNAL_COPY_RE.test(plan.polishedTitle) || INTERNAL_COPY_RE.test(plan.polishedSummary)) {
      applyBlockers.push(`internal_language:${plan.marsha}`);
    }
    if (!plan.recentMomentumEligible) {
      rowsWouldHold.push({
        recordId: plan.recordId,
        marsha: plan.marsha,
        reason: "not_recent_momentum_ready",
      });
      applyBlockers.push(`not_announcement_quality:${plan.marsha}`);
    }
    if (bodyClaimsPrWithoutUrl(plan.proposedBody, plan.announcementUrl)) {
      applyBlockers.push(`false_pr_claim:${plan.marsha}`);
    }
    const announcementLabelSourceTypes = [
      "Marriott Press Release",
      "Marriott Newsroom",
      "Official Brand Announcement",
      "Owner / Developer Announcement",
      "Hotel Opening Announcement",
    ];
    if (
      plan.linkLabel.toLowerCase().includes("announcement") &&
      !PR_URL_RE.test(plan.announcementUrl) &&
      !announcementLabelSourceTypes.includes(plan.sourceClassification)
    ) {
      applyBlockers.push(`false_announcement_label:${plan.marsha}`);
    }
    if (/marriott\.com\/en-us\/hotels\//i.test(plan.announcementUrl)) {
      applyBlockers.push(`generic_property_url:${plan.marsha}`);
    }
    if (/tribute-portfolio\.marriott\.com/i.test(plan.announcementUrl)) {
      applyBlockers.push(`generic_consumer_hub_url:${plan.marsha}`);
    }

    const needsUpdate =
      nz(live.title) !== plan.polishedTitle ||
      normalizeBody(live.body) !== plan.proposedBody ||
      live.active === false;

    if (needsUpdate && plan.recentMomentumEligible) {
      rowsWouldUpdate.push({
        ...plan,
        action: plan.isReplacement ? "replace_content" : "update",
        currentTitle: live.title,
        currentBody: live.body,
      });
    }
  }

  const rowsWithoutAnnouncementSource = [
    {
      marsha: MOMENTUM_REPLACEMENT_PACKAGE.replacesMarsha,
      propertyName: MOMENTUM_REPLACEMENT_PACKAGE.replacedPropertyName,
      reason: MOMENTUM_REPLACEMENT_PACKAGE.replacementReason,
      action: "replace_with_sourced_row",
    },
  ];

  const finalEligibleRows = allPlans.filter((p) => p.recentMomentumEligible);
  const atLeastThreeAnnouncementQuality = finalEligibleRows.length >= MIN_ELIGIBLE_ROWS;

  if (!atLeastThreeAnnouncementQuality) {
    applyBlockers.push(`insufficient_announcement_rows:${finalEligibleRows.length}<${MIN_ELIGIBLE_ROWS}`);
  }

  const internalLanguageRemoved = allPlans.every(
    (p) => !INTERNAL_COPY_RE.test(p.polishedTitle) && !INTERNAL_COPY_RE.test(p.polishedSummary)
  );

  const genericLinksRemoved = allPlans.every(
    (p) =>
      !/tribute-portfolio\.marriott\.com/i.test(p.announcementUrl) &&
      !/marriott\.com\/en-us\/hotels\//i.test(p.announcementUrl)
  );

  const applyGatesReady =
    apply && approveBatch && founderReviewed && announcementQualitySourcesConfirmed;
  const canApply =
    applyGatesReady && applyBlockers.length === 0 && rowsWouldUpdate.length > 0;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const updated = [];
    const errors = [];
    for (const row of rowsWouldUpdate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({ fields: row.fields, typecast: true }),
        },
        row.recordId
      );
      if (!res.ok) {
        errors.push({
          recordId: row.recordId,
          marsha: row.marsha,
          message: json.error?.message || res.status,
        });
      } else {
        updated.push({
          recordId: row.recordId,
          marsha: row.marsha,
          title: row.polishedTitle,
          isReplacement: row.isReplacement,
        });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = updated.length > 0 && errors.length === 0;
    applyResults = { updated, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(brandRecordId));
  } else if (apply) {
    applyResults = { updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C3FWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      name: BRAND_NAME,
      recordId: brandRecordId,
      slug: "tribute-portfolio",
    },
    marriottValidationImplied: false,
    governanceLabels: [...GOVERNANCE_LABELS],
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    currentMomentumRowsInspected: liveMomentum.map((r) => ({
      recordId: r.recordId,
      title: r.title,
      bodyPreview: nz(r.body).slice(0, 160),
      sortOrder: r.sortOrder,
      active: r.active,
    })),
    announcementSourcesFoundByRow: rowDiagnostics.map((r) => ({
      marsha: r.marsha,
      sources: r.announcementSourcesFound,
      eligible: r.recentMomentumEligible,
    })),
    rowsWithoutAnnouncementQualitySource: rowsWithoutAnnouncementSource,
    replacementRowsProposed: [MOMENTUM_REPLACEMENT_PACKAGE],
    finalEligibleRecentMomentumRows: finalEligibleRows.map((r) => ({
      marsha: r.marsha,
      recordId: r.recordId,
      title: r.polishedTitle,
      sourceUrl: r.announcementUrl,
      sourceType: r.sourceClassification,
      linkLabel: r.linkLabel,
      isReplacement: r.isReplacement,
    })),
    rowDiagnostics,
    proposedPolishedTitles: allPlans.map((p) => ({
      marsha: p.marsha,
      title: p.polishedTitle,
      isReplacement: p.isReplacement,
    })),
    proposedPolishedBodies: allPlans.map((p) => ({
      marsha: p.marsha,
      body: p.proposedBody,
    })),
    proposedSourceUrls: allPlans.map((p) => ({
      marsha: p.marsha,
      url: p.announcementUrl,
      sourceType: p.sourceClassification,
    })),
    proposedLinkLabels: allPlans.map((p) => ({
      marsha: p.marsha,
      linkLabel: p.linkLabel,
      sourceType: p.sourceClassification,
    })),
    rowsWouldUpdate,
    rowsWouldCreate,
    rowsWouldHold,
    atLeastThreeAnnouncementQualityRows: atLeastThreeAnnouncementQuality,
    announcementQualityRowCount: finalEligibleRows.length,
    targetAnnouncementQualityRowCount: TARGET_ELIGIBLE_ROWS,
    internalSourceCaptureLanguageRemoved: internalLanguageRemoved,
    genericPropertyConsumerLinksRemoved: genericLinksRemoved,
    casaNizucExcludedFromMomentum: !casaNizucInMomentum,
    loyaltyRowsUntouched: true,
    loyaltyRowsSnapshot: loyaltySnapshot,
    openingsRowsUntouched: true,
    openingsRowsSnapshot: openingsSnapshot,
    imagesUntouched: true,
    sortOrderUntouched: true,
    brandBasicsUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    nonMomentumRowsModified: false,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      announcementQualitySourcesConfirmed,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers,
    applyResults,
    exactApplyCommand: buildApplyCommand("tribute-portfolio"),
    idempotentAfterApply: rowsWouldUpdate.length === 0,
    doesNotDo: [
      "Create or delete momentum rows",
      "Fabricate press releases or newsroom URLs",
      "Link generic Tribute consumer hub as announcement source",
      "Modify loyalty or openings rows",
      "Change images, Sort Order, or Company Validated",
      "Imply Marriott validated anything",
    ],
  };
}

export function buildBrandExplorerMomentumAnnouncementSourceUpgradeWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Momentum Announcement Source Upgrade Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- v25C-3F exists: **${report.v25C3FWriterExists ? "yes" : "no"}**`,
    "",
    "## Summary",
    "",
    "| Metric | Value |",
    "|--------|-------|",
    `| Momentum rows inspected | ${report.currentMomentumRowsInspected.length} |`,
    `| Announcement-quality eligible rows | ${report.announcementQualityRowCount} |`,
    `| Rows would update | ${report.rowsWouldUpdate.length} |`,
    `| Rows without announcement source (replaced) | ${report.rowsWithoutAnnouncementQualitySource.length} |`,
    `| At least 3 announcement-quality rows | ${report.atLeastThreeAnnouncementQualityRows ? "yes" : "no"} |`,
    `| Internal language removed | ${report.internalSourceCaptureLanguageRemoved ? "yes" : "no"} |`,
    `| Generic property/consumer links removed | ${report.genericPropertyConsumerLinksRemoved ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    "",
    "## Final eligible Recent Momentum rows",
    "",
  ];

  for (const row of report.finalEligibleRecentMomentumRows) {
    lines.push(
      `- **${row.title}** (\`${row.marsha}\`) · ${row.sourceType} · [source](${row.sourceUrl}) · link: *${row.linkLabel}*`
    );
  }
  lines.push("");

  if (report.replacementRowsProposed?.length) {
    lines.push("## Replacement rows proposed", "");
    for (const rep of report.replacementRowsProposed) {
      lines.push(
        `- Replaces **${rep.replacedPropertyName}** (\`${rep.replacesMarsha}\`): ${rep.replacementReason}`
      );
      lines.push(`  - New: **${rep.polishedTitle}** · ${rep.announcementUrl}`);
    }
    lines.push("");
  }

  lines.push("## Title / body / link changes", "", "| MARSHA | Proposed title | Link label | Source type |", "|--------|----------------|------------|-------------|");
  for (const row of report.rowDiagnostics) {
    lines.push(
      `| ${row.marsha} | ${row.proposedTitle} | ${row.proposedLinkLabel} | ${row.proposedSourceType} |`
    );
  }
  lines.push("");

  if (report.applyBlockers?.length) {
    lines.push("## Apply blockers", "");
    for (const b of report.applyBlockers) {
      lines.push(`- ${b}`);
    }
    lines.push("");
  }

  lines.push("## Exact apply command", "", "```bash", report.exactApplyCommand, "```", "");
  return lines.join("\n");
}
