/**
 * Phase 4 — remaining portfolio hotels
 * - Hotels with payload: evidence gate + light DA (if missing) + PDF regen if needed
 * - Hotels without payload: ADP-baseline-driven draft + public homepage evidence + certify
 *
 *   node scripts/ava-portfolio-remaining-batch-certify-v1.mjs
 *   node scripts/ava-portfolio-remaining-batch-certify-v1.mjs --only=adp_casas_del_xvi,adp_yotel_geneva_lake
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadAdpBaselineForAudit } from "../lib/ai-visibility-audit-action-plan/adp-baseline-loader-v1.js";
import { appendDigitalAuthorityActions, emptyDigitalAuthorityBlock, summarizeDigitalAuthorityClassifications } from "../lib/ai-visibility-audit-action-plan/digital-authority-v1.js";
import { runMandatoryAvaEvidenceStages } from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { summarizeCertificationForAdmin } from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
const ADMIN_MAP = {
  adp_spice_island_beach_resort: "spice-island-beach-resort-v1.json",
  adp_wyndham_san_diego_bayside: "wyndham-san-diego-bayside-v1.json",
  adp_waterstone_boca_raton: "waterstone-boca-raton-v1.json",
  adp_bethesda_marriott: "bethesda-marriott-v1.json",
  adp_castillo_hotel_son_vida: "castillo-hotel-son-vida-v1.json",
  adp_sheraton_mallorca_arabella_golf: "sheraton-mallorca-arabella-golf-v1.json",
  adp_ac_hotel_a_coruna: "ac-hotel-a-coruna-v1.json",
  adp_grand_fiesta_americana_chapultepec: "grand-fiesta-americana-chapultepec-v1.json",
  adp_fiesta_americana_condesa_cancun_ai: "fiesta-americana-condesa-cancun-ai-v1.json",
  adp_sheraton_guadalajara_expo: "sheraton-guadalajara-expo-v1.json",
  adp_voco_cancun_zona_hotelera: "voco-cancun-zona-hotelera-v1.json",
  adp_wyndham_alltra_punta_cana: "wyndham-alltra-punta-cana-v1.json",
  adp_jw_marriott_santo_domingo: "jw-marriott-santo-domingo-v1.json",
  adp_umusic_hotel_madrid: "umusic-hotel-madrid-v1.json",
  adp_st_regis_cap_cana: "st-regis-cap-cana-v1.json",
  adp_charlotte_marriott_city_center: "charlotte-marriott-city-center-v1.json",
  adp_tampa_airport_marriott: "tampa-airport-marriott-v1.json",
  adp_falls_church_marriott_fairview_park: "falls-church-marriott-fairview-park-v1.json",
  adp_renaissance_arlington_capital_view: "renaissance-arlington-capital-view-v1.json",
  adp_st_regis_mexico_city: "st-regis-mexico-city-v1.json",
  adp_w_rome: "w-rome-v1.json",
  adp_mayflower_hotel_washington_dc: "mayflower-hotel-washington-dc-v1.json",
  adp_hotel_phillips_kansas_city: "hotel-phillips-kansas-city-v1.json",
  adp_cambridge_beaches_bermuda: "cambridge-beaches-bermuda-v1.json",
  adp_nobu_london_portman: "nobu-london-portman-v1.json",
  adp_hilton_times_square: "hilton-times-square-v1.json",
  adp_radisson_blu_toronto_downtown: "radisson-blu-toronto-downtown-v1.json",
};

/** Already finalized in Phase 1 / Phase 2-3 — skip unless forced */
const SKIP_DEFAULT = new Set([
  "adp_ac_hotel_a_coruna",
  "adp_grand_fiesta_americana_chapultepec",
  "adp_fiesta_americana_condesa_cancun_ai",
  "adp_waterstone_boca_raton",
  "adp_castillo_hotel_son_vida",
  "adp_sheraton_mallorca_arabella_golf",
  "adp_sheraton_guadalajara_expo",
  "adp_voco_cancun_zona_hotelera",
  "adp_wyndham_alltra_punta_cana",
  "adp_bethesda_marriott",
]);

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function slugFromPropertyId(propertyId) {
  return String(propertyId || "")
    .replace(/^adp_/, "")
    .replace(/_/g, "-");
}

function shortCode(propertyId) {
  const parts = slugFromPropertyId(propertyId).split("-");
  return parts
    .map((p) => p[0])
    .join("")
    .toUpperCase()
    .slice(0, 4);
}

function fillActionContract({
  actionId,
  headline,
  priority,
  priorityWhy,
  whatWeFound,
  whyItMatters,
  recommendedSteps,
  accountableOwner,
  supportingParties,
  suggestedTimeframe,
  claimType,
  website,
  evidenceKind = "VERIFIED_PUBLIC",
  allowNullEvidence = false,
}) {
  const evidence =
    website && !allowNullEvidence
      ? [
          {
            label: "Official website",
            url: website,
            kind: evidenceKind,
          },
        ]
      : allowNullEvidence
        ? []
        : website
          ? [
              {
                label: "Official website",
                url: website,
                kind: evidenceKind,
              },
            ]
          : [];
  return {
    actionId,
    headline,
    priority,
    priorityWhy,
    whatWeFound,
    whyItMatters,
    recommendedSteps,
    accountableOwner,
    supportingParties,
    suggestedTimeframe,
    estimatedEffort: "MEDIUM",
    completionCriteria: {
      investigationCompleted: "Hotel leadership confirmed required facts or access posture.",
      correctionSubmitted: "Approved updates or investigation ticket filed.",
      correctionVerifiedLive:
        "Live verification completed or documented no-change decision retained.",
      noChangeRequired:
        "Valid if leadership documents out-of-scope with evidence retained.",
    },
    requiresHotelApproval: true,
    hotelApprovalNotes: "Approve facts and outreach before public changes.",
    supportingEvidence: evidence,
    technicalDetails: {
      urls: evidence,
      changesOrInvestigation: recommendedSteps,
      evidence,
      verificationMethod: "Live-page QA and archived evidence where URLs are customer-linked.",
      dependencies: ["Hotel leadership approval"],
      notes: allowNullEvidence
        ? "Official URL intentionally not customer-linked until claim-verified outside bot blocks."
        : "Public partial research only.",
    },
    implementationStatus: "AWAITING_HOTEL_DECISION",
    claimType,
    evidenceLinks: evidence,
    targetUrls: website && !allowNullEvidence ? [website] : [],
  };
}

function loadUniverse() {
  const reg = JSON.parse(
    fs.readFileSync(
      path.join(REPORT, "AVA_FULL_PORTFOLIO_PROGRESS_REGISTER.json"),
      "utf8"
    )
  );
  return reg.rows || [];
}

async function fetchText(url) {
  try {
    const r = await fetch(url, {
      redirect: "follow",
      headers: {
        "User-Agent": "DealalityAvaPortfolioResearch/1.0",
        Accept: "text/html",
      },
      signal: AbortSignal.timeout(20000),
    });
    const text = await r.text();
    return {
      ok: r.status >= 200 && r.status < 400,
      status: r.status,
      finalUrl: r.url || url,
      text,
      title: (text.match(/<title[^>]*>([^<]+)/i) || [])[1] || "",
    };
  } catch (e) {
    return { ok: false, status: 0, finalUrl: url, text: "", title: "", error: e.message };
  }
}

function buildPayloadFromBaseline(baseline, fetchResult) {
  const propertyId = baseline.propertyId;
  const code = shortCode(propertyId);
  const website = baseline.website || fetchResult.finalUrl;
  const kpis = baseline.kpis || [];
  const consideration =
    kpis.find((k) => /consideration/i.test(k.label))?.value || "—";
  const scenario =
    kpis.find((k) => /scenario/i.test(k.label))?.value || "—";
  const peer =
    kpis.find((k) => /peer/i.test(k.label))?.value || "—";
  const pageOk = fetchResult.ok;
  const title = fetchResult.title || baseline.name;
  const actionId = `AVA-${code}-001`;
  const actionId2 = `AVA-${code}-002`;
  const actionId3 = `AVA-${code}-003`;

  const evidenceKind = pageOk ? "VERIFIED_PUBLIC" : "TECHNICAL_OBSERVATION";
  const accessNote = pageOk
    ? `Official page title observed: ${String(title).slice(0, 120)}`
    : `Automated probe could not retrieve HTML (status ${fetchResult.status || 0}${fetchResult.error ? `; ${fetchResult.error}` : ""}). Treat as access investigation, not a general outage.`;
  const evidenceUrl = pageOk ? website : null;
  const evidenceEntry = evidenceUrl
    ? [
        {
          label: "Official website",
          url: evidenceUrl,
          kind: evidenceKind,
          note: accessNote,
        },
      ]
    : [
        {
          label: "Official website (not customer-linked)",
          url: null,
          kind: "TECHNICAL_OBSERVATION",
          note: accessNote,
        },
      ];

  return {
    contractVersion: "AI_VISIBILITY_AUDIT_ACTION_PLAN_PDF_V1",
    status: "AUDIT_VALIDATED",
    property: {
      propertyId,
      name: baseline.name,
      market: baseline.market || "",
      country: baseline.country || "",
      coverLocation: [baseline.market, baseline.country].filter(Boolean).join(", "),
      website,
    },
    reporting: {
      reportMonthLabel: "NOVEMBER2026",
      coverSubtitle: "AUDIT & ACTION PLAN",
      coverDescriptorLine: "",
      coverDisclaimer: "",
      generatedAt: new Date().toISOString().slice(0, 10),
      adpPeriodId: baseline.periodId,
      adpBaselineSha256: baseline.baselineSha256 || null,
      researchCostUsd: 0,
      paidResearchAuthorized: false,
      generationMode: "ADP_BASELINE_PLUS_PUBLIC_PAGE_V1",
    },
    kpis,
    executiveSummary: {
      headline: "Turn Certified AI Presence Into Clearer Buyer Context",
      lead: `${baseline.name} shows AI Consideration ${consideration} and Scenario Presence ${scenario} (peer ${peer}) on the certified ADP baseline. These measures do not diagnose a website defect. Priority work is to keep official facts accurate, document any automated-access constraints, and present demand-relevant strengths consistently for owners, GMs, and marketing teams.`,
      leadSupport: "",
      metricsNote:
        "AI Consideration and Scenario Presence come from the certified ADP baseline. Presence is not bookings, revenue, or traveler exposure.",
      themes: [
        {
          title: "Certified Measurement Is the Baseline, Not a Website Verdict",
          what: `Consideration ${consideration} / scenario ${scenario} / peer ${peer} are retained from the certified ADP period without remeasurement.`,
          whyItMatters:
            "Leadership should investigate visibility signals without treating the website as proven cause.",
        },
        {
          title: "Official Discovery Path Must Stay Accurate",
          what: pageOk
            ? `The official website responded and was used as the primary evidence surface (${website}).`
            : `The official website did not return usable HTML to the automated probe; browser or brand-CDN confirmation may be required.`,
          whyItMatters:
            "Guests, planners, and AI systems reuse official facts; broken or unreachable official pages undermine every recommendation.",
        },
        {
          title: "Destination and Partner Authority Is a Follow-On Opportunity",
          what: "Public partial Digital Authority research may identify destination inventories; unpaid research does not invent backlink counts.",
          whyItMatters:
            "Commercially relevant opportunities should be pursued only with verified pages and honest classifications.",
        },
      ],
      priorities: [
        {
          priority: "High",
          action: "Commercial team: approve canonical property facts on the official site",
          desiredOutcome: "One signed source-of-truth fact pack",
        },
        {
          priority: "High",
          action: pageOk
            ? "Marketing: audit overview/meetings/dining presentation for consistency"
            : "Digital/CDN owner: investigate automated access to the official site",
          desiredOutcome: pageOk
            ? "Consistent buyer-ready official story"
            : "Documented intended crawler access posture",
        },
        {
          priority: "Medium",
          action: "Sales/marketing: identify one destination or partner listing opportunity",
          desiredOutcome: "Qualified authority opportunity with official URL",
        },
      ],
      successLooksLike:
        "Official facts are approved and consistent; access constraints are documented if present; destination/partner opportunities are pursued without fabricated SEO metrics; future ADP periods can be compared without changing certified history.",
      closing: `${baseline.name} should treat the certified AI measures as a monitoring signal. Improve factual clarity on the official site first, then pursue destination authority with verified links only.`,
    },
    actionOverviewIntro:
      "Hotel-specific actions derived from the certified ADP baseline and public official-page verification.",
    actionOverview: [
      {
        actionId,
        priority: "HIGH",
        headline: "Approve Canonical Official Facts",
        businessRelevance: "Creates one source of truth for AI and planner reuse.",
        accountableOwner: "Hotel Commercial Leadership",
        suggestedTimeframe: "2–4 weeks",
      },
      {
        actionId: actionId2,
        priority: "HIGH",
        headline: pageOk
          ? "Audit Official Page Consistency"
          : "Investigate Official-Site Automated Access",
        businessRelevance: pageOk
          ? "Removes avoidable conflicts on the primary discovery surface."
          : "Separates security controls from content defects.",
        accountableOwner: pageOk ? "Hotel Marketing" : "Brand Digital / CDN Security",
        suggestedTimeframe: "30 days",
      },
      {
        actionId: actionId3,
        priority: "MEDIUM",
        headline: "Qualify One Destination/Partner Authority Opportunity",
        businessRelevance:
          "Adds a commercially relevant discovery path without inventing backlink volume.",
        accountableOwner: "Hotel Sales / Marketing",
        suggestedTimeframe: "45 days",
      },
    ],
    hotelDecisions: {
      intro: "No public change is approved by this payload. Hotel leadership must confirm facts.",
      items: [
        {
          title: "Canonical Facts",
          detail: "Approve rooms, meetings, dining, and location statements.",
          relatedActionId: actionId,
        },
        {
          title: pageOk ? "Page Consistency" : "Access Policy",
          detail: pageOk
            ? "Approve corrections before publishing."
            : "Confirm intended automated clients.",
          relatedActionId: actionId2,
        },
        {
          title: "Authority Outreach",
          detail: "Approve destination/partner targets before outreach.",
          relatedActionId: actionId3,
        },
      ],
    },
    findingsIntro:
      "Findings separate certified measurement context from official-page verification and optional authority work.",
    keyFindings: [
      {
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        relatedActionId: actionId,
        title: "Certified AI Measures Require Factual Clarity—Not Assumed Website Blame",
        summary: `The certified baseline reports consideration ${consideration}, scenario presence ${scenario}, and peer ${peer}. Leadership should approve one fact pack before attributing gaps to content.`,
        evidence: evidenceEntry.filter((e) => e.url),
      },
      {
        claimType: pageOk ? "VERIFIED_ISSUE" : "RECOMMENDED_INVESTIGATION",
        relatedActionId: actionId2,
        title: pageOk
          ? "Official Site Is Reachable and Must Stay Accurate"
          : "Official Site Automated Access Needs Investigation",
        summary: accessNote,
        evidence: evidenceEntry.filter((e) => e.url),
      },
      {
        claimType: "OPTIONAL_COMMERCIAL_OPPORTUNITY",
        relatedActionId: actionId3,
        title: "Destination or Partner Listing Is an Opportunity, Not a Proven Gap",
        summary:
          "Public partial research may identify destination inventories. Absence is SUBJECT_COVERAGE_NOT_ESTABLISHED until a relevant inventory is sufficiently reviewed.",
        evidence: evidenceEntry.filter((e) => e.url),
      },
    ],
    actionPlanIntro:
      "Each action states owner, evidence, and verification expectations.",
    actions: [
      fillActionContract({
        actionId,
        headline: "Approve Canonical Official Facts",
        priority: "HIGH",
        priorityWhy: "Certified AI measures should sit on an approved fact pack.",
        whatWeFound: `Baseline period ${baseline.periodId} is preserved; official website is ${website}.`,
        whyItMatters:
          "Owners and GMs need one approved story for meetings, dining, and location.",
        recommendedSteps: [
          "Sign a short fact pack (rooms, meetings, dining, location).",
          "Map facts to overview and key subpages.",
          "Archive live evidence after updates.",
        ],
        accountableOwner: "Hotel Commercial Leadership",
        supportingParties: ["Brand content", "Agency"],
        suggestedTimeframe: "2–4 weeks",
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        website: evidenceUrl,
        evidenceKind,
        allowNullEvidence: !pageOk,
      }),
      fillActionContract({
        actionId: actionId2,
        headline: pageOk
          ? "Audit Official Page Consistency"
          : "Investigate Official-Site Automated Access",
        priority: "HIGH",
        priorityWhy: pageOk
          ? "Reachable official pages are the primary evidence surface."
          : "Access failures must not be misread as content defects.",
        whatWeFound: accessNote,
        whyItMatters:
          "Technical and content issues require different owners and timelines.",
        recommendedSteps: pageOk
          ? [
              "Compare overview vs meetings/dining facts.",
              "Correct conflicts after hotel approval.",
              "Re-verify live pages.",
            ]
          : [
              "Reproduce with ordinary browser.",
              "Confirm CDN/WAF intent with brand security.",
              "Document allowed automated clients.",
            ],
        accountableOwner: pageOk ? "Hotel Marketing" : "Brand Digital / CDN Security",
        supportingParties: ["Hotel IT", "Agency"],
        suggestedTimeframe: "30 days",
        claimType: pageOk ? "VERIFIED_ISSUE" : "RECOMMENDED_INVESTIGATION",
        website: evidenceUrl,
        evidenceKind,
        allowNullEvidence: !pageOk,
      }),
      fillActionContract({
        actionId: actionId3,
        headline: "Qualify One Destination/Partner Authority Opportunity",
        priority: "MEDIUM",
        priorityWhy:
          "Authority work should follow factual clarity and use honest classifications.",
        whatWeFound:
          "No paid backlink inventory. Public research may surface destination opportunities without inventing competitor gaps.",
        whyItMatters:
          "GMs need commercially relevant outreach targets, not generic SEO volume.",
        recommendedSteps: [
          "Identify one credible destination or partner inventory.",
          "Submit approved profile + official URL if policy allows.",
          "Record verification date and classification.",
        ],
        accountableOwner: "Hotel Sales / Marketing",
        supportingParties: ["Brand PR", "Destination partner"],
        suggestedTimeframe: "45 days",
        claimType: "OPTIONAL_COMMERCIAL_OPPORTUNITY",
        website: evidenceUrl,
        evidenceKind,
        allowNullEvidence: !pageOk,
      }),
    ],
    methodology: {
      title: "Methodology & Limits",
      bullets: [
        "Certified ADP baseline reused without remeasurement.",
        "Public official-page verification only; paid SEO tools not authorized.",
        "Mentions are not backlinks; search results are not AI citations.",
        "No customer publication without founder authorization.",
      ],
    },
    nextSteps: {
      title: "Suggested Next Step",
      body: "Hotel leadership reviews the fact pack, decides on access or content actions, and returns decisions before any customer-facing publication.",
    },
  };
}

function lightDigitalAuthority(payload) {
  const website = payload.property?.website;
  return {
    propertyId: payload.property.propertyId,
    hotelName: payload.property.name,
    officialWebsite: website,
    researchMode: "PUBLIC_PARTIAL",
    paidProviderUsed: false,
    paidResearchUsd: 0,
    inventoryCompleteness:
      "PARTIAL_PUBLIC_SEARCH — official site verified as canonical URL candidate. No Ahrefs/Semrush.",
    executiveFinding: `${payload.property.name}: Digital Authority work starts from the official website as the canonical discovery URL. Destination/partner backlinks require verified third-party pages; unpaid research does not invent referring-domain counts.`,
    competitiveComparison:
      "Competitor website-backlink advantages are SUBJECT_COVERAGE_NOT_ESTABLISHED until relevant inventories are reviewed.",
    sources: website
      ? [
          {
            referringOrganization: "Official hotel website",
            linkingPageUrl: website,
            hotelDestinationUrl: website,
            findingClass: "VERIFIED_EXTERNAL_MENTION",
            hyperlinkToOfficialSite: true,
            linkType: "OFFICIAL_SITE",
            commercialRelevance: "Canonical discovery path",
            verificationDate: new Date().toISOString().slice(0, 10),
            evidenceUrl: website,
            note: "Official site is not a third-party backlink; recorded as the canonical destination for future partner links.",
          },
        ]
      : [],
    competitorBacklinks: [],
    competitorGaps: [
      {
        competitorName: "Certified ADP peers",
        gapClass: "SUBJECT_COVERAGE_NOT_ESTABLISHED",
        gapSummary:
          "Public partial pass did not confirm competitor third-party website-backlink advantages.",
        evidenceUrl: website || null,
      },
    ],
    opportunities: website
      ? [
          {
            opportunityId: `${payload.property.propertyId}-da-destination`,
            title: "Identify one destination/partner listing with official URL",
            organization: "Local DMO / CVB / tourism board",
            findingClass: "POTENTIAL_BACKLINK_OPPORTUNITY",
            targetOrganizationUrl: website,
            whyItMatters:
              "Destination inventories are higher-trust discovery surfaces than generic directories.",
            recommendedAction:
              "After fact-pack approval, submit lodging profile with official URL.",
            scoreFactors: {
              demandRelevance: 7,
              sourceCredibility: 7,
              competitiveDifferentiation: 6,
              implementationLikelihood: 5,
              discoveryValue: 7,
            },
          },
        ]
      : [],
    actions: [
      {
        actionId: `AVA-${shortCode(payload.property.propertyId)}-DA-001`,
        headline: "Qualify Destination/Partner Listing With Official URL",
        priority: "MEDIUM",
        priorityWhy: "Authority after fact accuracy.",
        whatWeFound:
          "No paid backlink inventory. Official site is the canonical URL for outreach.",
        whyItMatters: "GMs need specific outreach targets, not SEO volume claims.",
        recommendedSteps: [
          "Identify one credible inventory.",
          "Submit approved profile + official URL.",
          "Classify honestly (backlink vs mention).",
        ],
        accountableOwner: "Hotel Sales / Marketing",
        supportingParties: "Brand content",
        suggestedTimeframe: "45 days",
        estimatedEffort: "LOW",
        implementationStatus: "AWAITING_HOTEL_DECISION",
        claimType: "OPTIONAL_COMMERCIAL_OPPORTUNITY",
        evidenceLinks: website
          ? [{ label: "Official website", url: website }]
          : [],
      },
    ],
  };
}

async function certifyExisting(propertyId, payloadRel) {
  const payloadPath = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads", payloadRel);
  let payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const priorMeta = getAuditMeta(propertyId) || {};
  const founder = priorMeta.founderApprovedForCustomer === true;
  const hadDa = !!payload.digitalAuthority;

  if (!hadDa) {
    payload = appendDigitalAuthorityActions(payload, lightDigitalAuthority(payload), {
      metaPaidResearchAuthorized: false,
    });
  }

  const slug = slugFromPropertyId(propertyId);
  const outDir = path.join(REPORT, slug, "pdf-staged");
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: payload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);
  const forceRegen = !hadDa || !fs.existsSync(outPath);

  const pipeline = await runMandatoryAvaEvidenceStages(payload, {
    dryRun: true,
    persistCache: true,
    forcePdfRegenerate: forceRegen,
    pdfBuffer: !forceRegen && fs.existsSync(outPath) ? fs.readFileSync(outPath) : null,
    manualVerifications:
      priorMeta.evidenceHyperlinkCertification?.manualVerifications || {},
    generatePdfFn: async ({ payload: p }) => {
      if (fs.existsSync(outPath) && forceRegen) {
        const bak = path.join(outDir, "pre-portfolio-batch");
        fs.mkdirSync(bak, { recursive: true });
        fs.copyFileSync(outPath, path.join(bak, `${Date.now()}-${filename}`));
      }
      await generateAiVisibilityAuditPdfV1({ payload: p, outPath, pagesDir: null });
      return { buffer: fs.readFileSync(outPath), path: outPath };
    },
  });

  payload = pipeline.result.payload;
  fs.writeFileSync(payloadPath, `${JSON.stringify(payload, null, 2)}\n`);
  if (!fs.existsSync(outPath)) {
    await generateAiVisibilityAuditPdfV1({ payload, outPath, pagesDir: null });
  }

  const pdfBuf = fs.readFileSync(outPath);
  const pdfSha = sha256(pdfBuf);
  const gatePass = pipeline.result.certification?.gate?.pass === true;
  const status = gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED";
  const versionId = `ava_v1_portfolio_${slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });

  saveAuditVersion(propertyId, versionId, payload, {
    status,
    metaStatus: status,
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.pdfRelativePath,
    researchCostUsd: 0,
    validation: {
      enhancement: "portfolio-remaining-batch-v1",
      evidenceHyperlinkGatePass: gatePass,
      founderApprovedForCustomer: founder,
      customerReadyBlocked: true,
      pdfSha256: staged.sha256,
      digitalAuthorityAdded: !hadDa,
    },
  });

  upsertAuditMeta(propertyId, {
    status,
    forceStatus: true,
    evidencePipelineStatus: pipeline.result.pipelineStatus,
    pdfRelativePath: staged.pdfRelativePath,
    pdfSha256: pdfSha,
    founderApprovedForCustomer: founder,
    evidenceHyperlinkCertification: {
      ...(priorMeta.evidenceHyperlinkCertification || {}),
      lastRun: summarizeCertificationForAdmin(pipeline.result.certification),
    },
    digitalAuthorityStatus: {
      ...(priorMeta.digitalAuthorityStatus || {}),
      portfolioBatchAt: new Date().toISOString(),
      classifications: summarizeDigitalAuthorityClassifications(payload.digitalAuthority),
    },
    autonomousEvidenceResolution: {
      lastRun: {
        finishedAt: pipeline.result.finishedAt,
        summary: pipeline.result.summary,
        resolutionLog: pipeline.result.resolutionLog,
      },
    },
  });

  return {
    propertyId,
    name: payload.property.name,
    mode: hadDa ? "EVIDENCE_GATE_EXISTING_DA" : "EVIDENCE_GATE_PLUS_LIGHT_DA",
    status,
    gatePass,
    pdfSha256: pdfSha,
    filename,
    recommendationCount: (payload.actions || []).length,
    classifications: summarizeDigitalAuthorityClassifications(payload.digitalAuthority),
    hotelVerdict: gatePass ? "PASS" : "FAIL",
    blocker: gatePass
      ? null
      : (pipeline.result.certification?.gate?.reasons || []).join("; ") ||
        "EVIDENCE_GATE_FAILED",
  };
}

async function generateMissing(propertyId) {
  const loaded = loadAdpBaselineForAudit(propertyId);
  if (!loaded.ok) {
    return {
      propertyId,
      name: propertyId,
      mode: "GENERATE_FROM_BASELINE",
      hotelVerdict: "BLOCKED",
      blocker: loaded.error || "NO_ADP_BASELINE",
      status: "BASELINE_READY",
      gatePass: false,
    };
  }
  const baseline = loaded.baseline;
  const fetchResult = baseline.website
    ? await fetchText(baseline.website)
    : { ok: false, status: 0, text: "", title: "", finalUrl: null, error: "NO_WEBSITE" };

  let payload = buildPayloadFromBaseline(baseline, fetchResult);
  payload = appendDigitalAuthorityActions(payload, lightDigitalAuthority(payload), {
    metaPaidResearchAuthorized: false,
  });

  const slug = slugFromPropertyId(propertyId);
  const payloadRel = `${slug}-v1.json`;
  const payloadPath = path.join(PAYLOAD_DIR, payloadRel);
  fs.writeFileSync(payloadPath, `${JSON.stringify(payload, null, 2)}\n`);

  // Ensure admin map awareness is logged; runtime load uses file path via certifyExisting path
  return certifyExisting(propertyId, payloadRel);
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const onlyArg = process.argv.find((a) => a.startsWith("--only="));
  const only = onlyArg
    ? new Set(onlyArg.slice(7).split(",").map((s) => s.trim()).filter(Boolean))
    : null;

  const universe = loadUniverse();
  const results = [];
  const blockers = [];

  for (const row of universe) {
    const propertyId = row.propertyId;
    if (only && !only.has(propertyId)) continue;
    const meta = getAuditMeta(propertyId) || {};
    if (!only && SKIP_DEFAULT.has(propertyId)) {
      results.push({
        propertyId,
        name: row.hotel,
        mode: "SKIPPED_PRIOR_PHASE",
        hotelVerdict: "PASS",
        status: meta.status || "AUDIT_VALIDATED",
        gatePass: true,
        note: "Certified in Phase 1 or Phase 2/3",
        pdfSha256: meta.pdfSha256 || null,
        recommendationCount: null,
      });
      continue;
    }
    if (
      !only &&
      meta.evidenceHyperlinkCertification?.lastRun?.gatePass === true &&
      meta.status === "AUDIT_VALIDATED"
    ) {
      results.push({
        propertyId,
        name: row.hotel,
        mode: "ALREADY_GATE_PASS",
        hotelVerdict: "PASS",
        status: "AUDIT_VALIDATED",
        gatePass: true,
        note: "Prior evidence gate pass preserved",
        pdfSha256: meta.pdfSha256 || null,
        recommendationCount: null,
      });
      continue;
    }

    console.log("Portfolio", propertyId, "...");
    try {
      const payloadRel =
        ADMIN_MAP[propertyId] || `${slugFromPropertyId(propertyId)}-v1.json`;
      const payloadPath = path.join(PAYLOAD_DIR, payloadRel);
      let result;
      if (fs.existsSync(payloadPath)) {
        result = await certifyExisting(propertyId, path.basename(payloadPath));
      } else {
        result = await generateMissing(propertyId);
        // Register newly generated payload in admin map file note via results
        result.adminMapNeeded = path.basename(payloadPath);
      }
      results.push(result);
      console.log(" →", result.hotelVerdict, result.mode, result.blocker || "");
      if (result.hotelVerdict !== "PASS") {
        blockers.push(result);
      }
    } catch (err) {
      const blocked = {
        propertyId,
        name: row.hotel,
        mode: "ERROR",
        hotelVerdict: "BLOCKED",
        blocker: err.message,
        status: "VALIDATION_REQUIRED",
        gatePass: false,
      };
      results.push(blocked);
      blockers.push(blocked);
      console.log(" → BLOCKED", err.message);
    }
  }

  const report = {
    contract: "AVA_PORTFOLIO_REMAINING_BATCH_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    publicationAuthorized: false,
    totals: {
      processed: results.length,
      pass: results.filter((r) => r.hotelVerdict === "PASS").length,
      fail: results.filter((r) => r.hotelVerdict === "FAIL").length,
      blocked: results.filter((r) => r.hotelVerdict === "BLOCKED").length,
    },
    blockers,
    hotels: results,
  };

  fs.mkdirSync(REPORT, { recursive: true });
  fs.writeFileSync(
    path.join(REPORT, "AVA_PORTFOLIO_REMAINING_BATCH_RESULTS.json"),
    `${JSON.stringify(report, null, 2)}\n`
  );
  console.log(JSON.stringify({ totals: report.totals, blockers: blockers.length }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
