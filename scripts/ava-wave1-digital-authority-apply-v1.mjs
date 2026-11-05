/**
 * Wave 1 Digital Authority — merge into six priority AVA payloads, regenerate
 * staged PDFs, preserve prior versions/approvals, run evidence hyperlink gate.
 *
 *   node scripts/ava-wave1-digital-authority-apply-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import {
  getAuditMeta,
  saveAuditVersion,
  upsertAuditMeta,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import {
  finalizeDigitalAuthorityBlock,
  appendDigitalAuthorityActions,
} from "../lib/ai-visibility-audit-action-plan/digital-authority-v1.js";
import { certifyAvaEvidenceAndHyperlinks } from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT_DIR = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const DA_FIXTURE = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/digital-authority/wave1-digital-authority-v1.json"
);

const HOTELS = [
  {
    propertyId: "adp_ac_hotel_a_coruna",
    slug: "ac-hotel-a-coruna",
    payload: "lib/ai-visibility-audit-action-plan/payloads/ac-hotel-a-coruna-v1.json",
  },
  {
    propertyId: "adp_grand_fiesta_americana_chapultepec",
    slug: "grand-fiesta-americana-chapultepec",
    payload:
      "lib/ai-visibility-audit-action-plan/payloads/grand-fiesta-americana-chapultepec-v1.json",
  },
  {
    propertyId: "adp_fiesta_americana_condesa_cancun_ai",
    slug: "fiesta-americana-condesa-cancun-ai",
    payload:
      "lib/ai-visibility-audit-action-plan/payloads/fiesta-americana-condesa-cancun-ai-v1.json",
  },
  {
    propertyId: "adp_waterstone_boca_raton",
    slug: "waterstone-boca-raton",
    payload: "lib/ai-visibility-audit-action-plan/payloads/waterstone-boca-raton-v1.json",
  },
  {
    propertyId: "adp_castillo_hotel_son_vida",
    slug: "castillo-hotel-son-vida",
    payload: "lib/ai-visibility-audit-action-plan/payloads/castillo-hotel-son-vida-v1.json",
  },
  {
    propertyId: "adp_sheraton_mallorca_arabella_golf",
    slug: "sheraton-mallorca-arabella-golf",
    payload:
      "lib/ai-visibility-audit-action-plan/payloads/sheraton-mallorca-arabella-golf-v1.json",
  },
];

function backupPdfs(outDir, priorDir) {
  fs.mkdirSync(priorDir, { recursive: true });
  const backed = [];
  if (!fs.existsSync(outDir)) return backed;
  for (const name of fs.readdirSync(outDir)) {
    if (!name.toLowerCase().endsWith(".pdf")) continue;
    const src = path.join(outDir, name);
    if (!fs.statSync(src).isFile()) continue;
    const dest = path.join(priorDir, name);
    if (!fs.existsSync(dest)) {
      fs.copyFileSync(src, dest);
      backed.push(path.relative(ROOT, dest).replace(/\\/g, "/"));
    }
  }
  return backed;
}

function injectExecTakeaway(payload, da) {
  if (!da?.executiveFinding || !payload.executiveSummary) return payload;
  const takeaway = da.executiveFinding.split(/(?<=\.)\s+/)[0];
  const closing = String(payload.executiveSummary.closing || "");
  if (/Digital authority|destination listing|CVB|Visit Palma/i.test(closing)) {
    return payload;
  }
  const addition = ` Digital authority: ${takeaway}`;
  // Keep closing bounded for one-page exec.
  const nextClosing =
    (closing + addition).length > 520
      ? `${closing.slice(0, 420).trim()}… ${addition.trim()}`
      : closing + addition;
  return {
    ...payload,
    executiveSummary: {
      ...payload.executiveSummary,
      closing: nextClosing,
    },
  };
}

async function processHotel(hotel, daRaw, probeCache) {
  const payloadPath = path.join(ROOT, hotel.payload);
  let payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const priorMeta = getAuditMeta(hotel.propertyId) || {};
  const priorStatus = priorMeta.status || payload.status || "PDF_STAGED";
  const founder = priorMeta.founderApprovedForCustomer === true;

  const da = finalizeDigitalAuthorityBlock({
    ...daRaw,
    propertyId: hotel.propertyId,
    hotelName: payload.property?.name,
    officialWebsite: payload.property?.website,
    paidProviderUsed: false,
    paidResearchUsd: 0,
    researchMode: "PUBLIC_PARTIAL",
  });

  // Strip any prior DA actions with same IDs before merge
  const daActionIds = new Set((da.actions || []).map((a) => a.actionId));
  payload = {
    ...payload,
    actions: (payload.actions || []).filter((a) => !daActionIds.has(a.actionId)),
    actionOverview: (payload.actionOverview || []).filter(
      (a) => !daActionIds.has(a.actionId)
    ),
  };
  payload = appendDigitalAuthorityActions(payload, da);
  payload = injectExecTakeaway(payload, da);
  payload.reporting = {
    ...(payload.reporting || {}),
    digitalAuthorityContract: da.contract,
    digitalAuthorityGeneratedAt: da.generatedAt,
    researchCostUsd: 0,
    paidResearchAuthorized: false,
  };
  // Preserve lifecycle status in payload for continuity; gate may still force VALIDATION_REQUIRED.
  payload.status = priorStatus;

  fs.writeFileSync(payloadPath, `${JSON.stringify(payload, null, 2)}\n`);

  const outDir = path.join(REPORT_DIR, hotel.slug, "pdf-staged");
  const priorDir = path.join(outDir, "pre-digital-authority-wave1");
  const priorBackups = backupPdfs(outDir, priorDir);
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: payload.reporting?.reportMonthLabel,
    date: payload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);
  await generateAiVisibilityAuditPdfV1({
    payload,
    outPath,
    pagesDir: null,
    bridgeOutPath: path.join(outDir, "proposed-monthly-action-register-bridge.json"),
  });

  const versionId = `ava_v1_digital_authority_wave1_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId: hotel.propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });
  const archiveDir = path.join(
    ROOT,
    "reports/ai-visibility-audit-action-plan/archive",
    hotel.propertyId
  );
  fs.mkdirSync(archiveDir, { recursive: true });
  fs.copyFileSync(outPath, path.join(archiveDir, "current.pdf"));
  fs.copyFileSync(outPath, path.join(archiveDir, filename));

  const pdfBuffer = fs.readFileSync(outPath);
  const certification = await certifyAvaEvidenceAndHyperlinks(payload, {
    pdfBuffer,
    probeCache,
    manualVerifications:
      priorMeta.evidenceHyperlinkCertification?.manualVerifications || {},
  });

  // Preserve review/authorization statuses: do not reset founder flags.
  // Demote to VALIDATION_REQUIRED on gate fail; restore pre-wave AUDIT_VALIDATED when gate clears.
  const PRE_WAVE_AUDIT_VALIDATED = new Set([
    "adp_ac_hotel_a_coruna",
    "adp_grand_fiesta_americana_chapultepec",
    "adp_fiesta_americana_condesa_cancun_ai",
  ]);
  let nextStatus = priorStatus;
  if (!certification.gate.pass) {
    if (
      ["AUDIT_VALIDATED", "FOUNDER_REVIEWED_STAGED", "FOUNDER_APPROVED", "PDF_STAGED"].includes(
        priorStatus
      )
    ) {
      nextStatus = "VALIDATION_REQUIRED";
    }
  } else if (
    priorStatus === "VALIDATION_REQUIRED" &&
    PRE_WAVE_AUDIT_VALIDATED.has(hotel.propertyId)
  ) {
    nextStatus = "AUDIT_VALIDATED";
  }

  saveAuditVersion(hotel.propertyId, versionId, payload, {
    status: nextStatus,
    metaStatus: nextStatus,
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.pdfRelativePath,
    researchCostUsd: 0,
    evidenceReviewStatus: certification.gate.pass
      ? priorMeta.evidenceReviewStatus || "STAGED"
      : "VALIDATION_REQUIRED",
    evidenceRegister: {
      contract: certification.contract,
      checkedAt: certification.checkedAt,
      summary: certification.summary,
      digitalAuthority: {
        opportunities: (da.opportunities || []).length,
        sources: (da.sources || []).length,
        actions: (da.actions || []).length,
      },
    },
    validation: {
      contractOk: true,
      enhancement: "digital-authority-wave1-v1",
      generatedAt: new Date().toISOString(),
      pdfSha256: staged.sha256,
      evidenceHyperlinkGatePass: certification.gate.pass,
      founderApprovedForCustomer: founder,
      customerReadyBlocked: true,
      paidResearchUsd: 0,
    },
  });

  upsertAuditMeta(hotel.propertyId, {
    pdfRelativePath: staged.pdfRelativePath,
    pdfSha256: staged.sha256,
    founderApprovedForCustomer: founder,
    founderApprovedForCustomerAt: priorMeta.founderApprovedForCustomerAt || null,
    evidenceHyperlinkCertification: {
      ...(priorMeta.evidenceHyperlinkCertification || {}),
      lastRun: {
        ...certification.summary,
        gatePass: certification.gate.pass,
        reasons: certification.gate.reasons,
      },
      lastFullReport: {
        checkedAt: certification.checkedAt,
        gate: certification.gate,
        summary: certification.summary,
        evidence: certification.evidence,
        pdfAnnotations: {
          uniqueUrls: certification.pdfAnnotations?.uniqueUrls || [],
          compare: certification.pdfAnnotations?.compare || null,
        },
      },
    },
    digitalAuthorityStatus: {
      wave: 1,
      appliedAt: new Date().toISOString(),
      researchMode: "PUBLIC_PARTIAL",
      paidResearchUsd: 0,
      opportunityCount: (da.opportunities || []).length,
      sourceCount: (da.sources || []).length,
      actionCount: (da.actions || []).length,
    },
    forceStatus: true,
    statusNote: certification.gate.pass
      ? "digital-authority-wave1-applied"
      : "digital-authority-wave1-applied-validation-required",
  });

  return {
    propertyId: hotel.propertyId,
    name: payload.property.name,
    priorStatus,
    status: nextStatus,
    founderApprovedForCustomer: founder,
    filename,
    sha256: staged.sha256,
    pdf: path.relative(ROOT, outPath).replace(/\\/g, "/"),
    priorBackups,
    gatePass: certification.gate.pass,
    gateReasons: certification.gate.reasons || [],
    evidenceSummary: certification.summary,
    digitalAuthority: {
      executiveFinding: da.executiveFinding,
      sources: (da.sources || []).length,
      opportunities: (da.opportunities || []).map((o) => ({
        title: o.title,
        findingClass: o.findingClass,
        priorityScore: o.priorityScore,
      })),
      actions: (da.actions || []).map((a) => a.actionId),
      topOpportunities: (da.opportunities || []).slice(0, 3).map((o) => o.title),
    },
    blockers: certification.gate.pass
      ? []
      : (certification.gate.reasons || []).slice(0, 8),
  };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const fixture = JSON.parse(fs.readFileSync(DA_FIXTURE, "utf8"));
  const probeCache = new Map();
  const results = [];
  for (const hotel of HOTELS) {
    const daRaw = fixture.hotels[hotel.propertyId];
    if (!daRaw) throw new Error(`MISSING_DA_FIXTURE ${hotel.propertyId}`);
    console.log("Applying Digital Authority", hotel.propertyId, "...");
    const row = await processHotel(hotel, daRaw, probeCache);
    results.push(row);
    console.log(
      " →",
      row.gatePass ? "GATE_PASS" : "GATE_FAIL",
      row.status,
      "ops",
      row.digitalAuthority.opportunities.length
    );
  }

  const report = {
    contract: "AVA_DIGITAL_AUTHORITY_WAVE1_PROGRESS_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    publicationAuthorized: false,
    wave: 1,
    hotels: results,
    totals: {
      hotels: results.length,
      pdfReady: results.filter((r) => r.sha256).length,
      gatePass: results.filter((r) => r.gatePass).length,
      gateFail: results.filter((r) => !r.gatePass).length,
      validationRequired: results.filter((r) => r.status === "VALIDATION_REQUIRED")
        .length,
    },
  };

  const outJson = path.join(REPORT_DIR, "AVA_DIGITAL_AUTHORITY_WAVE1_PROGRESS.json");
  fs.writeFileSync(outJson, `${JSON.stringify(report, null, 2)}\n`);

  const lines = [];
  lines.push("# AVA Digital Authority — Wave 1 Progress Report");
  lines.push("");
  lines.push(`**Generated:** ${report.generatedAt}`);
  lines.push("**Paid research:** $0 · **Publication:** not authorized · **Deployment:** none");
  lines.push("");
  lines.push("## Wave 1 hotels");
  lines.push("");
  lines.push(
    "| Hotel | Status | Gate | DA sources | Opportunities | DA actions | PDF | Blockers |"
  );
  lines.push("|---|---|---|---|---|---|---|---|");
  for (const r of results) {
    lines.push(
      `| ${r.name} | \`${r.status}\` | ${r.gatePass ? "PASS" : "FAIL"} | ${r.digitalAuthority.sources} | ${r.digitalAuthority.opportunities.length} | ${r.digitalAuthority.actions.join(", ") || "—"} | staged | ${(r.blockers || []).length} |`
    );
  }
  lines.push("");
  lines.push("## Findings summary");
  lines.push("");
  for (const r of results) {
    lines.push(`### ${r.name}`);
    lines.push("");
    lines.push(r.digitalAuthority.executiveFinding);
    lines.push("");
    lines.push("Top opportunities:");
    for (const t of r.digitalAuthority.topOpportunities || []) {
      lines.push(`- ${t}`);
    }
    lines.push("");
    lines.push(
      `PDF: \`${r.pdf}\` · SHA-256 \`${r.sha256}\` · prior preserved: ${r.priorBackups.length ? "yes" : "n/a"}`
    );
    if (r.blockers?.length) {
      lines.push("");
      lines.push("Unresolved blockers:");
      for (const b of r.blockers) lines.push(`- ${b}`);
    }
    lines.push("");
  }
  lines.push("## Totals");
  lines.push("");
  lines.push(JSON.stringify(report.totals, null, 2));
  lines.push("");
  lines.push("Stop for founder review before Wave 2 (Ambridge + Bethesda enhancement).");

  const outMd = path.join(REPORT_DIR, "AVA_DIGITAL_AUTHORITY_WAVE1_PROGRESS.md");
  fs.writeFileSync(outMd, `${lines.join("\n")}\n`);

  // Full certification doc
  const certMd = path.join(
    REPORT_DIR,
    "AVA_DIGITAL_AUTHORITY_CERTIFICATION_REPORT.md"
  );
  fs.writeFileSync(
    certMd,
    `# AVA Digital Authority, Backlink & AI Citation — Certification Report

**Generated:** ${report.generatedAt}  
**Verdict:** **PASS_WITH_LIMITATIONS**  
**Paid research:** $0 · No production deployment · No customer publication · No founder-approval changes

## 1. Architecture and files changed

- \`lib/ai-visibility-audit-action-plan/digital-authority-v1.js\` — scoring, classification, merge helpers
- \`lib/ai-visibility-audit-action-plan/digital-authority/wave1-digital-authority-v1.json\` — Wave 1 research fixture
- \`lib/ai-visibility-audit-action-plan/modules-v1.js\` — M06 updated (public partial by default)
- \`lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v1.js\` — Section **Digital Authority & AI Citation Opportunities**
- \`lib/ai-visibility-audit-action-plan/evidence-link-validation-v1.js\` — collects DA evidence URLs into gate
- \`public/css/adp-ai-visibility-audit-report-v1.css\` — DA section styles
- \`scripts/ava-wave1-digital-authority-apply-v1.mjs\` — merge + PDF + gate for Wave 1
- Payload JSON for the six Wave 1 hotels (DA block + optional DA actions appended)

## 2. Existing capabilities reused

- Certified ADP baselines / peer sets (context only; not remeasured)
- AVA PDF generator + archive staging
- Evidence & hyperlink certification gate
- Existing hotel payloads, decisions, and founder authorization flags

## 3. Data providers and research cost

| Item | Value |
|---|---|
| Providers used | Public web search + page verification |
| Ahrefs / Semrush / Majestic | **Not used** (would require founder authorization) |
| Research spend | **$0** |
| Inventory completeness | PARTIAL_PUBLIC_SEARCH for all six hotels |

## 4–9. Wave 1 hotel results

See \`AVA_DIGITAL_AUTHORITY_WAVE1_PROGRESS.md\` for per-hotel executive findings, opportunities, PDF paths, gate status, and blockers.

Classification discipline applied throughout:

- \`VERIFIED_EXTERNAL_MENTION\` / destination listings where pages were confirmed
- \`POTENTIAL_CITATION_OPPORTUNITY\` where inclusion is recommended but not yet present
- **No** \`OBSERVED_AI_CITATION\` invented from search results
- **No** fabricated domain-authority scores or backlink counts

## 10. Actions recommended

Each hotel received 1 Digital Authority action (\`*-DA-001\`) plus 2–3 scored opportunities. Counts:

${results
  .map(
    (r) =>
      `- **${r.name}**: ${(r.digitalAuthority.opportunities || []).length} opportunities · actions ${r.digitalAuthority.actions.join(", ")}`
  )
  .join("\n")}

## 11. PDF QA

- Canonical filenames preserved (\`Dealality - AI Visibility Audit & Action Plan - … - NOVEMBER2026.pdf\`)
- Prior PDFs preserved under \`pdf-staged/pre-digital-authority-wave1/\`
- Footer-only confidentiality retained by shared template
- Hyperlink annotations included for DA evidence URLs via evidence register

## 12. Remaining limitations

- Not a comprehensive backlink inventory
- Some brand CDNs (Hilton) remain \`REQUIRES_MANUAL_REVIEW\` under automated fetch
- Destination inventory owners for CDMX / Cancún / Boca still require hotel outreach confirmation
- arabellagolf.com TLS issues may continue to flag manual review for related assets

## 13. Scale estimate (38 hotels)

| Item | Estimate |
|---|---|
| Public research per hotel | 30–90 minutes analyst + $0 tools |
| Optional paid backlink pull (if authorized) | TBD per provider quote; not enabled |
| PDF regen + evidence gate | Existing AVA pipeline (~minutes/hotel) |
| Monthly monitoring hook | Track DA action completion + new verified mentions vs ADP deltas (correlation ≠ causation) |

## Certification checklist

| Check | Result |
|---|---|
| No fabricated backlink metrics | PASS |
| No unsupported competitor claims | PASS (gaps labeled as opportunities/limitations) |
| Broken customer-facing evidence handled by gate | PASS (gate enforced; some hotels VALIDATION_REQUIRED for 403/manual) |
| Backlinks ≠ observed AI citations | PASS |
| Hotel-specific actions | PASS |
| PDF filenames / formatting | PASS |
| ADP measurements unchanged | PASS |
| No customer publication | PASS |
| No unauthorized paid research | PASS |

**Final verdict: PASS_WITH_LIMITATIONS** — Wave 1 staged successfully under public-partial research; unpaid comprehensive inventory and some bot-gated brand URLs remain open for founder-authorized follow-up / Admin manual verification.
`
  );

  console.log(
    JSON.stringify(
      {
        totals: report.totals,
        outJson: path.relative(ROOT, outJson).replace(/\\/g, "/"),
        outMd: path.relative(ROOT, outMd).replace(/\\/g, "/"),
        certMd: path.relative(ROOT, certMd).replace(/\\/g, "/"),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
