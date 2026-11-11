#!/usr/bin/env node
/**
 * Waterstone governed publication cutover — preflight + optional authorized publish.
 *
 * Default: PREPARE ONLY (no publish).
 * Publish only when BOTH are present:
 *   1) --authorize-publish
 *   2) Active authorization file with FOUNDER_APPROVED_FOR_PUBLICATION for exact staged version
 *
 *   node scripts/dealality-adp-waterstone-governed-publication-cutover-v1.mjs
 *   node scripts/dealality-adp-waterstone-governed-publication-cutover-v1.mjs --authorize-publish
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";

import {
  loadStagedOwnerPayload,
  loadStagedManifest,
} from "../lib/ai-demand-positioning/review/adp-staged-report-store-v1.js";
import {
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { derivePropertyRealityCoverage } from "../lib/ai-demand-positioning/presentation/adp-trends-presentation-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/dealality-adp-waterstone-governed-publication-cutover-v1"
);
const PROPERTY_ID = "adp_waterstone_boca_raton";
const EXPECTED_STAGED = "staged_mw14o9lq_bb8bd9570c";
const EXPECTED_STAGED_SHA =
  "9fe5fb1929ce3f745ed1c50f74285fba0d1015efa061eb6caa855ae512ca33c6";
const EXPECTED_PDF_SHA =
  "c8b8d6df17810fd47f2f8f25001cf18af79a82af603c782591553d14ab776ecf";
const METH = "ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1";
const SHARE_ID = "sht_e0b96090b0246151ec2db44a";
const AUTH_PATH = path.join(OUT, "FOUNDER_APPROVED_FOR_PUBLICATION.json");
const DO_PUBLISH = process.argv.includes("--authorize-publish");
const PRIOR_AUTH_CANDIDATE = "staged_mw134onw_939c6994bf";

fs.mkdirSync(OUT, { recursive: true });
function write(name, body) {
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string"
      ? body.endsWith("\n")
        ? body
        : body + "\n"
      : JSON.stringify(body, null, 2) + "\n"
  );
  return p;
}
function sha256File(p) {
  return crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

const staged = loadStagedOwnerPayload(PROPERTY_ID);
const man = loadStagedManifest(PROPERTY_ID);
const pub = loadPublishedManifest(PROPERTY_ID);
const stagedAbs = path.join(
  ROOT,
  "data/ai-demand-positioning/staged",
  PROPERTY_ID,
  man.reportFile
);
const stagedSha = fs.existsSync(stagedAbs) ? sha256File(stagedAbs) : null;
const p = staged?.payload || {};
const em = p.executiveMetrics || {};
const providers = (p.evidence?.providers || []).map((r) => ({
  provider: r.provider,
  comparable: r.comparable,
  mentioned: r.mentioned,
  presence: r.presence,
}));
const overall = p.competitiveRankingByTerritory?.byTerritory?.overall;
const subject = (overall?.displayRows || []).find((r) => r.isSubject);
const pdfAbs = path.join(
  ROOT,
  "data/ai-demand-positioning/full-adp-e2e-pdf",
  PROPERTY_ID,
  EXPECTED_STAGED,
  "report.pdf"
);
const reconDir = path.join(
  ROOT,
  "reports/dealality-adp-waterstone-final-report-reconciliation-v1"
);
const phase7Path = path.join(
  ROOT,
  "reports/dealality-adp-universal-provider-grain-repair-and-e2e-cert-v1/PHASE7_WATERSTONE_REVALIDATION.json"
);
const phase7 = fs.existsSync(phase7Path)
  ? JSON.parse(fs.readFileSync(phase7Path, "utf8"))
  : null;
const browserParity = fs.existsSync(
  path.join(reconDir, "BROWSER_HTML_PDF_PARITY.json")
)
  ? JSON.parse(
      fs.readFileSync(path.join(reconDir, "BROWSER_HTML_PDF_PARITY.json"), "utf8")
    )
  : null;
const reconGates = fs.existsSync(path.join(reconDir, "GATES.json"))
  ? JSON.parse(fs.readFileSync(path.join(reconDir, "GATES.json"), "utf8"))
  : null;

// Preserve snapshot of current published for rollback
const priorPublishedSnapshot = {
  propertyId: PROPERTY_ID,
  latestPeriodId: pub?.latestPeriodId || null,
  latestPublishedAt: pub?.latestPublishedAt || null,
  publishStatus: pub?.publishStatus || null,
  reportFile: pub?.reportFile || null,
  artifactHash: pub?.artifactHash || null,
  externalDistributionHold: pub?.externalDistributionHold || null,
  shareId: SHARE_ID,
  capturedAt: new Date().toISOString(),
};
if (pub?.reportFile) {
  const pubReportAbs = path.join(
    ROOT,
    "data/ai-demand-positioning/published",
    PROPERTY_ID,
    pub.reportFile
  );
  if (fs.existsSync(pubReportAbs)) {
    const backupDir = path.join(OUT, "PRIOR_PUBLISHED_BACKUP");
    fs.mkdirSync(backupDir, { recursive: true });
    const backupFile = path.join(backupDir, pub.reportFile);
    fs.copyFileSync(pubReportAbs, backupFile);
    priorPublishedSnapshot.backupPath = path
      .relative(ROOT, backupFile)
      .replace(/\\/g, "/");
    priorPublishedSnapshot.backupSha256 = sha256File(backupFile);
    if (pub.manifestFile || true) {
      const manAbs = path.join(
        ROOT,
        "data/ai-demand-positioning/published",
        PROPERTY_ID,
        "manifest.json"
      );
      if (fs.existsSync(manAbs)) {
        const manBackup = path.join(backupDir, "manifest.json");
        fs.copyFileSync(manAbs, manBackup);
        priorPublishedSnapshot.manifestBackupPath = path
          .relative(ROOT, manBackup)
          .replace(/\\/g, "/");
        priorPublishedSnapshot.manifestBackupSha256 = sha256File(manBackup);
      }
    }
  }
}
write("PRIOR_PUBLISHED_PRESERVATION.json", priorPublishedSnapshot);

const preChecks = {
  stagedVersionMatch: staged?.versionId === EXPECTED_STAGED,
  manifestPointsToExpected: man?.latestStagedVersionId === EXPECTED_STAGED,
  stagedFileExists: fs.existsSync(stagedAbs),
  stagedFileSha256: stagedSha,
  manifestArtifactHash: man?.artifactHash || null,
  hashReconciles:
    Boolean(stagedSha) &&
    Boolean(man?.artifactHash) &&
    (stagedSha === man.artifactHash ||
      stagedSha.startsWith(String(man.artifactHash).slice(0, 16)) ||
      String(man.artifactHash).includes(stagedSha.slice(0, 16))),
  stagedShaExactMatch: stagedSha === EXPECTED_STAGED_SHA,
  methodologyId: em.considerationRate?.methodologyId || null,
  methodologyMatch: em.considerationRate?.methodologyId === METH,
  consideration535: em.considerationRate?.rate === 53.5,
  consideration137of256:
    em.considerationRate?.presentObservations === 137 &&
    em.considerationRate?.comparableObservations === 256,
  presence766:
    em.scenarioPresence?.rate === 76.6 &&
    em.scenarioPresence?.eligibleScenarios === 64,
  presence49of64:
    em.scenarioPresence?.capturedScenarios === 49 &&
    em.scenarioPresence?.eligibleScenarios === 64,
  coverage241: derivePropertyRealityCoverage(p.realityGap || {}) === 24.1,
  gap69: p.realityGap?.gapScore === 69,
  providersAll64: providers.every((x) => Number(x.comparable) === 64),
  providerSum256: providers.reduce((a, x) => a + Number(x.comparable || 0), 0) === 256,
  providerMentions137:
    providers.reduce((a, x) => a + Number(x.mentioned || 0), 0) === 137,
  providerBreakdownExact:
    Number(providers.find((x) => x.provider === "gemini")?.mentioned) === 44 &&
    Number(providers.find((x) => x.provider === "perplexity")?.mentioned) === 33 &&
    Number(providers.find((x) => x.provider === "claude")?.mentioned) === 32 &&
    Number(providers.find((x) => x.provider === "openai")?.mentioned) === 28,
  no78InProviders: providers.every((x) => Number(x.comparable) !== 78),
  subjectRank2: Number(subject?.displayRank ?? subject?.observedRank ?? subject?.rank) === 2,
  subjectAppearances137: Number(subject?.appearances) === 137,
  subjectNameWaterstone: /waterstone/i.test(String(subject?.name || "")),
  lineagePresent: Boolean(p.correctionLineage),
  lineagePublicationFalse: p.correctionLineage?.publicationAuthorized === false,
  evidenceDenominatorNote: Boolean(p.evidence?.denominatorNote),
  pdfExists: fs.existsSync(pdfAbs),
  pdfSha256: fs.existsSync(pdfAbs) ? sha256File(pdfAbs) : null,
  pdfShaExactMatch:
    fs.existsSync(pdfAbs) && sha256File(pdfAbs) === EXPECTED_PDF_SHA,
  pdfBytes: fs.existsSync(pdfAbs) ? fs.statSync(pdfAbs).size : 0,
  // Prefer certified mw14o9lq Phase7 revalidation; legacy recon is informational only.
  phase7Pass:
    phase7?.checks?.pass === true &&
    phase7?.checks?.versionId === EXPECTED_STAGED &&
    phase7?.checks?.browserPass === true,
  browserParityPassLegacy: browserParity?.pass === true,
  reconGatesCriticalLegacy: reconGates?.allCritical === true,
  priorPublishedPreserved: Boolean(priorPublishedSnapshot.backupPath),
  priorAuthCandidateSuperseded: PRIOR_AUTH_CANDIDATE,
  providers,
  subject: {
    rank: subject?.displayRank ?? subject?.observedRank ?? subject?.rank,
    appearances: subject?.appearances,
    name: subject?.name,
  },
};
preChecks.pass = [
  "stagedVersionMatch",
  "manifestPointsToExpected",
  "stagedFileExists",
  "stagedShaExactMatch",
  "methodologyMatch",
  "consideration535",
  "consideration137of256",
  "presence766",
  "presence49of64",
  "coverage241",
  "gap69",
  "providersAll64",
  "providerSum256",
  "providerMentions137",
  "providerBreakdownExact",
  "no78InProviders",
  "subjectRank2",
  "subjectAppearances137",
  "subjectNameWaterstone",
  "lineagePresent",
  "evidenceDenominatorNote",
  "pdfExists",
  "pdfShaExactMatch",
  "phase7Pass",
  "priorPublishedPreserved",
].every((k) => preChecks[k] === true);

write("PRE_PUBLICATION_CHECKS.json", preChecks);

// Exact authorization record template (NOT active until founder signs)
const authTemplate = {
  contract: "ADP_WATERSTONE_GOVERNED_PUBLICATION_CUTOVER_V1",
  authorizationType: "FOUNDER_APPROVED_FOR_PUBLICATION",
  status: "AWAITING_FOUNDER_SIGNATURE",
  active: false,
  propertyId: PROPERTY_ID,
  hotelName: "Waterstone Resort & Marina",
  stagedVersionId: EXPECTED_STAGED,
  methodologyId: METH,
  periodId: p.period?.periodId || man?.periodId || null,
  stagedFileSha256: stagedSha,
  manifestArtifactHash: man?.artifactHash || null,
  pdfSha256: preChecks.pdfSha256,
  expectedKpis: {
    considerationRate: 53.5,
    scenarioPresenceRate: 76.6,
    realityCoverage: 24.1,
    realityGap: 69.0,
    competitiveRank: 2,
    providerDenominator: 64,
    comparableObservations: 256,
    subjectMentions: 137,
    presenceScenarios: 49,
  },
  externalShare: {
    shareId: SHARE_ID,
    reportScope: "current_published",
    rotateToken: false,
    createSecondShare: false,
    keepDistributionHoldUntilPostPubCert: true,
  },
  cutoverRules: {
    publishOnlyExactStagedSuccessor: true,
    preservePriorPublished: true,
    doNotModifyUnrelatedHotels: true,
    preferPublishInPlace: true,
  },
  prePublicationChecksPass: preChecks.pass,
  preparedAt: new Date().toISOString(),
  // Founder must set ALL of the following to activate:
  founderSignature: null,
  founderSignedAt: null,
  explicitPhrase: null,
  activationInstructions: [
    "Set status to ACTIVE",
    "Set active to true",
    'Set founderSignature to your name/handle',
    "Set founderSignedAt to ISO timestamp",
    `Set explicitPhrase exactly to: FOUNDER_APPROVED_FOR_PUBLICATION ${EXPECTED_STAGED}`,
    "Then re-run: node scripts/dealality-adp-waterstone-governed-publication-cutover-v1.mjs --authorize-publish",
  ],
  supersedes: {
    priorCandidateStagedVersionId: PRIOR_AUTH_CANDIDATE,
    priorMixedGrainCandidate: "staged_mw0jzj3n_90c1663c9f",
    certifiedReference: EXPECTED_STAGED,
  },
  requiredPhrase: `FOUNDER_APPROVED_FOR_PUBLICATION ${EXPECTED_STAGED}`,
};

// Never overwrite an already-active signed auth with a template
let authRecord = authTemplate;
if (fs.existsSync(AUTH_PATH)) {
  try {
    const existing = JSON.parse(fs.readFileSync(AUTH_PATH, "utf8"));
    if (
      existing?.active === true &&
      existing?.status === "ACTIVE" &&
      existing?.stagedVersionId === EXPECTED_STAGED &&
      existing?.explicitPhrase ===
        `FOUNDER_APPROVED_FOR_PUBLICATION ${EXPECTED_STAGED}`
    ) {
      authRecord = existing;
    } else if (existing?.active !== true) {
      // Refresh template fields but keep any partial founder fields
      authRecord = {
        ...authTemplate,
        founderSignature: existing.founderSignature ?? null,
        founderSignedAt: existing.founderSignedAt ?? null,
        explicitPhrase: existing.explicitPhrase ?? null,
        status: existing.status || authTemplate.status,
        active: false,
      };
      write("FOUNDER_APPROVED_FOR_PUBLICATION.json", authRecord);
    }
  } catch {
    write("FOUNDER_APPROVED_FOR_PUBLICATION.json", authTemplate);
  }
} else {
  write("FOUNDER_APPROVED_FOR_PUBLICATION.json", authTemplate);
}

function isActiveAuthorization(rec) {
  return (
    rec &&
    rec.active === true &&
    rec.status === "ACTIVE" &&
    rec.authorizationType === "FOUNDER_APPROVED_FOR_PUBLICATION" &&
    rec.stagedVersionId === EXPECTED_STAGED &&
    rec.methodologyId === METH &&
    rec.explicitPhrase === `FOUNDER_APPROVED_FOR_PUBLICATION ${EXPECTED_STAGED}` &&
    Boolean(rec.founderSignature) &&
    Boolean(rec.founderSignedAt) &&
    rec.stagedFileSha256 === stagedSha &&
    rec.stagedFileSha256 === EXPECTED_STAGED_SHA &&
    (rec.pdfSha256 == null || rec.pdfSha256 === EXPECTED_PDF_SHA)
  );
}

const authorizationActive = isActiveAuthorization(authRecord);

const result = {
  cutoverStatus: "BLOCKED_AWAITING_FOUNDER_AUTHORIZATION",
  prePublicationChecksPass: preChecks.pass,
  authorizationActive,
  authorizationPath: path
    .relative(ROOT, AUTH_PATH)
    .replace(/\\/g, "/"),
  authorizePublishFlag: DO_PUBLISH,
  published: false,
  previousPublished: priorPublishedSnapshot,
  newPublishedVersionId: null,
  productionReadback: null,
  externalUrl: null,
  externalAccessVerification: null,
  safeToShare: false,
  customerReady: false,
  distributionHold: "HOLD_UNTIL_POST_PUBLICATION_CERT",
  rollbackReadiness: {
    priorBackupPath: priorPublishedSnapshot.backupPath || null,
    priorManifestBackup: priorPublishedSnapshot.manifestBackupPath || null,
    rollbackReady: Boolean(priorPublishedSnapshot.backupPath),
  },
};

if (!preChecks.pass) {
  result.cutoverStatus = "BLOCKED_PRE_PUBLICATION_CHECKS_FAILED";
} else if (!authorizationActive) {
  result.cutoverStatus = "BLOCKED_AWAITING_FOUNDER_AUTHORIZATION";
  result.stopReason =
    `No explicit active FOUNDER_APPROVED_FOR_PUBLICATION for ${EXPECTED_STAGED}. Preparation request is not authorization.`;
} else if (!DO_PUBLISH) {
  result.cutoverStatus = "AUTHORIZED_AWAITING_PUBLISH_FLAG";
  result.stopReason =
    "Authorization active, but --authorize-publish was not passed.";
} else {
  // Intentionally not auto-importing publish here in prepare mode path —
  // only reached when auth+flag present; publish happens in authorized branch below.
  result.cutoverStatus = "AUTHORIZED_READY_TO_PUBLISH";
}

if (preChecks.pass && authorizationActive && DO_PUBLISH) {
  // Governed FS publish: promote exact staged payload → Live published/, preserve prior.
  const { savePublishedSnapshotBundle } = await import(
    "../lib/ai-demand-positioning/published-snapshot.js"
  );
  const { loadPropertyProfile } = await import(
    "../lib/ai-demand-positioning/data-model.js"
  );
  const { getCensusRecordIdForAdpProperty } = await import(
    "../lib/ai-demand-positioning/census-link-registry.js"
  );
  const { ADP_PRODUCT_VERSION } = await import(
    "../lib/ai-demand-positioning/customer/owner-payload.js"
  );

  const profile = loadPropertyProfile(PROPERTY_ID);
  const hpc = getCensusRecordIdForAdpProperty(PROPERTY_ID);
  const periodId =
    p.period?.periodId ||
    man?.periodId ||
    priorPublishedSnapshot.latestPeriodId;
  const publishedAt = new Date().toISOString();
  const payload = {
    ...p,
    ok: true,
    propertyId: PROPERTY_ID,
    correctionLineage: {
      ...(p.correctionLineage || {}),
      publicationAuthorized: true,
      replacesPublishedForCustomerFacing: true,
      publishedAt,
      founderAuthorizationRef: AUTH_PATH,
      priorPublishedPeriodId: priorPublishedSnapshot.latestPeriodId,
      priorPublishedBackup: priorPublishedSnapshot.backupPath,
    },
  };
  const report = {
    ok: true,
    publishedAt,
    productVersion: ADP_PRODUCT_VERSION || "ai_demand_positioning_v1",
    propertyId: PROPERTY_ID,
    periodId,
    censusRecordId: hpc,
    measurementContractVersion:
      payload.measurementContractVersion || "ADP_MEASUREMENT_CONTRACT_V1",
    reportEdition: "WATERSTONE_GOVERNED_PUBLICATION_CUTOVER_V1",
    sourceStagedVersionId: EXPECTED_STAGED,
    methodologyId: METH,
    previousPublished: {
      periodId: priorPublishedSnapshot.latestPeriodId,
      publishedAt: priorPublishedSnapshot.latestPublishedAt,
      reportFile: priorPublishedSnapshot.reportFile,
      backupPath: priorPublishedSnapshot.backupPath,
    },
    payload,
  };
  const manifest = {
    propertyId: PROPERTY_ID,
    propertyName: profile?.name || "Waterstone Resort & Marina",
    city: profile?.city || payload.property?.city || "",
    state: profile?.state || payload.property?.state || "",
    market: profile?.market || null,
    latestPeriodId: periodId,
    latestPublishedAt: publishedAt,
    productVersion: ADP_PRODUCT_VERSION || "ai_demand_positioning_v1",
    censusRecordId: hpc,
    publishStatus: "Live",
    reportFile: `report-${periodId}.json`,
    evidenceFile: `evidence-${periodId}.json`,
    demandCaptureRate: payload.demandCapture?.overallRate ?? null,
    providerCount: payload.period?.providerCount ?? null,
    measurementContractVersion:
      payload.measurementContractVersion || "ADP_MEASUREMENT_CONTRACT_V1",
    reportEdition: "WATERSTONE_GOVERNED_PUBLICATION_CUTOVER_V1",
    sourceStagedVersionId: EXPECTED_STAGED,
    methodologyId: METH,
    // Keep hold until post-publication external cert clears it
    externalDistributionHold: {
      status: "HOLD",
      reason: "AWAITING_POST_PUBLICATION_EXTERNAL_CERT",
      holdUntil: "CUSTOMER_READY_AFTER_EXTERNAL_VERIFICATION",
      shareId: SHARE_ID,
      setAt: publishedAt,
    },
    previousPublishedPeriodId: priorPublishedSnapshot.latestPeriodId,
    previousPublishedAt: priorPublishedSnapshot.latestPublishedAt,
    previousPublishedBackup: priorPublishedSnapshot.backupPath,
  };
  const evidenceIndex = {
    ok: true,
    propertyId: PROPERTY_ID,
    periodId,
    publishedAt,
    sourceStagedVersionId: EXPECTED_STAGED,
    items: payload.evidence?.items || payload.evidenceItems || [],
  };

  const saved = savePublishedSnapshotBundle(
    {
      ok: true,
      manifest,
      report,
      evidenceIndex,
      summary: {
        propertyId: PROPERTY_ID,
        periodId,
        demandCapture: payload.demandCapture?.display || null,
      },
      certificationRecord: {
        certificationStatus: "CERTIFIED_WITH_DISCLOSURES",
        status: "CERTIFIED_WITH_DISCLOSURES",
        assuranceVersion: "waterstone_governed_publication_cutover_v1",
        certificationTimestamp: publishedAt,
        methodologyId: METH,
        sourceStagedVersionId: EXPECTED_STAGED,
        founderAuthorization: {
          type: "FOUNDER_APPROVED_FOR_PUBLICATION",
          stagedVersionId: EXPECTED_STAGED,
          founderSignature: authRecord.founderSignature,
          founderSignedAt: authRecord.founderSignedAt,
          explicitPhrase: authRecord.explicitPhrase,
        },
        disclosures: [
          "Independent-unprompted appearance methodology (64 scenarios)",
          "Reality Gap RG_WEIGHTED_PARTIAL_V2 (29 tested-eligible attributes)",
          "Prior Live 78-grain publication preserved in cutover backup",
          "External distribution hold until post-publication certification",
        ],
      },
    },
    {
      seed: false,
      officialCustomerPublish: true,
      allowUncertifiedPublish: false,
    }
  );

  result.published = saved?.ok === true || Boolean(saved?.reportFile);
  result.newPublishedVersionId = EXPECTED_STAGED;
  result.newPublishedPeriodId = periodId;
  result.publishResult = {
    ok: result.published,
    saved,
    shareRotated: false,
    shareId: SHARE_ID,
    externalDistributionHold: manifest.externalDistributionHold,
  };
  result.cutoverStatus = result.published
    ? "PUBLISHED_AWAITING_EXTERNAL_CERT"
    : "PUBLISH_FAILED";
  result.distributionHold = "HOLD";
}

const existingUrl =
  "https://my-operators-backend-production.up.railway.app/owner-ai-demand-share.html?share=adpshare.v1.eyJ2IjoxLCJ0aWQiOiJzaHRfZTBiOTYwOTBiMDI0NjE1MWVjMmRiNDRhIiwicHJvcGVydHlJZCI6ImFkcF93YXRlcnN0b25lX2JvY2FfcmF0b24iLCJzdXJmYWNlcyI6WyJyZXBvcnQiLCJldmlkZW5jZSIsInByb3BlcnRpZXMiLCJwdWJsaWNhdGlvbl9tZXRhIl0sInJlcG9ydFNjb3BlIjoiY3VycmVudF9wdWJsaXNoZWQiLCJpYXQiOjE3ODg0MTAwNDMsImV4cCI6bnVsbH0.rJHRikqKyBqflArhTVyuLjViBzQHd8KN6tkJJrcbTqY";
result.externalUrl = existingUrl;
result.safeToShare = false;
result.customerReady = false;

write("CUTOVER_STATUS.json", result);

const md = `# Waterstone Governed Publication Cutover V1

## Cutover status

**${result.cutoverStatus}**

${result.stopReason ? `Stop reason: ${result.stopReason}` : ""}

## 1. Publication authorization

| Field | Value |
|-------|-------|
| Type | \`FOUNDER_APPROVED_FOR_PUBLICATION\` |
| Active | **${authorizationActive}** |
| Staged version | \`${EXPECTED_STAGED}\` |
| Record | \`${result.authorizationPath}\` |

${
  authorizationActive
    ? "Authorization is ACTIVE."
    : "Authorization is **NOT** active. This preparation request does not authorize publication."
}

### Exact phrase required to activate

\`\`\`
FOUNDER_APPROVED_FOR_PUBLICATION staged_mw14o9lq_bb8bd9570c
\`\`\`

Set in \`${result.authorizationPath}\`:
- \`status\`: \`ACTIVE\`
- \`active\`: \`true\`
- \`founderSignature\`: your name
- \`founderSignedAt\`: ISO timestamp
- \`explicitPhrase\`: exactly the phrase above
- \`stagedFileSha256\` must still match \`${stagedSha}\`

Then run:

\`\`\`bash
node scripts/dealality-adp-waterstone-governed-publication-cutover-v1.mjs --authorize-publish
\`\`\`

## 2. Pre-publication checks

**Pass: ${preChecks.pass}**

- Staged identity: \`${staged?.versionId}\` ${preChecks.stagedVersionMatch ? "OK" : "FAIL"}
- File SHA-256: \`${stagedSha}\`
- Methodology: \`${preChecks.methodologyId}\` ${preChecks.methodologyMatch ? "OK" : "FAIL"}
- KPIs: Consideration 53.5 / Presence 76.6 / Coverage 24.1 / Gap 69 ${preChecks.consideration535 && preChecks.presence766 && preChecks.coverage241 && preChecks.gap69 ? "OK" : "FAIL"}
- Providers all /64: ${preChecks.providersAll64 ? "OK" : "FAIL"}
- Competitive rank #2: ${preChecks.subjectRank2 ? "OK" : "FAIL"}
- PDF present: ${preChecks.pdfExists ? "OK" : "FAIL"}
- Browser HTML/PDF parity: ${preChecks.browserParityPass ? "OK" : "FAIL"}
- Prior published preserved: ${preChecks.priorPublishedPreserved ? "OK" : "FAIL"}

## 3. Version IDs

| Role | ID |
|------|----|
| Prior published period | \`${priorPublishedSnapshot.latestPeriodId}\` |
| Prior published at | \`${priorPublishedSnapshot.latestPublishedAt}\` |
| New published | ${result.newPublishedVersionId || "_(not published)_"} |
| Approved staged candidate | \`${EXPECTED_STAGED}\` |

## 4–7. Production / external

| Item | Status |
|------|--------|
| Production readback | ${result.productionReadback || "N/A — not published"} |
| External URL | preserved existing share (no rotation) |
| External-access verification | ${result.externalAccessVerification || "N/A — not published"} |
| Safe to share corrected methodology | **${result.safeToShare ? "YES" : "NO"}** |
| CUSTOMER_READY | **${result.customerReady ? "YES" : "NO"}** |
| Distribution hold | ${result.distributionHold} |

## 8. Rollback readiness

**${result.rollbackReadiness.rollbackReady ? "READY" : "NOT READY"}** — prior Live backup at \`${result.rollbackReadiness.priorBackupPath || "n/a"}\`

No paid monitoring. No unrelated hotel changes. No publish executed unless authorization was active and \`--authorize-publish\` was passed.
`;

write("CUTOVER_REPORT.md", md);
console.log(JSON.stringify(result, null, 2));
