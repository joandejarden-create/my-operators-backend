import Airtable from "airtable";
import { fetchDealWithMergedLinkedRecords } from "./my-deals.js";
import {
  DEALS_TABLE,
  COMMERCIAL_READINESS_STATUS_AIRTABLE_FIELD,
  COMMERCIAL_READINESS_INPUTS_JSON_AIRTABLE_FIELD,
  COMMERCIAL_READINESS_SNAPSHOT_JSON_AIRTABLE_FIELD,
  COMMERCIAL_READINESS_NARRATIVE_AIRTABLE_FIELD,
  COMMERCIAL_READINESS_LEVEL_AIRTABLE_FIELD,
  COMMERCIAL_READINESS_EVIDENCE_CONFIDENCE_AIRTABLE_FIELD,
  COMMERCIAL_READINESS_OTA_RISK_AIRTABLE_FIELD,
  COMMERCIAL_READINESS_DIRECT_CAPABILITY_AIRTABLE_FIELD,
  COMMERCIAL_READINESS_BRAND_NEED_AIRTABLE_FIELD,
  COMMERCIAL_READINESS_OPERATOR_NEED_AIRTABLE_FIELD,
  COMMERCIAL_READINESS_LAST_GENERATED_AT_AIRTABLE_FIELD,
} from "./schemas/deal-setup-fields.js";
import { buildCommercialReadinessSnapshot } from "../lib/commercial-readiness-snapshot-build.js";
import { enrichCommercialReadinessSnapshot } from "../lib/commercial-readiness-snapshot-enrich.js";
import { extractCommercialReadinessUrlEvidence } from "../lib/commercial-readiness-url-evidence.js";

const REQUIRED_FIELDS = [
  "hotelWebsiteUrl",
  "currentBrandStatus",
  "currentOperatorStatus",
  "estimatedOtaShare",
  "estimatedDirectBookingShare",
  "crmGuestEmailCapture",
  "mainCommercialConcern",
  "ownerGoal",
];

function baseClient() {
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

function asText(v) {
  return v == null ? "" : String(v).trim();
}

function sanitizeInputs(raw) {
  const payload = raw && typeof raw === "object" ? raw : {};
  return {
    hotelWebsiteUrl: asText(payload.hotelWebsiteUrl),
    bookingComUrl: asText(payload.bookingComUrl),
    expediaUrl: asText(payload.expediaUrl),
    googleBusinessProfileUrl: asText(payload.googleBusinessProfileUrl),
    currentBrandStatus: asText(payload.currentBrandStatus),
    currentOperatorStatus: asText(payload.currentOperatorStatus),
    estimatedOtaShare: asText(payload.estimatedOtaShare),
    estimatedDirectBookingShare: asText(payload.estimatedDirectBookingShare),
    bookingEngineProvider: asText(payload.bookingEngineProvider),
    crmGuestEmailCapture: asText(payload.crmGuestEmailCapture),
    mainCommercialConcern: asText(payload.mainCommercialConcern),
    ownerGoal: asText(payload.ownerGoal),
    actualOtaBookingShare: asText(payload.actualOtaBookingShare),
    actualDirectBookingShare: asText(payload.actualDirectBookingShare),
    estimatedOtaCommission: asText(payload.estimatedOtaCommission),
    websiteConversionRate: asText(payload.websiteConversionRate),
    repeatGuestPercentage: asText(payload.repeatGuestPercentage),
    topSourceMarkets: asText(payload.topSourceMarkets),
    primaryGuestSegments: Array.isArray(payload.primaryGuestSegments)
      ? payload.primaryGuestSegments.map((x) => asText(x)).filter(Boolean)
      : asText(payload.primaryGuestSegments),
    additionalOwnerNotes: asText(payload.additionalOwnerNotes),
    bookingCtaVisibility: asText(payload.bookingCtaVisibility),
    mobileBookingClarity: asText(payload.mobileBookingClarity),
    directValueProposition: asText(payload.directValueProposition),
    roomDescriptionQuality: asText(payload.roomDescriptionQuality),
    faqAndReassurance: asText(payload.faqAndReassurance),
    socialProofIntegration: asText(payload.socialProofIntegration),
    offersAndPackages: asText(payload.offersAndPackages),
    guestSegmentClarity: asText(payload.guestSegmentClarity),
  };
}

function validateInputs(inputs) {
  const missing = REQUIRED_FIELDS.filter((k) => !asText(inputs[k]));
  return { ok: missing.length === 0, missing };
}

function parseJsonField(value) {
  if (!value) return null;
  if (typeof value === "object") return value;
  if (typeof value !== "string") return null;
  try {
    return JSON.parse(value);
  } catch (err) {
    return null;
  }
}

function buildWriteFieldsFromResult(inputs, result) {
  return {
    [COMMERCIAL_READINESS_STATUS_AIRTABLE_FIELD]: result.labels.status,
    [COMMERCIAL_READINESS_INPUTS_JSON_AIRTABLE_FIELD]: JSON.stringify(inputs),
    [COMMERCIAL_READINESS_SNAPSHOT_JSON_AIRTABLE_FIELD]: JSON.stringify(result.snapshot),
    [COMMERCIAL_READINESS_NARRATIVE_AIRTABLE_FIELD]: result.narrative,
    [COMMERCIAL_READINESS_LEVEL_AIRTABLE_FIELD]: result.labels.readinessLevel,
    [COMMERCIAL_READINESS_EVIDENCE_CONFIDENCE_AIRTABLE_FIELD]: result.labels.confidence,
    [COMMERCIAL_READINESS_OTA_RISK_AIRTABLE_FIELD]: result.labels.otaRisk,
    [COMMERCIAL_READINESS_DIRECT_CAPABILITY_AIRTABLE_FIELD]: result.labels.directCapability,
    [COMMERCIAL_READINESS_BRAND_NEED_AIRTABLE_FIELD]: result.labels.brandNeed,
    [COMMERCIAL_READINESS_OPERATOR_NEED_AIRTABLE_FIELD]: result.labels.operatorNeed,
    [COMMERCIAL_READINESS_LAST_GENERATED_AT_AIRTABLE_FIELD]: new Date().toISOString(),
  };
}

function mapDealFields(fields) {
  const inputs = parseJsonField(fields[COMMERCIAL_READINESS_INPUTS_JSON_AIRTABLE_FIELD]) || {};
  const snapshot = parseJsonField(fields[COMMERCIAL_READINESS_SNAPSHOT_JSON_AIRTABLE_FIELD]) || null;
  return {
    status: asText(fields[COMMERCIAL_READINESS_STATUS_AIRTABLE_FIELD]) || "Not started",
    inputs,
    snapshot,
    narrative: asText(fields[COMMERCIAL_READINESS_NARRATIVE_AIRTABLE_FIELD]),
    level: asText(fields[COMMERCIAL_READINESS_LEVEL_AIRTABLE_FIELD]),
    evidenceConfidence: asText(fields[COMMERCIAL_READINESS_EVIDENCE_CONFIDENCE_AIRTABLE_FIELD]),
    otaDependencyRisk: asText(fields[COMMERCIAL_READINESS_OTA_RISK_AIRTABLE_FIELD]),
    directBookingCapability: asText(fields[COMMERCIAL_READINESS_DIRECT_CAPABILITY_AIRTABLE_FIELD]),
    brandDistributionNeed: asText(fields[COMMERCIAL_READINESS_BRAND_NEED_AIRTABLE_FIELD]),
    operatorCommercialCapabilityNeed: asText(fields[COMMERCIAL_READINESS_OPERATOR_NEED_AIRTABLE_FIELD]),
    lastGeneratedAt: asText(fields[COMMERCIAL_READINESS_LAST_GENERATED_AT_AIRTABLE_FIELD]),
  };
}

export async function getCommercialReadinessSnapshot(req, res) {
  try {
    const dealId = req.params?.dealId || req.params?.recordId;
    if (!dealId) return res.status(400).json({ ok: false, error: "Missing dealId" });
    const record = await baseClient()(DEALS_TABLE).find(dealId);
    return res.json({ ok: true, commercialReadiness: mapDealFields(record.fields || {}) });
  } catch (error) {
    return res.status(500).json({ ok: false, error: "Failed to load Commercial Readiness Snapshot." });
  }
}

export async function postCommercialReadinessSaveInputs(req, res) {
  try {
    const dealId = req.body?.dealId || req.body?.recordId;
    if (!dealId) return res.status(400).json({ ok: false, error: "Missing dealId" });
    const inputs = sanitizeInputs(req.body?.inputs || {});
    await baseClient()(DEALS_TABLE).update(dealId, {
      [COMMERCIAL_READINESS_STATUS_AIRTABLE_FIELD]: "Draft",
      [COMMERCIAL_READINESS_INPUTS_JSON_AIRTABLE_FIELD]: JSON.stringify(inputs),
    });
    return res.json({ ok: true, status: "Draft", inputs });
  } catch (error) {
    return res.status(500).json({ ok: false, error: "Failed to save Commercial Readiness inputs." });
  }
}

function buildGenerateResponse(result, extra = {}) {
  return {
    ok: true,
    commercialReadiness: {
      status: result.labels.status,
      labels: result.labels,
      narrative: result.narrative,
      snapshot: result.snapshot,
      ...extra,
    },
  };
}

function shouldExtractUrlEvidence(body = {}) {
  if (body.extractUrlEvidence != null) {
    const v = String(body.extractUrlEvidence).trim().toLowerCase();
    return v === "1" || v === "true" || v === "yes";
  }
  // MVP default: attempt extraction unless explicitly disabled via env
  return process.env.COMMERCIAL_READINESS_URL_EXTRACTION_ENABLED !== "0";
}

/** Standalone generation — no dealId, no Airtable write */
export async function postCommercialReadinessGenerateStandalone(req, res) {
  try {
    const inputs = sanitizeInputs(req.body?.inputs || {});
    const validation = validateInputs(inputs);
    if (!validation.ok) {
      return res.status(400).json({ ok: false, error: "Missing required Commercial Readiness inputs.", missing: validation.missing });
    }
    let urlEvidence = null;
    if (shouldExtractUrlEvidence(req.body || {})) {
      try {
        urlEvidence = await extractCommercialReadinessUrlEvidence(inputs);
      } catch (_err) {
        urlEvidence = {
          enabled: true,
          sources: {
            hotelWebsite: { url: inputs.hotelWebsiteUrl || "", status: "failed", reason: "extraction_failed" },
            bookingCom: { url: inputs.bookingComUrl || "", status: "failed", reason: "extraction_failed" },
            expedia: { url: inputs.expediaUrl || "", status: "failed", reason: "extraction_failed" },
            googleBusinessProfile: {
              url: inputs.googleBusinessProfileUrl || "",
              status: "failed",
              notes: "Google Business Profile URL provided, but content extraction was not available in this MVP.",
            },
          },
          ownedVsOtaComparison: {
            assessment: "Insufficient extracted evidence",
            confidence: "Low",
            ownedChannelStrengths: [],
            otaStrengths: [],
            contentGaps: [],
            directBookingGaps: [],
            guestReassuranceGaps: [],
            dealImplication:
              "URL-level analysis was attempted, but content could not be extracted from provided sources. Manual comparison is recommended.",
          },
        };
      }
    }
    const result = buildCommercialReadinessSnapshot(inputs, { urlEvidence });
    const enriched = await enrichCommercialReadinessSnapshot({
      deterministicResult: result,
      inputs,
      mode: "standalone",
      enrichNarrative: req.body?.enrichNarrative,
    });
    return res.json(buildGenerateResponse(enriched, { standalone: true, generatedAt: new Date().toISOString() }));
  } catch (error) {
    console.error("[commercial-readiness-snapshot] standalone generate failed:", error?.message || error);
    return res.status(500).json({ ok: false, error: "Failed to generate Commercial Readiness Snapshot." });
  }
}

export async function postCommercialReadinessGenerate(req, res) {
  try {
    const dealId = req.body?.dealId || req.body?.recordId;
    const inputs = sanitizeInputs(req.body?.inputs || {});
    const validation = validateInputs(inputs);
    if (!validation.ok) {
      if (dealId) {
        try {
          await baseClient()(DEALS_TABLE).update(dealId, {
            [COMMERCIAL_READINESS_STATUS_AIRTABLE_FIELD]: "Needs inputs",
            [COMMERCIAL_READINESS_INPUTS_JSON_AIRTABLE_FIELD]: JSON.stringify(inputs),
          });
        } catch (_) {}
      }
      return res.status(400).json({ ok: false, error: "Missing required Commercial Readiness inputs.", missing: validation.missing });
    }

    let urlEvidence = null;
    if (shouldExtractUrlEvidence(req.body || {})) {
      try {
        urlEvidence = await extractCommercialReadinessUrlEvidence(inputs);
      } catch (_err) {
        urlEvidence = null;
      }
    }
    const result = buildCommercialReadinessSnapshot(inputs, { urlEvidence });
    const enriched = await enrichCommercialReadinessSnapshot({
      deterministicResult: result,
      inputs,
      mode: dealId ? "deal-linked" : "standalone",
      enrichNarrative: req.body?.enrichNarrative,
    });

    if (!dealId) {
      return res.json(buildGenerateResponse(enriched, { standalone: true, generatedAt: new Date().toISOString() }));
    }

    const deal = await fetchDealWithMergedLinkedRecords(process.env.AIRTABLE_BASE_ID, process.env.AIRTABLE_API_KEY, dealId);
    if (!deal || !deal.id) return res.status(404).json({ ok: false, error: "Deal not found." });

    const writeFields = buildWriteFieldsFromResult(inputs, enriched);
    await baseClient()(DEALS_TABLE).update(dealId, writeFields);

    return res.json(
      buildGenerateResponse(enriched, {
        dealId,
        generatedAt: writeFields[COMMERCIAL_READINESS_LAST_GENERATED_AT_AIRTABLE_FIELD],
      })
    );
  } catch (error) {
    console.error("[commercial-readiness-snapshot] generate failed:", error?.message || error);
    return res.status(500).json({ ok: false, error: "Failed to generate Commercial Readiness Snapshot." });
  }
}
