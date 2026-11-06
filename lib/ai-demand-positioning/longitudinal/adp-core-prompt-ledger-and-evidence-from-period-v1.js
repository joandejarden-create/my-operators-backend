/**
 * ADP_CORE_PROMPT_LEDGER_AND_EVIDENCE_FROM_PERIOD_V1
 *
 * Build Core Measurement-History Prompt Ledger (+ optional evidence index expansion)
 * from authoritative period / archive observations — not from UI evidence-card caps alone.
 *
 * Assisted / subject-prompted observations are retained for historical audit with
 * Measurement Eligibility = false so they cannot contaminate independent-discovery KPIs.
 */

import path from "path";
import { createHash } from "crypto";
import { HISTORY_TABLES } from "./airtable-history-schema-final-v1.js";
import {
  buildPromptProvenanceFields,
  hashPrompt,
  hashResponse,
} from "../measurement-assurance/prompt-persistence-v1.js";
import { CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID } from "../certification/adp-unprompted-independent-discovery-standard-v1.js";
import { isV11CoreComparableObservation } from "../measurement-assurance/v1-1-core-eligibility.js";
import { isComparableObservation } from "../metrics/grain-governance.js";
import { ARCHIVE_ROOT } from "../execution/durable-observation-archive-v1.js";

export const ADP_CORE_PROMPT_LEDGER_AND_EVIDENCE_FROM_PERIOD_V1 =
  "ADP_CORE_PROMPT_LEDGER_AND_EVIDENCE_FROM_PERIOD_V1";

function periodKey(propertyId, periodId) {
  return `${propertyId}|${periodId}`;
}

function shortHash(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 16);
}

function previewPrompt(exact, max = 500) {
  const t = String(exact || "");
  return t.length <= max ? t : t.slice(0, max);
}

/**
 * Resolve observation list from a loaded Core period object.
 */
export function extractPeriodObservations(period) {
  if (!period || typeof period !== "object") return [];
  if (Array.isArray(period.observations)) return period.observations;
  if (Array.isArray(period.observationRecords)) return period.observationRecords;
  return [];
}

/**
 * Determine independent-discovery measurement eligibility for one observation.
 * Preserves assisted observations with eligibility=false (historical audit).
 */
export function resolveObservationMeasurementEligibility(obs, _period = null) {
  if (obs?.measurementEligibility === false || obs?.measurementEligible === false) {
    return false;
  }
  if (obs?.metricInclusion === false || obs?.coreMeasurementStatus === "EXCLUDED_FROM_CORE") {
    return false;
  }
  try {
    if (isV11CoreComparableObservation(obs)) return true;
  } catch {
    /* fall through to legacy comparable */
  }
  const cls = String(obs?.scenarioClass || obs?.eligibilityClass || "").toUpperCase();
  if (
    cls.includes("SUBJECT_PROMPTED") ||
    cls.includes("ASSISTED") ||
    cls.includes("ATTRIBUTE_VERIFICATION") ||
    cls.includes("COMPETITIVE_COMPARISON")
  ) {
    return false;
  }
  // Legacy Core periods: unmarked comparable observations remain eligible for ledger KPI flag
  try {
    return Boolean(isComparableObservation(obs));
  } catch {
    return true;
  }
}

/**
 * Build Prompt Ledger Airtable records for Core from period observations.
 * Does not invent provider response IDs when absent.
 */
export function buildCorePromptLedgerRecordsFromPeriod({
  propertyId,
  periodId,
  period,
  historicalVersion = 1,
} = {}) {
  const observations = extractPeriodObservations(period);
  const fk = periodKey(propertyId, periodId);
  const records = [];
  const defects = [];

  for (const obs of observations) {
    const observationId =
      obs.observationId ||
      obs.id ||
      shortHash(
        `${propertyId}|${periodId}|${obs.scenarioId}|${obs.provider}|${obs.timestamp || ""}`
      );
    const exact =
      obs.exactRenderedPrompt ||
      obs.prompt ||
      obs.query ||
      obs.renderedPrompt ||
      "";
    if (!exact && !obs.rawResponse && !obs.response) {
      defects.push({
        code: "OBSERVATION_MISSING_PROMPT_AND_RESPONSE",
        observationId,
        scenarioId: obs.scenarioId || null,
      });
    }

    const measurementEligibility = resolveObservationMeasurementEligibility(obs, period);

    const provenance = buildPromptProvenanceFields({
      observationId,
      propertyId,
      periodId,
      provider: obs.provider || null,
      scenarioId: obs.scenarioId || null,
      scenarioClass: obs.scenarioClass || "NEUTRAL_DEMAND",
      territoryId: obs.territoryId || obs.intent || obs.ownerIntent || null,
      ownerIntent: obs.ownerIntent || obs.intent || null,
      exactRenderedPrompt: exact,
      promptTemplateId: obs.promptTemplateId || obs.templateId || null,
      promptTemplateVersion: obs.promptTemplateVersion || obs.templateVersion || null,
      promptGeneratorVersion: obs.promptGeneratorVersion || null,
      requestTimestamp: obs.requestTimestamp || obs.timestamp || obs.executedAt || null,
      measurementEligibility,
    });

    const immutablePromptPath = exact
      ? path
          .join(
            "observation-archive",
            propertyId,
            periodId,
            "prompts",
            `${observationId}.txt`
          )
          .replace(/\\/g, "/")
      : null;
    const immutableObsRef = path
      .join("observation-archive", propertyId, periodId, "observations", `${observationId}.json`)
      .replace(/\\/g, "/");

    // Never invent response IDs
    const responseId = obs.responseId || obs.providerResponseId || null;

    records.push({
      table: HISTORY_TABLES.PROMPT_LEDGER,
      idempotencyKey: `${fk}|${observationId}`,
      fields: {
        "Prompt Ledger Key": `${fk}|${observationId}`,
        "Observation ID": observationId,
        "Property ID": propertyId,
        "Period ID": periodId,
        "Scenario ID": provenance.scenarioId,
        Provider: provenance.provider,
        Territory: provenance.territoryId,
        "Owner Intent": provenance.ownerIntent,
        "Scenario Class": provenance.scenarioClass,
        "Prompt Hash": provenance.promptHash || (exact ? hashPrompt(exact) : null),
        "Exact Prompt Preview": previewPrompt(exact),
        "Template ID": provenance.promptTemplateId,
        "Template Version": provenance.promptTemplateVersion,
        "Generator Version": provenance.promptGeneratorVersion,
        "Measurement Eligibility": Boolean(measurementEligibility),
        "Integrity Classification":
          obs.integrityClassification ||
          (measurementEligibility
            ? "INDEPENDENT_UNPROMPTED_ELIGIBLE"
            : "ASSISTED_OR_INELIGIBLE_HISTORICAL"),
        "Request Timestamp": provenance.requestTimestamp,
        "Response ID": responseId,
        "Immutable Prompt Path": immutablePromptPath,
        "Immutable Observation Ref": immutableObsRef,
      },
      _meta: {
        hasRawResponse: Boolean(obs.rawResponse || obs.response || obs.originalResponse),
        model: obs.model || null,
        subjectMentioned: obs.subjectMentioned ?? obs.subjectPresent ?? null,
        methodologyId: CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
        archiveRoot: ARCHIVE_ROOT,
        contract: ADP_CORE_PROMPT_LEDGER_AND_EVIDENCE_FROM_PERIOD_V1,
      },
    });
  }

  return {
    ok: defects.length === 0,
    defects,
    records,
    observationCount: observations.length,
    eligibleCount: records.filter((r) => r.fields["Measurement Eligibility"] === true).length,
    assistedOrIneligibleCount: records.filter(
      (r) => r.fields["Measurement Eligibility"] !== true
    ).length,
    contract: ADP_CORE_PROMPT_LEDGER_AND_EVIDENCE_FROM_PERIOD_V1,
  };
}

/**
 * Map customer-visible evidence cards to Evidence Index rows with traceability
 * to observation IDs when present. Does not fabricate missing observation links.
 */
export function buildEvidenceIndexRecordsFromCustomerVisible({
  propertyId,
  periodId,
  evidencePack = {},
  historicalVersion = 1,
  observationIdSet = null,
} = {}) {
  const version = historicalVersion;
  const fk = periodKey(propertyId, periodId);
  const records = [];
  const untraceable = [];
  const lanes = [
    ["positive", evidencePack.positive || evidencePack.positiveExamples || []],
    ["missing", evidencePack.missing || evidencePack.missingExamples || []],
    ["displacement", evidencePack.displacement || evidencePack.displacementExamples || []],
  ];

  for (const [lane, items] of lanes) {
    for (const ev of items) {
      const observationId =
        ev.observationId ||
        ev.id ||
        shortHash(`${lane}|${ev.provider}|${periodId}|${ev.scenarioId || ""}`);
      const response = ev.aiResponse || ev.responseExcerpt || ev.response || "";
      const responseHash = response ? hashResponse(response).slice(0, 16) : shortHash(observationId);
      const storeRef = `evidence-blobs/${propertyId}/${periodId}/${observationId}.txt`;
      const traced =
        observationIdSet == null
          ? Boolean(ev.observationId)
          : observationIdSet.has(String(ev.observationId || ""));

      if (!traced && !ev.observationId) {
        untraceable.push({
          lane,
          observationId,
          provider: ev.provider || null,
          reason: "NO_OBSERVATION_ID_ON_EVIDENCE_CARD",
        });
      } else if (observationIdSet && ev.observationId && !observationIdSet.has(String(ev.observationId))) {
        untraceable.push({
          lane,
          observationId: ev.observationId,
          provider: ev.provider || null,
          reason: "OBSERVATION_ID_NOT_IN_PERIOD_CORPUS",
        });
      }

      records.push({
        table: HISTORY_TABLES.EVIDENCE_INDEX,
        idempotencyKey: `${fk}|${observationId}`,
        fields: {
          "Evidence Key": `${fk}|${observationId}`,
          "Property ID": propertyId,
          "Period ID": periodId,
          "Observation ID": observationId,
          "Evidence Lane": lane,
          Provider: ev.provider || null,
          Territory: ev.territory || ev.territoryId || null,
          "Subject Present": Boolean(
            ev.subjectStatus === "Appeared" ||
              ev.subjectPresent === true ||
              lane === "positive"
          ),
          "Subject Rank": ev.rank ?? ev.position ?? null,
          "Response Store Ref": storeRef,
          "Response Length": response.length,
          "Response Content Hash": responseHash,
          "Mention Spans JSON": JSON.stringify(ev.subjectMentionSpans || []),
          "Competitors JSON": JSON.stringify(
            ev.competitors || ev.competitorsAlongside || []
          ),
          "Citations JSON": JSON.stringify(ev.citations || []),
        },
        _meta: {
          tracedToObservation: traced,
          historicalVersion: version,
          contract: ADP_CORE_PROMPT_LEDGER_AND_EVIDENCE_FROM_PERIOD_V1,
        },
      });
    }
  }

  return {
    records,
    untraceable,
    cardCount: records.length,
    contract: ADP_CORE_PROMPT_LEDGER_AND_EVIDENCE_FROM_PERIOD_V1,
  };
}

/**
 * When staged UI evidence cards are absent, propose Evidence Index rows from the
 * observation corpus for subject-present (and optional missing) lanes.
 * These are CORPUS-derived index candidates — still require MEASUREMENT_HISTORY auth.
 */
export function buildEvidenceIndexRecordsFromObservationCorpus({
  propertyId,
  periodId,
  period,
  historicalVersion = 1,
  includeMissingLane = false,
  maxMissing = 40,
} = {}) {
  const observations = extractPeriodObservations(period);
  const fk = periodKey(propertyId, periodId);
  const records = [];
  let missingAdded = 0;

  for (const obs of observations) {
    const observationId = obs.observationId || obs.id;
    if (!observationId) continue;
    const mentioned = obs.mentioned === true || obs.subjectPresent === true;
    const response = obs.rawResponse || obs.response || "";
    const responseHash = response
      ? hashResponse(response).slice(0, 16)
      : shortHash(observationId);
    const storeRef = `evidence-blobs/${propertyId}/${periodId}/${observationId}.txt`;

    if (mentioned) {
      records.push({
        table: HISTORY_TABLES.EVIDENCE_INDEX,
        idempotencyKey: `${fk}|${observationId}`,
        fields: {
          "Evidence Key": `${fk}|${observationId}`,
          "Property ID": propertyId,
          "Period ID": periodId,
          "Observation ID": observationId,
          "Evidence Lane": "positive",
          Provider: obs.provider || null,
          Territory: obs.territoryId || obs.intent || null,
          "Subject Present": true,
          "Subject Rank": obs.position ?? obs.rank ?? null,
          "Response Store Ref": storeRef,
          "Response Length": String(response).length,
          "Response Content Hash": responseHash,
          "Mention Spans JSON": "[]",
          "Competitors JSON": JSON.stringify(obs.competitorsMentioned || []),
          "Citations JSON": JSON.stringify(obs.sourcesCited || []),
        },
        _meta: {
          tracedToObservation: true,
          source: "OBSERVATION_CORPUS_SUBJECT_PRESENT",
          historicalVersion,
          contract: ADP_CORE_PROMPT_LEDGER_AND_EVIDENCE_FROM_PERIOD_V1,
        },
      });
      continue;
    }

    if (includeMissingLane && missingAdded < maxMissing && response) {
      missingAdded += 1;
      records.push({
        table: HISTORY_TABLES.EVIDENCE_INDEX,
        idempotencyKey: `${fk}|${observationId}`,
        fields: {
          "Evidence Key": `${fk}|${observationId}`,
          "Property ID": propertyId,
          "Period ID": periodId,
          "Observation ID": observationId,
          "Evidence Lane": "missing",
          Provider: obs.provider || null,
          Territory: obs.territoryId || obs.intent || null,
          "Subject Present": false,
          "Subject Rank": null,
          "Response Store Ref": storeRef,
          "Response Length": String(response).length,
          "Response Content Hash": responseHash,
          "Mention Spans JSON": "[]",
          "Competitors JSON": JSON.stringify(obs.competitorsMentioned || []),
          "Citations JSON": JSON.stringify(obs.sourcesCited || []),
        },
        _meta: {
          tracedToObservation: true,
          source: "OBSERVATION_CORPUS_MISSING_SAMPLE",
          historicalVersion,
          contract: ADP_CORE_PROMPT_LEDGER_AND_EVIDENCE_FROM_PERIOD_V1,
        },
      });
    }
  }

  return {
    records,
    untraceable: [],
    cardCount: records.length,
    subjectPresentCount: records.filter((r) => r.fields["Evidence Lane"] === "positive").length,
    contract: ADP_CORE_PROMPT_LEDGER_AND_EVIDENCE_FROM_PERIOD_V1,
  };
}
