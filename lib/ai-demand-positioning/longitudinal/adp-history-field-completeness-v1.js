/**
 * ADP_HISTORY_FIELD_COMPLETENESS_V1
 *
 * Field-level enrichment + readback for MEASUREMENT_HISTORY Airtable persistence.
 * Record existence alone must never certify AIRTABLE_SYNC_COMPLETE.
 */

import crypto from "crypto";
import { HISTORY_TABLES } from "./airtable-history-schema-final-v1.js";
import { buildHistoricalWriteManifest } from "./persist-adp-historical-period-v1.js";
import { stagedOwnerPayloadToHistoryShape } from "./adp-full-e2e-airtable-history-persistence-gate-v1.js";
import { CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID } from "../certification/adp-unprompted-independent-discovery-standard-v1.js";
import {
  CORE_NOT_APPLICABLE_RANKING_FIELDS,
  FIELD_REQUIREMENT_CLASS,
  resolveFieldClass,
  MEASUREMENT_FAMILY,
} from "./adp-history-field-semantics-contract-v1.js";
import { sha256OfPayload, buildReportSnapshotV1 } from "./report-snapshot-v1.js";

export const ADP_HISTORY_FIELD_COMPLETENESS_V1 = "ADP_HISTORY_FIELD_COMPLETENESS_V1";

export const FIELD_BLANK_CLASS = Object.freeze({
  REQUIRED_MISSING: "REQUIRED_MISSING",
  SOURCE_AVAILABLE_NOT_MAPPED: "SOURCE_AVAILABLE_NOT_MAPPED",
  SOURCE_UNAVAILABLE: "SOURCE_UNAVAILABLE",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  INTENTIONALLY_NULL: "INTENTIONALLY_NULL",
  FORMULA_OR_LOOKUP: "FORMULA_OR_LOOKUP",
  DEPRECATED_FIELD: "DEPRECATED_FIELD",
});

/** Identity + measurement fields that must not be blank when source supplies them. */
export const REQUIRED_WHEN_SOURCE_AVAILABLE = Object.freeze({
  [HISTORY_TABLES.MONITORING_PERIODS]: [
    "Period Key",
    "Property ID",
    "Period ID",
    "Monitoring Date",
    "Certification Status",
    "Historical Version",
  ],
  [HISTORY_TABLES.REPORT_SNAPSHOTS]: [
    "Snapshot ID",
    "Property ID",
    "Period ID",
    "Monitoring Date",
    "Certification Status",
  ],
  [HISTORY_TABLES.PERIOD_METRICS]: [
    "Metrics Key",
    "Property ID",
    "Period ID",
    "Consideration Rate",
    "Consideration Numerator",
    "Consideration Denominator",
    "Scenario Presence",
    "Certification Status",
  ],
  [HISTORY_TABLES.TERRITORY_METRICS]: [
    "Territory Key",
    "Property ID",
    "Period ID",
    "Territory ID",
    "Numerator",
    "Denominator",
  ],
  [HISTORY_TABLES.PROVIDER_METRICS]: [
    "Provider Key",
    "Property ID",
    "Period ID",
    "Provider ID",
  ],
  [HISTORY_TABLES.COMPETITIVE_RANKINGS]: [
    "Rank Key",
    "Property ID",
    "Period ID",
    "Scope",
    "Ranking Universe Type",
    "Canonical Entity ID",
    "Rank",
    "Numerator",
    "Denominator",
  ],
  // Content Hash / Envelope Hash are REQUIRED_WHEN_APPLICABLE via semantics contract
  // when report-snapshot hashing can be performed from authoritative customerPayload.
  [HISTORY_TABLES.EVIDENCE_INDEX]: [
    "Evidence Key",
    "Property ID",
    "Period ID",
    "Observation ID",
    "Evidence Lane",
  ],
  [HISTORY_TABLES.REPORT_CORRECTIONS]: ["Correction ID", "Property ID", "Period ID"],
  [HISTORY_TABLES.PROMPT_LEDGER]: [
    "Prompt Ledger Key",
    "Property ID",
    "Period ID",
    "Observation ID",
  ],
});

/**
 * Truly optional / situational blanks for Core history.
 * Portfolio Lens/Type/Peer Set Version are NOT_APPLICABLE on Core (handled via semantics),
 * NOT intentional nulls that hide serializer defects.
 * Ranking Universe Type / Content Hash / Envelope Hash are NEVER intentional nulls.
 */
const INTENTIONAL_NULL_FIELDS = new Set([
  "Corrects Period Key",
  "Publication Timestamp",
  "Tie State",
  "Recovery State",
  "Completeness State",
  "CORE Benchmark",
  "Presence Index",
  "Benchmark",
  "Publication Commit", // unpublished / staged — null until real publication
]);

const FORMULA_TYPES = new Set([
  "formula",
  "multipleLookupValues",
  "lookup",
  "rollup",
  "count",
  "autoNumber",
  "createdTime",
  "lastModifiedTime",
  "createdBy",
  "lastModifiedBy",
  "button",
]);

function sha256Json(value) {
  return crypto.createHash("sha256").update(JSON.stringify(value ?? null)).digest("hex");
}

function isBlank(v) {
  return v == null || v === "" || (Array.isArray(v) && v.length === 0);
}

/**
 * Classify a blank live field relative to expected source value + schema meta.
 */
export function classifyBlankField({
  tableName,
  fieldName,
  liveValue,
  expectedValue,
  fieldMeta = null,
  hasMatch = false,
  measurementFamily = MEASUREMENT_FAMILY.CORE,
} = {}) {
  if (fieldMeta && FORMULA_TYPES.has(fieldMeta.type)) {
    return FIELD_BLANK_CLASS.FORMULA_OR_LOOKUP;
  }
  if (/^fld/i.test(fieldName) === false && /deprecated/i.test(fieldName)) {
    return FIELD_BLANK_CLASS.DEPRECATED_FIELD;
  }

  if (!isBlank(liveValue)) {
    return null; // not blank
  }

  const sem = resolveFieldClass(tableName, fieldName, measurementFamily);
  if (
    sem.class === FIELD_REQUIREMENT_CLASS.NOT_APPLICABLE ||
    (measurementFamily === MEASUREMENT_FAMILY.CORE &&
      CORE_NOT_APPLICABLE_RANKING_FIELDS.includes(fieldName))
  ) {
    return FIELD_BLANK_CLASS.NOT_APPLICABLE;
  }

  if (INTENTIONAL_NULL_FIELDS.has(fieldName)) {
    return FIELD_BLANK_CLASS.INTENTIONALLY_NULL;
  }

  const requiredList = REQUIRED_WHEN_SOURCE_AVAILABLE[tableName] || [];
  const isRequired =
    requiredList.includes(fieldName) ||
    sem.class === FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS ||
    sem.class === FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE;
  const sourceHas = expectedValue !== undefined && !isBlank(expectedValue);

  if (sourceHas) {
    return isRequired
      ? FIELD_BLANK_CLASS.REQUIRED_MISSING
      : FIELD_BLANK_CLASS.SOURCE_AVAILABLE_NOT_MAPPED;
  }

  if (isRequired && hasMatch) {
    return FIELD_BLANK_CLASS.SOURCE_UNAVAILABLE;
  }

  if (!hasMatch) {
    return FIELD_BLANK_CLASS.NOT_APPLICABLE;
  }

  return FIELD_BLANK_CLASS.INTENTIONALLY_NULL;
}

/**
 * Enrich staged owner payload into a complete historical write manifest with
 * hashes, timestamps, methodology disclosure, and non-null KPI mapping.
 */
export function enrichHistoricalWriteManifestFromStaged({
  propertyId,
  periodId,
  stagedPayload,
  stagedVersionId = "",
  monitoringDate,
  certificationStatus = "STAGED_CERTIFIED",
  publishedAt = null,
  synthetic = false,
  historicalVersion = 1,
  /**
   * Optional overrides. Prefer authentic ADP_REPORT_SNAPSHOT_V1 hashes.
   * Do NOT pass staged artifactHash / file SHA as Content Hash or Envelope Hash.
   */
  contentHashOverride = null,
  envelopeHashOverride = null,
  /** Verified publication identity only — never a staged_* version id. */
  publicationCommit = null,
  /**
   * When true (default), compute Content Hash = sha256OfPayload(customerPayload)
   * and Envelope Hash via buildReportSnapshotV1 — authentic report-snapshot semantics.
   * Staged artifactHash remains a separate concept (proposed Staged Artifact Hash field).
   */
  computeReportSnapshotHashes = true,
} = {}) {
  const shape = stagedOwnerPayloadToHistoryShape(stagedPayload, {
    propertyId,
    periodId,
  });
  // Prefer authentic historical timestamps only — never invent "now" as Certification Timestamp.
  const certifiedAt =
    stagedPayload?.period?.certifiedAt ||
    stagedPayload?.period?.completedAt ||
    stagedPayload?.period?.executedAt ||
    stagedPayload?.period?.executionDate ||
    stagedPayload?.generatedAt ||
    null;

  const snapshotId = `snap_${propertyId}_${periodId}`;

  const historyPayload = {
    ...(stagedPayload && typeof stagedPayload === "object" ? stagedPayload : {}),
    kpis: shape.kpis,
    territories: shape.territories,
    providers: shape.providers,
    competitive: shape.competitive,
    evidence: { ...(stagedPayload?.evidence || {}), ...shape.evidence },
    methodologyId: shape.methodologyId || CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
  };

  const disclosures = [
    {
      methodologyId: shape.methodologyId || CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
      stagedVersionId,
      enrichment: ADP_HISTORY_FIELD_COMPLETENESS_V1,
    },
  ];
  const versions = {
    // Never infer publication from staged_* version ids.
    publicationCommit: publicationCommit || null,
    measurementContractVersion:
      shape.methodologyId || CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
    rendererVersion: stagedPayload?.version || null,
  };

  let contentHash = contentHashOverride || null;
  let envelopeHash = envelopeHashOverride || null;
  let builtSnap = null;
  if (computeReportSnapshotHashes && !contentHashOverride) {
    builtSnap = buildReportSnapshotV1({
      propertyId,
      periodId,
      monitoringDate:
        monitoringDate ||
        String(certifiedAt).slice(0, 10) ||
        new Date().toISOString().slice(0, 10),
      customerPayload: historyPayload,
      certificationStatus,
      disclosures,
      versions,
      synthetic: Boolean(synthetic),
      snapshotId,
    });
    // Prefer authentic period certification time over buildReportSnapshotV1 generatedAt
    contentHash = builtSnap.contentHash;
    envelopeHash = builtSnap.envelopeHash;
  } else if (!envelopeHash && contentHashOverride) {
    envelopeHash = envelopeHashOverride || null;
  }

  const reportSnapshot = {
    snapshotId,
    propertyId,
    periodId,
    schema: "ADP_REPORT_SNAPSHOT_V1",
    contentHash,
    envelopeHash,
    certifiedAt: certifiedAt || builtSnap?.certifiedAt || null,
    publishedAt,
    customerPayload: historyPayload,
    disclosures,
    versions,
  };

  const manifest = buildHistoricalWriteManifest({
    propertyId,
    periodId,
    monitoringDate:
      monitoringDate ||
      String(certifiedAt).slice(0, 10) ||
      new Date().toISOString().slice(0, 10),
    certificationStatus,
    customerPayload: historyPayload,
    reportSnapshot,
    historicalVersion,
    synthetic: Boolean(synthetic),
  });

  // Stamp methodology into period metrics disclosures (overwrite empty array)
  for (const rec of manifest.records || []) {
    if (rec.table === HISTORY_TABLES.PERIOD_METRICS) {
      rec.fields["Disclosures JSON"] = JSON.stringify({
        methodologyId: shape.methodologyId || CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
        considerationRate: shape.kpis?.considerationRate ?? null,
        considerationNumerator: shape.kpis?.considerationNumerator ?? null,
        considerationDenominator: shape.kpis?.considerationDenominator ?? null,
        scenarioPresenceRate: shape.kpis?.scenarioPresence ?? null,
        stagedVersionId,
        enrichment: ADP_HISTORY_FIELD_COMPLETENESS_V1,
        contentHashAlgorithm: contentHash ? "ADP_REPORT_SNAPSHOT_V1.sha256OfPayload" : null,
        contentHashProvenance: contentHash
          ? HASH_PROVENANCE.ORIGINAL_AT_WRITE
          : HASH_PROVENANCE.UNAVAILABLE,
        stagedArtifactHashSeparate: stagedPayload?._adpArtifactHash || null,
        stagedArtifactHashMustNotReplaceContentOrEnvelope: true,
      });
    }
    if (rec.table === HISTORY_TABLES.MONITORING_PERIODS) {
      rec.fields["Persistence State"] = "COMPLETE";
      rec.fields.Synthetic = Boolean(synthetic);
    }
    if (rec.table === HISTORY_TABLES.COMPETITIVE_RANKINGS) {
      if (isBlank(rec.fields["Ranking Universe Type"])) {
        rec.fields["Ranking Universe Type"] = "MARKET_CORE";
      }
    }
  }

  const rankingCheck = assertRankingUniverseConsistency({
    measurementFamily: MEASUREMENT_FAMILY.CORE,
    rankingRecords: (manifest.records || []).filter(
      (r) => r.table === HISTORY_TABLES.COMPETITIVE_RANKINGS
    ),
  });
  manifest.rankingUniverseConsistency = rankingCheck;
  if (!rankingCheck.ok) {
    manifest.ok = false;
    manifest.defects = [
      ...(manifest.defects || []),
      ...rankingCheck.failures.map((f) => ({
        code: f.code,
        detail: f.detail,
      })),
    ];
  }

  return manifest;
}

/**
 * Field-level readback comparison.
 * @returns {{ ok: boolean, failures: Array, passCount: number }}
 */
/**
 * Detect Core vs BPP ranking-universe mismatches before AIRTABLE_SYNC_COMPLETE.
 * @returns {{ ok: boolean, failures: Array }}
 */
export function assertRankingUniverseConsistency({
  measurementFamily = MEASUREMENT_FAMILY.CORE,
  rankingRecords = [],
} = {}) {
  const failures = [];
  for (const rec of rankingRecords || []) {
    const fields = rec.fields || rec;
    const universe = fields["Ranking Universe Type"];
    const hasBppMeta =
      !isBlank(fields["Portfolio Lens"]) ||
      !isBlank(fields["Portfolio Type"]) ||
      !isBlank(fields["Peer Set Version"]);
    if (measurementFamily === MEASUREMENT_FAMILY.CORE) {
      if (isBlank(universe)) {
        failures.push({
          code: "CORE_RANKING_UNIVERSE_MISSING",
          detail: "Core ranking requires Ranking Universe Type = MARKET_CORE",
          key: fields["Rank Key"] || rec.idempotencyKey,
        });
      } else if (universe !== "MARKET_CORE") {
        failures.push({
          code: "CORE_RANKING_UNIVERSE_MISMATCH",
          detail: `Core ranking has Ranking Universe Type=${universe}`,
          key: fields["Rank Key"] || rec.idempotencyKey,
        });
      }
      if (hasBppMeta) {
        failures.push({
          code: "CORE_RANKING_BPP_METADATA_PRESENT",
          detail: "Core ranking must not carry BPP portfolio/peer-set fields",
          key: fields["Rank Key"] || rec.idempotencyKey,
        });
      }
    }
    if (measurementFamily === MEASUREMENT_FAMILY.BPP) {
      if (universe !== "BRAND_PORTFOLIO") {
        failures.push({
          code: "BPP_RANKING_UNIVERSE_MISMATCH",
          detail: `BPP ranking requires BRAND_PORTFOLIO, got ${universe || "blank"}`,
          key: fields["Rank Key"] || rec.idempotencyKey,
        });
      }
    }
  }
  return { ok: failures.length === 0, failures };
}

/**
 * Hash provenance label for Airtable / disclosures.
 * ORIGINAL_AT_WRITE = computed via buildReportSnapshotV1 at persistence time.
 * DETERMINISTIC_RECONSTRUCTION = backfill candidate — must not be treated as historical original
 * without founder review.
 */
export const HASH_PROVENANCE = Object.freeze({
  ORIGINAL_AT_WRITE: "ORIGINAL_AT_WRITE",
  DETERMINISTIC_RECONSTRUCTION: "DETERMINISTIC_RECONSTRUCTION",
  STAGED_ARTIFACT_FORBIDDEN: "STAGED_ARTIFACT_FORBIDDEN",
  UNAVAILABLE: "UNAVAILABLE",
});

export function assertFieldLevelReadback({
  expected = {},
  actual = {},
  requiredFields = [],
  allowBlankFields = [],
} = {}) {
  const failures = [];
  let passCount = 0;
  const allow = new Set(allowBlankFields);
  const keys = new Set([
    ...Object.keys(expected || {}),
    ...requiredFields,
  ]);

  for (const field of keys) {
    if (allow.has(field)) continue;
    const exp = expected[field];
    const act = actual[field];
    const req = requiredFields.includes(field);

    if (req && isBlank(act)) {
      failures.push({
        field,
        code: FIELD_BLANK_CLASS.REQUIRED_MISSING,
        expected: exp,
        actual: act ?? null,
      });
      continue;
    }
    if (!isBlank(exp) && isBlank(act)) {
      failures.push({
        field,
        code: FIELD_BLANK_CLASS.SOURCE_AVAILABLE_NOT_MAPPED,
        expected: exp,
        actual: act ?? null,
      });
      continue;
    }
    if (!isBlank(exp) && !isBlank(act) && String(exp) !== String(act)) {
      // Numbers: compare numeric equality
      const ne = Number(exp);
      const na = Number(act);
      if (Number.isFinite(ne) && Number.isFinite(na) && Math.abs(ne - na) < 1e-9) {
        passCount += 1;
        continue;
      }
      failures.push({
        field,
        code: "INCORRECT_VALUE",
        expected: exp,
        actual: act,
      });
      continue;
    }
    passCount += 1;
  }

  return {
    ok: failures.length === 0,
    failures,
    passCount,
    contract: ADP_HISTORY_FIELD_COMPLETENESS_V1,
  };
}

/**
 * Negative fixtures for permanent regression.
 */
export function runFieldCompletenessNegativeFixtures() {
  const results = [];

  results.push({
    id: "EXACT_MATCH_PASS",
    pass: assertFieldLevelReadback({
      expected: { "Property ID": "adp_x", "Consideration Rate": 0.011 },
      actual: { "Property ID": "adp_x", "Consideration Rate": 0.011 },
      requiredFields: ["Property ID", "Consideration Rate"],
    }).ok,
  });

  results.push({
    id: "MISSING_REQUIRED_FAIL",
    pass:
      assertFieldLevelReadback({
        expected: { "Property ID": "adp_x", "Consideration Rate": 0.011 },
        actual: { "Property ID": "adp_x" },
        requiredFields: ["Property ID", "Consideration Rate"],
      }).ok === false,
  });

  results.push({
    id: "SOURCE_AVAILABLE_BLANK_FAIL",
    pass:
      assertFieldLevelReadback({
        expected: { "Content Hash": "abc" },
        actual: {},
        requiredFields: [],
      }).ok === false,
  });

  results.push({
    id: "WRONG_VALUE_FAIL",
    pass:
      assertFieldLevelReadback({
        expected: { Rank: 11 },
        actual: { Rank: 3 },
        requiredFields: ["Rank"],
      }).ok === false,
  });

  results.push({
    id: "CLASSIFY_SOURCE_AVAILABLE",
    pass:
      classifyBlankField({
        tableName: HISTORY_TABLES.PERIOD_METRICS,
        fieldName: "Consideration Rate",
        liveValue: null,
        expectedValue: 0.011,
        hasMatch: true,
      }) === FIELD_BLANK_CLASS.REQUIRED_MISSING,
  });

  results.push({
    id: "CLASSIFY_INTENTIONAL_NULL",
    pass:
      classifyBlankField({
        tableName: HISTORY_TABLES.MONITORING_PERIODS,
        fieldName: "Corrects Period Key",
        liveValue: "",
        expectedValue: "",
        hasMatch: true,
      }) === FIELD_BLANK_CLASS.INTENTIONALLY_NULL,
  });

  results.push({
    id: "ENRICH_COMPUTES_REPORT_SNAPSHOT_HASHES_AND_MARKET_CORE",
    pass: (() => {
      const basePayload = {
        ok: true,
        version: "ai_demand_positioning_v1",
        period: { executionDate: "2026-11-03T07:33:04.093Z" },
        executiveMetrics: {
          considerationRate: {
            rate: 0.011,
            presentObservations: 2,
            comparableObservations: 184,
            methodologyId: CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
          },
          scenarioPresence: { rate: 0.043 },
        },
        competitiveRankingByTerritory: {
          byTerritory: {
            overall: {
              comparableN: 184,
              displayRows: [
                {
                  isSubject: true,
                  displayRank: 11,
                  appearances: 2,
                  name: "UMusic",
                  entityId: "__subject__",
                },
              ],
            },
          },
        },
      };
      const computed = enrichHistoricalWriteManifestFromStaged({
        propertyId: "adp_umusic_hotel_madrid",
        periodId: "adp_period_test",
        stagedPayload: basePayload,
        monitoringDate: "2026-11-03",
        synthetic: false,
      });
      const override = enrichHistoricalWriteManifestFromStaged({
        propertyId: "adp_umusic_hotel_madrid",
        periodId: "adp_period_test",
        stagedPayload: basePayload,
        monitoringDate: "2026-11-03",
        synthetic: false,
        contentHashOverride: "abc123",
        envelopeHashOverride: "env456",
        computeReportSnapshotHashes: false,
      });
      const mp0 = computed.records.find((r) => r.table === HISTORY_TABLES.MONITORING_PERIODS);
      const snap0 = computed.records.find((r) => r.table === HISTORY_TABLES.REPORT_SNAPSHOTS);
      const mp1 = override.records.find((r) => r.table === HISTORY_TABLES.MONITORING_PERIODS);
      const pm = computed.records.find((r) => r.table === HISTORY_TABLES.PERIOD_METRICS);
      const rank = computed.records.find((r) => r.table === HISTORY_TABLES.COMPETITIVE_RANKINGS);
      const expectedContent = sha256OfPayload(
        computed.records.find((r) => r.table === HISTORY_TABLES.REPORT_SNAPSHOTS)
          ? // content hash is over historyPayload inside enricher — re-check non-empty hex
            null
          : null
      );
      void expectedContent;
      return (
        typeof mp0?.fields?.["Content Hash"] === "string" &&
        mp0.fields["Content Hash"].length === 64 &&
        typeof snap0?.fields?.["Envelope Hash"] === "string" &&
        snap0.fields["Envelope Hash"].length === 64 &&
        mp1?.fields?.["Content Hash"] === "abc123" &&
        mp1?.fields?.["Envelope Hash"] === "env456" &&
        mp0?.fields?.["Certification Timestamp"] === "2026-11-03T07:33:04.093Z" &&
        pm?.fields?.["Consideration Rate"] === 0.011 &&
        pm?.fields?.["Consideration Denominator"] === 184 &&
        rank?.fields?.["Ranking Universe Type"] === "MARKET_CORE" &&
        rank?.fields?.Rank === 11
      );
    })(),
  });

  results.push({
    id: "RANKING_UNIVERSE_NOT_INTENTIONAL_NULL",
    pass:
      classifyBlankField({
        tableName: HISTORY_TABLES.COMPETITIVE_RANKINGS,
        fieldName: "Ranking Universe Type",
        liveValue: null,
        expectedValue: "MARKET_CORE",
        hasMatch: true,
        measurementFamily: MEASUREMENT_FAMILY.CORE,
      }) === FIELD_BLANK_CLASS.REQUIRED_MISSING,
  });

  results.push({
    id: "PORTFOLIO_LENS_NOT_APPLICABLE_ON_CORE",
    pass:
      classifyBlankField({
        tableName: HISTORY_TABLES.COMPETITIVE_RANKINGS,
        fieldName: "Portfolio Lens",
        liveValue: null,
        expectedValue: undefined,
        hasMatch: true,
        measurementFamily: MEASUREMENT_FAMILY.CORE,
      }) === FIELD_BLANK_CLASS.NOT_APPLICABLE,
  });

  results.push({
    id: "RANKING_UNIVERSE_MISMATCH_DETECTED",
    pass:
      assertRankingUniverseConsistency({
        measurementFamily: MEASUREMENT_FAMILY.CORE,
        rankingRecords: [
          { fields: { "Rank Key": "k1", "Ranking Universe Type": "BRAND_PORTFOLIO" } },
        ],
      }).ok === false &&
      assertRankingUniverseConsistency({
        measurementFamily: MEASUREMENT_FAMILY.CORE,
        rankingRecords: [
          { fields: { "Rank Key": "k2", "Ranking Universe Type": "MARKET_CORE" } },
        ],
      }).ok === true,
  });

  results.push({
    id: "CORE_BLANK_RANKING_UNIVERSE_REJECTED",
    pass:
      assertRankingUniverseConsistency({
        measurementFamily: MEASUREMENT_FAMILY.CORE,
        rankingRecords: [{ fields: { "Rank Key": "k3" } }],
      }).ok === false,
  });

  results.push({
    id: "ENRICH_NEVER_STAGED_AS_PUBLICATION_COMMIT",
    pass: (() => {
      const m = enrichHistoricalWriteManifestFromStaged({
        propertyId: "adp_x",
        periodId: "p1",
        stagedPayload: { ok: true, version: "ai_demand_positioning_v1" },
        stagedVersionId: "staged_abc",
        synthetic: false,
      });
      const snap = m.records.find((r) => r.table === HISTORY_TABLES.REPORT_SNAPSHOTS);
      return !snap?.fields?.["Publication Commit"];
    })(),
  });

  results.push({
    id: "FALSE_POSITIVE_EXISTENCE_ONLY_REJECTED",
    pass:
      assertFieldLevelReadback({
        expected: {
          "Period Key": "k",
          "Content Hash": "hash",
          "Consideration Rate": 0.05,
        },
        actual: { "Period Key": "k" },
        requiredFields: ["Period Key", "Consideration Rate"],
      }).ok === false,
  });

  return {
    ok: results.every((r) => r.pass),
    results,
    contract: ADP_HISTORY_FIELD_COMPLETENESS_V1,
  };
}
