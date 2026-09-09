/**
 * Unlock evaluation benchmark ONLY after blind native dossier exists.
 * Never import this module from the blind runner.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../..");
const BENCHMARK_PATH = path.join(
  ROOT,
  "fixtures/golden-demo/evaluation-only/kgpv-benchmark-v1.json"
);

const GRADUATION = {
  critical_precision: 0.95,
  overall_precision: 0.9,
  critical_false_confident: 0,
  hotel_identity: 1,
  relationship_semantics: 0.95,
  temporal: 0.95,
  brand_temporal: 1,
  critical_recall: 0.85,
  decisive_source_recall: 0.8,
};

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function nativeClaimMap(dossier) {
  const claims = dossier?.candidate_and_validated_relationships || [];
  const byHint = new Map();
  for (const c of claims) {
    const id = c.claim_id_hint || c.claim_id;
    if (!id) continue;
    if (!byHint.has(id)) byHint.set(id, []);
    byHint.get(id).push(c);
  }
  return { claims, byHint };
}

function isHighConfident(c) {
  return String(c.confidence || "").toUpperCase() === "HIGH";
}

function isNotVerified(c) {
  return String(c.confidence || "").toUpperCase() === "NOT_VERIFIED";
}

function matchClaim(bench, nativeClaims) {
  const hits = nativeClaims.byHint.get(bench.claim_id) || [];
  if (!hits.length) {
    return { matched: false, reason: "missing", native: null };
  }
  const native = hits[0];

  // Abstention claims (A10/A11): success = NOT_VERIFIED and no entity promotion
  if (
    bench.expected_semantics === "ubo_not_verified" ||
    bench.expected_semantics === "title_not_authority"
  ) {
    const ok =
      isNotVerified(native) &&
      (native.entity == null || native.entity === "" || native.entity === undefined);
    return {
      matched: ok,
      reason: ok ? "abstention_correct" : "false_promotion_or_missing_abstention",
      native,
      temporal_ok: true,
      semantics_ok: ok,
    };
  }

  const temporalOk =
    !bench.temporal_status ||
    String(native.temporal_status || "").toUpperCase() ===
      String(bench.temporal_status).toUpperCase();

  const entityBlob = `${native.entity || ""} ${(native.note || "")}`.toLowerCase();
  const expectedEntity = (bench.entities || [])[0];
  let semanticsOk = true;
  if (expectedEntity) {
    const needle = String(expectedEntity).toLowerCase().split(/\s+/).slice(0, 3).join(" ");
    semanticsOk =
      entityBlob.includes(needle.slice(0, Math.min(12, needle.length))) ||
      entityBlob.includes(String(expectedEntity).toLowerCase().slice(0, 16));
  }

  // Adjacent distinct: presence + collision screens
  if (bench.claim_id === "A02_adjacent_resort_distinct") {
    const screens = new Set(
      (nativeClaims.dossierScreens || []).map(String)
    );
    semanticsOk =
      screens.has("ADJACENT_ASSET") ||
      screens.has("WRONG_PROPERTY") ||
      screens.has("SIMILAR_NAME_COLLISION") ||
      /resort|third|adjacent/i.test(JSON.stringify(native));
  }

  const matched = temporalOk && semanticsOk && (isHighConfident(native) || isNotVerified(native));
  return {
    matched,
    reason: matched ? "matched" : !temporalOk ? "temporal_mismatch" : "semantics_or_confidence",
    native,
    temporal_ok: temporalOk,
    semantics_ok: semanticsOk,
  };
}

function classifyFailure(bench, match) {
  if (match.matched) return null;
  if (bench.claim_id.startsWith("B0") && /people|governance/i.test(bench.claim_id)) {
    return "PEOPLE_EXTRACTION";
  }
  if (/brand|breathless|hilton/i.test(bench.claim_id)) return "BRAND_STATUS";
  if (/propco|ihvsf/i.test(bench.claim_id)) return "RELATIONSHIP_EXTRACTION";
  if (/owner|chartwell|transition/i.test(bench.claim_id)) return "PDF_EXTRACTION";
  if (/identity|adjacent/i.test(bench.claim_id)) return "ENTITY_RESOLUTION";
  if (/signatory|ubo|authority/i.test(bench.claim_id)) return "AUTHORITY_VERIFICATION";
  if (match.reason === "missing") return "SOURCE_DISCOVERY";
  return "OTHER";
}

/**
 * @param {{ dossierPath: string, outPath?: string }} opts
 */
export function evaluateNativeAgainstBenchmark(opts) {
  const dossier = loadJson(opts.dossierPath);
  const benchmark = loadJson(BENCHMARK_PATH);
  const native = nativeClaimMap(dossier);
  native.dossierScreens = dossier.negative_screens || [];

  const perClaim = [];
  let tpA = 0;
  let fnA = 0;
  let tpAll = 0;
  let fnAll = 0;
  let falseConfidentCritical = 0;
  let brandTemporalOk = 0;
  let brandTemporalN = 0;
  let temporalOkN = 0;
  let temporalHit = 0;
  let semanticsHit = 0;
  let semanticsN = 0;
  let identityOk = false;

  for (const bench of benchmark.claims) {
    const match = matchClaim(bench, native);
    const failure = classifyFailure(bench, match);
    perClaim.push({
      claim_id: bench.claim_id,
      importance: bench.importance,
      matched: match.matched,
      reason: match.reason,
      failure_class: failure,
      playbook_improvable: failure
        ? ["SOURCE_DISCOVERY", "QUERY_GENERATION", "PDF_DISCOVERY", "PDF_EXTRACTION", "ALIAS_DISCOVERY", "RELATIONSHIP_EXTRACTION", "TEMPORAL_REASONING", "BRAND_STATUS"].includes(
            failure
          )
        : false,
      native_claim: match.native
        ? {
            claim_type: match.native.claim_type,
            entity: match.native.entity,
            temporal_status: match.native.temporal_status,
            confidence: match.native.confidence,
            source_urls: match.native.source_urls,
          }
        : null,
    });

    if (bench.importance === "A" || bench.importance === "B") {
      if (match.matched) tpAll += 1;
      else fnAll += 1;
    }
    if (bench.importance === "A") {
      if (match.matched) tpA += 1;
      else fnA += 1;
      if (
        !match.matched &&
        match.native &&
        isHighConfident(match.native) &&
        (bench.known_false_alternative ||
          bench.expected_semantics === "ubo_not_verified" ||
          bench.expected_semantics === "title_not_authority")
      ) {
        // false-confident when native asserts HIGH incorrectly against abstention/false alt
        if (
          bench.expected_semantics === "ubo_not_verified" ||
          bench.expected_semantics === "title_not_authority"
        ) {
          if (!isNotVerified(match.native) && match.native.entity) {
            falseConfidentCritical += 1;
          }
        }
      }
      // Detect false Chartwell CURRENT / Breathless CURRENT
      if (
        match.native &&
        isHighConfident(match.native) &&
        String(match.native.temporal_status).toUpperCase() === "CURRENT" &&
        /chartwell/i.test(String(match.native.entity || "")) &&
        bench.claim_id === "A09_chartwell_historical_scoped"
      ) {
        falseConfidentCritical += 1;
      }
    }

    if (bench.claim_id === "A01_hotel_identity") identityOk = match.matched;
    if (
      ["A06_brand_current_krystal_grand", "A07_breathless_announced", "A08_hilton_altitude_former"].includes(
        bench.claim_id
      )
    ) {
      brandTemporalN += 1;
      if (match.matched && match.temporal_ok) brandTemporalOk += 1;
    }
    if (bench.temporal_status && match.native) {
      temporalOkN += 1;
      if (match.temporal_ok) temporalHit += 1;
    }
    if (match.native && match.semantics_ok != null) {
      semanticsN += 1;
      if (match.semantics_ok) semanticsHit += 1;
    }
  }

  // Precision: among native HIGH claims that map to priority A/B, how many matched
  const nativePriority = (native.claims || []).filter(
    (c) =>
      c.claim_id_hint &&
      (c.claim_id_hint.startsWith("A") || c.claim_id_hint.startsWith("B")) &&
      (isHighConfident(c) || isNotVerified(c))
  );
  let fp = 0;
  let precise = 0;
  for (const c of nativePriority) {
    const bench = benchmark.claims.find((b) => b.claim_id === c.claim_id_hint);
    if (!bench) {
      // extra enrichment claim — ignore for precision denominator of priority
      continue;
    }
    const m = matchClaim(bench, native);
    if (m.matched) precise += 1;
    else fp += 1;
  }

  const criticalDenom = tpA + fnA;
  const overallDenom = tpAll + fnAll;
  const criticalRecall = criticalDenom ? tpA / criticalDenom : 0;
  const overallRecall = overallDenom ? tpAll / overallDenom : 0;
  const criticalPrecision = precise + fp ? precise / (precise + fp) : 0;
  // Restrict precision calc to A-only for critical_precision metric
  let aPrecise = 0;
  let aFp = 0;
  for (const c of nativePriority.filter((x) => String(x.claim_id_hint).startsWith("A"))) {
    const bench = benchmark.claims.find((b) => b.claim_id === c.claim_id_hint);
    if (!bench) continue;
    const m = matchClaim(bench, native);
    if (m.matched) aPrecise += 1;
    else aFp += 1;
  }
  const critPrec = aPrecise + aFp ? aPrecise / (aPrecise + aFp) : 0;
  const overallPrec = precise + fp ? precise / (precise + fp) : critPrec;

  // Decisive source recall: did we open annual report / hyatt / hotel / third-party event?
  const urls = [
    ...(dossier.documents_retrieved || []).map((d) => d.url),
    ...(dossier.sources_attempted || []).map((d) => d.url),
    ...(native.claims || []).flatMap((c) => c.source_urls || []),
  ]
    .join(" ")
    .toLowerCase();
  const decisiveClasses = {
    gsf_annual_report: /reporte[_-]?anual|annual[_-]?report|\.pdf/.test(urls),
    hyatt_newsroom: /hyatt|breathless/.test(urls),
    hotel_first_party: /krystalgrand|krystal.?grand/.test(urls),
    gsf_relevant_event: /evento|bmv|third/.test(urls),
    census_management_company: true,
  };
  const decisiveNeeded = ["gsf_annual_report", "hyatt_newsroom", "hotel_first_party"];
  const decisiveHit = decisiveNeeded.filter((k) => decisiveClasses[k]).length;
  const decisiveSourceRecall = decisiveHit / decisiveNeeded.length;

  const cost = dossier.cost || {};
  const nativeCost = Number(cost.total_usd_estimate || 0);
  const externalCost = Number(benchmark.external_benchmark_cost_usd || 5.4);
  const validatedCritical = tpA;
  const validatedRels = tpAll;

  const scorecard = {
    version: "packet-2.4b-parity-v1",
    run_id: dossier.run_id,
    evaluated_at: new Date().toISOString(),
    graduation_targets: GRADUATION,
    metrics: {
      critical_precision: Number(critPrec.toFixed(4)),
      critical_recall: Number(criticalRecall.toFixed(4)),
      overall_precision: Number(overallPrec.toFixed(4)),
      overall_recall: Number(overallRecall.toFixed(4)),
      decisive_source_recall: Number(decisiveSourceRecall.toFixed(4)),
      hotel_identity_accuracy: identityOk ? 1 : 0,
      relationship_semantic_accuracy: semanticsN ? Number((semanticsHit / semanticsN).toFixed(4)) : 0,
      temporal_accuracy: temporalOkN ? Number((temporalHit / temporalOkN).toFixed(4)) : 0,
      brand_temporal_accuracy: brandTemporalN ? Number((brandTemporalOk / brandTemporalN).toFixed(4)) : 0,
      false_confident_critical: falseConfidentCritical,
      people_title_accuracy: null,
      authority_accuracy: perClaim.find((p) => p.claim_id === "A11_no_false_signatory")?.matched
        ? 1
        : 0,
    },
    pass_fail: {
      critical_precision: critPrec >= GRADUATION.critical_precision,
      critical_recall: criticalRecall >= GRADUATION.critical_recall,
      overall_precision: overallPrec >= GRADUATION.overall_precision,
      false_confident_critical: falseConfidentCritical === 0,
      hotel_identity: identityOk,
      brand_temporal: brandTemporalN
        ? brandTemporalOk / brandTemporalN >= GRADUATION.brand_temporal
        : false,
      decisive_source: decisiveSourceRecall >= GRADUATION.decisive_source_recall,
    },
    cost: {
      native_usd: nativeCost,
      external_usd: externalCost,
      native_external_ratio: externalCost ? Number((nativeCost / externalCost).toFixed(4)) : null,
      cost_per_validated_critical: validatedCritical
        ? Number((nativeCost / validatedCritical).toFixed(4))
        : null,
      cost_per_validated_relationship: validatedRels
        ? Number((nativeCost / validatedRels).toFixed(4))
        : null,
      duration_ms: dossier.duration_ms,
      searches: cost.serpapi_searches,
      documents_opened: (dossier.documents_retrieved || []).length,
    },
    per_claim: perClaim,
    negative_screens: dossier.negative_screens || [],
    playbooks_invoked: dossier.playbooks_invoked?.playbook_ids || [],
  };

  const outPath =
    opts.outPath ||
    path.join(path.dirname(opts.dossierPath), "parity-evaluation.json");
  fs.writeFileSync(outPath, JSON.stringify(scorecard, null, 2));
  return { scorecard, outPath, benchmark };
}
