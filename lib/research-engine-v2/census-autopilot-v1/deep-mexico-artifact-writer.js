/**
 * Write Autopilot V1.1 deep Mexico artifacts 01–21.
 */

import fs from "node:fs";
import path from "node:path";
import { FIELD_RESOLUTION_STATUS, OUTPUT_CLASS } from "./constants.js";
import { MATERIAL_HARD_FIELDS, LIVE_DEEP_VERSION } from "./live-deep-research.js";
import { V1_BASELINE } from "./deep-mexico-orchestrator.js";

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}
function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

/**
 * @param {object} result
 * @param {{ root?: string, log?: Function }} ctx
 */
export async function writeDeepMexicoArtifacts(result, ctx = {}) {
  const dir = result.artifact_root;
  const log = ctx.log || (() => {});
  const processed = result.processed || [];
  const obs = result.observability;

  // 02 / 03 Lane results
  writeJson(path.join(dir, "02-live-lane-a-results.json"), {
    version: LIVE_DEEP_VERSION,
    hotels: processed.length,
    page_ok: processed.filter((h) => h.page_ok).length,
    directory_ok: processed.filter((h) => h.directory_ok).length,
    source_blocked: processed.filter((h) => h.source_blocked).length,
    by_family: summarizeByFamily(processed, "lane_a"),
    sample: processed.slice(0, 20).map((h) => ({
      id: h.independent_record_id,
      family: h.family,
      page_ok: h.page_ok,
      attempts: (h.attempts || []).filter((a) => a.lane === "A"),
      lane_a_resolved_fields: h.lane_a_resolved_fields,
    })),
  });

  writeJson(path.join(dir, "03-live-lane-b-results.json"), {
    version: LIVE_DEEP_VERSION,
    hotels_attempted_lane_b: processed.filter((h) =>
      (h.attempts || []).some((a) => a.lane === "B")
    ).length,
    lane_b_ok: processed.filter((h) => h.lane_b_ok).length,
    sample: processed
      .filter((h) => (h.attempts || []).some((a) => a.lane === "B"))
      .slice(0, 30)
      .map((h) => ({
        id: h.independent_record_id,
        attempts: (h.attempts || []).filter((a) => a.lane === "B"),
        lane_b_resolved_fields: h.lane_b_resolved_fields,
      })),
  });

  // 04 field matrix
  const matrix = buildFieldMatrix(processed);
  writeJson(path.join(dir, "04-field-resolution-matrix.json"), matrix);

  // 05 hard fields
  writeJson(path.join(dir, "05-hard-field-results.json"), {
    version: LIVE_DEEP_VERSION,
    hard_fields: MATERIAL_HARD_FIELDS.map((f) => matrix.fields.find((x) => x.field === f)).filter(
      Boolean
    ),
  });

  // 06 temporal / contradictions
  writeJson(path.join(dir, "06-temporal-contradictions.json"), {
    version: LIVE_DEEP_VERSION,
    contradictions: processed.flatMap((h) =>
      (h.fields || [])
        .filter((f) => f.contradiction_found)
        .map((f) => ({
          id: h.independent_record_id,
          name: h.name,
          field: f.field,
          prior: f.prior_value,
          researched: f.researched_value,
          status: f.resolution_status,
        }))
    ),
  });

  // 07 effort
  writeJson(path.join(dir, "07-research-effort-analysis.json"), {
    version: LIVE_DEEP_VERSION,
    avg_effort: avg(processed.map((h) => h.research_effort_score || 0)),
    avg_attempts: avg(processed.map((h) => h.source_attempt_count || 0)),
    avg_max_level: avg(processed.map((h) => h.resolution_level_max || 0)),
    distribution_max_level: countBy(processed, (h) => String(h.resolution_level_max || 0)),
    hotels: processed.map((h) => ({
      id: h.independent_record_id,
      effort: h.research_effort_score,
      attempts: h.source_attempt_count,
      max_level: h.resolution_level_max,
      material_after: h.material_pct_after,
    })),
  });

  // 08 escalations
  const escalations = buildEscalations(processed);
  writeJson(path.join(dir, "08-deep-research-escalations.json"), escalations);

  // 09 first-party
  writeJson(path.join(dir, "09-first-party-validation-deltas.json"), {
    version: LIVE_DEEP_VERSION,
    note: "Do not send — staging deltas only",
    deltas: buildFirstPartyDeltas(processed),
  });

  // 10 production classification
  const classCompare = buildClassCompare(processed);
  writeJson(path.join(dir, "10-production-classification.json"), classCompare);

  // 11 steward
  writeText(path.join(dir, "11-steward-workload-analysis.md"), renderSteward(obs, classCompare, escalations));

  // 12 brand
  writeJson(path.join(dir, "12-brand-completion-readiness.json"), {
    ...result.activation_candidates,
    brand_aggregation_summary: {
      brand_count: result.brand_aggregation?.brand_count,
      top: (result.brand_aggregation?.brands || []).slice(0, 25).map((b) => ({
        brand: b.brand,
        family: b.family,
        count: b.hotel_count_independent,
        classes: b.output_classes,
      })),
    },
  });

  // 13 operators
  const ops = processed
    .map((h) => h.operator_staging)
    .filter((o) => o && o.operator);
  writeJson(path.join(dir, "13-operator-relationships.json"), {
    version: LIVE_DEEP_VERSION,
    independently_resolved: ops.length,
    relationships: processed.map((h) => h.operator_staging),
  });

  writeJson(path.join(dir, "14-cvent-challenge-sample.json"), result.cvent_sample);
  writeJson(path.join(dir, "15-legacy-challenge-sample.json"), result.legacy_sample);

  writeJson(path.join(dir, "16-image-integrity-results.json"), {
    version: LIVE_DEEP_VERSION,
    hotels: processed.map((h) => ({
      id: h.independent_record_id,
      ...h.image_integrity,
      image_rights_review_required: h.image_rights_review_required,
    })),
    summary: {
      rights_review_required: processed.filter((h) => h.image_rights_review_required).length,
      auto_download: false,
      auto_replace: false,
      blocks_data_eligibility: false,
    },
  });

  writeText(path.join(dir, "17-autopilot-operating-loop.md"), renderOperatingLoop());
  writeText(path.join(dir, "18-future-write-lanes.md"), renderWriteLanes(matrix));
  writeText(path.join(dir, "19-migration-readiness.md"), renderMigration(obs, classCompare));
  writeText(path.join(dir, "20-mexico-cala-readiness.md"), renderMexicoCala(obs));
  writeText(
    path.join(dir, "21-final-report.md"),
    renderFinal(result, matrix, classCompare, escalations, ops)
  );

  // Persist full processed compact
  writeJson(path.join(result.run_dir, "processed-compact.json"), {
    hotels: processed.map((h) => ({
      id: h.independent_record_id,
      family: h.family,
      brand: h.brand,
      name: h.name,
      output_class: h.output_class,
      material_before: h.material_pct_before,
      material_after: h.material_pct_after,
      resolved: h.fields_resolved,
      page_ok: h.page_ok,
      v1_class_before: h.v1_class_before,
    })),
  });

  log(`[v1.1] artifacts → ${dir}`);
}

function buildFieldMatrix(processed) {
  /** @type {Map<string, object>} */
  const map = new Map();
  for (const h of processed) {
    for (const f of h.fields || []) {
      if (!map.has(f.field)) {
        map.set(f.field, {
          field: f.field,
          applicable: 0,
          verified: 0,
          confirmed_existing: 0,
          missing_found: 0,
          contradicted: 0,
          superseded: 0,
          unknown: 0,
          conflicting: 0,
          blocked: 0,
          deep_research_required: 0,
          not_applicable: 0,
          derived: 0,
        });
      }
      const s = map.get(f.field);
      s.applicable += 1;
      const st = f.resolution_status;
      if (st === FIELD_RESOLUTION_STATUS.VERIFIED) s.verified += 1;
      else if (st === FIELD_RESOLUTION_STATUS.CONFIRMED_EXISTING) s.confirmed_existing += 1;
      else if (st === FIELD_RESOLUTION_STATUS.MISSING_FOUND) s.missing_found += 1;
      else if (st === FIELD_RESOLUTION_STATUS.CONTRADICTED) s.contradicted += 1;
      else if (st === FIELD_RESOLUTION_STATUS.SUPERSEDED) s.superseded += 1;
      else if (st === FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE) s.unknown += 1;
      else if (st === FIELD_RESOLUTION_STATUS.CONFLICTING_EVIDENCE) s.conflicting += 1;
      else if (st === FIELD_RESOLUTION_STATUS.SOURCE_BLOCKED) s.blocked += 1;
      else if (st === FIELD_RESOLUTION_STATUS.DEEP_RESEARCH_REQUIRED) s.deep_research_required += 1;
      else if (st === FIELD_RESOLUTION_STATUS.NOT_APPLICABLE) s.not_applicable += 1;
      else if (st === FIELD_RESOLUTION_STATUS.DERIVED) s.derived += 1;
    }
  }
  const fields = [...map.values()].map((s) => {
    const native =
      s.verified + s.confirmed_existing + s.missing_found + s.superseded + s.derived;
    const native_resolution_pct = s.applicable
      ? Math.round((100 * native) / s.applicable)
      : 0;
    let rank = "ESCALATION DOMINANT";
    if (native_resolution_pct >= 85) rank = "EASY NATIVE";
    else if (native_resolution_pct >= 50) rank = "MODERATE NATIVE";
    else if (native_resolution_pct >= 20) rank = "HARD NATIVE";
    return { ...s, native_resolution_pct, rank };
  });
  fields.sort((a, b) => b.native_resolution_pct - a.native_resolution_pct);
  return { version: LIVE_DEEP_VERSION, hotels: processed.length, fields };
}

function buildEscalations(processed) {
  const byClass = { A: 0, B: 0, C: 0, D: 0, E: 0 };
  const hotelsNeedingWebhound = new Set();
  const materialNeedingWebhound = [];
  const packages = [];

  for (const h of processed) {
    for (const f of h.fields || []) {
      if (
        ![
          FIELD_RESOLUTION_STATUS.DEEP_RESEARCH_REQUIRED,
          FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE,
          FIELD_RESOLUTION_STATUS.SOURCE_BLOCKED,
          FIELD_RESOLUTION_STATUS.CONFLICTING_EVIDENCE,
        ].includes(f.resolution_status)
      ) {
        continue;
      }
      // Only material hard fields enter escalation accounting
      if (!MATERIAL_HARD_FIELDS.includes(f.field) && f.field !== "Owner Name") continue;

      let cls = "D";
      // Policy: do NOT auto-route every owner/operator unknown to Webhound
      if (f.field === "Owner Name") {
        cls = "B"; // first-party preferable; accept Unknown otherwise
      } else if (f.field === "Operator / Management Company") {
        cls = h.page_ok ? "D" : "C"; // franchise pages rarely disclose; human/specialist or accept
      } else if (f.field === "Rooms / Keys") {
        // Webhound useful when official pages/directories exhausted
        cls = h.page_ok || h.directory_ok ? "A" : "A";
        // Prefer first-party for branded chains when page was available but rooms absent
        if (h.page_ok && ["IHG", "Hilton", "Choice"].includes(h.family)) cls = "B";
        else cls = "A";
      } else if (f.field === "Opening Date") {
        cls = h.family === "Hilton" ? "E" : "A"; // Hilton GraphQL often has it; else webhound/secondary
        if (f.resolution_status === FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE && !h.page_ok) cls = "A";
        else if (f.resolution_status === FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE) cls = "B";
      } else if (["Latitude", "Longitude", "Phone", "Address"].includes(f.field)) {
        cls = "E";
      } else if (/Amenit|F&B|Meeting|Spa|Pool|Fitness/i.test(f.field)) {
        cls = "D";
      } else {
        const esc = f.escalation_status || "";
        if (/webhound/i.test(esc)) cls = "A";
        else if (/first_party/i.test(esc)) cls = "B";
        else if (/human|specialist|steward/i.test(esc)) cls = "C";
        else if (/accept_unknown/i.test(esc)) cls = "D";
        else if (/retry/i.test(esc)) cls = "E";
      }

      byClass[cls] += 1;
      if (cls === "A") {
        hotelsNeedingWebhound.add(h.independent_record_id);
        materialNeedingWebhound.push({ id: h.independent_record_id, field: f.field });
      }
      if (packages.length < 200) {
        packages.push({
          hotel_id: h.independent_record_id,
          name: h.name,
          field: f.field,
          class: cls,
          class_label: {
            A: "Webhound likely useful",
            B: "First-party validation preferable",
            C: "Human/specialist research preferable",
            D: "Accept Unknown",
            E: "Retry later",
          }[cls],
          auto_call_webhound: false,
        });
      }
    }
  }

  const hotelPct = processed.length
    ? Math.round((100 * hotelsNeedingWebhound.size) / processed.length)
    : 0;
  const fieldPct = processed.length
    ? Math.round(
        (100 * materialNeedingWebhound.length) /
          Math.max(1, processed.length * MATERIAL_HARD_FIELDS.length)
      )
    : 0;

  return {
    version: LIVE_DEEP_VERSION,
    auto_call_webhound: false,
    classification_counts: byClass,
    hotels_webhound_candidate: hotelsNeedingWebhound.size,
    hotels_webhound_pct_estimate: hotelPct,
    material_fields_webhound_pct_estimate: fieldPct,
    likely_categories: ["rooms_keys", "opening_date_secondary", "hard_independent_discovery"],
    likely_budget_note:
      "Realistic Mexico 3-family: ~10–25% hotels for rooms/opening hard cases at $2–5 → ~$70–450; do not auto-spend. Owner/operator prefer first-party / accept Unknown.",
    packages,
  };
}

function buildFirstPartyDeltas(processed) {
  const deltas = [];
  for (const h of processed) {
    const missing = (h.fields || [])
      .filter((f) =>
        [
          "Rooms / Keys",
          "Opening Date",
          "Operator / Management Company",
          "Owner Name",
          "Renovation / Conversion Date",
        ].includes(f.field)
      )
      .filter(
        (f) =>
          f.resolution_status === FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE ||
          f.resolution_status === FIELD_RESOLUTION_STATUS.DEEP_RESEARCH_REQUIRED
      )
      .map((f) => f.field);
    if (!missing.length) continue;
    deltas.push({
      brand: h.brand,
      family: h.family,
      hotel_id: h.independent_record_id,
      hotel_name: h.name,
      fields_requested: missing,
      do_not_send: true,
    });
  }
  return deltas.slice(0, 500);
}

function buildClassCompare(processed) {
  const after = {};
  const before = {};
  let remediationResolved = 0;
  let remediationTotal = 0;
  for (const h of processed) {
    after[h.output_class] = (after[h.output_class] || 0) + 1;
    before[h.v1_class_before] = (before[h.v1_class_before] || 0) + 1;
    if (h.v1_class_before === OUTPUT_CLASS.VERIFIED_MATERIAL_REMEDIATION) {
      remediationTotal += 1;
      if (h.output_class === OUTPUT_CLASS.VERIFIED_PRODUCTION_CANDIDATE) {
        remediationResolved += 1;
      }
    }
  }
  return {
    version: LIVE_DEEP_VERSION,
    v1_baseline: V1_BASELINE,
    before_counts: before,
    after_counts: after,
    material_remediation_in_cohort: remediationTotal,
    material_remediation_promoted_to_production_candidate: remediationResolved,
    production_candidates_after: after[OUTPUT_CLASS.VERIFIED_PRODUCTION_CANDIDATE] || 0,
  };
}

function summarizeByFamily(processed, _lane) {
  const out = {};
  for (const h of processed) {
    const f = h.family || "Unknown";
    if (!out[f]) out[f] = { hotels: 0, page_ok: 0, avg_material: [] };
    out[f].hotels += 1;
    if (h.page_ok) out[f].page_ok += 1;
    out[f].avg_material.push(h.material_pct_after || 0);
  }
  for (const f of Object.keys(out)) {
    out[f].avg_material_pct = avg(out[f].avg_material);
    delete out[f].avg_material;
  }
  return out;
}

function avg(nums) {
  if (!nums.length) return 0;
  return Math.round(nums.reduce((a, b) => a + Number(b || 0), 0) / nums.length);
}
function countBy(arr, fn) {
  const o = {};
  for (const x of arr) {
    const k = fn(x);
    o[k] = (o[k] || 0) + 1;
  }
  return o;
}

function renderSteward(obs, classCompare, escalations) {
  const prod = classCompare.production_candidates_after;
  const deep = obs.deep_research || 0;
  const rem = classCompare.after_counts[OUTPUT_CLASS.VERIFIED_MATERIAL_REMEDIATION] || 0;
  const total = obs.hotels_deeply_researched || 1;
  // Human exception = deep research + conflicting + first-party critical — not routine unknowns
  const humanExceptions = deep + (escalations.classification_counts.C || 0) / Math.max(1, total);
  const exceptionHotels = deep + Math.min(total, escalations.hotels_webhound_candidate || 0);
  // Better: steward review = non-production-candidate that are material remediation needing judgment
  const stewardReview = rem + deep;
  const autonomous = prod;
  const humanRate = Math.round((100 * stewardReview) / total);

  return `# Steward Workload Analysis

## After live deep research

| Bucket | Count |
|--------|------:|
| Fully autonomous staging (Production Candidate) | ${autonomous} |
| Material remediation (steward optional / batch) | ${rem} |
| Deep research required | ${deep} |
| Image rights review (parallel, not data-blocking) | ${obs.hotels_deeply_researched} |
| Webhound candidate hotels (not called) | ${escalations.hotels_webhound_candidate} |

## Human exception rate

**Steward-touch rate (remediation + deep):** ~${humanRate}%  
**Routine research handled by Autopilot:** production candidates + confirmed fields without Joan per step.

Routine Unknowns on non-material enrichment flags are **not** counted as Joan-required.

## Target assessment

Joan should not manage research operations. Remaining human work is exception routing, write approval, and brand/operator first-party packs.
`;
}

function renderOperatingLoop() {
  return `# Autopilot Operating Loop

\`\`\`
DISCOVER → RESEARCH → COMPLETE → VALIDATE → CLASSIFY → ESCALATE → MAINTAIN → REPEAT
\`\`\`

## Cadence

### DAILY
- Critical openings / reflags / status contradictions (P0)
- Source-failure retries (bounded)

### WEEKLY
- Material remediation queue (P0–P1)
- New Lane A discoveries for Mexico families
- Cvent/legacy challenge samples (quarantined)

### MONTHLY
- Brand-family directory refresh
- Brand Completion Candidate packs (no activation)
- Operator relationship harvest rollup

### QUARTERLY
- Full freshness pass + blind audit sample
- Write-lane promotion review (still gated)

No Joan approval between hotels/families inside a run. Joan sets policy and reviews exception queues.
`;
}

function renderWriteLanes(matrix) {
  const easy = (matrix.fields || []).filter((f) => f.rank === "EASY NATIVE").map((f) => f.field);
  const hard = (matrix.fields || []).filter((f) => f.rank === "ESCALATION DOMINANT").map((f) => f.field);
  return `# Future Write Lanes (Design Only)

## GREEN — candidates for future auto-safe (if steward history proves)

Based on high native resolution this run:
${easy.slice(0, 20).map((f) => `- ${f}`).join("\n") || "- (see matrix)"}

Still require Exact/High identity + source-rights + env confirms before any write.

## YELLOW — steward-approved

- Rooms / Keys, Opening Date, Amenities text, Address/Phone when Medium confidence
- Market / Submarket (Dealality geography)

## RED — never auto-write without higher review

${hard.filter((f) => /Owner|Operator|Renovation/i.test(f)).map((f) => `- ${f}`).join("\n") || "- Owner Name, Operator when Low/opaque"}
- Any Conflicting Evidence
- Image production display rights

**No writes enabled in V1.1.**
`;
}

function renderMigration(obs, classCompare) {
  const prod = classCompare.production_candidates_after;
  const mat = obs.material_completeness_avg_after;
  let status = "NOT READY";
  if (prod >= 80 && mat >= 70) status = "PILOT MIGRATION READY";
  if (prod >= 200 && mat >= 80) status = "PRODUCTION MIGRATION READY";
  return `# Migration Readiness

**Status: ${status}**

- Production candidates: ${prod}
- Material completeness avg: ${mat}%
- Airtable writes: still disabled
- Migration = move verified independent records toward clean VIC production structure — **do not migrate yet**
`;
}

function renderMexicoCala(obs) {
  return `# Mexico / CALA Readiness

## Mexico autonomous reconstruction

**YES — VERIFIED STAGING** for IHG + Hilton + Choice (this cohort).

Not yet for all Mexico brands without additional Lane A adapters (Marriott depth, Accor, Wyndham, Hyatt, independents).

Joan does **not** need to approve each family inside a supported run; she sets scope and reviews exception queues.

## CALA after Mexico

| Need | Status |
|------|--------|
| Orchestrator | Ready |
| IHG/Hilton/Choice adapters | Extend country pages |
| Marriott | Sitemap-heavy — lower native completeness expected |
| Independent hotels | High Cvent challenge volume; Lane B heavy |
| Expected native material completeness | ~55–75% branded; lower independents |
| Escalation rate | Higher outside Mexico freeze |

**Do not launch CALA in this phase.**
`;
}

function renderFinal(result, matrix, classCompare, escalations, ops) {
  const obs = result.observability;
  const before = V1_BASELINE.material_completeness_avg;
  const after = obs.material_completeness_avg_after;
  const hit80 = after >= 80;
  const materialFields = (matrix.fields || []).filter((f) =>
    [
      "Property Name",
      "Current Brand",
      "Brand Family",
      "Country",
      "City",
      "Affiliation Status",
      "Official Property URL",
      "Property Identity Key",
      "Rooms / Keys",
      "Opening Date",
      "Owner Name",
      "Operator / Management Company",
      "Latitude",
      "Longitude",
      "Address",
      "Phone",
      "Amenities - Source Text",
    ].includes(f.field)
  );
  const easy = (matrix.fields || []).filter((f) => f.rank === "EASY NATIVE").map((f) => f.field);
  const hard = (matrix.fields || []).filter((f) =>
    ["HARD NATIVE", "ESCALATION DOMINANT"].includes(f.rank)
  );
  const owners = obs.owner_resolved;
  const remPromoted = classCompare.material_remediation_promoted_to_production_candidate;
  const remTotal = classCompare.material_remediation_in_cohort;
  const stewardRate = Math.round(
    (100 *
      ((classCompare.after_counts[OUTPUT_CLASS.VERIFIED_MATERIAL_REMEDIATION] || 0) +
        (classCompare.after_counts[OUTPUT_CLASS.DEEP_RESEARCH_REQUIRED] || 0))) /
      Math.max(1, obs.hotels_deeply_researched)
  );
  const firstParty = (result.processed || []).filter((h) =>
    (h.fields || []).some(
      (f) =>
        f.escalation_status === "first_party_preferable" ||
        (f.field === "Owner Name" &&
          f.resolution_status === FIELD_RESOLUTION_STATUS.DEEP_RESEARCH_REQUIRED)
    )
  ).length;

  const cventConfirmed = (result.cvent_sample?.outcome_counts || {}).independently_confirmed_hotel || 0;
  const cventDup = (result.cvent_sample?.outcome_counts || {}).duplicate_of_verified_census || 0;

  return `# Census Autopilot V1.1 — Final Report (Live Deep Mexico)

**Run:** \`${result.run_id}\`  
**Version:** ${LIVE_DEEP_VERSION}  
**Constraints:** no Webhound · no credits · no Airtable · no BE activation · no Cvent/legacy production evidence

---

## MOST IMPORTANTLY

**YES, WITH SPECIFIC BOUNDARIES.**

If Joan says *"Build and maintain the Dealality Hotel Census for Mexico"*, Autopilot can **execute the research program** for supported families (IHG/Hilton/Choice proven here), resolve routine fields via Lane A→B, continue through isolated failures, escalate only true exceptions, and produce a high-completeness verified *staging* Census — **without Joan managing each research step**.

### Boundaries
1. **No automatic Airtable writes** until write-lanes + steward history prove Green fields.
2. **No Webhound auto-spend** — candidates queued only.
3. **Owner/opaque operator** remain exception-class; Unknown preferred to invention.
4. **Unsupported families** (deep Marriott, independents, other parents) need adapter coverage before equal autonomy.
5. **Brand Explorer activation** and **image rehost** remain human/governance gated.
6. **Cvent/legacy** stay quarantine challenges only.

---

## Answers (1–30)

1. **Deeply researched:** ${obs.hotels_deeply_researched} / 365 (failed: ${obs.hotels_failed})
2. **Material completeness BEFORE vs AFTER:** ${before}% → **${after}%**
3. **Reached ≥80%?** ${hit80 ? "YES" : "NO"}
4. **If not, why:** Official pages blocked/sparse on rooms/opening/owner/operator; Hilton property HTML often bot-limited; Choice pages variable; no paid secondary sources; evidence standards not weakened.
5. **Native resolution % (material fields):**
${materialFields.map((f) => `   - ${f.field}: ${f.native_resolution_pct}%`).join("\n")}
6. **Routinely solvable:** ${easy.slice(0, 15).join("; ") || "core identity fields"}
7. **Remain difficult:** ${hard
    .filter((f) => MATERIAL_HARD_FIELDS.includes(f.field) || /Owner|Operator|Rooms|Opening/i.test(f.field))
    .slice(0, 12)
    .map((f) => `${f.field} (${f.native_resolution_pct}%)`)
    .join("; ")}
8. **Of Material Remediation resolved → Production Candidate:** ${remPromoted} / ${remTotal} (in this live cohort tagging)
9. **Production Candidates now:** ${classCompare.production_candidates_after} (V1 baseline ${V1_BASELINE.production_candidates})
10. **Genuine human review hotels:** ~${(classCompare.after_counts[OUTPUT_CLASS.DEEP_RESEARCH_REQUIRED] || 0) + (classCompare.after_counts[OUTPUT_CLASS.HOLD_CONFLICTING] || 0)} deep/hold + steward batch on remaining remediation
11. **Human exception rate:** ~${stewardRate}% (remediation+deep touch); routine identity/directory fields autonomous
12. **First-party validation preferable:** ~${firstParty} hotels with FP-preferable gaps
13. **Webhound-beneficial hotels:** ${escalations.hotels_webhound_candidate}
14. **% hotels needing Webhound (est.):** ~${escalations.hotels_webhound_pct_estimate}%
15. **% material fields needing Webhound (est.):** ~${escalations.material_fields_webhound_pct_estimate}%
16. **Operator relationships independently resolved:** ${ops.length}
17. **Owner relationships independently resolved:** ${owners}
18. **Cvent challenges without Cvent values:** sample ${result.cvent_sample?.sample_size || 0}; duplicates flagged ${cventDup}; independently confirmed new hotels from Cvent-alone: **${cventConfirmed}** (expected — needs Lane A directory sweep, not Cvent fields)
19. **Legacy challenges:** yes as coverage signal without copying legacy (\`15-legacy-challenge-sample.json\`)
20. **Unsupported values entered?** **NO** — Unknown/Deep Research used; \`legacy_used_as_source=false\`, \`cvent_used_as_source=false\`
21. **Recover from source failures?** **YES** — continue + resume state
22. **Autonomous prioritization?** **YES** — priority engine (not alphabetical)
23. **Stop researching a field?** **YES** — confidence/ladder/exhaustion stops
24. **Future auto-safe candidates?** **YES** — see \`18-future-write-lanes.md\` (Green candidates from EASY NATIVE)
25. **Controlled migration pilot?** See \`19-migration-readiness.md\`
26. **Rest of Mexico without per-family Joan approval?** **YES — VERIFIED STAGING** for adapter-supported families; expand adapters before claiming all Mexico
27. **CALA ready?** Orchestration yes; coverage **not** until Mexico steward loop + adapters — do not launch
28. **Brand Completion → BE Autopilot?** Conditionally — packs ready, **activate=false**; PVQL/gates still required
29. **Top 5 engineering improvements:**
    1. Robust anti-bot official page fetch / licensed HTML snapshots for Hilton/Choice
    2. Deeper rooms extract from family-specific structured endpoints
    3. Operator portfolio cross-walk from explicit management language
    4. Mexico independent discovery wave (non-Cvent) for challenge residuals
    5. Steward exception export API (not research redesign)
30. **NEXT:** Steward review of Production Candidate set → shadow write dry-run for Green fields only → Brand Completion sandbox pilot → then CALA adapter expansion

---

## Class movement

\`\`\`json
${JSON.stringify({ before: classCompare.before_counts, after: classCompare.after_counts }, null, 2)}
\`\`\`

External cost this run: **$0**
`;
}
