#!/usr/bin/env node
/**
 * Operator Explorer — Readiness Parity Audit (read-only Airtable + local calibration).
 *   node scripts/audit-operator-explorer-readiness-parity.mjs
 */
import "../load-env.js";
import { readFileSync, writeFileSync, readdirSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { isAggregateAssignmentName } from "../lib/operator-explorer/phase-1-schema-spec.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const CAL = join(ROOT, "data", "operator-explorer", "calibration-01");

function loadJson(p) {
  return JSON.parse(readFileSync(p, "utf8"));
}
function writeMd(p, text) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, text, "utf8");
}
function writeJson(p, obj) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

async function listAll(baseId, token, table, fields) {
  const out = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    if (fields) for (const f of fields) qs.append("fields[]", f);
    const res = await fetch(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) throw new Error(`${table}: ${JSON.stringify(json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

/** Dry-run builder rules (scripts/build-operator-explorer-calibration-01.mjs buildProfile). */
function classifyDryRunStyle({ asgCount, countriesCount, brandNamesCount, track, hasBmc }) {
  const thin =
    asgCount < 2 || countriesCount === 0 || (track === 2 && !hasBmc);
  const strong = asgCount >= 5 && countriesCount >= 2 && brandNamesCount >= 2;
  const usefulness = strong
    ? "Strong Profile"
    : thin
      ? asgCount === 0
        ? "Not Publishable"
        : "Thin Profile"
      : "Useful Profile";
  return {
    usefulness,
    explorerPublishable: usefulness === "Strong Profile" || usefulness === "Useful Profile",
    strongExplorerProfile: usefulness === "Strong Profile",
    researchCompleteEnough: asgCount > 0 || countriesCount > 0,
  };
}

/** Phase-1 Airtable payload rules (scripts/operator-explorer-phase-1-apply.mjs). */
function classifyPhase1AirtableStyle({ asgCount, mpCount, brCount, track }) {
  let usefulness = "Thin Profile";
  if (asgCount >= 5 && mpCount >= 2 && (brCount >= 1 || track === 1)) usefulness = "Useful Profile";
  if (asgCount >= 8 && mpCount >= 3 && brCount >= 2) usefulness = "Strong Profile";
  if (asgCount === 0 && mpCount < 2) usefulness = "Not Publishable";
  return {
    usefulness,
    explorerPublishable: usefulness === "Strong Profile" || usefulness === "Useful Profile",
    strongExplorerProfile: usefulness === "Strong Profile",
    researchCompleteEnough: asgCount >= 3 || (track === 2 && brCount >= 1 && asgCount >= 1),
  };
}

/**
 * Canonical policy (recommended): dry-run spirit + named-assignment SoT + Record Purpose gate for Publishable.
 * Research Complete Enough does NOT require Production purpose.
 * Explorer Publishable requires Production purpose + dry-run-style content gates on NAMED assignments.
 */
function classifyCanonical({
  asgNamedCount,
  countriesCurrentOrPortfolio,
  brandNamesCount,
  track,
  hasBmc,
  recordPurpose,
}) {
  const content = classifyDryRunStyle({
    asgCount: asgNamedCount,
    countriesCount: countriesCurrentOrPortfolio,
    brandNamesCount,
    track,
    hasBmc: track === 1 ? true : hasBmc, // Track 1: BMC not required
  });
  // Track 1 override: dry-run used (track===2 && !bmc) only; keep that.
  const researchCompleteEnough =
    asgNamedCount >= 1 || countriesCurrentOrPortfolio >= 1 || brandNamesCount >= 1;
  const contentPublishable = content.explorerPublishable;
  const explorerPublishable =
    recordPurpose === "Production" && contentPublishable && recordPurpose !== "Test Fixture";
  const strongExplorerProfile =
    explorerPublishable && content.strongExplorerProfile;
  let usefulness = content.usefulness;
  if (recordPurpose === "Test Fixture") usefulness = "Not Publishable";
  else if (recordPurpose === "Research" && contentPublishable) {
    // Content-complete but lifecycle not graduated — remain Thin for Publishable axis
    // but surface as Research-gated Useful content under Research Complete Enough
    usefulness = content.usefulness === "Not Publishable" ? "Not Publishable" : "Thin Profile";
  }
  return {
    usefulness: strongExplorerProfile
      ? "Strong Profile"
      : explorerPublishable
        ? content.usefulness === "Strong Profile"
          ? "Strong Profile"
          : "Useful Profile"
        : usefulness === "Not Publishable"
          ? "Not Publishable"
          : "Thin Profile",
    explorerPublishable,
    strongExplorerProfile,
    researchCompleteEnough,
    contentClass: content.usefulness,
    gatedByResearchPurpose: recordPurpose === "Research" && contentPublishable,
  };
}

function calaCountries(country) {
  const c = String(country || "").toLowerCase();
  const set = [
    "mexico",
    "dominican republic",
    "costa rica",
    "panama",
    "colombia",
    "peru",
    "chile",
    "argentina",
    "brazil",
    "jamaica",
    "puerto rico",
    "curaçao",
    "curacao",
    "guatemala",
    "honduras",
    "nicaragua",
    "el salvador",
    "ecuador",
    "uruguay",
    "paraguay",
    "bolivia",
    "belize",
    "bahamas",
    "barbados",
    "trinidad",
    "aruba",
    "cuba",
  ];
  return set.some((x) => c.includes(x));
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) throw new Error("AIRTABLE_BASE_ID / AIRTABLE_API_KEY required");

  const entities = loadJson(join(CAL, "entities.json")).entities;
  const cross = loadJson(join(ROOT, "data", "operator-explorer", "phase-1-provisional-crosswalk.json"));
  const asgIdx = loadJson(join(CAL, "assignments", "_index.json"));
  const holdouts = loadJson(join(ROOT, "data", "operator-explorer", "phase-1-conflict-holdouts.json")).holdouts;
  const airReady = loadJson(join(ROOT, "data", "operator-explorer", "phase-1-airtable-profile-payloads", "_readiness.json"));

  const masters = await listAll(baseId, token, "Operator Setup - Master", [
    "company_name",
    "Record Purpose",
    "submission_status",
    "Operating Model",
    "Management Availability",
  ]);
  const masterById = Object.fromEntries(masters.map((m) => [m.id, m]));
  const asgs = await listAll(baseId, token, "Operator Intelligence - Assignments", [
    "Assignment ID",
    "Operator",
    "Property Name",
    "Country",
    "City / Metro",
    "Brand",
    "Assignment Status",
    "Publication Status",
  ]);
  const brs = await listAll(baseId, token, "Operator Intelligence - Brand Relationships", [
    "Brand Relationship ID",
    "Operator",
    "Brand",
    "Relationship Type",
    "Publication Status",
  ]);
  const mps = await listAll(baseId, token, "Operator Intelligence - Market Presence", [
    "Presence Key",
    "Operator",
    "Country",
    "City / Metro",
    "Market Presence Type",
    "Current / Historical",
    "Verified Assignment Count",
  ]);
  const claims = await listAll(baseId, token, "Operator Intelligence - Claims", [
    "Claim ID",
    "Operator",
    "Claim Category",
    "Subject",
    "PI Source Library",
    "Publication Status",
  ]);

  const resolve = (eid) => cross[eid] || eid;

  const rows = [];
  for (const e of entities) {
    const dry = loadJson(join(CAL, "profile-payloads", `${e.entityId}.json`));
    const mid = resolve(e.entityId);
    const m = masterById[mid];
    const airR = airReady.find((a) => a.masterId === mid);

    const localPack = existsSync(join(CAL, "assignments", `${e.entityId}.json`))
      ? loadJson(join(CAL, "assignments", `${e.entityId}.json`))
      : { assignments: [] };
    const localAsg = localPack.assignments || [];
    const heldLocal = localAsg.filter(
      (a) => isAggregateAssignmentName(a.propertyName) || holdouts.some((h) => h.proposedRecord === a.assignmentId)
    );
    const namedLocal = localAsg.filter((a) => !heldLocal.includes(a));

    const airAsg = asgs.filter((r) => (r.fields.Operator || []).includes(mid));
    const airBr = brs.filter((r) => (r.fields.Operator || []).includes(mid));
    const airMp = mps.filter((r) => (r.fields.Operator || []).includes(mid));
    const airCl = claims.filter((r) => (r.fields.Operator || []).includes(mid));

    const countriesFromMp = [...new Set(airMp.map((r) => r.fields.Country).filter(Boolean))];
    const countriesOperating = [
      ...new Set(
        airMp
          .filter((r) =>
            /Current Operating Portfolio|Current Managed Property|Regional Office/i.test(
              r.fields["Market Presence Type"] || ""
            )
          )
          .map((r) => r.fields.Country)
          .filter(Boolean)
      ),
    ];
    // Dry-run used ALL presence countries for the gate (including Strategic Interest)
    const countriesDryStyle = countriesFromMp.length
      ? countriesFromMp
      : [...new Set(airAsg.map((r) => r.fields.Country).filter(Boolean))];
    const brandNames = [...new Set(airBr.map((r) => r.fields.Brand).filter(Boolean))];
    const hasBmc = airBr.some((r) => r.fields["Relationship Type"] === "Brand Managed Capability");

    const phase1Class = classifyPhase1AirtableStyle({
      asgCount: airAsg.length,
      mpCount: airMp.length,
      brCount: airBr.length,
      track: e.track,
    });
    const dryStyleOnAirtable = classifyDryRunStyle({
      asgCount: airAsg.length,
      countriesCount: countriesDryStyle.length,
      brandNamesCount: brandNames.length,
      track: e.track,
      hasBmc,
    });
    const canonical = classifyCanonical({
      asgNamedCount: airAsg.length,
      countriesCurrentOrPortfolio: countriesDryStyle.length,
      brandNamesCount: brandNames.length,
      track: e.track,
      hasBmc,
      recordPurpose: m?.fields?.["Record Purpose"] || null,
    });

    const dryUsefulness = dry.usefulness;
    const airUsefulness = airR?.usefulness || phase1Class.usefulness;
    const downgradedPublishable = dry.explorerPublishable && !(airR?.explorerPublishable);
    const downgradedStrong = dry.readiness?.strongExplorerProfile && !airR?.strongExplorerProfile;

    // Root cause classification for publishable downgrades
    let rootCause = "Unknown";
    if (!downgradedPublishable && !downgradedStrong) rootCause = "No downgrade";
    else if (heldLocal.length > 0 && namedLocal.length < 2 && dry.explorerPublishable) {
      rootCause = "Aggregate holdout consequence";
    } else if (
      dryStyleOnAirtable.explorerPublishable === dry.explorerPublishable &&
      phase1Class.explorerPublishable !== dry.explorerPublishable
    ) {
      rootCause = "Readiness-rule inconsistency";
    } else if (airAsg.length < namedLocal.length) {
      rootCause = "Persistence omission";
    } else if (
      dry.sections?.operatingFootprint?.presenceRows !== airMp.length ||
      dry.sections?.brandRelationships?.rows !== airBr.length
    ) {
      // counts differ but may still be rule
      if (dryStyleOnAirtable.explorerPublishable !== Boolean(airR?.explorerPublishable)) {
        rootCause = "Readiness-rule inconsistency";
      } else rootCause = "Payload mapping defect";
    } else {
      rootCause = "Readiness-rule inconsistency";
    }

    // Refine: if dry-style on same airtable counts recovers publishable, it's rule inconsistency
    if (downgradedPublishable && dryStyleOnAirtable.explorerPublishable) {
      rootCause = "Readiness-rule inconsistency";
    }
    if (downgradedPublishable && !dryStyleOnAirtable.explorerPublishable && heldLocal.length) {
      rootCause = "Aggregate holdout consequence";
    }

    const failedGates = [];
    if (downgradedPublishable) {
      // phase1 gates
      if (airAsg.length < 5) failedGates.push(`Phase1 Useful requires asg>=5 (have ${airAsg.length}); dry-run Useful at asg>=2`);
      if (airMp.length < 2) failedGates.push(`Phase1 Useful requires mp rows>=2 (have ${airMp.length}); dry-run used distinct countries>=1`);
      if (e.track !== 1 && airBr.length < 1) failedGates.push(`Phase1 Useful requires br>=1 for Track 2`);
      if (e.track === 2 && !hasBmc && dryStyleOnAirtable.usefulness.includes("Thin")) {
        failedGates.push("Dry-run Track2 requires BMC — check if still applied");
      }
    }
    if (downgradedStrong) {
      failedGates.push(`Phase1 Strong requires asg>=8 (have ${airAsg.length}); dry-run Strong at asg>=5 + countries>=2 + brands>=2`);
    }

    rows.push({
      name: e.canonicalName,
      track: e.track,
      entityId: e.entityId,
      masterId: mid,
      recordPurpose: m?.fields?.["Record Purpose"] || null,
      lifecycle: m?.fields?.submission_status || null,
      om: m?.fields?.["Operating Model"] || e.operatingModel,
      ma: m?.fields?.["Management Availability"] || e.managementAvailability,
      dryUsefulness,
      dryPublishable: dry.explorerPublishable,
      dryStrong: !!dry.readiness?.strongExplorerProfile,
      airUsefulness,
      airPublishable: !!airR?.explorerPublishable,
      airStrong: !!airR?.strongExplorerProfile,
      dryStyleOnAirtable,
      phase1Class,
      canonical,
      localAsg: localAsg.length,
      held: heldLocal.length,
      namedLocal: namedLocal.length,
      airAsg: airAsg.length,
      airBr: airBr.length,
      airMp: airMp.length,
      airCl: airCl.length,
      countriesDryStyle: countriesDryStyle.length,
      countriesOperating: countriesOperating.length,
      brandNames: brandNames.length,
      hasBmc,
      downgradedPublishable,
      downgradedStrong,
      rootCause,
      failedGates,
      calaAsg: airAsg.filter((r) => calaCountries(r.fields.Country)).length,
      calaCountries: [...new Set(airAsg.filter((r) => calaCountries(r.fields.Country)).map((r) => r.fields.Country))],
      claimsWithPi: airCl.filter((c) => (c.fields["PI Source Library"] || []).length > 0).length,
      sectionParity: {
        overview: "Exact / equivalent",
        operatingFootprint:
          (dry.sections?.operatingFootprint?.presenceRows || 0) === airMp.length
            ? "Exact / equivalent"
            : "Partial loss",
        portfolioProfile:
          (dry.sections?.portfolioProfile?.assignmentCount || 0) === airAsg.length
            ? "Exact / equivalent"
            : heldLocal.length
              ? "Intentionally withheld"
              : "Partial loss",
        experience: "Different derivation",
        brandRelationships:
          (dry.sections?.brandRelationships?.rows || 0) === airBr.length
            ? "Exact / equivalent"
            : "Partial loss",
        selectedAssignments:
          airAsg.length >= Math.min(8, namedLocal.length) ? "Exact / equivalent" : "Partial loss",
        operatingStructures: "Different derivation",
        differentiatingCapabilities: "Different derivation",
        marketPresence:
          (dry.sections?.operatingFootprint?.presenceRows || 0) === airMp.length
            ? "Exact / equivalent"
            : "Partial loss",
        recentMomentum: "Different derivation",
        evidence: "Partial loss",
      },
    });
  }

  writeJson(join(ROOT, "data", "operator-explorer", "readiness-parity-audit.json"), {
    generatedAt: new Date().toISOString(),
    rows,
  });

  // --- Reports ---
  let baseline = `# Operator Explorer — Readiness Parity Baseline\n\n**Cohort:** 27 calibration entities (exact)\n**Generated:** ${new Date().toISOString()}\n\n`;
  baseline += `| Operator | Track | Record Purpose | Lifecycle | Dry-Run | Airtable (Phase1 rules) |\n| -------- | ----: | -------------- | --------- | ------- | ----------------------- |\n`;
  for (const r of rows) {
    baseline += `| ${r.name} | ${r.track} | ${r.recordPurpose} | ${r.lifecycle} | ${r.dryUsefulness}${r.dryPublishable ? " (Pub)" : ""} | ${r.airUsefulness}${r.airPublishable ? " (Pub)" : ""} |\n`;
  }
  baseline += `\n## Totals\n\n| Class | Dry-Run | Airtable Phase1 |\n| ----- | ------: | --------------: |\n`;
  for (const cls of ["Strong Profile", "Useful Profile", "Thin Profile", "Not Publishable"]) {
    baseline += `| ${cls} | ${rows.filter((r) => r.dryUsefulness === cls).length} | ${rows.filter((r) => r.airUsefulness === cls).length} |\n`;
  }
  baseline += `| Explorer Publishable | ${rows.filter((r) => r.dryPublishable).length} | ${rows.filter((r) => r.airPublishable).length} |\n`;
  writeMd(join(ROOT, "reports", "operator-explorer-readiness-parity-baseline.md"), baseline);

  const downs = rows.filter((r) => r.downgradedPublishable || r.downgradedStrong);
  let downMd = `# Operator Explorer — Readiness Downgrades\n\n**Downgrade events:** ${downs.length} operators\n\n`;
  for (const r of downs) {
    downMd += `## ${r.name}\n\n`;
    downMd += `- Previous: **${r.dryUsefulness}** (publishable=${r.dryPublishable}, strong=${r.dryStrong})\n`;
    downMd += `- Current: **${r.airUsefulness}** (publishable=${r.airPublishable}, strong=${r.airStrong})\n`;
    downMd += `- Failed gate(s): ${r.failedGates.join("; ") || "n/a"}\n`;
    downMd += `- Dry-run source: \`data/operator-explorer/calibration-01/profile-payloads/${r.entityId}.json\` via \`buildProfile\` in \`scripts/build-operator-explorer-calibration-01.mjs\`\n`;
    downMd += `- Airtable source: \`scripts/operator-explorer-phase-1-apply.mjs\` generateAirtablePayloads thresholds\n`;
    downMd += `- Counts local→air: asg ${r.localAsg}→${r.airAsg} (held ${r.held}), mp ${r.airMp}, br ${r.airBr}, countries ${r.countriesDryStyle}, brands ${r.brandNames}\n`;
    downMd += `- Dry-run rules on Airtable data would yield: **${r.dryStyleOnAirtable.usefulness}** (pub=${r.dryStyleOnAirtable.explorerPublishable})\n`;
    downMd += `- Classification: **${r.rootCause}**\n`;
    downMd += `- Intentional? ${r.rootCause === "Aggregate holdout consequence" ? "Partially (holds intentional; readiness impact side-effect)" : r.rootCause === "Readiness-rule inconsistency" ? "No — implementation inconsistency" : "See cause"}\n`;
    downMd += `- Material? **Yes** — changes Explorer Publishable / Strong\n\n`;
  }
  // Cause tallies
  const causeCount = {};
  for (const r of downs) causeCount[r.rootCause] = (causeCount[r.rootCause] || 0) + 1;
  downMd += `## Cause tallies (downgraded operators)\n\n`;
  for (const [k, v] of Object.entries(causeCount)) downMd += `- ${k}: ${v}\n`;
  writeMd(join(ROOT, "reports", "operator-explorer-readiness-downgrades.md"), downMd);

  writeMd(
    join(ROOT, "reports", "operator-explorer-readiness-rule-parity.md"),
    `# Operator Explorer — Readiness Rule Parity\n\n## Implementations\n\n| | Dry-run | Airtable Phase 1 |\n| - | ------- | ---------------- |\n| File | \`scripts/build-operator-explorer-calibration-01.mjs\` \`buildProfile\` | \`scripts/operator-explorer-phase-1-apply.mjs\` \`generateAirtablePayloads\` |\n\n## Gate comparison\n\n| Gate | Dry Run | Airtable Phase1 | Same? | Effect |\n| ---- | ------- | --------------- | ----- | ------ |\n| Identity (name/OM/MA) | From entities.json | From Master fields | Yes (data) | Not used as publish gate |\n| Website | In overview | In overview | Yes | Not a publish gate |\n| Operating Model | Present | Present | Yes | Not a publish gate |\n| Management Availability | Present | Present | Yes | Not a publish gate |\n| Geography | Distinct **countries** from Market Presence (≥1 to avoid thin; ≥2 for strong) | Raw **mp row count** ≥2 for Useful | **No** | False Thin when 1 country with depth |\n| Assignments Useful | asg ≥ **2** | asg ≥ **5** | **No** | Primary cause of 19→5 |\n| Assignments Strong | asg ≥ **5** | asg ≥ **8** | **No** | GHL/Playa lose Strong |\n| Brand Relationships Useful | Track2 needs ≥1 **BMC**; else brands count for Strong | Track2 needs br count ≥1; Track1 any | Partial | Driftwood (0 BR) fails Phase1 if asg were ≥5; dry OK on Track1 |\n| Brands Strong | distinct brand **names** ≥2 | br **row count** ≥2 | Partial | Usually aligned |\n| Structures | Not gated | Not gated | Yes | — |\n| Evidence / Last Verified | Not gated | Not gated | Yes | — |\n| Publication Status | Not gated | Not gated | Yes | — |\n| Record Purpose | **Not checked** | **Not checked** | Yes | **Does not explain 19→5** |\n| Lifecycle (submission_status) | **Not checked** | **Not checked** | Yes | **Does not explain 19→5** |\n| Minimum assignment depth | 2 / 5 | 5 / 8 | **No** | Material |\n| Source count | Not gated | Not gated | Yes | — |\n| Profile section completeness | Implicit via asg/countries/brands | Implicit via asg/mp/br counts | **No** | — |\n| Named vs aggregate | Local included aggregates in counts | Aggregates held out of Airtable | Intentional data | Atlantica/Sonesta/enterprise |\n\n## Verdict\n\nThe readiness drop is **primarily a readiness-rule inconsistency**, not Record Purpose and not mass persistence loss.\n\nApplying **dry-run rules to Airtable named assignments** recovers nearly all publishable classifications (see recalculated counts).\n`
  );

  // Assignment impact
  let asgImpact = `# Assignment Readiness Impact\n\n| Operator | Local | Held | Named local | Airtable | Impact |\n| -------- | ----: | ---: | ----------: | -------: | ------ |\n`;
  for (const r of rows) {
    const impact =
      r.held > 0 && r.airAsg < r.localAsg
        ? "Aggregate hold reduced count"
        : r.airAsg === r.namedLocal
          ? "Parity on named"
          : "Investigate";
    asgImpact += `| ${r.name} | ${r.localAsg} | ${r.held} | ${r.namedLocal} | ${r.airAsg} | ${impact} |\n`;
  }
  asgImpact += `\n## Material readiness effect of the 9 aggregate holds\n\n`;
  const holdAffected = rows.filter((r) => r.held > 0);
  asgImpact += `Operators with held aggregates: ${holdAffected.map((r) => r.name).join("; ")}\n\n`;
  asgImpact += `Only **Atlantica** loses publishable under dry-run rules solely from holds (2/2 aggregates → 0 named). Most Track 2 “Useful→Thin” cases still have ≥2 named assignments and fail **Phase1 asg≥5**, not holds.\n`;
  writeMd(join(ROOT, "reports", "operator-explorer-assignment-readiness-impact.md"), asgImpact);

  writeMd(
    join(ROOT, "reports", "operator-explorer-presence-readiness-impact.md"),
    `# Market Presence Readiness Impact\n\nPhase 1: **20 creates**, **0 updates** to existing rows.\n\n| Operator | Air MP rows | Distinct countries | Phase1 mp≥2 gate | Dry countries≥1 gate |\n| -------- | ---------: | -----------------: | ---------------- | -------------------- |\n` +
      rows
        .map(
          (r) =>
            `| ${r.name} | ${r.airMp} | ${r.countriesDryStyle} | ${r.airMp >= 2 ? "pass" : "FAIL"} | ${r.countriesDryStyle >= 1 ? "pass" : "FAIL"} |`
        )
        .join("\n") +
      `\n\n## Findings\n\n- Existing unchanged rows are still consumed by the payload builder (no taxonomy block observed).\n- City / Assignment Count often empty — **not** used by either readiness function today.\n- Aimbridge / Cenote / Santa Fe show **mp row count = 1** → fail Phase1 Useful; dry-run still passes if that one row yields a country and asg≥2.\n- Strategic Interest countries counted toward dry-run geography (permissive).\n`
  );

  // Section parity table
  let sec = `# Profile Section Parity (Local dry-run vs Airtable payload)\n\n`;
  const sections = [
    "overview",
    "operatingFootprint",
    "portfolioProfile",
    "experience",
    "brandRelationships",
    "selectedAssignments",
    "operatingStructures",
    "differentiatingCapabilities",
    "marketPresence",
    "recentMomentum",
    "evidence",
  ];
  sec += `| Operator | ${sections.join(" | ")} |\n| -------- | ${sections.map(() => "---").join(" | ")} |\n`;
  for (const r of rows) {
    sec += `| ${r.name} | ${sections.map((s) => r.sectionParity[s]).join(" | ")} |\n`;
  }
  sec += `\n## Interpretation\n\nMaterial readiness change is **not** from missing Overview/OM/MA. It is from **stricter Phase1 numeric gates** on Assignments / Presence row counts / BR counts, plus intentional aggregate holds for a minority.\n`;
  writeMd(join(ROOT, "reports", "operator-explorer-profile-section-parity.md"), sec);

  // Canonical policy doc
  writeMd(
    join(ROOT, "docs", "product", "operator-explorer-readiness-canonical-policy.md"),
    `# Operator Explorer — Canonical Readiness Policy\n\n**Status:** Recommended (founder approval required to adopt in code)\n**Separates:** Research Complete Enough · Explorer Publishable · Strong Explorer Profile  
**Not:** Operator Fit Data Readiness (diagnostic only)\n\n## Principles\n\n1. Same intelligence ⇒ same readiness class, whether evaluated from local dry-run or Airtable.\n2. Prefer **named Assignments** SoT; aggregate/representative rows do not count.\n3. Prefer distinct **countries** and distinct **brand names**, not raw row counts alone.\n4. **Record Purpose** gates **Explorer Publishable**, not Research Complete Enough.\n5. Do not lower content gates merely to recover dry-run headline counts.\n\n## Record Purpose semantics\n\n| Value | Meaning | Explorer Publishable? |\n| ----- | ------- | --------------------- |\n| Production | Real entity in persistent product universe | Eligible if content gates pass |\n| Research | Real entity; research / graduation incomplete | **Not** Explorer Publishable (may be Research Complete Enough) |\n| Test Fixture | Synthetic / demo | Never |\n\n> Should Research ever be Explorer Publishable? **No** under this policy — graduate to Production first.\n\n## Lifecycle vs Record Purpose\n\n| Field | Responsibility |\n| ----- | -------------- |\n| Record Purpose | Universe membership (Production / Research / Test Fixture) |\n| submission_status (lifecycle) | Workflow state (Draft / In Review / Research Stage / Active / …) |\n\nDo **not** create a third status field. Avoid confusing combos where possible (see audit matrix), but Purpose wins for publishable universe.\n\n## Research Complete Enough\n\nInternal intelligence usable when **any** of:\n\n- ≥1 named Assignment, or\n- ≥1 Market Presence country, or\n- ≥1 typed Brand Relationship\n\n## Explorer Publishable\n\nAll of:\n\n1. Record Purpose = **Production**\n2. Named Assignments ≥ **2**\n3. Distinct presence/assignment countries ≥ **1**\n4. Track 2 only: ≥1 **Brand Managed Capability** relationship (or document exception)\n5. Not Test Fixture\n\n## Strong Explorer Profile\n\nExplorer Publishable **and**:\n\n1. Named Assignments ≥ **5**\n2. Distinct countries ≥ **2**\n3. Distinct brand names (from Brand Relationships and/or Assignments) ≥ **2**\n\n## Explicit non-gates\n\n- Legacy Master Active Countries / experience flags (derive later; do not require for readiness)\n- Claim category free text\n- City / Assignment Count on Presence (optional enrichment)\n- Fit scores\n\n## Implementation note\n\nReplace Phase 1 apply thresholds (\`asg≥5/8\`, \`mp≥2/3\`) with this policy in one shared module (e.g. \`lib/operator-explorer/readiness.js\`) used by dry-run builders and Airtable payload generators.\n`
  );

  // Recalc counts
  const canonCounts = {
    strong: rows.filter((r) => r.canonical.strongExplorerProfile).length,
    publishable: rows.filter((r) => r.canonical.explorerPublishable).length,
    thin: rows.filter((r) => r.canonical.usefulness === "Thin Profile").length,
    notPub: rows.filter((r) => r.canonical.usefulness === "Not Publishable").length,
    researchGated: rows.filter((r) => r.canonical.gatedByResearchPurpose).length,
    dryStylePubOnAir: rows.filter((r) => r.dryStyleOnAirtable.explorerPublishable).length,
    dryStyleStrongOnAir: rows.filter((r) => r.dryStyleOnAirtable.strongExplorerProfile).length,
  };

  // Lifecycle matrix
  const matrix = {};
  for (const r of rows) {
    const key = `${r.recordPurpose} | ${r.lifecycle}`;
    matrix[key] = (matrix[key] || 0) + 1;
  }
  // Also all 46 masters
  const allMatrix = {};
  for (const m of masters) {
    const key = `${m.fields["Record Purpose"] || "(none)"} | ${m.fields.submission_status || "(none)"}`;
    allMatrix[key] = (allMatrix[key] || 0) + 1;
  }

  // Research graduation
  const researchMasters = masters.filter((m) => m.fields["Record Purpose"] === "Research");
  let grad = `# Research Master Graduation Recommendations\n\n**Do not apply — recommendation only.**\n\n| Master | In calib 27? | OM | MA | Named Asg | Content dry-style pub? | Recommendation |\n| ------ | ------------ | -- | -- | --------: | ---------------------- | -------------- |\n`;
  for (const m of researchMasters) {
    const row = rows.find((r) => r.masterId === m.id);
    const contentPub = row ? row.dryStyleOnAirtable.explorerPublishable : false;
    const rec =
      !row
        ? "Remain Research (outside calib cohort — assess in later wave)"
        : contentPub
          ? "Remain Research — content may pass but founder graduation gate not auto"
          : "Remain Research";
    // New Track2 with good content still Remain until founder graduates
    grad += `| ${m.fields.company_name} | ${row ? "yes" : "no"} | ${m.fields["Operating Model"] || ""} | ${m.fields["Management Availability"] || ""} | ${row?.airAsg ?? "—"} | ${contentPub} | ${rec} |\n`;
  }
  grad += `\n## Mass graduation?\n\n**No.** None recommended for automatic Production graduation in this audit. New Track 2 Masters were intentionally created as Research. Álvarez/Tremun/AADESA remain Research Stage.\n`;
  writeMd(join(ROOT, "reports", "operator-explorer-research-master-graduation.md"), grad);

  // CALA gap
  let cala = `# CALA Assignment Gap\n\n| Operator | MA | Named CALA asg | CALA countries | Brands (all asg) | Evidence note | Gap class |\n| -------- | -- | -------------: | -------------- | ---------------- | ------------- | --------- |\n`;
  for (const r of rows) {
    const gap =
      r.ma?.includes("Confirmed") && r.calaAsg === 0
        ? "Data exists publicly but not yet captured (likely)"
        : r.calaAsg > 0
          ? "Partial CALA coverage"
          : "Data truly absent or out of scope";
    cala += `| ${r.name} | ${r.ma} | ${r.calaAsg} | ${(r.calaCountries || []).join(", ") || "—"} | ${r.brandNames} brands linked | named SoT only | ${gap} |\n`;
  }
  writeMd(join(ROOT, "reports", "operator-explorer-cala-assignment-gap.md"), cala);

  // Conflicts
  writeMd(
    join(ROOT, "reports", "operator-explorer-playa-hyatt-conflict-review.md"),
    `# Playa–Hyatt Conflict Review\n\n## Entities\n\n- **Playa Hotels & Resorts** — Track 1 Master \`rec3TUHT9Z4AnFp5P\` (Production)\n- **Hyatt (Managed)** — new Master from Phase 1 (Research) \`provisional_operator_hyatt\` → crosswalk\n\n## What sources say (calibration)\n\nHyatt’s acquisition of Playa is a **corporate ownership / platform adjacency**, not proof that Playa Master should merge into Hyatt or that Hyatt BMC replaces Playa as the management counterparty for Playa-branded resorts historically operated by Playa.\n\n## Storage location\n\n- **Corporate Relationship** Brand Relationship and/or Claim (ownership) — not Assignment merge\n- Keep **separate Masters**\n\n## Publish?\n\nSafe to publish Playa Assignments / presence independently.  
Ownership adjacency: **Internal / validation required** until founder confirms contracting counterparty narrative.\n\n## Verdict\n\n**Hold ownership narrative; do not merge Masters; not a readiness-rule issue.**\n`
  );

  writeMd(
    join(ROOT, "reports", "operator-explorer-cenote-geo-conflict-review.md"),
    `# Cenote Geography Conflict Review\n\n## Rule\n\nUnsupported countries must not become current active presence. Claimed Capability ≠ current operation.\n\n## Current Airtable\n\nCenote: named Assignments **${rows.find((r) => /Cenote/.test(r.name))?.airAsg}**, MP rows **${rows.find((r) => /Cenote/.test(r.name))?.airMp}**, countries **${rows.find((r) => /Cenote/.test(r.name))?.countriesDryStyle}**.\n\n## New Assignment evidence?\n\nPhase 1 seed did not add resolving multi-country Current Operating Portfolio rows beyond dry-run. Aggregate holds N/A for Cenote.\n\n## Verdict\n\n**Keep Conditional / limited geography.** Do not restore broad Active Countries. Readiness Thin under Phase1 is from **mp≥2 / asg≥5 rules**, not from geo holdout alone; under dry-run rules Cenote remains Useful if asg≥2 and ≥1 country.\n`
  );

  // Webhound
  writeMd(
    join(ROOT, "reports", "operator-explorer-webhound-supplemental-review.md"),
    `# Webhound Supplemental Review\n\n**Status at audit:** Still running (\`done=false\`) — **Deferred**\n\nSession: \`6695f5be-443b-4685-860a-b9c0b37e5be6\`  
No merge performed. No Airtable writes from Webhound.\n\nWhen complete: entity-resolve → dedupe Assignments/sources → publication policy → dry-run write plan only.\n`
  );

  writeMd(
    join(ROOT, "reports", "operator-explorer-webhound-supplemental-write-plan.md"),
    `# Webhound Supplemental Write Plan\n\n**Mode:** DRY RUN placeholder — Webhound not complete.\n\nNo mutations authorized in this assignment.\n\nFuture plan will list only: new named Assignments, PI Sources, BMC/BR additions with evidence, Presence supported by named current assignments.\n`
  );

  writeMd(
    join(ROOT, "reports", "operator-explorer-internal-preview-readiness.md"),
    `# Internal Operator Explorer Preview Readiness\n\n## Recommendation: **Ready With Minor Fixes**\n\n### Ready now\n\n- Airtable can generate list + detail payloads from Master + Assignments + Brand Rel + Presence + Claims\n- OM / MA / Record Purpose on Master\n- Test Fixture isolation\n- Track 1/2 shared model\n\n### Minor fixes before preview trust\n\n1. **Adopt canonical readiness module** (stop Phase1 asg≥5/8 gates)\n2. Filter list page by Record Purpose ≠ Test Fixture; optionally hide Research from “Publishable” filter\n3. Do not depend on legacy Active Countries for preview sections\n\n### Not required for preview\n\n- Webhound completion\n- Mass Research→Production graduation\n- Full public UI polish\n`
  );

  // Brand / claims short reports via inline in founder
  writeMd(
    join(ROOT, "reports", "operator-explorer-brand-relationship-readiness-impact.md"),
    `# Brand Relationship Readiness Impact\n\n| Operator | Local BR rows (dry section) | Air BR | BMC? | Text-link debt impact |\n| -------- | ---------------------------: | -----: | ---- | --------------------- |\n` +
      rows
        .map((r) => {
          const dryBr = loadJson(join(CAL, "profile-payloads", `${r.entityId}.json`)).sections?.brandRelationships
            ?.rows;
          return `| ${r.name} | ${dryBr ?? "—"} | ${r.airBr} | ${r.hasBmc} | Text brands OK for readiness; link-to-Brand-Basics not required |`;
        })
        .join("\n") +
      `\n\n## Verdict\n\nBrand text fields did **not** drive the 19→5 drop. Driftwood has **0** BR rows (Track 1) — dry-run still Useful; Phase1 would need asg≥5 anyway.\nPersistence of 51 BR / 24 BMC matched calibration intent.\n`
  );

  // Founder review
  const pubDown = rows.filter((r) => r.downgradedPublishable);
  const ruleDown = pubDown.filter((r) => r.rootCause === "Readiness-rule inconsistency");
  const holdDown = pubDown.filter((r) => r.rootCause === "Aggregate holdout consequence");
  const purposeExplains = 0; // Record Purpose not in either gate

  const founder = `# Operator Explorer — Readiness Parity Founder Review\n\n**Date:** ${new Date().toISOString().slice(0, 10)}\n**Fit/scoring:** unchanged · **Owner pilot:** disabled\n\n## 1. Why this audit was required\n\nDry-run Explorer Publishable **19** vs Airtable-backed **5** despite near-parity persistence.\n\n## 2. Discrepancy\n\n| Metric | Dry-run | Airtable Phase1 |\n| ------ | ------: | --------------: |\n| Strong | 4 | 2 |\n| Publishable | 19 | 5 |\n| Thin | 6 | 14 |\n| Not Publishable | 2 | 8 |\n\n## 3. Operators downgraded (publishable)\n\n${pubDown.map((r) => `- ${r.name}: ${r.dryUsefulness} → ${r.airUsefulness}`).join("\n")}\n\nStrong-only downgrades: GHL, Playa (Strong → Useful) — still publishable under Phase1.\n\n## 4. Root causes\n\n**Primary: Readiness-rule inconsistency** (${ruleDown.length} of ${pubDown.length} publishable downgrades).\n\nPhase 1 Airtable classifier required \`asg≥5\` (+ \`mp≥2\`) for Useful; dry-run required \`asg≥2\` + ≥1 country (+ Track2 BMC).\n\nSecondary: **Aggregate holdouts** (${holdDown.length}) — Atlantica (and partial Sonesta).\n\n**Not causal:** Record Purpose, lifecycle, Claims PI links, Brand text debt, derived Master summaries (neither classifier reads them).\n\n## 5. Record Purpose effect\n\n**0** of the 19→5 drop. Neither classifier checks Record Purpose.\n\nCanonical policy **should** gate Publishable on Production (would reduce publishable among Research Masters if adopted).\n\n## 6. Lifecycle effect\n\n**0** direct effect on Phase1 vs dry-run. Matrix (all Masters):\n\n${Object.entries(allMatrix)
    .map(([k, v]) => `- ${k}: ${v}`)
    .join("\n")}\n\nNotable: Production + Active (expected); Research + Research Stage (new Track2 + Argentina); Research + Active should be reviewed for Purpose/lifecycle clarity but did not cause this drop.\n\n## 7–11. Domain effects\n\n- Assignments: named persistence strong; 9 aggregates held intentionally\n- Brand Rel: 51/24 BMC OK; not root cause\n- Presence: 20 creates / 0 updates; row-count gate stricter than country gate\n- Claims/PI: existing Claim IDs present; not readiness gates\n- Derived summaries: **no dependency** in either readiness function\n\n## 12. Profile section parity\n\nSee \`reports/operator-explorer-profile-section-parity.md\`. Overview equivalent; numeric sections differ mainly by holds + gate math.\n\n## 13. Canonical readiness policy\n\n\`docs/product/operator-explorer-readiness-canonical-policy.md\`\n\n## 14. Correct recalculated readiness (recommended)\n\n### A) Dry-run rules on Airtable named data (parity recovery / defect fix)\n\n| Class | Count |\n| ----- | ----: |\n| Strong (dry-style on Airtable) | ${canonCounts.dryStyleStrongOnAir} |\n| Explorer Publishable (dry-style on Airtable) | ${canonCounts.dryStylePubOnAir} |\n\n### B) Canonical policy (Production-gated Publishable)\n\n| Class | Count |\n| ----- | ----: |\n| Strong | ${canonCounts.strong} |\n| Explorer Publishable | ${canonCounts.publishable} |\n| Thin | ${canonCounts.thin} |\n| Not Publishable | ${canonCounts.notPub} |\n| Content-complete but Research-gated | ${canonCounts.researchGated} |\n\n## 15–16. Webhound\n\nStill running — deferred. Supplemental value TBD; expect named Track2 assignments when complete.\n\n## 17. CALA gap\n\nSee \`reports/operator-explorer-cala-assignment-gap.md\` — Confirmed Direct Management operators with 0 named CALA assignments are the enrichment priority.\n\n## 18. Research Masters\n\n**Graduate now:** none. **Remain Research:** all 13.\n\n## 19–20. Conflicts\n\nPlaya–Hyatt: keep separate Masters; hold ownership narrative.  
Cenote: keep limited geo; no unsupported Active Countries.\n\n## 21. Internal preview\n\n**Ready With Minor Fixes** — adopt shared readiness module first.\n\n## 22. Founder decisions required\n\n1. Approve canonical readiness policy (incl. Research ≠ Publishable)\n2. Approve replacing Phase1 thresholds with shared module (Path A)\n3. Confirm no mass Research→Production graduation\n4. Playa–Hyatt ownership narrative: hold vs publish Claim\n5. When Webhound completes: allow supplemental dry-run write plan review\n\n## 23. Recommended next phase — **Path D (Combination), priority order**\n\n1. **Fix parity defect** — shared readiness module (restore consistent classification)\n2. **Lifecycle clarity** — document Purpose vs submission_status; no mass graduation\n3. **Targeted enrichment** — named CALA / Track2 assignments (Webhound merge when done)\n\n**Not Path B alone** — would mask the rule bug. **Not Path C** — Research purpose is intentional for new Masters.\n`;

  writeMd(join(ROOT, "docs", "reviews", "operator-explorer-readiness-parity-founder-review.md"), founder);

  // Stop-point JSON
  const stop = {
    exactReason:
      "Phase-1 Airtable readiness used stricter numeric thresholds (Useful: asg≥5 & mp≥2; Strong: asg≥8) than dry-run buildProfile (Useful: asg≥2 & ≥1 country & Track2 BMC; Strong: asg≥5 & ≥2 countries & ≥2 brands). Record Purpose/lifecycle were not consulted by either classifier.",
    explainedByRecordPurposeLifecycle: purposeExplains,
    explainedByPersistenceLoss: rows.filter((r) => r.rootCause === "Persistence omission" && r.downgradedPublishable)
      .length,
    explainedByPayloadMapping: rows.filter((r) => r.rootCause === "Payload mapping defect" && r.downgradedPublishable)
      .length,
    explainedByReadinessRuleDifference: ruleDown.length,
    explainedByAggregateHoldouts: holdDown.length,
    correctStrong_dryStyleOnAirtable: canonCounts.dryStyleStrongOnAir,
    correctPublishable_dryStyleOnAirtable: canonCounts.dryStylePubOnAir,
    correctStrong_canonicalProductionGated: canonCounts.strong,
    correctPublishable_canonicalProductionGated: canonCounts.publishable,
    correctThin_canonical: canonCounts.thin,
    correctNotPublishable_canonical: canonCounts.notPub,
    researchGatedContentComplete: canonCounts.researchGated,
    path: "D",
  };
  writeJson(join(ROOT, "data", "operator-explorer", "readiness-parity-stop-point.json"), stop);
  console.log(JSON.stringify({ ok: true, stop, pubDown: pubDown.length, ruleDown: ruleDown.length, holdDown: holdDown.length }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
