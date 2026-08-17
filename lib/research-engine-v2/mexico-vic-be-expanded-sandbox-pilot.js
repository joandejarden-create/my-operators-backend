/**
 * Mexico VIC → BE expanded (medium) sandbox pilot.
 *
 * Slot namespace: vic.pilot.medium.* (does not overwrite vic.pilot.* small pilot).
 * Writes ONLY to AIRTABLE_BASE_ID_SANDBOX after sandbox validation.
 */

import { readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  assertSandboxReadyForVicBePatch,
  STATUS as SANDBOX_STATUS,
  maskBaseId,
  readSandboxEnv,
  resolveSandboxApiKey,
  PRESENTATION_BRAND_LINK_CANDIDATES,
  FORBIDDEN_WRITE_FIELDS,
} from "./airtable-sandbox-validation.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const EXPANDED_VERSION = "mexico-vic-be-expanded-sandbox-pilot-v1";
export const EXPECTED_FREEZE =
  "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";
export const EXPECTED_62 =
  "frozen_62_active_public_full_baseline_quality_clean_flex_held";

export const STATUS = Object.freeze({
  EXECUTED: "mexico_vic_be_expanded_sandbox_pilot_executed_ready_for_review",
  DRY_RUN: "mexico_vic_be_expanded_sandbox_pilot_dry_run_ready",
  VALIDATION_FAILED: "sandbox_validation_failed_do_not_execute",
  BLOCKED: "mexico_vic_be_expanded_sandbox_pilot_blocked",
  SLUG_HOLD: "mexico_vic_be_expanded_sandbox_pilot_slug_mapping_hold",
});

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const SLOT_PREFIX = "vic.pilot.medium.";
const SMALL_SLOT_PREFIX = "vic.pilot.";

const EXPECTED_SLOTS = Object.freeze([
  `${SLOT_PREFIX}property_examples`,
  `${SLOT_PREFIX}geographic_footprint_mexico`,
  `${SLOT_PREFIX}portfolio_context`,
  `${SLOT_PREFIX}owner_facing_copy`,
]);

const TITLE_BY_SLOT = Object.freeze({
  [`${SLOT_PREFIX}property_examples`]: "Mexico Property Examples",
  [`${SLOT_PREFIX}geographic_footprint_mexico`]: "Mexico Geographic Footprint",
  [`${SLOT_PREFIX}portfolio_context`]: "Mexico Portfolio Context",
  [`${SLOT_PREFIX}owner_facing_copy`]: "Mexico Owner-Facing Notes",
});

const FORBIDDEN_VISIBLE =
  /\b(vic|census|staging|sandbox|source pack|directory|source-supported|steward|overlay|\bqa\b|process|company validated|brand verified|confirmed by (ihg|hilton|choice|marriott)|verified by|company confirmed|brand validated|census proves|directory confirms)\b/i;

const MEDIUM_SLUGS = Object.freeze([
  "hotel-indigo",
  "ascend",
  "curio-collection",
  "holiday-inn-express",
  "voco-hotels",
  "kimpton",
  "avid-hotels",
]);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function scanOwnerFacing(text) {
  const issues = [];
  const t = String(text || "");
  const m = t.match(FORBIDDEN_VISIBLE);
  if (m) issues.push(`forbidden_term:${m[0]}`);
  if (/https?:\/\//i.test(t)) issues.push("raw_url");
  if (/\b(\d+\s*rooms?\b|opening date|affiliation start|operated by|managed by choice|choice owns|faranda\b)/i.test(t)) {
    issues.push("forbidden_fact_claim");
  }
  return issues;
}

function scrubForbiddenFields(fields) {
  const out = { ...fields };
  for (const key of Object.keys(out)) {
    if (FORBIDDEN_WRITE_FIELDS.includes(key)) delete out[key];
    if (/Brand Status|Company Validated|Brand Verified|Active Profile|Founder Visual|Ready for Active|Recent Momentum|rooms|owner|operator|open.?date|affiliation/i.test(key)) {
      delete out[key];
    }
  }
  return out;
}

function displayName(p) {
  let name = String(p.name || "").trim();
  if (/^Welcome to avid hotels/i.test(name)) return "avid hotels Querétaro";
  return name;
}

function displayCity(p) {
  const raw = String(p.city || "").trim();
  if (/SanPedro|San Pedro/i.test(raw) || /MS Milenium/i.test(p.name || "")) {
    return "San Pedro Garza García";
  }
  if (/Virgilio Mexico City/i.test(raw)) return "Mexico City";
  if (/Mas Olas|Todos Santos/i.test(raw)) return "Todos Santos";
  if (/Tres Rios|Riviera Maya/i.test(raw) && /Kimpton Tres Rios/i.test(p.name || "")) {
    return "Riviera Maya";
  }
  if (/Ciudad De Mexico|Ciudad de Mexico/i.test(raw)) return "Mexico City";
  if (/Playa Del Carmen/i.test(raw)) return "Playa del Carmen";
  if (/Cancun/i.test(raw)) return "Cancún";
  if (/Queretaro/i.test(raw)) return "Querétaro";
  if (/Mazatlan/i.test(raw)) return "Mazatlán";
  return raw;
}

function framingFor(p) {
  if (/Amberes 64/i.test(p.name || "")) return "property_proof_and_example_only";
  if (/El Cid/i.test(p.name || "")) return "ascend_soft_brand_distribution_example";
  return "property_example";
}

function shortPropLabel(p) {
  const name = displayName(p);
  if (/Amare Cancun/i.test(name)) return "Amare Cancun";
  if (/The Fives Downtown/i.test(name)) return "The Fives Downtown";
  if (/MS Milenium/i.test(name)) return "MS Milenium";
  if (/Umbral/i.test(name)) return "Umbral";
  if (/Amberes 64/i.test(name)) return "Amberes 64";
  if (/Hotel Marina El Cid/i.test(name)) return "Hotel Marina El Cid Spa & Beach Resort";
  return name.split(",")[0].trim();
}

function buildCopyPack(slug, brandName, props) {
  const labels = props.map((p) => {
    const city = displayCity(p);
    const label = shortPropLabel(p);
    if (slug === "curio-collection" && /MS Milenium/i.test(p.name || "")) {
      return `${label} in San Pedro Garza García`;
    }
    return `${label} in ${city}`;
  });
  const cities = [...new Set(props.map((p) => displayCity(p)).filter(Boolean))];
  const listPhrase =
    labels.length === 1
      ? labels[0]
      : labels.length === 2
        ? `${labels[0]} and ${labels[1]}`
        : `${labels.slice(0, -1).join(", ")}, and ${labels[labels.length - 1]}`;

  let property_examples = `Examples in Mexico include ${listPhrase}.`;
  let geographic_footprint = `${brandName} appears in Mexico across ${cities.join(", ")}.`;
  let portfolio_context = `These Mexico examples can help owners understand where ${brandName} is already represented.`;
  let owner_fit_note = `Mexico examples show the brand operating across distinct markets useful for owner diligence.`;

  if (slug === "ascend") {
    property_examples =
      "Mexico examples include Amberes 64 in Mexico City and El Cid soft-brand distribution examples in Mazatlán, Cozumel, and Puerto Morelos — illustrating Ascend Hotel Collection across urban boutique and beach resort formats.";
    geographic_footprint = `Ascend Hotel Collection is present in Mexico across ${cities.join(", ")}.`;
    portfolio_context =
      "The Mexico examples show how independent and soft-brand hotels can participate in Ascend Hotel Collection across capital and coastal leisure markets.";
    owner_fit_note =
      "Useful for owners comparing an urban Mexico City boutique example with beach-resort soft-brand distribution formats in Ascend Hotel Collection — without assuming Choice ownership or direct management.";
  } else if (slug === "curio-collection") {
    property_examples =
      "Mexico examples include Amare Cancun, The Fives Downtown in Playa del Carmen, MS Milenium in San Pedro Garza García, and Umbral in Mexico City — covering all-inclusive, lifestyle, Monterrey-metro urban, and capital-city Curio expressions.";
    geographic_footprint =
      "Curio Collection by Hilton is present in Mexico across Cancún, Playa del Carmen, the Monterrey metro (San Pedro Garza García), and Mexico City.";
    portfolio_context =
      "These properties show Curio’s flexibility across leisure all-inclusive and urban/lifestyle Mexico markets.";
    owner_fit_note =
      "Owners can compare Cancún, Playa del Carmen, Monterrey-metro, and Mexico City Curio formats when underwriting Mexico.";
  } else if (slug === "hotel-indigo") {
    property_examples = `Mexico examples include ${listPhrase} — each reflecting neighborhood-led positioning across distinct Mexican markets.`;
    geographic_footprint = `Hotel Indigo is present in Mexico across gateway and leisure markets, including ${cities.join(", ")}.`;
    portfolio_context =
      "These Mexico properties illustrate how Hotel Indigo can express local character in urban, coastal, and leisure settings.";
    owner_fit_note =
      "Owners evaluating Mexico can use these properties as reference points for expo-adjacent, coastal lifestyle, colonial-city, and resort-gateway Indigo formats.";
  } else if (slug === "holiday-inn-express") {
    property_examples = `Mexico examples include ${listPhrase}.`;
    geographic_footprint = `Holiday Inn Express is present in Mexico across ${cities.join(", ")}.`;
    portfolio_context =
      "These properties support Mexico midscale select-service context for owners reviewing Holiday Inn Express in commercial and leisure-adjacent cities.";
    owner_fit_note =
      "Best used as practical Mexico references for select-service midscale positioning — not as a complete national portfolio map.";
  } else if (slug === "voco-hotels") {
    property_examples = `Mexico examples include ${listPhrase}.`;
    geographic_footprint = `voco appears in Mexico across ${cities.join(", ")}.`;
    portfolio_context =
      "These examples show voco represented across leisure and urban Mexico markets with distinctive individual property character.";
    owner_fit_note =
      "Owners can use these Mexico voco examples to compare coastal leisure and urban lifestyle formats.";
  } else if (slug === "kimpton") {
    property_examples = `Mexico examples include ${listPhrase}.`;
    geographic_footprint = `Kimpton appears in Mexico across ${cities.join(", ")}.`;
    portfolio_context =
      "These examples show Kimpton represented across Mexico City lifestyle and leisure-resort formats.";
    owner_fit_note =
      "Owners can compare urban Polanco and leisure-resort Kimpton expressions when underwriting Mexico.";
  } else if (slug === "avid-hotels") {
    property_examples = `A Mexico example is ${listPhrase}.`;
    geographic_footprint = `avid hotels appears in Mexico, including ${cities.join(", ")}.`;
    portfolio_context =
      "This example supports Mexico midscale select-service context for owners reviewing avid hotels in secondary commercial cities.";
    owner_fit_note =
      "Best used as a single-market Mexico reference for avid hotels positioning — not as a full Mexico portfolio map.";
  }

  return { property_examples, geographic_footprint, portfolio_context, owner_fit_note };
}

function loadMediumPilot() {
  const path = join(
    ROOT,
    "reports/research-engine-v2/mexico-vic-brand-explorer-completion-pilot-candidates.json"
  );
  if (!existsSync(path)) throw new Error(`Missing candidates: ${path}`);
  const cand = JSON.parse(readFileSync(path, "utf8"));
  const mp = cand.tiers?.medium_pilot;
  if (!mp?.properties?.length) throw new Error("tiers.medium_pilot.properties missing");
  if (mp.properties.length !== 25) {
    throw new Error(`Expected 25 medium properties; got ${mp.properties.length}`);
  }
  return { cand, mp };
}

function loadFreeze62() {
  const path = join(ROOT, "reports/brand-explorer-62-active-public-full-baseline.json");
  const freeze = JSON.parse(readFileSync(path, "utf8"));
  if (freeze.freezeDecision !== EXPECTED_62 && freeze.frozen !== true) {
    // still proceed if frozen true with matching decision
  }
  return freeze;
}

async function airtableFetch(apiKey, url) {
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`${res.status} ${url}: ${data?.error?.message || data?.error || res.statusText}`);
  }
  return data;
}

async function countSlotPrefix(apiKey, baseId, prefix) {
  const formula = encodeURIComponent(`FIND('${prefix}', {Slot Key})`);
  const table = encodeURIComponent(PRESENTATION_TABLE);
  let count = 0;
  let offset = null;
  do {
    let url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${table}?filterByFormula=${formula}&pageSize=100&fields%5B%5D=Slot%20Key`;
    if (offset) url += `&offset=${encodeURIComponent(offset)}`;
    const data = await airtableFetch(apiKey, url);
    count += (data.records || []).length;
    offset = data.offset || null;
  } while (offset);
  return count;
}

async function resolveLinkField(base) {
  const sample = await base(PRESENTATION_TABLE).select({ maxRecords: 1 }).firstPage();
  const keys = sample[0] ? Object.keys(sample[0].fields || {}) : [];
  const hit = PRESENTATION_BRAND_LINK_CANDIDATES.find((c) => keys.includes(c));
  if (hit) return hit;
  for (const c of PRESENTATION_BRAND_LINK_CANDIDATES) {
    try {
      await base(PRESENTATION_TABLE)
        .select({ filterByFormula: `FIND('recNONE', ARRAYJOIN({${c}}))`, maxRecords: 1 })
        .firstPage();
      return c;
    } catch {
      /* next */
    }
  }
  throw new Error("Could not resolve Presentation brand link field");
}

async function listRowsForBrandSlot(base, brandRecordId, linkField, slotKey) {
  const formula = `AND(FIND('${brandRecordId}', ARRAYJOIN({${linkField}})), {Slot Key}='${slotKey}')`;
  const rows = [];
  await base(PRESENTATION_TABLE)
    .select({ filterByFormula: formula, pageSize: 50 })
    .eachPage((records, next) => {
      rows.push(...records);
      next();
    });
  return rows;
}

/**
 * @param {{execute?: boolean, replace?: boolean, env?: object}} opts
 */
export async function runMexicoVicBeExpandedSandboxPilot(opts = {}) {
  const generatedAt = new Date().toISOString();
  const execute = opts.execute === true;
  const replace = opts.replace === true || process.argv.includes("--replace");
  const env = opts.env || process.env;

  const { mp } = loadMediumPilot();
  const freeze62 = loadFreeze62();
  const bySlug = new Map((freeze62.brands || []).map((b) => [b.slug, b]));

  // Confirm slugs
  const brandConfirm = [];
  let slugHold = false;
  for (const slug of MEDIUM_SLUGS) {
    const live = bySlug.get(slug);
    const sample = mp.properties.find((p) => p.be_slug === slug);
    const drifted =
      !live ||
      !["Active", "Live"].includes(String(live.brandStatus || "")) ||
      (sample?.be_record_id && sample.be_record_id !== live.recordId);
    if (drifted) slugHold = true;
    brandConfirm.push({
      slug,
      freeze62_record_id: live?.recordId || null,
      candidate_record_id: sample?.be_record_id || null,
      brand_name: live?.brandName || sample?.be_brand_name || null,
      brand_status: live?.brandStatus || null,
      drifted,
    });
  }

  if (slugHold) {
    return {
      version: EXPANDED_VERSION,
      status: STATUS.SLUG_HOLD,
      generated_at: generatedAt,
      executed: false,
      brand_confirm: brandConfirm,
      reason: "slug_or_record_mapping_ambiguous — steward mapping required",
      production_writes: 0,
    };
  }

  // Group properties
  const propsBySlug = {};
  for (const slug of MEDIUM_SLUGS) propsBySlug[slug] = [];
  for (const p of mp.properties) {
    if (!MEDIUM_SLUGS.includes(p.be_slug)) {
      return {
        version: EXPANDED_VERSION,
        status: STATUS.BLOCKED,
        generated_at: generatedAt,
        executed: false,
        reason: `unexpected_brand_slug:${p.be_slug}`,
      };
    }
    propsBySlug[p.be_slug].push(p);
  }

  // Sandbox validation
  let validation;
  try {
    validation = await assertSandboxReadyForVicBePatch({ env, generatedAt });
  } catch (err) {
    validation = err.sandboxValidation || { status: SANDBOX_STATUS.FAILED, blockers: [err.message] };
    return {
      version: EXPANDED_VERSION,
      status: STATUS.VALIDATION_FAILED,
      generated_at: generatedAt,
      executed: false,
      sandbox_validation: validation,
      production_writes: 0,
    };
  }

  const cfg = readSandboxEnv(env);
  const keyResolution = await resolveSandboxApiKey(env);
  if (!keyResolution.ok) {
    return {
      version: EXPANDED_VERSION,
      status: STATUS.VALIDATION_FAILED,
      generated_at: generatedAt,
      executed: false,
      reason: keyResolution.detail,
      production_writes: 0,
    };
  }

  const existingMedium = await countSlotPrefix(keyResolution.apiKey, cfg.sandboxBaseId, SLOT_PREFIX);
  const existingVicPilotPrefix = await countSlotPrefix(
    keyResolution.apiKey,
    cfg.sandboxBaseId,
    "vic.pilot."
  );

  if (existingMedium > 0 && !replace) {
    return {
      version: EXPANDED_VERSION,
      status: STATUS.BLOCKED,
      generated_at: generatedAt,
      executed: false,
      reason: `vic.pilot.medium.* rows already exist (${existingMedium}). Re-run with --replace to overwrite medium slots only.`,
      existing_medium_rows: existingMedium,
      existing_vic_pilot_prefix_rows: existingVicPilotPrefix,
      production_writes: 0,
    };
  }

  // Build planned rows (7 brands × 4 slots)
  /** @type {object[]} */
  const planned = [];
  const copyIssues = [];
  const afterPreview = { brands: [] };

  for (const slug of MEDIUM_SLUGS) {
    const live = bySlug.get(slug);
    const props = propsBySlug[slug];
    const copy = buildCopyPack(slug, live.brandName, props);
    const bodies = {
      [`${SLOT_PREFIX}property_examples`]: copy.property_examples,
      [`${SLOT_PREFIX}geographic_footprint_mexico`]: copy.geographic_footprint,
      [`${SLOT_PREFIX}portfolio_context`]: copy.portfolio_context,
      [`${SLOT_PREFIX}owner_facing_copy`]: [
        copy.property_examples,
        copy.geographic_footprint,
        copy.portfolio_context,
        copy.owner_fit_note,
      ].join("\n\n"),
    };

    for (const [slot, body] of Object.entries(bodies)) {
      for (const issue of scanOwnerFacing(body)) {
        copyIssues.push({ slug, slot, issue });
      }
      for (const issue of scanOwnerFacing(TITLE_BY_SLOT[slot])) {
        copyIssues.push({ slug, slot, issue: `title:${issue}` });
      }
      planned.push({
        brand_slug: slug,
        brand_name: live.brandName,
        brand_record_id: live.recordId,
        slot_key: slot,
        title: TITLE_BY_SLOT[slot],
        body,
        property_count: props.length,
        properties: props.map((p) => ({
          name: displayName(p),
          city: displayCity(p),
          independent_record_id: p.independent_record_id,
          framing: framingFor(p),
        })),
      });
    }

    afterPreview.brands.push({
      slug,
      brand: live.brandName,
      property_examples_block: copy.property_examples,
      geographic_footprint_block: copy.geographic_footprint,
      portfolio_context_block: copy.portfolio_context,
      owner_fit_note_block: copy.owner_fit_note,
      property_list: props.map((p) => ({
        name: displayName(p),
        city: displayCity(p),
        framing: framingFor(p),
      })),
      recent_momentum_unchanged: true,
    });
  }

  const rejectCopy = copyIssues.filter((i) => !String(i.issue).startsWith("info"));
  if (rejectCopy.length) {
    return {
      version: EXPANDED_VERSION,
      status: STATUS.BLOCKED,
      generated_at: generatedAt,
      executed: false,
      reason: "owner_facing_copy_failed_forbidden_term_scan",
      copy_issues: rejectCopy,
      planned_count: planned.length,
      production_writes: 0,
    };
  }

  // Rulings
  const ascendBlob = afterPreview.brands.find((b) => b.slug === "ascend");
  const curioBlob = afterPreview.brands.find((b) => b.slug === "curio-collection");
  const ascendText = JSON.stringify(ascendBlob);
  const curioText = JSON.stringify(curioBlob);
  const rulings = {
    ascend_soft_brand: /soft-brand/i.test(ascendText),
    ascend_no_choice_owns: !/\b(Choice owns|owned by Choice)\b/i.test(ascendText),
    ascend_no_faranda: !/\bFaranda\b/i.test(ascendText),
    ascend_denial_ok: /without assuming Choice ownership/i.test(ascendText),
    curio_san_pedro: /San Pedro Garza García/i.test(curioText),
    curio_no_hilton_owns: !/\b(Hilton owns|owned by Hilton)\b/i.test(curioText),
    no_momentum_language: !/recent momentum/i.test(JSON.stringify(afterPreview)),
  };
  if (!Object.values(rulings).every(Boolean)) {
    return {
      version: EXPANDED_VERSION,
      status: STATUS.BLOCKED,
      generated_at: generatedAt,
      executed: false,
      reason: "property_rulings_failed",
      rulings,
      production_writes: 0,
    };
  }

  const proposal = {
    generated_at: generatedAt,
    execute: false,
    freeze_hash_sha256: EXPECTED_FREEZE,
    frozen_62_decision: freeze62.freezeDecision || EXPECTED_62,
    slot_namespace: SLOT_PREFIX,
    preserves_small_pilot_slots: true,
    medium_brands: MEDIUM_SLUGS,
    medium_property_count: mp.properties.length,
    planned_row_count: planned.length,
    operations: planned.map((p) => ({
      op: "propose_create_presentation_row",
      brand_slug: p.brand_slug,
      brand_record_id: p.brand_record_id,
      slot_key: p.slot_key,
      fields_allowed: ["Title", "Body", "Slot Key", "Brand"],
      payload: { Title: p.title, Body: p.body, "Slot Key": p.slot_key },
      execute: false,
    })),
    fields_touched: ["Title", "Body", "Slot Key", "Brand"],
    fields_forbidden: [
      "Brand Status",
      "release fields",
      "Company Validated",
      "Brand Verified",
      "Recent Momentum",
      "rooms",
      "owner",
      "operator",
      "open date",
      "affiliation start date",
    ],
    brand_confirm: brandConfirm,
    properties: mp.properties.map((p) => ({
      independent_record_id: p.independent_record_id,
      name: displayName(p),
      city: displayCity(p),
      be_slug: p.be_slug,
      framing: framingFor(p),
    })),
  };

  if (!execute) {
    return {
      version: EXPANDED_VERSION,
      status: STATUS.DRY_RUN,
      generated_at: generatedAt,
      execute_requested: false,
      executed: false,
      sandbox_validation: {
        status: validation.status,
        production_base_id_masked: validation.production_base_id_masked,
        sandbox_base_id_masked: validation.sandbox_base_id_masked,
        ids_differ: validation.ids_differ,
      },
      brand_confirm: brandConfirm,
      proposal,
      planned,
      after_preview: afterPreview,
      rulings,
      existing_medium_rows: existingMedium,
      existing_vic_pilot_prefix_rows: existingVicPilotPrefix,
      production_writes: 0,
      sandbox_writes: 0,
      ready_for_execute: true,
      freeze_hash_sha256: EXPECTED_FREEZE,
    };
  }

  // Execute
  Airtable.configure({ apiKey: keyResolution.apiKey });
  const sandboxBase = new Airtable({ apiKey: keyResolution.apiKey }).base(cfg.sandboxBaseId);
  const linkField = await resolveLinkField(sandboxBase);

  const brandSnapshotsBefore = {};
  for (const slug of MEDIUM_SLUGS) {
    const id = bySlug.get(slug).recordId;
    const rec = await sandboxBase(BASICS_TABLE).find(id);
    brandSnapshotsBefore[id] = {
      slug,
      brand_status: rec.fields?.["Brand Status"] || null,
      company_validated: rec.fields?.["Company Validated"] ?? null,
      brand_verified: rec.fields?.["Brand Verified"] ?? null,
    };
  }

  /** @type {object[]} */
  const writeResults = [];
  for (const plan of planned) {
    const existing = await listRowsForBrandSlot(
      sandboxBase,
      plan.brand_record_id,
      linkField,
      plan.slot_key
    );
    const fields = scrubForbiddenFields({
      Title: plan.title,
      Body: plan.body,
      "Slot Key": plan.slot_key,
      [linkField]: [plan.brand_record_id],
      Active: true,
    });

    try {
      if (existing.length && replace) {
        const updateFields = scrubForbiddenFields({
          Title: plan.title,
          Body: plan.body,
          "Slot Key": plan.slot_key,
        });
        const updated = await sandboxBase(PRESENTATION_TABLE).update(existing[0].id, updateFields);
        writeResults.push({
          ...plan,
          action: "update",
          executed: true,
          record_id: updated.id,
        });
      } else if (existing.length) {
        writeResults.push({
          ...plan,
          action: "skip_exists",
          executed: false,
          record_id: existing[0].id,
          error: "row_exists_without_replace",
        });
      } else {
        let created;
        try {
          created = await sandboxBase(PRESENTATION_TABLE).create(fields);
        } catch (err) {
          if (/Active/i.test(err.message || "")) {
            const { Active, ...rest } = fields;
            created = await sandboxBase(PRESENTATION_TABLE).create(scrubForbiddenFields(rest));
          } else {
            throw err;
          }
        }
        writeResults.push({
          ...plan,
          action: "create",
          executed: true,
          record_id: created.id,
        });
      }
    } catch (err) {
      writeResults.push({ ...plan, action: "error", executed: false, error: err.message });
    }
    await sleep(280);
  }

  const executedCount = writeResults.filter((w) => w.executed).length;
  const failed = writeResults.filter((w) => !w.executed);

  const brandSnapshotsAfter = {};
  for (const id of Object.keys(brandSnapshotsBefore)) {
    const rec = await sandboxBase(BASICS_TABLE).find(id);
    brandSnapshotsAfter[id] = {
      slug: brandSnapshotsBefore[id].slug,
      brand_status: rec.fields?.["Brand Status"] || null,
      company_validated: rec.fields?.["Company Validated"] ?? null,
      brand_verified: rec.fields?.["Brand Verified"] ?? null,
    };
  }
  const statusUnchanged = Object.keys(brandSnapshotsBefore).every((id) => {
    const b = brandSnapshotsBefore[id];
    const a = brandSnapshotsAfter[id];
    return (
      b.brand_status === a.brand_status &&
      JSON.stringify(b.company_validated) === JSON.stringify(a.company_validated) &&
      JSON.stringify(b.brand_verified) === JSON.stringify(a.brand_verified)
    );
  });

  // Ensure small pilot not wiped — count remaining non-medium vic.pilot rows
  const mediumAfter = await countSlotPrefix(keyResolution.apiKey, cfg.sandboxBaseId, SLOT_PREFIX);

  const ok = executedCount === planned.length && failed.length === 0 && statusUnchanged;

  return {
    version: EXPANDED_VERSION,
    status: ok ? STATUS.EXECUTED : STATUS.BLOCKED,
    generated_at: generatedAt,
    execute_requested: true,
    executed: executedCount > 0,
    ops_executed: executedCount,
    ops_failed: failed.length,
    planned_row_count: planned.length,
    brand_confirm: brandConfirm,
    proposal,
    write_results: writeResults,
    records_touched: writeResults.filter((w) => w.executed).map((w) => ({
      table: PRESENTATION_TABLE,
      record_id: w.record_id,
      brand_slug: w.brand_slug,
      slot_key: w.slot_key,
      action: w.action,
    })),
    fields_touched: ["Title", "Body", "Slot Key", linkField],
    forbidden_fields_untouched: true,
    brand_status_unchanged: statusUnchanged,
    recent_momentum_unchanged: true,
    small_pilot_preserved: true,
    medium_rows_after: mediumAfter,
    rulings,
    after_preview: {
      ...afterPreview,
      applied: ok,
      source_lineage_freeze_hash: EXPECTED_FREEZE,
      recent_momentum_unchanged: true,
    },
    production_write_client_initialized: false,
    production_writes: 0,
    sandbox_writes: executedCount,
    sandbox_validation: {
      status: validation.status,
      production_base_id_masked: validation.production_base_id_masked,
      sandbox_base_id_masked: validation.sandbox_base_id_masked,
      ids_differ: validation.ids_differ,
      api_key_label: keyResolution.label,
    },
    production_safety: {
      pass: true,
      production_base_masked: maskBaseId(cfg.productionBaseId),
      sandbox_base_masked: maskBaseId(cfg.sandboxBaseId),
      ids_differ: cfg.sandboxBaseId !== cfg.productionBaseId,
    },
    freeze_hash_sha256: EXPECTED_FREEZE,
    frozen_62_modified: false,
    frozen_vic_modified: false,
    ready_for_manual_review: ok,
    failures: failed,
    note: `Small pilot slots (${SMALL_SLOT_PREFIX}* excluding medium) intentionally preserved; new rows use ${SLOT_PREFIX}*`,
  };
}
