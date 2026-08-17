/**
 * Airtable sandbox isolation validation for Mexico VIC → Brand Explorer pilot.
 *
 * Read-only against sandbox (when configured). Never initializes a production
 * write client. Patch execution must call assertSandboxReadyForVicBePatch().
 */

export const SANDBOX_VALIDATION_VERSION = "airtable-sandbox-validation-v1";

export const STATUS = Object.freeze({
  READY: "airtable_sandbox_validated_ready_for_vic_be_patch",
  FAILED: "sandbox_validation_failed",
});

export const REQUIRED_TABLES = Object.freeze([
  "Brand Setup - Brand Basics",
  "Brand Setup - Brand Explorer Presentation",
]);

/** Brand Basics fields required for identity + safety gating. */
export const REQUIRED_BASICS_FIELDS = Object.freeze([
  "Brand Name",
  "Brand Status",
]);

/** Presentation fields used by BE pilot property/copy overlays. */
export const REQUIRED_PRESENTATION_FIELDS = Object.freeze(["Title", "Body"]);

/** Presentation brand-link field candidates (first match wins). */
export const PRESENTATION_BRAND_LINK_CANDIDATES = Object.freeze([
  "Brand",
  "Brand_Basic_ID",
  "Brand Setup - Brand Basics",
  "Brand Basics",
]);

/**
 * Pilot overlay field intents — may map to Presentation Title/Body rows rather
 * than discrete Airtable columns. Validation reports availability class.
 */
export const PILOT_FIELD_INTENTS = Object.freeze([
  "property_examples",
  "geographic_footprint_mexico",
  "portfolio_context",
  "property_proof",
  "owner_facing_copy",
]);

/** Forbidden write targets for VIC pilot (must exist for gating, never patched). */
export const FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Brand Status",
  "Company Validated",
  "Brand Verified",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Founder Visual Review Pass",
]);

export const TARGET_BRANDS = Object.freeze([
  {
    slug: "hotel-indigo",
    expectedRecordId: "recegXrqaPiSLGCIe",
    nameMatchers: [/Hotel Indigo/i],
  },
  {
    slug: "ascend",
    expectedRecordId: "reclkgOzvAcBheUSo",
    nameMatchers: [/^Ascend Hotel Collection$/i, /^Ascend Collection$/i, /Ascend/i],
  },
  {
    slug: "curio-collection",
    expectedRecordId: "receQkxgjlezsc1xg",
    nameMatchers: [/Curio Collection/i],
  },
  {
    slug: "holiday-inn-express",
    expectedRecordId: "recmGmiIqDtAsm01f",
    nameMatchers: [/Holiday Inn Express/i],
  },
]);

const SANDBOX_NAME_RE = /\b(sandbox|staging|test|dev)\b/i;

export function maskBaseId(id) {
  if (!id) return null;
  if (String(id).length < 10) return "(short)";
  return `${String(id).slice(0, 6)}…${String(id).slice(-4)}`;
}

export function readSandboxEnv(env = process.env) {
  const productionBaseId = env.AIRTABLE_BASE_ID || null;
  const sandboxBaseId =
    env.AIRTABLE_BASE_ID_SANDBOX ||
    env.AIRTABLE_SANDBOX_BASE_ID ||
    env.BE_SANDBOX_BASE_ID ||
    env.AIRTABLE_STAGING_BASE_ID ||
    null;
  const airtableEnv = String(env.AIRTABLE_ENV || "").trim().toLowerCase();
  const confirmed = String(env.BE_PILOT_SANDBOX_CONFIRMED || "").trim() === "1";
  return {
    productionBaseId,
    sandboxBaseId,
    airtableEnv,
    confirmed,
    apiKeyPresent: Boolean(
      env.AIRTABLE_SANDBOX_API_KEY ||
        env.AIRTABLE_PAT ||
        env.AIRTABLE_TOKEN ||
        env.AIRTABLE_API_KEY
    ),
  };
}

/**
 * Prefer a token scoped to the sandbox base.
 * AIRTABLE_API_KEY often covers production only; AIRTABLE_PAT / SANDBOX key may include sandbox.
 */
export function listSandboxApiKeyCandidates(env = process.env) {
  const ordered = [
    ["AIRTABLE_SANDBOX_API_KEY", env.AIRTABLE_SANDBOX_API_KEY],
    ["AIRTABLE_PAT", env.AIRTABLE_PAT],
    ["AIRTABLE_TOKEN", env.AIRTABLE_TOKEN],
    ["AIRTABLE_API_KEY", env.AIRTABLE_API_KEY],
  ];
  const seen = new Set();
  const out = [];
  for (const [label, value] of ordered) {
    if (!value || seen.has(value)) continue;
    seen.add(value);
    out.push({ label, apiKey: value });
  }
  return out;
}

export async function resolveSandboxApiKey(env = process.env) {
  const sandboxBaseId =
    env.AIRTABLE_BASE_ID_SANDBOX ||
    env.AIRTABLE_SANDBOX_BASE_ID ||
    env.BE_SANDBOX_BASE_ID ||
    env.AIRTABLE_STAGING_BASE_ID ||
    null;
  const candidates = listSandboxApiKeyCandidates(env);
  if (!candidates.length) {
    return { ok: false, label: null, apiKey: null, detail: "no Airtable API key candidates" };
  }
  if (!sandboxBaseId) {
    const first = candidates[0];
    return {
      ok: false,
      label: first.label,
      apiKey: first.apiKey,
      detail: "sandbox base id missing — cannot verify token access",
    };
  }

  /** @type {object[]} */
  const attempts = [];
  for (const c of candidates) {
    try {
      const data = await airtableFetch(c.apiKey, "https://api.airtable.com/v0/meta/bases");
      const hit = (data.bases || []).find((b) => b.id === sandboxBaseId);
      attempts.push({
        label: c.label,
        bases_count: (data.bases || []).length,
        sandbox_visible: Boolean(hit),
        sandbox_name: hit?.name || null,
      });
      if (hit) {
        return {
          ok: true,
          label: c.label,
          apiKey: c.apiKey,
          sandboxName: hit.name || null,
          attempts,
          detail: `${c.label} can see sandbox base "${hit.name || sandboxBaseId}"`,
        };
      }
    } catch (err) {
      attempts.push({ label: c.label, error: err.message, sandbox_visible: false });
    }
  }
  return {
    ok: false,
    label: null,
    apiKey: null,
    attempts,
    detail:
      "No configured token can access AIRTABLE_BASE_ID_SANDBOX. Grant the sandbox base to AIRTABLE_PAT or set AIRTABLE_SANDBOX_API_KEY.",
  };
}

function check(id, pass, detail) {
  return { id, pass: Boolean(pass), detail };
}

async function airtableFetch(apiKey, url) {
  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = data?.error?.message || data?.error || res.statusText;
    throw new Error(`${res.status} ${url}: ${msg}`);
  }
  return data;
}

async function fetchBaseName(apiKey, baseId) {
  const data = await airtableFetch(apiKey, "https://api.airtable.com/v0/meta/bases");
  const hit = (data.bases || []).find((b) => b.id === baseId);
  return hit?.name || null;
}

async function fetchTables(apiKey, baseId) {
  const data = await airtableFetch(
    apiKey,
    `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`
  );
  return data.tables || [];
}

async function fetchRecordById(apiKey, baseId, tableName, recordId) {
  const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableName)}/${encodeURIComponent(recordId)}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  if (res.status === 404) return null;
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = data?.error?.message || data?.error || res.statusText;
    throw new Error(`${res.status} fetch record ${recordId}: ${msg}`);
  }
  return data;
}

async function searchBrandByName(apiKey, baseId, tableName, nameHint) {
  const formula = `FIND('${String(nameHint).replace(/'/g, "\\'")}', {Brand Name})`;
  const params = new URLSearchParams({
    filterByFormula: formula,
    maxRecords: "5",
  });
  const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableName)}?${params}`;
  const data = await airtableFetch(apiKey, url);
  return data.records || [];
}

/**
 * Validate sandbox isolation + schema/record readiness.
 * Never writes. Never constructs an Airtable production write client.
 */
export async function validateAirtableSandbox(options = {}) {
  const env = options.env || process.env;
  const generatedAt = options.generatedAt || new Date().toISOString();
  const cfg = readSandboxEnv(env);

  /** @type {{id:string,pass:boolean,detail:string}[]} */
  const checks = [];
  let productionWriteClientInitialized = false;
  let sandboxReadPerformed = false;
  let sandboxBaseName = null;
  let sandboxApiKeyLabel = null;
  let tables = [];
  /** @type {object[]} */
  const targetRecords = [];
  /** @type {object[]} */
  const tableAvailability = [];
  /** @type {object[]} */
  const fieldAvailability = [];
  /** @type {string[]} */
  const blockers = [];

  // 1–4 env guards
  checks.push(
    check(
      "airtable_env_equals_sandbox",
      cfg.airtableEnv === "sandbox",
      cfg.airtableEnv ? `AIRTABLE_ENV=${cfg.airtableEnv}` : "AIRTABLE_ENV unset (required: sandbox)"
    )
  );
  checks.push(
    check(
      "be_pilot_sandbox_confirmed",
      cfg.confirmed,
      cfg.confirmed ? "BE_PILOT_SANDBOX_CONFIRMED=1" : "BE_PILOT_SANDBOX_CONFIRMED not set to 1"
    )
  );
  checks.push(
    check(
      "sandbox_base_id_present",
      Boolean(cfg.sandboxBaseId),
      cfg.sandboxBaseId
        ? `AIRTABLE_BASE_ID_SANDBOX=${maskBaseId(cfg.sandboxBaseId)}`
        : "AIRTABLE_BASE_ID_SANDBOX unset"
    )
  );
  checks.push(
    check(
      "sandbox_base_differs_from_production",
      Boolean(cfg.sandboxBaseId && cfg.productionBaseId && cfg.sandboxBaseId !== cfg.productionBaseId),
      cfg.sandboxBaseId && cfg.productionBaseId
        ? cfg.sandboxBaseId === cfg.productionBaseId
          ? "sandbox base ID equals production AIRTABLE_BASE_ID — unsafe"
          : `IDs differ (prod ${maskBaseId(cfg.productionBaseId)} vs sandbox ${maskBaseId(cfg.sandboxBaseId)})`
        : "cannot compare — missing sandbox and/or production base ID"
    )
  );

  // 9 — production write client never initialized by this validator
  checks.push(
    check(
      "production_write_client_not_initialized",
      productionWriteClientInitialized === false,
      "Validator does not construct Airtable(production) write client"
    )
  );

  const envOk = checks.every((c) => c.pass);
  if (!envOk) {
    blockers.push("sandbox_env_guardrails_failed");
  }

  const keyResolution = cfg.apiKeyPresent
    ? await resolveSandboxApiKey(env)
    : { ok: false, label: null, apiKey: null, detail: "no API key candidates", attempts: [] };
  const apiKey = keyResolution.ok ? keyResolution.apiKey : null;
  sandboxApiKeyLabel = keyResolution.label || null;

  if (!cfg.apiKeyPresent) {
    checks.push(check("api_key_present", false, "AIRTABLE_API_KEY / PAT unset — cannot inspect sandbox schema"));
    blockers.push("api_key_missing");
  } else {
    checks.push(
      check(
        "sandbox_token_can_access_base",
        keyResolution.ok,
        keyResolution.detail
      )
    );
    if (!keyResolution.ok) blockers.push("sandbox_token_cannot_access_base");
  }

  if (envOk && cfg.sandboxBaseId && apiKey) {
    try {
      sandboxBaseName = keyResolution.sandboxName || (await fetchBaseName(apiKey, cfg.sandboxBaseId));
      sandboxReadPerformed = true;
      const nameOk = Boolean(sandboxBaseName && SANDBOX_NAME_RE.test(sandboxBaseName));
      checks.push(
        check(
          "sandbox_base_name_includes_sandbox_staging_or_test",
          nameOk,
          sandboxBaseName
            ? `base name="${sandboxBaseName}"`
            : `sandbox base ${maskBaseId(cfg.sandboxBaseId)} not visible to API token (or renamed)`
        )
      );
      if (!nameOk) blockers.push("sandbox_base_name_not_recognized");

      tables = await fetchTables(apiKey, cfg.sandboxBaseId);
      const byName = new Map(tables.map((t) => [t.name, t]));

      for (const tableName of REQUIRED_TABLES) {
        const t = byName.get(tableName);
        tableAvailability.push({
          table: tableName,
          present: Boolean(t),
          field_count: t?.fields?.length ?? 0,
        });
        checks.push(
          check(
            `table_present_${tableName.replace(/\W+/g, "_").toLowerCase()}`,
            Boolean(t),
            t ? `table present (${t.fields?.length || 0} fields)` : `missing table: ${tableName}`
          )
        );
        if (!t) blockers.push(`missing_table:${tableName}`);
      }

      const basics = byName.get("Brand Setup - Brand Basics");
      const presentation = byName.get("Brand Setup - Brand Explorer Presentation");
      const basicsFieldNames = new Set((basics?.fields || []).map((f) => f.name));
      const presentationFieldNames = new Set((presentation?.fields || []).map((f) => f.name));

      for (const field of REQUIRED_BASICS_FIELDS) {
        const present = basicsFieldNames.has(field);
        fieldAvailability.push({ table: "Brand Setup - Brand Basics", field, present, role: "required" });
        checks.push(
          check(
            `basics_field_${field.replace(/\W+/g, "_").toLowerCase()}`,
            present,
            present ? `Brand Basics.${field} present` : `missing Brand Basics field: ${field}`
          )
        );
        if (!present) blockers.push(`missing_basics_field:${field}`);
      }

      for (const field of REQUIRED_PRESENTATION_FIELDS) {
        const present = presentationFieldNames.has(field);
        fieldAvailability.push({
          table: "Brand Setup - Brand Explorer Presentation",
          field,
          present,
          role: "required",
        });
        checks.push(
          check(
            `presentation_field_${field.replace(/\W+/g, "_").toLowerCase()}`,
            present,
            present ? `Presentation.${field} present` : `missing Presentation field: ${field}`
          )
        );
        if (!present) blockers.push(`missing_presentation_field:${field}`);
      }

      const linkField = PRESENTATION_BRAND_LINK_CANDIDATES.find((c) =>
        presentationFieldNames.has(c)
      );
      fieldAvailability.push({
        table: "Brand Setup - Brand Explorer Presentation",
        field: linkField || PRESENTATION_BRAND_LINK_CANDIDATES[0],
        present: Boolean(linkField),
        role: "brand_link",
      });
      checks.push(
        check(
          "presentation_brand_link_field",
          Boolean(linkField),
          linkField
            ? `Presentation brand link field="${linkField}"`
            : `no brand link among: ${PRESENTATION_BRAND_LINK_CANDIDATES.join(", ")}`
        )
      );
      if (!linkField) blockers.push("missing_presentation_brand_link");

      for (const intent of PILOT_FIELD_INTENTS) {
        fieldAvailability.push({
          table: "Brand Setup - Brand Explorer Presentation",
          field: intent,
          present: Boolean(presentation),
          role: "pilot_overlay_intent",
          note: "Maps to Presentation Title/Body rows — not a discrete Airtable column",
        });
      }

      for (const forbidden of FORBIDDEN_WRITE_FIELDS) {
        fieldAvailability.push({
          table: "Brand Setup - Brand Basics",
          field: forbidden,
          present: basicsFieldNames.has(forbidden),
          role: "forbidden_write_target",
          note: "Must not be written by VIC pilot even if present",
        });
      }

      // Target brand records
      for (const brand of TARGET_BRANDS) {
        let record = null;
        let resolution = "missing";
        try {
          record = await fetchRecordById(
            apiKey,
            cfg.sandboxBaseId,
            "Brand Setup - Brand Basics",
            brand.expectedRecordId
          );
          if (record) resolution = "exact_record_id";
        } catch (err) {
          // non-404 errors already thrown inside; treat as miss + note
          resolution = `lookup_error:${err.message}`;
        }

        if (!record) {
          const hint =
            brand.slug === "hotel-indigo"
              ? "Hotel Indigo"
              : brand.slug === "ascend"
                ? "Ascend"
                : brand.slug === "curio-collection"
                  ? "Curio"
                  : "Holiday Inn Express";
          try {
            const matches = await searchBrandByName(
              apiKey,
              cfg.sandboxBaseId,
              "Brand Setup - Brand Basics",
              hint
            );
            const hit = matches.find((r) =>
              brand.nameMatchers.some((re) => re.test(String(r.fields?.["Brand Name"] || "")))
            );
            if (hit) {
              record = hit;
              resolution = "name_search";
            }
          } catch (err) {
            resolution = `name_search_error:${err.message}`;
          }
        }

        const brandName = record?.fields?.["Brand Name"] || null;
        const brandStatus = record?.fields?.["Brand Status"] || null;
        const found = Boolean(record?.id);
        targetRecords.push({
          slug: brand.slug,
          expected_record_id: brand.expectedRecordId,
          sandbox_record_id: record?.id || null,
          brand_name: brandName,
          brand_status: brandStatus,
          resolution,
          found,
        });
        checks.push(
          check(
            `target_record_${brand.slug.replace(/-/g, "_")}`,
            found,
            found
              ? `found ${brand.slug} as ${record.id} (${resolution}) name="${brandName}"`
              : `missing sandbox Brand Basics record for ${brand.slug}`
          )
        );
        if (!found) blockers.push(`missing_target_record:${brand.slug}`);
      }
    } catch (err) {
      checks.push(
        check("sandbox_meta_read", false, `sandbox meta/read failed: ${err.message}`)
      );
      blockers.push("sandbox_meta_read_failed");
    }
  } else {
    checks.push(
      check(
        "sandbox_base_name_includes_sandbox_staging_or_test",
        false,
        "skipped — env guardrails failed before base name check"
      )
    );
    for (const tableName of REQUIRED_TABLES) {
      tableAvailability.push({ table: tableName, present: false, skipped: true });
      checks.push(
        check(
          `table_present_${tableName.replace(/\W+/g, "_").toLowerCase()}`,
          false,
          "skipped — sandbox env not ready"
        )
      );
    }
    for (const brand of TARGET_BRANDS) {
      targetRecords.push({
        slug: brand.slug,
        expected_record_id: brand.expectedRecordId,
        sandbox_record_id: null,
        found: false,
        resolution: "skipped_env_not_ready",
      });
      checks.push(
        check(
          `target_record_${brand.slug.replace(/-/g, "_")}`,
          false,
          "skipped — sandbox env not ready"
        )
      );
    }
  }

  // 10 — patch execution gate
  const allPass = checks.every((c) => c.pass) && blockers.length === 0;
  checks.push(
    check(
      "patch_execution_gated_on_validation",
      true,
      allPass
        ? "validation PASS — VIC sandbox patch may execute only via sandbox write adapter"
        : "validation FAIL — patch execution must remain impossible"
    )
  );

  const status = allPass ? STATUS.READY : STATUS.FAILED;
  const vicSandboxPatchMayExecute = status === STATUS.READY;

  return {
    version: SANDBOX_VALIDATION_VERSION,
    status,
    generated_at: generatedAt,
    sandbox_base_id_masked: maskBaseId(cfg.sandboxBaseId),
    sandbox_base_id_raw_for_ops_only: null, // never echo full ID into reports by default
    sandbox_base_name: sandboxBaseName,
    sandbox_api_key_label: sandboxApiKeyLabel,
    production_base_id_masked: maskBaseId(cfg.productionBaseId),
    ids_differ: Boolean(
      cfg.sandboxBaseId && cfg.productionBaseId && cfg.sandboxBaseId !== cfg.productionBaseId
    ),
    env: {
      AIRTABLE_ENV: cfg.airtableEnv || null,
      BE_PILOT_SANDBOX_CONFIRMED: cfg.confirmed ? "1" : null,
      AIRTABLE_BASE_ID_SANDBOX_present: Boolean(cfg.sandboxBaseId),
    },
    checks,
    table_availability: tableAvailability,
    field_availability: fieldAvailability,
    target_records: targetRecords,
    write_safety: {
      production_write_client_initialized: productionWriteClientInitialized,
      sandbox_read_performed: sandboxReadPerformed,
      production_airtable_writes: false,
      sandbox_airtable_writes: false,
      patch_execution_allowed: vicSandboxPatchMayExecute,
    },
    vic_sandbox_patch_may_execute: vicSandboxPatchMayExecute,
    blockers,
    manual_setup_required: !allPass,
    manual_setup_instructions: [
      "In Airtable, duplicate production Brand Explorer base (Deal Capture MVP) as a new base.",
      "Name it clearly, e.g. \"Deal Capture MVP — Sandbox\" or \"… Staging / Test\".",
      "Ensure Brand Setup - Brand Basics + Brand Explorer Presentation (and pilot brand records) are present.",
      "Copy the new base ID into .env as AIRTABLE_BASE_ID_SANDBOX=app…",
      "Set AIRTABLE_ENV=sandbox",
      "Set BE_PILOT_SANDBOX_CONFIRMED=1",
      "Keep AIRTABLE_BASE_ID pointed at production for read-only comparison only.",
      "Re-run: npm run research-engine-v2:validate-airtable-sandbox",
    ],
    constraints: {
      production_airtable_writes: false,
      frozen_62_modified: false,
      frozen_vic_modified: false,
      webhound_used: false,
    },
  };
}

/**
 * Hard gate for any future VIC → BE sandbox patch runner.
 * Throws if sandbox validation is not READY.
 */
export async function assertSandboxReadyForVicBePatch(options = {}) {
  const report = await validateAirtableSandbox(options);
  if (report.status !== STATUS.READY || !report.vic_sandbox_patch_may_execute) {
    const err = new Error(
      `Sandbox validation failed (${report.status}). VIC BE patch execution blocked. Blockers: ${(report.blockers || []).join(", ") || "see checks"}`
    );
    err.sandboxValidation = report;
    throw err;
  }
  return report;
}

export function renderSandboxValidationMarkdown(report) {
  const lines = [
    `# Airtable Sandbox Validation — VIC → Brand Explorer Pilot`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Generated:** ${report.generated_at}`,
    `**Version:** ${report.version}`,
    ``,
    `## Base IDs`,
    ``,
    `| Role | Masked ID | Name |`,
    `|------|-----------|------|`,
    `| Production (\`AIRTABLE_BASE_ID\`) | \`${report.production_base_id_masked || "(unset)"}\` | (production — do not write) |`,
    `| Sandbox (\`AIRTABLE_BASE_ID_SANDBOX\`) | \`${report.sandbox_base_id_masked || "(unset)"}\` | ${report.sandbox_base_name || "(n/a)"} |`,
    ``,
    `- IDs differ: **${report.ids_differ}**`,
    `- VIC sandbox patch may execute: **${report.vic_sandbox_patch_may_execute}**`,
    ``,
    `## Environment`,
    ``,
    `| Variable | Value |`,
    `|----------|-------|`,
    `| AIRTABLE_ENV | \`${report.env.AIRTABLE_ENV || "(unset)"}\` |`,
    `| BE_PILOT_SANDBOX_CONFIRMED | \`${report.env.BE_PILOT_SANDBOX_CONFIRMED || "(unset)"}\` |`,
    `| AIRTABLE_BASE_ID_SANDBOX present | ${report.env.AIRTABLE_BASE_ID_SANDBOX_present} |`,
    ``,
    `## Checks`,
    ``,
    ...(report.checks || []).map((c) => `- [${c.pass ? "PASS" : "FAIL"}] **${c.id}** — ${c.detail}`),
    ``,
    `## Tables`,
    ``,
    `| Table | Present |`,
    `|-------|---------|`,
    ...(report.table_availability || []).map(
      (t) => `| ${t.table} | ${t.present}${t.skipped ? " (skipped)" : ""} |`
    ),
    ``,
    `## Target records`,
    ``,
    `| Slug | Expected ID | Sandbox ID | Found | Resolution |`,
    `|------|-------------|------------|-------|------------|`,
    ...(report.target_records || []).map(
      (r) =>
        `| ${r.slug} | \`${r.expected_record_id}\` | \`${r.sandbox_record_id || "—"}\` | ${r.found} | ${r.resolution} |`
    ),
    ``,
    `## Write safety`,
    ``,
    `- Production write client initialized: **${report.write_safety?.production_write_client_initialized}**`,
    `- Production Airtable writes: **${report.write_safety?.production_airtable_writes}**`,
    `- Sandbox Airtable writes: **${report.write_safety?.sandbox_airtable_writes}**`,
    `- Patch execution allowed: **${report.write_safety?.patch_execution_allowed}**`,
    ``,
    `## Blockers`,
    ``,
    report.blockers?.length
      ? report.blockers.map((b) => `- ${b}`).join("\n")
      : "- (none)",
    ``,
    `## Manual setup (if not ready)`,
    ``,
    ...(report.manual_setup_instructions || []).map((s, i) => `${i + 1}. ${s}`),
    ``,
  ];
  return lines.join("\n");
}
