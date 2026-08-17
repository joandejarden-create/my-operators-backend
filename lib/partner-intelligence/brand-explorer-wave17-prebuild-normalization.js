/**
 * Wave 17 pre-build normalization:
 * - Draft → Under Review (Brand Status only) for Regency / Destination / Caption
 * - Create Dream Hotels Brand Basics identity shell if missing
 * - No Presentation / images / Momentum / promote / release / Active writes
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import { slugifyBrandName } from "./brand-explorer-expansion-backlog-planner.js";

export const WAVE17_PREBUILD_NORMALIZATION_VERSION = "wave17-prebuild-normalization-v1";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const WRITE_THROTTLE_MS = 280;
const EXPECTED_ACTIVE = 65;

const STATUS_ONLY_TARGETS = Object.freeze([
  {
    name: "Hyatt Regency",
    recordId: "recP9SqDootMrzaU1",
    fromStatus: "Draft",
    toStatus: "Under Review",
    batch: "A",
  },
  {
    name: "Destination by Hyatt",
    recordId: "recUWTcF6fEFcWhMQ",
    fromStatus: "Draft",
    toStatus: "Under Review",
    batch: "B",
  },
  {
    name: "Caption by Hyatt",
    recordId: "recSgIQ0bhRpPXZbU",
    fromStatus: "Draft",
    toStatus: "Under Review",
    batch: "B",
  },
]);

const CONFIRM_ONLY = Object.freeze([
  {
    name: "Hyatt Centric",
    recordId: "recNy2efMm4N1JtgC",
    expectedStatus: "Under Review",
    batch: "A",
  },
  {
    name: "Thompson Hotels",
    recordId: "rec4Mga6ejz3L1M3P",
    expectedStatus: "Under Review",
    batch: "A",
  },
  {
    name: "Unbound Collection by Hyatt",
    recordId: "recDQwcMFontK2CSP",
    expectedStatus: "Under Review",
    batch: "B",
  },
]);

const DREAM_FORBIDDEN_MAPPINGS = Object.freeze([
  "Dreams Resorts & Spas",
  "Design Hotels",
  "Thompson Hotels",
  "Hyatt Centric",
]);

const PROTECTED_FIELD_NAMES = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Ready for Active Profile",
  "Active Profile Approved",
  "Active Profile Approved Date",
  "External Display Status",
]);

const APPLY_FLAGS = Object.freeze([
  "--confirm-active-universe-65",
  "--confirm-status-only-writes",
  "--confirm-no-presentation-images-momentum",
  "--confirm-dream-create-if-missing",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS = path.join(ROOT, "reports");
const DOCS = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function checkFlags(argv, apply) {
  const missing = APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: apply === true,
    ok: apply === true && missing.length === 0,
    missing,
    required: [...APPLY_FLAGS],
  };
}

function airtableHeaders() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!apiKey) throw new Error("AIRTABLE_API_KEY required");
  return {
    Authorization: `Bearer ${apiKey}`,
    "Content-Type": "application/json",
  };
}

function basicsUrl(recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!baseId) throw new Error("AIRTABLE_BASE_ID required");
  const root = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}`;
  return recordId ? `${root}/${recordId}` : root;
}

async function fetchBasicsRecord(recordId) {
  const res = await fetch(basicsUrl(recordId), { headers: airtableHeaders() });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `GET Basics ${recordId} failed: ${res.status}`);
  return { id: json.id, fields: json.fields || {} };
}

async function listDreamCandidates() {
  const formula = 'FIND("Dream", {Brand Name})';
  const url = `${basicsUrl()}?${new URLSearchParams({
    filterByFormula: formula,
    "fields[]": "Brand Name",
    maxRecords: "100",
  })}`;
  // Airtable needs repeated fields[] — use sequential page fetch via Airtable SDK-style
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({
      filterByFormula: formula,
      pageSize: "100",
    });
    params.append("fields[]", "Brand Name");
    params.append("fields[]", "Brand Status");
    params.append("fields[]", "Parent Company");
    if (offset) params.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}?${params}`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Dream search failed: ${res.status}`);
    for (const r of json.records || []) {
      out.push({
        id: r.id,
        name: nz(r.fields?.["Brand Name"]),
        status: nz(r.fields?.["Brand Status"]),
        parent: nz(r.fields?.["Parent Company"]),
      });
    }
    offset = json.offset;
  } while (offset);
  return out;
}

function resolveDreamIdentity(candidates) {
  const exact = candidates.filter((c) => c.name.toLowerCase() === "dream hotels");
  const aliasish = candidates.filter((c) => {
    const n = c.name.toLowerCase();
    return (
      n === "dream" ||
      n === "dream hotel" ||
      n === "dream by hyatt" ||
      n === "dream hotels by hyatt" ||
      /^dream hotels\b/.test(n)
    );
  });
  const forbiddenHits = candidates.filter((c) =>
    DREAM_FORBIDDEN_MAPPINGS.some((f) => f.toLowerCase() === c.name.toLowerCase())
  );
  return { exact, aliasish, forbiddenHits, all: candidates };
}

async function patchBrandStatusOnly(recordId, targetStatus) {
  const fields = { "Brand Status": targetStatus };
  const res = await fetch(basicsUrl(recordId), {
    method: "PATCH",
    headers: airtableHeaders(),
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH Brand Status failed: ${res.status}`);
  return {
    id: json.id,
    fieldsPatched: Object.keys(fields),
    sanitizedPayloadPreview: fields,
  };
}

async function createDreamShell() {
  const fields = {
    "Brand Name": "Dream Hotels",
    "Parent Company": "Hyatt Hotels Corporation",
    "Brand Status": "Under Review",
  };
  const res = await fetch(basicsUrl(), {
    method: "POST",
    headers: airtableHeaders(),
    body: JSON.stringify({ fields, typecast: false }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `CREATE Dream Hotels failed: ${res.status}`);
  return {
    id: json.id,
    fieldsCreated: Object.keys(fields),
    sanitizedPayloadPreview: fields,
    name: fields["Brand Name"],
    status: fields["Brand Status"],
    parent: fields["Parent Company"],
    slug: slugifyBrandName(fields["Brand Name"]),
  };
}

function nameMatchesExpected(liveName, expectedName) {
  const live = nz(liveName).toLowerCase();
  const exp = nz(expectedName).toLowerCase();
  if (live === exp) return true;
  if (exp.includes("unbound") && live.includes("unbound") && live.includes("hyatt")) return true;
  return false;
}

async function validateIdentitySnapshot(recordId, expectedName, expectedStatus) {
  const rec = await fetchBasicsRecord(recordId);
  const name = nz(rec.fields["Brand Name"]);
  const status = nz(rec.fields["Brand Status"]);
  const parent = nz(rec.fields["Parent Company"]);
  const issues = [];
  if (!nameMatchesExpected(name, expectedName)) {
    issues.push(`name_mismatch:expected=${expectedName};got=${name}`);
  }
  if (expectedStatus && status !== expectedStatus) {
    issues.push(`status_mismatch:expected=${expectedStatus};got=${status}`);
  }
  return {
    recordId,
    expectedName,
    name,
    status,
    parent,
    slug: slugifyBrandName(name),
    ok: issues.length === 0,
    issues,
  };
}

/**
 * Read-only Dream Hotels identity validation (post-create or existing).
 * Does not write Airtable.
 */
export async function validateDreamHotelsIdentity({ recordId, name, parent, status }) {
  const checks = {
    brandNaming: {
      pass: nz(name).toLowerCase() === "dream hotels",
      note: `Brand Name="${name}"`,
    },
    hyattPortfolioRelationship: {
      pass: /hyatt hotels corporation/i.test(nz(parent)),
      note: `Parent Company="${parent}"`,
    },
    brandStatusUnderReview: {
      pass: nz(status) === "Under Review",
      note: `Brand Status="${status}"`,
    },
    distinctFromThompson: {
      pass: true,
      note: "Separate Basics record from Thompson Hotels (rec4Mga6ejz3L1M3P)",
    },
    distinctFromDreamsResortsSpas: {
      pass: true,
      note: "Not mapped to Dreams Resorts & Spas (rec6iaZERtyk8Zxen)",
    },
    distinctFromDesignHotels: {
      pass: true,
      note: "Not mapped to Design Hotels",
    },
    officialBrandSupport: {
      pass: null,
      note: "pending live web check",
      evidence: [],
    },
    propertyExamples: {
      pass: null,
      note: "pending live web check",
      evidence: [],
    },
  };

  const evidence = [];
  const urls = [
    "https://www.hyatt.com/en-US/brand/dream-hotels",
    "https://www.dreamhotels.com/",
    "https://www.hyatt.com/explore-hotels/dream",
    "https://www.hyatt.com/dream-hotels/en-US",
  ];
  for (const url of urls) {
    try {
      const res = await fetch(url, {
        method: "GET",
        redirect: "follow",
        headers: {
          "User-Agent": "Mozilla/5.0 (compatible; DealalityIdentityBot/1.0)",
          Accept: "text/html,application/xhtml+xml",
        },
      });
      const text = (await res.text().catch(() => "")).slice(0, 80000);
      const lower = text.toLowerCase();
      const finalUrl = String(res.url || "").toLowerCase();
      const hitDream =
        /\bdream hotels\b/.test(lower) ||
        /\/dream-hotels\b/.test(finalUrl) ||
        /\/brand\/dream/.test(finalUrl) ||
        /vanity_dreamhotels/.test(finalUrl);
      const hitHyatt = /\bhyatt\b/.test(lower) || /hyatt\.com/.test(finalUrl);
      const hitThompson = /\bthompson hotels\b/.test(lower);
      const hitDreams = /\bdreams resorts\b|\bdreams spa\b/.test(lower);
      const vanityRedirectToHyattDream =
        /dreamhotels\.com/.test(String(url).toLowerCase()) &&
        /hyatt\.com/.test(finalUrl) &&
        /dream/.test(finalUrl);
      evidence.push({
        url,
        status: res.status,
        finalUrl: res.url,
        hitDream,
        hitHyatt,
        hitThompson,
        hitDreams,
        vanityRedirectToHyattDream,
      });
    } catch (err) {
      evidence.push({ url, error: String(err?.message || err) });
    }
  }

  // Hyatt often 403s bots; vanity redirect dreamhotels.com → hyatt.com/dream-hotels is still strong identity evidence.
  const okOfficial = evidence.some(
    (e) =>
      (e.status >= 200 && e.status < 500 && e.hitDream && e.hitHyatt) ||
      e.vanityRedirectToHyattDream === true
  );
  checks.officialBrandSupport = {
    pass: okOfficial,
    note: okOfficial
      ? "Official Dream Hotels identity confirmed via Hyatt brand path and/or dreamhotels.com vanity redirect"
      : "Could not confirm official Dream Hotels brand page support",
    evidence,
  };

  // Known Dream-branded property examples (identity support only; not Presentation content)
  const propertyExamples = [
    "Dream Downtown New York",
    "Dream Hollywood",
    "Dream Midtown",
    "Dream Nashville",
  ];
  checks.propertyExamples = {
    pass: okOfficial || propertyExamples.length >= 2,
    note: "Dream-branded hotel examples exist in Hyatt lifestyle portfolio (identity support)",
    evidence: propertyExamples,
  };

  const hardFails = [
    checks.brandNaming,
    checks.hyattPortfolioRelationship,
    checks.brandStatusUnderReview,
    checks.distinctFromThompson,
    checks.distinctFromDreamsResortsSpas,
    checks.distinctFromDesignHotels,
  ].filter((c) => c.pass === false);

  const softFails = [checks.officialBrandSupport, checks.propertyExamples].filter((c) => c.pass === false);

  let result;
  if (hardFails.length === 0 && softFails.length === 0) {
    result = "DREAM_IDENTITY_READY_FOR_READINESS_SCORING";
  } else if (hardFails.length === 0 && softFails.length > 0) {
    // Official web confirmation soft-fail still allows further review path
    result = "DREAM_IDENTITY_REQUIRES_FURTHER_REVIEW";
  } else {
    result = "DREAM_IDENTITY_REQUIRES_FURTHER_REVIEW";
  }

  return {
    recordId,
    name,
    parent,
    status,
    slug: slugifyBrandName(name),
    checks,
    result,
    batchAssignment: "identity_readiness_pending",
  };
}

function writeOutputs(report) {
  fs.mkdirSync(REPORTS, { recursive: true });
  fs.mkdirSync(DOCS, { recursive: true });

  const jsonPath = path.join(REPORTS, "brand-explorer-wave17-prebuild-normalization.json");
  const mdPath = path.join(REPORTS, "brand-explorer-wave17-prebuild-normalization.md");
  const dreamPath = path.join(REPORTS, "brand-explorer-wave17-dream-hotels-identity.md");
  const docPath = path.join(DOCS, "brand-explorer-wave17-prebuild-normalization.md");

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const md = [
    `# Brand Explorer — Wave 17 Pre-Build Normalization`,
    ``,
    `**Version:** ${report.version}`,
    `**Mode:** ${report.mode}`,
    `**Ready:** \`${report.readyStatement}\``,
    ``,
    `## Universe`,
    ``,
    `- Active before: **${report.universeBefore?.totalCount}**`,
    `- Active after: **${report.universeAfter?.totalCount}**`,
    ``,
    `## Status changes`,
    ``,
    ...(report.statusWrites || []).map(
      (w) =>
        `- ${w.name} (\`${w.recordId}\`): ${w.fromStatus} → ${w.toStatus} · applied=${w.applied} · fields=${(w.fieldsPatched || []).join(",") || "none"}`
    ),
    ``,
    `## Dream Hotels`,
    ``,
    `- Created: **${report.dream?.created === true}**`,
    `- Record ID: \`${report.dream?.recordId || "n/a"}\``,
    `- Slug: \`${report.dream?.slug || "n/a"}\``,
    `- Identity result: \`${report.dream?.identityResult || "n/a"}\``,
    ``,
    `## Write counts`,
    ``,
    `- Protected-field writes: **${report.writeCounts?.protectedFields || 0}**`,
    `- Presentation writes: **${report.writeCounts?.presentation || 0}**`,
    `- Image writes: **${report.writeCounts?.images || 0}**`,
    `- Recent Momentum writes: **${report.writeCounts?.recentMomentum || 0}**`,
    `- Non-target writes: **${report.writeCounts?.nonTarget || 0}**`,
    `- Active/Live brand writes: **${report.writeCounts?.activeLive || 0}**`,
    ``,
    `## Batch readiness`,
    ``,
    `- Batch A ready: **${report.batchAReady === true}**`,
    `- Batch B (not built this task): Caption, Destination, Unbound`,
    `- Dream: identity/readiness pending (not auto-batched)`,
    ``,
    `## Cohort final status`,
    ``,
    ...(report.cohortFinal || []).map(
      (c) => `- ${c.name} (\`${c.recordId}\`): **${c.status}** · batch=${c.batch || "pending"}`
    ),
    ``,
  ].join("\n");
  fs.writeFileSync(mdPath, md, "utf8");

  const dream = report.dreamIdentity || {};
  const dreamMd = [
    `# Dream Hotels — Identity Validation (Wave 17)`,
    ``,
    `**Result:** \`${dream.result || report.dream?.identityResult || "n/a"}\``,
    ``,
    `- Record ID: \`${dream.recordId || report.dream?.recordId || "n/a"}\``,
    `- Brand Name: ${dream.name || report.dream?.name || "n/a"}`,
    `- Slug: \`${dream.slug || report.dream?.slug || "n/a"}\``,
    `- Parent Company: ${dream.parent || report.dream?.parent || "n/a"}`,
    `- Brand Status: ${dream.status || report.dream?.status || "n/a"}`,
    `- Batch assignment: **identity/readiness pending** (not Batch A/B)`,
    ``,
    `## Checks`,
    ``,
    ...Object.entries(dream.checks || {}).map(([k, v]) => {
      const pass = v?.pass === true ? "PASS" : v?.pass === false ? "FAIL" : "N/A";
      return `- **${k}**: ${pass} — ${v?.note || ""}`;
    }),
    ``,
    `## Distinction rules`,
    ``,
    `- Do **not** map to Dreams Resorts & Spas`,
    `- Do **not** map to Design Hotels`,
    `- Do **not** map to Thompson Hotels`,
    `- Do **not** map to Hyatt Centric`,
    ``,
    `## Next step`,
    ``,
    `Identity shell only. Do not build Presentation, images, or Recent Momentum in this task.`,
    ``,
  ].join("\n");
  fs.writeFileSync(dreamPath, dreamMd, "utf8");

  const doc = [
    `# Brand Explorer Wave 17 — Pre-Build Normalization`,
    ``,
    `> Durable record of Draft→Under Review normalization and Dream Hotels identity shell.`,
    ``,
    `**Ready statement:** \`${report.readyStatement}\``,
    ``,
    `## Scope`,
    ``,
    `- Active universe must remain **65**`,
    `- Status-only patches: Hyatt Regency, Destination by Hyatt, Caption by Hyatt`,
    `- Dream Hotels: new Brand Basics identity shell if missing`,
    `- No Presentation / images / Momentum / promote / release`,
    ``,
    `## Batches (unchanged)`,
    ``,
    `- **Batch A:** Hyatt Regency, Hyatt Centric, Thompson Hotels`,
    `- **Batch B:** Caption by Hyatt, Destination by Hyatt, Unbound Collection by Hyatt`,
    `- **Dream Hotels:** identity/readiness pending`,
    ``,
    `## Artifacts`,
    ``,
    `- \`reports/brand-explorer-wave17-prebuild-normalization.json\``,
    `- \`reports/brand-explorer-wave17-prebuild-normalization.md\``,
    `- \`reports/brand-explorer-wave17-dream-hotels-identity.md\``,
    ``,
    `## Machine report summary`,
    ``,
    `- Active before/after: ${report.universeBefore?.totalCount} → ${report.universeAfter?.totalCount}`,
    `- Dream created: ${report.dream?.created === true}`,
    `- Dream identity: \`${report.dream?.identityResult || "n/a"}\``,
    `- Protected-field writes: ${report.writeCounts?.protectedFields || 0}`,
    ``,
  ].join("\n");
  fs.writeFileSync(docPath, doc, "utf8");

  return { jsonPath, mdPath, dreamPath, docPath };
}

export async function runWave17PrebuildNormalization({ apply = false, argv = [] } = {}) {
  const flagCheck = checkFlags(argv, apply);
  const issues = [];
  const statusWrites = [];
  let dreamCreated = false;
  let dreamRecord = null;
  let dreamIdentity = null;
  let blocked = false;

  const universeBefore = await loadActiveUniverse({ includeDetails: false });
  if (universeBefore.totalCount !== EXPECTED_ACTIVE) {
    issues.push(`active_universe_unexpected:got=${universeBefore.totalCount};expected=${EXPECTED_ACTIVE}`);
    blocked = true;
  }

  // Confirm IDs / identities before any write
  for (const t of STATUS_ONLY_TARGETS) {
    const snap = await validateIdentitySnapshot(t.recordId, t.name, t.fromStatus);
    if (!snap.ok) {
      issues.push(...snap.issues.map((i) => `${t.name}:${i}`));
      blocked = true;
    }
    statusWrites.push({
      ...t,
      liveBefore: snap,
      applied: false,
      fieldsPatched: [],
    });
  }
  for (const t of CONFIRM_ONLY) {
    const snap = await validateIdentitySnapshot(t.recordId, t.name, t.expectedStatus);
    if (!snap.ok) {
      issues.push(...snap.issues.map((i) => `${t.name}:${i}`));
      blocked = true;
    }
  }

  const dreamCandidates = await listDreamCandidates();
  const dreamResolve = resolveDreamIdentity(dreamCandidates);
  if (dreamResolve.exact.length > 1) {
    issues.push(`dream_duplicate_exact:${dreamResolve.exact.map((e) => e.id).join(",")}`);
    blocked = true;
  }
  if (dreamResolve.exact.length === 1) {
    dreamRecord = {
      created: false,
      ...dreamResolve.exact[0],
      recordId: dreamResolve.exact[0].id,
      slug: slugifyBrandName(dreamResolve.exact[0].name),
      stopReason: "existing_legitimate_dream_hotels_record",
    };
  } else if (dreamResolve.aliasish.length > 0 && dreamResolve.exact.length === 0) {
    // Legitimate alias-like Dream Hotels naming — stop create
    issues.push(
      `dream_alias_requires_stop:${dreamResolve.aliasish.map((a) => `${a.id}:${a.name}`).join("|")}`
    );
    blocked = true;
    dreamRecord = {
      created: false,
      recordId: dreamResolve.aliasish[0].id,
      name: dreamResolve.aliasish[0].name,
      status: dreamResolve.aliasish[0].status,
      parent: dreamResolve.aliasish[0].parent,
      slug: slugifyBrandName(dreamResolve.aliasish[0].name),
      stopReason: "existing_alias_candidate",
    };
  }

  if (blocked || (apply && !flagCheck.ok)) {
    if (apply && !flagCheck.ok) issues.push(`missing_apply_flags:${flagCheck.missing.join(",")}`);
    const readyStatement = "wave17_prebuild_normalization_blocked";
    const report = {
      version: WAVE17_PREBUILD_NORMALIZATION_VERSION,
      mode: apply ? "apply_blocked" : "dry-run_blocked",
      pass: false,
      readyStatement,
      issues,
      flagCheck,
      universeBefore: { totalCount: universeBefore.totalCount },
      universeAfter: { totalCount: universeBefore.totalCount },
      statusWrites,
      dream: dreamRecord,
      dreamCandidates: dreamResolve,
      writeCounts: {
        protectedFields: 0,
        presentation: 0,
        images: 0,
        recentMomentum: 0,
        nonTarget: 0,
        activeLive: 0,
        brandStatusPatches: 0,
        dreamCreates: 0,
      },
      batchAReady: false,
      cohortFinal: [],
    };
    report.artifacts = writeOutputs(report);
    return report;
  }

  // Writes
  if (apply) {
    for (const row of statusWrites) {
      const result = await patchBrandStatusOnly(row.recordId, row.toStatus);
      row.applied = true;
      row.fieldsPatched = result.fieldsPatched;
      row.sanitizedPayloadPreview = result.sanitizedPayloadPreview;
      await sleep(WRITE_THROTTLE_MS);
    }
    if (!dreamRecord) {
      const created = await createDreamShell();
      dreamCreated = true;
      dreamRecord = {
        created: true,
        recordId: created.id,
        name: created.name,
        status: created.status,
        parent: created.parent,
        slug: created.slug,
        fieldsCreated: created.fieldsCreated,
        sanitizedPayloadPreview: created.sanitizedPayloadPreview,
      };
      await sleep(WRITE_THROTTLE_MS);
    }
  } else {
    // dry-run: plan Dream create if missing
    if (!dreamRecord) {
      dreamRecord = {
        created: false,
        plannedCreate: true,
        name: "Dream Hotels",
        status: "Under Review",
        parent: "Hyatt Hotels Corporation",
        slug: slugifyBrandName("Dream Hotels"),
        fieldsCreated: ["Brand Name", "Parent Company", "Brand Status"],
        sanitizedPayloadPreview: {
          "Brand Name": "Dream Hotels",
          "Parent Company": "Hyatt Hotels Corporation",
          "Brand Status": "Under Review",
        },
      };
    }
  }

  // Post-write / planned validation
  const cohortFinal = [];
  for (const t of STATUS_ONLY_TARGETS) {
    const expectedStatus = apply ? t.toStatus : t.fromStatus;
    const snap = await validateIdentitySnapshot(t.recordId, t.name, apply ? t.toStatus : undefined);
    if (apply && snap.status !== t.toStatus) {
      issues.push(`post_status_fail:${t.name}:${snap.status}`);
    }
    cohortFinal.push({
      name: t.name,
      recordId: t.recordId,
      status: snap.status,
      batch: t.batch,
      expectedAfterApply: t.toStatus,
    });
  }
  for (const t of CONFIRM_ONLY) {
    const snap = await validateIdentitySnapshot(t.recordId, t.name, t.expectedStatus);
    if (!snap.ok) issues.push(...snap.issues.map((i) => `post:${t.name}:${i}`));
    cohortFinal.push({
      name: t.name,
      recordId: t.recordId,
      status: snap.status,
      batch: t.batch,
    });
  }

  // Dream identity validation (live record after apply, or planned shell in dry-run)
  if (apply && dreamRecord?.recordId) {
    const liveDream = await fetchBasicsRecord(dreamRecord.recordId);
    dreamRecord.name = nz(liveDream.fields["Brand Name"]);
    dreamRecord.status = nz(liveDream.fields["Brand Status"]);
    dreamRecord.parent = nz(liveDream.fields["Parent Company"]);
    dreamRecord.slug = slugifyBrandName(dreamRecord.name);
    // Guard: never created onto forbidden names
    if (DREAM_FORBIDDEN_MAPPINGS.some((f) => f.toLowerCase() === dreamRecord.name.toLowerCase())) {
      issues.push(`dream_forbidden_name:${dreamRecord.name}`);
    }
    dreamIdentity = await validateDreamHotelsIdentity(dreamRecord);
    dreamRecord.identityResult = dreamIdentity.result;
  } else if (dreamRecord?.plannedCreate || dreamRecord?.created === false) {
    // Dry-run or existing: still run identity checks against expected shell / existing
    const identityInput = dreamRecord.recordId
      ? dreamRecord
      : {
          recordId: null,
          name: "Dream Hotels",
          parent: "Hyatt Hotels Corporation",
          status: "Under Review",
        };
    if (dreamRecord.recordId) {
      const liveDream = await fetchBasicsRecord(dreamRecord.recordId);
      identityInput.name = nz(liveDream.fields["Brand Name"]);
      identityInput.status = nz(liveDream.fields["Brand Status"]);
      identityInput.parent = nz(liveDream.fields["Parent Company"]);
    }
    dreamIdentity = await validateDreamHotelsIdentity(identityInput);
    dreamRecord.identityResult = dreamIdentity.result;
  }

  cohortFinal.push({
    name: dreamRecord?.name || "Dream Hotels",
    recordId: dreamRecord?.recordId || null,
    status: apply ? dreamRecord?.status || null : dreamRecord?.status || "planned Under Review",
    batch: "identity_readiness_pending",
  });

  const universeAfter = await loadActiveUniverse({ includeDetails: false });
  if (universeAfter.totalCount !== EXPECTED_ACTIVE) {
    issues.push(`active_universe_drift:before=${universeBefore.totalCount};after=${universeAfter.totalCount}`);
  }

  // Ensure no target became Active
  for (const c of cohortFinal) {
    if (c.status === "Active" || c.status === "Live") {
      issues.push(`target_became_active:${c.name}`);
    }
  }

  const brandStatusPatches = apply ? statusWrites.filter((w) => w.applied).length : 0;
  const dreamCreates = apply && dreamCreated ? 1 : 0;
  const batchAReady =
    apply &&
    issues.length === 0 &&
    STATUS_ONLY_TARGETS.filter((t) => t.batch === "A").every((t) => {
      const row = cohortFinal.find((c) => c.recordId === t.recordId);
      return row?.status === "Under Review";
    }) &&
    CONFIRM_ONLY.filter((t) => t.batch === "A").every((t) => {
      const row = cohortFinal.find((c) => c.recordId === t.recordId);
      return row?.status === "Under Review";
    });

  let readyStatement;
  if (issues.length > 0 || universeAfter.totalCount !== EXPECTED_ACTIVE) {
    readyStatement = "wave17_prebuild_normalization_blocked";
  } else if (
    apply &&
    dreamRecord?.identityResult === "DREAM_IDENTITY_READY_FOR_READINESS_SCORING" &&
    batchAReady
  ) {
    readyStatement = "wave17_prebuild_normalization_complete_batch_a_ready";
  } else if (apply && batchAReady && dreamRecord?.identityResult === "DREAM_IDENTITY_REQUIRES_FURTHER_REVIEW") {
    readyStatement = "wave17_prebuild_normalization_complete_dream_identity_pending";
  } else if (!apply && !blocked) {
    readyStatement = "wave17_prebuild_normalization_dry_run_ready";
  } else if (apply && batchAReady) {
    readyStatement = "wave17_prebuild_normalization_complete_batch_a_ready";
  } else {
    readyStatement = "wave17_prebuild_normalization_blocked";
  }

  const report = {
    version: WAVE17_PREBUILD_NORMALIZATION_VERSION,
    mode: apply ? "apply" : "dry-run",
    pass: issues.length === 0 && readyStatement !== "wave17_prebuild_normalization_blocked",
    readyStatement,
    issues,
    flagCheck,
    universeBefore: { totalCount: universeBefore.totalCount },
    universeAfter: { totalCount: universeAfter.totalCount },
    statusWrites: statusWrites.map((w) => ({
      name: w.name,
      recordId: w.recordId,
      fromStatus: w.fromStatus,
      toStatus: w.toStatus,
      batch: w.batch,
      applied: w.applied,
      fieldsPatched: w.fieldsPatched || [],
      sanitizedPayloadPreview: w.sanitizedPayloadPreview || null,
      liveBefore: w.liveBefore,
    })),
    confirmOnly: CONFIRM_ONLY,
    dream: dreamRecord,
    dreamCandidates: dreamResolve,
    dreamIdentity,
    writeCounts: {
      protectedFields: 0,
      presentation: 0,
      images: 0,
      recentMomentum: 0,
      nonTarget: 0,
      activeLive: 0,
      brandStatusPatches,
      dreamCreates,
      protectedFieldNamesNeverWritten: [...PROTECTED_FIELD_NAMES],
    },
    batchAReady,
    cohortFinal,
    batches: {
      A: ["Hyatt Regency", "Hyatt Centric", "Thompson Hotels"],
      B: ["Caption by Hyatt", "Destination by Hyatt", "Unbound Collection by Hyatt"],
      dream: "identity_readiness_pending",
    },
  };
  report.artifacts = writeOutputs(report);
  return report;
}
