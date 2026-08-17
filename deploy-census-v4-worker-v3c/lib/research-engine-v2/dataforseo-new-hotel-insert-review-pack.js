/**
 * Founder/steward insert review pack for DataForSEO new-hotel candidates.
 * Queue-only — never inserts into Hotel Property Census.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const INSERT_REVIEW_PACK_VERSION =
  "dataforseo-new-hotel-insert-review-pack-v1";

export const INSERT_RECOMMENDED_ACTIONS = Object.freeze([
  "approve_insert_high",
  "steward_review",
  "duplicate_review",
  "reject",
]);

const HOTEL_CATEGORY_RE =
  /\b(hotel|resort|boutique hotel|aparthotel|inn|lodge|宿)\b/i;
const NON_HOTEL_RE =
  /\b(restaurant|bar|cafe|coffee|hostel|motel|camping|apartment rental|vacation rental|airbnb|spa(?! hotel)|gym|clinic|hospital|office|cowork)\b/i;

const BRAND_HINT_RE =
  /\b(marriott|hilton|hyatt|ihg|intercontinental|holiday inn|sheraton|westin|radisson|accor|novotel|ibis|mercure|sofitel|wyndham|best western|four seasons|ritz[- ]carlton|st\.?\s*regis|kimpton|curio|autograph|tribute|design hotels|melia|meliá|barcelo|barceló|nh hotels|choice|ascend|comfort inn|quality inn|days inn|equities|arbor)\b/i;

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

function hostFromUrl(url) {
  try {
    return new URL(String(url || "")).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function stripUtm(url) {
  try {
    const u = new URL(String(url || ""));
    [...u.searchParams.keys()].forEach((k) => {
      if (/^utm_/i.test(k)) u.searchParams.delete(k);
    });
    return u.toString();
  } catch {
    return String(url || "").trim() || null;
  }
}

/**
 * Locate the newest candidate-insert-queue.json under reports autopilot runs.
 * @param {{ root?: string, queuePath?: string|null, runDir?: string|null }} [opts]
 */
export function resolveLatestCandidateInsertQueue(opts = {}) {
  if (opts.queuePath && fs.existsSync(opts.queuePath)) {
    return opts.queuePath;
  }
  if (opts.runDir) {
    const local = path.join(opts.runDir, "candidate-insert-queue.json");
    if (fs.existsSync(local)) return local;
  }
  const root = opts.root || ROOT;
  const autoRoot = path.join(root, "reports/research-engine-v2/autopilot");
  if (!fs.existsSync(autoRoot)) return null;
  const dirs = fs
    .readdirSync(autoRoot, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name)
    .sort()
    .reverse();
  for (const name of dirs) {
    const p = path.join(autoRoot, name, "candidate-insert-queue.json");
    if (fs.existsSync(p)) return p;
  }
  return null;
}

function scoreCandidate(row) {
  const cand = row.candidate || {};
  const raw = cand.raw || {};
  const match = row.match || {};
  const near = row.near_duplicates || [];
  const title = String(raw.title || cand.title || "").trim();
  const category = String(raw.category || cand.category || "").trim();
  const address = String(raw.address || cand.address || "").trim();
  const website = String(raw.website || cand.website || "").trim();
  const placeId = String(raw.place_id || cand.place_id || "").trim();
  const lat = raw.latitude ?? cand.latitude ?? null;
  const lng = raw.longitude ?? cand.longitude ?? null;
  const market = String(row.market_label || row.market_id || "").trim();
  const discovery =
    String(row.discovery_class || cand.discovery_class || "").trim();
  const matchClass = String(match.match_class || cand.match_class || "").trim();
  const nameSim = Number(match.name_similarity ?? cand.match_confidence ?? 0);

  let score = 0;
  const reasonsFor = [];
  const reasonsAgainst = [];

  if (/new_hotel_candidate_high/i.test(discovery)) {
    score += 40;
    reasonsFor.push("new_hotel_candidate_high");
  } else if (/new_hotel_candidate/i.test(discovery)) {
    score += 25;
    reasonsFor.push("new_hotel_candidate");
  }

  if (near.length === 0 && !/match_high|match_medium/i.test(matchClass)) {
    score += 20;
    reasonsFor.push("no_duplicate_risk_signal");
  } else {
    score -= 25;
    reasonsAgainst.push("duplicate_or_near_match_signal");
  }

  if (BRAND_HINT_RE.test(title)) {
    score += 15;
    reasonsFor.push("branded_or_operator_recognizable");
  }

  if (address && website) {
    score += 15;
    reasonsFor.push("address_and_website_present");
  } else if (address) {
    score += 8;
    reasonsFor.push("address_present");
  } else {
    reasonsAgainst.push("missing_address");
  }

  if (website) {
    score += 5;
  } else {
    reasonsAgainst.push("missing_website");
  }

  if (market) {
    score += 8;
    reasonsFor.push("market_relevance");
  }

  if (HOTEL_CATEGORY_RE.test(category) && !NON_HOTEL_RE.test(category)) {
    score += 12;
    reasonsFor.push("clean_hotel_category");
  } else if (NON_HOTEL_RE.test(category) || NON_HOTEL_RE.test(title)) {
    score -= 40;
    reasonsAgainst.push("non_hotel_category");
  } else if (!category) {
    reasonsAgainst.push("missing_category");
  }

  if (placeId) score += 4;
  if (lat != null && lng != null) score += 4;

  if (nameSim >= 0.75 && /match_/i.test(matchClass)) {
    score -= 15;
    reasonsAgainst.push("high_name_similarity_to_existing");
  }

  return {
    score,
    reasons_for_insert: reasonsFor,
    reasons_hold_or_reject: reasonsAgainst,
    signals: {
      title,
      category,
      address: Boolean(address),
      website: Boolean(website),
      place_id: Boolean(placeId),
      coords: lat != null && lng != null,
      market,
      discovery,
      match_class: matchClass,
      name_similarity: nameSim,
      near_duplicate_count: near.length,
    },
  };
}

function recommendAction(row, scored) {
  const category = scored.signals.category || "";
  const title = scored.signals.title || "";
  const matchClass = scored.signals.match_class || "";
  const nearCount = scored.signals.near_duplicate_count || 0;

  if (NON_HOTEL_RE.test(category) || NON_HOTEL_RE.test(title)) {
    return {
      recommended_action: "reject",
      confidence: "High",
      reason_for_insert_recommendation: null,
      reason_for_hold_or_reject: "non_hotel_or_non_lodging_category",
    };
  }

  if (
    nearCount > 0 ||
    /match_high|match_medium/i.test(matchClass) ||
    scored.signals.name_similarity >= 0.8
  ) {
    return {
      recommended_action: "duplicate_review",
      confidence: "Medium",
      reason_for_insert_recommendation: null,
      reason_for_hold_or_reject:
        "possible_duplicate_or_near_match_against_existing_census",
    };
  }

  const strong =
    scored.score >= 70 &&
    scored.signals.address &&
    (scored.signals.website || scored.signals.place_id) &&
    HOTEL_CATEGORY_RE.test(category);

  if (strong) {
    return {
      recommended_action: "approve_insert_high",
      confidence: "High",
      reason_for_insert_recommendation:
        "high-scoring new hotel candidate with address + provenance and no duplicate risk",
      reason_for_hold_or_reject: null,
    };
  }

  return {
    recommended_action: "steward_review",
    confidence: scored.score >= 45 ? "Medium" : "Low",
    reason_for_insert_recommendation:
      scored.reasons_for_insert.join("; ") || null,
    reason_for_hold_or_reject:
      scored.reasons_hold_or_reject.join("; ") ||
      "needs_steward_confirmation_before_insert",
  };
}

/**
 * Build a ranked insert review pack from a candidate queue payload.
 * @param {{ queue?: object[], count?: number }|object[]} queuePayload
 * @param {{ sourceQueuePath?: string|null }} [meta]
 */
export function buildInsertReviewPack(queuePayload, meta = {}) {
  const queue = Array.isArray(queuePayload)
    ? queuePayload
    : queuePayload?.queue || [];

  const rows = queue.map((row, idx) => {
    const cand = row.candidate || {};
    const raw = cand.raw || {};
    const match = row.match || {};
    const scored = scoreCandidate(row);
    const action = recommendAction(row, scored);
    const website = stripUtm(raw.website || cand.website || "");
    return {
      rank: 0,
      candidate_name: String(raw.title || cand.title || "").trim() || null,
      market: row.market_label || row.market_id || null,
      city: row.city || extractCityHint(raw.address) || null,
      country: row.country || null,
      address: raw.address || cand.address || null,
      latitude_candidate: raw.latitude ?? cand.latitude ?? null,
      longitude_candidate: raw.longitude ?? cand.longitude ?? null,
      website_or_source_url: website,
      website_host: hostFromUrl(website),
      place_id: raw.place_id || cand.place_id || null,
      external_id: raw.cid || cand.cid || raw.place_id || null,
      category: raw.category || cand.category || null,
      duplicate_check_result: {
        match_class: match.match_class || cand.match_class || null,
        matched_census_record_id:
          match.record_id || cand.matched_census_record_id || null,
        name_similarity: match.name_similarity ?? null,
        city_match: match.city_match ?? null,
        near_duplicates: row.near_duplicates || [],
        reasons: match.reasons || cand.match_reasons || [],
      },
      confidence: action.confidence,
      reason_for_insert_recommendation: action.reason_for_insert_recommendation,
      reason_for_hold_or_reject: action.reason_for_hold_or_reject,
      nearest_existing_census_match: match.record_id
        ? {
            record_id: match.record_id,
            match_class: match.match_class,
            match_confidence: match.match_confidence,
            name_similarity: match.name_similarity,
          }
        : null,
      recommended_action: action.recommended_action,
      ranking_score: scored.score,
      ranking_signals: scored.signals,
      source_index: idx,
      inserts: false,
    };
  });

  rows.sort((a, b) => {
    const order = {
      approve_insert_high: 0,
      steward_review: 1,
      duplicate_review: 2,
      reject: 3,
    };
    const ao = order[a.recommended_action] ?? 9;
    const bo = order[b.recommended_action] ?? 9;
    if (ao !== bo) return ao - bo;
    return b.ranking_score - a.ranking_score;
  });
  rows.forEach((r, i) => {
    r.rank = i + 1;
  });

  const byAction = Object.fromEntries(
    INSERT_RECOMMENDED_ACTIONS.map((a) => [
      a,
      rows.filter((r) => r.recommended_action === a).length,
    ])
  );

  return {
    ok: true,
    version: INSERT_REVIEW_PACK_VERSION,
    generated_at: new Date().toISOString(),
    inserts: 0,
    note: "Review pack only — ENABLE_DATAFORSEO_LOCAL_INSERTS + ENABLE_HIGH_CONFIDENCE_INSERTS required before any insert",
    source_queue_path: meta.sourceQueuePath || null,
    candidate_count: rows.length,
    action_counts: byAction,
    candidates: rows,
    top_35: rows.slice(0, 35),
    production_target: {
      base: "Deal Capture Platform",
      table: "Hotel Property Census",
      table_id: "tbl9aY5ijiuIzzWam",
      role: "insert_review_only_no_writes",
    },
    brand_setup_writes: 0,
    brand_explorer_writes: 0,
  };
}

function extractCityHint(address) {
  const s = String(address || "");
  if (!s) return null;
  const parts = s.split(",").map((p) => p.trim()).filter(Boolean);
  if (parts.length >= 2) return parts[parts.length - 2] || null;
  return null;
}

export function renderInsertReviewPackMarkdown(pack) {
  const top = pack.top_35 || pack.candidates?.slice(0, 35) || [];
  const lines = [
    `# DataForSEO New Hotel Insert Review Pack`,
    ``,
    `**Status:** queue-only (no inserts)`,
    `**Version:** \`${pack.version}\``,
    `**Generated:** ${pack.generated_at}`,
    `**Candidates:** **${pack.candidate_count}**`,
    `**Source queue:** \`${pack.source_queue_path || "n/a"}\``,
    ``,
    `## Action counts`,
    ``,
    ...Object.entries(pack.action_counts || {}).map(
      ([k, n]) => `- \`${k}\`: **${n}**`
    ),
    ``,
    `## Top ${Math.min(35, top.length)} candidates`,
    ``,
    `| Rank | Name | Market | Action | Confidence | Score | Nearest Census |`,
    `| ---: | --- | --- | --- | --- | ---: | --- |`,
    ...top.map((c) => {
      const nearest =
        c.nearest_existing_census_match?.record_id ||
        c.duplicate_check_result?.matched_census_record_id ||
        "—";
      return `| ${c.rank} | ${escapeMd(c.candidate_name)} | ${escapeMd(c.market)} | \`${c.recommended_action}\` | ${c.confidence} | ${c.ranking_score} | \`${nearest}\` |`;
    }),
    ``,
    `## Candidate detail (top 35)`,
    ``,
  ];
  for (const c of top) {
    lines.push(
      `### ${c.rank}. ${c.candidate_name || "(unnamed)"}`,
      ``,
      `- Market / city / country: ${c.market || "—"} / ${c.city || "—"} / ${c.country || "—"}`,
      `- Address: ${c.address || "—"}`,
      `- Lat/long candidate: ${c.latitude_candidate ?? "—"}, ${c.longitude_candidate ?? "—"}`,
      `- Website/source: ${c.website_or_source_url || "—"}`,
      `- Place / external ID: \`${c.place_id || "—"}\` / \`${c.external_id || "—"}\``,
      `- Category: ${c.category || "—"}`,
      `- Duplicate check: match=\`${c.duplicate_check_result?.match_class || "—"}\` near=${c.duplicate_check_result?.near_duplicates?.length || 0}`,
      `- Recommended: **\`${c.recommended_action}\`** (confidence ${c.confidence})`,
      `- Insert reason: ${c.reason_for_insert_recommendation || "—"}`,
      `- Hold/reject reason: ${c.reason_for_hold_or_reject || "—"}`,
      ``
    );
  }
  lines.push(
    `## Constraints`,
    ``,
    `- No automatic inserts in this pack`,
    `- Hotel Property Census only when founder later enables insert flags`,
    `- Brand Setup / Brand Explorer / owner / operator / date writes: never`,
    ``
  );
  return lines.join("\n");
}

function escapeMd(s) {
  return String(s || "—").replace(/\|/g, "\\|");
}

/**
 * Build + write founder insert review pack artifacts.
 */
export function writeDataForSeoNewHotelInsertReviewPack(opts = {}) {
  const queuePath = resolveLatestCandidateInsertQueue(opts);
  if (!queuePath) {
    const empty = buildInsertReviewPack([], { sourceQueuePath: null });
    empty.ok = false;
    empty.error = "candidate_insert_queue_not_found";
    return empty;
  }
  const raw = JSON.parse(fs.readFileSync(queuePath, "utf8"));
  const pack = buildInsertReviewPack(raw, { sourceQueuePath: queuePath });
  const reportsDir = path.join(
    opts.root || ROOT,
    "reports/research-engine-v2"
  );
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(
    reportsDir,
    "dataforseo-new-hotel-insert-review-pack.json"
  );
  const mdPath = path.join(
    reportsDir,
    "dataforseo-new-hotel-insert-review-pack.md"
  );
  const md = renderInsertReviewPackMarkdown(pack);
  fs.writeFileSync(jsonPath, JSON.stringify(pack, null, 2), "utf8");
  fs.writeFileSync(mdPath, md, "utf8");
  if (opts.runDir) {
    fs.mkdirSync(opts.runDir, { recursive: true });
    fs.writeFileSync(
      path.join(opts.runDir, "dataforseo-new-hotel-insert-review-pack.json"),
      JSON.stringify(pack, null, 2),
      "utf8"
    );
    fs.writeFileSync(
      path.join(opts.runDir, "dataforseo-new-hotel-insert-review-pack.md"),
      md,
      "utf8"
    );
  }
  pack.report_paths = { json: jsonPath, md: mdPath };
  return pack;
}
