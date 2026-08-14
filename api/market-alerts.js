import Airtable from "airtable";
import { sanitizeMarketAlertText } from "./lib/market-alerts-rss-airtable.js";
import { audienceWorthField, MAP_INTEL } from "./lib/market-alerts-intelligence-map.js";
import { intelligencePayloadForAudience } from "../lib/market-alerts-intelligence.js";
import { resolveMarketAlertsAudience } from "../lib/market-alerts-audience-resolve.js";
import { dedupeFeedItemsByEntityKey } from "../lib/market-alerts-correlation.js";
import { sanitizeUserFacingTags } from "../lib/market-alerts-user-tags.js";

// Airtable table + field configuration – MUST match contract in spec
const TABLE_ALERTS = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
const TABLE_USER_STATUS = process.env.AIRTABLE_TABLE_USER_STATUS || "UserAlertStatus";

const F_ALERT = {
  title: "Title",
  dedupeId: "Dedupe ID",
  summary: "Summary",
  sourceName: "Source Name",
  sourceUrl: "Source URL",
  publishedAt: "Published At",
  category: "Category",
  regionGroup: "Region Group",
  priority: "Priority",
  tags: "Tags",
};

const F_STATUS = {
  table: TABLE_USER_STATUS,
  userId: "User ID",
  alert: "Alert",
  saved: "Saved",
  dismissed: "Dismissed",
  read: "Read",
  savedAt: "Saved At",
  dismissedAt: "Dismissed At",
  readAt: "Read At",
};

function getBase() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) return null;
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
}

function getCurrentUserId(req) {
  // Try multiple possible auth sources; fallbacks allowed
  if (req.user && (req.user.id || req.user.email)) return req.user.id || req.user.email;
  if (req.headers["x-user-id"]) return String(req.headers["x-user-id"]);
  if (req.headers["x-user-email"]) return String(req.headers["x-user-email"]);
  if (req.query && req.query.userId) return String(req.query.userId);
  if (req.body && req.body.userId) return String(req.body.userId);
  return null;
}

function parseTimeWindowParam(value) {
  switch (value) {
    case "24h":
      return 1;
    case "30d":
      return 30;
    case "7d":
      return 7;
    case "all":
      return null; // no time filter – show all time
    default:
      return 7;
  }
}

function buildAlertsFilterFormula({
  category,
  regionGroup,
  days,
  search,
  worthReviewing,
  actionable,
  audience,
}) {
  let formulaParts = [];

  formulaParts.push(
    `OR(BLANK({${MAP_INTEL.intelligenceTreatment}}), {${MAP_INTEL.intelligenceTreatment}} != 'IGNORE')`
  );

  // Time window: include if Published At is within last N days, OR if Published At is blank (e.g. manually added records)
  if (days) {
    formulaParts.push(
      `OR(BLANK({${F_ALERT.publishedAt}}), IS_AFTER({${F_ALERT.publishedAt}}, DATEADD(NOW(), -${days}, 'days')))`
    );
  }

  if (category && category !== "all") {
    formulaParts.push(`{${F_ALERT.category}} = '${escapeAirtableString(category)}'`);
  }

  if (regionGroup && regionGroup !== "all") {
    formulaParts.push(
      `{${F_ALERT.regionGroup}} = '${escapeAirtableString(regionGroup)}'`
    );
  }

  if (search) {
    const term = escapeAirtableString(search.toLowerCase());
    const concat = `LOWER({${F_ALERT.title}} & ' ' & {${F_ALERT.summary}} & ' ' & {${F_ALERT.sourceName}})`;
    formulaParts.push(`SEARCH('${term}', ${concat}) > 0`);
  }

  if (worthReviewing && audience) {
    const worthField = audienceWorthField(audience);
    if (worthField) {
      formulaParts.push(`{${worthField}} = TRUE()`);
    }
  }

  // Actionable uses Worth Reviewing as Airtable superset; actionable is enforced in JS.

  if (!formulaParts.length) return "";
  if (formulaParts.length === 1) return formulaParts[0];
  return `AND(${formulaParts.join(",")})`;
}

/** Escape user input for Airtable filterByFormula: backslashes, single quotes, remove newlines */
function escapeAirtableString(str) {
  if (str == null) return "";
  return String(str)
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'")
    .replace(/[\r\n]+/g, " ")
    .trim();
}

function escapeAirtableValue(v) {
  return escapeAirtableString(v);
}

const USER_STATUS_CHUNK_SIZE = 30;

async function fetchUserStatusForAlerts(base, userId, alertIds) {
  if (!userId || !alertIds.length) return {};
  const table = base(F_STATUS.table);
  const escapedUserId = escapeAirtableString(userId);
  const byAlert = {};

  for (let i = 0; i < alertIds.length; i += USER_STATUS_CHUNK_SIZE) {
    const chunk = alertIds.slice(i, i + USER_STATUS_CHUNK_SIZE);
    const orParts = chunk.map(
      (id) => `FIND('${escapeAirtableString(id)}', ARRAYJOIN({${F_STATUS.alert}})) > 0`
    );
    const formula = `AND({${F_STATUS.userId}} = '${escapedUserId}', OR(${orParts.join(",")}))`;

    const records = await table
      .select({
        filterByFormula: formula,
        maxRecords: chunk.length * 2,
      })
      .all();

    records.forEach((r) => {
      const linked = r.fields[F_STATUS.alert];
      if (!linked || !linked.length) return;
      const alertId = linked[0];
      byAlert[alertId] = {
        id: r.id,
        saved: !!r.fields[F_STATUS.saved],
        dismissed: !!r.fields[F_STATUS.dismissed],
        read: !!r.fields[F_STATUS.read],
        savedAt: r.fields[F_STATUS.savedAt] || null,
        dismissedAt: r.fields[F_STATUS.dismissedAt] || null,
        readAt: r.fields[F_STATUS.readAt] || null,
      };
    });
  }

  return byAlert;
}

async function upsertUserStatus(base, userId, alertId, changes) {
  if (!userId || !alertId) {
    throw new Error("userId and alertId are required for UserAlertStatus upsert.");
  }

  const table = base(F_STATUS.table);
  const filter = `AND({${F_STATUS.userId}} = '${escapeAirtableString(
    userId
  )}', FIND('${escapeAirtableString(alertId)}', ARRAYJOIN({${F_STATUS.alert}})) > 0)`;

  const existing = await table
    .select({
      filterByFormula: filter,
      maxRecords: 1,
    })
    .all();

  if (existing.length) {
    await table.update(existing[0].id, changes);
    return existing[0].id;
  } else {
    const created = await table.create({
      [F_STATUS.userId]: userId,
      [F_STATUS.alert]: [alertId],
      ...changes,
    });
    return created.id;
  }
}

function mapAlertListItem(r, userStatusMap, audience) {
  const fields = r.fields;
  const status = userStatusMap[r.id] || null;
  const intelligence = intelligencePayloadForAudience(fields, audience);
  const userTags = sanitizeUserFacingTags(fields[F_ALERT.tags] || []);
  return {
    id: r.id,
    fields: {
      [F_ALERT.title]: sanitizeMarketAlertText(fields[F_ALERT.title] || ""),
      [F_ALERT.summary]: sanitizeMarketAlertText(fields[F_ALERT.summary] || "", {
        preserveWhitespace: true,
      }),
      [F_ALERT.sourceName]: sanitizeMarketAlertText(fields[F_ALERT.sourceName] || ""),
      [F_ALERT.sourceUrl]: fields[F_ALERT.sourceUrl] || "",
      [F_ALERT.publishedAt]: fields[F_ALERT.publishedAt] || null,
      [F_ALERT.category]: fields[F_ALERT.category] || "",
      [F_ALERT.regionGroup]: fields[F_ALERT.regionGroup] || "Global",
      [F_ALERT.priority]: fields[F_ALERT.priority] || "",
      [F_ALERT.tags]: userTags,
    },
    intelligence,
    userStatus: status,
  };
}

function isIgnoredAlertItem(item) {
  return item?.intelligence?.treatment === "IGNORE";
}

// GET /api/market-alerts
export async function listMarketAlerts(req, res) {
  try {
    const base = getBase();
    if (!base) {
      return res.status(503).json({ error: "Airtable not configured" });
    }

    const {
      category,
      regionGroup,
      timeWindow = "7d",
      search,
      includeDismissed,
      limit = "100",
      worthReviewing,
      actionable,
    } = req.query || {};

    const days = parseTimeWindowParam(timeWindow);
    const max = Math.min(Math.max(parseInt(String(limit), 10) || 100, 1), 200);
    const currentUserId = getCurrentUserId(req);
    const includeDismissedBool = String(includeDismissed).toLowerCase() === "true";
    const actionableBool =
      String(actionable || "").toLowerCase() === "1" ||
      String(actionable || "").toLowerCase() === "true";
    const worthReviewingBool =
      !actionableBool &&
      (String(worthReviewing || "").toLowerCase() === "1" ||
        String(worthReviewing || "").toLowerCase() === "true");

    const { audience, source: audienceSource } = await resolveMarketAlertsAudience(req);

    if ((worthReviewingBool || actionableBool) && !audience) {
      return res.json({
        items: [],
        meta: {
          totalReturned: 0,
          timeWindow,
          category: category || "all",
          regionGroup: regionGroup || "all",
          search: search || "",
          worthReviewing: worthReviewingBool,
          actionable: actionableBool,
          audience: null,
          audienceSource,
          emptyReason: "audience_unavailable",
        },
      });
    }

    const filterByFormula = buildAlertsFilterFormula({
      category,
      regionGroup,
      days,
      search,
      worthReviewing: worthReviewingBool || actionableBool,
      audience,
    });

    const selectParams = {
      sort: [{ field: F_ALERT.publishedAt, direction: "desc" }],
      maxRecords: max,
    };
    if (filterByFormula) {
      selectParams.filterByFormula = filterByFormula;
    }

    const records = await base(TABLE_ALERTS).select(selectParams).all();

    if (records.length === 0) {
      console.warn(
        "[market-alerts] listMarketAlerts returned 0 items. filterByFormula:",
        filterByFormula || "(none)"
      );
      console.warn(
        "[market-alerts] Verify MarketAlerts has records and 'Published At' field exists and is populated as Airtable date."
      );
    }

    const alertIds = records.map((r) => r.id);
    const userStatusMap = currentUserId
      ? await fetchUserStatusForAlerts(base, currentUserId, alertIds)
      : {};

    let items = records.map((r) => mapAlertListItem(r, userStatusMap, audience));
    items = items.filter((item) => !isIgnoredAlertItem(item));

    if (currentUserId && !includeDismissedBool) {
      items = items.filter((item) => !(item.userStatus && item.userStatus.dismissed));
    }

    if (worthReviewingBool) {
      items = items.filter(
        (item) => item.intelligence?.worthReviewing && !item.intelligence?.actionable
      );
      items = dedupeFeedItemsByEntityKey(items, { windowDays: 14 });
    }

    if (actionableBool) {
      items = items.filter((item) => item.intelligence?.actionable);
      items = dedupeFeedItemsByEntityKey(items, { windowDays: 14 });
    }

    return res.json({
      items,
      meta: {
        totalReturned: items.length,
        timeWindow,
        category: category || "all",
        regionGroup: regionGroup || "all",
        search: search || "",
        worthReviewing: worthReviewingBool,
        actionable: actionableBool,
        audience,
        audienceSource,
      },
    });
  } catch (err) {
    console.error("Error in listMarketAlerts:", err);
    return res.status(500).json({ error: "Failed to load market alerts" });
  }
}

// GET /api/market-alerts/rail
export async function getMarketAlertsRail(req, res) {
  try {
    const base = getBase();
    if (!base) {
      return res.status(503).json({ error: "Airtable not configured" });
    }

    const currentUserId = getCurrentUserId(req);
    const { audience, source: audienceSource } = await resolveMarketAlertsAudience(req);

    const ignoreFilter = `OR(BLANK({${MAP_INTEL.intelligenceTreatment}}), {${MAP_INTEL.intelligenceTreatment}} != 'IGNORE')`;

    const latestRecords = await base(TABLE_ALERTS)
      .select({
        filterByFormula: ignoreFilter,
        sort: [{ field: F_ALERT.publishedAt, direction: "desc" }],
        maxRecords: 10,
      })
      .all();

    let worthRecords = [];
    if (audience) {
      const worthField = audienceWorthField(audience);
      if (worthField) {
        try {
          worthRecords = await base(TABLE_ALERTS)
            .select({
              filterByFormula: `AND(${ignoreFilter}, {${worthField}} = TRUE())`,
              sort: [{ field: F_ALERT.publishedAt, direction: "desc" }],
              maxRecords: 24,
            })
            .all();
        } catch (err) {
          console.warn(
            "[market-alerts] worth reviewing rail query failed (fields may be missing):",
            err.message || err
          );
        }
      }
    }

    if (latestRecords.length === 0) {
      console.warn(
        "[market-alerts] getMarketAlertsRail returned 0 items. No filterByFormula (rail uses sort by Published At only)."
      );
      console.warn(
        "[market-alerts] Verify MarketAlerts has records and 'Published At' field exists and is populated as Airtable date."
      );
    }

    const allForStatus = [...worthRecords, ...latestRecords];
    const alertIds = [...new Set(allForStatus.map((r) => r.id))];
    const userStatusMap = currentUserId
      ? await fetchUserStatusForAlerts(base, currentUserId, alertIds)
      : {};

    const mappedWorth = worthRecords.map((r) => mapAlertListItem(r, userStatusMap, audience));

    const actionableNow = dedupeFeedItemsByEntityKey(
      mappedWorth.filter((item) => item.intelligence?.actionable),
      { windowDays: 14 }
    )
      .filter((item) => !isIgnoredAlertItem(item))
      .slice(0, 5);

    const actionableIds = new Set(actionableNow.map((i) => i.id));

    const worthReviewing = dedupeFeedItemsByEntityKey(mappedWorth, { windowDays: 14 })
      .filter(
        (item) =>
          !isIgnoredAlertItem(item) &&
          !actionableIds.has(item.id) &&
          item.intelligence?.worthReviewing &&
          !item.intelligence?.actionable
      )
      .slice(0, 5);

    const liveFeed = latestRecords
      .map((r) => mapAlertListItem(r, userStatusMap, audience))
      .filter((item) => !isIgnoredAlertItem(item));
    const latestMarketActivity = liveFeed.slice(0, 5);

    return res.json({
      actionableNow,
      worthReviewing,
      latestMarketActivity,
      liveFeed,
      topRead: latestMarketActivity,
      meta: { audience, audienceSource },
    });
  } catch (err) {
    console.error("Error in getMarketAlertsRail:", err);
    return res.status(500).json({ error: "Failed to load rail data" });
  }
}

// POST /api/market-alerts/:id/read
export async function markAlertRead(req, res) {
  try {
    const alertId = req.params.id;
    const userId = getCurrentUserId(req);
    if (!alertId) return res.status(400).json({ error: "Alert ID is required" });
    if (!userId) return res.status(401).json({ error: "Authentication required" });

    const base = getBase();
    if (!base) return res.status(503).json({ error: "Airtable not configured" });

    const nowIso = new Date().toISOString();
    await upsertUserStatus(base, userId, alertId, {
      [F_STATUS.read]: true,
      [F_STATUS.readAt]: nowIso,
    });

    return res.json({ success: true });
  } catch (err) {
    console.error("Error in markAlertRead:", err);
    return res.status(500).json({ error: "Failed to update read state" });
  }
}

// POST /api/market-alerts/:id/save
export async function saveAlert(req, res) {
  try {
    const alertId = req.params.id;
    const userId = getCurrentUserId(req);
    if (!alertId) return res.status(400).json({ error: "Alert ID is required" });
    if (!userId) return res.status(401).json({ error: "Authentication required" });

    const base = getBase();
    if (!base) return res.status(503).json({ error: "Airtable not configured" });

    const saved = req.body && typeof req.body.saved === "boolean" ? req.body.saved : true;
    const changes = { [F_STATUS.saved]: saved };
    if (saved) changes[F_STATUS.savedAt] = new Date().toISOString();

    await upsertUserStatus(base, userId, alertId, changes);
    return res.json({ success: true });
  } catch (err) {
    console.error("Error in saveAlert:", err);
    return res.status(500).json({ error: "Failed to update saved state" });
  }
}

// POST /api/market-alerts/:id/dismiss
export async function dismissAlert(req, res) {
  try {
    const alertId = req.params.id;
    const userId = getCurrentUserId(req);
    if (!alertId) return res.status(400).json({ error: "Alert ID is required" });
    if (!userId) return res.status(401).json({ error: "Authentication required" });

    const base = getBase();
    if (!base) return res.status(503).json({ error: "Airtable not configured" });

    const dismissed =
      req.body && typeof req.body.dismissed === "boolean" ? req.body.dismissed : true;
    const changes = { [F_STATUS.dismissed]: dismissed };
    if (dismissed) changes[F_STATUS.dismissedAt] = new Date().toISOString();

    await upsertUserStatus(base, userId, alertId, changes);
    return res.json({ success: true });
  } catch (err) {
    console.error("Error in dismissAlert:", err);
    return res.status(500).json({ error: "Failed to update dismissed state" });
  }
}

