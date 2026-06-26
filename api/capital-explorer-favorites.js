import Airtable from "airtable";
import {
  DEAL_PROVIDER_LIST_FIELDS as F,
  EXPLORER_FAVORITE_LIST_STATUS,
  TABLE_DEAL_PROVIDER_LIST,
} from "../lib/capital-setup/airtable-capital-setup-fields.js";

const USERS_TABLE = process.env.USERS_TABLE_ID || "tbl6shiyz2wdUqE5F";

function getBase() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) return null;
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

function listTableName() {
  return process.env.CAPITAL_DEAL_PROVIDER_LIST_TABLE_ID || TABLE_DEAL_PROVIDER_LIST;
}

function airtableStringLiteral(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

async function resolveUserId(userId) {
  let id = String(userId || "").trim();
  if (id && id.startsWith("rec")) return id;

  const base = getBase();
  if (!base) return null;

  try {
    const users = await base(USERS_TABLE).select({ maxRecords: 1 }).firstPage();
    if (users.length > 0) return users[0].id;
  } catch (_) {
    /* ignore */
  }
  return null;
}

function linkedRecordIds(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.filter((id) => typeof id === "string" && id.startsWith("rec"));
  if (typeof value === "string" && value.startsWith("rec")) return [value];
  return [];
}

function explorerFavoritesFormula(userId) {
  const uid = airtableStringLiteral(userId);
  const status = airtableStringLiteral(EXPLORER_FAVORITE_LIST_STATUS);
  return `AND({${F.userId}} = '${uid}', {${F.listStatus}} = '${status}', NOT({${F.relatedDeal}}))`;
}

async function loadExplorerFavoriteRecordsForUser(userId) {
  const base = getBase();
  const table = listTableName();
  if (!base || !table || !userId) return [];

  const records = await base(table)
    .select({
      filterByFormula: explorerFavoritesFormula(userId),
      maxRecords: 500,
    })
    .all();

  return records;
}

function findFavoriteForProvider(records, providerId) {
  return (records || []).find((record) => {
    const ids = linkedRecordIds(record.fields[F.capitalProvider]);
    return ids.includes(providerId);
  });
}

function mapFavoriteRecord(record) {
  const fields = record.fields || {};
  const providerRaw = fields[F.capitalProvider];
  let providerId = null;
  if (Array.isArray(providerRaw) && providerRaw.length > 0) providerId = providerRaw[0];
  else if (typeof providerRaw === "string" && providerRaw.startsWith("rec")) providerId = providerRaw;

  return {
    id: record.id,
    providerId,
    listStatus: fields[F.listStatus] || null,
    dateAdded: fields[F.dateAdded] || null,
  };
}

/** GET /api/capital-explorer/favorites?userId=rec… */
export async function getCapitalExplorerFavorites(req, res) {
  try {
    const base = getBase();
    if (!base) return res.status(500).json({ error: "Airtable not configured" });

    const userId = await resolveUserId(req.query.userId);
    if (!userId) return res.json({ favorites: [], userId: null });

    const records = await loadExplorerFavoriteRecordsForUser(userId);
    const favorites = records.map(mapFavoriteRecord).filter((f) => f.providerId);

    res.json({ favorites, userId });
  } catch (error) {
    console.error("[capital-explorer-favorites] list failed:", error);
    res.status(500).json({ error: "Failed to fetch favorites", details: error.message });
  }
}

/** POST /api/capital-explorer/favorites  { userId, providerId, providerName? } */
export async function createCapitalExplorerFavorite(req, res) {
  try {
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    const base = getBase();
    if (!base) return res.status(500).json({ error: "Airtable not configured" });

    const { providerId, providerName } = req.body || {};
    const userId = await resolveUserId(req.body?.userId);

    if (!providerId || !String(providerId).startsWith("rec")) {
      return res.status(400).json({ error: "Missing or invalid providerId (Airtable record id)" });
    }
    if (!userId) {
      return res.status(400).json({
        error: "User not identified",
        help: "Sign in or set #airtable-user-id / ?userId=rec… for development",
      });
    }

    const userFavorites = await loadExplorerFavoriteRecordsForUser(userId);
    const existing = findFavoriteForProvider(userFavorites, providerId);
    if (existing) {
      return res.json({
        message: "Favorite already exists",
        favorite: mapFavoriteRecord(existing),
      });
    }

    const today = new Date().toISOString().slice(0, 10);
    const listName =
      String(providerName || "").trim() || `Explorer save — ${providerId.slice(0, 8)}`;

    const record = await base(listTableName()).create(
      {
        [F.listItemName]: listName,
        [F.userId]: [userId],
        [F.capitalProvider]: [providerId],
        [F.listStatus]: EXPLORER_FAVORITE_LIST_STATUS,
        [F.dateAdded]: today,
      },
      { typecast: true }
    );

    res.json({
      message: "Favorite created",
      favorite: mapFavoriteRecord(record),
    });
  } catch (error) {
    console.error("[capital-explorer-favorites] create failed:", error);
    res.status(500).json({ error: "Failed to create favorite", details: error.message });
  }
}

/** DELETE /api/capital-explorer/favorites/:favoriteId?userId=rec… */
export async function deleteCapitalExplorerFavorite(req, res) {
  try {
    if (req.method !== "DELETE") return res.status(405).json({ error: "Method not allowed" });

    const base = getBase();
    if (!base) return res.status(500).json({ error: "Airtable not configured" });

    const favoriteId = req.params.favoriteId || null;
    let { providerId } = req.query;
    const userId = await resolveUserId(req.query.userId);

    if (!favoriteId && !providerId) {
      return res.status(400).json({ error: "favoriteId or providerId is required" });
    }

    let recordId = favoriteId;

    if (!recordId && providerId && userId) {
      const userFavorites = await loadExplorerFavoriteRecordsForUser(userId);
      const match = findFavoriteForProvider(userFavorites, providerId);
      if (!match) {
        return res.json({ message: "Favorite not found", id: null });
      }
      recordId = match.id;
    }

    if (!recordId) {
      return res.status(400).json({ error: "Could not resolve favorite to delete" });
    }

    if (userId) {
      try {
        const record = await base(listTableName()).find(recordId);
        const fields = record.fields || {};
        const recordUserId = fields[F.userId];
        const recordUserIdValue = Array.isArray(recordUserId) ? recordUserId[0] : recordUserId;
        if (recordUserIdValue && recordUserIdValue !== userId) {
          return res.status(403).json({ error: "Unauthorized: You can only delete your own favorites" });
        }
        const dealIds = linkedRecordIds(fields[F.relatedDeal]);
        if (dealIds.length > 0) {
          return res.status(403).json({ error: "This list row is tied to a deal and cannot be removed from Explorer favorites" });
        }
      } catch (error) {
        console.warn("[capital-explorer-favorites] ownership check skipped:", error.message);
      }
    }

    await base(listTableName()).destroy(recordId);

    res.json({ message: "Favorite deleted", id: recordId });
  } catch (error) {
    console.error("[capital-explorer-favorites] delete failed:", error);
    res.status(500).json({ error: "Failed to delete favorite", details: error.message });
  }
}
