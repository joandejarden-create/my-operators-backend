import Airtable from "airtable";

const FIELDS = {
  userId: process.env.BRAND_EXPLORER_FAVORITES_USER_FIELD || "User_ID",
  brand: process.env.BRAND_EXPLORER_FAVORITES_BRAND_FIELD || "Brand",
  favoritedDate: process.env.BRAND_EXPLORER_FAVORITES_DATE_FIELD || "Favorited Date",
};

const USERS_TABLE = process.env.USERS_TABLE_ID || "tbl6shiyz2wdUqE5F";
const USER_FAVORITES_LINK_FIELD =
  process.env.BRAND_EXPLORER_USER_FAVORITES_LINK_FIELD || "Brand Explorer Favorites";

function getBase() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) return null;
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

function favoritesTableId() {
  return process.env.BRAND_EXPLORER_FAVORITES_TABLE_ID || "";
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

/** Load favorites via Users → Brand Explorer Favorites (link filters by rec id are unreliable). */
async function loadFavoriteRecordsForUser(userId) {
  const base = getBase();
  const tableId = favoritesTableId();
  if (!base || !tableId || !userId) return [];

  const userRec = await base(USERS_TABLE).find(userId);
  const favIds = linkedRecordIds(userRec.fields[USER_FAVORITES_LINK_FIELD]);
  if (!favIds.length) return [];

  const records = await Promise.all(
    favIds.map((id) =>
      base(tableId)
        .find(id)
        .catch(() => null)
    )
  );
  return records.filter(Boolean);
}

function findFavoriteForBrand(records, brandId) {
  return (records || []).find((record) => {
    const ids = linkedRecordIds(record.fields[FIELDS.brand]);
    return ids.includes(brandId);
  });
}

function mapFavoriteRecord(record) {
  const fields = record.fields || {};
  const brandRaw = fields[FIELDS.brand];
  let brandId = null;
  if (Array.isArray(brandRaw) && brandRaw.length > 0) brandId = brandRaw[0];
  else if (typeof brandRaw === "string" && brandRaw.startsWith("rec")) brandId = brandRaw;

  return {
    id: record.id,
    brandId,
    favoritedDate: fields[FIELDS.favoritedDate] || null,
  };
}

/** GET /api/brand-explorer/favorites?userId=rec… */
export async function getBrandExplorerFavorites(req, res) {
  try {
    const tableId = favoritesTableId();
    if (!tableId) {
      return res.status(503).json({
        error: "Brand Explorer favorites table not configured",
        help: "Set BRAND_EXPLORER_FAVORITES_TABLE_ID and run scripts/ensure-brand-explorer-favorites-table.mjs",
      });
    }

    const base = getBase();
    if (!base) return res.status(500).json({ error: "Airtable not configured" });

    const userId = await resolveUserId(req.query.userId);
    if (!userId) return res.json({ favorites: [], userId: null });

    const records = await loadFavoriteRecordsForUser(userId);
    const favorites = records.map(mapFavoriteRecord).filter((f) => f.brandId);

    res.json({ favorites, userId });
  } catch (error) {
    console.error("[brand-explorer-favorites] list failed:", error);
    res.status(500).json({ error: "Failed to fetch favorites", details: error.message });
  }
}

/** POST /api/brand-explorer/favorites  { userId, brandId } */
export async function createBrandExplorerFavorite(req, res) {
  try {
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    const tableId = favoritesTableId();
    if (!tableId) {
      return res.status(503).json({
        error: "Brand Explorer favorites table not configured",
        help: "Set BRAND_EXPLORER_FAVORITES_TABLE_ID",
      });
    }

    const base = getBase();
    if (!base) return res.status(500).json({ error: "Airtable not configured" });

    const { brandId } = req.body || {};
    const userId = await resolveUserId(req.body?.userId);

    if (!brandId || !String(brandId).startsWith("rec")) {
      return res.status(400).json({ error: "Missing or invalid brandId (Airtable record id)" });
    }
    if (!userId) {
      return res.status(400).json({
        error: "User not identified",
        help: "Sign in or set #airtable-user-id / ?userId=rec… for development",
      });
    }

    const userFavorites = await loadFavoriteRecordsForUser(userId);
    const existing = findFavoriteForBrand(userFavorites, brandId);
    if (existing) {
      const fav = mapFavoriteRecord(existing);
      return res.json({ message: "Favorite already exists", favorite: fav });
    }

    const record = await base(tableId).create(
      {
        [FIELDS.userId]: [userId],
        [FIELDS.brand]: [brandId],
        [FIELDS.favoritedDate]: new Date().toISOString(),
      },
      { typecast: true }
    );

    res.json({
      message: "Favorite created",
      favorite: mapFavoriteRecord(record),
    });
  } catch (error) {
    console.error("[brand-explorer-favorites] create failed:", error);
    res.status(500).json({ error: "Failed to create favorite", details: error.message });
  }
}

/** DELETE /api/brand-explorer/favorites/:favoriteId?userId=rec… */
export async function deleteBrandExplorerFavorite(req, res) {
  try {
    if (req.method !== "DELETE") return res.status(405).json({ error: "Method not allowed" });

    const tableId = favoritesTableId();
    if (!tableId) {
      return res.status(503).json({ error: "Brand Explorer favorites table not configured" });
    }

    const base = getBase();
    if (!base) return res.status(500).json({ error: "Airtable not configured" });

    const favoriteId = req.params.favoriteId || null;
    let { brandId } = req.query;
    const userId = await resolveUserId(req.query.userId);

    if (!favoriteId && !brandId) {
      return res.status(400).json({ error: "favoriteId or brandId is required" });
    }

    let recordId = favoriteId;

    if (!recordId && brandId && userId) {
      const userFavorites = await loadFavoriteRecordsForUser(userId);
      const match = findFavoriteForBrand(userFavorites, brandId);
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
        const record = await base(tableId).find(recordId);
        const fields = record.fields || {};
        const recordUserId = fields[FIELDS.userId];
        const recordUserIdValue = Array.isArray(recordUserId) ? recordUserId[0] : recordUserId;
        if (recordUserIdValue && recordUserIdValue !== userId) {
          return res.status(403).json({ error: "Unauthorized: You can only delete your own favorites" });
        }
      } catch (error) {
        console.warn("[brand-explorer-favorites] ownership check skipped:", error.message);
      }
    }

    await base(tableId).destroy(recordId);

    res.json({ message: "Favorite deleted", id: recordId });
  } catch (error) {
    console.error("[brand-explorer-favorites] delete failed:", error);
    res.status(500).json({ error: "Failed to delete favorite", details: error.message });
  }
}
