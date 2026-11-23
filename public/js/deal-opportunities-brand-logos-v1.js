/**
 * DealSignal — brand logo URL resolution for Hotel / Portfolio column.
 * Prefer verified CDN / absolute URLs from Brand Library; never blank white squares.
 * CUSTOMER_BLOCKED. No Airtable writes.
 */
(function (root) {
  "use strict";

  const DEFAULT_ALIASES = {
    hilton: "hilton hotels & resorts",
    "hilton hotels": "hilton hotels & resorts",
    kimpton: "kimpton hotels",
    "kimpton (ihg)": "kimpton hotels",
    voco: "voco hotels",
    hampton: "hampton by hilton",
    "hampton inn": "hampton by hilton",
    "hampton inn & suites": "hampton by hilton",
    fairfield: "fairfield by marriott",
    "fairfield inn": "fairfield by marriott",
    "fairfield inn & suites": "fairfield by marriott",
    home2: "home2 suites by hilton",
    "home2 suites": "home2 suites by hilton",
    courtyard: "courtyard by marriott",
    "courtyard by marriott": "courtyard by marriott",
    "residence inn": "residence inn by marriott",
    "springhill suites": "springhill suites by marriott",
    "ac hotels": "ac hotels by marriott",
    "ac hotel": "ac hotels by marriott",
    "delta hotels": "delta hotels by marriott",
    radisson: "radisson by choice",
    "radisson (choice)": "radisson by choice",
    westin: "westin",
    mercure: "mercure",
    "four seasons": "four seasons",
    "embassy suites": "embassy suites by hilton",
    doubletree: "doubletree by hilton",
    "holiday inn": "holiday inn",
    "holiday inn express": "holiday inn express",
    "crowne plaza": "crowne plaza",
    intercontinental: "intercontinental hotels & resorts",
  };

  const PORTFOLIO_MARKERS =
    /\b(portfolio|aimbridge|ahip|operator|management company|multi[- ]brand)\b/i;

  function normalizeBrandKey(raw) {
    return String(raw || "")
      .toLowerCase()
      .replace(/[®™]/g, "")
      .replace(/\s+/g, " ")
      .trim();
  }

  function isUsableLogoUrl(url) {
    const u = String(url || "").trim();
    if (!u) return false;
    if (/^https?:\/\//i.test(u)) return true;
    if (u.indexOf("//") === 0) return true;
    // Same-origin Brand Basics logo proxy (fresh Airtable attachment stream)
    if (/^\/api\/brand-library\/logo\/rec[a-zA-Z0-9]+/i.test(u)) return true;
    if (u.charAt(0) === "/") return true;
    return false;
  }

  /**
   * Build name → logo URL map from Brand Library brands payload.
   * Prefer same-origin `/api/brand-library/logo/:id` (fresh signed upstream)
   * when a Brand Basics record id is present; fall back to absolute CDN URL.
   */
  function buildBrandLogoMap(brands, aliases) {
    const map = Object.create(null);
    const aliasTable = Object.assign({}, DEFAULT_ALIASES, aliases || {});
    const list = Array.isArray(brands) ? brands : [];

    list.forEach(function (b) {
      const name = String((b && b.name) || "").trim();
      if (!name) return;
      const id = String((b && b.id) || "").trim();
      const logo = (b && (b.logo || b.logoUrl || b.imageUrl)) || "";
      let url = "";
      if (/^rec[a-zA-Z0-9]{10,}$/.test(id) && logo) {
        url = "/api/brand-library/logo/" + encodeURIComponent(id);
      } else if (isUsableLogoUrl(logo)) {
        url = String(logo).indexOf("//") === 0 ? "https:" + logo : String(logo);
      }
      if (!url) return;
      map[normalizeBrandKey(name)] = {
        id: id,
        name: name,
        url: url,
      };
    });

    Object.keys(aliasTable).forEach(function (alias) {
      const target = normalizeBrandKey(aliasTable[alias]);
      const aliasKey = normalizeBrandKey(alias);
      if (map[target] && !map[aliasKey]) {
        map[aliasKey] = map[target];
      }
    });

    return map;
  }

  function resolveBrandLogoUrl(brandRaw, logoMap) {
    const map = logoMap || {};
    const brand = String(brandRaw || "").trim();
    if (!brand) return null;

    if (PORTFOLIO_MARKERS.test(brand) || /[\/|]/.test(brand)) {
      return null;
    }

    const variants = [];
    const pushVariant = function (raw) {
      const k = normalizeBrandKey(raw);
      if (k && variants.indexOf(k) === -1) variants.push(k);
    };
    pushVariant(brand);
    // Strip commercial annotations: "Four Seasons (management ending)"
    pushVariant(brand.replace(/\([^)]*\)/g, " "));
    // Drop trailing clauses after em dash / hyphen annotations
    pushVariant(brand.split(/\s+[—–-]\s+/)[0]);

    for (let v = 0; v < variants.length; v++) {
      const key = variants[v];
      if (map[key] && map[key].url) return map[key];

      const byIdx = key.indexOf(" by ");
      if (byIdx > 0) {
        const short = key.slice(0, byIdx).trim();
        if (map[short] && map[short].url) return map[short];
      }
    }

    const keys = Object.keys(map).sort(function (a, b) {
      return b.length - a.length;
    });
    for (let v = 0; v < variants.length; v++) {
      const key = variants[v];
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        if (!map[k] || !map[k].url) continue;
        if (key === k || key.indexOf(k + " ") === 0 || k.indexOf(key + " ") === 0) {
          return map[k];
        }
      }
    }
    return null;
  }

  root.DealSignalBrandLogos = {
    DEFAULT_ALIASES: DEFAULT_ALIASES,
    normalizeBrandKey: normalizeBrandKey,
    isUsableLogoUrl: isUsableLogoUrl,
    buildBrandLogoMap: buildBrandLogoMap,
    resolveBrandLogoUrl: resolveBrandLogoUrl,
  };
})(typeof globalThis !== "undefined" ? globalThis : this);
