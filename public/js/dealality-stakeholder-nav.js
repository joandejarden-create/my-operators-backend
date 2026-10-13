/**
 * Central stakeholder-aware nav visibility for AI Visibility products.
 * Keep role checks here — do not scatter product-specific role lists in multiple nav builders.
 *
 * Product naming (locked):
 * - Brand-Side → Brand AI Intelligence
 * - Operator-Side → Operator AI Intelligence
 * - Owner-Side → Brand AI Intelligence (development/brand-selection view) + AI Demand Positioning
 *   (AI Recommendation Intelligence remains future — not shown as a separate Brand product)
 * - Admin / All Workspaces → governed access per existing admin conventions
 */
(function (global) {
  "use strict";

  var PRODUCT_NAV = {
    brand_ai_visibility: {
      route: "/ai-visibility",
      label: "Brand AI Intelligence",
      // Owner workspace: development / brand-selection perspective (how AI surfaces hotel brands).
      // Brand workspace: brand-side monitoring. Admin: all workspaces.
      allowedNavRoles: ["owner", "brand", "admin"],
    },
    operator_ai_intelligence: {
      route: "/operator/ai-intelligence",
      label: "Operator AI Intelligence",
      allowedNavRoles: ["operator", "admin"],
    },
    ai_recommendation_intelligence: {
      route: null,
      label: "AI Recommendation Intelligence",
      allowedNavRoles: ["owner", "admin"],
      future: true,
    },
  };

  function normalizeNavRole(role) {
    var r = String(role || "")
      .toLowerCase()
      .replace(/_/g, "-");
    if (r === "owner-operator") return "owner";
    if (r === "all") return "all";
    return r;
  }

  /**
   * @param {string} productKey
   * @param {string} navRole — active workspace nav role (owner|brand|operator|admin|all)
   * @returns {boolean}
   */
  function stakeholderProductVisible(productKey, navRole) {
    var product = PRODUCT_NAV[productKey];
    if (!product || product.future) return false;
    var role = normalizeNavRole(navRole);
    if (role === "admin" || role === "all") return true;
    return product.allowedNavRoles.indexOf(role) !== -1;
  }

  /**
   * Filter a nav item that may include `roles` and/or `stakeholderProduct`.
   * @param {{ roles?: string[], stakeholderProduct?: string, route?: string }} item
   * @param {string} navRole
   */
  function navItemVisibleForStakeholder(item, navRole) {
    if (!item) return false;
    if (item.stakeholderProduct) {
      return stakeholderProductVisible(item.stakeholderProduct, navRole);
    }
    var role = normalizeNavRole(navRole);
    if (role === "admin" || role === "all") return true;
    var roles = item.roles || [];
    if (!roles.length) return true;
    return roles.indexOf(role) !== -1;
  }

  global.DealalityStakeholderNav = {
    PRODUCT_NAV: PRODUCT_NAV,
    stakeholderProductVisible: stakeholderProductVisible,
    navItemVisibleForStakeholder: navItemVisibleForStakeholder,
    normalizeNavRole: normalizeNavRole,
  };
})(typeof window !== "undefined" ? window : globalThis);
