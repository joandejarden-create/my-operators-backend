/**
 * Dealality UI copy conventions (display only — never change Airtable/API values here).
 *
 * Proper Case: short labels, section titles, dropdown options, badges, list chips
 *   (e.g. Workspace, Owner-Side, Demo Mode, Third-Party Management Availability).
 * Sentence case: helper text and tooltips that read as full sentences
 *   (e.g. "Preview workspace views without production permissions.").
 */
(function () {
  var WORKSPACE_SIDE_LABELS = {
    Owner: "Owner-Side",
    Operator: "Operator-Side",
    Brand: "Brand-Side",
    Demo: "Demo Mode",
    Admin: "Admin",
  };

  var WORKSPACE_KEY_ALIASES = {
    owner: "Owner",
    operator: "Operator",
    brand: "Brand",
    demo: "Demo",
    admin: "Admin",
  };

  function formatWorkspaceSideLabel(workspaceKey) {
    var raw = workspaceKey == null ? "" : String(workspaceKey).trim();
    if (!raw) return "";
    if (WORKSPACE_SIDE_LABELS[raw]) return WORKSPACE_SIDE_LABELS[raw];
    var canon = WORKSPACE_KEY_ALIASES[raw.toLowerCase()];
    if (canon && WORKSPACE_SIDE_LABELS[canon]) return WORKSPACE_SIDE_LABELS[canon];
    return raw;
  }

  function formatWorkspaceAccessDisplay(workspaceAccess) {
    var list = Array.isArray(workspaceAccess) ? workspaceAccess : [];
    return list
      .map(formatWorkspaceSideLabel)
      .filter(Boolean)
      .join(", ");
  }

  function formatDevWorkspaceSwitcherLabel(roleKey) {
    var key = String(roleKey || "").toLowerCase();
    if (key === "all") return "All Workspaces";
    if (key === "demo") return "Demo Mode";
    return formatWorkspaceSideLabel(WORKSPACE_KEY_ALIASES[key] || roleKey);
  }

  /** Badge/list line for third-party availability (value keeps Airtable spelling). */
  function formatThirdPartyManagementBadge(availability) {
    var v = availability == null ? "" : String(availability).trim();
    if (!v) return "";
    return "Third-Party Management: " + v;
  }

  function formatCompanyTypeBadge(companyTypeDisplay) {
    var v = companyTypeDisplay == null ? "" : String(companyTypeDisplay).trim();
    if (!v) return "";
    return v;
  }

  var OPERATOR_EXPLORER_BADGES_TOOLTIP =
    "Owner-Operator means this company owns or controls hotel assets and also operates hotels. Availability for third-party management may vary by market and deal type.";

  /** Card subtitle under operator name (Proper Case). */
  function formatOperatorExplorerTypeLabel(op) {
    var row = op || {};
    if (row.isOwnerOperator || row.normalizedCompanyType === "OWNER_OPERATOR") {
      return "Hotel Owner - Operator";
    }
    return "3rd Party Operator";
  }

  /**
   * Operator Explorer list badges (Proper Case). Empty when no badge fields on row.
   * @param {object} op — normalized list row
   * @returns {string[]}
   */
  function buildOperatorExplorerCardBadges(op) {
    var row = op || {};
    var badges = [];
    if (Array.isArray(row.companyDisplayBadges) && row.companyDisplayBadges.length) {
      badges = row.companyDisplayBadges.slice();
    } else {
      if (row.isOwnerOperator) badges.push("Hotel Owner - Operator");
      var status = String(row.thirdPartyManagementAvailabilityStatus || "").trim();
      if (status && status !== "Unknown / Legacy") {
        var line = formatThirdPartyManagementBadge(status);
        if (line) badges.push(line);
      } else if (String(row.thirdPartyManagementAvailability || "").toLowerCase() === "yes") {
        badges.push(formatThirdPartyManagementBadge("Yes"));
      }
    }
    if (row.reviewBeforeOutreach) {
      badges.push("Review Availability Before Outreach");
    }
    var ws = Array.isArray(row.workspaceAccess) ? row.workspaceAccess : [];
    for (var i = 0; i < ws.length; i++) {
      if (String(ws[i] || "").toLowerCase() === "demo") {
        badges.push(formatWorkspaceSideLabel("Demo"));
        break;
      }
    }
    return badges;
  }

  function getOperatorExplorerCardBadgesTooltip() {
    return OPERATOR_EXPLORER_BADGES_TOOLTIP;
  }

  window.DEALALITY_UI_LABELS = {
    WORKSPACE_SIDE_LABELS: WORKSPACE_SIDE_LABELS,
    formatWorkspaceSideLabel: formatWorkspaceSideLabel,
    formatWorkspaceAccessDisplay: formatWorkspaceAccessDisplay,
    formatDevWorkspaceSwitcherLabel: formatDevWorkspaceSwitcherLabel,
    formatThirdPartyManagementBadge: formatThirdPartyManagementBadge,
    formatCompanyTypeBadge: formatCompanyTypeBadge,
    formatOperatorExplorerTypeLabel: formatOperatorExplorerTypeLabel,
    buildOperatorExplorerCardBadges: buildOperatorExplorerCardBadges,
    getOperatorExplorerCardBadgesTooltip: getOperatorExplorerCardBadgesTooltip,
  };
})();
