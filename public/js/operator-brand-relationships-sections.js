/**
 * Brand & Relationships — owner-facing subsections (Explorer / DNA).
 * Subsection intros and KPI notes use second-person / owner-facing copy.
 * JSON via prefill / explorerProfileJson (brand_* keys) with defaults until Setup fields exist.
 */
(function (global) {
  "use strict";

  /** @see api/lib/operator-brand-explorer-field-map.js */
  var FIELD = {
    portfolioMix: "brand_portfolio_mix_json",
    relationshipDepth: "brand_relationship_depth_json",
    executionCapabilities: "brand_execution_capabilities_json",
    governanceCompliance: "brand_governance_compliance_json",
    softIndependentNarrative: "brand_soft_independent_narrative",
    conversionProjectCount: "brand_conversion_project_count",
    brandedIndependentMix: "brandedVsIndependentMix",
    numberOfBrands: "numberOfBrands",
  };

  var DEFAULTS = {
    brand_portfolio_mix_json: [
      {
        brandFlagType: "Marriott Family",
        portfolioMix: "18%",
        assetContext: "Upscale / Lifestyle / Select Service",
        relationshipStatus: "Active / Prior",
      },
      {
        brandFlagType: "Hilton Family",
        portfolioMix: "14%",
        assetContext: "Full-Service / Resort / Select Service",
        relationshipStatus: "Active / Approved",
      },
      {
        brandFlagType: "Hyatt Family",
        portfolioMix: "9%",
        assetContext: "Lifestyle / Resort / Luxury",
        relationshipStatus: "Prior / Selective",
      },
      {
        brandFlagType: "IHG Family",
        portfolioMix: "8%",
        assetContext: "Lifestyle / Upscale / Conversion",
        relationshipStatus: "Active / Prior",
      },
      {
        brandFlagType: "Wyndham / Choice / Other",
        portfolioMix: "13%",
        assetContext: "Select-Service / Conversion / Resort-Adjacent",
        relationshipStatus: "Prior / Target",
      },
      {
        brandFlagType: "Independent / Soft Brand",
        portfolioMix: "38%",
        assetContext:
          "Independent Resorts, Collections, Condo-Hotels, Owner-Led Assets",
        relationshipStatus: "Active Strength",
      },
    ],
    brand_relationship_depth_json: [
      {
        brandSegment: "Global Luxury",
        relationshipType: "Prior experience",
        depth: "Selective",
        ownerContext:
          "Luxury resort, branded residence, high-touch service, elevated owner expectations",
      },
      {
        brandSegment: "Upper Upscale / Full Service",
        relationshipType: "Active / approved",
        depth: "Strong",
        ownerContext:
          "Conversions, resort assets, urban-leisure hybrids, owner reporting discipline",
      },
      {
        brandSegment: "Lifestyle",
        relationshipType: "Active / prior",
        depth: "Strong",
        ownerContext:
          "Independent conversions, experience-led positioning, F&B and programming relevance",
      },
      {
        brandSegment: "Resort",
        relationshipType: "Active / approved",
        depth: "Strong",
        ownerContext:
          "Beach, leisure, spa, F&B-heavy assets, complex staffing and guest experience",
      },
      {
        brandSegment: "Select Service",
        relationshipType: "Prior / target",
        depth: "Moderate",
        ownerContext:
          "Resort-adjacent, airport, business-leisure demand, development corridor opportunities",
      },
      {
        brandSegment: "Independent Collections",
        relationshipType: "Active",
        depth: "Strong",
        ownerContext:
          "Owner-controlled assets seeking distribution lift without losing identity",
      },
      {
        brandSegment: "Soft Brands",
        relationshipType: "Active / target",
        depth: "Strong",
        ownerContext:
          "Conversion flexibility, story-driven positioning, owner-friendly brand transition",
      },
    ],
    brand_execution_capabilities_json: [
      {
        title: "Brand Onboarding",
        description:
          "Supports owners through application, brand review, onboarding, documentation, and initial implementation.",
      },
      {
        title: "Standards Translation",
        description:
          "Helps owners understand the operational and capex implications of brand standards, technical services, and PIPs.",
      },
      {
        title: "Conversion / Reflag Execution",
        description:
          "Coordinates operating readiness, systems, training, staffing, and guest-facing transition issues.",
      },
      {
        title: "Brand-Owner Coordination",
        description:
          "Manages communication between ownership, brand development, operations, technical services, and property teams.",
      },
      {
        title: "Soft Brand Strategy",
        description:
          "Helps preserve asset identity while leveraging distribution, loyalty, and brand credibility.",
      },
      {
        title: "Independent-to-Branded Transition",
        description:
          "Supports owners moving from independent operations into a branded or collection environment.",
      },
    ],
    brand_governance_compliance_json: [
      {
        title: "Brand Compliance",
        description:
          "QA readiness, operating standards, audit preparation, and recurring compliance tracking.",
      },
      {
        title: "Technical Services Coordination",
        description:
          "Design, PIP, life safety, systems, opening checklists, and brand-required deliverables.",
      },
      {
        title: "Brand Reporting",
        description:
          "Brand performance reviews, guest metrics, loyalty contribution, and standards-related action plans.",
      },
      {
        title: "Owner Decision Support",
        description:
          "Clear explanation of brand trade-offs, obligations, timing, and operational requirements.",
      },
    ],
    brand_soft_independent_narrative:
      "If you are evaluating soft-brand affiliation, an independent-to-branded conversion, or repositioning an independent resort, you can expect help interpreting brand requirements, protecting local identity, preparing operationally for a flag transition, and understanding how distribution and loyalty may affect your operating model.",
  };

  function nz(v) {
    if (v == null) return "";
    return String(v).trim();
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function pick(ex, p, key) {
    if (ex && nz(ex[key])) return ex[key];
    if (p && nz(p[key])) return p[key];
    return "";
  }

  function parseJsonArray(raw) {
    if (raw == null || raw === "") return null;
    if (Array.isArray(raw)) return raw;
    try {
      var parsed = JSON.parse(String(raw));
      return Array.isArray(parsed) ? parsed : null;
    } catch (e) {
      return null;
    }
  }

  var PLATFORM_FIELD_MAP = {
    brand_portfolio_mix_json: "portfolioMix",
    brand_relationship_depth_json: "relationshipDepth",
    brand_execution_capabilities_json: "executionCapabilities",
    brand_governance_compliance_json: "governanceCompliance",
  };

  function brandRelationshipsFromVm(vm) {
    if (vm && vm.brandRelationships && typeof vm.brandRelationships === "object") {
      return vm.brandRelationships;
    }
    if (
      vm &&
      vm.prefill &&
      vm.prefill.brandRelationships &&
      typeof vm.prefill.brandRelationships === "object"
    ) {
      return vm.prefill.brandRelationships;
    }
    return null;
  }

  function snapshotValueFromPlatform(platform, rowKey) {
    if (!platform || !Array.isArray(platform.snapshotMetrics)) return "";
    for (var i = 0; i < platform.snapshotMetrics.length; i++) {
      var k = platform.snapshotMetrics[i];
      if (nz(k.rowKey) === rowKey) return nz(k.value);
    }
    return "";
  }

  function sectionData(vm, fieldKey) {
    var platform = brandRelationshipsFromVm(vm);
    if (platform) {
      var bucketKey = PLATFORM_FIELD_MAP[fieldKey];
      if (bucketKey && Array.isArray(platform[bucketKey]) && platform[bucketKey].length) {
        return platform[bucketKey];
      }
      if (fieldKey === FIELD.softIndependentNarrative && nz(platform.softIndependentNarrative)) {
        return platform.softIndependentNarrative;
      }
    }
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var fromRecord = parseJsonArray(pick(ex, p, fieldKey));
    if (fromRecord && fromRecord.length) return fromRecord;
    if (fieldKey === FIELD.softIndependentNarrative) {
      var text = nz(pick(ex, p, fieldKey));
      if (text) return text;
      return DEFAULTS[fieldKey] || "";
    }
    return DEFAULTS[fieldKey] || [];
  }

  function parsePercentToken(raw) {
    var s = nz(raw);
    var m = s.match(/(\d+(?:\.\d+)?)\s*%?/);
    return m ? parseFloat(m[1], 10) : NaN;
  }

  /** True when portfolio mix uses qualitative CALA labels (no room-weighted %). */
  function portfolioMixIsQualitative(vm) {
    var rows = sectionData(vm, FIELD.portfolioMix);
    if (!rows.length) return false;
    var anyPct = false;
    rows.forEach(function (row) {
      if (!isNaN(parsePercentToken(row.portfolioMix || row.mix || row.share))) anyPct = true;
    });
    return !anyPct;
  }

  function portfolioMixColumnLabel(vm) {
    var rows = sectionData(vm, FIELD.portfolioMix);
    var hasEnterprise = rows.some(function (row) {
      return /enterprise/i.test(nz(row.portfolioMix || row.mix || row.share));
    });
    if (hasEnterprise) return "Platform scope";
    if (portfolioMixIsQualitative(vm)) return "CALA presence";
    return "Portfolio mix";
  }

  function buildScopeNoticeHtml(vm) {
    if (!portfolioMixIsQualitative(vm)) return "";
    var rows = sectionData(vm, FIELD.portfolioMix);
    var hasEnterprise = rows.some(function (row) {
      return /enterprise/i.test(nz(row.portfolioMix || row.mix || row.share));
    });
    if (hasEnterprise) {
      return (
        '<p class="gold-mock-tab-empty odna-subsection-intro oe-brand-scope-note">' +
        "Scope: enterprise U.S. platform brand experience per Arbor ALM materials (slide 19)—not a CALA room-weighted census. " +
        "CALA hotel counts appear on Markets &amp; Footprint." +
        "</p>"
      );
    }
    return (
      '<p class="gold-mock-tab-empty odna-subsection-intro oe-brand-scope-note">' +
      "Scope: CALA brand relationships and approved franchise families. " +
      "Presence labels below are qualitative—not room-weighted percentages of the parent U.S. platform " +
      "(42 hotels, 5,000+ rooms). CALA hotel counts appear on Markets &amp; Footprint." +
      "</p>"
    );
  }

  function formatPercent(n) {
    if (n == null || isNaN(n)) return "";
    var rounded = Math.round(n);
    return String(rounded) + "%";
  }

  function portfolioMixPercents(vm) {
    var platform = brandRelationshipsFromVm(vm);
    var brandedSnap = snapshotValueFromPlatform(platform, "branded_portfolio");
    var indepSnap = snapshotValueFromPlatform(platform, "independent_soft");
    if (brandedSnap || indepSnap) {
      return {
        branded: brandedSnap,
        independent: indepSnap,
      };
    }
    var rows = sectionData(vm, FIELD.portfolioMix);
    var branded = 0;
    var independent = NaN;
    rows.forEach(function (row) {
      var pct = parsePercentToken(row.portfolioMix || row.mix || row.share);
      if (isNaN(pct)) return;
      var label = nz(row.brandFlagType || row.brand || row.name).toLowerCase();
      if (label.indexOf("independent") >= 0 || label.indexOf("soft brand") >= 0) {
        independent = pct;
      } else {
        branded += pct;
      }
    });
    return {
      branded: branded > 0 ? formatPercent(branded) : "",
      independent: !isNaN(independent) ? formatPercent(independent) : "",
    };
  }

  function parseMixStringPercents(mixRaw) {
    var s = nz(mixRaw);
    if (!s) return { branded: "", independent: "" };
    var brandedM = s.match(/(\d+(?:\.\d+)?)\s*%?\s*(?:branded|flagged|chain)/i);
    var indepM = s.match(/(\d+(?:\.\d+)?)\s*%?\s*independent/i);
    return {
      branded: brandedM ? formatPercent(parseFloat(brandedM[1], 10)) : "",
      independent: indepM ? formatPercent(parseFloat(indepM[1], 10)) : "",
    };
  }

  function meaningfulSignal(v) {
    var s = nz(v);
    return s && s !== "—" && s !== "–" && s !== "-";
  }

  var LABEL_ACRONYMS = { ihg: "IHG", usa: "USA", uk: "UK", cala: "CALA", qa: "QA", pip: "PIP" };

  var BRAND_FLAG_TYPE_LABELS = {
    "marriott family": "Marriott Family",
    "hilton family": "Hilton Family",
    "hyatt family": "Hyatt Family",
    "ihg family": "IHG Family",
    "wyndham / choice / other": "Wyndham / Choice / Other",
    "independent / soft brand": "Independent / Soft Brand",
  };

  function titleCaseToken(token) {
    var t = nz(token);
    if (!t) return "";
    return t
      .split("-")
      .map(function (part) {
        var lower = part.toLowerCase();
        if (LABEL_ACRONYMS[lower]) return LABEL_ACRONYMS[lower];
        return lower.charAt(0).toUpperCase() + lower.slice(1);
      })
      .join("-");
  }

  /** Proper case for portfolio mix cells (slashes, commas, hyphens). */
  function formatPortfolioMixCell(raw) {
    var s = nz(raw);
    if (!s) return "";
    if (s.indexOf(",") >= 0) {
      return s
        .split(",")
        .map(function (part) {
          return formatPortfolioMixCell(part.trim());
        })
        .join(", ");
    }
    if (s.indexOf("/") >= 0) {
      return s
        .split(/\s*\/\s*/)
        .map(function (segment) {
          return formatPortfolioMixCell(segment.trim());
        })
        .join(" / ");
    }
    return s
      .split(/\s+/)
      .map(titleCaseToken)
      .join(" ");
  }

  /** Title Case for execution / governance tile headings (not sentence case). */
  function formatBrandCardTitle(raw) {
    if (
      global.OperatorExplorerCardTitle &&
      typeof global.OperatorExplorerCardTitle.formatCardTitle === "function"
    ) {
      return global.OperatorExplorerCardTitle.formatCardTitle(raw);
    }
    return nz(raw);
  }

  function formatBrandFlagTypeLabel(raw) {
    var s = nz(raw);
    if (!s) return "";
    var mapped = BRAND_FLAG_TYPE_LABELS[s.toLowerCase()];
    if (mapped) return mapped;
    return formatPortfolioMixCell(s);
  }

  function brandRelationshipsCount(vm) {
    var fromSnap = snapshotValueFromPlatform(brandRelationshipsFromVm(vm), "brand_relationships_count");
    if (fromSnap) return fromSnap;
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var n = parseInt(nz(pick(ex, p, FIELD.numberOfBrands, p.numberOfBrands)), 10);
    if (!isNaN(n) && n > 0) return n >= 10 ? n + "+" : String(n);
    var brands = p.brands || ex.brands;
    if (Array.isArray(brands) && brands.length) {
      return brands.length >= 10 ? brands.length + "+" : String(brands.length);
    }
    var mixRows = sectionData(vm, FIELD.portfolioMix);
    var families = mixRows.filter(function (row) {
      var label = nz(row.brandFlagType || row.brand).toLowerCase();
      return label.indexOf("independent") < 0;
    }).length;
    if (families > 0) return families >= 10 ? families + "+" : String(families);
    return "";
  }

  function conversionProjectsDisplay(vm) {
    var fromSnap = snapshotValueFromPlatform(brandRelationshipsFromVm(vm), "conversion_reflag");
    if (fromSnap) return fromSnap;
    var ex = (vm && vm.ex) || {};
    var p = (vm && vm.prefill) || {};
    var count = nz(pick(ex, p, FIELD.conversionProjectCount, ""));
    if (/^\d+$/.test(count)) return count;
    var reflag = nz(pick(ex, p, "brand_signal_reflag", ""));
    if (/^\d+$/.test(reflag)) return reflag;
    var m = reflag.match(/^(\d+)\s+(?:projects?|conversions?|reflags?)/i);
    return m ? m[1] : "";
  }

  function approvedFamiliesCount(vm) {
    var ex = (vm && vm.ex) || {};
    var p = (vm && vm.prefill) || {};
    var n = parseInt(nz(pick(ex, p, FIELD.numberOfBrands, p.numberOfBrands)), 10);
    if (!isNaN(n) && n > 0) return String(n);
    var rows = sectionData(vm, FIELD.portfolioMix);
    var families = rows.filter(function (row) {
      return nz(row.brandFlagType || row.brand).toLowerCase().indexOf("independent") < 0;
    }).length;
    return families > 0 ? String(families) : "";
  }

  function primarySegmentsCount(vm) {
    var rows = sectionData(vm, FIELD.relationshipDepth);
    if (rows.length) return String(rows.length);
    return "";
  }

  /**
   * Top KPI row for Brand & Relationships tab.
   * @param {object} vm
   * @returns {Array<{label:string,labelLines?:string[],value:string,note:string}>}
   */
  function deriveSnapshotMetrics(vm) {
    vm = vm || {};
    var ex = vm.ex || {};
    var p = vm.prefill || {};
    var mixPct = portfolioMixPercents(vm);
    var mixStr = parseMixStringPercents(
      nz(vm.brandMixDisplay) ||
        pick(ex, p, FIELD.brandedIndependentMix, "") ||
        pick(ex, p, "Branded vs Independent Mix", "")
    );
    var brandedPct = mixPct.branded || mixStr.branded;
    var indepPct = mixPct.independent || mixStr.independent;
    var qualitative = portfolioMixIsQualitative(vm);

    function row(labelLines, value, note) {
      if (!meaningfulSignal(value)) return null;
      var lines = Array.isArray(labelLines) ? labelLines : [labelLines];
      return {
        label: lines.join(" "),
        labelLines: lines,
        value: value,
        note: note,
      };
    }

    return [
      row(
        ["Brand", "Relationships"],
        brandRelationshipsCount(vm),
        "Brand families this operator works with today, has worked with before, or is targeting for assets like yours"
      ),
      row(
        ["Branded", "Portfolio"],
        brandedPct,
        qualitative
          ? "Qualitative CALA branded presence—not a room-weighted share of the parent U.S. portfolio"
          : "How much of this operator's portfolio runs under a brand flag—relevant if you want chain distribution and standards"
      ),
      row(
        ["Independent /", "Soft Brand"],
        indepPct,
        qualitative
          ? "Independent, collection, and soft-brand targets in CALA—pipeline and conversion opportunities"
          : "Independent, collection, lifestyle, or owner-controlled assets—useful if you want flexibility or a lighter affiliation"
      ),
      row(
        ["Conversion /", "Reflag Projects"],
        conversionProjectsDisplay(vm),
        "Representative conversion or reflag experience you can draw on when planning a brand change for your property"
      ),
      row(
        ["Approved Brand", "Families"],
        approvedFamiliesCount(vm),
        "Brand families where this operator has formal approval or demonstrated day-to-day operating familiarity"
      ),
      row(
        ["Primary Brand", "Segments"],
        primarySegmentsCount(vm),
        "Segments where this operator is most active—luxury, upscale, lifestyle, resort, and related positioning"
      ),
    ].filter(Boolean);
  }

  function wrapSection(title, intro, bodyHtml, extraClass) {
    if (!bodyHtml) return "";
    return (
      '<section class="section oe-brand-section' +
      (extraClass ? " " + extraClass : "") +
      '">' +
      '<h2 class="section-title">' +
      escapeHtml(title) +
      "</h2>" +
      (intro
        ? '<p class="gold-mock-tab-empty odna-subsection-intro">' + escapeHtml(intro) + "</p>"
        : "") +
      bodyHtml +
      "</section>"
    );
  }

  function depthBadgeClass(depth) {
    var d = nz(depth).toLowerCase();
    if (d.indexOf("selective") >= 0) return "oe-brand-depth oe-brand-depth--selective";
    if (d.indexOf("moderate") >= 0) return "oe-brand-depth oe-brand-depth--moderate";
    if (d.indexOf("emerging") >= 0) return "oe-brand-depth oe-brand-depth--emerging";
    return "oe-brand-depth oe-brand-depth--strong";
  }

  function iconCard(row, cardClass, iconClass) {
    if (!row || !nz(row.title)) return "";
    return (
      '<div class="card ' +
      cardClass +
      '">' +
      '<span class="' +
      iconClass +
      '" aria-hidden="true"></span>' +
      '<div class="oe-brand-card__body">' +
      "<h3>" +
      escapeHtml(formatBrandCardTitle(row.title)) +
      "</h3>" +
      "<p>" +
      escapeHtml(row.description || "") +
      "</p></div></div>"
    );
  }

  function buildPortfolioMixSection(vm) {
    var rows = sectionData(vm, FIELD.portfolioMix);
    if (!rows.length) return "";
    var qualitative = portfolioMixIsQualitative(vm);
    var mixColLabel = portfolioMixColumnLabel(vm);
    var intro = qualitative
      ? /enterprise/i.test(
          rows
            .map(function (row) {
              return nz(row.portfolioMix || row.mix || row.share);
            })
            .join(" ")
        )
        ? "Named flags Arbor cites on its enterprise U.S. platform (ALM materials)—qualitative scope labels, not CALA hotel counts."
        : "How this operator's approved brand families show up in CALA today—qualitative presence labels, not room-weighted percentages of the parent U.S. portfolio."
      : "How this operator's portfolio is split across major brand families versus independent and soft-brand assets—context for your asset type and brand strategy.";
    var body = rows
      .map(function (row) {
        var mix = nz(row.portfolioMix || row.mix || row.share);
        return (
          "<tr><th scope=\"row\">" +
          escapeHtml(
            formatBrandFlagTypeLabel(row.brandFlagType || row.brand || row.name || "")
          ) +
          "</th><td>" +
          (mix
            ? '<span class="oe-brand-mix-pill">' + escapeHtml(mix) + "</span>"
            : "—") +
          "</td><td>" +
          escapeHtml(formatPortfolioMixCell(row.assetContext || row.context || "")) +
          "</td><td>" +
          escapeHtml(formatPortfolioMixCell(row.relationshipStatus || row.status || "")) +
          "</td></tr>"
        );
      })
      .join("");
    return wrapSection(
      "Portfolio Mix by Brand / Flag Type",
      intro,
      '<div class="oe-brand-table-wrap">' +
        '<table class="oe-brand-table">' +
        "<thead><tr><th scope=\"col\">Brand / flag type</th><th scope=\"col\">" +
        escapeHtml(mixColLabel) +
        "</th><th scope=\"col\">Typical asset context</th><th scope=\"col\">Relationship status</th></tr></thead>" +
        "<tbody>" +
        body +
        "</tbody></table></div>",
      "oe-brand-section--mix"
    );
  }

  function buildRelationshipDepthSection(vm) {
    var rows = sectionData(vm, FIELD.relationshipDepth);
    if (!rows.length) return "";
    var body = rows
      .map(function (row) {
        var depth = nz(row.depth);
        return (
          "<tr><th scope=\"row\">" +
          escapeHtml(row.brandSegment || row.segment || "") +
          "</th><td>" +
          escapeHtml(row.relationshipType || row.type || "") +
          "</td><td>" +
          (depth
            ? '<span class="' + depthBadgeClass(depth) + '">' + escapeHtml(depth) + "</span>"
            : "—") +
          "</td><td>" +
          escapeHtml(row.ownerContext || row.context || "") +
          "</td></tr>"
        );
      })
      .join("");
    return wrapSection(
      "Brands & Relationship Depth",
      "Where this operator has meaningful familiarity by segment—for your comparison only; not a brand ranking, endorsement, or pay-to-play placement.",
      '<div class="oe-brand-table-wrap">' +
        '<table class="oe-brand-table oe-brand-table--depth">' +
        "<thead><tr><th scope=\"col\">Brand segment</th><th scope=\"col\">Relationship type</th><th scope=\"col\">Depth</th><th scope=\"col\">Relevant context for owners</th></tr></thead>" +
        "<tbody>" +
        body +
        "</tbody></table></div>",
      "oe-brand-section--depth"
    );
  }

  function buildExecutionSection(vm) {
    var rows = sectionData(vm, FIELD.executionCapabilities);
    if (!rows.length) return "";
    return wrapSection(
      "Brand Execution Capabilities",
      "What you can expect this operator to help you with once you are evaluating, negotiating, or implementing a brand decision.",
      '<div class="grid-3 oe-brand-exec-grid">' +
        rows
          .map(function (row) {
            return iconCard(row, "oe-brand-exec-card", "oe-brand-exec-card__icon");
          })
          .join("") +
        "</div>",
      "oe-brand-section--exec"
    );
  }

  function buildGovernanceSection(vm) {
    var rows = sectionData(vm, FIELD.governanceCompliance);
    if (!rows.length) return "";
    return wrapSection(
      "Brand Governance & Compliance Support",
      "How this operator helps you stay on top of brand requirements during opening, conversion, and stabilized operations.",
      '<div class="grid-2 oe-brand-gov-grid">' +
        rows
          .map(function (row) {
            return iconCard(row, "oe-brand-gov-card", "oe-brand-gov-card__icon");
          })
          .join("") +
        "</div>",
      "oe-brand-section--gov"
    );
  }

  function buildSoftIndependentSection(vm) {
    var raw = sectionData(vm, FIELD.softIndependentNarrative);
    var text = typeof raw === "string" ? raw : nz(raw && raw.text);
    if (!text) return "";
    var company = nz((vm && vm.companyName) || "");
    if (company && /^if you are evaluating/i.test(text)) {
      text = company + " can help if you are evaluating" + text.slice("If you are evaluating".length);
    } else if (company && text.indexOf("This operator") === 0) {
      text = text.replace(/^This operator/, company);
    }
    return wrapSection(
      "Soft Brand / Independent Experience",
      "When a full flag may be more than you need—how this operator can help you add distribution and credibility while protecting your property's identity.",
      '<div class="oe-brand-narrative-well"><p>' + escapeHtml(text) + "</p></div>",
      "oe-brand-section--soft"
    );
  }

  function buildAllSectionsHtml(vm) {
    return (
      buildPortfolioMixSection(vm) +
      buildRelationshipDepthSection(vm) +
      buildExecutionSection(vm) +
      buildGovernanceSection(vm) +
      buildSoftIndependentSection(vm)
    );
  }

  global.OperatorBrandRelationshipsSections = {
    buildAllSectionsHtml: buildAllSectionsHtml,
    buildScopeNoticeHtml: buildScopeNoticeHtml,
    deriveSnapshotMetrics: deriveSnapshotMetrics,
    portfolioMixIsQualitative: portfolioMixIsQualitative,
    FIELD: FIELD,
    DEFAULTS: DEFAULTS,
  };
})(typeof globalThis !== "undefined" ? globalThis : global);
