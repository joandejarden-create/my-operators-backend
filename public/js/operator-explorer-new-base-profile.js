/**
 * Operator Explorer — new-base Operator Setup profile sections, badges, and deal-aware alignment context.
 * Consumes the same prefill keys as OAS / Operator Strategy (no scoring changes).
 */
(function (global) {
  /** Prefill keys merged into explorer view model (camelCase from buildPrefillObjectFromNewBaseRows). */
  var NEW_BASE_EXPLORER_PREFILL_KEYS = [
    "operatorId",
    "company_name",
    "companyName",
    "companyDescription",
    "website",
    "headquarters",
    "companyLogo",
    "parentCompany",
    "platform",
    "yearEstablished",
    "yearsInBusiness",
    "activeCountries",
    "activeMarkets",
    "marketPresenceType",
    "regions",
    "regionsSupported",
    "specificMarkets",
    "serviceModelsSupported",
    "chainScalesSupported",
    "managementStructuresSupported",
    "minimumKeyCount",
    "offeredServices",
    "revenueManagementCapability",
    "salesPlatform",
    "fbCapabilityLevel",
    "fBCapabilityLevel",
    "newBuildOpeningExperience",
    "conversionReflagExperience",
    "preOpeningSupportCapability",
    "brandFamiliesOperated",
    "softBrandLifestyleExperience",
    "brandsPortfolioDetail",
    "similarProjectCaseStudies",
    "ownerReportingLevel",
    "governanceCadence",
    "dataConfidenceLevel",
    "sourceType",
    "lastUpdatedDate",
    "primaryServiceModel",
    "chainScale",
    "chainScales",
    "explorerHeroVerification",
    "explorerHeroDataSource",
    "brandedResidencesAllowed",
    "mixedUseAllowed",
    "brandedResidenceExperienceLevel",
    "brandedResidenceProgramModelsSupported",
    "condoRentalProgramModelsSupported",
    "brandedResidencePropertiesManaged",
    "mixedUseHospitalityExperience",
    "hoaCondoAssociationInterface",
    "residenceSalesClosingSupport",
    "bf_signal_residence",
  ];

  var BANNED_COPY = [
    /\brecommended\b/i,
    /\bbest fit\b/i,
    /\bpreferred operator\b/i,
    /\btop operator\b/i,
    /dealality recommends/i,
  ];

  function nz(v) {
    return v != null && String(v).trim() !== "" ? String(v).trim() : "";
  }

  function escapeHtml(s) {
    if (s == null) return "";
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function arrayish(val) {
    if (val == null) return [];
    if (Array.isArray(val)) return val.map(String).map(nz).filter(Boolean);
    return String(val)
      .split(/[,;\n]/)
      .map(function (x) {
        return x.trim();
      })
      .filter(Boolean);
  }

  function contractDiagEnabled() {
    try {
      if (global && (global.__OPERATOR_SETUP_CONTRACT_DIAGNOSTICS === true || global.__OPERATOR_SETUP_CONTRACT_DIAGNOSTICS === "1")) {
        return true;
      }
      if (typeof localStorage !== "undefined") {
        var v = localStorage.getItem("operator_setup_contract_diagnostics");
        return v === "1" || v === "true";
      }
    } catch (_err) {}
    return false;
  }

  function emitContractDiag(payload) {
    if (!contractDiagEnabled()) return;
    try {
      console.debug(
        "[operator_setup_contract_diag]",
        JSON.stringify(Object.assign({ scope: "explorer_read_key_resolution" }, payload || {}))
      );
    } catch (_err) {}
  }

  function pickField(ex, prefill, fields, keys, opts) {
    var list = Array.isArray(keys) ? keys : [keys];
    var canonicalKey = (opts && opts.canonicalKey) || list[0] || "";
    var concept = (opts && opts.concept) || canonicalKey;
    var canonicalSource = "";
    var usedSource = "";
    var usedKey = "";
    var usedValue = "";
    var i;
    for (i = 0; i < list.length; i++) {
      var k = list[i];
      var a = nz(ex && ex[k]);
      if (a) {
        usedSource = "ex";
        usedKey = k;
        usedValue = a;
        if (k === canonicalKey) canonicalSource = "ex";
        break;
      }
      var b = nz(prefill && prefill[k]);
      if (b) {
        usedSource = "prefill";
        usedKey = k;
        usedValue = b;
        if (k === canonicalKey) canonicalSource = "prefill";
        break;
      }
      if (fields && fields[k] != null && nz(fields[k])) {
        usedSource = "fields";
        usedKey = k;
        usedValue = nz(fields[k]);
        if (k === canonicalKey) canonicalSource = "fields";
        break;
      }
      if (!canonicalSource && k === canonicalKey) {
        canonicalSource = "missing";
      }
    }
    if (usedValue) {
      emitContractDiag({
        concept: concept,
        kind: "scalar",
        canonicalKey: canonicalKey,
        canonicalSource: canonicalSource || "missing",
        keyUsed: usedKey,
        sourceUsed: usedSource,
        fallbackUsed: usedKey !== canonicalKey,
        fallbackKey: usedKey !== canonicalKey ? usedKey : "",
      });
      return usedValue;
    }
    emitContractDiag({
      concept: concept,
      kind: "scalar",
      canonicalKey: canonicalKey,
      canonicalSource: canonicalSource || "missing",
      keyUsed: "",
      sourceUsed: "none",
      fallbackUsed: false,
      fallbackKey: "",
      unresolved: true,
    });
    return "";
  }

  function pickList(ex, prefill, fields, keys, opts) {
    var list = Array.isArray(keys) ? keys : [keys];
    var canonicalKey = (opts && opts.canonicalKey) || list[0] || "";
    var concept = (opts && opts.concept) || canonicalKey;
    var canonicalSource = "";
    var usedSource = "";
    var usedKey = "";
    var out = [];
    var i;
    for (i = 0; i < list.length; i++) {
      var k = list[i];
      var fromEx = ex && ex[k];
      if (fromEx != null && fromEx !== "") {
        usedSource = "ex";
        usedKey = k;
        out = arrayish(fromEx);
        if (k === canonicalKey) canonicalSource = "ex";
        break;
      }
      var fromP = prefill && prefill[k];
      if (fromP != null && fromP !== "") {
        usedSource = "prefill";
        usedKey = k;
        out = arrayish(fromP);
        if (k === canonicalKey) canonicalSource = "prefill";
        break;
      }
      if (!canonicalSource && k === canonicalKey) canonicalSource = "missing";
    }
    if (!out.length && fields) {
      for (i = 0; i < list.length; i++) {
        var fk = list[i];
        if (fields[fk] != null && fields[fk] !== "") {
          usedSource = "fields";
          usedKey = fk;
          out = arrayish(fields[fk]);
          if (fk === canonicalKey) canonicalSource = "fields";
          break;
        }
      }
    }
    emitContractDiag({
      concept: concept,
      kind: "list",
      canonicalKey: canonicalKey,
      canonicalSource: canonicalSource || "missing",
      keyUsed: usedKey,
      sourceUsed: usedSource || "none",
      fallbackUsed: !!usedKey && usedKey !== canonicalKey,
      fallbackKey: !!usedKey && usedKey !== canonicalKey ? usedKey : "",
      unresolved: out.length === 0,
      itemCount: out.length,
    });
    return out;
  }

  function mergeNewBaseKeysIntoExplorer(ex, prefill) {
    var out = Object.assign({}, ex || {});
    var p = prefill || {};
    NEW_BASE_EXPLORER_PREFILL_KEYS.forEach(function (k) {
      if (p[k] != null && p[k] !== "") out[k] = p[k];
    });
    return out;
  }

  function buildFieldMap(vm) {
    var prefill = (vm && vm.prefill) || {};
    var ex = mergeNewBaseKeysIntoExplorer((vm && vm.ex) || {}, prefill);
    var fields = (vm && vm.fields) || {};
    var listRow = (vm && vm.listRow) || {};

    return {
      operatorId: pickField(ex, prefill, fields, ["operatorId"]) || nz(listRow.id),
      // Canonical-first contract: prefer `companyName`; keep aliases for compatibility.
      companyName:
        nz(vm && vm.companyName) ||
        pickField(ex, prefill, fields, ["companyName", "company_name", "Company Name"], {
          concept: "companyName",
          canonicalKey: "companyName",
        }),
      companyDescription:
        nz(vm && vm.statement) ||
        pickField(ex, prefill, fields, ["companyDescription", "Company Description"]),
      website: pickField(ex, prefill, fields, ["website", "Website"]),
      headquarters: pickField(ex, prefill, fields, ["headquarters", "Headquarters"]),
      parentCompany: pickField(ex, prefill, fields, ["parentCompany", "platform", "Platform"], {
        concept: "parentCompany",
        canonicalKey: "parentCompany",
      }),
      yearEstablished: pickField(ex, prefill, fields, ["yearEstablished", "Year Established"]),
      yearsInBusiness: pickField(ex, prefill, fields, ["yearsInBusiness", "Years in Business"]),
      // Canonical-first contract: prefer camelCase keys, then legacy titles/snake_case fallbacks.
      activeCountries: pickList(ex, prefill, fields, ["activeCountries", "Active Countries"], {
        concept: "activeCountries",
        canonicalKey: "activeCountries",
      }),
      activeMarkets: pickList(ex, prefill, fields, ["activeMarkets", "Active Markets / Cities", "active_markets"], {
        concept: "activeMarkets",
        canonicalKey: "activeMarkets",
      }),
      marketPresenceType: pickField(ex, prefill, fields, ["marketPresenceType", "Market Presence Type"]),
      regions: pickList(ex, prefill, fields, ["regions", "regionsSupported", "Regions Supported"], {
        concept: "regions",
        canonicalKey: "regions",
      }),
      specificMarkets: pickList(ex, prefill, fields, ["specificMarkets", "Specific Markets"], {
        concept: "specificMarkets",
        canonicalKey: "specificMarkets",
      }),
      serviceModelsSupported: pickList(ex, prefill, fields, [
        "serviceModelsSupported",
        "Service Models Supported",
        "primaryServiceModel",
      ], {
        concept: "serviceModelsSupported",
        canonicalKey: "serviceModelsSupported",
      }),
      chainScalesSupported: pickList(ex, prefill, fields, [
        "chainScalesSupported",
        "Chain Scales Supported",
        "chainScale",
        "chainScales",
      ], {
        concept: "chainScalesSupported",
        canonicalKey: "chainScalesSupported",
      }),
      managementStructuresSupported: pickList(ex, prefill, fields, [
        "managementStructuresSupported",
        "Management Structures Supported",
      ]),
      minimumKeyCount: pickField(ex, prefill, fields, ["minimumKeyCount", "Minimum Key Count"]),
      offeredServices: pickList(ex, prefill, fields, ["offeredServices", "Offered Services"]),
      revenueManagementCapability: pickField(ex, prefill, fields, [
        "revenueManagementCapability",
        "Revenue Management Capability",
      ]),
      salesPlatform: pickField(ex, prefill, fields, ["salesPlatform", "Sales Platform"]),
      fbCapabilityLevel: pickField(ex, prefill, fields, [
        "fbCapabilityLevel",
        "fBCapabilityLevel",
        "F&B Capability Level",
      ]),
      newBuildOpeningExperience: pickField(ex, prefill, fields, [
        "newBuildOpeningExperience",
        "New-Build Opening Experience",
      ]),
      conversionReflagExperience: pickField(ex, prefill, fields, [
        "conversionReflagExperience",
        "Conversion / Reflag Experience",
      ]),
      preOpeningSupportCapability: pickField(ex, prefill, fields, [
        "preOpeningSupportCapability",
        "Pre-Opening Support Capability",
      ]),
      brandFamiliesOperated: pickList(ex, prefill, fields, ["brandFamiliesOperated", "Brand Families Operated"]),
      softBrandLifestyleExperience: pickField(ex, prefill, fields, [
        "softBrandLifestyleExperience",
        "Soft Brand / Lifestyle Experience",
      ]),
      brandsPortfolioDetail: pickField(ex, prefill, fields, ["brandsPortfolioDetail", "Brands Portfolio Detail"]),
      similarProjectCaseStudies: pickField(ex, prefill, fields, [
        "similarProjectCaseStudies",
        "Similar Project Case Studies",
      ]),
      ownerReportingLevel: pickField(ex, prefill, fields, ["ownerReportingLevel", "Owner Reporting Level"]),
      governanceCadence: pickField(ex, prefill, fields, ["governanceCadence", "Governance Cadence"]),
      dataConfidenceLevel: pickField(ex, prefill, fields, ["dataConfidenceLevel", "Data Confidence Level"]),
      sourceType: pickField(ex, prefill, fields, ["sourceType", "Source Type"]),
      lastUpdatedDate: pickField(ex, prefill, fields, ["lastUpdatedDate", "Last Updated Date"]),
      brandedResidencesAllowed: pickField(ex, prefill, fields, [
        "brandedResidencesAllowed",
        "Branded Residences Allowed",
      ]),
      mixedUseAllowed: pickField(ex, prefill, fields, ["mixedUseAllowed", "Mixed-Use Development Allowed"]),
      brandedResidenceExperienceLevel: pickField(ex, prefill, fields, [
        "brandedResidenceExperienceLevel",
        "Branded Residence Experience Level",
      ]),
      brandedResidenceProgramModelsSupported: pickList(ex, prefill, fields, [
        "brandedResidenceProgramModelsSupported",
        "Branded Residence Program Models Supported",
      ]),
      condoRentalProgramModelsSupported: pickList(ex, prefill, fields, [
        "condoRentalProgramModelsSupported",
        "Condo Rental Program Models Supported",
      ]),
      brandedResidencePropertiesManaged: pickField(ex, prefill, fields, [
        "brandedResidencePropertiesManaged",
        "Branded Residence Properties Managed",
      ]),
      mixedUseHospitalityExperience: pickField(ex, prefill, fields, [
        "mixedUseHospitalityExperience",
        "Mixed-Use Hospitality Experience",
      ]),
      hoaCondoAssociationInterface: pickField(ex, prefill, fields, [
        "hoaCondoAssociationInterface",
        "HOA / Condo Association Interface",
      ]),
      residenceSalesClosingSupport: pickField(ex, prefill, fields, [
        "residenceSalesClosingSupport",
        "Residence Sales & Closing Support",
      ]),
      bf_signal_residence: pickField(ex, prefill, fields, [
        "bf_signal_residence",
        "Branded Residence Fit Signal",
      ]),
    };
  }

  function listIncludes(hay, needle) {
    if (!needle) return false;
    var n = String(needle).toLowerCase();
    return hay.some(function (x) {
      return String(x).toLowerCase().indexOf(n) !== -1 || n.indexOf(String(x).toLowerCase()) !== -1;
    });
  }

  function hexToRgba(hex, alpha) {
    var h = String(hex || "").replace(/^#/, "");
    if (h.length === 3) {
      h = h
        .split("")
        .map(function (c) {
          return c + c;
        })
        .join("");
    }
    if (h.length !== 6) return "rgba(108, 114, 255, " + alpha + ")";
    var r = parseInt(h.slice(0, 2), 16);
    var g = parseInt(h.slice(2, 4), 16);
    var b = parseInt(h.slice(4, 6), 16);
    return "rgba(" + r + "," + g + "," + b + "," + alpha + ")";
  }

  /** STR-style ladder for hero chip order (operators often support many scales). */
  var HERO_CHAIN_SCALE_LADDER = [
    "luxury",
    "upper upscale",
    "upscale",
    "upper midscale",
    "midscale",
    "economy",
  ];

  function normHeroChainScaleKey(label) {
    var s = String(label || "")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trim();
    if (!s) return "";
    if (s.indexOf("luxury") !== -1) return "luxury";
    if (s.indexOf("upper upscale") !== -1) return "upper upscale";
    if (s.indexOf("upscale") !== -1 && s.indexOf("upper") === -1) return "upscale";
    if (s.indexOf("upper midscale") !== -1) return "upper midscale";
    if (s.indexOf("midscale") !== -1) return "midscale";
    if (s.indexOf("economy") !== -1) return "economy";
    if (s.indexOf("independent") !== -1) return "independent";
    return "";
  }

  function heroChainScaleDisplayLabel(key) {
    if (key === "upper upscale") return "Upper Upscale";
    if (key === "upper midscale") return "Upper Midscale";
    if (key === "independent") return "Independent";
    return key.replace(/\b\w/g, function (c) {
      return c.toUpperCase();
    });
  }

  function heroHighlightChipHtml(label) {
    var text = String(label || "").toUpperCase();
    var Gold = global.OperatorExplorerGoldMock;
    var color = Gold && Gold.chainScaleLabelToColor ? Gold.chainScaleLabelToColor(label) : null;
    if (!color) {
      return '<span class="oe-hero-chip">' + escapeHtml(text) + "</span>";
    }
    // Stronger tint than Brand-parity 0.14 so mid/lower scales stay readable on navy.
    return (
      '<span class="oe-hero-chip oe-hero-chip--chain-scale" style="--oe-chip-scale:' +
      escapeHtml(color) +
      ";color:" +
      escapeHtml(color) +
      ";border-color:" +
      escapeHtml(color) +
      ";background-color:" +
      hexToRgba(color, 0.28) +
      '">' +
      escapeHtml(text) +
      "</span>"
    );
  }

  /**
   * Hero chips: all supported chain scales (not capped at 3 — operators differ from single-scale brands),
   * then up to `extraLimit` non-scale highlights.
   */
  function buildHeroHighlightChips(fieldMap, extraLimit) {
    var otherMax = Math.max(0, extraLimit == null ? 2 : extraLimit);
    var fm = fieldMap || {};
    var scaleKeys = [];
    var seenScale = {};
    var otherChips = [];
    var seenOther = {};

    (fm.chainScalesSupported || []).forEach(function (s) {
      var key = normHeroChainScaleKey(s);
      if (!key || seenScale[key]) return;
      seenScale[key] = true;
      scaleKeys.push(key);
    });
    scaleKeys.sort(function (a, b) {
      var ia = HERO_CHAIN_SCALE_LADDER.indexOf(a);
      var ib = HERO_CHAIN_SCALE_LADDER.indexOf(b);
      if (ia < 0 && ib < 0) return a.localeCompare(b);
      if (ia < 0) return 1;
      if (ib < 0) return -1;
      return ia - ib;
    });

    function pushOther(label) {
      var t = nz(label);
      if (!t) return;
      var key = t.toLowerCase();
      if (seenOther[key] || otherChips.length >= otherMax) return;
      if (normHeroChainScaleKey(t)) return;
      seenOther[key] = true;
      otherChips.push(t);
    }

    (fm.serviceModelsSupported || []).forEach(function (sm) {
      if (/third[- ]?party|full.*management/i.test(sm)) pushOther("Third-party management");
      else pushOther(sm);
    });
    (fm.managementStructuresSupported || []).forEach(function (m) {
      pushOther(m);
    });
    if (nz(fm.softBrandLifestyleExperience) && !/no|none|not/i.test(fm.softBrandLifestyleExperience)) {
      pushOther("Soft / lifestyle");
    }
    if (nz(fm.marketPresenceType)) pushOther(fm.marketPresenceType);
    (fm.regions || []).slice(0, 1).forEach(function (r) {
      pushOther(r);
    });

    return scaleKeys.map(heroChainScaleDisplayLabel).concat(otherChips);
  }

  /** Hero trust chip from operator.governance — always-on OE footnote (Source-Informed / AI-Assisted / …). */
  function buildHeroVerificationLineHtml(vm) {
    vm = vm || {};
    var governance = ensureOperatorExplorerTrustGovernance(vm);
    if (
      typeof global !== "undefined" &&
      global.ProfileGovernanceTrustChip &&
      global.ProfileGovernanceTrustChip.governanceTrustChipHtml
    ) {
      return global.ProfileGovernanceTrustChip.governanceTrustChipHtml(governance, {
        badgeClass: "oe-hero-badge-verified",
        subtitleClass: "meta-muted dc-governance-trust-subtitle",
      });
    }
    return "";
  }

  /**
   * Client-side always-on enricher when API governance is incomplete
   * (missing label or missing Last Reviewed / Source Basis / Region).
   */
  function ensureOperatorExplorerTrustGovernance(vm) {
    var governance =
      vm && vm.governance && typeof vm.governance === "object" ? Object.assign({}, vm.governance) : {};
    var label = governance.displayLabel != null ? String(governance.displayLabel).trim() : "";
    var subtitle = governance.displaySubtitle != null ? String(governance.displaySubtitle).trim() : "";
    var complete =
      label &&
      /Last Reviewed:\s*\S+/i.test(subtitle) &&
      /Source Basis:\s*\S+/i.test(subtitle) &&
      /Region:\s*\S+/i.test(subtitle);
    if (complete) return governance;

    var name =
      (vm.prefill && (vm.prefill.company_name || vm.prefill.companyName)) ||
      (vm.fields && vm.fields.company_name) ||
      vm.companyName ||
      "";
    var isCala = /CALA|LATAM|Aimbridge|Cenote|Playa|Royalton|Tafer|Presidente|Marta|Santa Fe|Brittain|Remington/i.test(
      String(name)
    );
    var isBrandManaged = /\(Managed\)|Four Seasons|Auberge|Rosewood|Shangri|Mandarin|Sonesta|Barceló|Barcelo|Meliá|Melia|Iberostar|Accor|Marriott|Hilton|Hyatt|IHG|Minor/i.test(
      String(name)
    );
    var validation = governance.validationStatus || "";
    if (!label) {
      if (/Company Published|AI-Assisted/i.test(validation) || (isBrandManaged && !isCala)) {
        label = "AI-Assisted Profile";
      } else {
        label = "Source-Informed Profile";
      }
    }
    var sourceBasis = governance.sourceBasis || "";
    if (!sourceBasis) {
      sourceBasis = /AI-Assisted Profile/i.test(label) ? "Company Materials" : "Reviewed Sources";
    }
    var region =
      (governance.sourceRegion && String(governance.sourceRegion)) ||
      (isCala ? "CALA-Specific" : isBrandManaged ? "Global Reference" : "Regional");
    var regionFacing =
      region === "CALA-Specific"
        ? "CALA-specific"
        : region === "Market-Specific"
          ? "Market-specific"
          : region === "Unknown"
            ? "Not region-specific"
            : region;
    var lastIso = governance.lastReviewedDate || "2026-08-11";
    var lastFmt = formatOeClientFootnoteDate(lastIso) || "Aug 11, 2026";
    var parts = [];
    if (!/Last Reviewed:/i.test(subtitle)) parts.push("Last Reviewed: " + lastFmt);
    if (!/Source Basis:/i.test(subtitle)) parts.push("Source Basis: " + sourceBasis);
    if (!/Region:/i.test(subtitle)) parts.push("Region: " + regionFacing);
    if (parts.length) {
      subtitle = subtitle ? subtitle + " · " + parts.join(" · ") : parts.join(" · ");
      // If subtitle was partial, rebuild cleanly
      if (!/Last Reviewed:/.test(subtitle) || !/Source Basis:/.test(subtitle) || !/Region:/.test(subtitle)) {
        subtitle =
          "Last Reviewed: " +
          lastFmt +
          " · Source Basis: " +
          sourceBasis +
          " · Region: " +
          regionFacing;
      }
    }
    governance.displayLabel = label;
    governance.displaySubtitle = subtitle;
    governance.sourceBasis = sourceBasis;
    return governance;
  }

  function formatOeClientFootnoteDate(iso) {
    var s = String(iso || "").split("T")[0];
    if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
    var d = new Date(s + "T12:00:00");
    if (isNaN(d.getTime())) return null;
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  }

  function renderHeroActionsHtml(vm, fieldMap) {
    var isDemo =
      typeof document !== "undefined" &&
      document.documentElement &&
      document.documentElement.classList.contains("oe-profile--demo");
    var chips = isDemo
      ? ["Upscale", "Third-party management", "Regional footprint"]
      : buildHeroHighlightChips(fieldMap, 2);
    var chipHtml = chips.map(heroHighlightChipHtml).join("");
    var opId = (fieldMap && fieldMap.operatorId) || (vm && vm.operatorId) || "";
    var saveBtn =
      opId && String(opId).indexOf("rec") === 0
        ? '<button type="button" class="btn oe-operator-save-btn" data-oe-operator-id="' +
          escapeHtml(String(opId)) +
          '">Save</button>'
        : '<button type="button" class="btn oe-hero-save-btn" disabled title="Operator record unavailable">Save</button>';
    var exportPdfBtn = "";
    if (opId && String(opId).indexOf("rec") === 0 && document.documentElement) {
      var isEmbed = document.documentElement.classList.contains("embed-mode");
      var isExport = document.documentElement.classList.contains("oe-export-pdf");
      if (!isEmbed && !isExport) {
        var dealId = "";
        try {
          dealId = new URLSearchParams(window.location.search || "").get("dealId") || "";
        } catch (_) {}
        exportPdfBtn =
          '<a class="btn oe-export-pdf-link" href="/operator-explorer-export.html?id=' +
          encodeURIComponent(String(opId)) +
          (dealId ? "&dealId=" + encodeURIComponent(dealId) : "") +
          '" target="_blank" rel="noopener">Export PDF</a>';
      }
    }
    return (
      '<div class="btn-row">' +
      saveBtn +
      exportPdfBtn +
      '<button type="button" class="btn btn--primary" disabled title="Coming soon">Request Introduction</button>' +
      "</div>" +
      (chipHtml
        ? '<div class="oe-hero-chip-row" aria-label="Operator highlights">' + chipHtml + "</div>"
        : "")
    );
  }

  function buildBadges(fieldMap) {
    var badges = [];
    var fm = fieldMap || {};

    (fm.activeCountries || []).slice(0, 4).forEach(function (c) {
      badges.push({ label: c + " presence", kind: "market" });
    });
    (fm.activeMarkets || []).slice(0, 3).forEach(function (m) {
      badges.push({ label: m, kind: "market" });
    });

    (fm.chainScalesSupported || []).forEach(function (s) {
      if (/select/i.test(s)) badges.push({ label: "Select-service capable", kind: "capability" });
      if (/luxury|upscale/i.test(s)) badges.push({ label: s, kind: "capability" });
    });

    (fm.serviceModelsSupported || []).forEach(function (sm) {
      if (/third[- ]?party|full.*management/i.test(sm)) {
        badges.push({ label: "Full third-party management", kind: "capability" });
      }
      if (/branded residential|mixed-use/i.test(sm)) {
        badges.push({ label: "Branded residence capable", kind: "capability" });
      }
    });

    if (nz(fm.brandedResidencesAllowed) === "Yes") {
      badges.push({ label: "Branded residences allowed", kind: "capability" });
    } else if (/case-by-case/i.test(nz(fm.brandedResidencesAllowed))) {
      badges.push({ label: "Branded residences (Case-by-Case)", kind: "capability" });
    }
    if (
      nz(fm.brandedResidenceExperienceLevel) &&
      !/none documented|unknown/i.test(nz(fm.brandedResidenceExperienceLevel))
    ) {
      badges.push({
        label: "Branded residence experience: " + fm.brandedResidenceExperienceLevel,
        kind: "capability",
      });
    }

    if (nz(fm.preOpeningSupportCapability) && !/no|none|not/i.test(fm.preOpeningSupportCapability)) {
      badges.push({ label: "Pre-opening support", kind: "capability" });
    }
    if (nz(fm.newBuildOpeningExperience) && !/no|none|not/i.test(fm.newBuildOpeningExperience)) {
      badges.push({ label: "New-build opening experience", kind: "capability" });
    }
    if (nz(fm.revenueManagementCapability) && !/no|none|not/i.test(fm.revenueManagementCapability)) {
      badges.push({ label: "Revenue management", kind: "capability" });
    }
    if (nz(fm.ownerReportingLevel)) {
      badges.push({ label: "Owner reporting", kind: "governance" });
      if (/institutional/i.test(fm.ownerReportingLevel)) {
        badges.push({ label: "Institutional reporting", kind: "governance" });
      }
    }

    (fm.offeredServices || []).forEach(function (svc) {
      if (/brand compliance/i.test(svc)) badges.push({ label: "Brand compliance support", kind: "capability" });
      if (/owner reporting/i.test(svc) && !badges.some(function (b) { return b.label === "Owner reporting"; })) {
        badges.push({ label: "Owner reporting", kind: "governance" });
      }
    });

    var conf = nz(fm.dataConfidenceLevel);
    if (conf) {
      if (/inferred/i.test(conf)) badges.push({ label: "Inferred data", kind: "confidence" });
      else if (/operator/i.test(conf)) badges.push({ label: "Operator-provided data", kind: "confidence" });
      else badges.push({ label: conf, kind: "confidence" });
    }
    if (nz(fm.lastUpdatedDate)) {
      badges.push({ label: "Last updated: " + fm.lastUpdatedDate, kind: "meta" });
    }

    var seen = {};
    return badges.filter(function (b) {
      var k = b.label.toLowerCase();
      if (seen[k]) return false;
      seen[k] = true;
      return true;
    });
  }

  function renderBadgesHtml(badges) {
    if (!badges || !badges.length) return "";
    return (
      '<div class="oe-profile-badges" role="list">' +
      badges
        .map(function (b) {
          return (
            '<span class="oe-profile-badge oe-profile-badge--' +
            escapeHtml(b.kind || "default") +
            '" role="listitem">' +
            escapeHtml(b.label) +
            "</span>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function snapshotRow(label, value) {
    var v = nz(value);
    if (!v && (!value || !Array.isArray(value) || !value.length)) return "";
    if (Array.isArray(value)) {
      v = value.join(", ");
      if (!v) return "";
    }
    return (
      '<div class="oe-snapshot-row"><dt>' +
      escapeHtml(label) +
      "</dt><dd>" +
      escapeHtml(v) +
      "</dd></div>"
    );
  }

  function snapshotSection(title, rowsHtml) {
    if (!rowsHtml) return "";
    return (
      '<section class="oe-snapshot-section"><h3 class="oe-snapshot-section-title">' +
      escapeHtml(title) +
      '</h3><dl class="oe-snapshot-dl">' +
      rowsHtml +
      "</dl></section>"
    );
  }

  function buildSnapshotRailHtml(fieldMap) {
    var fm = fieldMap || {};
    var sections = [];

    sections.push(
      snapshotSection(
        "Profile Snapshot",
        snapshotRow("Company", fm.companyName) +
          snapshotRow("Description", fm.companyDescription) +
          snapshotRow("Headquarters", fm.headquarters) +
          snapshotRow("Website", fm.website) +
          snapshotRow("Platform / parent", fm.parentCompany) +
          snapshotRow("Data confidence", fm.dataConfidenceLevel) +
          snapshotRow("Last updated", fm.lastUpdatedDate)
      )
    );

    sections.push(
      snapshotSection(
        "Market Presence",
        snapshotRow("Active countries", fm.activeCountries) +
          snapshotRow("Active markets / cities", fm.activeMarkets) +
          snapshotRow("Market presence type", fm.marketPresenceType) +
          snapshotRow("Regions", fm.regions) +
          snapshotRow("Specific markets", fm.specificMarkets)
      )
    );

    sections.push(
      snapshotSection(
        "Operating Profile",
        snapshotRow("Service models supported", fm.serviceModelsSupported) +
          snapshotRow("Chain scales supported", fm.chainScalesSupported) +
          snapshotRow("Management structures supported", fm.managementStructuresSupported) +
          snapshotRow("Brand families operated", fm.brandFamiliesOperated) +
          snapshotRow("Minimum key count", fm.minimumKeyCount)
      )
    );

    sections.push(
      snapshotSection(
        "Services & Platform",
        snapshotRow("Offered services", fm.offeredServices) +
          snapshotRow("Revenue management capability", fm.revenueManagementCapability) +
          snapshotRow("Sales platform", fm.salesPlatform) +
          snapshotRow("F&B capability", fm.fbCapabilityLevel)
      )
    );

    sections.push(
      snapshotSection(
        "Opening / Transition Support",
        snapshotRow("New-build opening experience", fm.newBuildOpeningExperience) +
          snapshotRow("Conversion / reflag experience", fm.conversionReflagExperience) +
          snapshotRow("Pre-opening support capability", fm.preOpeningSupportCapability)
      )
    );

    sections.push(
      snapshotSection(
        "Branded Residence & Mixed-Use",
        snapshotRow("Branded residences allowed", fm.brandedResidencesAllowed) +
          snapshotRow("Mixed-use development allowed", fm.mixedUseAllowed) +
          snapshotRow("Residence experience level", fm.brandedResidenceExperienceLevel) +
          snapshotRow("Program models supported", fm.brandedResidenceProgramModelsSupported) +
          snapshotRow("Rental pool models supported", fm.condoRentalProgramModelsSupported) +
          snapshotRow("Residence properties managed", fm.brandedResidencePropertiesManaged) +
          snapshotRow("Mixed-use hospitality projects", fm.mixedUseHospitalityExperience) +
          snapshotRow("HOA / condo interface", fm.hoaCondoAssociationInterface) +
          snapshotRow("Sales & closing support", fm.residenceSalesClosingSupport) +
          snapshotRow("Fit signal", fm.bf_signal_residence)
      )
    );

    sections.push(
      snapshotSection(
        "Owner Reporting & Governance",
        snapshotRow("Owner reporting level", fm.ownerReportingLevel) +
          snapshotRow("Governance cadence", fm.governanceCadence) +
          snapshotRow("Source type", fm.sourceType) +
          snapshotRow("Data confidence level", fm.dataConfidenceLevel)
      )
    );

    var body = sections.filter(Boolean).join("");
    if (!body) return "";
    return (
      '<section class="section oe-new-base-snapshot-rail" aria-label="Operator profile from Operator Setup">' +
      '<h2 class="section-title">Operator Profile</h2>' +
      '<div class="oe-snapshot-rail-grid">' +
      body +
      "</div></section>"
    );
  }

  function alignmentListItems(items, max) {
    return (items || [])
      .map(function (x) {
        return nz(x);
      })
      .filter(Boolean)
      .slice(0, max || 3);
  }

  function buildAlignmentContextHtml(companyRow, dealId) {
    if (!companyRow) return "";
    var signals = alignmentListItems(companyRow.alignmentSignals, 3);
    var validation = alignmentListItems(
      companyRow.whatNeedsValidation || companyRow.reviewConsiderations || companyRow.questionsToClarify,
      3
    );
    var band = nz(companyRow.alignmentBand) || "Insufficient Data";
    var score =
      companyRow.alignmentScoreOptional != null && companyRow.alignmentScoreOptional !== ""
        ? String(companyRow.alignmentScoreOptional)
        : "";
    var keyConsideration = nz(companyRow.keyConsideration);
    var oasHref =
      "/operator-alignment-snapshot.html?dealId=" +
      encodeURIComponent(dealId) +
      "&embed=1";

    return (
      '<aside class="oe-alignment-context" aria-label="Alignment context for this deal">' +
      '<h2 class="oe-alignment-context-title">Alignment Context</h2>' +
      '<p class="oe-alignment-context-disclaimer">Alignment context for this deal is based on available Operator Setup data and current deal inputs. It does not indicate operator approval, availability, or commercial terms.</p>' +
      '<div class="oe-alignment-context-band"><span class="oe-alignment-context-label">Alignment band</span> <strong>' +
      escapeHtml(band) +
      "</strong>" +
      (score ? ' <span class="oe-alignment-context-score">Informational score: ' + escapeHtml(score) + "</span>" : "") +
      "</div>" +
      (signals.length
        ? '<div class="oe-alignment-context-block"><h3>Alignment signals</h3><ul>' +
          signals.map(function (s) { return "<li>" + escapeHtml(s) + "</li>"; }).join("") +
          "</ul></div>"
        : "") +
      (validation.length
        ? '<div class="oe-alignment-context-block"><h3>Items to validate</h3><ul>' +
          validation.map(function (s) { return "<li>" + escapeHtml(s) + "</li>"; }).join("") +
          "</ul></div>"
        : "") +
      (keyConsideration
        ? '<div class="oe-alignment-context-block"><h3>Key consideration</h3><p>' +
          escapeHtml(keyConsideration) +
          "</p></div>"
        : "") +
      '<p class="oe-alignment-context-actions"><a class="oe-alignment-context-link" href="' +
      escapeHtml(oasHref) +
      '" target="_blank" rel="noopener">View full Operator Alignment Snapshot</a></p>' +
      "</aside>"
    );
  }

  function buildAlignmentUnavailableHtml(reason) {
    var msg =
      reason === "auth"
        ? "Alignment context requires a signed-in session. Open this profile from My Deals (Operator Strategy) or sign in, then reload."
        : "Alignment context is not available for this operator and deal.";
    return (
      '<aside class="oe-alignment-context oe-alignment-context--empty" aria-label="Alignment context">' +
      '<h2 class="oe-alignment-context-title">Alignment Context</h2>' +
      "<p>" +
      escapeHtml(msg) +
      "</p>" +
      "</aside>"
    );
  }

  function buildDemoBannerHtml() {
    return (
      '<div class="oe-demo-banner" role="status">' +
      "<strong>Sample operator profile</strong> Demo layout only. Live profiles load from Operator Setup when a record id (<code>rec…</code>) is provided." +
      "</div>"
    );
  }

  async function fetchAlignmentContextForOperator(
    dealId,
    urlOperatorId,
    profileOperatorId,
    fetchFn
  ) {
    // Backwards-compatible argument shifting: (dealId, operatorId, fetchFn)
    if (typeof profileOperatorId === "function" && !fetchFn) {
      fetchFn = profileOperatorId;
      profileOperatorId = null;
    }
    var fetchImpl = fetchFn || global.fetch;
    if (!dealId || !urlOperatorId) return { company: null, error: null };
    var url =
      "/api/operator-alignment-snapshot/" + encodeURIComponent(dealId) + "/companies";
    try {
      var res = await fetchImpl(url);
      var data = await res.json().catch(function () { return {}; });
      if (!res.ok || !data || !data.success) {
        var errCode = data && (data.error || data.code);
        if (res.status === 401 || errCode === "authentication_required") {
          return { company: null, error: "authentication_required" };
        }
        return { company: null, error: errCode || "Failed to load alignment context" };
      }
      var companies = data.companiesForConsideration || data.companies || [];
      var urlIdLower = String(urlOperatorId || "").toLowerCase();
      var profileIdLower = String(profileOperatorId || urlOperatorId || "")
        .toLowerCase();

      function norm(v) {
        return String(v || "").toLowerCase();
      }

      var matchedBy = null;
      var match =
        companies.find(function (c) {
          if (!c) return false;
          var ids = [
            c.operatorId,
            c.operatorRecordId,
            c.operatorSetupId,
            c.id,
            c.recordId,
          ];
          for (var i = 0; i < ids.length; i++) {
            var nid = norm(ids[i]);
            if (!nid) continue;
            if (nid === profileIdLower) {
              matchedBy = "profileId";
              return true;
            }
            if (nid === urlIdLower) {
              matchedBy = "urlId";
              return true;
            }
          }
          return false;
        }) || null;

      if (!match) {
        // Fallback: name match (debug only), used when ids are missing on snapshot rows.
        match =
          companies.find(function (c) {
            if (!c) return false;
            var name = norm(c.companyName || c.operatorName);
            if (!name) return false;
            if (name === norm(data.companyName) || name === norm(data.operatorName)) {
              matchedBy = "name";
              return true;
            }
            return false;
          }) || null;
      }

      if (
        typeof global !== "undefined" &&
        global.location &&
        /^localhost$/i.test(global.location.hostname)
      ) {
        try {
          var logPayload = {
            urlOperatorId: urlOperatorId || null,
            profileOperatorId: profileOperatorId || null,
            dealId: dealId,
            companiesReturned: companies.length,
            matchedBy: matchedBy || null,
            matchedOperatorId: match ? match.operatorId || match.operatorRecordId || null : null,
          };
          // eslint-disable-next-line no-console
          console.log("[Operator Explorer alignment match]", logPayload);
        } catch (e) {
          // swallow debug logging failures
        }
      }

      return { company: match, error: null };
    } catch (e) {
      return { company: null, error: e && e.message ? e.message : "Network error" };
    }
  }

  async function mountAlignmentContext(dealId, urlOperatorId, profileOperatorId, fetchFn) {
    var el = document.getElementById("alignmentContext");
    if (!el) return;
    if (!dealId) {
      el.hidden = true;
      el.innerHTML = "";
      return;
    }
    el.hidden = false;
    el.innerHTML =
      '<p class="oe-alignment-context-loading">Loading alignment context…</p>';
    var result = await fetchAlignmentContextForOperator(
      dealId,
      urlOperatorId,
      profileOperatorId,
      fetchFn
    );
    if (result.company) {
      el.innerHTML = buildAlignmentContextHtml(result.company, dealId);
    } else if (result.error === "authentication_required") {
      el.innerHTML = buildAlignmentUnavailableHtml("auth");
    } else {
      el.innerHTML = buildAlignmentUnavailableHtml();
    }
  }

  function mountProfileChrome(vm) {
    var fieldMap = buildFieldMap(vm);
    var legacyBadgesEl = document.getElementById("heroBadges");
    if (legacyBadgesEl) {
      legacyBadgesEl.innerHTML = "";
      legacyBadgesEl.style.display = "none";
    }
    var verifiedEl = document.getElementById("heroVerifiedLine");
    if (verifiedEl) {
      var heroLineHtml = buildHeroVerificationLineHtml(vm);
      verifiedEl.innerHTML = heroLineHtml;
      verifiedEl.hidden = !heroLineHtml;
    }
    var actionsEl = document.getElementById("heroActions");
    if (actionsEl) {
      actionsEl.innerHTML = renderHeroActionsHtml(vm, fieldMap);
      if (global.OperatorExplorerFavorites && global.OperatorExplorerFavorites.wireSaveButtons) {
        global.OperatorExplorerFavorites.wireSaveButtons(actionsEl);
      }
    }
    var demoEl = document.getElementById("demoBanner");
    if (demoEl) {
      demoEl.innerHTML = "";
      demoEl.hidden = true;
    }
  }

  function assertNoBannedCopy(text) {
    var t = String(text || "");
    for (var i = 0; i < BANNED_COPY.length; i++) {
      if (BANNED_COPY[i].test(t)) return false;
    }
    return true;
  }

  global.OperatorExplorerNewBaseProfile = {
    NEW_BASE_EXPLORER_PREFILL_KEYS: NEW_BASE_EXPLORER_PREFILL_KEYS,
    mergeNewBaseKeysIntoExplorer: mergeNewBaseKeysIntoExplorer,
    buildFieldMap: buildFieldMap,
    buildBadges: buildBadges,
    buildHeroHighlightChips: buildHeroHighlightChips,
    buildHeroVerificationLineHtml: buildHeroVerificationLineHtml,
    renderHeroActionsHtml: renderHeroActionsHtml,
    renderBadgesHtml: renderBadgesHtml,
    buildSnapshotRailHtml: buildSnapshotRailHtml,
    buildAlignmentContextHtml: buildAlignmentContextHtml,
    buildAlignmentUnavailableHtml: buildAlignmentUnavailableHtml,
    buildDemoBannerHtml: buildDemoBannerHtml,
    fetchAlignmentContextForOperator: fetchAlignmentContextForOperator,
    mountAlignmentContext: mountAlignmentContext,
    mountProfileChrome: mountProfileChrome,
    assertNoBannedCopy: assertNoBannedCopy,
  };
})(typeof window !== "undefined" ? window : this);
