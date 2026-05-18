/**
 * Deal Readiness Snapshot — two-page document renderer (narrative + technical).
 * Data: POST /api/ai/deal-readiness-review (buildReadinessFromFields).
 */
(function (global) {
  "use strict";

  var OUTPUT_NOTE =
    "This Dealality output organizes readiness signals based on current deal inputs. " +
    "It is intended to support internal owner/advisor review and does not constitute a recommendation, " +
    "endorsement, valuation, legal advice, franchise advice, or investment advice.";

  var DEALALITY_LOGO_URL =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png";

  var REVIEW_AREAS = [
    { label: "Basic Project Information", tabs: ["Project Overview"] },
    { label: "Ownership / Control", tabs: ["Location & Site Details", "Deal & Capital Structure"] },
    { label: "Market Context", tabs: ["Location & Site Details", "Market & Performance"] },
    { label: "Brand Review Readiness", tabs: ["Brand & Op. Status"] },
    { label: "Operator Review Readiness", tabs: ["Brand & Op. Status", "Operational Expectations"] },
    { label: "Capex / PIP Clarity", tabs: ["Deal & Capital Structure", "Property Specs"] },
    { label: "Agreement Strategy", tabs: ["Lease Structure", "Deal & Capital Structure"] },
    { label: "Documentation Package", tabs: ["Uploads & Attachments", "Deal Room"] },
  ];

  var WORKFLOW_STEPS = [
    "Deal intake",
    "Readiness review",
    "Owner clarification",
    "Brand alignment review",
    "Operator capability review",
    "Opportunity brief",
    "External outreach",
  ];

  function esc(t) {
    return String(t == null ? "" : t)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function strVal(v) {
    if (v == null || v === "") return "";
    if (typeof v === "string") return v.trim();
    if (typeof v === "number" && Number.isFinite(v)) return String(v);
    if (Array.isArray(v)) {
      return v
        .map(function (x) {
          return typeof x === "string" ? x : x && x.name ? String(x.name) : "";
        })
        .filter(Boolean)
        .join(", ");
    }
    if (typeof v === "object" && v.name) return String(v.name).trim();
    return String(v).trim();
  }

  function fieldPresent(fields, names) {
    if (!fields) return "";
    for (var i = 0; i < names.length; i++) {
      var v = strVal(fields[names[i]]);
      if (v) return v;
    }
    return "";
  }

  function formatDate(iso) {
    if (!iso) return "";
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return String(iso).slice(0, 10);
      return d.toLocaleDateString(undefined, { year: "numeric", month: "long", day: "numeric" });
    } catch (_) {
      return String(iso).slice(0, 10);
    }
  }

  function readinessStageKey(stage) {
    return String(stage || "").trim().toLowerCase();
  }

  function isEarlyReadinessStage(stage) {
    var sl = readinessStageKey(stage);
    return sl === "discovery" || sl === "shaping" || sl === "advancing";
  }

  function isReadyReadinessStage(stage) {
    return readinessStageKey(stage) === "ready";
  }

  function isHighReadinessScore(score) {
    var n = Number(score);
    return Number.isFinite(n) && n >= 90;
  }

  function isSubstantiallyCompleteReadiness(score, stage) {
    return isReadyReadinessStage(stage);
  }

  function isBroadlyCompleteReadiness(stage) {
    var sl = readinessStageKey(stage);
    return sl === "ready" || sl.indexOf("ready for external") >= 0;
  }

  var FOUNDATIONAL_MISSING_PATTERNS = [
    /project type/i,
    /stage of development/i,
    /currently branded/i,
    /currently managed/i,
    /submarket|city|state|country|location/i,
    /rooms|keys/i,
    /ownership|control/i,
    /preferred deal structure/i,
    /pip|capex|renovation budget/i,
  ];

  function countMissingFoundationalFields(data) {
    var n = 0;
    (data.missingInformation || []).forEach(function (m) {
      var f = String((m && (m.field || m.label)) || "");
      if (FOUNDATIONAL_MISSING_PATTERNS.some(function (re) {
        return re.test(f);
      })) {
        n += 1;
      }
    });
    return n;
  }

  function normalizeLocationToken(s) {
    return String(s || "")
      .trim()
      .replace(/\s+/g, " ");
  }

  function locationTokensMatch(a, b) {
    if (!a || !b) return false;
    return a.toLowerCase() === b.toLowerCase();
  }

  function locationSegmentContainsToken(segment, token) {
    if (!segment || !token) return false;
    var segLow = segment.toLowerCase();
    var tokLow = token.toLowerCase();
    if (segLow === tokLow) return true;
    return tokLow.length >= 3 && segLow.indexOf(tokLow) >= 0;
  }

  /** Avoid duplicate city/country/region tokens (e.g. "Buenos Aires, Argentina, Argentina"). */
  function formatLocationLine(meta) {
    meta = meta || {};
    var market = meta.market && meta.market !== "—" ? normalizeLocationToken(meta.market) : "";
    var country = meta.country && meta.country !== "—" ? normalizeLocationToken(meta.country) : "";

    if (!market && !country) return "—";
    if (!market) return country;
    if (!country) return market;
    if (locationSegmentContainsToken(market, country) && !locationTokensMatch(market, country)) {
      return market;
    }

    var segments = market.split(/,\s*/).map(normalizeLocationToken).filter(Boolean);
    var deduped = [];
    segments.forEach(function (seg) {
      if (
        !deduped.some(function (existing) {
          return locationTokensMatch(existing, seg);
        })
      ) {
        deduped.push(seg);
      }
    });
    deduped = deduped.filter(function (seg) {
      return !locationTokensMatch(seg, country);
    });

    if (!deduped.length) return country;

    var core = deduped.join(", ");
    if (locationSegmentContainsToken(core, country)) return core;
    return core + ", " + country;
  }

  function reviewPriorityLabel(severity) {
    var s = String(severity || "").toLowerCase();
    if (s === "high" || s === "critical") return "High";
    if (s === "medium" || s === "warning") return "Medium";
    return "Low";
  }

  var EXECUTIVE_FIELD_LABELS = {
    "Project Type": "Project type",
    "Stage of Development": "Development stage",
    "Property Name": "Project name",
    "Total Number of Rooms/Keys": "Key count / room program",
    "Number of Standard Rooms": "Standard room count",
    "Number of Suites": "Suite count",
    "Current Form of Site Control": "Site control",
    "Zoning Status": "Zoning status",
    "Zoned for Hotel Development": "Hotel zoning",
    "Total Project Cost Range": "Project cost range",
    "PIP Budget Range (if conversion)": "PIP budget range",
    "PIP / CapEx Status": "PIP / CapEx status",
    "Preferred Deal Structure": "Preferred deal structure",
    "Lease Type": "Lease type",
    "Primary Demand Drivers": "Demand drivers",
    "Primary Demand Drivers Other": "Other demand drivers",
    "Key Competitors": "Key competitors",
    "Is the hotel currently branded?": "Current brand status",
    "Is the hotel currently managed by a third-party operator?": "Current operator status",
    "Ownership Type": "Ownership type",
    "Ownership Structure": "Ownership structure",
    "Primary Goal for the Hotel": "Primary owner goal",
    "Top Priorities for Project": "Owner priorities",
    "Total Renovation / PIP Budget": "Capex / PIP budget",
    "Estimated PIP or Renovation Budget Range": "Capex / PIP budget",
    "Estimated PIP or Renovation Budget": "Capex / PIP budget",
    "Property Condition Assessment": "Property condition",
    "Current Property Condition": "Property condition",
    "Brand Positioning": "Target positioning",
    "Target Chain Scale": "Target positioning",
    "Primary Competitive Set": "Competitive set",
    "Franchise vs Management Preference": "Deal structure preference",
    "Local Identity / Design Story": "Local identity / design story",
    "Design / Local Identity Narrative": "Local identity / design story",
    "Additional Amenities": "Amenities & facilities",
    "Financial Model Available?": "Financial model",
    "Main Contact Name": "Primary contact",
    "Email Address": "Contact email",
    "Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site?":
      "Prior brand / management agreement history",
    "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?":
      "Prior brand / management agreement history",
  };

  var CLARIFICATION_LABELS = EXECUTIVE_FIELD_LABELS;

  var CLARIFICATION_WHY = {
    "Capex / PIP budget": "Determines which brand pathways are realistic.",
    "Property condition": "Impacts conversion feasibility and standards compliance.",
    "Operating model": "Determines whether operator review should happen before or alongside brand review.",
    "Target positioning": "Clarifies upscale vs. upper-upscale direction.",
    "Competitive set": "Helps validate rate, demand, and positioning assumptions.",
    "Agreement priorities": "Helps frame future brand/operator conversations.",
    "Local identity / design story": "Important if soft-brand or lifestyle pathways are being considered.",
  };

  function executiveFieldLabel(field) {
    var f = String(field || "").trim();
    if (!f) return "Additional clarification";
    if (EXECUTIVE_FIELD_LABELS[f]) return EXECUTIVE_FIELD_LABELS[f];
    var low = f.toLowerCase();
    if (
      (/agreeement/i.test(f) || /similar agreement pertaining/i.test(low)) &&
      /franchise|branded management|affiliation/i.test(low)
    ) {
      return "Prior brand / management agreement history";
    }
    if (/franchise|branded management|affiliation/i.test(low) && /proposed hotel|proposed hotel or site/i.test(low)) {
      return "Prior brand / management agreement history";
    }
    if (/^total number of rooms|^number of rooms|\/keys/i.test(low)) return "Key count / room program";
    if (/site control|form of site control/i.test(low)) return "Site control";
    if (/total project cost/i.test(low)) return "Project cost range";
    if (/preferred deal structure/i.test(low)) return "Preferred deal structure";
    if (/primary demand drivers/i.test(low)) return "Demand drivers";
    if (/pip\s*\/\s*capex|capex status/i.test(low)) return "PIP / CapEx status";
    if (/currently branded/i.test(low)) return "Current brand status";
    if (/currently managed|third.party operator/i.test(low)) return "Current operator status";
    if (/project type/i.test(low)) return "Project type";
    if (/stage of development/i.test(low)) return "Development stage";
    return clarificationAreaLabelHeuristic(f, low);
  }

  function clarificationAreaLabelHeuristic(f, low) {
    if (!low) low = String(f || "").toLowerCase();
    if (/^project type$/i.test(f)) return "Project classification";
    if (/stage of development|development stage|project stage/i.test(low)) return "Development stage";
    if (/currently branded|brand affiliation|hotel branded/i.test(low)) return "Current brand status";
    if (/additional amenit|amenities &|facilities/i.test(low)) return "Amenities & facilities";
    if (/budget|pip|renovation|capex|capital/i.test(low)) return "Capex / PIP budget";
    if (/condition|renovation status|property age/i.test(low)) return "Property condition";
    if (/operator|management model|operating/i.test(low)) return "Operating model";
    if (/position|chain scale|brand tier/i.test(low)) return "Target positioning";
    if (/competitive|comp set|competition/i.test(low)) return "Competitive set";
    if (/agreement|deal structure|franchise|lease/i.test(low)) return "Agreement priorities";
    if (/design|identity|story|concept/i.test(low)) return "Local identity / design story";
    if (/submarket|location detail|market study/i.test(low)) return "Market / submarket detail";
    if (/photo|floor plan|document|attachment|upload/i.test(low)) return "Documentation package";
    if (/\?/.test(f)) return "Owner confirmation item";
    return f.length > 48 ? f.slice(0, 45).trim() + "…" : f;
  }

  function clarificationAreaLabel(field) {
    return executiveFieldLabel(field);
  }

  var GAP_BUCKET_ORDER = [
    "projectDefinition",
    "capex",
    "property",
    "brandPath",
    "operations",
    "market",
    "commercial",
    "amenities",
    "documentation",
  ];

  var GAP_BUCKET_PHRASES = {
    projectDefinition:
      "project definition (including project type, development stage, and how the asset is positioned today)",
    capex: "capex, PIP, and renovation investment expectations",
    property: "property condition and conversion feasibility",
    brandPath: "brand positioning, affiliation strategy, and standards tolerance",
    operations: "operating model and third-party management direction",
    market: "market, submarket, and competitive context",
    commercial: "agreement priorities and commercial structure preferences",
    amenities: "amenities, facilities, and guest-facing product definition",
    documentation: "supporting documentation such as photos, plans, and performance materials",
  };

  function gapBucketForField(field) {
    var f = String(field || "").trim();
    var low = f.toLowerCase();
    if (/^project type$|stage of development|development stage|project stage/i.test(low)) {
      return "projectDefinition";
    }
    if (/currently branded|brand affiliation|hotel branded|chain scale|brand position/i.test(low)) {
      return "brandPath";
    }
    if (/additional amenit|amenities|facilities|fb outlet|meeting space/i.test(low)) return "amenities";
    if (/budget|pip|renovation|capex|capital|investment/i.test(low)) return "capex";
    if (/condition|renovation status|property age|building/i.test(low)) return "property";
    if (/operator|management model|operating|third.party/i.test(low)) return "operations";
    if (/competitive|comp set|submarket|market study|demand|performance/i.test(low)) return "market";
    if (/agreement|deal structure|franchise|lease|preferred deal/i.test(low)) return "commercial";
    if (/photo|floor plan|upload|attachment|document|deal room/i.test(low)) return "documentation";
    if (/owner objective|strategic intent|motivation/i.test(low)) return null;
    if (/location|city|country|address/i.test(low)) return null;
    if (/room|keys/i.test(low)) return null;
    if (/\?/.test(f)) {
      if (/brand|affiliation|flag/i.test(low)) return "brandPath";
      if (/operator|manag/i.test(low)) return "operations";
      return "projectDefinition";
    }
    return "projectDefinition";
  }

  function collectGapBuckets(data) {
    var buckets = {};
    function mark(field) {
      var b = gapBucketForField(field);
      if (b) buckets[b] = true;
    }
    (data.missingInformation || []).forEach(function (m) {
      mark(m && (m.field || m.label));
    });
    (data.weakInformation || []).forEach(function (w) {
      mark(w && (w.field || w.label));
    });
    return buckets;
  }

  function joinNaturalList(items) {
    if (!items || !items.length) return "";
    if (items.length === 1) return items[0];
    if (items.length === 2) return items[0] + " and " + items[1];
    return items.slice(0, -1).join(", ") + ", and " + items[items.length - 1];
  }

  function executiveGapPhrases(data) {
    var buckets = collectGapBuckets(data);
    return GAP_BUCKET_ORDER.filter(function (k) {
      return buckets[k];
    }).map(function (k) {
      return GAP_BUCKET_PHRASES[k];
    });
  }

  function describeProjectType(pt) {
    var p = String(pt || "").trim();
    if (!p) return "";
    if (/conversion/i.test(p) && /reposition|rebrand/i.test(p)) {
      return "urban conversion and repositioning project";
    }
    if (/conversion|reposition|rebrand/i.test(p)) {
      return p.toLowerCase() + " project";
    }
    return p.toLowerCase() + " hospitality project";
  }

  function isReadyForExternalReviewStage(stage) {
    return readinessStageKey(stage).indexOf("ready for external") >= 0;
  }

  /** Context label for Readiness Summary (no screening phrase — composed in buildReadinessSummaryLeadSentence). */
  function readinessSummaryContextLabel(projectType) {
    var p = String(projectType || "").trim();
    if (!p) return "";
    if (/new build|ground.?up/i.test(p)) return "new-build";
    if (/conversion|reflag|re-flag/i.test(p)) return "conversion/reflag";
    if (/renovation|reposition|rebrand/i.test(p)) return "renovation/repositioning";
    if (/land|development site|greenfield/i.test(p)) return "land/development-site";
    if (/operating|existing/i.test(p)) return "existing operating-asset";
    return p.toLowerCase().replace(/\s+/g, " ");
  }

  var READINESS_SUMMARY_SCREENING_PHRASE = " that may be relevant for brand and operator screening";

  /** Page 1 Readiness Summary lead — screening phrase appears once. */
  function buildReadinessSummaryLeadSentence(meta) {
    var pt = meta.projectType && meta.projectType !== "—" ? meta.projectType : "";
    var pos = meta.targetPositioning || "";
    var contextLabel = readinessSummaryContextLabel(pt);
    var sentence;
    if (!contextLabel) {
      sentence = "The current inputs describe a hospitality opportunity" + READINESS_SUMMARY_SCREENING_PHRASE;
    } else {
      sentence =
        "The current inputs describe a " + contextLabel + " hospitality opportunity" + READINESS_SUMMARY_SCREENING_PHRASE;
    }
    if (pos) {
      sentence += ", with potential fit across " + positioningReviewPhrase(pos);
    } else if (pt && /conversion|reposition|rebrand/i.test(pt)) {
      sentence += ", with potential fit across upscale, upper-upscale, lifestyle, or soft-brand pathways";
    }
    return sentence + ".";
  }

  function whyItMattersForArea(areaLabel) {
    return (
      CLARIFICATION_WHY[areaLabel] ||
      "Supports a more complete review package before external conversations."
    );
  }

  function fieldLabel(item) {
    if (!item) return "";
    if (typeof item === "string") return item;
    return item.label || item.field || item.highlightField || "";
  }

  /** Display label for gap lists (Page 1 + Page 2); Airtable field names unchanged. */
  function displayFieldLabel(item) {
    return executiveFieldLabel(fieldLabel(item));
  }

  function tabScoreMap(data) {
    var map = {};
    var labeled =
      data.tabScoresLabeled && data.tabScoresLabeled.length
        ? data.tabScoresLabeled
        : data.sectionScoresLabeled || [];
    labeled.forEach(function (row) {
      var key = row.label || row.id;
      if (key) map[key] = row.score;
    });
    var sections = data.tabScores || data.sectionScores || {};
    Object.keys(sections).forEach(function (k) {
      if (map[k] == null) map[k] = sections[k];
    });
    return map;
  }

  function avgTabPct(tabMap, tabs) {
    var sum = 0;
    var n = 0;
    tabs.forEach(function (t) {
      if (tabMap[t] != null && tabMap[t] !== "") {
        sum += Number(tabMap[t]);
        n += 1;
      }
    });
    if (!n) return null;
    return Math.round(sum / n);
  }

  function countMissingInTabs(data, tabs) {
    var n = 0;
    (data.missingInformation || []).forEach(function (m) {
      if (!m) return;
      var tab = m.relatedTab || m.section || "";
      if (tabs.indexOf(tab) >= 0) n += 1;
    });
    return n;
  }

  function reviewAreaStatus(areaLabel, pct, missingInArea) {
    if (
      areaLabel === "Documentation Package" &&
      pct == null &&
      missingInArea === 0
    ) {
      return "Not Evaluated in Current Readiness Run";
    }
    return pctToStatus(pct, missingInArea);
  }

  function pctToStatus(pct, missingInArea) {
    if (pct == null && missingInArea > 2) return "Not Provided";
    if (pct == null) return missingInArea > 0 ? "Needs Clarification" : "Not Provided";
    if (missingInArea >= 3 || pct < 25) return "Needs Clarification";
    if (pct >= 90 && missingInArea === 0) return "Strong";
    if (pct >= 75) return "Mostly Clear";
    if (pct >= 60) return "Partially Complete";
    if (pct >= 40) return "Developing";
    return "Needs Clarification";
  }

  function areaExecutiveNote(areaLabel, status, meta, fields, missingInArea) {
    var market = meta.market && meta.market !== "—" ? meta.market : "";
    var keys = meta.keyCount && meta.keyCount !== "—" ? meta.keyCount : "";
    var pt = meta.projectType && meta.projectType !== "—" ? meta.projectType : "";
    switch (areaLabel) {
      case "Basic Project Information":
        if (status === "Strong" || status === "Mostly Clear") {
          return "Location, asset type, key count, and project type are clear.";
        }
        return "Core project descriptors still need confirmation before outreach.";
      case "Ownership / Control":
        if (status === "Strong" || status === "Mostly Clear") {
          return "Owner control appears likely, but documentation should be confirmed.";
        }
        return "Ownership and control structure should be confirmed in writing.";
      case "Market Context":
        if (status === "Strong") return "Market context appears well documented.";
        if (market) {
          return formatLocationLine(meta) + " is identified, but submarket and competitive context may need detail.";
        }
        return "Market and submarket context need more definition.";
      case "Brand Review Readiness":
        if (status === "Strong" || status === "Mostly Clear") {
          return "Brand path is reasonably defined for initial screening.";
        }
        return "Brand path is open, but positioning and standards tolerance need refinement.";
      case "Operator Review Readiness":
        if (status === "Strong") return "Operating model appears clear for operator screening.";
        return "Owner-operated or mixed status creates a decision point around third-party management.";
      case "Capex / PIP Clarity":
        if (status === "Strong" || status === "Mostly Clear") {
          return "Investment range and tolerance appear documented.";
        }
        return "Budget range and investment tolerance are not yet defined.";
      case "Agreement Strategy":
        if (status === "Strong" || status === "Mostly Clear") {
          return "Commercial structure preferences appear documented.";
        }
        return "Franchise, soft brand, operator, and commercial preferences need more detail.";
      case "Documentation Package":
        if (status === "Not Evaluated in Current Readiness Run") {
          return "Upload and deal-room tabs are not part of the current intake score; supporting documents may still be added for owner/advisor review.";
        }
        if (status === "Strong") return "Supporting materials appear sufficient for initial review.";
        if (status === "Partially Complete" || status === "Developing") {
          return "Additional photos, floor plans, performance data, and capex notes would help.";
        }
        return "Documentation package is thin relative to a full outreach brief.";
      default:
        if (status === "Strong") return "This area appears well supported.";
        if (missingInArea > 0) return "Additional detail would strengthen this part of the package.";
        return "This area may benefit from further owner input.";
    }
  }

  function buildReviewAreaRows(data, meta, fields) {
    meta = meta || {};
    fields = fields || {};
    var tabMap = tabScoreMap(data);
    return REVIEW_AREAS.map(function (area) {
      var miss = countMissingInTabs(data, area.tabs);
      var pct = avgTabPct(tabMap, area.tabs);
      var status = reviewAreaStatus(area.label, pct, miss);
      return {
        area: area.label,
        status: status,
        notes: areaExecutiveNote(area.label, status, meta, fields, miss),
      };
    });
  }

  function buildClarificationAreas(data) {
    var seen = {};
    var rows = [];
    function pushRow(row) {
      var f = String(row.field || row.label || "").trim();
      var area = clarificationAreaLabel(f);
      var key = area.toLowerCase();
      if (!f || seen[key]) return;
      seen[key] = true;
      rows.push({
        area: area,
        whyItMatters: row.whyItMatters || whyItMattersForArea(area),
        priority: row.priority || "Medium",
      });
    }
    (data.missingInformation || []).forEach(function (m) {
      if (!m) return;
      pushRow({ field: m.field || m.label, tab: m.relatedTab || m.section, priority: "High" });
    });
    (data.weakInformation || []).forEach(function (w) {
      if (!w) return;
      pushRow({
        field: w.field || w.label,
        tab: w.relatedTab || w.section,
        priority: "Medium",
      });
    });
    var plan = data.scoreImprovementPlan || {};
    (plan.priorityActions || []).forEach(function (a) {
      if (!a) return;
      var f = a.relatedField || "";
      if (!f && a.label) f = String(a.label).replace(/^Complete\\s+["']?|["']$/g, "").trim();
      pushRow({
        field: f || a.label,
        tab: a.relatedTab || "",
        priority: reviewPriorityLabel(a.severity),
      });
    });
    rows.sort(function (a, b) {
      var pr = { High: 0, Medium: 1, Low: 2 };
      return (pr[a.priority] != null ? pr[a.priority] : 2) - (pr[b.priority] != null ? pr[b.priority] : 2);
    });
    return rows.slice(0, 24);
  }

  var PAGE1_CLARIFICATION_MAX = 8;

  /** Page 1 — top clarification areas by severity (blocking → limiting → enhancement), max 8. */
  function buildClarificationAreasForPage1(data) {
    var limit = PAGE1_CLARIFICATION_MAX;
    var seen = {};
    var rows = [];

    function pushIssue(item, priority, severityKey) {
      if (!item || rows.length >= limit) return;
      var f = fieldLabel(item);
      var area = executiveFieldLabel(f);
      var key = area.toLowerCase();
      if (!f || seen[key]) return;
      seen[key] = true;
      rows.push({
        area: area,
        field: f,
        whyItMatters: whyItMattersForArea(area),
        priority: priority,
        severityKey: severityKey,
      });
    }

    (data.blockingIssues || []).forEach(function (item) {
      pushIssue(item, "High", "blocking");
    });
    (data.limitingIssues || []).forEach(function (item) {
      pushIssue(item, "Medium", "limiting");
    });
    if (rows.length < limit) {
      (data.enhancementIssues || []).forEach(function (item) {
        pushIssue(item, "Low", "enhancement");
      });
    }

    if (!rows.length) {
      (data.missingInformation || []).forEach(function (item) {
        pushIssue(item, "High", "missing");
      });
      if (rows.length < limit) {
        (data.weakInformation || []).forEach(function (item) {
          pushIssue(item, "Medium", "weak");
        });
      }
    }

    if (!rows.length) {
      buildClarificationAreas(data).forEach(function (row) {
        if (rows.length >= limit) return;
        pushIssue({ field: row.field || row.area, label: row.area }, row.priority || "Medium", "legacy");
      });
    }

    return rows.slice(0, limit);
  }

  /** Total clarification pool size (for Page 1 overflow note). */
  function countClarificationPoolSize(data) {
    var blocking = (data.blockingIssues || []).length;
    var limiting = (data.limitingIssues || []).length;
    var enhancement = (data.enhancementIssues || []).length;
    var ble = blocking + limiting + enhancement;
    if (ble > 0) return ble;
    var missing = (data.missingInformation || []).length;
    var weak = (data.weakInformation || []).length;
    if (missing + weak > 0) return missing + weak;
    return buildClarificationAreas(data).length;
  }

  function truncateText(text, max) {
    var s = String(text || "").trim();
    if (s.length <= max) return s;
    return s.slice(0, max - 1).trim() + "…";
  }

  function positioningReviewPhrase(pos) {
    if (!pos) return "upscale, upper-upscale, lifestyle, or soft-brand review";
    var p = String(pos).toLowerCase();
    if (/upscale|luxury|premium|lifestyle|soft|collection|boutique/i.test(p)) {
      return pos + " and adjacent brand pathways";
    }
    return pos + " review pathways";
  }

  function summarizeGapThemes(data) {
    return executiveGapPhrases(data);
  }

  function buildDocumentedStrengthsParagraph(meta, fields) {
    var sentences = [];
    var loc = formatLocationLine(meta);
    var keys = meta.keyCount && meta.keyCount !== "—" ? meta.keyCount : "";
    var pt = meta.projectType && meta.projectType !== "—" ? meta.projectType : "";
    var pos = meta.targetPositioning || "";

    if (loc && loc !== "—") {
      sentences.push(
        "the package anchors the opportunity in " +
          loc +
          (keys ? ", with an identified scale of approximately " + keys + " keys" : "")
      );
    } else if (keys) {
      sentences.push("the package identifies an asset scale of approximately " + keys + " keys");
    }

    if (pt) {
      sentences.push(
        "a defined project concept (" + pt.toLowerCase() + ") that helps narrow which brand and operator conversations are relevant"
      );
    }

    var ownerObj = fieldPresent(fields, [
      "Owner's Objective for the Project",
      "Owner Objective",
      "Owner's Objective",
      "Strategic Intent Summary",
    ]);
    if (ownerObj) {
      sentences.push(
        "an owner narrative that explains why affiliation or operating partners are being explored"
      );
    }

    if (pos) {
      sentences.push(
        "initial positioning direction (" + pos + ") that supports screening against appropriate chain scales"
      );
    }

    var brandSt = fieldPresent(fields, [
      "Is the hotel currently branded?",
      "Is the hotel currently managed by a third-party operator?",
    ]);
    if (brandSt) {
      sentences.push("enough brand and operating context to understand the starting point for review");
    }

    var agreement = fieldPresent(fields, [
      "Preferred Deal Structure",
      "Franchise vs Management Preference",
    ]);
    if (agreement) {
      sentences.push("documented preferences on deal structure that can frame early commercial conversations");
    }

    if (!sentences.length) return "";

    return (
      "Taken together, " +
      joinNaturalList(sentences) +
      ". These elements give an advisor or investment committee enough context to assess whether the opportunity merits deeper work, even where specific diligence items remain open."
    );
  }

  function buildReadinessPostureParagraph(score, stage, missing, data) {
    var n = Number(score);
    var stageLabel = stage && stage !== "—" ? String(stage) : "not yet staged";
    var parts = [];

    if (Number.isFinite(n)) {
      parts.push(
        "On a final readiness basis, the opportunity registers at " +
          n +
          "/100 with a current readiness stage of \"" +
          stageLabel +
          "\"."
      );
    } else {
      parts.push(
        "A final readiness score is not yet available, and the deal should be treated as in-progress from an intake standpoint."
      );
    }

    var sl = readinessStageKey(stage);
    if (sl === "ready") {
      parts.push(
        "That profile suggests the package is substantially complete for structured internal review based on current inputs."
      );
      parts.push(
        "The opportunity may support selective external conversations after owner/advisor validation of positioning, economics, and execution assumptions."
      );
    } else if (sl.indexOf("ready for external") >= 0) {
      parts.push(
        "That profile suggests the package is broadly complete for structured review, with remaining validation items to address before broader circulation."
      );
    } else if (sl === "advancing") {
      if (!hasFoundationalCapApplied(data)) {
        parts.push(
          "That profile indicates the deal can support internal review, but several important inputs should still be clarified before formal external outreach."
        );
      }
    } else if (sl === "shaping") {
      parts.push(
        "That profile suggests early structure is in place, but additional information is needed before the package can support reliable brand/operator review."
      );
    } else if (sl === "discovery") {
      parts.push(
        "That profile suggests the opportunity remains in early intake and requires core deal information before structured review."
      );
    } else if (Number.isFinite(n) && n >= 85 && missing === 0) {
      parts.push(
        "That profile is comparatively strong for an early-stage package and may support selective external conversations once an advisor has validated the underlying assumptions."
      );
      parts.push(
        "At this level, the focus may shift from basic completeness to quality of narrative: positioning logic, economics, and execution credibility."
      );
    } else if (Number.isFinite(n) && n >= 70) {
      parts.push(
        "That profile indicates the deal is beyond raw intake and can support a structured internal review, but it should not yet be circulated as a finished outreach brief."
      );
      parts.push(
        "The next step is targeted owner clarification on the highest-impact gaps, followed by a refreshed readiness pass before brand or operator outreach expands materially."
      );
    } else if (Number.isFinite(n) && n >= 55) {
      parts.push(
        "That profile suggests meaningful progress, but the opportunity still requires owner-led clarification across several core workstreams before it is treated as outreach-ready."
      );
      parts.push(
        "Internal review can begin in parallel, provided stakeholders understand that external conversations may generate rework until the open items are resolved."
      );
    } else {
      parts.push(
        "That profile suggests the opportunity remains in an earlier intake posture and should be strengthened before it is shared broadly inside or outside the organization."
      );
      parts.push(
        "Priority should be given to completing core project, market, and commercial inputs so downstream brand and operator screening is efficient rather than speculative."
      );
    }

    var blocking = (data.blockingIssues || []).length;
    if (blocking > 0) {
      parts.push(
        hasFoundationalCapApplied(data)
          ? "Blocking foundational clarification items are also present and should be resolved early so they do not undermine screening conclusions."
          : "Blocking clarification signals are also present and should be resolved early so they do not undermine screening conclusions."
      );
    }

    return parts.join(" ");
  }

  function buildGapsAndPathParagraph(data, stage) {
    var phrases = executiveGapPhrases(data);
    if (!phrases.length) return "";

    if (hasFoundationalCapApplied(data) && readinessStageKey(stage) === "advancing") {
      return (
        "Closing the open foundational clarification items (" +
        joinNaturalList(phrases) +
        ") may improve screening quality and clarify which affiliation pathways appear realistic. " +
        "Once addressed, the package should be re-run through readiness review so the narrative, score, and workflow status reflect the updated inputs."
      );
    }

    var intro = isEarlyReadinessStage(stage)
      ? "The opportunity is not yet fully ready for broad outreach. "
      : hasFoundationalCapApplied(data)
        ? "Foundational clarification items remain open. "
        : "Based on current inputs, a small number of items may still benefit from owner validation before broader circulation. ";

    return (
      intro +
      "The highest-priority gaps relate to " +
      joinNaturalList(phrases) +
      ". Closing these items will improve screening quality, reduce iterative Q&A with brands and operators, and clarify which affiliation pathways may be realistic. " +
      "Once addressed, the package should be re-run through readiness review so the narrative, score, and workflow status reflect the updated story."
    );
  }

  function buildOutreachReadyParagraph(score, stage) {
    var n = Number(score);
    return (
      "Based on current inputs, the opportunity appears substantially complete for structured internal review" +
      (Number.isFinite(n) ? " (final readiness score " + n + "/100)" : "") +
      ". It may support selective external conversations after owner/advisor validation of positioning, economics, and execution assumptions."
    );
  }

  function hasFoundationalCapApplied(data) {
    return !!(data && data.appliedScoreCaps && data.appliedScoreCaps.length);
  }

  function getLowestAppliedCap(data) {
    var caps = (data && data.appliedScoreCaps) || [];
    if (!caps.length) return null;
    var lowest = caps[0];
    for (var i = 1; i < caps.length; i++) {
      if (caps[i].maxScore < lowest.maxScore) lowest = caps[i];
    }
    return lowest;
  }

  /** API cap reason → display label (technical + narrative copy). */
  var CAP_REASON_FIELD_LABELS = {
    "Missing market / country": "Market / Country",
    "Missing project type": "Project Type",
    "Missing stage of development": "Stage of Development",
    "Missing key count": "Key Count",
    "Missing ownership / control status": "Ownership / Control Status",
    "Missing current brand status": "Current Brand Status",
    "Missing current operator status": "Current Operator Status",
    "Missing preferred deal structure": "Preferred Deal Structure",
    "Missing capex / PIP status": "Capex / PIP Status",
    "Missing owner objectives / priorities": "Owner Objectives / Priorities",
    "Missing contact / decision-maker info": "Contact / Decision-Maker Info",
    "Missing documentation package signals": "Documentation Package",
  };

  function capReasonToFieldLabel(reason) {
    var r = String(reason || "").trim();
    if (CAP_REASON_FIELD_LABELS[r]) return CAP_REASON_FIELD_LABELS[r];
    var m = /^Missing (.+)$/i.exec(r);
    if (m) {
      return m[1]
        .replace(/\s*\/\s*/g, " / ")
        .split(/\s+/)
        .map(function (w) {
          return w.charAt(0).toUpperCase() + w.slice(1);
        })
        .join(" ");
    }
    return r || "A foundational field";
  }

  function foundationalCapFieldLabels(data) {
    var caps = (data && data.appliedScoreCaps) || [];
    var seen = {};
    var labels = [];
    caps.forEach(function (c) {
      var lab = capReasonToFieldLabel(c.reason);
      if (lab && !seen[lab]) {
        seen[lab] = true;
        labels.push(lab);
      }
    });
    return labels;
  }

  /** Plain-language cap explanation for Page 1 (no weighted score or technical mechanics). */
  function contextProjectTypePhrase(ctx) {
    if (!ctx) return "";
    switch (ctx.projectTypeContext) {
      case "new_build":
        return "new-build";
      case "conversion_reflag":
        return "conversion/reflag";
      case "renovation_repositioning":
        return "renovation/repositioning";
      case "operating_asset":
        return "existing operating asset";
      case "land_development_site":
        return "land/development site";
      default:
        return "";
    }
  }

  function buildContextNarrativeParagraph(data) {
    var ctx = data && data.readinessContext;
    if (!ctx || !data.contextAwareScoring) return "";
    var parts = [];
    var ptPhrase = contextProjectTypePhrase(ctx);
    if (ptPhrase) {
      parts.push("The current inputs describe a " + ptPhrase + " opportunity.");
    }
    if (
      ctx.projectTypeContext === "conversion_reflag" ||
      ctx.projectTypeContext === "renovation_repositioning" ||
      ctx.projectTypeContext === "operating_asset"
    ) {
      parts.push(
        "For this " +
          (ctx.projectTypeContext === "conversion_reflag" ? "conversion/reflag" : ptPhrase || "conversion or operating") +
          " context, current brand status and PIP/CapEx clarity carry higher readiness impact than they would for a pre-opening new-build case."
      );
    } else if (ctx.projectTypeContext === "new_build" || ctx.projectTypeContext === "land_development_site") {
      parts.push(
        "For this pre-operating context, site control, development stage, and total project cost carry more weight than operating PIP status or current brand affiliation."
      );
    }
    if (ctx.dealStructureContext === "franchise_only") {
      parts.push(
        "Because the preferred deal structure is franchise-oriented in the current inputs, lease-specific fields are not treated as readiness gaps unless a lease path is also in scope."
      );
    } else if (ctx.dealStructureContext === "lease" || ctx.dealStructureContext === "flexible_open") {
      parts.push(
        "Because the preferred deal structure is lease-oriented in the current inputs, lease-type and related economics fields are included in readiness when applicable."
      );
    } else if (ctx.dealStructureContext === "brand_operator" || ctx.dealStructureContext === "management_agreement") {
      parts.push(
        "Because brand and operator paths are in scope in the current inputs, operator status and operator criteria carry more readiness weight than in a franchise-only review."
      );
    }
    return parts.join(" ");
  }

  function buildNarrativeCapParagraph(data) {
    if (!hasFoundationalCapApplied(data)) return "";
    var stageLabel = data.readinessStage && data.readinessStage !== "—" ? String(data.readinessStage) : "its current stage";
    var labels = foundationalCapFieldLabels(data);
    var parts = [
      "The opportunity is substantially populated across several readiness areas, but one or more foundational inputs remain unresolved.",
    ];
    if (labels.length) {
      parts.push(
        "Because " +
          joinNaturalList(labels) +
          " still require clarification, the opportunity is categorized as " +
          stageLabel +
          " rather than Ready."
      );
    } else {
      var lowest = getLowestAppliedCap(data);
      if (lowest && lowest.reason) {
        var fieldLabel = capReasonToFieldLabel(lowest.reason);
        parts.push(
          "Because " +
            fieldLabel +
            " still requires clarification, the opportunity is categorized as " +
            stageLabel +
            " rather than Ready."
        );
      } else {
        parts.push(
          "Foundational clarification items remain open, and the opportunity is categorized as " +
            stageLabel +
            " rather than Ready."
        );
      }
    }
    return parts.join(" ");
  }

  function scoreCalcRow(label, value) {
    return "<tr><th>" + esc(label) + "</th><td>" + value + "</td></tr>";
  }

  /** Page 2 — structured weighted-v2 score calculation (supporting technical detail). */
  function renderTechnicalScoreCalculation(data) {
    var score = data.dealReadinessScore;
    var stage = data.readinessStage || "—";
    var weighted =
      data.weightedCompletionScore != null
        ? data.weightedCompletionScore
        : data.scoreBreakdown && data.scoreBreakdown.weightedCompletionScore != null
          ? data.scoreBreakdown.weightedCompletionScore
          : null;
    var caps = data.appliedScoreCaps || [];
    var capApplied = caps.length > 0;
    var lowestCap = capApplied ? getLowestAppliedCap(data) : null;
    var lowestCapScore =
      data.scoreBreakdown && data.scoreBreakdown.lowestCap != null
        ? data.scoreBreakdown.lowestCap
        : lowestCap
          ? lowestCap.maxScore
          : null;
    var g = data.gapSeverityCounts || {};
    var isV2 = data.scoringModelVersion === "weighted-v2";

    var html = '<div class="drs-score-calc">';
    html +=
      '<p class="drs-muted drs-score-calc-lead">Readiness scoring is context-aware. Some fields are weighted or excluded based on project type, stage, and deal structure. For example, lease fields are only required for lease-oriented deals, and PIP fields carry more weight for conversions than new-build projects. Headline readiness score reflects weighted readiness across applicable core deal domains; foundational fields may cap the final score when missing because they materially affect the ability to evaluate the opportunity in that context. Tab percentages measure fill rate within each setup section and may differ from the headline score.</p>';

    html += '<div class="drs-table-wrap"><table class="drs-brief-table drs-score-calc-table"><tbody>';
    html += scoreCalcRow(
      "Final Readiness Score",
      esc(score != null && score !== "" ? score + " / 100" : "—")
    );
    html += scoreCalcRow("Readiness Stage", esc(stage));

    if (isV2) {
      html += scoreCalcRow(
        "Weighted Completion Score",
        esc(weighted != null && weighted !== "" ? weighted + " / 100" : "—")
      );
      html += scoreCalcRow("Foundational Cap Applied", capApplied ? "Yes" : "No");
      if (capApplied && lowestCap) {
        html += scoreCalcRow(
          "Lowest Cap Applied",
          esc(lowestCap.reason + " → maximum score " + lowestCap.maxScore)
        );
      } else {
        html += scoreCalcRow("Lowest Cap Applied", "—");
      }
      html += scoreCalcRow("Scoring Model", esc(data.scoringModelVersion || "weighted-v2"));
      if (data.draftValidationCapApplied) {
        html += scoreCalcRow(
          "Draft Validation Cap",
          esc(
            "Yes — displayed score capped at " +
              (data.scoreBreakdown && data.scoreBreakdown.draftValidationMaxScore != null
                ? data.scoreBreakdown.draftValidationMaxScore
                : "98") +
              "/100" +
              (data.computedReadinessScore != null
                ? " (computed " + data.computedReadinessScore + "/100)"
                : "")
          )
        );
      }
      if (data.contextAwareScoring && data.readinessContext) {
        var rcx = data.readinessContext;
        html += scoreCalcRow(
          "Project Type Context",
          esc(rcx.projectTypeContext || "—")
        );
        html += scoreCalcRow(
          "Deal Structure Context",
          esc(rcx.dealStructureContext || "—")
        );
        html += scoreCalcRow(
          "Context-Adjusted Required Fields",
          esc(
            data.contextAdjustedRequiredFieldCount != null
              ? String(data.contextAdjustedRequiredFieldCount)
              : "—"
          )
        );
        if (data.contextExcludedFields && data.contextExcludedFields.length) {
          html += scoreCalcRow(
            "Fields Excluded (N/A)",
            esc(String(data.contextExcludedFields.length))
          );
        }
      }
    }

    if (g.blocking != null || g.limiting != null || g.enhancement != null) {
      html += scoreCalcRow("Gap Severity — Blocking", esc(g.blocking != null ? g.blocking : "—"));
      html += scoreCalcRow("Gap Severity — Limiting", esc(g.limiting != null ? g.limiting : "—"));
      html += scoreCalcRow("Gap Severity — Enhancement", esc(g.enhancement != null ? g.enhancement : "—"));
    }
    html += "</tbody></table></div>";

    if (capApplied && caps.length) {
      html += '<h3 class="drs-score-calc-subtitle">Applied Caps</h3><ul class="drs-detail-list drs-score-calc-caps">';
      caps.forEach(function (c) {
        html += "<li>" + esc(c.reason + " → maximum score " + c.maxScore) + "</li>";
      });
      html += "</ul>";
    }

    if (data.contextExcludedFields && data.contextExcludedFields.length) {
      html += '<h3 class="drs-score-calc-subtitle">Excluded (Not Applicable in Context)</h3><ul class="drs-detail-list drs-score-calc-caps">';
      data.contextExcludedFields.slice(0, 12).forEach(function (row) {
        html +=
          "<li>" +
          esc(displayFieldLabel(row.field) + (row.reason ? " — " + row.reason : "")) +
          "</li>";
      });
      if (data.contextExcludedFields.length > 12) {
        html += "<li>" + esc("…and " + (data.contextExcludedFields.length - 12) + " more") + "</li>";
      }
      html += "</ul>";
    }
    if (data.contextTooEarlyFields && data.contextTooEarlyFields.length) {
      html += '<h3 class="drs-score-calc-subtitle">Deferred (Too Early in Context)</h3><ul class="drs-detail-list drs-score-calc-caps">';
      data.contextTooEarlyFields.forEach(function (row) {
        html +=
          "<li>" +
          esc(displayFieldLabel(row.field) + (row.reason ? " — " + row.reason : "")) +
          "</li>";
      });
      html += "</ul>";
    }

    if (capApplied && Number.isFinite(Number(weighted)) && Number.isFinite(Number(score)) && Number(weighted) > Number(score)) {
      html += '<p class="drs-muted drs-score-calc-note">';
      html +=
        "Although the weighted completion score is " +
        weighted +
        "/100, the final readiness score is capped at " +
        score +
        "/100 because one or more foundational clarification items remain unresolved.";
      if (lowestCap && lowestCap.reason) {
        var capFieldLabel = capReasonToFieldLabel(lowestCap.reason);
        html +=
          " Most readiness domains are substantially populated, but " +
          capFieldLabel +
          " is missing. Because " +
          capFieldLabel +
          " is a foundational field, the final readiness score cannot exceed " +
          lowestCapScore +
          ".";
      }
      html += "</p>";
    } else if (!capApplied && isV2) {
      html += '<p class="drs-muted drs-score-calc-note">No foundational cap was applied. The final readiness score reflects weighted domain completion and weak-response adjustments.</p>';
    }

    html += "</div>";
    return html;
  }

  function buildNarrativeStrengths(data, meta, fields) {
    var items = [];
    var ownerObj = fieldPresent(fields, [
      "Owner's Objective for the Project",
      "Owner Objective",
      "Owner's Objective",
      "Strategic Intent Summary",
    ]);
    if (ownerObj) {
      items.push({
        title: "Clear repositioning objective",
        body:
          ownerObj.length < 160
            ? "The owner appears to have a defined reason for exploring brand/operator options: " +
              truncateText(ownerObj, 160)
            : "The owner appears to have a defined reason for exploring brand/operator options, including improved distribution, stronger positioning, and increased asset value.",
      });
    }
    if (meta.keyCount && meta.keyCount !== "—") {
      items.push({
        title: "Identified asset scale",
        body:
          "The key count has been identified, which supports initial brand/operator screening. Depending on positioning, location, and economics, the asset scale may be relevant for select review pathways.",
      });
    }
    var flexible = fieldPresent(fields, [
      "Are you open to lesser-known or emerging brands with favorable terms?",
      "Are you open to considering other brands with favorable terms?",
    ]);
    if (/yes|open|flexible|willing|consider/i.test(flexible)) {
      items.push({
        title: "Flexible brand path",
        body:
          "The owner is open to different affiliation structures, which creates room to evaluate multiple potential pathways.",
      });
    }
    var pt = meta.projectType && meta.projectType !== "—" ? meta.projectType : "";
    if (/conversion|reposition|rebrand|urban/i.test(pt) || /conversion|reposition|rebrand|urban/i.test(meta.market || "")) {
      items.push({
        title: "Potential urban conversion relevance",
        body:
          "The opportunity may be relevant to brands and operators that understand urban repositioning, conversion execution, and flexible standards.",
      });
    } else if (pt) {
      items.push({
        title: "Defined project concept",
        body:
          "The opportunity is framed as a " +
          pt.toLowerCase() +
          " project, which helps narrow the universe of relevant brand and operator conversations.",
      });
    }
    var locLine = formatLocationLine(meta);
    if (locLine && locLine !== "—") {
      items.push({
        title: "Clear market anchor",
        body:
          "The deal is anchored in " + locLine + ", which supports initial market screening.",
      });
    }
    var timeline = fieldPresent(fields, [
      "Decision Timeline for Brand/Operator",
      "Development / Renovation Timeline Importance",
      "Expected Opening Date",
    ]);
    if (timeline) {
      items.push({
        title: "Timeline orientation",
        body:
          "Timing expectations appear on file, which helps frame how quickly internal review should move toward outreach planning.",
      });
    }
    if (!items.length) {
      items.push({
        title: "Foundation for review",
        body:
          "Enough baseline information exists to begin structured internal review, though additional owner input will sharpen the narrative.",
      });
    }
    return items.slice(0, 6);
  }

  function scoreInterpretation(score, stage, data) {
    var n = Number(score);
    var blocking = (data.blockingIssues || []).length;
    var sl = readinessStageKey(stage);
    if (!Number.isFinite(n) && !sl) {
      return (
        "This opportunity has a partial information base. Additional owner input may be needed before a structured readiness summary can support review."
      );
    }
    if (blocking > 0) {
      return (
        "Based on current inputs, this opportunity may support early structured review, but blocking clarification items should be resolved before broader circulation or formal external use."
      );
    }
    if (sl === "ready") {
      return (
        "This opportunity appears substantially complete for structured internal review and may support selective external conversations, subject to final owner/advisor validation."
      );
    }
    if (sl.indexOf("ready for external") >= 0) {
      return (
        "This opportunity appears broadly complete for structured review, with remaining validation items that should be addressed before broader circulation."
      );
    }
    if (sl === "advancing") {
      if (hasFoundationalCapApplied(data)) {
        return (
          "This opportunity is substantially populated across several readiness areas; foundational clarification items remain open based on current inputs."
        );
      }
      return (
        "This opportunity has enough information to support internal review, but several important inputs should be clarified before formal external outreach."
      );
    }
    if (sl === "shaping") {
      return (
        "This opportunity has early structure but requires additional information before it can support reliable brand/operator review."
      );
    }
    if (sl === "discovery") {
      return (
        "This opportunity remains in an early intake stage and requires core deal information before structured review."
      );
    }
    if (isHighReadinessScore(n)) {
      return (
        "This opportunity appears broadly complete for structured review, with remaining validation items that should be addressed before broader circulation."
      );
    }
    if (n >= 70) {
      return (
        "This opportunity has enough information to support internal review, but several important inputs should be clarified before formal external outreach."
      );
    }
    return (
      "This opportunity remains in an early intake stage and requires core deal information before structured review."
    );
  }

  function buildBusinessSummary(data, meta, fields) {
    var paragraphs = [];
    var name =
      meta.projectName && meta.projectName !== "Deal" ? meta.projectName : "This opportunity";
    var pt = meta.projectType && meta.projectType !== "—" ? meta.projectType : "";
    var loc = formatLocationLine(meta);
    var score = Number(data.dealReadinessScore);
    var stage = data.readinessStage || "";
    var missing = (data.missingInformation || []).length;

    var thesis =
      (name !== "This opportunity" ? name + ". " : "") + buildReadinessSummaryLeadSentence(meta);

    if (loc && loc !== "—") {
      thesis +=
        " The deal is situated in " +
        loc +
        ", which provides a tangible market anchor for early assessment.";
    }

    var ownerObj = fieldPresent(fields, [
      "Owner's Objective for the Project",
      "Owner Objective",
      "Owner's Objective",
      "Strategic Intent Summary",
    ]);
    if (ownerObj) {
      thesis +=
        " The owner’s stated objective suggests a deliberate strategic rationale for exploring affiliation or operating alternatives" +
        (ownerObj.length < 140 ? ": " + truncateText(ownerObj, 140) : ", centered on distribution, positioning, and long-term asset value.");
    } else if (pt && /reposition|rebrand|conversion/i.test(pt)) {
      thesis +=
        " The repositioning framing implies the owner is seeking stronger market relevance, improved commercial performance, and a clearer path to value creation through the right brand or operator partnership.";
    }

    paragraphs.push(thesis);

    var contextPara = buildContextNarrativeParagraph(data);
    if (contextPara) paragraphs.push(contextPara);

    var strengthsPara = buildDocumentedStrengthsParagraph(meta, fields);
    if (strengthsPara) paragraphs.push(strengthsPara);

    paragraphs.push(buildReadinessPostureParagraph(score, stage, missing, data));

    var capPara = buildNarrativeCapParagraph(data);
    if (capPara) paragraphs.push(capPara);

    var skipGapsPara =
      capPara && readinessStageKey(stage) === "advancing";
    var gapsPara = skipGapsPara ? "" : buildGapsAndPathParagraph(data, stage);
    if (gapsPara) {
      paragraphs.push(gapsPara);
    } else if (missing === 0 || isReadyReadinessStage(stage) || isBroadlyCompleteReadiness(stage)) {
      paragraphs.push(buildOutreachReadyParagraph(score, stage));
    }

    return paragraphs;
  }

  function mapReviewStatusLabel(stage, score) {
    var s = String(stage || "").trim();
    var sl = readinessStageKey(stage);
    var n = Number(score);
    if (!Number.isFinite(n) && !s) {
      return {
        label: "Needs Readiness Review",
        explain: "A readiness review has not been saved or scored for this deal based on current inputs.",
      };
    }
    if (sl === "discovery") {
      return {
        label: "Core Intake Still Needed",
        explain:
          "This opportunity is in early intake. Core project, market, and commercial inputs should be completed before structured review.",
      };
    }
    if (sl === "shaping") {
      return {
        label: "Needs Clarification Before Structured Review",
        explain:
          "This opportunity is taking shape, but priority clarification items should be addressed before structured review.",
      };
    }
    if (sl === "advancing") {
      return {
        label: "Eligible for Structured Review",
        explain:
          "This opportunity is ready for controlled internal review and may support selective external conversations after foundational clarification items are addressed.",
      };
    }
    if (isReadyForExternalReviewStage(stage)) {
      return {
        label: "Ready for Controlled External Review",
        explain:
          "This opportunity appears broadly complete for structured review and may support selective external conversations after owner/advisor validation of remaining inputs.",
      };
    }
    if (sl === "ready") {
      return {
        label: "Ready for Advanced Review",
        explain:
          "This opportunity appears well documented for structured internal review and selective external conversations, subject to final owner/advisor validation.",
      };
    }
    if (Number.isFinite(n) && n >= 75) {
      return {
        label: "Eligible for Structured Review",
        explain:
          "This opportunity is ready for a controlled internal review and may be prepared for selective external conversations after priority clarification items are addressed.",
      };
    }
    return {
      label: "Needs Clarification Before Structured Review",
      explain:
        "This opportunity should complete priority clarification items before it is treated as ready for structured review or outreach planning.",
    };
  }

  function workflowStatusForStep(step, data, stage, score) {
    var n = Number(score);
    var missing = (data.missingInformation || []).length;
    var blocking = (data.blockingIssues || []).length;
    var substantial = isReadyReadinessStage(stage);
    var broadlyComplete = isBroadlyCompleteReadiness(stage);
    switch (step) {
      case "Deal intake":
        return Number.isFinite(n) ? "Complete enough for initial review" : "Needed";
      case "Readiness review":
        return "Completed";
      case "Owner clarification":
        if (isBroadlyCompleteReadiness(stage)) {
          return missing > 0 ? "Owner/advisor validation needed" : "Complete enough for initial review";
        }
        return missing > 0 ? "Needed" : "Complete enough for initial review";
      case "Brand alignment review":
        return n >= 60 ? "Available" : "Needed";
      case "Operator capability review":
        return n >= 60 ? "Available" : "Needed";
      case "Opportunity brief":
        if (substantial || n >= 75) return "Draftable";
        return "Supports internal draft with noted gaps";
      case "External outreach":
        if (blocking > 0 || isEarlyReadinessStage(stage)) {
          return "Not yet recommended for broad circulation";
        }
        if (substantial) return "Available for selective external conversations";
        if (broadlyComplete && missing > 0) {
          return "Selective conversations only, after validation of remaining inputs";
        }
        if (missing > 0) return "Selective conversations only, after validation of remaining inputs";
        return broadlyComplete ? "Available for selective external conversations" : "Available";
      default:
        return "Needed";
    }
  }

  function buildWorkflowRows(data, stage, score) {
    return WORKFLOW_STEPS.map(function (step) {
      return { step: step, status: workflowStatusForStep(step, data, stage, score) };
    });
  }

  function dealMetaFromSources(sources) {
    sources = sources || {};
    var deal = sources.deal || {};
    var fields = sources.fields || sources.sourceFields || {};
    if (deal.fields && typeof deal.fields === "object") fields = deal.fields;
    var norm = sources.normalized || {};
    var listDeal = sources.listDeal || {};
    var targetPositioning =
      fieldPresent(fields, [
        "Brand Positioning",
        "Target Chain Scale",
        "Hotel Chain Scale",
        "Brand positioning preference",
      ]) || strVal(norm.chainScale) || strVal(norm.brandPositioning) || "";
    return {
      projectName:
        listDeal.projectName ||
        deal.projectName ||
        strVal(fields["Property Name"]) ||
        strVal(norm.propertyName) ||
        "Deal",
      market:
        listDeal.hotelLocation ||
        deal.hotelLocation ||
        strVal(fields["Hotel Submarket & Location"]) ||
        strVal(fields["City & State"]) ||
        strVal(norm.submarket) ||
        strVal(norm.city) ||
        "—",
      country: strVal(fields["Country"]) || strVal(norm.country) || "—",
      keyCount:
        strVal(fields["Total Number of Rooms/Keys"]) || strVal(norm.totalNumberOfRoomsKeys) || "—",
      projectType:
        listDeal.projectType ||
        deal.projectType ||
        strVal(fields["Project Type"]) ||
        strVal(norm.projectType) ||
        "—",
      targetPositioning: targetPositioning,
    };
  }

  function renderTabGrid(data) {
    var labeled = data.tabScoresLabeled && data.tabScoresLabeled.length ? data.tabScoresLabeled : null;
    var sections = data.tabScores || data.sectionScores || {};
    var html = '<div class="drs-tab-grid drs-tab-grid--brief">';
    if (labeled) {
      labeled.forEach(function (row) {
        var pctStr = row.score == null || row.score === "" ? "—" : esc(row.score) + "%";
        html +=
          '<div class="drs-tab-card drs-avoid-break"><div class="drs-tab-pct">' +
          pctStr +
          '</div><div class="drs-tab-label">' +
          esc(row.label || row.id) +
          "</div></div>";
      });
    } else {
      Object.keys(sections).forEach(function (k) {
        var v = sections[k];
        var pctStr2 = v == null || v === "" ? "—" : esc(v) + "%";
        html +=
          '<div class="drs-tab-card drs-avoid-break"><div class="drs-tab-pct">' +
          pctStr2 +
          '</div><div class="drs-tab-label">' +
          esc(k) +
          "</div></div>";
      });
    }
    html += "</div>";
    return html;
  }

  function renderExecutiveStrengths(strengths) {
    var html = '<div class="drs-strength-blocks">';
    (strengths || []).forEach(function (s) {
      if (typeof s === "string") {
        html += '<article class="drs-strength-block"><p class="drs-strength-body">' + esc(s) + "</p></article>";
        return;
      }
      html += '<article class="drs-strength-block"><h3 class="drs-strength-title">' + esc(s.title) + "</h3>";
      html += '<p class="drs-strength-body">' + esc(s.body) + "</p></article>";
    });
    html += "</div>";
    return html;
  }

  function renderDetailSection(title, items, emptyMsg) {
    var html = '<section class="drs-section drs-section--brief"><h2 class="drs-section-title">' + esc(title) + "</h2>";
    if (!items || !items.length) {
      html += '<p class="drs-muted">' + esc(emptyMsg) + "</p></section>";
      return html;
    }
    html += '<ul class="drs-detail-list">';
    items.forEach(function (item) {
      if (typeof item === "string") {
        html += "<li>" + esc(displayFieldLabel(item)) + "</li>";
        return;
      }
      var line = displayFieldLabel(item);
      var tab = item.relatedTab || item.section || "";
      if (tab) line += " (" + tab + ")";
      html += "<li>" + esc(line) + "</li>";
    });
    html += "</ul></section>";
    return html;
  }

  function coverLocationLine(meta) {
    return formatLocationLine(meta);
  }

  function coverDealHook(meta, score, stage) {
    var parts = [];
    if (meta.keyCount && meta.keyCount !== "—") parts.push(meta.keyCount + " keys");
    if (meta.projectType && meta.projectType !== "—") parts.push(meta.projectType);
    if (meta.targetPositioning) parts.push(meta.targetPositioning);
    var hook = parts.join(" · ");
    if (score != null && score !== "") {
      var scoreBit = "Readiness " + score + "/100";
      if (stage && stage !== "—") scoreBit += " · " + stage;
      hook = hook ? hook + " · " + scoreBit : scoreBit;
    }
    return hook || "Generated from current deal inputs";
  }

  function renderCoverPage(data, options, ctx) {
    var meta = ctx.meta;
    var score = data.dealReadinessScore;
    var stage = data.readinessStage || "—";
    var generatedAt = options.generatedAt || data.savedAt || new Date().toISOString();
    var location = coverLocationLine(meta);
    var hook = coverDealHook(meta, score, stage);
    var dateLabel = formatDate(generatedAt) || "—";

    var html = '<section class="drs-cover-page drs-book-page-surface drs-avoid-break" aria-label="Cover">';
    html += '<div class="drs-cover-geometric" aria-hidden="true"></div>';
    html +=
      '<p class="drs-cover-confidential">Draft for validation · Internal owner/advisor review</p>';
    html += '<div class="drs-cover-block">';
    html += '<p class="drs-cover-doc-type">Deal Readiness Snapshot</p>';
    html += '<h1 class="drs-cover-title">' + esc(meta.projectName) + "</h1>";
    html += '<p class="drs-cover-location">' + esc(location) + "</p>";
    html += '<div class="drs-cover-accent-line" aria-hidden="true"></div>';
    html += '<p class="drs-cover-deal-hook">' + esc(hook) + "</p>";
    html += '<p class="drs-cover-sub">Readiness narrative &amp; technical detail</p>';
    html += '<p class="drs-cover-date">Generated ' + esc(dateLabel) + " · current deal inputs</p>";
    html += "</div>";
    html +=
      '<p class="drs-cover-disclaimer">This output organizes readiness signals for internal review. It is not a recommendation, endorsement, valuation, or legal advice.</p>';
    html += '<div class="drs-cover-hero">';
    html += '<div class="drs-cover-logo-block">';
    html +=
      '<img src="' +
      esc(DEALALITY_LOGO_URL) +
      '" alt="Dealality" class="drs-cover-logo-img" width="140" height="auto">';
    html += "</div></div></section>";
    return html;
  }

  function renderPage1Narrative(data, options, ctx) {
    var meta = ctx.meta;
    var fields = ctx.fields;
    var score = data.dealReadinessScore;
    var stage = data.readinessStage || "—";
    var reviewStatus = mapReviewStatusLabel(stage, score);
    var areaRows = buildReviewAreaRows(data, meta, fields);
    var clarifications = buildClarificationAreasForPage1(data);
    var clarificationTotal = countClarificationPoolSize(data);
    var strengths = buildNarrativeStrengths(data, meta, fields);
    var workflowRows = buildWorkflowRows(data, stage, score);
    var summaryParagraphs = buildBusinessSummary(data, meta, fields);

    var html = '<div class="drs-book-page-inner drs-content-page drs-page-narrative">';

    html += '<div class="drs-brief-highlights">';
    html += '<p class="drs-brief-kicker">Readiness Narrative</p>';
    html += '<div class="drs-brief-score-cards">';
    html += '<div class="drs-brief-card"><div class="drs-brief-card-title">Final Readiness Score</div>';
    html += '<div class="drs-brief-card-body drs-brief-card-body--score"><span class="drs-score-num">' + esc(score != null && score !== "" ? score : "—") + '</span><span class="drs-score-of"> / 100</span></div></div>';
    html += '<div class="drs-brief-card"><div class="drs-brief-card-title">Current Readiness Stage</div>';
    html += '<div class="drs-brief-card-body">' + esc(stage) + "</div></div>";
    html += "</div>";
    html += '<p class="drs-brief-lead">' + esc(scoreInterpretation(score, stage, data)) + "</p>";
    html += "</div>";

    html += '<div class="drs-brief-panel">';
    html += '<section class="drs-section drs-section--brief drs-section--keep"><h2 class="drs-section-title">Readiness Summary</h2>';
    summaryParagraphs.forEach(function (para) {
      html += '<p class="drs-summary">' + esc(para) + "</p>";
    });
    html += "</section>";

    html += '<section class="drs-section drs-section--brief drs-section--keep"><h2 class="drs-section-title">Readiness Breakdown</h2>';
    html += '<div class="drs-table-wrap"><table class="drs-breakdown-table drs-brief-table"><thead><tr><th>Review Area</th><th>Status</th><th>Notes</th></tr></thead><tbody>';
    areaRows.forEach(function (row) {
      html += "<tr><td>" + esc(row.area) + "</td><td>" + esc(row.status) + "</td><td>" + esc(row.notes) + "</td></tr>";
    });
    html += "</tbody></table></div></section>";

    html += '<section class="drs-section drs-section--brief drs-section--keep"><h2 class="drs-section-title">Primary Clarification Areas</h2>';
    if (!clarifications.length) {
      html += '<p class="drs-muted">No primary clarification areas are flagged at this time.</p>';
    } else {
      html += '<div class="drs-table-wrap"><table class="drs-clar-table drs-brief-table"><thead><tr><th>Clarification Area</th><th>Why It Matters</th><th>Priority</th></tr></thead><tbody>';
      clarifications.forEach(function (row) {
        html += "<tr><td>" + esc(row.area) + "</td><td>" + esc(row.whyItMatters) + "</td><td>" + esc(row.priority) + "</td></tr>";
      });
      html += "</tbody></table></div>";
      if (clarificationTotal > PAGE1_CLARIFICATION_MAX) {
        html +=
          '<p class="drs-muted drs-clarification-more">Additional clarification items are shown in the Technical Readiness Detail section.</p>';
      }
    }
    html += "</section>";

    html += '<section class="drs-section drs-section--brief drs-section--keep"><h2 class="drs-section-title">Key Strengths Identified</h2>';
    html += renderExecutiveStrengths(strengths) + "</section>";

    html += '<div class="drs-narrative-tail">';
    html += '<section class="drs-section drs-section--brief drs-section--keep"><h2 class="drs-section-title">Current Review Status</h2>';
    html += '<p class="drs-review-status-label">' + esc(reviewStatus.label) + "</p>";
    html += '<p class="drs-muted">' + esc(reviewStatus.explain) + "</p></section>";
    html += '<section class="drs-section drs-section--brief drs-section--keep"><h2 class="drs-section-title">Suggested Workflow Status</h2>';
    html += '<div class="drs-table-wrap drs-table-wrap--keep"><table class="drs-breakdown-table drs-brief-table"><thead><tr><th>Step</th><th>Current Status</th></tr></thead><tbody>';
    workflowRows.forEach(function (row) {
      html += "<tr><td>" + esc(row.step) + "</td><td>" + esc(row.status) + "</td></tr>";
    });
    html += "</tbody></table></div></section>";
    html += '<footer class="drs-output-note drs-output-note--brief drs-section--keep"><p><strong>Output Note.</strong> ' + esc(OUTPUT_NOTE) + "</p></footer>";
    html += "</div>";
    html += "</div></div>";
    return html;
  }

  function renderPage2Technical(data, options, ctx) {
    var score = data.dealReadinessScore;
    var stage = data.readinessStage || "—";
    var missing = (data.missingInformation || []).length;
    var weak = (data.weakInformation || []).length;
    var blocking = (data.blockingIssues || []).length;
    var limiting = (data.limitingIssues || []).length;
    var enhancement = (data.enhancementIssues || []).length;
    var weighted =
      data.weightedCompletionScore != null
        ? data.weightedCompletionScore
        : data.scoreBreakdown && data.scoreBreakdown.weightedCompletionScore != null
          ? data.scoreBreakdown.weightedCompletionScore
          : null;
    var lastReviewed = data.savedAt || options.generatedAt || "";

    var html = '<div class="drs-book-page-inner drs-content-page drs-page-technical">';

    html += '<div class="drs-brief-highlights">';
    html += '<p class="drs-brief-kicker">Technical Readiness Detail</p>';
    html += '<p class="drs-brief-lead">Supporting field-level readiness information</p>';
    html += "</div>";

    html += '<div class="drs-brief-panel">';
    html += '<section class="drs-section drs-section--brief drs-section--technical-note drs-section--keep"><h2 class="drs-section-title">Score Calculation</h2>';
    html += renderTechnicalScoreCalculation(data);
    html += "</section>";
    html += '<section class="drs-section drs-section--brief"><h2 class="drs-section-title">Tab / Section Score Grid</h2>';
    html += renderTabGrid(data) + "</section>";
    html += renderDetailSection(
      "Missing Information",
      data.missingInformation || [],
      "Current inputs suggest no missing required fields."
    );
    html += renderDetailSection(
      "Weak Information",
      data.weakInformation || [],
      "Current inputs suggest no weak text fields."
    );
    html += renderDetailSection(
      "Blocking Clarification Areas",
      data.blockingIssues || [],
      "Current inputs suggest no blocking signals."
    );
    html += renderDetailSection(
      "Limiting Issues",
      data.limitingIssues || [],
      "Current inputs suggest no limiting signals."
    );
    if (data.enhancementIssues && data.enhancementIssues.length) {
      html += renderDetailSection(
        "Enhancement Issues",
        data.enhancementIssues,
        "Current inputs suggest no enhancement-level gaps."
      );
    }
    if (options.editDealHref) {
      html += '<section class="drs-section drs-section--brief drs-no-print"><h3 class="drs-section-title">Field-Level Gap Links</h3>';
      html += '<p><a class="drs-edit-link" data-deal-readiness-form="1" href="' + esc(options.editDealHref) + '">Edit deal — highlight gaps on each tab →</a></p>';
      html += '<p class="drs-muted">Opens Deal Setup; save there to update readiness signals, then re-run this snapshot.</p></section>';
    }

    html += '<section class="drs-section drs-section--brief"><h2 class="drs-section-title">Raw Readiness Counts</h2>';
    html += '<div class="drs-table-wrap"><table class="drs-status-table drs-brief-table"><tbody>';
    html += "<tr><th>Final readiness score</th><td>" + esc(score != null && score !== "" ? score + " / 100" : "—") + "</td></tr>";
    html += "<tr><th>Readiness stage</th><td>" + esc(stage) + "</td></tr>";
    if (data.scoringModelVersion === "weighted-v2" && weighted != null && weighted !== "") {
      html += "<tr><th>Weighted completion score</th><td>" + esc(weighted + " / 100") + "</td></tr>";
    }
    html += "<tr><th>Missing count</th><td>" + esc(missing) + "</td></tr>";
    html += "<tr><th>Weak count</th><td>" + esc(weak) + "</td></tr>";
    html += "<tr><th>Blocking count</th><td>" + esc(blocking) + "</td></tr>";
    html += "<tr><th>Limiting count</th><td>" + esc(limiting) + "</td></tr>";
    html += "<tr><th>Enhancement count</th><td>" + esc(enhancement) + "</td></tr>";
    if (data.scoringModelVersion) {
      html += "<tr><th>Scoring model</th><td>" + esc(data.scoringModelVersion) + "</td></tr>";
    }
    html += "<tr><th>Last reviewed</th><td>" + esc(lastReviewed ? formatDate(lastReviewed) : "—") + "</td></tr>";
    html += "</tbody></table></div></section>";
    html += '<footer class="drs-output-note drs-output-note--brief"><p>' + esc(OUTPUT_NOTE) + "</p></footer>";
    html += "</div></div>";
    return html;
  }

  function wrapBookPage(index, innerHtml, active) {
    return (
      '<div class="drs-book-page' +
      (active ? " active" : "") +
      '" data-drs-page="' +
      index +
      '" role="region" aria-hidden="' +
      (active ? "false" : "true") +
      '">' +
      innerHtml +
      "</div>"
    );
  }

  function buildHtml(data, options) {
    options = options || {};
    var fields = data.sourceFields || (data.deal && data.deal.fields) || {};
    var meta = options.dealMeta || dealMetaFromSources({
      deal: data.deal,
      fields: fields,
      normalized: data.normalized,
      listDeal: options.listDeal,
    });
    var ctx = { meta: meta, fields: fields };

    var html = "";
    var snapClass = "deal-readiness-snapshot";
    if (options.embed) snapClass += " drs--embed";
    if (options.fullPage) snapClass += " drs--full-page";
    html += '<div class="' + snapClass + '">';
    html += '<div class="drs-toolbar drs-no-print"><div class="drs-toolbar-actions">';
    html +=
      '<span class="drs-print-tip drs-no-print">' +
      "Turn off <strong>Headers and footers</strong> and enable <strong>Background graphics</strong> in the print dialog." +
      "</span>";
    html +=
      '<button type="button" class="drs-btn drs-btn-primary drs-toolbar-print" data-drs-print>Print / Save as PDF</button>';
    html += "</div></div>";

    html += '<div class="drs-book-shell">';
    html += '<article class="drs-document drs-book-document">';
    html += '<div class="drs-book-viewport" data-drs-book-viewport tabindex="0">';
    html += '<div class="drs-book-stage">';
    html += wrapBookPage(0, renderCoverPage(data, options, ctx), true);
    html += wrapBookPage(1, renderPage1Narrative(data, options, ctx), false);
    html += wrapBookPage(2, renderPage2Technical(data, options, ctx), false);
    html += "</div>";
    html +=
      '<button type="button" class="drs-turn-btn drs-turn-prev drs-no-print" data-drs-turn-prev aria-label="Previous page" disabled>‹</button>';
    html +=
      '<button type="button" class="drs-turn-btn drs-turn-next drs-no-print" data-drs-turn-next aria-label="Next page">›</button>';
    html += '<span class="drs-page-indicator drs-no-print" data-drs-page-indicator>1 of 3</span>';
    html += "</div>";
    if (options.footerHtml) {
      html += '<div class="drs-host-footer drs-no-print">' + options.footerHtml + "</div>";
    }
    html += "</article>";
    html += "</div>";
    return html;
  }

  function bindPageFlip(root) {
    if (!root) return;
    var viewport = root.querySelector("[data-drs-book-viewport]");
    var pages = viewport
      ? Array.prototype.slice.call(viewport.querySelectorAll(".drs-book-page"))
      : [];
    if (!viewport || pages.length < 2) return;
    var current = 0;
    var prevBtn = root.querySelector("[data-drs-turn-prev]");
    var nextBtn = root.querySelector("[data-drs-turn-next]");
    var indicator = root.querySelector("[data-drs-page-indicator]");
    var animating = false;
    var flipMs = 750;
    function updateControls() {
      if (indicator) indicator.textContent = current + 1 + " of " + pages.length;
      if (prevBtn) prevBtn.disabled = current === 0 || animating;
      if (nextBtn) nextBtn.disabled = current === pages.length - 1 || animating;
    }
    function clearFlipClasses() {
      pages.forEach(function (p) {
        p.classList.remove("flip-out-forward", "flip-out-back", "flip-in-forward", "flip-in-back");
      });
    }
    function goTo(nextIndex) {
      if (animating || nextIndex === current) return;
      if (nextIndex < 0 || nextIndex >= pages.length) return;
      animating = true;
      updateControls();
      var outPage = pages[current];
      var inPage = pages[nextIndex];
      var forward = nextIndex > current;
      clearFlipClasses();
      outPage.classList.add(forward ? "flip-out-forward" : "flip-out-back");
      inPage.classList.add(forward ? "flip-in-forward" : "flip-in-back");
      inPage.classList.add("active");
      inPage.setAttribute("aria-hidden", "false");
      global.setTimeout(function () {
        outPage.classList.remove("active", "flip-out-forward", "flip-out-back");
        outPage.setAttribute("aria-hidden", "true");
        inPage.classList.remove("flip-in-forward", "flip-in-back");
        pages.forEach(function (p, i) {
          if (i !== nextIndex) {
            p.classList.remove("active");
            p.setAttribute("aria-hidden", "true");
          }
        });
        current = nextIndex;
        animating = false;
        updateControls();
      }, flipMs);
    }
    if (prevBtn) prevBtn.addEventListener("click", function () { goTo(current - 1); });
    if (nextBtn) nextBtn.addEventListener("click", function () { goTo(current + 1); });
    viewport.addEventListener("keydown", function (e) {
      if (e.key === "ArrowRight" || e.key === "PageDown") { e.preventDefault(); goTo(current + 1); }
      else if (e.key === "ArrowLeft" || e.key === "PageUp") { e.preventDefault(); goTo(current - 1); }
    });
    updateControls();
  }

  function getSnapshotRoot(container) {
    if (!container) return null;
    if (container.classList && container.classList.contains("deal-readiness-snapshot")) return container;
    return container.querySelector(".deal-readiness-snapshot");
  }

  function getPrintHost() {
    var host = document.getElementById("drs-print-host");
    if (!host) {
      host = document.createElement("div");
      host.id = "drs-print-host";
      host.setAttribute("aria-hidden", "true");
      document.body.appendChild(host);
    }
    return host;
  }

  function printSnapshot(root) {
    var snapshot = getSnapshotRoot(root);
    if (!snapshot) {
      window.print();
      return;
    }
    var printHost = getPrintHost();
    var clone = snapshot.cloneNode(true);
    clone.classList.add("drs-printing");
    clone.classList.remove("drs--embed");
    printHost.innerHTML = "";
    printHost.appendChild(clone);

    document.body.classList.add("drs-print-active");
    function cleanup() {
      document.body.classList.remove("drs-print-active");
      printHost.innerHTML = "";
    }
    function onAfterPrint() {
      cleanup();
      window.removeEventListener("afterprint", onAfterPrint);
    }
    window.addEventListener("afterprint", onAfterPrint);
    window.setTimeout(function () {
      if (document.body.classList.contains("drs-print-active")) cleanup();
    }, 3000);
    window.requestAnimationFrame(function () {
      window.setTimeout(function () {
        window.print();
      }, 50);
    });
  }

  function bindPrint(root) {
    if (!root) return;
    var btn = root.querySelector("[data-drs-print]");
    if (btn && !btn._drsPrintBound) {
      btn._drsPrintBound = true;
      btn.addEventListener("click", function () {
        printSnapshot(root);
      });
    }
  }

  function render(container, data, options) {
    if (!container || !data) return null;
    options = options || {};
    container.innerHTML = buildHtml(data, options);
    bindPrint(container);
    bindPageFlip(container);
    return {
      meta: options.dealMeta || dealMetaFromSources({
        deal: data.deal,
        fields: data.sourceFields,
        normalized: data.normalized,
        listDeal: options.listDeal,
      }),
      clarifications: buildClarificationAreas(data),
    };
  }

  global.DealReadinessSnapshot = {
    render: render,
    buildHtml: buildHtml,
    dealMetaFromSources: dealMetaFromSources,
    buildClarificationAreas: buildClarificationAreas,
    OUTPUT_NOTE: OUTPUT_NOTE,
  };
})(typeof window !== "undefined" ? window : globalThis);
