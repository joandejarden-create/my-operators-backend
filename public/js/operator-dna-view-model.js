/**
 * Operator DNA view model — maps Operator Explorer / Operator Setup detail API
 * into the Operator DNA Profile shape. No Airtable writes; read-only normalization.
 *
 * Consumed by: operator-dna-profile.js
 * Detail API (same as gold-mock): GET /api/intake/third-party-operators/:id
 */
(function (global) {
  "use strict";

  var DNA_SECTIONS = [
    { id: 1, title: "Operator Identity", tabs: ["hero", "overview"] },
    { id: 2, title: "Best-Fit Owner / Project Profile", tabs: ["overview", "owner-fit"] },
    { id: 3, title: "Local / Regional Expertise", tabs: ["markets"] },
    { id: 4, title: "Brand Relationships", tabs: ["brands"] },
    { id: 5, title: "Conversion / Repositioning Capability", tabs: ["capabilities"] },
    { id: 6, title: "Commercial Engine", tabs: ["capabilities"] },
    { id: 7, title: "Owner Reporting & Communication", tabs: ["capabilities"] },
    { id: 8, title: "Pre-Opening & Transition Support", tabs: ["capabilities"] },
    { id: 9, title: "F&B / Lifestyle / Resort Capability", tabs: ["capabilities"] },
    { id: 10, title: "Asset Value Creation", tabs: ["asset-value", "case-studies"] },
  ];

  var CAPABILITY_TAB_IDS = ["conversion", "commercial", "reporting", "preopening", "fb"];

  var CAPABILITY_ID_BY_TITLE = {
    "Commercial Engine": "commercial",
    "Owner Reporting & Governance": "reporting",
    "Owner Reporting & Communication": "reporting",
    "Pre-Opening & Transition": "preopening",
    "Pre-Opening & Transition Support": "preopening",
    "Conversion & Repositioning": "conversion",
    "Conversion / Repositioning Capability": "conversion",
    "F&B / Lifestyle / Resort": "fb",
    "F&B, Lifestyle & Resort Capability": "fb",
  };

  function nz(v) {
    if (v == null) return "";
    return String(v).trim();
  }

  function isInternalFillPlaceholder(v) {
    var t = nz(v);
    if (!t) return false;
    return (
      /^Select deal types Arbor pursues in CALA/i.test(t) ||
      /^Choose values that match the CALA deals you pursue/i.test(t) ||
      /^\[Internal fill guidance/i.test(t)
    );
  }

  function arrayishWithoutPlaceholders(v) {
    return arrayish(v).filter(function (item) {
      return !isInternalFillPlaceholder(item);
    });
  }

  function arrayish(v) {
    if (Array.isArray(v)) return v.filter(Boolean).map(String);
    if (v == null || v === "") return [];
    return String(v)
      .split(/[,;|\n]+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
  }

  function unique(arr) {
    var seen = {};
    return (arr || []).filter(function (x) {
      var k = String(x).toLowerCase();
      if (!k || seen[k]) return false;
      seen[k] = true;
      return true;
    });
  }

  function parseBullets(v) {
    return unique(arrayish(v));
  }

  function buildHeroFacts(vm) {
    var s = vm.scale || {};
    var me = vm.marketExperience || {};
    var current = (me.current || []).slice(0, 3).join(", ");
    return [
      {
        label: "Hotels Managed",
        value: s.hotels || "—",
        sub: s.hotelsSub || "",
      },
      {
        label: "Rooms",
        value: s.rooms || "—",
        sub: s.roomsSub || "Open & pipeline",
      },
      {
        label: "Primary Markets",
        value: current || s.markets || "—",
        sub: me.current && me.current.length > 3 ? "+" + (me.current.length - 3) + " more" : "",
      },
      {
        label: "Asset Focus",
        value: (vm.assetFocus || []).slice(0, 2).join(", ") || "—",
        sub: (vm.assetFocus || []).slice(2, 4).join(", "),
      },
      {
        label: "Brand Relationships",
        value: vm.brandSummaryShort || vm.brandSummary || "—",
        sub: "",
      },
    ];
  }

  function buildMarketLayers(me, notesByLayer) {
    me = me || { current: [], team: [], target: [] };
    notesByLayer = notesByLayer || {};
    return [
      {
        key: "current",
        title: "Current Operating Markets",
        subtitle: "Where the company currently manages hotels",
        tone: "teal",
        chips: me.current || [],
        notes: notesByLayer.current || [],
      },
      {
        key: "team",
        title: "Team Experience Markets",
        subtitle: "Where leadership or team members have credible experience",
        tone: "slate",
        chips: me.team || [],
        notes: notesByLayer.team || [],
      },
      {
        key: "target",
        title: "Target Growth Markets",
        subtitle: "Where the operator wants to pursue future opportunities",
        tone: "amber",
        chips: me.target || [],
        notes: notesByLayer.target || [],
      },
    ];
  }

  function buildBestFitRows(ownerFit, summary) {
    var fit = ownerFit || {};
    var rows = [];
    if (summary) rows.push(["Operating profile", summary]);
    if ((fit.bestProjectTypes || []).length)
      rows.push(["Ideal asset types", fit.bestProjectTypes.join(", ")]);
    if ((fit.bestOwnerProfile || []).length)
      rows.push(["Ideal ownership profile", fit.bestOwnerProfile.join(", ")]);
    if ((fit.bestAssetStages || []).length)
      rows.push(["Project stages", fit.bestAssetStages.join(", ")]);
    if (fit.idealRoomCount) rows.push(["Ideal scale", fit.idealRoomCount]);
    if (fit.idealChainScale) rows.push(["Chain scale", fit.idealChainScale]);
    if ((fit.lessIdeal || []).length)
      rows.push(["Less ideal for", fit.lessIdeal.join("; ")]);
    return rows;
  }

  /** Demo only — Apex Coastal full-length mockup data, clearly prototype. */
  var DEMO_OPERATOR_DNA = {
    mode: "demo",
    operatorId: "demo-apex-coastal",
    companyName: "Apex Coastal Hospitality",
    heroLabel: "Operator DNA · Demo sample",
    positioningStatement:
      "A coastal and resort-focused operator built for complex leisure assets, owner-aligned reporting, disciplined commercial execution, and long-term asset value creation. (Demo sample — not a live operator record.)",
    dnaHeadline: "Resort-minded. Data-driven. Owner-aligned.",
    dnaBody:
      "Demo narrative: combines leisure expertise with disciplined operations to support sustainable returns, sharper owner visibility, and stronger guest experiences.",
    dnaTags: [
      "Resort Specialist",
      "Condo-Hotel Expert",
      "Conversion Experience",
      "Owner-Aligned",
      "Revenue Optimization",
      "CALA Team Experience",
      "F&B Capability",
      "Pre-Opening Support",
    ],
    scale: {
      hotels: "42",
      hotelsSub: "29 open | 13 pipeline",
      rooms: "7,842",
      roomsSub: "Open & pipeline",
      markets: "5",
    },
    assetFocus: ["Resort", "Lifestyle", "Condo-Hotel", "Full-service", "Select-service"],
    brandSummary:
      "15+ global brand families across resort, lifestyle, and upscale full-service (demo).",
    brandSummaryShort: "15+",
    marketExperience: {
      current: ["Florida", "Hawaii", "California", "South Carolina", "Southeast U.S."],
      team: ["Mexico", "Caribbean", "Bahamas", "Puerto Rico", "Dominican Republic"],
      target: ["Mexico", "Caribbean", "Pacific Islands", "Southeast Asia", "Australia"],
    },
    marketLayerNotes: {
      current: [
        "Active operating infrastructure (demo)",
        "Established owner reporting rhythms (demo)",
      ],
      team: [
        "Team-level regional exposure (demo)",
        "Potential bridge into CALA growth (demo)",
      ],
      target: ["Growth-stage positioning (demo)", "Selective market entry (demo)"],
    },
    brandRelationships: {
      approved: ["Upscale Full Service", "Lifestyle", "Resort", "Soft Brands"],
      active: ["Resort", "Lifestyle", "Independent Collections"],
      prior: ["Global Luxury", "Select Service"],
    },
    brandRows: [
      ["Global Luxury", "Prior experience", "Selective", "Luxury resort, branded residence (demo)"],
      ["Upscale Full Service", "Active / approved", "Strong", "Conversions, resort assets (demo)"],
      ["Lifestyle", "Active / prior", "Strong", "Independent conversions (demo)"],
      ["Resort", "Active / approved", "Strong", "Beach, leisure, spa, F&B-heavy (demo)"],
      ["Select Service", "Prior / target", "Moderate", "Resort-adjacent demand (demo)"],
      ["Independent Collections", "Active", "Strong", "Owner-controlled assets (demo)"],
      ["Soft Brands", "Active / target", "Strong", "Conversion flexibility (demo)"],
    ],
    brandsPositioning: {
      softBrandExperience:
        "Demo: supports soft-brand affiliation and independent-to-branded conversion without losing local identity.",
      independentExperience:
        "Demo: collection-style and independent resort positioning with owner-specific story.",
      brandOnboarding:
        "Demo: coordinates QA readiness, opening milestones, and franchisor workflows.",
      ownerBrandNavigation:
        "Demo: helps owners interpret brand standards, PIP scope, and operating economics.",
    },
    positioningCards: [
      ["Brand Navigation", "Helps owners understand which brand families fit the asset, market, and ownership objectives. (Demo)"],
      ["Standards Translation", "Supports conversations around brand standards, PIPs, and conversion expectations. (Demo)"],
      ["Conversion Support", "Guides operational realities of reflagging and repositioning. (Demo)"],
      ["Owner-Brand Coordination", "Aligns owner priorities with brand expectations before deeper negotiations. (Demo)"],
    ],
    identity: {
      companyDescription:
        "Coastal and resort-focused third-party operator (demo sample).",
      companyHistory: "Founded 2008; expanded through resort and conversion assignments (demo).",
      ownershipStructure: "Privately held (demo)",
      platformModel: "Third-party management (demo)",
      brandedIndependentMix: "Balanced branded and independent (demo)",
      chainScaleFocus: "Upscale, upper upscale, resort",
      serviceModelFocus: "Third-party / owner-aligned management",
    },
    regionalExpertise: {
      narrative:
        "Demo: Miami-based leadership with regional travel cadence; team experience in CALA may exceed current operating footprint.",
      localOffices: ["Miami regional hub (demo)"],
      languages: ["English", "Spanish (regional team)"],
      laborMarketFamiliarity: ["Resort seasonal labor", "Urban branded pools"],
      regulatoryFluency: ["Island and CALA permitting awareness (demo)"],
      culturalFluency: ["Caribbean leisure", "Mexico resort corridors"],
    },
    regionalExpertiseRows: [
      ["Local Office / Regional Presence", "Miami-based leadership with regional travel cadence (demo)."],
      ["Language Capabilities", "English and Spanish-language operating support (demo)."],
      ["Labor Market Familiarity", "Leisure and seasonal demand staffing models (demo)."],
      ["Regulatory Familiarity", "Island and CALA markets require local legal and labor guidance (demo)."],
      ["Cultural Fluency", "Service and owner communication adapted to local expectations (demo)."],
      ["Vendor / Partner Network", "Regional advisors and procurement partners as markets deepen (demo)."],
    ],
    marketFitSignals: [
      ["Coastal Destinations", "Consistent performance in beach and waterfront markets (demo)."],
      ["Urban Leisure Gateway", "Mixed leisure/business demand experience (demo)."],
      ["Island Complexity", "Staffing, procurement, and service in harder-to-serve markets (demo)."],
      ["Pipeline Selectivity", "Growth favors quality of fit over broad coverage (demo)."],
    ],
    overview: {
      dnaSnapshot: [],
      dnaSnapshotTiles: [
        ["Resort & Leisure Expertise", "High"],
        ["Condo-Hotel Experience", "High"],
        ["Conversion Experience", "Strong"],
        ["Revenue Management", "Excellent"],
        ["Owner Reporting", "Excellent"],
        ["Pre-Opening Support", "Very Strong"],
        ["F&B Capability", "Strong"],
        ["Asset Value Enhancement", "Strong"],
      ],
      bestFitSummary:
        "Coastal destinations, resort and lifestyle assets, conversion and repositioning stages (demo summary).",
      bestFitRows: [
        ["Ideal Markets", "Coastal destinations, Caribbean, Mexico, Southeast U.S. (demo)"],
        ["Ideal Asset Types", "Resorts, lifestyle, condo-hotels, soft-brand conversions (demo)"],
        ["Ideal Ownership Profile", "Long-term holders, family offices, institutional owners (demo)"],
        ["Project Stages", "Conversion, repositioning, new development, takeover (demo)"],
        ["Less Ideal For", "Passive low-touch mandates outside leisure/coastal focus (demo)"],
      ],
      whyOwners: [
        "Maximize performance through revenue strategy and expense discipline (demo).",
        "Aligned partnership with transparent reporting and escalation paths (demo).",
        "Enhance asset value through positioning and operating rhythm (demo).",
      ],
      whyOwnersCards: [
        ["Maximize Performance", "Revenue strategies and expense discipline that support operating results. (Demo)"],
        ["Aligned Partnership", "Transparent reporting, proactive communication, partner mindset. (Demo)"],
        ["Enhance Asset Value", "Strategic guidance and brand positioning for long-term value. (Demo)"],
      ],
      quickFacts: {
        companyUrl: "https://example.com/demo-only",
        yearsInBusiness: "2008",
        primaryServiceModel: "Third-party / owner-aligned",
        dataConfidence: "Demo sample — not validated",
        teamMembers: "210+",
        avgHotelSize: "186 rooms",
        typicalAgreement: "Flexible by asset profile",
      },
    },
    capabilities: [
      {
        id: "commercial",
        title: "Commercial Engine",
        strength: "Excellent",
        description:
          "Drives top-line performance through revenue strategy, pricing, distribution, and direct-booking focus. (Demo)",
        bullets: ["Revenue management", "Sales strategy", "Distribution", "Pricing discipline", "Direct booking", "Forecasting"],
      },
      {
        id: "reporting",
        title: "Owner Reporting & Communication",
        strength: "Excellent",
        description:
          "Transparent governance and reporting that keeps owners informed. (Demo)",
        bullets: ["Reporting cadence", "Monthly business reviews", "Dashboards & KPIs", "CapEx visibility", "Budget process"],
      },
      {
        id: "preopening",
        title: "Pre-Opening & Transition Support",
        strength: "Very Strong",
        description: "End-to-end opening and transition support from planning to stabilization. (Demo)",
        bullets: ["Recruiting", "Procurement", "Systems setup", "Transition planning", "Training", "Opening support"],
      },
      {
        id: "conversion",
        title: "Conversion & Repositioning",
        strength: "Strong",
        description: "Repositioning assets through thoughtful operational change. (Demo)",
        bullets: ["Brand transitions", "PIP execution", "Renovation coordination", "Turnaround", "Reopening ramp"],
      },
      {
        id: "fb",
        title: "F&B, Lifestyle & Resort Capability",
        strength: "Strong",
        description: "Resort programming and F&B that drive guest satisfaction and local relevance. (Demo)",
        bullets: ["Restaurant concepts", "Beach & pool", "Programming", "Local partnerships", "Spa & wellness"],
      },
    ],
    assetValue: {
      summary:
        "Moves from “we manage hotels” to protecting and enhancing asset value through operating discipline. (Demo)",
      metrics: [
        ["90%+", "Guest satisfaction", "Service scores across stabilized resort assets (demo)"],
        ["+11%", "RevPAR index lift", "Representative improvement after revenue reset (demo)"],
        ["36%", "Direct booking contribution", "Digital and loyalty capture (demo)"],
        ["15.8%", "Avg. GOP margin", "Illustrative portfolio discipline (demo)"],
      ],
      levers: [
        ["Revenue Growth", "Pricing, channel mix, direct booking, segmentation. (Demo)"],
        ["Margin Protection", "Labor productivity, procurement, energy, controls. (Demo)"],
        ["Reputation Lift", "Guest experience, service recovery, programming. (Demo)"],
        ["Exit Value", "Cleaner reporting, stabilized ops, buyer confidence. (Demo)"],
        ["CapEx Discipline", "Investments tied to performance and guest impact. (Demo)"],
        ["Owner Visibility", "Dashboards, monthly reporting, decision-ready narratives. (Demo)"],
      ],
      revparExamples: ["+11% RevPAR index lift (demo illustrative)"],
      noiExamples: [],
      marginExamples: ["15.8% avg. GOP margin (demo illustrative)"],
      costControlExamples: ["Procurement and labor productivity programs (demo)"],
      guestSatisfactionExamples: ["90%+ guest satisfaction theme (demo)"],
      exitSupportExamples: ["Exit-readiness reporting packs (demo)"],
      beforeAfter: [
        ["Before", "Fragmented reporting and inconsistent revenue strategy (demo)."],
        ["Operator action", "Rebuilt cadence, commercial strategy, and owner visibility (demo)."],
        ["After", "Stronger performance narrative and owner confidence (demo)."],
      ],
    },
    caseStudies: [
      {
        title: "Coastal Resort Repositioning (demo)",
        assetType: "Independent full-service resort",
        market: "Southeast U.S.",
        challenge: "Underperforming leisure asset with weak direct booking.",
        action: "Rebuilt commercial strategy, programming, and owner reporting.",
        outcome: "Improved satisfaction and direct contribution (demo narrative).",
        relevance: "Relevant for independent resort repositioning.",
        dataStatus: "Demo case format — replace with operator-provided proof before live use.",
      },
      {
        title: "Condo-Hotel Operating Reset (demo)",
        assetType: "Condo-hotel resort",
        market: "Florida",
        challenge: "Complex ownership and fragmented communication.",
        action: "Standardized owner communication and operating controls.",
        outcome: "Stronger rhythm and transparency (demo narrative).",
        relevance: "Relevant for mixed-ownership structures.",
        dataStatus: "Demo case format.",
      },
    ],
    representativeProperties: [],
    ownerFit: {
      bestOwnerProfile: ["Institutional owner", "Family office", "Developer sponsor"],
      bestProjectTypes: ["Resort", "Lifestyle", "Condo-hotel", "Conversion"],
      bestAssetStages: ["Conversion", "Repositioning", "Pre-opening", "Stabilized"],
      idealRoomCount: "120–450 keys",
      idealChainScale: "Upscale, upper upscale",
      lessIdeal: ["Passive advisory-only mandates outside leisure/coastal focus"],
      ownerQuestions: [
        "Can this operator execute in my market?",
        "Do they understand my asset type and guest mix?",
        "Can they manage a conversion or reflag without disrupting value?",
        "Will they give me clear reporting and responsiveness?",
        "Can they improve revenue, margin, and guest experience?",
        "Do they have the right brand relationships for my strategy?",
        "Are they a good fit for my ownership style?",
        "Can they help protect long-term asset value?",
      ],
      ownerConsiderations: [
        ["Regional proof", "Confirm how team-level experience translates into owner support."],
        ["Brand execution", "Clarify approved brand families and prior experience."],
        ["Reporting cadence", "Confirm reporting format, budget rhythm, and escalation."],
      ],
    },
    demoLeadership: [
      {
        name: "Sarah Chen (demo)",
        title: "CEO",
        summary: "20+ years resort and coastal operations across U.S. and Caribbean (demo).",
      },
      {
        name: "Marcus Rivera (demo)",
        title: "COO",
        summary: "Conversion and repositioning specialist; CALA team experience (demo).",
      },
    ],
    demoPlatformNotes: [
      "Technology stack and reporting infrastructure aligned with institutional owners (demo).",
      "Risk and compliance workflows for branded and independent assets (demo).",
      "Data confidence: demo sample — use operatorId=rec… for live Setup record.",
    ],
    contact: {
      website: "https://example.com/demo-only",
      headquarters: "Miami, Florida (demo)",
      email: "demo@example.com",
      contactName: "Michael Turner (demo)",
      contactTitle: "SVP, Business Development (demo)",
      phone: "(305) 555-0117 (demo)",
    },
    logoUrl: "",
    alignment: null,
    prefill: {},
    fields: {},
  };

  function buildCapabilitiesFromPrefill(p, fields) {
    var offered = arrayish(p.offeredServices);
    function cap(title, rawStrength, evidence, bullets) {
      return {
        id: CAPABILITY_ID_BY_TITLE[title] || "",
        title: title,
        strength: normalizeStrength(rawStrength),
        description: nz(evidence),
        evidence: nz(evidence),
        bullets: unique(bullets).slice(0, 8),
      };
    }
    return [
      cap(
        "Commercial Engine",
        p.revenueManagementCapability || fields["Revenue Management Capability"],
        [p.salesPlatform, p.revenueManagementSystem].filter(Boolean).join(" · "),
        offered.filter(function (s) {
          return /revenue|sales|marketing|commercial/i.test(s);
        })
      ),
      cap(
        "Owner Reporting & Communication",
        p.ownerReportingLevel || fields["Owner Reporting Level"],
        [p.governanceCadence, p.reportingFrequency].filter(Boolean).join(" · "),
        offered.filter(function (s) {
          return /report|governance|owner/i.test(s);
        })
      ),
      cap(
        "Pre-Opening & Transition Support",
        p.preOpeningSupportCapability || fields["Pre-Opening Support Capability"],
        [p.newBuildOpeningExperience, p.transitionExperience].filter(Boolean).join(" · "),
        [p.conversionExperience, p.turnaroundExperience].filter(Boolean)
      ),
      cap(
        "Conversion & Repositioning",
        p.conversionReflagExperience || fields["Conversion / Reflag Experience"],
        p.conversionExperience || "",
        parseBullets(p.differentiators).filter(function (x) {
          return /conversion|reflag|reposition|turnaround|pip/i.test(x);
        })
      ),
      cap(
        "F&B, Lifestyle & Resort Capability",
        p.fbCapabilityLevel || p.fBCapabilityLevel || fields["F&B Capability Level"],
        p.primaryServiceModel || "",
        offered.filter(function (s) {
          return /f&b|food|beverage|resort|lifestyle|spa/i.test(s);
        })
      ),
    ];
  }

  function normalizeStrength(raw) {
    var t = nz(raw);
    if (!t) return "Not yet provided";
    if (/not\s*(yet\s*)?provided|n\/a|unknown/i.test(t)) return "Not yet provided";
    return t;
  }

  function buildIdentityFromPrefill(p, fields) {
    return {
      companyDescription: nz(p.companyDescription) || nz(fields["Company Description"]) || "",
      companyHistory: nz(p.companyHistory) || nz(fields["Company History"]) || "",
      ownershipStructure: nz(p.parentCompany) || nz(fields["Parent Company"]) || "",
      platformModel:
        nz(p.primaryServiceModel) || (arrayish(p.serviceModelsSupported)[0] || "") || "",
      brandedIndependentMix: nz(p.brandedVsIndependentMix) || nz(fields["Branded vs Independent Mix"]) || "",
      chainScaleFocus: arrayish(p.chainScalesSupported).join(", "),
      serviceModelFocus: nz(p.primaryServiceModel) || arrayish(p.serviceModelsSupported).join(", "),
    };
  }

  function buildRegionalFromPrefill(p, fields) {
    return {
      narrative: nz(p.regionalTeams) || nz(p.mkt_narrative_depth) || "",
      localOffices: parseBullets(p.regionalTeams || fields["Regional Management Teams"]),
      languages: [],
      laborMarketFamiliarity: parseBullets(p.propertyTypes),
      regulatoryFluency: [],
      culturalFluency: parseBullets(p.specificMarkets),
    };
  }

  function buildBrandsPositioningFromPrefill(p, fields) {
    return {
      softBrandExperience: nz(p.softBrandLifestyleExperience) || nz(fields["Soft Brand / Lifestyle Experience"]) || "",
      independentExperience: nz(p.differentiators) || "",
      brandOnboarding: nz(p.brandsPortfolioDetail) || "",
      ownerBrandNavigation: nz(p.serviceDifferentiators) || "",
    };
  }

  function buildAssetValueFromPrefill(p, fields) {
    var achievements = parseBullets(p.achievements || fields["Notable Achievements"]);
    return {
      summary: achievements[0] || nz(p.revparImprovement) || nz(p.noiImprovement) || "",
      metrics: [],
      levers: [],
      revparExamples: nz(p.revparImprovement) ? [nz(p.revparImprovement)] : [],
      noiExamples: nz(p.noiImprovement) ? [nz(p.noiImprovement)] : [],
      marginExamples: [],
      costControlExamples: parseBullets(p.procurementServices),
      guestSatisfactionExamples: [],
      exitSupportExamples: [],
      beforeAfter: achievements.slice(0, 3).map(function (a) {
        return ["Achievement", a];
      }),
    };
  }

  function mapCaseStudy(row) {
    return {
      title: nz(row.property_name) || "Case example",
      assetType: nz(row.hotel_type),
      market: nz(row.region) || nz(row.city),
      challenge: nz(row.challenge) || nz(row.situation),
      action: nz(row.services),
      outcome: nz(row.outcome),
      relevance: nz(row.owner_relevance),
      dataStatus: nz(row.data_status) || nz(row.dataStatus) || "",
    };
  }

  function enrichViewModel(vm) {
    var p = vm.prefill || {};
    var f = vm.fields || {};
    if (!vm.identity) vm.identity = buildIdentityFromPrefill(p, f);
    if (!vm.regionalExpertise) vm.regionalExpertise = buildRegionalFromPrefill(p, f);
    if (!vm.regionalExpertiseRows) {
      var r = vm.regionalExpertise;
      vm.regionalExpertiseRows = [
        ["Local Office / Regional Presence", (r.localOffices || []).join(", ")],
        ["Language Capabilities", (r.languages || []).join(", ")],
        ["Labor Market Familiarity", (r.laborMarketFamiliarity || []).join(", ")],
        ["Regulatory Familiarity", (r.regulatoryFluency || []).join(", ")],
        ["Cultural Fluency", (r.culturalFluency || []).join(", ")],
      ].filter(function (row) {
        return nz(row[1]);
      });
    }
    if (!vm.brandsPositioning) vm.brandsPositioning = buildBrandsPositioningFromPrefill(p, f);
    if (!vm.assetValue) vm.assetValue = buildAssetValueFromPrefill(p, f);
    if (!vm.representativeProperties) vm.representativeProperties = [];
    if (!vm.capabilities || !vm.capabilities.length) vm.capabilities = buildCapabilitiesFromPrefill(p, f);
    vm.capabilities = vm.capabilities
      .map(function (c) {
        if (!c.id && c.title) c.id = CAPABILITY_ID_BY_TITLE[c.title] || "";
        if (!c.description && c.evidence) c.description = c.evidence;
        return c;
      })
      .filter(function (c) {
        return !c.id || CAPABILITY_TAB_IDS.indexOf(c.id) !== -1;
      });
    if (!vm.marketLayers) vm.marketLayers = buildMarketLayers(vm.marketExperience, vm.marketLayerNotes);
    if (!vm.heroFacts) vm.heroFacts = buildHeroFacts(vm);
    if (!vm.overview) vm.overview = {};
    if (!vm.overview.bestFitRows) vm.overview.bestFitRows = buildBestFitRows(vm.ownerFit, vm.overview.bestFitSummary);
    if (!vm.overview.dnaSnapshotTiles && vm.overview.dnaSnapshot && vm.overview.dnaSnapshot.length) {
      vm.overview.dnaSnapshotTiles = vm.overview.dnaSnapshot.map(function (s) {
        return [s, ""];
      });
    }
    if (!vm.brandRows && vm.brandRelationships) {
      var br = vm.brandRelationships;
      vm.brandRows = []
        .concat((br.approved || []).map(function (b) {
          return [b, "Approved", "", ""];
        }))
        .concat((br.active || []).map(function (b) {
          return [b, "Active", "", ""];
        }))
        .concat((br.prior || []).map(function (b) {
          return [b, "Prior", "", ""];
        }));
    }
    if (!vm.positioningCards && vm.brandsPositioning) {
      var bp = vm.brandsPositioning;
      vm.positioningCards = [
        ["Soft-brand experience", bp.softBrandExperience],
        ["Independent experience", bp.independentExperience],
        ["Brand onboarding", bp.brandOnboarding],
        ["Owner-brand navigation", bp.ownerBrandNavigation],
      ].filter(function (row) {
        return nz(row[1]);
      });
    }
    return vm;
  }

  function mapLiveOperatorDetail(payload) {
    var op = payload.operator || payload || {};
    var prefill = op.prefill || {};
    var fields = op.fields || {};
    var p = prefill;

    var companyName =
      nz(p.companyName) || nz(fields["Company Name"]) || nz(p.company_name) || "Operator";

    var currentMarkets = unique(
      []
        .concat(arrayish(p.activeCountries))
        .concat(arrayish(p.activeMarkets))
        .concat(arrayish(fields["Active Countries"]))
        .concat(arrayish(fields["Active Markets / Cities"]))
    );

    var teamMarkets = unique(
      [].concat(arrayish(p.teamExperienceMarkets)).concat(arrayish(fields["Team Experience Markets"]))
    );

    var targetMarkets = unique(
      []
        .concat(arrayish(p.targetGrowthMarkets))
        .concat(arrayish(p.priorityMarkets))
        .concat(arrayish(p.specificMarkets))
        .concat(arrayish(fields["Target Growth Markets"]))
    );

    var dnaTags = unique(
      []
        .concat(arrayish(p.chainScalesSupported))
        .concat(arrayish(p.serviceModelsSupported))
        .concat(
          [p.conversionReflagExperience, p.revenueManagementCapability, p.fbCapabilityLevel].filter(Boolean)
        )
    ).slice(0, 12);

    var brands = unique(
      arrayish(op.brandProfiles)
        .map(function (b) {
          return b && (b.name || b.brandName);
        })
        .concat(arrayish(p.brands))
    );

    var caseRows = Array.isArray(op.caseStudiesDetail) ? op.caseStudiesDetail : [];
    var caseStudies = caseRows.map(mapCaseStudy).filter(function (c) {
      return c.title || c.challenge || c.action;
    });

    var lessIdeal = parseBullets(p.bf_not_ideal_for || fields["Not Ideal for"] || p.marketsToAvoid);

    var vm = enrichViewModel({
      mode: "live",
      operatorId: op.id || "",
      companyName: companyName,
      heroLabel: "Operator DNA · Live data (prototype UI)",
      positioningStatement:
        nz(p.companyTagline) ||
        nz(fields["Company Tagline"]) ||
        nz(p.companyDescription).slice(0, 320) ||
        "",
      dnaHeadline: nz(p.companyTagline) || "",
      dnaBody: nz(p.missionStatement) || nz(p.managementPhilosophy) || "",
      dnaTags: dnaTags,
      scale: {
        hotels: nz(p.totalProperties) || nz(fields["Total Properties Managed"]) || "",
        rooms: nz(p.totalRooms) || nz(fields["Total Rooms Managed"]) || "",
        markets: String(currentMarkets.length || ""),
      },
      assetFocus: unique(
        []
          .concat(arrayish(p.propertyTypes))
          .concat(arrayish(p.chainScalesSupported))
          .concat(arrayish(p.bestFitAssetTypes))
      ),
      brandSummary:
        nz(p.brandsPortfolioDetail) ||
        nz(p.softBrandLifestyleExperience) ||
        (brands.length ? brands.slice(0, 6).join(", ") : ""),
      brandSummaryShort: brands.length ? String(brands.length) + "+" : "",
      marketExperience: {
        current: currentMarkets,
        team: teamMarkets,
        target: targetMarkets,
      },
      marketLayerNotes: {},
      brandRelationships: {
        approved: arrayish(p.brandFamiliesOperated),
        active: brands.slice(0, 8),
        prior: arrayish(p.additionalBrands),
      },
      overview: {
        dnaSnapshot: unique(
          []
            .concat(parseBullets(p.differentiators))
            .concat(parseBullets(p.managementPhilosophy))
            .concat(parseBullets(p.missionStatement))
        ).slice(0, 8),
        bestFitSummary: (function () {
          var summary =
            nz(p.bf_operating_situations) || nz(fields["Operating Situations"]) || "";
          return isInternalFillPlaceholder(summary) ? "" : summary;
        })(),
        whyOwners: unique(
          [].concat(parseBullets(p.differentiators)).concat(parseBullets(p.companyDescription))
        ).slice(0, 5),
        quickFacts: (function () {
          var hotels = Number(p.totalProperties);
          var rooms = Number(p.totalRooms);
          var avgHotel = "";
          if (hotels > 0 && rooms > 0) {
            avgHotel = Math.round(rooms / hotels) + " rooms";
          } else if (p.minPropertySize || p.maxPropertySize) {
            avgHotel = [p.minPropertySize, p.maxPropertySize].filter(Boolean).join("–");
            if (avgHotel) avgHotel += " rooms";
          }
          var team = "";
          var te = Number(p.totalEmployees);
          if (te > 0) team = te >= 200 ? String(te) + "+" : String(te);
          else team = nz(p.companySize) || nz(fields["Company Size"]);
          return {
            companyUrl: nz(p.website) || nz(fields["Website"]),
            yearsInBusiness: nz(p.yearsInBusiness) || nz(fields["Years in Business"]),
            yearFounded:
              nz(p.yearEstablished) ||
              nz(fields["Year Established"]) ||
              nz(p.yearsInBusiness) ||
              nz(fields["Years in Business"]),
            primaryServiceModel:
              nz(p.primaryServiceModel) || (arrayish(p.serviceModelsSupported)[0] || ""),
            teamMembers: team,
            avgHotelSize: avgHotel,
            typicalAgreement: (function () {
              var agreement =
                nz(p.typicalAgreement) ||
                arrayishWithoutPlaceholders(p.managementStructuresSupported)
                  .slice(0, 2)
                  .join(" · ");
              return isInternalFillPlaceholder(agreement) ? "" : agreement;
            })(),
            dataConfidence:
              nz(p.dataConfidenceLevel) ||
              nz(fields["Data Confidence Level"]) ||
              "From Operator Setup",
          };
        })(),
      },
      capabilities: buildCapabilitiesFromPrefill(p, fields),
      caseStudies: caseStudies,
      ownerFit: {
        bestOwnerProfile: arrayish(p.bestFitOwnerTypes),
        bestProjectTypes: unique(
          []
            .concat(arrayish(p.bf_selected_asset_types))
            .concat(arrayish(p.propertyTypes))
            .concat(arrayish(p.bestFitAssetTypes))
        ),
        bestAssetStages: arrayish(p.bf_selected_situation_types),
        idealRoomCount:
          [p.minPropertySize, p.maxPropertySize].filter(Boolean).join("–") || nz(p.minimumKeyCount),
        idealChainScale: arrayish(p.chainScalesSupported).join(", "),
        lessIdeal: lessIdeal,
        ownerQuestions: [],
        ownerConsiderations: [],
      },
      contact: {
        website: nz(p.website) || nz(fields["Website"]),
        headquarters: nz(p.headquarters) || nz(fields["Headquarters Location"]),
        email: nz(p.contactEmail) || nz(fields["Primary Contact Email"]),
        contactName: nz(p.contactName) || nz(fields["Primary Contact Name"]),
        contactTitle: nz(p.contactTitle) || "",
        phone: nz(p.contactPhone) || nz(fields["Primary Contact Phone"]),
      },
      logoUrl: (function () {
        var att = p.companyLogo;
        if (Array.isArray(att) && att[0] && att[0].url) return String(att[0].url);
        return "";
      })(),
      alignment: null,
      prefill: p,
      fields: fields,
    });

    return vm;
  }

  function createDemoViewModel() {
    return enrichViewModel(JSON.parse(JSON.stringify(DEMO_OPERATOR_DNA)));
  }

  /**
   * Attach Operator Explorer (gold-mock) panel HTML — same source as live Explorer popup.
   * @param {object} dnaVm
   * @param {object} apiOperator - payload.operator from intake detail API
   * @param {object|null} listRow - row from /api/third-party-operators list
   */
  function attachExplorerPanels(dnaVm, apiOperator, listRow) {
    var Gold = global.OperatorExplorerGoldMock;
    if (!Gold || !apiOperator) {
      dnaVm.explorerPanels = null;
      return dnaVm;
    }
    try {
      var exVm = Gold.buildViewModel(apiOperator, listRow || null);
      dnaVm.explorerPanels = Gold.buildPanels(exVm, {
        useBrandExplorerCaseStudies: true,
        ownerFacingProofKpis: true,
        omitProofDecisionSignals: true,
      });
      dnaVm.explorerVm = exVm;
      mergeHeroFromExplorer(dnaVm, exVm);
    } catch (e) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[operator-dna-view-model] explorer panels failed", e);
      }
      dnaVm.explorerPanels = null;
    }
    return dnaVm;
  }

  function mergeHeroFromExplorer(dnaVm, exVm) {
    if (!exVm) return;
    if (exVm.companyName && dnaVm.mode === "live") dnaVm.companyName = exVm.companyName;
    if (exVm.statement && !dnaVm.positioningStatement) dnaVm.positioningStatement = exVm.statement;
    if (exVm.tagline && !dnaVm.dnaHeadline) dnaVm.dnaHeadline = exVm.tagline;
    if (exVm.logoUrl) dnaVm.logoUrl = exVm.logoUrl;
    if (exVm.heroMeta && exVm.heroMeta.length) {
      dnaVm.heroFacts = exVm.heroMeta.slice(0, 5).map(function (pair) {
        return { label: pair[0], value: pair[1], sub: "" };
      });
    }
    var p = exVm.prefill || {};
    if (arrayish(p.activeCountries).length || arrayish(p.activeMarkets).length) {
      dnaVm.marketExperience = dnaVm.marketExperience || { current: [], team: [], target: [] };
      if (!(dnaVm.marketExperience.current || []).length) {
        dnaVm.marketExperience.current = unique(
          [].concat(arrayish(p.activeCountries)).concat(arrayish(p.activeMarkets))
        );
      }
    }
    dnaVm.marketLayers = buildMarketLayers(dnaVm.marketExperience, dnaVm.marketLayerNotes);
  }

  /** Which Explorer tabs embed under each DNA tab (plus dedicated DNA tabs). */
  var EXPLORER_PANELS_BY_DNA_TAB = {
    overview: ["Profile & Positioning"],
    markets: ["Markets & Footprint"],
    brands: ["Brand & Relationships"],
    capabilities: ["Operating Platform", "Owner Engagement & Reporting"],
    "case-studies": ["Proof & Track Record"],
    "owner-fit": ["Project Fit & Deal Profile"],
    leadership: ["Leadership"],
    "platform-governance": ["Infrastructure & Data"],
  };

  global.OperatorDnaViewModel = {
    DNA_SECTIONS: DNA_SECTIONS,
    CAPABILITY_TAB_IDS: CAPABILITY_TAB_IDS,
    DEMO_OPERATOR_DNA: DEMO_OPERATOR_DNA,
    createDemoViewModel: createDemoViewModel,
    mapLiveOperatorDetail: mapLiveOperatorDetail,
    attachExplorerPanels: attachExplorerPanels,
    EXPLORER_PANELS_BY_DNA_TAB: EXPLORER_PANELS_BY_DNA_TAB,
    enrichViewModel: enrichViewModel,
    buildMarketLayers: buildMarketLayers,
    buildHeroFacts: buildHeroFacts,
    nz: nz,
    unique: unique,
    arrayish: arrayish,
  };
})(typeof window !== "undefined" ? window : global);
