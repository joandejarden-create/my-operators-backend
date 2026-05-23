/**
 * Deal Brief HTML builders (shared: deal-summary + My Deals modal snapshot).
 */
(function (global) {
  "use strict";

  function esc(t) {
    return String(t == null ? "" : t)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function formatFieldValue(val) {
                if (val == null || val === '') return '';
                if (Array.isArray(val)) {
                    var parts = val.map(function(v) {
                        if (v && typeof v === 'object' && typeof v.name === 'string') return v.name;
                        return String(v).trim();
                    }).filter(Boolean);
                    return parts.join(', ');
                }
                if (typeof val === 'string') return val.trim();
                if (typeof val === 'object' && val !== null && typeof val.name === 'string') return val.name;
                if (val instanceof Date || (typeof val === 'string' && /^\d{4}-\d{2}-\d{2}/.test(val))) {
                    try {
                        var d = new Date(val);
                        if (!isNaN(d.getTime())) return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
                    } catch (e) {}
                }
                return String(val);
            }
            function getField(fields, keys) {
                for (var i = 0; i < keys.length; i++) {
                    var v = fields[keys[i]];
                    if (v != null && v !== '') return formatFieldValue(v);
                }
                return '';
            }

            /* Detail columns: 1 = Executive Summary + Project Info + Property, 2 = Market + Deal, 3 = Strategic */
            var detailSections = [
                /* Project Information: omit Full Address & Submarket (in Opportunity card) */
                { title: 'Project Information', col: 1, rows: [
                    ['Currently branded?', ['Is the hotel currently branded?']],
                    ['Current brand/operator', ['Parent Company Name', 'Current Brand Affiliation', 'Operator Name Current']],
                    ['Chain Scale', ['Hotel Chain Scale']],
                    ['Hotel Type', ['Hotel Type', 'Property Type']],
                    ['Service Model', ['Hotel Service Model']]
                ]},
                { title: 'Property Details', col: 1, rows: [
                    ['Hotel Chain Scale', ['Hotel Chain Scale', 'Chain Scale', 'Preferred Chain Scales']],
                    ['Property Type', ['Property Type', 'Hotel Type', 'Asset Type']],
                    ['Hotel Service Model', ['Hotel Service Model', 'Service Model']],
                    ['Room breakdown', ['Number of Standard Rooms', 'Number of Suites']],
                    ['Building Type', ['Building Type']],
                    ['Stories', ['Number of Stories']],
                    ['Meeting Space', ['Meeting Space', 'Meeting Space Unit']],
                    ['F&B Outlets?', ['F&B Outlets?']],
                    ['F&B Concept', ['Outlet Names / Concepts']],
                    ['Parking', ['Parking Amenities?']],
                    ['Additional Amenities', ['Additional Amenities']]
                ]},
                { title: 'Market & Ownership', col: 2, rows: [
                    ['Demand Drivers', ['Primary Demand Drivers']],
                    ['Key Competitors', ['Key Competitors']],
                    ['Regulatory Issues?', ['Regulatory or Permitting Issues?']],
                    ['Preferred Brands', ['Preferred Brands (up to 4)']],
                    ['Preferred Chain Scales', ['Preferred Chain Scales']]
                ]},
                { title: 'Deal & Capital Structure', col: 2, rows: [
                    ['Ownership Structure', ['Ownership Structure']],
                    ['Preferred Deal Structure', ['Preferred Deal Structure']],
                    ['PIP/CapEx Status', ['PIP / CapEx Status']],
                    ['Lease Type', ['Lease Type']],
                    ['Initial Lease Term', ['Initial Lease Term (years)']],
                    ['Lease Start Date', ['Lease Start Date (or Availability)']],
                    ['Lease End Date', ['Lease Expiration or End Date']],
                    ['Base Rent', ['Base Rent (annual or structure)']],
                    ['Percentage Rent', ['Percentage Rent (if applicable)']],
                    ['CAM / Insurance / Tax', ['CAM Insurance Tax Responsibility']],
                    ['Key Money / TI Allowance', ['Key Money or TI Allowance']],
                    ['Renewal Options', ['Renewal Options']],
                    ['Early Termination / Break', ['Early Termination or Break Clause']],
                    ['Security Deposit / Guarantees', ['Security Deposit or Guarantees']],
                    ['Lease Structure Notes', ['Lease Structure Notes']],
                    ['Total Project Cost Range', ['Total Project Cost Range']],
                    ['Equity vs Debt Split', ['Equity vs Debt Split']],
                    ['PIP Budget (if conversion)', ['PIP Budget Range (if conversion)']],
                    ['IRR/Yield Goals', ['IRR/Yield Goals']],
                    ['Soft vs Hard Brand', ['Soft vs Hard Brand Preference']],
                    ['Plan to Self-Manage or Third Party?', ['Plan to Self-Manage or Hire Third Party?']]
                ]},
                { title: 'Brand / Operator Review Path', col: 3, rows: [
                    ['Who receives bids', ['Who should receive bids for this project?']],
                    ['Preferred brands', ['Preferred Brands (up to 4)']],
                    ['Preferred chain scales', ['Preferred Chain Scales']],
                    ['Franchise vs management', ['Franchise vs Management Preference']]
                ]},
                { title: 'Strategic Priorities', col: 3, rows: [
                    ['Top Success Metrics', ['Top 3 Success Metrics']],
                    ['Top Priorities', ['Top Priorities for Project']],
                    ['Top Concerns', ['Top Concerns for this Project']],
                    ['Deal breakers', ['Top 3 Deal Breakers']],
                    ['Must-haves from brand/operator', ['Must-haves From Brand or Operator']],
                    ['Must-haves (other)', ['Must-haves From Brand or Operator Other']],
                    ['Planned Hold Period', ['Planned Hold Period']],
                    ['Primary Goal for the Hotel', ['Primary Goal for the Hotel']],
                    ['Brand Flexibility vs Prestige', ['Brand Flexibility vs Prestige']],
                    ['Target opening', ['Expected Opening or Rebranding Date', 'Target Opening Date']],
                    ['Decision Timeline', ['Decision Timeline for Brand/Operator']],
                    ['Proposal Deadline', ['Proposal Deadline']]
                ]},
                { title: 'Contact & Next Steps', col: 'contact', rows: [
                    ['Main Contact', ['Main Contact Name']],
                    ['Company', ['Entity or Company Name']]
                ]}
            ];

            function hasValue(val) {
                if (val == null) return false;
                if (typeof val === 'string' && val.trim() === '') return false;
                return true;
            }
            function isOwnerDraftMode(ctx) {
                return ctx && ctx.mode === (global.DealBriefV2 && global.DealBriefV2.MODES.OWNER_DRAFT);
            }
            function renderDetailSection(fields, section, skipTitle, ctx) {
                ctx = ctx || {};
                var V2 = global.DealBriefV2;
                var ownerDraft = isOwnerDraftMode(ctx);
                var visible = [];
                section.rows.forEach(function(r) {
                    var label = r[0], keys = r[1];
                    var airtableKey = keys[0] || '';
                    if (V2 && !V2.isFieldShownInBrief(airtableKey, fields, ctx.readiness)) return;
                    var val = getField(fields, keys);
                    if (val === 'Franchise + Third-Party Management') val = 'Franchise + 3rd Party Mgmt.';
                    if (label === 'Current brand/operator') {
                        var parent = (fields['Parent Company Name'] != null && fields['Parent Company Name'] !== '') ? String(fields['Parent Company Name']).trim() : '';
                        var brand = (fields['Current Brand Affiliation'] != null && fields['Current Brand Affiliation'] !== '') ? String(fields['Current Brand Affiliation']).trim() : '';
                        var op = (fields['Operator Name Current'] != null && fields['Operator Name Current'] !== '') ? String(fields['Operator Name Current']).trim() : '';
                        val = [parent && ('Parent: ' + parent), brand && ('Brand: ' + brand), op && ('Operator: ' + op)].filter(Boolean).join(' · ') || '';
                    }
                    if (label === 'Room breakdown') {
                        var std = (fields['Number of Standard Rooms'] != null && fields['Number of Standard Rooms'] !== '') ? String(fields['Number of Standard Rooms']) : '';
                        var suite = (fields['Number of Suites'] != null && fields['Number of Suites'] !== '') ? String(fields['Number of Suites']) : '';
                        val = [std && (std + ' Standard'), suite && (suite + ' Suites')].filter(Boolean).join(', ') || '';
                    }
                    if (label === 'Meeting Space') {
                        val = (fields['Meeting Space'] != null && fields['Meeting Space'] !== '') ? (String(fields['Meeting Space']) + (fields['Meeting Space Unit'] ? ' ' + formatFieldValue(fields['Meeting Space Unit']) : '')) : '';
                    }
                    if (!hasValue(val)) {
                        if (!ownerDraft) return;
                        val = 'Not provided';
                    }
                    var displayLabel = V2 ? V2.executiveFieldLabel(airtableKey || label) : label;
                    visible.push({ label: displayLabel, val: val });
                });
                if (visible.length === 0) return '';
                var block = skipTitle ? '' : '<div class="brochure-detail-section-title">' + esc(section.title) + '</div>';
                visible.forEach(function(item, i) {
                    var label = item.label, val = item.val;
                    var isExecSummaryRow = (label === 'Executive summary');
                    var rowClass = isExecSummaryRow ? ' brochure-detail-row exec-summary-text' : ' brochure-detail-row';
                    if (i === visible.length - 1) rowClass += ' last-in-section';
                    var valClass = val === 'Not provided' ? ' v empty' : ' v';
                    if (isExecSummaryRow) {
                        block += '<div class="' + rowClass.trim() + '"><span class="v">' + esc(val) + '</span></div>';
                    } else {
                        block += '<div class="' + rowClass.trim() + '"><span class="l">' + esc(label) + '</span><span class="' + valClass.trim() + '">' + esc(val) + '</span></div>';
                    }
                });
                return block;
            }

  function buildContentPageHtml(fields, normalized, ctx) {
    ctx = ctx || { mode: (global.DealBriefV2 && global.DealBriefV2.MODES.OWNER_DRAFT) };
    var V2 = global.DealBriefV2;
    var ownerDraft = ctx.mode === (V2 && V2.MODES.OWNER_DRAFT);

    function buildRecipientOpportunitySummary() {
      if (!V2) return getField(fields, ["Company Executive Summary"]) || "";
      var meta = {
        keyCount: getField(fields, ["Total Number of Rooms/Keys"]) || normalized.totalKeys || "",
        marketLine:
          getField(fields, ["City & State", "Country", "Hotel Submarket & Location"]) ||
          normalized.hotelLocation ||
          "the identified market",
        projectType: normalized.projectType || getField(fields, ["Project Type"]) || "",
      };
      return V2.buildRecipientOpportunityLead(meta, normalized);
    }
    function buildOwnerOpportunitySummary() {
      if (!V2) return getField(fields, ["Company Executive Summary"]) || "";
      var meta = {
        projectType: normalized.projectType || getField(fields, ["Project Type"]) || "—",
        targetPositioning:
          getField(fields, ["Brand Positioning", "Target Chain Scale", "Hotel Chain Scale", "Preferred Chain Scales"]) ||
          "",
      };
      return V2.buildOwnerOpportunityLead(meta);
    }

    var projectType = normalized.projectType || getField(fields, ["Project Type"]) || "—";
    var roomKeys = getField(fields, ["Total Number of Rooms/Keys"]);
    var dealBidRaw = getField(fields, ["Who should receive bids for this project?"]);
    var dealBid =
      dealBidRaw || (normalized.dealBidType === "Both" ? "Both brands and third-party operators" : normalized.dealBidType) || "—";
    var dealBidDisplay =
      dealBid === "Both brands and third-party operators" || dealBid === "Both" ? "Both Brands & 3rd Party Ops." : dealBid;

    var execSummaryInCard = getField(fields, ["Company Executive Summary"]);
    if (!execSummaryInCard) {
      execSummaryInCard = ownerDraft ? buildOwnerOpportunitySummary() : buildRecipientOpportunitySummary();
    }
    var oppLines = [];
    if (execSummaryInCard) oppLines.push('<div class="card-line">' + esc(execSummaryInCard) + "</div>");

    var expandedLoc =
      normalized && normalized.expandedLocation && typeof normalized.expandedLocation === "object"
        ? normalized.expandedLocation
        : {};
    var building =
      getField(fields, ["Building Type", "Asset Type", "Hotel Type"]) ||
      formatFieldValue(expandedLoc["Building Type"] || expandedLoc["Hotel Type"]);
    var fbCount =
      getField(fields, ["Number of F&B Outlets", "F&B Outlets?", "F&B Program Type"]) ||
      formatFieldValue(expandedLoc["Number of F&B Outlets"] || expandedLoc["F&B Outlets?"] || expandedLoc["F&B Program Type"]);
    var parking = getField(fields, ["Parking Amenities?"]) || formatFieldValue(expandedLoc["Parking Amenities?"]);
    var stdRooms = "";
    if (fields["Number of Standard Rooms"] != null && fields["Number of Standard Rooms"] !== "")
      stdRooms = String(fields["Number of Standard Rooms"]);
    else if (expandedLoc["Number of Standard Rooms"] != null && expandedLoc["Number of Standard Rooms"] !== "")
      stdRooms = String(expandedLoc["Number of Standard Rooms"]);
    var suites = "";
    if (fields["Number of Suites"] != null && fields["Number of Suites"] !== "") suites = String(fields["Number of Suites"]);
    else if (expandedLoc["Number of Suites"] != null && expandedLoc["Number of Suites"] !== "")
      suites = String(expandedLoc["Number of Suites"]);
    var roomBreakdown = [stdRooms && stdRooms + " Standard", suites && suites + " Suites"].filter(Boolean).join(", ") || "";
    var stories =
      getField(fields, ["Number of Stories", "Stories"]) ||
      formatFieldValue(expandedLoc["Number of Stories"] || expandedLoc["# of Stories"] || expandedLoc["Stories"]);
    var hotelServiceModel =
      getField(fields, ["Hotel Service Model", "Service Model"]) ||
      formatFieldValue(expandedLoc["Hotel Service Model"] || expandedLoc["Service Model"]);
    var meetingSpace = "";
    if (fields["Meeting Space"] != null && fields["Meeting Space"] !== "") {
      var meetingVal = String(fields["Meeting Space"]).trim();
      var meetingUnit = fields["Meeting Space Unit"] ? formatFieldValue(fields["Meeting Space Unit"]).trim() : "";
      meetingSpace = meetingVal + (meetingUnit ? " " + meetingUnit : "");
    } else if (expandedLoc["Meeting Space"] != null && expandedLoc["Meeting Space"] !== "") {
      var meetingVal2 = String(expandedLoc["Meeting Space"]).trim();
      var meetingUnit2 = expandedLoc["Meeting Space Unit"] ? formatFieldValue(expandedLoc["Meeting Space Unit"]).trim() : "";
      meetingSpace = meetingVal2 + (meetingUnit2 ? " " + meetingUnit2 : "");
    }
    var propParts = [];
    if (building) propParts.push(building);
    if (fbCount) propParts.push(fbCount + " F&B");
    if (parking) propParts.push("Parking");
    var propLines = [];
    if (propParts.length)
      propLines.push(
        '<div class="card-line"><span class="card-label">Amenities:</span> <span class="stat">' +
          esc(propParts.join(" · ")) +
          "</span></div>"
      );
    if (roomBreakdown)
      propLines.push(
        '<div class="card-line"><span class="card-label">Rooms:</span> <span class="stat">' + esc(roomBreakdown) + "</span></div>"
      );
    if (hotelServiceModel)
      propLines.push(
        '<div class="card-line"><span class="card-label">Service model:</span> <span class="stat">' +
          esc(hotelServiceModel) +
          "</span></div>"
      );
    if (stories)
      propLines.push(
        '<div class="card-line"><span class="card-label">Stories:</span> <span class="stat">' + esc(stories) + "</span></div>"
      );
    if (meetingSpace)
      propLines.push(
        '<div class="card-line"><span class="card-label">Meeting:</span> <span class="stat">' + esc(meetingSpace) + "</span></div>"
      );

    var dealStructRaw = getField(fields, ["Preferred Deal Structure"]);
    var dealStruct = dealStructRaw === "Franchise + Third-Party Management" ? "Franchise + 3rd Party Mgmt." : dealStructRaw;
    var isLeaseDeal =
      dealStructRaw &&
      (String(dealStructRaw).toLowerCase() === "lease" || String(dealStructRaw).toLowerCase() === "flexible/open");
    var leaseType = getField(fields, ["Lease Type"]);
    var leaseTerm = getField(fields, ["Initial Lease Term (years)"]);
    var baseRent = getField(fields, ["Base Rent (annual or structure)"]);
    var renewalOpts = getField(fields, ["Renewal Options"]);
    var dealLines = [];
    if (dealStruct) dealLines.push('<div class="card-line"><span class="stat">' + esc(dealStruct) + "</span></div>");
    if (leaseType) dealLines.push('<div class="card-line"><span class="card-label">Lease:</span> ' + esc(leaseType) + "</div>");
    if (isLeaseDeal && leaseTerm)
      dealLines.push('<div class="card-line"><span class="card-label">Lease term:</span> ' + esc(leaseTerm) + "</div>");
    if (isLeaseDeal && baseRent)
      dealLines.push('<div class="card-line"><span class="card-label">Base rent:</span> ' + esc(baseRent) + "</div>");
    if (isLeaseDeal && renewalOpts)
      dealLines.push('<div class="card-line"><span class="card-label">Renewal:</span> ' + esc(renewalOpts) + "</div>");

    var existingExecSummary = getField(fields, ["Company Executive Summary"]);
    if (!existingExecSummary && ownerDraft) fields["Company Executive Summary"] = buildOwnerOpportunitySummary();
    else if (!existingExecSummary && !ownerDraft) fields["Company Executive Summary"] = buildRecipientOpportunitySummary();

    var c1 = "";
    var c2 = "";
    var c3 = "";
    var contactHtml = "";
    var showContact = false;
    detailSections.forEach(function (s) {
      var skipTitle = s.col === "contact";
      var block = renderDetailSection(fields, s, skipTitle, ctx);
      if (s.col === 1) c1 += block;
      else if (s.col === 2) c2 += block;
      else if (s.col === 3) c3 += block;
      else if (s.col === "contact") {
        contactHtml = block;
        showContact = !!block;
      }
    });

    var V2contact = global.DealBriefV2;
    var contact = V2contact ? V2contact.contactCopyForMode(ctx.mode) : { cta: "", showProposalCta: false };
    var ownerClass = ownerDraft ? " brief-mode-owner" : " brief-mode-recipient";

    var html = '<div class="bas-book-page-inner brochure-content-page' + ownerClass + '">';
    html += '<div class="brochure-highlights">';
    html += '<div class="brochure-icon-strip">';
    html +=
      '<div class="brochure-icon-item"><span class="icon"></span> <strong>' +
      esc(roomKeys || "—") +
      "</strong> Rooms</div>";
    html +=
      '<div class="brochure-icon-item"><span class="icon"></span> <strong>' +
      esc(projectType) +
      "</strong> Deal Type</div>";
    html +=
      '<div class="brochure-icon-item"><span class="icon"></span> <strong>' +
      esc(dealBidDisplay) +
      "</strong> Who Receives Bids</div>";
    html += "</div>";
    html += '<div class="brochure-cards">';
    html +=
      '<div class="brochure-card"><div class="brochure-card-title">The Opportunity</div><div class="brochure-card-body" id="cardOpportunity">' +
      (oppLines.length ? oppLines.join("") : '<div class="card-line">See Project Information below.</div>') +
      "</div></div>";
    html +=
      '<div class="brochure-card"><div class="brochure-card-title">Property at a Glance</div><div class="brochure-card-body" id="cardProperty">' +
      (propLines.length ? propLines.join("") : '<div class="card-line">See Property Details column below.</div>') +
      "</div></div>";
    html +=
      '<div class="brochure-card"><div class="brochure-card-title">Deal Structure</div><div class="brochure-card-body" id="cardDeal">' +
      (dealLines.length ? dealLines.join("") : '<div class="card-line">See Deal &amp; Capital Structure column below.</div>') +
      "</div></div>";
    html += "</div></div>";
    html += '<div class="brochure-detail-strip"><div class="brochure-detail-cols">';
    html += "<div>" + c1 + "</div><div>" + c2 + "</div><div>" + c3 + "</div>";
    html += "</div></div>";
    html += '<div class="brochure-two-cols-wrap">';
    if (showContact) {
      html += '<div class="brochure-section brochure-contact-full" id="contactSectionWrap">';
      html += '<div class="brochure-section-title">Contact &amp; Next Steps</div>';
      html += '<div class="brochure-contact-row"><div class="brochure-contact-detail" id="detailContact">' + contactHtml + "</div>";
      html +=
        '<div class="brochure-contact-cta-wrap"><p class="brochure-cta" id="briefContactCta">' +
        esc(contact.cta) +
        "</p>";
      if (contact.showProposalCta) {
        html +=
          '<a href="#" class="brochure-recipient-cta-btn brochure-recipient-only" id="briefProposalCta">Submit proposal in Dealality</a>';
      }
      html +=
        '<img src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png" alt="Dealality" class="brochure-cta-logo" width="80" height="auto"></div></div></div>';
    }
    html += '<div class="brochure-footer">Deal Brief · Dealality</div></div>';
    html += "</div>";
    return html;
  }

  function buildCoverPageHtml(fields, normalized, ctx, options) {
    ctx = ctx || { mode: (global.DealBriefV2 && global.DealBriefV2.MODES.OWNER_DRAFT) };
    var V2 = global.DealBriefV2;
    var ownerDraft = ctx.mode === (V2 && V2.MODES.OWNER_DRAFT);
    var cover = V2 ? V2.coverCopyForMode(ctx.mode) : {};
    var projectName = normalized.projectName || getField(fields, ["Project Name", "Property Name"]) || "—";
    var location = normalized.hotelLocation || "—";
    var roomKeys = getField(fields, ["Total Number of Rooms/Keys"]);
    var projectType = normalized.projectType || getField(fields, ["Project Type"]) || "—";
    var positioning = getField(fields, ["Brand Positioning", "Target Chain Scale", "Hotel Chain Scale", "Preferred Chain Scales"]);
    var dealHookParts = [];
    if (roomKeys) dealHookParts.push(roomKeys + " keys");
    if (projectType && projectType !== "—") dealHookParts.push(projectType);
    if (positioning) dealHookParts.push(positioning);
    var prepDate = new Date();
    var dateLine =
      (ownerDraft ? "Generated " : "Prepared ") +
      prepDate.toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" }) +
      (cover && ownerDraft ? " · " + cover.dateSuffix : "");

    var html = '<section class="bas-cover-page bas-book-page-surface bas-avoid-break" aria-label="Cover">';
    html += '<div class="bas-cover-geometric" aria-hidden="true"></div>';
    html += '<p class="bas-cover-confidential">' + esc(cover.confidential || "Confidential") + "</p>";
    html += '<div class="bas-cover-block">';
    html += '<p class="bas-cover-doc-type">Deal Brief</p>';
    html += '<h1 class="bas-cover-title">' + esc(projectName) + "</h1>";
    html += '<p class="bas-cover-location">' + esc(location) + "</p>";
    html += '<div class="bas-cover-accent-line" aria-hidden="true"></div>';
    if (dealHookParts.length) html += '<p class="bas-cover-deal-hook">' + esc(dealHookParts.join(" · ")) + "</p>";
    html += '<p class="bas-cover-sub">' + esc(cover.sub || "") + "</p>";
    html += '<p class="bas-cover-date">' + esc(dateLine) + "</p>";
    html += "</div>";
    html += '<p class="bas-cover-disclaimer">' + esc(cover.disclaimer || "") + "</p>";
    html +=
      '<div class="bas-cover-hero"><div class="bas-cover-logo-block"><img src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png" alt="Dealality" class="bas-cover-logo-img" width="140" height="auto"></div></div>';
    html += "</section>";
    return html;
  }

  function mergeFieldsFromNormalized(fields, normalized) {
    fields = Object.assign({}, fields || {});
    normalized = normalized || {};
    if (normalized.expandedLocation) {
      var loc = normalized.expandedLocation;
      Object.assign(fields, {
        "Full Address": loc.fullAddress,
        "City & State": loc.city,
        Country: loc.country,
        "Hotel Type": loc.hotelType,
        "Hotel Chain Scale": loc.hotelChainScale,
        "Hotel Submarket & Location": loc.submarket,
        "Hotel Service Model": loc.hotelServiceModel,
        "Company Executive Summary": loc.companyExecutiveSummary,
        "Portfolio Size": loc.portfolioSize,
        "Ownership/Brand History or Track Record": loc.ownershipTrackRecord,
        "Total Site Size": loc.totalSiteSize,
        "Total Site Size Unit": loc.totalSiteSizeUnit,
        "Max Height Allowed By Zoning": loc.maxHeightAllowedByZoning,
        "Max Height Allowed By Zoning Unit": loc.maxHeightAllowedByZoningUnit,
        "Ownership Type": Array.isArray(loc.ownershipType) ? loc.ownershipType.join(", ") : loc.ownershipType,
        "Current Form of Site Control": loc.currentFormOfSiteControl,
        "Current Form of Site Control Other Text": loc.currentFormOfSiteControlOtherText,
        "Zoning Status": loc.zoningStatus,
        "Zoning Status Other Text": loc.zoningStatusOtherText,
        "Parking Ratio": loc.parkingRatio,
        "Access to Transit or Highway": Array.isArray(loc.accessToTransit)
          ? loc.accessToTransit.join(", ")
          : loc.accessToTransit,
        "Access to Transit / Highway Other Text": loc.accessToTransitOtherText,
        "Total Number of Rooms/Keys":
          loc.totalNumberOfRoomsKeys || loc["Total Number of Rooms/Keys"] || fields["Total Number of Rooms/Keys"],
        "Number of Standard Rooms": loc.numberStandardRooms,
        "Number of Suites": loc.numberSuites,
        "Number of Stories": loc.numberStories,
        "Building Type": loc.buildingType,
        "Year Built (Years Open as a Hotel)": loc.yearBuilt,
        "PMS or Tech is in Place": loc.pmsOrTech,
        "Ceiling Heights": loc.ceilingHeights,
        "Ceiling Heights Unit": loc.ceilingHeightsUnit,
        "Column Spacing": loc.columnSpacing,
        "Column Spacing Unit": loc.columnSpacingUnit,
        "Existing MEP Capacity (Conversion)": loc.existingMEPCapacity,
      });
    }
    if (!fields["Hotel Chain Scale"] && normalized.hotelChainScale) fields["Hotel Chain Scale"] = normalized.hotelChainScale;
    if (!fields["Property Type"] && normalized.hotelType) fields["Property Type"] = normalized.hotelType;
    if (!fields["Hotel Service Model"] && normalized.hotelServiceModel) fields["Hotel Service Model"] = normalized.hotelServiceModel;
    return fields;
  }

  global.DealBriefRenderHtml = {
    esc: esc,
    formatFieldValue: formatFieldValue,
    getField: getField,
    detailSections: detailSections,
    mergeFieldsFromNormalized: mergeFieldsFromNormalized,
    buildCoverPageHtml: buildCoverPageHtml,
    buildContentPageHtml: buildContentPageHtml,
  };
})(typeof window !== "undefined" ? window : globalThis);
