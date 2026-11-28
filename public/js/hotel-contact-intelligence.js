/**
 * Hotel Explorer — Contact Intelligence V3 (professional directory table).
 * Sortable purpose-filtered table; no Recommended First hierarchy.
 * Surfe "Find Email & Phone" is user-initiated only (never on mount/GET).
 * Org website/email routes never leak from economic sponsor onto unrelated employers.
 */
(function (global) {
  "use strict";

  var SHOWCASE_PATHS = {
    recUNycnMwOVFX0hc: "/data/hotel-contact-intelligence/kgpv-recUNycnMwOVFX0hc.json",
  };

  var FIND_LABEL = "Find Email & Phone";
  var FIND_LABEL_HTML =
    '<span class="hci-enrich-btn__line">Find Email</span>' +
    '<span class="hci-enrich-btn__line">&amp; Phone</span>';
  var LABELS = {
    directEmail: "Direct Business Email",
    directPhone: "Direct Business Phone",
    companyWebsite: "Company website",
    companyEmail: "Company email",
    officePhone: "Office phone",
    orgRoute: "Company contact",
    findEmail: "Find Email & Phone",
    findPhone: "Find Email & Phone",
    findBoth: "Find Email & Phone",
  };
  var _enrichToastHost = null;

  /* Match GDI gdi-commercial-list-table-v1 contact-column icons exactly. */
  var ICON_EMAIL =
    '<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>';
  var ICON_PHONE =
    '<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72c.13.96.37 1.9.72 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.91.35 1.85.59 2.81.72A2 2 0 0 1 22 16.92z"/></svg>';
  var ICON_LINKEDIN =
    '<svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor" aria-hidden="true"><path d="M4.98 3.5C4.98 4.88 3.86 6 2.5 6S0 4.88 0 3.5 1.12 1 2.5 1s2.48 1.12 2.48 2.5zM.5 8.5h4V23h-4V8.5zM8.5 8.5h3.8v2h.05c.53-1 1.82-2.05 3.75-2.05 4.01 0 4.75 2.64 4.75 6.07V23h-4v-5.9c0-1.41-.03-3.22-1.96-3.22-1.96 0-2.26 1.53-2.26 3.12V23h-4V8.5z"/></svg>';

  /** Purpose sections — mirrored from contact-purpose-sections-v1 (client cannot import ESM). */
  var PURPOSE_SECTIONS = [
    {
      id: "hotel_leadership",
      label: "Hotel Leadership",
      description: "Property general management and on-site leadership.",
      showWhenEmpty: true,
      emptyHint:
        "Property general manager and on-site leadership have not been established for this hotel yet.",
    },
    {
      id: "sales_commercial",
      label: "Sales & Commercial Leadership",
      description: "Sales, revenue, and commercial performance contacts.",
      showWhenEmpty: true,
      emptyHint:
        "Director of sales / marketing and revenue leadership have not been established for this hotel yet.",
    },
    {
      id: "ownership_asset",
      label: "Ownership & Asset Management",
      description:
        "Owner-side and asset-management contacts. Appearance here does not confirm hotel-specific decision authority.",
      showWhenEmpty: false,
    },
    {
      id: "management_company",
      label: "Management Company & Regional Leadership",
      description: "Operator / management-company and regional leadership.",
      showWhenEmpty: false,
    },
    {
      id: "brand_development",
      label: "Brand & Development",
      description: "Brand, franchise, and development contacts.",
      showWhenEmpty: false,
    },
  ];
  var BUSINESS_PURPOSE_FILTERS = [
    { id: "all", label: "All Contacts", sectionIds: PURPOSE_SECTIONS.map(function (s) { return s.id; }) },
    { id: "hotel_leadership", label: "Hotel Leadership", sectionIds: ["hotel_leadership"] },
    { id: "sales_commercial", label: "Sales & Commercial", sectionIds: ["sales_commercial"] },
    { id: "ownership_asset", label: "Ownership & Asset Management", sectionIds: ["ownership_asset"] },
    { id: "management_company", label: "Management Company", sectionIds: ["management_company"] },
    { id: "brand_development", label: "Brand & Development", sectionIds: ["brand_development"] },
  ];
  var _inflightReveal = Object.create(null);
  /** Session-only Surfe results — never written into person channel arrays. */
  var _sessionSurfeByPerson = Object.create(null);

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  /**
   * DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1 — presentation only.
   * Prefer shared util when loaded; never mutate stored channel values.
   */
  function formatPhonePresentation(phone) {
    var raw = String(phone == null ? "" : phone);
    var trimmed = raw.trim();
    if (!trimmed) {
      return { displayValue: "", telHref: "", originalRaw: raw };
    }
    var api = global.DealalitySharedPhoneFormattingV1;
    if (api && typeof api.formatDealalityPhone === "function") {
      var r = api.formatDealalityPhone(trimmed);
      return {
        displayValue: r.displayValue || trimmed,
        telHref: r.telHref || "",
        originalRaw: r.originalRaw != null ? r.originalRaw : raw,
      };
    }
    var keepPlus = trimmed.charAt(0) === "+";
    var digits = trimmed.replace(/[^\d]/g, "");
    return {
      displayValue: trimmed,
      telHref: digits ? "tel:" + (keepPlus ? "+" : "") + digits : "",
      originalRaw: raw,
    };
  }

  function telHrefForPhone(phone) {
    return formatPhonePresentation(phone).telHref;
  }

  function phoneAnchorHtml(phone, className) {
    var p = formatPhonePresentation(phone);
    if (!p.displayValue) return "";
    var cls = className || "hci-action-link";
    if (p.telHref) {
      return (
        '<a class="' +
        cls +
        '" href="' +
        esc(p.telHref) +
        '">' +
        esc(p.displayValue) +
        "</a>"
      );
    }
    return "<span>" + esc(p.displayValue) + "</span>";
  }

  function present(v) {
    if (v == null) return false;
    var s = String(v).trim();
    return s !== "" && s !== "—" && s !== "-" && s.toLowerCase() !== "n/a";
  }

  function humanResponsibility(p) {
    var key = String(p.propertyResponsibility || "").toUpperCase();
    if (key === "CONFIRMED") return "Confirmed hotel responsibility";
    if (key === "INFERRED") return "Ownership-side association — hotel-specific authority not confirmed";
    return "Hotel responsibility not confirmed";
  }

  /** Only surface qualification when ownership-side INFERRED (not on every non-CONFIRMED row). */
  function showResponsibilityQualification(p) {
    var key = String(p.propertyResponsibility || "").toUpperCase();
    if (key !== "INFERRED") return false;
    var purposes = assignPurposeSectionsClient(p);
    return purposes.indexOf("ownership_asset") >= 0 && purposes.indexOf("hotel_leadership") < 0;
  }

  function normOrgName(v) {
    return String(v || "")
      .toLowerCase()
      .replace(/[.,]/g, " ")
      .replace(/\b(inc|llc|ltd|corp|corporation|company|co|group|international|hotels?|hospitality)\b/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function personBelongsToOrganization(person, organizationName) {
    var org = normOrgName(organizationName);
    if (!org) return false;
    var personOrg = normOrgName(person.organization || person.organizationName || "");
    if (!personOrg) return false;
    return (
      personOrg === org ||
      personOrg.indexOf(org) >= 0 ||
      org.indexOf(personOrg) >= 0 ||
      (personOrg.indexOf("marriott") >= 0 && org.indexOf("marriott") >= 0) ||
      (personOrg.indexOf("mullen") >= 0 && org.indexOf("mullen") >= 0)
    );
  }

  function resolveDisplayHotelName(vm, fallbackId) {
    var name =
      (vm && vm.hotelName) ||
      (vm && vm.ownershipSummary && vm.ownershipSummary.hotel && vm.ownershipSummary.hotel.name) ||
      "";
    name = String(name || "").trim();
    if (!name || /^rec[a-zA-Z0-9]{14}$/i.test(name)) {
      if (String(fallbackId || "") === "recLuxvwwxID7U2B8") return "Bethesda Marriott";
      return "Hotel contacts";
    }
    return name;
  }

  function personBlob(p) {
    return [
      p.title,
      p.whyRelevant,
      p.contactClass,
      p.contactClassLabel,
      p.hotelResponsibility,
      p.commercialRole,
      p.functionRole,
    ]
      .filter(Boolean)
      .join(" ");
  }

  function assignPurposeSectionsClient(p) {
    if (Array.isArray(p.purposeSections) && p.purposeSections.length) return p.purposeSections.slice();
    var text = personBlob(p);
    var contactClass = String(p.contactClass || "").toUpperCase();
    var ids = {};
    function add(id) {
      ids[id] = true;
    }
    if (
      /\b(general\s*manager|\bgm\b|resident\s*manager|hotel\s*manager|director\s*of\s*operations)\b/i.test(text) ||
      contactClass === "HOTEL_LEADERSHIP" ||
      contactClass === "PRIMARY_PROPERTY_DECISION_MAKER" ||
      /VERIFIED_PROPERTY_LEADERSHIP|PROPERTY LEADERSHIP/i.test(text)
    ) {
      add("hotel_leadership");
    }
    if (
      /\b(director\s*of\s*sales|dosm|area\s*sales|sales\s*leader|revenue\s*manag|commercial\s*director|director\s*of\s*revenue)\b/i.test(
        text
      ) ||
      /PROPERTY LEADERSHIP\s*[—-]\s*SALES|VERIFIED_PROPERTY_COMMERCIAL/i.test(text)
    ) {
      add("sales_commercial");
    }
    if (
      contactClass === "OWNER_DECISION_AUTHORITY" ||
      contactClass === "RELEVANT_COMMERCIAL_ENTRY_POINT" ||
      /\b(owner|ownership|president|ceo|asset\s*manag|portfolio\s*manag|investments?)\b/i.test(text)
    ) {
      if (
        !/\b(director\s*of\s*sales|dosm|area\s*sales)\b/i.test(text) ||
        /asset|owner|invest|president|ceo/i.test(text)
      ) {
        add("ownership_asset");
      }
    }
    if (/\b(management\s*company|regional\s*|cluster\s*manager|multi[- ]property)\b/i.test(text)) {
      add("management_company");
    }
    if (
      (ids.hotel_leadership || ids.sales_commercial) &&
      /marriott|hilton|hyatt|ihg|accor|operator/i.test(String(p.organization || p.organizationName || ""))
    ) {
      add("management_company");
    }
    if (/\b(brand\s*development|franchise|development\s*director|openings?|pipeline)\b/i.test(text)) {
      add("brand_development");
    }
    var out = Object.keys(ids);
    if (!out.length) {
      if (/OWNER|SPONSOR|MULLEN/i.test(String(p.organization || "") + text)) out = ["ownership_asset"];
      else out = ["management_company"];
    }
    return out;
  }

  function buildPurposeSectionsFromPeople(people) {
    var list = Array.isArray(people) ? people : [];
    return PURPOSE_SECTIONS.map(function (meta) {
      var members = list
        .filter(function (p) {
          return assignPurposeSectionsClient(p).indexOf(meta.id) >= 0;
        })
        .slice()
        .sort(function (a, b) {
          var ra = String(a.propertyResponsibility || "").toUpperCase() === "CONFIRMED" ? 0 : 1;
          var rb = String(b.propertyResponsibility || "").toUpperCase() === "CONFIRMED" ? 0 : 1;
          if (ra !== rb) return ra - rb;
          return String(a.name || "").localeCompare(String(b.name || ""));
        });
      return {
        id: meta.id,
        label: meta.label,
        description: meta.description,
        people: members,
        empty: members.length === 0,
      };
    });
  }

  function linkedInUrl(p) {
    if (present(p.linkedinUrl)) return String(p.linkedinUrl).trim();
    var hit = (p.profileUrls || []).find(function (pr) {
      return pr && present(pr.url) && /linkedin/i.test(String(pr.type || pr.url || ""));
    });
    return hit ? String(hit.url).trim() : "";
  }

  function companyUrl(p) {
    var raw = "";
    if (present(p.organizationWebsite)) raw = String(p.organizationWebsite).trim();
    else if (present(p.companyWebsite)) raw = String(p.companyWebsite).trim();
    else if (present(p.organizationDomain)) {
      var d = String(p.organizationDomain).replace(/^https?:\/\//i, "").replace(/\/$/, "");
      if (d) raw = "https://" + d + "/";
    }
    if (!present(raw)) return "";
    // Never inherit mullencap (or other sponsor domains) onto non-matching employers.
    var host = "";
    try {
      host = new URL(/^https?:\/\//i.test(raw) ? raw : "https://" + raw).hostname
        .replace(/^www\./i, "")
        .toLowerCase();
    } catch (e) {
      host = "";
    }
    if (host.indexOf("mullencap") >= 0 && !personBelongsToOrganization(p, "Mullen Real Estate Capital")) {
      return "";
    }
    return raw;
  }

  function isCorporateMislabel(ch) {
    var classification = String(ch.classification || ch.type || "").toLowerCase();
    var value = String(ch.value || ch.email || "").toLowerCase();
    var subject = String(ch.subject_type || ch.subjectType || "").toLowerCase();
    var note = String(ch.note || "").toLowerCase();
    return /official_org|org_general|organization|switchboard|info@|contact@|hello@|organizational/.test(
      classification + " " + value + " " + subject + " " + note
    );
  }

  function isEphemeralOrSurfe(ch) {
    return !!(
      ch &&
      (ch.surfeDerived === true ||
        ch.ephemeral === true ||
        String(ch.provider || "").toLowerCase() === "surfe" ||
        String(ch.verification_status || ch.verificationStatus || "")
          .toUpperCase()
          .indexOf("PROVIDER_RETURNED") >= 0)
    );
  }

  function personEmails(p) {
    var fromTyped = (p.emailContacts || p.emails || []).filter(function (c) {
      return (
        c &&
        present(c.value || c.email) &&
        !isCorporateMislabel(c) &&
        !isEphemeralOrSurfe(c)
      );
    });
    if (fromTyped.length) return fromTyped;
    return (p.channels || [])
      .filter(function (c) {
        var kind = c && (c.kind || c.channelType || c.type);
        var val = c && (c.value || c.channelValue || c.email);
        return (
          c &&
          present(val) &&
          channelKindIsEmail(kind) &&
          !isCorporateMislabel(c) &&
          !isEphemeralOrSurfe(c)
        );
      })
      .map(function (c) {
        return {
          value: c.value || c.channelValue || c.email,
          type: c.kind || c.channelType,
          verification_status: c.verificationStatus || c.verification_status,
        };
      });
  }

  function personPhones(p) {
    var fromTyped = (p.phoneContacts || p.phones || []).filter(function (c) {
      return c && present(c.value || c.phone) && !isCorporateMislabel(c) && !isEphemeralOrSurfe(c);
    });
    if (fromTyped.length) return fromTyped;
    return (p.channels || [])
      .filter(function (c) {
        var kind = c && (c.kind || c.channelType || c.type);
        var val = c && (c.value || c.channelValue || c.phone);
        return (
          c &&
          present(val) &&
          channelKindIsPhone(kind) &&
          !isCorporateMislabel(c) &&
          !isEphemeralOrSurfe(c)
        );
      })
      .map(function (c) {
        return {
          value: c.value || c.channelValue || c.phone,
          type: c.kind || c.channelType,
          verification_status: c.verificationStatus || c.verification_status,
        };
      });
  }

  function personOrgRoutes(p, vm) {
    var out = [];
    var seen = {};
    function push(val, kind, source) {
      if (!present(val)) return;
      var key = kind + ":" + String(val).toLowerCase();
      if (seen[key]) return;
      seen[key] = true;
      out.push({ value: val, kind: kind, source: source || null });
    }
    (p.emailContacts || p.emails || []).forEach(function (c) {
      if (c && present(c.value || c.email) && isCorporateMislabel(c) && !isEphemeralOrSurfe(c)) {
        push(c.value || c.email, "email", c.source_url || c.sourceUrl);
      }
    });
    (p.phoneContacts || p.phones || []).forEach(function (c) {
      if (c && present(c.value || c.phone) && isCorporateMislabel(c) && !isEphemeralOrSurfe(c)) {
        push(c.value || c.phone, "phone", c.source_url || c.sourceUrl);
      }
    });
    (p.organizationalChannels || []).forEach(function (c) {
      var v = c && (c.value || c.email || c.phone || c.url);
      if (!present(v)) return;
      if (String(v).indexOf("@") >= 0) push(v, "email", c.source_url);
      else if (/^https?:/i.test(String(v))) push(v, "web", c.source_url);
      else push(v, "phone", c.source_url);
    });
    // Person-owned company website only (never silent sponsor inheritance).
    var ownWeb = companyUrl(p);
    if (present(ownWeb)) push(ownWeb, "web", ownWeb);

    // Owner/sponsor org routes attach ONLY when this person belongs to that org.
    var org = (vm && vm.contactableOwnerOrg) || null;
    if (org && personBelongsToOrganization(p, org.name)) {
      (org.emails || []).forEach(function (c) {
        push(c.value || c.email, "email", c.source_url);
      });
      (org.phones || []).forEach(function (c) {
        push(c.value || c.phone, "phone", c.source_url);
      });
      if (present(org.website)) push(org.website, "web", org.website);
    }
    return out;
  }

  function resolveEnrichMeta(p, capability) {
    var hasName = present(p.name) && String(p.name).trim().split(/\s+/).length >= 2;
    var hasOrg = present(p.organization) || present(p.organizationName);
    var hasEmail = personEmails(p).length > 0;
    var hasPhone = personPhones(p).length > 0;
    var hasLinkedIn = Boolean(linkedInUrl(p));
    var action = p.enrichAction || {};
    var providerReady =
      capability &&
      (capability.surfeEnabled === true ||
        capability.mockEnabled === true ||
        capability.findymailEnabled === true ||
        capability.hybridEnrichEnabled === true);
    // Approved customer label stays constant; contextual title describes missing channel.
    var label = FIND_LABEL;
    var contextualDescription = null;
    var missing = [];
    var channel = "both";
    if (!hasEmail) missing.push("professional_email");
    if (!hasPhone) missing.push("direct_business_phone");
    if (!hasEmail && hasPhone) {
      channel = "email";
      contextualDescription = "Find Missing Email";
    } else if (hasEmail && !hasPhone) {
      channel = "phone";
      contextualDescription = "Find Missing Phone";
    } else if (hasEmail && hasPhone) {
      channel = "complete";
    } else {
      contextualDescription = FIND_LABEL;
    }

    // Button always visible. Dimmed only when both email + phone already on file.
    if (hasEmail && hasPhone) {
      return {
        label: label,
        contextualDescription: null,
        enabled: false,
        dimmedComplete: true,
        channel: channel,
        missing: [],
        hasVerifiedEmail: true,
        hasVerifiedPhone: true,
        hasVerifiedLinkedIn: hasLinkedIn,
        needsEmail: false,
        needsPhone: false,
        reason: "Verified email and phone already on file.",
      };
    }
    if (!hasName || !hasOrg) {
      return {
        label: label,
        contextualDescription: contextualDescription,
        enabled: false,
        dimmedComplete: false,
        channel: null,
        missing: missing,
        hasVerifiedEmail: hasEmail,
        hasVerifiedPhone: hasPhone,
        hasVerifiedLinkedIn: hasLinkedIn,
        needsEmail: !hasEmail,
        needsPhone: !hasPhone,
        reason: "Enrichment needs a full name and organization.",
        providerUnavailable: false,
      };
    }
    var enabled =
      action.enabled === true ||
      (providerReady && action.enabled !== false);
    return {
      label: label,
      contextualDescription: contextualDescription,
      enabled: enabled,
      dimmedComplete: false,
      channel: channel,
      missing: missing,
      hasVerifiedEmail: hasEmail,
      hasVerifiedPhone: hasPhone,
      hasVerifiedLinkedIn: hasLinkedIn,
      needsEmail: !hasEmail,
      needsPhone: !hasPhone,
      providerUnavailable: !providerReady,
      reason:
        contextualDescription ||
        (capability && capability.userReason) ||
        action.reason ||
        (!providerReady
          ? "Contact enrichment is not configured in this environment."
          : null),
    };
  }

  function renderLoading() {
    // Same shared HeLoadingUx toast used by Ownership / News / Sentiment — no custom HCI loader.
    var body =
      window.HeLoadingUx && typeof window.HeLoadingUx.sectionHtml === "function"
        ? window.HeLoadingUx.sectionHtml("Loading Contacts…")
        : window.HeLoadingUx && typeof window.HeLoadingUx.toastHtml === "function"
          ? window.HeLoadingUx.toastHtml("Loading Contacts…")
          : window.HeAsyncSection && typeof window.HeAsyncSection.waveCompact === "function"
            ? window.HeAsyncSection.waveCompact("Loading Contacts…")
            : window.DealalityWaveLoader && typeof window.DealalityWaveLoader.toastHtml === "function"
              ? window.DealalityWaveLoader.toastHtml("Loading Contacts…")
              : '<div class="dealality-loading-toast-host he-loading-toast-host" role="status" aria-live="polite">Loading Contacts…</div>';
    return (
      '<div class="hci-root hci-root--loading" data-hci-state="loading">' +
      '<section class="hci-section he-async-loading" data-hci-wave-loading="1" data-he-loading-toast="1">' +
      body +
      "</section></div>"
    );
  }

  function renderEmpty(msg) {
    return (
      '<div class="hci-root" data-hci-state="empty">' +
      '<div class="hci-empty">' +
      esc(msg || "Contact Intelligence is not available for this hotel yet.") +
      "</div></div>"
    );
  }

  function waveHtml(label, sub) {
    if (window.HeLoadingUx && window.HeLoadingUx.inlineHtml) {
      return window.HeLoadingUx.inlineHtml(label, { subText: sub || "" });
    }
    if (window.DealalityWaveLoader && window.DealalityWaveLoader.compactHtml) {
      return window.DealalityWaveLoader.compactHtml(label, {
        progress: false,
        subText: sub || "",
      });
    }
    return '<p class="hci-muted">' + esc(label) + "</p>";
  }

  /** Shared Dealality black-wave toast — never inject into table rows. */
  function ensureEnrichToastHost() {
    if (_enrichToastHost && document.body.contains(_enrichToastHost)) return _enrichToastHost;
    var existing = document.getElementById("hci-enrich-loading-toast");
    if (existing) {
      _enrichToastHost = existing;
      return existing;
    }
    var host = document.createElement("div");
    host.id = "hci-enrich-loading-toast";
    host.className =
      "dealality-loading-toast-host he-loading-toast-host hci-enrich-loading-toast";
    host.setAttribute("role", "status");
    host.setAttribute("aria-live", "polite");
    host.hidden = true;
    host.style.display = "none";
    var markup = "";
    if (window.HeLoadingUx && typeof window.HeLoadingUx.toastHtml === "function") {
      markup = window.HeLoadingUx.toastHtml("Finding Contact Details…", {
        subText: "Surfe · Session Only",
      });
    } else if (
      window.DealalitySharedLoadingUx &&
      typeof window.DealalitySharedLoadingUx.toastHtml === "function"
    ) {
      markup = window.DealalitySharedLoadingUx.toastHtml("Finding Contact Details…", {
        subText: "Surfe · Session Only",
      });
    } else {
      markup =
        '<div class="dealality-wave-loader dealality-wave-loader--inline dealality-wave-loader--compact dealality-wave-loader--toast">' +
        '<div class="loading-text-main">Finding Contact Details…</div></div>';
    }
    host.innerHTML = markup;
    document.body.appendChild(host);
    _enrichToastHost = host;
    return host;
  }

  function showEnrichToast(message) {
    var host = ensureEnrichToastHost();
    var msg =
      host.querySelector("[data-dealality-loading-message], .loading-text-main, .toast-message") ||
      null;
    if (msg) msg.textContent = message || "Finding Contact Details…";
    if (
      window.DealalitySharedLoadingUx &&
      typeof window.DealalitySharedLoadingUx.setPageToastBusy === "function"
    ) {
      window.DealalitySharedLoadingUx.setPageToastBusy(
        host,
        true,
        message || "Finding Contact Details…",
      );
      return;
    }
    host.hidden = false;
    host.style.display = "flex";
    host.classList.add("show");
    host.setAttribute("aria-busy", "true");
  }

  function hideEnrichToast() {
    var host = _enrichToastHost || document.getElementById("hci-enrich-loading-toast");
    if (!host) return;
    if (
      window.DealalitySharedLoadingUx &&
      typeof window.DealalitySharedLoadingUx.setPageToastBusy === "function"
    ) {
      window.DealalitySharedLoadingUx.setPageToastBusy(host, false);
      return;
    }
    host.classList.remove("show");
    host.hidden = true;
    host.style.display = "none";
    host.setAttribute("aria-busy", "false");
  }

  function setEnrichButtonLabel(btn, mode) {
    if (!btn) return;
    if (mode === "finding") {
      btn.innerHTML = '<span class="hci-enrich-btn__busy">Finding…</span>';
      return;
    }
    if (mode === "complete") {
      btn.textContent = "Lookup complete";
      return;
    }
    btn.innerHTML = FIND_LABEL_HTML;
    btn.setAttribute("data-enrich-label", FIND_LABEL);
  }

  function findEnrichSlot(btn) {
    if (!btn || !btn.isConnected) return null;
    var wrap = btn.closest("[data-hci-enrich-slot]");
    if (wrap) return wrap.querySelector(".hci-enrich__result");
    return btn.parentElement
      ? btn.parentElement.querySelector(".hci-enrich__result")
      : null;
  }

  function findEnrichButton(personId, hotelId) {
    var sel =
      '[data-hci-enrich][data-person-id="' +
      String(personId || "").replace(/"/g, '\\"') +
      '"]';
    if (hotelId) {
      sel += '[data-hotel-id="' + String(hotelId || "").replace(/"/g, '\\"') + '"]';
    }
    return document.querySelector(sel);
  }

  function channelValue(ch) {
    return String((ch && (ch.value || ch.email || ch.phone)) || "").trim();
  }

  function renderCompactChannel(ch, kind) {
    var val = channelValue(ch);
    if (!val) return "";
    if (kind === "phone") return phoneAnchorHtml(val);
    var href =
      kind === "email"
        ? "mailto:" + val
        : /^https?:/i.test(val)
          ? val
          : "";
    return (
      '<a class="hci-action-link" href="' +
      esc(href || "#") +
      '"' +
      (kind === "web" || /^https?:/i.test(val)
        ? ' target="_blank" rel="noopener"'
        : "") +
      ">" +
      esc(val) +
      "</a>"
    );
  }

  function renderLabeledChannel(label, ch, kind) {
    var val = channelValue(ch);
    if (!val) return "";
    return (
      '<div class="hci-channel-row"><span class="hci-channel-label">' +
      esc(label) +
      "</span> " +
      renderCompactChannel(ch, kind) +
      "</div>"
    );
  }

  function renderPersonActions(p, hotelId, capability, opts) {
    opts = opts || {};
    var li = linkedInUrl(p);
    var company = companyUrl(p);
    var emails = personEmails(p);
    var phones = personPhones(p);
    var parts = [];

    if (emails.length) {
      parts.push(
        '<a class="hci-btn hci-btn--primary" href="mailto:' +
          esc(channelValue(emails[0])) +
          '">Email</a>'
      );
    }
    if (phones.length) {
      var callPhone = formatPhonePresentation(channelValue(phones[0]));
      if (callPhone.telHref) {
        parts.push(
          '<a class="hci-btn hci-btn--primary" href="' +
            esc(callPhone.telHref) +
            '" title="' +
            esc(callPhone.displayValue) +
            '">Call</a>'
        );
      }
    }
    if (li) {
      parts.push(
        '<a class="hci-action-link hex-rms-ext-link" href="' +
          esc(li) +
          '" target="_blank" rel="noopener">Professional profile</a>'
      );
    }
    if (company) {
      parts.push(
        '<a class="hci-action-link hex-rms-ext-link" href="' +
          esc(company) +
          '" target="_blank" rel="noopener">Company website</a>'
      );
    }

    if (!emails.length && !phones.length) {
      parts.push(renderEnrichAction(p, hotelId, capability, { inline: true }));
    }

    return '<div class="hci-actions">' + parts.filter(Boolean).join("") + "</div>";
  }

  function renderEnrichAction(p, hotelId, capability, opts) {
    opts = opts || {};
    var meta = resolveEnrichMeta(p, capability);
    var li = linkedInUrl(p);
    var compact = opts.compact === true;
    var disabled = !meta.enabled;
    var title = meta.contextualDescription || meta.reason || "";
    var btnClass =
      "hci-btn hci-btn--enrich hci-btn--enrich-two-line" +
      (compact ? " hci-btn--enrich-compact" : "") +
      (meta.dimmedComplete ? " hci-btn--enrich-dimmed" : "") +
      (disabled ? " is-disabled" : "");
    return (
      '<div class="hci-enrich' +
      (compact ? " hci-enrich--inline-cell" : "") +
      '">' +
      '<button type="button" class="' +
      btnClass +
      '" data-hci-enrich="1" data-hci-one-click-surfe="1" data-person-id="' +
      esc(p.personId || "") +
      '" data-hotel-id="' +
      esc(hotelId || "") +
      '" data-person-name="' +
      esc(p.name || "") +
      '" data-organization="' +
      esc(p.organization || p.organizationName || "") +
      '" data-job-title="' +
      esc(p.title || "") +
      '" data-linkedin-url="' +
      esc(li) +
      '" data-company-domain="' +
      esc(p.organizationDomain || "") +
      '" data-enrich-channel="' +
      esc(meta.channel || "both") +
      '" data-enrich-label="' +
      esc(meta.label || FIND_LABEL) +
      '" data-needs-email="' +
      (meta.needsEmail ? "1" : "0") +
      '" data-needs-phone="' +
      (meta.needsPhone ? "1" : "0") +
      '" data-dimmed-complete="' +
      (meta.dimmedComplete ? "1" : "0") +
      '" title="' +
      esc(title) +
      '" aria-label="' +
      esc(title || meta.label || FIND_LABEL) +
      '"' +
      (disabled ? " disabled aria-disabled=\"true\"" : "") +
      ">" +
      FIND_LABEL_HTML +
      "</button>" +
      (compact
        ? ""
        : '<div class="hci-enrich__result" hidden></div>') +
      "</div>"
    );
  }

  function orgRouteLabel(c) {
    if (!c) return LABELS.orgRoute;
    if (c.kind === "web") return LABELS.companyWebsite;
    if (c.kind === "email") return LABELS.companyEmail;
    if (c.kind === "phone") return LABELS.officePhone;
    return LABELS.orgRoute;
  }

  function customerWhyRelevant(p) {
    var raw = String(p.whyRelevant || p.relationshipToHotel || "").trim();
    if (!raw) return "";
    // Strip internal enum-like prefixes for primary card copy
    raw = raw
      .replace(/^VERIFIED_[A-Z_]+\s*[—\-:]?\s*/i, "")
      .replace(/^RELEVANT_COMMERCIAL_ENTRY_POINT\s*[—\-:]?\s*/i, "")
      .replace(/^OWNER_DECISION_AUTHORITY\s*[—\-:]?\s*/i, "");
    if (raw.length > 180) {
      var cut = raw.indexOf(". ");
      raw = cut > 40 && cut < 180 ? raw.slice(0, cut + 1) : raw.slice(0, 177) + "…";
    }
    return raw;
  }

  function renderPerson(p, hotelId, capability, opts) {
    opts = opts || {};
    var detailsId =
      "hci-person-" +
      String(p.personId || Math.random().toString(36).slice(2)).replace(/[^a-zA-Z0-9_-]/g, "_") +
      (opts.sectionId ? "-" + opts.sectionId : "");
    var relevance = customerWhyRelevant(p);
    if (present(opts.rationaleOverride)) relevance = customerWhyRelevant({ whyRelevant: opts.rationaleOverride });
    var emails = personEmails(p);
    var phones = personPhones(p);
    var orgRoutes = personOrgRoutes(p, opts.vm || null);
    var session = _sessionSurfeByPerson[p.personId || ""] || null;
    var qualify = showResponsibilityQualification(p);

    var channelPreview = "";
    if (emails.length || phones.length || orgRoutes.length || session) {
      channelPreview =
        '<div class="hci-person__channels-preview" data-hci-channel-hierarchy="1">' +
        emails
          .map(function (c) {
            return renderLabeledChannel(LABELS.directEmail, c, "email");
          })
          .join("") +
        phones
          .map(function (c) {
            return renderLabeledChannel(LABELS.directPhone, c, "phone");
          })
          .join("") +
        orgRoutes
          .slice(0, 3)
          .map(function (c) {
            return renderLabeledChannel(
              orgRouteLabel(c),
              c,
              c.kind === "email" ? "email" : c.kind === "web" ? "web" : "phone"
            );
          })
          .join("") +
        (session
          ? '<div class="hci-channel-row hci-channel-row--session" data-surfe-session="1">' +
            '<span class="hci-channel-label">Lookup result (session only)</span> ' +
            (session.email
              ? '<a class="hci-action-link" href="mailto:' +
                esc(session.email) +
                '">' +
                esc(session.email) +
                "</a> "
              : "") +
            (session.phone ? phoneAnchorHtml(session.phone) : "") +
            ' <span class="hci-muted">· not saved</span></div>'
          : "") +
        "</div>";
    }

    return (
      '<article class="hci-person" data-person-id="' +
      esc(p.personId || "") +
      '" data-contact-class="' +
      esc(p.contactClass || "") +
      '" data-property-responsibility="' +
      esc(p.propertyResponsibility || "") +
      '" data-purpose-sections="' +
      esc(assignPurposeSectionsClient(p).join(",")) +
      '" data-identity-verified="' +
      (linkedInUrl(p) || p.identityVerification === "VERIFIED_PUBLIC" ? "1" : "0") +
      '" data-contact-details-verified="' +
      (emails.length || phones.length ? "1" : "0") +
      '">' +
      '<div class="hci-person__row">' +
      '<div class="hci-person__identity">' +
      "<h5>" +
      esc(p.name) +
      "</h5>" +
      (present(p.title) ? '<div class="hci-person__title">' + esc(p.title) + "</div>" : "") +
      (present(p.organization || p.organizationName)
        ? '<div class="hci-person__org">' + esc(p.organization || p.organizationName) + "</div>"
        : "") +
      "</div></div>" +
      (present(relevance) ? '<p class="hci-person__why">' + esc(relevance) + "</p>" : "") +
      (qualify
        ? '<p class="hci-person__qualify">' + esc(humanResponsibility(p)) + "</p>"
        : "") +
      channelPreview +
      renderPersonActions(p, hotelId, capability, opts) +
      '<button type="button" class="hci-provenance-toggle" data-hci-toggle="' +
      detailsId +
      '" aria-expanded="false">Details</button>' +
      '<div class="hci-person__details" id="' +
      detailsId +
      '" hidden>' +
      "<div><strong>Hotel responsibility</strong> " +
      esc(humanResponsibility(p)) +
      "</div>" +
      (present(p.contactClassLabel || p.commercialRole)
        ? '<div class="hci-admin-only" data-hci-admin="1"><strong>Internal role class</strong> ' +
          esc(p.contactClassLabel || p.commercialRole) +
          "</div>"
        : "") +
      (present(p.propertyResponsibilityNote)
        ? "<div><strong>Note</strong> " + esc(p.propertyResponsibilityNote) + "</div>"
        : "") +
      (present(p.verificationNotes)
        ? "<div><strong>Notes</strong> " + esc(p.verificationNotes) + "</div>"
        : "") +
      (present(p.lastVerified)
        ? "<div><strong>Last reviewed</strong> " + esc(p.lastVerified) + "</div>"
        : "") +
      (present(p.airtableId)
        ? '<div class="hci-admin-only" data-hci-admin="1"><strong>Record</strong> ' +
          esc(p.airtableId) +
          "</div>"
        : "") +
      '<p class="hci-muted">Company websites and switchboards are organization routes, not personal inboxes. On-demand lookup results stay in this session and are not saved.</p>' +
      "</div></article>"
    );
  }

  var vmCapability = null;

  function renderOwnershipSlim(vm) {
    var o = vm.ownershipSummary || {};
    var bits = [];
    if (o.propco && present(o.propco.shortName || o.propco.name)) {
      bits.push("Property company: " + (o.propco.shortName || o.propco.name));
    }
    if (o.sponsor && present(o.sponsor.name)) {
      var certified = o.sponsor.economicOwnershipCertified === true;
      bits.push(
        certified
          ? "Economic owner: " + o.sponsor.name
          : "Ownership organization (pending certification): " + o.sponsor.name
      );
    }
    if (o.operator && present(o.operator.name)) bits.push("Operator: " + o.operator.name);
    if (o.brand && present(o.brand.name)) bits.push("Brand: " + o.brand.name);
    if (!bits.length) return "";
    return (
      '<section class="hci-section hci-ownership-slim">' +
      '<p class="hci-muted">' +
      esc(bits.join(" · ")) +
      "</p></section>"
    );
  }

  function renderPurposeFilter(activeId) {
    var active = activeId || "all";
    return (
      '<div class="hci-purpose-filter" role="group" aria-label="Business purpose">' +
      '<label class="hci-purpose-filter__label" for="hci-purpose-select">Business purpose</label>' +
      '<select id="hci-purpose-select" class="hci-purpose-filter__select" data-hci-purpose-filter="1">' +
      BUSINESS_PURPOSE_FILTERS.map(function (f) {
        return (
          '<option value="' +
          esc(f.id) +
          '"' +
          (f.id === active ? " selected" : "") +
          ">" +
          esc(f.label) +
          "</option>"
        );
      }).join("") +
      "</select></div>"
    );
  }

  function relationshipLabel(p) {
    var why = customerWhyRelevant(p);
    if (present(why)) return why;
    var purposes = assignPurposeSectionsClient(p)
      .map(function (id) {
        var meta = PURPOSE_SECTIONS.find(function (s) {
          return s.id === id;
        });
        return meta ? meta.label : id;
      })
      .filter(Boolean);
    return purposes.length ? purposes[0] : "—";
  }

  function missingChannelHtml(label) {
    return '<span class="hci-missing" title="' + esc(label) + ' not verified">—</span>';
  }

  function directoryPeopleForFilter(vm, purposeId) {
    var filter =
      BUSINESS_PURPOSE_FILTERS.find(function (f) {
        return f.id === (purposeId || "all");
      }) || BUSINESS_PURPOSE_FILTERS[0];
    var allow = {};
    (filter.sectionIds || []).forEach(function (id) {
      allow[id] = true;
    });
    var seen = {};
    var out = [];
    (vm.people || []).forEach(function (p) {
      if (!p || !present(p.name)) return;
      var purposes = assignPurposeSectionsClient(p);
      var match = purposes.some(function (id) {
        return allow[id];
      });
      if (!match) return;
      var key = String(p.personId || p.airtableId || p.name).toLowerCase();
      if (seen[key]) return;
      seen[key] = true;
      out.push(p);
    });
    out.sort(function (a, b) {
      var ra = String(a.propertyResponsibility || "").toUpperCase() === "CONFIRMED" ? 0 : 1;
      var rb = String(b.propertyResponsibility || "").toUpperCase() === "CONFIRMED" ? 0 : 1;
      if (ra !== rb) return ra - rb;
      return String(a.name || "").localeCompare(String(b.name || ""));
    });
    return out;
  }

  /**
   * CONTACT DETAILS cell — channels stacked left; Find Email & Phone button right.
   * Never falls back to employer website or organizational switchboard.
   */
  function renderVerifiedChannelSummary(p, hotelId, capability) {
    var emails = personEmails(p);
    var phones = personPhones(p);
    var li = linkedInUrl(p);
    var session = _sessionSurfeByPerson[p.personId || ""] || null;
    var email = emails.length ? channelValue(emails[0]) : "";
    var phone = phones.length ? channelValue(phones[0]) : "";
    var sessionEmail = !email && session && session.email ? session.email : "";
    var sessionPhone = !phone && session && session.phone ? session.phone : "";
    var emailLine = email
      ? '<a class="gdi-list-contact-link" href="mailto:' + esc(email) + '">' + esc(email) + "</a>"
      : sessionEmail
        ? '<a class="gdi-list-contact-link gdi-list-contact-link--session" href="mailto:' +
          esc(sessionEmail) +
          '" title="Surfe · Session Only">' +
          esc(sessionEmail) +
          '</a><span class="gdi-list-session-tag">Surfe · Session Only</span>'
        : '<span class="gdi-list-contact-missing">Email Not Available</span>';
    var phoneLine = phone
      ? phoneAnchorHtml(phone, "gdi-list-contact-link")
      : sessionPhone
        ? phoneAnchorHtml(sessionPhone, "gdi-list-contact-link gdi-list-contact-link--session") +
          '<span class="gdi-list-session-tag">Surfe · Session Only</span>'
        : '<span class="gdi-list-contact-missing">Phone Not Available</span>';
    var linkedinLine = li
      ? '<a class="gdi-list-contact-link gdi-list-linkedin" href="' +
        esc(li) +
        '" target="_blank" rel="noopener noreferrer" title="Open LinkedIn profile" aria-label="Open LinkedIn profile for ' +
        esc(p.name || "contact") +
        '">' +
        '<span class="gdi-list-contact-icon" aria-hidden="true">' +
        ICON_LINKEDIN +
        "</span>" +
        '<span class="gdi-list-linkedin-label">LinkedIn Profile</span></a>'
      : '<span class="gdi-list-contact-icon" aria-hidden="true">' +
        ICON_LINKEDIN +
        "</span>" +
        '<span class="gdi-list-contact-missing">LinkedIn Not Available</span>';
    var channelStack =
      '<div class="gdi-list-contact-channels" data-hci-channel-stack="1">' +
      '<div class="gdi-list-contact-line">' +
      '<span class="gdi-list-contact-icon" aria-hidden="true">' +
      ICON_EMAIL +
      "</span>" +
      emailLine +
      "</div>" +
      '<div class="gdi-list-contact-line">' +
      '<span class="gdi-list-contact-icon" aria-hidden="true">' +
      ICON_PHONE +
      "</span>" +
      phoneLine +
      "</div>" +
      '<div class="gdi-list-contact-line gdi-list-contact-linkedin">' +
      linkedinLine +
      "</div>" +
      "</div>";
    var enrichBtn = renderEnrichAction(p, hotelId || "", capability || vmCapability, {
      compact: true,
    });
    return (
      '<div class="gdi-list-contact-details gdi-list-contact-details--with-action" data-hci-contact-details-cell="1">' +
      channelStack +
      '<div class="gdi-list-contact-action" data-hci-enrich-slot="1">' +
      enrichBtn +
      '<div class="hci-enrich__result hci-enrich__result--cell" hidden></div>' +
      "</div></div>"
    );
  }

  function renderDirectoryRowCells(p, hotelId, capability, vm) {
    var emails = personEmails(p);
    var phones = personPhones(p);
    var orgRoutes = personOrgRoutes(p, vm);
    var li = linkedInUrl(p);
    var detailsId =
      "hci-dir-" +
      String(p.personId || Math.random().toString(36).slice(2)).replace(/[^a-zA-Z0-9_-]/g, "_");
    var qualify = showResponsibilityQualification(p)
      ? '<div class="hci-dir-qualify">' + esc(humanResponsibility(p)) + "</div>"
      : "";
    // DETAILS column = Details button only. Find Email & Phone lives in Contact Details.
    var actions =
      '<button type="button" class="hci-btn hci-btn--ghost hci-dir-details-btn" data-hci-toggle="' +
      detailsId +
      '" aria-expanded="false">Details</button>';
    var detailOrgRoutes = orgRoutes
      .map(function (c) {
        return (
          "<div><strong>" +
          esc(orgRouteLabel(c)) +
          "</strong> " +
          renderCompactChannel(c, c.kind === "email" ? "email" : c.kind === "web" ? "web" : "phone") +
          "</div>"
        );
      })
      .join("");
    var authorityNote =
      p.propertyAuthorityVerified === false || p.propertyAuthority === "unverified"
        ? '<p class="hci-muted">Property-level authority for this contact is not yet verified.</p>'
        : "";
    var detailsHtml =
      '<div class="hci-dir-details-panel" id="' +
      detailsId +
      '" hidden>' +
      '<div class="hci-dir-details">' +
      "<div><strong>Name</strong> " +
      esc(p.name || "—") +
      "</div>" +
      "<div><strong>Title</strong> " +
      esc(p.title || "—") +
      "</div>" +
      "<div><strong>Organization</strong> " +
      esc(p.organization || p.organizationName || "—") +
      "</div>" +
      "<div><strong>Function / Relationship</strong> " +
      esc(relationshipLabel(p)) +
      "</div>" +
      qualify +
      "<div><strong>Hotel relationship</strong> " +
      esc(humanResponsibility(p)) +
      "</div>" +
      (present(customerWhyRelevant(p))
        ? "<div><strong>Relevance</strong> " + esc(customerWhyRelevant(p)) + "</div>"
        : "") +
      (emails.length
        ? "<div><strong>Email</strong> " + renderCompactChannel(emails[0], "email") + "</div>"
        : "") +
      (phones.length
        ? "<div><strong>Telephone</strong> " + renderCompactChannel(phones[0], "phone") + "</div>"
        : "") +
      (li
        ? "<div><strong>LinkedIn</strong> <a class=\"hci-action-link hex-rms-ext-link\" href=\"" +
          esc(li) +
          '" target="_blank" rel="noopener">Profile</a></div>'
        : "") +
      detailOrgRoutes +
      authorityNote +
      (present(p.lastVerified)
        ? "<div><strong>Last reviewed</strong> " + esc(p.lastVerified) + "</div>"
        : "") +
      '<p class="hci-muted">Organization websites and switchboards are company routes, not personal inboxes. Findymail durable results save to Airtable. Surfe results stay in this session and are not saved.</p>' +
      "</div></div>";
    return {
      contact: (p.name || "") + " " + (p.title || ""),
      organization: p.organization || p.organizationName || "",
      relationship: relationshipLabel(p),
      channels:
        (emails.length ? channelValue(emails[0]) : "") +
        " " +
        (phones.length ? channelValue(phones[0]) : "") +
        " " +
        (li || ""),
      details: "",
      _rowClass: "hci-dir-row",
      _cells: {
        contact:
          '<div class="hci-dir-contact-stack">' +
          '<div class="hci-dir-contact-stack__name">' +
          esc(p.name || "—") +
          "</div>" +
          '<div class="hci-dir-contact-stack__title">' +
          esc(p.title || "—") +
          "</div></div>",
        organization: esc(p.organization || p.organizationName || "—"),
        relationship:
          '<div class="hci-dir-relationship">' + esc(relationshipLabel(p)) + "</div>",
        channels: renderVerifiedChannelSummary(p, hotelId, capability),
        details: actions,
      },
      _detailsHtml: detailsHtml,
      _person: p,
    };
  }

  function renderContactDirectoryTable(vm, purposeId) {
    var hotelId = vm.hotelId || "";
    var people = directoryPeopleForFilter(vm, purposeId);
    var columns = [
      { key: "contact", label: "Contact", sortable: true },
      { key: "organization", label: "Organization", sortable: true },
      { key: "relationship", label: "Function / Relationship", sortable: true },
      { key: "channels", label: "Contact Details", sortable: false },
      { key: "details", label: "Details", sortable: false },
    ];
    var rows = people.map(function (p) {
      return renderDirectoryRowCells(p, hotelId, vmCapability, vm);
    });
    var api = global.DealalitySortableTable;
    var tableHtml;
    if (api && typeof api.renderMarkup === "function") {
      tableHtml = api.renderMarkup({
        columns: columns,
        rows: rows,
        tableClass: "hci-directory-table hci-directory-table--5col",
        emptyMessage: "No contacts match this business purpose yet.",
        defaultSortKey: "contact",
        defaultSortDir: "asc",
      });
    } else {
      tableHtml =
        '<p class="hci-muted">Contact directory table unavailable (sortable table component not loaded).</p>';
    }
    // Keep detail panels outside the sortable tbody so column sort cannot scramble rows.
    var detailsPanels = rows
      .map(function (row) {
        return row._detailsHtml || "";
      })
      .join("");
    var about =
      global.HeAboutThisAnalysis && typeof global.HeAboutThisAnalysis.defaultContactNote === "function"
        ? global.HeAboutThisAnalysis.defaultContactNote()
        : "";
    return (
      '<section class="hci-section hci-section--directory" data-hci-directory="v3" data-hci-columns="5">' +
      '<div class="hci-directory-meta hci-muted">' +
      people.length +
      " contact" +
      (people.length === 1 ? "" : "s") +
      "</div>" +
      tableHtml +
      '<div class="hci-dir-details-host">' +
      detailsPanels +
      "</div>" +
      about +
      "</section>"
    );
  }

  function renderPurposeSections(vm, purposeId) {
    return renderContactDirectoryTable(vm, purposeId);
  }

  function renderOrgChannels(vm) {
    var rows = [];
    var contactable = vm.contactableOwnerOrg;
    if (contactable) {
      if (present(contactable.website)) {
        rows.push({ label: LABELS.companyWebsite, value: contactable.website, kind: "web" });
      }
      (contactable.emails || []).forEach(function (c) {
        if (c && present(c.value)) {
          rows.push({
            label: LABELS.companyEmail,
            value: c.value,
            kind: "email",
            note: "Company-wide — not a personal inbox",
          });
        }
      });
      (contactable.phones || []).forEach(function (c) {
        if (c && present(c.value)) {
          rows.push({
            label: LABELS.officePhone,
            value: c.value,
            kind: "phone",
            note: "Published office line",
          });
        }
      });
      if (present(contactable.hq)) {
        rows.push({ label: "Headquarters", value: contactable.hq, kind: "text" });
      }
    }
    (vm.bestContactPaths || []).forEach(function (b) {
      if (!b || !present(b.value)) return;
      var dup = rows.some(function (r) {
        return String(r.value).toLowerCase() === String(b.value).toLowerCase();
      });
      if (dup) return;
      var kind = /^https?:/i.test(b.value) ? "web" : /@/.test(b.value) ? "email" : "text";
      rows.push({
        label: kind === "web" ? LABELS.companyWebsite : kind === "email" ? LABELS.companyEmail : LABELS.orgRoute,
        value: b.value,
        kind: kind,
        note: "Company-wide contact route",
      });
    });
    if (!rows.length) return "";
    return (
      '<section class="hci-section hci-section--org-channels">' +
      "<h3>Company contact routes</h3>" +
      '<p class="hci-muted">Organization-wide channels — not personal direct lines.</p>' +
      '<ul class="hci-org-channel-list">' +
      rows
        .map(function (r) {
          var link =
            r.kind === "web"
              ? '<a class="hci-action-link hex-rms-ext-link" href="' +
                esc(r.value) +
                '" target="_blank" rel="noopener">' +
                esc(r.value) +
                "</a>"
              : r.kind === "email"
                ? '<a class="hci-action-link" href="mailto:' +
                  esc(r.value) +
                  '">' +
                  esc(r.value) +
                  "</a>"
                : r.kind === "phone"
                  ? phoneAnchorHtml(r.value)
                  : esc(r.value);
          return (
            "<li><span class=\"hci-org-channel-list__label\">" +
            esc(r.label) +
            "</span> " +
            link +
            (r.note ? ' <span class="hci-muted">(' + esc(r.note) + ")</span>" : "") +
            "</li>"
          );
        })
        .join("") +
      "</ul></section>"
    );
  }

  function renderResearchGaps(vm) {
    var items = vm.unresolvedItems || [];
    if (!items.length) return "";
    var id = "hci-gaps-" + Math.random().toString(36).slice(2, 8);
    return (
      '<section class="hci-section hci-section--gaps">' +
      '<button type="button" class="hci-provenance-toggle hci-gaps-toggle" data-hci-toggle="' +
      id +
      '" aria-expanded="false">Open items (' +
      items.length +
      ")</button>" +
      '<div id="' +
      id +
      '" hidden>' +
      '<ul class="hci-list">' +
      items
        .map(function (i) {
          return "<li>" + esc(i) + "</li>";
        })
        .join("") +
      "</ul></div></section>"
    );
  }

  function renderViewModel(vm, purposeId) {
    if (!vm) return renderEmpty();
    var hotelName = resolveDisplayHotelName(vm, vm.hotelId);
    var purpose = purposeId || vm.activeBusinessPurpose || "all";
    if (!Array.isArray(vm.purposeSections) || !vm.purposeSections.length) {
      vm.purposeSections = buildPurposeSectionsFromPeople(vm.people || []);
    }
    return (
      '<div class="hci-root" data-hci-state="ready" data-hci-cx="v3" data-hci-presentation="directory-table" data-hotel-id="' +
      esc(vm.hotelId || "") +
      '" data-hci-purpose="' +
      esc(purpose) +
      '">' +
      '<header class="hci-header hci-header--compact">' +
      "<div><h2>Contact Intelligence</h2>" +
      '<p class="hci-subtitle">' +
      esc(hotelName) +
      " · professional directory</p></div>" +
      renderPurposeFilter(purpose) +
      "</header>" +
      renderOwnershipSlim(vm) +
      renderPurposeSections(vm, purpose) +
      renderOrgChannels(vm) +
      renderResearchGaps(vm) +
      "</div>"
    );
  }

  function paintEnrichResult(slot, payload, btn) {
    if (!slot && btn) slot = findEnrichSlot(btn);
    if (!slot) return;
    slot.hidden = false;
    slot.removeAttribute("hidden");
    if (payload && payload.pending) {
      slot.innerHTML =
        '<div class="hci-note">' +
        esc(
          (payload && payload.userMessage) ||
            "Surfe enrichment is still in progress. Try again shortly — this is not a no-result."
        ) +
        "</div>";
      return;
    }
    if (!payload || payload.ok === false) {
      var err = String((payload && (payload.error || "")) || "");
      var msg;
      if (err === "surfe_unavailable_for_hotel_explorer" || err === "enrichment_not_configured") {
        msg =
          (payload && payload.userMessage) ||
          "Contact enrichment is not available in this environment.";
      } else if (err === "reveal_in_flight") {
        msg = "A lookup is already in progress for this person.";
      } else if (err === "INSUFFICIENT_CREDITS") {
        msg =
          (payload && payload.userMessage) ||
          "Surfe credits are insufficient for this lookup.";
      } else if (err === "AUTH" || err === "FORBIDDEN") {
        msg =
          (payload && payload.userMessage) ||
          "Surfe authorization failed for this environment.";
      } else if (err === "API route not found" || /route not found/i.test(err)) {
        msg =
          "Contact enrichment API route is unavailable on this server. Restart the integrated main tip and retry.";
      } else if (err === "surfe_no_result") {
        msg =
          "Surfe did not return additional professional contact details for this lookup.";
      } else if (err === "no_result" || err === "findymail_no_result") {
        msg =
          "No additional professional contact details were found for this lookup.";
      } else {
        msg =
          (payload && payload.userMessage) ||
          (payload && payload.message && !/CONTACT_ENRICHMENT|SURFE_API_KEY|stack/i.test(payload.message)
            ? payload.message
            : null) ||
          "No additional professional contact details found.";
      }
      slot.innerHTML = '<div class="hci-note hci-note--unresolved">' + esc(msg) + "</div>";
      return;
    }
    if (payload.cached) {
      var ch = payload.channels || {};
      var emails = (ch.emails || [])
        .map(function (c) {
          return c && (c.value || c.email);
        })
        .filter(Boolean);
      var phones = (ch.phones || [])
        .map(function (c) {
          return c && (c.value || c.phone);
        })
        .filter(Boolean);
      slot.innerHTML =
        '<div class="hci-enrich-result">' +
        '<div class="hci-note">Existing channels on file — no provider credit used.</div>' +
        (emails.length
          ? "<div><strong>Email</strong> " +
            emails
              .map(function (e) {
                return (
                  '<a class="hci-action-link" href="mailto:' + esc(e) + '">' + esc(e) + "</a>"
                );
              })
              .join(", ") +
            "</div>"
          : "") +
        (phones.length
          ? "<div><strong>Phone</strong> " +
            phones.map(function (ph) {
              return phoneAnchorHtml(ph);
            }).join(", ") +
            "</div>"
          : "") +
        "</div>";
      return;
    }
    var person = payload.person || {};
    var email =
      person.email ||
      person.workEmail ||
      (person.emails && person.emails[0] && (person.emails[0].email || person.emails[0].value)) ||
      null;
    var phone =
      person.phone ||
      person.mobile ||
      (person.phones && person.phones[0] && (person.phones[0].phone || person.phones[0].value)) ||
      null;
    if (!email && !phone) {
      slot.innerHTML =
        '<div class="hci-note hci-note--unresolved">Surfe did not return additional professional contact details for this lookup.</div>';
      return;
    }
    var personId =
      person.personId ||
      (btn && btn.getAttribute("data-person-id")) ||
      slot.getAttribute("data-person-id") ||
      "";
    if (personId) {
      _sessionSurfeByPerson[personId] = {
        email: email || null,
        phone: phone || null,
        at: Date.now(),
        mock: payload.mock === true,
      };
    }
    // Refresh Contact Details channel stack so session values are visible left of the button.
    var cell =
      (btn && btn.closest("[data-hci-contact-details-cell]")) ||
      (slot && slot.closest("[data-hci-contact-details-cell]"));
    if (cell && personId) {
      var seed = {
        personId: personId,
        name: (btn && btn.getAttribute("data-person-name")) || person.personName || "",
        organization:
          (btn && btn.getAttribute("data-organization")) || person.companyName || "",
        title: (btn && btn.getAttribute("data-job-title")) || person.jobTitle || "",
        linkedinUrl:
          (btn && btn.getAttribute("data-linkedin-url")) || person.linkedinUrl || "",
        organizationDomain: (btn && btn.getAttribute("data-company-domain")) || "",
        email: null,
        phone: null,
        emails: [],
        phones: [],
      };
      var hotelId = (btn && btn.getAttribute("data-hotel-id")) || "";
      cell.outerHTML = renderVerifiedChannelSummary(seed, hotelId, vmCapability);
      var liveBtn = findEnrichButton(personId, hotelId);
      var liveSlot = findEnrichSlot(liveBtn);
      // Session Surfe results are ephemeral — durable completeness stays Airtable-gated.
      if (liveBtn) {
        var durableComplete = liveBtn.getAttribute("data-dimmed-complete") === "1";
        if (durableComplete) {
          liveBtn.disabled = true;
          setEnrichButtonLabel(liveBtn, "complete");
          liveBtn.classList.add("hci-btn--enrich-dimmed");
        } else {
          liveBtn.disabled = false;
          setEnrichButtonLabel(liveBtn, "idle");
          liveBtn.classList.remove("hci-btn--enrich-dimmed");
        }
      }
      if (liveSlot) {
        liveSlot.hidden = true;
        liveSlot.setAttribute("hidden", "");
        liveSlot.setAttribute("data-person-id", personId);
        liveSlot.innerHTML = "";
      }
      return;
    }
    var attribution =
      payload.mock === true
        ? "Mock Surfe provider (no-cost) · Surfe · Session Only · not persisted"
        : (payload.providerAttribution || "Surfe · Session Only") +
          " · not persisted to Airtable";
    slot.innerHTML =
      '<div class="hci-enrich-result" data-verification="PROVIDER_RETURNED_UNREVIEWED" data-surfe-session="1" data-persist="forbidden">' +
      '<div class="hci-note">' +
      esc(attribution) +
      (payload.surfeCalls != null
        ? " · credits used: " + esc(String(payload.surfeCalls))
        : "") +
      "</div>" +
      (email
        ? '<div class="hci-channel-row"><span class="hci-channel-label">Surfe session email</span> ' +
          '<a class="hci-action-link" href="mailto:' +
          esc(email) +
          '">' +
          esc(email) +
          "</a></div>"
        : "") +
      (phone
        ? '<div class="hci-channel-row"><span class="hci-channel-label">Surfe session phone</span> ' +
          phoneAnchorHtml(phone) +
          "</div>"
        : "") +
      '<p class="hci-muted">Surfe · Session Only. Results are ephemeral — never saved as public research, never written to Airtable from this action, and disappear after reload. Canonical saved channels are unchanged.</p>' +
      "</div>";
  }

  function applyHybridRowUpdate(btn, body) {
    var cell = btn && btn.closest("[data-hci-contact-details-cell]");
    if (!cell || !body || !(body.email || body.phone)) return null;
    var person = {
      personId: btn.getAttribute("data-person-id") || "",
      name: btn.getAttribute("data-person-name") || "",
      organization: btn.getAttribute("data-organization") || "",
      title: btn.getAttribute("data-job-title") || "",
      linkedinUrl: btn.getAttribute("data-linkedin-url") || "",
      organizationDomain: btn.getAttribute("data-company-domain") || "",
      channels: body.channels || [],
      email: body.email || null,
      phone: body.phone || null,
    };
    if (body.email) {
      person.emails = [{ value: body.email, kind: "email" }];
      person.emailContacts = person.emails;
    }
    if (body.phone) {
      person.phones = [{ value: body.phone, kind: "phone" }];
      person.phoneContacts = person.phones;
    }
    if (Array.isArray(body.channels)) {
      person.channels = body.channels.map(function (c) {
        return {
          kind: c.channelType || c.kind,
          value: c.channelValue || c.value,
          provider: c.provider,
        };
      });
    }
    var hotelId = btn.getAttribute("data-hotel-id") || "";
    var parent = cell.parentElement;
    var nextHtml = renderVerifiedChannelSummary(person, hotelId, vmCapability);
    cell.outerHTML = nextHtml;
    if (!parent) return null;
    return parent.querySelector(
      '[data-hci-contact-details-cell] [data-hci-enrich][data-person-id="' +
        String(person.personId).replace(/"/g, '\\"') +
        '"]',
    );
  }

  function paintSurfeFallbackOffer() {
    // CONTACT INTELLIGENCE V4 — customer UI no longer offers Try Surfe.
    // Find Email & Phone calls /reveal (Surfe) in one click.
    return;
  }

  function bindEnrich(root) {
    if (!root) return;
    root.addEventListener("click", function (ev) {
      // Ignore legacy Try Surfe nodes if any residual markup appears.
      if (ev.target.closest("[data-hci-surfe-fallback]")) {
        ev.preventDefault();
        return;
      }
      var btn = ev.target.closest("[data-hci-enrich]");
      if (!btn || btn.disabled) return;
      var hotelId = btn.getAttribute("data-hotel-id") || "";
      var personId = btn.getAttribute("data-person-id") || "";
      var personName = btn.getAttribute("data-person-name") || "";
      var organization = btn.getAttribute("data-organization") || "";
      var key = hotelId + "::" + personId;
      if (_inflightReveal[key]) return;
      var slot = findEnrichSlot(btn);
      var card = btn.closest(".hci-person, .hci-dir-row, tr");
      btn.disabled = true;
      btn.setAttribute("aria-busy", "true");
      setEnrichButtonLabel(btn, "finding");
      showEnrichToast("Finding Contact Details…");
      if (slot) {
        slot.hidden = true;
        slot.setAttribute("hidden", "");
        slot.innerHTML = "";
      }
      if (card) card.setAttribute("data-enriching", "1");
      _inflightReveal[key] = true;

      // V4: one-click Surfe only — never Findymail / enrich-hybrid on customer click.
      var url =
        "/api/contact-intelligence/hotels/" +
        encodeURIComponent(hotelId) +
        "/people/" +
        encodeURIComponent(personId) +
        "/reveal";
      fetch(url, {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({
          personName: personName,
          organization: organization,
          jobTitle: btn.getAttribute("data-job-title") || "",
          linkedinUrl: btn.getAttribute("data-linkedin-url") || "",
          companyDomain: btn.getAttribute("data-company-domain") || "",
          channel: btn.getAttribute("data-enrich-channel") || "both",
          persist: false,
          allowPaidFindymail: false,
        }),
      })
        .then(function (r) {
          return r.json().then(
            function (j) {
              return { status: r.status, body: j };
            },
            function () {
              return {
                status: r.status,
                body: {
                  ok: false,
                  error: r.status === 404 ? "API route not found" : "http_" + r.status,
                  userMessage:
                    r.status === 404
                      ? "Contact enrichment API route is unavailable on this server."
                      : "Lookup could not be completed. Try again.",
                },
              };
            },
          );
        })
        .then(function (pack) {
          var body = pack.body || { ok: false, error: "http_" + pack.status };
          if (body && body.person && !body.person.personId) body.person.personId = personId;
          if (body && body.missingChannelState && btn && btn.isConnected) {
            btn.setAttribute(
              "data-enrich-channel",
              body.missingChannelState.requestChannel || "both",
            );
            btn.setAttribute(
              "data-needs-email",
              body.missingChannelState.needsEmail ? "1" : "0",
            );
            btn.setAttribute(
              "data-needs-phone",
              body.missingChannelState.needsPhone ? "1" : "0",
            );
          }

          btn = findEnrichButton(personId, hotelId) || btn;
          slot = findEnrichSlot(btn) || slot;
          paintEnrichResult(slot, body, btn);

          var hasSession =
            body &&
            body.ok &&
            !body.pending &&
            (body.person || body.cached) &&
            ((body.person && body.person.email) ||
              (body.person && body.person.phone) ||
              body.email ||
              body.phone);
          if (hasSession) {
            // Durable completeness remains Airtable-driven — keep button available
            // when durable channels are still missing (session results do not dim).
            if (btn && btn.isConnected) {
              var durableComplete = btn.getAttribute("data-dimmed-complete") === "1";
              if (durableComplete) {
                setEnrichButtonLabel(btn, "complete");
                btn.disabled = true;
                btn.classList.add("hci-btn--enrich-dimmed");
              } else {
                btn.disabled = false;
                setEnrichButtonLabel(btn, "idle");
              }
            }
            return;
          }
          if (body && body.ok && body.pending) {
            if (btn && btn.isConnected) {
              btn.disabled = false;
              setEnrichButtonLabel(btn, "idle");
            }
            return;
          }
          // No result / error — leave available for intentional retry.
          if (btn && btn.isConnected) {
            btn.disabled = false;
            setEnrichButtonLabel(btn, "idle");
          }
        })
        .catch(function (err) {
          btn = findEnrichButton(personId, hotelId) || btn;
          slot = findEnrichSlot(btn) || slot;
          paintEnrichResult(
            slot,
            {
              ok: false,
              error: "network_error",
              userMessage: "Lookup could not be completed. Try again.",
              message: String((err && err.message) || err),
            },
            btn,
          );
          if (btn && btn.isConnected) {
            btn.disabled = false;
            setEnrichButtonLabel(btn, "idle");
          }
        })
        .finally(function () {
          delete _inflightReveal[key];
          hideEnrichToast();
          btn = findEnrichButton(personId, hotelId) || btn;
          if (btn && btn.isConnected) btn.removeAttribute("aria-busy");
          if (card && card.isConnected) card.removeAttribute("data-enriching");
        });
    });
  }

  function bindPurposeFilter(root, vm) {
    if (!root || !vm) return;
    var select = root.querySelector("[data-hci-purpose-filter]");
    if (!select || select.getAttribute("data-hci-purpose-bound") === "1") return;
    select.setAttribute("data-hci-purpose-bound", "1");
    select.addEventListener("change", function () {
      var next = select.value || "all";
      vm.activeBusinessPurpose = next;
      root.innerHTML = renderViewModel(vm, next);
      bindToggles(root);
      if (
        Array.isArray(vm.contactEvidence) &&
        window.HotelIntelligenceWorkspace &&
        typeof window.HotelIntelligenceWorkspace.applyContactEvidenceCitations === "function"
      ) {
        window.HotelIntelligenceWorkspace.applyContactEvidenceCitations(vm.contactEvidence);
      }
    });
  }

  function bindToggles(root) {
    if (!root) return;
    var vm = root._hciViewModel || null;
    if (global.DealalitySortableTable && typeof global.DealalitySortableTable.bind === "function") {
      global.DealalitySortableTable.bind(root);
    }
    if (root.getAttribute("data-hci-bound") === "1") {
      bindEnrich(root);
      bindPurposeFilter(root, vm);
      return;
    }
    root.setAttribute("data-hci-bound", "1");
    root.addEventListener("click", function (ev) {
      var btn = ev.target && ev.target.closest && ev.target.closest("[data-hci-toggle]");
      if (!btn || !root.contains(btn)) return;
      ev.preventDefault();
      var id = btn.getAttribute("data-hci-toggle");
      var el = null;
      if (id) {
        try {
          el = root.querySelector("#" + String(id).replace(/([^a-zA-Z0-9_-])/g, "\\$1"));
        } catch (e) {
          el = null;
        }
        if (!el) el = document.getElementById(id);
      }
      if (!el && btn.parentElement) {
        el = btn.parentElement.querySelector(".hci-person__details, [id^='hci-gaps-']");
      }
      if (!el) return;
      var open = el.hasAttribute("hidden") || el.hidden === true;
      if (open) {
        el.hidden = false;
        el.removeAttribute("hidden");
      } else {
        el.hidden = true;
        el.setAttribute("hidden", "");
      }
      btn.setAttribute("aria-expanded", open ? "true" : "false");
    });
    bindEnrich(root);
    bindPurposeFilter(root, vm);
  }

  function renderHpcPropertyFallback(hotelId, contacts, hotelName, ownershipSummary) {
    var phone = contacts && contacts.property_phone;
    var website = contacts && contacts.website;
    var source =
      (contacts && contacts.property_fallback_source) || "Hotel Property Census";
    var phoneHtml = phone
      ? phoneAnchorHtml(phone)
      : '<span class="hci-muted">Not published</span>';
    var websiteHtml = website
      ? '<a class="hci-action-link hex-rms-ext-link" href="' +
        esc(website) +
        '" target="_blank" rel="noopener">' +
        esc(website) +
        "</a>"
      : '<span class="hci-muted">Not published</span>';
    var summaryHtml = "";
    if (ownershipSummary) {
      summaryHtml =
        '<p class="hci-muted">' +
        esc(
          [
            ownershipSummary.organization ? "Org: " + ownershipSummary.organization : "",
            ownershipSummary.operator ? "Operator: " + ownershipSummary.operator : "",
          ]
            .filter(Boolean)
            .join(" · ")
        ) +
        "</p>";
    }
    return (
      '<div class="hci-root" data-hci-state="hpc-fallback">' +
      '<header class="hci-header hci-header--compact"><div><h2>Contact Intelligence</h2>' +
      '<p class="hci-subtitle">' +
      esc(hotelName || hotelId) +
      "</p></div></header>" +
      summaryHtml +
      '<section class="hci-section"><h3>Property contacts</h3>' +
      '<p class="hci-muted">Read-only HPC fallback from ' +
      esc(source) +
      ". Named decision-makers are not staged for this hotel.</p>" +
      "<div>Phone: " +
      phoneHtml +
      "</div><div>Website: " +
      websiteHtml +
      "</div></section></div>"
    );
  }

  function loadHpcPropertyFallback(hotelId) {
    hotelId = String(hotelId || "").trim();
    if (!hotelId) return Promise.resolve(null);
    return fetch(
      "/api/golden-demo/ownership/hotel/" + encodeURIComponent(hotelId),
      { credentials: "same-origin" }
    )
      .then(function (r) {
        if (!r.ok) throw new Error("http_" + r.status);
        return r.json();
      })
      .then(function (json) {
        var contacts =
          (json && json.report && json.report.corporate_contacts) || null;
        if (!contacts || (!contacts.property_phone && !contacts.website)) {
          return null;
        }
        var control =
          (json.report && json.report.ownership_and_control) || {};
        return {
          hotelId: hotelId,
          hotelName:
            (json.hotel && (json.hotel.name || json.hotel.display_name)) || hotelId,
          contacts: contacts,
          ownershipSummary: {
            organization:
              (json.organization &&
                (json.organization.display_name || json.organization.legal_name)) ||
              null,
            operator: (control.operator && control.operator.name) || null,
            economic_owner_verified: Boolean(
              (control.economic_owner && control.economic_owner.known) ||
                (control.economic_owner_or_group &&
                  control.economic_owner_or_group.known)
            ),
            source: "HI_AIRTABLE_GRAPH",
          },
        };
      })
      .catch(function () {
        return null;
      });
  }

  function channelKindIsLinkedIn(kind) {
    return /linkedin/i.test(String(kind || ""));
  }

  function channelKindIsEmail(kind) {
    return /email/i.test(String(kind || ""));
  }

  function channelKindIsPhone(kind) {
    return /phone|mobile|tel/i.test(String(kind || ""));
  }

  function channelKindIsWebsite(kind) {
    return /website|web|url|contact_page/i.test(String(kind || ""));
  }

  function classifyContactFromPackage(raw) {
    var role = String(
      (raw.why_relevant || raw.whyRelevant || "") +
        " " +
        (raw.function_role || raw.title || "") +
        " " +
        (raw.hotel_commercial_responsibility || "")
    );
    var upper = role.toUpperCase();
    if (/VERIFIED_PROPERTY_LEADERSHIP|PROPERTY LEADERSHIP|GENERAL MANAGER|\bGM\b/.test(upper)) {
      return {
        contactClass: "HOTEL_LEADERSHIP",
        contactClassLabel: "Hotel leadership",
      };
    }
    if (
      /VERIFIED_PROPERTY_COMMERCIAL|DIRECTOR OF SALES|AREA SALES|DOSM|REVENUE MANAGEMENT/.test(upper)
    ) {
      return {
        contactClass: "HOTEL_LEADERSHIP",
        contactClassLabel: "Sales & commercial leadership",
      };
    }
    if (/OWNER_DECISION|OWNERSHIP\s*\/\s*DECISION|PRESIDENT|CEO/.test(upper)) {
      return {
        contactClass: "OWNER_DECISION_AUTHORITY",
        contactClassLabel: "Ownership / decision authority",
      };
    }
    if (/RELEVANT_COMMERCIAL|ASSET MANAGEMENT|VP,?\s*ASSET|DIRECTOR,?\s*ASSET/.test(upper)) {
      return {
        contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
        contactClassLabel: "Asset management",
      };
    }
    return {
      contactClass: "VERIFIED_ORGANIZATION_CONTACT",
      contactClassLabel: "Organization contact",
    };
  }

  function mapPackagePersonToUi(raw) {
    if (!raw || typeof raw !== "object") return null;
    var channels = Array.isArray(raw.channels) ? raw.channels : [];
    var linkedin = null;
    var emails = [];
    var phones = [];
    channels.forEach(function (ch) {
      if (!ch) return;
      var kind = ch.kind || ch.type || "";
      var val = ch.value || ch.email || ch.phone || ch.url || "";
      if (!present(val)) return;
      if (channelKindIsLinkedIn(kind) || /linkedin\.com\/in\//i.test(String(val))) {
        linkedin = String(val).trim();
      } else if (channelKindIsEmail(kind)) {
        emails.push({ value: val, type: kind, verification_status: ch.verification_status || null });
      } else if (channelKindIsPhone(kind)) {
        phones.push({ value: val, type: kind, verification_status: ch.verification_status || null });
      }
    });
    // Recover LinkedIn from Notes when channels table is quarantined
    if (!linkedin && present(raw.notes) && /linkedin\.com\/in\//i.test(String(raw.notes))) {
      var m = String(raw.notes).match(/https?:\/\/[^\s)]*linkedin\.com\/in\/[^\s)|]+/i);
      if (m) linkedin = m[0].replace(/[.,;]+$/, "");
    }
    var name = raw.display_name || raw.name || raw.fullName || null;
    if (!present(name)) return null;
    var classMeta = classifyContactFromPackage(raw);
    var responsibilityBlob = String(
      (raw.hotel_commercial_responsibility || "") +
        " " +
        (raw.why_relevant || raw.whyRelevant || "") +
        " " +
        (raw.title || "")
    );
    var propRel = String(raw.property_relevance || "").toUpperCase();
    var propertyResponsibility;
    if (
      /VERIFIED_PROPERTY|PROPERTY LEADERSHIP|GENERAL MANAGER|DIRECTOR OF SALES/i.test(
        responsibilityBlob
      ) ||
      propRel === "HOTEL" ||
      propRel === "PROPERTY_DIRECT"
    ) {
      propertyResponsibility = "CONFIRMED";
    } else if (propRel === "OWNER_ORG") {
      propertyResponsibility = "INFERRED";
    } else {
      propertyResponsibility = "UNKNOWN";
    }
    var mapped = {
      personId: raw.person_id || raw.personId || null,
      name: name,
      title: raw.title || null,
      organization: raw.organization_name || raw.organization || null,
      organizationName: raw.organization_name || raw.organization || null,
      whyRelevant: raw.why_relevant || raw.whyRelevant || null,
      relationshipToHotel: raw.why_relevant || raw.whyRelevant || null,
      hotelResponsibility: raw.hotel_commercial_responsibility || null,
      contactClass: classMeta.contactClass,
      contactClassLabel: classMeta.contactClassLabel,
      commercialRole: classMeta.contactClassLabel,
      emailContacts: emails,
      phoneContacts: phones,
      profileUrls: linkedin
        ? [{ type: "LINKEDIN", url: linkedin, verified: true }]
        : [],
      linkedinUrl: linkedin,
      organizationWebsite: null,
      propertyResponsibility: propertyResponsibility,
      propertyResponsibilityLabel: humanResponsibility({
        propertyResponsibility: propertyResponsibility,
      }),
      confidence:
        (raw.provenance && (raw.provenance.confidence || raw.provenance.verification_state)) ||
        raw.confidence ||
        "MEDIUM",
      contactability: linkedin || emails.length || phones.length ? "PROFILE_ONLY" : "ORG_FALLBACK",
      displayState: "PARTIAL",
      sources: raw.provenance && raw.provenance.source ? [raw.provenance.source] : ["HI_ORG_CONTACTS"],
      airtableId: raw.provenance && raw.provenance.airtable_id ? raw.provenance.airtable_id : null,
      lastVerified: raw.role_check_date || raw.contact_source_date || null,
    };
    mapped.purposeSections = assignPurposeSectionsClient(mapped);
    return mapped;
  }

  function mapContactPackageToViewModel(json, hotelId) {
    var pkg = (json && json.package) || {};
    var owner = (json && json.owner_anchor) || {};
    var people = (pkg.people || []).map(mapPackagePersonToUi).filter(Boolean);
    var purposeSections = buildPurposeSectionsFromPeople(people);
    var orgChannels = (pkg.organization_contact_route && pkg.organization_contact_route.channels) || [];
    var orgWebsite = null;
    var orgPhones = [];
    var orgEmails = [];
    orgChannels.forEach(function (ch) {
      if (!ch) return;
      var kind = ch.kind || ch.type || "";
      var val = ch.value || "";
      if (!present(val)) return;
      if (channelKindIsWebsite(kind) || /^https?:/i.test(String(val))) orgWebsite = val;
      else if (channelKindIsPhone(kind)) orgPhones.push({ type: kind, value: val });
      else if (channelKindIsEmail(kind)) orgEmails.push({ type: kind, value: val });
    });
    var ownerName =
      owner.owner_display_name ||
      pkg.owner_display_name ||
      (pkg.organization_contact_route && pkg.organization_contact_route.owner_display_name) ||
      null;
    var hotelName = pkg.hotel_name || "";
    if (!present(hotelName) || /^rec[a-zA-Z0-9]{14}$/i.test(String(hotelName))) {
      hotelName =
        (pkg.hotel_contact && (pkg.hotel_contact.name || pkg.hotel_contact.display_name)) ||
        (String(hotelId) === "recLuxvwwxID7U2B8" ? "Bethesda Marriott" : "");
    }
    return {
      version: "contact-intelligence-viewmodel-v3",
      hotelId: pkg.hotel_id || hotelId,
      hotelName: hotelName,
      activeBusinessPurpose: "all",
      ownershipSummary: {
        hotel: { name: hotelName || "Hotel contacts", role: "HOTEL" },
        sponsor: ownerName
          ? {
              name: ownerName,
              role: "OWNER_SPONSOR_GROUP",
              label: "Ownership organization",
              economicOwnershipCertified: false,
            }
          : null,
        operator: null,
        brand: null,
        propco: null,
      },
      qualitySummary: {
        ownerOrgStatus: ownerName ? "ASSOCIATED" : "UNRESOLVED",
        primaryDecisionMakers: people.length,
        verifiedEmails: 0,
        verifiedOrPublicPhones: 0,
        fallbackContactPaths: orgWebsite || orgPhones.length || orgEmails.length ? 1 : 0,
        lastReviewed: null,
      },
      contactableOwnerOrg: ownerName
        ? {
            organizationId: owner.owner_entity_id || pkg.owner_entity_id || null,
            name: ownerName,
            website: orgWebsite,
            phones: orgPhones,
            emails: orgEmails,
            verification: "HI_AIRTABLE_GRAPH",
            confidence: "MEDIUM",
            displayState: people.length || orgWebsite ? "AVAILABLE" : "PARTIAL",
          }
        : null,
      people: people,
      purposeSections: purposeSections,
      recommendedContacts: [],
      otherContacts: people,
      recommendation: null,
      organizations: [],
      researchWorkflow: false,
      hiGraphOverlay: json && json.hi_graph_overlay ? json.hi_graph_overlay : null,
      _rawPackage: pkg,
    };
  }

  function loadViewModel(hotelId) {
    hotelId = String(hotelId || "").trim();
    if (!hotelId) return Promise.resolve(null);
    var showcase = SHOWCASE_PATHS[hotelId];
    var tries = [];
    if (showcase) tries.push(showcase);
    tries.push(
      "/api/contact-intelligence/hotels/" + encodeURIComponent(hotelId) + "?audience=internal"
    );

    function next(i) {
      if (i >= tries.length) return Promise.resolve(null);
      return fetch(tries[i], { credentials: "same-origin" })
        .then(function (r) {
          if (!r.ok) throw new Error("http_" + r.status);
          return r.json();
        })
        .then(function (json) {
          if (json && json.version && json.ownershipSummary) return json;
          if (json && json.success && json.package) {
            return mapContactPackageToViewModel(json, hotelId);
          }
          throw new Error("unsupported_shape");
        })
        .catch(function () {
          return next(i + 1);
        });
    }
    return next(0);
  }

  function loadEnrichmentCapability() {
    return fetch("/api/contact-intelligence/enrichment-capability", {
      credentials: "same-origin",
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        var mockEnabled = Boolean(j && (j.mockEnabled || j.dryRunProvider));
        var surfeEnabled = Boolean(j && j.surfeEnabled) || mockEnabled;
        return {
          surfeEnabled: surfeEnabled,
          mockEnabled: mockEnabled,
          reason: (j && j.reason) || null,
          userReason:
            (j && j.userReason) ||
            (!surfeEnabled
              ? "Contact enrichment is not configured in this environment."
              : null),
          hasApiKey: Boolean(j && j.hasApiKey),
        };
      })
      .catch(function () {
        return {
          surfeEnabled: false,
          mockEnabled: false,
          reason: "capability_unavailable",
          userReason: "Contact enrichment is not configured in this environment.",
          hasApiKey: false,
        };
      });
  }

  function patchPersonEnrichAction(p, capability) {
    if (!p) return p;
    var meta = resolveEnrichMeta(p, capability);
    return Object.assign({}, p, {
      enrichAction: {
        label: meta.label || FIND_LABEL,
        enabled: meta.enabled === true,
        reason: meta.reason || null,
        channel: meta.channel || null,
        missing: meta.missing || [],
      },
    });
  }

  function mount(host, hotelId, opts) {
    if (!host) return Promise.resolve();
    opts = opts || {};
    hotelId = String(hotelId || "").trim();
    if (!hotelId) {
      host.innerHTML = renderEmpty(
        "Hotel identity could not be resolved. Open a property from Opportunity Radar or use a link with a canonical hotel id.",
      );
      return Promise.resolve();
    }
    // Surfe session results must not survive hotel remount / reload.
    _sessionSurfeByPerson = Object.create(null);
    host.innerHTML = renderLoading();
    return Promise.all([loadViewModel(hotelId), loadEnrichmentCapability()])
      .then(function (pack) {
        var vm = pack[0];
        vmCapability = pack[1] || null;
        if (
          vm &&
          present(opts.hotelName) &&
          (!present(vm.hotelName) || /^rec[a-zA-Z0-9]{14}$/.test(String(vm.hotelName)))
        ) {
          vm.hotelName = opts.hotelName;
          if (vm.ownershipSummary && vm.ownershipSummary.hotel) {
            vm.ownershipSummary.hotel.name = opts.hotelName;
          }
        }
        var hasPeople = vm && Array.isArray(vm.people) && vm.people.length > 0;
        var hasOwner =
          vm &&
          vm.ownershipSummary &&
          (vm.ownershipSummary.sponsor ||
            vm.ownershipSummary.operator ||
            vm.contactableOwnerOrg);
        if (vm && vm.ownershipSummary && (hasPeople || hasOwner)) {
          if (Array.isArray(vm.people)) {
            vm.people = vm.people.map(function (p) {
              return patchPersonEnrichAction(p, vmCapability);
            });
          }
          if (Array.isArray(vm.recommendedContacts)) {
            vm.recommendedContacts = vm.recommendedContacts.map(function (p) {
              return patchPersonEnrichAction(p, vmCapability);
            });
          }
          if (Array.isArray(vm.otherContacts)) {
            vm.otherContacts = vm.otherContacts.map(function (p) {
              return patchPersonEnrichAction(p, vmCapability);
            });
          }
          if (vm.recommendation && vm.recommendation.person) {
            vm.recommendation.person = patchPersonEnrichAction(
              vm.recommendation.person,
              vmCapability
            );
          }
          if (vm.recommendation && Array.isArray(vm.recommendation.alternatives)) {
            vm.recommendation.alternatives = vm.recommendation.alternatives.map(function (p) {
              return patchPersonEnrichAction(p, vmCapability);
            });
          }
          if (!Array.isArray(vm.purposeSections) || !vm.purposeSections.length) {
            vm.purposeSections = buildPurposeSectionsFromPeople(vm.people || []);
          }
          host.innerHTML = renderViewModel(vm, vm.activeBusinessPurpose || "all");
          host._hciViewModel = vm;
          var painted =
            typeof host.querySelector === "function" ? host.querySelector(".hci-root") : null;
          if (painted) painted._hciViewModel = vm;
          bindToggles(host);
          if (
            Array.isArray(vm.contactEvidence) &&
            window.HotelIntelligenceWorkspace &&
            typeof window.HotelIntelligenceWorkspace.applyContactEvidenceCitations === "function"
          ) {
            window.HotelIntelligenceWorkspace.applyContactEvidenceCitations(vm.contactEvidence);
          }
          return;
        }
        return loadHpcPropertyFallback(hotelId).then(function (fallback) {
          if (fallback) {
            host.innerHTML = renderHpcPropertyFallback(
              hotelId,
              fallback.contacts,
              fallback.hotelName,
              fallback.ownershipSummary
            );
            return;
          }
          host.innerHTML = renderEmpty(
            "Contact Intelligence staging is not available for this hotel yet."
          );
        });
      })
      .catch(function (err) {
        console.warn("[HotelContactIntelligence] mount failed", err);
        host.innerHTML = renderEmpty(
          "Contact Intelligence is temporarily unavailable. Please try again."
        );
      });
  }

  global.HotelContactIntelligence = {
    renderLoading: renderLoading,
    renderEmpty: renderEmpty,
    renderHpcPropertyFallback: renderHpcPropertyFallback,
    renderViewModel: renderViewModel,
    renderPerson: renderPerson,
    mount: mount,
    loadViewModel: loadViewModel,
    loadHpcPropertyFallback: loadHpcPropertyFallback,
    FIND_LABEL: FIND_LABEL,
  };
})(typeof window !== "undefined" ? window : globalThis);
