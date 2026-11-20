/**
 * Hotel Explorer — Contact Intelligence tab (generic shared renderer).
 * Compact directory of ownership / decision-maker contacts.
 * Surfe "Find Email & Phone" is user-initiated only (never on mount/GET).
 */
(function (global) {
  "use strict";

  var SHOWCASE_PATHS = {
    recUNycnMwOVFX0hc: "/data/hotel-contact-intelligence/kgpv-recUNycnMwOVFX0hc.json",
  };

  var FIND_LABEL = "Find Email & Phone";
  var LABELS = {
    directEmail: "Direct Business Email",
    directPhone: "Direct Business Phone",
    orgRoute: "Organizational Contact Route",
    findEmail: "Find Email",
    findPhone: "Find Phone",
    findBoth: "Find Email & Phone",
  };
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

  function present(v) {
    if (v == null) return false;
    var s = String(v).trim();
    return s !== "" && s !== "—" && s !== "-" && s.toLowerCase() !== "n/a";
  }

  function humanResponsibility(p) {
    var raw = String(p.propertyResponsibilityLabel || p.propertyResponsibility || "").trim();
    var key = String(p.propertyResponsibility || "").toUpperCase();
    if (key === "CONFIRMED") return "Confirmed for this hotel";
    if (key === "INFERRED") return "Inferred · not property-confirmed";
    if (key === "UNKNOWN" || /not confirmed|unverified|unknown/i.test(raw))
      return "Unverified for this hotel";
    return present(raw) ? raw : "Unverified for this hotel";
  }

  function responsibilityClass(p) {
    var key = String(p.propertyResponsibility || "").toUpperCase();
    if (key === "CONFIRMED") return "hci-chip hci-chip--ok";
    if (key === "INFERRED") return "hci-chip hci-chip--inferred";
    return "hci-chip hci-chip--gap";
  }

  function linkedInUrl(p) {
    if (present(p.linkedinUrl)) return String(p.linkedinUrl).trim();
    var hit = (p.profileUrls || []).find(function (pr) {
      return pr && present(pr.url) && /linkedin/i.test(String(pr.type || pr.url || ""));
    });
    return hit ? String(hit.url).trim() : "";
  }

  function companyUrl(p) {
    if (present(p.organizationWebsite)) return String(p.organizationWebsite).trim();
    if (present(p.companyWebsite)) return String(p.companyWebsite).trim();
    if (present(p.organizationDomain)) {
      var d = String(p.organizationDomain).replace(/^https?:\/\//i, "").replace(/\/$/, "");
      if (d) return "https://" + d + "/";
    }
    return "";
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
    return (p.emailContacts || p.emails || []).filter(function (c) {
      return (
        c &&
        present(c.value || c.email) &&
        !isCorporateMislabel(c) &&
        !isEphemeralOrSurfe(c)
      );
    });
  }

  function personPhones(p) {
    return (p.phoneContacts || p.phones || []).filter(function (c) {
      return c && present(c.value || c.phone) && !isCorporateMislabel(c) && !isEphemeralOrSurfe(c);
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
    // Person-attached org-mislabelled channels only. Do NOT copy
    // contactableOwnerOrg / bestContactPaths onto every person card —
    // those belong in Organization Contact Channels once.
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
    void vm;
    return out;
  }

  function resolveEnrichMeta(p, capability) {
    var hasName = present(p.name) && String(p.name).trim().split(/\s+/).length >= 2;
    var hasOrg = present(p.organization) || present(p.organizationName);
    var hasEmail = personEmails(p).length > 0;
    var hasPhone = personPhones(p).length > 0;
    var action = p.enrichAction || {};
    var providerReady =
      capability &&
      (capability.surfeEnabled === true || capability.mockEnabled === true);
    if (hasEmail && hasPhone) {
      return { label: null, enabled: false, channel: null, missing: [] };
    }
    if (!hasName || !hasOrg) {
      return {
        label: LABELS.findBoth,
        enabled: false,
        channel: null,
        missing: [],
        reason: "Enrichment needs a full name and organization.",
      };
    }
    var channel = "both";
    var label = LABELS.findBoth;
    var missing = [];
    if (!hasEmail && hasPhone) {
      channel = "email";
      label = LABELS.findEmail;
      missing = ["professional_email"];
    } else if (hasEmail && !hasPhone) {
      channel = "phone";
      label = LABELS.findPhone;
      missing = ["direct_business_phone"];
    } else {
      missing = ["professional_email", "direct_business_phone"];
    }
    if (present(action.label) && /find email|find phone/i.test(action.label)) {
      label = action.label;
    }
    var enabled =
      action.enabled === true ||
      (providerReady && action.enabled !== false);
    return {
      label: label,
      enabled: enabled,
      channel: channel,
      missing: missing,
      reason:
        (capability && capability.userReason) ||
        action.reason ||
        (!providerReady
          ? "Contact enrichment is not configured in this environment."
          : null),
    };
  }

  function renderLoading() {
    var body =
      window.HeLoadingUx && window.HeLoadingUx.toastHtml
        ? window.HeLoadingUx.toastHtml("Loading Contacts…")
        : window.HeAsyncSection && window.HeAsyncSection.waveCompact
          ? window.HeAsyncSection.waveCompact("Loading Contacts…")
          : window.DealalityWaveLoader && window.DealalityWaveLoader.toastHtml
            ? window.DealalityWaveLoader.toastHtml("Loading Contacts…")
            : window.DealalityWaveLoader && window.DealalityWaveLoader.compactHtml
              ? '<div class="dealality-loading-toast-host">' +
                window.DealalityWaveLoader.compactHtml("Loading Contacts…", {
                  progress: false,
                }) +
                "</div>"
              : '<div class="dealality-loading-toast-host" role="status">Loading Contacts…</div>';
    return (
      '<div class="hci-root hci-root--loading" data-hci-state="loading">' +
      '<div class="hci-loading" data-hci-wave-loading="1" data-he-loading-toast="1">' +
      body +
      "</div></div>"
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

  function channelValue(ch) {
    return String((ch && (ch.value || ch.email || ch.phone)) || "").trim();
  }

  function renderCompactChannel(ch, kind) {
    var val = channelValue(ch);
    if (!val) return "";
    var href =
      kind === "email"
        ? "mailto:" + val
        : kind === "phone"
          ? "tel:" + val.replace(/\s+/g, "")
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
    var parts = [];

    if (li) {
      parts.push(
        '<a class="hci-action-link hex-rms-ext-link" href="' +
          esc(li) +
          '" target="_blank" rel="noopener">LinkedIn</a>'
      );
    }
    if (company) {
      parts.push(
        '<a class="hci-action-link hex-rms-ext-link" href="' +
          esc(company) +
          '" target="_blank" rel="noopener">Company Profile</a>'
      );
    }

    parts.push(renderEnrichAction(p, hotelId, capability, { inline: true }));

    return '<div class="hci-actions">' + parts.filter(Boolean).join("") + "</div>";
  }

  function renderEnrichAction(p, hotelId, capability, opts) {
    opts = opts || {};
    var meta = resolveEnrichMeta(p, capability);
    if (!meta.label) {
      return opts.inline
        ? ""
        : '<p class="hci-muted">Verified public direct channels already on file.</p>';
    }
    if (!meta.enabled) {
      // Never show a nonfunctional enabled-looking button.
      return (
        '<p class="hci-muted hci-enrich-unavailable" title="' +
        esc(meta.reason || "") +
        '">' +
        esc(meta.reason || "Contact enrichment is unavailable in this environment.") +
        "</p>"
      );
    }
    var li = linkedInUrl(p);
    return (
      '<div class="hci-enrich">' +
      '<button type="button" class="hci-btn hci-btn--enrich" data-hci-enrich="1" data-person-id="' +
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
      esc(meta.label) +
      '">' +
      esc(meta.label) +
      "</button>" +
      '<div class="hci-enrich__result" hidden></div></div>'
    );
  }

  function renderPerson(p, hotelId, capability, opts) {
    opts = opts || {};
    var featured = opts.featured === true;
    var compact = opts.compact === true || !featured;
    var detailsId =
      "hci-person-" +
      String(p.personId || Math.random().toString(36).slice(2)).replace(/[^a-zA-Z0-9_-]/g, "_");
    var relevance = p.whyRelevant || p.recommendationRationale || p.relationshipToHotel || "";
    if (featured && present(opts.rationaleOverride)) relevance = opts.rationaleOverride;
    // Keep relevance to one concise sentence on the card
    if (relevance && relevance.length > 220) {
      var cut = relevance.indexOf(". ");
      relevance = cut > 40 && cut < 220 ? relevance.slice(0, cut + 1) : relevance.slice(0, 217) + "…";
    }
    var emails = personEmails(p);
    var phones = personPhones(p);
    var orgRoutes = personOrgRoutes(p, opts.vm || null);
    var session = _sessionSurfeByPerson[p.personId || ""] || null;

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
              LABELS.orgRoute,
              c,
              c.kind === "email" ? "email" : c.kind === "web" ? "web" : "phone"
            );
          })
          .join("") +
        (session
          ? '<div class="hci-channel-row hci-channel-row--session" data-surfe-session="1">' +
            '<span class="hci-channel-label">Surfe session result</span> ' +
            (session.email
              ? '<a class="hci-action-link" href="mailto:' +
                esc(session.email) +
                '">' +
                esc(session.email) +
                "</a> "
              : "") +
            (session.phone
              ? '<a class="hci-action-link" href="tel:' +
                esc(String(session.phone).replace(/\s+/g, "")) +
                '">' +
                esc(session.phone) +
                "</a>"
              : "") +
            ' <span class="hci-muted">· not persisted</span></div>'
          : "") +
        "</div>";
    }

    return (
      '<article class="hci-person' +
      (featured ? " hci-person--featured" : "") +
      (compact ? " hci-person--compact" : "") +
      '" data-person-id="' +
      esc(p.personId || "") +
      '" data-contact-class="' +
      esc(p.contactClass || "") +
      '" data-property-responsibility="' +
      esc(p.propertyResponsibility || "") +
      '" data-identity-verified="' +
      (linkedInUrl(p) || p.identityVerification === "VERIFIED_PUBLIC" ? "1" : "0") +
      '" data-contact-details-verified="' +
      (emails.length || phones.length ? "1" : "0") +
      '" data-demo-state="' +
      esc(
        emails.length && phones.length
          ? "A"
          : emails.length
            ? "B"
            : phones.length
              ? "C"
              : orgRoutes.length
                ? "D"
                : "D"
      ) +
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
      "</div>" +
      '<span class="' +
      responsibilityClass(p) +
      '">' +
      esc(humanResponsibility(p)) +
      "</span>" +
      "</div>" +
      (present(relevance) ? '<p class="hci-person__why">' + esc(relevance) + "</p>" : "") +
      channelPreview +
      renderPersonActions(p, hotelId, capability, opts) +
      '<button type="button" class="hci-provenance-toggle" data-hci-toggle="' +
      detailsId +
      '" aria-expanded="false">Details</button>' +
      '<div class="hci-person__details" id="' +
      detailsId +
      '" hidden>' +
      (present(p.contactClassLabel || p.commercialRole)
        ? "<div><strong>Role</strong> " + esc(p.contactClassLabel || p.commercialRole) + "</div>"
        : "") +
      (present(p.propertyResponsibilityNote)
        ? "<div><strong>Property note</strong> " + esc(p.propertyResponsibilityNote) + "</div>"
        : "") +
      (present(p.verificationNotes)
        ? "<div><strong>Notes</strong> " + esc(p.verificationNotes) + "</div>"
        : "") +
      (present(p.lastVerified)
        ? "<div><strong>Last reviewed</strong> " + esc(p.lastVerified) + "</div>"
        : "") +
      '<p class="hci-muted">Verified identity ≠ verified personal contact channels. Organization emails are not personal inboxes. Surfe results stay session-only and are never saved as public research.</p>' +
      "</div></article>"
    );
  }

  var vmCapability = null;

  function renderOwnershipSlim(vm) {
    var o = vm.ownershipSummary || {};
    var bits = [];
    if (o.propco && present(o.propco.shortName || o.propco.name)) {
      bits.push("PropCo: " + (o.propco.shortName || o.propco.name));
    }
    if (o.sponsor && present(o.sponsor.name)) {
      var sponsorLabel =
        /PROPOSED|proposed/i.test(String(o.sponsor.label || o.sponsor.role || ""))
          ? "Proposed economic sponsor"
          : /VERIFIED|validated/i.test(String(o.sponsor.label || ""))
            ? "Economic owner"
            : "Economic sponsor (unconfirmed legal owner)";
      bits.push(sponsorLabel + ": " + o.sponsor.name);
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

  function contactGroupKey(p) {
    var title = String(p.title || "").toLowerCase();
    var role = [
      p.contactClass,
      p.contactClassLabel,
      p.commercialRole,
      p.functionRole,
      p.category,
      p.decision_authority_category,
    ]
      .map(function (x) {
        return String(x || "");
      })
      .join(" ")
      .toLowerCase();
    // Ownership / AM first — "RELEVANT_COMMERCIAL_ENTRY_POINT" must not force hotel_commercial.
    if (
      /owner|asset management|investment|sponsor|propco|decision authority|acquisition|verified_organization_contact|relevant_commercial_entry_point/.test(
        role + " " + title
      )
    ) {
      return "ownership_am";
    }
    if (
      /general manager|hotel manager|director of sales|dosm|revenue|catering|group sales|operations|f&b|rooms division|commercial director/.test(
        title + " " + role
      )
    ) {
      return "hotel_commercial";
    }
    if (/regional|area |management company|portfolio|franchise|brand representative/.test(role + " " + title)) {
      return "mgmt_regional";
    }
    return "other";
  }

  var CONTACT_GROUP_META = {
    hotel_commercial: {
      title: "Hotel & Commercial Leadership",
      blurb: "Hotel executives and commercial decision-makers.",
    },
    ownership_am: {
      title: "Ownership & Asset Management",
      blurb: "Verified owners, asset managers and investment-related representatives.",
    },
    mgmt_regional: {
      title: "Management Company & Regional Leadership",
      blurb: "Relevant management company, regional and portfolio executives.",
    },
    other: {
      title: "Other Relevant Organizational Contacts",
      blurb: "Credible organizational entry points and referrals.",
    },
  };

  function renderRecommendation(vm) {
    var rec = vm.recommendation;
    if (!rec || !rec.person) return "";
    var hotelId = vm.hotelId || "";
    var alts = rec.alternatives || [];
    // Prefer full alternative people from recommendedContacts / people
    if (!alts.length && (vm.recommendedContacts || []).length) {
      alts = (vm.recommendedContacts || []).filter(function (p) {
        return p && p.personId !== rec.person.personId;
      });
    }
    return (
      '<section class="hci-section hci-section--recommend">' +
      "<h3>" +
      esc(rec.label || "Recommended First Contact") +
      "</h3>" +
      '<div class="hci-people hci-people--featured">' +
      renderPerson(rec.person, hotelId, vmCapability, {
        featured: true,
        compact: false,
        rationaleOverride: rec.rationale || rec.person.whyRelevant,
        vm: vm,
      }) +
      "</div></section>"
    );
  }

  function renderAlternatives(vm) {
    var hotelId = vm.hotelId || "";
    var featuredId =
      vm.recommendation && vm.recommendation.person && vm.recommendation.person.personId;
    var alts = [];
    if (vm.recommendation && Array.isArray(vm.recommendation.alternatives)) {
      alts = vm.recommendation.alternatives.slice();
    }
    (vm.recommendedContacts || []).forEach(function (p) {
      if (!p || !p.personId || p.personId === featuredId) return;
      if (
        !alts.some(function (a) {
          return a && a.personId === p.personId;
        })
      ) {
        alts.push(p);
      }
    });
    (vm.otherContacts || []).forEach(function (p) {
      if (!p || !p.personId || p.personId === featuredId) return;
      if (
        !alts.some(function (a) {
          return a && a.personId === p.personId;
        })
      ) {
        alts.push(p);
      }
    });
    if (!alts.length && (vm.people || []).length) {
      alts = (vm.people || []).filter(function (p) {
        return p && p.personId !== featuredId;
      });
    }
    if (!alts.length) return "";

    var groups = { hotel_commercial: [], ownership_am: [], mgmt_regional: [], other: [] };
    alts.forEach(function (p) {
      groups[contactGroupKey(p)].push(p);
    });
    var order = ["hotel_commercial", "ownership_am", "mgmt_regional", "other"];
    var sections = order
      .filter(function (k) {
        return groups[k].length;
      })
      .map(function (k) {
        var meta = CONTACT_GROUP_META[k];
        return (
          '<section class="hci-section hci-section--alts" data-contact-group="' +
          esc(k) +
          '">' +
          "<h3>" +
          esc(meta.title) +
          "</h3>" +
          '<p class="hci-muted">' +
          esc(meta.blurb) +
          "</p>" +
          '<div class="hci-people hci-people--compact">' +
          groups[k]
            .map(function (p) {
              return renderPerson(p, hotelId, vmCapability, { compact: true, vm: vm });
            })
            .join("") +
          "</div></section>"
        );
      });
    return sections.join("");
  }

  function renderOrgChannels(vm) {
    var rows = [];
    var contactable = vm.contactableOwnerOrg;
    if (contactable) {
      if (present(contactable.website)) {
        rows.push({
          label: LABELS.orgRoute,
          value: contactable.website,
          kind: "web",
        });
      }
      (contactable.emails || []).forEach(function (c) {
        if (c && present(c.value)) {
          rows.push({
            label: LABELS.orgRoute,
            value: c.value,
            kind: "email",
            note: "Not a personal inbox",
          });
        }
      });
      (contactable.phones || []).forEach(function (c) {
        if (c && present(c.value)) {
          rows.push({
            label: LABELS.orgRoute,
            value: c.value,
            kind: "phone",
            note: "Organization / published business line",
          });
        }
      });
      if (present(contactable.hq)) {
        rows.push({ label: "HQ", value: contactable.hq, kind: "text" });
      }
    }
    (vm.bestContactPaths || []).forEach(function (b) {
      if (!b || !present(b.value)) return;
      var dup = rows.some(function (r) {
        return String(r.value).toLowerCase() === String(b.value).toLowerCase();
      });
      if (dup) return;
      rows.push({
        label: LABELS.orgRoute,
        value: b.value,
        kind: /^https?:/i.test(b.value) ? "web" : /@/.test(b.value) ? "email" : "text",
        note: "Organization-level — not a personal decision-maker inbox",
      });
    });
    if (!rows.length) return "";
    return (
      '<section class="hci-section hci-section--org-channels">' +
      "<h3>Organization Contact Channels</h3>" +
      '<p class="hci-muted">Organizational Contact Routes — not personal direct channels.</p>' +
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
                  ? '<a class="hci-action-link" href="tel:' +
                    esc(String(r.value).replace(/\s+/g, "")) +
                    '">' +
                    esc(r.value) +
                    "</a>"
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
      '" aria-expanded="false">Research gaps (' +
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

  function renderViewModel(vm) {
    if (!vm) return renderEmpty();
    return (
      '<div class="hci-root" data-hci-state="ready" data-hotel-id="' +
      esc(vm.hotelId || "") +
      '">' +
      '<header class="hci-header hci-header--compact">' +
      "<div><h2>Contact Intelligence</h2>" +
      '<p class="hci-subtitle">' +
      esc(vm.hotelName || "") +
      (vm.researchWorkflow
        ? ' · <span class="hci-muted">Public research first · Surfe on demand</span>'
        : "") +
      "</p></div></header>" +
      renderOwnershipSlim(vm) +
      renderRecommendation(vm) +
      renderAlternatives(vm) +
      renderOrgChannels(vm) +
      renderResearchGaps(vm) +
      "</div>"
    );
  }

  function paintEnrichResult(slot, payload) {
    if (!slot) return;
    slot.removeAttribute("hidden");
    if (!payload || payload.ok === false) {
      var err = String((payload && (payload.error || "")) || "");
      var msg;
      if (err === "surfe_unavailable_for_hotel_explorer" || err === "enrichment_not_configured") {
        msg =
          (payload && payload.userMessage) ||
          "Contact enrichment is not available in this environment.";
      } else if (err === "reveal_in_flight") {
        msg = "A lookup is already in progress for this person.";
      } else if (err === "no_result" || err === "surfe_no_result") {
        msg = "No additional professional contact details found.";
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
            phones
              .map(function (ph) {
                return (
                  '<a class="hci-action-link" href="tel:' +
                  esc(String(ph).replace(/\s+/g, "")) +
                  '">' +
                  esc(ph) +
                  "</a>"
                );
              })
              .join(", ") +
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
        '<div class="hci-note hci-note--unresolved">No additional professional contact details found.</div>';
      return;
    }
    var personId = person.personId || slot.getAttribute("data-person-id") || "";
    if (personId) {
      _sessionSurfeByPerson[personId] = {
        email: email || null,
        phone: phone || null,
        at: Date.now(),
        mock: payload.mock === true,
      };
    }
    var attribution =
      payload.mock === true
        ? "Mock Surfe provider (no-cost) · session-only · not persisted"
        : (payload.providerAttribution || "Surfe on-demand") +
          " · session-only · not persisted to Airtable";
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
          '<a class="hci-action-link" href="tel:' +
          esc(String(phone).replace(/\s+/g, "")) +
          '">' +
          esc(phone) +
          "</a></div>"
        : "") +
      '<p class="hci-muted">Surfe results are ephemeral. They are never saved as public research, never written to Airtable from this action, and disappear after reload.</p>' +
      "</div>";
  }

  function bindEnrich(root) {
    if (!root) return;
    root.addEventListener("click", function (ev) {
      var btn = ev.target.closest("[data-hci-enrich]");
      if (!btn || btn.disabled) return;
      var hotelId = btn.getAttribute("data-hotel-id") || "";
      var personId = btn.getAttribute("data-person-id") || "";
      var personName = btn.getAttribute("data-person-name") || "";
      var organization = btn.getAttribute("data-organization") || "";
      var key = hotelId + "::" + personId;
      if (_inflightReveal[key]) return;
      var slot = btn.parentElement && btn.parentElement.querySelector(".hci-enrich__result");
      var card = btn.closest(".hci-person");
      btn.disabled = true;
      btn.setAttribute("aria-busy", "true");
      btn.textContent = "Looking up…";
      if (slot) {
        slot.hidden = false;
        slot.setAttribute("data-hci-wave-loading", "1");
        slot.innerHTML = waveHtml("Looking up contact details…", "On-demand enrichment");
      }
      if (card) card.setAttribute("data-enriching", "1");
      _inflightReveal[key] = true;
      fetch(
        "/api/contact-intelligence/hotels/" +
          encodeURIComponent(hotelId) +
          "/people/" +
          encodeURIComponent(personId) +
          "/reveal",
        {
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
          }),
        }
      )
        .then(function (r) {
          return r.json().then(function (j) {
            return { status: r.status, body: j };
          });
        })
        .then(function (pack) {
          var body = pack.body || { ok: false, error: "http_" + pack.status };
          if (slot) slot.setAttribute("data-person-id", personId);
          if (body && body.person && !body.person.personId) body.person.personId = personId;
          paintEnrichResult(slot, body);
          var restoreLabel = btn.getAttribute("data-enrich-label") || FIND_LABEL;
          if (body && body.ok && !body.pending && (body.person || body.cached)) {
            btn.textContent = "Lookup complete";
            btn.disabled = true;
            btn.removeAttribute("data-hci-enrich");
            return;
          }
          btn.disabled = false;
          btn.textContent = restoreLabel;
        })
        .catch(function (err) {
          paintEnrichResult(slot, {
            ok: false,
            error: "network_error",
            userMessage: "Lookup could not be completed. Try again.",
            message: String((err && err.message) || err),
          });
          btn.disabled = false;
          btn.textContent = btn.getAttribute("data-enrich-label") || FIND_LABEL;
        })
        .finally(function () {
          delete _inflightReveal[key];
          btn.removeAttribute("aria-busy");
          if (slot) slot.removeAttribute("data-hci-wave-loading");
          if (card) card.removeAttribute("data-enriching");
        });
    });
  }

  function bindToggles(root) {
    if (!root) return;
    if (root.getAttribute("data-hci-bound") === "1") {
      bindEnrich(root);
      return;
    }
    root.setAttribute("data-hci-bound", "1");
    root.addEventListener("click", function (ev) {
      var btn = ev.target && ev.target.closest && ev.target.closest("[data-hci-toggle]");
      if (!btn || !root.contains(btn)) return;
      ev.preventDefault();
      var id = btn.getAttribute("data-hci-toggle");
      var el = id ? document.getElementById(id) : null;
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
  }

  function renderHpcPropertyFallback(hotelId, contacts, hotelName, ownershipSummary) {
    var phone = contacts && contacts.property_phone;
    var website = contacts && contacts.website;
    var source =
      (contacts && contacts.property_fallback_source) || "Hotel Property Census";
    var phoneHtml = phone
      ? '<a class="hci-action-link" href="tel:' +
        esc(String(phone).replace(/\s+/g, "")) +
        '">' +
        esc(phone) +
        "</a>"
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

  function buildLiveVmFromOwnershipPayload(hotelId, json) {
    var report = (json && json.report) || {};
    var people = report.decision_makers || (report.decision_authority && report.decision_authority.people) || [];
    if (!Array.isArray(people) || !people.length) return null;
    var hotelName =
      (json.hotel && (json.hotel.name || json.hotel.display_name)) || hotelId;
    var control = report.ownership_and_control || {};
    var corp = report.corporate_contacts || {};
    function scrubPublicCopy(s) {
      return String(s || "")
        .replace(/HE50_PILOT_HOTEL:\S+/gi, "")
        .replace(/\s{2,}/g, " ")
        .replace(/\s+([.;])/g, "$1")
        .trim();
    }
    var ranked = people.map(function (p, idx) {
      var why = scrubPublicCopy(
        p.whyRelevant || p.relationshipToHotel || p.hotelResponsibility || ""
      );
      return Object.assign({}, p, {
        personId: p.personId || p.person_id || "live:" + idx + ":" + String(p.name || "person"),
        organization: p.organization || p.organizationName || null,
        whyRelevant: why,
        propertyResponsibility: p.propertyResponsibility || "UNKNOWN",
        propertyResponsibilityLabel: p.propertyResponsibilityLabel || null,
        contactClass: p.contactClass || p.functionRole || p.commercialRole || "",
        contactClassLabel: p.contactClassLabel || p.commercialRole || p.functionRole || "",
        lastVerified: p.lastVerified || null,
      });
    });
    // Prefer strongest commercial entry point (VP/AM) over referral directors
    var featured =
      ranked.find(function (p) {
        return /katherine button/i.test(String(p.name || ""));
      }) ||
      ranked.find(function (p) {
        return (
          /RELEVANT_COMMERCIAL_ENTRY_POINT/i.test(String(p.contactClass || p.commercialRole || "")) &&
          /VP|Vice President|Asset Management and Investments/i.test(String(p.title || ""))
        );
      }) ||
      ranked.find(function (p) {
        return /RELEVANT_COMMERCIAL_ENTRY_POINT/i.test(String(p.contactClass || p.commercialRole || ""));
      }) ||
      ranked[0];
    var alts = ranked.filter(function (p) {
      return p.personId !== featured.personId;
    });
    var orgEmails = corp.organization_emails || [];
    var orgPhones = corp.organization_phones || [];
    return {
      version: "contact-intelligence-viewmodel-v2-live-hi-org-contacts",
      preview_mode: "LIVE_AIRTABLE",
      hotelId: hotelId,
      hotelName: hotelName,
      researchWorkflow: "Canonical HI Org Contacts · Public research first · Surfe on demand",
      lastReviewed: featured.lastVerified || null,
      ownershipSummary: {
        hotel: { name: hotelName, role: "HOTEL" },
        propco: control.property_company && control.property_company.known
          ? {
              name: control.property_company.name,
              role: "PROPERTY_OWNER_LEGAL_ENTITY",
              label: "PropCo",
              shortName: control.property_company.name,
            }
          : null,
        sponsor: control.economic_owner && control.economic_owner.known
          ? {
              name: control.economic_owner.name,
              role: "OWNER_SPONSOR_GROUP",
              label: "Economic owner",
            }
          : {
              name: featured.organization || null,
              role: "OWNER_SPONSOR_GROUP",
              label: "Economic sponsor (unconfirmed legal owner)",
            },
        operator: control.operator && control.operator.name
          ? { name: control.operator.name, role: "OPERATOR", label: "Current Operator" }
          : null,
        brand: control.brand && control.brand.name
          ? { name: control.brand.name, role: "CURRENT_BRAND", label: "Current Brand" }
          : null,
      },
      recommendation: {
        label: "Suggested Contact — Property Responsibility Not Confirmed",
        person: featured,
        rationale: featured.whyRelevant,
        alternatives: alts,
      },
      recommendedContacts: ranked,
      otherContacts: [],
      people: ranked,
      contactableOwnerOrg: {
        name: featured.organization || null,
        website: corp.website || null,
        emails: orgEmails.map(function (c) {
          return {
            value: c.value,
            classification: c.classification || "official_org",
            source_url: c.source_url,
            note: c.note,
          };
        }),
        phones: orgPhones.map(function (c) {
          return {
            value: c.value,
            classification: c.classification || "official_org",
            source_url: c.source_url,
            note: c.note,
          };
        }),
      },
      unresolvedItems: [
        "Named professional emails for researched people (public sources)",
        "Direct business telephone numbers for researched people",
        "Confirmed property-level responsibility assignments",
      ],
      source: "HI_ORG_CONTACTS",
    };
  }

  function loadLiveHiOrgContactsViewModel(hotelId) {
    return fetch(
      "/api/golden-demo/ownership/hotel/" + encodeURIComponent(hotelId),
      { credentials: "same-origin" }
    )
      .then(function (r) {
        if (!r.ok) throw new Error("http_" + r.status);
        return r.json();
      })
      .then(function (json) {
        return buildLiveVmFromOwnershipPayload(hotelId, json);
      })
      .catch(function () {
        return null;
      });
  }

  function loadHpcPropertyFallback(hotelId) {
    return fetch(
      "/api/golden-demo/ownership/hotel/" + encodeURIComponent(hotelId),
      { credentials: "same-origin" }
    )
      .then(function (r) {
        if (!r.ok) throw new Error("http_" + r.status);
        return r.json();
      })
      .then(function (json) {
        // Prefer canonical HI Org Contacts when present — never demote to Census fallback.
        var liveVm = buildLiveVmFromOwnershipPayload(hotelId, json);
        if (liveVm) return { liveVm: liveVm };
        var contacts =
          (json && json.report && json.report.corporate_contacts) || null;
        if (
          contacts &&
          contacts.source === "HI_CONTACT_CHANNELS" &&
          (contacts.organization_emails || contacts.organization_phones)
        ) {
          // Org routes only — still better than Census fallback for Contact Intelligence.
          return {
            liveVm: {
              version: "contact-intelligence-viewmodel-v2-live-org-routes",
              hotelId: hotelId,
              hotelName:
                (json.hotel && (json.hotel.name || json.hotel.display_name)) || hotelId,
              ownershipSummary: {
                sponsor: { name: null, label: "Economic sponsor (unconfirmed legal owner)" },
                operator: {
                  name:
                    (json.report &&
                      json.report.ownership_and_control &&
                      json.report.ownership_and_control.operator &&
                      json.report.ownership_and_control.operator.name) ||
                    null,
                },
              },
              people: [],
              recommendedContacts: [],
              contactableOwnerOrg: {
                website: contacts.website || null,
                emails: contacts.organization_emails || [],
                phones: contacts.organization_phones || [],
              },
              unresolvedItems: ["Named decision-makers for this hotel"],
            },
          };
        }
        if (!contacts || (!contacts.property_phone && !contacts.website)) {
          return null;
        }
        if (contacts.source === "HI_CONTACT_CHANNELS") return null;
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

  function loadViewModel(hotelId) {
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
            return {
              hotelId: hotelId,
              hotelName: hotelId,
              _rawPackage: json.package,
              people: [],
              organizations: [],
            };
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

  function mount(host, hotelId) {
    if (!host) return Promise.resolve();
    // Surfe session results must not survive hotel remount / reload.
    _sessionSurfeByPerson = Object.create(null);
    host.innerHTML = renderLoading();
    return Promise.all([loadViewModel(hotelId), loadEnrichmentCapability()]).then(function (pack) {
      var vm = pack[0];
      vmCapability = pack[1] || null;
      if (vm && vm.ownershipSummary) {
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
        host.innerHTML = renderViewModel(vm);
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
      return loadLiveHiOrgContactsViewModel(hotelId).then(function (liveVm) {
        if (liveVm && liveVm.ownershipSummary) {
          if (Array.isArray(liveVm.people)) {
            liveVm.people = liveVm.people.map(function (p) {
              return patchPersonEnrichAction(p, vmCapability);
            });
          }
          if (Array.isArray(liveVm.recommendedContacts)) {
            liveVm.recommendedContacts = liveVm.recommendedContacts.map(function (p) {
              return patchPersonEnrichAction(p, vmCapability);
            });
          }
          if (liveVm.recommendation && liveVm.recommendation.person) {
            liveVm.recommendation.person = patchPersonEnrichAction(
              liveVm.recommendation.person,
              vmCapability
            );
          }
          host.innerHTML = renderViewModel(liveVm);
          bindToggles(host);
          return;
        }
        return loadHpcPropertyFallback(hotelId).then(function (fallback) {
          if (fallback && fallback.liveVm) {
            host.innerHTML = renderViewModel(fallback.liveVm);
            bindToggles(host);
            return;
          }
          if (fallback && fallback.contacts) {
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
      });
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
