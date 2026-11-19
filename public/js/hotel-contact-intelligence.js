/**
 * Hotel Explorer — Contact Intelligence tab (generic renderer).
 * Surfe "Find Contact Details" is user-initiated only (never on mount/GET).
 */
(function (global) {
  "use strict";

  var SHOWCASE_PATHS = {
    recUNycnMwOVFX0hc: "/data/hotel-contact-intelligence/kgpv-recUNycnMwOVFX0hc.json",
  };

  var _inflightReveal = Object.create(null);

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

  function kvRow(label, value) {
    if (!present(value)) return "";
    return (
      '<div class="hci-kv"><span>' +
      esc(label) +
      "</span><strong>" +
      esc(value) +
      "</strong></div>"
    );
  }

  function unresolvedNote(msg) {
    return '<p class="hci-note hci-note--unresolved">' + esc(msg) + "</p>";
  }

  function renderLoading() {
    return (
      '<div class="hci-root" data-hci-state="loading">' +
      '<div class="hci-loading">Loading Contact Intelligence…</div>' +
      "</div>"
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

  function badgeClass(status) {
    var s = String(status || "").toUpperCase();
    if (s.indexOf("VERIFIED") === 0) return "hci-badge hci-badge--verified";
    if (s === "PUBLICLY_PUBLISHED" || s === "PUBLIC_SOURCE") return "hci-badge hci-badge--public";
    if (s === "INFERRED" || s === "DOMAIN_PATTERN_SUPPORTED" || s === "PROBABLE")
      return "hci-badge hci-badge--inferred";
    if (s === "STALE" || s === "INVALID") return "hci-badge hci-badge--warn";
    if (s === "UNRESOLVED") return "hci-badge hci-badge--gap";
    return "hci-badge";
  }

  function sourceBadge(ch) {
    var st = String((ch && ch.verification_status) || "").toUpperCase();
    if (st.indexOf("VERIFIED_OFFICIAL") === 0) return '<span class="hci-src">OFFICIAL WEBSITE</span>';
    if (st.indexOf("VERIFIED") === 0) return '<span class="hci-src">PUBLIC SOURCE</span>';
    if (st === "PUBLICLY_PUBLISHED") return '<span class="hci-src">PUBLIC SOURCE</span>';
    if (st === "INFERRED") return '<span class="hci-src hci-src--inferred">UNVERIFIED</span>';
    if (String((ch && ch.provider) || "").toLowerCase() === "surfe")
      return '<span class="hci-src">SURFE ON-DEMAND</span>';
    return '<span class="hci-src">UNVERIFIED</span>';
  }

  function renderChannel(ch) {
    if (!ch || !ch.value) {
      return '<div class="hci-channel hci-channel--empty">Not yet verified</div>';
    }
    var id = "hci-ch-" + Math.random().toString(36).slice(2, 9);
    return (
      '<div class="hci-channel">' +
      '<div class="hci-channel__main">' +
      '<span class="hci-channel__type">' +
      esc(ch.type || "CONTACT") +
      "</span>" +
      (String(ch.value).indexOf("http") === 0
        ? '<a class="hci-channel__value" href="' + esc(ch.value) + '" target="_blank" rel="noopener">' + esc(ch.value) + "</a>"
        : '<span class="hci-channel__value">' + esc(ch.value) + "</span>") +
      '<span class="' +
      badgeClass(ch.verification_status) +
      '">' +
      esc(ch.verification_status || "UNRESOLVED") +
      "</span>" +
      sourceBadge(ch) +
      "</div>" +
      '<button type="button" class="hci-provenance-toggle" data-hci-toggle="' +
      id +
      '" aria-expanded="false">Provenance</button>' +
      '<div class="hci-provenance" id="' +
      id +
      '" hidden>' +
      "<div>Source type: " +
      esc(ch.source_type || ch.source || "—") +
      "</div>" +
      "<div>Source URL: " +
      (ch.source_url
        ? '<a href="' + esc(ch.source_url) + '" target="_blank" rel="noopener">' + esc(ch.source_url) + "</a>"
        : "—") +
      "</div>" +
      "<div>Discovery: " +
      esc(ch.discovery_method || "—") +
      "</div>" +
      "<div>Verification: " +
      esc(ch.verification_method || "—") +
      "</div>" +
      "<div>Last verified: " +
      esc(ch.last_verified || "—") +
      "</div>" +
      "<div>Confidence: " +
      esc(ch.confidence || "—") +
      "</div>" +
      (ch.note ? "<div>Note: " + esc(ch.note) + "</div>" : "") +
      "</div></div>"
    );
  }

  function renderOwnershipSummary(vm) {
    var o = vm.ownershipSummary || {};
    var hist = (o.brandHistory || [])
      .map(function (b) {
        return (
          '<span class="hci-chip hci-chip--' +
          esc(String(b.status || "").toLowerCase()) +
          '">' +
          esc(b.name) +
          " · " +
          esc(b.status) +
          "</span>"
        );
      })
      .join("");
    return (
      '<section class="hci-section hci-ownership-summary">' +
      "<h3>Ownership contact summary</h3>" +
      '<div class="hci-chain">' +
      '<div class="hci-chain__node"><div class="hci-chain__label">Hotel</div><div class="hci-chain__value">' +
      esc((o.hotel && o.hotel.name) || vm.hotelName) +
      "</div></div>" +
      '<div class="hci-chain__arrow" aria-hidden="true">↓ owned by</div>' +
      '<div class="hci-chain__node"><div class="hci-chain__label">' +
      esc((o.propco && o.propco.label) || "Property Owner / PropCo") +
      '</div><div class="hci-chain__value">' +
      esc((o.propco && (o.propco.shortName || o.propco.name)) || "Not yet verified") +
      "</div></div>" +
      '<div class="hci-chain__arrow" aria-hidden="true">↓ sponsored / controlled by</div>' +
      '<div class="hci-chain__node hci-chain__node--focus"><div class="hci-chain__label">' +
      esc((o.sponsor && o.sponsor.label) || "Economic Owner") +
      '</div><div class="hci-chain__value">' +
      esc((o.sponsor && o.sponsor.name) || "Not yet verified") +
      "</div></div>" +
      '<div class="hci-chain__arrow" aria-hidden="true">↓ operated by</div>' +
      '<div class="hci-chain__node"><div class="hci-chain__label">' +
      esc((o.operator && o.operator.label) || "Current Operator") +
      '</div><div class="hci-chain__value">' +
      esc((o.operator && o.operator.name) || "Not yet verified") +
      (o.operator && o.operator.sameAsSponsor
        ? '<div class="hci-muted">Same organization also acts as current operator</div>'
        : "") +
      "</div></div>" +
      '<div class="hci-chain__arrow" aria-hidden="true">↓ branded as</div>' +
      '<div class="hci-chain__node"><div class="hci-chain__label">' +
      esc((o.brand && o.brand.label) || "Current Brand") +
      '</div><div class="hci-chain__value">' +
      esc((o.brand && o.brand.name) || "Not yet verified") +
      "</div></div>" +
      "</div>" +
      (o.formerOwner
        ? '<div class="hci-muted hci-former">Former co-owner (historical): ' +
          esc(o.formerOwner.name) +
          " · " +
          esc(o.formerOwner.status) +
          "</div>"
        : "") +
      (hist ? '<div class="hci-brand-history"><span class="hci-muted">Brand history:</span> ' + hist + "</div>" : "") +
      "</section>"
    );
  }

  function renderQuality(vm) {
    var q = vm.qualitySummary || {};
    return (
      '<aside class="hci-quality" aria-label="Contact quality summary">' +
      '<div class="hci-quality__item"><span>Owner org</span><strong>' +
      esc(q.ownerOrgStatus || "—") +
      "</strong></div>" +
      '<div class="hci-quality__item"><span>Primary decision makers</span><strong>' +
      esc(q.primaryDecisionMakers != null ? q.primaryDecisionMakers : "—") +
      "</strong></div>" +
      '<div class="hci-quality__item"><span>Verified emails</span><strong>' +
      esc(q.verifiedEmails != null ? q.verifiedEmails : "—") +
      "</strong></div>" +
      '<div class="hci-quality__item"><span>Verified / public phones</span><strong>' +
      esc(q.verifiedOrPublicPhones != null ? q.verifiedOrPublicPhones : "—") +
      "</strong></div>" +
      '<div class="hci-quality__item"><span>Fallback contact paths</span><strong>' +
      esc(q.fallbackContactPaths != null ? q.fallbackContactPaths : "—") +
      "</strong></div>" +
      '<div class="hci-quality__item"><span>Last reviewed</span><strong>' +
      esc(q.lastReviewed || vm.lastReviewed || "—") +
      "</strong></div>" +
      "</aside>"
    );
  }

  function renderOrgCard(org, opts) {
    opts = opts || {};
    var title = opts.title || org.role || "Organization";
    var badges = (org.relationshipBadges || [])
      .map(function (b) {
        return '<span class="hci-rel-badge">' + esc(b) + "</span>";
      })
      .join("");
    var channelsHtml = "";
    if (org.noSeparatePublicContactPath) {
      channelsHtml =
        '<div class="hci-fallback-box">' +
        "<strong>Legal owner identified</strong>" +
        "<p>No separate public contact path confirmed.</p>" +
        "<p>Use parent/sponsor contact path: <strong>" +
        esc(org.useParentContactPath || "—") +
        "</strong></p></div>";
    } else {
      var parts = [];
      if (org.website)
        parts.push(
          renderChannel({
            type: "ORG_CONTACT_PAGE",
            value: org.website,
            verification_status: org.verification || "VERIFIED_OFFICIAL",
            source_type: "first_party",
            source_url: org.website,
            last_verified: org.lastVerified,
            confidence: org.confidence,
          })
        );
      (org.emails || []).forEach(function (c) {
        parts.push(renderChannel(c));
      });
      (org.phones || []).forEach(function (c) {
        parts.push(renderChannel(c));
      });
      if (!parts.length) {
        parts.push('<div class="hci-channel hci-channel--empty">No public contact found</div>');
      }
      channelsHtml = parts.join("");
    }
    return (
      '<article class="hci-org-card' +
      (opts.prominent ? " hci-org-card--prominent" : "") +
      '" data-org-role="' +
      esc(org.role || "") +
      '">' +
      '<div class="hci-org-card__head">' +
      '<div class="hci-org-card__kicker">' +
      esc(title) +
      "</div>" +
      "<h4>" +
      esc(org.shortName ? org.shortName + " — " + org.name : org.name) +
      "</h4>" +
      '<div class="hci-org-card__meta">' +
      badges +
      '<span class="' +
      badgeClass(org.verification) +
      '">' +
      esc(org.verification || "UNRESOLVED") +
      "</span>" +
      "</div></div>" +
      (org.hq ? '<div class="hci-kv"><span>HQ / location</span><strong>' + esc(org.hq) + "</strong></div>" : "") +
      (org.entityType
        ? '<div class="hci-kv"><span>Entity type</span><strong>' + esc(org.entityType) + "</strong></div>"
        : "") +
      (org.relationship
        ? '<div class="hci-kv"><span>Relationship</span><strong>' + esc(org.relationship) + "</strong></div>"
        : "") +
      (org.sameOrganizationAlsoOperator
        ? '<p class="hci-note">Same organization also acts as current operator.</p>'
        : "") +
      (org.brandNotOwner
        ? '<p class="hci-note">Brand contact path — not owner contact.</p>'
        : "") +
      (org.note ? '<p class="hci-muted">' + esc(org.note) + "</p>" : "") +
      '<div class="hci-channels">' +
      channelsHtml +
      "</div>" +
      '<div class="hci-org-card__foot">' +
      "<span>Confidence: " +
      esc(org.confidence || "—") +
      "</span>" +
      "<span>Last verified: " +
      esc(org.lastVerified || "—") +
      "</span>" +
      (org.sourceCount != null ? "<span>Sources: " + esc(org.sourceCount) + "</span>" : "") +
      "</div></article>"
    );
  }

  function renderEnrichAction(p, hotelId, capability) {
    var hasName = present(p.name) && String(p.name).trim().split(/\s+/).length >= 2;
    var hasOrg = present(p.organization) || present(p.organizationName);
    var hasDirectContact =
      (p.emailContacts || []).some(function (c) {
        return c && c.value;
      }) ||
      (p.phoneContacts || []).some(function (c) {
        return c && c.value;
      });
    var action = p.enrichAction || {};
    var label = action.label || "Find Contact Details";
    var enabled =
      action.enabled === true ||
      (capability &&
        capability.surfeEnabled === true &&
        hasName &&
        hasOrg &&
        !hasDirectContact &&
        action.enabled !== false);
    if (hasDirectContact) {
      return unresolvedNote("Verified or published channels already present — Surfe not required.");
    }
    if (!hasName || !hasOrg) {
      return unresolvedNote(
        "Find Contact Details unavailable until a full person name and organization are verified."
      );
    }
    if (!enabled) {
      var reason =
        action.reason ||
        (capability && capability.reason) ||
        "Surfe on-demand is not available in this environment";
      return unresolvedNote(label + " unavailable: " + reason + ".");
    }
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
      '">' +
      esc(label) +
      "</button>" +
      '<p class="hci-muted">User-initiated only. Checks existing channels first. Surfe credits used only when needed. Review required before Airtable persistence.</p>' +
      '<div class="hci-enrich__result" hidden></div></div>'
    );
  }

  function renderPerson(p, hotelId, capability) {
    var hasEmail = (p.emailContacts || []).some(function (c) {
      return c && c.value;
    });
    var hasPhone = (p.phoneContacts || []).some(function (c) {
      return c && c.value;
    });
    var emails = (p.emailContacts || []).map(renderChannel).join("") ||
      unresolvedNote("No direct email verified yet (person identity may still be verified).");
    var phones = (p.phoneContacts || []).map(renderChannel).join("") ||
      unresolvedNote("No public phone verified yet.");
    var profiles = (p.profileUrls || [])
      .map(function (pr) {
        return (
          '<a class="hci-profile-link" href="' +
          esc(pr.url) +
          '" target="_blank" rel="noopener">' +
          esc(pr.type || "Profile") +
          " · " +
          esc(pr.verification_status || (pr.verified ? "VERIFIED_PUBLIC" : "UNRESOLVED")) +
          "</a>"
        );
      })
      .join("");
    var fallback = "";
    if (p.fallbackPath) {
      fallback =
        '<div class="hci-fallback-box">' +
        "<strong>Fallback contact path</strong>" +
        "<p>" +
        esc(p.fallbackPath.label) +
        (present(p.fallbackPath.organization) ? " — " + esc(p.fallbackPath.organization) : "") +
        "</p>" +
        (p.fallbackPath.channels || [])
          .map(function (c) {
            return "<div>" + esc(c.label || c.type) + ": " + esc(c.value) + "</div>";
          })
          .join("") +
        "</div>";
    }
    var detailsId = "hci-person-" + esc(p.personId || Math.random().toString(36).slice(2));
    var metaBits = [p.organization, p.stakeholderClass].filter(present);
    return (
      '<article class="hci-person" data-person-id="' +
      esc(p.personId || "") +
      '" data-display-state="' +
      esc(p.displayState || "") +
      '" data-identity-verified="' +
      (p.displayState === "AVAILABLE" || p.displayState === "PARTIAL" ? "1" : "0") +
      '" data-contact-details-verified="' +
      (hasEmail || hasPhone ? "1" : "0") +
      '">' +
      '<div class="hci-person__head">' +
      "<div><h5>" +
      esc(p.name) +
      "</h5>" +
      (present(p.title) ? '<div class="hci-person__title">' + esc(p.title) + "</div>" : "") +
      (metaBits.length
        ? '<div class="hci-muted">' + esc(metaBits.join(" · ")) + "</div>"
        : "") +
      "</div>" +
      (present(p.displayState)
        ? '<span class="' +
          badgeClass(p.displayState === "AVAILABLE" ? "VERIFIED_PUBLIC" : p.displayState) +
          '">' +
          esc(p.displayState) +
          "</span>"
        : "") +
      "</div>" +
      kvRow("Why relevant", p.whyRelevant || p.relationshipToHotel) +
      kvRow("Likely commercial role", p.commercialRole || p.stakeholderClass) +
      kvRow("Contactability", p.contactability) +
      kvRow("Confidence", p.confidence) +
      kvRow("Last verified", p.lastVerified) +
      (profiles
        ? '<div class="hci-profiles">' + profiles + "</div>"
        : unresolvedNote("Public profile not yet verified.")) +
      '<button type="button" class="hci-provenance-toggle" data-hci-toggle="' +
      detailsId +
      '">Contact details</button>' +
      '<div class="hci-person__details" id="' +
      detailsId +
      '" hidden>' +
      "<h6>Email</h6>" +
      emails +
      "<h6>Phone</h6>" +
      phones +
      fallback +
      (p.verificationNotes ? '<p class="hci-muted">' + esc(p.verificationNotes) + "</p>" : "") +
      '<p class="hci-muted">Decision authority: ' +
      esc(p.decisionAuthority || "Authority Not Verified") +
      " · Signing authority: " +
      esc(p.signingAuthority || "Authority Not Verified") +
      " (not inferred). Verified identity ≠ verified personal contact details.</p>" +
      renderEnrichAction(p, hotelId, capability) +
      "</div></article>"
    );
  }

  var vmCapability = null;

  function renderPeopleGroups(vm) {
    var hotelId = vm.hotelId || "";
    var groups = [
      { id: "PRIMARY_OWNER_SIDE", title: "Primary owner-side contacts" },
      { id: "SECONDARY_OWNER_CORPORATE", title: "Secondary owner / corporate contacts" },
      { id: "OPERATOR_PROPERTY", title: "Operator / property contacts" },
    ];
    return groups
      .map(function (g) {
        var rows = (vm.people || []).filter(function (p) {
          return p.group === g.id;
        });
        if (!rows.length) return "";
        return (
          '<section class="hci-section"><h3>' +
          esc(g.title) +
          '</h3><div class="hci-people">' +
          rows
            .map(function (p) {
              return renderPerson(p, hotelId, vmCapability);
            })
            .join("") +
          "</div></section>"
        );
      })
      .join("");
  }

  function renderViewModel(vm) {
    if (!vm) return renderEmpty();
    var orgs = vm.organizations || [];
    var contactable = vm.contactableOwnerOrg;
    var propco = orgs.find(function (o) {
      return o.role === "PROPERTY_OWNER_LEGAL_ENTITY";
    });
    var brand = orgs.find(function (o) {
      return o.role === "CURRENT_BRAND";
    });
    var unresolved =
      (vm.unresolvedItems || []).length > 0
        ? '<section class="hci-section"><h3>Research / unresolved items</h3><ul class="hci-list">' +
          vm.unresolvedItems
            .map(function (i) {
              return "<li>" + esc(i) + "</li>";
            })
            .join("") +
          "</ul></section>"
        : "";
    var best =
      (vm.bestContactPaths || []).length > 0
        ? '<section class="hci-section"><h3>Fallback / best contact paths</h3><ol class="hci-list">' +
          vm.bestContactPaths
            .map(function (b) {
              return (
                "<li><strong>" +
                esc(b.label) +
                "</strong>: " +
                esc(b.value || "—") +
                ' <span class="' +
                badgeClass(b.verification_status) +
                '">' +
                esc(b.verification_status || "") +
                "</span></li>"
              );
            })
            .join("") +
          "</ol></section>"
        : "";

    return (
      '<div class="hci-root" data-hci-state="ready" data-hotel-id="' +
      esc(vm.hotelId || "") +
      '">' +
      '<header class="hci-header">' +
      "<div><h2>Contact Intelligence</h2>" +
      '<p class="hci-subtitle">' +
      esc(vm.hotelName || "") +
      "</p></div>" +
      renderQuality(vm) +
      "</header>" +
      renderOwnershipSummary(vm) +
      '<section class="hci-section"><h3>Organization contacts</h3><div class="hci-org-grid">' +
      (contactable
        ? renderOrgCard(
            Object.assign({}, contactable, {
              relationshipBadges: contactable.relationshipBadges,
              role: "CONTACTABLE_OWNER_ORG",
            }),
            { title: "Contactable owner organization", prominent: true }
          )
        : "") +
      (propco ? renderOrgCard(propco, { title: "Property owner / PropCo" }) : "") +
      (contactable && contactable.relationshipBadges && contactable.relationshipBadges.indexOf("OPERATOR") >= 0
        ? '<article class="hci-org-card"><div class="hci-org-card__head"><div class="hci-org-card__kicker">Current operator</div><h4>Grupo Hotelero Santa Fe</h4><div class="hci-org-card__meta"><span class="hci-rel-badge">OPERATOR</span><span class="hci-rel-badge">OWNER / SPONSOR</span></div></div><p class="hci-note">Same organization also acts as current operator — contact details shown on the contactable owner organization card.</p></article>'
        : "") +
      (brand ? renderOrgCard(brand, { title: "Current brand" }) : "") +
      "</div></section>" +
      renderPeopleGroups(vm) +
      best +
      unresolved +
      "</div>"
    );
  }

  function paintEnrichResult(slot, payload) {
    if (!slot) return;
    slot.removeAttribute("hidden");
    if (!payload || !payload.ok) {
      slot.innerHTML =
        '<div class="hci-note hci-note--unresolved">' +
        esc((payload && (payload.message || payload.error)) || "Enrichment failed") +
        "</div>";
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
        '<div class="hci-note">Existing verified channels found — no Surfe credit used.</div>' +
        (emails.length
          ? "<div><strong>Email:</strong> " + esc(emails.join(", ")) + "</div>"
          : "") +
        (phones.length
          ? "<div><strong>Phone:</strong> " + esc(phones.join(", ")) + "</div>"
          : "");
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
    slot.innerHTML =
      '<div class="hci-note">Surfe on-demand result · ephemeral · review required before Airtable persistence · provider attribution: Surfe</div>' +
      (email ? "<div><strong>Email (provider):</strong> " + esc(email) + "</div>" : unresolvedNote("No email returned by provider")) +
      (phone ? "<div><strong>Phone (provider):</strong> " + esc(phone) + "</div>" : unresolvedNote("No phone returned by provider")) +
      '<p class="hci-muted">Channel verification status: Provider returned / unreviewed. Identity confidence is separate from channel confidence. Not persisted.</p>';
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
      btn.disabled = true;
      btn.textContent = "Looking up…";
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
          paintEnrichResult(slot, pack.body || { ok: false, error: "http_" + pack.status });
        })
        .catch(function (err) {
          paintEnrichResult(slot, {
            ok: false,
            error: "network_error",
            message: String((err && err.message) || err),
          });
        })
        .finally(function () {
          delete _inflightReveal[key];
          btn.disabled = false;
          btn.textContent = "Find Contact Details";
        });
    });
  }

  function bindToggles(root) {
    if (!root) return;
    root.addEventListener("click", function (ev) {
      var btn = ev.target.closest("[data-hci-toggle]");
      if (!btn) return;
      var id = btn.getAttribute("data-hci-toggle");
      var el = document.getElementById(id);
      if (!el) return;
      var open = el.hasAttribute("hidden");
      if (open) el.removeAttribute("hidden");
      else el.setAttribute("hidden", "");
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
      ? '<a class="hci-channel__value" href="tel:' +
        esc(String(phone).replace(/\s+/g, "")) +
        '">' +
        esc(phone) +
        "</a>"
      : '<span class="hci-channel__value hci-channel--empty">Not published on HPC</span>';
    var websiteHtml = website
      ? '<a class="hci-channel__value" href="' +
        esc(website) +
        '" target="_blank" rel="noopener">' +
        esc(website) +
        "</a>"
      : '<span class="hci-channel__value hci-channel--empty">Not published on HPC</span>';
    var summaryHtml = "";
    if (ownershipSummary) {
      summaryHtml =
        '<section class="hci-section"><div class="hci-section__head"><h3>Ownership summary (graph)</h3>' +
        '<p class="hci-note">Derived from HI graph + HPC property channels. Named decision-makers are not verified.</p></div>' +
        '<div class="hci-channel"><div class="hci-channel__main"><span class="hci-channel__type">ORGANIZATION</span><span class="hci-channel__value">' +
        esc(ownershipSummary.organization || "—") +
        '</span></div></div>' +
        '<div class="hci-channel"><div class="hci-channel__main"><span class="hci-channel__type">OPERATOR</span><span class="hci-channel__value">' +
        esc(ownershipSummary.operator || "—") +
        '</span></div></div>' +
        '<div class="hci-channel"><div class="hci-channel__main"><span class="hci-channel__type">ECONOMIC OWNER</span><span class="hci-channel__value">' +
        esc(
          ownershipSummary.economic_owner_verified
            ? "Verified"
            : "Not verified"
        ) +
        "</span></div></div></section>";
    }
    return (
      '<div class="hci-root" data-hci-state="hpc-fallback">' +
      summaryHtml +
      '<section class="hci-section">' +
      '<div class="hci-section__head"><h3>Property contacts (HPC fallback)</h3>' +
      '<p class="hci-note">Read-only fallback from ' +
      esc(source) +
      " for " +
      esc(hotelName || hotelId) +
      ". Not HI Contact Channels / person enrichment. Person-level Contact Intelligence staging is unavailable for this hotel.</p></div>" +
      '<div class="hci-channel"><div class="hci-channel__main">' +
      '<span class="hci-channel__type">PROPERTY PHONE</span>' +
      phoneHtml +
      '<span class="hci-badge hci-badge--public">HPC FIELD</span>' +
      '<span class="hci-src">HOTEL PROPERTY CENSUS</span>' +
      "</div></div>" +
      '<div class="hci-channel"><div class="hci-channel__main">' +
      '<span class="hci-channel__type">PROPERTY WEBSITE</span>' +
      websiteHtml +
      '<span class="hci-badge hci-badge--public">HPC FIELD</span>' +
      '<span class="hci-src">HOTEL PROPERTY CENSUS</span>' +
      "</div></div>" +
      '<p class="hci-note">Provenance: source=' +
      esc((contacts && contacts.source) || "HPC_PROPERTY_FALLBACK") +
      "; quality=PROPERTY_FALLBACK_ONLY; no Contact Intelligence write on GET.</p>" +
      "</section></div>"
    );
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
        var contacts =
          (json && json.report && json.report.corporate_contacts) || null;
        if (
          !contacts ||
          (!contacts.property_phone && !contacts.website)
        ) {
          return null;
        }
        var control =
          (json.report && json.report.ownership_and_control) || {};
        return {
          hotelId: hotelId,
          hotelName:
            (json.hotel && (json.hotel.name || json.hotel.display_name)) ||
            hotelId,
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
          quality_summary: {
            status: "PROPERTY_FALLBACK_ONLY",
            named_contacts: 0,
            property_channels:
              (contacts.property_phone ? 1 : 0) + (contacts.website ? 1 : 0),
            source: contacts.source || "HPC_PROPERTY_FALLBACK",
          },
          fallback_paths: {
            property_phone: contacts.property_phone || null,
            website: contacts.website || null,
            source: contacts.source || "HPC_PROPERTY_FALLBACK",
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
    tries.push("/api/contact-intelligence/hotels/" + encodeURIComponent(hotelId) + "?audience=internal");

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
            return { hotelId: hotelId, hotelName: hotelId, _rawPackage: json.package, people: [], organizations: [] };
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
        return {
          surfeEnabled: Boolean(j && j.surfeEnabled),
          reason: (j && j.reason) || null,
          hasApiKey: Boolean(j && j.hasApiKey),
        };
      })
      .catch(function () {
        return {
          surfeEnabled: false,
          reason: "Could not read Surfe enrichment capability",
          hasApiKey: false,
        };
      });
  }

  function mount(host, hotelId) {
    if (!host) return Promise.resolve();
    host.innerHTML = renderLoading();
    return Promise.all([loadViewModel(hotelId), loadEnrichmentCapability()]).then(function (pack) {
      var vm = pack[0];
      vmCapability = pack[1] || null;
      if (vm && vm.ownershipSummary) {
        // Enable Find Contact for eligible people when Surfe is live-capable
        if (vmCapability && vmCapability.surfeEnabled && Array.isArray(vm.people)) {
          vm.people = vm.people.map(function (p) {
            var next = Object.assign({}, p);
            if (!next.enrichAction) next.enrichAction = { label: "Find Contact Details" };
            if (next.enrichAction.enabled !== true) {
              next.enrichAction = Object.assign({}, next.enrichAction, {
                enabled: true,
                reason: null,
              });
            }
            return next;
          });
        }
        host.innerHTML = renderViewModel(vm);
        bindToggles(host);
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
          "Contact Intelligence staging is not available for this hotel yet. KGPV showcase data is local/dev only."
        );
      });
    });
  }

  global.HotelContactIntelligence = {
    renderLoading: renderLoading,
    renderEmpty: renderEmpty,
    renderHpcPropertyFallback: renderHpcPropertyFallback,
    renderViewModel: renderViewModel,
    mount: mount,
    loadViewModel: loadViewModel,
    loadHpcPropertyFallback: loadHpcPropertyFallback,
  };
})(typeof window !== "undefined" ? window : globalThis);
