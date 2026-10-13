/**
 * Hotel Explorer — Contact Intelligence tab (generic renderer).
 * Loads ContactIntelligenceViewModel from staging/API. No Surfe live calls.
 */
(function (global) {
  "use strict";

  var SHOWCASE_PATHS = {
    recUNycnMwOVFX0hc: "/data/hotel-contact-intelligence/kgpv-recUNycnMwOVFX0hc.json",
  };

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
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
      esc((o.propco && (o.propco.shortName || o.propco.name)) || "—") +
      "</div></div>" +
      '<div class="hci-chain__arrow" aria-hidden="true">↓ sponsored / controlled by</div>' +
      '<div class="hci-chain__node hci-chain__node--focus"><div class="hci-chain__label">' +
      esc((o.sponsor && o.sponsor.label) || "Economic Owner") +
      '</div><div class="hci-chain__value">' +
      esc((o.sponsor && o.sponsor.name) || "—") +
      "</div></div>" +
      '<div class="hci-chain__arrow" aria-hidden="true">↓ operated by</div>' +
      '<div class="hci-chain__node"><div class="hci-chain__label">' +
      esc((o.operator && o.operator.label) || "Current Operator") +
      '</div><div class="hci-chain__value">' +
      esc((o.operator && o.operator.name) || "—") +
      (o.operator && o.operator.sameAsSponsor
        ? '<div class="hci-muted">Same organization also acts as current operator</div>'
        : "") +
      "</div></div>" +
      '<div class="hci-chain__arrow" aria-hidden="true">↓ branded as</div>' +
      '<div class="hci-chain__node"><div class="hci-chain__label">' +
      esc((o.brand && o.brand.label) || "Current Brand") +
      '</div><div class="hci-chain__value">' +
      esc((o.brand && o.brand.name) || "—") +
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

  function renderPerson(p) {
    var emails = (p.emailContacts || []).map(renderChannel).join("") ||
      '<div class="hci-channel hci-channel--empty">No direct email verified</div>';
    var phones = (p.phoneContacts || []).map(renderChannel).join("") ||
      '<div class="hci-channel hci-channel--empty">Public phone not found</div>';
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
        " — " +
        esc(p.fallbackPath.organization) +
        "</p>" +
        (p.fallbackPath.channels || [])
          .map(function (c) {
            return "<div>" + esc(c.label || c.type) + ": " + esc(c.value) + "</div>";
          })
          .join("") +
        "</div>";
    }
    var enrich =
      p.enrichAction && !p.enrichAction.enabled
        ? '<button type="button" class="hci-btn hci-btn--disabled" disabled title="' +
          esc(p.enrichAction.reason || "Unavailable") +
          '">' +
          esc(p.enrichAction.label || "Find Contact Details") +
          "</button>"
        : "";
    var detailsId = "hci-person-" + esc(p.personId || Math.random().toString(36).slice(2));
    return (
      '<article class="hci-person" data-person-id="' +
      esc(p.personId || "") +
      '" data-display-state="' +
      esc(p.displayState || "") +
      '">' +
      '<div class="hci-person__head">' +
      "<div><h5>" +
      esc(p.name) +
      "</h5>" +
      '<div class="hci-person__title">' +
      esc(p.title || "—") +
      "</div>" +
      '<div class="hci-muted">' +
      esc(p.organization || "") +
      " · " +
      esc(p.stakeholderClass || "") +
      "</div></div>" +
      '<span class="' +
      badgeClass(p.displayState === "AVAILABLE" ? "VERIFIED_PUBLIC" : p.displayState) +
      '">' +
      esc(p.displayState || "UNRESOLVED") +
      "</span></div>" +
      '<div class="hci-kv"><span>Why relevant</span><strong>' +
      esc(p.whyRelevant || "—") +
      "</strong></div>" +
      '<div class="hci-kv"><span>Contactability</span><strong>' +
      esc(p.contactability || "—") +
      "</strong></div>" +
      '<div class="hci-kv"><span>Confidence</span><strong>' +
      esc(p.confidence || "—") +
      "</strong></div>" +
      '<div class="hci-kv"><span>Last verified</span><strong>' +
      esc(p.lastVerified || "—") +
      "</strong></div>" +
      (profiles ? '<div class="hci-profiles">' + profiles + "</div>" : '<div class="hci-muted">Profile: Not yet verified</div>') +
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
      " (not inferred)</p>" +
      enrich +
      "</div></article>"
    );
  }

  function renderPeopleGroups(vm) {
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
          rows.map(renderPerson).join("") +
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
      '<details class="hci-dev"><summary>Dev diagnostic</summary><pre class="hci-dev-json">' +
      esc(JSON.stringify(vm, null, 2)) +
      "</pre></details>" +
      "</div>"
    );
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

  function mount(host, hotelId) {
    if (!host) return Promise.resolve();
    host.innerHTML = renderLoading();
    return loadViewModel(hotelId).then(function (vm) {
      if (!vm || !vm.ownershipSummary) {
        host.innerHTML = renderEmpty(
          "Contact Intelligence staging is not available for this hotel yet. KGPV showcase data is local/dev only."
        );
        return;
      }
      host.innerHTML = renderViewModel(vm);
      bindToggles(host);
    });
  }

  global.HotelContactIntelligence = {
    renderLoading: renderLoading,
    renderEmpty: renderEmpty,
    renderViewModel: renderViewModel,
    mount: mount,
    loadViewModel: loadViewModel,
  };
})(typeof window !== "undefined" ? window : globalThis);
