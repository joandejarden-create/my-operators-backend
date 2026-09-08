/**
 * Packet 2.6B-R9 Combined — Continuous Deep Research document.
 * Shared semantic report for screen + print. Canonical Download PDF via Playwright.
 * Cover is the only intentional full A4 page.
 */
(function (global) {
  "use strict";

  var HID_BUILD_ID = "2.7-R6";
  var HID_ASSET_VERSION = "28";

  var DEALALITY_LOGO_URL =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png";

  var STATUS_LABELS = {
    VERIFIED_INTELLIGENCE: "Verified Intelligence",
    RESEARCH_FINDING: "Research Finding",
    UNRESOLVED: "Unresolved",
    CONTRADICTORY_EVIDENCE: "Contradictory Evidence",
    SUPERSEDED_FINDING: "Superseded Finding",
  };

  /** Deprecated packing budget — retained for API compat; continuous flow does not pack sheets. */
  var PAGE_CHAR_BUDGET = 0;
  var ZOOM_PRESETS = { fit_page: "fit_page", fit_width: "fit_width", pct: "pct" };
  var SESSION_ZOOM_KEY = "hid_reader_zoom_v1";

  function esc(t) {
    return String(t == null ? "" : t)
      .replace(/\u00ad/g, "") // soft hyphen — never leak into PDF text layer
      .replace(/\ufffe|\ufffd/g, "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function formatDate(iso) {
    if (!iso) return "—";
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return String(iso).slice(0, 10);
      return d.toLocaleDateString(undefined, { year: "numeric", month: "long", day: "numeric" });
    } catch (_) {
      return String(iso).slice(0, 10);
    }
  }

  function statusChip(status) {
    var s = String(status || "RESEARCH_FINDING");
    var label = STATUS_LABELS[s] || s.replace(/_/g, " ");
    return (
      '<span class="hid-status hid-status--' +
      esc(s.toLowerCase()) +
      '">' +
      esc(label) +
      "</span>"
    );
  }

  /**
   * Packet 2.6B-R9.1 — single canonical citation component.
   * Visible text is ONLY "[n]". Never concatenate number + '">' fragments.
   */
  function renderCitation(sourceNumber) {
    var n = String(sourceNumber == null ? "" : sourceNumber).replace(/[^\d]/g, "");
    if (!n) return "";
    return (
      '<button type="button" class="hid-cite" data-hid-cite="' +
      esc(n) +
      '" aria-label="Open citation ' +
      esc(n) +
      '">[' +
      esc(n) +
      "]</button>"
    );
  }

  function citeButtons(nums) {
    if (!nums || !nums.length) return "";
    return nums.map(renderCitation).filter(Boolean).join("");
  }

  function citeClusterHtml(citesToken, opts) {
    opts = opts || {};
    var html = String(citesToken || "").replace(/\[(\d+)\]/g, function (_m, n) {
      return (opts.nbspBefore ? "&nbsp;" : "") + renderCitation(n);
    });
    return html;
  }

  /**
   * Keep citation tokens attached to the nearest word only (R7 orphan guard)
   * without wrapping long prose in nowrap (R9.1 mid-word break repair).
   *
   * CRITICAL: tokenize on plain text first, then escape — never run citation
   * regexes on HTML that already contains data-hid-cite="n">[n] (caused "1\"> [1]").
   */
  function richText(text) {
    var raw = String(text == null ? "" : text);
    var tokens = [];
    var re = /(\[(?:\d+)\])+/g;
    var last = 0;
    var m;
    while ((m = re.exec(raw)) !== null) {
      if (m.index > last) {
        tokens.push({ type: "text", value: raw.slice(last, m.index) });
      }
      tokens.push({ type: "cites", value: m[0] });
      last = m.index + m[0].length;
    }
    if (last < raw.length) tokens.push({ type: "text", value: raw.slice(last) });

    var out = [];
    for (var i = 0; i < tokens.length; i++) {
      var tok = tokens[i];
      if (tok.type === "text") {
        var next = tokens[i + 1];
        if (next && next.type === "cites") {
          var trimmed = tok.value.replace(/\s+$/, "");
          var wordMatch = trimmed.match(/^(.*?)(\S+)$/);
          if (wordMatch) {
            out.push(esc(wordMatch[1]));
            out.push(
              '<span class="hid-cite-keep">' +
                esc(wordMatch[2]) +
                citeClusterHtml(next.value, { nbspBefore: true }) +
                "</span>"
            );
            i += 1;
            continue;
          }
        }
        out.push(esc(tok.value));
        continue;
      }
      // cites token — attach to following word when present
      var fol = tokens[i + 1];
      if (fol && fol.type === "text") {
        var lead = fol.value.match(/^(\s*)(\S+)([\s\S]*)$/);
        if (lead && lead[2]) {
          out.push(
            '<span class="hid-cite-keep">' +
              citeClusterHtml(tok.value, { nbspBefore: false }) +
              "&nbsp;" +
              esc(lead[2]) +
              "</span>"
          );
          tokens[i + 1] = { type: "text", value: lead[3] || "" };
          continue;
        }
      }
      out.push(
        '<span class="hid-cite-keep">' + citeClusterHtml(tok.value, { nbspBefore: false }) + "</span>"
      );
    }
    return out.join("");
  }

  function classifyTable(headers) {
    var h = (headers || []).map(function (x) {
      return String(x || "").toLowerCase().trim();
    });
    var joined = h.join("|");
    var isPeople =
      h.indexOf("name") >= 0 &&
      (h.indexOf("title") >= 0 || h.indexOf("organization") >= 0) &&
      h.length >= 5;
    if (isPeople) return "people";
    if (h.indexOf("level") >= 0 && h.indexOf("entity") >= 0) return "ownership";
    if (h.indexOf("period") >= 0 && /brand|trading/.test(joined)) return "brand_chronology";
    if (h.indexOf("year") >= 0 && h.indexOf("event") >= 0) return "property_history";
    if (
      (h.indexOf("#") >= 0 || h.indexOf("title") >= 0) &&
      (h.indexOf("url") >= 0 || h.indexOf("publisher") >= 0) &&
      h.indexOf("type") >= 0
    ) {
      return "sources";
    }
    if (h.indexOf("channel") >= 0 && h.indexOf("contact") >= 0) return "contact";
    if (h.indexOf("property") >= 0 && h.indexOf("rooms") >= 0) return "portfolio";
    if ((headers || []).length <= 4) return "executive";
    return "evidence";
  }

  function headerIndexMap(headers) {
    var idx = {};
    (headers || []).forEach(function (h, i) {
      idx[String(h || "").toLowerCase().trim()] = i;
    });
    return idx;
  }

  function cellByKeys(row, idx, keys) {
    for (var i = 0; i < keys.length; i++) {
      var k = keys[i].toLowerCase();
      if (idx[k] != null && row[idx[k]] != null && String(row[idx[k]]).trim()) {
        return String(row[idx[k]]);
      }
    }
    return "";
  }

  function confidenceBadge(raw) {
    var c = String(raw || "").trim() || "—";
    var slug = c.toLowerCase().replace(/[^a-z0-9]+/g, "-");
    return (
      '<span class="hid-conf hid-conf--' +
      esc(slug) +
      '">' +
      esc(c) +
      "</span>"
    );
  }

  function statusConfidenceInline(status, confidence) {
    var parts = [];
    if (status) parts.push(esc(status));
    if (confidence) parts.push(confidenceBadge(confidence));
    return parts.length
      ? '<span class="hid-status-inline">' + parts.join(" · ") + "</span>"
      : "";
  }

  function linkifyUrl(url, label) {
    var u = String(url || "").trim();
    if (!u) return "";
    var safe = esc(u);
    var text = label ? esc(label) : safe;
    if (/^https?:\/\//i.test(u)) {
      return (
        '<a class="hid-ext-link" href="' +
        safe +
        '" target="_blank" rel="noopener noreferrer">' +
        text +
        ' <span class="hid-ext-link__mark" aria-hidden="true">↗</span></a>'
      );
    }
    return text;
  }

  function isVerifiedLinkedInProfileUrl(url) {
    if (!url) return false;
    try {
      var u = new URL(String(url).trim());
      if (!/^https?:$/i.test(u.protocol)) return false;
      if (!/(^|\.)linkedin\.com$/i.test(u.hostname)) return false;
      // Exact person profiles only — never company pages or search
      return /\/in\/[A-Za-z0-9\-_%]+\/?$/i.test(u.pathname);
    } catch (_) {
      return false;
    }
  }

  function extractLinkedInProfileUrl(text) {
    var s = String(text || "");
    var m = s.match(/https?:\/\/(?:[a-z]+\.)?linkedin\.com\/in\/[A-Za-z0-9\-_%]+\/?/i);
    if (!m) return "";
    var url = m[0].replace(/[),.;]+$/, "");
    return isVerifiedLinkedInProfileUrl(url) ? url : "";
  }

  function normalizePersonKey(name) {
    return String(name || "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  }

  /**
   * Professional profiles already present in the KGPV dossier corpus.
   * Do NOT add URLs that are not already captured in research/source records.
   */
  var CORPUS_PROFESSIONAL_PROFILES = {
    "francisco medina elizalde": {
      professional_profile_url:
        "https://mx.linkedin.com/in/francisco-medina-elizalde-83aa26a3",
      professional_profile_type: "LINKEDIN",
      professional_profile_verified: true,
      profile_source:
        "fixtures/hotel-intelligence/dossier Public contact + source-archive/modules/06-people-decision-authority.md",
      profile_match_basis:
        "Exact name match; evidence states LinkedIn profile confirms CEO role at GSF",
      profile_verified_at: "2026-09-04",
    },
  };

  function resolveProfessionalProfile(personName, publicContact) {
    var fromContact = extractLinkedInProfileUrl(publicContact);
    var key = normalizePersonKey(personName);
    var fromCorpus = CORPUS_PROFESSIONAL_PROFILES[key] || null;
    var url = fromContact || (fromCorpus && fromCorpus.professional_profile_url) || "";
    if (!isVerifiedLinkedInProfileUrl(url)) {
      return {
        professional_profile_url: "",
        professional_profile_type: "",
        professional_profile_verified: false,
        profile_source: "",
        profile_match_basis: "",
        profile_verified_at: "",
      };
    }
    return {
      professional_profile_url: url,
      professional_profile_type: "LINKEDIN",
      professional_profile_verified: true,
      profile_source:
        (fromCorpus && fromCorpus.profile_source) ||
        "dossier Public contact field",
      profile_match_basis:
        (fromCorpus && fromCorpus.profile_match_basis) ||
        "Exact LinkedIn /in/ URL present in dossier corpus",
      profile_verified_at:
        (fromCorpus && fromCorpus.profile_verified_at) || "",
    };
  }

  function stripLinkedInFromContact(contact, profileUrl) {
    var s = String(contact || "");
    if (profileUrl) {
      s = s.replace(profileUrl, "");
    }
    s = s.replace(/LinkedIn\s*:\s*/gi, "");
    s = s.replace(/https?:\/\/(?:[a-z]+\.)?linkedin\.com\/in\/[A-Za-z0-9\-_%]+\/?/gi, "");
    s = s.replace(/\s{2,}/g, " ").replace(/^[\s;,:·-]+|[\s;,:·-]+$/g, "").trim();
    // Only clear empty leftovers like "LinkedIn:" — keep notes such as
    // "LinkedIn via GSF company page" (not a profile URL).
    if (!s || /^linkedin\s*:?\s*$/i.test(s)) return "";
    return s;
  }

  function renderProfessionalProfileField(profile) {
    var url = profile && profile.professional_profile_url;
    if (!url || !profile.professional_profile_verified) {
      return (
        '<p class="hid-people-card__label">Professional Profile</p>' +
        '<p class="hid-body hid-profile-line hid-profile-line--empty">—</p>'
      );
    }
    return (
      '<p class="hid-people-card__label">Professional Profile</p>' +
      '<p class="hid-body hid-profile-line">' +
      '<a class="hid-ext-link hid-profile-link" href="' +
      esc(url) +
      '" target="_blank" rel="noopener noreferrer">' +
      "View profile" +
      ' <span class="hid-ext-link__mark" aria-hidden="true">↗</span></a>' +
      "</p>"
    );
  }

  function renderPeopleCards(headers, rows) {
    var idx = headerIndexMap(headers);
    function cell(row, key) {
      return cellByKeys(row, idx, [key]);
    }
    var html = '<div class="hid-people-grid">';
    rows.forEach(function (row) {
      var name = cell(row, "name");
      var title = cell(row, "title");
      var org = cell(row, "organization");
      var role = cellByKeys(row, idx, ["role / relevance", "role/relevance"]);
      var evidence = cell(row, "evidence");
      var contactRaw = cell(row, "public contact");
      var profile = resolveProfessionalProfile(name, contactRaw);
      var contact = stripLinkedInFromContact(contactRaw, profile.professional_profile_url);
      html +=
        '<article class="hid-people-card" data-hid-person="' +
        esc(name) +
        '"' +
        (profile.professional_profile_url
          ? ' data-hid-profile-url="' + esc(profile.professional_profile_url) + '"'
          : "") +
        ">";
      html += '<h4 class="hid-people-card__name">' + esc(name) + "</h4>";
      html +=
        '<p class="hid-people-card__meta"><strong>' +
        esc(title) +
        "</strong>" +
        (org ? " · " + esc(org) : "") +
        "</p>";
      if (role) {
        html +=
          '<p class="hid-people-card__label">Role / Relevance</p><p class="hid-body">' +
          richText(role) +
          "</p>";
      }
      if (evidence) {
        html +=
          '<p class="hid-people-card__label">Evidence</p><p class="hid-body">' +
          richText(evidence) +
          "</p>";
      }
      html += renderProfessionalProfileField(profile);
      html +=
        '<p class="hid-people-card__label">Public Contact</p><p class="hid-body hid-contact-line">' +
        (contact ? richText(contact) : "—") +
        "</p>";
      html += "</article>";
    });
    html += "</div>";
    return html;
  }

  function renderScreenGridTable(headers, rows, kind) {
    var cls =
      "bas-brief-table hid-table hid-table--" +
      esc(kind) +
      (kind === "executive" || kind === "contact" || kind === "portfolio"
        ? " hid-table--compact"
        : " hid-table--evidence");
    var html =
      '<div class="bas-table-wrap hid-table-wrap hid-table-wrap--' +
      esc(kind) +
      '"><table class="' +
      cls +
      '"><thead><tr>';
    headers.forEach(function (h) {
      html += '<th scope="col">' + esc(h) + "</th>";
    });
    html += "</tr></thead><tbody>";
    rows.forEach(function (row) {
      html += "<tr>";
      row.forEach(function (cell, ci) {
        var h = String(headers[ci] || "").toLowerCase();
        var isUrl = h === "url" || /^https?:\/\//i.test(String(cell || ""));
        html +=
          '<td class="hid-td-' +
          esc(h.replace(/[^a-z0-9]+/g, "-") || "cell") +
          '">' +
          (isUrl && h === "url" ? linkifyUrl(cell, "Open source") : richText(cell)) +
          "</td>";
      });
      html += "</tr>";
    });
    html += "</tbody></table></div>";
    return html;
  }

  function renderOwnershipPrint(headers, rows) {
    var idx = headerIndexMap(headers);
    var html = '<div class="hid-print-stack hid-print-stack--ownership" data-hid-layout="ownership">';
    rows.forEach(function (row) {
      var level = cellByKeys(row, idx, ["level", "role", "relationship"]);
      var entity = cellByKeys(row, idx, ["entity"]);
      var conf = cellByKeys(row, idx, ["confidence"]);
      var evidence = cellByKeys(row, idx, ["evidence"]);
      html += '<article class="hid-research-row">';
      html +=
        '<header class="hid-research-row__head"><span class="hid-research-row__role">' +
        esc(level) +
        "</span> " +
        confidenceBadge(conf) +
        "</header>";
      html += '<p class="hid-research-row__entity">' + richText(entity) + "</p>";
      if (evidence) {
        html +=
          '<div class="hid-research-row__evidence-block">' +
          '<p class="hid-research-row__evidence-label">Evidence</p><p class="hid-research-row__evidence">' +
          richText(evidence) +
          "</p></div>";
      }
      html += "</article>";
    });
    html += "</div>";
    return html;
  }

  function renderBrandChronologyPrint(headers, rows) {
    var idx = headerIndexMap(headers);
    var html =
      '<div class="hid-print-stack hid-print-stack--brand" data-hid-layout="brand_chronology">';
    rows.forEach(function (row) {
      var period = cellByKeys(row, idx, ["period"]);
      var brand = cellByKeys(row, idx, ["brand/trading name", "brand", "trading name"]);
      var status = cellByKeys(row, idx, ["status"]);
      var conf = cellByKeys(row, idx, ["confidence"]);
      var evidence = cellByKeys(row, idx, ["evidence"]);
      html += '<article class="hid-timeline-block">';
      html += '<p class="hid-timeline-block__when">' + esc(period) + "</p>";
      html += '<h4 class="hid-timeline-block__title">' + esc(brand) + "</h4>";
      html +=
        '<p class="hid-timeline-block__status">' +
        statusConfidenceInline(status, conf) +
        "</p>";
      if (evidence) {
        html += '<p class="hid-timeline-block__evidence">' + richText(evidence) + "</p>";
      }
      html += "</article>";
    });
    html += "</div>";
    return html;
  }

  function renderPropertyTimelinePrint(headers, rows) {
    var idx = headerIndexMap(headers);
    var html =
      '<div class="hid-print-stack hid-print-stack--history" data-hid-layout="property_history">';
    rows.forEach(function (row) {
      var year = cellByKeys(row, idx, ["year", "year / date", "date"]);
      var event = cellByKeys(row, idx, ["event"]);
      var conf = cellByKeys(row, idx, ["confidence"]);
      var source = cellByKeys(row, idx, ["source", "source / status"]);
      // Vertical stacked event — never a 2-column date/event grid (print fragmentation).
      html += '<article class="hid-timeline-block hid-timeline-block--history">';
      html +=
        '<p class="hid-timeline-block__when">' +
        esc(year) +
        "</p>";
      html +=
        '<p class="hid-timeline-block__title">' +
        richText(event) +
        "</p>";
      html +=
        '<p class="hid-timeline-block__status">' +
        confidenceBadge(conf) +
        "</p>";
      if (source) {
        html +=
          '<p class="hid-timeline-block__evidence-label">Evidence</p>' +
          '<p class="hid-timeline-block__evidence">' +
          richText(source) +
          "</p>";
      }
      html += "</article>";
    });
    html += "</div>";
    return html;
  }

  function renderSourcesBibliography(headers, rows) {
    var idx = headerIndexMap(headers);
    var html =
      '<ol class="hid-bibliography" data-hid-layout="bibliography" start="1">';
    rows.forEach(function (row) {
      var num = cellByKeys(row, idx, ["#", "no", "n"]);
      var title = cellByKeys(row, idx, ["title"]);
      var publisher = cellByKeys(row, idx, ["publisher"]);
      var type = cellByKeys(row, idx, ["type", "source type"]);
      var url = cellByKeys(row, idx, ["url"]);
      html += '<li class="hid-bibliography__item" value="' + esc(num || "") + '">';
      html +=
        '<p class="hid-bibliography__title"><span class="hid-bibliography__num">[' +
        esc(num || "") +
        "]</span> " +
        esc(title) +
        "</p>";
      html += '<p class="hid-bibliography__meta">';
      if (publisher) html += "<span>Publisher: " + esc(publisher) + "</span>";
      if (type) {
        html +=
          (publisher ? '<span class="hid-bibliography__sep">·</span>' : "") +
          "<span>Source type: " +
          esc(type) +
          "</span>";
      }
      html += "</p>";
      if (url) {
        html +=
          '<p class="hid-bibliography__url">URL: ' +
          linkifyUrl(url, title || url) +
          "</p>";
      }
      html += "</li>";
    });
    html += "</ol>";
    return html;
  }

  function renderContactPrint(headers, rows) {
    var idx = headerIndexMap(headers);
    var html =
      '<div class="hid-contact-block" data-hid-keep-together="1">' +
      '<table class="hid-contact-table" data-hid-layout="contact"><colgroup>' +
      '<col class="hid-contact-col-channel" />' +
      '<col class="hid-contact-col-contact" />' +
      '<col class="hid-contact-col-basis" />' +
      "</colgroup><thead><tr>" +
      "<th>Channel</th><th>Contact</th><th>Source basis</th></tr></thead><tbody>";
    rows.forEach(function (row) {
      html +=
        "<tr><td>" +
        esc(cellByKeys(row, idx, ["channel"])) +
        '</td><td class="hid-contact-line">' +
        richText(cellByKeys(row, idx, ["contact"])) +
        "</td><td>" +
        richText(cellByKeys(row, idx, ["source basis", "source"])) +
        "</td></tr>";
    });
    html += "</tbody></table></div>";
    return html;
  }

  function renderPortfolioPrint(headers, rows) {
    var idx = headerIndexMap(headers);
    var html = '<div class="hid-print-stack hid-print-stack--portfolio">';
    rows.forEach(function (row) {
      html += '<article class="hid-research-row hid-research-row--compact">';
      html +=
        '<p class="hid-research-row__entity">' +
        esc(cellByKeys(row, idx, ["property"])) +
        "</p>";
      html +=
        '<p class="hid-body">' +
        esc(cellByKeys(row, idx, ["rooms"])) +
        " rooms · " +
        esc(cellByKeys(row, idx, ["ownership"])) +
        " · " +
        esc(cellByKeys(row, idx, ["status"])) +
        "</p>";
      var addr = cellByKeys(row, idx, ["address"]);
      if (addr) html += '<p class="hid-muted">' + esc(addr) + "</p>";
      html += "</article>";
    });
    html += "</div>";
    return html;
  }

  function renderEvidenceStackedPrint(headers, rows) {
    var html = '<div class="hid-print-stack hid-print-stack--evidence">';
    rows.forEach(function (row) {
      html += '<article class="hid-research-row">';
      headers.forEach(function (h, i) {
        var val = row[i];
        if (val == null || !String(val).trim()) return;
        var key = String(h || "").toLowerCase();
        if (i === 0) {
          html +=
            '<header class="hid-research-row__head"><span class="hid-research-row__role">' +
            esc(val) +
            "</span></header>";
        } else if (/evidence|note|detail|analysis/.test(key)) {
          html +=
            '<p class="hid-research-row__evidence-label">' +
            esc(h) +
            '</p><p class="hid-research-row__evidence">' +
            richText(val) +
            "</p>";
        } else if (/confidence/.test(key)) {
          html += '<p class="hid-timeline-block__status">' + confidenceBadge(val) + "</p>";
        } else {
          html +=
            '<p class="hid-body"><strong>' +
            esc(h) +
            ":</strong> " +
            richText(val) +
            "</p>";
        }
      });
      html += "</article>";
    });
    html += "</div>";
    return html;
  }

  function renderPrintVariant(kind, headers, rows) {
    if (kind === "ownership") return renderOwnershipPrint(headers, rows);
    if (kind === "brand_chronology") return renderBrandChronologyPrint(headers, rows);
    if (kind === "property_history") return renderPropertyTimelinePrint(headers, rows);
    if (kind === "sources") return renderSourcesBibliography(headers, rows);
    if (kind === "contact") return renderContactPrint(headers, rows);
    if (kind === "portfolio") return renderPortfolioPrint(headers, rows);
    if (kind === "executive") return renderScreenGridTable(headers, rows, kind);
    return renderEvidenceStackedPrint(headers, rows);
  }

  function renderTable(headers, rows, kindHint) {
    if (!rows || !rows.length) return '<p class="hid-body hid-muted">No rows available.</p>';
    var kind = kindHint || classifyTable(headers);
    if (kind === "people") return renderPeopleCards(headers, rows);
    // Institutional stacked layouts for dense research — shared screen + print (no dual grid).
    if (
      kind === "ownership" ||
      kind === "brand_chronology" ||
      kind === "property_history" ||
      kind === "sources" ||
      kind === "portfolio"
    ) {
      return (
        '<div class="hid-dual-present hid-dual-present--shared" data-hid-table-kind="' +
        esc(kind) +
        '">' +
        renderPrintVariant(kind, headers, rows) +
        "</div>"
      );
    }
    if (kind === "executive" || kind === "contact") {
      return (
        '<div class="hid-dual-present hid-dual-present--shared" data-hid-table-kind="' +
        esc(kind) +
        '">' +
        renderPrintVariant(kind, headers, rows) +
        "</div>"
      );
    }
    var screen = renderScreenGridTable(headers, rows, kind);
    var print = renderPrintVariant(kind, headers, rows);
    return (
      '<div class="hid-dual-present" data-hid-table-kind="' +
      esc(kind) +
      '">' +
      '<div class="hid-media-screen">' +
      screen +
      "</div>" +
      '<div class="hid-media-print">' +
      print +
      "</div>" +
      "</div>"
    );
  }

  function renderOwnershipDiagram(b) {
    var nodes = b.nodes || [];
    var html = '<div class="hid-ownership-diagram" role="img" aria-label="Ownership and control chain">';
    nodes.forEach(function (n, i) {
      html +=
        '<div class="hid-ownership-diagram__node hid-ownership-diagram__node--' +
        esc(n.role || "entity") +
        '"><span class="hid-ownership-diagram__role">' +
        esc(String(n.role || "").replace(/_/g, " ")) +
        '</span><span class="hid-ownership-diagram__name">' +
        esc(n.name || "") +
        "</span></div>";
      if (i < nodes.length - 1) {
        html += '<div class="hid-ownership-diagram__arrow" aria-hidden="true">↓</div>';
      }
    });
    if (b.note) {
      html += '<p class="hid-ownership-diagram__note">' + esc(b.note) + "</p>";
    }
    html += "</div>";
    return html;
  }

  function renderBlock(b) {
    if (!b) return "";
    if (b.type === "ownership_diagram") return renderOwnershipDiagram(b);
    if (b.type === "heading") {
      var lvl = Math.min(4, Math.max(2, Number(b.level) || 3));
      var cls = lvl <= 2 ? "hid-chapter-h" : "hid-subhead";
      return "<h" + lvl + ' class="' + cls + '">' + esc(b.text || "") + "</h" + lvl + ">";
    }
    if (b.type === "paragraphs") {
      return (b.paragraphs || [])
        .map(function (p) {
          return '<p class="hid-body">' + richText(p) + "</p>";
        })
        .join("");
    }
    // Legacy addendum blocks used type:"para" — render as paragraphs (R4.2).
    if (b.type === "para" || b.type === "paragraph") {
      var paraText = b.text || b.body || "";
      if (!paraText) return "";
      return '<p class="hid-body">' + richText(paraText) + "</p>";
    }
    if (b.type === "list") {
      var html = '<ul class="hid-list">';
      (b.items || []).forEach(function (item) {
        html += "<li>" + richText(item) + "</li>";
      });
      html += "</ul>";
      return html;
    }
    if (b.type === "table") return renderTable(b.headers || [], b.rows || [], b.table_kind);
    if (b.type === "finding_callout") {
      return (
        '<aside class="hid-finding-callout">' +
        statusChip(b.status) +
        '<strong class="hid-finding-callout__headline">' +
        esc(b.headline || "Finding") +
        "</strong>" +
        '<p class="hid-body hid-finding-callout__body">' +
        richText(b.body || b.explanation || "") +
        "</p></aside>"
      );
    }
    if (b.type === "callout") {
      return (
        '<aside class="hid-callout hid-callout--' +
        esc(b.kind || "note") +
        '"><div class="hid-callout__title">' +
        esc(b.title || "Note") +
        '</div><p class="hid-body">' +
        richText(b.body || "") +
        "</p></aside>"
      );
    }
    if (b.type === "people_profile") {
      var profile = resolveProfessionalProfile(
        b.name,
        b.professional_profile_url || b.contact || ""
      );
      if (b.professional_profile_url && isVerifiedLinkedInProfileUrl(b.professional_profile_url)) {
        profile = {
          professional_profile_url: b.professional_profile_url,
          professional_profile_type: b.professional_profile_type || "LINKEDIN",
          professional_profile_verified: b.professional_profile_verified !== false,
          profile_source: b.profile_source || "",
          profile_match_basis: b.profile_match_basis || "",
          profile_verified_at: b.profile_verified_at || "",
        };
      }
      var contactClean = stripLinkedInFromContact(
        b.contact || "",
        profile.professional_profile_url
      );
      return (
        '<article class="hid-people hid-people-card hid-people-card--block">' +
        '<h4 class="hid-subhead hid-people-card__name">' +
        esc(b.name || "") +
        '</h4><p class="hid-body hid-people-card__meta"><strong>' +
        esc(b.title || "") +
        "</strong> · " +
        esc(b.org || "") +
        '</p><p class="hid-body">' +
        richText(b.relevance || "") +
        '</p><p class="hid-muted">' +
        richText(b.evidence || "") +
        "</p>" +
        renderProfessionalProfileField(profile) +
        '<p class="hid-people-card__label">Public Contact</p><p class="hid-body hid-contact-line">' +
        (contactClean ? richText(contactClean) : "—") +
        "</p></article>"
      );
    }
    if (b.type === "trace") {
      var t = '<ol class="hid-trace">';
      (b.nodes || []).forEach(function (n) {
        var roleLabel =
          n.role_label ||
          String(n.role || "")
            .replace(/_/g, " ")
            .replace(/\b\w/g, function (c) {
              return c.toUpperCase();
            });
        if (/^propco$/i.test(String(n.role || ""))) roleLabel = "Property company";
        if (/^economic_owner$/i.test(String(n.role || ""))) roleLabel = "Economic owner";
        if (/^sponsor_principals$/i.test(String(n.role || ""))) roleLabel = "Sponsor / buyer principals";
        if (/^former_owner$/i.test(String(n.role || ""))) roleLabel = "Former owner";
        if (/^ubo$/i.test(String(n.role || ""))) roleLabel = "Ultimate beneficial owner";
        if (/^hotel$/i.test(String(n.role || ""))) roleLabel = "Hotel";
        var statusRaw = String(n.status || "").trim();
        var statusLabel = statusRaw
          .replace(/^HIGH$/i, "High")
          .replace(/^UNKNOWN$/i, "Unverified")
          .replace(/^PROBABLE$/i, "Probable")
          .replace(/^FORMER$/i, "Former")
          .replace(/^VERIFIED$/i, "Verified")
          .replace(/^CURRENT$/i, "Current")
          .replace(/^MEDIUM$/i, "Medium")
          .replace(/^LOW$/i, "Low");
        t +=
          '<li class="hid-trace__item">' +
          '<div class="hid-trace__head">' +
          '<strong class="hid-trace__role">' +
          esc(roleLabel) +
          "</strong>" +
          '<span class="hid-trace__entity"> — ' +
          esc(n.name || "—") +
          "</span>" +
          (statusLabel
            ? ' <span class="hid-trace__status">(' + esc(statusLabel) + ")</span>"
            : "") +
          "</div>" +
          (n.note ? '<p class="hid-trace__note">' + esc(n.note) + "</p>" : "") +
          "</li>";
      });
      t += "</ol>";
      return t;
    }
    if (b.type === "exec_group") {
      var g =
        '<div class="hid-exec-group"><h3 class="hid-exec-group__title">' +
        esc(b.title || "") +
        "</h3>";
      (b.paragraphs || []).forEach(function (p) {
        g += '<p class="hid-body">' + richText(p) + "</p>";
      });
      g += "</div>";
      return g;
    }
    return "";
  }

  function blockChars(b) {
    var html = renderBlock(b);
    // Web packing should ignore print-only twin markup so page density stays stable.
    var screenOnly = html.match(
      /hid-media-screen[^>]*>[\s\S]*?<\/div>\s*<div class="hid-media-print"/
    );
    if (screenOnly) {
      html = screenOnly[0].replace(/hid-media-print[\s\S]*$/, "");
    }
    return html.replace(/<[^>]+>/g, " ").length;
  }

  function wrapSheet(index, innerHtml, opts) {
    opts = opts || {};
    var continued = !!opts.continued;
    var title = opts.title || "";
    var band = "";
    if (title && !opts.cover) {
      band =
        '<div class="hid-page-band' +
        (continued ? " hid-page-band--continued" : "") +
        '"><span>' +
        esc(title) +
        (continued ? " (continued)" : "") +
        "</span></div>";
    }
    return (
      '<section class="hid-pdf-sheet' +
      (opts.cover ? " hid-pdf-sheet--cover" : "") +
      '" data-hid-page="' +
      index +
      '" data-hid-section="' +
      esc(opts.sectionId || "") +
      '" aria-label="Page ' +
      (index + 1) +
      '">' +
      band +
      '<div class="hid-pdf-sheet__body">' +
      innerHtml +
      "</div>" +
      '<footer class="hid-pdf-sheet__footer"><span>Dealality · Full Hotel Intelligence Investigation</span><span class="hid-pdf-sheet__num">' +
      (index + 1) +
      "</span></footer></section>"
    );
  }

  function packBlocksIntoPages(title, blocks, startIdx, sectionId) {
    var pages = [];
    var idx = startIdx;
    var current = [];
    var chars = 0;
    var pageInSection = 0;
    function onlyHeading(list) {
      return list.length > 0 && list.every(function (b) {
        return b && (b.type === "heading" || (b.type === "exec_group" && !(b.paragraphs || []).length));
      });
    }
    function pushPage(force) {
      if (!current.length) return;
      if (!force && onlyHeading(current)) return; // keep orphan headings with following content
      var heading =
        pageInSection === 0
          ? '<h2 class="hid-chapter-h">' + esc(title) + "</h2>"
          : "";
      var inner = heading + current.map(renderBlock).join("");
      pages.push({
        index: idx,
        html: wrapSheet(idx, inner, {
          title: title,
          continued: pageInSection > 0,
          sectionId: sectionId,
        }),
        sectionId: sectionId || "",
        title: title,
        continued: pageInSection > 0,
        charCount: inner.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().length,
      });
      idx += 1;
      current = [];
      chars = 0;
      pageInSection += 1;
    }
    (blocks || []).forEach(function (b, i) {
      var c = blockChars(b);
      if (current.length && chars + c > PAGE_CHAR_BUDGET && !onlyHeading(current)) pushPage(true);
      if (b.type === "heading" && current.length && chars > PAGE_CHAR_BUDGET * 0.72 && !onlyHeading(current)) {
        pushPage(true);
      }
      current.push(b);
      chars += c;
      if (i === (blocks || []).length - 1) pushPage(true);
    });
    if (current.length) pushPage(true);
    if (!pages.length) {
      pages.push({
        index: idx,
        html: wrapSheet(idx, '<p class="hid-body">No content.</p>', { title: title, sectionId: sectionId }),
        sectionId: sectionId || "",
        title: title,
        continued: false,
        charCount: 11,
      });
    }
    return pages;
  }

  var FULL_HI_REQUIRED_CHAPTER_IDS = [
    "property_identity",
    "ownership_chain_propco",
    "operator_management",
    "brand_reflag",
    "property_history",
    "organization_portfolio",
    "people_decision_authority",
    "transactions_capital",
    "commercial_pursuit",
    "open_questions",
    "sources_evidence",
  ];

  var CHANGE_OPPORTUNITY_REQUIRED_SECTIONS = [
    "executive_answer",
    "why_now",
    "recent_change_triggers",
    "asset_product_risk",
    "product_investment_capex",
    "operating_quality",
    "operator_management_stability",
    "what_could_derail",
    "deal_risk_flags",
    "what_looks_stable",
    "what_to_verify_next",
    "open_questions",
    "sources_evidence",
  ];

  var GENERIC_ADDENDUM_REQUIRED_SECTIONS = [
    "executive_answer",
    "research_question",
    "key_findings",
    "open_questions",
    "sources_evidence",
  ];

  var ADDENDUM_SECTION_ALIASES = {
    operating_quality_management_risk: "operating_quality",
    opportunity_thesis: "why_now",
  };

  /**
   * Packet 2.6C-R4.1 — resolve required sections from active report contract.
   * Never use a single global Full-HI chapter list for every report.
   */
  function resolveReportContract(dossier) {
    var type = String(
      (dossier && (dossier.dossier_type || dossier.report_type)) || ""
    ).toUpperCase();
    var templateId = String((dossier && dossier.template_id) || "").trim();
    if (type === "RESEARCH_ADDENDUM" || type === "RESEARCH_ADDENDUM".toUpperCase()) {
      if (templateId === "CHANGE_OPPORTUNITY") {
        return {
          report_type: "RESEARCH_ADDENDUM",
          template_id: templateId,
          contract_id: "ChangeOpportunityContract",
          requiredSectionIds: CHANGE_OPPORTUNITY_REQUIRED_SECTIONS.slice(),
          aliases: ADDENDUM_SECTION_ALIASES,
          known: true,
        };
      }
      return {
        report_type: "RESEARCH_ADDENDUM",
        template_id: templateId || null,
        contract_id: templateId
          ? "ResearchAddendumContract:" + templateId
          : "ResearchAddendumContract",
        requiredSectionIds: GENERIC_ADDENDUM_REQUIRED_SECTIONS.slice(),
        aliases: {},
        known: Boolean(templateId),
      };
    }
    if (type === "FULL_HOTEL_INTELLIGENCE_INVESTIGATION" || type === "FULL_INVESTIGATION") {
      return {
        report_type: "FULL_HOTEL_INTELLIGENCE_INVESTIGATION",
        template_id: null,
        contract_id: "FullInvestigationContract",
        requiredSectionIds: FULL_HI_REQUIRED_CHAPTER_IDS.slice(),
        aliases: {},
        known: true,
      };
    }
    return {
      report_type: type || null,
      template_id: templateId || null,
      contract_id: null,
      requiredSectionIds: [],
      aliases: {},
      known: false,
      error: "unknown_report_contract:" + (type || "missing") + "/" + (templateId || "none"),
    };
  }

  function countWords(text) {
    return String(text || "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .split(" ")
      .filter(Boolean).length;
  }

  function resolveAddendumSection(sections, id) {
    var direct = sections.find(function (s) {
      return s && s.id === id;
    });
    if (direct) return direct;
    for (var alias in ADDENDUM_SECTION_ALIASES) {
      if (ADDENDUM_SECTION_ALIASES[alias] === id) {
        var hit = sections.find(function (s) {
          return s && s.id === alias;
        });
        if (hit) return hit;
      }
    }
    return null;
  }

  function sectionHasContent(sec) {
    if (!sec || !(sec.blocks || []).length) return false;
    var total = 0;
    for (var i = 0; i < sec.blocks.length; i += 1) {
      var b = sec.blocks[i];
      if (!b) continue;
      if (b.type === "paragraphs") total += countWords((b.paragraphs || []).join(" "));
      else if (b.type === "para" || b.type === "paragraph") total += countWords(b.text || "");
      else if (b.type === "list") total += countWords((b.items || []).join(" "));
      else if (b.type === "finding_callout") {
        total += countWords((b.headline || "") + " " + (b.body || ""));
      } else if (b.type === "table") total += countWords(JSON.stringify(b.rows || []));
      else if (b.type === "heading") total += countWords(b.text || "");
      else total += countWords(JSON.stringify(b));
    }
    return total >= 12;
  }

  /**
   * Packet 2.6C-R4 — type-aware live integrity.
   * Full HI keeps strict chapters + depth floor.
   * Research Addenda use template contracts (never Full HI chapters / 3500 gate).
   */
  function assertLiveIntegrity(dossier, options) {
    options = options || {};
    var errors = [];
    if (!dossier || typeof dossier !== "object") {
      return { ok: false, errors: ["dossier_missing"], report_type: null };
    }
    if (dossier.architecture && !dossier.dossier_type) {
      errors.push("payload_is_page_contract_not_dossier");
    }

    var type = String(dossier.dossier_type || dossier.report_type || "").toUpperCase();
    var hotelName = String(dossier.hotel_name || "").trim();
    if (!hotelName || hotelName === "Hotel") {
      errors.push("hotel_name_missing");
    }

    var expectedIds = options.expectedHotelIds || options.hotelIds || [];
    if (expectedIds.length) {
      var ids = [dossier.hotel_id, dossier.hotel_airtable_record_id]
        .filter(Boolean)
        .map(function (x) {
          return String(x).trim();
        });
      var hit = expectedIds.some(function (id) {
        return ids.indexOf(String(id).trim()) !== -1;
      });
      if (!hit) {
        errors.push(
          "report_hotel_mismatch:expected=" +
            expectedIds.join("|") +
            ";got=" +
            (ids.join("|") || "none")
        );
      }
    }

    // ——— Research Addendum path ———
    if (type === "RESEARCH_ADDENDUM") {
      var contractAddendum = resolveReportContract(dossier);
      var templateId = String(dossier.template_id || "").trim();
      if (!templateId) errors.push("template_id_required");
      var sections = dossier.sections || [];
      var required = contractAddendum.requiredSectionIds || [];
      required.forEach(function (id) {
        var sec = resolveAddendumSection(sections, id);
        if (!sec) errors.push("missing_addendum_section:" + id);
        else if (!sectionHasContent(sec)) errors.push("thin_addendum_section:" + id);
      });
      if (!(dossier.key_findings || []).length && !(dossier.findings || []).length) {
        errors.push("findings_missing");
      }
      if (Number(dossier.source_count || (dossier.sources || []).length || 0) < 1) {
        errors.push("sources_missing");
      }
      return {
        ok: errors.length === 0,
        errors: errors,
        report_type: "RESEARCH_ADDENDUM",
        template_id: templateId,
        contract: contractAddendum,
        wordCount: countWords(
          JSON.stringify({
            executive_summary: dossier.executive_summary,
            key_findings: dossier.key_findings,
            sections: sections,
          })
        ),
        hotelName: hotelName,
      };
    }

    // ——— Full Hotel Intelligence Investigation path (unchanged strictness) ———
    var contractFull = resolveReportContract(dossier);
    if (!contractFull.known || contractFull.report_type !== "FULL_HOTEL_INTELLIGENCE_INVESTIGATION") {
      errors.push(contractFull.error || "dossier_type_invalid");
      return {
        ok: false,
        errors: errors,
        report_type: type || null,
        contract: contractFull,
        hotelName: hotelName,
      };
    }
    var exec = (dossier.executive_summary && dossier.executive_summary.paragraphs) || [];
    if (!exec.length) errors.push("executive_summary_empty");
    if (!(dossier.key_findings || []).length) errors.push("key_findings_empty");
    if (Number(dossier.source_count || 0) < 12) errors.push("source_count_low");
    var hiSections = dossier.sections || [];
    if (hiSections.length < 8) errors.push("sections_incomplete");
    (contractFull.requiredSectionIds || []).forEach(function (id) {
      var sec = hiSections.find(function (s) {
        return s && s.id === id;
      });
      if (!sec) errors.push("missing_section:" + id);
      else if (!(sec.blocks || []).length) errors.push("empty_section:" + id);
    });
    var words = Number(dossier.substantive_word_count || 0);
    if (!words) {
      var blob = JSON.stringify({
        executive_summary: dossier.executive_summary,
        key_findings: dossier.key_findings,
        sections: hiSections,
      });
      words = countWords(blob);
    }
    var minWords = options.minSubstantiveWords != null ? Number(options.minSubstantiveWords) : 3500;
    if (words < minWords) errors.push("substantive_word_count_below_" + minWords + ":" + words);
    return {
      ok: errors.length === 0,
      errors: errors,
      report_type: "FULL_HOTEL_INTELLIGENCE_INVESTIGATION",
      contract: contractFull,
      wordCount: words,
      hotelName: hotelName,
    };
  }

  function customerIntegrityFailureHtml(integrity, options) {
    options = options || {};
    var founderDebug =
      options.founderDebug === true ||
      (typeof location !== "undefined" &&
        /[?&](founderDebug|hidDebug)=1/.test(String(location.search || "")));
    if (founderDebug) {
      return (
        '<div class="hid-runtime-error" role="alert" data-hid-root data-hid-integrity-failed="1">' +
        "<h2>Report validation failed (founder debug)</h2>" +
        "<p>Internal diagnostics:</p><ul>" +
        (integrity.errors || [])
          .map(function (e) {
            return "<li>" + esc(e) + "</li>";
          })
          .join("") +
        "</ul></div>"
      );
    }
    return (
      '<div class="hid-runtime-error" role="alert" data-hid-root data-hid-integrity-failed="1" data-hid-customer-safe="1">' +
      "<h2>This report is not yet ready</h2>" +
      "<p>Report processing is incomplete. Please try again shortly.</p>" +
      "</div>"
    );
  }

  function renderCover(dossier) {
    var status = dossier.investigation_status || dossier.status || "";
    var statusLabel = String(status || "")
      .replace(/_/g, " ")
      .replace(/\b\w/g, function (c) {
        return c.toUpperCase();
      });
    var loc =
      (dossier.report_hotel_identity && dossier.report_hotel_identity.display_location) ||
      (dossier.hotel_location &&
        (dossier.hotel_location.label ||
          [dossier.hotel_location.city, dossier.hotel_location.country]
            .filter(Boolean)
            .join(", "))) ||
      "";
    if (!loc || /^location\s+pending$/i.test(String(loc).trim()) || /^location\s+not\s+available$/i.test(String(loc).trim())) {
      loc =
        (dossier.hotel_location &&
          [dossier.hotel_location.city, dossier.hotel_location.country].filter(Boolean).join(", ")) ||
        "";
    }
    if (!loc || /^location\s+pending$/i.test(String(loc).trim()) || /^location\s+not\s+available$/i.test(String(loc).trim())) {
      loc = "Location not available";
    }
    var Chrome = global.DealalityReportPrintChrome;
    var logoUrl = (Chrome && Chrome.DEALALITY_LOGO_URL) || DEALALITY_LOGO_URL;
    var confidential =
      "Dealality Intelligence Dossier · " +
      ((Chrome && Chrome.CONFIDENTIAL_LINE) || "Confidential · For recipient only");
    var isAddendum =
      String(dossier.dossier_type || "").toUpperCase() === "RESEARCH_ADDENDUM" ||
      String(dossier.report_type || "").toUpperCase() === "RESEARCH_ADDENDUM";
    var docType = isAddendum
      ? "RESEARCH ADDENDUM"
      : "FULL HOTEL INTELLIGENCE INVESTIGATION";
    var investigationName = isAddendum
      ? String(
          (dossier.cover && dossier.cover.investigation_name) ||
            dossier.report_type_label ||
            "Change & Opportunity Investigation"
        ).toUpperCase()
      : null;
    var disclaimer = isAddendum
      ? (dossier.cover && dossier.cover.methodology_note) ||
        "This Research Addendum provides focused follow-up intelligence based on reviewed public and proprietary research sources. Findings should be validated against primary legal, contractual or property-level documents where indicated."
      : "This Intelligence Dossier presents a comprehensive investigation report. Findings should be validated against primary legal, contractual or property-level documents where indicated. Verified Intelligence appears only where Dealality validation rules already support it.";
    var subline = isAddendum
      ? esc(investigationName || "FOCUSED INVESTIGATION")
      : "Ownership · Corporate Structure · Brand &amp; Operator · People · Development Intelligence";
    var footLeft = isAddendum
      ? "Dealality · Research Addendum"
      : "Dealality · Full Hotel Intelligence Investigation";
    var parentLine = "";
    if (isAddendum && dossier.lineage && dossier.lineage.follow_up_to) {
      parentLine =
        '<p class="bas-cover-date">Follow-up to: ' +
        esc(dossier.lineage.follow_up_to.label || "Full Hotel Intelligence Investigation") +
        (dossier.lineage.follow_up_to.completed_display
          ? " · " + esc(dossier.lineage.follow_up_to.completed_display)
          : "") +
        "</p>";
    }
    var html =
      '<section class="bas-cover-page bas-book-page-surface bas-avoid-break hid-cover-page" data-hid-section="cover" data-hid-layout="COVER" aria-label="Cover">';
    html += '<div class="bas-cover-geometric" aria-hidden="true"></div>';
    html += '<p class="bas-cover-confidential">' + esc(confidential) + "</p>";
    html += '<div class="bas-cover-block">';
    html += '<p class="bas-cover-doc-type">' + esc(docType) + "</p>";
    if (investigationName) {
      html += '<p class="bas-cover-sub" style="margin-top:0.35rem">' + esc(investigationName) + "</p>";
    }
    html += '<h1 class="bas-cover-title">' + esc(dossier.hotel_name) + "</h1>";
    html += '<p class="bas-cover-location">' + esc(loc) + "</p>";
    html += '<div class="bas-cover-accent-line" aria-hidden="true"></div>';
    if (!isAddendum) {
      html += '<p class="bas-cover-sub">' + subline + "</p>";
    }
    html += parentLine;
    html +=
      '<p class="bas-cover-date">Sources ' +
      esc(dossier.source_count) +
      " · Findings " +
      esc(dossier.finding_count) +
      " · Open questions " +
      esc(dossier.open_question_count) +
      "</p>";
    html +=
      '<p class="bas-cover-date">Completed ' +
      esc(formatDate(dossier.completed_at)) +
      " · " +
      esc(statusLabel) +
      "</p>";
    html += "</div>";
    html += '<p class="bas-cover-disclaimer">' + esc(disclaimer) + "</p>";
    html +=
      '<div class="bas-cover-hero"><div class="bas-cover-logo-block"><img src="' +
      esc(logoUrl) +
      '" alt="Dealality" class="bas-cover-logo-img" width="140" height="auto"></div></div>';
    html +=
      '<footer class="hid-cover-page__foot bas-no-print" aria-hidden="false">' +
      '<span class="hid-chapter__foot-left">' +
      esc(footLeft) +
      "</span>" +
      '<span class="hid-chapter__foot-right" data-hid-page-label>Page __N__ of __TOTAL__</span>' +
      "</footer>";
    html += "</section>";
    return html;
  }

  function classifyExecParagraph(p) {
    var s = String(p || "").toLowerCase();
    if (/propco|ihvsf|inmobiliaria/.test(s)) return "PropCo";
    if (/shareholder|ancira|beneficial|control/.test(s)) return "Control";
    if (/operator|self-operat|management/.test(s)) return "Operator";
    if (/breathless|brand|reflag|hilton|krystal/.test(s)) return "Brand / Reflag";
    if (/commercial|pursuit|approach|relationship/.test(s)) return "Commercial implication";
    if (/own|gsf|grupo hotelero|economic/.test(s)) return "Ownership";
    return "Overview";
  }

  function buildExecBlocks(dossier) {
    var blocks = [];
    var paras = (dossier.executive_summary && dossier.executive_summary.paragraphs) || [];
    var groups = {};
    var order = [];
    paras.forEach(function (p) {
      var g = classifyExecParagraph(p);
      if (!groups[g]) {
        groups[g] = [];
        order.push(g);
      }
      groups[g].push(p);
    });
    order.forEach(function (g) {
      blocks.push({ type: "exec_group", title: g, paragraphs: groups[g] });
    });
    blocks.push({ type: "heading", level: 2, text: "Key Findings" });
    (dossier.key_findings || []).forEach(function (kf) {
      var bodyParts = [];
      if (kf.explanation) bodyParts.push(String(kf.explanation));
      if (kf.summary && kf.summary !== kf.headline) bodyParts.push(String(kf.summary));
      else if (kf.body && kf.body !== kf.headline) bodyParts.push(String(kf.body));
      if (kf.citations && kf.citations.length) {
        bodyParts.push(
          kf.citations
            .map(function (n) {
              return "[" + n + "]";
            })
            .join("")
        );
      }
      blocks.push({
        type: "finding_callout",
        headline: kf.headline || "Finding",
        body: bodyParts.join("\n\n"),
        status: kf.status,
      });
    });
    return blocks;
  }

  function layoutTypeForSection(sectionId) {
    if (sectionId === "cover") return "COVER";
    if (sectionId === "executive_summary") return "EXECUTIVE_SUMMARY";
    if (sectionId === "property_history") return "TIMELINE";
    if (sectionId === "people_decision_authority") return "PROFILE_CARDS";
    if (sectionId === "sources_evidence") return "BIBLIOGRAPHY";
    if (sectionId === "contacts_appendix") return "STANDARD_TABLE";
    if (sectionId === "ownership_chain_propco" || sectionId === "brand_reflag") {
      return "RESEARCH_STACK";
    }
    return "NARRATIVE";
  }

  function ensureOwnershipDiagram(blocks, dossier) {
    var list = (blocks || []).slice();
    var hasDiagram = list.some(function (b) {
      return b && b.type === "ownership_diagram";
    });
    if (hasDiagram) return list;
    // Prefer dossier-provided diagram; never inject a hotel-specific hardcoded chain.
    if (dossier && dossier.ownership_diagram && Array.isArray(dossier.ownership_diagram.nodes)) {
      list.unshift({
        type: "ownership_diagram",
        nodes: dossier.ownership_diagram.nodes,
        note: dossier.ownership_diagram.note || "",
      });
      return list;
    }
    return list;
  }

  /**
   * Presentation-only: merge citation-only paragraph blocks into the previous
   * paragraph/finding so PDF pages never open with a lone "[n]" (R7).
   * Does not alter dossier research semantics or drop citations.
   */
  function coalesceCitationOnlyParagraphs(blocks) {
    var out = [];
    (blocks || []).forEach(function (b) {
      var onlyCites =
        b &&
        ((b.type === "paragraphs" &&
          Array.isArray(b.paragraphs) &&
          b.paragraphs.length === 1 &&
          /^\s*(?:\[\d+\])+\s*$/.test(String(b.paragraphs[0] || ""))) ||
          ((b.type === "para" || b.type === "paragraph") &&
            /^\s*(?:\[\d+\])+\s*$/.test(String(b.text || ""))));
      if (onlyCites) {
        var cite = String(
          b.type === "paragraphs" ? b.paragraphs[0] : b.text || ""
        ).trim();
        var prev = out[out.length - 1];
        if (prev && prev.type === "paragraphs" && prev.paragraphs && prev.paragraphs.length) {
          var cloned = Object.assign({}, prev, {
            paragraphs: prev.paragraphs.slice(),
          });
          var li = cloned.paragraphs.length - 1;
          cloned.paragraphs[li] =
            String(cloned.paragraphs[li] || "").replace(/\s+$/, "") + " " + cite;
          out[out.length - 1] = cloned;
          return;
        }
        if (prev && prev.type === "finding_callout") {
          out[out.length - 1] = Object.assign({}, prev, {
            body: String(prev.body || "").replace(/\s+$/, "") + " " + cite,
          });
          return;
        }
      }
      out.push(b);
    });
    return out;
  }

  function chapterFootHtml(dossier) {
    var isAddendum =
      dossier &&
      (String(dossier.dossier_type || "").toUpperCase() === "RESEARCH_ADDENDUM" ||
        String(dossier.report_type || "").toUpperCase() === "RESEARCH_ADDENDUM");
    return (
      '<footer class="hid-chapter__foot">' +
      '<span class="hid-chapter__foot-left">' +
      (isAddendum
        ? "Dealality · Research Addendum"
        : "Dealality · Full Hotel Intelligence Investigation") +
      "</span>" +
      '<span class="hid-chapter__foot-right" data-hid-page-label>Page __N__ of __TOTAL__</span>' +
      "</footer>"
    );
  }

  function renderChapter(sec, dossier) {
    if (!sec) return "";
    var blocks =
      sec.id === "ownership_chain_propco"
        ? ensureOwnershipDiagram(sec.blocks, dossier)
        : (sec.blocks || []).slice();
    blocks = coalesceCitationOnlyParagraphs(blocks);
    var layout = layoutTypeForSection(sec.id);
    return (
      '<section class="hid-chapter" data-hid-layout="' +
      esc(layout) +
      '" data-hid-section="' +
      esc(sec.id || "") +
      '">' +
      '<header class="hid-chapter__band"><span>' +
      esc(sec.title || "") +
      "</span></header>" +
      '<h2 class="hid-chapter-h">' +
      esc(sec.title || "") +
      "</h2>" +
      blocks.map(renderBlock).join("") +
      chapterFootHtml(dossier) +
      "</section>"
    );
  }

  /**
   * ONE continuous semantic document for screen and print/PDF.
   * Cover is the only fixed full-page composition.
   * Research Addenda flow from their template sections (no Full-HI exec regrouping).
   */
  function buildDocumentHtml(dossier) {
    var parts = [];
    var isAddendum =
      String(dossier.dossier_type || "").toUpperCase() === "RESEARCH_ADDENDUM" ||
      String(dossier.report_type || "").toUpperCase() === "RESEARCH_ADDENDUM";
    parts.push('<div class="hid-doc-flow" data-hid-doc-flow="1">');
    parts.push(renderCover(dossier));
    if (!isAddendum) {
      parts.push(
        '<section class="hid-chapter" data-hid-layout="EXECUTIVE_SUMMARY" data-hid-section="executive_summary">' +
          '<header class="hid-chapter__band"><span>Executive Summary</span></header>' +
          '<h2 class="hid-chapter-h">Executive Summary</h2>' +
          buildExecBlocks(dossier).map(renderBlock).join("") +
          chapterFootHtml(dossier) +
          "</section>"
      );
    }
    (dossier.sections || []).forEach(function (sec) {
      if (!sec || sec.id === "sources_evidence") return;
      parts.push(renderChapter(sec, dossier));
    });
    var sourcesSec = (dossier.sections || []).find(function (s) {
      return s && s.id === "sources_evidence";
    });
    if (sourcesSec) parts.push(renderChapter(sourcesSec, dossier));
    parts.push("</div>");
    var html = parts.join("");
    // Fill Page X of Y for every visual chapter/cover (web reader).
    var total = (html.match(/data-hid-page-label/g) || []).length;
    var n = 0;
    html = html.replace(
      /data-hid-page-label>Page __N__ of __TOTAL__/g,
      function () {
        n += 1;
        return "data-hid-page-label>Page " + n + " of " + total;
      }
    );
    return html;
  }

  function buildPrintFlowHtml(dossier, opts) {
    opts = opts || {};
    var Chrome = global.DealalityReportPrintChrome;
    // R7: Playwright PDF export must NOT include DOM running footer (dual chrome).
    // window.print() may still inject it when omitRunningFooter is false.
    var footer = "";
    if (
      !opts.omitRunningFooter &&
      Chrome &&
      typeof Chrome.buildRunningFooterHtml === "function"
    ) {
      footer = Chrome.buildRunningFooterHtml("Full Hotel Intelligence Investigation");
    }
    return (
      '<div class="hid-print-flow brand-alignment-snapshot hotel-intelligence-dossier" data-hid-print-flow="1" data-hid-build="' +
      esc(HID_BUILD_ID) +
      '" data-hid-asset-version="' +
      esc(HID_ASSET_VERSION) +
      '">' +
      buildDocumentHtml(dossier) +
      footer +
      "</div>"
    );
  }

  function buildToolbar(sectionCount) {
    return (
      '<div class="hid-reader-toolbar bas-no-print" role="toolbar" aria-label="Deep Research reader controls">' +
      '<div class="hid-reader-toolbar__left">' +
      '<button type="button" class="bas-btn bas-btn-secondary" data-hid-close aria-label="Back to Hotel Explorer">← Back</button>' +
      '<span class="hid-reader-pageind" data-hid-page-indicator aria-live="polite">1 of ' +
      sectionCount +
      "</span>" +
      "</div>" +
      '<div class="hid-reader-toolbar__zoom">' +
      '<button type="button" class="hid-tool-btn" data-hid-zoom-out aria-label="Zoom out">−</button>' +
      '<span class="hid-zoom-label" data-hid-zoom-label>Fit Width</span>' +
      '<button type="button" class="hid-tool-btn" data-hid-zoom-in aria-label="Zoom in">+</button>' +
      '<button type="button" class="hid-tool-btn" data-hid-fit-page aria-label="Fit page">Fit Page</button>' +
      '<button type="button" class="hid-tool-btn" data-hid-fit-width aria-label="Fit width">Fit Width</button>' +
      '<button type="button" class="hid-tool-btn" data-hid-zoom-100 aria-label="Zoom 100 percent">100%</button>' +
      "</div>" +
      '<div class="hid-reader-toolbar__right">' +
      '<button type="button" class="bas-btn bas-btn-primary" data-hid-download-pdf aria-label="Download PDF">Download PDF</button>' +
      '<button type="button" class="bas-btn bas-btn-secondary" data-hid-print aria-label="Print">Print</button>' +
      "</div></div>"
    );
  }

  function buildHtml(dossier, options) {
    options = options || {};
    var integrity = assertLiveIntegrity(dossier, {
      expectedHotelIds: options.expectedHotelIds || options.hotelIds || null,
      minSubstantiveWords: options.minSubstantiveWords,
    });
    if (!integrity.ok && options.allowIncomplete !== true) {
      return customerIntegrityFailureHtml(integrity, options);
    }
    var docHtml = buildDocumentHtml(dossier);
    var sectionCount =
      1 +
      1 +
      ((dossier.sections || []).filter(function (s) {
        return s && s.id;
      }).length || 0);
    var rootClass = "hotel-intelligence-dossier hid-reader hid-reader--flow brand-alignment-snapshot";
    if (options.embed) rootClass += " hid-reader--embed bas--embed";
    var html =
      '<div class="' +
      rootClass +
      '" data-hid-root data-hid-build="' +
      esc(HID_BUILD_ID) +
      '" data-hid-asset-version="' +
      esc(HID_ASSET_VERSION) +
      '" data-hid-dossier-id="' +
      esc(dossier.dossier_id) +
      '" data-hid-page-count="' +
      sectionCount +
      '" data-hid-hotel-name="' +
      esc(dossier.hotel_name || "") +
      '" data-hid-flow="continuous">';
    html += buildToolbar(sectionCount);
    html +=
      '<div class="hid-reader-canvas" data-hid-canvas>' +
      '<div class="hid-reader-scale-slot" data-hid-scale-slot>' +
      '<div class="hid-reader-stage hid-doc-stage" data-hid-stage>' +
      docHtml +
      "</div></div>" +
      '<button type="button" class="hid-nav-fab hid-nav-fab--prev bas-no-print" data-hid-prev aria-label="Previous section">‹</button>' +
      '<button type="button" class="hid-nav-fab hid-nav-fab--next bas-no-print" data-hid-next aria-label="Next section">›</button>' +
      "</div></div>";
    return html;
  }

  function findSourceByNumber(dossier, num) {
    var n = Number(num);
    return (dossier.sources || []).find(function (s) {
      return Number(s.number) === n;
    });
  }

  function loadZoomPref() {
    try {
      var raw = sessionStorage.getItem(SESSION_ZOOM_KEY);
      if (!raw) return { mode: ZOOM_PRESETS.fit_page, pct: 100 };
      return JSON.parse(raw);
    } catch (_) {
      return { mode: ZOOM_PRESETS.fit_page, pct: 100 };
    }
  }

  function saveZoomPref(pref) {
    try {
      sessionStorage.setItem(SESSION_ZOOM_KEY, JSON.stringify(pref));
    } catch (_) {}
  }

  function bind(container, dossier, options) {
    options = options || {};
    var root = container.querySelector("[data-hid-root]") || container;
    var canvas = root.querySelector("[data-hid-canvas]");
    var stage = root.querySelector("[data-hid-stage]");
    var scaleSlot = root.querySelector("[data-hid-scale-slot]");
    var sections = Array.prototype.slice.call(
      root.querySelectorAll("[data-hid-section]")
    );
    var indicator = root.querySelector("[data-hid-page-indicator]");
    var zoomLabel = root.querySelector("[data-hid-zoom-label]");
    var pref = loadZoomPref();
    if (!pref || !pref.mode) pref = { mode: ZOOM_PRESETS.fit_width, pct: 100 };
    var current = 0;

    function updateIndicator() {
      if (indicator) indicator.textContent = current + 1 + " of " + sections.length;
      var prev = root.querySelector("[data-hid-prev]");
      var next = root.querySelector("[data-hid-next]");
      if (prev) prev.disabled = current <= 0;
      if (next) next.disabled = current >= sections.length - 1;
    }

    function applyZoom() {
      if (!stage || !canvas || !scaleSlot) return;
      var cw = canvas.clientWidth - 48;
      var docW = 794;
      var scale = 1;
      if (pref.mode === ZOOM_PRESETS.fit_width || pref.mode === ZOOM_PRESETS.fit_page) {
        scale = Math.max(0.55, Math.min(1.25, cw / docW));
      } else {
        scale = Math.max(0.55, Math.min(1.4, (pref.pct || 100) / 100));
      }
      // Prefer CSS zoom (Chromium) — no transform clipping of continuous content.
      stage.style.transform = "";
      stage.style.width = docW + "px";
      stage.style.zoom = String(scale);
      scaleSlot.style.width = Math.round(docW * scale) + "px";
      scaleSlot.style.height = "auto";
      scaleSlot.style.overflow = "visible";
      stage.style.setProperty("--hid-scale", String(scale));
      if (zoomLabel) {
        if (pref.mode === ZOOM_PRESETS.fit_page) zoomLabel.textContent = "Fit Page";
        else if (pref.mode === ZOOM_PRESETS.fit_width) zoomLabel.textContent = "Fit Width";
        else zoomLabel.textContent = Math.round(scale * 100) + "%";
      }
      saveZoomPref(pref);
    }

    function currentScale() {
      var z = Number(stage && stage.style.zoom);
      return z > 0 ? z : 1;
    }

    function goTo(i, behavior) {
      if (i < 0 || i >= sections.length) return;
      current = i;
      var top = Math.max(0, sections[i].offsetTop - 8);
      if (canvas) {
        canvas.scrollTo({ top: top, behavior: behavior || "smooth" });
      } else {
        sections[i].scrollIntoView({ behavior: behavior || "smooth", block: "start" });
      }
      updateIndicator();
    }

    function syncFromScroll() {
      if (!canvas) return;
      var y = canvas.scrollTop + 24;
      var best = 0;
      var bestDist = Infinity;
      sections.forEach(function (sec, i) {
        var d = Math.abs(sec.offsetTop - y);
        if (d < bestDist) {
          bestDist = d;
          best = i;
        }
      });
      if (best !== current) {
        current = best;
        updateIndicator();
      }
    }

    function printReport() {
      var hostId = "hid-print-host";
      var old = document.getElementById(hostId);
      if (old) old.remove();
      var host = document.createElement("div");
      host.id = hostId;
      host.className = "hid-print-host hid-print-host--flow";
      host.innerHTML = buildPrintFlowHtml(dossier);
      document.body.classList.add("hid-print-active");
      document.body.appendChild(host);
      var cleanup = function () {
        document.body.classList.remove("hid-print-active");
        host.remove();
        global.removeEventListener("afterprint", cleanup);
      };
      global.addEventListener("afterprint", cleanup);
      global.setTimeout(function () {
        global.print();
      }, 50);
    }

    function downloadCanonicalPdf() {
      var dossierId = String(dossier.dossier_id || "").trim();
      var recordId = String(
        dossier.hotel_airtable_record_id ||
          dossier.airtable_record_id ||
          dossier.hotel_record_id ||
          ""
      ).trim();
      var url = dossierId
        ? "/api/hotel-intelligence/dossiers/" + encodeURIComponent(dossierId) + "/pdf"
        : "/api/hotel-intelligence/hotels/" +
          encodeURIComponent(recordId || "recUNycnMwOVFX0hc") +
          "/dossiers/pdf";
      var btn = root.querySelector("[data-hid-download-pdf]");
      if (btn) {
        btn.disabled = true;
        btn.textContent = "Preparing PDF…";
      }
      fetch(url, { credentials: "same-origin" })
        .then(function (res) {
          if (!res.ok) {
            return res.json().catch(function () {
              return { error: "pdf_failed" };
            }).then(function (j) {
              throw new Error((j && j.message) || (j && j.error) || "pdf_failed");
            });
          }
          var cd = res.headers.get("Content-Disposition") || "";
          var m =
            cd.match(/filename\*=UTF-8''([^;]+)/i) ||
            cd.match(/filename=\"([^\"]+)\"/i) ||
            cd.match(/filename=([^;]+)/i);
          var filename =
            (m && m[1] && decodeURIComponent(String(m[1]).trim())) ||
            "hotel-intelligence-report.pdf";
          return res.blob().then(function (blob) {
            return { blob: blob, filename: filename };
          });
        })
        .then(function (payload) {
          var a = document.createElement("a");
          var href = URL.createObjectURL(payload.blob);
          a.href = href;
          a.download = payload.filename;
          document.body.appendChild(a);
          a.click();
          a.remove();
          setTimeout(function () {
            URL.revokeObjectURL(href);
          }, 2000);
        })
        .catch(function (err) {
          console.error("[hid] download pdf", err);
          if (typeof options.onPdfError === "function") {
            options.onPdfError(err);
          } else {
            global.alert(
              "Could not download the canonical PDF. " +
                (err && err.message ? err.message : "Please try again.")
            );
          }
        })
        .finally(function () {
          if (btn) {
            btn.disabled = false;
            btn.textContent = "Download PDF";
          }
        });
    }

    applyZoom();
    updateIndicator();
    global.addEventListener("resize", applyZoom);
    if (canvas) {
      canvas.addEventListener(
        "scroll",
        function () {
          syncFromScroll();
        },
        { passive: true }
      );
    }

    root.addEventListener("click", function (e) {
      var closeBtn = e.target.closest("[data-hid-close]");
      if (closeBtn && typeof options.onClose === "function") {
        options.onClose();
        return;
      }
      if (e.target.closest("[data-hid-download-pdf]")) {
        downloadCanonicalPdf();
        return;
      }
      if (e.target.closest("[data-hid-print]")) {
        printReport();
        return;
      }
      if (e.target.closest("[data-hid-prev]")) {
        goTo(current - 1);
        return;
      }
      if (e.target.closest("[data-hid-next]")) {
        goTo(current + 1);
        return;
      }
      if (e.target.closest("[data-hid-fit-page]")) {
        pref = { mode: ZOOM_PRESETS.fit_page, pct: 100 };
        applyZoom();
        return;
      }
      if (e.target.closest("[data-hid-fit-width]")) {
        pref = { mode: ZOOM_PRESETS.fit_width, pct: 100 };
        applyZoom();
        return;
      }
      if (e.target.closest("[data-hid-zoom-100]")) {
        pref = { mode: ZOOM_PRESETS.pct, pct: 100 };
        applyZoom();
        return;
      }
      if (e.target.closest("[data-hid-zoom-in]")) {
        var up = Math.round((pref.pct || 100) + 10);
        if (pref.mode !== ZOOM_PRESETS.pct) up = Math.round(110);
        pref = { mode: ZOOM_PRESETS.pct, pct: Math.min(220, up) };
        applyZoom();
        return;
      }
      if (e.target.closest("[data-hid-zoom-out]")) {
        var down = Math.round((pref.pct || 100) - 10);
        if (pref.mode !== ZOOM_PRESETS.pct) down = Math.round(90);
        pref = { mode: ZOOM_PRESETS.pct, pct: Math.max(40, down) };
        applyZoom();
        return;
      }
      var cite = e.target.closest("[data-hid-cite]");
      if (cite && typeof options.onCite === "function") {
        var src = findSourceByNumber(dossier, cite.getAttribute("data-hid-cite"));
        options.onCite(src, cite.getAttribute("data-hid-cite"));
      }
    });

    root.addEventListener("keydown", function (e) {
      var tag = (e.target && e.target.tagName) || "";
      if (tag === "INPUT" || tag === "TEXTAREA") return;
      if (e.key === "ArrowRight" || e.key === "PageDown") {
        e.preventDefault();
        goTo(current + 1);
      } else if (e.key === "ArrowLeft" || e.key === "PageUp") {
        e.preventDefault();
        goTo(current - 1);
      } else if (e.key === "Home") {
        e.preventDefault();
        goTo(0);
      } else if (e.key === "End") {
        e.preventDefault();
        goTo(sections.length - 1);
      }
    });
    if (root.tabIndex < 0) root.tabIndex = 0;

    root.__hidDossier = dossier;
    root.__hidBuildPrintFlow = function (printOpts) {
      return buildPrintFlowHtml(dossier, printOpts || { omitRunningFooter: true });
    };
    root.__hidReader = {
      pageCount: sections.length,
      goTo: goTo,
      getCurrent: function () {
        return current;
      },
      buildPrintFlowHtml: function (printOpts) {
        return buildPrintFlowHtml(dossier, printOpts);
      },
      getPageFingerprints: function () {
        return sections.map(function (s, i) {
          var text = (s.innerText || "").replace(/\s+/g, " ").trim().slice(0, 240);
          return {
            page: i + 1,
            sectionId: s.getAttribute("data-hid-section") || "",
            preview: text,
          };
        });
      },
    };
  }

  function renderLibrary(list, options) {
    options = options || {};
    if (!list || !list.length) {
      var empty = options.emptyState || {};
      return (
        '<div class="hid-library"><h2>' +
        esc(empty.title || "Full Hotel Intelligence Investigation") +
        '</h2><p class="bas-summary">' +
        esc(
          empty.description ||
            "No investigation has been completed for this hotel yet."
        ) +
        '</p><p class="bas-muted">Status: ' +
        esc(empty.status || "NOT_STARTED") +
        "</p></div>"
      );
    }
    var html = '<div class="hid-library"><h2>Intelligence Dossier</h2>';
    html += '<p class="bas-muted">Full Hotel Intelligence Investigation runs for this hotel.</p><ul class="hid-library-list">';
    list.forEach(function (card) {
      html +=
        '<li><button type="button" class="hid-library-item" data-hid-open="' +
        esc(card.dossier_id) +
        '"><strong>' +
        esc(card.title) +
        "</strong><span>" +
        esc(card.status) +
        " · " +
        esc(formatDate(card.completed_at || card.updated_at)) +
        " · " +
        esc(card.source_count) +
        " sources · " +
        esc(card.finding_count) +
        " findings · " +
        esc(card.open_question_count) +
        " open questions</span></button></li>";
    });
    html += "</ul></div>";
    return html;
  }

  global.HotelIntelligenceDossier = {
    BUILD_ID: HID_BUILD_ID,
    ASSET_VERSION: HID_ASSET_VERSION,
    buildHtml: buildHtml,
    bind: bind,
    renderLibrary: renderLibrary,
    findSourceByNumber: findSourceByNumber,
    assertLiveIntegrity: assertLiveIntegrity,
    resolveReportContract: resolveReportContract,
    buildPrintFlowHtml: buildPrintFlowHtml,
    buildDocumentHtml: buildDocumentHtml,
    classifyTable: classifyTable,
    PAGE_CHAR_BUDGET: PAGE_CHAR_BUDGET,
    // Compatibility alias — always resolved from Full Investigation contract, never a bare global.
    FULL_HI_REQUIRED_CHAPTER_IDS: FULL_HI_REQUIRED_CHAPTER_IDS.slice(),
    getRequiredSectionIds: function (dossier) {
      return resolveReportContract(dossier).requiredSectionIds.slice();
    },
  };
})(typeof window !== "undefined" ? window : globalThis);
