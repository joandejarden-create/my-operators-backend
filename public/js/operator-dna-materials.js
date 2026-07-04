/**

 * Operator Materials — owner-facing tab on Operator DNA profile.

 * Layout parity: Brand Explorer Official Brand Materials + Image Gallery.

 */

(function (global) {

  "use strict";



  var TAB_NAME = "Operator Materials";



  var TAB_ICON =

    '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path><polyline points="14 2 14 8 20 8"></polyline></svg>';



  var TAB_LABEL = "Operator<br>Materials";



  var DEFAULT_BADGE = "";



  var GALLERY_DEFAULT_LABELS = [
    "Resort Exterior",
    "Hotel Lobby & Lounge",
    "Guest Room",
    "Resort Pool & Terrace",
    "Restaurant & Dining Room",
    "Front Desk & Reception",
  ];



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



  function hasVal(v) {

    return nz(v).length > 0;

  }

  function gallerySlotIsMeaningful(slot) {
    if (!slot) return false;
    if (hasVal(slot.imageUrl)) return true;
    var title = nz(slot.title);
    if (!title) return false;
    return GALLERY_DEFAULT_LABELS.indexOf(title) < 0;
  }

  function galleryHasMeaningfulContent(gallery) {
    return Array.isArray(gallery) && gallery.some(gallerySlotIsMeaningful);
  }

  var FILE_TITLE_ACRONYMS = {
    cala: "CALA",
    fdd: "FDD",
    faq: "FAQ",
    pdf: "PDF",
    zip: "ZIP",
    doc: "DOC",
  };

  /** Title Case for file-card titles (filenames, Setup JSON, URL fallbacks). */
  function toProperCaseFileTitle(raw) {
    var s = nz(raw);
    if (!s) return s;
    var ext = "";
    var m = s.match(/^(.+?)(\.[a-z0-9]{2,8})$/i);
    if (m) {
      s = m[1];
      ext = m[2].toLowerCase();
    }
    var single = s.replace(/[-_+.]+/g, " ").replace(/\s+/g, " ").trim().toLowerCase();
    if (single === "dummy" || single === "sample") {
      return "Sample Operator Document" + ext;
    }
    s = s.replace(/[-_+.]+/g, " ").replace(/\s+/g, " ").trim();
    if (!s) return nz(raw);
    var words = s.split(" ");
    var out = [];
    for (var i = 0; i < words.length; i++) {
      var w = words[i];
      if (!w) continue;
      if (w === "&") {
        out.push("&");
        continue;
      }
      var low = w.toLowerCase();
      if (FILE_TITLE_ACRONYMS[low]) {
        out.push(FILE_TITLE_ACRONYMS[low]);
        continue;
      }
      out.push(w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());
    }
    var joined = out.join(" ");
    return ext ? joined + ext : joined;
  }

  function isSafeHttpUrl(url) {

    var s = nz(url);

    if (!s) return false;

    try {

      var u = new URL(s);

      return u.protocol === "http:" || u.protocol === "https:";

    } catch (e) {

      return false;

    }

  }



  function pick(ex, p, key) {

    if (ex && nz(ex[key])) return ex[key];

    if (p && nz(p[key])) return p[key];

    return "";

  }



  function fileKindLabelFromUrl(url, titleOpt) {

    var path = String(url || "").split("?")[0].toLowerCase();

    var title = String(titleOpt || "").toLowerCase();

    var hay = path + " " + title;

    if (hay.indexOf(".zip") !== -1) return "ZIP";

    if (hay.indexOf(".pdf") !== -1) return "PDF";

    if (hay.indexOf(".docx") !== -1) return "DOC";

    if (hay.indexOf(".doc") !== -1) return "DOC";

    if (hay.indexOf(".xlsx") !== -1 || hay.indexOf(".xls") !== -1) return "XLS";

    return "FILE";

  }



  function titleFromUrl(url) {

    try {

      var path = new URL(url).pathname || "";

      var base = decodeURIComponent(path.split("/").pop() || "").split("?")[0];

      if (base) return toProperCaseFileTitle(base);

    } catch (e) {

      /* ignore */

    }

    try {

      return toProperCaseFileTitle(new URL(url).hostname.replace(/^www\./i, ""));

    } catch (e2) {

      return "Resource Link";

    }

  }



  function hostnameFromUrl(url) {

    try {

      return new URL(url).hostname.replace(/^www\./i, "");

    } catch (e) {

      return "";

    }

  }



  function parseDocumentLinkLines(raw) {

    return String(raw || "")

      .split(/\n+/)

      .map(function (line) {

        return line.replace(/^[\s•\-–—*]+/, "").trim();

      })

      .filter(function (line) {

        return line && isSafeHttpUrl(line);

      });

  }



  function normalizeFileRow(row) {

    if (!row || typeof row !== "object") return null;

    var href = nz(row.href || row.url);

    if (!isSafeHttpUrl(href)) return null;

    var title = toProperCaseFileTitle(nz(row.title || row.name) || titleFromUrl(href));

    var body = nz(row.body || row.description || row.meta || row.subtitle);

    if (!body) {

      var host = hostnameFromUrl(href);

      body = host

        ? "External resource hosted on " + host + "."

        : "External resource link shared by this operator.";

    }

    var kind = nz(row.kind || row.type).toUpperCase();

    if (!kind) kind = fileKindLabelFromUrl(href, title);

    if (kind === "LINK") kind = "FILE";

    return {

      href: href,

      title: title,

      kind: kind,

      body: body,

      badge: nz(row.badge) || DEFAULT_BADGE,

    };

  }



  function normalizeGalleryRow(row, fallbackLabel) {

    return {

      title: (row && nz(row.title || row.caption)) || fallbackLabel,

      imageUrl: row && isSafeHttpUrl(row.imageUrl || row.url || row.href) ? nz(row.imageUrl || row.url || row.href) : "",

    };

  }



  /**

   * @param {unknown} raw

   * @returns {{ files: object[], gallery: object[] }}

   */

  function parseOperatorMaterialsPayload(raw) {

    var s = nz(raw);

    if (!s) return { files: [], gallery: [] };

    var data;

    try {

      data = JSON.parse(s);

    } catch (e) {

      return { files: [], gallery: [] };

    }

    var fileRows = [];

    var galleryRows = [];

    if (Array.isArray(data)) {

      fileRows = data;

    } else if (data && typeof data === "object") {

      fileRows = Array.isArray(data.files) ? data.files : [];

      galleryRows = Array.isArray(data.gallery) ? data.gallery : [];

    }

    var files = [];

    for (var i = 0; i < fileRows.length; i++) {

      var f = normalizeFileRow(fileRows[i]);

      if (f) files.push(f);

    }

    var gallery = [];

    for (var g = 0; g < 6; g++) {

      gallery.push(normalizeGalleryRow(galleryRows[g], GALLERY_DEFAULT_LABELS[g]));

    }

    if (!galleryHasMeaningfulContent(gallery)) gallery = [];

    return { files: files, gallery: gallery };

  }



  function parseGalleryOnlyPayload(raw) {

    var s = nz(raw);

    if (!s) return null;

    try {

      var data = JSON.parse(s);

      var rows = Array.isArray(data) ? data : data && Array.isArray(data.gallery) ? data.gallery : [];

      var gallery = [];

      for (var i = 0; i < 6; i++) {

        gallery.push(normalizeGalleryRow(rows[i], GALLERY_DEFAULT_LABELS[i]));

      }

      return galleryHasMeaningfulContent(gallery) ? gallery : null;

    } catch (e2) {

      return null;

    }

  }



  function firstHttpUrlInString(text) {
    var s = nz(text);
    if (!s) return "";
    var m = s.match(/https?:\/\/[^\s<>"']+/i);
    return m && m[0] ? m[0].replace(/[.,;:!?)]+$/, "") : "";
  }

  function materialsBodyLines(body) {
    return String(body || "")
      .split(/\n+/)
      .map(function (line) {
        return line.trim();
      })
      .filter(Boolean);
  }

  function materialsMetaFromBody(body) {
    return materialsBodyLines(body)
      .filter(function (line) {
        return !isSafeHttpUrl(line) && !/^badge\s*:/i.test(line);
      })
      .join(" · ")
      .trim();
  }

  function materialsBadgeFromBody(body) {
    var badgeLine = materialsBodyLines(body).find(function (line) {
      return /^badge\s*:/i.test(line);
    });
    if (!badgeLine) return DEFAULT_BADGE;
    return badgeLine.replace(/^badge\s*:\s*/i, "").trim() || DEFAULT_BADGE;
  }

  function materialsFileHrefFromBlock(block) {
    if (!block) return "";
    var fromBody = firstHttpUrlInString(block.body);
    if (fromBody) return fromBody;
    var img = nz(block.imageUrl);
    return isSafeHttpUrl(img) ? img : "";
  }

  function blocksForSlot(blocks, slotKey) {
    return (blocks || []).filter(function (b) {
      return b && nz(b.slotKey) === slotKey;
    });
  }

  function bundleFromPresentation(presentation) {
    var blocks = presentation && Array.isArray(presentation.blocks) ? presentation.blocks : [];
    if (!blocks.length) return null;

    var files = [];
    var fileRows = blocksForSlot(blocks, "materials.file");
    for (var i = 0; i < fileRows.length; i++) {
      var row = fileRows[i];
      var href = materialsFileHrefFromBlock(row);
      if (!href) continue;
      var title = toProperCaseFileTitle(nz(row.title) || titleFromUrl(href));
      var normalized = normalizeFileRow({
        href: href,
        title: title,
        body: materialsMetaFromBody(row.body),
        kind: fileKindLabelFromUrl(href, title),
        badge: materialsBadgeFromBody(row.body),
      });
      if (normalized) files.push(normalized);
    }

    var gallery = [];
    for (var g = 1; g <= 6; g++) {
      var slot = "materials.gallery." + g;
      var galleryBlock = blocksForSlot(blocks, slot)[0] || null;
      gallery.push(
        normalizeGalleryRow(
          {
            title: galleryBlock && nz(galleryBlock.title),
            imageUrl: galleryBlock && nz(galleryBlock.imageUrl),
          },
          GALLERY_DEFAULT_LABELS[g - 1]
        )
      );
    }

    return { files: files, gallery: gallery };
  }

  function materialsFromUrlLines(raw) {

    return parseDocumentLinkLines(raw)

      .map(function (href) {

        var title = toProperCaseFileTitle(titleFromUrl(href));

        var host = hostnameFromUrl(href);

        return {

          href: href,

          title: title,

          kind: fileKindLabelFromUrl(href, title),

          body: host

            ? "External resource hosted on " + host + "."

            : "External resource link shared by this operator.",

          badge: DEFAULT_BADGE,

        };

      });

  }



  /**

   * @param {object} vm

   * @returns {{ files: object[], gallery: object[] }}

   */

  function deriveOperatorMaterialsBundle(vm) {

    var p = (vm && vm.prefill) || {};

    var ex = (vm && vm.ex) || {};

    var fields = (vm && vm.fields) || {};

    var presentationBundle = bundleFromPresentation(vm && vm.operatorExplorerMaterials);
    var bundle =
      presentationBundle &&
      (presentationBundle.files.length || galleryHasMeaningfulContent(presentationBundle.gallery))
        ? presentationBundle
        : null;

    var jsonRaw = pick(ex, p, "operator_materials_json") || nz(fields.operator_materials_json);

    if (!bundle) bundle = parseOperatorMaterialsPayload(jsonRaw);



    var galleryOnly =

      pick(ex, p, "operator_materials_gallery_json") || nz(fields.operator_materials_gallery_json);

    var galleryParsed = parseGalleryOnlyPayload(galleryOnly);

    if (galleryParsed && galleryHasMeaningfulContent(galleryParsed)) bundle.gallery = galleryParsed;



    if (!bundle.files.length) {

      var linkRaw =

        pick(ex, p, "diligenceDocumentLinks") ||

        nz(fields["Owner Diligence Document Links"]) ||

        nz(fields.diligenceDocumentLinks) ||

        "";

      bundle.files = materialsFromUrlLines(linkRaw);

    }



    if (!bundle.gallery || !bundle.gallery.length) {

      bundle.gallery = GALLERY_DEFAULT_LABELS.map(function (lab) {

        return { title: lab, imageUrl: "" };

      });

    }

    return bundle;

  }



  /** Matches brand-explorer-atelier-from-api.js fileCard(). */

  function fileCard(icon, title, body, hrefOpt, badge) {

    var href = hrefOpt && isSafeHttpUrl(String(hrefOpt)) ? String(hrefOpt).trim() : "";

    var badgeLabel = hasVal(badge) ? String(badge).trim() : "";

    var bodyHtml = hasVal(body)

      ? '<div class="file-card__meta">' + escapeHtml(body) + "</div>"

      : '<div class="file-card__meta oe-dd--empty">&nbsp;</div>';

    var actions =

      '<div style="margin-top:10px">' +

      (href

        ? '<a class="btn" href="' +

          escapeHtml(href) +

          '" target="_blank" rel="noopener noreferrer">View</a> <a class="btn" href="' +

          escapeHtml(href) +

          '" target="_blank" rel="noopener noreferrer" download>Download</a>'

        : '<button type="button" class="btn" disabled>View</button> <button type="button" class="btn" disabled>Download</button>') +

      "</div>";

    return (

      '<div class="file-card">' +

      '<div class="file-card__icon">' +

      escapeHtml(icon) +

      "</div><div>" +

      '<p class="file-card__title">' +

      (hasVal(title) ? escapeHtml(title) : "&nbsp;") +

      "</p>" +

      bodyHtml +

      (badgeLabel
        ? '<span class="file-card__badge">' + escapeHtml(badgeLabel) + "</span>"
        : "") +

      actions +

      "</div></div>"

    );

  }



  function fileCardFromItem(item) {

    return fileCard(item.kind || "FILE", item.title, item.body, item.href, item.badge);

  }



  function buildGalleryHtml(slots) {

    return (slots || [])

      .map(function (slot) {

        var imgUrl = slot && hasVal(slot.imageUrl) ? String(slot.imageUrl).trim() : "";

        var caption = slot && hasVal(slot.title) ? String(slot.title).trim() : "Image";

        if (imgUrl && isSafeHttpUrl(imgUrl)) {

          return (

            '<div class="gallery-card gallery-card--has-image" role="img" aria-label="' +

            escapeHtml(caption) +

            '"><img src="' +

            escapeHtml(imgUrl) +

            '" alt=""' +
            (global.document &&
            global.document.documentElement &&
            global.document.documentElement.classList.contains("oe-export-pdf")
              ? ' loading="eager" decoding="sync"'
              : ' loading="lazy" decoding="async"') +
            ' referrerpolicy="no-referrer" />' +

            '<span class="gallery-card__cap">' +

            escapeHtml(caption) +

            "</span></div>"

          );

        }

        return (

          '<div class="gallery-card gallery-card--empty" role="img" aria-label="' +

          escapeHtml(caption) +

          '"><span>' +

          escapeHtml(caption) +

          "</span></div>"

        );

      })

      .join("");

  }



  function buildEmptyFilesGrid(company) {

    return (

      '<div class="file-card-grid">' +

      '<div class="odna-materials-empty" role="status">' +

      '<p class="odna-materials-empty__title">No materials published yet</p>' +

      '<p class="odna-materials-empty__body">When ' +

      escapeHtml(company) +

      " adds company overviews, decks, or other resources in Operator Setup, they will appear here as downloadable files.</p>" +

      "</div></div>"

    );

  }



  function buildPanelHtml(vm) {

    var bundle = deriveOperatorMaterialsBundle(vm);

    var company = nz(vm && vm.companyName) || "this operator";

    var filesGrid = bundle.files.length

      ? '<div class="file-card-grid">' + bundle.files.map(fileCardFromItem).join("") + "</div>"

      : buildEmptyFilesGrid(company);



    return (

      '<div class="be-atelier-oe odna-materials-tab">' +

      '<section class="oe-section">' +

      '<h2 class="oe-section-title">Operator Materials</h2>' +

      filesGrid +

      "</section>" +

      '<section class="oe-section">' +

      '<h2 class="oe-section-title">Image Gallery</h2>' +

      '<div class="gallery-grid">' +

      buildGalleryHtml(bundle.gallery) +

      "</div></section></div>"

    );

  }



  function mountTab(vm) {

    var Mount = global.OperatorDnaProfileMount;

    if (!Mount || !Mount.appendCustomTab) return;

    var ok = Mount.appendCustomTab({

      tabName: TAB_NAME,

      iconHtml: TAB_ICON,

      labelHtml: TAB_LABEL,

      panelHtml: buildPanelHtml(vm),

      insertBeforeTab: "Dealality Insights",

      activateOnAppend: false,

    });

    if (!ok && global.location && /localhost|127\.0\.0\.1/i.test(global.location.hostname)) {

      console.warn("[operator-dna-materials] tab not appended (duplicate or missing nav)");

    }

  }



  global.OperatorDnaMaterials = {

    TAB_NAME: TAB_NAME,

    deriveOperatorMaterialsBundle: deriveOperatorMaterialsBundle,

    buildPanelHtml: buildPanelHtml,

    mountTab: mountTab,

  };

})(typeof window !== "undefined" ? window : global);


