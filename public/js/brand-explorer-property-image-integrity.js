/**
 * Browser mirror — property image integrity + research-source separation.
 * Article/publication thumbnails are not property photographs.
 */
(function (global) {
  'use strict';

  var BE_PROPERTY_IMAGE_INTEGRITY_V1 = 'BE_PROPERTY_IMAGE_INTEGRITY_V1';

  var PUBLICATION_OR_TRADE_HOST_RE =
    /travelweekly\.com|traveldailynews\.com|hotel-online\.com|prnewswire\.com|globenewswire\.com|businesswire\.com|hotelnewsresource\.com|breakingtravelnews\.com|travelprnews\.com|newsismybusiness\.com|hotelmanagement-network\.com|journaldespalaces\.com|ladevi\.info|semana\.com|ithic\.it/i;

  var PROPERTY_FACING_SLOT_RE =
    /^(?:footprint\.(?:openings|momentum)|materials\.caseStudy|materials\.gallery\.\d+|overview\.scenario\.\d+|valueOwners\.scenario\.\d+)$/i;

  var VERIFIED_REPLACEMENTS = [
    {
      matchTitleRe: /royal\s*palm\s*gal[aá]pagos/i,
      imageUrl:
        'https://stories-editor.hilton.com/wp-content/uploads/2022/07/Royal-Palm-Galapagos-Curio-Collection-by-Hilton_Aerial_Courtesy-of-Royal-Palm-Galapagos-Curio-Collection-by-Hilton.jpg',
      sourceUrl: 'https://stories.hilton.com/releases/royal-palm-galapagos-curio-collection-by-hilton-opens',
      caption: 'Royal Palm Galápagos | Hotel & Guest Villas',
      imageType: 'operating_photograph',
      rejectFilenameRe: /-tw\.(jpe?g|png|webp)$/i
    }
  ];

  function nz(s) {
    return s == null ? '' : String(s).trim();
  }

  function basename(urlOrName) {
    var s = nz(urlOrName).split('?')[0];
    var parts = s.split('/');
    try {
      return decodeURIComponent(parts[parts.length - 1] || '');
    } catch (e) {
      return parts[parts.length - 1] || '';
    }
  }

  function filenameMarksPublicationGraphic(filenameOrUrl) {
    var base = basename(filenameOrUrl);
    if (!base) return false;
    if (/-tw\.(jpe?g|png|webp|gif)$/i.test(base)) return true;
    if (/travel[-_]?weekly|masthead|article[-_]?thumb|og[-_]?image|social[-_]?share/i.test(base)) return true;
    return false;
  }

  function isPublicationHost(url) {
    try {
      return PUBLICATION_OR_TRADE_HOST_RE.test(new URL(nz(url)).hostname);
    } catch (e) {
      return PUBLICATION_OR_TRADE_HOST_RE.test(nz(url));
    }
  }

  function isUnsuitablePropertyDisplayImage(url, meta) {
    meta = meta || {};
    var u = nz(url);
    var filename = nz(meta.imageFilename || meta.filename || basename(u));
    if (filenameMarksPublicationGraphic(filename) || filenameMarksPublicationGraphic(u)) {
      return { unsuitable: true, code: 'filename_marks_publication_graphic', detail: filename || basename(u) };
    }
    if (u && isPublicationHost(u)) {
      return { unsuitable: true, code: 'trade_press_or_news_host_image', detail: u };
    }
    if (
      global.BeEditorialTitleCase &&
      false
    ) {
      /* logos handled below via BePermanent gallery helper if present */
    }
    if (u && /logo|wordmark|social_image|hotel-brand-lo|Curio-Collection-by-Hilton\.webp/i.test(u)) {
      return { unsuitable: true, code: 'logo_as_property_photo', detail: basename(u) };
    }
    return { unsuitable: false, code: null, detail: null };
  }

  function findReplacement(block) {
    var title = nz(block && block.title);
    var filename = nz(block && block.imageFilename);
    for (var i = 0; i < VERIFIED_REPLACEMENTS.length; i++) {
      var row = VERIFIED_REPLACEMENTS[i];
      if (!row.matchTitleRe.test(title)) continue;
      if (row.rejectFilenameRe && filename && !row.rejectFilenameRe.test(filename)) {
        var cur = isUnsuitablePropertyDisplayImage(block.imageUrl, { imageFilename: filename });
        if (!cur.unsuitable) continue;
      }
      return row;
    }
    return null;
  }

  function applyPropertyImageIntegrityToBlocks(blocks) {
    var list = Array.isArray(blocks) ? blocks : [];
    var changed = [];
    var out = list.map(function (b) {
      var slotKey = nz(b.slotKey || b.slot);
      if (!PROPERTY_FACING_SLOT_RE.test(slotKey)) return b;
      var hasDisplayAsset = !!(nz(b.imageUrl) || nz(b.imageFilename));
      var check = isUnsuitablePropertyDisplayImage(b.imageUrl, {
        imageFilename: b.imageFilename,
        imageType: b.imageType
      });
      var replacement = findReplacement(b);
      if (!hasDisplayAsset) {
        if (!replacement) return b;
        var filled = {};
        Object.keys(b).forEach(function (k) {
          filled[k] = b[k];
        });
        filled.imageUrl = replacement.imageUrl;
        filled.imageFilename = basename(replacement.imageUrl);
        filled.imageType = replacement.imageType;
        filled.__propertyImageIntegrity = {
          action: 'filled_verified_property_photo',
          replacementUrl: replacement.imageUrl,
          caption: replacement.caption,
          sourceUrl: replacement.sourceUrl
        };
        changed.push({
          slotKey: slotKey,
          from: '',
          to: replacement.imageUrl,
          reason: 'verified_property_photo_fill',
          caption: replacement.caption
        });
        return filled;
      }
      if (replacement && check.unsuitable) {
        var next = {};
        Object.keys(b).forEach(function (k) {
          next[k] = b[k];
        });
        next.imageUrl = replacement.imageUrl;
        next.imageFilename = basename(replacement.imageUrl);
        next.imageType = replacement.imageType;
        next.__propertyImageIntegrity = {
          action: 'replaced_with_verified_property_photo',
          rejectedUrl: nz(b.imageUrl),
          rejectedFilename: nz(b.imageFilename),
          replacementUrl: replacement.imageUrl,
          caption: replacement.caption,
          sourceUrl: replacement.sourceUrl
        };
        changed.push({
          slotKey: slotKey,
          from: nz(b.imageUrl),
          to: replacement.imageUrl,
          reason: 'verified_property_photo_replacement',
          caption: replacement.caption
        });
        return next;
      }
      if (check.unsuitable) {
        var stripped = {};
        Object.keys(b).forEach(function (k) {
          stripped[k] = b[k];
        });
        stripped.imageUrl = null;
        stripped.imageFilename = '';
        stripped.__propertyImageIntegrity = {
          action: 'stripped_unsuitable_display_image',
          rejectedUrl: nz(b.imageUrl),
          rejectionCode: check.code,
          researchRequired: true
        };
        changed.push({ slotKey: slotKey, from: nz(b.imageUrl), to: null, reason: check.code });
        return stripped;
      }
      return b;
    });
    return {
      contract: BE_PROPERTY_IMAGE_INTEGRITY_V1,
      blocks: out,
      changed: changed,
      changedCount: changed.length
    };
  }

  /** Display gate for proposed column only. */
  function proposedPropertyImageUrl(brand, url, meta) {
    if (!(brand && (brand.beCumulativeProposal || brand.beCandidateOverlay))) {
      return nz(url);
    }
    var check = isUnsuitablePropertyDisplayImage(url, meta || {});
    if (check.unsuitable) return '';
    return nz(url);
  }

  global.BePropertyImageIntegrity = {
    BE_PROPERTY_IMAGE_INTEGRITY_V1: BE_PROPERTY_IMAGE_INTEGRITY_V1,
    isUnsuitablePropertyDisplayImage: isUnsuitablePropertyDisplayImage,
    filenameMarksPublicationGraphic: filenameMarksPublicationGraphic,
    applyPropertyImageIntegrityToBlocks: applyPropertyImageIntegrityToBlocks,
    proposedPropertyImageUrl: proposedPropertyImageUrl,
    VERIFIED_REPLACEMENTS: VERIFIED_REPLACEMENTS
  };
})(typeof globalThis !== 'undefined' ? globalThis : window);
