/**
 * Browser mirror — no repeated card headings in descriptions.
 */
(function (global) {
  'use strict';

  var BE_NO_REPEATED_CARD_HEADING_V1 = 'BE_NO_REPEATED_CARD_HEADING_V1';

  function nz(s) {
    return s == null ? '' : String(s).trim();
  }

  function normalizeHeadingForEchoCompare(text) {
    return nz(text)
      .toLowerCase()
      .replace(/[\u2013\u2014]/g, '-')
      .replace(/[“”]/g, '"')
      .replace(/[‘’]/g, "'")
      .replace(/[^a-z0-9&]+/gi, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function stripRepeatedCardHeading(title, body) {
    var t = nz(title);
    var b = nz(body);
    if (!t || !b) return b;
    var normTitle = normalizeHeadingForEchoCompare(t);
    if (!normTitle || normTitle.length < 3) return b;

    var escaped = t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    var prefixRe = new RegExp('^' + escaped + '\\s*[:\\-–—.|]\\s*', 'i');
    if (prefixRe.test(b)) {
      var stripped = b.replace(prefixRe, '').trim();
      return stripped || b;
    }

    var normBody = normalizeHeadingForEchoCompare(b);
    if (normBody === normTitle) return '';
    if (normBody.indexOf(normTitle + ' ') === 0) {
      var sepMatch = b.match(/^[\s\S]{1,160}?[:\-–—.|]\s+/);
      if (sepMatch) {
        var headNorm = normalizeHeadingForEchoCompare(sepMatch[0].replace(/[:\-–—.|]\s*$/, ''));
        if (headNorm === normTitle) {
          var rest = b.slice(sepMatch[0].length).trim();
          return rest || b;
        }
      }
    }
    return b;
  }

  function applyNoRepeatedCardHeadingToBlocks(blocks) {
    var list = Array.isArray(blocks) ? blocks : [];
    var changed = [];
    var out = list.map(function (b) {
      var title = nz(b.title);
      var body = nz(b.body);
      if (!title || !body) return b;
      var nextBody = stripRepeatedCardHeading(title, body);
      if (nextBody === body) return b;
      var next = {};
      Object.keys(b).forEach(function (k) {
        next[k] = b[k];
      });
      next.body = nextBody;
      next.__noRepeatedCardHeading = { action: 'stripped_leading_title_echo' };
      changed.push({
        slotKey: nz(b.slotKey || b.slot),
        from: body,
        to: nextBody,
        reason: 'repeated_card_heading_in_description'
      });
      return next;
    });
    return {
      contract: BE_NO_REPEATED_CARD_HEADING_V1,
      blocks: out,
      changed: changed,
      changedCount: changed.length
    };
  }

  global.BeNoRepeatedCardHeading = {
    BE_NO_REPEATED_CARD_HEADING_V1: BE_NO_REPEATED_CARD_HEADING_V1,
    stripRepeatedCardHeading: stripRepeatedCardHeading,
    customerFacingCardBody: function (title, body) {
      return stripRepeatedCardHeading(title, body);
    },
    applyNoRepeatedCardHeadingToBlocks: applyNoRepeatedCardHeadingToBlocks,
    normalizeHeadingForEchoCompare: normalizeHeadingForEchoCompare
  };
})(typeof globalThis !== 'undefined' ? globalThis : window);
