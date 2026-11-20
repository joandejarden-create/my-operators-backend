/**
 * Browser mirror of lib/brand-explorer/be-editorial-title-case-v1.js
 * Editorial Title Case for Brand Explorer white semantic headings (RIGHT proposed only).
 * Universal conjunction: spaced "and"/"And" → " & " (never break F&B).
 */
(function (global) {
  'use strict';

  var BE_EDITORIAL_TITLE_CASE_V1 = 'BE_EDITORIAL_TITLE_CASE_V1';
  var HEADING_CONJUNCTION_AND_RE = /\s+[Aa][Nn][Dd]\s+/g;

  var SMALL_WORDS = {
    a: 1,
    an: 1,
    and: 1,
    as: 1,
    at: 1,
    but: 1,
    by: 1,
    for: 1,
    from: 1,
    in: 1,
    into: 1,
    nor: 1,
    of: 1,
    on: 1,
    or: 1,
    per: 1,
    the: 1,
    to: 1,
    via: 1,
    vs: 1,
    'vs.': 1,
    with: 1
  };

  var ALWAYS_CAPITALIZE = { since: 1, vs: 1, via: 1 };

  var ACRONYMS = {
    adr: 1,
    api: 1,
    apac: 1,
    b2b: 1,
    cala: 1,
    crs: 1,
    emea: 1,
    'f&b': 1,
    fdd: 1,
    gds: 1,
    gop: 1,
    hr: 1,
    ihg: 1,
    kpi: 1,
    latam: 1,
    mea: 1,
    nda: 1,
    ota: 1,
    otas: 1,
    pip: 1,
    pms: 1,
    qa: 1,
    rfp: 1,
    rms: 1,
    roi: 1,
    sme: 1,
    smerf: 1,
    uk: 1,
    us: 1,
    usa: 1,
    ux: 1
  };

  var PRESERVE = {
    curio: 'Curio',
    ascend: 'Ascend',
    hilton: 'Hilton',
    honors: 'Honors',
    marriott: 'Marriott',
    hyatt: 'Hyatt',
    ihg: 'IHG',
    radisson: 'Radisson',
    choice: 'Choice',
    kimpton: 'Kimpton',
    tapestry: 'Tapestry',
    autograph: 'Autograph',
    tribute: 'Tribute',
    vignette: 'Vignette',
    voco: 'voco',
    'f&b': 'F&B',
    capex: 'CapEx'
  };

  function nz(s) {
    return s == null ? '' : String(s).trim();
  }

  function wordCount(s) {
    return nz(s).split(/\s+/).filter(Boolean).length;
  }

  function normalizeHeadingConjunctionAmpersand(raw) {
    var s = nz(raw);
    if (!s) return '';
    return s.replace(HEADING_CONJUNCTION_AND_RE, ' & ');
  }

  function capitalizeAtomic(part, forceCap) {
    var raw = nz(part);
    if (!raw) return raw;
    var m = raw.match(/^([^A-Za-z0-9]*)(.*?)([^A-Za-z0-9]*)$/);
    if (!m) return raw;
    var lead = m[1];
    var core = m[2];
    var trail = m[3];
    if (!core) return raw;
    var lower = core.toLowerCase();
    if (PRESERVE[lower]) return lead + PRESERVE[lower] + trail;
    if (lower === 'capex') return lead + 'CapEx' + trail;
    if (lower === 'f&b') return lead + 'F&B' + trail;
    if (lower === 'kpis') return lead + 'KPIs' + trail;
    if (ACRONYMS[lower]) return lead + lower.toUpperCase() + trail;
    if (/^\d+[a-z]?$/i.test(core)) return lead + core + trail;
    if (!forceCap && SMALL_WORDS[lower] && !ALWAYS_CAPITALIZE[lower]) {
      return lead + lower + trail;
    }
    if (core.indexOf('.') >= 0) {
      return (
        lead +
        core
          .split('.')
          .map(function (seg) {
            if (!seg) return seg;
            if (ACRONYMS[seg.toLowerCase()]) return seg.toUpperCase();
            return seg.charAt(0).toUpperCase() + seg.slice(1);
          })
          .join('.') +
        trail
      );
    }
    return lead + core.charAt(0).toUpperCase() + core.slice(1).toLowerCase() + trail;
  }

  function capitalizeHyphenated(token, forceCap) {
    return token
      .split('-')
      .map(function (part, idx) {
        return capitalizeAtomic(part, forceCap || idx === 0);
      })
      .join('-');
  }

  function toEditorialTitleCase(raw, opts) {
    opts = opts || {};
    var s = nz(raw);
    if (!s) return '';
    if (
      !opts.force &&
      (s.length > 120 || /[.!?]\s+[A-Z]/.test(s) || /[.!?]$/.test(s) || wordCount(s) > 14)
    ) {
      return s;
    }
    if (!opts._inner && !opts._skipConjunction) {
      s = normalizeHeadingConjunctionAmpersand(s);
    }
    if (/\s+&\s+/.test(s) && !opts._inner) {
      return s
        .split(/\s+&\s+/)
        .map(function (part) {
          return toEditorialTitleCase(part.trim(), {
            _inner: true,
            _skipConjunction: true,
            forceFirst: true,
            forceLast: true
          });
        })
        .join(' & ');
    }
    if (/\s+\/\s+/.test(s) && !opts._slash) {
      return s
        .split(/\s+\/\s+/)
        .map(function (part) {
          return toEditorialTitleCase(part.trim(), {
            _slash: true,
            _inner: true,
            _skipConjunction: true,
            forceFirst: true
          });
        })
        .join(' / ');
    }
    var words = s.split(/\s+/).filter(Boolean);
    return words
      .map(function (word, i) {
        var forceCap =
          i === 0 ||
          i === words.length - 1 ||
          opts.forceFirst ||
          opts.forceLast ||
          ALWAYS_CAPITALIZE[word.toLowerCase()];
        if (word.indexOf('-') >= 0) return capitalizeHyphenated(word, forceCap);
        return capitalizeAtomic(word, forceCap);
      })
      .join(' ');
  }

  function isEditorialTitleCase(raw) {
    var s = nz(raw);
    if (!s) return true;
    return toEditorialTitleCase(s) === s;
  }

  function applyEditorialTitleCaseToBlocks(blocks) {
    var list = Array.isArray(blocks) ? blocks : [];
    var changedSlots = [];
    var out = list.map(function (b) {
      var title = nz(b && b.title);
      if (!title) return b;
      if (title.length > 80 || wordCount(title) > 12) return b;
      var nextTitle = toEditorialTitleCase(title);
      if (nextTitle === title) return b;
      changedSlots.push({
        slotKey: nz(b.slotKey || b.slot),
        from: title,
        to: nextTitle
      });
      var copy = {};
      Object.keys(b).forEach(function (k) {
        copy[k] = b[k];
      });
      copy.title = nextTitle;
      return copy;
    });
    return {
      contract: BE_EDITORIAL_TITLE_CASE_V1,
      blocks: out,
      changedSlots: changedSlots,
      changedCount: changedSlots.length
    };
  }

  /**
   * RIGHT-column only: when brand is a cumulative proposal, normalize heading text.
   * LEFT current profile must remain verbatim.
   */
  function proposedHeading(brand, raw) {
    if (brand && (brand.beCumulativeProposal || brand.beCandidateOverlay)) {
      return toEditorialTitleCase(raw);
    }
    return nz(raw);
  }

  global.BeEditorialTitleCase = {
    BE_EDITORIAL_TITLE_CASE_V1: BE_EDITORIAL_TITLE_CASE_V1,
    toEditorialTitleCase: toEditorialTitleCase,
    isEditorialTitleCase: isEditorialTitleCase,
    normalizeHeadingConjunctionAmpersand: normalizeHeadingConjunctionAmpersand,
    applyEditorialTitleCaseToBlocks: applyEditorialTitleCaseToBlocks,
    proposedHeading: proposedHeading,
    FOUNDER_EXAMPLES: [
      { input: 'Global soft-collection scale', expected: 'Global Soft-Collection Scale' },
      { input: 'Conversion-weighted growth', expected: 'Conversion-Weighted Growth' },
      { input: 'Curio since 2014', expected: 'Curio Since 2014' },
      {
        input: 'Commercial Platform And Lobby Experience',
        expected: 'Commercial Platform & Lobby Experience'
      },
      {
        input: 'Independent & Soft-Brand Conversion',
        expected: 'Independent & Soft-Brand Conversion'
      }
    ]
  };
})(typeof globalThis !== 'undefined' ? globalThis : window);
