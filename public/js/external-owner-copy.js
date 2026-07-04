/**
 * Browser sanitizer for Brand Explorer display copy (loaded before atelier-from-api.js).
 */
(function (global) {
  'use strict';

  var REPLACEMENTS = [
    [
      /\s*Parsed from Choice FDD text\s+[\w.-]+\.txt\s*\(Item\s*\d+\)\.?\s*(?:Confirm against (?:current )?countersigned FDD\.?)?/gi,
      ''
    ],
    [
      /\s*Derived from Choice FDD\s+[\w.-]+\.txt\s*Item\s*\d+[^.]*\.?\s*(?:Confirm against countersigned franchise agreement\.?)?/gi,
      ''
    ],
    [/\s*Parsed from Choice FDD text[^\n.]*(?:\(Item\s*\d+\))?\.?/gi, ''],
    [/\s*Derived from Choice FDD[^\n.]*(?:\(Item\s*\d+\))?\.?/gi, ''],
    [/\s*\bConfirm against (?:current )?countersigned (?:FDD|franchise agreement)\.?/gi, ''],
    [/\s*\(Choice internal messaging\)/gi, ''],
    [/\s*\(Choice internal data, press kit\)/gi, ''],
    [/\s*\(Choice press kit\)/gi, ''],
    [/\s*\(press kit internal data\)/gi, ''],
    [/\s*\([^)]*\bpress kit\b[^)]*\)/gi, ''],
    [/\s*\(press kit\)/gi, ''],
    [/\s*—\s*Choice internal data, press kit\.?/gi, '.'],
    [/\s*\(consumer marketing claim\)/gi, ''],
    [/\s*—\s*press kit\.?/gi, '.'],
    [/\bChoice internal data, press kit\b/gi, 'Choice Hotels published figures'],
    [/\bTier 1 CHI Item 19 set\b/gi, 'published Choice franchise disclosures'],
    [/\bTier 1 CHI brands\b/gi, 'Choice Hotels franchise brands'],
    [/\bDealality CHI reference\b/gi, 'franchise disclosure document'],
    [/\bCHI Brands Architecture Oct 2025\b/gi, 'Choice Hotels brand architecture portfolio'],
    [/\bCHI reference\b/gi, 'franchise disclosure'],
    [/\bfixtures\/choice-media-center-text\/[^\s)]+/gi, 'Choice Hotels media center'],
    [/\bdocs\/choice-privileges[^\s.]*/gi, 'choicehotels.com/choice-privileges'],
    [
      /\bUpload property-specific assets in Brand Setup materials when ready\.?/gi,
      'Add property-specific photos and floor plans to your deal materials when available.'
    ],
    [/\bSource:\s*fixtures\//gi, 'Available from Choice Hotels '],
    [/\bpress kit\b/gi, 'Choice Hotels brand materials'],
    [/\bPatch-missing only\.?/gi, ''],
    [/\bconfirm current counts in FDD\b/gi, 'confirm current counts in your franchise disclosure document'],
    [/\bConfirm in FDD\b/g, 'Confirm in your franchise disclosure document'],
    [/\bin FDD\b/g, 'in your franchise disclosure document'],
    [/\bprior indicator copy:\s*/gi, ''],
    [/Flexibility indicators on [^\n]+ use canonical levels only[^\n]*\n*/gi, ''],
    [/\s*Sample\s+[Bb]rand-to-[Oo]wner\s+[Mm]essage\s*/gi, ''],
    [/\s*Common owner talking point:\s*/gi, ''],
    [/\s*Owners hear about\s+/gi, 'Expect '],
    [/\s*Brands often quantify\s+/gi, 'Franchise materials may quantify '],
    [/\s*Brands caveat\s+/gi, 'Performance varies '],
    [/\s*Brands position this as\s+/gi, 'This is often framed as '],
    [/\s*is a recurring sales line\s+/gi, 'can support ']
  ];

  var INTERNAL_LINE = [
    /^parsed from choice fdd/i,
    /^derived from choice fdd/i,
    /^source:\s*fixtures\//i,
    /^upload property-specific assets in brand setup/i,
    /^patch-missing only/i,
    /^sample brand-to-owner message/i,
    /^common owner talking point/i
  ];

  function isInternalProcessLine(line) {
    var t = String(line || '').trim();
    if (!t) return false;
    var lower = t.toLowerCase();
    for (var i = 0; i < INTERNAL_LINE.length; i++) {
      if (INTERNAL_LINE[i].test(lower)) return true;
    }
    return false;
  }

  function filterInternalLines(text) {
    return String(text || '')
      .split(/\n/)
      .filter(function (line) {
        return !isInternalProcessLine(line);
      })
      .join('\n');
  }

  function tidyWhitespace(text) {
    return text
      .replace(/[ \t]+\n/g, '\n')
      .replace(/\n{3,}/g, '\n\n')
      .replace(/[ \t]{2,}/g, ' ')
      .replace(/\s+([.,;:!?])/g, '$1')
      .replace(/\(\s*\)/g, '')
      .replace(/\s+\./g, '.')
      .trim();
  }

  function sanitizeExternalCopy(text) {
    if (text == null) return '';
    if (typeof text !== 'string') return String(text);
    var s = filterInternalLines(text);
    for (var i = 0; i < REPLACEMENTS.length; i++) {
      s = s.replace(REPLACEMENTS[i][0], REPLACEMENTS[i][1]);
    }
    s = tidyWhitespace(s);
    if (/^parsed from choice fdd/i.test(s) || /^derived from choice fdd/i.test(s)) return '';
    return s;
  }

  global.DealalitySanitizeExternalCopy = { sanitizeExternalCopy: sanitizeExternalCopy };
})(typeof window !== 'undefined' ? window : globalThis);
