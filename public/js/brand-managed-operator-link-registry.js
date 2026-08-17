/**
 * Browser mirror of lib/partner-intelligence/brand-managed-operator-link-registry.js
 * Keep aliases + Master IDs in sync with the Node registry.
 */
(function (global) {
  'use strict';

  var LINKS = [
    {
      slug: 'marriott-international-managed',
      recordId: 'recGmiPhRt6hiayd9',
      companyName: 'Marriott International (Managed)',
      explorerPath: '/operator-explorer-gold-mock.html?id=recGmiPhRt6hiayd9',
      canonicalParent: 'Marriott International',
      aliases: ['Marriott International', 'Marriott', 'Marriott International, Inc.']
    },
    {
      slug: 'ihg-managed',
      recordId: 'rec7IXYQYpKMYsrDl',
      companyName: 'IHG Hotels & Resorts (Managed)',
      explorerPath: '/operator-explorer-gold-mock.html?id=rec7IXYQYpKMYsrDl',
      canonicalParent: 'IHG Hotels & Resorts',
      aliases: [
        'IHG Hotels & Resorts',
        'IHG',
        'InterContinental Hotels Group',
        'InterContinental Hotels Group (IHG)'
      ]
    },
    {
      slug: 'hilton-managed',
      recordId: 'rec3Uwxe6ovpiokuN',
      companyName: 'Hilton (Managed)',
      explorerPath: '/operator-explorer-gold-mock.html?id=rec3Uwxe6ovpiokuN',
      canonicalParent: 'Hilton',
      aliases: ['Hilton', 'Hilton Worldwide', 'Hilton Worldwide Holdings']
    },
    {
      slug: 'accor-managed',
      recordId: 'recF2WqLqNVyKGz9E',
      companyName: 'Accor (Managed)',
      explorerPath: '/operator-explorer-gold-mock.html?id=recF2WqLqNVyKGz9E',
      canonicalParent: 'Accor',
      aliases: ['Accor', 'AccorHotels', 'Accor Hotels', 'Accor Group']
    },
    {
      slug: 'minor-hotels-managed',
      recordId: 'rec8SrT3VjRkkYTxm',
      companyName: 'Minor Hotels (Managed)',
      explorerPath: '/operator-explorer-gold-mock.html?id=rec8SrT3VjRkkYTxm',
      canonicalParent: 'Minor Hotels',
      aliases: ['Minor Hotels', 'Minor International', 'Minor Hotel Group']
    }
  ];

  function normalizeParentKey(value) {
    return String(value || '')
      .trim()
      .toLowerCase()
      .replace(/\s+/g, ' ');
  }

  var aliasIndex = {};
  LINKS.forEach(function (link) {
    link.aliases.forEach(function (alias) {
      aliasIndex[normalizeParentKey(alias)] = link;
    });
    aliasIndex[normalizeParentKey(link.canonicalParent)] = link;
  });

  function resolve(parentCompany) {
    var key = normalizeParentKey(parentCompany);
    if (!key) return null;
    if (aliasIndex[key]) return aliasIndex[key];
    var keys = Object.keys(aliasIndex);
    for (var i = 0; i < keys.length; i += 1) {
      var aliasKey = keys[i];
      if (key === aliasKey || key.indexOf(aliasKey + ' ') === 0 || key.indexOf(aliasKey + ',') === 0) {
        return aliasIndex[aliasKey];
      }
    }
    return null;
  }

  function escapeHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  /** Calm single link chip — unused in Brand Explorer (kept for tooling / future Operator chrome). */
  function linkChipHtml(parentCompany) {
    var link = resolve(parentCompany);
    if (!link) return '';
    return (
      '<p class="be-brand-managed-operator-link">' +
      '<a class="be-brand-managed-operator-link__a" href="' +
      escapeHtml(link.explorerPath) +
      '" title="Open Operator Explorer brand-managed profile for ' +
      escapeHtml(link.companyName) +
      '">View brand-managed operator profile</a>' +
      '</p>'
    );
  }

  global.BrandManagedOperatorLink = {
    links: LINKS,
    resolve: resolve,
    linkChipHtml: linkChipHtml
  };
})(typeof window !== 'undefined' ? window : globalThis);
