/**
 * Client-side deal package validation (My Deals). Optional findings for save UI.
 */
(function (global) {
  'use strict';

  function validateDealPackage(payload) {
    return { ok: true, findings: [] };
  }

  global.validateDealPackage = validateDealPackage;
})(typeof window !== 'undefined' ? window : globalThis);
