/**
 * Canonical My Deals wave loader markup.
 */
(function () {
  'use strict';

  function escapeHtml(text) {
    if (text == null || text === '') return '';
    return String(text)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function waveParticlesHtml() {
    return (
      '<div class="wave-particles">' +
      '<div class="particle"></div>' +
      '<div class="particle"></div>' +
      '<div class="particle"></div>' +
      '<div class="particle"></div>' +
      '</div>'
    );
  }

  /**
   * @param {string} mainText e.g. "Loading Operators…"
   * @param {{ subText?: string, mainId?: string, fixed?: boolean, wrapperClass?: string }} [opts]
   */
  function html(mainText, opts) {
    opts = opts || {};
    var mainId = opts.mainId ? ' id="' + escapeHtml(opts.mainId) + '"' : '';
    var sub = opts.subText
      ? '<div class="loading-text-time">' + escapeHtml(opts.subText) + '</div>'
      : '<div class="loading-text-time"></div>';
    var loaderClass =
      'loading dealality-wave-loader' + (opts.fixed === false ? '' : ' dealality-wave-loader--fixed');
    var wrapOpen = opts.wrapperClass
      ? '<div class="' + escapeHtml(opts.wrapperClass) + '">'
      : '';
    var wrapClose = opts.wrapperClass ? '</div>' : '';

    return (
      wrapOpen +
      '<div class="' +
      loaderClass +
      '" role="status" aria-live="polite">' +
      '<div class="loading-content">' +
      '<div class="wave-container">' +
      '<div class="wave wave-1"></div>' +
      '<div class="wave wave-2"></div>' +
      '<div class="wave wave-3"></div>' +
      waveParticlesHtml() +
      '</div>' +
      '<div>' +
      '<div class="loading-text-main"' +
      mainId +
      '>' +
      escapeHtml(mainText) +
      '</div>' +
      sub +
      '</div>' +
      '</div>' +
      '<div class="loading-progress"><div class="loading-progress-bar"></div></div>' +
      '</div>' +
      wrapClose
    );
  }

  window.DealalityWaveLoader = {
    html: html,
    waveParticlesHtml: waveParticlesHtml
  };
})();
