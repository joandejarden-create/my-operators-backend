/**
 * Canonical Dealality wave loader markup.
 * Shared design-system SoT for GDI · ADP · Brand Explorer · Hotel Explorer.
 * Contract: DEALALITY_SHARED_LOADING_COMPONENT_V1
 * Presentation: Animated Dealality Wave + Compact Black Rounded Container + message.
 * Do not invent a second wave animation or competing toast system.
 */
(function () {
  'use strict';

  var CONTRACT = 'DEALALITY_SHARED_LOADING_COMPONENT_V1';

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
   * @param {{
   *   subText?: string,
   *   mainId?: string,
   *   fixed?: boolean,
   *   compact?: boolean,
   *   size?: 'sm'|'md',
   *   progress?: boolean,
   *   wrapperClass?: string,
   *   label?: string
   * }} [opts]
   */
  function html(mainText, opts) {
    opts = opts || {};
    var mainId = opts.mainId ? ' id="' + escapeHtml(opts.mainId) + '"' : '';
    var sub = opts.subText
      ? '<div class="loading-text-time">' + escapeHtml(opts.subText) + '</div>'
      : '<div class="loading-text-time"></div>';
    var fixed = opts.fixed !== false && opts.compact !== true;
    var size = opts.size === 'sm' || opts.compact ? 'sm' : 'md';
    var showProgress = opts.progress !== false && opts.compact !== true;
    var loaderClass =
      'loading dealality-wave-loader' +
      (fixed ? ' dealality-wave-loader--fixed' : ' dealality-wave-loader--inline') +
      (opts.compact || size === 'sm' ? ' dealality-wave-loader--compact' : '') +
      (size === 'sm' ? ' dealality-wave-loader--sm' : '');
    var wrapOpen = opts.wrapperClass
      ? '<div class="' + escapeHtml(opts.wrapperClass) + '">'
      : '';
    var wrapClose = opts.wrapperClass ? '</div>' : '';
    var ariaLabel = opts.label || mainText || 'Loading';

    return (
      wrapOpen +
      '<div class="' +
      loaderClass +
      '" role="status" aria-live="polite" aria-busy="true" aria-label="' +
      escapeHtml(ariaLabel) +
      '">' +
      '<div class="loading-content">' +
      '<div class="wave-container" aria-hidden="true">' +
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
      (showProgress
        ? '<div class="loading-progress"><div class="loading-progress-bar"></div></div>'
        : '') +
      '</div>' +
      wrapClose
    );
  }

  /** Compact inline subsection loader (Hotel Explorer / shared products). */
  function compactHtml(mainText, opts) {
    opts = opts || {};
    opts.compact = true;
    opts.fixed = false;
    if (opts.progress == null) opts.progress = false;
    if (!opts.size) opts.size = 'sm';
    return html(mainText, opts);
  }

  /**
   * Compact centered loading toast — same wave orb + Dealality toast chrome.
   * Use for short subsection fetches; not a full-width banner and not a fixed page overlay.
   */
  function toastHtml(mainText, opts) {
    opts = opts || {};
    opts.compact = true;
    opts.fixed = false;
    opts.progress = false;
    opts.size = 'sm';
    var hostClass = opts.hostClass || 'dealality-loading-toast-host';
    var inner = html(mainText, Object.assign({}, opts, { wrapperClass: null }));
    // Inject toast modifier onto the loader root class list.
    inner = inner.replace(
      'dealality-wave-loader--compact',
      'dealality-wave-loader--compact dealality-wave-loader--toast'
    );
    return (
      '<div class="' +
      escapeHtml(hostClass) +
      '" data-dealality-loading-toast="1">' +
      inner +
      '</div>'
    );
  }

  window.DealalityWaveLoader = {
    CONTRACT: CONTRACT,
    html: html,
    compactHtml: compactHtml,
    toastHtml: toastHtml,
    waveParticlesHtml: waveParticlesHtml
  };
})();
