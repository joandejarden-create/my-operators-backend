/**
 * Shared stage controller for Harbour House marketing capture scenes.
 * Capture scripts call window.__paSetStage(n) and window.__paReady.
 */
(function () {
  function applyFades(stage) {
    document.querySelectorAll('[data-fade]').forEach(function (el) {
      var on = Number(el.getAttribute('data-on-stage') || 0);
      el.classList.toggle('is-on', stage >= on);
    });
  }

  function setStage(stage) {
    var n = Number(stage) || 0;
    document.body.setAttribute('data-stage', String(n));
    var label = document.getElementById('stage-label');
    if (label) label.textContent = 'Stage ' + n;
    applyFades(n);
    if (window.__paSceneHooks && typeof window.__paSceneHooks.applyStage === 'function') {
      window.__paSceneHooks.applyStage(n);
    }
  }

  window.__paSetStage = setStage;
  window.__paReady = false;

  function boot() {
    setStage(0);
    window.__paReady = true;
    document.body.setAttribute('data-pa-ready', '1');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
