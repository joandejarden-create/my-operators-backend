(function () {
  var tabs = [
    document.getElementById('persp-tab-owners'),
    document.getElementById('persp-tab-brands'),
    document.getElementById('persp-tab-advisors'),
  ].filter(Boolean);
  var panels = {
    'persp-panel-owners': document.getElementById('persp-panel-owners'),
    'persp-panel-brands': document.getElementById('persp-panel-brands'),
    'persp-panel-advisors': document.getElementById('persp-panel-advisors'),
  };
  function setPanel(el, on) {
    if (!el) return;
    if (on) {
      el.removeAttribute('hidden');
      el.setAttribute('aria-hidden', 'false');
      el.style.display = '';
    } else {
      el.setAttribute('hidden', '');
      el.setAttribute('aria-hidden', 'true');
      el.style.display = 'none';
    }
  }
  function show(panelId) {
    tabs.forEach(function (tab) {
      var on = tab.getAttribute('data-panel') === panelId || tab.getAttribute('aria-controls') === panelId;
      tab.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    Object.keys(panels).forEach(function (id) {
      setPanel(panels[id], id === panelId);
    });
  }
  tabs.forEach(function (tab) {
    tab.addEventListener('click', function (e) {
      e.preventDefault();
      show(tab.getAttribute('data-panel') || tab.getAttribute('aria-controls'));
    });
  });
  show('persp-panel-owners');
})();
