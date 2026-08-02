/**
 * Old Home Ecosystem Spanish i18n (v20260802a)
 * Path-gated to /es.
 *
 * Translates ecosystem chrome that is not available as localizable Webflow nodes:
 * badge, connector labels, and role chips. Also repairs step-title casing to match EN Title Case.
 */
(function () {
  "use strict";
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/es" && path.indexOf("/es/") !== 0) return;
    if (window.__ohEcoI18n >= 2026080201) return;
    window.__ohEcoI18n = 2026080201;
  } catch (e) {
    return;
  }

  var MAP = {
    "Built for the Full Decision Ecosystem": "Creado para el Ecosistema Completo de Decisión",
    "Owner-Led. Participant-Connected.": "Dirigido por el Propietario. Conectado con los Participantes.",
    "Opportunity Context": "Contexto de la Oportunidad",
    Responses: "Respuestas",
    "Decision Lead": "Líder de Decisión",
    "Process Lead": "Líder de Proceso",
    "Capital Criteria": "Criterios de Capital",
  };

  function norm(s) {
    return String(s || "").replace(/\s+/g, " ").trim();
  }

  function applyText(el, value) {
    if (!el || !value) return;
    if (norm(el.textContent) !== value) el.textContent = value;
  }

  function translateTree(root) {
    if (!root) return;
    var eco = root.querySelector ? root.querySelector("#ecosystem") : null;
    if (!eco && root.id === "ecosystem") eco = root;
    if (!eco) return;

    Object.keys(MAP).forEach(function (en) {
      var es = MAP[en];
      eco.querySelectorAll("span, h3, p, div, li").forEach(function (el) {
        if (el.children && el.children.length) return;
        if (norm(el.textContent) === en) applyText(el, es);
      });
    });

    eco.querySelectorAll(".oh-eco-step-h").forEach(function (h) {
      var txt = norm(h.textContent);
      if (/Decisi[oó]n mejor/i.test(txt) || (/alineada/i.test(txt) && /Decisi/i.test(txt))) {
        if (/<br\s*\/?>/i.test(h.innerHTML || "")) {
          h.innerHTML = "Decisión Mejor<br/>Alineada";
        } else {
          h.textContent = "Decisión Mejor Alineada";
        }
      }
    });
  }

  function boot() {
    translateTree(document);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
  [0, 400, 1200, 2500, 5000].forEach(function (ms) {
    window.setTimeout(boot, ms);
  });
})();
