/**
 * Old Home — Ecosystem / 3-step / pricing plain-English lock (v20260808b)
 * Path-gated to /, /es, /old-home.
 * Designer cannot set_text on Span/ListItem; this reinforces Clear-Not-Clever copy.
 * v20260808b: sentence-case eco close line.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    var isEs = path === "/es" || path.indexOf("/es/") === 0;
    if (path !== "/" && path !== "/old-home" && !isEs) return;
    if (window.__ohEcoPricingCopy >= 2026080802) return;
    window.__ohEcoPricingCopy = 2026080802;

    var EN = {
      ownerBullets: [
        "Define the opportunity",
        "Choose who participates",
        "Control who sees what",
        "Choose the path",
      ],
      brandBullets: [
        "Check fit",
        "Respond with the right information",
        "Clarify terms and requirements",
      ],
      advisorBullets: [
        "Lead the process for owners",
        "Bring judgment",
        "Run outreach and responses",
        "Protect the owner",
      ],
      capitalBullets: [
        "Review structured opportunities",
        "Clarify financing needs",
        "Spot missing information",
      ],
      steps: [
        { h: "One hotel opportunity", p: "All context, one place" },
        { h: "Owner or advisor leads", p: "Structured, confidential, connected" },
        { h: "Better-aligned decision", p: "Clear next steps" },
      ],
      close:
        "The right people. The right information. The right time.",
      permission: "You choose who sees what, and when",
      exploreCta: "Explore your hotel opportunity",
      startReview: "Start an opportunity review",
    };

    var ES = {
      ownerBullets: [
        "Define la oportunidad",
        "Elige quién participa",
        "Controla quién ve qué",
        "Elige el camino",
      ],
      brandBullets: [
        "Revisa el encaje",
        "Responde con la información correcta",
        "Aclara términos y requisitos",
      ],
      advisorBullets: [
        "Lidera el proceso para propietarios",
        "Aporta criterio",
        "Gestiona prospección y respuestas",
        "Protege al propietario",
      ],
      capitalBullets: [
        "Revisa oportunidades estructuradas",
        "Aclara necesidades de financiamiento",
        "Detecta información faltante",
      ],
      steps: [
        { h: "Una oportunidad hotelera", p: "Todo el contexto, en un solo lugar" },
        { h: "El propietario o asesor lidera", p: "Estructurado, confidencial, conectado" },
        { h: "Decisión mejor alineada", p: "Próximos pasos claros" },
      ],
      close:
        "Las personas correctas. La información correcta. El momento correcto.",
      permission: "Tú eliges quién ve qué, y cuándo",
      exploreCta: "Explora tu oportunidad hotelera",
      startReview: "Inicia una revisión de oportunidad",
    };

    var C = isEs ? ES : EN;

    function setText(el, text) {
      if (!el || !text) return;
      if ((el.textContent || "").trim() === text) return;
      el.textContent = text;
    }

    function setListBullets(cardSelector, bullets) {
      var card = document.querySelector(cardSelector);
      if (!card) return;
      var items = card.querySelectorAll("li");
      for (var i = 0; i < items.length && i < bullets.length; i++) {
        var span = items[i].querySelector("span") || items[i];
        setText(span, bullets[i]);
      }
    }

    function applyRoleBullets() {
      // Cards are ordered in the ecosystem band; match by heading text.
      var cards = document.querySelectorAll("#ecosystem article, #eco article, .oh-eco-card, [class*='oh-eco-card']");
      if (!cards || !cards.length) {
        // Fallback: walk headings near known eco section
        var heads = document.querySelectorAll("h3.oh-eco-card-h-1, #eco-h2 ~ * h3, #ecosystem h3");
        for (var h = 0; h < heads.length; h++) {
          var title = (heads[h].textContent || "").toLowerCase();
          var list = heads[h].parentElement && heads[h].parentElement.querySelector("ul");
          if (!list) continue;
          var bullets = null;
          if (title.indexOf("owner") !== -1 || title.indexOf("propietar") !== -1 || title.indexOf("investor") !== -1 || title.indexOf("inversor") !== -1) {
            bullets = C.ownerBullets;
          } else if (title.indexOf("brand") !== -1 || title.indexOf("marca") !== -1 || title.indexOf("operator") !== -1 || title.indexOf("operador") !== -1) {
            bullets = C.brandBullets;
          } else if (title.indexOf("advisor") !== -1 || title.indexOf("asesor") !== -1) {
            bullets = C.advisorBullets;
          } else if (title.indexOf("capital") !== -1) {
            bullets = C.capitalBullets;
          }
          if (!bullets) continue;
          var items = list.querySelectorAll("li");
          for (var i = 0; i < items.length && i < bullets.length; i++) {
            var span = items[i].querySelector("span") || items[i];
            setText(span, bullets[i]);
          }
        }
        return;
      }
      for (var c = 0; c < cards.length; c++) {
        var h3 = cards[c].querySelector("h3");
        if (!h3) continue;
        var t = (h3.textContent || "").toLowerCase();
        var b = null;
        if (t.indexOf("owner") !== -1 || t.indexOf("propietar") !== -1 || t.indexOf("investor") !== -1 || t.indexOf("inversor") !== -1) b = C.ownerBullets;
        else if (t.indexOf("brand") !== -1 || t.indexOf("marca") !== -1 || t.indexOf("operator") !== -1 || t.indexOf("operador") !== -1) b = C.brandBullets;
        else if (t.indexOf("advisor") !== -1 || t.indexOf("asesor") !== -1) b = C.advisorBullets;
        else if (t.indexOf("capital") !== -1) b = C.capitalBullets;
        if (!b) continue;
        var lis = cards[c].querySelectorAll("li");
        for (var j = 0; j < lis.length && j < b.length; j++) {
          var s = lis[j].querySelector("span") || lis[j];
          setText(s, b[j]);
        }
      }
    }

    function applySteps() {
      var hs = document.querySelectorAll("h3.oh-eco-step-h");
      var ps = document.querySelectorAll("p.oh-eco-step-p");
      for (var i = 0; i < hs.length && i < C.steps.length; i++) {
        setText(hs[i], C.steps[i].h);
      }
      for (var k = 0; k < ps.length && k < C.steps.length; k++) {
        setText(ps[k], C.steps[k].p);
      }
    }

    function applyClose() {
      var close = document.querySelector("p.oh-eco-close-primary");
      if (close) setText(close, C.close);
    }

    function applyPricingPermission() {
      var lists = document.querySelectorAll(
        "#pricing-brands-features li, #pricing-operators-features li, #pricing ul li"
      );
      for (var i = 0; i < lists.length; i++) {
        var t = (lists[i].textContent || "").toLowerCase();
        if (
          t.indexOf("permission-based") !== -1 ||
          t.indexOf("basada en permisos") !== -1 ||
          t.indexOf("sala de acuerdos") !== -1
        ) {
          setText(lists[i], C.permission);
        }
      }
    }

    function applyCtas() {
      var explore = document.getElementById("fsw-btn-text");
      if (explore) setText(explore, C.exploreCta);
      var start = document.getElementById("cta-band-btn-text") || document.querySelector("#cta-band a, #cta a");
      if (start) {
        var label = (start.textContent || "").toLowerCase();
        if (label.indexOf("opportunity review") !== -1 || label.indexOf("revisión") !== -1 || label.indexOf("start") !== -1 || label.indexOf("inic") !== -1) {
          setText(start, C.startReview);
        }
      }
      var pricingOwners = document.getElementById("pricing-owners-cta");
      if (pricingOwners) setText(pricingOwners, isEs ? "Explora tu oportunidad" : "Explore your opportunity");
    }

    function apply() {
      applyRoleBullets();
      applySteps();
      applyClose();
      applyPricingPermission();
      applyCtas();
    }

    function boot() {
      apply();
      window.setTimeout(apply, 50);
      window.setTimeout(apply, 250);
      window.setTimeout(apply, 800);
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
    window.addEventListener("load", apply);
  } catch (err) {
    if (typeof console !== "undefined" && console.error) {
      console.error("[oh-eco-pricing-copy]", err);
    }
  }
})();
