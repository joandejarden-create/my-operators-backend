/**
 * Old Home — Insights feed (v20260817a)
 * Replaces hardcoded homepage cards with the newest published Insights Posts.
 * Reads the live /insights CMS collection list so new posts appear without
 * republishing the homepage. Filters English vs Spanish by page locale.
 */
(function () {
  "use strict";
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    var isEs = path === "/es" || path.indexOf("/es/") === 0;
    if (path !== "/" && !isEs) return;

    var VERSION = "17a";
    var VERSION_NUM = 2026081701;
    if (window.__ohInsightsFeed >= VERSION_NUM) return;
    window.__ohInsightsFeed = VERSION_NUM;

    var GRID_ID = "insights-grid";
    var HUB_URL = "/insights";
    var MAX_CARDS = 18;

    var ES_TITLE_RE = /[áéíóúñü¿¡]/i;
    var ES_WORD_RE =
      /\b(qué|como|cómo|guía|guia|propietarios|hotelera|hotelero|franquicia|marcas|visibilidad|contrato|selección|seleccion|costo|oculto|negociación|negociacion|residencias|cambio|suaves|duras|administración|administracion)\b/i;
    var ES_SLUG_RE =
      /(^|-)(de|del|la|el|los|las|una|un|para|como|guia|propietarios|hotelera|hotelero|franquicia|marcas|visibilidad|contrato|seleccion|costo|oculto|negociacion|residencias|cambio|suaves|duras|que|por-que|socio-desarrollo|programas-desarrollo)(-|$)/i;

    function isSpanishPost(title, href) {
      var slug = "";
      try {
        slug = (href || "").split("/insights-posts/")[1] || "";
        slug = slug.replace(/\/+$/, "").split("?")[0];
      } catch (_e) {}
      return ES_TITLE_RE.test(title || "") || ES_WORD_RE.test(title || "") || ES_SLUG_RE.test(slug);
    }

    function categoryFor(title, href) {
      var t = ((title || "") + " " + (href || "")).toLowerCase();
      if (/aeo|geo|ai visibility|visibilidad/.test(t)) return isEs ? "Visibilidad IA" : "AI Visibility";
      if (/reflag|conversion|cambio de marca/.test(t)) return isEs ? "Conversión" : "Conversion";
      if (/soft brand|hard brand|marca suave|marca dura/.test(t)) return isEs ? "Estrategia de Marca" : "Brand Strategy";
      if (/marriott|hilton|ihg|hyatt|wyndham|development partner|socio de desarrollo|programas de desarrollo/.test(t))
        return isEs ? "Desarrollo" : "Development";
      if (/management agreement|franchise agreement|contrato de (administraci|gesti)/.test(t))
        return isEs ? "Contratos" : "Agreements";
      if (/key money/.test(t)) return isEs ? "Franquicia" : "Franchise";
      if (/branded residences|residencias con marca|mixed-use|uso mixto/.test(t))
        return isEs ? "Uso Mixto" : "Mixed-Use";
      if (/operator|operador/.test(t)) return isEs ? "Operador" : "Operators";
      if (/brand|marca/.test(t)) return isEs ? "Estrategia de Marca" : "Brand Strategy";
      return "Insights";
    }

    function esc(s) {
      return String(s || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }

    function absUrl(href) {
      if (!href) return "/insights";
      try {
        return new URL(href, location.origin).pathname;
      } catch (_e) {
        return href;
      }
    }

    function parseHub(html) {
      var doc = new DOMParser().parseFromString(html, "text/html");
      var nodes = doc.querySelectorAll(".insights-card, a.card.insights-card, .w-dyn-item a[href*='/insights-posts/']");
      var seen = {};
      var posts = [];
      for (var i = 0; i < nodes.length; i++) {
        var a = nodes[i];
        if (a.tagName !== "A") a = a.querySelector("a[href*='/insights-posts/']") || a;
        var href = a.getAttribute("href") || "";
        if (href.indexOf("/insights-posts/") === -1) continue;
        var path = absUrl(href);
        if (seen[path]) continue;
        seen[path] = true;
        var titleEl = a.querySelector(".insights-card-title, h3");
        var dateEl = a.querySelector(".insights-card-date");
        var imgEl = a.querySelector("img");
        var title = ((titleEl && titleEl.textContent) || a.getAttribute("aria-label") || "").replace(/\s+/g, " ").trim();
        if (!title) continue;
        posts.push({
          href: path,
          title: title,
          date: ((dateEl && dateEl.textContent) || "").replace(/\s+/g, " ").trim(),
          img: imgEl ? imgEl.getAttribute("src") || "" : "",
          alt: imgEl ? imgEl.getAttribute("alt") || title : title,
          spanish: isSpanishPost(title, path)
        });
      }
      return posts;
    }

    function cardHtml(post, i) {
      var more = isEs ? "Leer más" : "Read more";
      var cat = categoryFor(post.title, post.href);
      var img = post.img
        ? '<img src="' +
          esc(post.img) +
          '" loading="lazy" alt="' +
          esc(post.alt || post.title) +
          '" class="oh-ins-img"/>'
        : "";
      return (
        '<article class="oh-ins-card" data-oh-ins="' +
        i +
        '">' +
        '<a href="' +
        esc(post.href) +
        '" class="oh-ins-img-wrap w-inline-block">' +
        img +
        "</a>" +
        '<div class="oh-ins-meta">' +
        '<span class="oh-ins-cat">' +
        esc(cat) +
        "</span>" +
        '<span class="oh-ins-dot"></span>' +
        '<span class="oh-ins-date">' +
        esc(post.date) +
        "</span>" +
        "</div>" +
        '<h3 class="oh-ins-title"><a href="' +
        esc(post.href) +
        '" class="oh-ins-title-link">' +
        esc(post.title) +
        "</a></h3>" +
        '<a href="' +
        esc(post.href) +
        '" class="oh-ins-more"><span>' +
        esc(more) +
        "</span></a>" +
        "</article>"
      );
    }

    function bindCarousel(grid) {
      var prev = document.getElementById("insights-prev");
      var next = document.getElementById("insights-next");
      function step() {
        var card = grid.querySelector(".oh-ins-card");
        if (!card) return grid.clientWidth || 360;
        var styles = getComputedStyle(grid);
        var gap = parseFloat(styles.columnGap || styles.gap) || 40;
        return card.getBoundingClientRect().width + gap;
      }
      function go(dir) {
        grid.scrollBy({ left: dir * step(), behavior: "smooth" });
      }
      function onPrev(e) {
        e.preventDefault();
        go(-1);
      }
      function onNext(e) {
        e.preventDefault();
        go(1);
      }
      if (prev) {
        prev.addEventListener("click", onPrev);
        prev.setAttribute("role", "button");
      }
      if (next) {
        next.addEventListener("click", onNext);
        next.setAttribute("role", "button");
      }
    }

    function render(grid, posts) {
      var html = "";
      for (var i = 0; i < posts.length; i++) html += cardHtml(posts[i], i);
      grid.innerHTML = html;
      grid.setAttribute("data-oh-insights-feed", VERSION);
      bindCarousel(grid);
    }

    function boot() {
      var grid = document.getElementById(GRID_ID);
      if (!grid) return;
      var original = grid.innerHTML;
      fetch(HUB_URL, { credentials: "same-origin" })
        .then(function (res) {
          if (!res.ok) throw new Error("insights hub " + res.status);
          return res.text();
        })
        .then(function (html) {
          var posts = parseHub(html).filter(function (p) {
            return isEs ? p.spanish : !p.spanish;
          });
          if (!posts.length) throw new Error("no locale posts");
          render(grid, posts.slice(0, MAX_CARDS));
        })
        .catch(function () {
          if (grid && !grid.getAttribute("data-oh-insights-feed")) grid.innerHTML = original;
          bindCarousel(grid);
        });
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
  } catch (_e) {}
})();
