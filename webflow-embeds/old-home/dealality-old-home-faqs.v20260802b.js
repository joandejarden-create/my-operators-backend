/**
 * Old Home FAQs (v20260802b)
 * Path-gated to /, /old-home, and /es.
 * COMBINES original Old Home stakeholder FAQs + dealality.com landing FAQs
 * (private-beta excluded). Ordered by importance to hotel owners.
 * Dedupes overlapping confidentiality (keeps landing operational detail).
 * v20260731f: dividers between items only (none after last; ensure after Q10).
 * v20260801a: chevron aligned to question row; rotates down when open.
 * v20260801b: copy polish + max 3 boot retries (no 50ms×40 storm).
 * v20260801d: mobile — after open, scroll so question top is readable first.
 * v20260802b: /es — localize question titles only; keep Webflow ES answers.
 */
(function () {
  "use strict";

  var IS_ES = false;
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    IS_ES = path === "/es" || path.indexOf("/es/") === 0;
    if (path !== "/" && path !== "/old-home" && !IS_ES) return;
    if (window.__ohFaqs >= 2026080202) return;
  } catch (ePath) {
    return;
  }

  var ES_QUESTIONS = [
    "¿Por qué no hablar directamente con una marca u operador hotelero?",
    "¿Acaso no es distinta cada oportunidad hotelera?",
    '¿Qué significa realmente "dejar valor sobre la mesa"?',
    "¿Cómo puede Dealality aumentar el valor de mi oportunidad hotelera?",
    "¿Puede Dealality decirme qué marca debo elegir?",
    "¿En qué se diferencia Dealality de contratar un consultor?",
    "¿Y si ya tengo una marca u operador preferido?",
    "¿Para qué tipos de proyectos hoteleros está diseñada Dealality?",
    "¿Mi información es confidencial?",
    "¿Qué sucede después de empezar?",
  ];

  /**
   * Owner-importance order:
   * control → confidentiality → commercial role → value thesis →
   * decision agency → fit/scope → onboarding → LOI economics → AI
   */
  var ITEMS = [
    {
      q: "Who sees my project?",
      paras: [
        "Only the parties you select. There are no public listings on Dealality and no open browsing capability. Owners control who sees their project at every stage. You can start with a blind teaser to protect your identity and remove any party from the process at any point.",
      ],
    },
    {
      q: "How is confidentiality handled?",
      paras: [
        "Confidentiality is the foundation of how the platform works — not a feature added on top. All projects are private by default. Confidential materials are stored in NDA-gated deal rooms accessible only after NDAs are executed digitally. No information is shared with parties outside the specific deal for which it was submitted.",
      ],
    },
    {
      q: "Is Dealality a broker?",
      paras: [
        "No. Dealality is a software platform — not a broker, advisor, or intermediary. We facilitate the structure and workflow of the evaluation process. We do not advise on brand choice, represent either party, or earn commissions from transactions. Our role ends at LOI.",
      ],
    },
    {
      q: "Why shouldn't I just talk directly to a hotel brand or operator?",
      paras: [
        "You should.",
        "The question is when.",
        "Most owners begin with the first available relationship before fully evaluating what the opportunity could become.",
        "Dealality helps you understand the strongest strategic paths before those conversations begin, so you're approaching the market with greater clarity, stronger positioning, and a more informed strategy.",
        "The platform also speeds communication, reduces repetitive work, improves participant coordination, and tracks deal activity to support faster execution.",
      ],
    },
    {
      q: 'What does "leaving value on the table" actually mean?',
      paras: [
        "Value isn't limited to purchase price or management fees.",
        "It can come from choosing a different brand, operating model, positioning, financing strategy, commercial structure, or development approach.",
        "Many owners never evaluate those alternatives.",
        "Dealality helps reveal where additional value may exist before long-term decisions are made.",
      ],
    },
    {
      q: "How can Dealality increase the value of my hotel opportunity?",
      paras: [
        "The value of a hotel opportunity isn't determined by the asset alone.",
        "It is also influenced by the strategy behind it.",
        "The right brand, operator, positioning, financing approach, commercial structure, and competitive process can materially affect long-term performance and flexibility.",
        "Dealality helps owners evaluate those strategic paths before committing to one, increasing the likelihood that the selected path aligns with the owner's objectives and captures more of the opportunity's potential.",
      ],
    },
    {
      q: "Can Dealality tell me which brand I should choose?",
      paras: [
        "No.",
        "Dealality is designed to help owners evaluate credible strategic paths—not make decisions for them.",
        "The goal is to help you understand the trade-offs, compare alternatives consistently, and move forward with greater confidence.",
      ],
    },
    {
      q: "What if I already have a preferred brand or operator?",
      paras: [
        "Many owners do.",
        "Dealality isn't about replacing your preferred option.",
        "It's about helping you confirm that it remains the strongest strategic choice after evaluating credible alternatives.",
        "Sometimes it does.",
        "Sometimes another path creates greater value.",
        "Either way, you'll make the decision with better information.",
      ],
    },
    {
      q: "Isn't every hotel opportunity different?",
      paras: [
        "Exactly.",
        'That\'s why there is rarely one "best" brand or operator.',
        "Every opportunity should be evaluated based on its market, objectives, ownership priorities, development constraints, financial goals, and long-term vision.",
        "Dealality helps structure that evaluation so the chosen path fits the opportunity—not the other way around.",
      ],
    },
    {
      q: "How is Dealality different from hiring a consultant?",
      paras: [
        "Dealality doesn't replace experienced advisors.",
        "It gives owners and their advisors a structured way to evaluate opportunities, compare alternatives, organize information, and support major hotel decisions.",
        "Many owners choose to use Dealality alongside trusted advisors.",
      ],
    },
    {
      q: "What types of hotel projects is Dealality designed for?",
      paras: [
        "Dealality supports a wide range of hotel opportunities, including:",
      ],
      list: [
        "New developments",
        "Hotel conversions",
        "Repositioning projects",
        "Mixed-use developments",
        "Branded residences",
        "Existing hotels evaluating new brands or operators",
      ],
      after: [
        "If you're making an important strategic decision about a hotel asset, Dealality is designed to support that process.",
      ],
    },
    {
      q: "How are brands and operators selected for the platform?",
      paras: [
        "Membership on the brand and operator side is by application and approval. We do not list every brand — only those actively engaged in development and willing to participate in a structured, responsive process. Brands complete a profile covering development criteria, geography, chain scale, and fee parameters.",
      ],
    },
    {
      q: "What happens after I start?",
      paras: [
        "You'll begin by describing your hotel opportunity and the decision you're evaluating.",
        "Dealality then helps organize the opportunity, explore strategic paths, evaluate alternatives, compare proposals, and support your decision process.",
        "You remain in control throughout.",
      ],
    },
    {
      q: "What happens at LOI?",
      paras: [
        "Dealality's role ends at LOI. Once the owner selects a preferred brand or operator and the parties are ready to proceed, they move offline for MA/FA drafting, franchise agreement negotiation, and legal documentation. Our success-based fee for owners applies at the LOI milestone.",
      ],
    },
    {
      q: "How does AI support the process?",
      paras: [
        "AI is used as a supporting capability — not the core product. It generates structured deal summaries from owner submissions, helping reduce preliminary qualification time. It does not make decisions, recommend brands, or substitute for professional judgment. All selection decisions remain with the owner and the parties involved.",
      ],
    },
  ];

  var H2 = "Questions Owners Actually Ask.";
  var LEAD =
    "Clear answers on confidentiality, control, value, how Dealality works with brands and operators, and where the platform's role begins and ends.";

  var started = false;
  var retries = 0;

  function injectCss() {
    var prev = document.getElementById("oh-faq-compact");
    if (prev && prev.parentNode) prev.parentNode.removeChild(prev);
    var css = document.createElement("style");
    css.id = "oh-faq-compact";
    css.textContent = [
      "#faq #faq-list{--oh-faq-num-w:2.35rem;padding:28px 48px!important}",
      "#faq details summary{",
      "position:relative!important;",
      "padding-top:2px!important;padding-bottom:2px!important;min-height:0!important}",
      "#faq [id$='-q'],#faq .oh-faq-q{",
      "display:grid!important;grid-template-columns:var(--oh-faq-num-w) minmax(0,1fr)!important;",
      "column-gap:.4rem!important;align-items:start!important;",
      "font-size:clamp(1rem,.92rem + .45vw,1.28rem)!important;",
      "line-height:1.35!important}",
      "#faq .oh-faq-num{display:block;font-variant-numeric:tabular-nums;",
      "text-align:left;white-space:nowrap}",
      "#faq .oh-faq-qtext{display:block;min-width:0}",
      /* Chevron: same row as question when collapsed; rotate down when open */
      "#faq .oh-faq-chev,#faq [id$='-chev']{",
      "position:absolute!important;right:0!important;top:.35em!important;",
      "margin:0!important;width:20px!important;height:20px!important;",
      "display:block!important;transform:none!important;",
      "transition:transform .25s ease!important}",
      "#faq details[open] .oh-faq-chev,#faq details[open] [id$='-chev']{",
      "transform:rotate(90deg)!important}",
      "#faq [id$='-body'],#faq .oh-faq-body{",
      "padding-left:calc(var(--oh-faq-num-w) + .4rem)!important;",
      "box-sizing:border-box!important}",
      "#faq [id$='-div'],#faq .oh-faq-div{margin:12px 0!important}",
      "#faq #faq-list ul{margin:8px 0 12px 1.15rem!important;padding:0!important;",
      "color:rgba(255,255,255,.78)!important}",
      "#faq #faq-list li{margin:4px 0!important}",
      "@media(max-width:767px){",
      "#faq #faq-list{--oh-faq-num-w:2.1rem;padding:22px 18px!important}",
      "#faq [id$='-div']{margin:10px 0!important}",
      /* Keep opened question clear of sticky nav when scrollIntoView runs */
      "#faq details summary{scroll-margin-top:72px!important}}",
    ].join("");
    document.head.appendChild(css);
  }

  function isMobileFaqViewport() {
    try {
      return window.matchMedia("(max-width: 767px)").matches;
    } catch (eM) {
      return window.innerWidth <= 767;
    }
  }

  function stickyNavOffset() {
    var nav = document.getElementById("nav");
    if (!nav) return 72;
    var h = Math.round(nav.getBoundingClientRect().height || 0);
    return Math.max(56, h + 8);
  }

  function preferReducedMotion() {
    try {
      return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    } catch (eR) {
      return false;
    }
  }

  function scrollOpenedQuestionIntoView(details) {
    if (!details || !details.open || !isMobileFaqViewport()) return;
    var sum = details.querySelector("summary") || details;
    var behavior = preferReducedMotion() ? "auto" : "smooth";
    var run = function () {
      if (!details.open) return;
      var top =
        sum.getBoundingClientRect().top +
        (window.pageYOffset || document.documentElement.scrollTop || 0) -
        stickyNavOffset();
      if (typeof window.scrollTo === "function") {
        try {
          window.scrollTo({ top: Math.max(0, top), behavior: behavior });
          return;
        } catch (eS) {}
      }
      window.scrollTo(0, Math.max(0, top));
    };
    /* Double rAF: wait for <details> open layout before measuring. */
    window.requestAnimationFrame(function () {
      window.requestAnimationFrame(run);
    });
  }

  function bindMobileOpenScroll(list) {
    if (!list || list.getAttribute("data-oh-faq-scroll") === "1") return;
    list.setAttribute("data-oh-faq-scroll", "1");
    var items = list.querySelectorAll("details.oh-faq-item, details[id^='faq-']");
    var i;
    for (i = 0; i < items.length; i++) {
      (function (d) {
        d.addEventListener("toggle", function () {
          scrollOpenedQuestionIntoView(d);
        });
      })(items[i]);
    }
  }

  function setText(el, text) {
    if (!el) return;
    el.textContent = text;
  }

  function setQuestion(el, n, question) {
    if (!el) return;
    el.textContent = "";
    var num = document.createElement("span");
    num.className = "oh-faq-num";
    num.textContent = n + ".";
    var text = document.createElement("span");
    text.className = "oh-faq-qtext";
    text.textContent = question;
    el.appendChild(num);
    el.appendChild(text);
  }

  function applyBody(body, item) {
    if (!body) return;
    body.innerHTML = "";
    (item.paras || []).forEach(function (t) {
      var p = document.createElement("p");
      p.textContent = t;
      body.appendChild(p);
    });
    if (item.list && item.list.length) {
      var ul = document.createElement("ul");
      item.list.forEach(function (t) {
        var li = document.createElement("li");
        li.textContent = t;
        ul.appendChild(li);
      });
      body.appendChild(ul);
    }
    (item.after || []).forEach(function (t) {
      var p = document.createElement("p");
      p.textContent = t;
      body.appendChild(p);
    });
  }

  function ensureSlot(list, n, templateDetails, templateDiv) {
    var details = list.querySelector("#faq-" + n);
    var div = list.querySelector("#faq-" + n + "-div");
    if (details) {
      details.removeAttribute("hidden");
      details.style.removeProperty("display");
      if (div) {
        div.removeAttribute("hidden");
        div.style.removeProperty("display");
      }
      return details;
    }
    if (!templateDetails) return null;

    var clone = templateDetails.cloneNode(true);
    clone.id = "faq-" + n;
    clone.open = false;
    clone.removeAttribute("hidden");
    clone.style.removeProperty("display");

    var mapIds = function (el) {
      if (!el || !el.id) return;
      el.id = el.id.replace(/faq-\d+/, "faq-" + n);
    };
    mapIds(clone);
    clone.querySelectorAll("[id]").forEach(mapIds);

    list.appendChild(clone);

    if (templateDiv) {
      var divClone = templateDiv.cloneNode(true);
      divClone.id = "faq-" + n + "-div";
      divClone.removeAttribute("hidden");
      divClone.style.removeProperty("display");
      divClone.querySelectorAll("[id]").forEach(mapIds);
      list.appendChild(divClone);
    }

    return clone;
  }

  function hideExtraSlots(list, keepThrough) {
    var i;
    for (i = keepThrough + 1; i <= 20; i++) {
      var d = list.querySelector("#faq-" + i);
      var div = list.querySelector("#faq-" + i + "-div");
      if (!d && !div) continue;
      if (d) {
        d.setAttribute("hidden", "");
        d.open = false;
      }
      if (div) div.setAttribute("hidden", "");
    }
  }

  /** Dividers sit after Qn and separate it from Qn+1. None after the last item. */
  function syncDividers(list, count, templateDiv) {
    if (!list || count < 1) return;
    var i;
    var mapIds = function (el, n) {
      if (!el || !el.id) return;
      el.id = el.id.replace(/faq-\d+/, "faq-" + n);
    };

    for (i = 1; i < count; i++) {
      var details = list.querySelector("#faq-" + i);
      var div = list.querySelector("#faq-" + i + "-div");
      if (!div && templateDiv && details) {
        div = templateDiv.cloneNode(true);
        div.id = "faq-" + i + "-div";
        div.querySelectorAll("[id]").forEach(function (el) {
          mapIds(el, i);
        });
        if (details.nextSibling) {
          list.insertBefore(div, details.nextSibling);
        } else {
          list.appendChild(div);
        }
      }
      if (div) {
        div.removeAttribute("hidden");
        div.style.removeProperty("display");
      }
    }

    var lastDiv = list.querySelector("#faq-" + count + "-div");
    if (lastDiv) {
      lastDiv.setAttribute("hidden", "");
      lastDiv.style.display = "none";
    }
  }

  function applyEsQuestionsOnly() {
    var root = document.getElementById("faq");
    if (!root) return false;
    var list = root.querySelector("#faq-list");
    if (!list) return false;

    injectCss();
    var i;
    var found = 0;
    for (i = 0; i < ES_QUESTIONS.length; i++) {
      var n = i + 1;
      var qEl = list.querySelector("#faq-" + n + "-q");
      if (!qEl) continue;
      setQuestion(qEl, n, ES_QUESTIONS[i]);
      found += 1;
    }
    if (!found) return false;
    bindMobileOpenScroll(list);
    return true;
  }

  function apply() {
    if (IS_ES) return applyEsQuestionsOnly();

    var root = document.getElementById("faq");
    if (!root) return false;
    var list = root.querySelector("#faq-list");
    if (!list) return false;

    injectCss();
    setText(root.querySelector("#faq-h2"), H2);
    setText(root.querySelector("#faq-lead"), LEAD);

    var templateDetails = list.querySelector("#faq-1");
    var templateDiv = list.querySelector("#faq-1-div");
    if (!templateDetails) return false;

    ITEMS.forEach(function (item, i) {
      var n = i + 1;
      var details = ensureSlot(list, n, templateDetails, templateDiv);
      if (!details) return;
      setQuestion(details.querySelector("#faq-" + n + "-q"), n, item.q);
      applyBody(details.querySelector("#faq-" + n + "-body"), item);
    });

    hideExtraSlots(list, ITEMS.length);
    syncDividers(list, ITEMS.length, templateDiv);
    bindMobileOpenScroll(list);
    return true;
  }

  function start() {
    if (started) return;
    if (!apply()) {
      scheduleRetry();
      return;
    }
    started = true;
    window.__ohFaqs = 2026080202;
    /* One soft re-apply after paint — avoid 120ms/600ms rewrite storm. */
    window.requestAnimationFrame(function () {
      apply();
    });
  }

  function scheduleRetry() {
    if (retries >= 3) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[oh-faqs] markup not ready after 3 retries");
      }
      return;
    }
    retries += 1;
    window.setTimeout(start, 100 * retries);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
