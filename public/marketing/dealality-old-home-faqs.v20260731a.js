/**
 * Old Home FAQs (v20260731a)
 * Path-gated to /old-home.
 * Brings dealality.com landing FAQs (minus private-beta) into #faq,
 * owner-first order, compact collapsed spacing to fit prior vertical budget.
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase();
    if (path !== "/old-home") return;
    if (window.__ohFaqs >= 202607331) return;
  } catch (ePath) {
    return;
  }

  /* Owner-first: control → confidentiality → role → commercial endpoint → network → AI */
  var ITEMS = [
    {
      q: "1. Who sees my project?",
      paras: [
        "Only the parties you select. There are no public listings on Dealality and no open browsing capability. Owners control who sees their project at every stage. You can start with a blind teaser to protect your identity and remove any party from the process at any point.",
      ],
    },
    {
      q: "2. How is confidentiality handled?",
      paras: [
        "Confidentiality is the foundation of how the platform works — not a feature added on top. All projects are private by default. Confidential materials are stored in NDA-gated deal rooms accessible only after NDAs are executed digitally. No information is shared across parties outside the specific deal it was submitted for.",
      ],
    },
    {
      q: "3. Is Dealality a broker?",
      paras: [
        "No. Dealality is a software platform — not a broker, advisor, or intermediary. We facilitate the structure and workflow of the evaluation process. We do not advise on brand choice, represent either party, or earn commissions from transactions. Our role ends at LOI.",
      ],
    },
    {
      q: "4. What happens at LOI?",
      paras: [
        "Dealality's role ends at LOI. Once the owner selects a preferred brand or operator and the parties are ready to proceed, they move offline for MA/FA drafting, franchise agreement negotiation, and legal documentation. Our success-based fee for owners applies at the LOI milestone.",
      ],
    },
    {
      q: "5. How are brands and operators selected for the platform?",
      paras: [
        "Membership on the brand and operator side is by application and approval. We do not list every brand — only those actively engaged in development and willing to participate in a structured, responsive process. Brands complete a profile covering development criteria, geography, chain scale, and fee parameters.",
      ],
    },
    {
      q: "6. How does AI support the process?",
      paras: [
        "AI is used as a supporting capability — not the core product. It generates structured deal summaries from owner submissions, helping reduce preliminary qualification time. It does not make decisions, recommend brands, or substitute for professional judgment. All selection decisions remain with the owner and the parties involved.",
      ],
    },
  ];

  var H2 = "Questions Owners Actually Ask.";
  var LEAD =
    "Clear answers on confidentiality, control, how Dealality works with brands and operators, and where the platform's role begins and ends.";

  var started = false;
  var retries = 0;

  function injectCss() {
    var prev = document.getElementById("oh-faq-compact");
    if (prev && prev.parentNode) prev.parentNode.removeChild(prev);
    var css = document.createElement("style");
    css.id = "oh-faq-compact";
    css.textContent = [
      "#faq #faq-list{padding:28px 48px!important}",
      "#faq #faq-lead{margin-bottom:28px!important}",
      "#faq details summary{",
      "padding-top:2px!important;padding-bottom:2px!important;min-height:0!important}",
      "#faq [id$='-q'],#faq .oh-faq-q{",
      "font-size:clamp(1rem,.92rem + .45vw,1.28rem)!important;",
      "line-height:1.35!important}",
      "#faq [id$='-div'],#faq .oh-faq-div{margin:12px 0!important}",
      "#faq #faq-7,#faq #faq-7-div,#faq #faq-8,#faq #faq-8-div,",
      "#faq #faq-9,#faq #faq-9-div,#faq #faq-10,#faq #faq-10-div{",
      "display:none!important}",
      "@media(max-width:767px){",
      "#faq #faq-list{padding:22px 18px!important}",
      "#faq [id$='-div']{margin:10px 0!important}}",
    ].join("");
    document.head.appendChild(css);
  }

  function setText(el, text) {
    if (!el) return;
    el.textContent = text;
  }

  function applyBody(body, paras) {
    if (!body) return;
    body.innerHTML = "";
    paras.forEach(function (t) {
      var p = document.createElement("p");
      p.textContent = t;
      body.appendChild(p);
    });
  }

  function apply() {
    var root = document.getElementById("faq");
    if (!root) return false;
    var list = root.querySelector("#faq-list");
    if (!list) return false;

    injectCss();
    setText(root.querySelector("#faq-h2"), H2);
    setText(root.querySelector("#faq-lead"), LEAD);

    ITEMS.forEach(function (item, i) {
      var n = i + 1;
      var details = root.querySelector("#faq-" + n);
      if (!details) return;
      setText(details.querySelector("#faq-" + n + "-q"), item.q);
      applyBody(details.querySelector("#faq-" + n + "-body"), item.paras);
      details.removeAttribute("hidden");
      details.style.removeProperty("display");
    });

    [7, 8, 9, 10].forEach(function (n) {
      var d = root.querySelector("#faq-" + n);
      var div = root.querySelector("#faq-" + n + "-div");
      if (d) {
        d.setAttribute("hidden", "");
        d.open = false;
      }
      if (div) div.setAttribute("hidden", "");
    });

    return true;
  }

  function start() {
    if (started) return;
    if (!apply()) {
      scheduleRetry();
      return;
    }
    started = true;
    window.__ohFaqs = 202607331;
    window.requestAnimationFrame(function () {
      apply();
    });
    window.setTimeout(function () {
      apply();
    }, 120);
    window.setTimeout(function () {
      apply();
    }, 600);
  }

  function scheduleRetry() {
    if (retries >= 40) return;
    retries += 1;
    window.setTimeout(start, 50);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
