(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;

    var cssHref =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/PLACEHOLDER_w19.css";
    var swapped = false;
    document.querySelectorAll('link[rel="stylesheet"]').forEach(function (link) {
      var href = link.getAttribute("href") || "";
      if (/dealality-old-home-freeform-head\.v20260729w\d+\.css/.test(href)) {
        link.setAttribute("href", cssHref);
        swapped = true;
      }
    });
    if (!swapped) {
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = cssHref;
      document.head.appendChild(link);
    }

    var root = document.getElementById("about");
    if (!root || root.getAttribute("data-oh-problem-v2") === "1") return;
    root.setAttribute("data-oh-problem-v2", "1");
    var h2 = document.getElementById("about-h2");
    if (h2)
      h2.innerHTML =
        "Most hotel owners do not lack options.<br>They lack a good way to compare them.";
    var lead = document.getElementById("about-lead");
    if (lead)
      lead.textContent =
        "Hotel opportunities are still evaluated across emails, slide decks, spreadsheets, calls, and separate advisor conversations. Different parties receive different information, respond in different formats, and use different assumptions. That makes the process slower, the options harder to compare, and the full potential of the asset easier to miss.";
    var lead2 = document.getElementById("about-lead-2");
    if (lead2) lead2.setAttribute("hidden", "");
    var close = document.getElementById("about-close");
    if (close) close.setAttribute("hidden", "");
    var cards = [
      {
        id: "about-point-1",
        title: "Fragmented outreach",
        body: "Owners repeat the same story across separate conversations.",
        icon: '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="12" cy="14" r="3.5" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="28" cy="14" r="3.5" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="20" cy="28" r="3.5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M15 15.5l8-1M25.5 16.5l-4.5 8M14.5 16.5l4.5 8" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".55"/><path d="M8 8l2.5 2.5M32 8l-2.5 2.5M20 34v-2" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".4"/></svg>',
      },
      {
        id: "about-point-2",
        title: "Slower comparison",
        body: "Brands and partners respond in different formats.",
        icon: '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="6" y="10" width="11" height="20" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><rect x="23" y="10" width="11" height="20" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><path d="M9 15h5M9 19h5M9 23h4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".55"/><path d="M26 15h5M26 19h3M26 23h5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".55"/><path d="M18 16h3M18 24h3" stroke="#9B8AFB" stroke-width="1.2" stroke-dasharray="1.5 1.5" opacity=".35"/><path d="M29 27l1.5 1.5 3-3.5" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round" opacity=".45"/></svg>',
      },
      {
        id: "about-point-3",
        title: "Missed upside",
        body: "Better-fit paths may never be explored.",
        icon: '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="20" cy="20" r="12" stroke="#9B8AFB" stroke-width="1.5" stroke-dasharray="3 3" opacity=".55"/><path d="M20 10v4M20 26v4M10 20h4M26 20h4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".4"/><circle cx="20" cy="20" r="3" fill="#9B8AFB" opacity=".55"/><path d="M27 11l5-5M32 6v4M32 6h-4" stroke="#9B8AFB" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/><path d="M13 27c2.2-2.8 5.2-4 7-4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".35"/></svg>',
      },
    ];
    cards.forEach(function (card) {
      var li = document.getElementById(card.id);
      if (!li) return;
      li.classList.add("oh-about-point");
      var iconId = card.id + "-icon";
      var existing = document.getElementById(iconId);
      if (!existing) {
        existing = document.createElement("div");
        existing.id = iconId;
        existing.className = "about-point-icon mod-icon";
        existing.setAttribute("aria-hidden", "true");
        li.insertBefore(existing, li.firstChild);
      }
      existing.classList.add("about-point-icon", "mod-icon");
      existing.innerHTML = card.icon;
      var strong = li.querySelector("strong");
      var span = li.querySelector("span");
      if (strong) strong.textContent = card.title;
      if (span) span.textContent = card.body;
    });
    var visual = document.getElementById("about-visual");
    if (!visual) return;
    visual.setAttribute("aria-label", "Fragmented evaluation process");
    visual.innerHTML =
      '<div id="about-frag">' +
      '<p id="about-frag-eyebrow">How it usually happens today</p>' +
      '<div id="about-frag-anchor">One hotel opportunity</div>' +
      '<div id="about-frag-scatter" aria-hidden="true">' +
      '<span class="about-frag-chip is-channel c1">Brand conversation</span>' +
      '<span class="about-frag-chip is-channel c2">Operator introduction</span>' +
      '<span class="about-frag-chip is-channel c3">Advisor recommendation</span>' +
      '<span class="about-frag-chip is-channel c4">Capital discussion</span>' +
      '<span class="about-frag-chip is-format c5">Proposal in email</span>' +
      '<span class="about-frag-chip is-format c6">Terms in spreadsheet</span>' +
      '<span class="about-frag-chip is-format c7">Questions in calls</span>' +
      '<span class="about-frag-chip is-format c8">Documents in PDFs</span>' +
      "</div>" +
      '<div id="about-frag-verdict">' +
      '<p id="about-frag-hard">Hard to compare fairly</p>' +
      '<div id="about-frag-diffs"><span>Different info</span><span>Different assumptions</span><span>Different formats</span></div>' +
      '<p id="about-frag-hidden">Potential value stays hidden</p>' +
      "</div>" +
      "</div>";
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-problem-v2]", err);
    }
  }
})();
