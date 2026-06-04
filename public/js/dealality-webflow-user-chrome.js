/**
 * Apply /api/me identity (name + Airtable profile photo) to Webflow left nav account chrome ONLY.
 * Photo target: [data-dealality-user-avatar] only — never dropdown menu item icons.
 */
(function (global) {
  "use strict";

  var ACCOUNT_LABEL_ROOTS =
    ".sidebar-footer, #accountDropdownWrap, .user-block, .account-dropdown-header, " +
    ".account-dropdown-trigger, [data-dealality-account-chrome]";

  var DROPDOWN_MENU_EXCLUDE =
    ".w-dropdown-list, [class*='dropdown-list'], [class*='dropdown-menu'], " +
    ".account-dropdown-item, [role='menu'], [role='menuitem']";

  var REAPPLY_DELAYS_MS = [400, 2000, 5000, 8000, 12000];
  var pinnedPhotoUrl = null;
  var pinnedInitial = "";
  var avatarObservers = [];

  function userPayloadFromMe(data) {
    if (!data || typeof data !== "object") return null;
    if (data.user && (data.user.email || data.user.firstName || data.user.lastName || data.user.profilePhotoUrl)) {
      return data.user;
    }
    var airtable = data.airtable;
    if (!airtable) return null;
    return {
      email: airtable.email || null,
      firstName: airtable.firstName || null,
      lastName: airtable.lastName || null,
      profilePhotoUrl:
        airtable.profilePhotoUrl ||
        (data.dealality && data.dealality.profilePhotoUrl) ||
        null,
    };
  }

  function displayNameFromUser(u) {
    var name = [u.firstName, u.lastName].filter(Boolean).join(" ").trim();
    if (name) return name;
    if (u.email) return String(u.email).split("@")[0];
    return "Account";
  }

  function findSidebarContainer(doc) {
    return (
      doc.querySelector("aside") ||
      doc.querySelector('[class*="sidebar"]:not([class*="main"])') ||
      doc.querySelector('[class*="side-bar"]') ||
      doc.querySelector('[class*="Side-Bar"]') ||
      null
    );
  }

  /** Webflow account trigger only — not the open menu list with item icons. */
  function findAccountDropdownToggle(doc) {
    var sidebar = findSidebarContainer(doc);
    if (!sidebar) return null;
    var toggles = sidebar.querySelectorAll(".w-dropdown-toggle, [class*='dropdown-toggle']");
    return toggles.length ? toggles[toggles.length - 1] : null;
  }

  function isInsideDropdownMenu(el) {
    return !!(el && el.closest && el.closest(DROPDOWN_MENU_EXCLUDE));
  }

  function getLabelUpdateRoots(doc) {
    var seen = new Set();
    var roots = [];

    function add(el) {
      if (!el || seen.has(el)) return;
      seen.add(el);
      roots.push(el);
    }

    doc.querySelectorAll(ACCOUNT_LABEL_ROOTS).forEach(add);
    var toggle = findAccountDropdownToggle(doc);
    if (toggle) add(toggle);

    return roots;
  }

  function isAvatarShell(el) {
    if (!el || !el.classList) return false;
    if (el.classList.contains("user-avatar")) return true;
    if (el.classList.contains("avatar-circle")) return true;
    return /avatar-circle/i.test(el.className || "");
  }

  /** Only the marked sidebar avatar — never menu icons. */
  function collectAvatarElements(doc) {
    var out = [];
    var seen = new Set();

    function add(el) {
      if (!el || seen.has(el) || isInsideDropdownMenu(el)) return;
      seen.add(el);
      out.push(el);
    }

    doc.querySelectorAll("[data-dealality-user-avatar]").forEach(function (marker) {
      if (isInsideDropdownMenu(marker)) return;
      if (marker.tagName === "IMG") {
        add(marker);
        return;
      }
      var img = marker.querySelector("img");
      if (img) add(img);
      else add(marker);
    });

    if (!out.length) {
      var toggle = findAccountDropdownToggle(doc);
      if (toggle) {
        var circle = toggle.querySelector(".avatar-circle, [class*='avatar-circle']");
        if (circle) {
          if (circle.tagName === "IMG") add(circle);
          else {
            var nested = circle.querySelector("img");
            add(nested || circle);
          }
        }
      }
    }

    return out;
  }

  function updateLabelsInRoot(root, name, meta) {
    root.querySelectorAll(".user-name").forEach(function (el) {
      if (!isInsideDropdownMenu(el)) el.textContent = name;
    });
    root.querySelectorAll(".user-meta").forEach(function (el) {
      if (!isInsideDropdownMenu(el)) el.textContent = meta;
    });

    root.querySelectorAll("div, span, p").forEach(function (el) {
      if (el.children.length > 0 || isInsideDropdownMenu(el)) return;
      var t = (el.textContent || "").trim();
      if (!t) return;
      if (/account\s*settings/i.test(t)) {
        el.textContent = meta;
        var prev = el.previousElementSibling;
        if (prev && !isInsideDropdownMenu(prev) && (!prev.querySelector("img") || prev.children.length === 0)) {
          prev.textContent = name;
        }
      }
    });
  }

  function urlsSame(a, b) {
    if (!a || !b) return false;
    if (a === b) return true;
    try {
      return String(a).split("?")[0] === String(b).split("?")[0];
    } catch (_) {
      return false;
    }
  }

  function disconnectAvatarObservers() {
    avatarObservers.forEach(function (observer) {
      observer.disconnect();
    });
    avatarObservers = [];
  }

  function watchAvatarImg(img, url, initial) {
    if (!img || img.tagName !== "IMG" || isInsideDropdownMenu(img)) return;
    var observer = new MutationObserver(function () {
      if (!pinnedPhotoUrl) return;
      if (!urlsSame(img.src, pinnedPhotoUrl) && img.getAttribute("src") !== pinnedPhotoUrl) {
        setImgSrc(img, pinnedPhotoUrl, initial || pinnedInitial);
      }
    });
    observer.observe(img, { attributes: true, attributeFilter: ["src", "srcset"] });
    avatarObservers.push(observer);
  }

  function pinMarkedAvatars(doc, url, initial) {
    pinnedPhotoUrl = url;
    pinnedInitial = initial;
    disconnectAvatarObservers();
    var applied = false;
    collectAvatarElements(doc).forEach(function (target) {
      if (applyPhotoToElement(target, url, initial)) applied = true;
      if (target.tagName === "IMG") watchAvatarImg(target, url, initial);
    });
    return applied;
  }

  function setImgSrc(img, url, initial) {
    if (!img || !url || isInsideDropdownMenu(img)) return false;
    var next = String(url).trim();
    if (!next) return false;
    img.alt = initial || "Profile";
    img.removeAttribute("hidden");
    img.style.display = "";
    img.removeAttribute("srcset");
    img.removeAttribute("sizes");
    if (img.src === next || img.getAttribute("src") === next) {
      img.removeAttribute("src");
      img.src = next;
    } else {
      img.src = next;
    }
    return true;
  }

  function applyPhotoToElement(el, url, initial) {
    if (!el || !url || isInsideDropdownMenu(el)) return false;
    if (el.tagName === "IMG") {
      return setImgSrc(el, url, initial);
    }
    var nested = el.querySelector("img");
    if (nested && !isInsideDropdownMenu(nested)) {
      if (setImgSrc(nested, url, initial)) {
        if (el !== nested) el.textContent = "";
        return true;
      }
      return false;
    }
    if (!isAvatarShell(el)) return false;
    el.textContent = "";
    el.style.backgroundImage = 'url("' + String(url).replace(/"/g, "%22") + '")';
    el.style.backgroundSize = "cover";
    el.style.backgroundPosition = "center center";
    el.style.backgroundRepeat = "no-repeat";
    el.style.color = "transparent";
    return true;
  }

  function applyInitialToElement(el, initial) {
    if (!el || el.tagName === "IMG" || isInsideDropdownMenu(el)) return;
    el.textContent = initial;
    el.style.backgroundImage = "";
    el.style.color = "";
    var nested = el.querySelector("img");
    if (nested) {
      nested.removeAttribute("src");
      nested.style.display = "none";
    }
  }

  function applyUserChrome(data) {
    var u = userPayloadFromMe(data);
    if (!u) return false;

    var doc = global.document;
    var name = displayNameFromUser(u);
    var meta = u.email || (data.dealality && data.dealality.role ? data.dealality.role : "Account settings");
    var initial = name.charAt(0).toUpperCase();
    var photoUrl = u.profilePhotoUrl ? String(u.profilePhotoUrl).trim() : "";

    getLabelUpdateRoots(doc).forEach(function (root) {
      updateLabelsInRoot(root, name, meta);
    });

    var photoApplied = false;
    if (photoUrl) {
      photoApplied = pinMarkedAvatars(doc, photoUrl, initial);
    } else {
      disconnectAvatarObservers();
      pinnedPhotoUrl = null;
      if (doc.querySelector("[data-dealality-user-avatar]") && global.location && /dealality\.com|railway/i.test(global.location.hostname || "")) {
        console.warn(
          "[DealalityWebflowUserChrome] No profilePhotoUrl from /api/me — add a Profile attachment on the Airtable Users row."
        );
      }
    }

    if (!photoApplied && !photoUrl) {
      collectAvatarElements(doc).forEach(function (el) {
        if (el.tagName !== "IMG") applyInitialToElement(el, initial);
      });
    }

    if (photoUrl && !photoApplied && global.location && /dealality\.com|localhost|127\.0\.0\.1|railway/i.test(global.location.hostname || "")) {
      console.warn(
        "[DealalityWebflowUserChrome] profilePhotoUrl present but no account avatar matched.",
        "Add data-dealality-user-avatar on the sidebar Avatar Circle image in Webflow."
      );
    }

    return photoApplied;
  }

  function debugState() {
    var doc = global.document;
    var ctx = global.__dealalityUserContext;
    var payload = userPayloadFromMe(ctx);
    var marker = doc.querySelector("[data-dealality-user-avatar]");
    var img = marker ? (marker.tagName === "IMG" ? marker : marker.querySelector("img")) : null;
    return {
      hasUserContext: !!ctx,
      profilePhotoUrl: (payload && payload.profilePhotoUrl) || null,
      hasAvatarMarker: !!marker,
      markerTag: marker ? marker.tagName : null,
      currentImgSrc: img ? img.src : null,
      avatarTargets: collectAvatarElements(doc).length,
      pinnedPhotoUrl: pinnedPhotoUrl || null,
    };
  }

  function tryApplyStoredContext() {
    if (global.__dealalityUserContext) {
      apply(global.__dealalityUserContext);
    }
  }

  function apply(data) {
    applyUserChrome(data);
    REAPPLY_DELAYS_MS.forEach(function (ms) {
      global.setTimeout(function () {
        applyUserChrome(data);
      }, ms);
    });
    return data;
  }

  global.DealalityWebflowUserChrome = {
    apply: apply,
    applyOnce: applyUserChrome,
    debug: debugState,
    userPayloadFromMe: userPayloadFromMe,
    collectAvatarElements: collectAvatarElements,
    accountRootSelectors: ACCOUNT_LABEL_ROOTS,
  };

  if (global.document) {
    global.addEventListener("load", tryApplyStoredContext);
    global.document.addEventListener("dealality-me-ready", function (ev) {
      if (ev && ev.detail && ev.detail.data) apply(ev.detail.data);
    });
  }
})(typeof window !== "undefined" ? window : global);
