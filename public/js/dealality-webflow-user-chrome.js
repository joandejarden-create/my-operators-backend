/**
 * Apply /api/me identity (name + Airtable profile photo) to Webflow left nav account chrome ONLY.
 * Never touches main content, nav menu icons, or header images.
 *
 * Webflow: targets the last sidebar dropdown (account menu) and .avatar-circle inside it.
 * Optional: add data-dealality-user-avatar on the account <img> in Webflow Designer.
 */
(function (global) {
  "use strict";

  /** Railway /app shell account block. */
  var ACCOUNT_ROOTS =
    ".sidebar-footer, #accountDropdownWrap, .user-block, .account-dropdown, " +
    ".account-dropdown-header, .account-dropdown-trigger, [data-dealality-account-chrome]";

  var REAPPLY_DELAYS_MS = [400, 2000, 5000, 8000];
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

  /** Webflow account menu = last dropdown in the left sidebar. */
  function findWebflowAccountRoots(doc) {
    var sidebar = findSidebarContainer(doc);
    if (!sidebar) return [];

    var dropdowns = sidebar.querySelectorAll('.w-dropdown, [class*="dropdown"]:not([class*="menu"])');
    if (dropdowns.length) {
      return [dropdowns[dropdowns.length - 1]];
    }

    var toggles = sidebar.querySelectorAll('[class*="dropdown-toggle"], .w-dropdown-toggle');
    if (toggles.length) {
      return [toggles[toggles.length - 1].closest(".w-dropdown, [class*='dropdown']") || toggles[toggles.length - 1]];
    }

    return [];
  }

  function getAllAccountRoots(doc) {
    var seen = new Set();
    var roots = [];

    function add(el) {
      if (!el || seen.has(el)) return;
      seen.add(el);
      roots.push(el);
    }

    doc.querySelectorAll(ACCOUNT_ROOTS).forEach(add);
    findWebflowAccountRoots(doc).forEach(add);
    doc.querySelectorAll("[data-dealality-account-chrome]").forEach(add);

    return roots;
  }

  function isInsideAccountChrome(el, roots) {
    if (!el || !el.closest) return false;
    if (el.matches && el.matches("[data-dealality-user-avatar]")) return true;
    if (el.closest("[data-dealality-user-avatar]")) return true;
    var list = roots || getAllAccountRoots(global.document);
    for (var i = 0; i < list.length; i++) {
      if (list[i].contains(el)) return true;
    }
    return !!el.closest(ACCOUNT_ROOTS);
  }

  function isAvatarShell(el) {
    if (!el || !el.classList) return false;
    if (el.classList.contains("user-avatar")) return true;
    if (el.classList.contains("avatar-circle")) return true;
    return /avatar-circle/i.test(el.className || "");
  }

  function collectAvatarElements(doc) {
    var roots = getAllAccountRoots(doc);
    var seen = new Set();
    var out = [];

    function add(el) {
      if (!el || seen.has(el)) return;
      if (!isInsideAccountChrome(el, roots) && !(el.matches && el.matches("[data-dealality-user-avatar]"))) return;
      seen.add(el);
      out.push(el);
    }

    doc.querySelectorAll("[data-dealality-user-avatar]").forEach(function (marker) {
      add(marker);
      marker.querySelectorAll("img").forEach(add);
    });

    roots.forEach(function (root) {
      root.querySelectorAll("img").forEach(add);
      root.querySelectorAll(
        ".user-avatar, img.user-avatar-image, .avatar-circle, [class*='avatar-circle']"
      ).forEach(add);
    });

    return out;
  }

  function updateLabelsInRoot(root, name, meta) {
    root.querySelectorAll(".user-name").forEach(function (el) {
      el.textContent = name;
    });
    root.querySelectorAll(".user-meta").forEach(function (el) {
      el.textContent = meta;
    });

    root.querySelectorAll("div, span, p").forEach(function (el) {
      if (el.children.length > 0) return;
      var t = (el.textContent || "").trim();
      if (!t) return;
      if (/account\s*settings/i.test(t)) {
        el.textContent = meta;
        var prev = el.previousElementSibling;
        if (prev && (!prev.querySelector("img") || prev.children.length === 0)) {
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
    if (!img || img.tagName !== "IMG") return;
    var observer = new MutationObserver(function () {
      if (!pinnedPhotoUrl) return;
      if (!urlsSame(img.src, pinnedPhotoUrl) && img.getAttribute("src") !== pinnedPhotoUrl) {
        setImgSrc(img, pinnedPhotoUrl, initial || pinnedInitial);
      }
    });
    observer.observe(img, { attributes: true, attributeFilter: ["src"] });
    avatarObservers.push(observer);
  }

  /** Keep Airtable photo when Memberstack (data-ms-member) rewrites src later. */
  function pinMarkedAvatars(doc, url, initial) {
    pinnedPhotoUrl = url;
    pinnedInitial = initial;
    disconnectAvatarObservers();
    doc.querySelectorAll("[data-dealality-user-avatar]").forEach(function (marker) {
      var imgs =
        marker.tagName === "IMG"
          ? [marker]
          : Array.prototype.slice.call(marker.querySelectorAll("img"));
      imgs.forEach(function (img) {
        setImgSrc(img, url, initial);
        watchAvatarImg(img, url, initial);
      });
    });
  }

  function setImgSrc(img, url, initial) {
    if (!img || !url) return false;
    var next = String(url).trim();
    if (!next) return false;
    img.alt = initial || "Profile";
    img.removeAttribute("hidden");
    img.style.display = "";
    if (img.src === next || img.getAttribute("src") === next) {
      img.removeAttribute("src");
      img.src = next;
    } else {
      img.src = next;
    }
    return true;
  }

  function applyPhotoToElement(el, url, initial) {
    if (!el || !url) return false;
    if (el.tagName === "IMG") {
      return setImgSrc(el, url, initial);
    }
    var nested = el.querySelector("img");
    if (nested) {
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
    if (!el || el.tagName === "IMG") return;
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
    var roots = getAllAccountRoots(doc);
    var name = displayNameFromUser(u);
    var meta = u.email || (data.dealality && data.dealality.role ? data.dealality.role : "Account settings");
    var initial = name.charAt(0).toUpperCase();
    var photoUrl = u.profilePhotoUrl ? String(u.profilePhotoUrl).trim() : "";

    roots.forEach(function (root) {
      updateLabelsInRoot(root, name, meta);
    });

    var photoApplied = false;
    if (photoUrl) {
      pinMarkedAvatars(doc, photoUrl, initial);
      collectAvatarElements(doc).forEach(function (el) {
        if (applyPhotoToElement(el, photoUrl, initial)) photoApplied = true;
      });
      if (!photoApplied && doc.querySelector("[data-dealality-user-avatar]")) {
        photoApplied = true;
      }
    } else {
      disconnectAvatarObservers();
      pinnedPhotoUrl = null;
    }

    if (!photoApplied) {
      roots.forEach(function (root) {
        root.querySelectorAll(".user-avatar, .avatar-circle, [class*='avatar-circle']").forEach(function (el) {
          if (el.tagName !== "IMG") applyInitialToElement(el, initial);
        });
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
    userPayloadFromMe: userPayloadFromMe,
    collectAvatarElements: collectAvatarElements,
    getAllAccountRoots: getAllAccountRoots,
    accountRootSelectors: ACCOUNT_ROOTS,
  };
})(typeof window !== "undefined" ? window : global);
