/**
 * Apply /api/me identity (name + Airtable profile photo) to Webflow left nav account chrome ONLY.
 * Never touches main content, nav menu icons, or header images.
 * Load on dealality.com site footer; call apply() after loadUserContext.
 *
 * Optional: add data-dealality-user-avatar on the sidebar account <img> in Webflow Designer.
 */
(function (global) {
  "use strict";

  /** Bottom-of-sidebar account block only — not the nav menu list. */
  var ACCOUNT_ROOTS =
    ".sidebar-footer, #accountDropdownWrap, .user-block, .account-dropdown, " +
    ".account-dropdown-header, .account-dropdown-trigger, [data-dealality-account-chrome]";

  var REAPPLY_DELAYS_MS = [400, 2000];

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

  function isInsideAccountChrome(el) {
    if (!el || !el.closest) return false;
    if (el.matches && el.matches("[data-dealality-user-avatar]")) return true;
    return !!el.closest(ACCOUNT_ROOTS);
  }

  function forEachAccountRoot(doc, fn) {
    doc.querySelectorAll(ACCOUNT_ROOTS).forEach(fn);
  }

  function collectAvatarElements(doc) {
    var seen = new Set();
    var out = [];

    function add(el) {
      if (!el || seen.has(el)) return;
      if (!isInsideAccountChrome(el) && !el.matches("[data-dealality-user-avatar]")) return;
      seen.add(el);
      out.push(el);
    }

    doc.querySelectorAll("[data-dealality-user-avatar]").forEach(add);

    forEachAccountRoot(doc, function (root) {
      root.querySelectorAll("img").forEach(add);
      root.querySelectorAll(".user-avatar, img.user-avatar-image").forEach(add);
    });

    return out;
  }

  function queryAccountText(doc, selector) {
    var out = [];
    forEachAccountRoot(doc, function (root) {
      root.querySelectorAll(selector).forEach(function (el) {
        out.push(el);
      });
    });
    return out;
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
    if (!el.classList || !el.classList.contains("user-avatar")) return false;
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
    var name = displayNameFromUser(u);
    var meta = u.email || (data.dealality && data.dealality.role ? data.dealality.role : "Account settings");
    var initial = name.charAt(0).toUpperCase();
    var photoUrl = u.profilePhotoUrl ? String(u.profilePhotoUrl).trim() : "";

    queryAccountText(doc, ".user-name").forEach(function (el) {
      el.textContent = name;
    });
    queryAccountText(doc, ".user-meta").forEach(function (el) {
      el.textContent = meta;
    });

    var photoApplied = false;
    if (photoUrl) {
      collectAvatarElements(doc).forEach(function (el) {
        if (applyPhotoToElement(el, photoUrl, initial)) photoApplied = true;
      });
    }

    if (!photoApplied) {
      forEachAccountRoot(doc, function (root) {
        root.querySelectorAll(".user-avatar").forEach(function (el) {
          if (el.tagName !== "IMG") applyInitialToElement(el, initial);
        });
      });
    }

    if (photoUrl && !photoApplied && global.location && /dealality\.com|localhost|127\.0\.0\.1|railway/i.test(global.location.hostname || "")) {
      console.warn(
        "[DealalityWebflowUserChrome] profilePhotoUrl present but no account avatar found inside",
        ACCOUNT_ROOTS,
        "— add data-dealality-user-avatar on the sidebar account image in Webflow."
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
    accountRootSelectors: ACCOUNT_ROOTS,
  };
})(typeof window !== "undefined" ? window : global);
