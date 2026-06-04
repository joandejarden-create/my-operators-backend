/**
 * Apply /api/me identity (name + Airtable profile photo) to Webflow left nav / account chrome.
 * Load on dealality.com site footer; call apply() after loadUserContext.
 */
(function (global) {
  "use strict";

  var AVATAR_SELECTORS =
    ".user-avatar, .user-block img, .account-dropdown-header img, img.user-avatar-image, " +
    "[data-dealality-user-avatar], .sidebar img, aside img, [class*='sidebar'] img, " +
    "[class*='nav-user'] img, [class*='profile-pic'] img, [class*='avatar'] img, " +
    "img[class*='avatar'], img[class*='profile']";

  var REAPPLY_DELAYS_MS = [300, 1000, 2500, 5000];
  var pendingReapplyTimer = null;

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

  function isLikelyNavAvatarImg(img) {
    if (!img || img.tagName !== "IMG") return false;
    try {
      var w = img.naturalWidth || img.width || img.offsetWidth || 0;
      var h = img.naturalHeight || img.height || img.offsetHeight || 0;
      if (w > 120 || h > 120) return false;
      if (w === 0 && h === 0) return true;
      return w <= 96 && h <= 96;
    } catch (_) {
      return true;
    }
  }

  function collectAvatarElements(doc) {
    var seen = new Set();
    var out = [];

    function add(el) {
      if (!el || seen.has(el)) return;
      seen.add(el);
      out.push(el);
    }

    doc.querySelectorAll(AVATAR_SELECTORS).forEach(add);

    doc.querySelectorAll(".user-name, [class*='user-name'], [class*='User-Name']").forEach(function (nameEl) {
      var root =
        nameEl.closest(
          "button, a, li, [class*='user'], [class*='account'], [class*='sidebar'], [class*='nav']"
        ) || nameEl.parentElement;
      if (!root) return;
      root.querySelectorAll("img").forEach(function (img) {
        if (isLikelyNavAvatarImg(img)) add(img);
      });
      root.querySelectorAll("[class*='avatar'], [class*='profile'], [class*='photo']").forEach(add);
    });

    doc.querySelectorAll("aside img, nav img, [class*='sidebar'] img").forEach(function (img) {
      if (isLikelyNavAvatarImg(img)) add(img);
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
    el.textContent = "";
    el.style.backgroundImage = 'url("' + String(url).replace(/"/g, "%22") + '")';
    el.style.backgroundSize = "cover";
    el.style.backgroundPosition = "center center";
    el.style.backgroundRepeat = "no-repeat";
    el.style.color = "transparent";
    return true;
  }

  function applyInitialToElement(el, initial) {
    if (!el) return;
    el.textContent = initial;
    el.style.backgroundImage = "";
    el.style.color = "";
    var nested = el.querySelector("img");
    if (nested && el.tagName !== "IMG") {
      nested.removeAttribute("src");
      nested.style.display = "none";
    }
  }

  function applyUserChrome(data) {
    var u = userPayloadFromMe(data);
    if (!u) return false;

    var name = displayNameFromUser(u);
    var meta = u.email || (data.dealality && data.dealality.role ? data.dealality.role : "Account settings");
    var initial = name.charAt(0).toUpperCase();
    var photoUrl = u.profilePhotoUrl ? String(u.profilePhotoUrl).trim() : "";

    global.document
      .querySelectorAll(
        ".user-block .user-name, .account-dropdown-header .user-name, " +
          "[class*='user-name'], [class*='User-Name']"
      )
      .forEach(function (el) {
        el.textContent = name;
      });
    global.document
      .querySelectorAll(
        ".user-block .user-meta, .account-dropdown-header .user-meta, " +
          "[class*='user-meta'], [class*='account-meta']"
      )
      .forEach(function (el) {
        el.textContent = meta;
      });

    var photoApplied = false;
    if (photoUrl) {
      collectAvatarElements(global.document).forEach(function (el) {
        if (applyPhotoToElement(el, photoUrl, initial)) photoApplied = true;
      });
    }

    if (!photoApplied) {
      global.document.querySelectorAll(".user-avatar, [class*='avatar']").forEach(function (el) {
        if (el.tagName === "IMG") return;
        applyInitialToElement(el, initial);
      });
    }

    if (photoUrl && !photoApplied && global.location && /localhost|127\.0\.0\.1|staging|railway|dealality\.com/i.test(global.location.hostname || "")) {
      console.warn("[DealalityWebflowUserChrome] profilePhotoUrl present but no nav avatar matched:", photoUrl.slice(0, 80));
    }

    return photoApplied;
  }

  function scheduleReapply(data) {
    if (pendingReapplyTimer) {
      global.clearTimeout(pendingReapplyTimer);
      pendingReapplyTimer = null;
    }
    REAPPLY_DELAYS_MS.forEach(function (ms) {
      global.setTimeout(function () {
        applyUserChrome(data);
      }, ms);
    });
  }

  function apply(data) {
    applyUserChrome(data);
    scheduleReapply(data);
    return data;
  }

  global.DealalityWebflowUserChrome = {
    apply: apply,
    applyOnce: applyUserChrome,
    userPayloadFromMe: userPayloadFromMe,
    collectAvatarElements: collectAvatarElements,
  };
})(typeof window !== "undefined" ? window : global);
