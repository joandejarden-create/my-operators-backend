/**
 * Apply /api/me identity (name + Airtable profile photo) to Webflow left nav / account chrome.
 * Load on dealality.com site footer after dealality-memberstack-auth.js; call apply() after loadUserContext.
 */
(function (global) {
  "use strict";

  var AVATAR_SELECTORS =
    ".user-avatar, .user-block img, .account-dropdown-header img, img.user-avatar-image, [data-dealality-user-avatar]";

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

  function applyPhotoToElement(el, url, initial) {
    if (!el || !url) return false;
    if (el.tagName === "IMG") {
      el.src = url;
      el.alt = initial || "Profile";
      el.removeAttribute("hidden");
      el.style.display = "";
      return true;
    }
    var nested = el.querySelector("img");
    if (nested) {
      nested.src = url;
      nested.alt = initial || "Profile";
      nested.removeAttribute("hidden");
      nested.style.display = "";
      if (el !== nested) el.textContent = "";
      return true;
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
    if (!u) return;

    var name = displayNameFromUser(u);
    var meta = u.email || (data.dealality && data.dealality.role ? data.dealality.role : "Account settings");
    var initial = name.charAt(0).toUpperCase();
    var photoUrl = u.profilePhotoUrl ? String(u.profilePhotoUrl).trim() : "";

    global.document.querySelectorAll(".user-block .user-name, .account-dropdown-header .user-name").forEach(function (el) {
      el.textContent = name;
    });
    global.document.querySelectorAll(".user-block .user-meta, .account-dropdown-header .user-meta").forEach(function (el) {
      el.textContent = meta;
    });

    var photoApplied = false;
    if (photoUrl) {
      global.document.querySelectorAll(AVATAR_SELECTORS).forEach(function (el) {
        if (applyPhotoToElement(el, photoUrl, initial)) photoApplied = true;
      });
    }

    if (!photoApplied) {
      global.document.querySelectorAll(".user-avatar").forEach(function (el) {
        applyInitialToElement(el, initial);
      });
    }
  }

  global.DealalityWebflowUserChrome = {
    apply: applyUserChrome,
    userPayloadFromMe: userPayloadFromMe,
  };
})(typeof window !== "undefined" ? window : global);
