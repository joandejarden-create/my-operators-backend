/**
 * /signup — Memberstack DOM signup first (sends verification email), then POST /api/signup for Airtable.
 */
(function (global) {
  "use strict";

  var MS_SCRIPT_SRC = "https://static.memberstack.com/scripts/v1/memberstack.js";

  function memberstackClientReady() {
    var auth = global.DealalityMemberstackAuth;
    var ms = auth && auth.getMemberstackDom ? auth.getMemberstackDom() : null;
    if (!ms) ms = global.$memberstackDom || global.memberstack;
    if (!ms || typeof ms !== "object") return null;
    if (typeof ms.signupMemberEmailPassword === "function") return ms;
    if (typeof ms.signupMember === "function") return ms;
    return null;
  }

  function loadMemberstackScript(appId) {
    return new Promise(function (resolve, reject) {
      if (!appId) {
        reject(new Error("Memberstack app id not configured"));
        return;
      }
      var ready = memberstackClientReady();
      if (ready) {
        resolve(ready);
        return;
      }
      var auth = global.DealalityMemberstackAuth;
      function waitReady() {
        if (auth && typeof auth.waitForMemberstackDom === "function") {
          auth.waitForMemberstackDom(20000).then(function (ms) {
            if (ms) resolve(ms);
            else reject(new Error("Memberstack did not initialize"));
          }).catch(reject);
          return;
        }
        var start = Date.now();
        var poll = setInterval(function () {
          var ms2 = memberstackClientReady();
          if (ms2) {
            clearInterval(poll);
            resolve(ms2);
          } else if (Date.now() - start > 20000) {
            clearInterval(poll);
            reject(new Error("Memberstack did not initialize"));
          }
        }, 150);
      }
      var existing = global.document && global.document.querySelector("script[data-memberstack-app]");
      if (existing) {
        waitReady();
        return;
      }
      var script = global.document.createElement("script");
      script.src = MS_SCRIPT_SRC;
      script.async = true;
      script.setAttribute("data-memberstack-app", appId);
      script.onload = waitReady;
      script.onerror = function () {
        reject(new Error("Memberstack script failed to load"));
      };
      (global.document.head || global.document.documentElement).appendChild(script);
    });
  }

  /** Must match lib/memberstack/memberstack-custom-fields.js defaults (slug of MS dashboard names). */
  var MS_CF = {
    firstName: "first-name",
    lastName: "last-name",
    companyName: "company-name",
    phone: "phone",
    companyType: "company-type",
    reasonToJoin: "reason-to-join",
    howDidYouHear: "how-did-you-hear",
  };

  function signupViaDom(ms, payload, pendingPlanId) {
    var customFields = {};
    if (payload.firstName) customFields[MS_CF.firstName] = payload.firstName;
    if (payload.lastName) customFields[MS_CF.lastName] = payload.lastName;
    if (payload.companyName) customFields[MS_CF.companyName] = payload.companyName;
    if (payload.phone) customFields[MS_CF.phone] = payload.phone;
    if (payload.companyType) customFields[MS_CF.companyType] = payload.companyType;
    if (payload.reasonToJoin) customFields[MS_CF.reasonToJoin] = payload.reasonToJoin;
    if (payload.howDidYouHear) customFields[MS_CF.howDidYouHear] = payload.howDidYouHear;

    var params = {
      email: payload.email,
      password: payload.password,
      customFields: customFields,
    };
    if (pendingPlanId) {
      params.plans = [{ planId: pendingPlanId }];
    }

    if (typeof ms.signupMemberEmailPassword === "function") {
      return ms.signupMemberEmailPassword(params);
    }
    if (typeof ms.signupMember === "function") {
      return ms.signupMember(params);
    }
    return Promise.reject(new Error("Memberstack signup method not available"));
  }

  function extractMemberId(result) {
    if (!result) return null;
    var m = result.data && result.data.member ? result.data.member : result.member;
    if (m && m.id) return m.id;
    if (result.data && result.data.id) return result.data.id;
    return null;
  }

  function friendlyMsError(err) {
    var msg = (err && (err.message || err.error)) || "Signup failed";
    var code = err && err.code;
    if (code === "email-already-in-use" || /already/i.test(msg)) {
      return "An account with this email already exists. Try logging in or use a different email.";
    }
    return msg;
  }

  /**
   * @param {object} payload - form fields including password
   * @returns {Promise<object>} fetch result from /api/signup
   */
  global.DealalitySignupFlow = {
    submit: function (payload) {
      return fetch("/api/signup/config")
        .then(function (r) { return r.json(); })
        .then(function (cfg) {
          if (!cfg.useDomSignup) {
            return fetch("/api/signup", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            });
          }
          return loadMemberstackScript(cfg.appId)
            .then(function (ms) {
              return signupViaDom(ms, payload, cfg.pendingPlanId).catch(function (err) {
                throw new Error(friendlyMsError(err));
              });
            })
            .then(function (msResult) {
              var memberstackId = extractMemberId(msResult);
              if (!memberstackId) {
                throw new Error("Memberstack signup succeeded but no member id returned");
              }
              var body = Object.assign({}, payload, { memberstackId: memberstackId });
              return fetch("/api/signup", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
              });
            });
        });
    },
    friendlyMsError: friendlyMsError,
  };
})(window);
