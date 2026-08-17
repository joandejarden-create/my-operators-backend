/**
 * Authenticated open/download for Deal Setup Tab 13 attachments (my-deals attachment API).
 */
(function (global) {
  'use strict';

  var MSG_LOGIN = 'Please log in again to view this attachment.';
  var MSG_FORBIDDEN = 'You do not have access to this attachment.';
  var MSG_GENERIC = 'Could not open this attachment. Please try again.';

  function friendlyErrorForStatus(status) {
    if (status === 401) return MSG_LOGIN;
    if (status === 403) return MSG_FORBIDDEN;
    return MSG_GENERIC;
  }

  function showAttachmentError(message, onError) {
    if (typeof onError === 'function') {
      onError(message);
      return;
    }
    var el = global.document && global.document.getElementById('attachment-upload-status');
    if (el) {
      el.textContent = message;
      el.style.color = 'var(--system--red-500, #b91c1c)';
      return;
    }
    global.alert(message);
  }

  function isDealAttachmentApiUrl(url) {
    try {
      var u = new URL(url, global.location.origin);
      return /\/api\/my-deals\/rec[^/]+\/attachments\//.test(u.pathname);
    } catch (_) {
      return false;
    }
  }

  function toApiPath(url) {
    try {
      var u = new URL(url, global.location.origin);
      return u.pathname + u.search;
    } catch (_) {
      return url;
    }
  }

  function authFetchAttachment(url) {
    var auth = global.DealalityMemberstackAuth;
    if (!auth || typeof auth.fetchMyDealsApi !== 'function') {
      return Promise.reject(new Error(MSG_LOGIN));
    }
    return auth.fetchMyDealsApi(toApiPath(url), { method: 'GET' });
  }

  function triggerBlobOpenOrDownload(blob, filename) {
    var objUrl = global.URL.createObjectURL(blob);
    var name = (filename && String(filename).trim()) || 'attachment';
    var type = blob && blob.type ? blob.type : '';
    var openInline =
      type === 'application/pdf' ||
      /^image\//.test(type) ||
      /\.(pdf|png|jpe?g|gif|webp)$/i.test(name);

    if (openInline) {
      var popup = global.open(objUrl, '_blank', 'noopener,noreferrer');
      if (popup) {
        global.setTimeout(function () {
          try {
            global.URL.revokeObjectURL(objUrl);
          } catch (_) {}
        }, 120000);
        return;
      }
    }

    var a = global.document.createElement('a');
    a.href = objUrl;
    a.download = name;
    a.rel = 'noopener noreferrer';
    global.document.body.appendChild(a);
    a.click();
    a.remove();
    global.setTimeout(function () {
      try {
        global.URL.revokeObjectURL(objUrl);
      } catch (_) {}
    }, 1000);
  }

  /**
   * Fetch with Bearer token, open PDF/images in new tab or download other types.
   * @param {string} url
   * @param {string} [filename]
   * @param {{ onError?: function(string): void }} [options]
   */
  function openAuthenticatedAttachment(url, filename, options) {
    options = options || {};
    if (!url) return Promise.resolve();
    if (!isDealAttachmentApiUrl(url)) {
      global.open(url, '_blank', 'noopener,noreferrer');
      return Promise.resolve();
    }
    return authFetchAttachment(url)
      .then(function (res) {
        if (!res.ok) {
          var err = new Error(friendlyErrorForStatus(res.status));
          err.status = res.status;
          throw err;
        }
        return res.blob();
      })
      .then(function (blob) {
        triggerBlobOpenOrDownload(blob, filename);
      })
      .catch(function (err) {
        var msg =
          err && err.message && err.message !== 'Failed to fetch'
            ? err.message
            : friendlyErrorForStatus(err && err.status);
        showAttachmentError(msg, options.onError);
      });
  }

  /**
   * Render Current attachments list with authenticated click handlers.
   * @param {Array} attachments
   * @param {{ listEl?: HTMLElement, emptyEl?: HTMLElement, onError?: function(string): void }} [options]
   */
  function renderCurrentAttachments(attachments, options) {
    options = options || {};
    var listEl = options.listEl || (global.document && global.document.getElementById('current-attachments-list'));
    var emptyEl = options.emptyEl || (global.document && global.document.getElementById('current-attachments-empty'));
    if (!listEl || !emptyEl) return;

    var arr = Array.isArray(attachments) ? attachments : [];
    listEl.innerHTML = '';

    arr.forEach(function (item) {
      var url =
        typeof item === 'object' && item && item.url
          ? item.url
          : typeof item === 'string'
            ? item
            : '';
      var name =
        typeof item === 'object' && item && (item.filename != null ? item.filename : item.name)
          ? String(item.filename != null ? item.filename : item.name)
          : '';
      if (!url) return;

      var li = global.document.createElement('li');
      li.style.marginBottom = '4px';

      var a = global.document.createElement('a');
      a.href = '#';
      a.setAttribute('role', 'button');
      a.textContent = name || url;
      a.style.color = 'var(--secondary--color-2, #4f46e5)';
      a.style.cursor = 'pointer';
      a.addEventListener('click', function (ev) {
        ev.preventDefault();
        var prev = a.textContent;
        a.textContent = 'Opening…';
        a.style.pointerEvents = 'none';
        openAuthenticatedAttachment(url, name, {
          onError: function (msg) {
            showAttachmentError(msg, options.onError);
          },
        }).finally(function () {
          a.textContent = prev;
          a.style.pointerEvents = '';
        });
      });

      li.appendChild(a);
      listEl.appendChild(li);
    });

    emptyEl.style.display = arr.length ? 'none' : 'block';
  }

  global.DealSetupAttachmentsUi = {
    renderCurrentAttachments: renderCurrentAttachments,
    openAuthenticatedAttachment: openAuthenticatedAttachment,
    isDealAttachmentApiUrl: isDealAttachmentApiUrl,
  };
})(typeof window !== 'undefined' ? window : globalThis);
