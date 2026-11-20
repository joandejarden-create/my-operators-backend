/**
 * Browser mirror of lib/brand-explorer/be-cumulative-proposal-assembler-v1.js
 * Cumulative publication proposal for Comparison Studio RIGHT column.
 */
(function (global) {
  'use strict';

  var BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1 = 'BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1';
  var PROVENANCE = {
    RETAINED_FROM_CURRENT: 'RETAINED_FROM_CURRENT',
    MODIFIED_EXISTING: 'MODIFIED_EXISTING',
    NEWLY_RESEARCHED: 'NEWLY_RESEARCHED',
    NEW_COMPONENT: 'NEW_COMPONENT',
    INCOMPLETE_DRAFT: 'INCOMPLETE_DRAFT',
    RESEARCH_REQUIRED: 'RESEARCH_REQUIRED'
  };

  var PLACEHOLDER_TITLE_RE =
    /^(loyalty proof points|proof points?|fee categories|standards diligence themes|owner diligence questions|demand themes|openings \(diligence\)|momentum \(diligence\)|growth themes|owner outcomes|owner considerations|curio proof \d+|risk|model|financials|negotiable items|rarely negotiable|\d+)$/i;
  var RESEARCH_GAP_RE =
    /RESEARCH_REQUIRED|Focus this card|Evidence-grounded|slotKey|TODO_RESEARCH|\[research\]/i;
  var DILIGENCE_META_RE =
    /diligence:|request current|confirm definitions and figures|do not reuse|Use disclosure\/scorecards only|from .+ development\s*[—\-]/i;
  var KPI_SLOT_RE = /\.kpi\.|kpi\./i;
  var COMMERCIAL_SUMMARY_SLOTS = {
    'commercial.kpi.channels': true,
    'commercial.kpi.campaigns': true,
    'commercial.kpi.b2b': true,
    'commercial.kpi.lens': true
  };
  var RESEARCH_INSTRUCTION_RE =
    /diligence:|KPI lens:|request (current|negotiated|channel)|confirm definitions|do not invent|do not rely on generated|KPI figures are not disclosed|Focus this card|RESEARCH_REQUIRED|owner diligence:|obtain brand\.com|ask which brand campaigns|define the primary KPI stack|account concentration/i;
  var GENERIC_GALLERY_CAPTION_RE =
    /^(property\s+imagery(\s+\d+)?|hotel\s+image(\s+\d+)?|imagery\s+unavailable|gallery\s*\d+|image\s*\d+|photo\s*\d+|untitled|n\/?a)$/i;

  function nz(s) {
    return s == null ? '' : String(s).trim();
  }

  function galleryImageUrl(block) {
    var body = bodyOf(block);
    return nz(
      (block && (block.imageUrl || block.image_url)) || (/^https?:\/\//i.test(body) ? body : '')
    );
  }

  function isEphemeralAirtableCdnUrl(url) {
    try {
      return /(^|\.)airtableusercontent\.com$/i.test(new URL(String(url || '')).hostname);
    } catch (e) {
      return /airtableusercontent\.com/i.test(String(url || ''));
    }
  }

  function currentImageDeliveryUrl(curBlock) {
    if (!curBlock) return '';
    var delivery = nz(curBlock.imageDeliveryUrl);
    if (delivery) return delivery;
    var recId = nz(curBlock.recordId);
    if (recId && /^rec[a-zA-Z0-9]+$/.test(recId) && (nz(curBlock.imageUrl) || nz(curBlock.imageAttachmentId))) {
      // Prefer allowlisted fresh-delivery endpoint over caching signed CDN URLs.
      return '/api/brand-library/presentation-image/' + encodeURIComponent(recId);
    }
    var img = nz(curBlock.imageUrl);
    if (img && !isEphemeralAirtableCdnUrl(img)) return img;
    if (img && isEphemeralAirtableCdnUrl(img) && recId && /^rec[a-zA-Z0-9]+$/.test(recId)) {
      return '/api/brand-library/presentation-image/' + encodeURIComponent(recId);
    }
    return img;
  }

  function isPresentationDeliveryUrl(url) {
    return /\/api\/brand-(?:library|explorer)\/presentation-image\//i.test(String(url || ''));
  }

  /**
   * Never let frozen/proposed ephemeral Airtable CDN URLs win over fresh CURRENT attachments.
   * Display-only rehydration — does not rewrite approval text hashes.
   */
  function rehydrateEphemeralImageUrls(proposedBlocks, currentBlocks) {
    var bySlot = {};
    (currentBlocks || []).forEach(function (b) {
      var sk = nz(b && b.slotKey);
      if (!sk) return;
      if (!bySlot[sk]) bySlot[sk] = b;
    });
    return (proposedBlocks || []).map(function (b) {
      if (!b) return b;
      var sk = nz(b.slotKey);
      var cur = bySlot[sk];
      var proposedUrl = nz(b.imageUrl || b.image_url);
      var delivery = currentImageDeliveryUrl(cur);
      if (!delivery) return b;
      if (isPresentationDeliveryUrl(proposedUrl)) return b;
      // Keep durable non-Airtable absolute sources (e.g. Marriott DAM).
      if (proposedUrl && !isEphemeralAirtableCdnUrl(proposedUrl) && /^https?:\/\//i.test(proposedUrl)) {
        return b;
      }
      // Missing or ephemeral CDN → prefer CURRENT attachment delivery.
      if (proposedUrl && !isEphemeralAirtableCdnUrl(proposedUrl)) return b;
      var out = Object.assign({}, b);
      out.imageUrl = delivery;
      out.imageDeliveryUrl = delivery;
      if (cur && cur.recordId) out.recordId = cur.recordId;
      if (cur && cur.imageAttachmentId) out.imageAttachmentId = cur.imageAttachmentId;
      out.imageRehydratedFromCurrent = true;
      return out;
    });
  }

  function isBrandLogoGalleryAsset(url) {
    var u = nz(url);
    if (!u) return false;
    if (/logo|wordmark|brand-mark|_HR-1\.png|favicon|hotel-brand-lo|social_image|brand[-_]?lockup/i.test(u)) {
      return true;
    }
    try {
      var path = new URL(u).pathname;
      var base = decodeURIComponent(path.split('/').pop() || '')
        .replace(/\.(webp|png|jpe?g|gif|svg)$/i, '')
        .replace(/_/g, '-');
      if (/^(curio|ascend|autograph|tapestry|kimpton|tribute)-collection(-by-\w+)?$/i.test(base)) {
        return true;
      }
    } catch (e) {
      /* ignore */
    }
    return false;
  }

  function isGenericGalleryCaption(title, body) {
    var t = nz(title);
    if (!t) return true;
    if (GENERIC_GALLERY_CAPTION_RE.test(t)) return true;
    if (/^https?:\/\//i.test(t)) return true;
    return false;
  }

  function galleryDupId(url) {
    try {
      var path = new URL(nz(url)).pathname;
      return decodeURIComponent(path.split('/').pop() || '')
        .toLowerCase()
        .replace(/\.(webp|png|jpe?g|gif|svg)$/i, '')
        .replace(/[^a-z0-9]+/g, '');
    } catch (e) {
      return nz(url).toLowerCase();
    }
  }

  function decideGallerySlotApply(slotKey, currentBlock, candidateBlock) {
    var curUrl = galleryImageUrl(currentBlock);
    var candUrl = galleryImageUrl(candidateBlock);
    var curTitle = titleOf(currentBlock);
    var candTitle = titleOf(candidateBlock);
    var currentOk =
      !!curUrl && !isBrandLogoGalleryAsset(curUrl) && !isGenericGalleryCaption(curTitle, bodyOf(currentBlock));
    var candidateOk =
      !!candUrl &&
      !isBrandLogoGalleryAsset(candUrl) &&
      !isGenericGalleryCaption(candTitle, bodyOf(candidateBlock)) &&
      /\|\s*\S+/.test(candTitle);
    if (currentOk) {
      if (candidateOk && galleryDupId(curUrl) === galleryDupId(candUrl) && candTitle !== curTitle) {
        return { applyCandidate: true, reason: 'candidate_gallery_caption_improvement_same_asset' };
      }
      return {
        applyCandidate: false,
        reason: 'retain_verified_current_gallery_photo',
        retainCurrent: true
      };
    }
    if (!candUrl || isBrandLogoGalleryAsset(candUrl)) {
      return { applyCandidate: false, reason: 'candidate_gallery_logo_or_missing', retainCurrent: false };
    }
    if (isGenericGalleryCaption(candTitle, bodyOf(candidateBlock))) {
      return { applyCandidate: false, reason: 'candidate_gallery_generic_caption', retainCurrent: false };
    }
    if (candidateOk) {
      return { applyCandidate: true, reason: 'candidate_fills_empty_or_invalid_current_gallery' };
    }
    return { applyCandidate: false, reason: 'candidate_gallery_not_safe', retainCurrent: !!curUrl };
  }

  function bodyOf(block) {
    return nz(block && (block.body || block.value || block.text));
  }

  function titleOf(block) {
    return nz(block && block.title);
  }

  function slotOf(block) {
    return nz(block && (block.slotKey || block.slot_key || block.fieldKey));
  }

  function countBullets(text) {
    var t = nz(text);
    if (!t) return 0;
    var lines = t
      .split(/\n+/)
      .map(function (l) {
        return l.replace(/^[\s•\-*]+/, '').trim();
      })
      .filter(Boolean);
    if (lines.length >= 2) return lines.length;
    return t.length >= 24 ? 1 : 0;
  }

  function isPlaceholderBlock(block) {
    var title = titleOf(block);
    var body = bodyOf(block);
    if (!body && PLACEHOLDER_TITLE_RE.test(title)) return true;
    if (RESEARCH_GAP_RE.test(title + '\n' + body)) return true;
    if (DILIGENCE_META_RE.test(body)) return true;
    if (!body && !title) return true;
    return false;
  }

  function looksLikeNumericOrUnavailable(text) {
    var t = nz(text);
    if (!t) return false;
    if (/^(n\/?a|not disclosed|unavailable|—|-)$/i.test(t)) return true;
    if (t.length <= 48 && /[\d%~$€£+]/.test(t) && t.split(/\s+/).length <= 8) return true;
    return false;
  }

  function countColonInsightLeads(text) {
    var lines = nz(text)
      .split(/\n+/)
      .map(function (l) {
        return l.replace(/^[\s•\-*]+/, '').trim();
      })
      .filter(Boolean);
    var n = 0;
    for (var i = 0; i < lines.length; i++) {
      if (/^[^:\n]{3,72}:\s+\S.{11,}/.test(lines[i])) n += 1;
    }
    return n;
  }

  function isWhyValueInsightRegression(slotKey, currentBlocks, candidateBlocks) {
    if (!/^overview\.why_value$/i.test(nz(slotKey))) return false;
    var curBody = (currentBlocks || []).map(bodyOf).join('\n');
    var candBody = (candidateBlocks || []).map(bodyOf).join('\n');
    var curLeads = countColonInsightLeads(curBody);
    var candLeads = countColonInsightLeads(candBody);
    if (curLeads >= 3 && candLeads < Math.min(curLeads, 4)) return true;
    if (curLeads >= 3 && candLeads === 0) return true;
    return false;
  }

  function isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks) {
    var cq = slotGroupQuality(currentBlocks);
    var pq = slotGroupQuality(candidateBlocks);
    var candBody = (candidateBlocks || [])
      .map(bodyOf)
      .join('\n');
    var curBody = (currentBlocks || [])
      .map(bodyOf)
      .join('\n');
    if (isWhyValueInsightRegression(slotKey, currentBlocks, candidateBlocks)) return true;
    if (cq.bulletCount >= 3 && pq.bulletCount < cq.bulletCount) return true;
    if (cq.bulletCount >= 3 && pq.bulletCount <= 1 && candBody.length > 80) return true;
    // Commercial Engine summary tiles are narrative — never replace with research stubs.
    if (COMMERCIAL_SUMMARY_SLOTS[String(slotKey || '').trim()]) {
      if (RESEARCH_INSTRUCTION_RE.test(candBody) || DILIGENCE_META_RE.test(candBody)) return true;
      if (curBody.length >= 24 && candBody.length + 20 < curBody.length) return true;
      return false;
    }
    if (KPI_SLOT_RE.test(slotKey || '')) {
      if (DILIGENCE_META_RE.test(candBody)) return true;
      if (
        looksLikeNumericOrUnavailable(curBody) &&
        !looksLikeNumericOrUnavailable(candBody) &&
        candBody.length > 40
      ) {
        return true;
      }
    }
    if (/negotiable_items|rarely_negotiable/i.test(slotKey || '')) {
      if (DILIGENCE_META_RE.test(candBody)) return true;
      if (cq.bulletCount >= 3 && pq.bulletCount < 3) return true;
    }
    return false;
  }

  function hasSubstantiveContent(block) {
    if (!block) return false;
    if (isPlaceholderBlock(block)) return false;
    var body = bodyOf(block);
    var title = titleOf(block);
    if (body.length >= 24) return true;
    if (title.length >= 3 && body.length >= 12) return true;
    if (nz(block.imageUrl || block.image_url)) return true;
    return false;
  }

  function slotGroupQuality(blocks) {
    var list = Array.isArray(blocks) ? blocks : [];
    var substantive = list.filter(hasSubstantiveContent);
    var placeholders = list.filter(isPlaceholderBlock);
    var emptyBodies = list.filter(function (b) {
      return !bodyOf(b) && !nz(b.imageUrl || b.image_url);
    });
    var totalBody = substantive.reduce(function (n, b) {
      return n + bodyOf(b).length;
    }, 0);
    var bulletCount = substantive.reduce(function (n, b) {
      return n + countBullets(bodyOf(b));
    }, 0);
    return {
      count: list.length,
      substantiveCount: substantive.length,
      placeholderCount: placeholders.length,
      emptyBodyCount: emptyBodies.length,
      totalBody: totalBody,
      bulletCount: bulletCount,
      titles: substantive.map(titleOf).filter(Boolean),
      fingerprint: substantive
        .map(function (b) {
          return titleOf(b) + '::' + bodyOf(b);
        })
        .sort()
        .join('||')
    };
  }

  function normalizeIncomingBlock(block, index, sourceTag) {
    var slotKey = slotOf(block);
    var body = bodyOf(block);
    var imageUrl = block.imageUrl || block.image_url || null;
    return {
      recordId: block.recordId || sourceTag + '-' + index,
      slotKey: slotKey,
      title: titleOf(block),
      body: body,
      sort:
        typeof block.sort_order === 'number'
          ? block.sort_order
          : typeof block.sort === 'number'
            ? block.sort
            : index + 1,
      imageUrl: imageUrl,
      imageFilename: block.imageFilename || null,
      summaryUrl: block.summaryUrl || '',
      caseSummaryOverview: block.caseSummaryOverview || '',
      caseSummaryOwnerObjective: block.caseSummaryOwnerObjective || '',
      caseSummaryBrandRelevance: block.caseSummaryBrandRelevance || '',
      caseSummaryInterpretation: block.caseSummaryInterpretation || '',
      caseSummaryTags: block.caseSummaryTags || ''
    };
  }

  function groupBySlot(blocks) {
    var map = {};
    (blocks || []).forEach(function (b, i) {
      var key = slotOf(b);
      if (!key) return;
      if (!map[key]) map[key] = [];
      map[key].push({ block: b, index: i });
    });
    return map;
  }

  function decideSlotProvenance(slotKey, currentBlocks, candidateBlocks) {
    var cq = slotGroupQuality(currentBlocks);
    var pq = slotGroupQuality(candidateBlocks);

    var advisoryApi = global.BeDealalityInsightAdvisory;
    if (
      advisoryApi &&
      advisoryApi.isCustomerFacingAdvisorySlot &&
      advisoryApi.isCustomerFacingAdvisorySlot(slotKey) &&
      advisoryApi.containsInternalCustomerLanguage
    ) {
      var candBlob = (candidateBlocks || [])
        .map(function (b) {
          return titleOf(b) + '\n' + bodyOf(b);
        })
        .join('\n');
      var curBlob = (currentBlocks || [])
        .map(function (b) {
          return titleOf(b) + '\n' + bodyOf(b);
        })
        .join('\n');
      if (advisoryApi.containsInternalCustomerLanguage(candBlob)) {
        return {
          provenance:
            cq.substantiveCount > 0 && !advisoryApi.containsInternalCustomerLanguage(curBlob)
              ? PROVENANCE.INCOMPLETE_DRAFT
              : PROVENANCE.RETAINED_FROM_CURRENT,
          applyCandidate: false,
          reason: 'candidate_internal_language_not_customer_facing',
          approvalBlocked: true
        };
      }
    }

    // Key Benefits & Program Strengths (loyalty.proof): EXACT 6 — Factory 1.0/2.0 permanent.
    if (/^loyalty\.proof$/i.test(nz(slotKey))) {
      var REQUIRED_LOYALTY_PROOF = 6;
      var candIncomplete =
        pq.substantiveCount > 0 && pq.substantiveCount < REQUIRED_LOYALTY_PROOF;
      var candRegression =
        cq.substantiveCount >= REQUIRED_LOYALTY_PROOF &&
        pq.substantiveCount > 0 &&
        pq.substantiveCount !== REQUIRED_LOYALTY_PROOF;
      if (candIncomplete || candRegression) {
        return {
          provenance: PROVENANCE.INCOMPLETE_DRAFT,
          applyCandidate: false,
          reason: candIncomplete
            ? 'loyalty_proof_requires_exact_six_cards'
            : 'loyalty_proof_cardinality_regression',
          currentQuality: cq,
          candidateQuality: pq
        };
      }
    }

    if (/^materials\.gallery\.\d+$/.test(nz(slotKey))) {
      var g = decideGallerySlotApply(slotKey, currentBlocks[0] || null, candidateBlocks[0] || null);
      if (!g.applyCandidate) {
        return {
          provenance:
            cq.substantiveCount > 0 ? PROVENANCE.INCOMPLETE_DRAFT : PROVENANCE.RETAINED_FROM_CURRENT,
          applyCandidate: false,
          reason: g.reason || 'gallery_standard_reject'
        };
      }
      return {
        provenance: cq.substantiveCount > 0 ? PROVENANCE.MODIFIED_EXISTING : PROVENANCE.NEW_COMPONENT,
        applyCandidate: true,
        reason: g.reason || 'gallery_standard_accept'
      };
    }

    if (pq.substantiveCount === 0 && cq.substantiveCount === 0) {
      try {
        if (
          candidateBlocks.length &&
          RESEARCH_GAP_RE.test(JSON.stringify(candidateBlocks))
        ) {
          return {
            provenance: PROVENANCE.RESEARCH_REQUIRED,
            applyCandidate: false,
            reason: 'candidate_marks_research_required'
          };
        }
      } catch (e) {
        /* ignore */
      }
      return {
        provenance: PROVENANCE.RETAINED_FROM_CURRENT,
        applyCandidate: false,
        reason: 'both_empty'
      };
    }

    if (pq.substantiveCount === 0 && cq.substantiveCount > 0) {
      return {
        provenance: PROVENANCE.INCOMPLETE_DRAFT,
        applyCandidate: false,
        reason: 'candidate_empty_or_placeholder_while_current_complete'
      };
    }

    if (
      cq.substantiveCount > 0 &&
      (pq.placeholderCount > 0 || pq.emptyBodyCount > 0) &&
      pq.substantiveCount < cq.substantiveCount
    ) {
      return {
        provenance: PROVENANCE.INCOMPLETE_DRAFT,
        applyCandidate: false,
        reason: 'candidate_incomplete_vs_complete_current'
      };
    }

    if (
      cq.substantiveCount > 0 &&
      (pq.substantiveCount < cq.substantiveCount ||
        pq.totalBody < Math.floor(cq.totalBody * 0.7) ||
        (cq.bulletCount >= 4 &&
          pq.bulletCount > 0 &&
          pq.bulletCount < 4 &&
          pq.totalBody < cq.totalBody) ||
        isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks))
    ) {
      return {
        provenance: PROVENANCE.INCOMPLETE_DRAFT,
        applyCandidate: false,
        reason: isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks)
          ? 'candidate_prose_or_list_regression'
          : 'candidate_regresses_depth_or_cardinality',
        currentQuality: cq,
        candidateQuality: pq
      };
    }

    if (cq.substantiveCount === 0 && pq.substantiveCount > 0) {
      if (
        isPlaceholderBlock(candidateBlocks[0]) ||
        isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks)
      ) {
        return {
          provenance: PROVENANCE.INCOMPLETE_DRAFT,
          applyCandidate: false,
          reason: 'new_slot_is_diligence_or_invalid_kpi_prose'
        };
      }
      return {
        provenance: PROVENANCE.NEW_COMPONENT,
        applyCandidate: true,
        reason: 'new_substantive_slot_not_in_current'
      };
    }

    if (cq.fingerprint === pq.fingerprint) {
      return {
        provenance: PROVENANCE.RETAINED_FROM_CURRENT,
        applyCandidate: false,
        reason: 'identical_content'
      };
    }

    if (
      pq.substantiveCount >= cq.substantiveCount &&
      pq.totalBody >= cq.totalBody * 0.95 &&
      pq.placeholderCount === 0 &&
      !isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks) &&
      !(cq.bulletCount >= 3 && pq.bulletCount < cq.bulletCount)
    ) {
      return {
        provenance: PROVENANCE.MODIFIED_EXISTING,
        applyCandidate: true,
        reason: 'candidate_complete_non_regressive_change'
      };
    }

    // Why Value Is Strongest: prefer better "Lead: explanation" structure.
    if (/^overview\.why_value$/i.test(nz(slotKey))) {
      var curLeads = countColonInsightLeads(
        (currentBlocks || []).map(bodyOf).join('\n')
      );
      var candLeads = countColonInsightLeads(
        (candidateBlocks || []).map(bodyOf).join('\n')
      );
      if (
        candLeads >= 3 &&
        candLeads > curLeads &&
        pq.placeholderCount === 0 &&
        pq.substantiveCount > 0 &&
        !isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks)
      ) {
        return {
          provenance: PROVENANCE.MODIFIED_EXISTING,
          applyCandidate: true,
          reason: 'why_value_colon_insight_upgrade',
          currentQuality: cq,
          candidateQuality: pq
        };
      }
    }

    // Key Differentiators Curio/Ascend bar: upgrade thin CURRENT (<4 bullets).
    if (/^overview\.differentiators\.(identity|commercial)$/i.test(nz(slotKey))) {
      if (
        pq.bulletCount >= 4 &&
        cq.bulletCount < 4 &&
        pq.placeholderCount === 0 &&
        pq.substantiveCount > 0 &&
        !isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks)
      ) {
        return {
          provenance: PROVENANCE.MODIFIED_EXISTING,
          applyCandidate: true,
          reason: 'differentiator_four_bullet_bar_upgrade',
          currentQuality: cq,
          candidateQuality: pq
        };
      }
    }

    return {
      provenance: PROVENANCE.INCOMPLETE_DRAFT,
      applyCandidate: false,
      reason: 'candidate_not_safe_to_replace_current',
      currentQuality: cq,
      candidateQuality: pq
    };
  }

  function assembleCumulativeProposal(currentBlocks, candidateBlocks, opts) {
    opts = opts || {};
    var current = Array.isArray(currentBlocks) ? currentBlocks : [];
    var candidate = Array.isArray(candidateBlocks) ? candidateBlocks : [];
    var currentGroups = groupBySlot(current);
    var candidateGroups = groupBySlot(candidate);
    var allSlots = {};
    Object.keys(currentGroups).forEach(function (k) {
      allSlots[k] = true;
    });
    Object.keys(candidateGroups).forEach(function (k) {
      allSlots[k] = true;
    });

    var provenanceBySlot = {};
    var incompleteDrafts = [];
    var assembled = [];
    var nextSort = 1;

    var sortedSlots = Object.keys(allSlots).sort(function (a, b) {
      var ai = (currentGroups[a] || candidateGroups[a] || [{ index: 9999 }])[0].index;
      var bi = (currentGroups[b] || candidateGroups[b] || [{ index: 9999 }])[0].index;
      return ai - bi || (a < b ? -1 : a > b ? 1 : 0);
    });

    sortedSlots.forEach(function (slotKey) {
      var curBlocks = (currentGroups[slotKey] || []).map(function (x) {
        return x.block;
      });
      var candBlocks = (candidateGroups[slotKey] || []).map(function (x) {
        return x.block;
      });
      var decision = decideSlotProvenance(slotKey, curBlocks, candBlocks);
      provenanceBySlot[slotKey] = {
        provenance: decision.provenance,
        applyCandidate: !!decision.applyCandidate,
        reason: decision.reason,
        currentQuality: slotGroupQuality(curBlocks),
        candidateQuality: slotGroupQuality(candBlocks)
      };

      if (decision.applyCandidate) {
        candBlocks.forEach(function (b) {
          var norm = normalizeIncomingBlock(b, nextSort, 'cumulative-proposed');
          norm.slotKey = slotKey;
          norm.sort = nextSort++;
          assembled.push(norm);
        });
      } else {
        var publicationProvenance = PROVENANCE.RETAINED_FROM_CURRENT;
        if (
          decision.provenance === PROVENANCE.INCOMPLETE_DRAFT ||
          decision.provenance === PROVENANCE.RESEARCH_REQUIRED
        ) {
          incompleteDrafts.push({
            slotKey: slotKey,
            provenance: decision.provenance,
            reason: decision.reason,
            candidateBlocks: candBlocks.map(function (b, i) {
              return normalizeIncomingBlock(b, i, 'incomplete-draft');
            }),
            retainedFromCurrent: true
          });
          provenanceBySlot[slotKey].provenance = publicationProvenance;
          provenanceBySlot[slotKey].draftStatus = decision.provenance;
          provenanceBySlot[slotKey].publicationAction = 'retain_current';
        } else {
          provenanceBySlot[slotKey].provenance = publicationProvenance;
          provenanceBySlot[slotKey].publicationAction = 'retain_current';
        }
        curBlocks.forEach(function (b) {
          var norm = normalizeIncomingBlock(b, nextSort, 'retained-current');
          norm.slotKey = slotKey;
          norm.sort = nextSort++;
          assembled.push(norm);
        });
      }
    });

    var counts = {
      RETAINED_FROM_CURRENT: 0,
      MODIFIED_EXISTING: 0,
      NEWLY_RESEARCHED: 0,
      NEW_COMPONENT: 0,
      INCOMPLETE_DRAFT: 0,
      RESEARCH_REQUIRED: 0
    };
    Object.keys(provenanceBySlot).forEach(function (k) {
      var p = provenanceBySlot[k].provenance;
      if (counts[p] != null) counts[p] += 1;
    });
    incompleteDrafts.forEach(function (d) {
      if (d.provenance === PROVENANCE.INCOMPLETE_DRAFT) counts.INCOMPLETE_DRAFT += 1;
      if (d.provenance === PROVENANCE.RESEARCH_REQUIRED) counts.RESEARCH_REQUIRED += 1;
    });

    var regressionBlocked = incompleteDrafts.some(function (d) {
      return (
        d.reason === 'candidate_regresses_depth_or_cardinality' ||
        d.reason === 'candidate_incomplete_vs_complete_current' ||
        d.reason === 'candidate_empty_or_placeholder_while_current_complete'
      );
    });

    return {
      contract: BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1,
      blocks: assembled,
      provenanceBySlot: provenanceBySlot,
      provenanceCounts: counts,
      incompleteDrafts: incompleteDrafts,
      approvalBlockedByRegression: regressionBlocked,
      summary: {
        currentBlockCount: current.length,
        candidateBlockCount: candidate.length,
        assembledBlockCount: assembled.length,
        incompleteDraftSlotCount: incompleteDrafts.length,
        retainedSlotCount: counts.RETAINED_FROM_CURRENT,
        modifiedSlotCount: counts.MODIFIED_EXISTING,
        newComponentSlotCount: counts.NEW_COMPONENT,
        newlyResearchedSlotCount: counts.NEWLY_RESEARCHED
      },
      note:
        opts.note ||
        'Cumulative publication proposal = CURRENT baseline + complete non-regressive staged changes only'
    };
  }

  function buildCumulativeProposedBrand(currentBrand, candidateOverlay, opts) {
    opts = opts || {};
    if (!currentBrand) throw new Error('currentBrand required');

    // Immutable frozen approval payload: do not re-assemble against live CURRENT
    // (avoids browser vs writeback hash divergence).
    var frozenContract = String((candidateOverlay && candidateOverlay.contract) || '');
    if (/FROZEN_APPROVAL_PAYLOAD/i.test(frozenContract)) {
      var frozenBlocks =
        (candidateOverlay.brandExplorer && candidateOverlay.brandExplorer.blocks) ||
        candidateOverlay.blocks ||
        [];
      var currentBlocksFrozen =
        (currentBrand.brandExplorer && currentBrand.brandExplorer.blocks) ||
        currentBrand.blocks ||
        [];
      // Display-only: replace expired airtableusercontent imageUrls with CURRENT delivery URLs.
      // Approval contentHash remains the frozen payload hash (text identity unchanged).
      var displayFrozenBlocks = rehydrateEphemeralImageUrls(frozenBlocks.slice(), currentBlocksFrozen);
      var brandFrozen = Object.assign({}, currentBrand, {
        brandExplorer: { version: 1, blocks: displayFrozenBlocks },
        beCumulativeProposal: true,
        beFrozenApprovalPayload: true,
        beCandidateOverlayMeta: Object.assign({}, currentBrand.beCandidateOverlayMeta || {}, {
          contentHash: candidateOverlay.approvalContentHash || null,
          lockId: candidateOverlay.lockId || null,
          frozenAt: candidateOverlay.frozenAt || null,
          contract: frozenContract,
          source: 'frozen-approval-payload',
          imageUrlsRehydratedFromCurrent: true
        })
      });
      return {
        brand: brandFrozen,
        assembly: {
          summary: {
            frozenApprovalPayload: true,
            assembledBlockCount: displayFrozenBlocks.length,
            approvalContentHash: candidateOverlay.approvalContentHash || null,
            imageUrlsRehydratedFromCurrent: true
          },
          blocks: displayFrozenBlocks,
          provenanceBySlot: {},
          incompleteDrafts: []
        }
      };
    }

    var currentBlocks =
      (currentBrand.brandExplorer && currentBrand.brandExplorer.blocks) ||
      currentBrand.blocks ||
      [];
    var candidateBlocks =
      (candidateOverlay &&
        candidateOverlay.brandExplorer &&
        candidateOverlay.brandExplorer.blocks) ||
      (candidateOverlay && candidateOverlay.blocks) ||
      [];
    var assembled = assembleCumulativeProposal(currentBlocks, candidateBlocks, opts);
    assembled.blocks = rehydrateEphemeralImageUrls(assembled.blocks, currentBlocks);
    var titleCaseApi = global.BeEditorialTitleCase;

    // RIGHT-only: reject publication thumbnails as property photos; apply verified replacements.
    var imageApi = global.BePropertyImageIntegrity;
    if (imageApi && imageApi.applyPropertyImageIntegrityToBlocks) {
      var imageIntegrity = imageApi.applyPropertyImageIntegrityToBlocks(assembled.blocks);
      assembled.blocks = imageIntegrity.blocks;
      assembled.propertyImageIntegrity = {
        contract: imageApi.BE_PROPERTY_IMAGE_INTEGRITY_V1,
        changedCount: imageIntegrity.changedCount,
        changed: imageIntegrity.changed
      };
      (imageIntegrity.changed || []).forEach(function (ch) {
        bumpModifiedSlot(assembled, ch.slotKey, {
          publicationAction: 'property_image_integrity',
          reason: ch.reason,
          imageFrom: ch.from,
          imageTo: ch.to,
          imageCaption: ch.caption || null
        });
      });
    }

    // RIGHT-only Dealality Insight advisory quality.
    var advisoryApi = global.BeDealalityInsightAdvisory;
    if (advisoryApi && advisoryApi.applyDealalityInsightAdvisoryToBlocks) {
      var advisory = advisoryApi.applyDealalityInsightAdvisoryToBlocks(assembled.blocks, {
        brand: {
          id: currentBrand.id || currentBrand.brandId || null,
          name: currentBrand.name || currentBrand.brandName || null
        }
      });
      assembled.blocks = advisory.blocks;
      assembled.dealalityInsightAdvisory = {
        contract: advisoryApi.BE_DEALALITY_INSIGHT_ADVISORY_V1,
        changedCount: advisory.changedCount,
        changed: advisory.changed,
        internalEvidence: advisory.internalEvidence
      };
      (advisory.changed || []).forEach(function (ch) {
        bumpModifiedSlot(assembled, ch.slotKey, {
          publicationAction: 'dealality_insight_advisory',
          reason: ch.reason,
          advisoryAction: ch.action || ch.reason
        });
      });
    }

    // RIGHT-only: strip leading title echoes from card bodies.
    var headingEchoApi = global.BeNoRepeatedCardHeading;
    if (headingEchoApi && headingEchoApi.applyNoRepeatedCardHeadingToBlocks) {
      var headingEcho = headingEchoApi.applyNoRepeatedCardHeadingToBlocks(assembled.blocks);
      assembled.blocks = headingEcho.blocks;
      assembled.noRepeatedCardHeading = {
        contract: headingEchoApi.BE_NO_REPEATED_CARD_HEADING_V1,
        changedCount: headingEcho.changedCount,
        changed: headingEcho.changed
      };
      (headingEcho.changed || []).forEach(function (ch) {
        bumpModifiedSlot(assembled, ch.slotKey, {
          publicationAction: 'no_repeated_card_heading',
          reason: ch.reason
        });
      });
    }

    // Final RIGHT-only editorial Title Case + & conjunction (after other RIGHT writers).
    if (titleCaseApi && titleCaseApi.applyEditorialTitleCaseToBlocks) {
      var titleCase = titleCaseApi.applyEditorialTitleCaseToBlocks(assembled.blocks);
      assembled.blocks = titleCase.blocks;
      assembled.titleCase = {
        contract: titleCaseApi.BE_EDITORIAL_TITLE_CASE_V1,
        changedCount: titleCase.changedCount,
        changedSlots: titleCase.changedSlots
      };
      (titleCase.changedSlots || []).forEach(function (ch) {
        bumpModifiedSlot(assembled, ch.slotKey, {
          publicationAction: 'title_case_normalized',
          reason: 'editorial_title_case_on_proposed',
          titleCaseFrom: ch.from,
          titleCaseTo: ch.to
        });
      });
    }

    var brand = JSON.parse(JSON.stringify(currentBrand));
    brand.brandExplorer = {
      version: 1,
      blocks: assembled.blocks,
      overlaySource: 'cumulative-proposal-v1'
    };
    brand.beCandidateOverlay = true;
    brand.beCumulativeProposal = true;
    brand.beCandidateOverlayMeta = {
      phase: (candidateOverlay && candidateOverlay.phase) || null,
      candidateVersion:
        (candidateOverlay && candidateOverlay.candidateVersion) ||
        opts.candidateVersion ||
        null,
      assembly: BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1,
      titleCase: titleCaseApi ? titleCaseApi.BE_EDITORIAL_TITLE_CASE_V1 : null,
      titleCaseChangedCount:
        assembled.titleCase && assembled.titleCase.changedCount != null
          ? assembled.titleCase.changedCount
          : 0,
      propertyImageIntegrity: imageApi ? imageApi.BE_PROPERTY_IMAGE_INTEGRITY_V1 : null,
      propertyImageIntegrityChangedCount:
        assembled.propertyImageIntegrity && assembled.propertyImageIntegrity.changedCount != null
          ? assembled.propertyImageIntegrity.changedCount
          : 0,
      dealalityInsightAdvisory: advisoryApi
        ? advisoryApi.BE_DEALALITY_INSIGHT_ADVISORY_V1
        : null,
      dealalityInsightAdvisoryChangedCount:
        assembled.dealalityInsightAdvisory &&
        assembled.dealalityInsightAdvisory.changedCount != null
          ? assembled.dealalityInsightAdvisory.changedCount
          : 0,
      note: assembled.note,
      incompleteDraftSlotCount: assembled.incompleteDrafts.length,
      approvalBlockedByRegression: assembled.approvalBlockedByRegression,
      provenanceCounts: assembled.provenanceCounts
    };
    return { brand: brand, assembly: assembled };
  }

  function countsSafeDec(counts, key) {
    if (!counts || counts[key] == null) return;
    counts[key] = Math.max(0, counts[key] - 1);
  }
  function countsSafeInc(counts, key) {
    if (!counts) return;
    counts[key] = (counts[key] || 0) + 1;
  }
  function bumpModifiedSlot(assembled, key, patch) {
    if (!key || !assembled || !assembled.provenanceBySlot) return;
    var prev = assembled.provenanceBySlot[key] || {};
    assembled.provenanceBySlot[key] = Object.assign({}, prev, {
      provenance: PROVENANCE.MODIFIED_EXISTING,
      applyCandidate: false
    }, patch || {});
    if (prev.provenance === PROVENANCE.RETAINED_FROM_CURRENT) {
      countsSafeDec(assembled.provenanceCounts, 'RETAINED_FROM_CURRENT');
      countsSafeInc(assembled.provenanceCounts, 'MODIFIED_EXISTING');
    }
  }

  global.BeCumulativeProposalAssembler = {
    BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1: BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1,
    PROVENANCE: PROVENANCE,
    slotGroupQuality: slotGroupQuality,
    decideSlotProvenance: decideSlotProvenance,
    assembleCumulativeProposal: assembleCumulativeProposal,
    buildCumulativeProposedBrand: buildCumulativeProposedBrand,
    isEphemeralAirtableCdnUrl: isEphemeralAirtableCdnUrl,
    isPresentationDeliveryUrl: isPresentationDeliveryUrl,
    currentImageDeliveryUrl: currentImageDeliveryUrl,
    rehydrateEphemeralImageUrls: rehydrateEphemeralImageUrls
  };
})(typeof window !== 'undefined' ? window : globalThis);
