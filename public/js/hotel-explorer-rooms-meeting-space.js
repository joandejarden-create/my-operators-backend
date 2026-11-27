/**
 * Hotel Explorer — Guest Rooms & Meeting Space tab (hotel-agnostic renderer).
 * Uses DealalitySortableTable with established Dealality dark-header table styling.
 */
(function (global) {
  "use strict";

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmt(n) {
    if (n == null || n === "" || n === "—") return "—";
    var num = typeof n === "number" ? n : Number(String(n).replace(/,/g, ""));
    if (!Number.isFinite(num)) return String(n);
    return Math.round(num).toLocaleString("en-US");
  }

  function statusLabel(raw, inventory) {
    var s = String(raw || "").toUpperCase();
    var fromHpc =
      inventory &&
      (inventory.sourceOfTruth === "HOTEL_PROPERTY_CENSUS" || inventory.persisted === true);
    if (s === "VERIFIED" || s === "VERIFIED_FACT") return "Verified";
    if (s === "BRAND_MATERIALS_REFERENCE") {
      if (window.HeCustomerPresentationFormat && typeof window.HeCustomerPresentationFormat.customerPersistenceLabel === "function") {
        return window.HeCustomerPresentationFormat.customerPersistenceLabel(inventory);
      }
      return fromHpc ? "Verified property record" : "Brand reference";
    }
    if (s === "MARKETING_FIGURE") return "Marketing figure · distinct from capacity chart";
    if (s === "NOT_YET_VERIFIED" || !s) return "Not yet verified";
    if (s === "PROPOSED" || s === "PROPOSED_STAGED") return "Under review";
    return s.replace(/_/g, " ");
  }

  function fieldLabel(label) {
    var fmt = global.HeCustomerPresentationFormat;
    if (fmt && typeof fmt.allCapsFieldLabel === "function") {
      return fmt.allCapsFieldLabel(label);
    }
    return String(label || "")
      .trim()
      .toUpperCase();
  }

  function metricCard(label, value, note) {
    return (
      '<div class="hex-rms-metric">' +
      '<div class="hex-rms-metric__value">' +
      esc(value == null || value === "" ? "—" : value) +
      "</div>" +
      '<div class="hex-rms-metric__label">' +
      esc(fieldLabel(label)) +
      "</div>" +
      (note ? '<div class="hex-rms-metric__note">' + esc(note) + "</div>" : "") +
      "</div>"
    );
  }

  function resolveInventory(hotel, ownership) {
    if (hotel && hotel.guestRoomsMeetingSpace) return hotel.guestRoomsMeetingSpace;
    if (ownership && ownership.guestRoomsMeetingSpace) return ownership.guestRoomsMeetingSpace;
    if (ownership && ownership.report && ownership.report.guestRoomsMeetingSpace) {
      return ownership.report.guestRoomsMeetingSpace;
    }
    return null;
  }

  function renderEmpty(hotel) {
    return (
      '<article class="hiw-report hex-rms">' +
      '<h2 class="hiw-report__title">Guest Rooms &amp; Meeting Space</h2>' +
      '<p class="hiw-report__subtitle">' +
      esc(hotel && hotel.name ? hotel.name : "This hotel") +
      " · physical product inventory</p>" +
      '<div class="hiw-empty">Detailed guestroom and meeting-space inventory is not yet available for this hotel. Total rooms and meeting-space aggregates still appear on Hotel Overview when verified.</div>' +
      "</article>"
    );
  }

  function prefs() {
    return (global.HeMeasurementPrefs && global.HeMeasurementPrefs.get()) || {
      area: "sqft",
      distance: "mi",
    };
  }

  function formatAreaSqFt(sqFt) {
    if (sqFt == null || sqFt === "") return "—";
    if (global.HeMeasurementPrefs && typeof global.HeMeasurementPrefs.formatAreaFromSqFt === "function") {
      return global.HeMeasurementPrefs.formatAreaFromSqFt(sqFt, prefs());
    }
    return fmt(sqFt) + " Sq. Ft.";
  }

  function areaColumnLabel() {
    if (global.HeMeasurementPrefs && typeof global.HeMeasurementPrefs.areaUnitLabel === "function") {
      return "Area (" + global.HeMeasurementPrefs.areaUnitLabel(prefs()) + ")";
    }
    return "Area (Sq. Ft.)";
  }

  /** Primary desktop columns — Config moved to expandable row details. */
  function meetingRoomColumns() {
    return [
      { key: "name", label: "Room", type: "text", align: "left" },
      { key: "area", label: areaColumnLabel(), type: "number", align: "right" },
      { key: "dimensions", label: "Dimensions", type: "text", align: "left" },
      { key: "ceiling", label: "Ceiling", type: "number", align: "right" },
      { key: "theater", label: "Theater", type: "number", align: "right" },
      { key: "classroom", label: "Classroom", type: "number", align: "right" },
      { key: "banquet", label: "Banquet", type: "number", align: "right" },
      { key: "uShape", label: "U-Shape", type: "number", align: "right" },
      { key: "reception", label: "Reception", type: "number", align: "right" },
    ];
  }

  function toSortRows(rows) {
    return rows.map(function (r, idx) {
      var kind = String(r.kind || "").replace(/_/g, " ");
      var excluded = r.includeInAggregate === false;
      var config = kind + (excluded ? " · not additive" : "");
      var detailId = "hex-rms-detail-" + idx;
      var nameHtml =
        '<div class="hex-rms-room-name">' +
        esc(r.name || "—") +
        "</div>" +
        '<button type="button" class="hex-rms-detail-toggle" data-hex-rms-toggle="' +
        detailId +
        '" aria-expanded="false">Details</button>' +
        '<div class="hex-rms-row-detail" id="' +
        detailId +
        '" hidden>' +
        "<div><strong>Configuration:</strong> " +
        esc(config || "—") +
        "</div>" +
        (excluded
          ? "<div>Excluded from additive meeting-space totals to avoid double-counting divisible / combined layouts.</div>"
          : "") +
        "</div>";
      return {
        name: r.name || "",
        // Sort on source sq ft — never on formatted display strings.
        area: r.areaSqFt != null ? r.areaSqFt : null,
        dimensions: r.dimensions || "",
        ceiling: r.ceilingFt != null ? r.ceilingFt : null,
        theater: r.theater != null ? r.theater : null,
        classroom: r.classroom != null ? r.classroom : null,
        banquet: r.banquet != null ? r.banquet : null,
        uShape: r.uShape != null ? r.uShape : null,
        reception: r.reception != null ? r.reception : null,
        _rowClass: excluded ? "hex-rms-row--secondary" : "",
        _cells: {
          name: nameHtml,
          area: r.areaSqFt != null ? esc(formatAreaSqFt(r.areaSqFt)) : "—",
          dimensions: esc(r.dimensions || "—"),
          ceiling: r.ceilingFt != null ? esc(fmt(r.ceilingFt)) + " ft" : "—",
          theater: r.theater != null ? esc(fmt(r.theater)) : "—",
          classroom: r.classroom != null ? esc(fmt(r.classroom)) : "—",
          banquet: r.banquet != null ? esc(fmt(r.banquet)) : "—",
          uShape: r.uShape != null ? esc(fmt(r.uShape)) : "—",
          reception: r.reception != null ? esc(fmt(r.reception)) : "—",
        },
      };
    });
  }

  function renderMeetingTable(rows) {
    if (!rows.length) return "";
    var api = global.DealalitySortableTable;
    var body = api
      ? api.renderMarkup({
          columns: meetingRoomColumns(),
          rows: toSortRows(rows),
          tableClass: "hex-rms-table dl-table--inventory ada-table",
          defaultSortKey: "area",
          defaultSortDir: "desc",
          emptyMessage: "No meeting-room inventory rows.",
        })
      : "";
    return (
      '<section class="hiw-section"><h3 class="hiw-section__h">Meeting Room Inventory</h3>' +
      '<p class="hiw-muted">Combined rooms and divisible salons are listed for planning. Open <em>Details</em> on a row for configuration notes. Rows marked not additive are excluded from space totals.</p>' +
      body +
      "</section>"
    );
  }

  function renderSupportingDocs(sum, sources, inventory) {
    var items = [];
    if (sum.floorPlanUrl && sum.floorPlanUrl.value) {
      items.push({
        title: sum.floorPlanUrl.label || "Official meeting & event space materials",
        url: sum.floorPlanUrl.value,
        publisher: "Brand / hotel website",
        verificationStatus: sum.floorPlanUrl.verificationStatus || "VERIFIED",
        supports: ["meeting_space_total", "meeting_room_inventory", "capacities"],
      });
    }
    (sources || []).forEach(function (s) {
      items.push(s);
    });
    if (!items.length) return "";
    return (
      '<section class="hiw-section hex-rms-supporting"><h3 class="hiw-section__h">Supporting Documentation</h3>' +
      '<p class="hiw-muted">Capacity-chart totals and marketing private-space figures are distinct. Divisible salon areas are not additive to combined-room totals.</p>' +
      '<ul class="hex-rms-sources">' +
      items
        .map(function (s) {
          var title = s.title || "Source";
          var link = s.url
            ? '<a class="hex-rms-ext-link" href="' +
              esc(s.url) +
              '" target="_blank" rel="noopener noreferrer">' +
              esc(title) +
              ' <span class="hex-rms-ext-ico" aria-hidden="true">↗</span></a>'
            : esc(title);
          return (
            "<li>" +
            link +
            (s.publisher
              ? ' <span class="hex-rms-src-pub">' + esc(s.publisher) + "</span>"
              : "") +
            ' <span class="hex-rms-src-state">' +
            esc(statusLabel(s.verificationStatus, inventory)) +
            "</span>" +
            (s.supports
              ? '<div class="hiw-muted">Supports: ' + esc((s.supports || []).join(", ")) + "</div>"
              : "") +
            (s.does_not ? '<div class="hiw-muted">Does not: ' + esc(s.does_not) + "</div>" : "") +
            "</li>"
          );
        })
        .join("") +
      "</ul></section>"
    );
  }

  function bindDetailToggles(host) {
    if (!host) return;
    host.addEventListener("click", function (ev) {
      var btn = ev.target.closest && ev.target.closest("[data-hex-rms-toggle]");
      if (!btn) return;
      var id = btn.getAttribute("data-hex-rms-toggle");
      var el = document.getElementById(id);
      if (!el) return;
      var open = el.hasAttribute("hidden");
      if (open) el.removeAttribute("hidden");
      else el.setAttribute("hidden", "");
      btn.setAttribute("aria-expanded", open ? "true" : "false");
    });
  }

  function render(hotel, ownership) {
    var inv = resolveInventory(hotel, ownership);
    if (!inv) {
      var rooms = hotel && hotel.rooms;
      var ms = hotel && hotel.meetingSpaceProvenance;
      if (!rooms && !(ms && ms.totalSqFt)) return renderEmpty(hotel);
      inv = {
        guestRooms: {
          totalGuestrooms: { value: rooms, verificationStatus: rooms ? "VERIFIED" : "NOT_YET_VERIFIED" },
        },
        meetingSpaceSummary: {
          totalMeetingSpaceSqFt: ms
            ? { value: ms.totalSqFt, verificationStatus: "VERIFIED", sourceUrl: ms.sourceUrl }
            : { value: hotel.totalMeetingSpace, verificationStatus: "NOT_YET_VERIFIED" },
          meetingRoomCount: ms
            ? { value: ms.meetingRooms, verificationStatus: "VERIFIED" }
            : { value: null, verificationStatus: "NOT_YET_VERIFIED" },
          largestRoom: ms && ms.largestRoom
            ? {
                name: ms.largestRoom.name,
                areaSqFt: ms.largestRoom.sqFt,
                verificationStatus: "VERIFIED",
              }
            : null,
        },
        meetingRooms: [],
        sources: [],
      };
    }

    var gr = inv.guestRooms || {};
    var sum = inv.meetingSpaceSummary || {};
    var rows = Array.isArray(inv.meetingRooms) ? inv.meetingRooms : [];
    var sources = Array.isArray(inv.sources) ? inv.sources : [];

    var guestHtml =
      '<section class="hiw-section"><h3 class="hiw-section__h">Guestroom Overview</h3>' +
      '<div class="hex-rms-metrics">' +
      metricCard(
        "Guestrooms",
        fmt(gr.totalGuestrooms && gr.totalGuestrooms.value),
        statusLabel(gr.totalGuestrooms && gr.totalGuestrooms.verificationStatus, inv)
      ) +
      metricCard(
        "Single-Bed Rooms",
        fmt(gr.singleBedRooms && gr.singleBedRooms.value),
        statusLabel(gr.singleBedRooms && gr.singleBedRooms.verificationStatus, inv)
      ) +
      metricCard(
        "Double-Bed Rooms",
        fmt(gr.doubleBedRooms && gr.doubleBedRooms.value),
        statusLabel(gr.doubleBedRooms && gr.doubleBedRooms.verificationStatus, inv)
      ) +
      metricCard(
        "Suites",
        fmt(gr.suites && gr.suites.value),
        statusLabel(gr.suites && gr.suites.verificationStatus, inv)
      ) +
      "</div>" +
      '<p class="hiw-muted">Total guestrooms is canonical. Suite count is a classification within the inventory and is not added on top of the total.</p>' +
      "</section>";

    var largest = sum.largestRoom || {};
    var meetHtml =
      '<section class="hiw-section"><h3 class="hiw-section__h">Meeting &amp; Event Overview</h3>' +
      '<div class="hex-rms-metrics">' +
      metricCard(
        "Meeting Space",
        sum.totalMeetingSpaceSqFt && sum.totalMeetingSpaceSqFt.value != null
          ? formatAreaSqFt(sum.totalMeetingSpaceSqFt.value)
          : "—",
        statusLabel(sum.totalMeetingSpaceSqFt && sum.totalMeetingSpaceSqFt.verificationStatus, inv)
      ) +
      metricCard(
        "Meeting Rooms",
        fmt(sum.meetingRoomCount && sum.meetingRoomCount.value),
        statusLabel(sum.meetingRoomCount && sum.meetingRoomCount.verificationStatus, inv)
      ) +
      metricCard(
        "Largest Meeting Room",
        largest.name
          ? largest.name +
              (largest.areaSqFt != null ? " · " + formatAreaSqFt(largest.areaSqFt) : "")
          : "—",
        statusLabel(largest.verificationStatus, inv)
      ) +
      metricCard(
        "Marketing Private-Space Figure",
        sum.privateSpaceMarketingSqFt && sum.privateSpaceMarketingSqFt.value != null
          ? formatAreaSqFt(sum.privateSpaceMarketingSqFt.value)
          : "—",
        statusLabel(
          sum.privateSpaceMarketingSqFt && sum.privateSpaceMarketingSqFt.verificationStatus,
          inv
        )
      ) +
      "</div>" +
      '<p class="hiw-muted">Capacity-chart totals and marketing private-space figures are not interchangeable. Units follow the Hotel Explorer measurement preference (presentation only).</p>' +
      "</section>";

    var areaPrefsHtml =
      global.HeMeasurementPrefs && typeof global.HeMeasurementPrefs.renderAreaControlHtml === "function"
        ? '<div class="hex-rms-meas-prefs" data-hex-rms-meas="area">' +
          global.HeMeasurementPrefs.renderAreaControlHtml("he-meas-prefs--tab") +
          "</div>"
        : "";

    return (
      '<article class="hiw-report hex-rms" data-hex-rms="1">' +
      '<header class="hex-rms-header">' +
      '<div class="hex-rms-header__copy">' +
      '<h2 class="hiw-report__title">Guest Rooms &amp; Meeting Space</h2>' +
      '<p class="hiw-report__subtitle">' +
      esc(hotel && hotel.name ? hotel.name : "Hotel") +
      " · physical product · capacities from official brand materials where verified</p>" +
      "</div>" +
      areaPrefsHtml +
      "</header>" +
      guestHtml +
      meetHtml +
      renderMeetingTable(rows) +
      renderSupportingDocs(sum, sources, inv) +
      "</article>"
    );
  }

  function mount(host, hotel, ownership) {
    if (!host) return;
    host._hexRmsHotel = hotel;
    host._hexRmsOwnership = ownership;
    host.innerHTML = render(hotel, ownership);
    if (global.DealalitySortableTable && typeof global.DealalitySortableTable.bind === "function") {
      global.DealalitySortableTable.bind(host);
    }
    bindDetailToggles(host);
    var prefsSlot = host.querySelector("[data-hex-rms-meas]");
    if (
      prefsSlot &&
      global.HeMeasurementPrefs &&
      typeof global.HeMeasurementPrefs.bindControls === "function"
    ) {
      global.HeMeasurementPrefs.bindControls(prefsSlot);
    }
    if (!host._hexRmsPrefsBound && global.HeMeasurementPrefs && typeof global.HeMeasurementPrefs.onChange === "function") {
      host._hexRmsPrefsBound = true;
      global.HeMeasurementPrefs.onChange(function () {
        if (!host.isConnected) return;
        mount(host, host._hexRmsHotel, host._hexRmsOwnership);
      });
    }
  }

  global.HotelExplorerRoomsMeetingSpace = {
    render: render,
    mount: mount,
    resolveInventory: resolveInventory,
    meetingRoomColumns: meetingRoomColumns,
  };
})(typeof window !== "undefined" ? window : globalThis);
