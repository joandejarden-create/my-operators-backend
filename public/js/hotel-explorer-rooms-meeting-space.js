/**
 * Hotel Explorer — Guest Rooms & Meeting Space tab (hotel-agnostic renderer).
 * Consumes hotel.guestRoomsMeetingSpace inventory; no Airtable writes.
 * Uses DealalitySortableTable for meeting-room inventory.
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

  function statusLabel(raw) {
    var s = String(raw || "").toUpperCase();
    if (s === "VERIFIED" || s === "VERIFIED_FACT") return "Verified";
    if (s === "BRAND_MATERIALS_REFERENCE") return "Brand reference · not Airtable-persisted";
    if (s === "MARKETING_FIGURE") return "Marketing figure · distinct from capacity chart";
    if (s === "NOT_YET_VERIFIED" || !s) return "Not yet verified";
    if (s === "PROPOSED" || s === "PROPOSED_STAGED") return "Proposed · not persisted";
    return s.replace(/_/g, " ");
  }

  function metricCard(label, value, note) {
    return (
      '<div class="hex-rms-metric">' +
      '<div class="hex-rms-metric__value">' +
      esc(value == null || value === "" ? "—" : value) +
      "</div>" +
      '<div class="hex-rms-metric__label">' +
      esc(label) +
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

  function meetingRoomColumns() {
    return [
      { key: "name", label: "Room", type: "text", align: "left" },
      { key: "area", label: "Area (sq ft)", type: "number", align: "right" },
      { key: "dimensions", label: "Dimensions", type: "text", align: "left" },
      { key: "ceiling", label: "Ceiling (ft)", type: "number", align: "right" },
      { key: "theater", label: "Theater", type: "number", align: "right" },
      { key: "classroom", label: "Classroom", type: "number", align: "right" },
      { key: "banquet", label: "Banquet", type: "number", align: "right" },
      { key: "uShape", label: "U-Shape", type: "number", align: "right" },
      { key: "reception", label: "Reception", type: "number", align: "right" },
      { key: "config", label: "Config", type: "text", align: "left" },
    ];
  }

  function toSortRows(rows) {
    return rows.map(function (r) {
      var kind = String(r.kind || "").replace(/_/g, " ");
      var excluded = r.includeInAggregate === false;
      var config = kind + (excluded ? " · not additive" : "");
      return {
        name: r.name || "",
        area: r.areaSqFt != null ? r.areaSqFt : null,
        dimensions: r.dimensions || "",
        ceiling: r.ceilingFt != null ? r.ceilingFt : null,
        theater: r.theater != null ? r.theater : null,
        classroom: r.classroom != null ? r.classroom : null,
        banquet: r.banquet != null ? r.banquet : null,
        uShape: r.uShape != null ? r.uShape : null,
        reception: r.reception != null ? r.reception : null,
        config: config,
        _rowClass: excluded ? "hex-rms-row--secondary" : "",
        _cells: {
          name: esc(r.name || "—"),
          area: r.areaSqFt != null ? esc(fmt(r.areaSqFt)) : "—",
          dimensions: esc(r.dimensions || "—"),
          ceiling: r.ceilingFt != null ? esc(fmt(r.ceilingFt)) : "—",
          theater: r.theater != null ? esc(fmt(r.theater)) : "—",
          classroom: r.classroom != null ? esc(fmt(r.classroom)) : "—",
          banquet: r.banquet != null ? esc(fmt(r.banquet)) : "—",
          uShape: r.uShape != null ? esc(fmt(r.uShape)) : "—",
          reception: r.reception != null ? esc(fmt(r.reception)) : "—",
          config: esc(config),
        },
      };
    });
  }

  function renderMeetingTable(rows) {
    if (!rows.length) return "";
    var api = global.DealalitySortableTable;
    var body;
    if (api && typeof api.renderMarkup === "function") {
      body = api.renderMarkup({
        columns: meetingRoomColumns(),
        rows: toSortRows(rows),
        tableClass: "hex-rms-table dl-table--inventory",
        defaultSortKey: "area",
        defaultSortDir: "desc",
        emptyMessage: "No meeting-room inventory rows.",
      });
    } else {
      body =
        '<div class="dl-table-wrap hiw-table-wrap"><table class="hiw-table hex-rms-table dl-table--inventory"><thead><tr>' +
        meetingRoomColumns()
          .map(function (c) {
            return "<th>" + esc(c.label) + "</th>";
          })
          .join("") +
        "</tr></thead><tbody>" +
        toSortRows(rows)
          .map(function (r) {
            return (
              "<tr" +
              (r._rowClass ? ' class="' + esc(r._rowClass) + '"' : "") +
              ">" +
              meetingRoomColumns()
                .map(function (c) {
                  return "<td>" + (r._cells[c.key] || "—") + "</td>";
                })
                .join("") +
              "</tr>"
            );
          })
          .join("") +
        "</tbody></table></div>";
    }
    return (
      '<section class="hiw-section"><h3 class="hiw-section__h">Meeting Room Inventory</h3>' +
      '<p class="hiw-muted">Combined rooms and divisible salons are listed for planning. Rows marked <em>not additive</em> are excluded from space totals to prevent double-counting.</p>' +
      body +
      "</section>"
    );
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
        "Total guestrooms",
        fmt(gr.totalGuestrooms && gr.totalGuestrooms.value),
        statusLabel(gr.totalGuestrooms && gr.totalGuestrooms.verificationStatus)
      ) +
      metricCard(
        "Single-bed rooms",
        fmt(gr.singleBedRooms && gr.singleBedRooms.value),
        statusLabel(gr.singleBedRooms && gr.singleBedRooms.verificationStatus)
      ) +
      metricCard(
        "Double-bed rooms",
        fmt(gr.doubleBedRooms && gr.doubleBedRooms.value),
        statusLabel(gr.doubleBedRooms && gr.doubleBedRooms.verificationStatus)
      ) +
      metricCard(
        "Suites",
        fmt(gr.suites && gr.suites.value),
        statusLabel(gr.suites && gr.suites.verificationStatus)
      ) +
      "</div>" +
      '<p class="hiw-muted">Aggregation rule: total guestrooms is canonical. Suite count is a classification within the inventory and is not added on top of the total. Room-type counts marked as brand reference are not Airtable-persisted.</p>' +
      "</section>";

    var largest = sum.largestRoom || {};
    var meetHtml =
      '<section class="hiw-section"><h3 class="hiw-section__h">Meeting &amp; Event Overview</h3>' +
      '<div class="hex-rms-metrics">' +
      metricCard(
        "Total meeting space",
        sum.totalMeetingSpaceSqFt && sum.totalMeetingSpaceSqFt.value != null
          ? fmt(sum.totalMeetingSpaceSqFt.value) + " sq ft"
          : "—",
        statusLabel(sum.totalMeetingSpaceSqFt && sum.totalMeetingSpaceSqFt.verificationStatus)
      ) +
      metricCard(
        "Meeting rooms (published)",
        fmt(sum.meetingRoomCount && sum.meetingRoomCount.value),
        statusLabel(sum.meetingRoomCount && sum.meetingRoomCount.verificationStatus)
      ) +
      metricCard(
        "Largest room",
        largest.name
          ? largest.name +
              (largest.areaSqFt != null ? " · " + fmt(largest.areaSqFt) + " sq ft" : "")
          : "—",
        statusLabel(largest.verificationStatus)
      ) +
      metricCard(
        "Marketing private-space figure",
        sum.privateSpaceMarketingSqFt && sum.privateSpaceMarketingSqFt.value != null
          ? fmt(sum.privateSpaceMarketingSqFt.value) + " sq ft"
          : "—",
        statusLabel(sum.privateSpaceMarketingSqFt && sum.privateSpaceMarketingSqFt.verificationStatus)
      ) +
      "</div>" +
      (sum.floorPlanUrl && sum.floorPlanUrl.value
        ? '<p class="hiw-prose"><a href="' +
          esc(sum.floorPlanUrl.value) +
          '" target="_blank" rel="noopener noreferrer">' +
          esc(sum.floorPlanUrl.label || "Official meeting-space materials") +
          "</a></p>"
        : "") +
      '<p class="hiw-muted">Do not treat the capacity-chart aggregate and marketing private-space figure as interchangeable. Divisible salons and combined configurations are listed for planning and are excluded from additive space totals.</p>' +
      "</section>";

    var tableHtml = renderMeetingTable(rows);

    var srcHtml = "";
    if (sources.length) {
      srcHtml =
        '<section class="hiw-section"><h3 class="hiw-section__h">Supporting Sources</h3><ul class="hex-rms-sources">' +
        sources
          .map(function (s) {
            var title = s.title || s.url || "Source";
            var link = s.url
              ? '<a href="' + esc(s.url) + '" target="_blank" rel="noopener noreferrer">' + esc(title) + "</a>"
              : esc(title);
            return (
              "<li>" +
              link +
              ' <span class="hex-rms-src-state">' +
              esc(statusLabel(s.verificationStatus)) +
              "</span>" +
              (s.supports
                ? '<div class="hiw-muted">Supports: ' + esc((s.supports || []).join(", ")) + "</div>"
                : "") +
              (s.does_not ? '<div class="hiw-muted">Does not: ' + esc(s.does_not) + "</div>" : "") +
              "</li>"
            );
          })
          .join("") +
        "</ul></section>";
    }

    return (
      '<article class="hiw-report hex-rms" data-hex-rms="1">' +
      '<h2 class="hiw-report__title">Guest Rooms &amp; Meeting Space</h2>' +
      '<p class="hiw-report__subtitle">' +
      esc(hotel && hotel.name ? hotel.name : "Hotel") +
      " · physical product · capacities from official brand materials where verified</p>" +
      guestHtml +
      meetHtml +
      tableHtml +
      srcHtml +
      "</article>"
    );
  }

  function mount(host, hotel, ownership) {
    if (!host) return;
    host.innerHTML = render(hotel, ownership);
    if (global.DealalitySortableTable && typeof global.DealalitySortableTable.bind === "function") {
      global.DealalitySortableTable.bind(host);
    }
  }

  global.HotelExplorerRoomsMeetingSpace = {
    render: render,
    mount: mount,
    resolveInventory: resolveInventory,
    meetingRoomColumns: meetingRoomColumns,
  };
})(typeof window !== "undefined" ? window : globalThis);
