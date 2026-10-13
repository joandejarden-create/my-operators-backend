/**
 * Lodging decision state — who controls, whether selection is open, block/list/self-book.
 */

import { SELECTION_STATUS } from "./constants.js";
import { customerSafeControllerCopy } from "../international-discovery-v2/demand-controller-model.js";

export function buildLodgingDecisionState({
  controller = null,
  selectionProcess = {},
  lodgingEvidenceClass = "NONE",
  publicFacts = {},
} = {}) {
  const status = selectionProcess.selectionStatus || SELECTION_STATUS.UNKNOWN;
  const model = selectionProcess.selectionModel || "UNKNOWN";

  const hotelsBeingSelected =
    [SELECTION_STATUS.OPEN, SELECTION_STATUS.EXPECTED, SELECTION_STATUS.UNDER_REVIEW, SELECTION_STATUS.PARTIALLY_PLACED].includes(
      status
    );

  const rateSubmissionsAccepted =
    publicFacts.rateSubmissionsAccepted === true ||
    status === SELECTION_STATUS.OPEN ||
    selectionProcess.RFPStatus === "OPEN";

  const officialHotelListExists =
    publicFacts.recommendedListPublished === true ||
    (selectionProcess.knownOfficialHotels || []).length > 0 ||
    String(lodgingEvidenceClass).toUpperCase() === "DIRECT_LODGING_EVIDENCE";

  const roomBlockExists = publicFacts.roomBlockConfirmed === true;
  const selfBookingExpected =
    model === "SELF_BOOKING_RECOMMENDED_LIST" || publicFacts.selfBooking === true;
  const secondaryOverflowPossible =
    publicFacts.overflowEvidenced === true || publicFacts.secondaryPartnerOpen === true;

  const copy = controller ? customerSafeControllerCopy(controller) : null;

  return {
    lodgingControllerId: controller?.demandControllerId || selectionProcess.selectionControllerId || null,
    lodgingControllerName: controller?.controllerName || null,
    lodgingControllerAuthority: controller?.lodgingAuthority || null,
    hotelsCurrentlyBeingSelected: hotelsBeingSelected,
    rateSubmissionsAccepted: rateSubmissionsAccepted === true,
    officialHotelListExists,
    roomBlockExists,
    selfBookingExpected,
    secondaryOverflowPossible,
    // Never invent overflow
    overflowInferred: false,
    selectionDecisionWindow: {
      open: selectionProcess.selectionOpenDate || null,
      close: selectionProcess.selectionCloseDate || null,
      listPublish: selectionProcess.hotelListPublishDate || null,
      rateDeadline: selectionProcess.rateSubmissionDeadline || null,
    },
    selectionStatus: status,
    selectionModel: model,
    lodgingEvidenceClass: lodgingEvidenceClass || "NONE",
    customerSafe: {
      lodgingDecisionLine: copy?.lodgingDecisionLine || null,
      hotelSelectionLine: selectionStatusCustomerLine(status, selectionProcess),
      bestRouteIn: copy?.bestRouteIn || selectionProcess.submissionPath || null,
    },
    evidenceSource: selectionProcess.evidenceSource || controller?.evidenceSource || null,
  };
}

function selectionStatusCustomerLine(status, process = {}) {
  switch (status) {
    case SELECTION_STATUS.OPEN:
      return "Hotel selection appears open for submissions.";
    case SELECTION_STATUS.EXPECTED:
      return process.hotelListPublishDate
        ? `Hotel list / partner selection expected around ${process.hotelListPublishDate}.`
        : "Hotel selection is expected but not yet published.";
    case SELECTION_STATUS.UNDER_REVIEW:
      return "A hotel list exists; inclusion / revision may still be possible.";
    case SELECTION_STATUS.PARTIALLY_PLACED:
      return "An official / host hotel appears locked; secondary participation needs confirmation.";
    case SELECTION_STATUS.PLACED:
    case SELECTION_STATUS.CLOSED:
      return "Hotel selection appears closed or fully placed.";
    case SELECTION_STATUS.NOT_STARTED:
      return "Hotel / housing process has not started publicly.";
    default:
      return "Hotel selection status is not yet confirmed.";
  }
}
