/**
 * Detail-path contact parity with the approved Sheraton drawer comparison RIGHT.
 *
 * Read-time only: recovers publicly published email/phone from opportunity
 * source pages when the packaged primaryContact lacks channels, then rebuilds
 * `scalingTwoContactPresentation` via the shared unified Who-to-Contact contract.
 *
 * Never calls Surfe. Never invents contacts. Never mutates publication bytes.
 */
import { recoverPublicContactChannels } from "./gdi-public-contact-channel-hierarchy-v1.js";
import { applyUnifiedWhoToContactStandard } from "../unified-who-to-contact-v1.js";

export const GDI_DRAWER_CONTACT_PARITY_PRESENTATION_V1 =
  "gdi-drawer-contact-parity-presentation-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * @param {object} opportunity
 * @param {{ timeoutMs?: number, maxUrls?: number, skipRecovery?: boolean }} [opts]
 * @returns {Promise<object>}
 */
export async function ensureDrawerContactParityPresentation(
  opportunity = {},
  opts = {},
) {
  if (!opportunity || typeof opportunity !== "object") return opportunity;

  let next = opportunity;
  if (opts.skipRecovery !== true) {
    const alreadyEmail = text(
      next.primaryContact?.email || next.contactEmail,
    );
    const alreadyPhone = text(
      next.primaryContact?.phone || next.contactPhone,
    );
    if (!(alreadyEmail && alreadyPhone)) {
      try {
        const recovered = await recoverPublicContactChannels(next, {
          timeoutMs: opts.timeoutMs || 7000,
          maxUrls: opts.maxUrls || 3,
        });
        next = recovered.opportunity || next;
        next = {
          ...next,
          publicChannelRecoveryMeta: {
            version: GDI_DRAWER_CONTACT_PARITY_PRESENTATION_V1,
            reason: recovered.reason || null,
            changed: Boolean(recovered.changed),
            found: recovered.found || null,
          },
        };
      } catch (err) {
        next = {
          ...next,
          publicChannelRecoveryMeta: {
            version: GDI_DRAWER_CONTACT_PARITY_PRESENTATION_V1,
            reason: "RECOVERY_ERROR",
            changed: false,
            error: err?.message || String(err),
          },
        };
      }
    }
  }

  return applyUnifiedWhoToContactStandard(next, { force: true });
}
