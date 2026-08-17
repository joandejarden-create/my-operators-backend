/**
 * Credit tracker for StayingAPI benchmark — never stores API keys.
 */

export class StayingCreditTracker {
  /**
   * @param {{ ceiling?: number, startingAvailable?: number|null }} [opts]
   */
  constructor(opts = {}) {
    this.ceiling = opts.ceiling ?? 120;
    this.startingAvailable = opts.startingAvailable ?? null;
    this.endingAvailable = null;
    this.charged = 0;
    this.entries = [];
    this.blocked = false;
    this.blockReason = null;
  }

  /**
   * @param {{ endpoint: string, hotelId?: string, purpose: string, credits: number, result?: string, useful?: boolean, requestId?: string|null }} entry
   */
  record(entry) {
    const credits = Number(entry.credits) || 0;
    this.charged += credits;
    this.entries.push({
      at: new Date().toISOString(),
      endpoint: entry.endpoint,
      hotel_id: entry.hotelId || null,
      purpose: entry.purpose,
      credits_charged: credits,
      result: entry.result || null,
      useful_field_resolved: Boolean(entry.useful),
      request_id: entry.requestId || null,
      // never auth
    });
    if (this.charged >= this.ceiling) {
      this.blocked = true;
      this.blockReason = `credit_ceiling_${this.ceiling}_reached`;
    }
  }

  canSpend(estimate = 1) {
    if (this.blocked) return false;
    return this.charged + estimate <= this.ceiling;
  }

  summary() {
    return {
      starting_available: this.startingAvailable,
      ending_available: this.endingAvailable,
      total_credits_charged: this.charged,
      ceiling: this.ceiling,
      blocked: this.blocked,
      block_reason: this.blockReason,
      entries: this.entries,
      useful_entries: this.entries.filter((e) => e.useful_field_resolved).length,
    };
  }
}
