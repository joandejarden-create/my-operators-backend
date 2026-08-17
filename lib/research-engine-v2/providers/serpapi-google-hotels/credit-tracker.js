/**
 * Credit / search tracker for SerpApi benchmark — never stores API keys.
 */

export class SerpApiCreditTracker {
  /**
   * @param {{ ceiling?: number, startingSearchesLeft?: number|null }} [opts]
   */
  constructor(opts = {}) {
    this.ceiling = opts.ceiling ?? 200;
    this.startingSearchesLeft = opts.startingSearchesLeft ?? null;
    this.endingSearchesLeft = null;
    this.charged = 0;
    this.entries = [];
    this.blocked = false;
    this.blockReason = null;
    this.failed = 0;
    this.successful = 0;
  }

  /**
   * @param {{ endpoint: string, hotelId?: string, purpose: string, credits: number, result?: string, useful?: boolean, searchId?: string|null }} entry
   */
  record(entry) {
    const credits = Number(entry.credits) || 0;
    this.charged += credits;
    if (entry.result === "ok" || entry.result === "success") this.successful += 1;
    else if (entry.result && entry.result !== "ok") this.failed += 1;
    this.entries.push({
      at: new Date().toISOString(),
      endpoint: entry.endpoint,
      hotel_id: entry.hotelId || null,
      purpose: entry.purpose,
      searches_charged_estimate: credits,
      result: entry.result || null,
      useful_field_resolved: Boolean(entry.useful),
      search_id: entry.searchId || null,
    });
    if (this.charged >= this.ceiling) {
      this.blocked = true;
      this.blockReason = `search_ceiling_${this.ceiling}_reached`;
    }
  }

  canSpend(estimate = 1) {
    if (this.blocked) return false;
    return this.charged + estimate <= this.ceiling;
  }

  summary() {
    return {
      starting_searches_left: this.startingSearchesLeft,
      ending_searches_left: this.endingSearchesLeft,
      total_searches_charged_estimate: this.charged,
      successful_requests: this.successful,
      failed_requests: this.failed,
      ceiling: this.ceiling,
      blocked: this.blocked,
      block_reason: this.blockReason,
      entries: this.entries,
      useful_entries: this.entries.filter((e) => e.useful_field_resolved).length,
      note: "Per-request estimate assumes 1 search per successful SerpApi call; Account API delta is authoritative.",
    };
  }
}
