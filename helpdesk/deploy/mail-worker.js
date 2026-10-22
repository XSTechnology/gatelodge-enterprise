/**
 * Gatelodge — inbound support mail worker.
 *
 * Deploy as a Cloudflare Email Worker (Workers + Email Routing):
 *   1. Create a token with Workers Scripts:Edit + deploy this file
 *      (wrangler deploy) into a worker named e.g. "gatelodge-mail".
 *   2. Set the worker's secrets/env:
 *        WEBHOOK_URL  = https://chat.datagatelodge.com/api/email/inbound
 *        INBOUND_KEY  = the INBOUND_KEY env on the gatekeeper container
 *        FALLBACK_MAILBOX (optional, recommended) = a human-monitored
 *            mailbox that receives the raw email when the webhook fails,
 *            so backend downtime never silently drops support mail.
 *   3. In Cloudflare Email Routing (datagatelodge.com), change the
 *      support@ rule from "forward to sales@xs.technology" to
 *      "send to worker gatelodge-mail". The catch-all can stay as-is
 *      (it feeds the same agent) or keep forwarding.
 *
 * The agent replies to confident questions from the same knowledge base
 * the chat widget uses; everything else queues in the Lodge Office.
 */
export default {
  async email(message, env, ctx) {
    const raw = await new Response(message.raw).text();
    try {
      const resp = await fetch(env.WEBHOOK_URL, {
        method: "POST",
        headers: {
          "Authorization": "Bearer " + env.INBOUND_KEY,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          message_id: message.headers.get("message-id") || "",
          raw: raw.slice(0, 50000),
        }),
      });
      // REVIEW FIX: fetch only rejects on network errors -- a non-2xx
      // (backend down, auth rejected) used to be silently swallowed, and
      // routed support mail was consumed with no trace anywhere.
      if (!resp.ok) {
        throw new Error("gatekeeper webhook HTTP " + resp.status);
      }
    } catch (e) {
      console.error("gatekeeper webhook failed", e);
      // REVIEW FIX: don't lose the mail -- fall back to forwarding it to
      // a human-monitored mailbox when FALLBACK_MAILBOX is configured,
      // so a backend outage degrades to slower human handling instead
      // of silent loss.
      if (env.FALLBACK_MAILBOX) {
        await message.forward(env.FALLBACK_MAILBOX);
      }
    }
  },
};
