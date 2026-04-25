import type { APIRoute } from "astro";
import { env } from "cloudflare:workers";
import { readUserHash } from "@/lib/cookie";
import { getLinkedHash, readCfAccessEmail } from "@/lib/links";
import { getSessionsSummary, getSessionEnds, getInFlight } from "@/lib/store";
import { buildSessionListFromSummary } from "@/lib/sessions";

export const prerender = false;

export const GET: APIRoute = async ({ request }) => {
  const cfEmail = readCfAccessEmail(request);
  const linked = await getLinkedHash(env.SESSION, cfEmail);
  const cookieHash = readUserHash(request.headers.get("cookie"));
  const userHash = linked ?? cookieHash;
  if (!userHash) {
    return new Response(JSON.stringify({ error: "unauthenticated" }), {
      status: 401,
      headers: { "content-type": "application/json" },
    });
  }
  try {
    const [buckets, sessionEnds, inFlight] = await Promise.all([
      getSessionsSummary(env.USER_STORE, userHash),
      getSessionEnds(env.USER_STORE, userHash),
      getInFlight(env.USER_STORE, userHash),
    ]);
    const inFlightSessionIds = new Set(
      inFlight.map((r) => r.session_id).filter((s): s is string => !!s),
    );
    const sessions = buildSessionListFromSummary(
      buckets,
      sessionEnds,
      inFlightSessionIds,
    );
    return new Response(JSON.stringify(sessions), {
      status: 200,
      headers: {
        "content-type": "application/json",
        "cache-control": "no-store",
      },
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return new Response(JSON.stringify({ error: msg }), {
      status: 502,
      headers: { "content-type": "application/json" },
    });
  }
};
