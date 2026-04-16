import type { APIRoute } from "astro";
import { env } from "cloudflare:workers";
import { readUserHash } from "@/lib/cookie";
import { getLinkedHash, readCfAccessEmail } from "@/lib/links";
import { getSessionTurns } from "@/lib/store";

export const prerender = false;

export const GET: APIRoute = async ({ request, url }) => {
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
  const id = url.searchParams.get("id");
  if (!id) {
    return new Response(JSON.stringify({ error: "missing id" }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }
  const limitRaw = url.searchParams.get("limit");
  const limit = limitRaw != null ? Number(limitRaw) : undefined;
  try {
    const rows = await getSessionTurns(env.USER_STORE, userHash, id, limit);
    return new Response(JSON.stringify(rows), {
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
