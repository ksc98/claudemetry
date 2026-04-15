// Read-side helpers for the email → user_hash mapping. The proxy worker is
// the only writer (auto-link side-effect of compute_user_hash); the dashboard
// just reads the link to scope itself to the caller's DO.

const LINK_PREFIX = "link:";

function keyFor(email: string): string {
  return LINK_PREFIX + email.trim().toLowerCase();
}

export function readCfAccessEmail(req: Request): string | null {
  const raw = req.headers.get("cf-access-authenticated-user-email");
  if (!raw) return null;
  const v = raw.trim();
  return v.length > 0 ? v : null;
}

export async function getLinkedHash(
  kv: KVNamespace,
  email: string | null,
): Promise<string | null> {
  if (!email) return null;
  const v = await kv.get(keyFor(email));
  if (v && /^[0-9a-f]{16}$/.test(v)) return v;
  return null;
}
