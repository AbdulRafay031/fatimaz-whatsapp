const PORTABLE_DB_NAME = /^[A-Za-z0-9_-]+$/;

export function resolveDatabaseName(uri, explicitName) {
  const explicit = String(explicitName || "").trim();
  if (explicit) {
    if (Buffer.byteLength(explicit, "utf8") >= 64 || !PORTABLE_DB_NAME.test(explicit)) {
      throw new Error(
        `Invalid MONGODB_DB ${JSON.stringify(explicit)}. Use only letters, numbers, _ or - (example: Fatimaz). ` +
        `Do not put commands such as \"npm run dev\" in the MONGODB_DB value.`
      );
    }
    return explicit;
  }

  try {
    const parsed = new URL(uri);
    const fromUri = decodeURIComponent(parsed.pathname.replace(/^\/+|\/+$/g, ""));
    if (!fromUri) return "";
    if (Buffer.byteLength(fromUri, "utf8") >= 64 || !PORTABLE_DB_NAME.test(fromUri)) {
      throw new Error(`Database name in MONGODB_URI is invalid: ${JSON.stringify(fromUri)}`);
    }
    return fromUri;
  } catch (error) {
    if (error?.message?.startsWith("Database name")) throw error;
    return "";
  }
}
