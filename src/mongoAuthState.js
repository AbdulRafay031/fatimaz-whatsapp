import { BufferJSON, initAuthCreds, proto } from "@whiskeysockets/baileys";

export async function useMongoAuthState(db, sessionId = "primary") {
  const collection = db.collection("whatsappauthstates");
  const keyFor = (key) => `${sessionId}:${key}`;

  const readData = async (key) => {
    const doc = await collection.findOne({ _id: keyFor(key) });
    if (!doc?.value) return null;
    try { return JSON.parse(doc.value, BufferJSON.reviver); }
    catch { return null; }
  };

  const writeData = async (data, key) => {
    const value = JSON.stringify(data, BufferJSON.replacer);
    await collection.updateOne(
      { _id: keyFor(key) },
      { $set: { value, updatedAt: new Date() } },
      { upsert: true }
    );
  };

  const removeData = async (key) => collection.deleteOne({ _id: keyFor(key) });
  const creds = (await readData("creds")) || initAuthCreds();

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const result = {};
          await Promise.all(ids.map(async (id) => {
            let value = await readData(`${type}-${id}`);
            if (type === "app-state-sync-key" && value) value = proto.Message.AppStateSyncKeyData.fromObject(value);
            result[id] = value;
          }));
          return result;
        },
        set: async (data) => {
          const tasks = [];
          for (const category of Object.keys(data || {})) {
            for (const id of Object.keys(data[category] || {})) {
              const value = data[category][id];
              tasks.push(value ? writeData(value, `${category}-${id}`) : removeData(`${category}-${id}`));
            }
          }
          await Promise.all(tasks);
        },
      },
    },
    saveCreds: async () => writeData(creds, "creds"),
    clearAuth: async () => collection.deleteMany({ _id: { $regex: `^${sessionId}:` } }),
  };
}
