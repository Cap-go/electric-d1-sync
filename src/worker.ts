/// <reference types="@cloudflare/workers-types" />

import { ShapeStream, isChangeMessage, isControlMessage, type Offset } from "@electric-sql/client";
import { 
  appVersionsSchema,
  channelsSchema,
  channelDevicesSchema,
  appsSchema,
  orgsSchema,
  stripeInfoSchema,
  syncStateSchema,
  syncLockSchema
} from "./schema.ts";

interface Env {
  DB_ENA: D1Database;  // East North America
  DB_WNA: D1Database;  // West North America
  DB_WEU: D1Database;  // West Europe
  DB_EEU: D1Database;  // East Europe
  DB_ASIA: D1Database; // Asia
  DB_OC: D1Database;   // Oceania
  ELECTRIC_URL: string;
  WEBHOOK_SECRET: string;
  ELECTRIC_SOURCE_ID: string;
  ELECTRIC_SOURCE_SECRET: string;
}

interface NukeRequest {
  type: 'all' | 'db' | 'table';
  db?: string;
  table?: string;
}

interface TableSchema {
  name: string;
  columns: string[];
  primaryKey: string;
}

const TABLES: TableSchema[] = [
  {
    name: "app_versions",
    columns: ["id", "owner_org", "app_id", "name", "r2_path", "user_id", "deleted", "external_url", "checksum", "session_key", "storage_provider", "min_update_version", "manifest"],
    primaryKey: "id"
  },
  {
    name: "channels",
    columns: ["id", "name", "app_id", "version", "created_by", "owner_org", "public", "disable_auto_update_under_native", "disable_auto_update", "ios", "android", "allow_device_self_set", "allow_emulator", "allow_dev"],
    primaryKey: "id"
  },
  {
    name: "channel_devices",
    columns: ["id", "channel_id", "app_id", "device_id", "owner_org"],
    primaryKey: "id"
  },
  {
    name: "apps",
    columns: ["id", "app_id", "icon_url", "user_id", "name", "last_version", "retention", "owner_org", "default_upload_channel", "transfer_history"],
    primaryKey: "id"
  },
  {
    name: "orgs",
    columns: ["id", "created_by", "logo", "name", "management_email", "customer_id"],
    primaryKey: "id"
  },
  {
    name: "stripe_info",
    columns: ["id", "customer_id", "status", "trial_at", "is_good_plan", "mau_exceeded", "storage_exceeded", "bandwidth_exceeded"],
    primaryKey: "id"
  }
];

interface DBSync {
  db: D1Database;
  region: string;
}

const TABLE_SCHEMAS = {
  app_versions: appVersionsSchema,
  channels: channelsSchema,
  channel_devices: channelDevicesSchema,
  apps: appsSchema,
  orgs: orgsSchema,
  stripe_info: stripeInfoSchema,
  sync_state: syncStateSchema,
  sync_lock: syncLockSchema
} as const;

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    const url = new URL(request.url);
    
    if (url.pathname === "/webhook") {
      return handleWebhook(request, env);
    }

    if (url.pathname === "/nuke") {
      return handleNuke(request, env);
    }
    
    return new Response("Not found", { status: 404 });
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(handleSync(env));
  }
};

async function checkAndCreateTables(db: D1Database, region: string) {
  const start = Date.now();
  try {
    // Check each table with a simple SELECT
    for (const table of TABLES) {
      try {
        // Try to select from the table
        await db.prepare(`SELECT 1 FROM ${table.name} LIMIT 1`).first();
      } catch (error) {
        // If table doesn't exist, create it
        if (error instanceof Error && error.message.includes('no such table')) {
          await db.exec(TABLE_SCHEMAS[table.name as keyof typeof TABLE_SCHEMAS]);
        } else {
          throw error;
        }
      }
    }
    console.log(`[${region}] Tables checked/created in ${Date.now() - start}ms`);
  } catch (error) {
    console.error(`[${region}] Error initializing database:`, error);
    throw error;
  }
}

async function handleWebhook(request: Request, env: Env) {
  if (request.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  const signature = request.headers.get("x-webhook-signature");
  if (!signature || signature !== env.WEBHOOK_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }

  // Start sync in background
  const syncPromise = handleSync(env);
  return new Response("Sync started", { status: 200 });
}

function handleMessage(msg: any, table: TableSchema) {
  const { headers, value } = msg;
  const columns = table.columns.filter(col => col !== table.primaryKey);
  const values = columns.map(col => value[col]);

  switch (headers.operation) {
    case "insert":
      return {
        sql: `INSERT OR REPLACE INTO ${table.name} (${table.columns.join(", ")}) VALUES (${table.columns.map(() => "?").join(", ")})`,
        params: [value[table.primaryKey], ...values],
      };
    case "update":
      return {
        sql: `UPDATE ${table.name} SET ${columns.map(col => `${col} = ?`).join(", ")} WHERE ${table.primaryKey} = ?`,
        params: [...values, value[table.primaryKey]],
      };
    case "delete":
      return {
        sql: `DELETE FROM ${table.name} WHERE ${table.primaryKey} = ?`,
        params: [value[table.primaryKey]],
      };
  }
}

async function syncTable(db: D1Database, table: TableSchema, region: string, env: Env) {
  const start = Date.now();
  const lastKnownOffset = await getLastOffset(db, table.name);
  
  console.log(`[${region}] Starting sync for table ${table.name} from offset ${lastKnownOffset || 'beginning'}`);
  
  const stream = new ShapeStream({
    url: env.ELECTRIC_URL,
    params: {
      table: table.name,
      replica: "full",
      source_id: env.ELECTRIC_SOURCE_ID,
      source_secret: env.ELECTRIC_SOURCE_SECRET,
    },
    subscribe: false,
    offset: lastKnownOffset,
  });

  let currentBatch: any[] = [];
  let finalOffset: Offset | undefined;

  try {
    await new Promise<void>((resolve, reject) => {
      stream.subscribe(
        async (messages) => {
          for (const msg of messages) {
            if (isChangeMessage(msg)) {
              currentBatch.push(handleMessage(msg, table));
            }
            else if (isControlMessage(msg) && msg.headers.control === "up-to-date") {
              finalOffset = stream.lastOffset;
              resolve();
            }
          }
        },
        (error: Error) => {
          reject(error);
        }
      );
    });

    // Apply changes and save offset only if we have changes
    if (currentBatch.length > 0) {
      console.log(`[${region}] Applying ${currentBatch.length} changes to table ${table.name}`);
      await db.batch(currentBatch);
      if (finalOffset) {
        await db.prepare("INSERT OR REPLACE INTO sync_state (table_name, last_offset) VALUES (?, ?)")
          .bind(table.name, finalOffset)
          .run();
      }
      console.log(`[${region}] Table ${table.name} synced in ${Date.now() - start}ms`);
    } else {
      console.log(`[${region}] No changes for table ${table.name}`);
    }
  } catch (error) {
    console.error(`[${region}] Error syncing table ${table.name}:`, error);
    throw error;
  }
}

async function syncDatabase(db: D1Database, region: string, env: Env) {
  const start = Date.now();
  console.log(`[${region}] Starting database sync`);
  
  for (const table of TABLES) {
    // Try to acquire lock for this table
    const hasLock = await acquireLock(db, table.name);
    if (!hasLock) {
      console.log(`[${region}] Another sync is already running for table ${table.name}, skipping`);
      continue;
    }

    try {
      await syncTable(db, table, region, env);
    } finally {
      // Always release the lock
      await releaseLock(db, table.name);
    }
  }
  
  console.log(`[${region}] Database sync completed in ${Date.now() - start}ms`);
}

async function acquireLock(db: D1Database, tableName: string): Promise<boolean> {
  const now = new Date().toISOString();
  try {
    // Try to insert a lock record
    await db.prepare(`
      INSERT INTO sync_lock (id, locked_at, locked_by)
      VALUES (?, ?, ?)
    `).bind(tableName, now, 'worker').run();
    return true;
  } catch (error) {
    // If insert fails, check if lock is stale (older than 5 minutes)
    const lock = await db.prepare(`
      SELECT locked_at FROM sync_lock WHERE id = ?
    `).bind(tableName).first() as { locked_at: string } | undefined;
    
    if (lock) {
      const lockTime = new Date(lock.locked_at);
      const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
      
      if (lockTime < fiveMinutesAgo) {
        // Lock is stale, try to update it
        await db.prepare(`
          UPDATE sync_lock 
          SET locked_at = ?, locked_by = ?
          WHERE id = ?
        `).bind(now, 'worker', tableName).run();
        return true;
      }
    }
    return false;
  }
}

async function releaseLock(db: D1Database, tableName: string): Promise<void> {
  await db.prepare(`
    DELETE FROM sync_lock WHERE id = ?
  `).bind(tableName).run();
}

async function handleSync(env: Env) {
  const start = Date.now();
  console.log('Starting global sync');
  
  const dbs: DBSync[] = [
    { db: env.DB_ENA, region: "ENA" },
    { db: env.DB_WNA, region: "WNA" },
    { db: env.DB_WEU, region: "WEU" },
    { db: env.DB_EEU, region: "EEU" },
    { db: env.DB_ASIA, region: "ASIA" },
    { db: env.DB_OC, region: "OC" },
  ];

  try {
    // Initialize all databases
    await Promise.all(dbs.map(({ db, region }) => checkAndCreateTables(db, region)));
    
    // Sync all databases in parallel
    await Promise.all(dbs.map(({ db, region }) => syncDatabase(db, region, env)));
    
    console.log(`Global sync completed in ${Date.now() - start}ms`);
  } catch (error) {
    console.error('Error during sync:', error);
    throw error;
  }
}

async function getLastOffset(db: D1Database, tableName: string): Promise<Offset | undefined> {
  const result = await db.prepare("SELECT last_offset FROM sync_state WHERE table_name = ?").bind(tableName).first();
  return result?.last_offset as Offset | undefined;
}

async function handleNuke(request: Request, env: Env) {
  if (request.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  const signature = request.headers.get("x-webhook-signature");
  if (!signature || signature !== env.WEBHOOK_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }

  const body = await request.json() as NukeRequest;
  const dbs: DBSync[] = [
    { db: env.DB_ENA, region: "ENA" },
    { db: env.DB_WNA, region: "WNA" },
    { db: env.DB_WEU, region: "WEU" },
    { db: env.DB_EEU, region: "EEU" },
    { db: env.DB_ASIA, region: "ASIA" },
    { db: env.DB_OC, region: "OC" },
  ];

  try {
    // First, try to acquire locks on all tables in all databases
    const lockPromises = dbs.flatMap(({ db, region }) => 
      TABLES.map(async (table) => {
        const hasLock = await acquireLock(db, table.name);
        if (!hasLock) {
          throw new Error(`Cannot acquire lock for table ${table.name} in ${region}`);
        }
        return { db, region, table };
      })
    );

    // Wait for all locks to be acquired
    await Promise.all(lockPromises);

    // Now proceed with nuking
    switch (body.type) {
      case 'all':
        await Promise.all(dbs.map(({ db, region }) => nukeDatabase(db, region)));
        return new Response("All databases nuked", { status: 200 });

      case 'db':
        if (!body.db) {
          return new Response("Database name required", { status: 400 });
        }
        const db = dbs.find(d => d.region.toLowerCase() === body.db?.toLowerCase());
        if (!db) {
          return new Response("Database not found", { status: 404 });
        }
        await nukeDatabase(db.db, db.region);
        return new Response(`Database ${db.region} nuked`, { status: 200 });

      case 'table':
        if (!body.db || !body.table) {
          return new Response("Database and table names required", { status: 400 });
        }
        const targetDb = dbs.find(d => d.region.toLowerCase() === body.db?.toLowerCase());
        if (!targetDb) {
          return new Response("Database not found", { status: 404 });
        }
        if (!TABLES.find(t => t.name === body.table)) {
          return new Response("Table not found", { status: 404 });
        }
        await nukeTable(targetDb.db, body.table, targetDb.region);
        return new Response(`Table ${body.table} nuked from ${targetDb.region}`, { status: 200 });

      default:
        return new Response("Invalid nuke type", { status: 400 });
    }
  } catch (error) {
    console.error("Error during nuke operation:", error);
    return new Response(error instanceof Error ? error.message : "Internal server error", { status: 500 });
  } finally {
    // Release all locks
    await Promise.all(dbs.flatMap(({ db, region }) => 
      TABLES.map(table => releaseLock(db, table.name))
    ));
  }
}

async function nukeDatabase(db: D1Database, region: string) {
  console.log(`[${region}] Nuking database`);
  for (const table of TABLES) {
    await nukeTable(db, table.name, region);
  }
  // Clear sync state
  await db.prepare("DELETE FROM sync_state").run();
  await db.prepare("DELETE FROM sync_lock").run();
  console.log(`[${region}] Database nuked`);
}

async function nukeTable(db: D1Database, tableName: string, region: string) {
  console.log(`[${region}] Nuking table ${tableName}`);
  // Drop the table and recreate it
  await db.prepare(`DROP TABLE IF EXISTS ${tableName}`).run();
  await db.exec(TABLE_SCHEMAS[tableName as keyof typeof TABLE_SCHEMAS]);
  await db.prepare("DELETE FROM sync_state WHERE table_name = ?").bind(tableName).run();
  console.log(`[${region}] Table ${tableName} nuked`);
}
