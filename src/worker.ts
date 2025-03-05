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

interface SyncRequest {
  region: string;
  table: string;
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
    
    if (url.pathname === "/nuke") {
      return handleNuke(request, env);
    }
    
    if (url.pathname === "/sync") {
      return handleSyncRequest(request, env, ctx);
    }
    
    return new Response("Not found", { status: 404 });
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    try {
      // Initialize all databases first
      const dbs: DBSync[] = [
        { db: env.DB_ENA, region: "ENA" },
        { db: env.DB_WNA, region: "WNA" },
        { db: env.DB_WEU, region: "WEU" },
        { db: env.DB_EEU, region: "EEU" },
        { db: env.DB_ASIA, region: "ASIA" },
        { db: env.DB_OC, region: "OC" },
      ];
      
      await Promise.all(dbs.map(({ db, region }) => checkAndCreateTables(db, region)));
      
      // Trigger individual sync requests for each table and database
      const promises = [];
      
      // Get the worker URL from the cron event
      const workerUrl = new URL("https://electric-d1-sync.workers.dev");
      
      for (const { region } of dbs) {
        for (const table of TABLES) {
          // Use fetch to call our own /sync endpoint
          const syncUrl = new URL(workerUrl);
          syncUrl.pathname = "/sync";
          
          const req = new Request(syncUrl.toString(), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "x-webhook-signature": env.WEBHOOK_SECRET,
            },
            body: JSON.stringify({ region, table: table.name }),
          });
          
          // Execute the fetch but don't wait for it (fire and forget)
          const promise = fetch(req).then(async (resp) => {
            if (!resp.ok) {
              const body = await resp.text();
              console.error(`Error syncing ${table.name} in ${region} status: ${resp.status} ${body}`);
            }
          }).catch(err => {
            console.error(`Error triggering sync for ${table.name} in ${region}:`, err);
          });
          
          promises.push(promise);
        }
      }
      
      // Wait for all triggers to be sent (not for them to complete)
      await Promise.all(promises);
      console.log(`Triggered sync for all tables and databases`);
    } catch (error) {
      console.error("Error during scheduled sync:", error);
    }
  }
};

async function checkAndCreateTables(db: D1Database, region: string) {
  const start = Date.now();
  try {
    // Check each table with a simple SELECT
    for (const table of Object.keys(TABLE_SCHEMAS)) {
      try {
        // Try to select from the table
        await db.prepare(`SELECT 1 FROM ${table} LIMIT 1`).first();
      } catch (error) {
        // If table doesn't exist, create it
        if (error instanceof Error && error.message.includes('no such table')) {
          const schema = TABLE_SCHEMAS[table as keyof typeof TABLE_SCHEMAS];
          console.log(`[${region}] Creating table ${table} ${schema}`);
          await db.exec(schema);
        } else {
          console.error(`[${region}] Error checking/creating table ${table}:`, error);
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

function handleMessage(msg: any, table: TableSchema) {
  const { headers, value } = msg;
  const columns = table.columns.filter(col => col !== table.primaryKey);
  // fix bigint issue with D1
  const values = columns.map(col => typeof value[col] === 'bigint' ? Number(value[col]) : value[col]);

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
  const tableName = table.name;
  
  // Try to acquire lock for this table
  const hasLock = await acquireLock(db, tableName);
  if (!hasLock) {
    console.log(`[${region}] Another sync is already running for table ${tableName}, skipping`);
    return;
  }
  
  try {
    const lastKnownOffset = await getLastOffset(db, tableName);
    
    console.log(`[${region}] Starting sync for table ${tableName} from offset ${lastKnownOffset || 'beginning'}`);
    
    const stream = new ShapeStream({
      url: env.ELECTRIC_URL,
      params: {
        table: tableName,
        replica: "full",
        source_id: env.ELECTRIC_SOURCE_ID,
        source_secret: env.ELECTRIC_SOURCE_SECRET,
      },
      subscribe: false,
      offset: lastKnownOffset,
    });

    let currentBatch: any[] = [];
    let finalOffset: Offset | undefined;

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
      console.log(`[${region}] Applying ${currentBatch.length} changes to table ${tableName}`);
      await db.batch(currentBatch);
      if (finalOffset) {
        await db.prepare("INSERT OR REPLACE INTO sync_state (table_name, last_offset) VALUES (?, ?)")
          .bind(tableName, finalOffset)
          .run();
      }
      console.log(`[${region}] Table ${tableName} synced in ${Date.now() - start}ms`);
    } else {
      console.log(`[${region}] No changes for table ${tableName}`);
    }
  } catch (error) {
    console.error(`[${region}] Error syncing table ${tableName}:`, error);
    throw error;
  } finally {
    // Always release the lock
    try {
      await releaseLock(db, tableName);
      console.log(`[${region}] Released lock for ${tableName}`);
    } catch (error) {
      console.error(`[${region}] Error releasing lock for ${tableName}:`, error);
    }
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
    } catch (error) {
      console.error(`[${region}] Error syncing table ${table.name}:`, error);
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

async function handleSyncRequest(request: Request, env: Env, ctx: ExecutionContext) {
  // Validate request method
  if (request.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }
  
  // Validate signature
  const signature = request.headers.get("x-webhook-signature");
  if (!signature || signature !== env.WEBHOOK_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }
  
  try {
    // Parse request body
    const body = await request.json() as SyncRequest;
    const { region, table: tableName } = body;
    
    // Validate region
    const dbMap: Record<string, D1Database> = {
      "ENA": env.DB_ENA,
      "WNA": env.DB_WNA,
      "WEU": env.DB_WEU,
      "EEU": env.DB_EEU,
      "ASIA": env.DB_ASIA,
      "OC": env.DB_OC,
    };
    
    const db = dbMap[region];
    if (!db) {
      return new Response(`Invalid region: ${region}`, { status: 400 });
    }
    
    // Validate table
    const table = TABLES.find(t => t.name === tableName);
    if (!table) {
      return new Response(`Invalid table: ${tableName}`, { status: 400 });
    }
    
    // Run the sync in the background
    ctx.waitUntil(
      syncTable(db, table, region, env)
        .catch(error => {
          console.error(`Error syncing ${tableName} in ${region} Error: ${error}`);
        })
    );
    
    // Return success immediately while the sync continues in the background
    return new Response(`Sync started for ${tableName} in ${region}`, { status: 202 });
  } catch (error) {
    console.error("Error handling sync request:", error);
    return new Response(error instanceof Error ? error.message : "Internal server error", { status: 500 });
  }
}
