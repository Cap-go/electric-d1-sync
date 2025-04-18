import { sqltag } from 'sql-template-tag';
import type { D1Database } from '@cloudflare/workers-types';

export type SQLiteType = 'INTEGER' | 'TEXT' | 'BOOLEAN' | 'JSON';

// Single source of truth for table definitions
export interface TableDefinition {
  primaryKey: string;
  columns: Record<string, SQLiteType>;
}

// Add UUID columns list
export const UUID_COLUMNS = new Set([
  'id',
  'owner_org',
  'user_id',
  'created_by',
  'device_id',
]);

// Table definitions - single source of truth
export const TABLE_DEFINITIONS: Record<string, TableDefinition> = {
    app_versions: {
    primaryKey: 'id',
    columns: {
      id: 'INTEGER',
      owner_org: 'TEXT',
      app_id: 'TEXT',
      name: 'TEXT',
      r2_path: 'TEXT',
      user_id: 'TEXT',
      deleted: 'BOOLEAN',
      external_url: 'TEXT',
      checksum: 'TEXT',
      session_key: 'TEXT',
      storage_provider: 'TEXT',
      min_update_version: 'TEXT',
    }
    },
    channels: {
    primaryKey: 'id',
    columns: {
      id: 'INTEGER',
      name: 'TEXT',
      app_id: 'TEXT',
      version: 'INTEGER',
      created_by: 'TEXT',
      owner_org: 'TEXT',
      public: 'BOOLEAN',
      disable_auto_update_under_native: 'BOOLEAN',
      disable_auto_update: 'TEXT',
      ios: 'BOOLEAN',
      android: 'BOOLEAN',
      allow_device_self_set: 'BOOLEAN',
      allow_emulator: 'BOOLEAN',
      allow_dev: 'BOOLEAN',
    }
    },
    channel_devices: {
    primaryKey: 'id',
    columns: {
      id: 'INTEGER',
      channel_id: 'INTEGER',
      app_id: 'TEXT',
      device_id: 'TEXT',
      owner_org: 'TEXT',
    }
    },
    apps: {
    primaryKey: 'id',
    columns: {
      id: 'TEXT',
      app_id: 'TEXT',
      icon_url: 'TEXT',
      user_id: 'TEXT',
      name: 'TEXT',
      last_version: 'TEXT',
      retention: 'INTEGER',
      owner_org: 'TEXT',
      default_upload_channel: 'TEXT',
      transfer_history: 'JSON',
    }
    },
    orgs: {
    primaryKey: 'id',
    columns: {
      id: 'TEXT',
      created_by: 'TEXT',
      logo: 'TEXT',
      name: 'TEXT',
      management_email: 'TEXT',
      customer_id: 'TEXT',
    }
    },
    stripe_info: {
    primaryKey: 'id',
    columns: {
      id: 'INTEGER',
      customer_id: 'TEXT',
      status: 'TEXT',
      trial_at: 'TEXT',
      is_good_plan: 'BOOLEAN',
      mau_exceeded: 'BOOLEAN',
      storage_exceeded: 'BOOLEAN',
      bandwidth_exceeded: 'BOOLEAN',
    },
  },
  manifest: {
    primaryKey: 'id',
    columns: {
      id: 'INTEGER',
      app_version_id: 'INTEGER',
      file_name: 'TEXT',
      s3_path: 'TEXT',
      file_hash: 'TEXT',
    }
  }
};

// Derived type for compatibility with existing code
export interface TableSchema {
  name: string;
  columns: string[];
  primaryKey: string;
}

// Generate TABLES array from definitions
export const TABLES: TableSchema[] = Object.entries(TABLE_DEFINITIONS).map(([name, def]) => ({
  name,
  columns: Object.keys(def.columns),
  primaryKey: def.primaryKey
}));

// Generate TABLE_SCHEMAS_TYPES from definitions
export const TABLE_SCHEMAS_TYPES: Record<string, Record<string, SQLiteType>> = 
  Object.fromEntries(
    Object.entries(TABLE_DEFINITIONS).map(([name, def]) => [name, def.columns])
  );

// Function to generate CREATE TABLE SQL for a table definition
function generateCreateTableSQL(tableName: string, tableDef: TableDefinition): string {
  const columns = Object.entries(tableDef.columns)
    .map(([colName, colType]) => {
      // Map from our type system to SQLite types
      let sqlType: string;
      switch (colType) {
        case 'INTEGER':
          sqlType = 'INTEGER';
          break;
        case 'BOOLEAN':
          sqlType = 'BOOLEAN';
          break;
        case 'JSON':
          sqlType = 'TEXT'; // SQLite stores JSON as TEXT
          break;
        case 'TEXT':
        default:
          sqlType = 'TEXT';
          break;
      }
      
      const isPrimary = colName === tableDef.primaryKey ? ' PRIMARY KEY' : '';
      return `    ${colName} ${sqlType}${isPrimary}`;
    })
    .join(',\n');

  return `CREATE TABLE IF NOT EXISTS ${tableName} (\n${columns}\n);`;
}

// Function to generate indexes for a table
function generateIndexesSQL(tableName: string, tableDef: TableDefinition): string[] {
  const indexes = [];
  
  // Add primary key index
  indexes.push(`CREATE INDEX IF NOT EXISTS idx_${tableName}_${tableDef.primaryKey} ON ${tableName}(${tableDef.primaryKey});`);
  
  // Add additional indexes based on column semantics
  const indexableColumns = ['app_id', 'owner_org', 'user_id', 'customer_id', 'created_by', 'device_id', 'channel_id', 'r2_path', 'external_url', 'name'];
  
  for (const col of indexableColumns) {
    if (col in tableDef.columns && col !== tableDef.primaryKey) {
      indexes.push(`CREATE INDEX IF NOT EXISTS idx_${tableName}_${col} ON ${tableName}(${col});`);
    }
  }
  
  // Special case for stripe_info table
  if (tableName === 'stripe_info') {
    indexes.push(`CREATE INDEX IF NOT EXISTS idx_stripe_info_status_is_good_plan ON stripe_info(status, is_good_plan);`);
    indexes.push(`CREATE INDEX IF NOT EXISTS idx_stripe_info_trial_at ON stripe_info(trial_at);`);
    indexes.push(`CREATE INDEX IF NOT EXISTS idx_stripe_info_exceeded ON stripe_info(mau_exceeded, storage_exceeded, bandwidth_exceeded);`);
  }
  
  return indexes;
}

// Generate SQL schema statements
export const appVersionsSchema = `${generateCreateTableSQL('app_versions', TABLE_DEFINITIONS.app_versions)}

${generateIndexesSQL('app_versions', TABLE_DEFINITIONS.app_versions).join('\n')}`;

export const channelsSchema = `${generateCreateTableSQL('channels', TABLE_DEFINITIONS.channels)}

${generateIndexesSQL('channels', TABLE_DEFINITIONS.channels).join('\n')}`;

export const channelDevicesSchema = `${generateCreateTableSQL('channel_devices', TABLE_DEFINITIONS.channel_devices)}

${generateIndexesSQL('channel_devices', TABLE_DEFINITIONS.channel_devices).join('\n')}`;

export const appsSchema = `${generateCreateTableSQL('apps', TABLE_DEFINITIONS.apps)}

${generateIndexesSQL('apps', TABLE_DEFINITIONS.apps).join('\n')}`;

export const orgsSchema = `${generateCreateTableSQL('orgs', TABLE_DEFINITIONS.orgs)}

${generateIndexesSQL('orgs', TABLE_DEFINITIONS.orgs).join('\n')}`;

export const stripeInfoSchema = `${generateCreateTableSQL('stripe_info', TABLE_DEFINITIONS.stripe_info)}

${generateIndexesSQL('stripe_info', TABLE_DEFINITIONS.stripe_info).join('\n')}`;

export const manifestSchema = `${generateCreateTableSQL('manifest', TABLE_DEFINITIONS.manifest)}

${generateIndexesSQL('manifest', TABLE_DEFINITIONS.manifest).join('\n')}`;

export const syncStateSchema = `CREATE TABLE IF NOT EXISTS sync_state (
    table_name TEXT PRIMARY KEY,
    last_offset TEXT NOT NULL,
    shape_handle TEXT
);`;

export const syncLockSchema = `CREATE TABLE IF NOT EXISTS sync_lock (
    table_name TEXT PRIMARY KEY,
    locked_at INTEGER NOT NULL,
    lock_id TEXT NOT NULL
);`;

// Group all schemas for easy access
export const TABLE_SCHEMAS = {
  app_versions: appVersionsSchema,
  channels: channelsSchema,
  channel_devices: channelDevicesSchema,
  apps: appsSchema,
  orgs: orgsSchema,
  stripe_info: stripeInfoSchema,
  manifest: manifestSchema,
  sync_state: syncStateSchema,
  sync_lock: syncLockSchema
} as const;
  