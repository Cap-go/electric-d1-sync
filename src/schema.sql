-- Tables
CREATE TABLE IF NOT EXISTS app_versions (
    id INTEGER PRIMARY KEY,
    owner_org TEXT,
    app_id TEXT,
    name TEXT,
    r2_path TEXT,
    user_id TEXT,
    deleted BOOLEAN,
    external_url TEXT,
    checksum TEXT,
    session_key TEXT,
    storage_provider TEXT,
    min_update_version TEXT,
    manifest JSON
);

CREATE TABLE IF NOT EXISTS channels (
    id INTEGER PRIMARY KEY,
    name TEXT,
    app_id TEXT,
    version INTEGER,
    created_by TEXT,
    owner_org TEXT,
    public BOOLEAN,
    disable_auto_update_under_native BOOLEAN,
    disable_auto_update TEXT,
    ios BOOLEAN,
    android BOOLEAN,
    allow_device_self_set BOOLEAN,
    allow_emulator BOOLEAN,
    allow_dev BOOLEAN
);

CREATE TABLE IF NOT EXISTS channel_devices (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER,
    app_id TEXT,
    device_id TEXT,
    owner_org TEXT
);

CREATE TABLE IF NOT EXISTS apps (
    id TEXT PRIMARY KEY,
    app_id TEXT,
    icon_url TEXT,
    user_id TEXT,
    name TEXT,
    last_version TEXT,
    retention INTEGER,
    owner_org TEXT,
    default_upload_channel TEXT,
    transfer_history TEXT
);

CREATE TABLE IF NOT EXISTS orgs (
    id TEXT PRIMARY KEY,
    created_by TEXT,
    logo TEXT,
    name TEXT,
    management_email TEXT,
    customer_id TEXT
);

CREATE TABLE IF NOT EXISTS stripe_info (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    status TEXT,
    trial_at TEXT,
    is_good_plan BOOLEAN,
    mau_exceeded BOOLEAN,
    storage_exceeded BOOLEAN,
    bandwidth_exceeded BOOLEAN
);

CREATE TABLE IF NOT EXISTS sync_state (
    table_name TEXT PRIMARY KEY,
    last_offset TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_lock (
    id INTEGER PRIMARY KEY,
    locked_at TIMESTAMP NOT NULL,
    locked_by TEXT NOT NULL
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_app_versions_id ON app_versions(id);
CREATE INDEX IF NOT EXISTS idx_app_versions_name ON app_versions(name);
CREATE INDEX IF NOT EXISTS idx_app_versions_app_id ON app_versions(app_id);
CREATE INDEX IF NOT EXISTS idx_app_versions_owner_org ON app_versions(owner_org);
CREATE INDEX IF NOT EXISTS idx_app_versions_r2_path ON app_versions(r2_path);
CREATE INDEX IF NOT EXISTS idx_app_versions_external_url ON app_versions(external_url);

CREATE INDEX IF NOT EXISTS idx_channels_id ON channels(id);
CREATE INDEX IF NOT EXISTS idx_channels_app_id_public_android_ios ON channels(app_id, public, android, ios);
CREATE INDEX IF NOT EXISTS idx_channels_app_id_name ON channels(app_id, name);
CREATE INDEX IF NOT EXISTS idx_channels_version ON channels(version);
CREATE INDEX IF NOT EXISTS idx_channels_owner_org ON channels(owner_org);

CREATE INDEX IF NOT EXISTS idx_channel_devices_id ON channel_devices(id);
CREATE INDEX IF NOT EXISTS idx_channel_devices_device_id_app_id ON channel_devices(device_id, app_id);
CREATE INDEX IF NOT EXISTS idx_channel_devices_channel_id ON channel_devices(channel_id);

CREATE INDEX IF NOT EXISTS idx_apps_id ON apps(id);
CREATE INDEX IF NOT EXISTS idx_apps_app_id ON apps(app_id);
CREATE INDEX IF NOT EXISTS idx_apps_owner_org ON apps(owner_org);
CREATE INDEX IF NOT EXISTS idx_apps_user_id ON apps(user_id);

CREATE INDEX IF NOT EXISTS idx_orgs_id ON orgs(id);
CREATE INDEX IF NOT EXISTS idx_orgs_created_by ON orgs(created_by);
CREATE INDEX IF NOT EXISTS idx_orgs_customer_id ON orgs(customer_id);

CREATE INDEX IF NOT EXISTS idx_stripe_info_customer_id ON stripe_info(customer_id);
CREATE INDEX IF NOT EXISTS idx_stripe_info_status_is_good_plan ON stripe_info(status, is_good_plan);
CREATE INDEX IF NOT EXISTS idx_stripe_info_trial_at ON stripe_info(trial_at);
CREATE INDEX IF NOT EXISTS idx_stripe_info_exceeded ON stripe_info(mau_exceeded, storage_exceeded, bandwidth_exceeded); 
