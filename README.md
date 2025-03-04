# electric-d1-sync
Sync postgres table to d1 db multiple region

# Basic Sync from Postgres to D1 using ElectricSQL

## Overview

This is a basic example of how to sync data from Postgres to Cloudflare D1 read replicas across multiple regions using ElectricSQL. The sync worker runs every minute as a scheduled task and can also be triggered via webhook when changes occur in the database.

## Setup

1. Clone the repository
2. Run `npm install` to install the dependencies
3. Copy `.env.example` to `.env` and fill in your environment variables
4. Create D1 databases for each region:
   ```bash
   wrangler d1 create electric-sync-na  # North America
   wrangler d1 create electric-sync-eu  # Europe
   wrangler d1 create electric-sync-asia # Asia
   wrangler d1 create electric-sync-sa  # South America
   wrangler d1 create electric-sync-oc  # Oceania
   wrangler d1 create electric-sync-af  # Africa
   ```
5. Update `wrangler.toml` with your D1 database IDs and set a secure `WEBHOOK_SECRET`
6. Deploy the worker:
   ```bash
   wrangler deploy
   ```

## How it Works

The sync process runs in two ways:

1. **Scheduled Sync**: Every minute, the worker automatically checks for changes
2. **Webhook Sync**: When changes occur in the database, it can trigger an immediate sync

Each sync:
1. Retrieves the last known offset from the `sync_state` table
2. Connects to ElectricSQL and streams changes since the last offset
3. Batches the changes and applies them to all D1 databases in parallel
4. Saves the new offset for the next sync

The sync will automatically stop after 25 seconds (before the worker timeout) or when it receives an "up-to-date" message from ElectricSQL.

## Webhook Integration

To trigger an immediate sync when changes occur, send a POST request to the `/webhook` endpoint with the webhook secret in the header:

```bash
curl -X POST https://your-worker.workers.dev/webhook \
  -H "x-webhook-signature: your-secret-here"
```

You can set up database triggers or application hooks to call this endpoint whenever relevant tables change.

## Nuke Webhook

To clear data from replicas, use the `/nuke` endpoint with different options:

### Nuke All Databases
```bash
curl -X POST https://your-worker.workers.dev/nuke \
  -H "x-webhook-signature: your-secret-here" \
  -H "Content-Type: application/json" \
  -d '{"type": "all"}'
```

### Nuke Specific Database
```bash
curl -X POST https://your-worker.workers.dev/nuke \
  -H "x-webhook-signature: your-secret-here" \
  -H "Content-Type: application/json" \
  -d '{"type": "db", "db": "electric-sync-eu"}'
```

### Nuke Specific Table
```bash
curl -X POST https://your-worker.workers.dev/nuke \
  -H "x-webhook-signature: your-secret-here" \
  -H "Content-Type: application/json" \
  -d '{"type": "table", "db": "electric-sync-eu", "table": "apps"}'
```

## Stopping the Sync

To stop the sync:

1. Remove the cron trigger from `wrangler.toml`
2. Redeploy the worker

## Resetting the Sync

To reset the sync:

1. Delete the data from all D1 databases
2. Restart the sync process
