require("dotenv").config(); // v2
const express    = require("express");
const { Pool }   = require("pg");
const { google } = require("googleapis");
const { Resend } = require("resend");
const PDFDocument = require("pdfkit");
const { v4: uuidv4 } = require("uuid");
const path       = require("path");
const cron       = require("node-cron");
const webpush    = require("web-push");
const OAuthClient = require("intuit-oauth");
const QuickBooks  = require("node-quickbooks");

const app  = express();
app.use(express.json({ limit: "10mb" }));

// Serve index.html with injected brand config (before static middleware)
const fs = require("fs");
let _indexHtmlCache = null;
app.get(["/", "/index.html"], (req, res) => {
  if (!_indexHtmlCache) {
    _indexHtmlCache = fs.readFileSync(path.join(__dirname, "public", "index.html"), "utf-8");
  }
  const brandScript = `<script>window.__BRAND=${JSON.stringify({
    name: CONFIG.BRAND_NAME,
    sub: CONFIG.BRAND_SUB,
    site: CONFIG.BRAND_SITE,
    logoUrl: CONFIG.BRAND_LOGO_URL,
    colorPrimary: CONFIG.BRAND_COLOR_PRIMARY,
    colorAccent: CONFIG.BRAND_COLOR_ACCENT,
    colorDarkBg: CONFIG.BRAND_COLOR_DARK_BG,
    defaultTheme: CONFIG.BRAND_DEFAULT_THEME,
    demoMode: CONFIG.DEMO_MODE,
  })};</script>`;
  const html = _indexHtmlCache.replace("<head>", "<head>\n  " + brandScript);
  res.type("html").send(html);
});

// Dynamic manifest.json
app.get("/manifest.json", (req, res) => {
  res.json({
    name: CONFIG.BRAND_NAME,
    short_name: CONFIG.BRAND_NAME,
    start_url: "/",
    display: "standalone",
    background_color: CONFIG.BRAND_COLOR_DARK_BG,
    theme_color: CONFIG.BRAND_COLOR_DARK_BG,
    icons: [{ src: CONFIG.BRAND_LOGO_URL, sizes: "512x512", type: "image/png" }]
  });
});

app.use(express.static(path.join(__dirname, "public")));

// ─────────────────────────────────────────
//  CONFIG
// ─────────────────────────────────────────
const CONFIG = {
  PAY_PERIOD_ANCHOR: process.env.PAY_PERIOD_ANCHOR || "2026-03-02",
  PAY_PERIOD_DAYS:   14,
  PAY_PERIOD_HISTORY_COUNT: 8,

  BRAND_NAME:           process.env.BRAND_NAME           || "Kronara",
  BRAND_SUB:            process.env.BRAND_SUB            || "Time. Teams. Simplified.",
  BRAND_SITE:           process.env.BRAND_SITE           || "https://kronara.app",
  BRAND_LOGO_URL:       process.env.BRAND_LOGO_URL       || "/logo.png",
  BRAND_COLOR_PRIMARY:  process.env.BRAND_COLOR_PRIMARY  || "#6C5CE7",
  BRAND_COLOR_ACCENT:   process.env.BRAND_COLOR_ACCENT   || "#E8832A",
  BRAND_COLOR_DARK_BG:  process.env.BRAND_COLOR_DARK_BG  || "#0A0A1A",
  BRAND_DEFAULT_THEME:  process.env.BRAND_DEFAULT_THEME  || "kronara",
  DEMO_MODE:            process.env.DEMO_MODE === "true",

  FORGOT_PIN_COOLDOWN_SECONDS: 300,
  LOGIN_MAX_ATTEMPTS:    5,
  LOGIN_LOCKOUT_SECONDS: 1800,  // 30 minutes
  LOGIN_ATTEMPT_WINDOW:  1800,

  // Set to true to re-enable per-user approval checkboxes on payroll tab
  APPROVAL_SYSTEM_ENABLED: false,
};

// Demo mode guard — prevents deletion of seed data
async function demoProtected(tableName, idCol, idVal, tid) {
  if (!CONFIG.DEMO_MODE) return false;
  const r = await query(`SELECT is_demo FROM ${tableName} WHERE ${idCol}=$1 AND tenant_id=$2`, [idVal, tid]);
  return r.rows.length && r.rows[0].is_demo === true;
}
const DEMO_BLOCK_MSG = "This is demo data and cannot be deleted.";

// ─────────────────────────────────────────
//  WEB PUSH (VAPID)
// ─────────────────────────────────────────
try {
  if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
    const vapidSubject = process.env.VAPID_SUBJECT || "mailto:support@kronara.app";
    const subject = vapidSubject.startsWith("mailto:") ? vapidSubject : `mailto:${vapidSubject}`;
    webpush.setVapidDetails(subject, process.env.VAPID_PUBLIC_KEY, process.env.VAPID_PRIVATE_KEY);
    console.log("Web Push: VAPID configured ✅");
  } else {
    console.log("Web Push: VAPID keys not set — push notifications disabled");
  }
} catch (err) {
  console.error("Web Push: VAPID setup failed —", err.message, "— push notifications disabled");
}

// ─────────────────────────────────────────
//  POSTGRES CONNECTION
// ─────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes("railway")
    ? { rejectUnauthorized: false }
    : false,
});

async function query(sql, params = [], retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    let client;
    try {
      client = await pool.connect();
      const res = await client.query(sql, params);
      return res;
    } catch (err) {
      const isTransient = err.code === 'EAI_AGAIN' || err.code === 'ENOTFOUND' ||
                          err.code === 'ECONNREFUSED' || err.code === 'ECONNRESET' ||
                          err.message?.includes('EAI_AGAIN');
      if (isTransient && attempt < retries) {
        console.warn(`DB connection attempt ${attempt}/${retries} failed (${err.code || err.message}), retrying in ${attempt * 2}s...`);
        await new Promise(r => setTimeout(r, attempt * 2000));
      } else {
        throw err;
      }
    } finally {
      if (client) client.release();
    }
  }
}

// ─────────────────────────────────────────
//  DATABASE SETUP (runs on startup)
// ─────────────────────────────────────────
async function setupDatabase() {
  await query(`
    CREATE TABLE IF NOT EXISTS tenants (
      id          SERIAL PRIMARY KEY,
      slug        TEXT UNIQUE NOT NULL,
      name        TEXT NOT NULL,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS users (
      id            SERIAL PRIMARY KEY,
      tenant_id     INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      email         TEXT NOT NULL,
      name          TEXT,
      type          TEXT DEFAULT 'Employee',
      pin           TEXT,
      username      TEXT,
      is_active     BOOLEAN DEFAULT TRUE,
      charge_gst    BOOLEAN DEFAULT FALSE,
      gst_number    TEXT DEFAULT '',
      email_reports BOOLEAN DEFAULT TRUE,
      profile_pic   TEXT DEFAULT '',
      requires_approval BOOLEAN DEFAULT FALSE,
      allow_late_periods BOOLEAN DEFAULT TRUE,
      require_pin_change BOOLEAN DEFAULT FALSE,
      created_at    TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(tenant_id, email)
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS entries (
      id              TEXT PRIMARY KEY,
      tenant_id       INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      user_email      TEXT NOT NULL,
      user_name       TEXT,
      user_type       TEXT,
      pay_period_start DATE NOT NULL,
      pay_period_end   DATE NOT NULL,
      date            DATE NOT NULL,
      location        TEXT,
      time            TEXT,
      class_party     TEXT,
      hours_offered   NUMERIC(8,2),
      hourly_rate     NUMERIC(8,2),
      total           NUMERIC(8,2),
      notes           TEXT DEFAULT '',
      pole_bonus      BOOLEAN DEFAULT FALSE,
      server_ts       TIMESTAMPTZ DEFAULT NOW(),
      user_agent      TEXT DEFAULT ''
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS settings (
      id        SERIAL PRIMARY KEY,
      tenant_id INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      key       TEXT NOT NULL,
      value     TEXT,
      UNIQUE(tenant_id, key)
    )
  `);

  // QuickBooks Online integration tokens
  await query(`
    CREATE TABLE IF NOT EXISTS qbo_tokens (
      id            SERIAL PRIMARY KEY,
      tenant_id     INTEGER REFERENCES tenants(id) ON DELETE CASCADE UNIQUE,
      realm_id      TEXT NOT NULL,
      access_token  TEXT NOT NULL,
      refresh_token TEXT NOT NULL,
      token_type    TEXT DEFAULT 'bearer',
      expires_at    TIMESTAMPTZ,
      created_at    TIMESTAMPTZ DEFAULT NOW(),
      updated_at    TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS locations (
      id        SERIAL PRIMARY KEY,
      tenant_id INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      name      TEXT NOT NULL,
      sort_order INTEGER DEFAULT 0
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS pending_submissions (
      id              TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      tenant_id       INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      entry_id        TEXT,
      submitted_at    TIMESTAMPTZ DEFAULT NOW(),
      user_email      TEXT,
      user_name       TEXT,
      user_type       TEXT,
      pay_period_start DATE,
      pay_period_end   DATE,
      date            DATE,
      location        TEXT,
      time            TEXT,
      class_party     TEXT,
      hours_offered   NUMERIC(8,2),
      hourly_rate     NUMERIC(8,2),
      total           NUMERIC(8,2),
      notes           TEXT
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS submission_log (
      id              TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      tenant_id       INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      reported_at     TIMESTAMPTZ DEFAULT NOW(),
      user_email      TEXT,
      user_name       TEXT,
      pay_period_start DATE,
      pay_period_end   DATE,
      date            DATE,
      location        TEXT,
      class_party     TEXT,
      hours_offered   NUMERIC(8,2),
      total           NUMERIC(8,2)
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS support_messages (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      tenant_id   INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      from_email  TEXT,
      from_name   TEXT,
      type        TEXT DEFAULT 'message',
      subject     TEXT,
      body        TEXT,
      submitted_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // Sent reports log
  await query(`
    CREATE TABLE IF NOT EXISTS sent_reports_log (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      tenant_id   INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      sent_at     TIMESTAMPTZ DEFAULT NOW(),
      sent_by     TEXT,
      report_type TEXT,
      pp_start    DATE,
      pp_end      DATE,
      recipients  TEXT,
      staff_count INTEGER,
      total_pay   NUMERIC(10,2),
      summary     TEXT
    )
  `).catch(()=>{});

  // Ensure default tenant exists
  const existing = await query(`SELECT id FROM tenants WHERE slug = 'default'`);
  if (existing.rows.length === 0) {
    await query(`INSERT INTO tenants (slug, name) VALUES ('default', $1)`, [CONFIG.BRAND_NAME]);
    console.log("Created default tenant");
  }

  // Add columns that may not exist in older deployments
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS require_pin_change BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`
    CREATE TABLE IF NOT EXISTS payroll_approvals (
      id              SERIAL PRIMARY KEY,
      tenant_id       INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      user_email      TEXT NOT NULL,
      pay_period_start DATE NOT NULL,
      pay_period_end   DATE NOT NULL,
      approved         BOOLEAN DEFAULT TRUE,
      approved_by      TEXT,
      approved_at      TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(tenant_id, user_email, pay_period_start, pay_period_end)
    )
  `).catch(()=>{});
  // Support message status columns
  await query(`ALTER TABLE support_messages ADD COLUMN IF NOT EXISTS resolved BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE support_messages ADD COLUMN IF NOT EXISTS replied  BOOLEAN DEFAULT FALSE`).catch(()=>{});
  // Changelog table
  await query(`
    CREATE TABLE IF NOT EXISTS support_changelog (
      id           SERIAL PRIMARY KEY,
      tenant_id    INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      created_at   TIMESTAMPTZ DEFAULT NOW(),
      version_label TEXT,
      items        TEXT
    )
  `).catch(()=>{});
  // Approval cache
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS requires_approval BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email_bug_updates BOOLEAN DEFAULT TRUE`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS attach_pdf_payroll BOOLEAN DEFAULT TRUE`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS attach_csv_payroll BOOLEAN DEFAULT FALSE`).catch(()=>{});
  // Flag correction tracking on entries
  await query(`ALTER TABLE entries ADD COLUMN IF NOT EXISTS flag_correction_id TEXT`).catch(()=>{});
  await query(`ALTER TABLE entry_flags ADD COLUMN IF NOT EXISTS hidden_from_staff BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE entry_flags ADD COLUMN IF NOT EXISTS hidden_from_admin BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE entry_flags ADD COLUMN IF NOT EXISTS rolled_over BOOLEAN DEFAULT FALSE`).catch(()=>{});

  // ── Entry Flags table (payroll flagging system) ──
  await query(`
    CREATE TABLE IF NOT EXISTS entry_flags (
      id                  TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      tenant_id           INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      entry_id            TEXT NOT NULL,
      user_email          TEXT NOT NULL,
      pay_period_start    DATE NOT NULL,
      pay_period_end      DATE NOT NULL,
      status              TEXT DEFAULT 'flagged',
      flagged_by          TEXT NOT NULL,
      flagged_by_name     TEXT,
      flag_note           TEXT,
      flagged_at          TIMESTAMPTZ DEFAULT NOW(),
      correction_entry_id TEXT,
      correction_note     TEXT,
      corrected_at        TIMESTAMPTZ,
      resolved_by         TEXT,
      resolved_by_name    TEXT,
      resolution_note     TEXT,
      resolved_at         TIMESTAMPTZ,
      approved_by         TEXT,
      approved_by_name    TEXT,
      approved_at         TIMESTAMPTZ,
      reminder_sent       BOOLEAN DEFAULT FALSE,
      original_data       JSONB
    )
  `).catch(()=>{});

  // Google Calendar OAuth tokens
  await query(`
    CREATE TABLE IF NOT EXISTS google_calendar_tokens (
      id            SERIAL PRIMARY KEY,
      tenant_id     INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      user_email    TEXT NOT NULL,
      access_token  TEXT,
      refresh_token TEXT,
      token_expiry  TIMESTAMPTZ,
      connected_at  TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(tenant_id, user_email)
    )
  `).catch(()=>{});

  // Add address column to locations for Google Calendar location matching
  await query(`ALTER TABLE locations ADD COLUMN IF NOT EXISTS address TEXT DEFAULT ''`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS gcal_default_calendar TEXT DEFAULT ''`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS preferred_theme TEXT DEFAULT 'light'`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS pin_changed_at TIMESTAMPTZ`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS contract_pdf TEXT DEFAULT ''`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS contract_pdf_name TEXT DEFAULT ''`).catch(()=>{});

  // Setup & Cleanup time tracking columns
  await query(`ALTER TABLE entries ADD COLUMN IF NOT EXISTS setup_minutes INTEGER DEFAULT 0`).catch(()=>{});
  await query(`ALTER TABLE entries ADD COLUMN IF NOT EXISTS cleanup_minutes INTEGER DEFAULT 0`).catch(()=>{});
  await query(`ALTER TABLE entries ADD COLUMN IF NOT EXISTS setup_cleanup_pay NUMERIC(8,2) DEFAULT 0`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS setup_cleanup_rate NUMERIC(8,2)`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS setup_cleanup_allowed BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE pending_submissions ADD COLUMN IF NOT EXISTS setup_minutes INTEGER DEFAULT 0`).catch(()=>{});
  await query(`ALTER TABLE pending_submissions ADD COLUMN IF NOT EXISTS cleanup_minutes INTEGER DEFAULT 0`).catch(()=>{});
  await query(`ALTER TABLE pending_submissions ADD COLUMN IF NOT EXISTS setup_cleanup_pay NUMERIC(8,2) DEFAULT 0`).catch(()=>{});
  await query(`ALTER TABLE entries ADD COLUMN IF NOT EXISTS rigging_minutes INTEGER DEFAULT 0`).catch(()=>{});
  await query(`ALTER TABLE pending_submissions ADD COLUMN IF NOT EXISTS rigging_minutes INTEGER DEFAULT 0`).catch(()=>{});
  await query(`ALTER TABLE entries ADD COLUMN IF NOT EXISTS studio_rental_fee NUMERIC(8,2) DEFAULT 0`).catch(()=>{});
  await query(`ALTER TABLE entries ADD COLUMN IF NOT EXISTS qbo_synced BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS shift_filter_keywords TEXT DEFAULT ''`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS can_create_images BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS tsps_enabled BOOLEAN DEFAULT TRUE`).catch(()=>{});
  await query(`UPDATE users SET tsps_enabled = TRUE WHERE tsps_enabled = FALSE OR tsps_enabled IS NULL`).catch(()=>{});

  // TSPS (Take a Shift, Post a Shift) board
  await query(`
    CREATE TABLE IF NOT EXISTS shift_posts (
      id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id  INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      poster_email TEXT NOT NULL,
      location   TEXT NOT NULL,
      shift_time TEXT NOT NULL,
      shift_date TEXT NOT NULL,
      notes      TEXT DEFAULT '',
      status     TEXT DEFAULT 'open',
      claimed_by TEXT,
      claimed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `).catch(()=>{});

  await query(`ALTER TABLE shift_posts ADD COLUMN IF NOT EXISTS class_name TEXT DEFAULT ''`).catch(()=>{});
  await query(`ALTER TABLE shift_posts ADD COLUMN IF NOT EXISTS poster_name TEXT DEFAULT ''`).catch(()=>{});
  await query(`ALTER TABLE shift_posts ADD COLUMN IF NOT EXISTS reminder_24h_sent BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE shift_posts ADD COLUMN IF NOT EXISTS claimed_by_name TEXT`).catch(()=>{});
  await query(`ALTER TABLE shift_posts ADD COLUMN IF NOT EXISTS duration INTEGER DEFAULT 60`).catch(()=>{});
  await query(`ALTER TABLE shift_posts ADD COLUMN IF NOT EXISTS front_desk BOOLEAN DEFAULT FALSE`).catch(()=>{});

  // Push notification category preferences (per user)
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS push_flags BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS push_pay BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS push_shifts BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS push_chat BOOLEAN DEFAULT FALSE`).catch(()=>{});
  // Disable all push notifications for all users
  await query(`UPDATE users SET push_flags=FALSE, push_pay=FALSE, push_shifts=FALSE WHERE push_flags=TRUE OR push_pay=TRUE OR push_shifts=TRUE`).catch(()=>{});

  // Email notification preferences for TSPS shifts
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email_shifts BOOLEAN DEFAULT TRUE`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email_shifts_urgent_only BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS front_desk_staff BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS front_desk_only BOOLEAN DEFAULT FALSE`).catch(()=>{});
  // Enable email_shifts for all existing users
  await query(`UPDATE users SET email_shifts = TRUE WHERE email_shifts IS NULL OR email_shifts = FALSE`).catch(()=>{});

  // Push notification subscriptions
  await query(`
    CREATE TABLE IF NOT EXISTS push_subscriptions (
      id         SERIAL PRIMARY KEY,
      tenant_id  INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      user_email TEXT NOT NULL,
      endpoint   TEXT NOT NULL UNIQUE,
      p256dh     TEXT NOT NULL,
      auth       TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `).catch(()=>{});

  // Class Proposals table
  await query(`
    CREATE TABLE IF NOT EXISTS class_proposals (
      id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id    INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      proposer_email TEXT NOT NULL,
      proposer_name  TEXT DEFAULT '',
      class_name   TEXT NOT NULL,
      proposal_date TEXT NOT NULL,
      start_time   TEXT NOT NULL,
      duration     INTEGER DEFAULT 60,
      location     TEXT NOT NULL,
      room         TEXT NOT NULL,
      color        TEXT DEFAULT '',
      notes        TEXT DEFAULT '',
      series_id    UUID DEFAULT NULL,
      series_index INTEGER DEFAULT NULL,
      series_total INTEGER DEFAULT NULL,
      created_at   TIMESTAMPTZ DEFAULT NOW()
    )
  `).catch(()=>{});
  // Add series columns if missing (existing tables)
  await query(`ALTER TABLE class_proposals ADD COLUMN IF NOT EXISTS series_id UUID DEFAULT NULL`).catch(()=>{});
  await query(`ALTER TABLE class_proposals ADD COLUMN IF NOT EXISTS series_index INTEGER DEFAULT NULL`).catch(()=>{});
  await query(`ALTER TABLE class_proposals ADD COLUMN IF NOT EXISTS series_total INTEGER DEFAULT NULL`).catch(()=>{});
  // Add status/archive columns if missing
  await query(`ALTER TABLE class_proposals ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending'`).catch(()=>{});
  await query(`ALTER TABLE class_proposals ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ DEFAULT NULL`).catch(()=>{});
  // Per-user admin permissions (JSON text)
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS admin_permissions TEXT DEFAULT ''`).catch(()=>{});
  // What the staff member teaches (free-text, comma-separated)
  await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS teaches TEXT DEFAULT ''`).catch(()=>{});

  // Auto-populate "teaches" categories from existing logged entries for staff who have it blank
  try {
    const blankTeachers = await query(`SELECT u.email, u.tenant_id FROM users u WHERE (u.teaches IS NULL OR u.teaches = '') AND u.type IN ('Employee','Contractor')`);
    for (const row of blankTeachers.rows) {
      const entries = await query(
        `SELECT DISTINCT LOWER(class_party) as cp FROM entries WHERE user_email=$1 AND tenant_id=$2 AND class_party IS NOT NULL AND class_party != ''`,
        [row.email, row.tenant_id]
      );
      if (entries.rows.length) {
        const allClasses = entries.rows.map(r => r.cp).join(" ");
        const cats = new Set();
        if (/pole/i.test(allClasses)) cats.add("pole");
        if (/aerial|silk|hoop|lyra|trapeze|hammock|sling/i.test(allClasses)) cats.add("aerial");
        if (/dance|heels|choreo|hip\s*hop|jazz|contemp|ballet|burlesque/i.test(allClasses)) cats.add("dance");
        if (/fitness|yoga|pilates|stretch|strength|conditioning|barre|cardio|flex/i.test(allClasses)) cats.add("fitness");
        if (/front\s*desk|reception|check.in/i.test(allClasses)) cats.add("front desk");
        // If they teach something but none of the known categories matched, mark as "other"
        if (cats.size === 0 && entries.rows.length > 0) cats.add("other");
        if (cats.size) {
          await query(`UPDATE users SET teaches=$1 WHERE email=$2 AND tenant_id=$3`, [Array.from(cats).join(", "), row.email, row.tenant_id]);
        }
      }
    }
  } catch(e) { console.log("teaches auto-populate skipped:", e.message); }

  // Re-map any old free-text teaches values to category checkboxes
  try {
    const validCats = ["pole", "aerial", "dance", "fitness", "front desk", "other"];
    const needsRemap = await query(`SELECT email, tenant_id, teaches FROM users WHERE teaches IS NOT NULL AND teaches != ''`);
    for (const row of needsRemap.rows) {
      const current = (row.teaches || "").toLowerCase().split(",").map(s => s.trim()).filter(Boolean);
      const allValid = current.every(c => validCats.includes(c));
      if (allValid) continue; // already in correct format
      const cats = new Set();
      const raw = row.teaches.toLowerCase();
      if (/pole/i.test(raw)) cats.add("pole");
      if (/aerial|silk|hoop|lyra|trapeze|hammock|sling/i.test(raw)) cats.add("aerial");
      if (/dance|heels|choreo|hip\s*hop|jazz|contemp|ballet|burlesque/i.test(raw)) cats.add("dance");
      if (/fitness|yoga|pilates|stretch|strength|conditioning|barre|cardio|flex/i.test(raw)) cats.add("fitness");
      if (/front\s*desk|reception|check.in/i.test(raw)) cats.add("front desk");
      if (cats.size === 0) cats.add("other");
      await query(`UPDATE users SET teaches=$1 WHERE email=$2 AND tenant_id=$3`, [Array.from(cats).join(", "), row.email, row.tenant_id]);
    }
  } catch(e) { console.log("teaches remap skipped:", e.message); }

  // Auto-add "front desk" to teaches for users flagged as front_desk_staff
  try {
    const fdStaff = await query(`SELECT email, tenant_id, teaches FROM users WHERE front_desk_staff = TRUE`);
    for (const row of fdStaff.rows) {
      const current = (row.teaches || "").toLowerCase();
      if (current.indexOf("front desk") >= 0) continue; // already has it
      const newTeaches = current ? (row.teaches + ", front desk") : "front desk";
      await query(`UPDATE users SET teaches=$1 WHERE email=$2 AND tenant_id=$3`, [newTeaches, row.email, row.tenant_id]);
    }
  } catch(e) { console.log("front desk teaches sync skipped:", e.message); }

  // Proposal dialogue/negotiation messages
  await query(`
    CREATE TABLE IF NOT EXISTS proposal_messages (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      proposal_id     UUID NOT NULL REFERENCES class_proposals(id) ON DELETE CASCADE,
      tenant_id       INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      sender_email    TEXT NOT NULL,
      sender_name     TEXT DEFAULT '',
      sender_role     TEXT NOT NULL,
      action          TEXT NOT NULL,
      message         TEXT DEFAULT '',
      suggested_fields JSONB DEFAULT NULL,
      created_at      TIMESTAMPTZ DEFAULT NOW()
    )
  `).catch(()=>{});
  // Add dialogue columns to class_proposals if missing
  await query(`ALTER TABLE class_proposals ADD COLUMN IF NOT EXISTS denied_at TIMESTAMPTZ DEFAULT NULL`).catch(()=>{});
  await query(`ALTER TABLE class_proposals ADD COLUMN IF NOT EXISTS last_action_by TEXT DEFAULT NULL`).catch(()=>{});
  await query(`ALTER TABLE class_proposals ADD COLUMN IF NOT EXISTS last_action_at TIMESTAMPTZ DEFAULT NULL`).catch(()=>{});

  // ── Chat system tables ──
  await query(`
    CREATE TABLE IF NOT EXISTS chat_conversations (
      id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id   INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      type        TEXT NOT NULL CHECK (type IN ('global', 'group', 'dm')),
      name        TEXT DEFAULT '',
      created_by  TEXT NOT NULL,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    )
  `).catch(()=>{});

  await query(`
    CREATE TABLE IF NOT EXISTS chat_members (
      id              SERIAL PRIMARY KEY,
      conversation_id UUID REFERENCES chat_conversations(id) ON DELETE CASCADE,
      tenant_id       INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      user_email      TEXT NOT NULL,
      role            TEXT DEFAULT 'member',
      joined_at       TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(conversation_id, user_email)
    )
  `).catch(()=>{});

  await query(`
    CREATE TABLE IF NOT EXISTS chat_messages (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      conversation_id UUID REFERENCES chat_conversations(id) ON DELETE CASCADE,
      tenant_id       INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      sender_email    TEXT NOT NULL,
      sender_name     TEXT DEFAULT '',
      body            TEXT NOT NULL,
      created_at      TIMESTAMPTZ DEFAULT NOW()
    )
  `).catch(()=>{});

  await query(`
    CREATE TABLE IF NOT EXISTS chat_read_cursors (
      id              SERIAL PRIMARY KEY,
      conversation_id UUID REFERENCES chat_conversations(id) ON DELETE CASCADE,
      tenant_id       INTEGER REFERENCES tenants(id) ON DELETE CASCADE,
      user_email      TEXT NOT NULL,
      last_read_at    TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(conversation_id, user_email)
    )
  `).catch(()=>{});

  await query(`ALTER TABLE chat_read_cursors ADD COLUMN IF NOT EXISTS archived BOOLEAN DEFAULT FALSE`).catch(()=>{});
  await query(`ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS edited_at TIMESTAMPTZ DEFAULT NULL`).catch(()=>{});

  // Auto-create a single "General" global conversation per tenant
  {
    const tid = await getDefaultTenantId();
    const existing = await query(`SELECT id FROM chat_conversations WHERE tenant_id=$1 AND type='global'`, [tid]);
    if (!existing.rows.length) {
      await query(`INSERT INTO chat_conversations (tenant_id, type, name, created_by) VALUES ($1, 'global', 'General', 'system')`, [tid]);
    }
  }

  // (Removed: auto-enable TSPS for admins — now managed manually per-user)

  // Default all existing staff to receive email reports
  await query(`UPDATE users SET email_reports = TRUE WHERE email_reports = FALSE`).catch(()=>{});

  // Ensure the admin account always has ADMIN type
  await query(`UPDATE users SET type='Admin' WHERE LOWER(username)='admin' AND UPPER(type)!='ADMIN'`).catch(()=>{});

  // Clean up orphaned pending_submissions whose entries no longer exist
  await query(`DELETE FROM pending_submissions WHERE entry_id IS NOT NULL AND entry_id NOT IN (SELECT id FROM entries)`).catch(()=>{});

  // Pre-populate pp settings cache
  await refreshPpSettingsCache().catch(() => {});

  // ── Demo mode: is_demo columns + seed data ──
  if (CONFIG.DEMO_MODE) {
    await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS is_demo BOOLEAN DEFAULT FALSE`).catch(()=>{});
    await query(`ALTER TABLE entries ADD COLUMN IF NOT EXISTS is_demo BOOLEAN DEFAULT FALSE`).catch(()=>{});
    await query(`ALTER TABLE locations ADD COLUMN IF NOT EXISTS is_demo BOOLEAN DEFAULT FALSE`).catch(()=>{});
    await query(`ALTER TABLE shift_posts ADD COLUMN IF NOT EXISTS is_demo BOOLEAN DEFAULT FALSE`).catch(()=>{});
    await query(`ALTER TABLE class_proposals ADD COLUMN IF NOT EXISTS is_demo BOOLEAN DEFAULT FALSE`).catch(()=>{});
    await seedDemoData();
  }

  console.log("Database ready");
}

// ─── Demo Seed Data ───
async function seedDemoData() {
  const tid = 1;
  // Check if already seeded
  const check = await query(`SELECT COUNT(*) FROM users WHERE tenant_id=$1 AND is_demo=TRUE`, [tid]);
  if (parseInt(check.rows[0].count) > 0) return;
  console.log("Seeding demo data...");

  const locations = [
    { name: "Downtown Studio", address: "123 Main St" },
    { name: "Kingsway Studio", address: "456 Kingsway Ave" },
    { name: "Southside Studio", address: "789 Whyte Ave" }
  ];
  for (const loc of locations) {
    await query(`INSERT INTO locations (tenant_id, name, address, is_demo) VALUES ($1,$2,$3,TRUE)`, [tid, loc.name, loc.address]);
  }

  const staff = [
    { email: "sarah@demo.kronara.com", name: "Sarah Mitchell", type: "Employee", pin: "1234", username: "sarah" },
    { email: "jessica@demo.kronara.com", name: "Jessica Chen", type: "Contractor", pin: "1234", username: "jessica" },
    { email: "alex@demo.kronara.com", name: "Alex Rivera", type: "Employee", pin: "1234", username: "alex" },
    { email: "maya@demo.kronara.com", name: "Maya Thompson", type: "Contractor", pin: "1234", username: "maya" },
    { email: "demo@demo.kronara.com", name: "Demo User", type: "Employee", pin: "1212", username: "demo" }
  ];
  for (const s of staff) {
    await query(
      `INSERT INTO users (tenant_id, email, name, type, pin, username, is_active, is_demo)
       VALUES ($1,$2,$3,$4,$5,$6,TRUE,TRUE) ON CONFLICT DO NOTHING`,
      [tid, s.email, s.name, s.type, s.pin, s.username]
    );
  }

  // Generate entries for the current and previous pay periods
  const classNames = ["Pole Foundations", "Aerial Silks", "Flexibility Flow", "Pole Tricks", "Lyra Basics", "Exotic Flow", "Floorwork", "Strength & Spin"];
  const rooms = ["Studio A", "Studio B", "Pole Room"];
  const times = ["9:00 AM", "10:30 AM", "12:00 PM", "4:00 PM", "5:30 PM", "7:00 PM"];
  const rates = [25, 30, 35, 28, 32];

  function demoDate(daysFromToday) {
    const d = new Date(); d.setDate(d.getDate() + daysFromToday);
    return d.toISOString().slice(0, 10);
  }
  function ppFor(dateStr) {
    const anchor = new Date(CONFIG.PAY_PERIOD_ANCHOR);
    const d = new Date(dateStr);
    const diff = Math.floor((d - anchor) / 86400000);
    const ppIndex = Math.floor(diff / 14);
    const start = new Date(anchor); start.setDate(start.getDate() + ppIndex * 14);
    const end = new Date(start); end.setDate(end.getDate() + 13);
    return { start: start.toISOString().slice(0,10), end: end.toISOString().slice(0,10) };
  }

  let entryIdx = 0;
  for (let dayOffset = -20; dayOffset <= 14; dayOffset++) {
    const dateStr = demoDate(dayOffset);
    const dayOfWeek = new Date(dateStr).getDay();
    if (dayOfWeek === 0) continue; // skip Sundays
    const classCount = dayOfWeek === 6 ? 2 : 3 + Math.floor(Math.random() * 2);
    for (let c = 0; c < classCount; c++) {
      const staffMember = staff[entryIdx % staff.length];
      const cn = classNames[entryIdx % classNames.length];
      const loc = locations[entryIdx % locations.length].name;
      const rm = rooms[entryIdx % rooms.length];
      const time = times[entryIdx % times.length];
      const rate = rates[entryIdx % rates.length];
      const hours = 1 + (entryIdx % 3) * 0.5;
      const pp = ppFor(dateStr);
      const eid = `demo-${dateStr}-${entryIdx}`;
      await query(
        `INSERT INTO entries (id, tenant_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, is_demo)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,TRUE) ON CONFLICT DO NOTHING`,
        [eid, tid, staffMember.email, staffMember.name, staffMember.type, pp.start, pp.end, dateStr, loc, time, cn, hours, rate, (hours*rate), ]
      );
      entryIdx++;
    }
  }

  // Seed a couple proposals
  const proposalData = [
    { email: "sarah@demo.kronara.com", name: "Sarah Mitchell", cn: "Chair Dance Basics", date: demoDate(7), time: "6:00 PM", loc: "Downtown Studio", room: "Studio B", status: "pending" },
    { email: "jessica@demo.kronara.com", name: "Jessica Chen", cn: "Handstand Workshop", date: demoDate(10), time: "11:00 AM", loc: "Kingsway Studio", room: "Studio A", status: "pending" },
    { email: "alex@demo.kronara.com", name: "Alex Rivera", cn: "Pole Combos", date: demoDate(5), time: "7:00 PM", loc: "Southside Studio", room: "Pole Room", status: "approved", archived_at: "NOW()" }
  ];
  for (const p of proposalData) {
    await query(
      `INSERT INTO class_proposals (tenant_id, proposer_email, proposer_name, class_name, proposal_date, start_time, duration, location, room, status, is_demo${p.status==='approved'?', archived_at':''})
       VALUES ($1,$2,$3,$4,$5,$6,60,$7,$8,$9,TRUE${p.status==='approved'?', NOW()':''})`,
      [tid, p.email, p.name, p.cn, p.date, p.time, p.loc, p.room, p.status]
    );
  }

  // Seed a shift swap
  await query(
    `INSERT INTO shift_posts (tenant_id, poster_email, poster_name, location, shift_time, shift_date, class_name, notes, status, is_demo)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'open',TRUE)`,
    [tid, "maya@demo.kronara.com", "Maya Thompson", "Downtown Studio", "5:30 PM", demoDate(3), "Aerial Silks", "Need someone to cover — family event!"]
  );

  console.log("Demo data seeded.");
}

// ─────────────────────────────────────────
//  MIGRATION: Google Sheets → Postgres
//  Runs once if triggered via /api/migrate
// ─────────────────────────────────────────
function getSheetsClient() {
  let credentials;
  if (process.env.GOOGLE_CREDENTIALS_JSON) {
    credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS_JSON);
  } else {
    let privateKey = process.env.GOOGLE_PRIVATE_KEY || "";
    privateKey = privateKey.replace(/\\n/g, "\n");
    credentials = {
      client_email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
      private_key: privateKey,
    };
  }
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  return google.sheets({ version: "v4", auth });
}

// ─────────────────────────────────────────
//  GOOGLE CALENDAR OAUTH
// ─────────────────────────────────────────
function getOAuth2Client() {
  const clientId = (process.env.GOOGLE_OAUTH_CLIENT_ID || "").trim();
  const clientSecret = (process.env.GOOGLE_OAUTH_CLIENT_SECRET || "").trim();
  const redirectUri = (process.env.GOOGLE_OAUTH_REDIRECT_URI || "").trim();
  console.log("[GCAL] OAuth client_id starts with:", JSON.stringify(clientId.slice(0, 20)), "length:", clientId.length);
  const { OAuth2 } = google.auth;
  return new OAuth2(clientId, clientSecret, redirectUri);
}

async function getCalendarClientForUser(userEmail) {
  const tid = await getDefaultTenantId();
  const res = await query(
    `SELECT access_token, refresh_token, token_expiry FROM google_calendar_tokens WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2)`,
    [tid, userEmail]
  );
  if (!res.rows.length) return null;
  const row = res.rows[0];
  const oauth2 = getOAuth2Client();
  oauth2.setCredentials({
    access_token: row.access_token,
    refresh_token: row.refresh_token,
    expiry_date: row.token_expiry ? new Date(row.token_expiry).getTime() : 0,
  });
  // Auto-refresh if expired
  if (row.token_expiry && new Date(row.token_expiry) <= new Date()) {
    try {
      const { credentials } = await oauth2.refreshAccessToken();
      oauth2.setCredentials(credentials);
      await query(
        `UPDATE google_calendar_tokens SET access_token=$1, token_expiry=$2 WHERE tenant_id=$3 AND LOWER(user_email)=LOWER($4)`,
        [credentials.access_token, credentials.expiry_date ? new Date(credentials.expiry_date) : null, tid, userEmail]
      );
    } catch (e) {
      console.error("Failed to refresh Google token for", userEmail, e.message);
      return null;
    }
  }
  return google.calendar({ version: "v3", auth: oauth2 });
}

async function getSheetValues(sheets, sheetName) {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: sheetName,
    });
    return res.data.values || [];
  } catch (e) {
    return [];
  }
}

function parseSheet(values) {
  if (!values || values.length < 2) return { headers: values?.[0] || [], rows: [] };
  const headers = values[0].map(h => String(h || "").trim());
  const rows = values.slice(1).map(r => {
    const obj = {};
    headers.forEach((h, i) => { obj[h] = String(r[i] == null ? "" : r[i]); });
    return obj;
  });
  return { headers, rows };
}

// ─────────────────────────────────────────
//  EMAIL
// ─────────────────────────────────────────
function getResend() {
  return new Resend(process.env.RESEND_API_KEY);
}

// Resend expects "to" as array, supports cc/bcc as arrays too
function toArray(val) {
  if (!val) return [];
  if (Array.isArray(val)) return val.filter(Boolean);
  return val.split(",").map(e => e.trim()).filter(Boolean);
}

// Rate limiter: max 2 emails/sec to avoid Resend 429s
const _mailRL = { queue: Promise.resolve() };
function mailRateLimit() {
  _mailRL.queue = _mailRL.queue.then(() => new Promise(r => setTimeout(r, 550)));
  return _mailRL.queue;
}
async function sendMailWithRetry(sendFn, maxRetries) {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try { return await sendFn(); }
    catch (err) {
      const status = err?.statusCode || err?.status || err?.code;
      if (status === 429 && attempt < maxRetries) {
        const wait = Math.min(1000 * Math.pow(2, attempt), 8000);
        console.log(`[MAIL] Rate limited, retrying in ${wait}ms (attempt ${attempt+1}/${maxRetries})`);
        await new Promise(r => setTimeout(r, wait));
        continue;
      }
      throw err;
    }
  }
}

async function sendMail(opts) {
  // ── Test mode: block or redirect emails ──
  const _settings = await getAdminSettings().catch(() => ({}));

  const allTo = [
    ...toArray(opts.to),
    ...toArray(opts.cc),
    ...toArray(opts.bcc)
  ].map(e => e.toLowerCase());

  // Gather admin emails that are exempt from whitelist filtering
  function getExemptAdminEmails() {
    if (!_settings.adminEmailsWhitelistExempt) return [];
    return [
      _settings.accountantEmail, _settings.adminEmail1, _settings.adminEmail2, _settings.adminEmail3,
      _settings.supportEmail, _settings.payrollAdminEmail
    ].map(e => (e||"").trim().toLowerCase()).filter(Boolean);
  }

  if (_settings.testModeEnabled) {
    const allowed = (_settings.testModeAllowedEmails || "")
      .split(",").map(e => e.trim().toLowerCase()).filter(Boolean)
      .concat(getExemptAdminEmails())
      .filter((v,i,a) => a.indexOf(v) === i); // dedupe

    const resend = getResend();
    const from   = process.env.RESEND_FROM || `${CONFIG.BRAND_NAME} <support@aradiafitness.app>`;

    // Admin/accountant reports always bypass test mode filtering — send to actual recipients
    if (opts.adminReport) {
      const to = toArray(opts.to);
      const cc = toArray(opts.cc);
      console.log(`[TEST MODE] Admin report — bypassing filter → ${[...to,...cc].join(", ")}`);
      await mailRateLimit();
      await sendMailWithRetry(() => resend.emails.send({
        from,
        to,
        cc:      cc.length ? cc : undefined,
        subject: `[TEST] ${opts.subject || ""}`,
        html:    opts.html || opts.text || "",
        ...(opts.attachments ? {
          attachments: opts.attachments.map(a => ({
            filename: a.filename,
            content:  Buffer.from(a.content).toString("base64"),
          }))
        } : {}),
      }), 3);
      return;
    }

    const permittedTo = toArray(opts.to).filter(e => allowed.includes(e.toLowerCase()));
    const blockedTo   = toArray(opts.to).filter(e => !allowed.includes(e.toLowerCase()));

    // Send directly to whitelisted recipients
    if (permittedTo.length) {
      console.log(`[TEST MODE] Sending to permitted: ${permittedTo.join(", ")}`);
      await mailRateLimit();
      await sendMailWithRetry(() => resend.emails.send({
        from,
        to:      permittedTo,
        subject: `[TEST] ${opts.subject || ""}`,
        html:    opts.html || opts.text || "",
        ...(opts.attachments ? {
          attachments: opts.attachments.map(a => ({
            filename: a.filename,
            content:  Buffer.from(a.content).toString("base64"),
          }))
        } : {}),
      }), 3);
    }

    // Redirect blocked staff emails to whitelisted addresses with a banner
    if (blockedTo.length && allowed.length) {
      console.log(`[TEST MODE] Redirecting blocked (${blockedTo.join(", ")}) → ${allowed.join(", ")} | Subject: ${opts.subject}`);
      const redirectBanner = `<div style="background:#fff3cd;border:2px solid #ffc107;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-family:Arial,sans-serif;font-size:13px;">
        <strong>🧪 TEST MODE REDIRECT</strong><br>
        This email would have been sent to: <strong>${blockedTo.join(", ")}</strong><br>
        <span style="color:#888;font-size:12px;">Redirected here because test mode is enabled.</span>
      </div>`;
      await mailRateLimit();
      await sendMailWithRetry(() => resend.emails.send({
        from,
        to:      allowed,
        subject: `[TEST → ${blockedTo.join(", ")}] ${opts.subject || ""}`,
        html:    redirectBanner + (opts.html || opts.text || ""),
        ...(opts.attachments ? {
          attachments: opts.attachments.map(a => ({
            filename: a.filename,
            content:  Buffer.from(a.content).toString("base64"),
          }))
        } : {}),
      }), 3);
    } else if (!permittedTo.length) {
      console.log(`[TEST MODE] Email suppressed entirely — no permitted or redirect recipients. Subject: ${opts.subject}`);
    }

    return;
  }

  // ── Suppress staff emails (non-test-mode) ──
  // If suppressStaffEmails is on, block email unless the recipient is in the whitelist
  // Admin/accountant reports always bypass suppression
  if (_settings.suppressStaffEmails && !opts.adminReport) {
    const allowed = (_settings.testModeAllowedEmails || "")
      .split(",").map(e => e.trim().toLowerCase()).filter(Boolean)
      .concat(getExemptAdminEmails())
      .filter((v,i,a) => a.indexOf(v) === i);
    const toList = toArray(opts.to);
    const permitted = toList.filter(e => allowed.includes(e.toLowerCase()));
    const blocked   = toList.filter(e => !allowed.includes(e.toLowerCase()));
    if (blocked.length)
      console.log(`[SUPPRESS STAFF] Blocked email to: ${blocked.join(", ")} | Subject: ${opts.subject}`);
    if (!permitted.length) return; // fully suppressed
    // Send only to permitted (whitelist bypasses suppression)
    opts = { ...opts, to: permitted.join(","), cc: undefined, bcc: undefined };
  }

  // Normal send
  const resend = getResend();
  const from = process.env.RESEND_FROM || `${CONFIG.BRAND_NAME} <support@aradiafitness.app>`;
  await mailRateLimit();
  await sendMailWithRetry(() => resend.emails.send({
    from,
    to:      toArray(opts.to),
    cc:      toArray(opts.cc).length   ? toArray(opts.cc)   : undefined,
    bcc:     toArray(opts.bcc).length  ? toArray(opts.bcc)  : undefined,
    replyTo: opts.replyTo || undefined,
    subject: opts.subject || "",
    html:    opts.html || opts.text || "",
    ...(opts.attachments ? {
      attachments: opts.attachments.map(a => ({
        filename: a.filename,
        content:  Buffer.from(a.content).toString("base64"),
      }))
    } : {}),
  }), 3);
}


// ── Send push notification ─────────
// category: "flags" | "pay" | "shifts" — checked against user preferences
async function sendPush(email, title, body, url, category) {
  if (!process.env.VAPID_PUBLIC_KEY || !process.env.VAPID_PRIVATE_KEY) return;
  try {
    const tid = await getDefaultTenantId();
    // Check user's category preference
    if (category) {
      const col = category === "flags" ? "push_flags" : category === "pay" ? "push_pay" : category === "shifts" ? "push_shifts" : category === "chat" ? "push_chat" : null;
      if (col) {
        const userRes = await query(`SELECT ${col} FROM users WHERE tenant_id=$1 AND LOWER(email)=LOWER($2)`, [tid, email]);
        if (userRes.rows.length && !userRes.rows[0][col]) return; // user opted out of this category
      }
    }
    const subs = await query(
      `SELECT * FROM push_subscriptions WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2)`,
      [tid, email]
    );
    for (const sub of subs.rows) {
      const pushSub = {
        endpoint: sub.endpoint,
        keys: { p256dh: sub.p256dh, auth: sub.auth }
      };
      try {
        await webpush.sendNotification(pushSub, JSON.stringify({ title, body, url: url || "/" }));
      } catch (err) {
        if (err.statusCode === 404 || err.statusCode === 410) {
          await query(`DELETE FROM push_subscriptions WHERE id=$1`, [sub.id]);
          console.log(`[PUSH] Removed stale subscription for ${email}`);
        } else {
          console.error(`[PUSH] Failed for ${email}:`, err.message);
        }
      }
    }
  } catch (err) {
    console.error("[PUSH] sendPush error:", err.message);
  }
}

// Send push to all subscribed staff (except excludeEmail)
async function sendPushToAll(title, body, url, excludeEmail, category) {
  if (!process.env.VAPID_PUBLIC_KEY || !process.env.VAPID_PRIVATE_KEY) return;
  try {
    const tid = await getDefaultTenantId();
    const subs = await query(
      `SELECT DISTINCT user_email FROM push_subscriptions WHERE tenant_id=$1`,
      [tid]
    );
    for (const row of subs.rows) {
      if (excludeEmail && row.user_email.toLowerCase() === excludeEmail.toLowerCase()) continue;
      sendPush(row.user_email, title, body, url, category).catch(() => {});
    }
  } catch (err) {
    console.error("[PUSH] sendPushToAll error:", err.message);
  }
}

// ── Build payroll PDF attachment ─────────
async function buildPayrollPdf(users, pp, settings, lateGroups) {
  return new Promise((resolve, reject) => {
    const doc  = new PDFDocument({ margin: 40, size: "LETTER", bufferPages: true });
    const bufs = [];
    doc.on("data", d => bufs.push(d));
    doc.on("end",  () => resolve(Buffer.concat(bufs)));
    doc.on("error", reject);

    const brand = CONFIG.BRAND_NAME || "Aradia Fitness";
    const col   = { red: "${CONFIG.BRAND_COLOR_PRIMARY}", dark: "#222", mid: "#555", light: "#888" };
    const pageW = doc.page.width;
    const contentW = pageW - 80;

    // Header
    doc.rect(40, 40, contentW, 40).fill(col.red);
    doc.fillColor("#fff").fontSize(18).font("Helvetica-Bold")
       .text(brand + " — Payroll Report", 52, 51);
    doc.fillColor(col.dark).fontSize(11).font("Helvetica")
       .text("Pay Period: " + pp.start + "  to  " + pp.end, 40, 90);

    // ── Summary box ──
    let grandHours = 0, grandGross = 0, grandGST = 0, grandTotal = 0;
    users.forEach(u => {
      const isC = (u.type||"").toLowerCase() === "contractor";
      const gst = (isC && u.chargeGST) ? round2(u.totalPay * 0.05) : 0;
      grandHours += u.totalHours; grandGross += u.totalPay; grandGST += gst; grandTotal += round2(u.totalPay + gst);
    });
    const boxY = 108;
    doc.rect(40, boxY, contentW, 48).lineWidth(1).strokeColor("#d0d0d0").fillAndStroke("#f7f8fa", "#d0d0d0");
    const boxItems = [
      { label: "Staff", value: String(users.length) },
      { label: "Total Hours", value: round2(grandHours).toFixed(2) },
      { label: "Gross Pay", value: "$" + round2(grandGross).toFixed(2) },
      { label: "GST", value: "$" + round2(grandGST).toFixed(2) },
      { label: "Total Owing", value: "$" + round2(grandTotal).toFixed(2) },
    ];
    const boxColW = contentW / boxItems.length;
    boxItems.forEach((item, i) => {
      const bx = 40 + i * boxColW;
      doc.fillColor(col.mid).font("Helvetica").fontSize(8)
         .text(item.label, bx + 8, boxY + 8, { width: boxColW - 16, align: "center" });
      doc.fillColor(col.dark).font("Helvetica-Bold").fontSize(13)
         .text(item.value, bx + 8, boxY + 22, { width: boxColW - 16, align: "center" });
    });

    // Table header
    const cols   = [170, 70, 65, 80, 65, contentW - 170 - 70 - 65 - 80 - 65];
    const labels = ["Name / Email", "Type", "Hours", "Gross Pay", "GST", "Total Owing"];
    let x = 40, y = boxY + 60;
    doc.rect(40, y, contentW, 22).fill("#333");
    doc.fillColor("#fff").font("Helvetica-Bold").fontSize(9);
    labels.forEach((lbl, i) => {
      doc.text(lbl, x + 4, y + 6, { width: cols[i] - 8, align: i > 1 ? "right" : "left" });
      x += cols[i];
    });
    y += 22;

    // Rows
    const rowH = 32;
    users.forEach((u, ri) => {
      const isC  = (u.type||"").toLowerCase() === "contractor";
      const gst  = (isC && u.chargeGST) ? round2(u.totalPay * 0.05) : 0;
      const tot  = round2(u.totalPay + gst);

      if (y + rowH + 4 > doc.page.height - 60) { doc.addPage(); y = 40; }
      if (ri % 2 === 0) doc.rect(40, y, contentW, rowH).fill("#fafafa");
      else doc.rect(40, y, contentW, rowH).fill("#fff");
      doc.fillColor(col.dark).font("Helvetica-Bold").fontSize(9)
         .text(u.name || u.email, 44, y + 5, { width: cols[0] - 8 });
      doc.fillColor(col.mid).font("Helvetica").fontSize(7.5)
         .text(u.email, 44, y + 17, { width: cols[0] - 8 });

      const vals = [u.type||"Employee", u.totalHours.toFixed(2),
        "$"+u.totalPay.toFixed(2), gst ? "$"+gst.toFixed(2) : "—", "$"+tot.toFixed(2)];
      x = 40 + cols[0];
      doc.fillColor(col.dark).font("Helvetica").fontSize(9);
      vals.forEach((v, i) => {
        doc.text(v, x + 4, y + 10, { width: cols[i+1] - 8, align: i > 0 ? "right" : "left" });
        x += cols[i+1];
      });
      doc.moveTo(40, y + rowH).lineTo(40 + contentW, y + rowH).strokeColor("#eee").lineWidth(0.5).stroke();
      y += rowH;
    });

    // Totals row
    if (y + 28 > doc.page.height - 60) { doc.addPage(); y = 40; }
    doc.rect(40, y, contentW, 28).fill(col.red);
    doc.fillColor("#fff").font("Helvetica-Bold").fontSize(10)
       .text("TOTALS", 44, y + 8, { width: cols[0] - 8 });
    const totVals = ["", grandHours.toFixed(2), "$"+round2(grandGross).toFixed(2), "$"+round2(grandGST).toFixed(2), "$"+round2(grandTotal).toFixed(2)];
    x = 40 + cols[0];
    totVals.forEach((v, i) => {
      if (v) doc.text(v, x + 4, y + 8, { width: cols[i+1] - 8, align: i > 0 ? "right" : "left" });
      x += cols[i+1];
    });

    // ── Late submissions section ──
    if (lateGroups && lateGroups.length) {
      doc.addPage();
      y = 40;
      // Orange header
      doc.rect(40, y, contentW, 36).fill("#e65100");
      doc.fillColor("#fff").fontSize(14).font("Helvetica-Bold")
         .text("Late Submissions — Require Payment", 52, y + 10);
      y += 44;
      doc.fillColor(col.mid).fontSize(9).font("Helvetica")
         .text("These entries are from prior pay periods and were submitted late. They represent additional amounts owed and are NOT included in the current period totals.", 40, y, { width: contentW });
      y += 28;

      let lateGrand = 0;
      lateGroups.forEach(g => {
        if (y + 50 > doc.page.height - 60) { doc.addPage(); y = 40; }
        // Person header
        doc.rect(40, y, contentW, 24).fill("#fff3cd");
        doc.fillColor(col.dark).font("Helvetica-Bold").fontSize(9)
           .text((g.name||g.email) + "   <" + g.email + ">", 44, y + 7, { width: contentW - 170 });
        doc.fillColor(col.mid).font("Helvetica").fontSize(8)
           .text("Period: " + g.ppStart + " to " + g.ppEnd, pageW - 200, y + 8, { width: 155, align: "right" });
        y += 24;

        // Column headers
        const lCols = [90, 160, 80, 70, 80];
        const lLabels = ["Date","Class / Party","Time","Hours","Total"];
        doc.rect(40, y, contentW, 20).fill("#fff8e1");
        let lx = 40;
        doc.fillColor(col.dark).font("Helvetica-Bold").fontSize(8);
        lLabels.forEach((lbl, i) => {
          doc.text(lbl, lx + 4, y + 5, { width: lCols[i] - 8, align: i > 2 ? "right" : "left" });
          lx += lCols[i];
        });
        y += 20;

        let subTotal = 0;
        g.entries.forEach((e, ri) => {
          if (y + 20 > doc.page.height - 60) { doc.addPage(); y = 40; }
          const date = e.date instanceof Date ? e.date.toISOString().slice(0,10) : String(e.date||"").slice(0,10);
          const hrs  = parseFloat(e.hours_offered||e.hours||0);
          const tot  = parseFloat(e.total||0);
          subTotal += tot; lateGrand += tot;
          if (ri % 2 === 0) doc.rect(40, y, contentW, 20).fill("#fffdf7");
          else doc.rect(40, y, contentW, 20).fill("#fff");
          const vals = [date, e.class_party||e.classParty||"", e.time||"", hrs.toFixed(2), "$"+tot.toFixed(2)];
          lx = 40;
          doc.fillColor(col.dark).font("Helvetica").fontSize(8);
          vals.forEach((v, i) => {
            doc.text(String(v), lx + 4, y + 5, { width: lCols[i] - 8, align: i > 2 ? "right" : "left" });
            lx += lCols[i];
          });
          doc.moveTo(40, y + 20).lineTo(40 + contentW, y + 20).strokeColor("#fde8c0").lineWidth(0.5).stroke();
          y += 20;
        });

        // Subtotal
        doc.rect(40, y, contentW, 22).fill("#fff3cd");
        doc.fillColor(col.dark).font("Helvetica-Bold").fontSize(9)
           .text("Subtotal", 40, y + 6, { width: contentW - 85, align: "right" });
        doc.text("$" + subTotal.toFixed(2), pageW - 120, y + 6, { width: 75, align: "right" });
        y += 30;
      });

      // Late grand total
      if (y + 28 > doc.page.height - 60) { doc.addPage(); y = 40; }
      doc.rect(40, y, contentW, 26).fill("#e65100");
      doc.fillColor("#fff").font("Helvetica-Bold").fontSize(11)
         .text("LATE TOTAL", 44, y + 7, { width: contentW - 110 });
      doc.text("$" + lateGrand.toFixed(2), pageW - 140, y + 7, { width: 95, align: "right" });
      y += 32;
    }

    // Add page numbers to all pages
    const totalPages = doc.bufferedPageRange().count;
    for (let i = 0; i < totalPages; i++) {
      doc.switchToPage(i);
      doc.fillColor(col.light).font("Helvetica").fontSize(8);
      const footerY = doc.page.height - 36;
      doc.text("Generated " + new Date().toLocaleString("en-CA") + " · " + brand, 40, footerY, { width: contentW * 0.6, align: "left", lineBreak: false });
      doc.text("Page " + (i + 1) + " of " + totalPages, 40 + contentW * 0.6, footerY, { width: contentW * 0.4, align: "right", lineBreak: false });
    }

    // Flush buffered pages to prevent PDFKit from appending a trailing blank page
    doc.flushPages();
    doc.end();
  });
}

// ── Build staff period PDF (single pay period report) ──
async function buildStaffPeriodPdf(user, tid, pp) {
  const entryRes = await query(
    `SELECT * FROM entries WHERE tenant_id=$1 AND user_email=$2 AND pay_period_start=$3 AND pay_period_end=$4 ORDER BY date ASC, server_ts ASC`,
    [tid, normEmail(user.email), pp.start, pp.end]
  );
  const entries = entryRes.rows.map(formatEntry);

  return new Promise((resolve, reject) => {
    const doc  = new PDFDocument({ margin: 40, size: "LETTER" });
    const bufs = [];
    doc.on("data", d => bufs.push(d));
    doc.on("end",  () => resolve(Buffer.concat(bufs)));
    doc.on("error", reject);

    const brand = CONFIG.BRAND_NAME || "Aradia Fitness";
    const col   = { red: "${CONFIG.BRAND_COLOR_PRIMARY}", dark: "#222", mid: "#555", light: "#888" };
    const isC   = (user.type||"").toLowerCase() === "contractor";
    const gst_  = isC && user.chargeGST;

    let totalHours = 0, totalPay = 0;
    entries.forEach(e => { totalHours += parseFloat(e.hours)||0; totalPay += (parseFloat(e.total)||0) - (parseFloat(e.studioRentalFee)||0); });
    totalHours = round2(totalHours);
    totalPay   = round2(totalPay);
    const gstAmt    = gst_ ? round2(totalPay * 0.05) : 0;
    const totalOwing = round2(totalPay + gstAmt);

    // Header
    doc.rect(40, 40, doc.page.width - 80, 36).fill(col.red);
    doc.fillColor("#fff").fontSize(16).font("Helvetica-Bold")
       .text("Pay Period Report — " + (user.name || user.email), 50, 50, { width: doc.page.width - 100 });

    // Meta row
    let y = 88;
    doc.fillColor(col.dark).fontSize(10).font("Helvetica");
    doc.text("Pay Period: " + pp.start + "  to  " + pp.end, 40, y);
    y += 16;
    const metaItems = [
      "Shifts: " + entries.length,
      "Total Hours: " + totalHours.toFixed(2),
      "Gross Pay: $" + totalPay.toFixed(2),
    ];
    if (gstAmt > 0) {
      metaItems.push("GST (5%): $" + gstAmt.toFixed(2));
      metaItems.push("Total Owing: $" + totalOwing.toFixed(2));
    }
    doc.text(metaItems.join("   |   "), 40, y, { width: doc.page.width - 80 });
    y += 24;

    // Group entries by date
    const byDate = {};
    const dateOrder = [];
    entries.forEach(e => {
      const d = e.date || "";
      if (!byDate[d]) { byDate[d] = []; dateOrder.push(d); }
      byDate[d].push(e);
    });
    dateOrder.sort();

    // Table header
    const contentW = doc.page.width - 80;
    const cols   = [80, 130, 90, 80, 65, contentW - 80 - 130 - 90 - 80 - 65];
    const labels = ["Date", "Class", "Time", "Location", "Hours", "Pay"];
    doc.rect(40, y, contentW, 20).fill("#333");
    let x = 40;
    doc.fillColor("#fff").font("Helvetica-Bold").fontSize(9);
    labels.forEach((lbl, i) => {
      doc.text(lbl, x + 4, y + 5, { width: cols[i] - 8, align: i >= 4 ? "right" : "left" });
      x += cols[i];
    });
    y += 20;

    // Rows
    dateOrder.forEach(d => {
      const dayEntries = byDate[d];
      const dayHrs = dayEntries.reduce((s,e) => s + (parseFloat(e.hours)||0), 0);
      const dayPay = dayEntries.reduce((s,e) => s + (parseFloat(e.total)||0), 0);

      // Date header row
      if (y + 20 > doc.page.height - 60) { doc.addPage(); y = 40; }
      doc.rect(40, y, contentW, 20).fill("#f5f5f5");
      x = 40;
      doc.fillColor(col.dark).font("Helvetica-Bold").fontSize(9)
         .text(d, x + 4, y + 5, { width: cols[0] - 8 });
      x += cols[0];
      doc.fillColor(col.mid).font("Helvetica").fontSize(8)
         .text(dayEntries.length + " shift" + (dayEntries.length===1?"":"s"), x + 4, y + 5, { width: cols[1] - 8 });
      x += cols[1] + cols[2] + cols[3];
      doc.fillColor(col.mid).font("Helvetica").fontSize(8)
         .text(round2(dayHrs).toFixed(2) + " hrs", x + 4, y + 5, { width: cols[4] - 8, align: "right" });
      x += cols[4];
      doc.fillColor(col.red).font("Helvetica-Bold").fontSize(8)
         .text("$" + round2(dayPay).toFixed(2), x + 4, y + 5, { width: cols[5] - 8, align: "right" });
      y += 20;

      // Individual entries
      dayEntries.forEach((e, ri) => {
        if (y + 20 > doc.page.height - 60) { doc.addPage(); y = 40; }
        if (ri % 2 === 0) doc.rect(40, y, contentW, 20).fill("#fafafa");
        else doc.rect(40, y, contentW, 20).fill("#fff");
        const isComm = (e.classParty||"") === "Commission";
        x = 40;
        doc.fillColor(col.dark).font("Helvetica").fontSize(8);
        doc.text("", x + 4, y + 5, { width: cols[0] - 8 }); x += cols[0];
        doc.text(isComm ? "Commission" : (e.classParty||""), x + 4, y + 5, { width: cols[1] - 8 }); x += cols[1];
        doc.fillColor(col.mid);
        doc.text(e.time||"", x + 4, y + 5, { width: cols[2] - 8 }); x += cols[2];
        doc.text(e.location||"", x + 4, y + 5, { width: cols[3] - 8 }); x += cols[3];
        doc.text(e.hours ? e.hours + " hrs" : "—", x + 4, y + 5, { width: cols[4] - 8, align: "right" }); x += cols[4];
        doc.fillColor(col.dark).font("Helvetica-Bold");
        doc.text("$" + parseFloat(e.total||0).toFixed(2), x + 4, y + 5, { width: cols[5] - 8, align: "right" });
        doc.moveTo(40, y + 20).lineTo(40 + contentW, y + 20).strokeColor("#eee").lineWidth(0.5).stroke();
        y += 20;
      });
    });

    // Footer total
    if (y + 24 > doc.page.height - 60) { doc.addPage(); y = 40; }
    doc.rect(40, y, contentW, 24).fill(col.red);
    doc.fillColor("#fff").font("Helvetica-Bold").fontSize(10)
       .text("Total", 44, y + 7, { width: contentW - 110 });
    let footerText = "$" + totalPay.toFixed(2);
    if (gstAmt > 0) footerText += " + GST $" + gstAmt.toFixed(2) + " = $" + totalOwing.toFixed(2);
    doc.text(footerText, doc.page.width - 140, y + 7, { width: 95, align: "right" });
    y += 30;

    // Generated footer
    doc.fillColor(col.light).font("Helvetica").fontSize(8)
       .text("Generated " + new Date().toLocaleString("en-CA") + " · " + brand, 40, doc.page.height - 40, { align: "center", width: doc.page.width - 80, lineBreak: false });

    doc.end();
  });
}

// ── Build per-user CSV for pay period ──
function buildStaffPeriodCsv(entries, user, pp) {
  const isC  = (user.type||"").toLowerCase() === "contractor";
  const gst_ = isC && user.chargeGST;
  const rows = [["Date","Class/Party","Time","Location","Hours","Rate","Pay","Notes"]];

  let totalHours = 0, totalPay = 0;
  const sorted = [...entries].sort((a, b) => String(a.date||"").localeCompare(String(b.date||"")));
  sorted.forEach(e => {
    const hrs  = parseFloat(e.hours || 0);
    const rate = parseFloat(e.rate || e.hourlyRate || 0);
    const pay  = parseFloat(e.total || 0);
    const srf  = parseFloat(e.studioRentalFee || 0);
    totalHours += hrs;
    totalPay   += pay - srf;
    rows.push([
      String(e.date||"").slice(0,10),
      e.classParty || e.class_party || "",
      e.time || "",
      e.location || "",
      hrs.toFixed(2),
      rate.toFixed(2),
      pay.toFixed(2),
      e.notes || ""
    ]);
  });
  totalHours = round2(totalHours);
  totalPay   = round2(totalPay);
  const gstAmt    = gst_ ? round2(totalPay * 0.05) : 0;
  const totalOwing = round2(totalPay + gstAmt);

  // Summary rows
  rows.push([]);
  rows.push(["","","","","Total Hours","",totalHours.toFixed(2),""]);
  rows.push(["","","","","Gross Pay","",totalPay.toFixed(2),""]);
  if (gstAmt > 0) rows.push(["","","","","GST (5%)","",gstAmt.toFixed(2),""]);
  rows.push(["","","","","Total Owing","",totalOwing.toFixed(2),""]);

  return rows.map(r => r.map(c => `"${String(c).replace(/"/g, '""')}"`).join(",")).join("\r\n");
}

// ── Build staff earnings PDF (date range earnings report) ──
async function buildStaffEarningsPdf(user, tid, dateFrom, dateTo) {
  let rows;
  if (dateFrom && dateTo) {
    const result = await query(
      `SELECT * FROM entries WHERE tenant_id=$1 AND user_email=$2 AND date >= $3 AND date <= $4 ORDER BY date ASC, server_ts ASC`,
      [tid, normEmail(user.email), dateFrom, dateTo]
    );
    rows = result.rows;
  } else {
    const result = await query(
      `SELECT * FROM entries WHERE tenant_id=$1 AND user_email=$2 ORDER BY date ASC`,
      [tid, normEmail(user.email)]
    );
    rows = result.rows;
  }

  // Group by pay period
  const periods = {};
  const periodOrder = [];
  rows.forEach(r => {
    const ps = r.pay_period_start instanceof Date ? r.pay_period_start.toISOString().slice(0,10) : String(r.pay_period_start||"").slice(0,10);
    const pe = r.pay_period_end   instanceof Date ? r.pay_period_end.toISOString().slice(0,10)   : String(r.pay_period_end||"").slice(0,10);
    const key = ps + "|" + pe;
    if (!periods[key]) { periods[key] = { ppStart: ps, ppEnd: pe, entries: [], totalHours: 0, totalPay: 0 }; periodOrder.push(key); }
    const entry = formatEntry(r);
    periods[key].entries.push(entry);
    periods[key].totalHours = round2(periods[key].totalHours + (parseFloat(entry.hours)||0));
    periods[key].totalPay   = round2(periods[key].totalPay   + (parseFloat(entry.total)||0) - (parseFloat(entry.studioRentalFee)||0));
  });

  const isC  = (user.type||"").toLowerCase() === "contractor";
  const periodList = periodOrder.map(k => {
    const p = periods[k];
    const gst = (isC && user.chargeGST) ? round2(p.totalPay * 0.05) : 0;
    return { ...p, gst, totalOwing: round2(p.totalPay + gst) };
  });

  const grandTotal = round2(periodList.reduce((s,p) => s + p.totalPay, 0));
  const grandHours = round2(periodList.reduce((s,p) => s + p.totalHours, 0));
  const grandGst   = round2(periodList.reduce((s,p) => s + p.gst, 0));
  const grandOwing = round2(periodList.reduce((s,p) => s + p.totalOwing, 0));

  return new Promise((resolve, reject) => {
    const doc  = new PDFDocument({ margin: 40, size: "LETTER" });
    const bufs = [];
    doc.on("data", d => bufs.push(d));
    doc.on("end",  () => resolve(Buffer.concat(bufs)));
    doc.on("error", reject);

    const brand = CONFIG.BRAND_NAME || "Aradia Fitness";
    const col   = { red: "${CONFIG.BRAND_COLOR_PRIMARY}", dark: "#222", mid: "#555", light: "#888" };
    const range = (dateFrom && dateTo) ? dateFrom + " to " + dateTo : "All time";

    // Header
    doc.rect(40, 40, doc.page.width - 80, 36).fill(col.red);
    doc.fillColor("#fff").fontSize(16).font("Helvetica-Bold")
       .text("Earnings Statement — " + (user.name || user.email), 50, 50, { width: doc.page.width - 100 });

    // Summary row
    let y = 88;
    doc.fillColor(col.dark).fontSize(10).font("Helvetica");
    const summaryItems = [
      "Period: " + range,
      "Total Hours: " + grandHours.toFixed(2),
      "Gross Pay: $" + grandTotal.toFixed(2),
    ];
    if (grandGst > 0) {
      summaryItems.push("GST: $" + grandGst.toFixed(2));
      summaryItems.push("Total Owing: $" + grandOwing.toFixed(2));
    }
    doc.text(summaryItems.join("   |   "), 40, y, { width: doc.page.width - 80 });
    y += 24;

    // Table header
    const cols   = [80, 160, 90, 70, 75];
    const labels = ["Date", "Class", "Time", "Hours", "Pay"];
    doc.rect(40, y, doc.page.width - 80, 20).fill("#333");
    let x = 40;
    doc.fillColor("#fff").font("Helvetica-Bold").fontSize(9);
    labels.forEach((lbl, i) => {
      doc.text(lbl, x + 3, y + 5, { width: cols[i] - 6, align: i >= 3 ? "right" : "left" });
      x += cols[i];
    });
    y += 20;

    // Period sections
    periodList.forEach(p => {
      // Period header
      if (y + 20 > doc.page.height - 60) { doc.addPage(); y = 40; }
      doc.rect(40, y, doc.page.width - 80, 20).fill("#f5f5f5");
      doc.fillColor(col.dark).font("Helvetica-Bold").fontSize(9)
         .text(p.ppStart + " → " + p.ppEnd, 43, y + 5, { width: doc.page.width - 220 });
      doc.fillColor(col.red).font("Helvetica-Bold").fontSize(9)
         .text("$" + p.totalPay.toFixed(2), doc.page.width - 120, y + 5, { width: 75, align: "right" });
      y += 20;

      // Entries
      p.entries.forEach(e => {
        if (y + 16 > doc.page.height - 60) { doc.addPage(); y = 40; }
        const isComm = (e.classParty||"") === "Commission";
        x = 40;
        doc.fillColor(col.dark).font("Helvetica").fontSize(8);
        doc.text(e.date||"", x + 3, y + 3, { width: cols[0] - 6 }); x += cols[0];
        doc.text(isComm ? "Commission" : (e.classParty||""), x + 3, y + 3, { width: cols[1] - 6 }); x += cols[1];
        doc.fillColor(col.mid);
        doc.text(e.time||"", x + 3, y + 3, { width: cols[2] - 6 }); x += cols[2];
        doc.text(e.hours ? e.hours + " hrs" : "—", x + 3, y + 3, { width: cols[3] - 6, align: "right" }); x += cols[3];
        doc.fillColor(col.dark).font("Helvetica-Bold");
        doc.text("$" + parseFloat(e.total||0).toFixed(2), x + 3, y + 3, { width: cols[4] - 6, align: "right" });
        doc.moveTo(40, y + 16).lineTo(doc.page.width - 40, y + 16).strokeColor("#eee").lineWidth(0.5).stroke();
        y += 16;
      });
    });

    // Grand total footer
    if (y + 24 > doc.page.height - 60) { doc.addPage(); y = 40; }
    doc.rect(40, y, doc.page.width - 80, 24).fill(col.red);
    doc.fillColor("#fff").font("Helvetica-Bold").fontSize(10)
       .text("TOTAL", 43, y + 7, { width: doc.page.width - 180 });
    doc.text("$" + grandTotal.toFixed(2), doc.page.width - 160, y + 7, { width: 115, align: "right" });
    y += 30;

    // Generated footer
    doc.fillColor(col.light).font("Helvetica").fontSize(8)
       .text("Generated " + new Date().toLocaleString("en-CA") + " · " + brand, 40, doc.page.height - 40, { align: "center", width: doc.page.width - 80, lineBreak: false });

    doc.end();
  });
}

// ─────────────────────────────────────────
//  DATE HELPERS
// ─────────────────────────────────────────
function todayStr() {
  return new Date(new Date().toLocaleString("en-US", { timeZone: "America/Edmonton" })).toISOString().slice(0, 10);
}

function nowStr() {
  return new Date(new Date().toLocaleString("en-US", { timeZone: "America/Edmonton" })).toISOString().replace("T", " ").slice(0, 19);
}

function addDays(dateStr, n) {
  const d = new Date(dateStr + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

// ── MST → UTC time helpers for configurable automation times ──
// MST is always UTC-7 (fixed, no DST switching — predictable for payroll)
function mstToUtcHour(mstTime) {
  const [h] = (mstTime || "12:00").split(":").map(Number);
  return (h + 7) % 24;
}
function isTimeToRun(mstTime) {
  return new Date().getUTCHours() === mstToUtcHour(mstTime);
}
const _cronSentToday = new Set();
function cronGuard(key) {
  const k = key + "_" + todayStr();
  if (_cronSentToday.has(k)) return false;
  _cronSentToday.add(k);
  for (const v of _cronSentToday) { if (!v.endsWith(todayStr())) _cronSentToday.delete(v); }
  return true;
}

// Cached settings for sync pay period calls (refreshed async)
let _ppSettingsCache = null;
async function refreshPpSettingsCache() {
  _ppSettingsCache = await getAdminSettings();
}

function getPpSettings() {
  // Fall back to env config if cache not yet populated
  return _ppSettingsCache || {
    ppFrequency:       "biweekly",
    ppAnchorDate:      CONFIG.PAY_PERIOD_ANCHOR,
    ppWeekStartDay:    "1",
    ppSemiMonthlyDay1: "1",
    ppSemiMonthlyDay2: "16",
    ppMonthlyStartDay: "1",
    ppHistoryCount:    CONFIG.PAY_PERIOD_HISTORY_COUNT,
  };
}

function listPayPeriods(settingsOverride) {
  const s     = settingsOverride || getPpSettings();
  const freq  = s.ppFrequency || "biweekly";
  const count = parseInt(s.ppHistoryCount || 8) || 8;
  const futureCount = 6; // number of future pay periods to include
  const today = todayStr();

  if (freq === "weekly") {
    // Weekly: anchor to most recent occurrence of chosen weekday
    const dayOfWeek = parseInt(s.ppWeekStartDay || "1"); // 0=Sun,1=Mon...
    const todayObj  = new Date(today + "T00:00:00Z");
    const todayDay  = todayObj.getUTCDay();
    const diff      = (todayDay - dayOfWeek + 7) % 7;
    const curStart  = addDays(today, -diff);
    const periods   = [];
    for (let i = 0; i < count; i++) {
      const start = addDays(curStart, -i * 7);
      periods.push({ start, end: addDays(start, 6) });
    }
    return _prependFuturePeriods(periods, futureCount);
  }

  if (freq === "semimonthly") {
    // Semi-monthly: two periods per month on fixed days
    const d1 = parseInt(s.ppSemiMonthlyDay1 || "1");
    const d2 = parseInt(s.ppSemiMonthlyDay2 || "16");
    const periods = [];
    const todayObj = new Date(today + "T00:00:00Z");
    let yr = todayObj.getUTCFullYear();
    let mo = todayObj.getUTCMonth() + 1; // 1-12
    let day = todayObj.getUTCDate();
    // Find current period start
    let curStart, curEnd;
    if (day >= d2) {
      // In second half of month
      curStart = `${yr}-${String(mo).padStart(2,"0")}-${String(d2).padStart(2,"0")}`;
      const lastDay = new Date(Date.UTC(yr, mo, 0)).getUTCDate();
      curEnd = `${yr}-${String(mo).padStart(2,"0")}-${String(lastDay).padStart(2,"0")}`;
    } else {
      // In first half of month
      curStart = `${yr}-${String(mo).padStart(2,"0")}-${String(d1).padStart(2,"0")}`;
      curEnd   = `${yr}-${String(mo).padStart(2,"0")}-${String(d2-1).padStart(2,"0")}`;
    }
    for (let i = 0; i < count; i++) {
      const sObj = new Date(curStart + "T00:00:00Z");
      sDay = sObj.getUTCDate();
      const sYr = sObj.getUTCFullYear();
      const sMo = sObj.getUTCMonth() + 1;
      let start, end;
      if (i === 0) { start = curStart; end = curEnd; }
      else {
        // Go back one period
        const prev = periods[i-1];
        const prevStartObj = new Date(prev.start + "T00:00:00Z");
        const pvYr = prevStartObj.getUTCFullYear();
        const pvMo = prevStartObj.getUTCMonth() + 1;
        const pvDay = prevStartObj.getUTCDate();
        if (pvDay === d2) {
          // prev was second half → go to first half same month
          start = `${pvYr}-${String(pvMo).padStart(2,"0")}-${String(d1).padStart(2,"0")}`;
          end   = `${pvYr}-${String(pvMo).padStart(2,"0")}-${String(d2-1).padStart(2,"0")}`;
        } else {
          // prev was first half → go to second half of previous month
          const pMo = pvMo === 1 ? 12 : pvMo - 1;
          const pYr = pvMo === 1 ? pvYr - 1 : pvYr;
          const lastDay = new Date(Date.UTC(pYr, pMo, 0)).getUTCDate();
          start = `${pYr}-${String(pMo).padStart(2,"0")}-${String(d2).padStart(2,"0")}`;
          end   = `${pYr}-${String(pMo).padStart(2,"0")}-${String(lastDay).padStart(2,"0")}`;
        }
      }
      periods.push({ start, end });
    }
    return _prependFuturePeriods(periods, futureCount);
  }

  if (freq === "monthly") {
    // Monthly: starts on chosen day each month
    const startDay = parseInt(s.ppMonthlyStartDay || "1");
    const todayObj = new Date(today + "T00:00:00Z");
    let yr = todayObj.getUTCFullYear();
    let mo = todayObj.getUTCMonth() + 1;
    const day = todayObj.getUTCDate();
    if (day < startDay) { mo--; if (mo < 1) { mo = 12; yr--; } }
    const periods = [];
    for (let i = 0; i < count; i++) {
      let pMo = mo - i; let pYr = yr;
      while (pMo < 1) { pMo += 12; pYr--; }
      const start    = `${pYr}-${String(pMo).padStart(2,"0")}-${String(startDay).padStart(2,"0")}`;
      const nextMo   = pMo === 12 ? 1 : pMo + 1;
      const nextYr   = pMo === 12 ? pYr + 1 : pYr;
      const end      = addDays(`${nextYr}-${String(nextMo).padStart(2,"0")}-${String(startDay).padStart(2,"0")}`, -1);
      periods.push({ start, end });
    }
    return _prependFuturePeriods(periods, futureCount);
  }

  // Default: biweekly
  const anchor    = s.ppAnchorDate || CONFIG.PAY_PERIOD_ANCHOR;
  const days      = 14;
  const anchorMs  = new Date(anchor + "T00:00:00Z").getTime();
  const todayMs   = new Date(today  + "T00:00:00Z").getTime();
  const diff      = Math.floor((todayMs - anchorMs) / (days * 86400000));
  const curStart  = addDays(anchor, diff * days);
  const periods   = [];
  for (let i = 0; i < count; i++) {
    const start = addDays(curStart, -i * days);
    periods.push({ start, end: addDays(start, days - 1) });
  }
  return _prependFuturePeriods(periods, futureCount);
}

// Generate future periods by walking forward from the current (first) period
function _prependFuturePeriods(periods, futureCount) {
  if (!periods.length || !futureCount) return periods;
  const current = periods[0];
  const periodDays = Math.round((new Date(current.end+"T00:00:00Z") - new Date(current.start+"T00:00:00Z")) / 86400000) + 1;
  const future = [];
  for (let i = 1; i <= futureCount; i++) {
    const start = addDays(current.start, i * periodDays);
    const end   = addDays(start, periodDays - 1);
    future.push({ start, end });
  }
  future.reverse(); // farthest future first, then closer, then current
  return [...future, ...periods];
}

function getCurrentPayPeriod() {
  // Current period is the first non-future period (where today falls within or is the most recent past)
  const all = listPayPeriods();
  const today = todayStr();
  // Find the period that contains today
  const current = all.find(p => today >= p.start && today <= p.end);
  return current || all.find(p => p.end <= today) || all[0];
}

function getPayPeriodForDate(dateStr) {
  // First try the pre-generated list
  const listed = listPayPeriods().find(p => dateStr >= p.start && dateStr <= p.end);
  if (listed) return listed;

  // Compute the correct period for any date based on pay period settings
  const s    = getPpSettings();
  const freq = s.ppFrequency || "biweekly";

  if (freq === "weekly") {
    const dayOfWeek = parseInt(s.ppWeekStartDay || "1");
    const dObj = new Date(dateStr + "T00:00:00Z");
    const dDay = dObj.getUTCDay();
    const diff = (dDay - dayOfWeek + 7) % 7;
    const start = addDays(dateStr, -diff);
    return { start, end: addDays(start, 6) };
  }

  if (freq === "semimonthly") {
    const d1 = parseInt(s.ppSemiMonthlyDay1 || "1");
    const d2 = parseInt(s.ppSemiMonthlyDay2 || "16");
    const dObj = new Date(dateStr + "T00:00:00Z");
    const yr  = dObj.getUTCFullYear();
    const mo  = dObj.getUTCMonth() + 1;
    const day = dObj.getUTCDate();
    if (day >= d2) {
      const lastDay = new Date(Date.UTC(yr, mo, 0)).getUTCDate();
      return {
        start: `${yr}-${String(mo).padStart(2,"0")}-${String(d2).padStart(2,"0")}`,
        end:   `${yr}-${String(mo).padStart(2,"0")}-${String(lastDay).padStart(2,"0")}`
      };
    } else {
      return {
        start: `${yr}-${String(mo).padStart(2,"0")}-${String(d1).padStart(2,"0")}`,
        end:   `${yr}-${String(mo).padStart(2,"0")}-${String(d2 - 1).padStart(2,"0")}`
      };
    }
  }

  if (freq === "monthly") {
    const startDay = parseInt(s.ppMonthlyStartDay || "1");
    const dObj = new Date(dateStr + "T00:00:00Z");
    let yr  = dObj.getUTCFullYear();
    let mo  = dObj.getUTCMonth() + 1;
    const day = dObj.getUTCDate();
    if (day < startDay) { mo--; if (mo < 1) { mo = 12; yr--; } }
    const start  = `${yr}-${String(mo).padStart(2,"0")}-${String(startDay).padStart(2,"0")}`;
    const nextMo = mo === 12 ? 1 : mo + 1;
    const nextYr = mo === 12 ? yr + 1 : yr;
    const end    = addDays(`${nextYr}-${String(nextMo).padStart(2,"0")}-${String(startDay).padStart(2,"0")}`, -1);
    return { start, end };
  }

  // Biweekly (default)
  const anchor   = s.ppAnchorDate || CONFIG.PAY_PERIOD_ANCHOR;
  const days     = 14;
  const anchorMs = new Date(anchor + "T00:00:00Z").getTime();
  const dateMs   = new Date(dateStr + "T00:00:00Z").getTime();
  const diff     = Math.floor((dateMs - anchorMs) / (days * 86400000));
  const start    = addDays(anchor, diff * days);
  return { start, end: addDays(start, days - 1) };
}

// ─────────────────────────────────────────
//  AUTH HELPERS
// ─────────────────────────────────────────
function esc_html(s){ return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
function normEmail(e) { return String(e || "").trim().toLowerCase(); }
function round2(n) { return Math.round(n * 100) / 100; }
function parseNum(s) { const n = parseFloat(String(s || "").trim()); return isNaN(n) ? null : n; }

// Build list of enabled report recipients from settings
function getEnabledReportRecipients(settings) {
  // If an email is configured, it's enabled — ignore toggle DB values (were corrupted by save race bug)
  return [
    settings.accountantEmail,
    settings.adminEmail1,
    settings.adminEmail2,
    settings.adminEmail3,
  ].filter(Boolean);
}

function getEnabledSupportEmail(settings) {
  if (settings.supportEmail)      return settings.supportEmail;
  if (settings.payrollAdminEmail) return settings.payrollAdminEmail;
  if (settings.adminEmail1)       return settings.adminEmail1;
  return null;
}

function getEnabledPayrollAdmin(settings) {
  if (settings.payrollAdminEmail) return settings.payrollAdminEmail;
  if (settings.adminEmail1)       return settings.adminEmail1;
  return null;
}

async function getDefaultTenantId() {
  const res = await query(`SELECT id FROM tenants WHERE slug = 'default' LIMIT 1`);
  return res.rows[0]?.id || 1;
}

async function resolveEmail(emailOrUsername) {
  const val = normEmail(emailOrUsername);
  if (val.includes("@")) return val;
  const tid = await getDefaultTenantId();
  const res = await query(
    `SELECT email FROM users WHERE tenant_id=$1 AND LOWER(username)=$2 AND is_active=TRUE LIMIT 1`,
    [tid, val]
  );
  return res.rows.length ? res.rows[0].email : val;
}

async function getAuthorizedUser(email, pin) {
  const tid = await getDefaultTenantId();
  const resolved = await resolveEmail(email);
  const res = await query(
    `SELECT * FROM users WHERE tenant_id=$1 AND email=$2 AND pin=$3 AND is_active=TRUE`,
    [tid, resolved, String(pin || "").trim()]
  );
  if (!res.rows.length) return null;
  return formatUser(res.rows[0]);
}

async function getUserByEmail(email) {
  const tid = await getDefaultTenantId();
  const resolved = await resolveEmail(email);
  const res = await query(
    `SELECT * FROM users WHERE tenant_id=$1 AND email=$2 LIMIT 1`,
    [tid, resolved]
  );
  if (!res.rows.length) return null;
  return formatUser(res.rows[0]);
}

function formatUser(row) {
  return {
    email:        row.email,
    name:         row.name || "",
    type:         row.type || "Employee",
    pin:          row.pin  || "",
    username:     row.username || "",
    gstNumber:    row.gst_number || "",
    chargeGST:    !!row.charge_gst,
    emailReports: !!row.email_reports,
    emailBugUpdates: row.email_bug_updates !== false,  // default true
    isActive:         !!row.is_active,
    profilePic:       row.profile_pic || "",
    requiresPinChange: !!row.require_pin_change,
    requiresManualApproval: !!row.requires_approval,
    attachPdfPayroll: row.attach_pdf_payroll !== false,  // default true
    attachCsvPayroll: !!row.attach_csv_payroll,           // default false
    gcalDefaultCalendar: row.gcal_default_calendar || "",
    preferredTheme: row.preferred_theme || "light",
    lastLoginAt: row.last_login_at || null,
    lastSeenAt: row.last_seen_at || null,
    pinChangedAt: row.pin_changed_at || null,
    contractPdfName: row.contract_pdf_name || "",
    setupCleanupRate: row.setup_cleanup_rate != null ? parseFloat(row.setup_cleanup_rate) : null,
    setupCleanupAllowed: !!row.setup_cleanup_allowed,
    canCreateImages: !!row.can_create_images,
    tspsEnabled: !!row.tsps_enabled,
    pushFlags: !!row.push_flags,
    pushPay: !!row.push_pay,
    pushShifts: !!row.push_shifts,
    pushChat: !!row.push_chat,
    emailShifts: row.email_shifts !== false,
    emailShiftsUrgentOnly: !!row.email_shifts_urgent_only,
    shiftFilterKeywords: row.shift_filter_keywords || "",
    frontDeskStaff: !!row.front_desk_staff,
    frontDeskOnly: !!row.front_desk_only,
    adminPermissions: row.admin_permissions || "",
    teaches: row.teaches || "",
  };
}

// ─────────────────────────────────────────
//  LOGIN LOCKOUT (in-memory)
// ─────────────────────────────────────────
const loginAttempts = {};

function checkLockout(email) {
  const key  = normEmail(email);
  const data = loginAttempts[key];
  if (!data) return { locked: false, attemptsLeft: CONFIG.LOGIN_MAX_ATTEMPTS };
  const now = Date.now();
  if (data.lockedUntil && now < data.lockedUntil)
    return { locked: true, secondsLeft: Math.ceil((data.lockedUntil - now) / 1000), attemptsLeft: 0 };
  if (data.lockedUntil && now >= data.lockedUntil) delete loginAttempts[key];
  const win = CONFIG.LOGIN_ATTEMPT_WINDOW * 1000;
  const recent = (data.attempts||[]).filter(t => now - t < win).length;
  return { locked: false, attemptsLeft: Math.max(0, CONFIG.LOGIN_MAX_ATTEMPTS - recent) };
}

function recordFailedAttempt(email) {
  const key = normEmail(email);
  const now = Date.now();
  const win = CONFIG.LOGIN_ATTEMPT_WINDOW * 1000;
  if (!loginAttempts[key]) loginAttempts[key] = { attempts: [], lockedUntil: null };
  const d = loginAttempts[key];
  d.attempts = d.attempts.filter(t => now - t < win);
  d.attempts.push(now);
  if (d.attempts.length >= CONFIG.LOGIN_MAX_ATTEMPTS)
    d.lockedUntil = now + CONFIG.LOGIN_LOCKOUT_SECONDS * 1000;
}

function clearFailedAttempts(email) { delete loginAttempts[normEmail(email)]; }

// ─────────────────────────────────────────
//  ENTRIES HELPERS
// ─────────────────────────────────────────
async function getEntriesForPeriod(email, ppStart, ppEnd, graceDays) {
  const tid = await getDefaultTenantId();
  const res = await query(
    `SELECT * FROM entries
     WHERE tenant_id=$1 AND user_email=$2 AND pay_period_start=$3 AND pay_period_end=$4
     ORDER BY date ASC, server_ts ASC`,
    [tid, normEmail(email), ppStart, ppEnd]
  );
  return res.rows.map(r => formatEntry(r, graceDays));
}

function formatEntry(row, graceDays) {
  // Determine if entry was submitted late (server_ts is after pay_period_end + grace)
  let lateSubmission = false;
  const grace = parseInt(graceDays) || 0;
  if (row.server_ts && row.pay_period_end) {
    const endDate = new Date(row.pay_period_end);
    endDate.setDate(endDate.getDate() + 1 + grace); // midnight after end date + grace days
    const serverTs = new Date(row.server_ts);
    lateSubmission = serverTs >= endDate;
  }
  return {
    id:         row.id,
    date:       row.date instanceof Date ? row.date.toISOString().slice(0,10) : String(row.date||"").slice(0,10),
    location:   row.location   || "",
    time:       row.time       || "",
    classParty: row.class_party|| "",
    hours:      row.hours_offered != null ? String(row.hours_offered) : "",
    rate:       row.hourly_rate != null   ? String(row.hourly_rate)   : "",
    total:      row.total != null         ? String(row.total)         : "",
    notes:      row.notes      || "",
    poleBonus:  !!row.pole_bonus,
    setupMinutes:    parseInt(row.setup_minutes) || 0,
    cleanupMinutes:  parseInt(row.cleanup_minutes) || 0,
    riggingMinutes:  parseInt(row.rigging_minutes) || 0,
    setupCleanupPay: row.setup_cleanup_pay != null ? parseFloat(row.setup_cleanup_pay) : 0,
    studioRentalFee: row.studio_rental_fee != null ? parseFloat(row.studio_rental_fee) : 0,
    flagCorrectionId: row.flag_correction_id || null,
    lateSubmission: lateSubmission,
  };
}

async function getUserSetupCleanupRate(userEmail, settings) {
  const tid = await getDefaultTenantId();
  const r = await query(`SELECT setup_cleanup_rate FROM users WHERE tenant_id=$1 AND LOWER(email)=LOWER($2)`, [tid, normEmail(userEmail)]);
  if (r.rows[0] && r.rows[0].setup_cleanup_rate != null) return parseFloat(r.rows[0].setup_cleanup_rate);
  return parseFloat(settings.setupCleanupBaseRate) || 21.75;
}

async function getAdminSettings() {
  const tid = await getDefaultTenantId();
  const res = await query(`SELECT key, value FROM settings WHERE tenant_id=$1`, [tid]);
  const map = {};
  res.rows.forEach(r => { map[r.key] = r.value; });


  return {
    accountantEmail:      map.accountantEmail      || "",
    adminEmail1:          map.adminEmail1           || "",
    adminEmail2:          map.adminEmail2           || "",
    adminEmail3:          map.adminEmail3           || "",
    autoSendDay:          map.autoSendDay           || "",
    autoRemindersEnabled: map.autoRemindersEnabled  === "true",
    autoSendEnabled:      map.autoSendEnabled       === "true",
    weeklyBonusEnabled:   map.weeklyBonusEnabled !== "false",
    weeklyBonusHours:     parseFloat(map.weeklyBonusHours  || "6")  || 6,
    weeklyBonusAmount:    parseFloat(map.weeklyBonusAmount || "5")  || 5,
    poleBonusEnabled:     map.poleBonusEnabled !== "false",
    poleClassBonus:       parseFloat(map.poleClassBonus    || "5")  || 5,
    bonusExclusions:      map.bonusExclusions || "admin",
    attachPdf:            map.attachPdf  === "true",
    attachQb:             map.attachQb   === "true",
    autoSendPending:      map.autoSendPending === "true",
    notifyLateAdmin:      map.notifyLateAdmin === "true",
    supportEmail:         map.supportEmail || "",
    forceAllPinChange:      map.forceAllPinChange === "true",
    suppressStaffEmails:    map.suppressStaffEmails === "true",
    testModeEnabled:        map.testModeEnabled === "true",
    testModeAllowedEmails:  map.testModeAllowedEmails || "",
    payrollAdminEmail:    map.payrollAdminEmail || "",
    // Email enabled toggles — default to true if not explicitly set
    accountantEmailEnabled:   map.accountantEmailEnabled   !== "false",
    adminEmail1Enabled:       map.adminEmail1Enabled       !== "false",
    adminEmail2Enabled:       map.adminEmail2Enabled       !== "false",
    adminEmail3Enabled:       map.adminEmail3Enabled       !== "false",
    supportEmailEnabled:      map.supportEmailEnabled      !== "false",
    payrollAdminEmailEnabled: map.payrollAdminEmailEnabled !== "false",
    adminEmailsWhitelistExempt: map.adminEmailsWhitelistExempt === "true",
    youtubeLink:                map.youtubeLink || "",
    autoRemindDaysBeforeEnabled: map.autoRemindDaysBeforeEnabled === "true",
    autoRemindDaysBefore:       parseInt(map.autoRemindDaysBefore || "2") || 2,
    autoSendTime:               map.autoSendTime || "12:00",
    autoRemindersTime:          map.autoRemindersTime || "12:00",
    autoRemindBeforeTime:       map.autoRemindBeforeTime || "12:00",
    // Pay period config
    ppFrequency:          map.ppFrequency    || "biweekly",
    ppAnchorDate:         map.ppAnchorDate   || CONFIG.PAY_PERIOD_ANCHOR,
    ppWeekStartDay:       map.ppWeekStartDay || "1",
    ppSemiMonthlyDay1:    map.ppSemiMonthlyDay1 || "1",
    ppSemiMonthlyDay2:    map.ppSemiMonthlyDay2 || "16",
    ppMonthlyStartDay:    map.ppMonthlyStartDay || "1",
    ppHistoryCount:       parseInt(map.ppHistoryCount || "8") || 8,
    // Feature flag — when false, all payroll submissions are auto-approved
    approvalSystemEnabled: CONFIG.APPROVAL_SYSTEM_ENABLED,
    // Setup & Cleanup time tracking
    setupCleanupEnabled:  map.setupCleanupEnabled === "true",
    setupCleanupBaseRate: parseFloat(map.setupCleanupBaseRate || "21.75") || 21.75,
    setupEnabled:         map.setupEnabled !== "false",
    cleanupEnabled:       map.cleanupEnabled !== "false",
    riggingEnabled:       map.riggingEnabled !== "false",
    setupLabel:           map.setupLabel || "",
    cleanupLabel:         map.cleanupLabel || "",
    riggingLabel:         map.riggingLabel || "",
    qboEnabled:           map.qboEnabled === "true",
    // What's New announcement banner
    whatsNewEnabled: map.whatsNewEnabled === "true",
    whatsNewContent: map.whatsNewContent || "",
    whatsNewVersion: map.whatsNewVersion || "",
    whatsNewImage: map.whatsNewImage || "",
    // Late submission grace period
    lateGraceDays: parseInt(map.lateGraceDays || "0") || 0,
    // TSPS scheduling manager
    schedulingManagerEmail: map.schedulingManagerEmail || "",
    // TSPS notification email (receives all shift posts/claims)
    tspsNotifyEmail: map.tspsNotifyEmail || "",
    tspsNotifyEnabled: map.tspsNotifyEnabled !== "false" && map.tspsNotifyEnabled !== undefined ? map.tspsNotifyEnabled === "true" : false,
    // Class Proposals weekly digest
    proposalDigestEmail: map.proposalDigestEmail || "",
    proposalDigestEnabled: map.proposalDigestEnabled === "true",
    proposalNotifyEmail: map.proposalNotifyEmail || "",
    chatExcludedEmails: map.chatExcludedEmails || "",
  };
}

// ─────────────────────────────────────────
//  API ROUTES
// ─────────────────────────────────────────
const api = express.Router();

// ── ping ──────────────────────────────────
api.get("/ping", (req, res) => {
  res.json({ ok: true, version: "ARADIA-TIME-PG-V2.0", now: new Date().toISOString() });
});

// ── migrate (one-time import from Google Sheets) ──
api.post("/migrate", async (req, res) => {
  const { secret } = req.body;
  if (secret !== process.env.MIGRATE_SECRET) return res.json({ ok: false, reason: "Unauthorized" });

  try {
    const sheets = getSheetsClient();
    const tid    = await getDefaultTenantId();
    const results = { users: 0, entries: 0, locations: 0, settings: 0 };

    // Migrate users
    const authVals = await getSheetValues(sheets, "Authorized_Users");
    const { rows: authRows } = parseSheet(authVals);
    for (const r of authRows) {
      if (!r.Email) continue;
      await query(`
        INSERT INTO users (tenant_id, email, name, type, pin, username, is_active, charge_gst, gst_number, email_reports, profile_pic)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT (tenant_id, email) DO UPDATE SET
          name=EXCLUDED.name, type=EXCLUDED.type, pin=EXCLUDED.pin,
          username=EXCLUDED.username, is_active=EXCLUDED.is_active,
          charge_gst=EXCLUDED.charge_gst, gst_number=EXCLUDED.gst_number,
          email_reports=EXCLUDED.email_reports, profile_pic=EXCLUDED.profile_pic
      `, [
        tid,
        normEmail(r.Email),
        r.Name || "",
        r.Type || "Employee",
        r.PIN  || "",
        r.Username || r.UserName || "",
        String(r.IsActive || "").toUpperCase() === "TRUE",
        String(r.ChargeGST || "").toUpperCase() === "TRUE",
        r.GSTNumber || "",
        String(r.EmailReports || "").toUpperCase() === "TRUE",
        r.ProfilePicture || "",
      ]);
      results.users++;
    }

    // Migrate entries
    const entryVals = await getSheetValues(sheets, "Timesheet_Entries");
    const { rows: entryRows } = parseSheet(entryVals);
    for (const r of entryRows) {
      if (!r.EntryId || !r.Email) continue;
      await query(`
        INSERT INTO entries (id, tenant_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, notes, pole_bonus)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        ON CONFLICT (id) DO NOTHING
      `, [
        r.EntryId,
        tid,
        normEmail(r.Email),
        r.Name || "",
        r.Type || "Employee",
        r.PayPeriodStart || null,
        r.PayPeriodEnd   || null,
        r.Date           || null,
        r.Location       || "",
        r.Time           || "",
        r.ClassParty     || "",
        parseNum(r.HoursOffered),
        parseNum(r.HourlyRate),
        parseNum(r.Total),
        r.Notes          || "",
        String(r.PoleBonus || "").toUpperCase() === "TRUE",
      ]);
      results.entries++;
    }

    // Migrate locations
    const locVals = await getSheetValues(sheets, "Locations");
    const locRows = locVals.slice(1);
    for (let i = 0; i < locRows.length; i++) {
      const name = String(locRows[i][0] || "").trim();
      if (!name) continue;
      await query(`
        INSERT INTO locations (tenant_id, name, sort_order)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
      `, [tid, name, i]);
      results.locations++;
    }

    // Migrate settings
    const settingVals = await getSheetValues(sheets, "Admin_Settings");
    const { rows: settingRows } = parseSheet(settingVals);
    for (const r of settingRows) {
      if (!r.Key) continue;
      await query(`
        INSERT INTO settings (tenant_id, key, value)
        VALUES ($1, $2, $3)
        ON CONFLICT (tenant_id, key) DO UPDATE SET value=EXCLUDED.value
      `, [tid, r.Key, r.Value || ""]);
      results.settings++;
    }

    res.json({ ok: true, results });
  } catch (e) {
    res.json({ ok: false, reason: e.message });
  }
});

// ── login ─────────────────────────────────
api.post("/login", async (req, res) => {
  try {
    const { email, pin } = req.body;
    if (!email) return res.json({ ok: false, reason: "Email is required." });
    if (!pin)   return res.json({ ok: false, reason: "PIN is required." });

    const lockout = checkLockout(email);
    if (lockout.locked) return res.json({ ok: false, reason: "Too many failed attempts.", locked: true, secondsLeft: lockout.secondsLeft, attemptsLeft: 0 });

    // Support username login
    let loginEmail = normEmail(email);
    if (!loginEmail.includes("@")) {
      const tid = await getDefaultTenantId();
      const res2 = await query(
        `SELECT email FROM users WHERE tenant_id=$1 AND (LOWER(username)=$2) AND is_active=TRUE LIMIT 1`,
        [tid, loginEmail]
      );
      if (res2.rows.length) loginEmail = res2.rows[0].email;
    }

    const user = await getAuthorizedUser(loginEmail, pin);
    if (!user) {
      recordFailedAttempt(loginEmail);
      const postLockout = checkLockout(loginEmail);
      if (postLockout.locked) {
        return res.json({ ok: false, reason: "Too many failed attempts. Account locked for 30 minutes.", locked: true, secondsLeft: postLockout.secondsLeft, attemptsLeft: 0 });
      }
      return res.json({ ok: false, reason: "Invalid email or PIN.", attemptsLeft: postLockout.attemptsLeft });
    }

    clearFailedAttempts(loginEmail);

    // Track last login time
    const tidLogin = await getDefaultTenantId();
    await query(`UPDATE users SET last_login_at=NOW(), last_seen_at=NOW() WHERE tenant_id=$1 AND email=$2`, [tidLogin, user.email]).catch(()=>{});

    const pp       = getCurrentPayPeriod();
    const periods  = listPayPeriods();
    const settings = await getAdminSettings();
    const entries  = await getEntriesForPeriod(user.email, pp.start, pp.end, settings.lateGraceDays);
    const tid      = await getDefaultTenantId();
    const locRes   = await query(`SELECT name FROM locations WHERE tenant_id=$1 ORDER BY sort_order, name`, [tid]);
    const locList  = locRes.rows.map(r => r.name);
    const uType    = String(user.type || "").toUpperCase();

    // Count active flags for this user (staff badge)
    const flagCountRes = await query(
      `SELECT COUNT(*) as cnt FROM entry_flags WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2) AND status != 'approved'`,
      [tid, user.email]
    ).catch(() => ({ rows: [{ cnt: 0 }] }));
    const myFlagCount = parseInt(flagCountRes.rows[0]?.cnt || 0);
    if (myFlagCount > 0) console.log(`[FLAG] User ${user.email} has ${myFlagCount} active flag(s)`);

    // Change request count for proposals banner
    const changeReqRes = await query(
      `SELECT COUNT(*) as cnt FROM class_proposals WHERE tenant_id=$1 AND LOWER(proposer_email)=LOWER($2) AND status='change_requested'`,
      [tid, user.email]
    ).catch(() => ({ rows: [{ cnt: 0 }] }));
    const myChangeRequestCount = parseInt(changeReqRes.rows[0]?.cnt || 0);

    // Admin: count proposals needing attention (staff declined changes)
    let proposalAdminAlertCount = 0;
    if (uType === "ADMIN" || hasAdminPermission(user, "proposals", "r")) {
      const adminAlertRes = await query(
        `SELECT COUNT(*) as cnt FROM class_proposals WHERE tenant_id=$1 AND status='change_denied'`,
        [tid]
      ).catch(() => ({ rows: [{ cnt: 0 }] }));
      proposalAdminAlertCount = parseInt(adminAlertRes.rows[0]?.cnt || 0);
    }

    // Check Google Calendar connection
    const gcalRes = await query(
      `SELECT id FROM google_calendar_tokens WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2)`,
      [tid, user.email]
    ).catch(() => ({ rows: [] }));
    const gcalConnected = gcalRes.rows.length > 0;

    res.json({
      ok: true,
      user,
      payPeriods:        periods,
      selectedPayPeriod: pp,
      entries,
      locations:         locList,
      adminSettings:     settings,
      isAdmin:           uType === "ADMIN" || uType === "MODERATOR" || uType === "ACCOUNTANT",
      isModerator:       uType === "MODERATOR",
      isAccountant:      uType === "ACCOUNTANT",
      weeklyBonusActive: false,
      requiresPinChange: !!(user.requiresPinChange || settings.forceAllPinChange),
      ppSettings:        { frequency: settings.ppFrequency, anchorDate: settings.ppAnchorDate, historyCount: settings.ppHistoryCount },
      myFlagCount,
      myChangeRequestCount,
      proposalAdminAlertCount,
      gcalConnected,
    });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── refreshData ───────────────────────────
api.post("/refreshData", async (req, res) => {
  try {
    const { email, pin, ppStart, ppEnd } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    // Update last-seen timestamp for active-user tracking
    const tidSeen = await getDefaultTenantId();
    await query(`UPDATE users SET last_seen_at=NOW() WHERE tenant_id=$1 AND email=$2`, [tidSeen, user.email]).catch(()=>{});

    const pp      = ppStart && ppEnd ? { start: ppStart, end: ppEnd } : getCurrentPayPeriod();
    const settings = await getAdminSettings();
    const entries = await getEntriesForPeriod(user.email, pp.start, pp.end, settings.lateGraceDays);
    const periods = listPayPeriods();
    const tid     = await getDefaultTenantId();
    const locRes  = await query(`SELECT name FROM locations WHERE tenant_id=$1 ORDER BY sort_order, name`, [tid]);
    const locList = locRes.rows.map(r => r.name);
    const uType   = String(user.type || "").toUpperCase();

    // Flag count for staff banner
    const flagCountRes = await query(
      `SELECT COUNT(*) as cnt FROM entry_flags WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2) AND status != 'approved'`,
      [tid, user.email]
    ).catch(() => ({ rows: [{ cnt: 0 }] }));
    const myFlagCount = parseInt(flagCountRes.rows[0]?.cnt || 0);

    // Change request count for proposals banner
    const changeReqRes2 = await query(
      `SELECT COUNT(*) as cnt FROM class_proposals WHERE tenant_id=$1 AND LOWER(proposer_email)=LOWER($2) AND status='change_requested'`,
      [tid, user.email]
    ).catch(() => ({ rows: [{ cnt: 0 }] }));
    const myChangeRequestCount = parseInt(changeReqRes2.rows[0]?.cnt || 0);

    // Admin: count proposals needing attention (staff declined changes)
    let proposalAdminAlertCount = 0;
    if (uType === "ADMIN" || hasAdminPermission(user, "proposals", "r")) {
      const adminAlertRes2 = await query(
        `SELECT COUNT(*) as cnt FROM class_proposals WHERE tenant_id=$1 AND status='change_denied'`,
        [tid]
      ).catch(() => ({ rows: [{ cnt: 0 }] }));
      proposalAdminAlertCount = parseInt(adminAlertRes2.rows[0]?.cnt || 0);
    }

    // Check Google Calendar connection
    const gcalRes2 = await query(
      `SELECT id FROM google_calendar_tokens WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2)`,
      [tid, user.email]
    ).catch(() => ({ rows: [] }));

    res.json({
      ok: true,
      user,
      payPeriods:        periods,
      selectedPayPeriod: pp,
      entries,
      locations:         locList,
      adminSettings:     settings,
      isAdmin:           uType === "ADMIN" || uType === "MODERATOR" || uType === "ACCOUNTANT",
      isModerator:       uType === "MODERATOR",
      isAccountant:      uType === "ACCOUNTANT",
      myFlagCount,
      myChangeRequestCount,
      proposalAdminAlertCount,
      gcalConnected:     gcalRes2.rows.length > 0,
    });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── calendarCounts — lightweight entry counts by date for calendar widget ──
api.post("/calendarCounts", async (req, res) => {
  try {
    const { email, pin, startDate, endDate, includeTimes } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const r = await query(
      `SELECT date::text AS d, COUNT(*)::int AS c FROM entries
       WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2) AND date >= $3 AND date <= $4
       GROUP BY date`,
      [tid, user.email, startDate, endDate]
    );
    const counts = {};
    for (const row of r.rows) counts[row.d] = row.c;
    // Optionally return times per date for duplicate detection
    let times = null;
    if (includeTimes) {
      const tr = await query(
        `SELECT date::text AS d, time AS t FROM entries
         WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2) AND date >= $3 AND date <= $4`,
        [tid, user.email, startDate, endDate]
      );
      times = {};
      for (const row of tr.rows) {
        if (!times[row.d]) times[row.d] = [];
        times[row.d].push(row.t || "");
      }
    }
    res.json({ ok: true, counts, times });
  } catch (e) {
    res.json({ ok: false, reason: e.message });
  }
});

// ── addEntriesBatch ───────────────────────
api.post("/addEntriesBatch", async (req, res) => {
  try {
    const { email, pin, date, location, notes, lines, userAgent, commission, studioRentalFee } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    const isContractor = (user.type || "").toLowerCase() === "contractor";
    if (!date)     return res.json({ ok: false, reason: "Date is required." });
    if (!location) return res.json({ ok: false, reason: "Location is required." });
    if (!lines || !lines.length) return res.json({ ok: false, reason: "Add at least one shift." });

    const pp = getPayPeriodForDate(date);
    if (!pp) return res.json({ ok: false, reason: "Date does not fall in any known pay period." });

    const settings    = await getAdminSettings();
    const bonusHours  = settings.weeklyBonusHours  || 6;
    const bonusAmount = settings.weeklyBonusAmount || 5;
    const poleBonus_  = settings.poleClassBonus    || 5;
    const exclusionList = String(settings.bonusExclusions || "admin")
      .split(",").map(s => s.trim().toLowerCase()).filter(Boolean);

    // Week range for bonus calculation
    const dateObj = new Date(date + "T00:00:00Z");
    const day  = dateObj.getUTCDay();
    const diff = (day === 0) ? -6 : (1 - day);
    const weekStart = new Date(dateObj.getTime() + diff * 86400000).toISOString().slice(0, 10);
    const weekEnd   = addDays(weekStart, 6);

    const tid = await getDefaultTenantId();

    // Existing week hours
    // Existing week hours — exclude bonus-excluded class names (contains match)
    const exclusionLikes = exclusionList.map((_, i) => `AND LOWER(class_party) NOT LIKE $${5 + i}`);
    const exclusionParams = exclusionList.map(ex => `%${ex}%`);
    const weekRes = await query(`
      SELECT COALESCE(SUM(hours_offered), 0) as total
      FROM entries
      WHERE tenant_id=$1 AND user_email=$2 AND date >= $3 AND date <= $4
        ${exclusionLikes.join(" ")}
    `, [tid, user.email, weekStart, weekEnd, ...exclusionParams]);
    const existingWeekHours = parseFloat(weekRes.rows[0].total) || 0;

    let submittedHours = 0;
    for (const ln of lines) {
      const cpLower = String(ln.classParty || "").trim().toLowerCase();
      if (exclusionList.some(ex => cpLower.includes(ex))) continue;
      const h = parseNum(ln.hours);
      if (h !== null) submittedHours += h;
    }

    const totalWeekHours   = round2(existingWeekHours + submittedHours);
    const weeklyBonusActive = settings.weeklyBonusEnabled !== false && !isContractor && totalWeekHours >= bonusHours;

    // Insert entries
    const studioRentalFee_ = parseFloat(studioRentalFee) || 0;
    let rentalFeeApplied = false;
    const insertedEntries = [];
    for (const ln of lines) {
      const classParty = String(ln.classParty || "").trim();
      const time       = String(ln.time || "").trim();
      if (!classParty) return res.json({ ok: false, reason: "Class/Party is required on each line." });
      if (!time)       return res.json({ ok: false, reason: "Time is required on each line." });

      const hours      = parseNum(ln.hours);
      const rate       = parseNum(ln.rate);
      const hasPoleBns = settings.poleBonusEnabled !== false && !isContractor && !!ln.poleBonus && /pole/i.test(classParty);

      // Setup, Cleanup & Rigging pay
      let setupMin_   = parseInt(ln.setupMinutes) || 0;
      let cleanupMin_ = parseInt(ln.cleanupMinutes) || 0;
      let riggingMin_ = parseInt(ln.riggingMinutes) || 0;
      let scPay = 0;
      if (settings.setupCleanupEnabled && (setupMin_ > 0 || cleanupMin_ > 0 || riggingMin_ > 0)) {
        const userScRate = await getUserSetupCleanupRate(user.email, settings);
        scPay = round2(((setupMin_ + cleanupMin_ + riggingMin_) / 60) * userScRate);
      } else { setupMin_ = 0; cleanupMin_ = 0; riggingMin_ = 0; }

      let total = null;
      if (hours !== null && rate !== null) {
        total = round2(hours * (rate + (hasPoleBns ? poleBonus_ : 0)) + scPay);
      } else if (ln.total) {
        total = round2((parseNum(ln.total) || 0) + scPay);
      } else if (scPay > 0) {
        total = scPay;
      }

      const eid = uuidv4();
      await query(`
        INSERT INTO entries (id, tenant_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, notes, pole_bonus, user_agent, setup_minutes, cleanup_minutes, rigging_minutes, setup_cleanup_pay, studio_rental_fee)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
      `, [
        eid, tid, user.email, user.name, user.type,
        pp.start, pp.end, date, location, time, classParty,
        hours, rate, total, notes || "", hasPoleBns, userAgent || "",
        setupMin_, cleanupMin_, riggingMin_, scPay, (!rentalFeeApplied && studioRentalFee_ > 0) ? studioRentalFee_ : 0
      ]);
      if (!rentalFeeApplied && studioRentalFee_ > 0) rentalFeeApplied = true;
      insertedEntries.push({ eid, date, location, time, classParty, hours, rate, total, notes: notes||"" });
    }

    // Insert commission as a separate line item if provided
    const commissionAmt = parseFloat(commission) || 0;
    if (commissionAmt > 0) {
      const cid = uuidv4();
      await query(`
        INSERT INTO entries (id, tenant_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, notes, pole_bonus, user_agent)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
      `, [
        cid, tid, user.email, user.name, user.type,
        pp.start, pp.end, date, location, "", "Commission",
        null, null, commissionAmt, notes || "", false, userAgent || ""
      ]);
      insertedEntries.push({ eid: cid, date, location: "", time: "", classParty: "Commission", hours: null, rate: null, total: commissionAmt, notes: notes||"" });
    }

    // Auto-insert into pending_submissions if this is a past pay period (beyond grace)
    const currentPp = getCurrentPayPeriod();
    const graceDays = settings.lateGraceDays || 0;
    const graceEnd = new Date(pp.end);
    graceEnd.setDate(graceEnd.getDate() + 1 + graceDays); // midnight after end date + grace
    const nowForGrace = new Date();
    if (nowForGrace >= graceEnd && pp.end < currentPp.start && insertedEntries.length) {
      // Remove existing pending for this user+period first, then re-insert all
      await query(`DELETE FROM pending_submissions WHERE tenant_id=$1 AND user_email=$2 AND pay_period_start=$3`, [tid, user.email, pp.start]);
      const allEntries = await getEntriesForPeriod(user.email, pp.start, pp.end);
      for (const e of allEntries) {
        let tot = parseNum(e.total);
        if ((tot === null || tot === 0) && parseNum(e.hours) && parseNum(e.rate)) {
          tot = round2(parseNum(e.hours) * parseNum(e.rate));
        }
        await query(`
          INSERT INTO pending_submissions (tenant_id, entry_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, notes, setup_minutes, cleanup_minutes, rigging_minutes, setup_cleanup_pay)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
          ON CONFLICT DO NOTHING
        `, [tid, e.id, user.email, user.name, user.type, pp.start, pp.end, e.date, e.location, e.time, e.classParty, parseNum(e.hours), parseNum(e.rate), tot, e.notes||"", e.setupMinutes||0, e.cleanupMinutes||0, e.riggingMinutes||0, e.setupCleanupPay||0]);
      }
    }

    // Cross-user duplicate check: find other staff with entries at the same date+time+location
    // Skip "Home" location — multiple staff working from home at the same time is normal
    const otherStaffWarnings = [];
    for (const ie of insertedEntries) {
      if (!ie.time) continue; // skip commissions
      const entryLoc = (ie.location || location || "").trim().toLowerCase();
      if (entryLoc === "home") continue;
      const dupRes = await query(
        `SELECT DISTINCT user_name FROM entries
         WHERE tenant_id=$1 AND LOWER(user_email) != LOWER($2)
           AND date=$3 AND LOWER(time)=LOWER($4) AND LOWER(location)=LOWER($5)`,
        [tid, user.email, ie.date, ie.time, ie.location || location]
      );
      for (const row of dupRes.rows) {
        otherStaffWarnings.push((row.user_name || "Another staff member") + " also has an entry at " + (ie.location || location) + " on " + ie.date + " at " + ie.time);
      }
    }

    const updatedEntries = await getEntriesForPeriod(user.email, pp.start, pp.end, graceDays);
    const resp = { ok: true, entries: updatedEntries, selectedPayPeriod: pp, weeklyBonusActive };
    if (otherStaffWarnings.length) resp.otherStaffWarning = otherStaffWarnings;
    res.json(resp);
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});


// ── addCommission ─────────────────────────
api.post("/addCommission", async (req, res) => {
  try {
    const { email, pin, amount, notes, date } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    const commAmt = parseFloat(amount) || 0;
    if (commAmt <= 0) return res.json({ ok: false, reason: "Commission amount must be greater than $0." });

    const useDate = date || todayStr();
    const pp = getPayPeriodForDate(useDate);
    if (!pp) return res.json({ ok: false, reason: "Date does not fall in any known pay period." });

    const tid = await getDefaultTenantId();
    const ua  = req.headers["user-agent"] || "";

    const entryId = uuidv4();
    await query(`
      INSERT INTO entries (id, tenant_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, notes, pole_bonus, user_agent)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
    `, [entryId, tid, user.email, user.name, user.type, pp.start, pp.end, useDate, "", "", "Commission", null, null, commAmt, notes||"", false, ua]);

    // Auto-insert into pending_submissions if this is a past pay period
    const currentPp = getCurrentPayPeriod();
    if (pp.end < currentPp.start) {
      await query(`
        INSERT INTO pending_submissions (tenant_id, entry_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, notes)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        ON CONFLICT DO NOTHING
      `, [tid, entryId, user.email, user.name, user.type, pp.start, pp.end, useDate, "", "", "Commission", null, null, commAmt, notes||""]);
    }

    const updatedEntries = await getEntriesForPeriod(user.email, pp.start, pp.end);
    res.json({ ok: true, entries: updatedEntries, selectedPayPeriod: pp });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── deleteEntry ───────────────────────────
api.post("/deleteEntry", async (req, res) => {
  try {
    const { email, pin, entryId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    const tid = await getDefaultTenantId();
    const existing = await query(
      `SELECT * FROM entries WHERE id=$1 AND tenant_id=$2 AND user_email=$3`,
      [entryId, tid, user.email]
    );
    if (!existing.rows.length) return res.json({ ok: false, reason: "Entry not found." });

    const row = existing.rows[0];
    if (await demoProtected("entries", "id", entryId, tid)) return res.json({ ok: false, reason: DEMO_BLOCK_MSG });
    await query(`DELETE FROM entries WHERE id=$1`, [entryId]);
    await query(`DELETE FROM pending_submissions WHERE tenant_id=$1 AND entry_id=$2`, [tid, entryId]);

    const ppStart = row.pay_period_start instanceof Date ? row.pay_period_start.toISOString().slice(0,10) : String(row.pay_period_start).slice(0,10);
    const ppEnd   = row.pay_period_end   instanceof Date ? row.pay_period_end.toISOString().slice(0,10)   : String(row.pay_period_end).slice(0,10);
    const updatedEntries = await getEntriesForPeriod(user.email, ppStart, ppEnd);
    res.json({ ok: true, entries: updatedEntries, selectedPayPeriod: { start: ppStart, end: ppEnd } });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── getEntriesByDate ─────────────────────────────
api.post("/getEntriesByDate", async (req, res) => {
  try {
    const { email, pin, date } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    const tid = await getDefaultTenantId();
    const result = await query(
      `SELECT * FROM entries WHERE tenant_id=$1 AND user_email=$2 AND date=$3 ORDER BY server_ts ASC`,
      [tid, normEmail(email), date]
    );
    const entries = result.rows.map(r => formatEntry(r));
    res.json({ ok: true, entries });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── deleteDay ─────────────────────────────
api.post("/deleteDay", async (req, res) => {
  try {
    const { email, pin, date, ppStart, ppEnd } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    const tid = await getDefaultTenantId();
    // Get entry IDs before deleting so we can clean up pending_submissions
    const toDelete = await query(
      `SELECT id FROM entries WHERE tenant_id=$1 AND user_email=$2 AND date=$3 AND pay_period_start=$4`,
      [tid, user.email, date, ppStart]
    );
    await query(
      `DELETE FROM entries WHERE tenant_id=$1 AND user_email=$2 AND date=$3 AND pay_period_start=$4`,
      [tid, user.email, date, ppStart]
    );
    const delIds = toDelete.rows.map(r => r.id);
    if (delIds.length) await query(`DELETE FROM pending_submissions WHERE tenant_id=$1 AND entry_id = ANY($2)`, [tid, delIds]);

    const updatedEntries = await getEntriesForPeriod(user.email, ppStart, ppEnd);
    res.json({ ok: true, entries: updatedEntries });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── editEntry ─────────────────────────────
api.post("/editEntry", async (req, res) => {
  try {
    const { email, pin, entryId, date, location, time, classParty, hours, rate, notes, poleBonus, setupMinutes, cleanupMinutes, riggingMinutes } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    const settings   = await getAdminSettings();
    const poleBonus_ = settings.poleClassBonus || 5;
    const h  = parseNum(hours);
    const rt = parseNum(rate);
    const hasPoleBns = settings.poleBonusEnabled !== false && !!poleBonus && /pole/i.test(classParty||"");

    // Setup, Cleanup & Rigging pay
    let setupMin_   = parseInt(setupMinutes) || 0;
    let cleanupMin_ = parseInt(cleanupMinutes) || 0;
    let riggingMin_ = parseInt(riggingMinutes) || 0;
    let scPay = 0;
    if (settings.setupCleanupEnabled && (setupMin_ > 0 || cleanupMin_ > 0 || riggingMin_ > 0)) {
      const userScRate = await getUserSetupCleanupRate(user.email, settings);
      scPay = round2(((setupMin_ + cleanupMin_ + riggingMin_) / 60) * userScRate);
    } else { setupMin_ = 0; cleanupMin_ = 0; riggingMin_ = 0; }

    let total = null;
    if (h !== null && rt !== null) total = round2(h * (rt + (hasPoleBns ? poleBonus_ : 0)) + scPay);
    else if (scPay > 0) total = scPay;

    const tid = await getDefaultTenantId();
    const existing = await query(
      `SELECT pay_period_start, pay_period_end FROM entries WHERE id=$1 AND tenant_id=$2 AND user_email=$3`,
      [entryId, tid, user.email]
    );
    if (!existing.rows.length) return res.json({ ok: false, reason: "Entry not found." });

    const row = existing.rows[0];
    const oldPpStart = row.pay_period_start instanceof Date ? row.pay_period_start.toISOString().slice(0,10) : String(row.pay_period_start).slice(0,10);
    const oldPpEnd   = row.pay_period_end   instanceof Date ? row.pay_period_end.toISOString().slice(0,10)   : String(row.pay_period_end).slice(0,10);

    // Determine the correct pay period for the (possibly new) date
    const newPp = getPayPeriodForDate(date);
    const newPpStart = newPp ? newPp.start : oldPpStart;
    const newPpEnd   = newPp ? newPp.end   : oldPpEnd;
    const periodChanged = newPpStart !== oldPpStart || newPpEnd !== oldPpEnd;

    await query(`
      UPDATE entries SET date=$1, location=$2, time=$3, class_party=$4,
        hours_offered=$5, hourly_rate=$6, total=$7, notes=$8, pole_bonus=$9,
        pay_period_start=$12, pay_period_end=$13,
        setup_minutes=$14, cleanup_minutes=$15, rigging_minutes=$17, setup_cleanup_pay=$16
      WHERE id=$10 AND tenant_id=$11
    `, [date, location, time, classParty, h, rt, total, notes || "", hasPoleBns, entryId, tid, newPpStart, newPpEnd, setupMin_, cleanupMin_, scPay, riggingMin_]);

    // Update pending_submissions (late tab) when date changes
    // Remove old pending entry for this entry_id
    await query(`DELETE FROM pending_submissions WHERE tenant_id=$1 AND entry_id=$2`, [tid, entryId]);

    // If the new period is past, re-insert into pending_submissions for the new period
    const currentPp = getCurrentPayPeriod();
    if (newPpEnd < currentPp.start) {
      const tot = total != null ? total : 0;
      await query(`
        INSERT INTO pending_submissions (tenant_id, entry_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, notes, setup_minutes, cleanup_minutes, rigging_minutes, setup_cleanup_pay)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
        ON CONFLICT DO NOTHING
      `, [tid, entryId, user.email, user.name, user.type, newPpStart, newPpEnd, date, location, time, classParty, h, rt, tot, notes || "", setupMin_, cleanupMin_, riggingMin_, scPay]);
    }

    // Return entries for the NEW period + signal if the period changed
    const updatedEntries = await getEntriesForPeriod(user.email, newPpStart, newPpEnd);
    const resp = { ok: true, entries: updatedEntries, selectedPayPeriod: { start: newPpStart, end: newPpEnd } };
    if (periodChanged) resp.periodChanged = true;
    res.json(resp);
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── forgotPin ─────────────────────────────
const forgotPinCooldowns = {};
api.post("/forgotPin", async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.json({ ok: false, reason: "Email is required." });
    const key = normEmail(email);
    const now = Date.now();
    if (forgotPinCooldowns[key] && now - forgotPinCooldowns[key] < CONFIG.FORGOT_PIN_COOLDOWN_SECONDS * 1000)
      return res.json({ ok: false, reason: "Please wait before requesting again." });
    forgotPinCooldowns[key] = now;

    const user = await getUserByEmail(email);
    if (!user) return res.json({ ok: true });

    await sendMail({
      to:      user.email,
      subject: `${CONFIG.BRAND_NAME} — Your PIN`,
      html:    `<p>Hi ${user.name},</p><p>Your PIN is: <strong>${user.pin}</strong></p><p>${CONFIG.BRAND_NAME}</p>`
    });
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── changePin ─────────────────────────────
api.post("/changePin", async (req, res) => {
  try {
    const { email, pin, newPin, username } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!newPin || newPin.length < 4) return res.json({ ok: false, reason: "PIN must be at least 4 digits." });
    const tid = await getDefaultTenantId();
    const uname = username ? String(username).trim() : "";
    if (uname) {
      if (!/^[a-zA-Z0-9.\-]+$/.test(uname))
        return res.json({ ok: false, reason: "Username can only contain letters, numbers, dots, and dashes." });
      const conflict = await query(
        `SELECT id FROM users WHERE tenant_id=$1 AND LOWER(username)=$2 AND email!=$3`,
        [tid, uname.toLowerCase(), user.email]
      );
      if (conflict.rows.length)
        return res.json({ ok: false, reason: "Username already taken. Please choose a different one." });
      await query(`UPDATE users SET pin=$1, require_pin_change=FALSE, username=$2, pin_changed_at=NOW() WHERE tenant_id=$3 AND email=$4`, [newPin, uname, tid, user.email]);
      res.json({ ok: true, username: uname });
    } else {
      await query(`UPDATE users SET pin=$1, require_pin_change=FALSE, pin_changed_at=NOW() WHERE tenant_id=$2 AND email=$3`, [newPin, tid, user.email]);
      res.json({ ok: true });
    }
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── changeEmail ───────────────────────────
api.post("/changeEmail", async (req, res) => {
  try {
    const { email, pin, newEmail } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!newEmail || !newEmail.includes("@")) return res.json({ ok: false, reason: "Invalid email." });
    const tid = await getDefaultTenantId();
    await query(`UPDATE users SET email=$1 WHERE tenant_id=$2 AND email=$3`, [normEmail(newEmail), tid, user.email]);
    res.json({ ok: true, newEmail: normEmail(newEmail) });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── changeUsername ────────────────────────
api.post("/changeUsername", async (req, res) => {
  try {
    const { email, pin, newUsername } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const conflict = await query(
      `SELECT id FROM users WHERE tenant_id=$1 AND LOWER(username)=$2 AND email!=$3`,
      [tid, String(newUsername||"").toLowerCase(), user.email]
    );
    if (conflict.rows.length) return res.json({ ok: false, reason: "Username already taken." });
    await query(`UPDATE users SET username=$1 WHERE tenant_id=$2 AND email=$3`, [newUsername, tid, user.email]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── adminChangeEmail ─────────────────────
api.post("/adminChangeEmail", async (req, res) => {
  try {
    const { email, pin, targetEmail, newEmail } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    if (!newEmail || !newEmail.includes("@")) return res.json({ ok: false, reason: "Invalid email address." });

    const tid = await getDefaultTenantId();
    const oldE = normEmail(targetEmail);
    const newE = normEmail(newEmail);
    if (oldE === newE) return res.json({ ok: true, newEmail: newE });

    // Check for conflicts
    const conflict = await query(`SELECT id FROM users WHERE tenant_id=$1 AND email=$2`, [tid, newE]);
    if (conflict.rows.length) return res.json({ ok: false, reason: "A user with that email already exists." });

    // Update all tables
    await query(`UPDATE users SET email=$1 WHERE tenant_id=$2 AND email=$3`, [newE, tid, oldE]);
    await query(`UPDATE entries SET user_email=$1 WHERE tenant_id=$2 AND user_email=$3`, [newE, tid, oldE]);
    await query(`UPDATE pending_submissions SET user_email=$1 WHERE tenant_id=$2 AND user_email=$3`, [newE, tid, oldE]);
    await query(`UPDATE submission_log SET user_email=$1 WHERE tenant_id=$2 AND user_email=$3`, [newE, tid, oldE]);
    await query(`UPDATE support_messages SET user_email=$1 WHERE tenant_id=$2 AND user_email=$3`, [newE, tid, oldE]);
    await query(`UPDATE payroll_approvals SET user_email=$1 WHERE tenant_id=$2 AND user_email=$3`, [newE, tid, oldE]);
    console.log(`[ADMIN] Email changed: ${oldE} → ${newE} by ${admin.email}`);
    res.json({ ok: true, newEmail: newE });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── adminResetPin ────────────────────────
api.post("/adminResetPin", async (req, res) => {
  try {
    const { email, pin, targetEmail } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    const tgtEmail = normEmail(targetEmail);
    const tgtUser = await query(`SELECT * FROM users WHERE tenant_id=$1 AND email=$2`, [tid, tgtEmail]);
    if (!tgtUser.rows.length) return res.json({ ok: false, reason: "User not found." });

    // Generate random 6-digit PIN
    const newPin = String(Math.floor(100000 + Math.random() * 900000));
    await query(`UPDATE users SET pin=$1, require_pin_change=TRUE, pin_changed_at=NULL WHERE tenant_id=$2 AND email=$3`, [newPin, tid, tgtEmail]);

    // Send PIN via email
    const userName = tgtUser.rows[0].name || tgtEmail;
    const appUrl = `https://${process.env.APP_DOMAIN || "aradiafitness.app"}`;
    const settings = await getAdminSettings();
    const ytLink = settings.youtubeLink || "";

    const ytButton = ytLink
      ? `<div style="text-align:center;margin:16px 0;">
          <a href="${ytLink}" style="display:inline-block;background:#c0392b;color:#fff;text-decoration:none;padding:12px 24px;border-radius:8px;font-weight:700;font-size:14px;">▶ Watch Tutorial</a>
         </div>`
      : "";

    await sendMail({
      to: tgtEmail,
      subject: `Your new login PIN — ${CONFIG.BRAND_NAME}`,
      html: `<div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;">
        <h2 style="color:${CONFIG.BRAND_COLOR_PRIMARY};">${CONFIG.BRAND_NAME}</h2>
        <p>Hi ${userName},</p>
        <p>Your login PIN has been reset by an administrator. Your new temporary PIN is:</p>
        <div style="background:#f5f5f5;border-radius:8px;padding:16px 24px;text-align:center;margin:16px 0;">
          <span style="font-size:28px;font-weight:700;letter-spacing:6px;color:#222;">${newPin}</span>
        </div>
        <p>You will be asked to set a new PIN when you next log in.</p>
        ${ytButton}
        <div style="text-align:center;margin:16px 0;">
          <a href="${appUrl}/guide.html" style="display:inline-block;background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;text-decoration:none;padding:12px 24px;border-radius:8px;font-weight:700;font-size:14px;">📖 User Guide</a>
        </div>
        <p style="color:#888;font-size:12px;margin-top:24px;">If you did not expect this, please contact your administrator.</p>
      </div>`,
    });

    console.log(`[ADMIN] PIN reset for ${tgtEmail} by ${admin.email}`);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── massResetPins ───────────────────────────
api.post("/massResetPins", async (req, res) => {
  try {
    const { email, pin, youtubeLink, respectSuppress } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN") return res.json({ ok: false, reason: "Full admin access required." });

    const tid = await getDefaultTenantId();
    const staffRes = await query(`SELECT * FROM users WHERE tenant_id=$1 AND is_active=TRUE`, [tid]);
    const brand = CONFIG.BRAND_NAME || "Aradia Fitness";
    const appUrl = `https://${process.env.APP_DOMAIN || "aradiafitness.app"}`;
    let count = 0;

    for (const u of staffRes.rows) {
      const newPin = String(Math.floor(100000 + Math.random() * 900000));
      await query(`UPDATE users SET pin=$1, require_pin_change=TRUE WHERE tenant_id=$2 AND email=$3`, [newPin, tid, u.email]);

      const ytButton = youtubeLink
        ? `<div style="text-align:center;margin:16px 0;">
            <a href="${youtubeLink}" style="display:inline-block;background:#c0392b;color:#fff;text-decoration:none;padding:12px 24px;border-radius:8px;font-weight:700;font-size:14px;">▶ Watch Tutorial</a>
           </div>`
        : "";

      await sendMail({
        to: u.email,
        adminReport: !respectSuppress, // bypass suppress unless user chose "respect suppress" button
        subject: `Welcome to ${brand} — Your Login PIN`,
        html: `<div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;">
          <h2 style="color:${CONFIG.BRAND_COLOR_PRIMARY};">${brand}</h2>
          <p>Hi ${u.name || u.email},</p>
          <p>Your account is ready! Use the PIN below to log in:</p>
          <div style="background:#f5f5f5;border-radius:8px;padding:16px 24px;text-align:center;margin:16px 0;">
            <span style="font-size:28px;font-weight:700;letter-spacing:6px;color:#222;">${newPin}</span>
          </div>
          <p>You'll be asked to set a new PIN when you first log in.</p>
          ${ytButton}
          <div style="text-align:center;margin:16px 0;">
            <a href="${appUrl}" style="display:inline-block;background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;text-decoration:none;padding:12px 24px;border-radius:8px;font-weight:700;font-size:14px;">Open ${brand}</a>
          </div>
          <div style="text-align:center;margin:16px 0;">
            <a href="${appUrl}/guide.html" style="display:inline-block;background:#555;color:#fff;text-decoration:none;padding:10px 20px;border-radius:8px;font-weight:700;font-size:13px;">📖 Read the User Guide</a>
          </div>
          <p style="color:#888;font-size:12px;margin-top:24px;">If you did not expect this, please contact your administrator.</p>
        </div>`,
      }).catch(err => console.error(`Mass PIN reset email failed for ${u.email}:`, err));
      count++;
    }

    console.log(`[ADMIN] Mass PIN reset for ${count} users by ${admin.email}`);
    res.json({ ok: true, message: `PIN reset emails sent to ${count} active user(s).` });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── deleteAllEntries (admin danger zone) ──
api.post("/deleteAllEntries", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN") return res.json({ ok: false, reason: "Full admin access required." });

    const tid = await getDefaultTenantId();
    if (CONFIG.DEMO_MODE) return res.json({ ok: false, reason: "Cannot clear all data in demo mode." });
    const result = await query(`DELETE FROM entries WHERE tenant_id=$1`, [tid]);
    const count = result.rowCount || 0;

    // Also clear pending/late submissions
    const lateResult = await query(`DELETE FROM pending_submissions WHERE tenant_id=$1`, [tid]).catch(() => ({ rowCount: 0 }));
    const lateCount = (lateResult && lateResult.rowCount) || 0;

    // Also clear payroll approvals since entries are gone
    await query(`DELETE FROM payroll_approvals WHERE tenant_id=$1`, [tid]).catch(() => {});

    console.log(`[ADMIN DANGER] ${admin.email} deleted ALL entries (${count} rows, ${lateCount} late)`);
    res.json({ ok: true, count, lateCount });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── saveProfilePic ────────────────────────
api.post("/saveProfilePic", async (req, res) => {
  try {
    const { email, pin, imageData } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const sizeKb = Math.round((imageData||"").length * 0.75 / 1024);
    console.log(`saveProfilePic: ${email} — ${sizeKb}KB`);
    await query(`UPDATE users SET profile_pic=$1 WHERE tenant_id=$2 AND email=$3`, [imageData||"", tid, user.email]);
    console.log(`saveProfilePic: saved OK for ${email}`);
    res.json({ ok: true });
  } catch (e) {
    console.error("saveProfilePic error:", e.message);
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── saveContract ─────────────────────────
api.post("/saveContract", async (req, res) => {
  try {
    const { email, pin, targetEmail, pdfData, fileName } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    if (!targetEmail) return res.json({ ok: false, reason: "Target email required." });
    const tid = await getDefaultTenantId();
    const sizeKb = Math.round((pdfData || "").length * 0.75 / 1024);
    console.log(`saveContract: ${targetEmail} — ${sizeKb}KB — file: ${fileName || "(cleared)"}`);
    await query(`UPDATE users SET contract_pdf=$1, contract_pdf_name=$2 WHERE tenant_id=$3 AND LOWER(email)=LOWER($4)`,
      [pdfData || "", fileName || "", tid, targetEmail]);
    res.json({ ok: true });
  } catch (e) {
    console.error("saveContract error:", e.message);
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── getContract ──────────────────────────
api.post("/getContract", async (req, res) => {
  try {
    const { email, pin, targetEmail } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    let lookupEmail = user.email;
    if (targetEmail && targetEmail.toLowerCase() !== user.email.toLowerCase()) {
      const uType = String(user.type || "").toUpperCase();
      if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "ACCOUNTANT")
        return res.json({ ok: false, reason: "Not authorized to view other contracts." });
      lookupEmail = targetEmail;
    }
    const r = await query(`SELECT contract_pdf, contract_pdf_name FROM users WHERE tenant_id=$1 AND LOWER(email)=LOWER($2)`, [tid, lookupEmail]);
    if (!r.rows.length) return res.json({ ok: false, reason: "User not found." });
    res.json({ ok: true, pdfData: r.rows[0].contract_pdf || "", fileName: r.rows[0].contract_pdf_name || "" });
  } catch (e) {
    console.error("getContract error:", e.message);
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── getAdminData ──────────────────────────
api.post("/getAdminData", async (req, res) => {
  try {
    const { email, pin, ppStart, ppEnd } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "ACCOUNTANT") return res.json({ ok: false, reason: "Admin access required." });

    const pp  = (ppStart && ppEnd) ? { start: ppStart, end: ppEnd } : getCurrentPayPeriod();
    const tid = await getDefaultTenantId();

    const entriesRes = await query(`
      SELECT e.*, u.charge_gst, u.gst_number, u.email_reports, u.requires_approval
      FROM entries e
      LEFT JOIN users u ON LOWER(u.email)=LOWER(e.user_email) AND u.tenant_id=e.tenant_id
      WHERE e.tenant_id=$1 AND e.pay_period_start=$2 AND e.pay_period_end=$3
      ORDER BY e.user_email, e.date
    `, [tid, pp.start, pp.end]);

    const groups = {};
    entriesRes.rows.forEach(r => {
      const em = r.user_email;
      if (!groups[em]) {
        groups[em] = {
          email:        em,
          name:         r.user_name || em,
          type:         r.user_type || "Employee",
          chargeGST:    !!r.charge_gst,
          gstNumber:    r.gst_number || "",
          emailReports: !!r.email_reports,
          requiresManualApproval: !!r.requires_approval,
          entries:      [],
          totalHours:   0,
          totalPay:     0,
        };
      }
      const h = parseFloat(r.hours_offered) || 0;
      const t = parseFloat(r.total)         || 0;
      const srf = parseFloat(r.studio_rental_fee) || 0;
      groups[em].totalHours = round2(groups[em].totalHours + h);
      groups[em].totalPay   = round2(groups[em].totalPay   + t - srf);
      groups[em].entries.push(formatEntry(r));
    });

    // Fallback: ensure requiresManualApproval is set from users table directly
    const userFlagsRes = await query(`SELECT email, requires_approval FROM users WHERE tenant_id=$1`, [tid]);
    const userApprMap = {};
    userFlagsRes.rows.forEach(r => { userApprMap[normEmail(r.email)] = !!r.requires_approval; });
    Object.values(groups).forEach(g => {
      const flag = userApprMap[normEmail(g.email)];
      if (flag !== undefined) g.requiresManualApproval = flag;
    });

    // Load entry flags for this period
    const entryFlagsRes = await query(
      `SELECT id, entry_id, user_email, status, flag_note, flagged_by_name, correction_entry_id FROM entry_flags WHERE tenant_id=$1 AND pay_period_start=$2 AND pay_period_end=$3`,
      [tid, pp.start, pp.end]
    );
    const entryFlagMap = {};
    entryFlagsRes.rows.forEach(r => {
      entryFlagMap[r.entry_id] = { id: r.id, status: r.status, flagNote: r.flag_note, flaggedByName: r.flagged_by_name, correctionEntryId: r.correction_entry_id || null };
    });

    const settings = await getAdminSettings();
    res.json({ ok: true, payPeriod: pp, users: Object.values(groups), settings, entryFlagMap });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── sendPayrollReport ─────────────────────
api.post("/sendPayrollReport", async (req, res) => {
  try {
    const { email, pin, ppStart, ppEnd, approvedEmails } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const pp       = { start: ppStart, end: ppEnd };
    const settings = await getAdminSettings();
    const tid      = await getDefaultTenantId();

    const entriesRes = await query(`
      SELECT e.*, u.charge_gst, u.gst_number, u.email_reports, u.requires_approval, u.attach_pdf_payroll, u.attach_csv_payroll
      FROM entries e
      LEFT JOIN users u ON LOWER(u.email)=LOWER(e.user_email) AND u.tenant_id=e.tenant_id
      WHERE e.tenant_id=$1 AND e.pay_period_start=$2 AND e.pay_period_end=$3
    `, [tid, pp.start, pp.end]);

    const groups = {};
    entriesRes.rows.forEach(r => {
      const em = r.user_email;
      if (!groups[em]) groups[em] = { email: em, name: r.user_name||em, type: r.user_type||"Employee", chargeGST: !!r.charge_gst, gstNumber: r.gst_number||"", emailReports: !!r.email_reports, requiresManualApproval: !!r.requires_approval, attachPdfPayroll: r.attach_pdf_payroll !== false, attachCsvPayroll: !!r.attach_csv_payroll, entries: [], totalHours: 0, totalPay: 0 };
      groups[em].totalHours = round2(groups[em].totalHours + (parseFloat(r.hours_offered)||0));
      groups[em].totalPay   = round2(groups[em].totalPay   + (parseFloat(r.total)||0) - (parseFloat(r.studio_rental_fee)||0));
      groups[em].entries.push(formatEntry(r));
    });
    const allUsers = Object.values(groups);

    // APPROVAL SYSTEM DISABLED — include all users regardless of approval state
    // When CONFIG.APPROVAL_SYSTEM_ENABLED is re-enabled, uncomment the block below:
    // const approvedSet = new Set((approvedEmails || []).map(e => normEmail(e)));
    // const users = approvedSet.size > 0
    //   ? allUsers.filter(u => approvedSet.has(normEmail(u.email)))
    //   : allUsers;
    const users = allUsers;

    if (!users.length) return res.json({ ok: false, reason: "No staff entries to include in report." });

    // Load approved late submissions — exclude entries from the period being reported
    // (those are already in the main payroll table)
    const lateGroups = (await loadApprovedLateGroups(tid))
      .filter(g => !(g.ppStart === pp.start && g.ppEnd === pp.end));

    const html = buildPayrollEmailHtml(users, pp, settings, lateGroups);
    const recipients = getEnabledReportRecipients(settings);
    if (!recipients.length) return res.json({ ok: false, reason: "No recipients configured in settings." });

    // Build attachments based on settings — only approved users
    const attachments = [];
    if (settings.attachQb) {
      const csvRows = [["Name","Email","Type","Pay Period Start","Pay Period End","Total Hours","Gross Pay","GST","Total Owing","GST Number"]];
      // Late submissions first (aggregated per user-period group, with real type)
      lateGroups.forEach(g => {
        const type = g.userType || "Employee";
        const isC  = type.toLowerCase() === "contractor";
        let totalHrs = 0, totalPay = 0;
        g.entries.forEach(e => {
          const hrs  = parseFloat(e.hours_offered||e.hours||0);
          const rate = parseFloat(e.hourly_rate||e.rate||0);
          let tot = parseFloat(e.total||0);
          if (tot === 0 && hrs > 0 && rate > 0) tot = round2(hrs * rate);
          const srf = parseFloat(e.studio_rental_fee||e.studioRentalFee||0);
          totalHrs += hrs;
          totalPay += tot - srf;
        });
        totalHrs = round2(totalHrs);
        totalPay = round2(totalPay);
        const gst = (isC && g.chargeGST) ? round2(totalPay * 0.05) : 0;
        csvRows.push([g.name||g.email, g.email, type, g.ppStart, g.ppEnd,
          totalHrs.toFixed(2), totalPay.toFixed(2), gst.toFixed(2),
          round2(totalPay + gst).toFixed(2),
          (isC && g.gstNumber) ? g.gstNumber : ""]);
      });
      // Current period rows
      users.forEach(u => {
        const isC = (u.type||"").toLowerCase() === "contractor";
        const gst = (isC && u.chargeGST) ? round2(u.totalPay * 0.05) : 0;
        csvRows.push([u.name||u.email, u.email, u.type||"Employee", pp.start, pp.end,
          round2(u.totalHours).toFixed(2), round2(u.totalPay).toFixed(2),
          gst.toFixed(2), round2(u.totalPay + gst).toFixed(2),
          (isC && u.gstNumber) ? u.gstNumber : ""]);
      });
      const csv = csvRows.map(r => r.map(c => `"${String(c).replace(/"/g,'""')}"`).join(",")).join("\r\n");
      attachments.push({ filename: `payroll_qb_${pp.start}_${pp.end}.csv`, content: csv });
    }
    if (settings.attachPdf) {
      const pdfBuf = await buildPayrollPdf(users, pp, settings, lateGroups);
      attachments.push({ filename: `payroll_${pp.start}_${pp.end}.pdf`, content: pdfBuf });
    }

    await sendMail({
      to:          recipients[0],
      cc:          recipients.slice(1).join(",") || undefined,
      subject:     `${CONFIG.BRAND_NAME} — Payroll Report ${pp.start} to ${pp.end}`,
      html,
      attachments: attachments.length ? attachments : undefined,
      adminReport: true,
    });

    // Only send individual staff emails to approved users
    for (const u of users) {
      if (u.emailReports && u.email) {
        const staffAttach = [];
        if (u.attachPdfPayroll !== false) {
          const pdfBuf = await buildStaffPeriodPdf(u, tid, pp);
          staffAttach.push({ filename: `pay_summary_${pp.start}_${pp.end}.pdf`, content: pdfBuf });
        }
        if (u.attachCsvPayroll) {
          const csv = buildStaffPeriodCsv(u.entries, u, pp);
          staffAttach.push({ filename: `pay_summary_${pp.start}_${pp.end}.csv`, content: csv });
        }
        await sendMail({
          to:      u.email,
          subject: `${CONFIG.BRAND_NAME} — Your Pay Summary ${pp.start} to ${pp.end}`,
          html:    buildStaffEmailHtml(u, pp, settings),
          attachments: staffAttach.length ? staffAttach : undefined,
        }).catch(() => {});
        sendPush(u.email, "💰 Pay Summary Ready", `Your pay summary for ${pp.start} to ${pp.end} is ready.`, "/", "pay").catch(() => {});
      }
    }

    // Clean up late submissions that were included in the payroll email
    if (lateGroups.length) {
      const allRowIds = lateGroups.flatMap(g => g.rowIds);
      if (allRowIds.length) await query(`DELETE FROM pending_submissions WHERE id = ANY($1)`, [allRowIds]);
      console.log(`sendPayrollReport: cleared ${allRowIds.length} late submission row(s) after including in payroll email.`);
    }

    // Log to sent_reports_log
    const grandTotal = round2(users.reduce((s,u) => s + u.totalPay, 0));
    await query(`INSERT INTO sent_reports_log (tenant_id, sent_by, report_type, pp_start, pp_end, recipients, staff_count, total_pay, summary)
      VALUES ($1,$2,'payroll',$3,$4,$5,$6,$7,$8)`,
      [tid, email, pp.start, pp.end, recipients.join(','), users.length, grandTotal,
       `Payroll report for ${users.length} staff, $${grandTotal.toFixed(2)} total`]);

    res.json({ ok: true, message: `Report sent for ${users.length} approved staff to ${recipients[0]}` });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── saveUserFlag ──────────────────────────
api.post("/saveUserFlag", async (req, res) => {
  try {
    const { email, pin, targetEmail, flag, value } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const colMap = { chargeGST: "charge_gst", emailReports: "email_reports", emailBugUpdates: "email_bug_updates" };
    const col = colMap[flag];
    if (!col) return res.json({ ok: false, reason: "Unknown flag." });

    const tid = await getDefaultTenantId();
    await query(`UPDATE users SET ${col}=$1 WHERE tenant_id=$2 AND email=$3`, [!!value, tid, normEmail(targetEmail)]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});



// ── sendTestEmail ─────────────────────────
api.post("/sendTestEmail", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const settings = await getAdminSettings();
    const allEmails = [settings.accountantEmail, settings.adminEmail1, settings.adminEmail2, settings.adminEmail3, settings.supportEmail, settings.payrollAdminEmail].filter(Boolean);
    if (!allEmails.length) return res.json({ ok: false, reason: "No email addresses configured in settings. Add at least one recipient and save first." });

    const results = [];
    // Send to all configured email addresses
    const targets = [...new Set([
      settings.supportEmail,
      settings.payrollAdminEmail,
      settings.adminEmail1,
      settings.adminEmail2,
      settings.adminEmail3,
      settings.accountantEmail,
    ].filter(Boolean))];

    for (const to of targets) {
      try {
        await sendMail({
          to,
          adminReport: true, // bypass test mode / suppress filtering
          subject: `[${CONFIG.BRAND_NAME}] ✅ Test Email — Settings Working`,
          html: `<div style="font-family:Arial,sans-serif;max-width:580px;">
            <h2 style="background:#2e7d32;color:#fff;padding:14px 18px;border-radius:8px 8px 0 0;margin:0;">✅ Email Delivery Confirmed</h2>
            <div style="border:1px solid #e0e0e0;border-top:0;padding:18px;">
              <p>This is a test email from <strong>${CONFIG.BRAND_NAME}</strong>.</p>
              <p>Your email settings are working correctly. This address (<strong>${to}</strong>) will receive notifications.</p>
              <p style="color:#888;font-size:12px;">Sent at ${new Date().toISOString()}</p>
            </div></div>`
        });
        results.push({ to, ok: true });
        console.log(`Test email sent to ${to}`);
      } catch (err) {
        results.push({ to, ok: false, error: err.message });
        console.error(`Test email failed to ${to}:`, err.message);
      }
    }

    const allOk = results.every(r => r.ok);
    const sent  = results.filter(r => r.ok).map(r => r.to);
    const failed = results.filter(r => !r.ok);
    res.json({
      ok: allOk || sent.length > 0,
      message: sent.length
        ? `Test email sent to: ${sent.join(", ")}` + (failed.length ? ` | Failed: ${failed.map(f=>f.to+' ('+f.error+')').join(", ")}` : "")
        : `All sends failed: ${failed.map(f=>f.to+' — '+f.error).join("; ")}`,
      results
    });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});


// ── debugEmail ────────────────────────────
api.post("/debugEmail", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const settings = await getAdminSettings();
    const report = {
      env: {},
      settings: {
        supportEmail:       settings.supportEmail       || "(not set)",
        payrollAdminEmail:  settings.payrollAdminEmail  || "(not set)",
        adminEmail1:        settings.adminEmail1        || "(not set)",
        accountantEmail:    settings.accountantEmail    || "(not set)",
        testModeEnabled:    settings.testModeEnabled,
        testModeAllowedEmails: settings.testModeAllowedEmails || "(none)",
        autoSendEnabled:    settings.autoSendEnabled,
        autoRemindersEnabled: settings.autoRemindersEnabled,
      },
      smtpTest: null,
    };

    // Test Resend API key by hitting their /domains endpoint
    const resendKey = process.env.RESEND_API_KEY || "";
    const resendFrom = process.env.RESEND_FROM || `(not set — will use: ${CONFIG.BRAND_NAME} <support@aradiafitness.app>)`;
    report.env.RESEND_API_KEY = resendKey ? "✅ Set (" + resendKey.length + " chars)" : "⚠️ NOT SET";
    report.env.RESEND_FROM    = resendFrom;
    // Remove old SMTP fields
    delete report.env.MAIL_USER;
    delete report.env.MAIL_PASS;

    if (resendKey) {
      try {
        const testResend = new Resend(resendKey);
        const { data, error } = await testResend.domains.list();
        if (error) {
          report.smtpTest = "❌ Resend API error: " + (error.message || JSON.stringify(error));
        } else {
          const domains = (data?.data || []).map(d => d.name).join(", ") || "(none verified yet)";
          report.smtpTest = "✅ Resend API connected — verified domains: " + domains;
        }
      } catch (err) {
        report.smtpTest = "❌ Resend connection failed: " + err.message;
      }
    } else {
      report.smtpTest = "⚠️ Skipped — RESEND_API_KEY not set in Railway";
    }

    res.json({ ok: true, report });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── checkSettings (debug) ─────────────────
api.post("/checkSettings", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    const settings = await getAdminSettings();
    res.json({ ok: true, settings });
  } catch (e) {
    res.json({ ok: false, reason: e.message });
  }
});

// ── saveAdminSettings ─────────────────────
api.post("/saveAdminSettings", async (req, res) => {
  try {
    const { email, pin, settings } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const tid  = await getDefaultTenantId();
    const keys = [
      "accountantEmail","adminEmail1","adminEmail2","adminEmail3",
      "autoSendDay","autoRemindersEnabled","autoSendEnabled",
      "weeklyBonusEnabled","weeklyBonusHours","weeklyBonusAmount","poleBonusEnabled","poleClassBonus","bonusExclusions",
      "attachPdf","attachQb",
      "notifyLateAdmin","autoSendPending",
      "ppFrequency","ppAnchorDate","ppWeekStartDay",
      "ppSemiMonthlyDay1","ppSemiMonthlyDay2","ppMonthlyStartDay","ppHistoryCount",
      "supportEmail","payrollAdminEmail","forceAllPinChange","testModeEnabled","testModeAllowedEmails","suppressStaffEmails",
      "accountantEmailEnabled","adminEmail1Enabled","adminEmail2Enabled","adminEmail3Enabled","supportEmailEnabled","payrollAdminEmailEnabled",
      "adminEmailsWhitelistExempt",
      "youtubeLink","autoRemindDaysBeforeEnabled","autoRemindDaysBefore",
      "autoSendTime","autoRemindersTime","autoRemindBeforeTime",
      "setupCleanupEnabled","setupCleanupBaseRate","setupEnabled","cleanupEnabled","riggingEnabled","setupLabel","cleanupLabel","riggingLabel","qboEnabled",
      "whatsNewEnabled","whatsNewContent","whatsNewVersion","whatsNewImage",
      "lateGraceDays",
      "schedulingManagerEmail",
      "tspsNotifyEmail","tspsNotifyEnabled",
      "proposalDigestEmail","proposalDigestEnabled",
      "proposalNotifyEmail",
      "chatExcludedEmails"
    ];
    for (const k of keys) {
      if (settings[k] === undefined) continue;
      await query(`
        INSERT INTO settings (tenant_id, key, value) VALUES ($1,$2,$3)
        ON CONFLICT (tenant_id, key) DO UPDATE SET value=EXCLUDED.value
      `, [tid, k, String(settings[k])]);
    }

    const updated = await getAdminSettings();
    res.json({ ok: true, settings: updated });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Location Management ──────────────────────────
api.post("/getLocations", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    const tid = await getDefaultTenantId();
    const rows = await query(`SELECT id, name, sort_order, address FROM locations WHERE tenant_id=$1 ORDER BY sort_order, name`, [tid]);
    res.json({ ok: true, locations: rows.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/addLocation", async (req, res) => {
  try {
    const { email, pin, name } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    if (!name || !name.trim()) return res.json({ ok: false, reason: "Location name is required." });
    const tid = await getDefaultTenantId();
    const maxSort = await query(`SELECT COALESCE(MAX(sort_order),0)+1 AS next FROM locations WHERE tenant_id=$1`, [tid]);
    const nextSort = maxSort.rows[0]?.next || 1;
    await query(`INSERT INTO locations (tenant_id, name, sort_order) VALUES ($1, $2, $3)`, [tid, name.trim(), nextSort]);
    const rows = await query(`SELECT id, name, sort_order FROM locations WHERE tenant_id=$1 ORDER BY sort_order, name`, [tid]);
    res.json({ ok: true, locations: rows.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/updateLocation", async (req, res) => {
  try {
    const { email, pin, locationId, name } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    if (!name || !name.trim()) return res.json({ ok: false, reason: "Location name is required." });
    const tid = await getDefaultTenantId();
    await query(`UPDATE locations SET name=$1 WHERE id=$2 AND tenant_id=$3`, [name.trim(), locationId, tid]);
    const rows = await query(`SELECT id, name, sort_order FROM locations WHERE tenant_id=$1 ORDER BY sort_order, name`, [tid]);
    res.json({ ok: true, locations: rows.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/deleteLocation", async (req, res) => {
  try {
    const { email, pin, locationId } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    const tid = await getDefaultTenantId();
    if (await demoProtected("locations", "id", locationId, tid)) return res.json({ ok: false, reason: DEMO_BLOCK_MSG });
    await query(`DELETE FROM locations WHERE id=$1 AND tenant_id=$2`, [locationId, tid]);
    const rows = await query(`SELECT id, name, sort_order FROM locations WHERE tenant_id=$1 ORDER BY sort_order, name`, [tid]);
    res.json({ ok: true, locations: rows.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/reorderLocations", async (req, res) => {
  try {
    const { email, pin, orderedIds } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    const tid = await getDefaultTenantId();
    for (let i = 0; i < orderedIds.length; i++) {
      await query(`UPDATE locations SET sort_order=$1 WHERE id=$2 AND tenant_id=$3`, [i + 1, orderedIds[i], tid]);
    }
    const rows = await query(`SELECT id, name, sort_order FROM locations WHERE tenant_id=$1 ORDER BY sort_order, name`, [tid]);
    res.json({ ok: true, locations: rows.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── getStaffList ──────────────────────────
api.post("/getStaffList", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "ACCOUNTANT") return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    const res2 = await query(`SELECT * FROM users WHERE tenant_id=$1 ORDER BY name`, [tid]);
    const staff = res2.rows.map(formatUser);

    // Attach Google Calendar connection status
    const gcalRes = await query(`SELECT LOWER(user_email) as em FROM google_calendar_tokens WHERE tenant_id=$1`, [tid]).catch(() => ({ rows: [] }));
    const gcalSet = new Set(gcalRes.rows.map(r => r.em));
    staff.forEach(s => { s.gcalConnected = gcalSet.has(normEmail(s.email)); });

    res.json({ ok: true, staff });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── addStaffMember ────────────────────────
api.post("/addStaffMember", async (req, res) => {
  try {
    const { email, pin, data } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    if (!data.name)  return res.json({ ok: false, reason: "Name is required." });
    if (!data.email) return res.json({ ok: false, reason: "Email is required." });
    if (!data.pin)   return res.json({ ok: false, reason: "PIN is required." });

    const tid = await getDefaultTenantId();
    // Duplicate username check
    if (data.username && String(data.username).trim()) {
      const uname = String(data.username).trim();
      if (!/^[a-zA-Z0-9.\-]+$/.test(uname))
        return res.json({ ok: false, reason: "Username can only contain letters, numbers, dots, and dashes." });
      const conflict = await query(
        `SELECT id FROM users WHERE tenant_id=$1 AND LOWER(username)=$2 AND email!=$3`,
        [tid, uname.toLowerCase(), normEmail(data.email)]
      );
      if (conflict.rows.length)
        return res.json({ ok: false, reason: "Username already taken. Please choose a different one." });
    }
    await query(`
      INSERT INTO users (tenant_id, email, name, type, pin, username, is_active, charge_gst, gst_number, email_reports, require_pin_change, teaches)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    `, [
      tid, normEmail(data.email), data.name, data.type||"Employee",
      String(data.pin), data.username||"",
      data.isActive !== false,
      !!data.chargeGst, data.gst||"", !!data.emailRpt,
      data.requirePinChange !== false,
      data.teaches || ""
    ]);
    res.json({ ok: true });
  } catch (e) {
    if (e.code === "23505") return res.json({ ok: false, reason: "A user with that email already exists." });
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── deleteStaffMember ─────────────────────
api.post("/deleteStaffMember", async (req, res) => {
  try {
    const { email, pin, targetEmail } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN") return res.json({ ok: false, reason: "Admin access required." });
    if (normEmail(targetEmail) === normEmail(admin.email))
      return res.json({ ok: false, reason: "You cannot delete your own account." });

    const tid = await getDefaultTenantId();
    if (CONFIG.DEMO_MODE) { const du = await query('SELECT is_demo FROM users WHERE tenant_id=$1 AND email=$2', [tid, normEmail(targetEmail)]); if (du.rows.length && du.rows[0].is_demo) return res.json({ ok: false, reason: DEMO_BLOCK_MSG }); }
    await query(`DELETE FROM entries WHERE tenant_id=$1 AND user_email=$2`, [tid, normEmail(targetEmail)]);
    await query(`DELETE FROM pending_submissions WHERE tenant_id=$1 AND user_email=$2`, [tid, normEmail(targetEmail)]);
    await query(`DELETE FROM users   WHERE tenant_id=$1 AND email=$2`,   [tid, normEmail(targetEmail)]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── updateStaffMember ─────────────────────
api.post("/updateStaffMember", async (req, res) => {
  try {
    const { email, pin, targetEmail } = req.body;
    let updates = req.body.updates || {};
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    const settings_ = await getAdminSettings();
    const isPayrollAdmin_ = settings_.payrollAdminEmail && normEmail(admin.email) === normEmail(settings_.payrollAdminEmail);
    if (uType !== "ADMIN" && uType !== "MODERATOR" && !isPayrollAdmin_) return res.json({ ok: false, reason: "Admin access required." });
    // Payroll admin can only change requiresManualApproval
    if (isPayrollAdmin_ && uType !== "ADMIN" && uType !== "MODERATOR") {
      const allowed = { requiresManualApproval: updates.requiresManualApproval };
      updates = allowed;
    }

    const tid    = await getDefaultTenantId();
    // Duplicate username check
    if (updates.username !== undefined && String(updates.username||"").trim()) {
      const uname = String(updates.username).trim();
      if (!/^[a-zA-Z0-9.\-]+$/.test(uname))
        return res.json({ ok: false, reason: "Username can only contain letters, numbers, dots, and dashes." });
      const conflict = await query(
        `SELECT id FROM users WHERE tenant_id=$1 AND LOWER(username)=$2 AND email!=$3`,
        [tid, uname.toLowerCase(), normEmail(targetEmail)]
      );
      if (conflict.rows.length)
        return res.json({ ok: false, reason: "Username already taken. Please choose a different one." });
    }
    // Prevent changing the admin account's type away from ADMIN
    const targetUser = await getUserByEmail(targetEmail);
    if (targetUser && String(targetUser.type||"").toUpperCase() === "ADMIN" && updates.type !== undefined && String(updates.type||"").toUpperCase() !== "ADMIN") {
      delete updates.type; // silently preserve admin type
    }
    const colMap = { name:"name", username:"username", type:"type", gstNumber:"gst_number", isActive:"is_active", chargeGST:"charge_gst", emailReports:"email_reports", requiresPinChange:"require_pin_change", requiresManualApproval:"requires_approval", setupCleanupRate:"setup_cleanup_rate", setupCleanupAllowed:"setup_cleanup_allowed", canCreateImages:"can_create_images", tspsEnabled:"tsps_enabled", pushFlags:"push_flags", pushPay:"push_pay", pushShifts:"push_shifts", emailShifts:"email_shifts", emailShiftsUrgentOnly:"email_shifts_urgent_only", shiftFilterKeywords:"shift_filter_keywords", frontDeskStaff:"front_desk_staff", frontDeskOnly:"front_desk_only", adminPermissions:"admin_permissions", teaches:"teaches" };
    const sets   = [];
    const vals   = [];
    let i = 1;
    for (const [key, col] of Object.entries(colMap)) {
      if (updates[key] === undefined) continue;
      sets.push(`${col}=$${i++}`);
      vals.push(updates[key]);
    }
    // Auto-sync front_desk_staff when teaches changes
    if (updates.teaches !== undefined) {
      const hasFrontDesk = String(updates.teaches || "").toLowerCase().includes("front desk");
      sets.push(`front_desk_staff=$${i++}`);
      vals.push(hasFrontDesk);
    }
    if (!sets.length) return res.json({ ok: true });
    vals.push(tid, normEmail(targetEmail));
    await query(`UPDATE users SET ${sets.join(",")} WHERE tenant_id=$${i++} AND email=$${i}`, vals);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── sendReminderEmails ────────────────────
api.post("/sendReminderEmails", async (req, res) => {
  try {
    const { email, pin, ppStart, ppEnd } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const pp  = { start: ppStart, end: ppEnd };
    const tid = await getDefaultTenantId();

    const submitted = await query(
      `SELECT user_email, COUNT(*) as entry_count FROM entries WHERE tenant_id=$1 AND pay_period_start=$2 GROUP BY user_email`,
      [tid, ppStart]
    );
    const submittedMap = {};
    submitted.rows.forEach(r => { submittedMap[r.user_email] = parseInt(r.entry_count); });

    const staff = await query(
      `SELECT * FROM users WHERE tenant_id=$1 AND is_active=TRUE AND UPPER(type) != 'ADMIN'`,
      [tid]
    );

    const appUrl = `https://${process.env.APP_DOMAIN||"aradiafitness.app"}`;
    let count = 0;
    for (const u of staff.rows) {
      const entryCount = submittedMap[u.email] || 0;
      const isPartial = entryCount > 0;
      const subject = isPartial
        ? `Reminder: Confirm your hours are complete — ${pp.start} to ${pp.end}`
        : `Reminder: Submit your hours for ${pp.start} to ${pp.end}`;
      const body = isPartial
        ? `<p>Hi ${u.name||""},</p><p>You have <strong>${entryCount} shift${entryCount===1?'':'s'}</strong> logged for the pay period ending <strong>${pp.end}</strong>. Please review and confirm all your hours are entered before the deadline.</p><p><a href="${appUrl}">Open Aradia Time</a></p>`
        : `<p>Hi ${u.name||""},</p><p>This is a reminder to submit your hours for the pay period ending <strong>${pp.end}</strong>.</p><p><a href="${appUrl}">Open Aradia Time</a></p>`;
      await sendMail({ to: u.email, subject, html: body }).catch(() => {});
      sendPush(u.email, "📋 Submit Your Hours", `Reminder: submit your hours for ${pp.start} to ${pp.end}.`, "/", "pay").catch(() => {});
      count++;
    }

    res.json({ ok: true, message: `Reminders sent to ${count} staff member(s).` });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});


// ── getMyEarnings ─────────────────────────
api.post("/getMyEarnings", async (req, res) => {
  try {
    const { email, pin, dateFrom, dateTo } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    const tid = await getDefaultTenantId();
    let rows;
    if (dateFrom && dateTo) {
      // Date range query
      const result = await query(
        `SELECT * FROM entries WHERE tenant_id=$1 AND user_email=$2 AND date >= $3 AND date <= $4 ORDER BY date ASC, server_ts ASC`,
        [tid, normEmail(user.email), dateFrom, dateTo]
      );
      rows = result.rows;
    } else {
      // All entries
      const result = await query(
        `SELECT * FROM entries WHERE tenant_id=$1 AND user_email=$2 ORDER BY date ASC`,
        [tid, normEmail(user.email)]
      );
      rows = result.rows;
    }

    // Group by pay period
    const periods = {};
    const periodOrder = [];
    rows.forEach(r => {
      const ps = r.pay_period_start instanceof Date ? r.pay_period_start.toISOString().slice(0,10) : String(r.pay_period_start||"").slice(0,10);
      const pe = r.pay_period_end   instanceof Date ? r.pay_period_end.toISOString().slice(0,10)   : String(r.pay_period_end||"").slice(0,10);
      const key = ps + "|" + pe;
      if (!periods[key]) { periods[key] = { ppStart: ps, ppEnd: pe, entries: [], totalHours: 0, totalPay: 0 }; periodOrder.push(key); }
      const entry = formatEntry(r);
      periods[key].entries.push(entry);
      periods[key].totalHours = round2(periods[key].totalHours + (parseFloat(entry.hours)||0));
      periods[key].totalPay   = round2(periods[key].totalPay   + (parseFloat(entry.total)||0) - (parseFloat(entry.studioRentalFee)||0));
    });

    const isC  = (user.type||"").toLowerCase() === "contractor";
    const result = periodOrder.map(k => {
      const p = periods[k];
      const gst = (isC && user.chargeGST) ? round2(p.totalPay * 0.05) : 0;
      return { ...p, gst, totalOwing: round2(p.totalPay + gst) };
    });

    const grandTotal    = round2(result.reduce((s,p) => s + p.totalPay, 0));
    const grandHours    = round2(result.reduce((s,p) => s + p.totalHours, 0));
    const grandGst      = round2(result.reduce((s,p) => s + p.gst, 0));
    const grandOwing    = round2(result.reduce((s,p) => s + p.totalOwing, 0));

    res.json({ ok: true, periods: result, grandTotal, grandHours, grandGst, grandOwing, user: { name: user.name, email: user.email, type: user.type } });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});


// ── saveUserProfile ───────────────────────
api.post("/saveUserProfile", async (req, res) => {
  try {
    const { email, pin, updates } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const sets = [];
    const vals = [];
    let idx = 1;
    if (updates.emailReports !== undefined) { sets.push(`email_reports=$${idx++}`); vals.push(!!updates.emailReports); }
    if (updates.gstNumber    !== undefined) { sets.push(`gst_number=$${idx++}`);    vals.push(String(updates.gstNumber||"")); }
    if (updates.chargeGST    !== undefined) { sets.push(`charge_gst=$${idx++}`);    vals.push(!!updates.chargeGST); }
    if (updates.attachPdfPayroll !== undefined) { sets.push(`attach_pdf_payroll=$${idx++}`); vals.push(!!updates.attachPdfPayroll); }
    if (updates.attachCsvPayroll !== undefined) { sets.push(`attach_csv_payroll=$${idx++}`); vals.push(!!updates.attachCsvPayroll); }
    if (updates.gcalDefaultCalendar !== undefined) { sets.push(`gcal_default_calendar=$${idx++}`); vals.push(String(updates.gcalDefaultCalendar||"")); }
    if (updates.preferredTheme !== undefined) { sets.push(`preferred_theme=$${idx++}`); vals.push(String(updates.preferredTheme||"light")); }
    if (updates.pushFlags  !== undefined) { sets.push(`push_flags=$${idx++}`);  vals.push(!!updates.pushFlags); }
    if (updates.pushPay    !== undefined) { sets.push(`push_pay=$${idx++}`);    vals.push(!!updates.pushPay); }
    if (updates.pushShifts !== undefined) { sets.push(`push_shifts=$${idx++}`); vals.push(!!updates.pushShifts); }
    if (updates.pushChat   !== undefined) { sets.push(`push_chat=$${idx++}`);   vals.push(!!updates.pushChat); }
    if (updates.emailShifts !== undefined) { sets.push(`email_shifts=$${idx++}`); vals.push(!!updates.emailShifts); }
    if (updates.emailShiftsUrgentOnly !== undefined) { sets.push(`email_shifts_urgent_only=$${idx++}`); vals.push(!!updates.emailShiftsUrgentOnly); }
    if (updates.shiftFilterKeywords !== undefined) { sets.push(`shift_filter_keywords=$${idx++}`); vals.push(String(updates.shiftFilterKeywords || "")); }
    if (updates.teaches !== undefined) {
      sets.push(`teaches=$${idx++}`); vals.push(String(updates.teaches || ""));
      // Auto-sync front_desk_staff based on teaches
      const hasFrontDesk = (updates.teaches || "").toLowerCase().includes("front desk");
      sets.push(`front_desk_staff=$${idx++}`); vals.push(hasFrontDesk);
    }
    if (!sets.length) return res.json({ ok: true });
    vals.push(tid, normEmail(user.email));
    await query(`UPDATE users SET ${sets.join(",")} WHERE tenant_id=$${idx++} AND email=$${idx++}`, vals);
    const updatedUser = await query(`SELECT * FROM users WHERE tenant_id=$1 AND email=$2`, [tid, normEmail(user.email)]);
    const u = updatedUser.rows[0];
    res.json({ ok: true, user: formatUser(u) });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── saveUserFlags (admin sets multiple flags) ──
api.post("/saveUserFlags", async (req, res) => {
  try {
    const { email, pin, targetEmail, flags } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    const tid = await getDefaultTenantId();
    const colMap = { chargeGST: "charge_gst", emailReports: "email_reports", emailBugUpdates: "email_bug_updates", requiresPinChange: "require_pin_change", canCreateImages: "can_create_images", tspsEnabled: "tsps_enabled", pushFlags: "push_flags", pushPay: "push_pay", pushShifts: "push_shifts", pushChat: "push_chat", emailShifts: "email_shifts", emailShiftsUrgentOnly: "email_shifts_urgent_only", shiftFilterKeywords: "shift_filter_keywords", frontDeskStaff: "front_desk_staff", frontDeskOnly: "front_desk_only" };
    const sets = [], vals = [];
    let idx = 1;
    for (const [k, v] of Object.entries(flags || {})) {
      const col = colMap[k];
      if (col) { sets.push(`${col}=$${idx++}`); vals.push(k === "shiftFilterKeywords" ? String(v || "") : (typeof v === "boolean" ? v : !!v)); }
    }
    if (!sets.length) return res.json({ ok: true });
    vals.push(tid, normEmail(targetEmail));
    await query(`UPDATE users SET ${sets.join(",")} WHERE tenant_id=$${idx++} AND email=$${idx++}`, vals);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── restoreEntries ────────────────────────
api.post("/restoreEntries", async (req, res) => {
  try {
    const { email, pin, entries } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const ua  = "";
    for (const e of (entries || [])) {
      const pp = getPayPeriodForDate(e.date);
      if (!pp) continue;
      await query(`
        INSERT INTO entries (id, tenant_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, notes, pole_bonus, user_agent)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        ON CONFLICT DO NOTHING
      `, [e.id || uuidv4(), tid, user.email, user.name, user.type, pp.start, pp.end,
          e.date, e.location||"", e.time||"", e.classParty||"",
          parseNum(e.hours), parseNum(e.rate), parseNum(e.total),
          e.notes||"", !!e.poleBonus, ua]);
    }
    const pp = getCurrentPayPeriod();
    const myEntries = await getEntriesForPeriod(user.email, pp.start, pp.end);
    res.json({ ok: true, myEntries });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});


// ── setPayrollApproval ────────────────────
api.post("/setPayrollApproval", async (req, res) => {
  try {
    const { email, pin, targetEmail, ppStart, ppEnd, approved } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    await query(`
      INSERT INTO payroll_approvals (tenant_id, user_email, pay_period_start, pay_period_end, approved, approved_by, approved_at)
      VALUES ($1,$2,$3,$4,$5,$6,NOW())
      ON CONFLICT (tenant_id, user_email, pay_period_start, pay_period_end)
      DO UPDATE SET approved=$5, approved_by=$6, approved_at=NOW()
    `, [tid, normEmail(targetEmail), ppStart, ppEnd, !!approved, normEmail(email)]);

    // If marking unapproved — email the staff member
    if (!approved) {
      const settings = await getAdminSettings();
      const staffRes = await query(`SELECT * FROM users WHERE tenant_id=$1 AND email=$2`, [tid, normEmail(targetEmail)]);
      const staff = staffRes.rows[0];
      if (staff && staff.email) {
        const replyTo = getEnabledPayrollAdmin(settings) || "";
        sendMail({
          to: staff.email,
          replyTo,
          subject: `[${CONFIG.BRAND_NAME}] Your timesheet submission needs attention`,
          html: `<div style="font-family:Arial,sans-serif;max-width:580px;">
            <h2 style="background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;padding:14px 18px;border-radius:8px 8px 0 0;margin:0;">${CONFIG.BRAND_NAME} — Submission Review Required</h2>
            <div style="border:1px solid #e0e0e0;border-top:0;padding:18px;">
              <p>Hi <strong>${staff.name||staff.email}</strong>,</p>
              <p>Your timesheet submission for the pay period <strong>${ppStart} → ${ppEnd}</strong> has been flagged for review and is currently <strong>not approved</strong> for payroll.</p>
              <p>Please review your entries and resubmit if needed. If you have questions, reply to this email or contact your payroll administrator.</p>
              <p style="color:#888;font-size:12px;">Pay period: ${ppStart} to ${ppEnd}</p>
            </div></div>`
        }).catch(err => console.error("Unapproval notify failed:", err));
        sendPush(staff.email, "⚠️ Timesheet Needs Attention", `Your timesheet for ${ppStart} → ${ppEnd} needs review.`, "/", "pay").catch(() => {});
      }
    }

    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── getPayrollApprovals ───────────────────
api.post("/getPayrollApprovals", async (req, res) => {
  try {
    const { email, pin, ppStart, ppEnd } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    const result = await query(
      `SELECT user_email, approved FROM payroll_approvals WHERE tenant_id=$1 AND pay_period_start=$2 AND pay_period_end=$3`,
      [tid, ppStart, ppEnd]
    );
    // Map: email → approved boolean
    const approvals = {};
    result.rows.forEach(r => { approvals[normEmail(r.user_email)] = !!r.approved; });
    res.json({ ok: true, approvals });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ══════════════════════════════════════════
//  ENTRY FLAGGING SYSTEM
// ══════════════════════════════════════════

// ── Flag an entry ─────────────────────────
api.post("/flagEntry", async (req, res) => {
  try {
    const { email, pin, entryId, note } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "PAYROLL_ADMIN")
      return res.json({ ok: false, reason: "Admin/mod/payroll admin access required." });

    if (!entryId) return res.json({ ok: false, reason: "Entry ID is required." });
    if (!note || !note.trim()) return res.json({ ok: false, reason: "A flag note is required." });

    const tid = await getDefaultTenantId();

    // Look up the entry
    const entryRes = await query(`SELECT * FROM entries WHERE id=$1 AND tenant_id=$2`, [entryId, tid]);
    if (!entryRes.rows.length) return res.json({ ok: false, reason: "Entry not found." });
    const entry = entryRes.rows[0];

    // Check not already flagged
    const existing = await query(`SELECT id FROM entry_flags WHERE entry_id=$1 AND tenant_id=$2 AND status != 'approved'`, [entryId, tid]);
    if (existing.rows.length) return res.json({ ok: false, reason: "This entry is already flagged." });

    // Snapshot the original data for audit trail
    const originalData = {
      date: entry.date, location: entry.location, time: entry.time,
      classParty: entry.class_party, hours: entry.hours_offered,
      rate: entry.hourly_rate, total: entry.total, notes: entry.notes,
    };

    const flagId = uuidv4();
    await query(`
      INSERT INTO entry_flags (id, tenant_id, entry_id, user_email, pay_period_start, pay_period_end, status, flagged_by, flagged_by_name, flag_note, original_data)
      VALUES ($1,$2,$3,$4,$5,$6,'flagged',$7,$8,$9,$10)
    `, [flagId, tid, entryId, entry.user_email, entry.pay_period_start, entry.pay_period_end,
        admin.email, admin.name, note.trim(), JSON.stringify(originalData)]);

    // Send email notification to the staff member
    const settings = await getAdminSettings();
    const ppStart = entry.pay_period_start instanceof Date ? entry.pay_period_start.toISOString().slice(0,10) : String(entry.pay_period_start).slice(0,10);
    const ppEnd   = entry.pay_period_end   instanceof Date ? entry.pay_period_end.toISOString().slice(0,10)   : String(entry.pay_period_end).slice(0,10);
    const entryDate = entry.date instanceof Date ? entry.date.toISOString().slice(0,10) : String(entry.date).slice(0,10);

    sendMail({
      to: entry.user_email,
      subject: `${CONFIG.BRAND_NAME} — Entry Flagged for Review`,
      html: `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
        <div style="background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;padding:16px 20px;border-radius:8px 8px 0 0;">
          <h2 style="margin:0;font-size:18px;">🚩 Entry Flagged for Review</h2>
        </div>
        <div style="padding:20px;border:1px solid #e0e0e0;border-top:0;border-radius:0 0 8px 8px;">
          <p>Hi <strong>${entry.user_name || entry.user_email}</strong>,</p>
          <p>One of your timesheet entries has been flagged by <strong>${admin.name || admin.email}</strong> and needs your attention.</p>
          <div style="background:#fff3e0;border:1px solid #ffcc02;border-radius:8px;padding:14px;margin:16px 0;">
            <div style="font-size:12px;color:#666;margin-bottom:6px;">FLAGGED ENTRY</div>
            <div style="font-weight:700;">${entryDate} — ${entry.class_party || "Shift"}</div>
            <div style="font-size:13px;color:#555;">${entry.location || ""} ${entry.time || ""}</div>
            <div style="font-size:13px;margin-top:4px;">Hours: ${entry.hours_offered || "—"} · Rate: $${entry.hourly_rate || "—"} · Total: $${entry.total || "0"}</div>
          </div>
          <div style="background:#fafafa;border-radius:8px;padding:14px;margin:16px 0;">
            <div style="font-size:12px;color:#666;margin-bottom:4px;">REASON</div>
            <div style="font-size:14px;">${note.trim()}</div>
          </div>
          <p>Please log in to the app to review and correct this entry. You have until the end of the current pay period (<strong>${ppEnd}</strong>) to resolve it.</p>
          <p style="color:#888;font-size:12px;">Pay period: ${ppStart} → ${ppEnd}</p>
        </div>
      </div>`
    }).catch(err => console.error("Flag notification email failed:", err));
    sendPush(entry.user_email, "🚩 Entry Flagged", `An entry on ${entryDate} has been flagged for review.`, "/", "flags").catch(() => {});

    res.json({ ok: true, flagId });
    console.log(`[FLAG] Entry ${entryId} flagged for ${entry.user_email} by ${admin.email} (flagId: ${flagId})`);
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Staff responds to a flag (submits correction) ──
api.post("/respondToFlag", async (req, res) => {
  try {
    const { email, pin, flagId, correctionNote, correctedEntry } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    if (!flagId) return res.json({ ok: false, reason: "Flag ID is required." });
    const tid = await getDefaultTenantId();

    // Verify the flag exists and belongs to this user
    const flagRes = await query(`SELECT * FROM entry_flags WHERE id=$1 AND tenant_id=$2`, [flagId, tid]);
    if (!flagRes.rows.length) return res.json({ ok: false, reason: "Flag not found." });
    const flag = flagRes.rows[0];
    if (normEmail(flag.user_email) !== normEmail(user.email))
      return res.json({ ok: false, reason: "You can only respond to your own flagged entries." });
    if (flag.status === "approved")
      return res.json({ ok: false, reason: "This flag has already been approved." });

    // If staff provides a corrected entry, insert it as a new entry (correction)
    let correctionEntryId = null;
    if (correctedEntry) {
      correctionEntryId = uuidv4();
      const ppS = flag.pay_period_start instanceof Date ? flag.pay_period_start.toISOString().slice(0,10) : String(flag.pay_period_start).slice(0,10);
      const ppE = flag.pay_period_end   instanceof Date ? flag.pay_period_end.toISOString().slice(0,10)   : String(flag.pay_period_end).slice(0,10);
      await query(`
        INSERT INTO entries (id, tenant_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, notes, pole_bonus, flag_correction_id, user_agent)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
      `, [correctionEntryId, tid, user.email, user.name, user.type, ppS, ppE,
          correctedEntry.date || ppS, correctedEntry.location || "", correctedEntry.time || "",
          correctedEntry.classParty || "Shift", parseFloat(correctedEntry.hours)||null,
          parseFloat(correctedEntry.rate)||null, parseFloat(correctedEntry.total)||0,
          correctedEntry.notes || "", false, flagId, req.headers["user-agent"] || ""]);
    }

    await query(`
      UPDATE entry_flags SET status='corrected', correction_entry_id=$1, correction_note=$2, corrected_at=NOW()
      WHERE id=$3
    `, [correctionEntryId, (correctionNote||"").trim(), flagId]);

    // Notify admins that staff responded
    const settings = await getAdminSettings();
    const adminEmails = [settings.adminEmail1, settings.adminEmail2, settings.accountantEmail].filter(Boolean);
    if (adminEmails.length) {
      let correctionHtml = "";
      if (correctedEntry) {
        const cHrs = parseFloat(correctedEntry.hours)||0;
        const cRate = parseFloat(correctedEntry.rate)||0;
        const cTotal = parseFloat(correctedEntry.total)||0;
        correctionHtml = `<div style="background:#e8f5e9;border:1px solid #c8e6c9;border-radius:8px;padding:14px;margin:12px 0;">
          <div style="font-size:12px;color:#666;margin-bottom:6px;">PROPOSED CORRECTION</div>
          <div style="font-weight:700;">${correctedEntry.date || "—"} — ${correctedEntry.classParty || "Shift"}</div>
          <div style="font-size:13px;color:#555;">${correctedEntry.location || ""} ${correctedEntry.time || ""}</div>
          <div style="font-size:13px;margin-top:4px;">Hours: ${cHrs.toFixed(1)} · Rate: $${cRate.toFixed(2)} · Total: <strong>$${cTotal.toFixed(2)}</strong></div>
        </div>`;
      }
      sendMail({
        to: adminEmails[0],
        cc: adminEmails.slice(1).join(",") || undefined,
        subject: `${CONFIG.BRAND_NAME} — Flag Response from ${user.name || user.email}`,
        html: `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
          <div style="background:#e65100;color:#fff;padding:16px 20px;border-radius:8px 8px 0 0;">
            <h2 style="margin:0;font-size:18px;">🟠 Flag Response Received</h2>
          </div>
          <div style="padding:20px;border:1px solid #e0e0e0;border-top:0;border-radius:0 0 8px 8px;">
            <p><strong>${user.name || user.email}</strong> has responded to a flagged entry.</p>
            <div style="background:#fff3e0;border-radius:8px;padding:14px;margin:12px 0;">
              <div style="font-size:12px;color:#666;">ORIGINAL FLAG NOTE</div>
              <div>${flag.flag_note || "—"}</div>
            </div>
            ${correctionNote ? `<div style="background:#fafafa;border-radius:8px;padding:14px;margin:12px 0;"><div style="font-size:12px;color:#666;">STAFF RESPONSE</div><div>${correctionNote}</div></div>` : ""}
            ${correctionHtml}
            <p>Log in to the admin panel to review and approve or take further action.</p>
          </div>
        </div>`,
        adminReport: true,
      }).catch(err => console.error("Flag response notify failed:", err));
    }

    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Admin resolves a flag (edits it themselves) ──
api.post("/resolveFlag", async (req, res) => {
  try {
    const { email, pin, flagId, resolutionNote } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "PAYROLL_ADMIN")
      return res.json({ ok: false, reason: "Admin access required." });

    if (!flagId) return res.json({ ok: false, reason: "Flag ID is required." });
    const tid = await getDefaultTenantId();

    const flagRes = await query(`SELECT * FROM entry_flags WHERE id=$1 AND tenant_id=$2`, [flagId, tid]);
    if (!flagRes.rows.length) return res.json({ ok: false, reason: "Flag not found." });
    if (flagRes.rows[0].status === "approved") return res.json({ ok: false, reason: "Already approved." });

    await query(`
      UPDATE entry_flags SET status='corrected', resolved_by=$1, resolved_by_name=$2, resolution_note=$3, resolved_at=NOW()
      WHERE id=$4
    `, [admin.email, admin.name, (resolutionNote||"").trim(), flagId]);

    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Admin approves a corrected flag ──
api.post("/approveFlag", async (req, res) => {
  try {
    const { email, pin, flagId } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "PAYROLL_ADMIN")
      return res.json({ ok: false, reason: "Admin access required." });

    if (!flagId) return res.json({ ok: false, reason: "Flag ID is required." });
    const tid = await getDefaultTenantId();

    const flagRes = await query(`SELECT * FROM entry_flags WHERE id=$1 AND tenant_id=$2`, [flagId, tid]);
    if (!flagRes.rows.length) return res.json({ ok: false, reason: "Flag not found." });
    const flag = flagRes.rows[0];
    if (flag.status === "approved") return res.json({ ok: false, reason: "Already approved." });

    // If there's a correction entry, the original should be excluded from payroll
    // We mark the original entry with a flag reference so payroll knows to skip it
    // The correction entry takes its place
    await query(`
      UPDATE entry_flags SET status='approved', approved_by=$1, approved_by_name=$2, approved_at=NOW()
      WHERE id=$3
    `, [admin.email, admin.name, flagId]);

    // Notify the staff member their flag was approved
    sendMail({
      to: flag.user_email,
      subject: `${CONFIG.BRAND_NAME} — Flagged Entry Approved ✅`,
      html: `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
        <div style="background:#2e7d32;color:#fff;padding:16px 20px;border-radius:8px 8px 0 0;">
          <h2 style="margin:0;font-size:18px;">✅ Flagged Entry Approved</h2>
        </div>
        <div style="padding:20px;border:1px solid #e0e0e0;border-top:0;border-radius:0 0 8px 8px;">
          <p>Your previously flagged entry has been reviewed and approved by <strong>${admin.name || admin.email}</strong>.</p>
          <p>This entry will be included in your next payroll report.</p>
          ${flag.flag_note ? `<div style="background:#fafafa;border-radius:8px;padding:12px;margin:12px 0;font-size:13px;"><strong>Original flag:</strong> ${flag.flag_note}</div>` : ""}
        </div>
      </div>`
    }).catch(err => console.error("Flag approval notify failed:", err));
    sendPush(flag.user_email, "✅ Flag Approved", "Your flagged entry has been approved for payroll.", "/", "flags").catch(() => {});

    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Unflag an entry (remove flag entirely) ──
api.post("/unflagEntry", async (req, res) => {
  try {
    const { email, pin, flagId } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "PAYROLL_ADMIN")
      return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    await query(`DELETE FROM entry_flags WHERE id=$1 AND tenant_id=$2`, [flagId, tid]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Clear all corrected flags (admin cleanup) ──
api.post("/clearCorrectedFlags", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "PAYROLL_ADMIN")
      return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    const result = await query(
      `DELETE FROM entry_flags WHERE tenant_id=$1 AND status='corrected'`,
      [tid]
    );
    const count = result.rowCount || 0;
    console.log(`[ADMIN] ${email} cleared ${count} corrected flag(s)`);
    res.json({ ok: true, count });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Clear approved flag history from staff or admin view ──
api.post("/clearFlagHistory", async (req, res) => {
  try {
    const { email, pin, target } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "PAYROLL_ADMIN")
      return res.json({ ok: false, reason: "Admin access required." });

    if (target !== "staff" && target !== "admin")
      return res.json({ ok: false, reason: "Target must be 'staff' or 'admin'." });

    const tid = await getDefaultTenantId();
    const col = target === "staff" ? "hidden_from_staff" : "hidden_from_admin";
    const result = await query(
      `UPDATE entry_flags SET ${col}=TRUE WHERE tenant_id=$1 AND status='approved' AND ${col}=FALSE`,
      [tid]
    );
    const count = result.rowCount || 0;
    console.log(`[ADMIN] ${email} cleared ${count} approved flag(s) from ${target} view`);
    res.json({ ok: true, count });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Admin corrects a flagged entry inline → creates correction in current period, auto-approves ──
api.post("/correctFlagEntry", async (req, res) => {
  try {
    const { email, pin, flagId, correctedData } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "PAYROLL_ADMIN")
      return res.json({ ok: false, reason: "Admin access required." });

    if (!flagId) return res.json({ ok: false, reason: "Flag ID is required." });
    if (!correctedData) return res.json({ ok: false, reason: "Corrected data is required." });

    const tid = await getDefaultTenantId();

    // Look up the flag
    const flagRes = await query(`SELECT * FROM entry_flags WHERE id=$1 AND tenant_id=$2`, [flagId, tid]);
    if (!flagRes.rows.length) return res.json({ ok: false, reason: "Flag not found." });
    const flag = flagRes.rows[0];
    if (flag.status === "approved") return res.json({ ok: false, reason: "Already approved." });

    // Look up the original entry for user info
    const origRes = await query(`SELECT * FROM entries WHERE id=$1`, [flag.entry_id]);
    if (!origRes.rows.length) return res.json({ ok: false, reason: "Original entry not found." });
    const orig = origRes.rows[0];

    // Determine current pay period for the correction entry
    const currentPp = getCurrentPayPeriod();

    // Calculate total
    const hours = parseFloat(correctedData.hours) || 0;
    const rate  = parseFloat(correctedData.rate)   || 0;
    let total = round2(hours * rate);
    if (correctedData.total !== undefined && correctedData.total !== null && correctedData.total !== "") {
      total = round2(parseFloat(correctedData.total) || 0);
    }

    // Create the corrected entry in the current pay period
    const corrEntryId = uuidv4();
    await query(`
      INSERT INTO entries (id, tenant_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, notes, pole_bonus, flag_correction_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
    `, [
      corrEntryId, tid, orig.user_email, orig.user_name, orig.user_type,
      currentPp.start, currentPp.end,
      correctedData.date || orig.date,
      correctedData.location || orig.location,
      correctedData.time || orig.time || "",
      correctedData.classParty || orig.class_party,
      hours, rate, total,
      correctedData.notes || `[Flag correction] ${orig.notes || ""}`.trim(),
      !!correctedData.poleBonus,
      flagId
    ]);

    // Auto-approve the flag
    await query(`
      UPDATE entry_flags SET status='approved', correction_entry_id=$1, correction_note=$2, corrected_at=NOW(), approved_by=$3, approved_by_name=$4, approved_at=NOW()
      WHERE id=$5
    `, [corrEntryId, `Admin corrected: ${correctedData.correctionNote || "Adjusted values"}`, admin.email, admin.name, flagId]);

    // Notify the staff member
    const entryDate = correctedData.date || (orig.date instanceof Date ? orig.date.toISOString().slice(0,10) : String(orig.date||"").slice(0,10));
    sendMail({
      to: orig.user_email,
      subject: `${CONFIG.BRAND_NAME} — Flagged Entry Corrected & Approved ✅`,
      html: `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
        <div style="background:#2e7d32;color:#fff;padding:16px 20px;border-radius:8px 8px 0 0;">
          <h2 style="margin:0;font-size:18px;">✅ Entry Corrected & Approved</h2>
        </div>
        <div style="padding:20px;border:1px solid #e0e0e0;border-top:0;border-radius:0 0 8px 8px;">
          <p>Hi <strong>${orig.user_name || orig.user_email}</strong>,</p>
          <p>Your flagged entry has been corrected by <strong>${admin.name || admin.email}</strong> and approved for payroll.</p>
          <div style="background:#fff3e0;border:1px solid #ffcc02;border-radius:8px;padding:14px;margin:16px 0;">
            <div style="font-size:12px;color:#666;margin-bottom:4px;">ORIGINAL FLAG</div>
            <div>${flag.flag_note || "—"}</div>
          </div>
          <div style="background:#e8f5e9;border:1px solid #c8e6c9;border-radius:8px;padding:14px;margin:16px 0;">
            <div style="font-size:12px;color:#666;margin-bottom:4px;">CORRECTED ENTRY</div>
            <div style="font-weight:700;">${entryDate} — ${correctedData.classParty || orig.class_party || "Shift"}</div>
            <div style="font-size:13px;">${correctedData.location || orig.location || ""} · ${hours} hrs × $${rate.toFixed(2)} = $${total.toFixed(2)}</div>
          </div>
          <p>The corrected entry will be included in the current pay period (${currentPp.start} → ${currentPp.end}).</p>
        </div>
      </div>`
    }).catch(err => console.error("Flag correction notify failed:", err));
    sendPush(orig.user_email, "✅ Entry Corrected", "Your flagged entry has been corrected and approved.", "/", "flags").catch(() => {});

    console.log(`[FLAG] Entry corrected: flag ${flagId} → new entry ${corrEntryId} in period ${currentPp.start} → ${currentPp.end} by ${admin.email}`);
    res.json({ ok: true, correctionEntryId: corrEntryId, payPeriod: currentPp });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Get all flags (admin) ─────────────────
api.post("/getFlags", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "PAYROLL_ADMIN")
      return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    const result = await query(`SELECT * FROM entry_flags WHERE tenant_id=$1 ORDER BY flagged_at DESC`, [tid]);

    // Enrich with entry data
    const flags = [];
    for (const f of result.rows) {
      const entryRes = await query(`SELECT * FROM entries WHERE id=$1`, [f.entry_id]);
      const entry = entryRes.rows[0] || null;
      let correctionEntry = null;
      if (f.correction_entry_id) {
        const cRes = await query(`SELECT * FROM entries WHERE id=$1`, [f.correction_entry_id]);
        correctionEntry = cRes.rows[0] || null;
      }
      flags.push({
        ...f,
        flaggedAt: f.flagged_at, correctedAt: f.corrected_at,
        resolvedAt: f.resolved_at, approvedAt: f.approved_at,
        flaggedBy: f.flagged_by, flaggedByName: f.flagged_by_name,
        flagNote: f.flag_note, correctionNote: f.correction_note,
        resolvedBy: f.resolved_by, resolvedByName: f.resolved_by_name,
        resolutionNote: f.resolution_note,
        approvedBy: f.approved_by, approvedByName: f.approved_by_name,
        reminderSent: f.reminder_sent,
        hiddenFromStaff: !!f.hidden_from_staff,
        hiddenFromAdmin: !!f.hidden_from_admin,
        ppStart: f.pay_period_start instanceof Date ? f.pay_period_start.toISOString().slice(0,10) : String(f.pay_period_start||"").slice(0,10),
        ppEnd:   f.pay_period_end   instanceof Date ? f.pay_period_end.toISOString().slice(0,10)   : String(f.pay_period_end||"").slice(0,10),
        userEmail: f.user_email,
        entryId: f.entry_id,
        correctionEntryId: f.correction_entry_id,
        originalData: f.original_data,
        entry: entry ? {
          date: entry.date instanceof Date ? entry.date.toISOString().slice(0,10) : String(entry.date||"").slice(0,10),
          location: entry.location, time: entry.time,
          classParty: entry.class_party, hours: entry.hours_offered,
          rate: entry.hourly_rate, total: entry.total, notes: entry.notes,
          userName: entry.user_name,
        } : null,
        correctionEntry: correctionEntry ? {
          date: correctionEntry.date instanceof Date ? correctionEntry.date.toISOString().slice(0,10) : String(correctionEntry.date||"").slice(0,10),
          location: correctionEntry.location, time: correctionEntry.time,
          classParty: correctionEntry.class_party, hours: correctionEntry.hours_offered,
          rate: correctionEntry.hourly_rate, total: correctionEntry.total, notes: correctionEntry.notes,
        } : null,
      });
    }
    res.json({ ok: true, flags });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Get my flags (staff) ──────────────────
api.post("/getMyFlags", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    const tid = await getDefaultTenantId();
    const result = await query(`SELECT * FROM entry_flags WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2) ORDER BY flagged_at DESC`, [tid, user.email]);

    const flags = [];
    for (const f of result.rows) {
      const entryRes = await query(`SELECT * FROM entries WHERE id=$1`, [f.entry_id]);
      const entry = entryRes.rows[0] || null;
      // Look up correction entry if one exists
      let correctionEntry = null;
      if (f.correction_entry_id) {
        const cRes = await query(`SELECT * FROM entries WHERE id=$1`, [f.correction_entry_id]);
        if (cRes.rows[0]) {
          const c = cRes.rows[0];
          correctionEntry = {
            date: c.date instanceof Date ? c.date.toISOString().slice(0,10) : String(c.date||"").slice(0,10),
            location: c.location, time: c.time,
            classParty: c.class_party, hours: c.hours_offered,
            rate: c.hourly_rate, total: c.total, notes: c.notes,
          };
        }
      }
      flags.push({
        id: f.id, status: f.status, entryId: f.entry_id,
        flagNote: f.flag_note, flaggedByName: f.flagged_by_name,
        flaggedAt: f.flagged_at, correctionNote: f.correction_note,
        correctedAt: f.corrected_at, resolutionNote: f.resolution_note,
        approvedAt: f.approved_at, approvedByName: f.approved_by_name,
        correctionEntryId: f.correction_entry_id || null,
        hiddenFromStaff: !!f.hidden_from_staff,
        ppStart: f.pay_period_start instanceof Date ? f.pay_period_start.toISOString().slice(0,10) : String(f.pay_period_start||"").slice(0,10),
        ppEnd:   f.pay_period_end   instanceof Date ? f.pay_period_end.toISOString().slice(0,10)   : String(f.pay_period_end||"").slice(0,10),
        entry: entry ? {
          date: entry.date instanceof Date ? entry.date.toISOString().slice(0,10) : String(entry.date||"").slice(0,10),
          location: entry.location, time: entry.time,
          classParty: entry.class_party, hours: entry.hours_offered,
          rate: entry.hourly_rate, total: entry.total, notes: entry.notes,
        } : null,
        correctionEntry,
        originalData: f.original_data,
      });
    }
    res.json({ ok: true, flags });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Get flags for a specific pay period (used by payroll tab) ──
api.post("/getFlagsForPeriod", async (req, res) => {
  try {
    const { email, pin, ppStart, ppEnd } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "PAYROLL_ADMIN")
      return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    const result = await query(
      `SELECT id, entry_id, user_email, status FROM entry_flags WHERE tenant_id=$1 AND pay_period_start=$2 AND pay_period_end=$3 AND status != 'approved'`,
      [tid, ppStart, ppEnd]
    );
    // Map: entryId → flag status
    const flagMap = {};
    result.rows.forEach(r => { flagMap[r.entry_id] = { id: r.id, status: r.status, userEmail: r.user_email }; });
    res.json({ ok: true, flagMap });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Send flag reminders (called by cron) ──
async function sendFlagReminders() {
  try {
    const tid = await getDefaultTenantId();
    const currentPp = getCurrentPayPeriod();
    if (!currentPp) return;

    // Find flags that are still 'flagged' (not corrected/approved) and reminder not yet sent
    // Reminder triggers 3 days before pay period end
    const ppEnd = new Date(currentPp.end + "T00:00:00");
    const reminderDate = new Date(ppEnd.getTime() - 3 * 86400000);
    const today = new Date();
    if (today < reminderDate) return; // too early

    const flags = await query(
      `SELECT * FROM entry_flags WHERE tenant_id=$1 AND status='flagged' AND reminder_sent=FALSE`,
      [tid]
    );

    for (const f of flags.rows) {
      await query(`UPDATE entry_flags SET reminder_sent=TRUE WHERE id=$1`, [f.id]);

      const ppS = f.pay_period_start instanceof Date ? f.pay_period_start.toISOString().slice(0,10) : String(f.pay_period_start).slice(0,10);
      const ppE = f.pay_period_end   instanceof Date ? f.pay_period_end.toISOString().slice(0,10)   : String(f.pay_period_end).slice(0,10);

      sendMail({
        to: f.user_email,
        subject: `${CONFIG.BRAND_NAME} — Reminder: Flagged Entry Needs Attention`,
        html: `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
          <div style="background:#e65100;color:#fff;padding:16px 20px;border-radius:8px 8px 0 0;">
            <h2 style="margin:0;font-size:18px;">⏰ Reminder: Flagged Entry</h2>
          </div>
          <div style="padding:20px;border:1px solid #e0e0e0;border-top:0;border-radius:0 0 8px 8px;">
            <p>You have a flagged timesheet entry that still needs your attention.</p>
            <div style="background:#fff3e0;border:1px solid #ffcc02;border-radius:8px;padding:14px;margin:16px 0;">
              <div style="font-size:12px;color:#666;">FLAG NOTE</div>
              <div style="font-size:14px;margin-top:4px;">${f.flag_note || "—"}</div>
            </div>
            <p><strong>⚠️ The pay period ends on ${ppE}.</strong> Please log in and resolve this before the payroll report is generated.</p>
            <p>If this entry is not corrected before the period ends, it will be held and rolled into the next pay cycle.</p>
          </div>
        </div>`
      }).catch(err => console.error("Flag reminder failed:", err));
      sendPush(f.user_email, "⏰ Flagged Entry Reminder", "You have a flagged entry that needs your attention.", "/", "flags").catch(() => {});
    }
    if (flags.rows.length) console.log(`Sent ${flags.rows.length} flag reminder(s).`);
  } catch (err) {
    console.error("sendFlagReminders error:", err);
  }
}

// ── Rollover unresolved flags 1 day after period ends ──
async function rolloverUnresolvedFlags(force = false) {
  try {
    const tid = await getDefaultTenantId();
    const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    const yesterday = addDays(today, -1);

    // When force=true, find ALL unresolved flags regardless of period end date
    const flags = force
      ? await query(
          `SELECT ef.*, e.user_name, e.user_type, e.date, e.location, e.time, e.class_party,
                  e.hours_offered, e.hourly_rate, e.total, e.notes, e.pole_bonus
           FROM entry_flags ef
           LEFT JOIN entries e ON e.id = ef.entry_id AND e.tenant_id = ef.tenant_id
           WHERE ef.tenant_id=$1
             AND ef.status='flagged'
             AND ef.rolled_over=FALSE`,
          [tid]
        )
      : await query(
          `SELECT ef.*, e.user_name, e.user_type, e.date, e.location, e.time, e.class_party,
                  e.hours_offered, e.hourly_rate, e.total, e.notes, e.pole_bonus
           FROM entry_flags ef
           LEFT JOIN entries e ON e.id = ef.entry_id AND e.tenant_id = ef.tenant_id
           WHERE ef.tenant_id=$1
             AND ef.status='flagged'
             AND ef.rolled_over=FALSE
             AND ef.pay_period_end=$2`,
          [tid, yesterday]
        );

    if (!flags.rows.length) return 0;

    // Get the current pay period (the one the entry rolls into)
    const currentPp = getCurrentPayPeriod();
    if (!currentPp) { console.warn("rolloverUnresolvedFlags: no current pay period"); return; }

    let rolledCount = 0;
    for (const f of flags.rows) {
      // Recalculate total if needed
      let tot = parseFloat(f.total) || 0;
      if (tot === 0 && parseFloat(f.hours_offered) > 0 && parseFloat(f.hourly_rate) > 0) {
        tot = round2(parseFloat(f.hours_offered) * parseFloat(f.hourly_rate));
      }

      const ppS = f.pay_period_start instanceof Date ? f.pay_period_start.toISOString().slice(0,10) : String(f.pay_period_start||"").slice(0,10);
      const ppE = f.pay_period_end instanceof Date ? f.pay_period_end.toISOString().slice(0,10) : String(f.pay_period_end||"").slice(0,10);
      const entryDate = f.date instanceof Date ? f.date.toISOString().slice(0,10) : String(f.date||"").slice(0,10);

      // Insert a copy into pending_submissions for the current period
      await query(`
        INSERT INTO pending_submissions (tenant_id, entry_id, user_email, user_name, user_type,
          pay_period_start, pay_period_end, date, location, time, class_party,
          hours_offered, hourly_rate, total, notes, setup_minutes, cleanup_minutes, rigging_minutes, setup_cleanup_pay)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
        ON CONFLICT DO NOTHING
      `, [tid, f.entry_id, f.user_email, f.user_name || "", f.user_type || "",
          ppS, ppE,
          entryDate, f.location || "", f.time || "", f.class_party || "",
          f.hours_offered, f.hourly_rate, tot, f.notes || "",
          parseInt(f.setup_minutes)||0, parseInt(f.cleanup_minutes)||0, parseInt(f.rigging_minutes)||0, parseFloat(f.setup_cleanup_pay)||0]);

      // Mark the flag as rolled over
      await query(`UPDATE entry_flags SET rolled_over=TRUE WHERE id=$1`, [f.id]);

      // Send reminder email to staff
      sendMail({
        to: f.user_email,
        subject: `${CONFIG.BRAND_NAME} — Flagged Entry Rolled to Next Pay Cycle`,
        html: `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
          <div style="background:#e65100;color:#fff;padding:16px 20px;border-radius:8px 8px 0 0;">
            <h2 style="margin:0;font-size:18px;">🔄 Flagged Entry Rolled Over</h2>
          </div>
          <div style="padding:20px;border:1px solid #e0e0e0;border-top:0;border-radius:0 0 8px 8px;">
            <p>Your flagged timesheet entry from the pay period <strong>${ppS} – ${ppE}</strong> was not corrected before the period ended.</p>
            <div style="background:#fff3e0;border:1px solid #ffcc02;border-radius:8px;padding:14px;margin:16px 0;">
              <div style="font-size:12px;color:#666;">FLAG NOTE</div>
              <div style="font-size:14px;margin-top:4px;">${f.flag_note || "—"}</div>
            </div>
            <p>This entry has been <strong>rolled into the next pay cycle</strong> as a late submission. It will appear in the current period's payroll pending review.</p>
            <p><strong>⚠️ Please log in and correct this entry as soon as possible.</strong></p>
            <p style="margin-top:20px;"><a href="https://${process.env.APP_DOMAIN || 'aradiafitness.app'}" style="display:inline-block;background:#e65100;color:#fff;padding:10px 24px;border-radius:6px;text-decoration:none;font-weight:700;">Log In & Correct</a></p>
          </div>
        </div>`
      }).catch(err => console.error("Rollover email failed:", err));

      rolledCount++;
    }

    if (rolledCount) console.log(`Rolled over ${rolledCount} unresolved flag(s) to current period.`);
    return rolledCount;
  } catch (err) {
    console.error("rolloverUnresolvedFlags error:", err);
    return 0;
  }
}

// ── getPendingSubmissions ─────────────────
api.post("/getPendingSubmissions", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    const res2 = await query(
      `SELECT p.*, COALESCE(NULLIF(p.setup_minutes,0), e.setup_minutes, 0) AS eff_setup_minutes,
              COALESCE(NULLIF(p.cleanup_minutes,0), e.cleanup_minutes, 0) AS eff_cleanup_minutes,
              COALESCE(NULLIF(p.rigging_minutes,0), e.rigging_minutes, 0) AS eff_rigging_minutes,
              COALESCE(NULLIF(p.setup_cleanup_pay,0), e.setup_cleanup_pay, 0) AS eff_setup_cleanup_pay
       FROM pending_submissions p
       LEFT JOIN entries e ON e.id = p.entry_id AND e.tenant_id = p.tenant_id
       WHERE p.tenant_id=$1 ORDER BY p.submitted_at DESC`,
      [tid]
    );

    const groups = {};
    res2.rows.forEach(r => {
      const ppStart = r.pay_period_start instanceof Date ? r.pay_period_start.toISOString().slice(0,10) : String(r.pay_period_start||"").slice(0,10);
      const ppEnd   = r.pay_period_end   instanceof Date ? r.pay_period_end.toISOString().slice(0,10)   : String(r.pay_period_end||"").slice(0,10);
      const key = r.user_email + "|" + ppStart + "|" + ppEnd;
      const approved = true; // auto-approve all
      if (!groups[key]) groups[key] = { email: r.user_email, name: r.user_name||"", ppStart, ppEnd, submittedAt: r.submitted_at, entries: [], rowIds: [],
        approved, requiresManualApproval: false };
      groups[key].entries.push({
        entryId: r.entry_id || "",
        date: r.date instanceof Date ? r.date.toISOString().slice(0,10) : String(r.date||"").slice(0,10),
        location: r.location||"", time: r.time||"", classParty: r.class_party||"",
        hours: r.hours_offered!=null?String(r.hours_offered):"", rate: r.hourly_rate!=null?String(r.hourly_rate):"",
        total: r.total!=null?String(r.total):"", notes: r.notes||"",
        setupMinutes: parseInt(r.eff_setup_minutes)||0, cleanupMinutes: parseInt(r.eff_cleanup_minutes)||0,
        riggingMinutes: parseInt(r.eff_rigging_minutes)||0,
        setupCleanupPay: r.eff_setup_cleanup_pay!=null?parseFloat(r.eff_setup_cleanup_pay):0,
      });
      groups[key].rowIds.push(r.id);
    });

    res.json({ ok: true, groups: Object.values(groups) });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── notifyLatePeriodSubmission ────────────
api.post("/notifyLatePeriodSubmission", async (req, res) => {
  try {
    const { email, pin, ppStart, ppEnd } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    const tid     = await getDefaultTenantId();
    const entries = await getEntriesForPeriod(user.email, ppStart, ppEnd);

    // Remove existing pending for this user+period
    await query(
      `DELETE FROM pending_submissions WHERE tenant_id=$1 AND user_email=$2 AND pay_period_start=$3`,
      [tid, user.email, ppStart]
    );

    // Insert new pending rows — skip duplicates already in main entries for CURRENT period
    const currentPp = getCurrentPayPeriod();
    const currentEntries = await getEntriesForPeriod(user.email, currentPp.start, currentPp.end);
    const dupSet = new Set(currentEntries.map(e => `${e.date}|${(e.classParty||"").toLowerCase()}|${(e.time||"").toLowerCase()}|${e.hours}`));

    let duplicates = 0;
    for (const e of entries) {
      const key = `${e.date}|${(e.classParty||"").toLowerCase()}|${(e.time||"").toLowerCase()}|${e.hours}`;
      if (dupSet.has(key)) { duplicates++; continue; }
      // Recalculate total if missing but hours+rate exist
      let tot = parseNum(e.total);
      if ((tot === null || tot === 0) && parseNum(e.hours) && parseNum(e.rate)) {
        tot = round2(parseNum(e.hours) * parseNum(e.rate));
      }
      await query(`
        INSERT INTO pending_submissions (tenant_id, entry_id, user_email, user_name, user_type, pay_period_start, pay_period_end, date, location, time, class_party, hours_offered, hourly_rate, total, notes, setup_minutes, cleanup_minutes, rigging_minutes, setup_cleanup_pay)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
        ON CONFLICT DO NOTHING
      `, [tid, e.id, user.email, user.name, user.type, ppStart, ppEnd, e.date, e.location, e.time, e.classParty, parseNum(e.hours), parseNum(e.rate), tot, e.notes||"", e.setupMinutes||0, e.cleanupMinutes||0, e.riggingMinutes||0, e.setupCleanupPay||0]);
    }

    res.json({ ok: true, duplicatesSkipped: duplicates });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── sendPendingReport ─────────────────────
api.post("/sendPendingReport", async (req, res) => {
  try {
    const { email, pin, groupIndices } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const tid  = await getDefaultTenantId();
    const res2 = await query(`SELECT * FROM pending_submissions WHERE tenant_id=$1 ORDER BY submitted_at DESC`, [tid]);

    // Rebuild groups (same as getPendingSubmissions)
    const groups = {};
    res2.rows.forEach(r => {
      const ppStart = r.pay_period_start instanceof Date ? r.pay_period_start.toISOString().slice(0,10) : String(r.pay_period_start||"").slice(0,10);
      const ppEnd   = r.pay_period_end   instanceof Date ? r.pay_period_end.toISOString().slice(0,10)   : String(r.pay_period_end||"").slice(0,10);
      const key = r.user_email+"|"+ppStart+"|"+ppEnd;
      if (!groups[key]) groups[key] = { email: r.user_email, name: r.user_name||"", ppStart, ppEnd, entries: [], rowIds: [] };
      groups[key].entries.push(r);
      groups[key].rowIds.push(r.id);
    });

    const allGroups = Object.values(groups);
    const selected  = groupIndices.map(i => allGroups[i]).filter(Boolean);
    if (!selected.length) return res.json({ ok: false, reason: "No groups selected." });

    const settings   = await getAdminSettings();
    const recipients = getEnabledReportRecipients(settings);
    if (!recipients.length) return res.json({ ok: false, reason: "No recipients configured." });

    // Delete rows first so user gets instant response
    const allRowIds = selected.flatMap(g => g.rowIds);
    if (allRowIds.length) {
      await query(`DELETE FROM pending_submissions WHERE id = ANY($1)`, [allRowIds]);
    }

    // Log to sent_reports_log
    const tid_ = await getDefaultTenantId();
    let lateTotalPay = 0;
    selected.forEach(g => g.entries.forEach(e => { lateTotalPay += parseFloat(e.total||0); }));
    lateTotalPay = round2(lateTotalPay);
    const ppRange = selected.map(g => `${g.ppStart}→${g.ppEnd}`).join(', ');
    await query(`INSERT INTO sent_reports_log (tenant_id, sent_by, report_type, pp_start, pp_end, recipients, staff_count, total_pay, summary)
      VALUES ($1,$2,'late',$3,$4,$5,$6,$7,$8)`,
      [tid_, email, selected[0].ppStart, selected[0].ppEnd, recipients.join(','), selected.length, lateTotalPay,
       `Late submissions: ${selected.length} group(s), ${ppRange}`]);

    // Respond immediately — email sends in background
    res.json({ ok: true, message: `Report sent for ${selected.length} submission(s).` });

    // Send email async (non-blocking)
    const html = buildPendingEmailHtml(selected);
    sendMail({
      to:      recipients[0],
      cc:      recipients.slice(1).join(",") || undefined,
      subject: `${CONFIG.BRAND_NAME} - Prior Period Submissions`,
      html,
    }).catch(err => console.error("Pending report email failed:", err));
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── getSentReports ────────────────────────
api.post("/getSentReports", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR" && uType !== "ACCOUNTANT") return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    const result = await query(`SELECT * FROM sent_reports_log WHERE tenant_id=$1 ORDER BY sent_at DESC LIMIT 50`, [tid]);
    res.json({ ok: true, reports: result.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── resendAsCorrection ────────────────────
api.post("/resendAsCorrection", async (req, res) => {
  try {
    const { email, pin, logId } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    const logRes = await query(`SELECT * FROM sent_reports_log WHERE id=$1 AND tenant_id=$2`, [logId, tid]);
    if (!logRes.rows.length) return res.json({ ok: false, reason: "Original report not found." });
    const orig = logRes.rows[0];

    const settings = await getAdminSettings();
    const recipients = getEnabledReportRecipients(settings);
    if (!recipients.length) return res.json({ ok: false, reason: "No recipients configured." });

    const pp = { start: orig.pp_start instanceof Date ? orig.pp_start.toISOString().slice(0,10) : String(orig.pp_start).slice(0,10),
                 end:   orig.pp_end   instanceof Date ? orig.pp_end.toISOString().slice(0,10)   : String(orig.pp_end).slice(0,10) };

    // Rebuild from live entries data
    const entriesRes = await query(`
      SELECT e.*, u.charge_gst, u.gst_number, u.email_reports, u.requires_approval, u.attach_pdf_payroll, u.attach_csv_payroll
      FROM entries e
      LEFT JOIN users u ON LOWER(u.email)=LOWER(e.user_email) AND u.tenant_id=e.tenant_id
      WHERE e.tenant_id=$1 AND e.pay_period_start=$2 AND e.pay_period_end=$3
    `, [tid, pp.start, pp.end]);

    const groups = {};
    entriesRes.rows.forEach(r => {
      const em = r.user_email;
      if (!groups[em]) groups[em] = { email: em, name: r.user_name||em, type: r.user_type||"Employee", chargeGST: !!r.charge_gst, gstNumber: r.gst_number||"", emailReports: !!r.email_reports, requiresManualApproval: !!r.requires_approval, attachPdfPayroll: r.attach_pdf_payroll !== false, attachCsvPayroll: !!r.attach_csv_payroll, entries: [], totalHours: 0, totalPay: 0 };
      groups[em].totalHours = round2(groups[em].totalHours + (parseFloat(r.hours_offered)||0));
      groups[em].totalPay   = round2(groups[em].totalPay   + (parseFloat(r.total)||0) - (parseFloat(r.studio_rental_fee)||0));
      groups[em].entries.push(formatEntry(r));
    });
    const users = Object.values(groups);
    if (!users.length) return res.json({ ok: false, reason: "No entries found for this period." });

    const origDate = new Date(orig.sent_at).toLocaleString("en-US", { timeZone: "America/Denver" });
    const correctionBanner = `<div style="background:#d32f2f;color:#fff;padding:14px 18px;border-radius:8px;margin-bottom:16px;font-weight:bold;">⚠️ CORRECTION — This corrected report replaces the one sent on ${origDate}</div>`;
    const html = correctionBanner + buildPayrollEmailHtml(users, pp, settings, []);

    // Build attachments
    const attachments = [];
    if (settings.attachQb) {
      const csvRows = [["Name","Email","Type","Pay Period Start","Pay Period End","Total Hours","Gross Pay","GST","Total Owing","GST Number"]];
      users.forEach(u => {
        const isC = (u.type||"").toLowerCase() === "contractor";
        const gst = (isC && u.chargeGST) ? round2(u.totalPay * 0.05) : 0;
        csvRows.push([u.name||u.email, u.email, u.type||"Employee", pp.start, pp.end,
          round2(u.totalHours).toFixed(2), round2(u.totalPay).toFixed(2),
          gst.toFixed(2), round2(u.totalPay + gst).toFixed(2),
          (isC && u.gstNumber) ? u.gstNumber : ""]);
      });
      const csv = csvRows.map(r => r.map(c => `"${String(c).replace(/"/g,'""')}"`).join(",")).join("\r\n");
      attachments.push({ filename: `payroll_correction_${pp.start}_${pp.end}.csv`, content: csv });
    }
    if (settings.attachPdf) {
      const pdfBuf = await buildPayrollPdf(users, pp, settings, []);
      attachments.push({ filename: `payroll_correction_${pp.start}_${pp.end}.pdf`, content: pdfBuf });
    }

    await sendMail({
      to:          recipients[0],
      cc:          recipients.slice(1).join(",") || undefined,
      subject:     `[CORRECTION] ${CONFIG.BRAND_NAME} — Payroll Report ${pp.start} to ${pp.end}`,
      html,
      attachments: attachments.length ? attachments : undefined,
      adminReport: true,
    });

    // Log correction
    const corrTotal = round2(users.reduce((s,u) => s + u.totalPay, 0));
    await query(`INSERT INTO sent_reports_log (tenant_id, sent_by, report_type, pp_start, pp_end, recipients, staff_count, total_pay, summary)
      VALUES ($1,$2,'correction',$3,$4,$5,$6,$7,$8)`,
      [tid, email, pp.start, pp.end, recipients.join(','), users.length, corrTotal,
       `Correction for report sent ${origDate}: ${users.length} staff, $${corrTotal.toFixed(2)} total`]);

    res.json({ ok: true, message: `Correction sent for period ${pp.start} to ${pp.end}` });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── submitSupportMessage ──────────────────
api.post("/submitSupportMessage", async (req, res) => {
  try {
    const { email, pin, data } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    const type    = String(data.type    || "message").toLowerCase();
    const subject = String(data.subject || (type === "bug" ? "Bug Report" : "Support Message")).trim();
    const body    = String(data.body    || "").trim();
    if (!body) return res.json({ ok: false, reason: "Message body is required." });

    const tid = await getDefaultTenantId();
    await query(`
      INSERT INTO support_messages (tenant_id, from_email, from_name, type, subject, body)
      VALUES ($1,$2,$3,$4,$5,$6)
    `, [tid, user.email, user.name, type, subject, body]);

    // Respond immediately — email is best-effort async so it never blocks the user
    res.json({ ok: true });

    // Notify payroll admin if this user is unapproved for this period
    const pp_ = getPayPeriodForDate(String(req.body?.data?.date || todayStr()));
    if (pp_) {
      getAdminSettings().then(async settings => {
        const tid_ = await getDefaultTenantId();
        const appr = await query(
          `SELECT approved FROM payroll_approvals WHERE tenant_id=$1 AND user_email=$2 AND pay_period_start=$3 AND pay_period_end=$4`,
          [tid_, normEmail(user.email), pp_.start, pp_.end]
        );
        if (appr.rows.length && appr.rows[0].approved === false) {
          const dest = getEnabledPayrollAdmin(settings);
          if (dest) sendMail({
            to: dest,
            subject: `[${CONFIG.BRAND_NAME}] Resubmission from ${user.name||user.email}`,
            html: `<div style="font-family:Arial,sans-serif;max-width:580px;">
              <h2 style="background:#2e7d32;color:#fff;padding:14px 18px;border-radius:8px 8px 0 0;margin:0;">📬 Timesheet Resubmitted</h2>
              <div style="border:1px solid #e0e0e0;border-top:0;padding:18px;">
                <p><strong>${user.name||user.email}</strong> (${user.email}) has resubmitted their timesheet for the pay period <strong>${pp_.start} → ${pp_.end}</strong>.</p>
                <p>Their submission was previously flagged as unapproved. Please log in to review and approve.</p>
              </div></div>`
          }).catch(() => {});
        }
      }).catch(() => {});
    }

    // Send email notification in background (don't await)
    getAdminSettings().then(settings => {
      const supportDest = getEnabledSupportEmail(settings);
      if (!supportDest) {
        console.warn("Support email skipped — no supportEmail, payrollAdminEmail, or adminEmail1 configured.");
        return;
      }
      const ccList = [settings.supportEmail, settings.payrollAdminEmail, settings.adminEmail1, settings.adminEmail2, settings.adminEmail3]
        .filter(e => e && e !== supportDest);
      // deduplicate cc
      const ccUniq = [...new Set(ccList)];
      const typeLabel = type === "bug" ? "🐛 Bug Report" : "💬 Support Message";
      console.log(`Sending support notification to ${supportDest} (cc: ${ccUniq.join(",")||"none"})`);
      sendMail({
        to:      supportDest,
        cc:      ccUniq.join(",") || undefined,
        replyTo: user.email,
        subject: `[${CONFIG.BRAND_NAME}] ${typeLabel}: ${subject}`,
        html:    `<div style="font-family:Arial,sans-serif;max-width:580px;">
          <h2 style="background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;padding:14px 18px;border-radius:8px 8px 0 0;margin:0;">${typeLabel}</h2>
          <div style="border:1px solid #e0e0e0;border-top:0;padding:18px;">
            <p><strong>From:</strong> ${esc_html(user.name||"")} &lt;${esc_html(user.email)}&gt;</p>
            <p><strong>Subject:</strong> ${esc_html(subject)}</p>
            <div style="background:#f9f9f9;padding:12px;border-radius:6px;white-space:pre-wrap;">${esc_html(body)}</div>
          </div></div>`
      }).catch(err => console.error("Support email failed:", err.message));
    }).catch(err => console.error("Support email settings load failed:", err.message));
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});


// ── AI Support Chat ──────────────────────
const SUPPORT_KNOWLEDGE_BASE = `
You are Aradia Assistant — a helpful, friendly support bot for Aradia Fitness Timesheets, a payroll timesheet app used by instructors at Aradia Fitness pole dancing and aerial fitness studios.

RULES:
- Only answer questions about the Aradia Fitness Timesheets app, pay periods, logging hours, and related topics.
- If you don't know the answer or it's outside the app scope, say so and suggest they use "Message Support" on the Support tab.
- Be concise and friendly. Use plain language. Keep answers under 3 short paragraphs.
- Never make up features that don't exist. If unsure, say "I'm not 100% sure about that — I'd recommend reaching out to your admin or using Message Support below."
- Do not discuss technical implementation details, server architecture, or code.

APP KNOWLEDGE BASE:

Logging a class/shift:
Go to the Home tab, fill in the date and location, then add shifts with "+ Add shift". Each shift needs a class name, time, hours, and rate. Hit Submit when done.

Pay periods:
Pay periods run bi-weekly (every two weeks). Current period dates are shown in the dropdown at the top of the Home tab. You can switch to past periods to review or add missed entries.

Submitting hours for a previous pay period:
Yes — select a past pay period from the dropdown on Home, enter your shifts, and submit. Prior period submissions are flagged as late and included in the next payroll report.

Late submission grace period:
You have a 1-day grace period after a pay period ends to submit your hours on time. For example, if the pay period ends on March 15, you can still submit until the end of the day on March 16 (midnight MST). Anything submitted on March 17 or later is marked as a late submission and will be included in the next payroll report instead.

Weekly shift bonus (6-hour bonus):
If you teach 6 or more hours in a single week, you earn a $5/hr bonus applied retroactively to all hours that week. The progress tracker on the Home tab shows how close you are each week.

Pole 11+ bonus:
When logging a pole class with 11 or more students, check the "Pole 11+" checkbox on that shift. You'll earn a $5 bonus on top of your hourly pay for that class.

Logging a commission:
On the Home tab there's a dedicated "Log Commission" card. Enter the amount, date (defaults to today), and an optional note. Commissions appear separately from shifts.

Fixing wrong hours:
On the Home tab, tap any entry to expand it and hit Edit. You can correct hours, rate, class name, or time. If the pay period has already been processed, contact admin directly.

Undoing a delete:
If you delete an entry by mistake, an Undo option appears briefly at the bottom of the screen. Tap it before it disappears to restore the entry.

Unapproved submissions:
Some staff require manual payroll approval. If your timesheet hasn't been approved you'll receive an email. Review your entries, make any corrections, and resubmit — your payroll admin is notified automatically.

Pay history:
Go to the Profile tab and scroll down to "My Pay History." Choose a date range, hit Load Pay History, and you'll see a full breakdown with period summary and shift-by-shift views. You can export as PDF.

Downloading a pay statement:
Once you have entries logged, an "Export" button appears in the top right of the "This Pay Period" card on Home, and on the Reports tab.

Changing email or PIN:
Go to the Profile tab. You can update your email, set a username for login, and change your PIN. Changes take effect immediately.

Setting a profile photo:
Go to the Profile tab, tap the pencil icon on the avatar, choose a photo, and hit "Save photo."

Checking hours at a glance:
The summary bar on the Home tab shows your total shifts, commissions, hours, and gross pay for the selected period.

Reports tab:
Shows a day-by-day breakdown of logged entries for the selected pay period. Commissions appear in their own gold-themed section below the weekly shift bonuses, listed line by line. Use the dropdown to switch periods and the "Export PDF" button to download a statement.

Navigating pay periods:
Use the dropdown at the top of the Home, Reports, or Export tab to switch pay periods. You can also use the ‹ (previous) and › (next) arrow buttons beside the dropdown to quickly step through periods. Up to 6 future pay periods are available.

Calendar widget:
The calendar on the Home tab shows dots on any day that has logged entries — including days outside the currently selected pay period. Clicking a day with entries will automatically switch the pay period dropdown to match that day, scroll down, and highlight the entries. Clicking an empty day in a future pay period will switch to that period and scroll to the entry form so you can start logging. The calendar supports both 2-week (pay period) and full month views.

Google Calendar integration:
You can connect your Google Calendar from the Profile tab under "Google Calendar." Once connected, use the "Import from Google Calendar" buttons on the Home tab to pull in events as timecard entries. The import modal lets you pick which calendar to pull from, review events, match locations, set rates, and choose which events to import.

Connecting Google Calendar — "unsafe" warning:
When connecting your Google Calendar, Google will show a warning that says the app "hasn't been verified." This is normal — it simply means the app hasn't gone through Google's lengthy verification process. Aradia Fitness only requests read-only access to your calendar event names, times, and locations. We never modify, delete, or share your calendar data. To continue: click "Advanced," then "Go to Aradia Fitness (unsafe)," then "Continue."

Default Google Calendar:
After connecting Google Calendar, go to the Profile tab. Below the connection status you'll see a "Default Calendar" dropdown. Select your preferred calendar and hit "Save Default Calendar." The import modal will automatically pre-select this calendar so you don't have to choose it every time.

Google Calendar won't connect / sync not working:
Google Calendar sync requires that your Gmail address is registered as an approved test user on our Google Cloud project (the app is in testing mode during Google's verification process). If you get an error when trying to connect, your email likely hasn't been added to the approved list yet. To fix this, go to the Support tab and use "Message Support" to let us know the Gmail address you'd like to use for Google Calendar sync. If you'd prefer to use a different Gmail than the one on your account, just include that in your message. An admin will add you and let you know when it's ready.

Duplicate detection:
The system prevents accidental duplicate entries. When importing from Google Calendar, events that match an existing entry by date and time are flagged as duplicates, marked with a red badge, and toggled off by default. If you override and toggle them back on, you'll get a confirmation prompt. When submitting entries manually, the system checks for matching date+time against your existing entries and asks you to confirm if duplicates are found.

Cross-user overlap warnings:
When you submit entries, the system checks if another staff member already has an entry at the same date, time, AND location. If so, you'll see a "Heads up" warning listing who else is scheduled. This is informational only — you can still submit. Home (admin) location is excluded from these checks since overlaps there are expected.

Email pay notifications:
If your admin has enabled email reports for your account, you'll receive a pay breakdown when the payroll report is processed. Ask your admin if you're unsure.

Installing as a phone app:
On iOS: open the site in Safari, tap Share, and choose "Add to Home Screen."
On Android: use Chrome and tap "Add to Home Screen" from the menu.

Reporting a bug or asking a question:
Go to the Support tab. Use "Report a Bug" for anything broken, or "Message Support" for general questions. An admin will reply to your email.

Flagged entries:
If an admin flags one of your entries, you'll see a banner on your Home tab and get an email. Tap "Review" to see what was flagged and respond with an explanation or submit corrected values.

Locations:
Aradia Fitness has four studio locations: South Edmonton, Kingsway (Edmonton), St. Albert, and Spruce Grove.

Theme/Dark mode:
Go to the Profile tab to switch between Light, Dark, and Aradia themes.
`;

api.post("/supportChat", async (req, res) => {
  try {
    const { email, pin, message, history } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) return res.json({ ok: false, reason: "AI support is not configured. Please use Message Support instead." });

    const userMsg = String(message || "").trim();
    if (!userMsg) return res.json({ ok: false, reason: "Please type a question." });
    if (userMsg.length > 1000) return res.json({ ok: false, reason: "Message too long (max 1000 characters)." });

    // Build conversation messages (keep last 8 exchanges max)
    const msgs = [];
    if (Array.isArray(history)) {
      history.slice(-16).forEach(h => {
        if (h.role === "user" || h.role === "assistant") {
          msgs.push({ role: h.role, content: String(h.content || "").slice(0, 2000) });
        }
      });
    }
    msgs.push({ role: "user", content: userMsg });

    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 600,
        system: SUPPORT_KNOWLEDGE_BASE,
        messages: msgs,
      }),
    });

    if (!response.ok) {
      const errText = await response.text().catch(() => "");
      console.error("Anthropic API error:", response.status, errText);
      return res.json({ ok: false, reason: "AI service temporarily unavailable. Please try again or use Message Support." });
    }

    const data = await response.json();
    const reply = (data.content && data.content[0] && data.content[0].text) || "Sorry, I couldn't generate a response. Please try again.";

    res.json({ ok: true, reply });
  } catch (e) {
    console.error("supportChat error:", e.message);
    res.json({ ok: false, reason: "Something went wrong. Please try Message Support instead." });
  }
});

// ── updateSupportMessage ─────────────────
api.post("/updateSupportMessage", async (req, res) => {
  try {
    const { email, pin, messageId, resolved, replied } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    const tid = await getDefaultTenantId();
    const sets = [];
    const vals = [];
    let i = 1;
    if (resolved !== undefined) { sets.push(`resolved=$${i++}`); vals.push(!!resolved); }
    if (replied  !== undefined) { sets.push(`replied=$${i++}`);  vals.push(!!replied);  }
    if (!sets.length) return res.json({ ok: true });
    vals.push(tid, messageId);
    await query(`UPDATE support_messages SET ${sets.join(",")} WHERE tenant_id=$${i++} AND id=$${i}`, vals);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── deleteSupportMessage ──────────────────
api.post("/deleteSupportMessage", async (req, res) => {
  try {
    const { email, pin, messageId } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    const tid = await getDefaultTenantId();
    await query(`DELETE FROM support_messages WHERE tenant_id=$1 AND id=$2`, [tid, messageId]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── getSupportMessages ────────────────────
api.post("/getSupportMessages", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const tid  = await getDefaultTenantId();
    const res2 = await query(`SELECT * FROM support_messages WHERE tenant_id=$1 ORDER BY submitted_at DESC`, [tid]);
    const messages = res2.rows.map(r => ({
      id:          r.id,
      submittedAt: r.submitted_at,
      fromEmail:   r.from_email   || "",
      fromName:    r.from_name    || "",
      type:        r.type         || "message",
      subject:     r.subject      || "",
      body:        r.body         || "",
      resolved:    !!r.resolved,
      replied:     !!r.replied,
    }));
    res.json({ ok: true, messages });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});


// ── saveChangelog ─────────────────────────
api.post("/saveChangelog", async (req, res) => {
  try {
    const { email, pin, versionLabel, items } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    const tid = await getDefaultTenantId();
    await query(
      `INSERT INTO support_changelog (tenant_id, version_label, items) VALUES ($1,$2,$3)`,
      [tid, versionLabel || "", JSON.stringify(items || [])]
    );
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── getChangelog ──────────────────────────
api.post("/getChangelog", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const r = await query(`SELECT * FROM support_changelog WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 50`, [tid]);
    const entries = r.rows.map(row => ({
      id:           row.id,
      createdAt:    row.created_at,
      versionLabel: row.version_label || "",
      items:        (() => { try { return JSON.parse(row.items); } catch { return []; } })(),
    }));
    res.json({ ok: true, entries });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── publishChangelog ──────────────────────
api.post("/publishChangelog", async (req, res) => {
  try {
    const { email, pin, changelogId } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    const tid = await getDefaultTenantId();
    // Get the changelog entry
    const r = await query(`SELECT * FROM support_changelog WHERE tenant_id=$1 AND id=$2`, [tid, changelogId]);
    if (!r.rows.length) return res.json({ ok: false, reason: "Changelog entry not found." });
    const entry = r.rows[0];
    const items = (() => { try { return JSON.parse(entry.items); } catch { return []; } })();
    const label = entry.version_label || String(entry.created_at).slice(0,10);
    // Email all staff with email_reports enabled
    const settings = await getAdminSettings();
    const staffRes = await query(`SELECT email, name FROM users WHERE tenant_id=$1 AND is_active=TRUE AND email_reports=TRUE`, [tid]);
    const html = `<div style="font-family:Arial,sans-serif;max-width:580px;">
      <h2 style="background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;padding:14px 18px;border-radius:8px 8px 0 0;margin:0;">📋 ${CONFIG.BRAND_NAME} — What's New: ${esc_html(label)}</h2>
      <div style="border:1px solid #e0e0e0;border-top:0;padding:18px;">
        <p>Here's a summary of recent fixes and improvements:</p>
        <ul>${items.map(i => `<li style="margin-bottom:6px;">${esc_html(String(i))}</li>`).join("")}</ul>
        <p style="color:#888;font-size:12px;">— The ${CONFIG.BRAND_NAME} Team</p>
      </div></div>`;
    res.json({ ok: true, recipientCount: staffRes.rows.length });
    for (const u of staffRes.rows) {
      await sendMail({ to: u.email, subject: `[${CONFIG.BRAND_NAME}] What's New — ${label}`, html })
        .catch(err => console.error("Changelog email failed:", u.email, err.message));
    }
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── deleteChangelog ──────────────────────
api.post("/deleteChangelog", async (req, res) => {
  try {
    const { email, pin, changelogId } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    const tid = await getDefaultTenantId();
    await query(`DELETE FROM support_changelog WHERE tenant_id=$1 AND id=$2`, [tid, changelogId]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ─────────────────────────────────────────
//  EMAIL BUILDERS
// ─────────────────────────────────────────
// ── Load approved late (pending) submissions ─
async function loadApprovedLateGroups(tid) {
  const pendingRes = await query(
    `SELECT p.*, COALESCE(NULLIF(p.setup_minutes,0), e.setup_minutes, 0) AS eff_setup_minutes,
            COALESCE(NULLIF(p.cleanup_minutes,0), e.cleanup_minutes, 0) AS eff_cleanup_minutes,
            COALESCE(NULLIF(p.rigging_minutes,0), e.rigging_minutes, 0) AS eff_rigging_minutes,
            COALESCE(NULLIF(p.setup_cleanup_pay,0), e.setup_cleanup_pay, 0) AS eff_setup_cleanup_pay
     FROM pending_submissions p
     LEFT JOIN entries e ON e.id = p.entry_id AND e.tenant_id = p.tenant_id
     WHERE p.tenant_id=$1 ORDER BY p.submitted_at DESC`, [tid]);
  if (!pendingRes.rows.length) return [];

  // APPROVAL SYSTEM DISABLED — include all pending submissions
  // When re-enabling, uncomment approval filtering below:
  // const approvalsRes = await query(`SELECT user_email, pay_period_start, pay_period_end, approved FROM payroll_approvals WHERE tenant_id=$1`, [tid]);
  // const approvalMap = {};
  // approvalsRes.rows.forEach(r => {
  //   const ps = r.pay_period_start instanceof Date ? r.pay_period_start.toISOString().slice(0,10) : String(r.pay_period_start||"").slice(0,10);
  //   const pe = r.pay_period_end   instanceof Date ? r.pay_period_end.toISOString().slice(0,10)   : String(r.pay_period_end||"").slice(0,10);
  //   approvalMap[normEmail(r.user_email)+"|"+ps+"|"+pe] = !!r.approved;
  // });

  const groups = {};
  pendingRes.rows.forEach(r => {
    const ps = r.pay_period_start instanceof Date ? r.pay_period_start.toISOString().slice(0,10) : String(r.pay_period_start||"").slice(0,10);
    const pe = r.pay_period_end   instanceof Date ? r.pay_period_end.toISOString().slice(0,10)   : String(r.pay_period_end||"").slice(0,10);
    // if (approvalMap[normEmail(r.user_email)+"|"+ps+"|"+pe] === false) return; // DISABLED — skip unapproved
    const key = normEmail(r.user_email)+"|"+ps+"|"+pe;
    if (!groups[key]) groups[key] = { email: r.user_email, name: r.user_name||"", ppStart: ps, ppEnd: pe, entries: [], rowIds: [] };
    // Use effective (joined) values so setup/cleanup/rigging shows even for old rows
    r.setup_minutes = parseInt(r.eff_setup_minutes)||0;
    r.cleanup_minutes = parseInt(r.eff_cleanup_minutes)||0;
    r.rigging_minutes = parseInt(r.eff_rigging_minutes)||0;
    r.setup_cleanup_pay = parseFloat(r.eff_setup_cleanup_pay)||0;
    groups[key].entries.push(r);
    groups[key].rowIds.push(r.id);
  });

  // Enrich each group with user type/GST data from users table
  const groupList = Object.values(groups);
  const uniqueEmails = [...new Set(groupList.map(g => normEmail(g.email)))];
  if (uniqueEmails.length) {
    const userRes = await query(
      `SELECT email, type, charge_gst, gst_number FROM users WHERE tenant_id=$1 AND email = ANY($2)`,
      [tid, uniqueEmails]
    );
    const userMap = {};
    userRes.rows.forEach(u => { userMap[normEmail(u.email)] = u; });
    groupList.forEach(g => {
      const u = userMap[normEmail(g.email)];
      if (u) {
        g.userType   = u.type || "Employee";
        g.chargeGST  = !!u.charge_gst;
        g.gstNumber  = u.gst_number || "";
      }
    });
  }
  return groupList;
}

function buildPayrollEmailHtml(users, pp, settings, lateGroups) {
  // ── Current period table ──
  const rows = users.map(u => {
    const isC = (u.type||"").toLowerCase() === "contractor";
    const gst = (isC && u.chargeGST) ? round2(u.totalPay * 0.05) : 0;
    return `<tr>
      <td style="padding:9px 10px;border-bottom:1px solid #eee;">${esc_html(u.name||u.email)}</td>
      <td style="padding:9px 10px;border-bottom:1px solid #eee;color:#666;font-size:12px;">${esc_html(u.email)}</td>
      <td style="padding:9px 10px;border-bottom:1px solid #eee;">${esc_html(u.type||"Employee")}</td>
      <td style="padding:9px 10px;border-bottom:1px solid #eee;text-align:right;">${u.totalHours.toFixed(2)}</td>
      <td style="padding:9px 10px;border-bottom:1px solid #eee;text-align:right;">$${u.totalPay.toFixed(2)}</td>
      <td style="padding:9px 10px;border-bottom:1px solid #eee;text-align:right;font-weight:700;">${gst>0 ? `$${round2(u.totalPay+gst).toFixed(2)}<br><span style="font-size:11px;font-weight:400;color:#888;">+GST $${gst.toFixed(2)}</span>` : `$${u.totalPay.toFixed(2)}`}</td>
    </tr>`;
  }).join("");
  const grandGross = users.reduce((s,u) => s + u.totalPay, 0);
  const grandTotal = users.reduce((s,u) => s + u.totalPay + ((u.type||"").toLowerCase()==="contractor" && u.chargeGST ? round2(u.totalPay*0.05) : 0), 0);

  // ── Late submissions section ──
  let lateHtml = "";
  if (lateGroups && lateGroups.length) {
    const lateSections = lateGroups.map(g => {
      const entryRows = g.entries.map(e => {
        const date = e.date instanceof Date ? e.date.toISOString().slice(0,10) : String(e.date||"").slice(0,10);
        const hrs  = parseFloat(e.hours_offered||e.hours||0);
        const rate = parseFloat(e.hourly_rate||e.rate||0);
        let tot  = parseFloat(e.total||0);
        // Recalculate if total is 0 but hours+rate exist
        if (tot === 0 && hrs > 0 && rate > 0) tot = round2(hrs * rate);
        const sm = parseInt(e.setup_minutes||e.setupMinutes||0)||0;
        const cm = parseInt(e.cleanup_minutes||e.cleanupMinutes||0)||0;
        const rm = parseInt(e.rigging_minutes||e.riggingMinutes||0)||0;
        const scp = parseFloat(e.setup_cleanup_pay||e.setupCleanupPay||0)||0;
        const scParts = []; if (sm>0) scParts.push('Setup '+sm+'m'); if (cm>0) scParts.push('Cleanup '+cm+'m'); if (rm>0) scParts.push('Rigging '+rm+'m');
        const scNote = scParts.length ? `<div style="font-size:10px;color:#2e7d32;">${scParts.join(' + ')}${scp>0?' (+$'+scp.toFixed(2)+')':''}</div>` : '';
        return `<tr>
          <td style="padding:7px 10px;border-bottom:1px solid #fde8c0;font-size:12px;">${date}</td>
          <td style="padding:7px 10px;border-bottom:1px solid #fde8c0;font-size:12px;">${esc_html(e.class_party||e.classParty||"")}${scNote}</td>
          <td style="padding:7px 10px;border-bottom:1px solid #fde8c0;font-size:12px;">${esc_html(e.time||"")}</td>
          <td style="padding:7px 10px;border-bottom:1px solid #fde8c0;font-size:12px;text-align:right;">${hrs.toFixed(2)}</td>
          <td style="padding:7px 10px;border-bottom:1px solid #fde8c0;font-size:12px;text-align:right;font-weight:700;">$${tot.toFixed(2)}</td>
        </tr>`;
      }).join("");
      const lateTotal = g.entries.reduce((s,e) => {
        const hrs = parseFloat(e.hours_offered||e.hours||0);
        const rate = parseFloat(e.hourly_rate||e.rate||0);
        let tot = parseFloat(e.total||0);
        if (tot === 0 && hrs > 0 && rate > 0) tot = round2(hrs * rate);
        return s + tot;
      }, 0);
      return `<div style="margin-bottom:14px;">
        <div style="background:#fff3cd;border:1px solid #ffc107;border-radius:6px;padding:8px 12px;margin-bottom:6px;font-size:13px;">
          <strong>${esc_html(g.name||g.email)}</strong> <span style="color:#666;">&lt;${esc_html(g.email)}&gt;</span>
          <span style="float:right;color:#888;font-size:12px;">Period: ${g.ppStart} → ${g.ppEnd}</span>
        </div>
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
          <thead><tr style="background:#fff8e1;">
            <th style="padding:7px 10px;text-align:left;">Date</th>
            <th style="padding:7px 10px;text-align:left;">Class</th>
            <th style="padding:7px 10px;text-align:left;">Time</th>
            <th style="padding:7px 10px;text-align:right;">Hours</th>
            <th style="padding:7px 10px;text-align:right;">Total</th>
          </tr></thead>
          <tbody>${entryRows}</tbody>
          <tfoot><tr>
            <td colspan="4" style="padding:7px 10px;text-align:right;font-weight:700;font-size:12px;border-top:1px solid #ffc107;">Subtotal</td>
            <td style="padding:7px 10px;text-align:right;font-weight:700;border-top:1px solid #ffc107;">$${lateTotal.toFixed(2)}</td>
          </tr></tfoot>
        </table>
      </div>`;
    }).join("");
    const lateGrandTotal = lateGroups.reduce((s,g) => s + g.entries.reduce((ss,e) => {
      const hrs = parseFloat(e.hours_offered||e.hours||0);
      const rate = parseFloat(e.hourly_rate||e.rate||0);
      let tot = parseFloat(e.total||0);
      if (tot === 0 && hrs > 0 && rate > 0) tot = round2(hrs * rate);
      return ss + tot;
    }, 0), 0);
    lateHtml = `
    <div style="margin-top:24px;">
      <div style="background:#e65100;color:#fff;padding:11px 16px;border-radius:8px 8px 0 0;display:flex;align-items:center;gap:8px;">
        <span style="font-size:18px;">⚠️</span>
        <span style="font-weight:700;font-size:15px;">Late Submissions — Require Payment</span>
      </div>
      <div style="border:1px solid #e65100;border-top:0;border-radius:0 0 8px 8px;padding:16px;background:#fffdf7;">
        <p style="color:#666;font-size:12px;margin:0 0 14px;">These entries are from prior pay periods and were submitted late. They represent additional amounts owed and are NOT included in the current period totals above.</p>
        ${lateSections}
        <p style="text-align:right;font-weight:800;font-size:15px;margin:8px 0 0;color:#e65100;">Late Total: $${lateGrandTotal.toFixed(2)}</p>
      </div>
    </div>`;
  }

  return `<div style="font-family:Arial,sans-serif;max-width:720px;">
    <h2 style="background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;padding:14px 18px;border-radius:8px 8px 0 0;margin:0;">${CONFIG.BRAND_NAME} — Payroll Report</h2>
    <div style="border:1px solid #e0e0e0;border-top:0;border-radius:0 0 8px 8px;padding:16px;">
      <p style="color:#666;font-size:13px;margin:0 0 10px;">Current pay period: <strong>${pp.start}</strong> to <strong>${pp.end}</strong></p>
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        <thead><tr style="background:#f0f1f4;">
          <th style="padding:9px 10px;text-align:left;">Name</th>
          <th style="padding:9px 10px;text-align:left;">Email</th>
          <th style="padding:9px 10px;text-align:left;">Type</th>
          <th style="padding:9px 10px;text-align:right;">Hours</th>
          <th style="padding:9px 10px;text-align:right;">Gross Pay</th>
          <th style="padding:9px 10px;text-align:right;">Total Owing</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
      <div style="text-align:right;margin-top:10px;font-size:13px;color:#666;">Gross: $${grandGross.toFixed(2)}</div>
      <p style="text-align:right;font-weight:800;font-size:16px;margin:4px 0 0;">Total Owing: $${grandTotal.toFixed(2)}</p>
      ${lateHtml}
    </div></div>`;
}

function buildStaffEmailHtml(user, pp, settings) {
  const isC  = (user.type||"").toLowerCase() === "contractor";
  const gst  = (isC && user.chargeGST) ? round2(user.totalPay * 0.05) : 0;
  const tot  = round2(user.totalPay + gst);
  const poleBonusRate = parseFloat(settings.poleClassBonus) || 5;

  // Sort entries by date
  const entries = (user.entries || []).slice().sort((a,b) => (a.date||"").localeCompare(b.date||""));

  // Pole bonus total
  const poleBonusTotal = entries.filter(e => e.poleBonus).reduce((sum, e) => sum + (parseFloat(e.hours)||0), 0) * poleBonusRate;

  // Build per-shift rows — stacked mobile-friendly layout
  const entryRows = entries.map(e => {
    const hrs  = parseFloat(e.hours || 0);
    const rate = parseFloat(e.rate  || 0);
    const pay  = parseFloat(e.total || 0);
    const isComm = (e.classParty||"") === "Commission";
    const poleBonusAmt = e.poleBonus ? round2(hrs * poleBonusRate) : 0;
    const bonusBadge = e.poleBonus
      ? ` <span style="display:inline-block;background:#e8f5e9;color:#2e7d32;border-radius:10px;padding:1px 7px;font-size:10px;font-weight:700;">+ Pole Bonus $${poleBonusAmt.toFixed(2)}</span>`
      : "";
    const timeLoc = [e.time, e.location].filter(Boolean).join(" · ");
    const notesHtml = e.notes
      ? `<div style="color:#888;font-size:11px;font-style:italic;margin-top:2px;">${esc_html(e.notes)}</div>`
      : "";
    return `<tr style="border-bottom:1px solid #f0f0f0;">
      <td style="padding:10px;font-size:13px;vertical-align:top;">
        <div style="font-weight:700;">${pay > 0 ? "$" + pay.toFixed(2) : "—"}</div>
        <div style="font-size:11px;color:#888;margin-top:1px;">${hrs > 0 ? hrs.toFixed(2) + " hrs" : ""}${rate > 0 ? " @ $" + rate.toFixed(2) + "/hr" : ""}</div>
      </td>
      <td style="padding:10px;font-size:13px;vertical-align:top;">
        <div>${isComm ? "💰 Commission" : esc_html(e.classParty||"")}${bonusBadge}</div>
        <div style="color:#999;font-size:11px;">${e.date}${timeLoc ? " · " + esc_html(timeLoc) : ""}</div>
        ${notesHtml}
      </td>
    </tr>`;
  }).join("");

  // Summary bar items
  const summaryItems = [
    `<strong>${entries.length}</strong> shift${entries.length!==1?"s":""}`,
    `<strong>${user.totalHours.toFixed(2)}</strong> hrs`,
    `Gross <strong>$${user.totalPay.toFixed(2)}</strong>`,
    gst > 0 ? `GST <strong>$${gst.toFixed(2)}</strong>` : null,
    poleBonusTotal > 0 ? `Pole bonus <strong>$${poleBonusTotal.toFixed(2)}</strong>` : null,
  ].filter(Boolean).join('<span style="color:#ccc;margin:0 6px;">|</span>');

  // Staff type label
  const typeLabel = isC ? "Contractor" : (user.type || "Employee");
  const gstInfo = isC && user.gstNumber ? ` · GST# ${esc_html(user.gstNumber)}` : "";

  // GST row
  const gstRow = gst > 0 ? `
    <tr style="background:#f9f9f9;">
      <td style="padding:8px 10px;font-size:13px;font-weight:600;color:#666;">$${gst.toFixed(2)}</td>
      <td style="padding:8px 10px;font-size:13px;color:#666;">GST (5%)</td>
    </tr>` : "";

  // Total row
  const totalRow = `
    <tr style="background:#f0f1f4;">
      <td style="padding:10px;font-size:15px;font-weight:800;color:${CONFIG.BRAND_COLOR_PRIMARY};">$${tot.toFixed(2)}</td>
      <td style="padding:10px;font-size:14px;font-weight:700;">Total Owing <span style="font-weight:400;font-size:12px;color:#888;">(pre-tax)</span></td>
    </tr>`;

  return `<div style="font-family:Arial,sans-serif;max-width:640px;">
    <div style="background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;padding:16px 20px;border-radius:8px 8px 0 0;">
      <div style="font-size:18px;font-weight:700;">${CONFIG.BRAND_NAME} — Your Pay Summary</div>
      <div style="font-size:13px;opacity:.85;margin-top:3px;">Pay period: ${pp.start} → ${pp.end}</div>
    </div>
    <div style="border:1px solid #e0e0e0;border-top:0;border-radius:0 0 8px 8px;overflow:hidden;">

      <div style="padding:14px 18px;background:#fafafa;border-bottom:1px solid #eee;">
        <div style="font-size:15px;font-weight:700;margin-bottom:2px;">Hi ${esc_html(user.name||user.email)},</div>
        <div style="font-size:12px;color:#666;">${typeLabel}${gstInfo} · All amounts are gross (pre-tax).</div>
      </div>

      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="background:#f0f1f4;">
            <th style="padding:9px 10px;text-align:left;font-size:12px;color:#666;font-weight:700;width:100px;">Pay</th>
            <th style="padding:9px 10px;text-align:left;font-size:12px;color:#666;font-weight:700;">Details</th>
          </tr>
        </thead>
        <tbody>${entryRows}</tbody>
        <tfoot>
          ${gstRow}
          ${totalRow}
        </tfoot>
      </table>

      <div style="padding:10px 18px 6px;background:#fff;border-top:1px solid #eee;font-size:12px;color:#555;display:flex;flex-wrap:wrap;gap:8px;">
        ${summaryItems}
      </div>

      <div style="padding:14px 18px;font-size:11px;color:#aaa;border-top:1px solid #f0f0f0;">
        This is an automated summary from ${CONFIG.BRAND_NAME}. For questions, reply to this email or contact your studio admin.
      </div>
    </div>
  </div>`;
}

function buildPendingEmailHtml(groups) {
  const sections = groups.map(g => {
    const entryRows = g.entries.map(e => {
      const date = e.date instanceof Date ? e.date.toISOString().slice(0,10) : String(e.date||"").slice(0,10);
      return `<tr>
        <td style="padding:8px 10px;border-bottom:1px solid #eee;">${date}</td>
        <td style="padding:8px 10px;border-bottom:1px solid #eee;">${e.class_party||e.classParty||""}</td>
        <td style="padding:8px 10px;border-bottom:1px solid #eee;">${e.time||""}</td>
        <td style="padding:8px 10px;border-bottom:1px solid #eee;">${e.location||""}</td>
        <td style="padding:8px 10px;border-bottom:1px solid #eee;text-align:right;">${e.hours_offered||e.hours||""}</td>
        <td style="padding:8px 10px;border-bottom:1px solid #eee;text-align:right;">$${e.total||"0"}</td>
      </tr>`;
    }).join("");
    return `<div style="margin-bottom:20px;">
      <h3 style="background:#f5f5f5;padding:10px 14px;border-radius:6px;margin:0 0 8px;">${g.name} &lt;${g.email}&gt; — ${g.ppStart} to ${g.ppEnd}</h3>
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        <thead><tr style="background:#fafafa;">
          <th style="padding:8px 10px;text-align:left;">Date</th>
          <th style="padding:8px 10px;text-align:left;">Class</th>
          <th style="padding:8px 10px;text-align:left;">Time</th>
          <th style="padding:8px 10px;text-align:left;">Location</th>
          <th style="padding:8px 10px;text-align:right;">Hours</th>
          <th style="padding:8px 10px;text-align:right;">Total</th>
        </tr></thead>
        <tbody>${entryRows}</tbody>
      </table></div>`;
  }).join("");
  return `<div style="font-family:Arial,sans-serif;max-width:700px;">
    <h2 style="background:#c07a00;color:#fff;padding:14px 18px;border-radius:8px 8px 0 0;margin:0;">${CONFIG.BRAND_NAME} — Prior Period Submissions</h2>
    <div style="border:1px solid #e0e0e0;border-top:0;padding:16px;">${sections}</div></div>`;
}


// ─────────────────────────────────────────
//  AUTO-SEND SCHEDULER
// ─────────────────────────────────────────
async function runAutoSend(force = false, testMode = false) {
  try {
    const settings = await getAdminSettings();
    if (!force && !settings.autoSendEnabled) return;

    const periods = listPayPeriods();
    const today   = new Date().toISOString().slice(0, 10);

    for (const pp of periods) {
      // Fire 2 days after pay period ends
      const endDate  = new Date(pp.end + "T00:00:00Z");
      const fireDate = new Date(endDate.getTime() + 2 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
      if (!force && fireDate !== today) continue;

      console.log(`Auto-send triggered for pay period ${pp.start} → ${pp.end}`);

      const tid = await getDefaultTenantId();
      const usersRes = await query(`SELECT * FROM users WHERE tenant_id=$1 AND is_active=TRUE`, [tid]);

      const users = [];
      for (const row of usersRes.rows) {
        const u = formatUser(row);
        const entries = await getEntriesForPeriod(u.email, pp.start, pp.end);
        if (!entries.length) continue;
        let totalHours = 0, totalPay = 0;
        entries.forEach(e => { totalHours += parseFloat(e.hours) || 0; totalPay += (parseFloat(e.total) || 0) - (parseFloat(e.studioRentalFee) || 0); });
        users.push({ ...u, entries, totalHours: round2(totalHours), totalPay: round2(totalPay) });
      }

      if (!users.length) { console.log("Auto-send: no entries found, skipping."); continue; }

      // APPROVAL SYSTEM DISABLED — treat all users as approved
      // When CONFIG.APPROVAL_SYSTEM_ENABLED is re-enabled, uncomment the block below:
      // const apprRes = await query(
      //   `SELECT user_email, approved FROM payroll_approvals WHERE tenant_id=$1 AND pay_period_start=$2 AND pay_period_end=$3`,
      //   [tid, pp.start, pp.end]
      // );
      // const approvalMap = {};
      // apprRes.rows.forEach(r => { approvalMap[normEmail(r.user_email)] = !!r.approved; });
      // users.forEach(u => {
      //   const em = normEmail(u.email);
      //   if (em in approvalMap) { u._approved = approvalMap[em]; }
      //   else { u._approved = !u.requiresManualApproval; }
      // });
      // const approvedUsers   = users.filter(u => u._approved !== false);
      // const unapprovedUsers = users.filter(u => u._approved === false);
      users.forEach(u => { u._approved = true; });
      const approvedUsers = users;

      // Load late submissions if setting enabled — exclude entries from the period being reported
      const allLateGroups = settings.autoSendPending ? await loadApprovedLateGroups(tid) : [];
      const lateGroups = allLateGroups.filter(g => !(g.ppStart === pp.start && g.ppEnd === pp.end));

      const recipients = getEnabledReportRecipients(settings);
      if (!recipients.length) { console.log("Auto-send: no recipients configured."); continue; }

      // Build payroll email HTML — reuse shared builder with late submissions
      const emailHtml = buildPayrollEmailHtml(users, pp, settings, lateGroups);

      // Build attachments — CSV/PDF include approved users + late submissions
      const attachments = [];
      if (settings.attachQb) {
        const csvRows = [["Name","Email","Type","Pay Period Start","Pay Period End","Total Hours","Gross Pay","GST","Total Owing","GST Number"]];
        // Late submissions first (aggregated per user-period group, with real type)
        lateGroups.forEach(g => {
          const type = g.userType || "Employee";
          const isC  = type.toLowerCase() === "contractor";
          let totalHrs = 0, totalPay = 0;
          g.entries.forEach(e => {
            const hrs  = parseFloat(e.hours_offered||e.hours||0);
            const rate = parseFloat(e.hourly_rate||e.rate||0);
            let tot = parseFloat(e.total||0);
            if (tot === 0 && hrs > 0 && rate > 0) tot = round2(hrs * rate);
            const srf = parseFloat(e.studio_rental_fee||e.studioRentalFee||0);
            totalHrs += hrs;
            totalPay += tot - srf;
          });
          totalHrs = round2(totalHrs);
          totalPay = round2(totalPay);
          const gst = (isC && g.chargeGST) ? round2(totalPay * 0.05) : 0;
          csvRows.push([g.name||g.email, g.email, type, g.ppStart, g.ppEnd,
            totalHrs.toFixed(2), totalPay.toFixed(2), gst.toFixed(2),
            round2(totalPay + gst).toFixed(2),
            (isC && g.gstNumber) ? g.gstNumber : ""]);
        });
        // Current period rows
        approvedUsers.forEach(u => {
          const isC = (u.type||"").toLowerCase() === "contractor";
          const gst = (isC && u.chargeGST) ? round2(u.totalPay * 0.05) : 0;
          csvRows.push([u.name||u.email, u.email, u.type||"Employee", pp.start, pp.end, u.totalHours.toFixed(2), u.totalPay.toFixed(2), gst.toFixed(2), round2(u.totalPay+gst).toFixed(2),
            (isC && u.gstNumber) ? u.gstNumber : ""]);
        });
        const csv = csvRows.map(r => r.map(c => '"'+String(c).replace(/"/g,'""')+'"').join(",")).join("\r\n");
        attachments.push({ filename: `payroll_qb_${pp.start}_${pp.end}.csv`, content: csv });
      }
      if (settings.attachPdf) {
        const pdfBuf = await buildPayrollPdf(users, pp, settings, lateGroups);
        attachments.push({ filename: `payroll_${pp.start}_${pp.end}.pdf`, content: pdfBuf });
      }

      await sendMail({
        to:          recipients[0],
        cc:          recipients.slice(1).join(",") || undefined,
        subject:     `${CONFIG.BRAND_NAME} — Payroll Report ${pp.start} to ${pp.end} (Auto)`,
        html:        emailHtml,
        attachments: attachments.length ? attachments : undefined,
        adminReport: true,
      });

      // Send individual staff copies only to approved users
      // sendMail handles test mode filtering: whitelisted staff get it directly,
      // non-whitelisted are redirected to the whitelist with a [TEST REDIRECT] banner.
      if (settings.emailReportsEnabled !== false) {
        for (const u of approvedUsers) {
          if (!u.emailReports) continue;
          const staffAttach = [];
          if (u.attachPdfPayroll !== false) {
            const pdfBuf = await buildStaffPeriodPdf(u, tid, pp);
            staffAttach.push({ filename: `pay_summary_${pp.start}_${pp.end}.pdf`, content: pdfBuf });
          }
          if (u.attachCsvPayroll) {
            const csv = buildStaffPeriodCsv(u.entries, u, pp);
            staffAttach.push({ filename: `pay_summary_${pp.start}_${pp.end}.csv`, content: csv });
          }
          await sendMail({ to: u.email, subject: `Your Pay Summary — ${CONFIG.BRAND_NAME}`, html: buildStaffEmailHtml(u, pp, settings), attachments: staffAttach.length ? staffAttach : undefined })
            .catch(err => console.error(`Staff email failed for ${u.email}:`, err));
        }
      }

      // Clean up late submissions that were included in the payroll email
      if (lateGroups.length) {
        const allRowIds = lateGroups.flatMap(g => g.rowIds);
        if (allRowIds.length) await query(`DELETE FROM pending_submissions WHERE id = ANY($1)`, [allRowIds]);
        console.log(`Auto-send: cleared ${allRowIds.length} late submission row(s) after including in payroll email.`);
      }

      // Log to sent_reports_log
      const autoTotal = round2(approvedUsers.reduce((s,u) => s + u.totalPay, 0));
      await query(`INSERT INTO sent_reports_log (tenant_id, sent_by, report_type, pp_start, pp_end, recipients, staff_count, total_pay, summary)
        VALUES ($1,$2,'payroll',$3,$4,$5,$6,$7,$8)`,
        [tid, 'auto-send', pp.start, pp.end, recipients.join(','), approvedUsers.length, autoTotal,
         `Auto-send payroll: ${approvedUsers.length} staff, $${autoTotal.toFixed(2)} total`]);

      console.log(`Auto-send complete for ${pp.start} → ${pp.end}, ${users.length} staff.`);
      break; // Only send one period per run
    }
  } catch (err) {
    console.error("Auto-send error:", err.message);
  }
}


// ── sendMassMessage ───────────────────────
api.post("/sendMassMessage", async (req, res) => {
  try {
    const { email, pin, subject, body, recipientGroups, individualEmails } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin or moderator access required." });
    if (!subject || !String(subject).trim()) return res.json({ ok: false, reason: "Subject is required." });
    if (!body || !String(body).trim()) return res.json({ ok: false, reason: "Message body is required." });

    const tid = await getDefaultTenantId();
    const usersRes = await query(`SELECT * FROM users WHERE tenant_id=$1 AND is_active=TRUE`, [tid]);
    const allUsers = usersRes.rows.map(r => formatUser(r));
    const groups   = Array.isArray(recipientGroups) ? recipientGroups : [];
    const indivs   = Array.isArray(individualEmails) ? individualEmails.map(e => normEmail(e)) : [];

    // Build de-duplicated target list
    const targetEmails = new Set();

    // Group-based recipients (exclude admins)
    if (groups.includes("all")) {
      allUsers.filter(u => (u.type||"").toUpperCase() !== "ADMIN").forEach(u => targetEmails.add(normEmail(u.email)));
    } else {
      allUsers.forEach(u => {
        const t = (u.type||"").toLowerCase();
        if (groups.includes("employees")   && t !== "contractor" && t !== "moderator" && t !== "admin") targetEmails.add(normEmail(u.email));
        if (groups.includes("contractors") && t === "contractor") targetEmails.add(normEmail(u.email));
        if (groups.includes("moderators")  && t === "moderator")  targetEmails.add(normEmail(u.email));
      });
    }

    // Individual recipients (any email, including ones not in the DB)
    indivs.forEach(e => { if (e) targetEmails.add(e); });

    if (!targetEmails.size) return res.json({ ok: false, reason: "No recipients selected." });

    const senderName = admin.name || admin.email;
    const htmlBody = `<div style="font-family:Arial,sans-serif;max-width:580px;">
      <h2 style="background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;padding:14px 18px;border-radius:8px 8px 0 0;margin:0;">📣 Message from ${esc_html(senderName)}</h2>
      <div style="border:1px solid #e0e0e0;border-top:0;padding:18px;">
        <div style="white-space:pre-wrap;font-size:14px;line-height:1.6;">${esc_html(String(body).trim())}</div>
        <hr style="border:none;border-top:1px solid #eee;margin:18px 0;">
        <p style="color:#888;font-size:12px;">This message was sent by ${esc_html(senderName)} via ${CONFIG.BRAND_NAME}.</p>
      </div></div>`;

    const count = targetEmails.size;
    res.json({ ok: true, message: `Sending to ${count} recipient(s)… Check Railway logs for results.` });

    // Send async in background — don't block response
    (async () => {
      let sent = 0, failed = 0;
      for (const toEmail of targetEmails) {
        try {
          await sendMail({ to: toEmail, subject: String(subject).trim(), html: htmlBody });
          sent++;
        } catch(err) {
          failed++;
          console.error(`Mass message failed to ${toEmail}:`, err.message);
        }
      }
      console.log(`Mass message complete: ${sent} sent, ${failed} failed. Subject: "${subject}"`);
    })().catch(err => console.error("Mass message background error:", err.message));
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Manual test endpoint (admin only) ────
api.post("/triggerAutoSend", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN") return res.json({ ok: false, reason: "Admin only." });

    await runAutoSend(true, true); // force=true, testMode=true — skips staff emails
    res.json({ ok: true, message: "Auto-send test complete — check your accountant/admin email. Staff pay emails were skipped." });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});


// ── Manual flag rollover test (admin only) ────
api.post("/triggerFlagRollover", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN") return res.json({ ok: false, reason: "Admin only." });

    const count = await rolloverUnresolvedFlags(true); // force=true
    if (count) {
      res.json({ ok: true, message: `Rolled over ${count} unresolved flag(s) to the current pay period.` });
    } else {
      res.json({ ok: true, message: "No unresolved flags to roll over." });
    }
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ─────────────────────────────────────────
//  AUTO REMIND N DAYS BEFORE PERIOD END
// ─────────────────────────────────────────

async function checkAutoRemindBeforePeriodEnd() {
  try {
    const settings = await getAdminSettings();
    if (!settings.autoRemindDaysBeforeEnabled) return;

    const daysBefore = settings.autoRemindDaysBefore || 2;
    const periods = listPayPeriods();
    const today = new Date().toISOString().slice(0, 10);

    for (const pp of periods) {
      // Calculate the reminder date: N days before period end
      const endDate = new Date(pp.end + "T00:00:00Z");
      const remindDate = new Date(endDate.getTime() - daysBefore * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
      if (remindDate !== today) continue;

      console.log(`Auto-remind: ${daysBefore} days before period end ${pp.end}, sending reminders...`);

      const tid = await getDefaultTenantId();
      const submitted = await query(
        `SELECT user_email, COUNT(*) as entry_count FROM entries WHERE tenant_id=$1 AND pay_period_start=$2 GROUP BY user_email`,
        [tid, pp.start]
      );
      const submittedMap = {};
      submitted.rows.forEach(r => { submittedMap[r.user_email] = parseInt(r.entry_count); });

      const staff = await query(
        `SELECT * FROM users WHERE tenant_id=$1 AND is_active=TRUE AND UPPER(type) NOT IN ('ADMIN')`,
        [tid]
      );

      let count = 0;
      const brand = CONFIG.BRAND_NAME || "Aradia Fitness";
      const appUrl = `https://${process.env.APP_DOMAIN || "aradiafitness.app"}`;
      for (const u of staff.rows) {
        const entryCount = submittedMap[u.email] || 0;
        const isPartial = entryCount > 0;
        const subject = isPartial
          ? `Reminder: Confirm your hours are complete — ${brand}`
          : `Reminder: Submit your hours — ${brand}`;
        const message = isPartial
          ? `<p>You have <strong>${entryCount} shift${entryCount===1?'':'s'}</strong> logged so far. Please review and confirm all your hours are entered before the deadline.</p>`
          : `<p>Please submit your hours before the deadline.</p>`;
        await sendMail({
          to: u.email,
          subject,
          html: `<div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;">
            <h2 style="color:${CONFIG.BRAND_COLOR_PRIMARY};">${brand}</h2>
            <p>Hi ${u.name || ""},</p>
            <p>This is a friendly reminder that the current pay period ends on <strong>${pp.end}</strong> — that's ${daysBefore} day${daysBefore === 1 ? "" : "s"} from now.</p>
            ${message}
            <div style="text-align:center;margin:16px 0;">
              <a href="${appUrl}" style="display:inline-block;background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;text-decoration:none;padding:12px 24px;border-radius:8px;font-weight:700;font-size:14px;">Open ${brand}</a>
            </div>
            <p style="color:#888;font-size:12px;">Pay period: ${pp.start} to ${pp.end}</p>
          </div>`,
        }).catch(() => {});
        count++;
      }
      console.log(`Auto-remind: sent ${count} reminder(s) for period ending ${pp.end}`);
      break; // Only process one matching period per run
    }
  } catch (err) {
    console.error("Auto-remind error:", err.message);
  }
}

//  LATE SUBMISSION NOTIFY (last day of period)
// ─────────────────────────────────────────

async function checkUnapprovedPayroll() {
  try {
    const settings = await getAdminSettings();
    const today    = todayStr();
    const periods  = listPayPeriods();
    const period   = periods.find(p => p.end === today);
    if (!period) return;

    const tid = await getDefaultTenantId();
    const appUrl = `https://${process.env.APP_DOMAIN || "aradiafitness.app"}`;

    // All active non-admin staff
    const allStaffRes = await query(
      `SELECT * FROM users WHERE tenant_id=$1 AND is_active=TRUE AND UPPER(type) != 'ADMIN'`,
      [tid]
    );
    const allStaff = allStaffRes.rows;
    if (!allStaff.length) return;

    // Staff who have entries this period
    const entriesRes = await query(
      `SELECT DISTINCT user_email, user_name FROM entries WHERE tenant_id=$1 AND pay_period_start=$2 AND pay_period_end=$3`,
      [tid, period.start, period.end]
    );
    const entryEmails = new Set(entriesRes.rows.map(r => normEmail(r.user_email)));

    // Approvals for this period
    const approvalsRes = await query(
      `SELECT user_email, approved FROM payroll_approvals WHERE tenant_id=$1 AND pay_period_start=$2 AND pay_period_end=$3`,
      [tid, period.start, period.end]
    );
    const approvalMap = {};
    approvalsRes.rows.forEach(r => { approvalMap[normEmail(r.user_email)] = !!r.approved; });

    // ── 1. Email ALL active staff — period ends today reminder ──
    // Even staff with some entries might have more shifts to add
    for (const u of allStaff) {
      const hasEntries = entryEmails.has(normEmail(u.email));
      sendMail({
        to:      u.email,
        subject: `[${CONFIG.BRAND_NAME}] Reminder: Pay period ends today`,
        html: `<div style="font-family:Arial,sans-serif;max-width:580px;">
          <h2 style="background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;padding:14px 18px;border-radius:8px 8px 0 0;margin:0;">${CONFIG.BRAND_NAME} — Period Reminder</h2>
          <div style="border:1px solid #e0e0e0;border-top:0;padding:18px;">
            <p>Hi <strong>${u.name||u.email}</strong>,</p>
            <p>Today is the last day of the pay period <strong>${period.start} → ${period.end}</strong>.</p>
            ${hasEntries
              ? `<p>You have entries on file for this period. If you have any additional shifts to log, please add them before the period closes.</p>`
              : `<p>We currently have <strong>0 entries</strong> on file for you this period. If you worked any shifts, please log in and add them before the period closes.</p>`
            }
            <p><a href="${appUrl}" style="background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:700;display:inline-block;">Open ${CONFIG.BRAND_NAME}</a></p>
            <p style="color:#888;font-size:12px;">If you didn't work this period, no action is needed.</p>
          </div></div>`
      }).catch(() => {});
    }
    const zeroEntryStaff = allStaff.filter(u => !entryEmails.has(normEmail(u.email)));

    // ── 2. Notify admins + payroll admin about unapproved staff ──
    const unapproved = entriesRes.rows.filter(r => approvalMap[normEmail(r.user_email)] === false);
    if (unapproved.length) {
      const admins = getEnabledReportRecipients(settings);
      // Also include payroll admin if enabled
      const pa = getEnabledPayrollAdmin(settings);
      if (pa && !admins.includes(pa)) admins.unshift(pa);

      if (admins.length) {
        const staffRows = unapproved.map(u =>
          `<li style="margin-bottom:6px;"><strong>${u.user_name||u.user_email}</strong> &lt;${u.user_email}&gt;</li>`
        ).join("");

        await sendMail({
          to:      admins[0],
          cc:      admins.slice(1).join(",") || undefined,
          subject: `[${CONFIG.BRAND_NAME}] ${unapproved.length} unapproved timesheet${unapproved.length===1?'':'s'} — pay period ends today`,
          html: `<div style="font-family:Arial,sans-serif;max-width:600px;">
            <h2 style="background:#c0392b;color:#fff;padding:14px 18px;border-radius:8px 8px 0 0;margin:0;">⚠️ Unapproved Timesheets — ${CONFIG.BRAND_NAME}</h2>
            <div style="border:1px solid #e0e0e0;border-top:0;padding:18px;">
              <p>Today is the last day of the pay period <strong>${period.start} → ${period.end}</strong>.</p>
              <p>The following <strong>${unapproved.length} staff member${unapproved.length===1?'':'s'}</strong> ${unapproved.length===1?'has':'have'} submitted timesheets that have <strong>not been approved</strong>:</p>
              <ul style="padding-left:20px;">${staffRows}</ul>
              <p>Please log in and review their submissions before payroll is processed.</p>
              ${zeroEntryStaff.length ? `<p style="color:#888;font-size:12px;">Additionally, ${zeroEntryStaff.length} staff member${zeroEntryStaff.length===1?'':'s'} had 0 entries and ${zeroEntryStaff.length===1?'has':'have'} been sent a reminder.</p>` : ""}
            </div></div>`
        });
        console.log(`Unapproved payroll notify sent: ${unapproved.length} unapproved, ${zeroEntryStaff.length} zero-entry reminders`);
      }
    } else if (zeroEntryStaff.length) {
      console.log(`Period end: ${zeroEntryStaff.length} zero-entry reminders sent, no unapproved staff.`);
    }

  } catch (err) {
    console.error("Unapproved payroll check error:", err.message);
  }
}

async function checkLateSubmissionNotify() {
  try {
    const settings = await getAdminSettings();
    if (!settings.notifyLateAdmin) return;

    const today   = new Date().toISOString().slice(0, 10);
    const periods = listPayPeriods();
    const period  = periods.find(p => p.end === today);
    if (!period) return; // not last day of any period

    const tid        = await getDefaultTenantId();
    const pendingRes = await query(
      `SELECT * FROM pending_submissions WHERE tenant_id=$1 ORDER BY submitted_at DESC`, [tid]
    );
    if (!pendingRes.rows.length) return; // nothing to report

    // Group by staff
    const groups = {};
    pendingRes.rows.forEach(r => {
      const ps = r.pay_period_start instanceof Date ? r.pay_period_start.toISOString().slice(0,10) : String(r.pay_period_start||"").slice(0,10);
      const pe = r.pay_period_end   instanceof Date ? r.pay_period_end.toISOString().slice(0,10)   : String(r.pay_period_end||"").slice(0,10);
      const key = r.user_email+"|"+ps+"|"+pe;
      if (!groups[key]) groups[key] = { email: r.user_email, name: r.user_name||r.user_email, ppStart: ps, ppEnd: pe, entries: [] };
      groups[key].entries.push(r);
    });
    const allGroups = Object.values(groups);
    const totalShifts = allGroups.reduce((s,g) => s + g.entries.length, 0);
    const totalPay    = allGroups.reduce((s,g) => s + g.entries.reduce((ss,e) => ss + (parseFloat(e.total)||0), 0), 0);

    const recipients = [settings.adminEmail1, settings.adminEmail2, settings.adminEmail3].filter(Boolean);
    if (!recipients.length) return;

    const staffList = allGroups.map(g => {
      const shifts = g.entries.length;
      const pay    = g.entries.reduce((s,e) => s + (parseFloat(e.total)||0), 0);
      return `<li style="margin-bottom:6px;"><strong>${g.name}</strong> (${g.email}) — ${shifts} shift${shifts===1?'':'s'}, $${pay.toFixed(2)} — period ${g.ppStart} to ${g.ppEnd}</li>`;
    }).join("");

    const html = `<div style="font-family:Arial,sans-serif;max-width:600px;">
      <h2 style="background:#c07a00;color:#fff;padding:14px 18px;border-radius:8px 8px 0 0;margin:0;">⏳ Late Submissions — ${CONFIG.BRAND_NAME}</h2>
      <div style="border:1px solid #e0e0e0;border-top:0;padding:16px;">
        <p>Today is the last day of the pay period (<strong>${period.start} → ${period.end}</strong>).</p>
        <p>There are <strong>${allGroups.length} staff member${allGroups.length===1?'':'s'}</strong> with <strong>${totalShifts} late submission${totalShifts===1?'':'s'}</strong> totalling approximately <strong>$${totalPay.toFixed(2)}</strong> in additional pay to expect this period.</p>
        <ul style="padding-left:20px;">${staffList}</ul>
        <p style="color:#888;font-size:12px;">These will be automatically included in the payroll report when it sends.</p>
      </div></div>`;

    await sendMail({
      to:      recipients[0],
      cc:      recipients.slice(1).join(",") || undefined,
      subject: `[${CONFIG.BRAND_NAME}] ${allGroups.length} late submission${allGroups.length===1?'':'s'} — pay period ends today`,
      html,
    });
    console.log(`Late submission notify sent: ${allGroups.length} staff, ${totalShifts} shifts`);
  } catch (err) {
    console.error("Late submission notify error:", err.message);
  }
}

// ── Staff PDF download endpoint ──
api.post("/staffPdf", async (req, res) => {
  try {
    const { email, pin, type, ppStart, ppEnd, dateFrom, dateTo } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.status(401).json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();

    let pdfBuf, filename;
    if (type === "earnings") {
      pdfBuf   = await buildStaffEarningsPdf(user, tid, dateFrom, dateTo);
      filename = "earnings_" + (user.name||user.email).replace(/\s+/g,"_") + ".pdf";
    } else {
      pdfBuf   = await buildStaffPeriodPdf(user, tid, { start: ppStart, end: ppEnd });
      filename = "pay_period_" + ppStart + "_" + ppEnd + ".pdf";
    }

    res.set({
      "Content-Type": "application/pdf",
      "Content-Disposition": `attachment; filename="${filename}"`,
      "Content-Length": pdfBuf.length,
    });
    res.send(pdfBuf);
  } catch (e) {
    console.error("staffPdf error:", e);
    res.status(500).json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ─────────────────────────────────────────
//  GOOGLE CALENDAR API ENDPOINTS
// ─────────────────────────────────────────

// 3a. Generate OAuth URL
api.post("/googleAuthUrl", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!process.env.GOOGLE_OAUTH_CLIENT_ID) return res.json({ ok: false, reason: "Google Calendar integration is not configured." });
    const oauth2 = getOAuth2Client();
    const statePayload = Buffer.from(JSON.stringify({ email: user.email })).toString("base64");
    const url = oauth2.generateAuthUrl({
      access_type: "offline",
      prompt: "consent",
      scope: ["https://www.googleapis.com/auth/calendar.readonly"],
      state: statePayload,
    });
    res.json({ ok: true, url });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// 3c. Disconnect Google Calendar
api.post("/googleCalendarDisconnect", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    // Try to revoke the token (best-effort)
    const tokenRes = await query(
      `SELECT access_token FROM google_calendar_tokens WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2)`,
      [tid, user.email]
    );
    if (tokenRes.rows.length && tokenRes.rows[0].access_token) {
      try {
        const oauth2 = getOAuth2Client();
        await oauth2.revokeToken(tokenRes.rows[0].access_token);
      } catch (e) { console.log("Token revocation failed (non-critical):", e.message); }
    }
    await query(`DELETE FROM google_calendar_tokens WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2)`, [tid, user.email]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// 3d. Fetch Google Calendar events
// List user's Google Calendars
api.post("/googleCalendarList", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const cal = await getCalendarClientForUser(user.email);
    if (!cal) return res.json({ ok: false, reason: "Google Calendar not connected." });
    const listRes = await cal.calendarList.list();
    const calendars = (listRes.data.items || []).map(c => ({
      id: c.id,
      name: c.summary || c.id,
      primary: !!c.primary,
      color: c.backgroundColor || "#4285f4",
    }));
    res.json({ ok: true, calendars });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/googleCalendarEvents", async (req, res) => {
  try {
    const { email, pin, dateMin, dateMax, calendarId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const cal = await getCalendarClientForUser(user.email);
    if (!cal) return res.json({ ok: false, reason: "Google Calendar not connected. Please connect in your Profile." });
    const tid = await getDefaultTenantId();
    // Fetch events — use Edmonton timezone bounds so we don't miss events at day boundaries
    // Add 1-day buffer on each side to handle UTC ↔ MST/MDT offset, then filter precisely below
    const minDate = new Date(dateMin + "T00:00:00Z");
    minDate.setDate(minDate.getDate() - 1);
    const maxDate = new Date(dateMax + "T00:00:00Z");
    maxDate.setDate(maxDate.getDate() + 2);
    // Paginate through all results (Google API returns max 250 per page)
    let items = [];
    let pageToken = undefined;
    do {
      const eventsRes = await cal.events.list({
        calendarId: calendarId || "primary",
        timeMin: minDate.toISOString(),
        timeMax: maxDate.toISOString(),
        singleEvents: true,
        orderBy: "startTime",
        maxResults: 250,
        timeZone: "America/Edmonton",
        ...(pageToken ? { pageToken } : {}),
      });
      items = items.concat(eventsRes.data.items || []);
      pageToken = eventsRes.data.nextPageToken;
    } while (pageToken);
    // Load locations with addresses for matching
    const locRows = await query(`SELECT id, name, address FROM locations WHERE tenant_id=$1 ORDER BY sort_order, name`, [tid]);
    const locations = locRows.rows;
    // Map events — extract date/time from Google's timezone-aware strings (Edmonton)
    // rather than using JS Date getHours() which would return UTC on a UTC server
    const events = [];
    for (const ev of items) {
      if (!ev.start || !ev.start.dateTime) continue; // skip all-day events
      const start = new Date(ev.start.dateTime);
      const end   = new Date(ev.end.dateTime);
      const diffMs = end - start;
      const hours  = Math.round((diffMs / 3600000) * 100) / 100;
      if (hours <= 0) continue;
      // Extract local date/time from the Google dateTime string (e.g. "2026-03-30T18:00:00-06:00")
      const dateStr = ev.start.dateTime.slice(0, 10);
      // Filter: only include events whose local date falls within the requested range
      if (dateStr < dateMin || dateStr > dateMax) continue;
      // Parse local hours/minutes from the dateTime strings
      const startH = parseInt(ev.start.dateTime.slice(11, 13), 10);
      const startM = parseInt(ev.start.dateTime.slice(14, 16), 10);
      const endH   = parseInt(ev.end.dateTime.slice(11, 13), 10);
      const endM   = parseInt(ev.end.dateTime.slice(14, 16), 10);
      const fmt12 = (h, m) => {
        const ampm = h >= 12 ? "PM" : "AM";
        const h12 = h % 12 || 12;
        return h12 + ":" + String(m).padStart(2, "0") + " " + ampm;
      };
      const fmt24 = (h, m) => String(h).padStart(2, "0") + ":" + String(m).padStart(2, "0");
      const time12 = fmt12(startH, startM) + " – " + fmt12(endH, endM);
      const time24 = fmt24(startH, startM) + " – " + fmt24(endH, endM);
      // Location matching — fuzzy: substring, word-based, and normalized
      const evLoc = (ev.location || "").trim();
      let matchedLocation = null;
      if (evLoc) {
        const evLocLower = evLoc.toLowerCase().replace(/[^a-z0-9\s]/g, " ").replace(/\s+/g, " ").trim();
        let bestScore = 0;
        for (const loc of locations) {
          const nameLower = (loc.name || "").toLowerCase().replace(/[^a-z0-9\s]/g, " ").replace(/\s+/g, " ").trim();
          const addrLower = (loc.address || "").toLowerCase().replace(/[^a-z0-9\s]/g, " ").replace(/\s+/g, " ").trim();
          if (!nameLower) continue;
          // Exact substring match (either direction)
          if (evLocLower.includes(nameLower) || nameLower.includes(evLocLower)) {
            matchedLocation = loc.name; break;
          }
          // Address substring match
          if (addrLower && (evLocLower.includes(addrLower) || addrLower.includes(evLocLower))) {
            matchedLocation = loc.name; break;
          }
          // Word-based: all words of the location name appear in the Google location
          const nameWords = nameLower.split(" ").filter(w => w.length > 1);
          if (nameWords.length > 0) {
            const matched = nameWords.filter(w => evLocLower.includes(w));
            const score = matched.length / nameWords.length;
            if (score >= 1) { matchedLocation = loc.name; break; } // all words match
            if (score > bestScore && score >= 0.5 && matched.length >= 2) {
              bestScore = score; matchedLocation = loc.name; // partial but strong match
            }
          }
        }
      }
      events.push({
        googleEventId: ev.id,
        title: ev.summary || "(No title)",
        date: dateStr,
        startTime12: fmt12(startH, startM),
        endTime12: fmt12(endH, endM),
        time12,
        time24,
        hours,
        googleLocation: evLoc,
        matchedLocation,
        description: ev.description || "",
      });
    }
    res.json({ ok: true, events, locations: locations.map(l => l.name) });
  } catch (e) {
    if (e.message && e.message.includes("invalid_grant")) {
      return res.json({ ok: false, reason: "Google Calendar connection expired. Please reconnect in your Profile." });
    }
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// 3e. Update location address (admin/moderator only)
api.post("/updateLocationAddress", async (req, res) => {
  try {
    const { email, pin, locationId, address } = req.body;
    const admin = await getAuthorizedUser(email, pin);
    if (!admin) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(admin.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });
    const tid = await getDefaultTenantId();
    await query(`UPDATE locations SET address=$1 WHERE id=$2 AND tenant_id=$3`, [(address || "").trim(), locationId, tid]);
    const rows = await query(`SELECT id, name, sort_order, address FROM locations WHERE tenant_id=$1 ORDER BY sort_order, name`, [tid]);
    res.json({ ok: true, locations: rows.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ─────────────────────────────────────────
//  MOUNT API + SERVE APP
// ─────────────────────────────────────────
app.use("/api", api);

// 3b. Google OAuth callback (top-level Express route)
app.get("/auth/google/callback", async (req, res) => {
  try {
    const { code, state } = req.query;
    if (!code || !state) return res.redirect("/?gcal=error&reason=missing_params");
    let userEmail;
    try {
      const decoded = JSON.parse(Buffer.from(state, "base64").toString());
      userEmail = decoded.email;
    } catch (e) {
      return res.redirect("/?gcal=error&reason=invalid_state");
    }
    const oauth2 = getOAuth2Client();
    const { tokens } = await oauth2.getToken(code);
    const tid = await getDefaultTenantId();
    await query(`
      INSERT INTO google_calendar_tokens (tenant_id, user_email, access_token, refresh_token, token_expiry, connected_at)
      VALUES ($1, $2, $3, $4, $5, NOW())
      ON CONFLICT (tenant_id, user_email) DO UPDATE SET
        access_token = EXCLUDED.access_token,
        refresh_token = COALESCE(EXCLUDED.refresh_token, google_calendar_tokens.refresh_token),
        token_expiry = EXCLUDED.token_expiry,
        connected_at = NOW()
    `, [tid, userEmail, tokens.access_token, tokens.refresh_token, tokens.expiry_date ? new Date(tokens.expiry_date) : null]);
    res.redirect("/?gcal=connected");
  } catch (e) {
    console.error("Google OAuth callback error:", e.message);
    res.redirect("/?gcal=error&reason=" + encodeURIComponent(e.message));
  }
});

// ── Image Generator Proxy Endpoints ────────────────────────────
const IMAGE_GENERATOR_URL = process.env.IMAGE_GENERATOR_URL || "";

api.post("/generateImage", async (req, res) => {
  try {
    const { email, pin, studio, class_name, subtitle, entries, background, template, layout } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!user.canCreateImages) return res.json({ ok: false, reason: "You do not have permission to create images." });
    if (!IMAGE_GENERATOR_URL) return res.json({ ok: false, reason: "Image generator not configured." });

    const payload = { studio, class_name, subtitle, entries, background, template: template || "square" };
    if (layout) payload.layout = layout;
    const resp = await fetch(`${IMAGE_GENERATOR_URL}/generate-custom`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      const errText = await resp.text();
      return res.json({ ok: false, reason: `Image generator error: ${errText}` });
    }
    const arrayBuf = await resp.arrayBuffer();
    const base64 = Buffer.from(arrayBuf).toString("base64");
    res.json({ ok: true, image: base64 });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/emailImage", async (req, res) => {
  try {
    const { email, pin, image, filename } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!user.canCreateImages) return res.json({ ok: false, reason: "You do not have permission to create images." });

    const resend = getResend();
    const from = process.env.RESEND_FROM || `${CONFIG.BRAND_NAME} <support@aradiafitness.app>`;
    const fname = filename || "aradia-card.png";

    await mailRateLimit();
    await sendMailWithRetry(() => resend.emails.send({
      from,
      to: [user.email],
      subject: `Your Custom Image: ${fname}`,
      html: `<p>Hi ${user.name || "there"},</p><p>Here's the custom social media card you generated. It's attached as a PNG.</p><p>— Aradia Bot</p>`,
      attachments: [{ filename: fname, content: image }],
    }), 2);

    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

app.get("/api/getBackgroundThumb", async (req, res) => {
  try {
    const name = req.query.name;
    const template = req.query.template || "square";
    if (!name || !IMAGE_GENERATOR_URL) return res.status(404).send("Not found");
    const resp = await fetch(`${IMAGE_GENERATOR_URL}/backgrounds/${encodeURIComponent(name)}?template=${encodeURIComponent(template)}`);
    if (!resp.ok) return res.status(404).send("Not found");
    res.set("Content-Type", "image/png");
    res.set("Cache-Control", "public, max-age=86400");
    const buf = Buffer.from(await resp.arrayBuffer());
    res.send(buf);
  } catch (e) {
    res.status(500).send("Error");
  }
});

api.post("/getBackgrounds", async (req, res) => {
  try {
    const { email, pin, template } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!IMAGE_GENERATOR_URL) return res.json({ ok: false, reason: "Image generator not configured." });

    const resp = await fetch(`${IMAGE_GENERATOR_URL}/backgrounds?template=${encodeURIComponent(template || "square")}`);
    if (!resp.ok) return res.json({ ok: false, reason: "Failed to fetch backgrounds." });
    const data = await resp.json();
    res.json({ ok: true, backgrounds: data.backgrounds || [] });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/uploadBackground", async (req, res) => {
  try {
    const { email, pin, fileBase64, fileName, template } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!user.canCreateImages) return res.json({ ok: false, reason: "You do not have permission to manage images." });
    if (!IMAGE_GENERATOR_URL) return res.json({ ok: false, reason: "Image generator not configured." });
    if (!fileBase64) return res.json({ ok: false, reason: "No file data provided." });

    const buf = Buffer.from(fileBase64, "base64");
    const formData = new FormData();
    formData.append("file", new Blob([buf], { type: "image/png" }), fileName || "upload.png");

    const resp = await fetch(`${IMAGE_GENERATOR_URL}/backgrounds/upload?template=${encodeURIComponent(template || "square")}`, {
      method: "POST",
      body: formData,
    });
    if (!resp.ok) {
      const errText = await resp.text();
      return res.json({ ok: false, reason: `Upload failed: ${errText}` });
    }
    const data = await resp.json();
    res.json({ ok: true, filename: data.filename });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/deleteBackground", async (req, res) => {
  try {
    const { email, pin, name, template } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!user.canCreateImages) return res.json({ ok: false, reason: "You do not have permission to manage images." });
    if (!IMAGE_GENERATOR_URL) return res.json({ ok: false, reason: "Image generator not configured." });
    if (!name) return res.json({ ok: false, reason: "No background name provided." });

    const resp = await fetch(`${IMAGE_GENERATOR_URL}/backgrounds/${encodeURIComponent(name)}?template=${encodeURIComponent(template || "square")}`, {
      method: "DELETE",
    });
    if (!resp.ok) {
      const errText = await resp.text();
      return res.json({ ok: false, reason: `Delete failed: ${errText}` });
    }
    const data = await resp.json();
    res.json({ ok: true, deleted: data.deleted });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/unhideBackground", async (req, res) => {
  try {
    const { email, pin, name, template } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!user.canCreateImages) return res.json({ ok: false, reason: "You do not have permission to manage images." });
    if (!IMAGE_GENERATOR_URL) return res.json({ ok: false, reason: "Image generator not configured." });
    if (!name) return res.json({ ok: false, reason: "No background name provided." });

    const resp = await fetch(`${IMAGE_GENERATOR_URL}/backgrounds/${encodeURIComponent(name)}/unhide?template=${encodeURIComponent(template || "square")}`, {
      method: "POST",
    });
    if (!resp.ok) {
      const errText = await resp.text();
      return res.json({ ok: false, reason: `Unhide failed: ${errText}` });
    }
    const data = await resp.json();
    res.json({ ok: true, unhidden: data.unhidden });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/scrapeClasses", async (req, res) => {
  // Extend Express response timeout for this long-running endpoint
  req.setTimeout(720000);
  res.setTimeout(720000);
  try {
    const { email, pin, search_term, studio } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!user.canCreateImages) return res.json({ ok: false, reason: "You do not have permission to create images." });
    if (!IMAGE_GENERATOR_URL) return res.json({ ok: false, reason: "Image generator not configured." });

    const resp = await fetch(`${IMAGE_GENERATOR_URL}/scrape`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ search_term: search_term || null, studio: studio || null }),
      signal: AbortSignal.timeout(660000), // 11 min — scraping all 4 studios takes ~10 min
    });
    if (!resp.ok) {
      const errText = await resp.text();
      return res.json({ ok: false, reason: `Scrape error: ${errText}` });
    }
    const data = await resp.json();
    res.json({ ok: data.ok !== false, classes: data.classes || [], reason: data.error || undefined });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/getLayouts", async (req, res) => {
  try {
    const { email, pin, template } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!IMAGE_GENERATOR_URL) return res.json({ ok: false, reason: "Image generator not configured." });

    const tpl = template || "square";
    const resp = await fetch(`${IMAGE_GENERATOR_URL}/layouts/${encodeURIComponent(tpl)}`);
    if (!resp.ok) return res.json({ ok: true, presets: [] });
    const data = await resp.json();
    res.json({ ok: true, presets: data.presets || [] });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/getLayout", async (req, res) => {
  try {
    const { email, pin, template, name } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!IMAGE_GENERATOR_URL) return res.json({ ok: false, reason: "Image generator not configured." });

    const tpl = template || "square";
    const presetName = name || "Default";
    const resp = await fetch(`${IMAGE_GENERATOR_URL}/layouts/${encodeURIComponent(tpl)}/${encodeURIComponent(presetName)}`);
    if (!resp.ok) return res.json({ ok: true, layout: {} });
    const data = await resp.json();
    res.json({ ok: true, layout: data.layout || {} });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/saveLayout", async (req, res) => {
  try {
    const { email, pin, template, name, layout } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!user.canCreateImages) return res.json({ ok: false, reason: "You do not have permission to manage images." });
    if (!IMAGE_GENERATOR_URL) return res.json({ ok: false, reason: "Image generator not configured." });

    const tpl = template || "square";
    const presetName = name || "Default";
    const resp = await fetch(`${IMAGE_GENERATOR_URL}/layouts/${encodeURIComponent(tpl)}/${encodeURIComponent(presetName)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(layout || {}),
    });
    if (!resp.ok) {
      const errText = await resp.text();
      return res.json({ ok: false, reason: `Save layout failed: ${errText}` });
    }
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/deleteLayout", async (req, res) => {
  try {
    const { email, pin, template, name } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!user.canCreateImages) return res.json({ ok: false, reason: "You do not have permission to manage images." });
    if (!IMAGE_GENERATOR_URL) return res.json({ ok: false, reason: "Image generator not configured." });

    const tpl = template || "square";
    const presetName = name || "Default";
    const resp = await fetch(`${IMAGE_GENERATOR_URL}/layouts/${encodeURIComponent(tpl)}/${encodeURIComponent(presetName)}`, {
      method: "DELETE",
    });
    if (!resp.ok) {
      const errText = await resp.text();
      return res.json({ ok: false, reason: `Delete layout failed: ${errText}` });
    }
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Remove Background (remove.bg API proxy) ──
const REMOVEBG_API_KEY = process.env.REMOVEBG_API_KEY || "";

api.post("/removeBackground", async (req, res) => {
  try {
    const { email, pin, imageBase64 } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!user.canCreateImages) return res.json({ ok: false, reason: "You do not have permission to create images." });
    if (!REMOVEBG_API_KEY) return res.json({ ok: false, reason: "Background removal not configured (no API key)." });
    if (!imageBase64) return res.json({ ok: false, reason: "No image data provided." });

    const resp = await fetch("https://api.remove.bg/v1.0/removebg", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Api-Key": REMOVEBG_API_KEY,
      },
      body: JSON.stringify({
        image_file_b64: imageBase64,
        size: "auto",
      }),
    });

    if (!resp.ok) {
      const errData = await resp.text();
      return res.json({ ok: false, reason: `remove.bg error (${resp.status}): ${errData}` });
    }

    const arrayBuf = await resp.arrayBuffer();
    const base64 = Buffer.from(arrayBuf).toString("base64");
    res.json({ ok: true, image: base64 });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── TSPS (Take a Shift, Post a Shift) ──

api.post("/getShiftPosts", async (req, res) => {
  try {
    const { email, pin, month, year } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const startDate = `${year}-${String(month).padStart(2,"0")}-01`;
    const endDate = `${year}-${String(month).padStart(2,"0")}-${new Date(year, month, 0).getDate()}`;
    const rows = await query(
      `SELECT sp.id, sp.poster_email, sp.location, sp.shift_time, sp.shift_date, sp.notes, sp.status,
              sp.claimed_by, COALESCE(sp.claimed_by_name, cu.name, sp.claimed_by) AS claimed_by_name,
              sp.claimed_at, sp.created_at, sp.class_name, sp.poster_name, sp.duration, sp.front_desk
       FROM shift_posts sp
       LEFT JOIN users cu ON LOWER(cu.email) = LOWER(sp.claimed_by) AND cu.tenant_id = sp.tenant_id
       WHERE sp.tenant_id=$1 AND sp.shift_date >= $2 AND sp.shift_date <= $3
       ORDER BY sp.shift_date, sp.shift_time`, [tid, startDate, endDate]
    );
    res.json({ ok: true, shifts: rows.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/postShift", async (req, res) => {
  try {
    const { email, pin, shifts } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    if (!Array.isArray(shifts) || shifts.length === 0) return res.json({ ok: false, reason: "No shifts provided." });
    // Look up poster's display name
    const posterRow = await query(`SELECT name FROM users WHERE tenant_id=$1 AND LOWER(email)=LOWER($2)`, [tid, email]);
    const posterName = posterRow.rows[0]?.name || email;
    const created = [];
    for (const s of shifts) {
      if (!s.location || !s.shift_time || !s.shift_date) continue;
      const r = await query(
        `INSERT INTO shift_posts (tenant_id, poster_email, location, shift_time, shift_date, notes, class_name, poster_name, duration, front_desk)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
        [tid, email, s.location, s.shift_time, s.shift_date, s.notes || "", s.class_name || "", posterName, parseInt(s.duration) || 60, !!s.front_desk]
      );
      created.push(r.rows[0].id);
    }
    res.json({ ok: true, created });
    // Notify all subscribed staff about new shift(s)
    if (created.length > 0) {
      const s0 = shifts[0];
      const desc = s0.class_name ? `${s0.class_name} at ${s0.location}` : s0.location;
      const _shiftText = shifts.map(s => `${s.class_name||""} ${s.location||""} ${s.notes||""}`).join(" ").toLowerCase();
      // Send push to all staff, filtering by shift keywords (and front desk)
      const _isFrontDesk = !!s0.front_desk;
      try {
        const _pushUsers = await query(
          `SELECT DISTINCT ps.user_email, u.shift_filter_keywords, u.front_desk_staff, u.front_desk_only FROM push_subscriptions ps LEFT JOIN users u ON LOWER(u.email)=LOWER(ps.user_email) AND u.tenant_id=ps.tenant_id WHERE ps.tenant_id=$1`,
          [tid]
        );
        for (const pu of _pushUsers.rows) {
          if (pu.user_email.toLowerCase() === email.toLowerCase()) continue;
          if (_isFrontDesk && !pu.front_desk_staff) continue;
          if (!_isFrontDesk && pu.front_desk_only) continue;
          if (pu.shift_filter_keywords) {
            const filters = pu.shift_filter_keywords.split(",").map(k => k.trim().toLowerCase()).filter(Boolean);
            if (filters.some(kw => _shiftText.includes(kw))) continue;
          }
          sendPush(pu.user_email, "🔄 New Shift Posted", `${posterName} posted a shift: ${desc} on ${s0.shift_date}`, "/", "shifts").catch(() => {});
        }
      } catch (e2) { console.error("TSPS filtered push error:", e2.message); }
      // Email TSPS notification recipient
      const _settings = await getAdminSettings();
      if (_settings.tspsNotifyEnabled && _settings.tspsNotifyEmail) {
        const shiftList = shifts.filter(s => s.location && s.shift_time && s.shift_date)
          .map(s => `<div style="background:#f5f5f5;border-radius:8px;padding:10px 14px;margin:6px 0;"><strong>${s.class_name || "Shift"}</strong> — ${s.location} @ ${s.shift_time} on ${s.shift_date}${s.notes ? ` (${s.notes})` : ""}</div>`).join("");
        sendMail({
          to: _settings.tspsNotifyEmail,
          subject: `${CONFIG.BRAND_NAME} — New Shift Posted by ${posterName}`,
          html: `<div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;">
            <h2 style="color:${CONFIG.BRAND_COLOR_PRIMARY};">${CONFIG.BRAND_NAME} — New Shift Posted</h2>
            <p><strong>${posterName}</strong> (${email}) posted ${created.length} shift(s):</p>
            ${shiftList}
            <p style="color:#888;font-size:12px;">TSPS automated notification</p>
          </div>`
        }).catch(e => console.error("TSPS notify email (post) failed:", e.message));
      }
      // Email all users with email_shifts enabled (not urgent-only), except the poster
      try {
        const emailUsers = await query(
          `SELECT email, name, shift_filter_keywords, front_desk_staff, front_desk_only FROM users WHERE tenant_id=$1 AND email_shifts=TRUE AND email_shifts_urgent_only=FALSE AND LOWER(email) != LOWER($2) AND tsps_enabled=TRUE AND (is_active IS NULL OR is_active=TRUE)`,
          [tid, email]
        );
        const s0 = shifts[0];
        // Build searchable text from all shifts for keyword matching
        const shiftSearchText = shifts.map(s => `${s.class_name||""} ${s.location||""} ${s.notes||""}`).join(" ").toLowerCase();
        const shiftListHtml = shifts.filter(s => s.location && s.shift_time && s.shift_date)
          .map(s => `<div style="background:#f5f5f5;border-radius:8px;padding:10px 14px;margin:6px 0;"><strong>${s.class_name || "Shift"}</strong> — ${s.location} @ ${s.shift_time} on ${s.shift_date}${s.notes ? ` (${s.notes})` : ""}</div>`).join("");
        for (const u of emailUsers.rows) {
          // Front desk filter: if shift is front-desk-only, skip non-front-desk staff
          if (_isFrontDesk && !u.front_desk_staff) continue;
          // Front desk only filter: if user is front-desk-only, skip non-front-desk shifts
          if (!_isFrontDesk && u.front_desk_only) continue;
          // Check keyword filter
          if (u.shift_filter_keywords) {
            const filters = u.shift_filter_keywords.split(",").map(k => k.trim().toLowerCase()).filter(Boolean);
            if (filters.some(kw => shiftSearchText.includes(kw))) continue;
          }
          sendMail({
            to: u.email,
            subject: `${CONFIG.BRAND_NAME} — New Shift Available`,
            html: `<div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;">
              <h2 style="color:${CONFIG.BRAND_COLOR_PRIMARY};">${CONFIG.BRAND_NAME} — New Shift Posted</h2>
              <p><strong>${posterName}</strong> posted ${created.length} shift(s):</p>
              ${shiftListHtml}
              <p style="margin-top:16px;text-align:center;">
                <a href="${process.env.BASE_URL || 'https://aradiafitness.app'}?tab=tsps" style="display:inline-block;background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;font-weight:700;font-size:15px;padding:12px 32px;border-radius:8px;text-decoration:none;">CLAIM SHIFT</a>
              </p>
              <p style="color:#888;font-size:12px;">You can manage email preferences in your profile settings.</p>
            </div>`
          }).catch(e => console.error("TSPS user email (post) failed:", e.message));
        }
      } catch (e2) { console.error("TSPS user email batch error:", e2.message); }
    }
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/claimShift", async (req, res) => {
  try {
    const { email, pin, shiftId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const shiftRes = await query(`SELECT * FROM shift_posts WHERE id=$1 AND tenant_id=$2`, [shiftId, tid]);
    if (shiftRes.rows.length === 0) return res.json({ ok: false, reason: "Shift not found." });
    const shift = shiftRes.rows[0];
    if (shift.status !== "open") return res.json({ ok: false, reason: "This shift has already been claimed." });
    if (shift.poster_email.toLowerCase() === email.toLowerCase()) return res.json({ ok: false, reason: "You cannot claim your own shift." });
    // Look up claimer name before the update
    const claimerUser = await query(`SELECT name FROM users WHERE tenant_id=$1 AND LOWER(email)=LOWER($2)`, [tid, email]);
    const claimerName = claimerUser.rows[0]?.name || email;
    await query(
      `UPDATE shift_posts SET status='claimed', claimed_by=$1, claimed_by_name=$2, claimed_at=NOW() WHERE id=$3`,
      [email, claimerName, shiftId]
    );
    const posterUser = await query(`SELECT name FROM users WHERE tenant_id=$1 AND LOWER(email)=LOWER($2)`, [tid, shift.poster_email]);
    const posterName = posterUser.rows[0]?.name || shift.poster_email;
    const subj = `${CONFIG.BRAND_NAME} — Your shift has been claimed`;
    const body = `<div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;">
      <h2 style="color:${CONFIG.BRAND_COLOR_PRIMARY};">${CONFIG.BRAND_NAME} — Shift Claimed</h2>
      <p>Hi ${posterName},</p>
      <p><strong>${claimerName}</strong> has claimed your posted shift:</p>
      <div style="background:#f5f5f5;border-radius:8px;padding:12px 16px;margin:12px 0;">
        <div><strong>Date:</strong> ${shift.shift_date}</div>
        <div><strong>Time:</strong> ${shift.shift_time}</div>
        <div><strong>Location:</strong> ${shift.location}</div>
        ${shift.notes ? `<div><strong>Notes:</strong> ${shift.notes}</div>` : ""}
      </div>
      <p style="color:#888;font-size:12px;">This is an automated notification from TSPS (Take a Shift, Post a Shift).</p>
    </div>`;
    await sendMail({ to: shift.poster_email, subject: subj, html: body }).catch(e => console.error("TSPS email to poster failed:", e.message));
    sendPush(shift.poster_email, "🔄 Shift Claimed", `${claimerName} claimed your shift on ${shift.shift_date}.`, "/", "shifts").catch(() => {});
    // Email scheduling manager
    const settings = await getAdminSettings();
    if (settings.schedulingManagerEmail) {
      const mgrBody = `<div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;">
        <h2 style="color:${CONFIG.BRAND_COLOR_PRIMARY};">${CONFIG.BRAND_NAME} — Shift Claimed (Manager Notice)</h2>
        <p><strong>${claimerName}</strong> (${email}) claimed a shift posted by <strong>${posterName}</strong> (${shift.poster_email}):</p>
        <div style="background:#f5f5f5;border-radius:8px;padding:12px 16px;margin:12px 0;">
          <div><strong>Date:</strong> ${shift.shift_date}</div>
          <div><strong>Time:</strong> ${shift.shift_time}</div>
          <div><strong>Location:</strong> ${shift.location}</div>
          ${shift.notes ? `<div><strong>Notes:</strong> ${shift.notes}</div>` : ""}
        </div>
      </div>`;
      await sendMail({ to: settings.schedulingManagerEmail, subject: `${CONFIG.BRAND_NAME} — Shift Claimed Notice`, html: mgrBody }).catch(e => console.error("TSPS email to manager failed:", e.message));
    }
    // Email TSPS notification recipient
    if (settings.tspsNotifyEnabled && settings.tspsNotifyEmail) {
      const notifyBody = `<div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;">
        <h2 style="color:${CONFIG.BRAND_COLOR_PRIMARY};">${CONFIG.BRAND_NAME} — Shift Claimed</h2>
        <p><strong>${claimerName}</strong> (${email}) claimed a shift posted by <strong>${posterName}</strong> (${shift.poster_email}):</p>
        <div style="background:#f5f5f5;border-radius:8px;padding:12px 16px;margin:12px 0;">
          ${shift.class_name ? `<div><strong>Class:</strong> ${shift.class_name}</div>` : ""}
          <div><strong>Date:</strong> ${shift.shift_date}</div>
          <div><strong>Time:</strong> ${shift.shift_time}</div>
          <div><strong>Location:</strong> ${shift.location}</div>
          ${shift.notes ? `<div><strong>Notes:</strong> ${shift.notes}</div>` : ""}
        </div>
        <p style="color:#888;font-size:12px;">TSPS automated notification</p>
      </div>`;
      sendMail({ to: settings.tspsNotifyEmail, subject: `${CONFIG.BRAND_NAME} — Shift Claimed: ${claimerName}`, html: notifyBody }).catch(e => console.error("TSPS notify email (claim) failed:", e.message));
    }
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/deleteShiftPost", async (req, res) => {
  try {
    const { email, pin, shiftId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const shiftRes = await query(`SELECT * FROM shift_posts WHERE id=$1 AND tenant_id=$2`, [shiftId, tid]);
    if (shiftRes.rows.length === 0) return res.json({ ok: false, reason: "Shift not found." });
    const shift = shiftRes.rows[0];
    const isAdmin = String(user.type || "").toUpperCase() === "ADMIN";
    if (shift.poster_email.toLowerCase() !== email.toLowerCase() && !isAdmin) return res.json({ ok: false, reason: "You can only delete your own shifts." });
    if (shift.status === "claimed" && !isAdmin) return res.json({ ok: false, reason: "Cannot delete a claimed shift." });
    if (await demoProtected("shift_posts", "id", shiftId, tid)) return res.json({ ok: false, reason: DEMO_BLOCK_MSG });
    await query(`DELETE FROM shift_posts WHERE id=$1`, [shiftId]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/getMyShiftPosts", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const rows = await query(
      `SELECT DISTINCT ON (location, shift_time) location, shift_time, notes, class_name
       FROM shift_posts WHERE tenant_id=$1 AND poster_email=LOWER($2)
       ORDER BY location, shift_time, created_at DESC`, [tid, email]
    );
    res.json({ ok: true, templates: rows.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/updateShiftPost", async (req, res) => {
  try {
    const { email, pin, shiftId, updates } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const shiftRes = await query(`SELECT * FROM shift_posts WHERE id=$1 AND tenant_id=$2`, [shiftId, tid]);
    if (shiftRes.rows.length === 0) return res.json({ ok: false, reason: "Shift not found." });
    const shift = shiftRes.rows[0];
    if (shift.status !== "open") return res.json({ ok: false, reason: "Cannot edit a claimed shift." });
    const isAdmin = String(user.type || "").toUpperCase() === "ADMIN";
    if (shift.poster_email.toLowerCase() !== email.toLowerCase() && !isAdmin) return res.json({ ok: false, reason: "You can only edit your own shifts." });
    const allowed = { location: "location", shift_time: "shift_time", shift_date: "shift_date", class_name: "class_name", notes: "notes", duration: "duration", front_desk: "front_desk" };
    const sets = [], vals = [];
    let idx = 1;
    for (const [k, col] of Object.entries(allowed)) {
      if (updates[k] !== undefined) { sets.push(`${col}=$${idx++}`); vals.push(updates[k]); }
    }
    if (!sets.length) return res.json({ ok: true });
    vals.push(shiftId);
    await query(`UPDATE shift_posts SET ${sets.join(",")} WHERE id=$${idx}`, vals);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── CLASS PROPOSALS (Staff Schedule Requests) ──

api.post("/getClassProposals", async (req, res) => {
  try {
    const { email, pin, month, year } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const startDate = `${year}-${String(month).padStart(2,"0")}-01`;
    const endDate = `${year}-${String(month).padStart(2,"0")}-${new Date(year, month, 0).getDate()}`;
    const rows = await query(
      `SELECT cp.*, pm.message AS latest_message, pm.suggested_fields AS latest_suggested, pm.sender_name AS latest_sender, pm.action AS latest_action, pm.created_at AS latest_message_at
       FROM class_proposals cp
       LEFT JOIN LATERAL (SELECT * FROM proposal_messages WHERE proposal_id = cp.id ORDER BY created_at DESC LIMIT 1) pm ON true
       WHERE cp.tenant_id=$1 AND cp.proposal_date >= $2 AND cp.proposal_date <= $3
         AND COALESCE(cp.status,'pending') IN ('pending','change_requested','change_denied','created')
       ORDER BY cp.proposal_date, cp.start_time`,
      [tid, startDate, endDate]
    );
    res.json({ ok: true, proposals: rows.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── getEntriesForMonth ─────────────────────────────
api.post("/getEntriesForMonth", async (req, res) => {
  try {
    const { email, pin, month, year } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const startDate = `${year}-${String(month).padStart(2,"0")}-01`;
    const endDate = `${year}-${String(month).padStart(2,"0")}-${new Date(year, month, 0).getDate()}`;
    const rows = await query(
      `SELECT e.date, e.time, e.class_party, e.location, u.name AS staff_name
       FROM entries e
       LEFT JOIN users u ON u.tenant_id = e.tenant_id AND LOWER(u.email) = LOWER(e.user_email)
       WHERE e.tenant_id=$1 AND e.date >= $2 AND e.date <= $3
       ORDER BY e.date, e.time`,
      [tid, startDate, endDate]
    );
    const entries = rows.rows.map(r => ({
      date: r.date instanceof Date ? r.date.toISOString().slice(0,10) : String(r.date||"").slice(0,10),
      time: r.time || "",
      className: r.class_party || "",
      location: r.location || "",
      staffName: r.name || r.staff_name || ""
    }));
    res.json({ ok: true, entries });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/postClassProposal", async (req, res) => {
  try {
    const { email, pin, proposals } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    if (!Array.isArray(proposals) || proposals.length === 0) return res.json({ ok: false, reason: "No proposals provided." });
    const posterRow = await query(`SELECT name FROM users WHERE tenant_id=$1 AND LOWER(email)=LOWER($2)`, [tid, email]);
    const proposerName = posterRow.rows[0]?.name || email;
    const created = [];
    // Generate a series_id only if multiple proposals AND isSeries flag is true
    const wantSeries = proposals.length > 1 && proposals[0]?.isSeries !== false;
    const seriesId = wantSeries ? require("crypto").randomUUID() : null;
    const seriesTotal = wantSeries ? proposals.length : null;
    let seriesIdx = 0;
    for (const p of proposals) {
      if (!p.class_name || !p.start_time || !p.proposal_date || !p.location || !p.room) continue;
      seriesIdx++;
      const r = await query(
        `INSERT INTO class_proposals (tenant_id, proposer_email, proposer_name, class_name, proposal_date, start_time, duration, location, room, color, notes, series_id, series_index, series_total)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING id`,
        [tid, email, proposerName, p.class_name, p.proposal_date, p.start_time, parseInt(p.duration) || 60, p.location, p.room, p.color || "", p.notes || "", seriesId, seriesId ? seriesIdx : null, seriesTotal]
      );
      created.push(r.rows[0].id);
    }

    // Send instant notification email if configured
    const settings = await getAdminSettings();
    const notifyEmail = settings.proposalNotifyEmail;
    if (notifyEmail && created.length > 0) {
      const first = proposals[0];
      const p = { class_name: first.class_name, proposal_date: first.proposal_date, start_time: first.start_time, duration: parseInt(first.duration) || 60, location: first.location, room: first.room, color: first.color, notes: first.notes, proposer_name: proposerName, proposer_email: email, series_index: seriesId ? 1 : null, series_total: seriesTotal };
      const seriesDates = seriesId ? proposals.map((pr, i) => ({ proposal_date: pr.proposal_date, series_index: i + 1, series_total: proposals.length })) : null;
      const seriesNote = seriesId ? ` (series of ${proposals.length})` : "";
      sendMail({ to: notifyEmail, subject: `${CONFIG.BRAND_NAME} — New proposal: "${first.class_name}"${seriesNote} by ${proposerName}`,
        html: proposalEmailHtml(p, {
          headerColor: "#1565c0",
          title: "New Class Proposal Submitted",
          intro: `<strong>${proposerName}</strong> has submitted a new class proposal${seriesNote}.`,
          seriesDates,
          cta: "Log in to the Proposals Dashboard to review."
        })
      }).catch(e => console.error("Proposal notify email failed:", e.message));
    }

    res.json({ ok: true, created });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/updateClassProposal", async (req, res) => {
  try {
    const { email, pin, proposalId, updates } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const pRes = await query(`SELECT * FROM class_proposals WHERE id=$1 AND tenant_id=$2`, [proposalId, tid]);
    if (pRes.rows.length === 0) return res.json({ ok: false, reason: "Proposal not found." });
    const proposal = pRes.rows[0];
    const isAdmin = String(user.type || "").toUpperCase() === "ADMIN";
    if (proposal.proposer_email.toLowerCase() !== email.toLowerCase() && !isAdmin) return res.json({ ok: false, reason: "You can only edit your own proposals." });
    const allowed = { class_name: "class_name", proposal_date: "proposal_date", start_time: "start_time", duration: "duration", location: "location", room: "room", color: "color", notes: "notes" };
    const sets = [], vals = [];
    let idx = 1;
    for (const [k, col] of Object.entries(allowed)) {
      if (updates[k] !== undefined) { sets.push(`${col}=$${idx++}`); vals.push(updates[k]); }
    }
    if (!sets.length) return res.json({ ok: true });
    vals.push(proposalId);
    await query(`UPDATE class_proposals SET ${sets.join(",")} WHERE id=$${idx}`, vals);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/deleteClassProposal", async (req, res) => {
  try {
    const { email, pin, proposalId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const pRes = await query(`SELECT * FROM class_proposals WHERE id=$1 AND tenant_id=$2`, [proposalId, tid]);
    if (pRes.rows.length === 0) return res.json({ ok: false, reason: "Proposal not found." });
    const proposal = pRes.rows[0];
    const isAdmin = String(user.type || "").toUpperCase() === "ADMIN";
    if (proposal.proposer_email.toLowerCase() !== email.toLowerCase() && !isAdmin) return res.json({ ok: false, reason: "You can only delete your own proposals." });
    if (await demoProtected("class_proposals", "id", proposalId, tid)) return res.json({ ok: false, reason: DEMO_BLOCK_MSG });
    await query(`DELETE FROM class_proposals WHERE id=$1`, [proposalId]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/deleteClassProposalSeries", async (req, res) => {
  try {
    const { email, pin, seriesId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    if (!seriesId) return res.json({ ok: false, reason: "No series ID provided." });
    // Verify ownership — check first proposal in series
    const first = await query(`SELECT proposer_email FROM class_proposals WHERE series_id=$1 AND tenant_id=$2 LIMIT 1`, [seriesId, tid]);
    if (first.rows.length === 0) return res.json({ ok: false, reason: "Series not found." });
    const isAdmin = String(user.type || "").toUpperCase() === "ADMIN";
    if (first.rows[0].proposer_email.toLowerCase() !== email.toLowerCase() && !isAdmin) return res.json({ ok: false, reason: "You can only delete your own proposals." });
    const result = await query(`DELETE FROM class_proposals WHERE series_id=$1 AND tenant_id=$2`, [seriesId, tid]);
    res.json({ ok: true, deleted: result.rowCount });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Admin: permanently delete a proposal (and its messages)
api.post("/deleteProposalForever", async (req, res) => {
  try {
    const { email, pin, proposalId, seriesId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!hasAdminPermission(user, "proposals", "w")) return res.json({ ok: false, reason: "Admin write access required." });
    const tid = await getDefaultTenantId();
    if (seriesId) {
      if (CONFIG.DEMO_MODE) { const dp = await query('SELECT is_demo FROM class_proposals WHERE series_id=$1 AND tenant_id=$2 AND is_demo=true LIMIT 1', [seriesId, tid]); if (dp.rows.length) return res.json({ ok: false, reason: DEMO_BLOCK_MSG }); }
      await query(`DELETE FROM proposal_messages WHERE tenant_id=$1 AND proposal_id IN (SELECT id FROM class_proposals WHERE series_id=$2 AND tenant_id=$1)`, [tid, seriesId]);
      const result = await query(`DELETE FROM class_proposals WHERE series_id=$1 AND tenant_id=$2`, [seriesId, tid]);
      res.json({ ok: true, deleted: result.rowCount });
    } else if (proposalId) {
      if (await demoProtected("class_proposals", "id", proposalId, tid)) return res.json({ ok: false, reason: DEMO_BLOCK_MSG });
      await query(`DELETE FROM proposal_messages WHERE proposal_id=$1 AND tenant_id=$2`, [proposalId, tid]);
      await query(`DELETE FROM class_proposals WHERE id=$1 AND tenant_id=$2`, [proposalId, tid]);
      res.json({ ok: true, deleted: 1 });
    } else {
      res.json({ ok: false, reason: "No proposalId or seriesId provided." });
    }
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Check if user has a specific admin permission (read or write)
function hasAdminPermission(user, feature, level) {
  if (String(user.type || "").toUpperCase() === "ADMIN") return true;
  try {
    const perms = JSON.parse(user.adminPermissions || user.admin_permissions || "{}");
    const val = perms[feature] || "";
    if (level === "r") return val === "r" || val === "rw";
    if (level === "w") return val === "w" || val === "rw";
    return false;
  } catch(e) { return false; }
}

// Check if user has proposals dashboard access (admin or has permission)
function hasProposalsDashboardAccess(user) {
  return hasAdminPermission(user, "proposals", "r");
}

// Admin dashboard: get all pending proposals grouped by staff, with counts
api.post("/getProposalsDashboard", async (req, res) => {
  try {
    const { email, pin, showArchived } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!hasProposalsDashboardAccess(user)) return res.json({ ok: false, reason: "Access denied." });
    const tid = await getDefaultTenantId();
    const activeStatuses = showArchived ? ['created','denied'] : ['pending','change_requested','change_denied'];
    const rows = await query(
      `SELECT cp.*, pm.message AS latest_message, pm.suggested_fields AS latest_suggested, pm.sender_name AS latest_sender, pm.action AS latest_action, pm.created_at AS latest_message_at
       FROM class_proposals cp
       LEFT JOIN LATERAL (SELECT * FROM proposal_messages WHERE proposal_id = cp.id ORDER BY created_at DESC LIMIT 1) pm ON true
       WHERE cp.tenant_id=$1 AND COALESCE(cp.status,'pending') = ANY($2)
       ORDER BY cp.proposer_name, cp.proposal_date, cp.start_time`,
      [tid, activeStatuses]
    );
    // Group by proposer and deduplicate series (count each series as 1)
    const staffMap = {};
    rows.rows.forEach(p => {
      const key = p.proposer_email.toLowerCase();
      if (!staffMap[key]) staffMap[key] = { email: key, name: p.proposer_name || p.proposer_email, proposals: [], seriesSeen: new Set() };
      staffMap[key].proposals.push(p);
    });
    // Build staff list with counts (series = 1 count)
    const staff = Object.values(staffMap).map(s => {
      let count = 0;
      const seen = new Set();
      s.proposals.forEach(p => {
        if (p.series_id) {
          if (!seen.has(p.series_id)) { seen.add(p.series_id); count++; }
        } else { count++; }
      });
      return { email: s.email, name: s.name, count, proposals: s.proposals };
    });
    staff.sort((a, b) => a.name.localeCompare(b.name));
    res.json({ ok: true, staff });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Proposal email helper ──
const PROPOSAL_APP_URL = "https://aradiafitness.app/";

function proposalGcalUrl(proposal) {
  // Parse start_time like "10:00 AM" or "2:30 PM" into 24h
  const t = (proposal.start_time || "").trim();
  const m = t.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
  if (!m || !proposal.proposal_date) return null;
  let h = parseInt(m[1]), min = parseInt(m[2]);
  const ampm = m[3].toUpperCase();
  if (ampm === "PM" && h !== 12) h += 12;
  if (ampm === "AM" && h === 12) h = 0;
  const dur = parseInt(proposal.duration) || 60;
  const parts = proposal.proposal_date.split("-");
  const startDt = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]), h, min);
  const endDt = new Date(startDt.getTime() + dur * 60000);
  const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,"0") + String(d.getDate()).padStart(2,"0") + "T" + String(d.getHours()).padStart(2,"0") + String(d.getMinutes()).padStart(2,"0") + "00";
  const title = encodeURIComponent(proposal.class_name || "Class");
  const location = encodeURIComponent((proposal.location || "") + (proposal.room ? " — " + proposal.room : ""));
  const details = encodeURIComponent(`Instructor: ${proposal.proposer_name || proposal.proposer_email || ""}${proposal.notes ? "\nNotes: " + proposal.notes : ""}`);
  return `https://calendar.google.com/calendar/render?action=TEMPLATE&text=${title}&dates=${fmt(startDt)}/${fmt(endDt)}&location=${location}&details=${details}`;
}

function proposalEmailHtml(p, opts = {}) {
  const dateObj = p.proposal_date ? new Date(p.proposal_date + "T12:00:00") : null;
  const dateStr = dateObj ? dateObj.toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric", year: "numeric" }) : "";
  const seriesTag = (p.series_index && p.series_total) ? ` (${p.series_index}/${p.series_total})` : "";
  const color = p.color || "#000";

  let html = `<div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;">`;
  html += `<div style="text-align:center;margin-bottom:16px;"><img src="${CONFIG.BRAND_LOGO_URL}" alt="${CONFIG.BRAND_NAME}" style="max-height:50px;"></div>`;
  html += `<h2 style="color:${opts.headerColor || CONFIG.BRAND_COLOR_PRIMARY};margin:0 0 16px;">${opts.title || ''}</h2>`;
  if (opts.intro) html += `<p style="font-size:15px;margin:0 0 16px;">${opts.intro}</p>`;

  // Class detail card
  html += `<div style="background:#f9f9f9;border-radius:10px;padding:16px 20px;margin:16px 0;border-left:5px solid ${color};">`;
  html += `<div style="font-weight:800;font-size:18px;margin-bottom:6px;">${p.class_name || ''}${seriesTag}</div>`;
  html += `<table style="font-size:14px;color:#444;border-collapse:collapse;">`;
  if (dateStr) html += `<tr><td style="padding:3px 12px 3px 0;font-weight:600;color:#666;">Date</td><td style="padding:3px 0;">${dateStr}</td></tr>`;
  if (p.start_time) html += `<tr><td style="padding:3px 12px 3px 0;font-weight:600;color:#666;">Time</td><td style="padding:3px 0;">${p.start_time}</td></tr>`;
  if (p.duration) html += `<tr><td style="padding:3px 12px 3px 0;font-weight:600;color:#666;">Duration</td><td style="padding:3px 0;">${p.duration} minutes</td></tr>`;
  if (p.location) html += `<tr><td style="padding:3px 12px 3px 0;font-weight:600;color:#666;">Location</td><td style="padding:3px 0;">${p.location}${p.room ? ' — ' + p.room : ''}</td></tr>`;
  if (p.proposer_name || p.proposer_email) html += `<tr><td style="padding:3px 12px 3px 0;font-weight:600;color:#666;">Instructor</td><td style="padding:3px 0;">${p.proposer_name || p.proposer_email}</td></tr>`;
  if (p.notes) html += `<tr><td style="padding:3px 12px 3px 0;font-weight:600;color:#666;">Notes</td><td style="padding:3px 0;">${p.notes}</td></tr>`;
  html += `</table></div>`;

  // Series dates list
  if (opts.seriesDates && opts.seriesDates.length > 1) {
    html += `<div style="background:#f0f0f0;border-radius:8px;padding:12px 16px;margin:12px 0;">`;
    html += `<div style="font-weight:700;font-size:13px;margin-bottom:6px;color:#555;">Series Dates (${opts.seriesDates.length} classes):</div>`;
    opts.seriesDates.forEach(d => {
      const dd = new Date(d.proposal_date + "T12:00:00");
      html += `<div style="font-size:13px;padding:2px 0;color:#555;">${d.series_index}/${d.series_total} — ${dd.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" })}</div>`;
    });
    html += `</div>`;
  }

  // Google Calendar buttons (only for approved proposals)
  if (opts.showGoogleCal) {
    const calItems = opts.seriesDates && opts.seriesDates.length > 1 ? opts.seriesDates : [p];
    html += `<div style="background:#e8f5e9;border-radius:8px;padding:12px 16px;margin:12px 0;">`;
    html += `<div style="font-weight:700;font-size:13px;margin-bottom:8px;color:#2e7d32;">📅 Add to Google Calendar</div>`;
    if (calItems.length === 1) {
      const gcUrl = proposalGcalUrl(calItems[0]);
      if (gcUrl) html += `<a href="${gcUrl}" target="_blank" style="display:inline-block;background:#4285f4;color:#fff;padding:8px 18px;border-radius:6px;text-decoration:none;font-weight:700;font-size:13px;">Add to Calendar</a>`;
    } else {
      calItems.forEach(item => {
        const gcUrl = proposalGcalUrl(item);
        if (!gcUrl) return;
        const dd = new Date(item.proposal_date + "T12:00:00");
        const label = dd.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });
        html += `<div style="margin-bottom:4px;"><a href="${gcUrl}" target="_blank" style="display:inline-block;background:#4285f4;color:#fff;padding:5px 14px;border-radius:5px;text-decoration:none;font-weight:600;font-size:12px;">${item.series_index}/${item.series_total} — ${label}</a></div>`;
      });
    }
    html += `</div>`;
  }

  // Suggested changes diff
  if (opts.suggestedFields && typeof opts.suggestedFields === "object") {
    html += `<div style="background:#fff3e0;border:1px solid #ffe0b2;border-radius:8px;padding:12px 16px;margin:12px 0;">`;
    html += `<div style="font-weight:700;font-size:13px;margin-bottom:6px;color:#e65100;">Suggested Changes:</div>`;
    const labels = { start_time: "Time", proposal_date: "Date", class_name: "Class Name", duration: "Duration", location: "Location", room: "Room" };
    for (const [k, v] of Object.entries(opts.suggestedFields)) {
      const origVal = p[k] || "";
      if (String(origVal) !== String(v)) {
        const label = labels[k] || k.charAt(0).toUpperCase() + k.slice(1);
        html += `<div style="font-size:13px;padding:2px 0;"><strong>${label}:</strong> <span style="text-decoration:line-through;color:#999;">${origVal}</span> → <span style="color:#e65100;font-weight:600;">${v}</span></div>`;
      }
    }
    html += `</div>`;
  }

  // Message
  if (opts.message) {
    html += `<div style="background:#f5f5f5;padding:12px 16px;border-radius:8px;margin:12px 0;border-left:3px solid #888;">`;
    html += `<div style="font-size:12px;color:#888;margin-bottom:4px;">${opts.messageLabel || 'Message'}:</div>`;
    html += `<div style="font-size:14px;font-style:italic;">"${opts.message}"</div>`;
    html += `</div>`;
  }

  if (opts.cta) html += `<p style="margin:16px 0;">${opts.cta}</p>`;
  html += `<p style="margin:20px 0 0;"><a href="${PROPOSAL_APP_URL}" style="display:inline-block;background:${CONFIG.BRAND_COLOR_PRIMARY};color:#fff;padding:10px 24px;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px;">Open ${CONFIG.BRAND_NAME}</a></p>`;
  html += `<p style="color:#aaa;font-size:11px;margin-top:20px;">This is an automated notification from ${CONFIG.BRAND_NAME}.</p>`;
  html += `</div>`;
  return html;
}

// Admin: mark proposal(s) as "class created" (archive)
api.post("/archiveProposal", async (req, res) => {
  try {
    const { email, pin, proposalId, seriesId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!hasAdminPermission(user, "proposals", "w")) return res.json({ ok: false, reason: "Access denied." });
    const tid = await getDefaultTenantId();
    let result, notifyProposals = [];
    if (seriesId) {
      const before = await query(`SELECT * FROM class_proposals WHERE series_id=$1 AND tenant_id=$2 ORDER BY series_index`, [seriesId, tid]);
      notifyProposals = before.rows;
      result = await query(`UPDATE class_proposals SET status='created', archived_at=NOW() WHERE series_id=$1 AND tenant_id=$2`, [seriesId, tid]);
    } else if (proposalId) {
      const before = await query(`SELECT * FROM class_proposals WHERE id=$1 AND tenant_id=$2`, [proposalId, tid]);
      notifyProposals = before.rows;
      result = await query(`UPDATE class_proposals SET status='created', archived_at=NOW() WHERE id=$1 AND tenant_id=$2`, [proposalId, tid]);
    } else {
      return res.json({ ok: false, reason: "No proposal or series ID provided." });
    }
    // Notify staff that their class was added to the schedule
    if (notifyProposals.length > 0) {
      const p = notifyProposals[0];
      const seriesDates = notifyProposals.length > 1 ? notifyProposals : null;
      sendPush(p.proposer_email, "Class added!", `Your "${p.class_name}" proposal has been added to the schedule!`, "/?tab=proposals").catch(()=>{});
      sendMail({ to: p.proposer_email, subject: `${CONFIG.BRAND_NAME} — "${p.class_name}" added to schedule!`,
        html: proposalEmailHtml(p, {
          headerColor: "#2e7d32",
          title: "Class Added to Schedule!",
          intro: `Great news! Your <strong>${p.class_name}</strong> proposal has been approved and added to the schedule.`,
          seriesDates,
          showGoogleCal: true,
          cta: "You can view your approved proposals in the Proposals tab."
        })
      }).catch(e => console.error("Proposal archive email failed:", e.message));
    }
    res.json({ ok: true, archived: result.rowCount });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Admin: restore archived proposal(s) back to pending
api.post("/unarchiveProposal", async (req, res) => {
  try {
    const { email, pin, proposalId, seriesId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!hasAdminPermission(user, "proposals", "w")) return res.json({ ok: false, reason: "Access denied." });
    const tid = await getDefaultTenantId();
    let result;
    if (seriesId) {
      result = await query(`UPDATE class_proposals SET status='pending', archived_at=NULL WHERE series_id=$1 AND tenant_id=$2`, [seriesId, tid]);
    } else if (proposalId) {
      result = await query(`UPDATE class_proposals SET status='pending', archived_at=NULL WHERE id=$1 AND tenant_id=$2`, [proposalId, tid]);
    } else {
      return res.json({ ok: false, reason: "No proposal or series ID provided." });
    }
    res.json({ ok: true, restored: result.rowCount });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── PROPOSAL DIALOGUE ENDPOINTS ──

// Admin: suggest changes to a proposal
api.post("/proposalSuggestChange", async (req, res) => {
  try {
    const { email, pin, proposalId, suggestedFields, message, seriesId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!hasAdminPermission(user, "proposals", "w")) return res.json({ ok: false, reason: "Access denied." });
    const tid = await getDefaultTenantId();
    // Get target proposals (single or entire series)
    let targets;
    if (seriesId) {
      targets = (await query(`SELECT * FROM class_proposals WHERE tenant_id=$1 AND series_id=$2`, [tid, seriesId])).rows;
    } else {
      targets = (await query(`SELECT * FROM class_proposals WHERE id=$1 AND tenant_id=$2`, [proposalId, tid])).rows;
    }
    if (!targets.length) return res.json({ ok: false, reason: "Proposal not found." });
    const proposal = targets[0];
    for (const t of targets) {
      await query(
        `INSERT INTO proposal_messages (proposal_id, tenant_id, sender_email, sender_name, sender_role, action, message, suggested_fields)
         VALUES ($1,$2,$3,$4,'admin','suggest_change',$5,$6)`,
        [t.id, tid, email, user.name || email, message || "", suggestedFields ? JSON.stringify(suggestedFields) : null]
      );
      await query(`UPDATE class_proposals SET status='change_requested', last_action_by=$1, last_action_at=NOW() WHERE id=$2`, [email, t.id]);
    }
    const staffEmail = proposal.proposer_email;
    const className = proposal.class_name;
    const seriesNote = seriesId ? ` (series of ${targets.length})` : "";
    const seriesDates = seriesId && targets.length > 1 ? targets : null;
    sendPush(staffEmail, "Change suggested", `Admin suggested changes to your "${className}"${seriesNote} proposal`, "/?tab=proposals").catch(()=>{});
    sendMail({ to: staffEmail, subject: `${CONFIG.BRAND_NAME} — Changes suggested for "${className}"${seriesNote}`,
      html: proposalEmailHtml(proposal, {
        headerColor: "#e65100",
        title: "Changes Suggested to Your Proposal",
        intro: `Admin has suggested changes to your <strong>${className}</strong>${seriesNote} proposal. Please review and accept or decline.`,
        suggestedFields: suggestedFields || null,
        message: message || null,
        messageLabel: "Admin's reason",
        seriesDates,
        cta: "Log in to review and respond to the suggested changes."
      })
    }).catch(e => console.error("Proposal suggest email failed:", e.message));
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Admin: deny a proposal outright
api.post("/proposalDeny", async (req, res) => {
  try {
    const { email, pin, proposalId, message, seriesId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!hasAdminPermission(user, "proposals", "w")) return res.json({ ok: false, reason: "Access denied." });
    const tid = await getDefaultTenantId();
    let targets;
    if (seriesId) {
      targets = (await query(`SELECT * FROM class_proposals WHERE tenant_id=$1 AND series_id=$2`, [tid, seriesId])).rows;
    } else {
      targets = (await query(`SELECT * FROM class_proposals WHERE id=$1 AND tenant_id=$2`, [proposalId, tid])).rows;
    }
    if (!targets.length) return res.json({ ok: false, reason: "Proposal not found." });
    const proposal = targets[0];
    for (const t of targets) {
      await query(
        `INSERT INTO proposal_messages (proposal_id, tenant_id, sender_email, sender_name, sender_role, action, message)
         VALUES ($1,$2,$3,$4,'admin','deny',$5)`,
        [t.id, tid, email, user.name || email, message || ""]
      );
      await query(`UPDATE class_proposals SET status='denied', denied_at=NOW(), last_action_by=$1, last_action_at=NOW() WHERE id=$2`, [email, t.id]);
    }
    const staffEmail = proposal.proposer_email;
    const className = proposal.class_name;
    const seriesNote = seriesId ? ` (series of ${targets.length})` : "";
    const seriesDates = seriesId && targets.length > 1 ? targets : null;
    sendPush(staffEmail, "Proposal denied", `Your "${className}"${seriesNote} proposal was denied`, "/?tab=proposals").catch(()=>{});
    sendMail({ to: staffEmail, subject: `${CONFIG.BRAND_NAME} — "${className}"${seriesNote} proposal denied`,
      html: proposalEmailHtml(proposal, {
        headerColor: "#c62828",
        title: "Proposal Denied",
        intro: `Your <strong>${className}</strong>${seriesNote} proposal has been denied.`,
        message: message || null,
        messageLabel: "Reason for denial",
        seriesDates,
      })
    }).catch(e => console.error("Proposal deny email failed:", e.message));
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Staff: respond to admin's suggested changes
api.post("/proposalRespondToChange", async (req, res) => {
  try {
    const { email, pin, proposalId, accept, message, seriesId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    let targets;
    if (seriesId) {
      targets = (await query(`SELECT * FROM class_proposals WHERE tenant_id=$1 AND series_id=$2 AND status='change_requested'`, [tid, seriesId])).rows;
    } else {
      targets = (await query(`SELECT * FROM class_proposals WHERE id=$1 AND tenant_id=$2`, [proposalId, tid])).rows;
    }
    if (!targets.length) return res.json({ ok: false, reason: "Proposal not found." });
    const proposal = targets[0];
    if (proposal.proposer_email.toLowerCase() !== email.toLowerCase()) return res.json({ ok: false, reason: "You can only respond to your own proposals." });
    for (const t of targets) {
      if (t.status !== "change_requested") continue;
      if (accept) {
        const msgRes = await query(
          `SELECT suggested_fields FROM proposal_messages WHERE proposal_id=$1 AND action='suggest_change' ORDER BY created_at DESC LIMIT 1`,
          [t.id]
        );
        const suggested = msgRes.rows[0]?.suggested_fields;
        if (suggested && typeof suggested === "object") {
          const allowed = ["class_name","start_time","duration","location","room","color","notes"];
          const sets = [], vals = [];
          let idx = 1;
          for (const [k, v] of Object.entries(suggested)) {
            if (allowed.includes(k) && v !== undefined) { sets.push(`${k}=$${idx++}`); vals.push(v); }
          }
          if (sets.length) { vals.push(t.id); await query(`UPDATE class_proposals SET ${sets.join(",")} WHERE id=$${idx}`, vals); }
        }
        await query(`INSERT INTO proposal_messages (proposal_id, tenant_id, sender_email, sender_name, sender_role, action, message) VALUES ($1,$2,$3,$4,'staff','approve_change',$5)`, [t.id, tid, email, user.name || email, message || ""]);
        await query(`UPDATE class_proposals SET status='pending', last_action_by=$1, last_action_at=NOW() WHERE id=$2`, [email, t.id]);
      } else {
        await query(`INSERT INTO proposal_messages (proposal_id, tenant_id, sender_email, sender_name, sender_role, action, message) VALUES ($1,$2,$3,$4,'staff','deny_change',$5)`, [t.id, tid, email, user.name || email, message || ""]);
        await query(`UPDATE class_proposals SET status='change_denied', last_action_by=$1, last_action_at=NOW() WHERE id=$2`, [email, t.id]);
      }
    }
    const admins = await query(`SELECT email FROM users WHERE tenant_id=$1 AND UPPER(type)='ADMIN'`, [tid]);
    const action = accept ? "accepted" : "declined";
    const staffName = user.name || email;
    const className = proposal.class_name;
    const seriesDates = seriesId && targets.length > 1 ? targets : null;
    for (const a of admins.rows) {
      sendPush(a.email, `Changes ${action}`, `${staffName} ${action} changes to "${className}"`, "/?tab=admin").catch(()=>{});
      sendMail({ to: a.email, subject: `${CONFIG.BRAND_NAME} — ${staffName} ${action} changes to "${className}"`,
        html: proposalEmailHtml(proposal, {
          headerColor: accept ? "#2e7d32" : "#c62828",
          title: `Changes ${accept ? 'Accepted' : 'Declined'}`,
          intro: `<strong>${staffName}</strong> has ${action} your suggested changes to <strong>${className}</strong>.${accept ? ' The proposal is now back to pending status.' : ''}`,
          message: !accept ? (message || null) : null,
          messageLabel: "Reason for declining",
          seriesDates,
          cta: accept ? "The proposal has been updated with the accepted changes and is ready for approval." : "Log in to the Proposals Dashboard to review and take next steps.",
        })
      }).catch(e => console.error("Proposal response email failed:", e.message));
    }
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Get conversation thread for a proposal
api.post("/getProposalMessages", async (req, res) => {
  try {
    const { email, pin, proposalId } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    // Verify access: must be proposal owner or have proposals read permission
    const pRes = await query(`SELECT proposer_email FROM class_proposals WHERE id=$1 AND tenant_id=$2`, [proposalId, tid]);
    if (!pRes.rows.length) return res.json({ ok: false, reason: "Proposal not found." });
    const isOwner = pRes.rows[0].proposer_email.toLowerCase() === email.toLowerCase();
    if (!isOwner && !hasAdminPermission(user, "proposals", "r")) return res.json({ ok: false, reason: "Access denied." });
    const msgs = await query(
      `SELECT * FROM proposal_messages WHERE proposal_id=$1 AND tenant_id=$2 ORDER BY created_at ASC`,
      [proposalId, tid]
    );
    res.json({ ok: true, messages: msgs.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── My Proposals (for any logged-in user) ──
api.post("/getMyProposals", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const rows = await query(
      `SELECT cp.*, pm.message AS latest_message, pm.suggested_fields AS latest_suggested,
              pm.sender_name AS latest_sender, pm.action AS latest_action, pm.created_at AS latest_message_at
       FROM class_proposals cp
       LEFT JOIN LATERAL (SELECT * FROM proposal_messages WHERE proposal_id = cp.id ORDER BY created_at DESC LIMIT 1) pm ON true
       WHERE cp.tenant_id=$1 AND LOWER(cp.proposer_email)=LOWER($2)
       ORDER BY
         CASE COALESCE(cp.status,'pending')
           WHEN 'change_requested' THEN 0
           WHEN 'change_denied' THEN 1
           WHEN 'pending' THEN 2
           WHEN 'created' THEN 3
           WHEN 'denied' THEN 4
         END,
         cp.proposal_date, cp.start_time`,
      [tid, email]
    );
    res.json({ ok: true, proposals: rows.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Also notify staff when admin clicks "Class Added" (archiveProposal already exists above)
// We'll add notification in the archiveProposal endpoint by updating it

// Admin-only: trigger proposal digest email on demand
api.post("/sendProposalDigest", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (String(user.type || "").toUpperCase() !== "ADMIN") return res.json({ ok: false, reason: "Admin only." });
    const s = await getAdminSettings();
    const targetEmail = s.proposalDigestEmail || email;
    const tid = await getDefaultTenantId();
    const now = new Date();
    const startStr = now.toISOString().slice(0, 10);
    const end = new Date(now); end.setDate(now.getDate() + 30);
    const endStr = end.toISOString().slice(0, 10);
    const rows = await query(`SELECT * FROM class_proposals WHERE tenant_id=$1 AND proposal_date >= $2 AND proposal_date <= $3 AND COALESCE(status,'pending')='pending' ORDER BY proposal_date, start_time`, [tid, startStr, endStr]);
    if (rows.rows.length === 0) return res.json({ ok: true, reason: "No proposals to send." });
    const grouped = {};
    rows.rows.forEach(p => { if (!grouped[p.proposal_date]) grouped[p.proposal_date] = []; grouped[p.proposal_date].push(p); });
    let html = `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
      <h2 style="color:${CONFIG.BRAND_COLOR_PRIMARY};">${CONFIG.BRAND_NAME} — Class Proposals Weekly Digest</h2>
      <p>Here are the class proposals for the next 30 days:</p>`;
    for (const [date, proposals] of Object.entries(grouped).sort((a,b) => a[0].localeCompare(b[0]))) {
      const dObj = new Date(date + "T12:00:00");
      const dayLabel = dObj.toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric", year: "numeric" });
      html += `<h3 style="margin:16px 0 8px;color:#333;border-bottom:1px solid #ddd;padding-bottom:4px;">${dayLabel}</h3>`;
      proposals.forEach(p => {
        html += `<div style="background:#f5f5f5;border-radius:8px;padding:10px 14px;margin:6px 0;border-left:4px solid ${p.color || '#000'};">
          <strong>${p.class_name}</strong>${p.series_index && p.series_total ? ` <span style="color:#999;font-weight:normal;">(${p.series_index}/${p.series_total})</span>` : ''} — ${p.start_time} (${p.duration}min)<br/>
          <span style="color:#666;">${p.location} / ${p.room} — ${p.proposer_name || p.proposer_email}</span>
          ${p.notes ? `<br/><em style="color:#888;">${p.notes}</em>` : ""}
        </div>`;
      });
    }
    html += `<p style="color:#888;font-size:12px;margin-top:16px;">This is an automated weekly digest.</p></div>`;
    await sendMail({ to: targetEmail, subject: `${CONFIG.BRAND_NAME} — Class Proposals Weekly Digest`, html });
    res.json({ ok: true, sentTo: targetEmail, count: rows.rows.length });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ─────────────────────────────────────────
//  CHAT ENDPOINTS
// ─────────────────────────────────────────

// Get all conversations the user belongs to (+ global)
api.post("/getChatConversations", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const email = user.email.toLowerCase();
    const showArchived = !!req.body.showArchived;

    // Get all conversations: global ones + ones where user is a member
    // Filter by archived status via chat_read_cursors
    const convos = await query(`
      SELECT c.*,
        (SELECT COUNT(*) FROM chat_messages m WHERE m.conversation_id = c.id
          AND m.created_at > COALESCE(
            (SELECT last_read_at FROM chat_read_cursors WHERE conversation_id = c.id AND LOWER(user_email) = LOWER($2)),
            '1970-01-01'
          )
        ) as unread_count,
        (SELECT body FROM chat_messages m2 WHERE m2.conversation_id = c.id ORDER BY m2.created_at DESC LIMIT 1) as last_message,
        (SELECT sender_name FROM chat_messages m3 WHERE m3.conversation_id = c.id ORDER BY m3.created_at DESC LIMIT 1) as last_sender,
        (SELECT created_at FROM chat_messages m4 WHERE m4.conversation_id = c.id ORDER BY m4.created_at DESC LIMIT 1) as last_message_at,
        COALESCE((SELECT archived FROM chat_read_cursors WHERE conversation_id = c.id AND LOWER(user_email) = LOWER($2)), FALSE) as is_archived
      FROM chat_conversations c
      WHERE c.tenant_id = $1 AND (
        c.type = 'global'
        OR c.id IN (SELECT conversation_id FROM chat_members WHERE LOWER(user_email) = LOWER($2))
      )
      ORDER BY last_message_at DESC NULLS LAST
    `, [tid, email]);

    // Filter by archived status client-side (simpler than SQL for the COALESCE logic)
    const filtered = convos.rows.filter(c => showArchived ? !!c.is_archived : !c.is_archived);

    // For each conversation, fetch members
    for (const c of filtered) {
      const members = await query(
        `SELECT cm.user_email, cm.role, u.name, u.profile_pic FROM chat_members cm
         LEFT JOIN users u ON LOWER(u.email) = LOWER(cm.user_email) AND u.tenant_id = $1
         WHERE cm.conversation_id = $2`,
        [tid, c.id]
      );
      c.members = members.rows;
    }

    res.json({ ok: true, conversations: filtered });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Get messages for a conversation (paginated)
api.post("/getChatMessages", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { conversationId, before } = req.body;

    // Verify membership (global is open to all)
    const convo = await query(`SELECT * FROM chat_conversations WHERE id=$1 AND tenant_id=$2`, [conversationId, tid]);
    if (!convo.rows.length) return res.json({ ok: false, reason: "Conversation not found" });

    if (convo.rows[0].type !== 'global') {
      const membership = await query(
        `SELECT 1 FROM chat_members WHERE conversation_id=$1 AND LOWER(user_email)=LOWER($2)`,
        [conversationId, user.email]
      );
      if (!membership.rows.length) return res.json({ ok: false, reason: "Not a member" });
    }

    let msgs;
    if (before) {
      msgs = await query(
        `SELECT m.*, u.profile_pic as sender_profile_pic FROM chat_messages m LEFT JOIN users u ON LOWER(u.email)=LOWER(m.sender_email) AND u.tenant_id=m.tenant_id WHERE m.conversation_id=$1 AND m.tenant_id=$2 AND m.created_at < $3 ORDER BY m.created_at DESC LIMIT 50`,
        [conversationId, tid, before]
      );
    } else {
      msgs = await query(
        `SELECT m.*, u.profile_pic as sender_profile_pic FROM chat_messages m LEFT JOIN users u ON LOWER(u.email)=LOWER(m.sender_email) AND u.tenant_id=m.tenant_id WHERE m.conversation_id=$1 AND m.tenant_id=$2 ORDER BY m.created_at DESC LIMIT 50`,
        [conversationId, tid]
      );
    }

    // Auto-update read cursor
    await query(
      `INSERT INTO chat_read_cursors (conversation_id, tenant_id, user_email, last_read_at) VALUES ($1, $2, $3, NOW())
       ON CONFLICT (conversation_id, user_email) DO UPDATE SET last_read_at = NOW()`,
      [conversationId, tid, user.email.toLowerCase()]
    );

    res.json({ ok: true, messages: msgs.rows.reverse() });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Send a message
api.post("/sendChatMessage", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { conversationId, body } = req.body;
    if (!body || !body.trim()) return res.json({ ok: false, reason: "Empty message" });

    // Verify conversation exists and user has access
    const convo = await query(`SELECT * FROM chat_conversations WHERE id=$1 AND tenant_id=$2`, [conversationId, tid]);
    if (!convo.rows.length) return res.json({ ok: false, reason: "Conversation not found" });

    if (convo.rows[0].type !== 'global') {
      const membership = await query(
        `SELECT 1 FROM chat_members WHERE conversation_id=$1 AND LOWER(user_email)=LOWER($2)`,
        [conversationId, user.email]
      );
      if (!membership.rows.length) return res.json({ ok: false, reason: "Not a member" });
    }

    const senderName = user.name || user.username || user.email;
    const msg = await query(
      `INSERT INTO chat_messages (conversation_id, tenant_id, sender_email, sender_name, body) VALUES ($1, $2, $3, $4, $5) RETURNING *`,
      [conversationId, tid, user.email.toLowerCase(), senderName, body.trim()]
    );

    // Update sender's read cursor
    await query(
      `INSERT INTO chat_read_cursors (conversation_id, tenant_id, user_email, last_read_at) VALUES ($1, $2, $3, NOW())
       ON CONFLICT (conversation_id, user_email) DO UPDATE SET last_read_at = NOW()`,
      [conversationId, tid, user.email.toLowerCase()]
    );

    // Send push notifications to other members
    const preview = body.trim().length > 60 ? body.trim().slice(0, 57) + "..." : body.trim();
    const chatUrl = "/?tab=chat&convo=" + conversationId;
    if (convo.rows[0].type === 'global') {
      sendPushToAll(`💬 ${senderName}`, preview, chatUrl, user.email, "chat").catch(() => {});
    } else {
      const members = await query(`SELECT user_email FROM chat_members WHERE conversation_id=$1`, [conversationId]);
      for (const m of members.rows) {
        if (m.user_email.toLowerCase() !== user.email.toLowerCase()) {
          sendPush(m.user_email, `💬 ${senderName}`, preview, chatUrl, "chat").catch(() => {});
        }
      }
    }

    res.json({ ok: true, message: msg.rows[0] });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Edit a chat message (only by sender)
api.post("/editChatMessage", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { messageId, body } = req.body;
    if (!body || !body.trim()) return res.json({ ok: false, reason: "Empty message" });
    const msg = await query(
      `UPDATE chat_messages SET body=$1, edited_at=NOW() WHERE id=$2 AND tenant_id=$3 AND LOWER(sender_email)=LOWER($4) RETURNING *`,
      [body.trim(), messageId, tid, user.email]
    );
    if (!msg.rows.length) return res.json({ ok: false, reason: "Message not found or not yours" });
    res.json({ ok: true, message: msg.rows[0] });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Delete a chat message (only by sender)
api.post("/deleteChatMessage", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { messageId } = req.body;
    const result = await query(
      `DELETE FROM chat_messages WHERE id=$1 AND tenant_id=$2 AND LOWER(sender_email)=LOWER($3)`,
      [messageId, tid, user.email]
    );
    if (!result.rowCount) return res.json({ ok: false, reason: "Message not found or not yours" });
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Search chat messages
api.post("/searchChatMessages", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { q } = req.body;
    if (!q || q.trim().length < 2) return res.json({ ok: true, results: [] });

    const results = await query(
      `SELECT m.id, m.conversation_id, m.sender_name, m.body, m.created_at
       FROM chat_messages m
       JOIN chat_conversations c ON c.id = m.conversation_id
       WHERE m.tenant_id = $1
         AND LOWER(m.body) LIKE '%' || LOWER($2) || '%'
         AND (c.type = 'global' OR m.conversation_id IN (SELECT conversation_id FROM chat_members WHERE LOWER(user_email) = LOWER($3)))
       ORDER BY m.created_at DESC
       LIMIT 20`,
      [tid, q.trim(), user.email]
    );
    res.json({ ok: true, results: results.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Mark conversation as read
api.post("/markChatRead", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    await query(
      `INSERT INTO chat_read_cursors (conversation_id, tenant_id, user_email, last_read_at) VALUES ($1, $2, $3, NOW())
       ON CONFLICT (conversation_id, user_email) DO UPDATE SET last_read_at = NOW()`,
      [req.body.conversationId, tid, user.email.toLowerCase()]
    );
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Create a group chat
api.post("/createGroupChat", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { name, memberEmails } = req.body;
    if (!name || !name.trim()) return res.json({ ok: false, reason: "Group name required" });

    const convo = await query(
      `INSERT INTO chat_conversations (tenant_id, type, name, created_by) VALUES ($1, 'group', $2, $3) RETURNING *`,
      [tid, name.trim(), user.email.toLowerCase()]
    );
    const convoId = convo.rows[0].id;

    // Add creator as admin
    await query(
      `INSERT INTO chat_members (conversation_id, tenant_id, user_email, role) VALUES ($1, $2, $3, 'admin')`,
      [convoId, tid, user.email.toLowerCase()]
    );

    // Add other members
    if (Array.isArray(memberEmails)) {
      for (const em of memberEmails) {
        if (em.toLowerCase() !== user.email.toLowerCase()) {
          await query(
            `INSERT INTO chat_members (conversation_id, tenant_id, user_email, role) VALUES ($1, $2, $3, 'member') ON CONFLICT DO NOTHING`,
            [convoId, tid, em.toLowerCase()]
          ).catch(() => {});
        }
      }
    }

    res.json({ ok: true, conversation: convo.rows[0] });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Update a group chat (rename, add/remove members)
api.post("/updateGroupChat", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { conversationId, updates } = req.body;

    // Check the user is the group admin or app admin
    const convo = await query(`SELECT * FROM chat_conversations WHERE id=$1 AND tenant_id=$2 AND type='group'`, [conversationId, tid]);
    if (!convo.rows.length) return res.json({ ok: false, reason: "Group not found" });

    const isGroupAdmin = convo.rows[0].created_by.toLowerCase() === user.email.toLowerCase();
    const isAppAdmin = (user.type || "").toLowerCase() === "admin";
    if (!isGroupAdmin && !isAppAdmin) return res.json({ ok: false, reason: "Only group creator or admin can edit" });

    if (updates.name) {
      await query(`UPDATE chat_conversations SET name=$1 WHERE id=$2`, [updates.name.trim(), conversationId]);
    }

    if (Array.isArray(updates.addMembers)) {
      for (const em of updates.addMembers) {
        await query(
          `INSERT INTO chat_members (conversation_id, tenant_id, user_email, role) VALUES ($1, $2, $3, 'member') ON CONFLICT DO NOTHING`,
          [conversationId, tid, em.toLowerCase()]
        ).catch(() => {});
      }
    }

    if (Array.isArray(updates.removeMembers)) {
      for (const em of updates.removeMembers) {
        // Don't allow removing the creator
        if (em.toLowerCase() === convo.rows[0].created_by.toLowerCase()) continue;
        await query(
          `DELETE FROM chat_members WHERE conversation_id=$1 AND LOWER(user_email)=LOWER($2)`,
          [conversationId, em]
        );
      }
    }

    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Delete a group chat
api.post("/deleteGroupChat", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { conversationId } = req.body;

    const convo = await query(`SELECT * FROM chat_conversations WHERE id=$1 AND tenant_id=$2 AND type='group'`, [conversationId, tid]);
    if (!convo.rows.length) return res.json({ ok: false, reason: "Group not found" });

    const isGroupAdmin = convo.rows[0].created_by.toLowerCase() === user.email.toLowerCase();
    const isAppAdmin = (user.type || "").toLowerCase() === "admin";
    if (!isGroupAdmin && !isAppAdmin) return res.json({ ok: false, reason: "Only group creator or admin can delete" });

    await query(`DELETE FROM chat_conversations WHERE id=$1`, [conversationId]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Start or find a DM conversation
api.post("/startDirectMessage", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { targetEmail } = req.body;
    if (!targetEmail) return res.json({ ok: false, reason: "Target email required" });

    const myEmail = user.email.toLowerCase();
    const theirEmail = targetEmail.toLowerCase();
    if (myEmail === theirEmail) return res.json({ ok: false, reason: "Cannot DM yourself" });

    // Check if DM already exists between these two users
    const existing = await query(`
      SELECT c.id FROM chat_conversations c
      WHERE c.tenant_id = $1 AND c.type = 'dm'
        AND c.id IN (SELECT conversation_id FROM chat_members WHERE LOWER(user_email) = $2)
        AND c.id IN (SELECT conversation_id FROM chat_members WHERE LOWER(user_email) = $3)
    `, [tid, myEmail, theirEmail]);

    if (existing.rows.length) {
      return res.json({ ok: true, conversationId: existing.rows[0].id, existing: true });
    }

    // Create new DM
    const convo = await query(
      `INSERT INTO chat_conversations (tenant_id, type, name, created_by) VALUES ($1, 'dm', '', $2) RETURNING *`,
      [tid, myEmail]
    );
    const convoId = convo.rows[0].id;
    await query(`INSERT INTO chat_members (conversation_id, tenant_id, user_email, role) VALUES ($1, $2, $3, 'member')`, [convoId, tid, myEmail]);
    await query(`INSERT INTO chat_members (conversation_id, tenant_id, user_email, role) VALUES ($1, $2, $3, 'member')`, [convoId, tid, theirEmail]);

    res.json({ ok: true, conversationId: convoId, existing: false });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Get total unread count across all conversations
api.post("/getChatUnreadTotal", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const email = user.email.toLowerCase();

    const result = await query(`
      SELECT COALESCE(SUM(unread), 0) as total FROM (
        SELECT COUNT(*) as unread FROM chat_messages m
        JOIN chat_conversations c ON c.id = m.conversation_id
        WHERE c.tenant_id = $1 AND (
          c.type = 'global'
          OR c.id IN (SELECT conversation_id FROM chat_members WHERE LOWER(user_email) = LOWER($2))
        )
        AND m.created_at > COALESCE(
          (SELECT last_read_at FROM chat_read_cursors WHERE conversation_id = m.conversation_id AND LOWER(user_email) = LOWER($2)),
          '1970-01-01'
        )
        AND LOWER(m.sender_email) != LOWER($2)
      ) sub
    `, [tid, email]);

    res.json({ ok: true, total: parseInt(result.rows[0].total) || 0 });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Get active staff list for chat (lightweight, any logged-in user)
api.post("/getChatStaffList", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();

    // Get excluded emails from admin settings
    const excludedSetting = await query(`SELECT value FROM settings WHERE tenant_id=$1 AND key='chatExcludedEmails'`, [tid]);
    const excludedRaw = excludedSetting.rows.length ? excludedSetting.rows[0].value : "";
    const excludedEmails = new Set(excludedRaw.split(/[\n,]+/).map(e => e.trim().toLowerCase()).filter(Boolean));

    const result = await query(
      `SELECT email, name, profile_pic, teaches, type FROM users WHERE tenant_id=$1 AND is_active=TRUE AND UPPER(type) != 'ADMIN' ORDER BY name`,
      [tid]
    );
    const staff = result.rows
      .filter(r => r.email.toLowerCase() !== user.email.toLowerCase() && !excludedEmails.has(r.email.toLowerCase()))
      .map(r => ({ email: r.email, name: r.name || "", profilePic: r.profile_pic || "", teaches: r.teaches || "" }));
    res.json({ ok: true, staff });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Leave a group chat
api.post("/leaveGroupChat", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { conversationId } = req.body;

    const convo = await query(`SELECT * FROM chat_conversations WHERE id=$1 AND tenant_id=$2 AND type='group'`, [conversationId, tid]);
    if (!convo.rows.length) return res.json({ ok: false, reason: "Group not found" });

    // Creator can't leave — they must delete the group
    if (convo.rows[0].created_by.toLowerCase() === user.email.toLowerCase()) {
      return res.json({ ok: false, reason: "Group creator cannot leave. Delete the group instead." });
    }

    await query(`DELETE FROM chat_members WHERE conversation_id=$1 AND LOWER(user_email)=LOWER($2)`, [conversationId, user.email]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Archive a chat conversation (per-user)
api.post("/archiveChatConvo", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { conversationId } = req.body;
    await query(
      `INSERT INTO chat_read_cursors (conversation_id, user_email, last_read_at, archived)
       VALUES ($1, LOWER($2), NOW(), TRUE)
       ON CONFLICT (conversation_id, user_email) DO UPDATE SET archived = TRUE`,
      [conversationId, user.email]
    );
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Unarchive a chat conversation (per-user)
api.post("/unarchiveChatConvo", async (req, res) => {
  try {
    const user = await getAuthorizedUser(req.body.email, req.body.pin);
    if (!user) return res.json({ ok: false, reason: "Unauthorized" });
    const tid = await getDefaultTenantId();
    const { conversationId } = req.body;
    await query(
      `UPDATE chat_read_cursors SET archived = FALSE WHERE conversation_id = $1 AND LOWER(user_email) = LOWER($2)`,
      [conversationId, user.email]
    );
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ─────────────────────────────────────────
//  PUSH NOTIFICATION ENDPOINTS
// ─────────────────────────────────────────
api.post("/getVapidKey", (req, res) => {
  res.json({ ok: true, key: process.env.VAPID_PUBLIC_KEY || "" });
});

api.post("/pushSubscribe", async (req, res) => {
  try {
    const { email, pin, subscription } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    const { endpoint, keys } = subscription;
    if (!endpoint || !keys || !keys.p256dh || !keys.auth)
      return res.json({ ok: false, reason: "Invalid subscription." });
    await query(
      `INSERT INTO push_subscriptions (tenant_id, user_email, endpoint, p256dh, auth)
       VALUES ($1, LOWER($2), $3, $4, $5)
       ON CONFLICT (endpoint) DO UPDATE SET user_email=LOWER($2), p256dh=$4, auth=$5`,
      [tid, email, endpoint, keys.p256dh, keys.auth]
    );
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/sendTestPush", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    if (!process.env.VAPID_PUBLIC_KEY || !process.env.VAPID_PRIVATE_KEY)
      return res.json({ ok: false, reason: "VAPID keys not configured on server." });
    const tid = await getDefaultTenantId();
    const subs = await query(
      `SELECT * FROM push_subscriptions WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2)`,
      [tid, email]
    );
    if (subs.rows.length === 0)
      return res.json({ ok: false, reason: "No push subscription found. Enable push in your Profile first." });
    let sent = 0, failed = 0;
    var errors = [];
    for (const sub of subs.rows) {
      try {
        await webpush.sendNotification(
          { endpoint: sub.endpoint, keys: { p256dh: sub.p256dh, auth: sub.auth } },
          JSON.stringify({ title: "🔔 Test Notification", body: "Push notifications are working!", url: "/" })
        );
        sent++;
      } catch (err) {
        failed++;
        var errDetail = `Status ${err.statusCode || "?"}: ${err.body || err.message || "unknown"}`;
        errors.push(errDetail);
        if (err.statusCode === 404 || err.statusCode === 410) {
          await query(`DELETE FROM push_subscriptions WHERE id=$1`, [sub.id]);
        }
        console.error(`[PUSH TEST] Failed:`, errDetail);
      }
    }
    if (sent > 0) {
      res.json({ ok: true, message: `Test push sent to ${sent} device(s).${failed ? ` ${failed} stale sub(s) removed.` : ""}` });
    } else {
      res.json({ ok: false, reason: `Push failed (${subs.rows.length} sub(s)). Error: ${errors[0] || "unknown"}` });
    }
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

api.post("/pushUnsubscribe", async (req, res) => {
  try {
    const { email, pin, endpoint } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const tid = await getDefaultTenantId();
    await query(
      `DELETE FROM push_subscriptions WHERE tenant_id=$1 AND LOWER(user_email)=LOWER($2) AND endpoint=$3`,
      [tid, email, endpoint]
    );
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ══════════════════════════════════════════════════════
//  QUICKBOOKS ONLINE INTEGRATION
// ══════════════════════════════════════════════════════

function getQboOAuthClient() {
  return new OAuthClient({
    clientId:     process.env.QBO_CLIENT_ID,
    clientSecret: process.env.QBO_CLIENT_SECRET,
    environment:  process.env.QBO_ENVIRONMENT || "sandbox", // "sandbox" or "production"
    redirectUri:  process.env.QBO_REDIRECT_URI || `${process.env.APP_URL || "https://app.kronara.app"}/api/qbo/callback`,
  });
}

// Start OAuth flow — admin clicks "Connect QuickBooks"
app.get("/api/qbo/connect", async (req, res) => {
  try {
    const oauthClient = getQboOAuthClient();
    const authUri = oauthClient.authorizeUri({
      scope: [OAuthClient.scopes.Accounting],
      state: "kronara-qbo",
    });
    res.redirect(authUri);
  } catch (e) {
    console.error("QBO connect error:", e);
    res.status(500).send("Failed to start QuickBooks connection.");
  }
});

// OAuth callback — Intuit redirects here after user authorizes
app.get("/api/qbo/callback", async (req, res) => {
  try {
    const oauthClient = getQboOAuthClient();
    const authResponse = await oauthClient.createToken(req.url);
    const token = authResponse.getJson();
    const tid = await getDefaultTenantId();

    await query(`
      INSERT INTO qbo_tokens (tenant_id, realm_id, access_token, refresh_token, expires_at, updated_at)
      VALUES ($1, $2, $3, $4, NOW() + INTERVAL '1 hour', NOW())
      ON CONFLICT (tenant_id) DO UPDATE SET
        realm_id = EXCLUDED.realm_id,
        access_token = EXCLUDED.access_token,
        refresh_token = EXCLUDED.refresh_token,
        expires_at = NOW() + INTERVAL '1 hour',
        updated_at = NOW()
    `, [tid, token.realmId || req.query.realmId, token.access_token, token.refresh_token]);

    // Redirect back to admin settings with success
    res.redirect("/#settings?qbo=connected");
  } catch (e) {
    console.error("QBO callback error:", e);
    res.redirect("/#settings?qbo=error");
  }
});

// Check QBO connection status
api.post("/qboStatus", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(user.type || "").toUpperCase();
    if (uType !== "ADMIN") return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    const r = await query(`SELECT realm_id, expires_at, updated_at FROM qbo_tokens WHERE tenant_id=$1`, [tid]);
    if (r.rows.length === 0) return res.json({ ok: true, connected: false });

    res.json({ ok: true, connected: true, realmId: r.rows[0].realm_id, lastUpdated: r.rows[0].updated_at });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Disconnect QuickBooks
api.post("/qboDisconnect", async (req, res) => {
  try {
    const { email, pin } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(user.type || "").toUpperCase();
    if (uType !== "ADMIN") return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    await query(`DELETE FROM qbo_tokens WHERE tenant_id=$1`, [tid]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// Helper: get a refreshed QBO client
async function getQboClient() {
  const tid = await getDefaultTenantId();
  const r = await query(`SELECT * FROM qbo_tokens WHERE tenant_id=$1`, [tid]);
  if (r.rows.length === 0) throw new Error("QuickBooks is not connected.");

  const row = r.rows[0];
  const oauthClient = getQboOAuthClient();

  // Check if token needs refresh (expired or expiring in next 5 min)
  const expiresAt = new Date(row.expires_at);
  if (expiresAt < new Date(Date.now() + 5 * 60 * 1000)) {
    oauthClient.setToken({
      access_token: row.access_token,
      refresh_token: row.refresh_token,
      token_type: "bearer",
      expires_in: 0,
    });
    const refreshResponse = await oauthClient.refresh();
    const newToken = refreshResponse.getJson();

    await query(`
      UPDATE qbo_tokens SET access_token=$1, refresh_token=$2, expires_at=NOW() + INTERVAL '1 hour', updated_at=NOW()
      WHERE tenant_id=$3
    `, [newToken.access_token, newToken.refresh_token, tid]);

    row.access_token = newToken.access_token;
    row.refresh_token = newToken.refresh_token;
  }

  const useSandbox = (process.env.QBO_ENVIRONMENT || "sandbox") === "sandbox";
  return new QuickBooks(
    process.env.QBO_CLIENT_ID,
    process.env.QBO_CLIENT_SECRET,
    row.access_token,
    false, // no token secret (OAuth2)
    row.realm_id,
    useSandbox,
    true,  // debug
    null,  // minor version
    "2.0", // OAuth version
    row.refresh_token
  );
}

// Sync timesheet entries to QuickBooks as TimeActivities
api.post("/qboSync", async (req, res) => {
  try {
    const { email, pin, ppStart, ppEnd } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(user.type || "").toUpperCase();
    if (uType !== "ADMIN") return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();
    const qb = await getQboClient();

    // Get all entries for the pay period that haven't been synced yet
    const entries = await query(`
      SELECT e.*, u.name as user_name, u.type as user_type
      FROM entries e
      JOIN users u ON LOWER(u.email) = LOWER(e.user_email) AND u.tenant_id = e.tenant_id
      WHERE e.tenant_id=$1 AND e.pay_period_start=$2 AND e.pay_period_end=$3 AND (e.qbo_synced IS NOT TRUE)
      ORDER BY e.user_email, e.date
    `, [tid, ppStart, ppEnd]);

    if (entries.rows.length === 0) {
      // Check if there are entries that were already synced
      const allEntries = await query(`SELECT COUNT(*) as cnt FROM entries WHERE tenant_id=$1 AND pay_period_start=$2 AND pay_period_end=$3`, [tid, ppStart, ppEnd]);
      const total = parseInt(allEntries.rows[0].cnt) || 0;
      if (total > 0) return res.json({ ok: true, synced: 0, skipped: 0, total: 0, message: "All entries for this pay period have already been synced to QuickBooks." });
      return res.json({ ok: false, reason: "No entries found for this pay period." });
    }

    // Get QBO employees and vendors to match by name
    const qboEmployees = await new Promise((resolve, reject) => {
      qb.findEmployees({ fetchAll: true }, (err, data) => {
        if (err) return reject(err);
        resolve(data.QueryResponse.Employee || []);
      });
    });
    const qboVendors = await new Promise((resolve, reject) => {
      qb.findVendors({ fetchAll: true }, (err, data) => {
        if (err) return reject(err);
        resolve(data.QueryResponse.Vendor || []);
      });
    });

    // Build name->ID maps (case-insensitive)
    const empMap = {};
    for (const emp of qboEmployees) {
      const fullName = `${emp.GivenName || ""} ${emp.FamilyName || ""}`.trim().toLowerCase();
      empMap[fullName] = emp.Id;
      if (emp.DisplayName) empMap[emp.DisplayName.toLowerCase()] = emp.Id;
    }
    const vendorMap = {};
    for (const v of qboVendors) {
      const fullName = `${v.GivenName || ""} ${v.FamilyName || ""}`.trim().toLowerCase();
      vendorMap[fullName] = v.Id;
      if (v.DisplayName) vendorMap[v.DisplayName.toLowerCase()] = v.Id;
    }

    let synced = 0;
    let skipped = 0;
    const unmatchedNames = new Set();

    for (const entry of entries.rows) {
      const name = (entry.user_name || "").trim().toLowerCase();
      const isContractor = (entry.user_type || "").toLowerCase() === "contractor";
      // Check both Employees and Vendors — different QBO setups handle contractors differently
      let empId = empMap[name];
      let matchedAsVendor = false;
      if (!empId && vendorMap[name]) {
        empId = vendorMap[name];
        matchedAsVendor = true;
      }

      if (!empId) {
        unmatchedNames.add(entry.user_name || entry.user_email);
        skipped++;
        continue;
      }

      const hours = parseFloat(entry.hours_offered) || 0;
      if (hours <= 0) { skipped++; continue; }

      const timeActivity = {
        TxnDate: entry.date,
        NameOf: matchedAsVendor ? "Vendor" : "Employee",
        EmployeeRef: !matchedAsVendor ? { value: empId } : undefined,
        VendorRef: matchedAsVendor ? { value: empId } : undefined,
        Hours: Math.floor(hours),
        Minutes: Math.round((hours % 1) * 60),
        Description: `${entry.class_party || ""}${entry.location ? " @ " + entry.location : ""}${entry.notes ? " — " + entry.notes : ""}`.trim(),
        HourlyRate: parseFloat(entry.hourly_rate) || 0,
      };

      try {
        await new Promise((resolve, reject) => {
          qb.createTimeActivity(timeActivity, (err, data) => {
            if (err) return reject(err);
            resolve(data);
          });
        });
        // Mark as synced
        await query(`UPDATE entries SET qbo_synced=TRUE WHERE id=$1 AND tenant_id=$2`, [entry.id, tid]);
        synced++;
      } catch (syncErr) {
        console.error("QBO sync entry error:", syncErr.Fault || syncErr);
        skipped++;
      }
    }

    const result = { ok: true, synced, skipped, total: entries.rows.length };
    if (unmatchedNames.size > 0) {
      result.unmatched = Array.from(unmatchedNames);
      result.warning = `${unmatchedNames.size} staff member(s) could not be matched to QuickBooks employees. Make sure their names match exactly.`;
    }
    res.json(result);
  } catch (e) {
    console.error("QBO sync error:", e);
    res.json({ ok: false, reason: e.message || "Failed to sync with QuickBooks." });
  }
});

// QBO sync status for a pay period
api.post("/qboSyncStatus", async (req, res) => {
  try {
    const { email, pin, ppStart, ppEnd } = req.body;
    const user = await getAuthorizedUser(email, pin);
    if (!user) return res.json({ ok: false, reason: "Invalid credentials." });
    const uType = String(user.type || "").toUpperCase();
    if (uType !== "ADMIN" && uType !== "MODERATOR") return res.json({ ok: false, reason: "Admin access required." });

    const tid = await getDefaultTenantId();

    // Get all staff with their entry counts and sync status for this period
    const result = await query(`
      SELECT
        u.name,
        u.email,
        u.type,
        COUNT(e.id) as total_entries,
        COUNT(CASE WHEN e.qbo_synced = TRUE THEN 1 END) as synced_entries,
        COALESCE(SUM(e.hours_offered), 0) as total_hours,
        COALESCE(SUM(CASE WHEN e.qbo_synced = TRUE THEN e.hours_offered ELSE 0 END), 0) as synced_hours
      FROM users u
      LEFT JOIN entries e ON LOWER(e.user_email) = LOWER(u.email) AND e.tenant_id = u.tenant_id
        AND e.pay_period_start = $2 AND e.pay_period_end = $3
      WHERE u.tenant_id = $1 AND u.is_active = TRUE AND u.type IN ('Employee', 'Contractor')
      GROUP BY u.name, u.email, u.type
      ORDER BY u.name
    `, [tid, ppStart, ppEnd]);

    res.json({ ok: true, staff: result.rows });
  } catch (e) {
    res.json({ ok: false, reason: "SERVER ERROR: " + e.message });
  }
});

// ── Onboarding form submission (from marketing site after Stripe payment) ──
app.options("/api/onboarding", (req, res) => {
  res.set("Access-Control-Allow-Origin", "https://kronara.app");
  res.set("Access-Control-Allow-Methods", "POST");
  res.set("Access-Control-Allow-Headers", "Content-Type");
  res.sendStatus(204);
});
app.post("/api/onboarding", async (req, res) => {
  res.set("Access-Control-Allow-Origin", "https://kronara.app");
  try {
    const { businessName, ownerName, email, phone, numStaff, numLocations, locations, numAdmins, domainName, features, startDate, referral, specialRequests } = req.body;
    if (!businessName || !ownerName || !email) {
      return res.status(400).json({ ok: false, reason: "Missing required fields" });
    }

    const resend = getResend();
    const onboardingFrom = `Kronara <noreply@kronara.app>`;
    const adminEmail = process.env.KRONARA_ADMIN_EMAIL || "admin@kronara.app";
    const locationsList = (locations || []).join(', ') || 'Not provided';
    const featuresList = (features || []).join(', ') || 'All features';
    const domainDisplay = domainName ? `${domainName}.kronara.app` : 'Not specified';

    await resend.emails.send({
      from: onboardingFrom,
      to: [adminEmail],
      replyTo: [email],
      subject: `New Kronara Signup: ${businessName}`,
      html: `
        <div style="font-family: sans-serif; max-width: 600px; margin: 0 auto; background: #141428; color: #e8eaf0; padding: 32px; border-radius: 16px;">
          <h1 style="color: #6C5CE7; margin-bottom: 24px;">New Client Signup</h1>
          <table style="width: 100%; border-collapse: collapse;">
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Business Name</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${businessName}</td></tr>
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Owner Name</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${ownerName}</td></tr>
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Email</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${email}</td></tr>
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Phone</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${phone || 'Not provided'}</td></tr>
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Number of Staff</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${numStaff}</td></tr>
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Locations (${numLocations})</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${locationsList}</td></tr>
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Admin Staff to Train</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${numAdmins}</td></tr>
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Requested Domain</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${domainDisplay}</td></tr>
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Features</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${featuresList}</td></tr>
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Preferred Start Date</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${startDate || 'ASAP'}</td></tr>
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Referral Source</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${referral || 'Not specified'}</td></tr>
            <tr><td style="padding: 8px 0; color: #8a8ea0;">Special Requests</td><td style="padding: 8px 0; color: #fff; font-weight: 600;">${specialRequests || 'None'}</td></tr>
          </table>
        </div>
      `,
    });

    // Also send a confirmation to the client
    await resend.emails.send({
      from: onboardingFrom,
      replyTo: [adminEmail],
      to: [email],
      subject: `Welcome to Kronara, ${ownerName}!`,
      html: `
        <div style="font-family: sans-serif; max-width: 600px; margin: 0 auto; background: #141428; color: #e8eaf0; padding: 32px; border-radius: 16px;">
          <h1 style="color: #6C5CE7; margin-bottom: 16px;">Welcome to Kronara!</h1>
          <p style="color: #b0b4c4; line-height: 1.6;">Hi ${ownerName},</p>
          <p style="color: #b0b4c4; line-height: 1.6;">Thank you for signing up! We've received your details for <strong style="color: #fff;">${businessName}</strong> and our team will have your studio set up within 24 hours.</p>
          <p style="color: #b0b4c4; line-height: 1.6;">We'll reach out to your email with login credentials and next steps for onboarding and training.</p>
          <p style="color: #b0b4c4; line-height: 1.6; margin-top: 24px;">Cheers,<br><strong style="color: #fff;">The Kronara Team</strong></p>
        </div>
      `,
    });

    res.json({ ok: true });
  } catch (e) {
    console.error("Onboarding email error:", e);
    res.status(500).json({ ok: false, reason: "Failed to send notification" });
  }
});

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

const PORT = process.env.PORT || 3000;

setupDatabase()
  .then(async () => {
    // One-time fix: delete email enabled toggle rows that were set to "false" by a save race bug.
    // Deleting them (rather than setting "true") lets getAdminSettings use its default (!== "false" → true).
    // Safe to leave in — it's a no-op once the rows are gone.
    const _tid = await getDefaultTenantId();
    const _ek = ["accountantEmailEnabled","adminEmail1Enabled","adminEmail2Enabled","adminEmail3Enabled","supportEmailEnabled","payrollAdminEmailEnabled"];
    const delRes = await query(`DELETE FROM settings WHERE tenant_id=$1 AND key = ANY($2) AND value='false'`, [_tid, _ek]);
    if (delRes.rowCount > 0) console.log(`Startup: repaired ${delRes.rowCount} email enabled toggle(s) that were stuck on false.`);

    const server = app.listen(PORT, () => {
      console.log(`Aradia Time PG running on port ${PORT}`);
      server.timeout = 720000; // 12 minutes — needed for long scrape requests (all studios)
      // Run automation checks every hour — each function fires only at its configured MST time
      cron.schedule("0 * * * *", async () => {
        try {
          const s = await getAdminSettings();
          const utcH = new Date().getUTCHours();
          console.log(`Cron: hourly check (UTC hour ${utcH})`);

          // Payroll report auto-send (configurable time, default noon MST)
          if (isTimeToRun(s.autoSendTime) && cronGuard("autoSend")) {
            console.log(`  → Auto-send payroll (${s.autoSendTime} MST)`);
            runAutoSend(false);
          }

          // End-of-period reminders, late notify, flag reminders (shared time)
          if (isTimeToRun(s.autoRemindersTime) && cronGuard("reminders")) {
            console.log(`  → Reminders & notifications (${s.autoRemindersTime} MST)`);
            checkUnapprovedPayroll();
            checkLateSubmissionNotify();
            sendFlagReminders();
            rolloverUnresolvedFlags();
          }

          // N-days-before-end reminder (configurable time)
          if (isTimeToRun(s.autoRemindBeforeTime) && cronGuard("remindBefore")) {
            console.log(`  → Pre-period-end reminder (${s.autoRemindBeforeTime} MST)`);
            checkAutoRemindBeforePeriodEnd();
          }

          // Class Proposals weekly digest — send Monday at 9 AM MST
          if (isTimeToRun("09:00") && cronGuard("proposalDigest")) {
            const now = new Date();
            const mstDay = new Date(now.toLocaleString("en-US", { timeZone: "America/Edmonton" })).getDay();
            if (mstDay === 1 && s.proposalDigestEnabled && s.proposalDigestEmail) {
              console.log("  → Sending class proposals weekly digest");
              try {
                const tid = await getDefaultTenantId();
                const today = new Date(now.toLocaleString("en-US", { timeZone: "America/Edmonton" }));
                const weekEnd = new Date(today); weekEnd.setDate(today.getDate() + 30);
                const startStr = today.getFullYear() + "-" + String(today.getMonth()+1).padStart(2,"0") + "-" + String(today.getDate()).padStart(2,"0");
                const endStr = weekEnd.getFullYear() + "-" + String(weekEnd.getMonth()+1).padStart(2,"0") + "-" + String(weekEnd.getDate()).padStart(2,"0");
                const rows = await query(`SELECT * FROM class_proposals WHERE tenant_id=$1 AND proposal_date >= $2 AND proposal_date <= $3 AND COALESCE(status,'pending')='pending' ORDER BY proposal_date, start_time`, [tid, startStr, endStr]);
                if (rows.rows.length > 0) {
                  const grouped = {};
                  rows.rows.forEach(p => { if (!grouped[p.proposal_date]) grouped[p.proposal_date] = []; grouped[p.proposal_date].push(p); });
                  let html = `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
                    <h2 style="color:${CONFIG.BRAND_COLOR_PRIMARY};">${CONFIG.BRAND_NAME} — Class Proposals Weekly Digest</h2>
                    <p>Here are the class proposals for the next 30 days:</p>`;
                  for (const [date, proposals] of Object.entries(grouped).sort((a,b) => a[0].localeCompare(b[0]))) {
                    const dObj = new Date(date + "T12:00:00");
                    const dayLabel = dObj.toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric", year: "numeric" });
                    html += `<h3 style="margin:16px 0 8px;color:#333;border-bottom:1px solid #ddd;padding-bottom:4px;">${dayLabel}</h3>`;
                    proposals.forEach(p => {
                      html += `<div style="background:#f5f5f5;border-radius:8px;padding:10px 14px;margin:6px 0;border-left:4px solid ${p.color || '#000'};">
                        <strong>${p.class_name}</strong> — ${p.start_time} (${p.duration}min)<br/>
                        <span style="color:#666;">${p.location} / ${p.room} — ${p.proposer_name || p.proposer_email}</span>
                        ${p.notes ? `<br/><em style="color:#888;">${p.notes}</em>` : ""}
                      </div>`;
                    });
                  }
                  html += `<p style="color:#888;font-size:12px;margin-top:16px;">This is an automated weekly digest.</p></div>`;
                  sendMail({ to: s.proposalDigestEmail, subject: `${CONFIG.BRAND_NAME} — Class Proposals Weekly Digest`, html }).catch(e => console.error("Proposal digest email failed:", e.message));
                }
              } catch (e2) { console.error("Proposal digest error:", e2.message); }
            }
          }
        } catch (err) {
          console.error("Cron error:", err.message);
        }
      });
      console.log("Automation cron scheduled (hourly, times configurable in MST)");

      // 24-hour unclaimed shift alert — runs every 30 minutes
      cron.schedule("*/30 * * * *", async () => {
        try {
          const tid = await getDefaultTenantId();
          // Find open shifts within 24 hours that haven't had a reminder sent
          const now = new Date();
          const in24h = new Date(now.getTime() + 24 * 60 * 60 * 1000);
          const todayStr = now.toISOString().slice(0, 10);
          const tomorrowStr = in24h.toISOString().slice(0, 10);
          const urgentShifts = await query(
            `SELECT * FROM shift_posts
             WHERE tenant_id=$1 AND status='open' AND reminder_24h_sent=FALSE
             AND shift_date IN ($2, $3)`,
            [tid, todayStr, tomorrowStr]
          );
          for (const shift of urgentShifts.rows) {
            // Check if the shift datetime is actually within 24 hours
            // Parse start time from shift_time which may be "2:00 PM – 3:00 PM" or "14:00"
            const stRaw = (shift.shift_time || "00:00").split(/\s*[–—\-]\s*/)[0].trim();
            const stMatch = stRaw.match(/(\d+):(\d+)\s*(AM|PM)/i);
            let st24 = stRaw.slice(0,5);
            if (stMatch) { let hh=parseInt(stMatch[1]),mm=stMatch[2],ap=stMatch[3].toUpperCase(); if(ap==='AM'&&hh===12)hh=0; if(ap==='PM'&&hh!==12)hh+=12; st24=String(hh).padStart(2,'0')+':'+mm; }
            const shiftDateTime = new Date(shift.shift_date + "T" + st24);
            if (shiftDateTime > now && shiftDateTime <= in24h) {
              const desc = shift.class_name ? `${shift.class_name} at ${shift.location}` : shift.location;
              const _urgentText = `${shift.class_name||""} ${shift.location||""} ${shift.notes||""}`.toLowerCase();
              const _urgFrontDesk = !!shift.front_desk;
              // Filtered push for urgent shifts
              try {
                const _urgPush = await query(
                  `SELECT DISTINCT ps.user_email, u.shift_filter_keywords, u.front_desk_staff, u.front_desk_only FROM push_subscriptions ps LEFT JOIN users u ON LOWER(u.email)=LOWER(ps.user_email) AND u.tenant_id=ps.tenant_id WHERE ps.tenant_id=$1`,
                  [tid]
                );
                for (const pu of _urgPush.rows) {
                  if (_urgFrontDesk && !pu.front_desk_staff) continue;
                  if (!_urgFrontDesk && pu.front_desk_only) continue;
                  if (pu.shift_filter_keywords) {
                    const filters = pu.shift_filter_keywords.split(",").map(k => k.trim().toLowerCase()).filter(Boolean);
                    if (filters.some(kw => _urgentText.includes(kw))) continue;
                  }
                  sendPush(pu.user_email, "🚨 Urgent: Unclaimed Shift", `${desc} on ${shift.shift_date} at ${shift.shift_time} needs coverage!`, "/", "shifts").catch(() => {});
                }
              } catch (e2) { console.error("TSPS urgent filtered push error:", e2.message); }
              // Email all users with email_shifts enabled (including urgent-only)
              try {
                const emailUsers = await query(
                  `SELECT email, name, shift_filter_keywords, front_desk_staff, front_desk_only FROM users WHERE tenant_id=$1 AND email_shifts=TRUE AND tsps_enabled=TRUE AND (is_active IS NULL OR is_active=TRUE)`,
                  [tid]
                );
                for (const u of emailUsers.rows) {
                  if (_urgFrontDesk && !u.front_desk_staff) continue;
                  if (!_urgFrontDesk && u.front_desk_only) continue;
                  if (u.shift_filter_keywords) {
                    const filters = u.shift_filter_keywords.split(",").map(k => k.trim().toLowerCase()).filter(Boolean);
                    if (filters.some(kw => _urgentText.includes(kw))) continue;
                  }
                  sendMail({
                    to: u.email,
                    subject: `${CONFIG.BRAND_NAME} — Urgent: Unclaimed Shift in 24 Hours`,
                    html: `<div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;">
                      <h2 style="color:#e65100;">🚨 Urgent: Unclaimed Shift</h2>
                      <p><strong>${desc}</strong> on <strong>${shift.shift_date}</strong> needs coverage!</p>
                      <div style="background:#fff3e0;border-radius:8px;padding:12px 16px;margin:12px 0;border-left:3px solid #e65100;">
                        ${shift.class_name ? `<div><strong>Class:</strong> ${shift.class_name}</div>` : ""}
                        <div><strong>Date:</strong> ${shift.shift_date}</div>
                        <div><strong>Time:</strong> ${shift.shift_time}</div>
                        <div><strong>Location:</strong> ${shift.location}</div>
                      </div>
                      <p style="text-align:center;"><a href="${process.env.BASE_URL || 'https://aradiafitness.app'}?tab=tsps" style="display:inline-block;background:#e65100;color:#fff;font-weight:700;font-size:15px;padding:12px 32px;border-radius:8px;text-decoration:none;">CLAIM SHIFT</a></p>
                      <p style="color:#888;font-size:12px;">You can manage email preferences in your profile settings.</p>
                    </div>`
                  }).catch(e => console.error("TSPS urgent email failed:", e.message));
                }
              } catch (e2) { console.error("TSPS urgent email batch error:", e2.message); }
              await query(`UPDATE shift_posts SET reminder_24h_sent=TRUE WHERE id=$1`, [shift.id]);
            }
          }
          if (urgentShifts.rows.length > 0) console.log(`[TSPS] Checked ${urgentShifts.rows.length} urgent shift(s)`);
        } catch (err) {
          console.error("TSPS 24h alert cron error:", err.message);
        }
      });
    });
  })
  .catch(err => {
    console.error("Database setup failed:", err.message);
    process.exit(1);
  });
