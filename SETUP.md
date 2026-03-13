# Kronara — Railway Deployment Setup

## 1. Create Railway Project
- Go to railway.app, create new project
- Connect the `beezkneez/kronara-build` GitHub repo
- Railway will auto-detect Node.js

## 2. Add Postgres Database
- In Railway, click "New" → "Database" → "PostgreSQL"
- Link it to your service — this auto-sets `DATABASE_URL`

## 3. Set Port
- When creating a public domain, enter port: **3000**

## 4. Set Environment Variables
Go to your service → Variables tab and add:

### Required
```
DATABASE_URL        → auto-set by Railway Postgres plugin
DEMO_MODE           → true
```

### Branding
```
BRAND_NAME          → Kronara
BRAND_SUB           → Time. Teams. Simplified.
BRAND_SITE          → https://kronara.app
BRAND_LOGO_URL      → /logo.png
BRAND_COLOR_PRIMARY → #0C7C80
BRAND_COLOR_ACCENT  → #D4942A
BRAND_COLOR_DARK_BG → #1A1A2E
BRAND_DEFAULT_THEME → kronara
APP_DOMAIN          → your-domain.up.railway.app
```

### Email (optional)
```
RESEND_API_KEY      → your Resend API key
RESEND_FROM         → Kronara <support@kronara.app>
```

### Not needed for demo (skip these)
```
VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY  → push notifications
GOOGLE_OAUTH_CLIENT_ID etc.           → Google Calendar
ANTHROPIC_API_KEY                     → AI support chat
REMOVEBG_API_KEY                      → image bg removal
IMAGE_GENERATOR_URL                   → image generation
```

## 5. Deploy
- Railway auto-deploys on push to main
- First boot creates all tables and seeds demo data automatically

## 6. Login
- Admin: username `admin`, pin `1212`
- Demo user: username `demo`, pin `1212`
- Demo staff: username `sarah` / `jessica` / `alex` / `maya`, pin `1234`

## Notes
- Demo data (seed users, entries, locations, proposals, shifts) cannot be deleted
- Users can add new data freely — only seed data is protected
- To reset demo data, drop the Postgres database and redeploy
