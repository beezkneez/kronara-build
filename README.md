# Kronara — Application (kronara-build)

**This is the app**, not the marketing website.

- **GitHub repo:** `beezkneez/kronara-build`
- **Stack:** Node/Express + Postgres, deployed on Railway
- **Multi-tenant:** one deploy per studio, branded via `BRAND_*` env vars
  (see `.env.example`); tenants live at `<tenant>.kronara.app`
- **Setup:** see [SETUP.md](SETUP.md)

## Layout

| Path | What it is |
|---|---|
| `server.js` | The entire backend — API, auth, payroll, email, Stripe |
| `public/index.html` | The single-page app (staff + admin) |
| `public/guide.html` | User guide |
| `public/privacy.html`, `public/terms.html`, `public/security.html` | Legal + security pages |
| `public/brand.css` | Shared brand stylesheet for the static pages |
| `public/sw.js` | Service worker (push notifications) |

## Theming

Four themes, selected per user and stored in `localStorage` under `app_theme`:

- `light`, `dark` — neutral
- `kronara` — the platform brand, charcoal + gold (**default**)
- `aradia` — the Aradia Fitness tenant's own red branding

Theme tokens are CSS custom properties defined per `[data-theme="..."]` block
near the top of `public/index.html`.

> **`--red` is the brand accent token and is dual-role** — it is used both as a
> solid fill behind white text (~44 places) and as text on dark surfaces
> (~31 places). Its value is chosen to clear contrast in *both* directions;
> don't brighten it without re-checking. Primary buttons in the `kronara`
> theme use a gold gradient with dark ink text instead.

Server-side brand defaults (name, tagline, colours, icons) are in the `CONFIG`
block in `server.js` and can be overridden per tenant with `BRAND_*` env vars.

## Don't confuse these repos

| Folder | Repo | What it is |
|---|---|---|
| `C:\dev\kronara` | `beezkneez/kronara-build` | **This** — the app |
| `C:\dev\kronara-website` | `beezkneez/kronara` | The marketing site at kronara.app |
| `C:\dev\kronara-admin` | `beezkneez/kronara-admin` | Tenant provisioning / admin tooling |
| `C:\dev\kronara-mobile` | — | Capacitor mobile shell |
