# Matcha — AI-Powered Matchmaking MVP

A full-stack matchmaking platform built with Next.js 16, TypeScript, Prisma (SQLite), and custom JWT authentication.

## Features

- **Auth** — Register, login, logout (httpOnly JWT cookies)
- **Profiles** — Name, age, bio, interests, location, preferences
- **Matching** — Rule-based compatibility scoring (location, age, interests, bio)
- **Messaging** — Chat between matched users
- **Admin** — User list, activate/deactivate, platform stats

## Tech stack

| Layer | Choice |
|-------|--------|
| Framework | Next.js 16 (App Router) |
| Language | TypeScript |
| Database | SQLite via Prisma |
| Auth | Custom JWT (`jose`) + bcrypt |
| UI | Tailwind CSS v4 + shadcn-style components |
| Forms | react-hook-form + Zod |

## Quick start

```bash
cd matcha
cp .env.example .env   # or copy .env if present
npm install
npx prisma generate
npx prisma db push
npm run db:seed
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

## Demo accounts

| Role | Email | Password |
|------|-------|----------|
| Admin | `admin@matcha.com` | `admin123` |
| User | `alice@matcha.com` | `password123` |
| User | `bob@matcha.com` | `password123` |
| User | `carol@matcha.com` | `password123` |

## Project structure

```
matcha/
├── app/
│   ├── (auth)/          # login, register
│   ├── (dashboard)/     # dashboard, profile, matches, messages, settings
│   ├── (admin)/         # admin stats, user management
│   └── api/             # REST API routes
├── components/          # UI + layout
├── lib/                 # auth, db, matching, validations
├── prisma/              # schema + seed
└── middleware.ts        # route protection
```

## API overview

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/auth/register` | Create account |
| POST | `/api/auth/login` | Sign in |
| POST | `/api/auth/logout` | Sign out |
| GET | `/api/auth/me` | Current user |
| GET/PUT | `/api/profile` | User profile |
| GET | `/api/matches` | Matches (auto-generates if empty) |
| GET/POST | `/api/messages` | Thread messages |
| GET | `/api/admin/stats` | Admin metrics |
| GET/PATCH | `/api/admin/users` | User management |

## Matching logic

Matches are computed in `lib/matching.ts`:

- Location overlap (+0.2)
- Age within 20 years (+0.15)
- Shared interests (+0.1 each, max 0.3)
- Bio similarity keywords (+0.15)

Minimum score **0.3** to create a match. Visiting `/api/matches` triggers generation when none exist.

## Environment

```env
DATABASE_URL="file:./dev.db"
JWT_SECRET="your-secret-min-32-chars"
```

## Scripts

- `npm run dev` — development server
- `npm run build` — production build
- `npm run db:seed` — seed admin + demo users
- `npm run db:studio` — Prisma Studio

## Deploy notes

For production, switch SQLite to PostgreSQL in `prisma/schema.prisma`, set strong `JWT_SECRET`, and deploy to Vercel/Railway with `prisma migrate deploy`.

## License

MIT
