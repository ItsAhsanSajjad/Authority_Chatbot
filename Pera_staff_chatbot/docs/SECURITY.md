# PERA AI — Security Configuration

## Authentication

### API Key Authentication
Set `AUTH_ENABLED=1` and provide API keys:

```env
AUTH_ENABLED=1
API_KEYS=your-key-1,your-key-2
```

Clients must include the key in requests:
```
X-API-Key: your-key-1
```

### JWT Authentication
For JWT-based auth, also set:
```env
JWT_SECRET=your-strong-secret
JWT_ALGORITHM=HS256
```

Clients send a Bearer token:
```
Authorization: Bearer <token>
```

### Public Endpoints
These endpoints do NOT require authentication:
- `GET /health` — Health check
- `GET /ready` — Readiness probe

### Fail-Fast in Production
If `AUTH_ENABLED=1` but no `API_KEYS` or `JWT_SECRET` are configured, the server will **refuse to start** with a clear error message.

---

## CORS Configuration

Controlled via `CORS_ORIGINS` (comma-separated):
```env
CORS_ORIGINS=https://yourfrontend.example.com,https://admin.example.com
```

**Default** (dev): `http://localhost:3000,http://127.0.0.1:3000`

> ⚠️ Never use `*` in production. Always specify exact origins.

---

## Rate Limiting

Powered by `slowapi`. Configure via environment:

```env
RATE_LIMIT_ENABLED=1
RATE_LIMIT_ASK=30/minute
RATE_LIMIT_TRANSCRIBE=5/minute
```

Rate limits are per-IP. Returns `429 Too Many Requests` when exceeded.

---

## Secret Management

| Secret | Env Var | Notes |
|--------|---------|-------|
| OpenAI API Key | `OPENAI_API_KEY` | Required. Stored as `SecretStr`. |
| API Keys | `API_KEYS` | Comma-separated. Required if `AUTH_ENABLED=1`. |
| JWT Secret | `JWT_SECRET` | Required for JWT auth. |

**Best Practices:**
- Never commit `.env` files to version control
- Use environment variables directly in production (not `.env` files)
- Rotate API keys regularly
- Use separate keys for different environments (dev/staging/prod)
