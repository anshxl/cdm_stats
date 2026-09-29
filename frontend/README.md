# CDM Stats frontend

Vite + React + TypeScript. Run the commands in `frontend/` unless noted.

```
npm install                 # once
npm run dev                 # http://localhost:5173, proxies /api to :8000
npm run build               # type-check + production build into dist/
npm run gen:types           # regenerate src/api/types.ts from the FastAPI schema
```

The API must run for `npm run dev` (from the repo root):

```
uv run uvicorn cdm_stats.api.app:app --reload --port 8000
```
