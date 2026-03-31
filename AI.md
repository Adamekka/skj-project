# Object Storage Service - Task Log

## Použité nástroje

| Nástroj                           | Role                                                      |
| --------------------------------- | --------------------------------------------------------- |
| **OpenCode** (CLI agent)          | Plánování, generování kódu, spouštění příkazů, verifikace |
| **Claude Sonnet 4.6** (Anthropic) | Jazykový model uvnitř OpenCode                            |

## Plan

- [x] requirements.txt (fastapi, uvicorn, sqlalchemy, python-multipart, aiofiles)
- [x] database.py - SQLAlchemy engine + session factory (SQLite)
- [x] models.py - `File` ORM model (id, user_id, filename, path, size, created_at)
- [x] schemas.py - Pydantic schemas (UploadResponse, FileRecord, DeleteResponse)
- [x] main.py - FastAPI app with POST /files/upload, GET /files, GET /files/{id}, DELETE /files/{id}
- [x] Install deps in .venv, verify import
- [x] Smoke-test all endpoints with curl

## Results

All endpoints verified working:

| Endpoint           | Result                                                             |
| ------------------ | ------------------------------------------------------------------ |
| POST /files/upload | Returns {id, filename, size}, saves bytes to storage/<user>/<uuid> |
| GET /files         | Returns list of FileRecord for the requesting user                 |
| GET /files/{id}    | Streams raw file bytes; 404 for wrong user                         |
| DELETE /files/{id} | Removes file from disk + DB; 404 for wrong user                    |

Cross-user access correctly returns 404 (not 403, to avoid leaking file existence).

## Notes for home assignment extension

- Replace SQLite with PostgreSQL (change `SQLALCHEMY_DATABASE_URL`, remove `check_same_thread`)
- Add real authentication (JWT / OAuth2) and derive `user_id` from the token
- Chunk large uploads with `shutil.copyfileobj` instead of reading full content into memory
- Add content-type detection and return correct `media_type` on download
- Add pagination to `GET /files`
