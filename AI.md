# Object Storage Service - AI Report

## Použité nástroje AI

| Nástroj                           | Role                                                                    |
| --------------------------------- | ----------------------------------------------------------------------- |
| **OpenCode** (CLI agent)          | Plánování, generování kódu, spouštění příkazů, verifikace               |
| **Claude Sonnet 4.6** (Anthropic) | Jazykový model uvnitř OpenCode — hlavní generátor kódu a dokumentace    |

---

## Příklady promptů

### Prompt 1 — Inicializace projektu

> Vytvoř FastAPI aplikaci simulující jednoduché S3 objektové úložiště. Aplikace musí mít:
> - `POST /files/upload` — nahrání souboru (multipart/form-data), uložení bytů na disk
> - `GET /files` — seznam souborů aktuálního uživatele (dle hlavičky X-User-Id)
> - `GET /files/{id}` — stažení souboru
> - `DELETE /files/{id}` — smazání souboru
> Metadata ukládej do SQLite přes SQLAlchemy 2.x. Uživatelé se neautentizují — identita
> pochází z HTTP hlavičky X-User-Id. Použij Pydantic modely pro všechny odpovědi.

### Prompt 2 — Rozšíření o validaci (tato iterace)

> Do stávajícího řešení přidej ke všem Pydantic schématům `Field` s `title`, `description`
> a omezujícími parametry (min_length, ge=0 …). Každé schéma musí mít `json_schema_extra`
> s příkladem hodnot. Endpointům přidej `tags`, `summary` a `response_description`.
> Přidej `pydantic>=2.0.0` explicitně do requirements.txt.

### Prompt 3 — Oprava Swagger UI (file picker)

> Swagger UI nezobrazuje input pro nahrání souboru u POST /files/upload. Oprav to tak,
> aby se zobrazilo tlačítko "Choose File".

---

## Co AI vygenerovala správně

- **Celková architektura** — rozdělení do `database.py`, `models.py`, `schemas.py`, `main.py`
  odpovídá doporučené struktuře FastAPI projektů.
- **SQLAlchemy ORM model** (`models.py`) — správně použité `Mapped[typ]` + `mapped_column`,
  `server_default=func.now()`, indexy na `id` a `user_id`.
- **Dependency injection** — vzor `get_db()` s `yield` a `finally: db.close()` je idiomaticky
  správný pro FastAPI + SQLAlchemy.
- **Ochrana před přístupem cizího uživatele** — záměrné vrácení HTTP 404 (ne 403), aby
  se nezveřejňovala existence souboru. AI správně přidala vysvětlující komentář.
- **Pydantic schémata** — `from_attributes = True` na `FileRecord` pro ORM-mode
  serializaci bylo vygenerováno bez nutnosti opravy.
- **`openapi_extra` workaround** pro file picker v Swagger UI — správně identifikovaný
  problém s rozdílem mezi JSON Schema (`contentMediaType`) a OpenAPI (`format: binary`).
- **Pydantic `Field` validace** — v druhé iteraci správně přidány parametry
  `title`, `description`, `min_length`, `ge=0` a `json_schema_extra` se vzorovou hodnotou.
- **Endpoint metadata** — `tags`, `summary`, `response_description` vygenerovány
  konzistentně pro všechny endpointy.

---

## Co bylo nutné opravit

- **Chybějící `response_model` u stahování** — AI zpočátku přidala `response_model` i na
  `GET /files/{id}`, který vrací `FileResponse` (binární data). To způsobovalo chybu při
  spuštění. Bylo opraveno odstraněním `response_model` a přidáním vysvětlujícího komentáře
  přímo do kódu.
- **Import `pydantic` v `requirements.txt`** — AI ho nezařadila do závislostí, protože
  Pydantic je tranzitivní závislost FastAPI. Po připomínce byl přidán explicitně
  (`pydantic>=2.0.0`).
- **Cesta k `storage/` adresáři** — AI generovala `STORAGE_DIR` relativně vůči souboru
  (`os.path.dirname(__file__)`), ale skutečné soubory skončily v kořeni repozitáře
  místo v `src/storage/`. Chování závisí na tom, odkud se spouští `uvicorn`.

---

## Chyby AI

- **Překryv `Session` a `SessionLocal`** — v první verzi AI v `main.py` použila
  `Session(bind=engine.connect())` (vzor ze zadání) místo `SessionLocal()` ze
  `sessionmaker`. To je nesprávné a způsobuje memory leak (každý request by vytvořil
  nové spojení bez poolingu). Opraveno zavedením `SessionLocal` v `database.py`.
- **`echo=True` zapomenuto** — zadání uvádí `echo=True` pro výpis raw SQL do konzole.
  AI tuto volbu v první iteraci vynechala; bylo nutné ji doplnit ručně (pro ladění).
- **Chybějící kontrola velikosti souboru** — AI nenavrhla omezení na maximální velikost
  uploadu. Celý obsah souboru se načítá do paměti (`await file.read()`), což může
  způsobit výpadek při velkých souborech. Jde o známé omezení zaznamenané v sekci Notes.
- **`nullable=False` redundance** — SQLAlchemy 2.x odvozuje `nullable` z `Mapped[typ]`
  (ne-optional typ = NOT NULL). AI explicitně přidala `nullable=False` ke všem sloupcům,
  což je sice funkčně správné, ale redundantní.
