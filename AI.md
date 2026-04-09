# Object Storage Service - AI Report

## Použité nástroje AI

| Nástroj                           | Role                                                                |
| --------------------------------- | ------------------------------------------------------------------- |
| **OpenCode** (CLI agent)          | Plánování, úpravy kódu, generování migrací, spouštění verifikace    |
| **GPT-5.4**                       | Návrh modelů, endpointů, Alembic konfigurace a opravy migračních chyb |

## Příklady promptů

### Prompt 1 — Nastavení Alembicu

> Přidej do projektu Alembic. Inicializuj `alembic/`, nastav `alembic.ini` na SQLite databázi `storage.db` a uprav `alembic/env.py` tak, aby importoval `Base.metadata` z `src.database` a modely ze `src.models`.

### Prompt 2 — Migrace 1 (Buckety)

> Přidej SQLAlchemy model `Bucket` a relaci `File.bucket_id -> Bucket.id`. Vygeneruj první Alembic migraci. Pozor: v databázi už existují řádky v tabulce `files`, takže migrace nesmí předpokládat prázdnou DB.

### Prompt 3 — Migrace 2 a 3

> Rozšiř `Bucket` o billing sloupce `bandwidth_bytes`, `current_storage_bytes`, `ingress_bytes`, `egress_bytes`, `internal_transfer_bytes` a přidej do `File` sloupec `is_deleted`. Vygeneruj další dvě migrace a uprav FastAPI endpointy tak, aby billing počítaly při uploadu/downloadu a mazání bylo soft delete.

## Co AI vygenerovala správně

- Správně navrhla použití **Alembicu** místo `Base.metadata.create_all(...)` jako hlavního mechanismu správy schématu.
- Správně nastavila `target_metadata = Base.metadata` a import modelů v `alembic/env.py`, takže `--autogenerate` začal detekovat změny.
- Správně doporučila `render_as_batch=True` pro SQLite, protože SQLite neumí většinu změn tabulek provést přímo přes `ALTER TABLE`.
- Správně navrhla oddělit migrace do tří kroků:
  1. zavedení bucketů,
  2. billing sloupce,
  3. soft delete.
- Správně doplnila Pydantic modely pro nové requesty a response (`BucketCreate`, `BucketRecord`, `ObjectUploadRequest`, `ObjectRecord`, `BucketBillingResponse`, `DeleteResponse`).
- Správně navrhla logiku billing counters:
  - upload zvyšuje `current_storage_bytes` a ingress/internal,
  - download zvyšuje egress/internal,
  - soft delete objekt fyzicky nemaže.
- Správně zachovala kompatibilitu starých endpointů přes skryté aliasy `/files/...`, zatímco nová dokumentovaná API používají `/objects/...` a `/buckets/...`.

## Co bylo nutné opravit

- **První migrace nebyla bezpečná pro existující data.** Autogenerate vytvořil `files.bucket_id` rovnou jako `NOT NULL`, což by na existující tabulce s daty nešlo aplikovat. Bylo nutné migraci ručně upravit:
  - nejdřív přidat `bucket_id` jako nullable,
  - vytvořit pro existující uživatele legacy buckety,
  - zpětně doplnit `bucket_id` do `files`,
  - až potom změnit sloupec na `NOT NULL`.
- **SQLite batch foreign key musel mít jméno.** Alembic vygeneroval `create_foreign_key(None, ...)`, což při `batch_alter_table` na SQLite selhalo chybou `ValueError: Constraint must have a name`. Bylo nutné doplnit explicitní jméno `fk_files_bucket_id_buckets`.
- **Billing migrace potřebovala data backfill.** Po přidání `current_storage_bytes` bylo nutné dopočítat aktuální uloženou velikost z tabulky `files`, jinak by billing po migraci začínal na nule i u existujících objektů.
- **`create_all()` muselo být odstraněno z aplikace.** Jinak by se při startu aplikace schéma obcházelo mimo Alembic a mohlo by dojít k driftu mezi modely a migracemi.
- **Cesta k úložišti byla sjednocena na kořen projektu** (`storage/`), aby odpovídala `.gitignore` a reálnému umístění už existujících souborů.

## Jaké chyby AI udělala

- V první verzi migrace 1 předpokládala prázdnou databázi a nevygenerovala žádný backfill existujících dat.
- U relace `files.bucket_id -> buckets.id` nechala Alembic v batch režimu vygenerovat bezejmenný foreign key, který na SQLite neprošel.
- Nezohlednila, že po neúspěšné ne-transakční migraci může SQLite zůstat v mezistavu a je potřeba databázi nejdřív vrátit do konzistentního stavu, než se migrace spustí znovu.
- V původní verzi aplikace stále existovalo automatické `create_all`, i když po zavedení Alembicu už to není správný způsob správy schématu.
- Bez doplnění ruční SQL části by migrace 2 sice přidala billing sloupce, ale neodpovídala by skutečnému stavu uložených dat.
