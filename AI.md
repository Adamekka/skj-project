# Haystack Storage Node a S3 Gateway - AI Report

Tento report se vztahuje k zadání pro Haystack Storage Node, event-driven S3 Gateway, čtení přes Gateway, soft delete a compaction.

## Použité AI nástroje

| Nástroj | Role |
| ------- | ---- |
| OpenCode | Prohlídka repozitáře, plán práce, úpravy kódu, spuštění testů a opravy integračních detailů |
| GPT-5.5 | Návrh a implementace event-driven Haystack storage flow, úpravy brokeru pro binární payloady, integrační ověření a review rizik |

## Jak byla AI použita

- AI pomohla napojit nové Haystack storage flow na existující FastAPI Gateway, broker a SQLite modely.
- AI navrhla stavový model `uploading -> ready`, aby `202 Accepted` odpovídalo eventual consistency.
- AI pomohla upravit broker tak, aby zvládal binární MessagePack payloady a durable replay zpráv s bytes.
- AI pomohla navrhnout background listenery přes FastAPI lifespan, aby Gateway i Haystack mohly zpracovávat broker zprávy bez blokování HTTP API.
- AI pomohla doplnit compaction skript a administrační endpointy pro přesun živých objektů na nové offsety.
- AI pomohla ověřit řešení přes migraci databáze a pytest integrační testy.

## Prompt

> Úkol 1: Haystack Storage Node (Zápis a Čtení). Vytvořte novou FastAPI aplikaci pro rychlý asynchronní zápis dat a jejich následné čtení. Při startu otevře nebo vytvoří `volume_1.dat` v režimu `ab+`, nastaví maximální velikost svazku, při překročení limitu rotuje na `volume_2.dat` atd. Aplikace se připojí jako subscriber k Message Brokeru na téma `storage.write` pomocí `asyncio.create_task`. Po doručení MessagePack zprávy zjistí offset přes `file.tell()`, zapíše payload fotky, získá velikost a odešle ACK do tématu `storage.ack` ve formátu `{"object_id": "uuid-z-gateway", "volume_id": 1, "offset": 10560, "size": 1024}`. Vytvořte GET endpoint `/volume/{volume_id}/{offset}/{size}`, který otevře příslušný volume soubor, provede `seek(offset)` a vrátí přesně daný počet bajtů.
>
> Úkol 2: Integrace S3 Gateway (Asynchronní zápis). Upravte původní S3 aplikaci tak, aby `POST /upload` místo ukládání na disk poslal soubor přes broker do `storage.write`, zpráva obsahovala unikátní `object_id` a binární data. Do SQLite uložte objekt se statusem `uploading` a vraťte `202 Accepted`. Gateway musí na pozadí poslouchat `storage.ack`, po ACK doplnit `volume_id`, `offset`, `size`, změnit status na `ready` a až potom provést billing.
>
> Úkol 3: Čtení a Soft Delete (S3 Gateway). Gateway je jediný kontakt pro klienty. `GET /download/{object_id}` zkontroluje databázi a oprávnění, pro `ready` objekt interně zavolá Haystack `/volume/...` přes `httpx` a data přepošle uživateli. `DELETE /download/{object_id}` implementuje strict soft delete pouze nastavením `is_deleted = True`; Haystack se o mazání nedozví a data ve volume fyzicky zůstávají.
>
> Úkol 4: Compaction. Vytvořte administrační skript `compact.py` nebo chráněný endpoint. Pro konkrétní svazek si vyžádá ze S3 Gateway seznam nesmazaných objektů a offsetů, vytvoří `volume_1_compacted.dat`, přepíše živá data těsně za sebe, průběžně aktualizuje v Gateway nové offsety a po dokončení nahradí starý volume soubor.

## Co AI pomohla navrhnout správně

- Oddělení Haystack Node do samostatné FastAPI aplikace `src/haystack.py`, zatímco S3 Gateway zůstává jediným veřejným API pro klienty.
- Použití stavového automatu `uploading -> ready`, aby HTTP `202 Accepted` odpovídalo eventual consistency modelu.
- Úpravu brokeru tak, aby MessagePack payloady zachovaly binární data a durable queue uměla bezpečně replayovat zprávy s bytes.
- Uložení upload záznamu a `storage.write` queue zprávy v jedné databázové transakci, aby nevznikl trvale pending objekt bez eventu.
- Zachování legacy fallbacku pro starší soubory s lokální cestou, aby migrace nerozbila existující data.
- Přidání integračního testu pro skutečný tok upload -> Haystack write -> storage ack -> download -> soft delete.

## Co bylo nutné upravit během implementace

- Původní durable broker persistence ukládala payload přes JSON, což nestačilo pro binární fotky; bylo nutné přidat explicitní serializaci binárních hodnot.
- ACK billing logika musela být podmíněná přechodem objektu do `ready`, aby duplicitní ACK nezapočítal upload opakovaně.
- Startup/shutdown background tasky byly implementovány přes FastAPI lifespan handler, aby se nepřidával deprecated `on_event` pattern.
- Worker musel po uploadu zpracovaného obrázku čekat na `ready`, jinak by mohl publikovat hotový výsledek dřív, než jsou bytes dostupné v Haystacku.
- Pro compaction bylo nutné přidat Gateway metadata endpointy pro seznam živých objektů ve volume a aktualizaci nové lokace objektu.

## Limity a chyby AI

- První verze Haystack integrace neměla přímý end-to-end test upload -> ACK -> download; doplněno po review.
- První návrh ACK billing logiky nebyl dostatečně odolný proti duplicitnímu zpracování ACK zprávy; upraveno na podmíněný přechod ze stavu mimo `ready`.
- První návrh MessagePack/JSON binární serializace mohl typově měnit běžné JSON payloady se stejným sentinel tvarem; omezeno na durable broker persistence.

## Výsledné řešení

- Haystack Storage Node zapisuje bytes append-only do rotovaných `volume_N.dat` souborů a čte přes `/volume/{volume_id}/{offset}/{size}`.
- S3 Gateway publikuje `storage.write`, ukládá objekt jako `uploading`, na `storage.ack` doplní lokaci a přepne objekt na `ready`.
- Gateway download interně čte z Haystacku a klient nikdy nepřistupuje k Haystack Node přímo.
- DELETE je soft delete pouze v databázi; Haystack soubor se při mazání neupravuje.
- `compact.py` získá živé objekty pro volume, přepíše je do compacted souboru a aktualizuje offsety přes administrační endpointy Gateway.
- Ověření proběhlo přes `./.venv/bin/python -m alembic upgrade head` a `./.venv/bin/python -m pytest`, výsledkem bylo `17 passed`.
