# Image Processing Worker - AI Report

Tento report se vztahuje pouze k zadání pro image processing worker, REST endpoint `POST /buckets/{bucket_id}/objects/{object_id}/process` a navazující integrační testy.

## Použité AI nástroje

| Nástroj | Role |
| ------- | ---- |
| OpenCode | Prohlídka repozitáře, plán práce, úpravy kódu, spuštění testů a opravy integračních detailů |
| GPT-5.4 | Návrh architektury worker flow, návrh testovací strategie a kontrola minimálního čistého řešení |

## Jak byla AI použita

- AI pomohla zvolit nejmenší čistou integraci nad existující storage aplikací a brokerem.
- AI doporučila, aby REST endpoint pouze enqueue-nul job do topicu `image.jobs` a nečekal na výsledek.
- AI doporučila ukládat zpracovaný obrázek jako nový objekt místo přepisování původního souboru.
- AI pomohla navrhnout importovatelnou async funkci `run_worker(...)`, aby šel worker spouštět jako script i z integračních testů.
- AI doporučila testovat worker proti dočasně spuštěnému Uvicornu, protože worker používá skutečné `websockets.connect` a `httpx`.

## Příklady promptů

### Prompt 1 - Architektura worker flow

> Navrhni co nejmenší čistou integraci image-processingu do existující FastAPI storage aplikace s WebSocket brokerem. Endpoint `POST /buckets/{bucket_id}/objects/{object_id}/process` má jen enqueue-nout job do topicu `image.jobs`, worker si má stáhnout originál přes interní HTTP API, zpracovat ho NumPy operací a uložit výsledek jako nový objekt.

### Prompt 2 - Integrační test workeru

> Navrhni spolehlivý pytest integrační test pro standalone async worker, který používá `websockets.connect` a `httpx`. Test má poslat 10 jobů do brokeru a ověřit 10 completion zpráv v topicu `image.done` bez křehké synchronizace přes `sleep`.

### Prompt 3 - NumPy operace nad obrázky

> Navrhni jednoduchou implementaci operací `invert`, `mirror`, `crop`, `brightness` a `grayscale` čistě přes NumPy pole tak, aby se Pillow použilo jen pro načtení a finální uložení obrázku.

## Co AI pomohla navrhnout správně

- Oddělení sdílené image-processing logiky do samostatného modulu `src/image_processing.py`.
- Použití jednoho request/job modelu pro REST API i worker payload, aby validace nebyla duplikovaná.
- Uložení výsledku jako nového objektu, což lépe sedí na současný append-only model `files`.
- Worker loop s ACK až po zpracování jobu a po publikaci výsledku do `image.done`.
- Integrační test přes dočasně spuštěný Uvicorn místo snahy napojit standalone worker na `TestClient`.
- Failure event do `image.done`, aby worker nepadal na neplatné operaci a šel chybový stav ověřit v testu.

## Co bylo nutné upravit během implementace

- `pytest-asyncio` běží v `STRICT` režimu, takže async fixture pro dočasný Uvicorn musela být označena `@pytest_asyncio.fixture`.
- Lokální `./.venv/bin/pip` wrapper mířil do jiného virtualenv než `./.venv/bin/python`, takže instalace `numpy` a `Pillow` musela proběhnout přes `./.venv/bin/python -m pip install -r requirements.txt`.
- Worker test potřeboval explicitní `ready_event`, aby publisher nezačal posílat joby dřív, než je worker opravdu přihlášený k `image.jobs`.

## Limity a chyby AI

- AI napoprvé neodhalila problém s rozbitým `pip` wrapperem v lokálním virtualenv; ukázalo se to až při prvním pytest collection runu.
- AI nejprve označila async fixture jen `@pytest.fixture`, což nestačilo pro `pytest-asyncio` strict režim.

## Výsledné řešení

- REST API přidává endpoint `POST /buckets/{bucket_id}/objects/{object_id}/process`, který job pouze enqueue-ne a vrátí `202 processing_started`.
- Worker běží jako samostatný async proces ve `worker.py`, subscribuje `image.jobs`, stáhne originál přes interní HTTP API, provede NumPy operaci a uploadne výsledek jako nový objekt.
- Po dokončení worker publikuje event do `image.done`; při chybě publikuje failure event se stručným popisem chyby.
- Worker podporuje operace `invert`, `mirror`, `crop`, `brightness` a `grayscale`.
- Integrační test ověřuje jak enqueue přes REST API, tak scénář `10 jobs -> 10 done messages`, plus odolnost workeru vůči neplatné operaci.
