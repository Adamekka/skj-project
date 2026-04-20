# Message Broker - AI Report

## Použité nástroje AI

| Nástroj | Role |
| ------- | ---- |
| OpenCode | Plánování práce, úpravy kódu, generování migrací, spouštění testů a benchmarků |
| GPT-5.4 | Návrh ConnectionManageru, WebSocket protokolu, durable queue logiky, testů a benchmark skriptů |

## Příklady promptů

### Prompt 1 - Návrh ConnectionManageru

> Navrhni jednoduchý in-memory ConnectionManager pro FastAPI WebSocket broker. Musí umět více klientů, subscriptions podle topicu, bezpečné odpojení klienta a broadcast do všech subscriberů daného topicu.

### Prompt 2 - Durable queue a ACK logika

> Přidej perzistenci zpráv do SQLite přes SQLAlchemy. Po publish se zpráva nejdřív uloží do DB s `is_delivered = False`, pak se doručí subscriberům. Po `ack` se označí jako doručená. Při novém subscribe se mají nejdřív poslat všechny nedoručené zprávy pro daný topic.

### Prompt 3 - Asynchronní WebSocket testy

> Jak nejjednodušeji otestovat FastAPI WebSocket endpoint pro scénáře subscribe, publish a nedoručení do jiného topicu? Preferuj řešení, které bude spolehlivé a nebude vyžadovat externě spuštěný server.

### Prompt 4 - Blokující SQLAlchemy ve WebSocket handleru

> Broker běží asynchronně, ale projekt už používá synchronní SQLAlchemy Session nad SQLite. Navrhni řešení, které nebude blokovat event loop, a vysvětli proč je vhodnější než kompletní přepis na AsyncSession pro tento malý projekt.

## Co AI vygenerovala správně

- Správně doporučila oddělit broker do samostatných modulů místo přepisování celé `src/main.py`.
- Správně navrhla `ConnectionManager` s mapováním `topic -> set[WebSocket]` a samostatným cleanupem při disconnectu.
- Správně zavedla jednotný protokol zpráv přes Pydantic modely (`subscribe`, `publish`, `ack`, `deliver`, `error`).
- Správně navrhla podporu dvou wire formátů: JSON přes textový frame a MessagePack přes binární frame.
- Správně upozornila, že synchronní SQLAlchemy Session nesmí běžet přímo v async WebSocket handleru.
- Správně doporučila použít `run_in_threadpool(...)` a vytvářet krátce žijící `SessionLocal()` instance uvnitř DB helper funkcí. Toto řešení bylo zvoleno, protože je malé, bezpečné a nevyžaduje přepis celé aplikace na async ORM.
- Správně doporučila použít FastAPI/Starlette `TestClient` pro WebSocket integrační testy. To se ukázalo jako nejjednodušší a zároveň spolehlivé řešení pro tento projekt.
- Správně navrhla benchmark skript založený na `asyncio.gather(...)` s více publishery a subscribery.

## Co bylo nutné opravit

- Původní benchmark ACKoval každou zprávu ze všech subscriberů. To zbytečně benchmarkovalo duplicitní ACK traffic do SQLite místo samotného publish/deliver výkonu. Bylo upraveno tak, že ACK posílá pouze jeden subscriber, protože databázový model má jen jeden globální příznak `is_delivered`.
- Původní plný benchmark narážel na WebSocket keepalive timeout. Bylo nutné spouštět Uvicorn s delším `--ws-ping-interval` a `--ws-ping-timeout`, aby dlouhý běh nespadl na keepalive místo skutečného výkonového limitu.
- Testy původně kontrolovaly stav `is_delivered` v databázi okamžitě po odeslání ACK. To bylo občas příliš brzy, protože broker ACK zpracovává asynchronně. Testy byly opraveny krátkým polling waitem.
- Pro pytest bylo nutné doplnit `tests/conftest.py`, aby se kořen projektu přidal do `sys.path` a testy uměly importovat `src.*` moduly.

## Jaké chyby AI udělala

- AI zpočátku podcenila, že při plném benchmarku budou keepalive timeouty reálný problém a že je potřeba upravit nastavení Uvicornu pro dlouhé WebSocket spojení.
- AI nejdřív benchmarkovala zbytečně drahý scénář s ACK od všech subscriberů, i když datový model durable queue používá jen jeden společný `is_delivered` flag.
- AI původně předpokládala, že stačí hned po `ack` číst databázi v testu, ale v praxi bylo potřeba počkat na doběhnutí broker loopu.
- AI zvažovala čistě async testy přes `pytest-asyncio`, ale nakonec se ukázalo, že pro WebSocket integrační testy je jednodušší a spolehlivější `TestClient`; `httpx` zůstal použit jen pro malý async HTTP smoke test.

## Zvolené řešení blokujících DB operací

- Broker endpoint běží asynchronně.
- Databázové helper funkce (`store`, `load pending`, `ack`) jsou synchronní a pracují s běžným `SessionLocal()`.
- Tyto helper funkce se volají přes `run_in_threadpool(...)`, takže neblokují event loop.
- Toto řešení bylo zvoleno místo `AsyncSession`, protože:
  - projekt už měl hotový sync SQLAlchemy stack,
  - SQLite + krátké DB operace pro cvičení fungují dobře,
  - změna byla malá a lokální,
  - testování i integrace s Alembicem zůstaly jednoduché.
