# Broker Benchmark Results

## Test Machine

- CPU: Apple M5 Pro
- Logical CPU cores: 18
- Memory: 24 GiB
- OS: macOS 26.4.1 (build 25E253)

## Benchmark Setup

- Broker server: `.venv/bin/python -m uvicorn src.main:app --host 127.0.0.1 --port 18767 --log-level warning --ws-ping-interval 600 --ws-ping-timeout 600`
- Benchmark client: `benchmark.py`
- Subscribers: 5
- Publishers: 5
- Messages per publisher: 10 000
- Total published messages per run: 50 000
- Total delivered messages per run: 250 000
- Throughput formula: `delivered_messages / elapsed_seconds`

## Results

| Format | Elapsed time | Throughput |
| ------ | ------------ | ---------- |
| JSON | 49.692 s | 5030.96 msg/s |
| MessagePack | 48.116 s | 5195.77 msg/s |

## Evaluation

- MessagePack was faster than JSON in this benchmark.
- The improvement was modest: about 3.3%.
- In this implementation, the main bottleneck is not text serialization itself, but durable persistence and ACK handling in SQLite for every published message.
- Because of that, the binary format does help, but not dramatically.

## Conclusion

- If maximum readability and easy debugging are the priority, JSON is still a good default.
- If raw throughput matters more, MessagePack is worth using, but in this project the gain is relatively small until the persistence layer becomes faster.
