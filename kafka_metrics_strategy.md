# Kafka メトリクス設計方針（Confluent.Kafka 使用）

## ✅ 方針概要

Kafka におけるプロデューサ／コンシューマのメトリクスについては、`Confluent.Kafka` パッケージが提供する統計機能（`statistics.handler`）を用いて観測します。

- 🎯 Kafka の稼働状況・性能・失敗率の観測が可能
- 🚀 初期構築が容易で拡張性も高い
- 🔄 必要に応じて他のモニタリング基盤（Datadog, Prometheus 等）への連携も可能

---

## ✅ 取得できる代表的メトリクス

| カテゴリ | 指標例 | 意味 |
|----------|--------|------|
| producer | `message.drops`, `queue.time.avg`, `batch.size.avg` | バッファ詰まり、送信遅延、バッチ効率 |
| consumer | `fetch.latency.avg`, `records.lag.max` | コンシューム遅延、レイテンシ |
| 全般     | `brokers`, `partitions`, `connection.count` | クラスタ接続の健全性や負荷指標 |

---

## ✅ 実装例（Producer）

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    StatisticsIntervalMs = 5000, // 5秒ごとに統計出力
};

var producer = new ProducerBuilder<string, string>(config)
    .SetStatisticsHandler((_, json) =>
    {
        Console.WriteLine(json); // JSONとしてログ出力
    })
    .Build();
```

---

## ✅ 利用上のメリット

| 観点 | 内容 |
|------|------|
| 🎯 シンプル実装 | 特別なライブラリなしで統計取得が可能 |
| 📊 高精度 | Kafka公式ドライバによる正確な値 |
| 🔧 出力先柔軟 | Datadog, Prometheus, OpenTelemetry にも中継可能 |
| ⚙ 低負荷 | 数秒おきの軽量なJSON出力（アプリ本体に影響を与えにくい） |

---

## ✅ 今後の拡張余地（必要に応じて）

- `IMetrics` 抽象化層の導入
- Prometheus Exporter の組み込み
- OpenTelemetryによる分散トレースとの連携

---

## ✅ 結論

Kafka DSL におけるメトリクスは、初期段階では `Confluent.Kafka` の組み込み `statistics.handler` を採用する。

- 構築コストが低く
- 実運用上の可視化要件を十分に満たし
- 拡張性も維持できる

この方針により、Kafka 処理の信頼性と可観測性を両立する。
