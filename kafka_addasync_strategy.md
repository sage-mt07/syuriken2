# Kafka AddAsync 処理における非同期化とパフォーマンス最適化方針

## ✅ 背景と課題

Kafka では `ProduceAsync(...)` による送信処理が **ack の受信まで await でブロックされる**設計になっており、これを `AddAsync(...)` にマッピングした場合：

- 各 `await AddAsync(...)` による逐次化が発生
- スループットが大きく低下
- 実運用におけるパフォーマンスボトルネックとなる

---

## ✅ 設計方針：AddAsync は即時戻りし、送信は非同期で行う

### 🎯 ゴール

- `AddAsync()` の呼び出しは高速に完了
- Kafkaへの `produce` は produceAsyncを使用する
- 同期的に使用する場合 await AddAsync()の利用方法とする

---


## ✅ Kafka的観点からの整合性

- Kafka Producer にはもともと `linger.ms`, `batch.size`, `acks` 等のバッファ/遅延制御機能あり
- 本設計は Kafka の思想に則った上で、Entity Framework 的抽象を提供するアプローチ

---

## ✅ 利用者視点のメリット

| 観点 | 内容 |
|------|------|
| 🔄 非同期高速化 | `AddAsync()` の逐次 await を回避し、高速なバルク処理が可能に |
| ⚙ 柔軟性 | AutoFlush 設定により開発者が制御粒度を選択可能 |
| 📦 スケーラビリティ | 実運用で数千〜数万TPSにも耐えうる設計へと拡張可能 |

---

## ✅ 結論

- Kafka に対する `AddAsync(...)` 即時処理とする
- この構成により、**Entity Framework 的な使いやすさと Kafka の高スループット特性の両立**を実現する
