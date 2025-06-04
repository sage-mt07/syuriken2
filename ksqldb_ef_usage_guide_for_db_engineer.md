# ksqlDB用 Entity Framework風ライブラリの使い方（DBエンジニア向けガイド）

## 🎯 目的

このライブラリは、Kafka / ksqlDB を使った **リアルタイムデータ処理**を  
従来の **リレーショナルデータベース（RDB）の感覚で扱えるようにする**ためのものです。

---

## 🔄 発想の転換：TABLE ≠ TABLE、STREAM ≠ LOG

| あなたの知ってる世界 | ksqlDBの世界 | このライブラリでは |
|----------------------|--------------|------------------|
| テーブル（TABLE）     | 最新状態の写像 | `[Latest]` で定義 |
| ログや履歴           | ストリーム（STREAM） | `[Stock]` で定義 |

- `TABLE` は「現在の状態（Latest）」を保持するためのビュー的存在  
- `STREAM` は「時系列ログ（Stock）」を保持する

---

## 🏗️ エンティティ定義とトピック

- 各エンティティ（クラス）は1つのKafkaトピックと対応
- トピック名は自動で生成（`TradeHistory` → `trade_history` など）

```csharp
[Stock] // = STREAM
public class TradeHistory
{
    public string Symbol { get; set; }
    public double Price { get; set; }
    public long Timestamp { get; set; }
}
```

```csharp
[Latest] // = TABLE
public class AccountBalance
{
    public string AccountId { get; set; }
    public double Balance { get; set; }
}
```

---

## 🛠️ 書き方（まるでRDB）

### クエリ

```csharp
var trades = context.Entities<TradeHistory>
    .Query
    .Where(t => t.Symbol == "USDJPY")
    .ToListAsync();
```

### 書き込み

```csharp
await context.Entities<TradeHistory>
    .InsertAsync(new TradeHistory { Symbol = "USDJPY", Price = 155.3, Timestamp = ... });
```

> TABLE（[Latest]）は読み取り専用とし、書き込みはSTREAMで行いましょう。

---

## 📄 CREATE文も自動生成されます

例：`[Stock]` なら

```sql
CREATE STREAM trade_history (
    symbol VARCHAR,
    price DOUBLE,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='trade_history',
    VALUE_FORMAT='JSON'
);
```

---

## 🤝 RDBとの使い分けイメージ

| 種類 | 例 | 使用すべきKSQL型 | 属性 |
|------|----|-------------------|------|
| 取引履歴 | 注文・ログ・アクセスログなど | STREAM | `[Stock]` |
| 状態保持 | 残高・在庫数・ログイン状態 | TABLE | `[Latest]` |

---

## ✅ メリットまとめ

- Kafka/ksqlDBの知識がなくても「RDB風に使える」
- モデルだけ書けばトピックやCREATE文も自動生成
- LINQベースなのでクエリも直感的
- RDB設計スキルをそのまま活かせる

---

## 💡 補足

- トピック名は自動生成されるが、必要に応じて明示的に指定可能
- Kafka未経験でも、安全にストリーミング処理を導入できる
- 実運用では、STREAM→TABLEの集約をパイプラインとして構成可能