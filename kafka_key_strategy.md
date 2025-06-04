# Kafka Key の設計方針と上書き手段に関する設計ルール

## 🎯 基本方針

Kafka（ksqlDB）における Partition Key（Kafka Key）は、通常の DB エンジニアが明示的に意識する必要がないように **LINQ 式から推論して自動的に設定**します。

## ✅ デフォルト動作：Kafka Key の自動推論

- `.GroupBy(...)` に指定された列を Kafka Key（Partition Key）として自動推論。
- `.WithPrimaryKey(...)` を併用することで、ドキュメント上の主キーを明示可能。
- Kafka Key の推論順序：
  1. `SetKafkaKey(...)` で明示された列（優先）
  2. `.GroupBy(...)` の列（次点）
  3. `.WithPrimaryKey(...)` の先頭列（最後）

---


### 補足：`SELECT *` の場合

LINQ クエリが `SELECT *` 相当（すなわち `Select(x => x)` のような全列選択）の場合は、
Kafka Key は元の `ToTable(...)` で指定されたエンティティの **PrimaryKey に指定された列**をそのまま使用します。

例：

```csharp
modelBuilder.Entity<Order>()
    .ToTable("orders")
    .WithPrimaryKey(e => new { e.CustomerId, e.ProductId });

var query = context.Orders
    .Where(o => o.Amount > 100)
    .Select(o => o); // SELECT * 相当
```

この場合、Kafka Key は `CustomerId + ProductId` の合成キーとして扱われます。


## ✅ 上級者向け：Kafka Key の上書き手段

Kafka Key を LINQ 式の中で明示的に指定したい場合は、**`.SetKafkaKey(...)` 拡張メソッド**を使用します。

### 使用例：

```csharp
var query = context.Orders
    .Where(o => o.Amount > 100)
    .SetKafkaKey(o => o.CustomerId)
    .GroupBy(o => new { o.CustomerId, o.ProductId })
    .Select(g => new {
        g.Key.CustomerId,
        g.Key.ProductId,
        Total = g.Sum(x => x.Amount)
    });
```

### 解説：
- `.SetKafkaKey(...)` により、Kafka に送信されるメッセージの `key` は `CustomerId` になります。
- `.GroupBy(...)` や `.WithPrimaryKey(...)` よりも優先して使用されます。

---

## 🧠 この設計のメリット

| 項目                     | 内容 |
|--------------------------|------|
| 意図の明示               | Kafka Key の使用理由がコードに自然に現れる |
| 推論と共存               | 自動推論と明示指定の両立が可能 |
| 可観測性の維持           | 必要時にログや生成コードに Kafka Key を表示可能 |
| 学習コストの最小化       | DB エンジニアには Kafka の partition 概念を見せない |

---

## ⚠ 注意点とガイドライン

- `.SetKafkaKey(...)` は必ず `.GroupBy(...)` の前に書くこと
- 不適切なキー指定を避けるため、整合性チェックは翻訳フェーズで行う
- `.SetKafkaKey(...)` を使用しない場合でも推論により自動処理される

---

## ✅ 結論

Kafka Key（Partition Key）は、基本的に推論により扱いを隠蔽し、必要に応じて `.SetKafkaKey(...)` を使って意図的に指定できる柔軟な設計とします。
