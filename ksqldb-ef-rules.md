# ksqlDB Entity Framework 抽象化ルール仕様

本ドキュメントは、Entity Framework の設計に基づき、ksqlDB における `STREAM` / `TABLE` の使い分けとトピック管理を抽象化するルールを定義する。

---

## 1. エンティティ = トピック

- C# で定義した各エンティティ（POCO）は Kafka の 1 トピックに対応する。
- トピック名は、エンティティ名をスネークケースに変換して生成する。

| エンティティ名       | トピック名            |
|----------------------|-----------------------|
| `TradeHistory`       | `trade_history`       |
| `AccountBalance`     | `account_balance`     |
| `OrderBookSnapshot`  | `order_book_snapshot` |

> 明示的に `[TopicName("custom_topic")]` 属性を指定した場合、上記ルールより優先される。

---

## 2. STREAM / TABLE の使い分け

### 2.1 アノテーションによる指定

- `[Stock]` 属性が付与されたエンティティは `CREATE STREAM` として扱う。
- `[Latest]` 属性が付与されたエンティティは `CREATE TABLE` として扱う。

### 2.2 アノテーションがない場合のデフォルト

- デフォルトでは `[Stock]`（= `STREAM`）として扱う。

---

## 3. トピック定義とCREATE文の自動生成

各エンティティに対して以下を自動生成する：

### STREAM（例）

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

### TABLE（例）

```sql
CREATE TABLE account_balance (
    account_id VARCHAR PRIMARY KEY,
    balance DOUBLE
) WITH (
    KAFKA_TOPIC='account_balance',
    VALUE_FORMAT='JSON'
);
```

> プライマリキーは最初のプロパティ名で推論されるが、必要に応じて `[Key]` 属性で明示可能。

---

## 4. LINQ → KSQLクエリへの変換ルール

- `[Stock]` → `EMIT CHANGES SELECT ... FROM STREAM`
- `[Latest]` → `SELECT ... FROM TABLE`

使用例：

```csharp
var results = context.Entities<TradeHistory>
    .Query
    .Where(t => t.Price > 100)
    .ToListAsync();
```

生成されるクエリ：

```sql
EMIT CHANGES
SELECT *
FROM trade_history
WHERE price > 100;
```

---

## 5. INSERT 処理の自動ルール

```csharp
await context.Entities<TradeHistory>
    .InsertAsync(new TradeHistory { ... });
```

上記は以下のように変換される：

```sql
INSERT INTO trade_history (symbol, price, timestamp) VALUES (...);
```

> `TABLE` への INSERT は制限があるため、設計上 `Latest`（=TABLE）には通常クエリ専用を推奨。

---

## 6. 高度な拡張（将来対応）

- `[Snapshot]` → 時点情報付きのストリームを自動集約して `TABLE` 化
- `[Aggregate]` → `CREATE TABLE AS SELECT` の構文自動生成
- `[Windowed]` → `HOPPING`, `TUMBLING`, `SESSION` によるウィンドウ定義

---

## 7. 推奨開発フロー

1. C# の POCO を定義し `[Stock]` or `[Latest]` を付ける
2. LINQ でクエリを書く（SELECT句、WHERE句など）
3. ライブラリが CREATE 文・クエリ文を自動生成
4. 実行時に Kafka にトピックがなければ自動作成
5. テスト用途ではメモリ上のモックにも対応可能（予定）

---

## 8. 注意事項

- `TABLE` を使用するにはプライマリキーが必要
- Kafka トピックが存在しない場合は自動生成（もしくはエラー）オプション選択式
- KSQL エンジンへの接続は REST 経由で行う

---