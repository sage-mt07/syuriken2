# Kafka トピック定義ルール（Entity DSL設計）

このドキュメントでは、Entity Framework ライクな DSL を使用して Kafka トピックを定義する際のルールを明文化します。
他の DSL仕様ファイル（例：`ksqldb-ef-rules.md`, `ksqldb_ef_usage_guide_for_db_engineer.md` など）と整合性を持たせた記述としています。

---

## ✅ 基本方針

Kafka におけるトピックの作成対象は、`modelBuilder.Entity<T>()` によって宣言された型 `T` です。
ただし、トピックとして扱うか否かは、**その型に対する LINQ 利用有無によって判定**されます。

---

## 🚦 Kafka トピック自動判定ルール

| 条件 | 意味 | DSLによる扱い |
|------|------|----------------|
| `modelBuilder.Entity<T>()` のみ定義されている | Kafka の **入力元トピック** | `CREATE STREAM` または `CREATE TABLE` を自動生成 |
| `modelBuilder.Entity<T>()` があり、かつ LINQ で `.Query`, `.Select`, `.GroupBy` 等が利用されている | Kafka の **中間処理ノード / 出力対象** | トピックとしては扱わず、KSQL クエリのみ生成 |
| `modelBuilder.Entity<T>()` 自体が存在しない | Kafka に関与しない | 対象外 |

---

## 🛠 トピック除外の方法

トピックとして扱いたくない型に対しては、**`modelBuilder.Entity<T>()` を記述しない**ことで対応してください。

> `.IgnoreKafka()` のような除外メソッドは提供しません。

この方針により、DSLの記述と Kafka 対象のスコープが 1:1 対応し、コードの明快性と保守性が向上します。

---

## 📄 明示すべき運用ルール

```csharp
// ✅ Kafka の入力トピックとしたい場合：
modelBuilder.Entity<TradeHistory>();

// ✅ Kafka クエリでのみ使いたい場合（中間ノードなど）
modelBuilder.Entity<JoinedResult>();

// ❌ Kafka に関与させたくない場合：
// → modelBuilder.Entity<T>() 自体を定義しない
```

---

## 🔁 他仕様との整合性

- 本ルールは `ksqldb-ef-rules.md` における `Entity = トピック` の方針に準拠
- トピックのフォーマットは `VALUE_FORMAT='AVRO'` を基本とする（別途記述）
- `[Stock]`, `[Latest]` による STREAM / TABLE の明示的指定も引き続き利用可能

---

## ✅ まとめ

Kafka のトピック定義は、以下のような構成とする：

- `modelBuilder.Entity<T>()` によって対象スコープを宣言
- LINQ 利用の有無により、トピックか中間ノードかを自動判定
- 除外したい場合は `Entity<T>()` 自体を定義しない

このルールにより、DSL定義の最小化と可読性、Kafkaトピックとの整合性を両立します。
