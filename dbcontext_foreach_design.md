# dbContext.ForEach 実行構文の設計方針

## 🎯 背景と目的

本プロジェクトでは、Kafka/KSQL に対して Entity Framework ライクな DSL を提供します。

- データスコープ（Stream/Table/Topic や Filter 条件）は **すべて `OnModelCreating` に定義**
- `dbContext.ForEach<T>()` は **それら定義済みエンティティに対する実行命令のみを担う**

---

## ✅ ForEach 実行構文の方針

```csharp
dbContext.ForEach<Order>(
    timeout: TimeSpan.FromSeconds(30),
    token: cancellationToken);
```

### 特徴

- `T` は事前に `OnModelCreating` で `ToStream()`, `ToTable()`, `ToTopic()` のいずれかで定義された Entity
- `timeout` と `token` のみを指定し、**実行制御の責務だけを持たせる**
- **LINQ式などのクエリロジックはここには一切含めない**

---

## ✅ OnModelCreating による定義例

```csharp
modelBuilder.Entity<Order>()
    .ToStream()
    .WithPrimaryKey(e => e.OrderId)
    .HasFilter(e => e.Status == "Active");
```

このように、エンティティがどういうスコープで利用されるかは完全に `OnModelCreating` 側に閉じます。

---

## ✅ ForEach の使用例

```csharp
dbContext.ForEach<Order>(); // timeout: Infinite, token: default

dbContext.ForEach<Customer>(
    timeout: TimeSpan.FromSeconds(15),
    token: cancellationToken);
```

---

## ✅ 設計メリット

| 項目 | 内容 |
|------|------|
| 📚 読みやすい | 「定義」と「実行」がコードレベルで分離されており、直感的に理解できる |
| 🔁 再利用可能 | ForEach は異なる用途（Consumer 登録 / バッチ処理 / UI更新など）で再利用可能 |
| ✅ 型安全 | コンパイル時に Stream/Table/Topic 定義ミスを検出可能 |
| ⚙ 拡張可能 | timeout, token 以外に log, retry, metric 等の拡張も容易 |

---

## ✅ 結論

本設計では、`dbContext.ForEach<T>(timeout, token)` 構文を「**定義済みエンティティに対する実行制御の最小構文**」と位置づけます。

これにより DSL 全体の一貫性・保守性・直感性を高め、DBエンジニア／アプリ開発者の両者にとって使いやすい Kafka アクセスモデルを実現します。
