# Kafka DSL における Dead Letter Queue (DLQ) 戦略設計

## ✅ 方針概要

Kafka において `ForEach<T>()` による処理中に例外が発生した場合、メッセージを安全に退避させるための **Dead Letter Queue (DLQ)** を導入します。

本設計では、以下の拡張を採用します：

- `SendToDeadLetterQueueAsync<T>(...)` により、DLQトピックへ送信
- DLQトピック名は `T` の型に基づいて自動決定（例：`order_dlq`）
- CommitStrategy に応じて commit 有無を分岐制御

---

## ✅ 使用例

```csharp
await dbContext.ForEach<Order>(..., commit: CommitStrategy.Auto)
    .OnMessageAsync(async msg =>
    {
        try
        {
            Process(msg);
        }
        catch (Exception ex)
        {
            await dbContext.SendToDeadLetterQueueAsync<Order>(msg, ex);
            // Auto の場合は自動 commit され、次のメッセージに進む
        }
    });
```

---

## ✅ CommitStrategy による DLQ 後の制御

| CommitStrategy | DLQ送信後に Commit | 理由 |
|----------------|-------------------|------|
| `Auto`         | ✅ 自動で Commit   | DLQ送信＝処理成功とみなし、次へ進む |
| `Manual`       | ❌ Commitしない     | 明示的に CommitAsync を呼ぶまで再送される |

---

## ✅ DLQトピックの定義戦略

```csharp
modelBuilder.Entity<Order>()
    .ToStream()
    .WithDeadLetterQueue(); // 自動で 'order_dlq' を定義
```

---

## ✅ SendToDeadLetterQueueAsync<T> の構成例

```csharp
public Task SendToDeadLetterQueueAsync<T>(
    T originalMessage,
    Exception exception,
    CancellationToken cancellationToken = default)
{
    var dlqTopic = typeof(T).Name.ToLower() + "_dlq";

    var envelope = new
    {
        Timestamp = DateTime.UtcNow,
        Message = originalMessage,
        Error = exception.ToString(),
    };

    return kafkaProducer.ProduceAsync(dlqTopic, envelope, cancellationToken);
}
```

---

## ✅ 利用者視点のメリット

| 観点 | 内容 |
|------|------|
| 💡 汎用性 | `T` を指定するだけで型ごとにDLQ送信可能 |
| 🔄 可観測性 | 失敗原因とメッセージの紐づけ記録が可能 |
| 🧱 再処理性 | DLQトピックから再投入／分析が可能 |

---

## ✅ 結論

DLQは「失敗を受け入れる設計」として実運用に不可欠。  
`SendToDeadLetterQueueAsync<T>` を導入し、型安全かつ CommitStrategy と連携した柔軟な制御が可能となる。
