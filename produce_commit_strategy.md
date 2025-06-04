# Produce 処理における Commit 戦略の設計方針

## ✅ 背景

Kafka へのデータ送信（Produce）は、通常「送った時点で完了」とされる Auto-Commit モデルが主流です。

しかし、業務システムにおいては次のようなニーズがあります：

- ✅ バッチ単位での明示的な commit
- ✅ 複数メッセージの一貫性制御
- ✅ エラー時の rollback / retry

これらに対応するため、Produce 処理においても **Auto / Manual Commit の戦略指定**を導入します。

---

## ✅ 基本方針

```csharp
await dbContext.AddAsync(new Order { ... }); // Auto-Commit（デフォルト）
```

明示的に制御したい場合：

```csharp
await using var tx = dbContext.BeginKafkaTransaction();
await dbContext.AddAsync(...);
await dbContext.AddAsync(...);
await tx.CommitAsync();
```

---

## ✅ Commit戦略の定義

```csharp
public enum CommitStrategy
{
    Auto,   // デフォルト：送信後に即 flush
    Manual  // 明示的に CommitAsync() を呼び出す
}
```

---

## ✅ 拡張構文案（バッチ挿入時）

```csharp
await dbContext.AddRangeAsync(new[] { e1, e2, e3 },
    commit: CommitStrategy.Manual);
```

上記のように、バッチ送信時にも commit 戦略を選択可能にすることで、柔軟な制御が可能になります。

---

## ✅ トランザクションスコープによる制御例

```csharp
await using var tx = dbContext.BeginKafkaTransaction();

await dbContext.AddAsync(new Order { ... });
await dbContext.AddAsync(new Order { ... });

await tx.CommitAsync(); // 明示的な flush + commit
```

> 内部的には Kafka Producer の `InitTransactions()` → `BeginTransaction()` → `Produce(...)` → `CommitTransaction()` の流れに変換されます。

---

## ✅ メリット

| 項目 | 内容 |
|------|------|
| ✅ 柔軟な制御 | Autoでシンプルに、Manualで冪等性や一貫性を担保 |
| ✅ 型安全・宣言的 | `enum CommitStrategy` により構文が明確 |
| ✅ 再送・中断制御 | 途中キャンセルや retry などの拡張がしやすい |

---

## ✅ 結論

Kafka への `Produce` 処理においても、Auto / Manual Commit の指定を導入することで、

- 実装の簡潔さ
- 実運用での信頼性
- 柔軟な運用制御

をすべて両立可能な設計が実現できます。
