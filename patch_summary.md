# 変更概要：AdditionalTranslationTests.cs のサポート拡張

このパッチは、以下の LINQ クエリを KSQL に変換するための機能を `KsqlExpressionVisitor` と `LinqToKsqlTranslator` に追加します：

1. `IS NULL` / `IS NOT NULL`
2. `IN (...)`（`Contains` メソッド）
3. `DISTINCT`
4. `CAST`

---

## 対応の方針と必要な変更点

### 1. IS NULL / IS NOT NULL
- `VisitBinary` において `null` を右辺または左辺に持つ比較を検出し、`IS NULL` / `IS NOT NULL` に変換。

### 2. IN 句（`Contains`）
- `VisitMethodCall` に `Contains` 検出ロジックを追加し、配列引数 + `.Contains()` を `IN (@p0, @p1, ...)` に変換。

### 3. DISTINCT
- `VisitMethodCall` に `Queryable.Distinct` を検出し、SELECT に `DISTINCT` を追加。

### 4. CAST
- `UnaryExpression` (`ExpressionType.Convert`) にて `CAST(column AS DOUBLE)` を返す。

---

## ファイル構成

- 📄 `patch_summary.md`：このパッチの説明
- 🛠️ 改修対象：
  - `KsqlExpressionVisitor.cs`
  - `KsqlMethodCallTranslator.cs`

---

このパッチ適用後、`AdditionalTranslationTests.cs` の全テストは正常に通過することが期待されます。