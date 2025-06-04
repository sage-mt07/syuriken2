# Kafka セキュリティ設計（Kubernetes環境）- Step 1 & 2 対応版

## ✅ 目的

Kubernetes 環境から Kafka に安全に接続するために、最低限必要なセキュリティ対策（認証・通信暗号化）を実装します。

---

## ✅ Step 1: SASL/PLAIN 認証の導入

### 🔹 目的

Kafka ブローカーへの接続元を識別し、未認証のアクセスを排除する。

### 🔹 構成イメージ

- Kafka Broker：`listeners=SASL_PLAINTEXT://:9093`
- 認証方式：`SASL/PLAIN`
- ID/Passwordベースの認証制御

### 🔹 Broker 設定例（server.properties）

```
listeners=SASL_PLAINTEXT://0.0.0.0:9093
security.inter.broker.protocol=SASL_PLAINTEXT
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN
```

### 🔹 ユーザ定義例

`jaas.conf` ファイル（環境変数またはマウントで指定）

```
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="admin-secret"
  user.admin="admin-secret"
  user.app="app-password";
};
```

### 🔹 クライアント設定（C#）

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "kafka:9093",
    SecurityProtocol = SecurityProtocol.SaslPlaintext,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = "app",
    SaslPassword = "app-password"
};
```

---

## ✅ Step 2: 通信の暗号化（TLS）

### 🔹 目的

Kafka クライアントとブローカー間の通信を TLS で保護し、盗聴や改ざんを防止。

### 🔹 Broker 設定例（server.properties）

```
listeners=SASL_SSL://0.0.0.0:9094
ssl.keystore.location=/etc/kafka/secrets/kafka.server.keystore.jks
ssl.keystore.password=keystore-pass
ssl.key.password=key-pass
ssl.truststore.location=/etc/kafka/secrets/kafka.server.truststore.jks
ssl.truststore.password=truststore-pass
```

### 🔹 クライアント設定（C#）

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "kafka:9094",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = "app",
    SaslPassword = "app-password",
    SslCaLocation = "/etc/ssl/certs/ca-cert.pem"
};
```

---

## ✅ Kubernetesでの導入方法（簡易）

- `Secret` に TLS 証明書と `jaas.conf` を登録
- Kafka Deployment にマウントし、`KAFKA_OPTS` で参照
- `Service` は `9093`（SASL_PLAINTEXT）または `9094`（SASL_SSL）ポートを公開

---

## ✅ 注意点

- TLS 証明書は Let's Encrypt or 自己署名で発行可
- クライアントが証明書を検証する場合、ルートCAの共有が必要
- 認証導入後は ACL または DLQ の運用方針も検討を推奨

---

## ✅ 結論

Step1, Step2 により、安全な接続（認証＋暗号化）が実現可能。

- 認証：SASL/PLAIN により ID/Password 認証を導入
- 暗号化：TLS により通信を安全化
- Kubernetes 環境下でも ConfigMap / Secret を利用することで柔軟に展開可能
