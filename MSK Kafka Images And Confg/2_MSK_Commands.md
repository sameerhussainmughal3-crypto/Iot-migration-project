# AWS MSK (Managed Kafka) Setup Notes

Amazon MSK is AWS's managed Apache Kafka service — it runs and maintains the Kafka cluster for us, so we don't have to install/manage Kafka brokers ourselves. This part of the setup was done through the **AWS Console** (not command-line), so this file documents what was configured and why, plus the one command that connects to it.

---

## 1. What We Set Up in the AWS Console

- **Created an MSK Cluster** (`iot-cluster-pro`, later recreated as `iot-cluster-02`) using "Quick Create" — a fast setup option that provisions a working Kafka cluster with sensible defaults.
- **Cluster type:** Provisioned, 3 brokers (for high availability — if one broker fails, the others keep working).
- **Apache Kafka version:** 3.9.x

---

## 2. Authentication Setup (SASL/SCRAM)

By default, a Quick Create MSK cluster does **not** enable SASL/SCRAM (username/password) authentication — it only enables IAM authentication. Since our Kafka client tools were configured for SASL/SCRAM, we had to manually enable it:

1. Went to the cluster → **Properties** tab → **Security settings** → **Edit**
2. Checked the box for **SASL/SCRAM authentication**
3. Saved — this update took **30–45 minutes** to apply (MSK security changes require a cluster update, which is slow)
4. After the update completed, linked our existing **AWS Secrets Manager** secret (`AmazonMSK_iot_secret`) to the cluster, so MSK knows which username/password pairs are valid

> **Lesson learned:** If starting a new MSK cluster, enable SASL/SCRAM authentication *during* cluster creation (not after) to avoid this 30-45 minute wait later.

---

## 3. Secrets Manager & KMS (Credential Storage)

- **AWS Secrets Manager**: Stores the Kafka username/password (`iotuser` / `Aircluster@002`) securely instead of hardcoding it everywhere. Secret name: `AmazonMSK_iot_secret`.
- **AWS KMS (Key Management Service)**: Provides the encryption key (`msk-scram-key`) used to encrypt that secret at rest.

---

## 4. Getting the Bootstrap Broker URLs

Bootstrap servers are the entry-point addresses your Kafka client tools use to connect to the cluster. These are found via:

**AWS Console → MSK → Clusters → (your cluster) → View client information**

Under **SASL/SCRAM**, the broker string looks like:

```
boot-z2e.iotclusterpro.16w0x0.c24.kafka.us-east-1.amazonaws.com:9096,
boot-i1n.iotclusterpro.16w0x0.c24.kafka.us-east-1.amazonaws.com:9096,
boot-lhe.iotclusterpro.16w0x0.c24.kafka.us-east-1.amazonaws.com:9096
```

> **Important:** These URLs are unique to each cluster and **change every time you delete and recreate a cluster**. Any time you rebuild MSK, you must re-fetch these URLs and update them in `client-scram.properties` and `connect-standalone.properties`.

---

## 5. Cost Management Note

MSK **cannot be "paused" or "stopped"** like an EC2 instance — it only has Create / Delete. If billing needs to be paused overnight (e.g., between work sessions), the only option is to **delete the cluster** and recreate it later.

Rebuilding after a delete takes about:
- **10–15 minutes** for cluster creation
- **A few minutes** to recreate the topic and update broker URLs in config files
- **30–45 minutes extra** *only if* SASL/SCRAM wasn't enabled at creation time (see note above)

---

## Bugs We Hit & Fixed (MSK Side)

| Problem | Cause | Fix |
|---|---|---|
| `Ident authentication failed for user iotuser` | This was actually a PostgreSQL-side issue triggered while testing the MSK → Kafka Connect → DB pipeline (see PostgreSQL doc) | Fixed in `pg_hba.conf`, not MSK itself |
| SASL/SCRAM showed "Not enabled" on a Quick Create cluster | Quick Create only enables IAM authentication by default | Manually enabled SASL/SCRAM via cluster security settings (took 30-45 min to apply) |
| Broker URLs stopped working after cost-saving cleanup | MSK cluster was deleted to pause billing; broker URLs are unique per cluster | Created a new cluster and updated all config files with the new broker URLs |
