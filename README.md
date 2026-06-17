# KafkaSpringBootDOC
Kafka Spring Boot Documentation
#STATELESS PROCESSING 
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/025f3274-be16-493f-bf6c-4c061180fab3" />

#STATEFULLL PROCESSSING
<img width="1693" height="929" alt="image" src="https://github.com/user-attachments/assets/3ebc975a-b974-490c-b9fa-3468d1080961" />
## Stateful vs Stateless Kafka Streams Topologies

| Aspect | Stateful (`UserCompanyAggregationTopology`) | Stateless (`UserProcessingTopology`) |
|----------|---------------------------------------------|--------------------------------------|
| **State Stores** | Yes – RocksDB (`company-stats-store`) | No |
| **Memory Usage** | Higher (stores per-key aggregates) | Minimal (no retained state) |
| **Operations** | `groupByKey`, `aggregate` | `filter`, `branch` |
| **Output Type** | `KTable` (changelog stream) | `KStream` (branched to multiple topics) |
| **Use Case** | Compute rolling statistics per company | Route and filter events without aggregation |
| **Exactly-Once Processing** | Requires state store recovery | Simpler, no recovery needed |
| **Scalability** | Scales with number of keys (companies) | Scales with throughput (no state) |
| **Example Output** | Company statistics (headcount, average salary, active/inactive counts) | Filtered users partitioned by country |

#STREAM A TO STREAM B JOIN 

#INNER JOIN
<img width="1693" height="929" alt="image" src="https://github.com/user-attachments/assets/47272426-8b6a-4703-a24b-1a4574613a6d" />

#LEFT JOIN 
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/d6a19f10-919a-40aa-9f00-1d1a0c7f2977" />


#OUTER JOIN 
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/798b2707-b173-408e-b25a-d8b5193b3fab" />

Given:
Stream A keys: 1, 2, 4  
Stream B keys: 1, 2, 5  

| Join Type | Output keys | Explanation |
|-----------|-------------|-------------|
| Inner     | 1, 2        | Keys present in both A and B. 4 and 5 are dropped. |
| Left      | 1, 2, 4     | All A keys. 4 has no match → right side = null. |
| Outer     | 1, 2, 4, 5  | All keys from A and B. 4 → right null; 5 → left null. |.

#ASYMETRIC INNER JOIN 
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/33b3d30b-19cf-4eb5-b72c-75d57f83c743" />


#WINDOWED INNER JOIN WITH GRACE 
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/9038b30e-bb58-43ec-a6bb-aef872c31820" />


#KSTEAM TO KTABLE JOINS 
#INNER JOIN 
<img width="1693" height="929" alt="image" src="https://github.com/user-attachments/assets/9bd7f5a9-7cee-4a02-bc45-03fd8c339b00" />

#LEFT JOIN 
<img width="1693" height="929" alt="image" src="https://github.com/user-attachments/assets/6d71cb16-f1ed-41a6-91ed-da484aa01bf7" />

#VERSIONED JOIN
<img width="1692" height="930" alt="image" src="https://github.com/user-attachments/assets/1bf2040e-faad-4157-ba40-4cf50da31a83" />


#FOREIGN KEY JOIN KTABLE TO KTABLE 
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/49632caa-65af-46af-8e55-640fa583f521" />


#KTABLE AND GLOBALKTABLE INNER JOIN 
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/9c0fbd40-d253-467c-9981-c0568c36f476" />


#KTABLE AND GLOBALKTABLE LEFT JOIN 
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/79e133f0-77ef-4118-ada7-ba97088bb392" />


# Kafka Streams: KStream vs KTable vs GlobalKTable

> A practical comparison for building stream processing topologies with Kafka Streams.

---

## Overview

| Feature | KStream | KTable | GlobalKTable |
|---|---|---|---|
| **Mental Model** | Append-only event log | Changelog — latest value per key wins | Fully replicated lookup table on every instance |
| **Data Semantics** | Each record is an independent fact — same key = multiple distinct events | Each record is an upsert — same key = update, latest value wins | Same as KTable but fully replicated across all instances |
| **Best For** | Events: clicks, payments, sensor readings, transactions, logs | Entity/reference data: user profiles, account balances, product prices | Small lookup tables: currency codes, country names, feature flags |
| **When To Use** | *"Did it happen?"* — event-shaped data | *"What is it now?"* — current state | Only when lookup table is small and you want to skip co-partitioning |

---

## Join Behaviour

| Feature | KStream | KTable | GlobalKTable |
|---|---|---|---|
| **Join With** | KTable, GlobalKTable, another KStream | KStream, another KTable | KStream only |
| **Co-partitioning Required** | Yes — when joining with KTable | Yes — when joining with KStream | ❌ No — main advantage over KTable |
| **Windowed Joins** | ✅ Yes — requires `JoinWindows` | ❌ No — stateless point-in-time lookup | ❌ No — always point-in-time lookup |
| **Key Flexibility** | Must rekey before join if keys differ | Must rekey before join if keys differ | `KeyValueMapper` at join time acts as rekey — no repartition needed |
| **Join Semantics** | Inner, Left, Outer | Inner, Left | Inner, Left |

---

## State & Storage

| Feature | KStream | KTable | GlobalKTable |
|---|---|---|---|
| **Stateful?** | ❌ Stateless (unless aggregating/windowing) | ✅ Stateful — local state store per partition | ✅ Stateful — full copy on every instance |
| **Memory/Storage** | Minimal | Proportional to partition data | High — entire table replicated to every instance |
| **Scalability** | Scales with partitions | Scales with partitions | ❌ Does not scale — full copy regardless of instance count |
| **State Store Type** | None (pure stream) | Local RocksDB per partition | Global RocksDB — loaded from all partitions |

---

## Fault Tolerance & Startup

| Feature | KStream | KTable | GlobalKTable |
|---|---|---|---|
| **Fault Tolerance** | Stateless — no recovery needed | Replayed from internal changelog topic on crash | Replayed from source topic on crash |
| **Startup Behaviour** | Immediate processing | Restores state store from changelog | ⚠️ Loads all partitions before processing starts — slower startup |
| **Changelog Topic** | None | Auto-created internal changelog | Uses source topic directly |

---

## Tombstone (null value) Handling

| Feature | KStream | KTable | GlobalKTable |
|---|---|---|---|
| **Null Value Behaviour** | Passes through as-is — treated as an event | Deletes key from state store | Same as KTable — deletes key from state store |
| **Inner Join + Null** | Null passes through | Record suppressed — not forwarded downstream | Record suppressed — not forwarded downstream |
| **Left/Outer Join + Null** | Null passes through | Null forwarded as right-side null | Null forwarded as right-side null |

---

## Re-keying

| Feature | KStream | KTable | GlobalKTable |
|---|---|---|---|
| **Can Re-key?** | ✅ Yes — `selectKey()` or `map()` | ✅ Yes — `.toStream().selectKey().toTable()` | ❌ No — but `KeyValueMapper` at join time provides key flexibility |
| **Triggers Repartition?** | ✅ Yes — auto internal repartition topic created | ✅ Yes | ❌ No repartition needed |

---

## Quick Decision Guide

```
Is the data event-shaped (something happened)?
└── YES → KStream

Is the data entity-shaped (current state of something)?
    ├── Is the dataset large (millions of rows)?
    │   └── YES → KTable (partitioned, scalable)
    │
    └── Is the dataset small (thousands of rows)?
        └── YES → GlobalKTable (no co-partitioning, simpler joins)
```

---

## Join Compatibility Matrix

| Left \ Right | KStream | KTable | GlobalKTable |
|---|---|---|---|
| **KStream** | ✅ Stream-Stream (windowed) | ✅ Stream-Table | ✅ Stream-GlobalTable |
| **KTable** | ✅ Table-Stream | ✅ Table-Table | ❌ Not supported |
| **GlobalKTable** | ❌ Not supported | ❌ Not supported | ❌ Not supported |

---

## Code Patterns

### KStream — Event Processing
```java
KStream<String, AvroUser> userStream = builder
        .stream("user-topic", Consumed.with(Serdes.String(), userSerde));
```

### KTable — Latest State Lookup
```java
KTable<String, AvroCompany> companyTable = builder
        .table("company-topic",
                Consumed.with(Serdes.String(), companySerde),
                Materialized.as("store-company-lookup"));
```

### GlobalKTable — Replicated Lookup (No Co-partitioning)
```java
GlobalKTable<String, AvroCompany> globalCompany = builder
        .globalTable("global-company-topic",
                Consumed.with(Serdes.String(), companySerde),
                Materialized.as("store-global-company"));
```

### Stream-Table Join (requires co-partitioning via selectKey)
```java
userStream
        .selectKey((k, user) -> user.getCompanyId())   // rekey to match company key
        .join(companyTable,
              (user, company) -> user.getName() + " works at " + company.getName());
```

### Stream-GlobalTable Join (no co-partitioning needed)
```java
userStream
        .join(globalCompany,
              (userId, user) -> user.getCompanyId(),   // KeyValueMapper — lookup key
              (user, company) -> user.getName() + " works at " + company.getName());
```

---

```mermaid
flowchart LR
    subgraph KStream[🔵 KStream – Event Log]
        direction TB
        KS1[("Source Topic<br>(append‑only)")]
        KS2[KStream<br>Stateless]
        KS3[Filter / Map / Branch]
        KS4[("Output Topic")]
        KS1 --> KS2 --> KS3 --> KS4
        KS5[<b>Technical Details</b><br>✅ Scales with partitions<br>✅ No state = low memory<br>❌ No fault‑tolerance for results<br>❌ Joins require co‑partitioning<br>❌ Tombstones pass through]
        KS5 -.-> KS2
    end

    subgraph KTable[🟢 KTable – Changelog]
        direction TB
        KT1[("Changelog Topic<br>(upsert)")]
        KT2[KTable<br>Stateful]
        KT3[State Store<br>RocksDB]
        KT4[Latest value per key]
        KT5[("Output Topic")]
        KT1 --> KT2 --> KT3 --> KT4 --> KT5
        KT6[<b>Technical Details</b><br>✅ Scales with partitions<br>✅ Fault‑tolerant (changelog replay)<br>❌ Joins require co‑partitioning<br>❌ filter/mapValues skip tombstones<br>❌ Startup replays state]
        KT6 -.-> KT2
    end

    subgraph GlobalKTable[🔴 GlobalKTable – Broadcast]
        direction TB
        GT1[("Lookup Topic")]
        GT2[GlobalKTable<br>Replicated]
        GT3[Full copy<br>per instance]
        GT4[Local lookup<br>by any key]
        GT5[("Local Output")]
        GT1 --> GT2 --> GT3 --> GT4 --> GT5
        GT6[<b>Technical Details</b><br>✅ NO co‑partition required<br>✅ Fast local lookups<br>❌ Memory = N × table_size<br>❌ Does NOT scale horizontally<br>❌ Slow startup (loads all data)<br>❌ Cannot re‑key – use mapper]
        GT6 -.-> GT2
    end

    Decision[<b>🎯 Decision Rule</b><br>“Did it happen?” → KStream<br>“What is it now?” → KTable<br>“Small lookup & skip partition?” → GlobalKTable]
    KStream ~~~ Decision ~~~ KTable ~~~ Decision ~~~ GlobalKTable
```

## Key Rules to Remember

- **Same topic cannot be registered as both KTable and GlobalKTable** in the same topology
- **Same topic cannot be registered as both KStream and KTable** in the same topology
- **Enable topology optimization** to merge duplicate KStream source registrations:
  ```properties
  spring.kafka.streams.properties.topology.optimization=all
  ```
- **GlobalKTable loads all data at startup** — keep it small or face slow restarts
- **Co-partitioning** = same number of partitions + same partitioning strategy + same key type
- **KTable FK join** handles key mismatch internally — no manual rekey needed:
  ```java
  userTable.join(companyTable, user -> user.getCompanyId(), ...)
  ```

