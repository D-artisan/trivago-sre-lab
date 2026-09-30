# Trivago SRE Learning Guide: HotelStream Reliability Lab

## Project: "HotelStream Reliability Lab"

This is a hands-on WSL2/Ubuntu project for learning the Kubernetes, Kafka, and SRE concepts used in large-scale data platforms.

The production context behind this lab is centered on Kubernetes and Kafka, with a data backbone that includes **8 production Kafka clusters moving more than 900K messages per second**, plus managed Flink, Cassandra, and Redis services. It also emphasizes observability, distributed-systems troubleshooting, on-call response, documentation, and automation.

This lab deliberately mirrors the architecture described in trivago's engineering article **"From Always-On to On-Demand: Scaling Kafka Sinks with KEDA"**.

By the end, you will have built and broken a small hotel-data platform that lets you directly observe:

- a producer publishing hotel-price events
- the Kafka client talking to brokers
- topics, partitions, offsets, leaders, replicas, and ISR
- consumer groups and partition assignment
- consumer lag and where the information used to calculate it lives
- Kafka sinks building a service-local materialized view
- Redis acting as that local read model
- Kubernetes running the consumers
- KEDA scaling consumers from 0 to N based on Kafka lag
- why replicas beyond the partition count do not improve consumer parallelism
- why CPU can stay low while Kafka lag grows
- broker failure and leader/ISR recovery
- consumer failure and group rebalancing
- downstream failure without losing committed progress
- replaying events by resetting offsets
- the operational signals an SRE should monitor

This project is designed for **understanding**, not for blindly running commands. After each phase there is a **Learning Checkpoint**. Stop and explain the concept aloud before moving on.

---

## 1. The Real-World Mental Model

A simplified trivago-style flow is:

~~~mermaid
flowchart LR
    U[Upstream pricing service] -->|Publishes price-change events| K[Kafka cluster]
    K -->|Consumer polls topic| S[Hotel view sink]
    S -->|Idempotent upsert| R[(Redis materialized view)]
    A[Hotel API] -->|Fast local read| R

    KD[KEDA] -. Reads consumer lag .-> K
    KD -->|Desired replicas| H[Kubernetes autoscaling]
    H --> S
~~~

A real event might mean:

~~~json
{
  "event_id": "a247...",
  "hotel_id": "H0042",
  "price": 145.30,
  "currency": "EUR",
  "available": true,
  "observed_at": "2026-09-22T18:00:00Z"
}
~~~

That message is not "Kafka". It is application data.

Kafka is the distributed event-streaming infrastructure that accepts, stores, replicates, and serves that event to consumers.

### The vocabulary you should be able to explain simply

| Term | Beginner mental model |
|---|---|
| Producer | An application that sends events to Kafka |
| Kafka client library | The bridge inside the application that speaks Kafka's network protocol |
| Broker | One Kafka server |
| Kafka cluster | Multiple brokers working as one system |
| Topic | A named stream of related events |
| Partition | An ordered logical slice of a topic, physically backed by log files on broker storage |
| Record offset | A record's sequential position inside one partition |
| Partition leader | The broker that handles reads/writes for that partition replica set |
| Replica | Another broker's copy of a partition |
| ISR | Replicas currently in-sync with the leader |
| Consumer | Application that polls Kafka for records |
| Consumer group | Consumers cooperating to process a topic |
| Committed offset | The group's recorded processing position |
| Lag | How far the committed position is behind the partition's latest position |
| Sink | A consumer that transforms events and writes them somewhere else |
| Materialized view | A local, query-friendly copy derived from an event stream |
| KEDA | Kubernetes event-driven autoscaler; here it scales from Kafka lag |

### One subtle point to remember

The producer does **not** need a network address for a partition.

The flow is:

1. The producer's Kafka client connects to one or more **bootstrap brokers**.
2. The client asks the cluster for metadata.
3. Kafka tells the client which broker is the leader for each partition.
4. The client selects a partition, usually using the message key.
5. The client sends the record to that partition's leader broker.

So a partition is a logical Kafka concept, not an independently addressed server.

---

# 2. What You Will Build

~~~text
Windows
└── WSL2/Ubuntu
    └── Docker Desktop engine
        └── Kind Kubernetes cluster: trivago-data-lab
            ├── control-plane
            ├── worker
            ├── worker2
            └── worker3

Kubernetes
├── kafka namespace
│   ├── Strimzi operator
│   └── 3 Kafka broker/controller pods
│       └── topic: hotel-price-updates
│           ├── partition 0
│           ├── partition 1
│           └── partition 2
│
├── keda namespace
│   └── KEDA operator
│
└── lab namespace
    ├── producer pods
    ├── hotel-view-sink Deployment
    ├── Redis
    └── hotel-view-api
~~~

The Kafka topic uses:

- 3 partitions
- replication factor 3
- min ISR 2
- keyed hotel events
- log compaction

The consumer deployment uses:

- consumer group: hotel-view-sink
- manual offset commits after Redis writes
- idempotent Redis upserts
- KEDA scale-to-zero
- lag target: 100 records per replica
- max replicas: 3 because the topic has 3 partitions

For the lab we use shorter KEDA intervals than trivago's article so you do not wait several minutes between experiments.

---

# 3. Prerequisites

Windows + Ubuntu WSL2 + Docker. If you have a native linux setup, you're already good to go!

Recommended laptop resources:

- 4+ CPU cores available to Docker
- 8 GB RAM minimum; 12 GB is more comfortable
- 15 GB free disk
- Docker Desktop with WSL integration enabled (for windows users)

In Ubuntu/WSL:

~~~bash
docker version
kubectl version --client
kind version
helm version
python3 --version
~~~

Install small helper tools if needed:

~~~bash
sudo apt update
sudo apt install -y curl jq git make python3 python3-pip redis-tools
~~~

Create the project:

~~~bash
mkdir -p ~/trivago-data-lab/{app,k8s,notes}
cd ~/trivago-data-lab
~~~

---

# Phase 1: Build the Local Kubernetes Environment

## Goal

Understand that Kubernetes is the runtime for our applications, while Kafka itself is a distributed application running inside that runtime.

Create the Kind configuration:

~~~bash
cat > kind-config.yaml << 'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
  - role: worker
EOF
~~~

Create the cluster:

~~~bash
kind create cluster --name trivago-data-lab --config kind-config.yaml

kubectl cluster-info --context kind-trivago-data-lab
kubectl get nodes -o wide
~~~

Create namespaces:

~~~bash
kubectl create namespace kafka
kubectl create namespace lab
~~~

## Install metrics-server

This lets you compare CPU usage against Kafka lag later.

~~~bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

kubectl patch deployment metrics-server -n kube-system   --type='json'   -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

kubectl rollout status deployment/metrics-server -n kube-system --timeout=180s
kubectl top nodes
~~~

If `kubectl top`, ClusterIP access, or in-cluster DNS unexpectedly fails in Kind/WSL, first check the system networking pods:

~~~bash
kubectl get pods -n kube-system -o wide | grep -E 'kube-proxy|coredns|kindnet'
~~~

If `kube-proxy` is in `CrashLoopBackOff`, inspect the previous container logs:

~~~bash
kubectl logs -n kube-system <kube-proxy-pod> --previous --tail=100
~~~

For the local WSL/Kind failure seen in this lab, `too many open files` was caused by exhausted inotify resources. Increase the host limits, persist them, then recreate the affected system pods:

~~~bash
sudo sysctl -w fs.inotify.max_user_watches=524288
sudo sysctl -w fs.inotify.max_user_instances=512

sudo tee /etc/sysctl.d/99-kind-inotify.conf > /dev/null <<'EOF'
fs.inotify.max_user_watches=524288
fs.inotify.max_user_instances=512
EOF

sudo sysctl --system

kubectl delete pod -n kube-system -l k8s-app=kube-proxy
kubectl rollout restart deployment/coredns -n kube-system
~~~

Verify every `kube-proxy`, `coredns`, and `kindnet` pod is healthy before continuing. A useful diagnostic distinction is:

~~~text
direct Pod IP works + ClusterIP fails
    -> Kubernetes Service routing / kube-proxy

DNS name fails + ClusterIP fails
    -> often the same Service-routing problem also affecting CoreDNS
~~~

### Learning Checkpoint

Explain aloud:

1. What is the difference between Docker, Kind, and Kubernetes?
2. Why would trivago run Kafka-related workloads on GKE/Rancher instead of directly on random VMs?
3. What does Kubernetes manage here, and what does Kafka manage?

Expected mental model:

- Kubernetes manages containers, scheduling, health, rollout, networking, and scaling.
- Kafka manages event storage, partition leadership, replication, consumer metadata, and streaming semantics.
- Kind is only our local Kubernetes implementation.

---

# Phase 2: Deploy a Real 3-Broker Kafka Cluster

We use **Strimzi**, a Kubernetes operator for Apache Kafka.

Install the current Strimzi operator:

~~~bash
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

kubectl rollout status deployment/strimzi-cluster-operator   -n kafka   --timeout=300s
~~~

Create the Kafka cluster definition:

~~~bash
cat > k8s/kafka.yaml << 'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: dual-role
  namespace: kafka
  labels:
    strimzi.io/cluster: hotel-kafka
spec:
  replicas: 3
  roles:
    - controller
    - broker
  storage:
    type: jbod
    volumes:
      - id: 0
        type: ephemeral
        kraftMetadata: shared
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: hotel-kafka
  namespace: kafka
spec:
  kafka:
    version: 4.3.1
    metadataVersion: 4.3-IV0
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    config:
      default.replication.factor: 3
      min.insync.replicas: 2
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
  entityOperator:
    topicOperator: {}
    userOperator: {}
EOF

kubectl apply -f k8s/kafka.yaml
kubectl get pods -n kafka -w
~~~

When the three Kafka pods are running, create the topic:

~~~bash
cat > k8s/topic.yaml << 'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: hotel-price-updates
  namespace: kafka
  labels:
    strimzi.io/cluster: hotel-kafka
spec:
  partitions: 3
  replicas: 3
  config:
    cleanup.policy: compact
    min.insync.replicas: 2
EOF

kubectl apply -f k8s/topic.yaml
kubectl get kafkatopic -n kafka
~~~

Find one broker pod:

~~~bash
kubectl get pods -n kafka -o wide

export KAFKA_POD=$(kubectl get pods -n kafka -o name   | grep 'hotel-kafka-dual-role'   | head -1   | cut -d/ -f2)

echo "$KAFKA_POD"
~~~

Before treating `Running` Kafka pods as a healthy cluster, check the KRaft controller quorum:

~~~bash
kubectl exec -n kafka "$KAFKA_POD" -- \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server hotel-kafka-kafka-bootstrap:9092 \
  describe --status
~~~

A healthy three-controller lab should show one stable `LeaderId`, voters `0,1,2`, and followers that are caught up (for example `MaxFollowerLag: 0`). Repeated controller elections or `leader is (none)` in broker logs indicate control-plane instability even when the pods themselves say `Running`.

Describe the topic:

~~~bash
kubectl exec -n kafka "$KAFKA_POD" --   /opt/kafka/bin/kafka-topics.sh   --bootstrap-server hotel-kafka-kafka-bootstrap:9092   --describe   --topic hotel-price-updates
~~~

Study these fields:

- Partition
- Leader
- Replicas
- Isr

Do not continue until you can explain each one.

## What you just created

~~~text
Topic: hotel-price-updates

Partition 0
  Leader: Broker A
  Replicas: A, B, C

Partition 1
  Leader: Broker B
  Replicas: B, C, A

Partition 2
  Leader: Broker C
  Replicas: C, A, B
~~~

The exact broker IDs can differ.

The important idea is that a topic is not one giant file copied everywhere. Kafka splits it into partitions and distributes replicated copies across brokers.

### Learning Checkpoint

Explain:

> A Kafka broker is one server. A cluster contains multiple brokers. A topic is logically split into partitions. Each partition has a leader and replicas on brokers. Producers write to the partition leader; followers replicate it. Replication is the fault-tolerance side of Kafka's distributed design.

---

# Phase 3: See the Physical Side of a Logical Partition

A partition is **logical in Kafka's data model**, but Kafka stores it as real log-segment files on broker storage.

Run:

~~~bash
for pod in $(kubectl get pods -n kafka -o name | grep hotel-kafka-dual-role | cut -d/ -f2); do
  echo
  echo "===== $pod ====="
  kubectl exec -n kafka "$pod" -- bash -lc     "find /var/lib/kafka -maxdepth 8 -type d -name 'hotel-price-updates-*' 2>/dev/null || true"
done
~~~

Once the topic has data later, repeat this and inspect a partition directory:

~~~bash
kubectl exec -n kafka "$KAFKA_POD" -- bash -lc   "find /var/lib/kafka -maxdepth 8 -type f 2>/dev/null | grep 'hotel-price-updates' | head -20"
~~~

You should eventually see Kafka files such as log segments and indexes.

Conceptually:

~~~text
Logical Kafka model

Topic
└── Partition 0

Physical broker storage

.../hotel-price-updates-0/
├── 00000000000000000000.log
├── 00000000000000000000.index
└── 00000000000000000000.timeindex
~~~

### Learning Checkpoint

Answer:

**Are Kafka partitions physical or logical?**

A strong answer:

> A partition is a logical unit of a Kafka topic, but each partition replica is physically persisted as append-only log segment files on broker storage.

---

# Phase 4: Build the Producer, Sink, and Read API

## 4.1 Python dependencies

~~~bash
cd ~/trivago-data-lab/app

cat > requirements.txt << 'EOF'
confluent-kafka>=2.6,<3
redis>=5,<7
fastapi>=0.115,<1
uvicorn>=0.34,<1
prometheus-client>=0.21,<1
EOF
~~~

## 4.2 Producer

~~~bash
cat > producer.py << 'EOF'
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "hotel-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
TOPIC = os.getenv("KAFKA_TOPIC", "hotel-price-updates")
MESSAGE_COUNT = int(os.getenv("MESSAGE_COUNT", "100"))
RATE_PER_SECOND = float(os.getenv("RATE_PER_SECOND", "100"))
HOTEL_COUNT = int(os.getenv("HOTEL_COUNT", "25"))

producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
    }
)

failures = 0


def delivery_report(err, msg):
    global failures
    if err is not None:
        failures += 1
        print(f"delivery failed: {err}", flush=True)


for i in range(MESSAGE_COUNT):
    hotel_id = f"H{(i % HOTEL_COUNT) + 1:04d}"

    event = {
        "event_id": str(uuid.uuid4()),
        "hotel_id": hotel_id,
        "price": round(random.uniform(70, 450), 2),
        "currency": "EUR",
        "available": random.random() > 0.05,
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }

    producer.produce(
        TOPIC,
        key=hotel_id.encode(),
        value=json.dumps(event).encode(),
        callback=delivery_report,
    )
    producer.poll(0)

    if RATE_PER_SECOND > 0:
        time.sleep(1 / RATE_PER_SECOND)

producer.flush()

print(
    f"produced={MESSAGE_COUNT} failures={failures} topic={TOPIC}",
    flush=True,
)
EOF
~~~

Notice the message **key** is the hotel ID.

Kafka's partitioner will consistently map the same hotel ID to the same partition. That gives ordering for updates for one hotel.

## 4.3 Sink consumer

~~~bash
cat > sink.py << 'EOF'
import json
import os
import signal
import time

import redis
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Histogram, start_http_server

BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "hotel-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
TOPIC = os.getenv("KAFKA_TOPIC", "hotel-price-updates")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "hotel-view-sink")
REDIS_HOST = os.getenv("REDIS_HOST", "redis.lab.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PROCESSING_DELAY = float(os.getenv("PROCESSING_DELAY", "0.02"))

processed = Counter(
    "hotel_sink_processed_total",
    "Successfully processed Kafka records",
)
errors = Counter(
    "hotel_sink_errors_total",
    "Failed processing attempts",
)
latency = Histogram(
    "hotel_sink_processing_seconds",
    "Sink processing latency",
)

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

start_http_server(8000)

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)

consumer.subscribe([TOPIC])

print(
    f"consumer started group={GROUP_ID} topic={TOPIC}",
    flush=True,
)

try:
    while running:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"kafka error: {msg.error()}", flush=True)
                errors.inc()
            continue

        try:
            started = time.time()
            event = json.loads(msg.value().decode())

            time.sleep(PROCESSING_DELAY)

            redis_key = f"hotel:{event['hotel_id']}"

            r.hset(
                redis_key,
                mapping={
                    "event_id": event["event_id"],
                    "price": event["price"],
                    "currency": event["currency"],
                    "available": str(event["available"]).lower(),
                    "observed_at": event["observed_at"],
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )

            # Commit only AFTER the materialized view was updated.
            consumer.commit(message=msg, asynchronous=False)

            processed.inc()
            latency.observe(time.time() - started)

            print(
                f"processed hotel={event['hotel_id']} "
                f"partition={msg.partition()} offset={msg.offset()}",
                flush=True,
            )

        except Exception as exc:
            # Do not commit the offset if downstream processing failed.
            # A restart/rebalance can therefore replay uncommitted work.
            errors.inc()
            print(f"processing failed: {exc}", flush=True)
            time.sleep(1)

finally:
    consumer.close()
EOF
~~~

The crucial reliability rule is:

~~~text
consume event
    ↓
update Redis
    ↓
commit Kafka offset
~~~

Do not commit first and then write Redis.

If Redis fails after an early commit, Kafka would think the event had already been processed.

## 4.4 API

~~~bash
cat > api.py << 'EOF'
import os

import redis
from fastapi import FastAPI, HTTPException

REDIS_HOST = os.getenv("REDIS_HOST", "redis.lab.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

app = FastAPI(title="Hotel Materialized View API")


@app.get("/health")
def health():
    r.ping()
    return {"status": "ok"}


@app.get("/hotels/{hotel_id}")
def get_hotel(hotel_id: str):
    data = r.hgetall(f"hotel:{hotel_id}")
    if not data:
        raise HTTPException(status_code=404, detail="hotel not found")
    return {"hotel_id": hotel_id, **data}
EOF
~~~

## 4.5 Container image

~~~bash
cat > Dockerfile << 'EOF'
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py sink.py api.py ./

CMD ["python", "sink.py"]
EOF

docker build -t hotel-stream-lab:0.1 .
kind load docker-image hotel-stream-lab:0.1 --name trivago-data-lab
~~~

---

# Phase 5: Deploy Redis, Sink, and API

Create the application manifests:

~~~bash
cd ~/trivago-data-lab

cat > k8s/apps.yaml << 'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  namespace: lab
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:7-alpine
          ports:
            - containerPort: 6379
---
apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: lab
spec:
  selector:
    app: redis
  ports:
    - port: 6379
      targetPort: 6379
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hotel-view-sink
  namespace: lab
spec:
  replicas: 1
  selector:
    matchLabels:
      app: hotel-view-sink
  template:
    metadata:
      labels:
        app: hotel-view-sink
    spec:
      containers:
        - name: sink
          image: hotel-stream-lab:0.1
          imagePullPolicy: IfNotPresent
          command: ["python", "sink.py"]
          env:
            - name: KAFKA_BOOTSTRAP
              value: hotel-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092
            - name: KAFKA_TOPIC
              value: hotel-price-updates
            - name: KAFKA_GROUP_ID
              value: hotel-view-sink
            - name: REDIS_HOST
              value: redis.lab.svc.cluster.local
            - name: REDIS_PORT
              value: "6379"
            - name: PROCESSING_DELAY
              value: "0.02"
          ports:
            - name: metrics
              containerPort: 8000
          resources:
            requests:
              cpu: 25m
              memory: 64Mi
            limits:
              cpu: 500m
              memory: 256Mi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hotel-view-api
  namespace: lab
spec:
  replicas: 1
  selector:
    matchLabels:
      app: hotel-view-api
  template:
    metadata:
      labels:
        app: hotel-view-api
    spec:
      containers:
        - name: api
          image: hotel-stream-lab:0.1
          imagePullPolicy: IfNotPresent
          command:
            - uvicorn
            - api:app
            - --host
            - "0.0.0.0"
            - --port
            - "8080"
          env:
            - name: REDIS_HOST
              value: redis.lab.svc.cluster.local
            - name: REDIS_PORT
              value: "6379"
          ports:
            - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: hotel-view-api
  namespace: lab
spec:
  selector:
    app: hotel-view-api
  ports:
    - port: 8080
      targetPort: 8080
EOF

kubectl apply -f k8s/apps.yaml
kubectl get pods -n lab -w
~~~

Set `REDIS_PORT` explicitly in both application Deployments. Kubernetes also injects Service environment variables, and without the explicit value an application can receive a value such as `REDIS_PORT=tcp://<cluster-ip>:6379` instead of the integer port expected by the Python Redis client.

Check sink logs:

~~~bash
kubectl logs -n lab deployment/hotel-view-sink -f
~~~

Stop with Ctrl+C.

---

# Phase 6: Publish Your First Real Events

Run a producer pod:

~~~bash
kubectl delete pod hotel-burst -n lab --ignore-not-found

kubectl run hotel-burst   -n lab   --restart=Never   --image=hotel-stream-lab:0.1   --image-pull-policy=IfNotPresent   --env=KAFKA_BOOTSTRAP=hotel-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092   --env=KAFKA_TOPIC=hotel-price-updates   --env=MESSAGE_COUNT=200   --env=RATE_PER_SECOND=100   --command -- python producer.py

kubectl wait -n lab --for=condition=Ready pod/hotel-burst --timeout=60s
kubectl logs -n lab -f pod/hotel-burst
~~~

Waiting first avoids a misleading `ContainerCreating` error from asking for logs before the container exists.

Because the producer has idempotence enabled, you may briefly see:

~~~text
Failed to acquire idempotence PID ... Coordinator load in progress: retrying
~~~

If the run ends with `produced=200 failures=0`, the retry was transient. If it continues for a long time, inspect Kafka's KRaft quorum health before retrying the producer.

Inspect Redis:

~~~bash
kubectl exec -n lab deployment/redis --   redis-cli HGETALL hotel:H0001
~~~

Test the API in a second terminal:

~~~bash
kubectl port-forward -n lab svc/hotel-view-api 8888:8080
~~~

Then:

~~~bash
curl http://localhost:8888/health
curl http://localhost:8888/hotels/H0001 | jq
~~~

You have now implemented the core trivago article pattern:

~~~text
upstream events
      ↓
Kafka
      ↓
sink consumer
      ↓
service-local materialized view
      ↓
API
~~~

### Learning Checkpoint

Explain why this architecture can be better than the API calling the upstream pricing service on every request.

Key ideas:

- low-latency local reads
- less coupling between services
- upstream outages do not immediately break reads
- Kafka gives replay/rebuild capability
- data becomes eventually consistent rather than synchronously coupled

---

# Phase 7: Understand Offsets and Consumer Lag

Find a broker pod again:

~~~bash
export KAFKA_POD=$(kubectl get pods -n kafka -o name   | grep 'hotel-kafka-dual-role'   | head -1   | cut -d/ -f2)
~~~

Inspect the consumer group:

~~~bash
kubectl exec -n kafka "$KAFKA_POD" --   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server hotel-kafka-kafka-bootstrap:9092   --describe   --group hotel-view-sink
~~~

You will see columns similar to:

~~~text
TOPIC                 PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
hotel-price-updates   0          68              68              0
hotel-price-updates   1          71              71              0
hotel-price-updates   2          61              61              0
~~~

Meaning:

- **CURRENT-OFFSET** = committed consumer-group position
- **LOG-END-OFFSET** = latest position in the partition
- **LAG** = work still waiting

Simplified:

~~~text
lag = log-end-offset - committed-offset
~~~

## Create lag deliberately

Stop the consumer:

~~~bash
kubectl scale deployment hotel-view-sink -n lab --replicas=0
~~~

Produce 1000 records:

~~~bash
kubectl delete pod hotel-burst -n lab --ignore-not-found

kubectl run hotel-burst   -n lab   --restart=Never   --image=hotel-stream-lab:0.1   --image-pull-policy=IfNotPresent   --env=KAFKA_BOOTSTRAP=hotel-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092   --env=MESSAGE_COUNT=1000   --env=RATE_PER_SECOND=500   --command -- python producer.py

kubectl wait -n lab   --for=jsonpath='{.status.phase}'=Succeeded   pod/hotel-burst   --timeout=180s
~~~

Now inspect the group again:

~~~bash
kubectl exec -n kafka "$KAFKA_POD" --   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server hotel-kafka-kafka-bootstrap:9092   --describe   --group hotel-view-sink
~~~

Lag should now be significant.

## Where are committed offsets stored?

List internal topics:

~~~bash
kubectl exec -n kafka "$KAFKA_POD" --   /opt/kafka/bin/kafka-topics.sh   --bootstrap-server hotel-kafka-kafka-bootstrap:9092   --list | grep consumer
~~~

You should see:

~~~text
__consumer_offsets
~~~

Kafka stores committed group offsets in this internal compacted topic.

**Kafka does not store "lag" as a separate backlog counter.**

Lag is derived by comparing:

~~~text
partition log end
minus
consumer group's committed position
~~~

### Learning Checkpoint

A strong concise answer:

> Kafka stores the records themselves in topic partitions and stores consumer-group committed offsets in the internal __consumer_offsets topic. Consumer lag is calculated from the difference between the partition's log-end offset and the group's committed offset.

---

# Phase 8: Consumer Groups and Why Partitions Limit Parallelism

Bring up one sink:

~~~bash
kubectl scale deployment hotel-view-sink -n lab --replicas=1
kubectl rollout status deployment/hotel-view-sink -n lab
~~~

Inspect assignments:

~~~bash
kubectl exec -n kafka "$KAFKA_POD" --   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server hotel-kafka-kafka-bootstrap:9092   --describe   --group hotel-view-sink
~~~

With one consumer, that consumer can own all three partitions.

Now scale to three:

~~~bash
kubectl scale deployment hotel-view-sink -n lab --replicas=3
kubectl get pods -n lab -l app=hotel-view-sink -o wide
~~~

Inspect again.

You should see work distributed across members of the same group.

Now scale to five:

~~~bash
kubectl scale deployment hotel-view-sink -n lab --replicas=5
kubectl get pods -n lab -l app=hotel-view-sink
~~~

The topic still has only **3 partitions**.

Therefore only three consumers can actively own partitions. Extra consumers remain idle from Kafka's perspective.

This is why trivago's article stresses:

~~~text
useful consumer replicas <= partition count
~~~

Restore three:

~~~bash
kubectl scale deployment hotel-view-sink -n lab --replicas=3
~~~

### Learning Checkpoint

Explain:

> Within one consumer group, a partition is assigned to only one consumer at a time. A consumer can own multiple partitions, but one partition cannot be processed concurrently by two consumers in the same group. Therefore a three-partition topic has at most three useful parallel consumers in that group.

---

# Phase 9: Install KEDA and Scale from Kafka Lag

Install KEDA:

~~~bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update

helm upgrade --install keda kedacore/keda   --namespace keda   --create-namespace

kubectl get pods -n keda
~~~

Create the ScaledObject:

~~~bash
cat > k8s/keda.yaml << 'EOF'
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: hotel-view-sink
  namespace: lab
spec:
  scaleTargetRef:
    name: hotel-view-sink

  pollingInterval: 5
  cooldownPeriod: 30

  minReplicaCount: 0
  maxReplicaCount: 3

  fallback:
    failureThreshold: 3
    replicas: 1

  triggers:
    - type: kafka
      metadata:
        bootstrapServers: hotel-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092
        consumerGroup: hotel-view-sink
        topic: hotel-price-updates
        lagThreshold: "100"
        activationLagThreshold: "0"
        offsetResetPolicy: earliest
        allowIdleConsumers: "false"
EOF

kubectl apply -f k8s/keda.yaml

kubectl get scaledobject,hpa -n lab
~~~

The lab uses:

~~~text
activationLagThreshold = 0
lagThreshold           = 100
min replicas           = 0
max replicas           = 3
~~~

Meaning:

- lag = 0: workload can remain at 0
- lag > 0: wake the sink
- roughly 100 lag is the target amount of backlog per desired replica
- scaling cannot exceed 3 because there are only 3 partitions

The 100 value is **not a hard capacity limit** for one sink. One sink can process far more than 100 records. It is an autoscaling target.

## Watch KEDA scale

First allow the existing backlog to drain.

~~~bash
watch -n 2 "kubectl get deploy,scaledobject,hpa -n lab"
~~~

In another terminal:

~~~bash
export KAFKA_POD=$(kubectl get pods -n kafka -o name   | grep 'hotel-kafka-dual-role'   | head -1   | cut -d/ -f2)

watch -n 2 "kubectl exec -n kafka $KAFKA_POD --   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server hotel-kafka-kafka-bootstrap:9092   --describe   --group hotel-view-sink 2>/dev/null"
~~~

Wait until lag reaches zero and KEDA eventually scales the deployment to zero.

Then produce a burst:

~~~bash
kubectl delete pod hotel-burst -n lab --ignore-not-found

kubectl run hotel-burst   -n lab   --restart=Never   --image=hotel-stream-lab:0.1   --image-pull-policy=IfNotPresent   --env=KAFKA_BOOTSTRAP=hotel-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092   --env=MESSAGE_COUNT=1500   --env=RATE_PER_SECOND=1000   --command -- python producer.py
~~~

Observe:

~~~text
new records arrive
      ↓
consumer lag appears
      ↓
KEDA reads lag
      ↓
Kubernetes raises sink replica count
      ↓
consumers drain partitions
      ↓
lag falls
      ↓
cooldown expires
      ↓
replicas return to 0
~~~

### Compare to the trivago article

The article's simplified example uses:

- activationLagThreshold: 0
- lagThreshold: 100
- pollingInterval: 30 seconds
- cooldownPeriod: 300 seconds
- minReplicaCount: 0
- maxReplicaCount: 1 for that particular example

We use shorter polling/cooldown values and max 3 to make the behavior easy to observe locally.

### Learning Checkpoint

Be able to answer:

**Why scale from lag rather than CPU?**

> Lag directly measures how much Kafka work is waiting. CPU is only an indirect resource signal. A sink can be blocked on database or network I/O, have low CPU usage, and still be falling further behind.

---

# Phase 10: Prove That CPU Can Stay Low While Lag Grows

This recreates one of the main operational lessons in the trivago article.

Make the sink artificially slow by simulating a slow downstream call:

~~~bash
kubectl set env deployment/hotel-view-sink   -n lab   PROCESSING_DELAY=0.5
~~~

Generate a large burst:

~~~bash
kubectl delete pod hotel-burst -n lab --ignore-not-found

kubectl run hotel-burst   -n lab   --restart=Never   --image=hotel-stream-lab:0.1   --image-pull-policy=IfNotPresent   --env=KAFKA_BOOTSTRAP=hotel-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092   --env=MESSAGE_COUNT=3000   --env=RATE_PER_SECOND=1500   --command -- python producer.py
~~~

Observe CPU:

~~~bash
watch -n 2 "kubectl top pods -n lab -l app=hotel-view-sink"
~~~

Observe lag in another terminal:

~~~bash
watch -n 2 "kubectl exec -n kafka $KAFKA_POD --   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server hotel-kafka-kafka-bootstrap:9092   --describe   --group hotel-view-sink 2>/dev/null"
~~~

The consumers spend much of their time sleeping, representing I/O wait.

CPU can therefore remain unimpressive while lag is clearly high.

Restore normal speed:

~~~bash
kubectl set env deployment/hotel-view-sink   -n lab   PROCESSING_DELAY=0.02
~~~

This is exactly the kind of distinction to understand in SRE work:

~~~text
CPU answers:
"How much compute is this pod using?"

Kafka lag answers:
"How much work is waiting?"
~~~

---

# Phase 11: Incident Drill - Consumer Failure and Rebalancing

Ensure the sink has multiple replicas:

~~~bash
kubectl get pods -n lab -l app=hotel-view-sink
~~~

Choose one sink pod and delete it:

~~~bash
export SINK_POD=$(kubectl get pods -n lab   -l app=hotel-view-sink   -o jsonpath='{.items[0].metadata.name}')

kubectl delete pod -n lab "$SINK_POD"
~~~

Immediately watch the group:

~~~bash
watch -n 1 "kubectl exec -n kafka $KAFKA_POD --   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server hotel-kafka-kafka-bootstrap:9092   --describe   --group hotel-view-sink 2>/dev/null"
~~~

What is happening:

1. Kafka detects that a group member disappeared.
2. The group rebalances.
3. Partitions are reassigned.
4. The surviving/new consumer starts from committed offsets.
5. Kubernetes recreates the failed pod if the Deployment still desires it.

### Learning Checkpoint

**What happens if a Kafka consumer dies?**

Do not answer only "Kubernetes restarts it."

Kafka also has its own consumer-group behavior. The group coordinator detects membership changes and reassigns partitions.

---

# Phase 12: Incident Drill - Redis Outage

This simulates a sink whose downstream dependency becomes unavailable.

Stop Redis:

~~~bash
kubectl scale deployment redis -n lab --replicas=0
~~~

Generate another burst:

~~~bash
kubectl delete pod hotel-burst -n lab --ignore-not-found

kubectl run hotel-burst   -n lab   --restart=Never   --image=hotel-stream-lab:0.1   --image-pull-policy=IfNotPresent   --env=KAFKA_BOOTSTRAP=hotel-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092   --env=MESSAGE_COUNT=500   --env=RATE_PER_SECOND=500   --command -- python producer.py
~~~

Check sink logs:

~~~bash
kubectl logs -n lab deployment/hotel-view-sink --tail=100
~~~

Check lag.

You should see processing failures and lag that does not drain cleanly because offsets are only committed after Redis succeeds.

Restore Redis:

~~~bash
kubectl scale deployment redis -n lab --replicas=1
kubectl rollout status deployment/redis -n lab
~~~

Watch recovery.

## What this teaches

Our materialized view uses idempotent upserts.

If a record is replayed after a crash, writing the same hotel state again is safe.

This is a common practical reliability pattern:

~~~text
at-least-once delivery
+
idempotent processing
=
safe replay
~~~

### Learning Checkpoint

Reliability principle:

> Commit the Kafka offset only after the side effect succeeds. Make the side effect idempotent so replaying the same record does not corrupt state.

---

# Phase 13: Incident Drill - Kafka Broker Failure

First inspect the topic:

~~~bash
kubectl exec -n kafka "$KAFKA_POD" --   /opt/kafka/bin/kafka-topics.sh   --bootstrap-server hotel-kafka-kafka-bootstrap:9092   --describe   --topic hotel-price-updates
~~~

Pick one broker pod:

~~~bash
kubectl get pods -n kafka -o wide
~~~

Delete one:

~~~bash
kubectl delete pod -n kafka hotel-kafka-dual-role-0
~~~

Immediately describe the topic repeatedly:

~~~bash
watch -n 1 "kubectl exec -n kafka $KAFKA_POD --   /opt/kafka/bin/kafka-topics.sh   --bootstrap-server hotel-kafka-kafka-bootstrap:9092   --describe   --topic hotel-price-updates 2>/dev/null"
~~~

If the pod stored a partition leader, Kafka elects an in-sync replica as the new leader.

Watch:

- leader IDs
- replica lists
- ISR lists
- broker pod recreation

The topic has:

~~~text
replication factor = 3
min ISR = 2
producer acks = all
~~~

So a single broker loss should still leave enough in-sync copies for safe writes.

### Why this is distributed-systems engineering

A distributed system is not merely "the same topic copied to several servers."

Kafka combines:

- partitioning for scale
- replication for fault tolerance
- leader election
- membership/coordinator state
- quorum/controller metadata
- clients that rediscover leaders
- recovery when nodes return

---

# Phase 14: Replay Data by Resetting Consumer Offsets

Kafka's retained log lets consumers replay historical data.

First pause the sink:

~~~bash
kubectl annotate scaledobject hotel-view-sink   -n lab   autoscaling.keda.sh/paused-replicas="0"   --overwrite
~~~

Reset the group to the beginning:

~~~bash
kubectl exec -n kafka "$KAFKA_POD" --   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server hotel-kafka-kafka-bootstrap:9092   --group hotel-view-sink   --topic hotel-price-updates   --reset-offsets   --to-earliest   --execute
~~~

Remove the pause:

~~~bash
kubectl annotate scaledobject hotel-view-sink   -n lab   autoscaling.keda.sh/paused-replicas-
~~~

The sink can rebuild the Redis materialized view from Kafka.

That illustrates why an event log can be much more powerful than a traditional fire-and-forget message queue.

### Learning Checkpoint

Answer:

**Why might a team choose Kafka instead of direct synchronous service-to-service calls?**

Possible dimensions:

- decoupling
- durable buffering
- independent consumer pace
- replay
- fan-out to multiple consumer groups
- partitioned throughput
- resilience to temporary downstream outages

---

# Phase 15: Understand Log Compaction

The topic was configured with:

~~~text
cleanup.policy = compact
~~~

Because the event key is hotel_id, Kafka can eventually retain the most recent value for each hotel key while older superseded values become eligible for compaction.

Conceptually:

~~~text
H0001 -> €100
H0002 -> €150
H0001 -> €120
H0001 -> €135

After compaction, the important current state is approximately:

H0001 -> €135
H0002 -> €150
~~~

Compaction is asynchronous. Do not expect old records to disappear immediately in this small lab.

Why this matters for the materialized-view pattern:

A new sink can consume the compacted topic and reconstruct current state without querying the original database.

---

# Phase 16: SRE Observability - What Would You Monitor?

A serious Kafka SRE should not monitor only "pods are Running."

Build this mental dashboard.

| Layer | Signal | Why it matters |
|---|---|---|
| Producer | records/sec | Is incoming demand changing? |
| Producer | error/retry rate | Are writes failing or throttling? |
| Broker | bytes in/out | Broker throughput |
| Broker | request latency | Client experience |
| Broker | disk usage | Kafka is storage-heavy |
| Broker | under-replicated partitions | Replication health |
| Broker | offline partitions | Severe availability problem |
| Broker | ISR shrink/expand | Replication instability |
| Topic | partition skew | Hot partitions reduce effective scale |
| Consumer | consumer lag | Work waiting |
| Consumer | records/sec | Drain capacity |
| Consumer group | rebalance frequency | Group instability |
| Sink | processing latency | Downstream bottleneck |
| Sink | error rate | Correctness/reliability |
| Redis | latency/errors | Materialized-view dependency |
| KEDA | desired/current replicas | Is autoscaling behaving correctly? |
| Kubernetes | restarts/OOMKilled | Runtime health |
| Kubernetes | CPU/memory | Capacity and resource pressure |

## A practical investigation order for rising lag

~~~text
Alert: consumer lag rising
        ↓
Is producer rate suddenly higher?
        ↓
Are all consumers healthy and assigned partitions?
        ↓
Are we already at partition-count parallelism?
        ↓
Is one partition much hotter than others?
        ↓
Is sink processing slow?
        ↓
Is Redis/network/downstream latency high?
        ↓
Are brokers healthy?
        ↓
Any under-replicated/offline partitions?
        ↓
Recent deploy/configuration/rebalance?
~~~

Do not jump directly to "add more pods."

If replicas already equal partition count, more consumers will not help.

This is directly relevant to trivago's later engineering write-up about PSE-kafka, where high lag could not simply be solved by adding more replicas because replica count had already reached partition count.

---

# Phase 17: Write a Runbook

Create your own operational runbook:

~~~bash
cat > notes/consumer-lag-runbook.md << 'EOF'
# Runbook: hotel-view-sink consumer lag

## Trigger
Consumer lag is above the agreed threshold for a sustained period.

## User impact
Hotel-price materialized views may become stale.

## First checks
1. Confirm total lag and per-partition lag.
2. Check producer rate.
3. Check sink replica count and pod health.
4. Compare active consumers with partition count.
5. Check sink logs and processing latency.
6. Check Redis availability and latency.
7. Check Kafka broker health, ISR, and partition leaders.
8. Check recent deployments/config changes.

## Recovery options
- Restore failed downstream dependency.
- Roll back a bad sink deployment.
- Let KEDA scale within the partition limit.
- Reduce processing bottleneck.
- Rebalance/restart only when justified.
- Add partitions only after considering key ordering and operational impact.

## Evidence to save
- consumer-group describe output
- Kafka topic describe output
- pod state/restarts
- KEDA/HPA state
- CPU/memory
- relevant logs
- timeline of changes

## After recovery
- verify lag returns to zero
- verify materialized view freshness
- document root cause
- create prevention action
EOF
~~~

This reinforces the SRE practices of on-call readiness, documentation, troubleshooting, and turning repetitive work into automation.

---

# Phase 18: Flink, Cassandra, and Redis - Know Their Roles

Kafka and Kubernetes are the core technologies in this learning path. Flink, Cassandra, Redis, and MySQL provide useful additional context.

You are already using Redis hands-on in this project.

## Redis

Mental model:

> Very fast in-memory key/value data store.

In this lab:

~~~text
hotel:H0001
    price = 145.30
    currency = EUR
    available = true
~~~

Good for:

- caching
- fast lookup
- counters
- ephemeral state
- materialized read models where durability requirements are understood

## Cassandra

Mental model:

> Distributed wide-column database designed for horizontal scale, high write throughput, and availability across nodes.

Think of Cassandra when:

- data volume is very large
- writes are heavy
- horizontal scaling matters
- multi-node fault tolerance matters
- access patterns can be designed around known partition keys

Do not describe Cassandra as "Redis but on disk." Their data models and operational trade-offs are different.

## Flink

Mental model:

> Distributed stateful stream-processing engine.

Kafka transports/stores event streams.

Flink computes over them.

Example:

~~~text
Kafka hotel-price events
        ↓
Flink
  - window over 5 minutes
  - join with hotel metadata
  - compute min/average price
  - detect anomalies
        ↓
Kafka / Cassandra / Redis / another sink
~~~

A useful distinction:

~~~text
Kafka = event transport + durable log
Flink = stream computation
Cassandra = distributed persistent database
Redis = very fast key/value data store
Kubernetes = workload orchestration
KEDA = event-driven autoscaling
~~~

---

# Phase 19: Kind vs trivago's GKE and Rancher Environments

The production environment discussed here includes:

- GCP / GKE
- Compute Engine
- on-premises Rancher

Your local environment is not those products, but the Kubernetes concepts transfer.

| Local lab | Production analogue |
|---|---|
| Kind cluster | GKE or Rancher-managed Kubernetes |
| Kind worker Docker container | Kubernetes worker node / VM |
| Local pod | Production pod |
| ClusterIP Service | Internal Kubernetes service |
| Strimzi Kafka | Production Kafka platform/operator approach |
| KEDA | KEDA in production Kubernetes |
| kubectl logs/describe | First-line Kubernetes troubleshooting |
| local broker failure | production broker/node failure concept |
| WSL/Ubuntu host | not a production fault domain |

Important limitation:

All of your Kind nodes ultimately live on one laptop. You can simulate broker/pod failure, but you cannot reproduce genuine datacenter, rack, availability-zone, or network-partition failure domains.

Keep this limitation clear when discussing the project.

---

# Phase 20: Learning Drills From This Project

Answer each without notes.

## Kafka fundamentals

1. What is Kafka?
2. What is an event?
3. What is a broker?
4. What is a Kafka cluster?
5. What is a topic?
6. What is a partition?
7. Is a partition physical or logical?
8. What is an offset?
9. What is a partition leader?
10. Why replicate partitions?
11. What is ISR?
12. What happens if a broker holding a leader dies?
13. What does a producer's Kafka client library do?
14. How does the producer know which broker to contact?
15. Why use message keys?

## Consumers

16. What is a consumer group?
17. What happens when a consumer dies?
18. Why can three partitions give at most three active consumers in one group?
19. What is a committed offset?
20. Where are committed offsets stored?
21. What is consumer lag?
22. Why can lag increase while CPU remains low?
23. What does replay mean?
24. Why is idempotency important?

## KEDA and Kubernetes

25. What does KEDA do?
26. Why is lag a better autoscaling signal than CPU for this sink?
27. What does activationLagThreshold do?
28. What does lagThreshold do?
29. Is lagThreshold a consumer's maximum processing capacity?
30. Why cap consumer replicas at partition count?
31. Why have a cooldown period?
32. What happens if KEDA cannot fetch Kafka metrics?
33. What does Kubernetes do when a sink pod crashes?
34. What does Kafka do when that consumer disappears?

## SRE troubleshooting

35. Consumer lag is rising. What do you check first?
36. All consumers are healthy but lag keeps rising. What next?
37. Replica count equals partition count. Would you add more pods?
38. One partition has 90% of the lag. What might that suggest?
39. Redis is slow. How can that affect Kafka lag?
40. A broker disappears. Which Kafka metrics do you inspect?
41. What signals would you alert on?
42. How would you safely roll out a new autoscaling strategy?

---

# Phase 21: Five Answers You Should Be Able to Give in 30 Seconds

## "What is Kafka?"

> Kafka is a distributed event-streaming platform. Producers publish records into topics, Kafka stores those records durably across partitioned and replicated logs, and consumers read them independently at their own pace. Because the log is retained, consumers can also replay data.

## "What is a partition?"

> A partition is an ordered logical slice of a Kafka topic. Each record in it gets an increasing offset. Physically, each partition replica is persisted as log-segment files on broker storage. Partitions provide both ordering boundaries and parallelism.

## "What is consumer lag?"

> Consumer lag is how far a consumer group is behind the latest data in Kafka. For each partition it is roughly the log-end offset minus the group's committed offset. Rising lag means work is arriving faster than the consumer group is successfully completing it.

## "Why would KEDA scale on lag?"

> Lag is a direct measure of pending Kafka work. CPU and memory can stay low when a consumer is blocked on network or database I/O, so those resource metrics can miss a growing backlog. KEDA can use lag to wake consumers from zero and scale them while work is waiting.

## "What happens if a consumer crashes?"

> Kubernetes may recreate the pod, but Kafka independently detects the consumer-group membership change and rebalances its partitions to available consumers. The replacement resumes from committed offsets, so correct commit strategy and idempotent processing are important.

---

# Phase 22: Practical Troubleshooting Scenario

Use this complete thought process.

### Scenario

~~~text
Alert:
hotel-view-sink consumer lag has risen from 0 to 600,000.
CPU is only 12%.
All pods are Running.
~~~

Do not say "CPU is fine."

Say:

1. **Confirm the symptom**  
   Check total and per-partition lag and whether it is still growing.

2. **Check demand**  
   Did producer throughput spike?

3. **Check consumer capacity**  
   How many partitions exist? How many consumers are active? Are we already at the parallelism ceiling?

4. **Look for skew**  
   Is lag evenly distributed or concentrated in one partition?

5. **Check processing latency**  
   Is Redis/database/network latency slowing each message?

6. **Check Kafka health**  
   Brokers, leaders, ISR, under-replicated/offline partitions.

7. **Check recent changes**  
   Deployment, configuration, schema, dependency, networking.

8. **Mitigate based on evidence**  
   Scaling only helps if unused partition parallelism exists. Otherwise fix the bottleneck, partitioning problem, downstream dependency, or processing path.

9. **Verify recovery**  
   Lag decreases at a healthy drain rate and data freshness returns.

10. **Prevent recurrence**  
   Alerting, capacity model, runbook, tuning, code/config fix, or automation.

That is the SRE mindset the project is designed to build.

---

# Phase 23: Suggested Study Order

For a first pass through the guide, do these first:

~~~text
1. Phases 1-7
   Core Kafka mental model

2. Phase 8
   Consumer groups and partitions

3. Phases 9-10
   KEDA and lag-based autoscaling

4. Phases 11-13
   Consumer, dependency, and broker failures

5. Phase 16
   Operational metrics and troubleshooting

6. Phase 18
   Flink/Cassandra/Redis conceptual map

7. Phase 20
   Answer every review question aloud
~~~

After that, complete replay, incident drills, the runbook, and the troubleshooting scenario until you can explain cause-and-effect without looking at commands.

---

# Phase 24: Clean Shutdown

The cluster is local but consumes Docker resources.

To destroy it completely:

~~~bash
kind delete cluster --name trivago-data-lab
~~~

This lab uses ephemeral Kafka storage, so deleting the Kind cluster intentionally deletes all Kafka data.

Recreate later with:

~~~bash
cd ~/trivago-data-lab

kind create cluster   --name trivago-data-lab   --config kind-config.yaml
~~~

Then repeat the install/apply phases.

If you want to keep the cluster but temporarily stop its Kind containers, first inspect them:

~~~bash
docker ps --filter name=trivago-data-lab
~~~

For practice, prefer a clean destroy/recreate cycle. Rebuilding the environment is itself useful Kubernetes/Kafka muscle memory.

---

# Sources Used for the Lab Design

- trivago engineering, Kafka sinks + KEDA: https://tech.trivago.com/post/2026-02-18-from-always-on-to-on-demand-scaling-kafka-sinks-with-keda
- trivago engineering, Kafka consumer performance: https://tech.trivago.com/post/2026-06-12-how-we-cut-kafka-consumer-deployment-costs-by-83
- Strimzi: https://strimzi.io/
- KEDA: https://keda.sh/

---

# Final Mental Model

~~~text
Producer application
      ↓
Kafka producer client
      ↓
bootstrap broker
      ↓
partition leader
      ↓
replicated partition log
      ↓
consumer group polls
      ↓
sink transforms data
      ↓
Redis materialized view
      ↓
API reads locally

Meanwhile:

Kafka offsets + log-end positions
      ↓
consumer lag
      ↓
KEDA
      ↓
Kubernetes replica count
~~~

If you can build this lab, break it, recover it, and explain why each component behaves the way it does, you will have a practical foundation for core Kafka/Kubernetes/SRE concepts used in large-scale data platforms.
