# Distributed Search Engine - Stage 3

## Project Overview

This project implements a distributed search engine capable of ingesting, indexing, and querying large document sets across multiple nodes. Features include:

* **Horizontal scalability** – Easily add more nodes for ingestion, indexing, and search.
* **Fault tolerance** – Handles node failures automatically via request rerouting.
* **Performance benchmarking** – Measures throughput and latency for ingestion, indexing, and search operations.
* **Monitoring** – Logs and dashboards provide insights into system metrics and resource usage.

---

## Architecture

The system consists of:

* **Ingest nodes** – Handle document ingestion requests.
* **Indexing nodes** – Build and update the distributed inverted index.
* **Search nodes** – Handle query requests.
* **Message broker (ActiveMQ)** – Coordinates communication between nodes.
* **Controller** – REST API to trigger ingestion, indexing, and queries.
* **Load balancer** – Distributes requests across nodes.

---

## Requirements

* Docker >= 20.10
* Docker Compose >= 1.29
* Java >= 21
* Optional: Kubernetes if using k8s manifests

---

## Running the System with Docker Compose

1. **Clone the repository**:

```bash
git clone <repository-url>
cd stage-3
```

2. **Start all containers**:

```bash
docker-compose up -d
```

3. **Scale services** (example with 3 nodes each):

```bash
docker-compose up -d --scale ingest=3 --scale index-worker=3 --scale search=3
```

4. **Check running containers**:

```bash
docker ps
```

5. **Stop and remove containers**:

```bash
docker-compose down
```

---

## Benchmarks

The system includes JMH benchmarks to measure:

* **Ingestion throughput** – documents per second processed by ingest nodes.
* **Ingestion latency** - time to execute request across ingest nodes
* **Indexing throughput** – documents per second indexed across nodes.
* **Search latency** – time to execute queries across search nodes.

### Running Benchmarks

1. Ensure system is running and environment variables are set.
2. Build the benchmark JAR:

```bash
mvn clean package
```

3. Run benchmarks:

```bash
java -jar target/Benchmarking.jar -rf csv -rff results.csv
```

4. **Benchmark parameters**:

* `nodes` – number of nodes for ingestion, indexing, and search.
* `indexBookId` – book ID used for indexing.
* `word` – query term used in search benchmark (only for search benchmark).

5. **CSV output** contains throughput (`ops/s`) and latency (`s/op` or `ms/op`) per benchmark.

---

## Observing System Behavior

* **Logs**:

```bash
docker-compose logs -f <service-name>
```

* **Resource monitoring**:

```bash
docker stats
```

* Optional: integrate Prometheus/Grafana for dashboards.

* **Failure simulation**:

```bash
docker kill <container-name>
```

This demonstrates fault tolerance as requests are rerouted automatically.

---

## Demonstration Video
For a quick demonstration of how to run the system and how to use the different features, watch our [Demonstration Video](https://youtu.be/A2nyGh450GQ)
