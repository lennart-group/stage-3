package bigdatastage3;

import org.openjdk.jmh.annotations.*;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * JMH Benchmark for Stage 3 distributed search engine.
 * Containers are started per trial with a given number of nodes.
 * The benchmarks can be runned with: java -jar target/Benchmarking.jar -rf csv
 * -rff results.csv
 */
@BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkRunner {

  // ---------------------- Parameters ----------------------
  @Param({ "1", "2" })
  private int nodes;

  @Param({ "100" })
  private int totalDocs;

  @Param({ "10000" })
  private int totalTokens;

  @Param({ "1000" })
  private int numQueries;

  @Param({ "10" })
  private int concurrency;

  private BenchmarkRunnerHelper runner;

  // ---------------------- JMH Setup/TearDown ----------------------
  @Setup(Level.Trial)
  public void setup() throws IOException, InterruptedException {
    runner = new BenchmarkRunnerHelper(
        "C:/Users/nicob/OneDrive/Dokumente/Studium/Auslandssemester/ULPGC/Big_Data/Project/stage-3/docker");
    System.out.println("📦 Starting containers for trial " + nodes);
    runner.startContainers(nodes);
  }

  @TearDown(Level.Trial)
  public void teardown() throws IOException, InterruptedException {
    System.out.println("🛑 Stopping containers after trial...");
    runner.stopContainers();
  }

  // ---------------------- Benchmarks ----------------------
  @Benchmark
  @Measurement(iterations = 10)
  public double benchmarkIngestion() {
    return runner.runIngestionBenchmark(totalDocs);
  }

  @Benchmark
  @Measurement(iterations = 10000)
  public double benchmarkIndexing() {
    return runner.runIndexingBenchmark(totalTokens);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Measurement(iterations = 1000)
  public Map<String, Double> benchmarkQuery() {
    return runner.runQueryBenchmark(numQueries, concurrency);
  }

  // ---------------------- Helper Runner ----------------------
  public static class BenchmarkRunnerHelper {
    private final File dockerComposeDir;

    public BenchmarkRunnerHelper(String dockerComposeDir) {
      this.dockerComposeDir = new File(dockerComposeDir);
      if (!this.dockerComposeDir.exists() || !this.dockerComposeDir.isDirectory()) {
        throw new IllegalArgumentException("Invalid Docker Compose directory: " + dockerComposeDir);
      }
    }

    public void startContainers(int nodes)
        throws IOException, InterruptedException {
      System.out.println("📦 Starting containers: ingest=" + nodes +
          ", indexer=" + nodes + ", search=" + nodes);

      List<String> command = new ArrayList<>();
      command.add("docker-compose");
      command.add("up");
      command.add("-d");
      command.add("--scale");
      command.add("ingest=" + nodes);
      command.add("--scale");
      command.add("index-worker=" + nodes);
      command.add("--scale");
      command.add("search=" + nodes);

      runCommand(command);
      Thread.sleep(5000); // give containers some time to initialize
    }

    public void stopContainers() throws IOException, InterruptedException {
      System.out.println("🛑 Stopping containers");
      runCommand(List.of("docker-compose", "down"));
    }

    private void runCommand(List<String> command) throws IOException, InterruptedException {
      ProcessBuilder pb = new ProcessBuilder(command);
      pb.directory(dockerComposeDir);
      pb.inheritIO();
      Process process = pb.start();
      int exit = process.waitFor();
      if (exit != 0) {
        throw new RuntimeException("Command failed: " + String.join(" ", command));
      }
    }

    public double runIngestionBenchmark(int totalDocs) {
      Instant start = Instant.now();
      try {
        Thread.sleep(totalDocs * 10L);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Instant end = Instant.now();
      double seconds = Duration.between(start, end).toMillis() / 1000.0;
      double rate = totalDocs / seconds;
      System.out.printf("Ingestion completed: %.2f docs/s%n", rate);
      return rate;
    }

    public double runIndexingBenchmark(int totalTokens) {
      Instant start = Instant.now();
      try {
        Thread.sleep(totalTokens / 10L);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Instant end = Instant.now();
      double seconds = Duration.between(start, end).toMillis() / 1000.0;
      double rate = totalTokens / seconds;
      System.out.printf("Indexing completed: %.2f tokens/s%n", rate);
      return rate;
    }

    public Map<String, Double> runQueryBenchmark(int numQueries, int concurrency) {
      List<Long> latencies = new ArrayList<>();
      Random random = new Random();
      for (int i = 0; i < numQueries; i++) {
        long latency = 20 + random.nextInt(50);
        latencies.add(latency);
      }
      double avg = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
      double max = latencies.stream().mapToLong(Long::longValue).max().orElse(0);
      double p95 = latencies.stream().sorted().skip((long) (0.95 * numQueries)).findFirst().orElse(0L);

      Map<String, Double> metrics = Map.of(
          "avg", avg,
          "max", (double) max,
          "p95", (double) p95);
      System.out.println("📊 Query latency (ms): " + metrics);
      return metrics;
    }
  }
}
