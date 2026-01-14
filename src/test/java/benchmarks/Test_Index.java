package benchmarks;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 8, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class Test_Index {

    // ---- Params to vary input size / conditions ----
    @Param({"small", "medium", "large"})
    public String textSize;              // controls size of synthetic text for tokenize

    @Param({"10000"})                    // number of lines (book IDs) written to indexed_books.txt
    public int indexedCount;

    // IDs for alreadyIndexed() benchmarks
    private int hitId;                   // present in file
    private int missId;                  // absent from file

    // Synthetic texts for tokenize()
    private String textForTokenize;

    // Reflection handles into IndexingAPI
    private Method mTokenize;
    private Method mAlreadyIndexed;

    // Paths matching IndexingAPI's constants (can't access them directly due to private)
    private final Path CONTROL_DIR = Paths.get("control");
    private final Path INDEXED_FILE = CONTROL_DIR.resolve("indexed_books.txt");

    // ---- Setup / TearDown ----
    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        // Prepare reflection
        Class<?> clazz = Class.forName("bigdatastage2.IndexingAPI");
        mTokenize = clazz.getDeclaredMethod("tokenize", String.class);
        mTokenize.setAccessible(true);
        mAlreadyIndexed = clazz.getDeclaredMethod("alreadyIndexed", int.class);
        mAlreadyIndexed.setAccessible(true);

        // Prepare control dir + indexed file with many IDs
        if (!Files.exists(CONTROL_DIR)) {
            Files.createDirectories(CONTROL_DIR);
        }
        createIndexedFile(indexedCount);

        // Choose IDs for hit/miss
        hitId = indexedCount / 2;            // guaranteed to be present
        missId = indexedCount + 123_456;     // guaranteed to be absent

        // Build synthetic text according to size parameter
        switch (textSize) {
            case "small":  textForTokenize = makeSyntheticText(2_000, 500);  break;   // ~2k words
            case "medium": textForTokenize = makeSyntheticText(20_000, 5_000); break; // ~20k words
            case "large":  textForTokenize = makeSyntheticText(200_000, 20_000); break; // ~200k words
            default:       textForTokenize = makeSyntheticText(2_000, 500);
        }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws IOException {
        // Clean up created files to avoid polluting workspace
        if (Files.exists(INDEXED_FILE)) {
            Files.delete(INDEXED_FILE);
        }
        // Leave control/ folder in place (safe); remove if empty
        try (var s = Files.list(CONTROL_DIR)) {
            if (s.findAny().isEmpty()) {
                Files.delete(CONTROL_DIR);
            }
        } catch (NoSuchFileException ignored) {
        }
    }

    // ---- Benchmarks ----

    @Benchmark
    public Object bench_tokenize() throws Exception {
        // returns the Set<String>; we just return it to prevent dead-code elimination
        return mTokenize.invoke(null, textForTokenize);
    }

    @Benchmark
    public boolean bench_alreadyIndexed_hit() throws Exception {
        // Should return true
        return (Boolean) mAlreadyIndexed.invoke(null, hitId);
    }

    @Benchmark
    public boolean bench_alreadyIndexed_miss() throws Exception {
        // Should return false
        return (Boolean) mAlreadyIndexed.invoke(null, missId);
    }

    // ---- Helpers ----

    /**
     * Creates ./control/indexed_books.txt with IDs 1..count (one per line).
     */
    private void createIndexedFile(int count) throws IOException {
        // Recreate file fresh
        if (Files.exists(INDEXED_FILE)) {
            Files.delete(INDEXED_FILE);
        }
        try (BufferedWriter bw = Files.newBufferedWriter(
                INDEXED_FILE, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            for (int i = 1; i <= count; i++) {
                bw.write(Integer.toString(i));
                bw.newLine();
            }
        }
    }

    /**
     * Generates a synthetic "book" text of roughly `totalWords` words drawn
     * from a vocabulary of size `vocab`. Words are lower-case alphabetic (2..10 chars),
     * matching the tokenizer's regex expectations.
     */
    private String makeSyntheticText(int totalWords, int vocab) {
        Random rnd = new Random(42);
        List<String> dict = new ArrayList<>(vocab);
        while (dict.size() < vocab) {
            dict.add(randomWord(rnd, 2 + rnd.nextInt(9))); // 2..10 letters
        }
        StringBuilder sb = new StringBuilder(totalWords * 6);
        for (int i = 0; i < totalWords; i++) {
            if (i > 0) sb.append(' ');
            sb.append(dict.get(rnd.nextInt(vocab)));
            // sprinkle punctuation/newlines to mimic real text; tokenizer ignores non-letters
            if (i % 33 == 0) sb.append('.');
            if (i % 77 == 0) sb.append('\n');
        }
        return sb.toString();
    }

    private String randomWord(Random rnd, int len) {
        char[] c = new char[len];
        for (int i = 0; i < len; i++) {
            c[i] = (char) ('a' + rnd.nextInt(26));
        }
        return new String(c);
    }

    // Optional: enable running directly from IDE without the JMH plugin.
    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(Test_Index.class.getSimpleName())
                .detectJvmArgs()
                .build();
        new Runner(opt).run();
    }
}
