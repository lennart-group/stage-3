import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 8, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class Test_SearchAPI {

    // ---------------- Parameterisierung ----------------
    @Param({"1e3", "1e4", "1e5"})
    public String listSizeParam;     // Größe der Listen für intersection

    @Param({"none", "half", "full"})
    public String overlap;           // Überlappung zwischen A und B

    @Param({"0", "10", "1000"})
    public int resultsCount;         // Größe der Ergebnisliste für buildResponse

    // ---------------- Reflection-Methoden ----------------
    private Method mExtractYear;
    private Method mIntersection;
    private Method mBuildResponse;
    private Method mCreateEmptyResponse;

    // ---------------- Testdaten ----------------
    private List<Integer> listA;
    private List<Integer> listB;

    private String yearStringSimple;
    private String yearStringLong;

    private List<Map<String, Object>> resultsForBuild;

    // ---------------- Setup ----------------
    @Setup(Level.Trial)
    public void setup() throws Exception {
        Class<?> clazz = Class.forName("bigdatastage2.SearchAPI");

        mExtractYear         = clazz.getDeclaredMethod("extractYear", String.class);
        mIntersection        = clazz.getDeclaredMethod("intersection", List.class, List.class);
        mBuildResponse       = clazz.getDeclaredMethod("buildResponse",
                String.class, String.class, String.class, String.class, List.class);
        mCreateEmptyResponse = clazz.getDeclaredMethod("createEmptyResponse",
                String.class, String.class, String.class, String.class);

        mExtractYear.setAccessible(true);
        mIntersection.setAccessible(true);
        mBuildResponse.setAccessible(true);
        mCreateEmptyResponse.setAccessible(true);

        // Daten für intersection
        int n = parseSize(listSizeParam);
        generateLists(n, overlap);

        // Daten für extractYear
        yearStringSimple = "Published: March 3, 1865";
        yearStringLong = makeLongYearBlob(30000); // großer Text mit verstreuten Jahreszahlen

        // Daten für buildResponse
        resultsForBuild = makeResults(resultsCount);
    }

    // ---------------- Benchmarks ----------------

    @Benchmark
    public Object bench_extractYear_simple() throws Exception {
        return mExtractYear.invoke(null, yearStringSimple);
    }

    @Benchmark
    public Object bench_extractYear_long() throws Exception {
        return mExtractYear.invoke(null, yearStringLong);
    }

    @Benchmark
    public Object bench_intersection() throws Exception {
        return mIntersection.invoke(null, listA, listB);
    }

    @Benchmark
    public Object bench_buildResponse() throws Exception {
        return mBuildResponse.invoke(null,
                "alice adventures",       // q
                "Carroll",                // author
                "English",                // language
                "1865",                   // year
                resultsForBuild);
    }

    @Benchmark
    public Object bench_createEmptyResponse() throws Exception {
        return mCreateEmptyResponse.invoke(null,
                "wonderland", null, null, null);
    }

    // ---------------- Hilfsfunktionen: Datengenerierung ----------------

    private int parseSize(String s) {
        return switch (s) {
            case "1e3" -> 1_000;
            case "1e4" -> 10_000;
            case "1e5" -> 100_000;
            default -> 1_000;
        };
    }

    /**
     * Erzeuge zwei Integer-Listen (A, B) mit gewünschter Überlappung:
     * none: disjunkt, half: ~50% Schnittmenge, full: identisch.
     */
    private void generateLists(int n, String overlapMode) {
        listA = new ArrayList<>(n);
        listB = new ArrayList<>(n);
        for (int i = 0; i < n; i++) listA.add(i);

        switch (overlapMode) {
            case "none" -> {
                // B = n..2n-1
                for (int i = 0; i < n; i++) listB.add(n + i);
            }
            case "half" -> {
                // Erste Hälfte identisch, zweite disjunkt
                int half = n / 2;
                for (int i = 0; i < half; i++) listB.add(i);      // überlappt mit A
                for (int i = half; i < n; i++) listB.add(n + i);  // disjunkt
            }
            case "full" -> {
                listB.addAll(listA); // komplett identisch
            }
            default -> {
                for (int i = 0; i < n; i++) listB.add(n + i);
            }
        }

        // Shuffle, um realistischere Verteilung zu bekommen
        Collections.shuffle(listA, new Random(42));
        Collections.shuffle(listB, new Random(1337));
    }

    private String makeLongYearBlob(int words) {
        Random rnd = new Random(7);
        StringBuilder sb = new StringBuilder(words * 6);
        String[] years = {"1800", "1815", "1865", "1900", "1955", "1999", "2001", "2012", "2020"};
        for (int i = 0; i < words; i++) {
            if (i % 200 == 0) {
                sb.append(years[rnd.nextInt(years.length)]).append(' ');
            } else {
                sb.append(randomWord(rnd, 2 + rnd.nextInt(8))).append(' ');
            }
            if (i % 77 == 0) sb.append('\n');
        }
        return sb.toString();
    }

    private String randomWord(Random rnd, int len) {
        char[] c = new char[len];
        for (int i = 0; i < len; i++) c[i] = (char) ('a' + rnd.nextInt(26));
        return new String(c);
    }

    private List<Map<String, Object>> makeResults(int n) {
        List<Map<String, Object>> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Map<String, Object> book = new HashMap<>();
            book.put("book_id", i + 1);
            book.put("title", "Title " + (i + 1));
            book.put("author", (i % 2 == 0) ? "Lewis Carroll" : "Mark Twain");
            book.put("language", "English");
            book.put("year", (i % 3 == 0) ? "1865" : "1900");
            out.add(book);
        }
        return out;
    }

    // Optionaler Main-Runner für IDE
    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(Test_SearchAPI.class.getSimpleName())
                .detectJvmArgs()
                .build();
        new Runner(opt).run();
    }
}
