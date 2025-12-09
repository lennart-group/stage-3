import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.reflect.Method;
import java.util.Random;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 8, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class Test_IngestionAPI {

    // ---- Parameterisierung der Eingaben ----
    @Param({"small", "medium", "large"})
    public String textSize;            // steuert Textlänge

    @Param({"present", "absent"})
    public String contentMarkers;      // ob START/END Marker vorhanden sind (für extractContent)

    @Param({"withUpdated", "noUpdated"})
    public String releaseUpdated;      // ob "Most recently updated:" Zeile existiert

    // vorbereitete Texte
    private String fullTextForMetadata;     // Enthält Header + Content
    private String fullTextForRelease;      // Für extractReleaseDate
    private String textForContent;          // Für extractContent

    // Reflection-Methoden
    private Method mExtractMetadata;
    private Method mExtractReleaseDate;
    private Method mExtractContent;
    private Method mBuildDbEntry;

    // Beispielwerte für buildDbEntry
    private int exampleId = 12345;
    private String exampleTitle = "Example Title";
    private String exampleAuthor = "Jane Doe";
    private String exampleRelease = "January 1, 1900";
    private String exampleLanguage = "English";
    private String exampleContent;
    private String exampleFooter = "*** END OF THE PROJECT GUTENBERG EBOOK";

    // ---- Setup ----
    @Setup(Level.Trial)
    public void setup() throws Exception {
        Class<?> clazz = Class.forName("bigdatastage2.IngestingAPI");
        mExtractMetadata   = clazz.getDeclaredMethod("extractMetadata", String.class, String.class);
        mExtractReleaseDate= clazz.getDeclaredMethod("extractReleaseDate", String.class);
        mExtractContent    = clazz.getDeclaredMethod("extractContent", String.class);
        mBuildDbEntry      = clazz.getDeclaredMethod("buildDbEntry",
                int.class, String.class, String.class, String.class, String.class, String.class, String.class);

        mExtractMetadata.setAccessible(true);
        mExtractReleaseDate.setAccessible(true);
        mExtractContent.setAccessible(true);
        mBuildDbEntry.setAccessible(true);

        // synthetische Inhalte erzeugen
        int words;
        switch (textSize) {
            case "small":  words = 5_000;   break;
            case "medium": words = 50_000;  break;
            case "large":  words = 300_000; break;
            default:       words = 5_000;
        }

        String body = makeWordSoup(words, 5000, 42);
        String header = makeHeader();
        String releaseBlock = makeReleaseBlock(releaseUpdated.equals("withUpdated"));
        String startMarker = "*** START OF THE PROJECT GUTENBERG EBOOK EXAMPLE ***\n";
        String endMarker   = "\n*** END OF THE PROJECT GUTENBERG EBOOK EXAMPLE ***\n";

        // Für Metadata: Header + Körper
        fullTextForMetadata = header + releaseBlock + body;

        // Für ReleaseDate: Header + ReleaseBlock + beliebiger Body
        fullTextForRelease = header + releaseBlock + body;

        // Für extractContent: mit oder ohne Marker
        if (contentMarkers.equals("present")) {
            textForContent = header + startMarker + body + endMarker + "FOOTER...";
            // Beispielcontent für buildDbEntry
            exampleContent = (startMarker + body + endMarker).replaceAll("(?m)^\\*{3}.*$", "").trim();
        } else {
            textForContent = header + body; // kein Marker
            exampleContent = body;
        }
    }

    // ---- Benchmarks: extractMetadata ----
    @Benchmark
    public Object bench_extractMetadata_Title() throws Exception {
        return mExtractMetadata.invoke(null, fullTextForMetadata, "Title:");
    }

    @Benchmark
    public Object bench_extractMetadata_Author() throws Exception {
        return mExtractMetadata.invoke(null, fullTextForMetadata, "Author:");
    }

    @Benchmark
    public Object bench_extractMetadata_Language() throws Exception {
        return mExtractMetadata.invoke(null, fullTextForMetadata, "Language:");
    }

    // ---- Benchmark: extractReleaseDate ----
    @Benchmark
    public Object bench_extractReleaseDate() throws Exception {
        return mExtractReleaseDate.invoke(null, fullTextForRelease);
    }

    // ---- Benchmarks: extractContent ----
    @Benchmark
    public Object bench_extractContent() throws Exception {
        return mExtractContent.invoke(null, textForContent);
    }

    // ---- Benchmark: buildDbEntry ----
    @Benchmark
    public Object bench_buildDbEntry() throws Exception {
        return mBuildDbEntry.invoke(null,
                exampleId,
                exampleContent,
                exampleTitle,
                exampleAuthor,
                exampleRelease,
                exampleLanguage,
                exampleFooter);
    }

    // ---- Hilfen zur Testdatengenerierung ----
    private String makeHeader() {
        return String.join("\n",
                "Title: The Example Book",
                "Author: Jane Doe",
                "Language: English") + "\n";
    }

    private String makeReleaseBlock(boolean withUpdated) {
        String base = "Release date: January 1, 1900\n";
        if (withUpdated) {
            base += "Most recently updated: February 1, 1950\n";
        }
        return base;
    }

    /**
     * Erzeugt einen großen Text mit pseudo-zufälligen Kleinbuchstaben-Wörtern.
     * Passt zur Tokenizer-Logik und simuliert realistische Länge/Zeilenumbrüche.
     */
    private String makeWordSoup(int totalWords, int vocab, long seed) {
        Random rnd = new Random(seed);
        String[] dict = new String[vocab];
        for (int i = 0; i < vocab; i++) {
            dict[i] = randomWord(rnd, 2 + rnd.nextInt(9)); // 2..10 Zeichen
        }
        StringBuilder sb = new StringBuilder(totalWords * 6);
        for (int i = 0; i < totalWords; i++) {
            if (i > 0) sb.append(' ');
            sb.append(dict[rnd.nextInt(vocab)]);
            if (i % 37 == 0) sb.append('.');
            if (i % 81 == 0) sb.append('\n');
        }
        return sb.toString();
    }

    private String randomWord(Random rnd, int len) {
        char[] c = new char[len];
        for (int i = 0; i < len; i++) c[i] = (char) ('a' + rnd.nextInt(26));
        return new String(c);
    }

    // Optionaler Main-Runner für IDE
    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(Test_IngestionAPI.class.getSimpleName())
                .detectJvmArgs()
                .build();
        new Runner(opt).run();
    }
}
