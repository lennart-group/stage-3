package bigdatastage3;

import com.google.gson.Gson;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import io.github.cdimascio.dotenv.Dotenv;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SearchAPI {
    private static final Gson gson = new Gson();
    private static MongoCollection<Document> booksCollection;
    private static MongoDatabase indexDb;
    private static MongoDatabase[] databases;

    public static void main(String[] args) {

        Dotenv dotenv = Dotenv.load();
        int PORT = Integer.parseInt(dotenv.get("SEARCH_PORT"));

        // Initialize MongoDB connection
        try {
            databases = RepositoryConnection.connectToDB();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        booksCollection = databases[0].getCollection("books");
        indexDb = databases[1];

        // Create Javalin server
        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(PORT);

        // Add CORS support
        app.before(ctx -> {
            ctx.header("Access-Control-Allow-Origin", "*");
            ctx.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
            ctx.header("Access-Control-Allow-Headers", "Content-Type");
        });

        // Health check status
        app.get("/status", ctx -> {
            Map<String, Object> status = Map.of(
                    "service", "search-service",
                    "status", "running",
                    "database", "connected");
            ctx.result(gson.toJson(status));
        });

        // Main search endpoint: GET
        // /search?q={term}&author={name}&language={code}&year={YYYY}
        app.get("/search", SearchAPI::handleSearch);
    }

    private static void handleSearch(Context ctx) {
        try {
            // Extract query parameters
            String query = ctx.queryParam("q");
            String author = ctx.queryParam("author");
            String language = ctx.queryParam("language");
            String yearStr = ctx.queryParam("year");

            // Log the request
            System.out.println("New search request: " + ctx.fullUrl());

            // Validate query parameter
            if (query == null || query.trim().isEmpty()) {
                ctx.status(400).result(gson.toJson(Map.of(
                        "error", "Query parameter 'q' is required.")));
                System.err.println("Invalid request: Query parameter 'q' is required.");
                return;
            }

            // Search for books containing the search term(s)
            List<Integer> bookIdsFromIndex = searchTerm(query);

            if (bookIdsFromIndex.isEmpty()) {
                ctx.result(gson.toJson(createEmptyResponse(query, author, language, yearStr)));
                System.out.println("Request successfully completed. No results found.");
                return;
            }

            // Apply metadata filters
            List<Integer> filteredBookIds = applyMetadataFilters(bookIdsFromIndex, author, language, yearStr);

            // Fetch book details
            List<Map<String, Object>> results = fetchBookDetails(filteredBookIds);

            // Build response
            Map<String, Object> response = buildResponse(query, author, language, yearStr, results);
            ctx.result(gson.toJson(response));
            System.out.println("Request successfully completed. " + results.size() + " results found.");

        } catch (Exception e) {
            System.err.println("Error in search: " + e.getMessage());
            e.printStackTrace();
            ctx.status(500).result(gson.toJson(Map.of(
                    "error", "Internal server error: " + e.getMessage())));
        }
    }

    /* Searches the inverted index for books containing all terms in the query. */
    private static List<Integer> searchTerm(String query) {
        String[] terms = query.toLowerCase().trim().split("\\s+");

        if (terms.length == 0) {
            return new ArrayList<>();
        }

        // Get postings for first term
        List<Integer> result = getPostingsForTerm(terms[0]);

        // Intersect with postings for remaining terms
        for (int i = 1; i < terms.length; i++) {
            List<Integer> nextPostings = getPostingsForTerm(terms[i]);
            result = intersection(result, nextPostings);

            if (result.isEmpty()) {
                break; // No need to continue if intersection is empty
            }
        }

        return result;
    }

    /*
     * Gets the list of book IDs (postings) for a single term from the inverted
     * index.
     */
    private static List<Integer> getPostingsForTerm(String term) {
        try {
            /* Search in the index in collections, which are separated by the first letter of the term */
            MongoCollection<Document> collection = indexDb.getCollection(term.substring(0, 1));
            System.out.println("Searching in the collection: " + collection.getNamespace());
            Document indexDoc = collection.find(Filters.eq("term", term)).first();
            System.out.println(indexDoc);

            if (indexDoc == null) {
                return new ArrayList<>();
            }

            List<?> postings = indexDoc.getList("postings", Integer.class);
            return postings != null ? new ArrayList<>((List<Integer>) postings) : new ArrayList<>();

        } catch (Exception e) {
            System.err.println("Error fetching postings for term '" + term + "':" + e.getMessage());
            return new ArrayList<>();
        }
    }

    /* Applies metadata filters (author, language, year) to the list of book IDs. */
    private static List<Integer> applyMetadataFilters(List<Integer> bookIds, String author, String language,
                                                      String yearStr) {
        if (bookIds.isEmpty() || (author == null && language == null && yearStr == null)) {
            return bookIds;
        }
        System.out.println("Applying filters: " + bookIds + " author=" + author + " language=" + language + " year=" + yearStr);

        // Build MongoDB filter
        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.in("id", bookIds));

        if (author != null && !author.trim().isEmpty()) {
            // Search in the "author" field directly, case-insensitive
            filters.add(Filters.regex("author", author, "i"));
            System.out.println("Added author filter for: " + author);
        }

        if (language != null && !language.trim().isEmpty()) {
            // Search in the "language" field directly, case-insensitive
            filters.add(Filters.regex("language", language, "i"));
            System.out.println("Added language filter for: " + language);
        }

        if (yearStr != null && !yearStr.trim().isEmpty()) {
            try {
                // Extract the year from the release_date field
                int year = Integer.parseInt(yearStr);
                // We need to search for the year in the release_date string using regex
                String yearPattern = "\\b" + year + "\\b"; // Word boundary to match exact year
                filters.add(Filters.regex("release_date", yearPattern));
                System.out.println("Added year filter for: " + year);
            } catch (NumberFormatException e) {
                System.err.println("Invalid year format: " + yearStr);
            }
        }

        Bson combinedFilter = Filters.and(filters);

        // Query books collection
        List<Integer> filteredIds = new ArrayList<>();
        try (MongoCursor<Document> cursor = booksCollection.find(combinedFilter).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                filteredIds.add(document.getInteger("id"));
            }
        }

        System.out.println("Filter result count: " + filteredIds.size());
        return filteredIds;
    }

    /* Fetches full book details for the given book IDs. */
    private static List<Map<String, Object>> fetchBookDetails(List<Integer> bookIds) {
        if (bookIds.isEmpty()) {
            return new ArrayList<>();
        }
        System.out.println(bookIds);

        List<Map<String, Object>> results = new ArrayList<>();

        try (MongoCursor<Document> cursor = booksCollection
                .find(Filters.in("id", bookIds))
                .iterator()) {

            while (cursor.hasNext()) {
                Document doc = cursor.next();

                Map<String, Object> bookInfo = new HashMap<>();
                bookInfo.put("book_id", doc.getInteger("id"));
                bookInfo.put("title", doc.getString("title"));
                bookInfo.put("author", doc.getString("author"));
                bookInfo.put("language", doc.getString("language"));
                String release_date_str = doc.getString("release_date");
                String year = extractYear(release_date_str);
                bookInfo.put("year", Objects.requireNonNullElse(year, "unknown"));
                results.add(bookInfo);
            }
        }

        return results;
    }

    /* Extracts just the year from the release_date string. */
    public static String extractYear(String text) {
        if (text == null || text.isEmpty()) {
            return "unknown";
        }

        // Try to find any 4-digit year in the text
        Pattern yearPattern = Pattern.compile("\\b(\\d{4})\\b");
        Matcher matcher = yearPattern.matcher(text);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return "unknown";
    }

    /* Returns the intersection of two lists. */
    private static List<Integer> intersection(List<Integer> list1, List<Integer> list2) {
        Set<Integer> set1 = new HashSet<>(list1);
        return list2.stream()
                .filter(set1::contains)
                .collect(Collectors.toList());
    }

    /* Builds the JSON response according to the API spec. */
    private static Map<String, Object> buildResponse(String query, String author, String language, String yearStr,
                                                     List<Map<String, Object>> results) {
        Map<String, Object> response = new HashMap<>();
        response.put("query", query);

        Map<String, Object> filters = new HashMap<>();
        if (author != null && !author.trim().isEmpty()) {
            filters.put("author", author);
        }
        if (language != null && !language.trim().isEmpty()) {
            filters.put("language", language);
        }
        if (yearStr != null && !yearStr.trim().isEmpty()) {
            filters.put("year", yearStr);
        }
        response.put("filters", filters);

        response.put("count", results.size());
        response.put("results", results);

        return response;
    }

    private static Map<String, Object> createEmptyResponse(String query, String author, String language, String yearStr) {
        return buildResponse(query, author, language, yearStr, new ArrayList<>());
    }
}