package bigdatastage3;

import io.github.cdimascio.dotenv.Dotenv;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;

/**
 * Kleine REST-Schicht zum Auslösen der Orchestrierung.
 *
 * Endpoints:
 * GET /control/status
 * POST /control/run/{book_id}
 * POST /control/run-batch (Body: {"book_ids":[1,2,3]})
 * GET /control/processed (Liste bereits verarbeiteter IDs)
 */
public class ControllingUnit {

  // private static final Gson gson = new Gson();

  public static void main(String[] args) {
    Dotenv dotenv = Dotenv.load();

    String ingestionBase = dotenv.get("INGESTING_API");
    String indexingBase = dotenv.get("INDEXING_API");
    String searchBase = dotenv.get("SEARCH_API");
    String processedPath = dotenv.get("INDEXED_FILES");

    ControlModule control = new ControlModule(ingestionBase, indexingBase, searchBase, Path.of(processedPath));

    Scanner scanner = new Scanner(System.in);

    System.out.println("Please enter an ID of a book, ypu want to process: ");
    String idString = scanner.nextLine();
    scanner.close();
    int bookId;
    try {
      bookId = Integer.parseInt(idString);
    } catch (NumberFormatException e) {
      System.err.println("Book Id must be a number!");
      return;
    }

    System.out.println("Starting processing for book ID: " + bookId);
    try {
      String result = control.downloadBook(bookId);
      System.out.println(result);
      result = control.indexBook(bookId);
      System.out.println(result);
      for (String line : Files.readAllLines(Path.of(processedPath), StandardCharsets.UTF_8)) {
        if (line.trim().equals(String.valueOf(bookId)))
          System.out.println("Book: " + bookId + " got indexed successfully!");
      }
      throw new Exception("Book ID " + bookId + " was not found in the processed list.");
    } catch (Exception e) {
      System.err.println("An error occured during processing: " + e.getMessage());
    }
  }
}
