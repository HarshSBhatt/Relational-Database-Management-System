package org.group15.core;

import org.group15.parser.QueryParser;
import org.group15.util.AppConstants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Group15 {

  public void rdbmsProvider(String username) throws Exception {
    // Default General Log File
    File generalLogs = new File(AppConstants.GENERAL_LOG_FILENAME);
    // Default Event Log File
    File eventLogs = new File(AppConstants.EVENT_LOG_FILENAME);
    // Default Query Log File
    File queryLogs = new File(AppConstants.QUERY_LOG_FILENAME);

    if (generalLogs.createNewFile()) {
      System.out.println("New General Logs created!");
    }

    if (eventLogs.createNewFile()) {
      System.out.println("New Event Logs created!");
    }

    if (queryLogs.createNewFile()) {
      System.out.println("New Query Logs created!");
    }

    // True indicates that data or text will be appended
    FileWriter eventLogsWriter = new FileWriter(eventLogs, true);
    FileWriter generalLogsWriter = new FileWriter(generalLogs, true);
    FileWriter queryLogsWriter = new FileWriter(queryLogs, true);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    List<String> queries = new ArrayList<>();


    QueryParser queryParserWithoutCommit =
        new QueryParser(eventLogsWriter,
            generalLogsWriter, queryLogsWriter, true, false);

    QueryParser queryParserWithCommit =
        new QueryParser(eventLogsWriter,
            generalLogsWriter, queryLogsWriter, false, false);

    QueryParser queryParserWithCommitAndBulkOperation =
        new QueryParser(eventLogsWriter,
            generalLogsWriter, queryLogsWriter, false, true);

    boolean valid = true;

    while (valid) {
      try {
        System.out.print("Enter Query: ");
        String input = br.readLine();
        boolean transaction;
        if (input.equalsIgnoreCase("START TRANSACTION")) {
          transaction = true;
          while (transaction) {
            System.out.print("Enter Transaction Query: ");
            String transactionInput = br.readLine();
            if (transactionInput.equalsIgnoreCase("COMMIT")) {
              if (queries.size() > 0) {
                for (String query : queries) {
                  queryParserWithoutCommit.parse(query, username);
                }

                for (String query : queries) {
                  queryParserWithCommitAndBulkOperation.parse(query, username);
                }
              } else {
                System.out.println("No queries to execute in this transaction");
              }
              transaction = false;
            } else if (transactionInput.equalsIgnoreCase("ROLLBACK")) {
              System.out.println("Transaction is successfully rolled back. To" +
                  " close program, type 'exit'");
              transaction = false;
              queries.removeAll(queries);
            } else {
              queries.add(transactionInput);
            }
          }
        } else if (input.equalsIgnoreCase("EXIT")) {
          eventLogsWriter.close();
          generalLogsWriter.close();
          queryLogsWriter.close();
          valid = false;
        } else {
          queryParserWithCommit.parse(input.trim(), username);
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }

}
