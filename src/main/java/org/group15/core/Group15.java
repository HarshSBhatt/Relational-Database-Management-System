package org.group15.core;

import org.group15.parser.QueryParser;
import org.group15.util.AppConstants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;

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
    QueryParser queryParser = new QueryParser(eventLogsWriter,
        generalLogsWriter, queryLogsWriter);

    boolean valid = true;

    while (valid) {
      try {
        System.out.print("Enter Query: ");
        String input = br.readLine();
        if (input.equalsIgnoreCase("exit")) {
          eventLogsWriter.close();
          generalLogsWriter.close();
          queryLogsWriter.close();
          valid = false;
        } else {
          queryParser.parse(input.trim(), username);
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }

}
