package org.group15.core;

import org.group15.parser.QueryParser;
import org.group15.util.AppConstants;

import java.io.*;

public class Group15 {

  public void rdbmsProvider(String username) throws Exception {
    // Default General Log File
    File generalLogs = new File(AppConstants.GENERAL_LOG_FILENAME);
    // Default Event Log File
    File eventLogs = new File(AppConstants.EVENT_LOG_FILENAME);

    if (generalLogs.createNewFile()) {
      System.out.println("New General Logs created!");
    }
    if (eventLogs.createNewFile()) {
      System.out.println("New Event Logs created!");
    }

    // True indicates that data or text will be appended
    FileWriter eventLogsWriter = new FileWriter(eventLogs, true);
    FileWriter generalLogsWriter = new FileWriter(generalLogs, true);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    QueryParser queryParser = new QueryParser(eventLogsWriter, generalLogsWriter);

    boolean valid = true;
    /**
     *
     */
    String inputWithoutFK = "create table Users (user_id int, last_name " +
        "varchar(255), first_name varchar(255), address varchar(255), country" +
        " varchar(255), PRIMARY KEY (user_id))";

    String inputWithFK = "create table Users (user_id int, last_name varchar" +
        "(255), " +
        "first_name varchar(255), address varchar(255), country varchar(255)," +
        " role_id int, PRIMARY KEY (user_id)), FOREIGN KEY role_id REFERENCES" +
        " roles (role_id)";
    queryParser.parse(inputWithoutFK, username);
    eventLogsWriter.close();
    generalLogsWriter.close();
    /**
     *
     */
//    while (valid) {
//      try {
//        String input = br.readLine();
//        if (input.equalsIgnoreCase("exit")) {
//          valid = false;
//          eventLogsWriter.close();
//          generalLogsWriter.close();
//        } else {
//          queryParser.parse(input);
//        }
//      } catch (Exception e) {
//        System.out.println("Invalid Input!");
//        System.out.println(e.getMessage());
//      }
//    }
  }

}
