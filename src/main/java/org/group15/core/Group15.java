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

    /**
     * Delete this when testing using user input
     */
    String roleDummyQuery = "create table roles (role_id int, role_name " +
        "varchar(255), PRIMARY KEY (role_id))";

    String createTableQueryWithoutFK = "create table Users (user_id int, " +
        "last_name " +
        "varchar(255), first_name varchar(255), address varchar(255), country" +
        " varchar(255), PRIMARY KEY (user_id))";

    String createTableQueryWithFK = "create table Users (user_id int, last_name varchar(255)," +
        " first_name varchar(255), address varchar(255), country varchar(255)," +
        " role_id int, PRIMARY KEY (user_id), FOREIGN KEY role_id" +
        " REFERENCES roles (role_id))";

    String insertQuery = "insert into roles (role_id,role_name) " +
        "values (2,'User')";

    String insertUserQuery = "insert into users (user_id,last_name," +
        "first_name,address,country,role_id) " +
        "values (15,'Bhatt','Harsh','Gujarat','India',1)";

    String createErd = "create erd";

    String createDump = "create dump";

    String alterAddTableQuery = "alter table users add income varchar(100)";
    String alterDropTableQuery = "alter table users drop column last_name";
    String alterChangeTableQuery = "alter table users change column last_name" +
        " l_name varchar(100)";
    String createDataDictionary = "create dd";

    String selectQuery = "select first_name,last_name,user_id from users " +
        "where last_name='Bhatt' or user_id=1";

    String dropQuery = "drop table roles";

//    queryParser.parse(roleDummyQuery, username);
//    queryParser.parse(createTableQueryWithoutFK, username);
//    queryParser.parse(createTableQueryWithFK, username);
//    queryParser.parse(insertUserQuery.trim(), username);
//    queryParser.parse(createDump.trim(), username);
//    queryParser.parse(alterAddTableQuery.trim(), username);
//    queryParser.parse(alterDropTableQuery.trim(), username);
//    queryParser.parse(alterChangeTableQuery.trim(), username);
//    queryParser.parse(createDataDictionary.trim(), username);
//    queryParser.parse(selectQuery.trim(), username);
    queryParser.parse(dropQuery.trim(), username);

    eventLogsWriter.close();
    generalLogsWriter.close();
    queryLogsWriter.close();


    /**
     * Uncomment this when testing using user input
     */
    //    while (valid) {
    //      try {
    //        String input = br.readLine();
    //        if (input.equalsIgnoreCase("exit")) {
    //          valid = false;
    //          eventLogsWriter.close();
    //          generalLogsWriter.close();
    //          queryLogsWriter.close();
    //        } else {
    //          queryParser.parse(input.trim(), username);
    //        }
    //      } catch (Exception e) {
    //        System.out.println("Invalid Input!");
    //        System.out.println(e.getMessage());
    //      }
    //    }
  }

}
