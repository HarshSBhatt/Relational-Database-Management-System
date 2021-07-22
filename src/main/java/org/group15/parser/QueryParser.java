package org.group15.parser;

import org.group15.database.ERD;
import org.group15.database.Schema;
import org.group15.database.Table;
import org.group15.sql.Create;
import org.group15.sql.Insert;
import org.group15.sql.Select;
import org.group15.sql.Show;

import java.io.File;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.Date;

public class QueryParser {

  FileWriter eventLogsWriter;

  FileWriter generalLogsWriter;

  FileWriter queryLogsWriter;

  Schema schema = new Schema();

  Table table;

  Create createSQL;

  Insert insertSQL;

  Select selectSQL;

  Show showSQL;

  ERD erd;

  public QueryParser(FileWriter eventLogsWriter, FileWriter generalLogsWriter
      , FileWriter queryLogsWriter) {
    this.eventLogsWriter = eventLogsWriter;
    this.generalLogsWriter = generalLogsWriter;
    this.queryLogsWriter = queryLogsWriter;
    table = new Table(eventLogsWriter);
    createSQL = new Create(eventLogsWriter);
    insertSQL = new Insert(eventLogsWriter);
    selectSQL = new Select(eventLogsWriter);
    showSQL = new Show(eventLogsWriter);
    erd = new ERD(eventLogsWriter);
  }

  public void parse(String query, String username) throws Exception {
    int size;

    String schemaName;

    boolean isValidSyntax;

    String tableName;

    String selectedSchema;

    long queryStartTime, queryEndTime, elapsedTime;

    // Ignoring any number of whitespace between words
    String[] queryParts = query.split("\\s+");

    String dbOperation = queryParts[0];

    // Checking whether CREATE statement is for SCHEMA or TABLE or ERD
    if (queryParts.length >= 2) {
      if (queryParts[1].equalsIgnoreCase("SCHEMA")) {
        if (queryParts.length > 2) {
          throw new Exception("Error: Wrong query for create schema");
        }
        dbOperation = "CREATE SCHEMA";
      }

      if (queryParts.length >= 2 && queryParts[1].equalsIgnoreCase("TABLE")) {
        dbOperation = "CREATE TABLE";
      }

      if (queryParts[1].equalsIgnoreCase("ERD")) {
        if (queryParts.length > 2) {
          throw new Exception("Error: Wrong query for create erd");
        }
        dbOperation = "CREATE ERD";
      }

    }

    // Logs related logic
    Date date = new Date();
    // getTime() returns current time in milliseconds
    long time = date.getTime();
    // Passed the milliseconds to constructor of Timestamp class
    Timestamp ts = new Timestamp(time);

    queryStartTime = System.nanoTime();
    switch (dbOperation.toUpperCase()) {
      /**
       * SCHEMA related operations
       */
      case "CREATE ERD":
        System.out.println("Here in ERD case at QueryParser.java");
        break;
      case "CREATE SCHEMA":
        size = queryParts.length;
        isValidSyntax = createSQL.parseCreateSchemaStatement(size,
            queryParts);
        if (isValidSyntax) {
          eventLogsWriter.append("[User: ").append(username).append("] [Query" +
              ": ").append(query).append("]\n");
          schemaName = queryParts[2].toLowerCase();
          schema.setSchemaName(schemaName);
        }
        break;
      case "USE":
        size = queryParts.length;
        isValidSyntax = selectSQL.parseUseSchemaStatement(size, queryParts);
        if (isValidSyntax) {
          eventLogsWriter.append("[User: ").append(username).append("] [Query" +
              ": ").append(query).append("]\n");
          schemaName = queryParts[1].toLowerCase();
          schema.setSchemaName(schemaName);
        }
        break;
      case "SHOW":
        size = queryParts.length;
        showSQL.parseShowSchemaStatement(size, queryParts);
        eventLogsWriter.append("[User: ").append(username).append("] [Query" +
            ": ").append(query).append("]\n");
        break;
      /**
       * TABLE related operations
       */
      case "CREATE TABLE":
        selectedSchema = schema.getSchemaName();
        if (selectedSchema == null) {
          System.out.println("Error! Schema is not selected");
        } else {
          isValidSyntax = createSQL.parseCreateTableStatement(query.toLowerCase(),
              selectedSchema);
          if (isValidSyntax) {
            eventLogsWriter.append("[User: ").append(username).append("] [Query" +
                ": ").append(query).append("]\n");
            tableName = queryParts[2].toLowerCase();
            table.setTableName(tableName);
            System.out.println("Table: " + tableName + " created successfully");
          }
        }
        break;
      case "INSERT":
        selectedSchema = schema.getSchemaName();
        if (selectedSchema == null) {
          System.out.println("Error! Schema is not selected");
        } else {
          isValidSyntax =
              insertSQL.parseInsertTableStatement(query,
                  selectedSchema);
          if (isValidSyntax) {
            eventLogsWriter.append("[User: ").append(username).append("] [Query" +
                ": ").append(query).append("]\n");
            tableName = queryParts[2].toLowerCase();
            table.setTableName(tableName);
            System.out.println("Record inserted successfully into table: " + tableName);
          }
        }
        break;
      default:
        System.out.println("Unexpected query: " + dbOperation);
        break;
    }

    // Logs related logic
    queryEndTime = System.nanoTime();
    elapsedTime = queryEndTime - queryStartTime;

    if (schema.getSchemaName() != null) {
      File file = new File("schemas/" + schema.getSchemaName() + "/tables");
      int totalTables = file.listFiles().length;
      generalLogsWriter.append("[Execution Time: ").append(String.valueOf(elapsedTime)).append(
          "ns] [Database Stat: Total tables -> ").append(String.valueOf(totalTables)).append("]\n");
    } else {
      generalLogsWriter.append("[Execution Time: ").append(String.valueOf(elapsedTime)).append(
          "ns] [Database Stat: ").append("No Schema was selected! Database " +
          "stat can not be retrieved").append("]\n");
    }
    queryLogsWriter.append("[Timestamp: ").append(String.valueOf(ts)).append(
        "] [Query Type: ").append(dbOperation).append("] [Query: ").append(query).append("]\n");
  }

}
