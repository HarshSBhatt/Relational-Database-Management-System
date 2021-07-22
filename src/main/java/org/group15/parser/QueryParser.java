package org.group15.parser;

import org.group15.database.Schema;
import org.group15.database.Table;
import org.group15.sql.Create;
import org.group15.sql.Insert;
import org.group15.sql.Select;
import org.group15.sql.Show;
import org.group15.util.AppConstants;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

public class QueryParser {

  FileWriter eventLogsWriter;

  FileWriter generalLogsWriter;

  FileWriter queryLogsWriter;

  Schema schema = new Schema();

  Table table = new Table();

  Create createSQL = new Create();

  Insert insertSQL = new Insert();

  Select selectSQL = new Select();

  Show showSQL = new Show();

  public QueryParser(FileWriter eventLogsWriter, FileWriter generalLogsWriter
      , FileWriter queryLogsWriter) {
    this.eventLogsWriter = eventLogsWriter;
    this.generalLogsWriter = generalLogsWriter;
    this.queryLogsWriter = queryLogsWriter;
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

    // Checking whether CREATE statement is for SCHEMA or TABLE
    if (queryParts.length >= 2 && queryParts[1].equalsIgnoreCase("SCHEMA")) {
      dbOperation = "CREATE SCHEMA";
    }

    if (queryParts.length >= 2 && queryParts[1].equalsIgnoreCase("TABLE")) {
      dbOperation = "CREATE TABLE";
    }

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
        selectedSchema = "harsh";
        //  selectedSchema = schema.getSchemaName();
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
        selectedSchema = "harsh";
        //  selectedSchema = schema.getSchemaName();
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
    queryEndTime = System.nanoTime();
    elapsedTime = queryEndTime - queryStartTime;
    queryLogsWriter.append("[Timestamp: ").append(String.valueOf(ts)).append(
        "] [Query Type: ").append(dbOperation).append("] [Query: ").append(query).append("]\n");
    generalLogsWriter.append("[Execution Time: ").append(String.valueOf(elapsedTime)).append(
        "ns] [Query Type: ").append(dbOperation).append("] [Query: ").append(query).append("]\n");
  }

}
