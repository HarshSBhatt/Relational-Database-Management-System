package org.group15.parser;

import org.group15.database.*;
import org.group15.sql.*;

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

  Alter alterSQL;

  Drop dropSQL;

  Delete deleteSQL;

  ERD erd;

  SQLDump sqlDump;

  DataDictionary dataDictionary;

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
    alterSQL = new Alter(eventLogsWriter);
    dropSQL = new Drop(eventLogsWriter);
    deleteSQL = new Delete(eventLogsWriter);
    erd = new ERD(eventLogsWriter);
    sqlDump = new SQLDump(eventLogsWriter);
    dataDictionary = new DataDictionary(eventLogsWriter);
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
    if (queryParts.length >= 2 && dbOperation.equalsIgnoreCase("CREATE")) {
      if (queryParts[1].equalsIgnoreCase("SCHEMA")) {
        if (queryParts.length > 2) {
          throw new Exception("Error: Wrong query for create schema");
        }
        dbOperation = "CREATE SCHEMA";
      }

      if (queryParts[1].equalsIgnoreCase("TABLE")) {
        dbOperation = "CREATE TABLE";
      }

      if (queryParts[1].equalsIgnoreCase("ERD")) {
        if (queryParts.length > 2) {
          throw new Exception("Error: Wrong query for create erd");
        }
        dbOperation = "CREATE ERD";
      }

      if (queryParts[1].equalsIgnoreCase("DUMP")) {
        if (queryParts.length > 2) {
          throw new Exception("Error: Wrong query for create dump");
        }
        dbOperation = "CREATE DUMP";
      }

      if (queryParts[1].equalsIgnoreCase("DD")) {
        if (queryParts.length > 2) {
          throw new Exception("Error: Wrong query for create data dictionary");
        }
        dbOperation = "CREATE DD";
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
//        selectedSchema = schema.getSchemaName();
        // Hard coding schema name for testing
        selectedSchema = "harsh";
        if (selectedSchema == null) {
          System.out.println("Error! Schema is not selected");
        } else {
          isValidSyntax = erd.generateERD(selectedSchema);
          if (isValidSyntax) {
            eventLogsWriter.append("[User: ").append(username).append("] [Query" +
                ": ").append(query).append("]\n");
            System.out.println("ERD created successfully");
          }
        }
        break;
      case "CREATE DUMP":
//        selectedSchema = schema.getSchemaName();
        // Hard coding schema name for testing
        selectedSchema = "harsh";
        if (selectedSchema == null) {
          System.out.println("Error! Schema is not selected");
        } else {
          isValidSyntax = sqlDump.generateDump(selectedSchema);
          if (isValidSyntax) {
            eventLogsWriter.append("[User: ").append(username).append("] [Query" +
                ": ").append(query).append("]\n");
          }
        }
        break;
      case "CREATE DD":
//        selectedSchema = schema.getSchemaName();
        // Hard coding schema name for testing
        selectedSchema = "harsh";
        if (selectedSchema == null) {
          System.out.println("Error! Schema is not selected");
        } else {
          isValidSyntax = dataDictionary.generateDataDictionary();
          if (isValidSyntax) {
            eventLogsWriter.append("[User: ").append(username).append("] [Query" +
                ": ").append(query).append("]\n");
          }
        }
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
//        selectedSchema = schema.getSchemaName();
        // Hard coding schema name for testing
        selectedSchema = "harsh";
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
//        selectedSchema = schema.getSchemaName();
        // Hard coding schema name for testing
        selectedSchema = "harsh";
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
      case "ALTER":
//        selectedSchema = schema.getSchemaName();
        // Hard coding schema name for testing
        selectedSchema = "harsh";
        if (selectedSchema == null) {
          System.out.println("Error! Schema is not selected");
        } else {
          isValidSyntax =
              alterSQL.parseAlterTableStatement(query,
                  selectedSchema);
          if (isValidSyntax) {
            eventLogsWriter.append("[User: ").append(username).append("] [Query" +
                ": ").append(query).append("]\n");
            tableName = queryParts[2].toLowerCase();
            table.setTableName(tableName);
            System.out.println("Table: " + tableName + " altered successfully");
          }
        }
        break;
      case "SELECT":
//        selectedSchema = schema.getSchemaName();
        // Hard coding schema name for testing
        selectedSchema = "harsh";
        if (selectedSchema == null) {
          System.out.println("Error! Schema is not selected");
        } else {
          isValidSyntax =
              selectSQL.parseSelectStatement(query,
                  selectedSchema);
          if (isValidSyntax) {
            eventLogsWriter.append("[User: ").append(username).append("] [Query" +
                ": ").append(query).append("]\n");
          }
        }
        break;
      case "DROP":
//        selectedSchema = schema.getSchemaName();
        // Hard coding schema name for testing
        selectedSchema = "harsh";
        if (selectedSchema == null) {
          System.out.println("Error! Schema is not selected");
        } else {
          isValidSyntax =
              dropSQL.parseDropTableStatement(query,
                  selectedSchema);
          if (isValidSyntax) {
            System.out.println("Table: " + queryParts[2] + " dropped " +
                "successfully from the schema: " + selectedSchema);
            eventLogsWriter.append("[User: ").append(username).append("] [Query" +
                ": ").append(query).append("]\n");
          }
        }
        break;
      case "DELETE":
//        selectedSchema = schema.getSchemaName();
        // Hard coding schema name for testing
        selectedSchema = "harsh";
        if (selectedSchema == null) {
          System.out.println("Error! Schema is not selected");
        } else {
          isValidSyntax =
              deleteSQL.parseDeleteStatement(query,
                  selectedSchema);
          if (isValidSyntax) {
            System.out.println("Delete operation performed successfully in " +
                "table: " + queryParts[2]);
            eventLogsWriter.append("[User: ").append(username).append("] [Query" +
                ": ").append(query).append("]\n");
          } else {
            System.out.println("Something went wrong while deleting row(s) in" +
                " table: " + queryParts[2] + "! Please check your query " +
                "syntax");
            eventLogsWriter.append("[User: ").append(username).append("] [Query" +
                ": ").append(query).append("]\n");
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
