package org.group15.sql;

import org.group15.database.Column;
import org.group15.database.Table;
import org.group15.io.SchemaIO;
import org.group15.io.TableIO;
import org.group15.util.Helper;

import java.io.File;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Alter {

  SchemaIO schemaIO;

  TableIO tableIO;

  FileWriter eventLogsWriter;

  Table table;

  public Alter(FileWriter eventLogsWriter) {
    this.schemaIO = new SchemaIO();
    this.tableIO = new TableIO();
    this.eventLogsWriter = eventLogsWriter;
    table = new Table(eventLogsWriter);
  }

  public boolean parseAlterTableStatement(String query, String schemaName) throws Exception {
    String[] queryParts = query.split("\\s+");

    if (queryParts.length < 6 || queryParts.length > 8) {
      this.eventLogsWriter.append("Syntax error: Error while parsing alter " +
          "query").append("\n");
      this.eventLogsWriter.close();
      throw new Exception("Syntax error: Error while parsing alter query");
    }

    if (!queryParts[1].equalsIgnoreCase("TABLE")) {
      this.eventLogsWriter.append("Syntax error: TABLE keyword not found " +
          "in Alter query").append("\n");
      this.eventLogsWriter.close();
      throw new Exception("Syntax error: TABLE keyword not found in " +
          "Alter query");
    }
    boolean hasSixWords = false, hasEightWords = false, isDropQuery = false;
    switch (queryParts[3].toUpperCase()) {
      case "ADD":
        hasSixWords = true;
        break;
      case "DROP":
        hasSixWords = true;
        isDropQuery = true;
        break;
      case "CHANGE":
        if (!queryParts[4].equalsIgnoreCase("COLUMN")) {
          this.eventLogsWriter.append("Syntax error: Unknown keyword " +
              "encountered while parsing alter table statement: ").append(queryParts[4]).append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Syntax error: Unknown keyword encountered while" +
              " parsing alter table statement: " + queryParts[4]);
        }
        hasEightWords = true;
        break;
      default:
        this.eventLogsWriter.append("Syntax error: Unknown keyword " +
            "encountered while parsing alter table statement: ").append(queryParts[3]).append("\n");
        this.eventLogsWriter.close();
        throw new Exception("Syntax error: Unknown keyword encountered while" +
            " parsing alter table statement: " + queryParts[3]);
    }

    String tableName = queryParts[2];

    String tablePath = Helper.getTablePath(schemaName, tableName);

    File tableFile = new File(tablePath);

    if (!tableFile.exists()) {
      this.eventLogsWriter.append("Something went wrong! Table does not " +
          "exist: ").append(tableName).append("\n");
      this.eventLogsWriter.close();
      throw new Exception("Something went wrong! Table does not exist: " + tableName);
    }

    String existingColumnName;
    String newColumn;
    // For eg: varchar(100) -> varchar 100, if int -> int
    String[] dataTypeRelatedInfo;
    HashSet<String> validDataTypes = Helper.getDataTypes();
    Map<String, Column> columns =
        this.table.getTableMetadataMap(schemaName, tableName);

    // If it is 6 words, then it must be a add or drop column query
    if (hasSixWords) {

      // If this is true, then it will be droop query, else it will be add query
      if (isDropQuery) {
        existingColumnName = queryParts[queryParts.length - 1];

        // Query: alter table users drop column last_name
        boolean exist = isColExist(existingColumnName, columns);
        if (!exist) {
          this.eventLogsWriter.append("Error: Unknown column: ").append(existingColumnName).append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Error: Unknown column: " + existingColumnName);
        }

        boolean isDropped = this.table.dropColumn(schemaName, tableName,
            existingColumnName);
        if (!isDropped) {
          this.eventLogsWriter.append("Error: Something went wrong while " +
              "dropping the column").append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Error: Something went wrong while dropping the column");
        }
      } else {
        newColumn = queryParts[4];
        dataTypeRelatedInfo = queryParts[queryParts.length - 1].replaceAll("[^a" +
            "-zA-Z,0-9_]", " ").split(" ");

        if (!validDataTypes.contains(dataTypeRelatedInfo[0])) {
          this.eventLogsWriter.append("Syntax error: Unknown data type " +
              "encountered while parsing alter table statement: ").append(dataTypeRelatedInfo[0]).append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Syntax error: Unknown data type encountered while" +
              " parsing alter table statement: " + dataTypeRelatedInfo[0]);
        }

        // Query: alter table users add income varchar(100)
        boolean exist = Helper.isColumnExist(columns, newColumn);
        if (exist) {
          this.eventLogsWriter.append("Error: Column already exist: ").append(newColumn).append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Error: Column already exist: " + newColumn);
        }
        boolean isAdded = this.table.addColumn(schemaName, tableName,
            newColumn, dataTypeRelatedInfo);
        if (!isAdded) {
          this.eventLogsWriter.append("Error: Something went wrong while " +
              "adding the column").append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Error: Something went wrong while adding the column");
        }
      }
    }

    // If it is 8 words, then it must be a change column query
    if (hasEightWords) {
      existingColumnName = queryParts[5];
      newColumn = queryParts[6];
      dataTypeRelatedInfo = queryParts[queryParts.length - 1].replaceAll("[^a" +
          "-zA-Z,0-9_]", " ").split(" ");

      if (!validDataTypes.contains(dataTypeRelatedInfo[0])) {
        this.eventLogsWriter.append("Syntax error: Unknown data type " +
            "encountered while parsing alter table statement: ").append(dataTypeRelatedInfo[0]).append("\n");
        this.eventLogsWriter.close();
        throw new Exception("Syntax error: Unknown data type encountered while" +
            " parsing alter table statement: " + dataTypeRelatedInfo[0]);
      }

      // Query: alter table users change column last_name l_name varchar(100)
      boolean exist = isColExist(existingColumnName, columns);
      if (!exist) {
        this.eventLogsWriter.append("Error: Unknown column: ").append(existingColumnName).append("\n");
        this.eventLogsWriter.close();
        throw new Exception("Error: Unknown column: " + existingColumnName);
      }
      boolean isChanged = this.table.changeColumn(schemaName, tableName,
          existingColumnName, newColumn, dataTypeRelatedInfo);
      if (!isChanged) {
        this.eventLogsWriter.append("Error: Something went wrong while " +
            "changing the column").append("\n");
        this.eventLogsWriter.close();
        throw new Exception("Error: Something went wrong while changing the column");
      }
    }

    return true;
  }

  public boolean isColExist(String existingColumnName, Map<String, Column> columns) throws Exception {
    boolean exist = false;
    for (String key : columns.keySet()) {
      Column column = columns.get(key);
      if (column.getColumnName().equalsIgnoreCase(existingColumnName)) {
        exist = true;
        if (column.isPrimaryKey()) {
          this.eventLogsWriter.append("Error: Primary key can not be " +
              "dropped").append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Error: Primary key can not be dropped");
        } else if (column.isForeignKey()) {
          this.eventLogsWriter.append("Error: Foreign key can not be " +
              "dropped").append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Error: Foreign key can not be dropped");
        }
      }
    }
    return exist;
  }

}
