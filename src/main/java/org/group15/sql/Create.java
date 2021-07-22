package org.group15.sql;

import org.group15.database.Column;
import org.group15.database.Table;
import org.group15.io.SchemaIO;
import org.group15.io.TableIO;
import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Create {

  SchemaIO schemaIO;

  TableIO tableIO;

  FileWriter eventLogsWriter;

  Table table;

  public Create(FileWriter eventLogsWriter) {
    this.schemaIO = new SchemaIO();
    this.tableIO = new TableIO();
    this.eventLogsWriter = eventLogsWriter;
    table = new Table(eventLogsWriter);
  }

  public boolean parseCreateSchemaStatement(int size, String[] queryParts) throws IOException {
    boolean isValidSyntax = false;
    if (size == 3 && queryParts[1].equalsIgnoreCase("SCHEMA")) {
      String schemaName = queryParts[2].toLowerCase();
      isValidSyntax = schemaIO.create(schemaName);
      this.eventLogsWriter.append("Schema created with name: ").append(schemaName).append("\n");
    } else {
      this.eventLogsWriter.append("Syntax error: Please check your syntax for" +
          " Create Schema").append("\n");
      System.out.println("Syntax error: Please check your syntax for Create Schema");
    }
    return isValidSyntax;
  }

  /**
   * @param query:      create table harsh (PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255))
   * @param schemaName: database name
   * @return true or false based on parsing
   */
  public boolean parseCreateTableStatement(String query, String schemaName) throws Exception {
    boolean isValidSyntax = false;
    if (query.contains("(")) {
      // We wil count the occurrence of ')' & '(' in the query
      long countOfOpeningBrace = Helper.getOccurrenceOf(query, '(');
      long countOfClosingBrace = Helper.getOccurrenceOf(query, ')');

      // If ')' & '(' count is different then it is a syntax error
      if (countOfOpeningBrace == countOfClosingBrace) {
        // tableRelatedStatement: create table users
        String tableRelatedStatement = query.substring(0, query.indexOf("("));

        // columnRelatedStatement: user_id int, last_name varchar(255), first_name varchar(255), address varchar(255), country varchar(255), PRIMARY KEY (user_id)
        String columnRelatedStatement = query.substring(query.indexOf("(") + 1,
            query.lastIndexOf(")"));

        String[] tableParts = tableRelatedStatement.trim().split("\\s+");

        if (tableParts.length == 3) {
          // tableParts[2]: users
          table.setTableName(tableParts[2]);

          // columnRelatedStatement: user_id int, last_name varchar 255, first_name varchar 255, address varchar 255, country varchar 255, PRIMARY KEY user_id
          String[] columns = columnRelatedStatement.replaceAll("[^a-zA-Z,0-9_]", " ").split(",");

          Map<String, Column> tableColumns = new HashMap<>();
          HashSet<String> validDataTypes = Helper.getDataTypes();
          for (String column : columns) {
            if (column.toUpperCase().contains(AppConstants.PRIMARY_KEY)) {
              // Ignoring any number of whitespace between words
              String[] currentColumnValues = column.trim().split("\\s+");
              if (currentColumnValues.length == 3 && tableColumns.containsKey(currentColumnValues[2])) {
                String columnName = currentColumnValues[2];
                Column primaryKeyColObj = tableColumns.get(columnName);
                primaryKeyColObj.setPrimaryKey(true);
                tableColumns.put(columnName, primaryKeyColObj);
              } else {
                this.eventLogsWriter.append("Syntax error: Error occurred while parsing primary key syntax").append("\n");
                throw new Exception("Syntax error: Error occurred while " +
                    "parsing primary key syntax");
              }
            } else if (column.toUpperCase().contains(AppConstants.FOREIGN_KEY)) {
              // Ignoring any number of whitespace between words
              String[] currentColumnValues = column.trim().split("\\s+");
              // Example: FOREIGN KEY user_id REFERENCES users user_id
              if (currentColumnValues.length == 6 && tableColumns.containsKey(currentColumnValues[2])) {
                String columnName = currentColumnValues[2];
                Column foreignKeyColObj = tableColumns.get(columnName);
                foreignKeyColObj.setForeignKey(true);
                foreignKeyColObj.setForeignKeyColumn(currentColumnValues[5]);
                foreignKeyColObj.setForeignKeyTable(currentColumnValues[4]);
                tableColumns.put(columnName, foreignKeyColObj);
              } else {
                this.eventLogsWriter.append("Syntax error: Error occurred " +
                    "while parsing foreign key syntax").append("\n");
                throw new Exception("Syntax error: Error occurred while " +
                    "parsing foreign key syntax");
              }
            } else {
              Column tableColumn = new Column();

              // Ignoring any number of whitespace between words
              String[] currentColumnValues = column.trim().split("\\s+");

              if (currentColumnValues.length >= 2) {
                String columnName = currentColumnValues[0];
                String columnDataType = currentColumnValues[1];
                if (!validDataTypes.contains(columnDataType)) {
                  throw new Exception("Unknown Datatype: " + columnDataType);
                }
                tableColumn.setColumnName(columnName);
                tableColumn.setColumnDataType(columnDataType);
                if (currentColumnValues.length == 3 && Helper.isInteger(currentColumnValues[2])) {
                  tableColumn.setColumnSize(Integer.parseInt(currentColumnValues[2].trim()));
                }
                if (currentColumnValues.length >= 3 && column.toUpperCase().contains(AppConstants.AUTO_INCREMENT)) {
                  tableColumn.setAutoIncrement(true);
                }
                tableColumns.put(columnName, tableColumn);
              } else {
                this.eventLogsWriter.append("Syntax error: Error occurred " +
                    "while parsing table columns").append("\n");
                throw new Exception("Syntax error: Error occurred while " +
                    "parsing table columns");
              }
            }
          }
          table.create(tableColumns, schemaName, table.getTableName());
          this.eventLogsWriter.append("Table: ").append(table.getTableName()).append(" created in schema: ").append(schemaName).append("\n");
          isValidSyntax = true;
        }
      } else {
        this.eventLogsWriter.append("Syntax error: Syntax error: Error " +
            "occurred due to mismatch parenthesis").append("\n");
        throw new Exception("Syntax error: Syntax error: Error occurred due " +
            "to mismatch parenthesis");
      }
    } else {
      this.eventLogsWriter.append("Syntax error: Please check your syntax for Create Table").append("\n");
      throw new Exception("Syntax error: Please check your syntax for Create Table");
    }
    return isValidSyntax;
  }

}
