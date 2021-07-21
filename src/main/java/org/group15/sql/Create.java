package org.group15.sql;

import org.group15.database.Column;
import org.group15.database.Table;
import org.group15.io.SchemaIO;
import org.group15.io.TableIO;
import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.util.*;

public class Create {

  SchemaIO schemaIO;

  TableIO tableIO;

  Table table = new Table();

  public Create() {
    this.schemaIO = new SchemaIO();
    this.tableIO = new TableIO();
  }

  public boolean parseCreateSchemaStatement(int size, String[] queryParts) {
    boolean isValidSyntax = false;
    if (size == 3 && queryParts[1].equalsIgnoreCase("SCHEMA")) {
      String schemaName = queryParts[2].toLowerCase();
      isValidSyntax = schemaIO.create(schemaName);
    } else {
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
        // tableRelatedStatement: create table harsh
        String tableRelatedStatement = query.substring(0, query.indexOf("("));

        // columnRelatedStatement: user_id int, last_name varchar(255), first_name varchar(255), address varchar(255), country varchar(255), PRIMARY KEY (user_id)
        String columnRelatedStatement = query.substring(query.indexOf("(") + 1,
            query.lastIndexOf(")"));

        String[] tableParts = tableRelatedStatement.trim().split("\\s+");

        if (tableParts.length == 3) {
          // tableParts[2]: harsh
          table.setTableName(tableParts[2].toLowerCase());

          // columnRelatedStatement: user_id int, last_name varchar 255, first_name varchar 255, address varchar 255, country varchar 255, PRIMARY KEY user_id
          String[] columns = columnRelatedStatement.replaceAll("[^a-zA-Z,0-9_]", " ").split(",");

          Map<String, Column> tableColumns = new HashMap<>();

          for (String column : columns) {
            //
            if (column.toUpperCase().contains(AppConstants.PRIMARY_KEY)) {
              // Ignoring any number of whitespace between words
              String[] currentColumnValues = column.trim().split("\\s+");
              if (currentColumnValues.length == 3 && tableColumns.containsKey(currentColumnValues[2])) {
                String columnName = currentColumnValues[2];
                Column primaryKeyColObj = tableColumns.get(columnName);
                primaryKeyColObj.setPrimaryKey(true);
                tableColumns.put(columnName, primaryKeyColObj);
              } else {
                throw new Exception("Syntax error: Error occurred while " +
                    "parsing primary key syntax");
              }
            } else if (column.toUpperCase().contains(AppConstants.FOREIGN_KEY)) {
              // Ignoring any number of whitespace between words
              String[] currentColumnValues = column.trim().split("\\s+");
              // Example: FOREIGN KEY user_id REFERENCES Users user_id
              if (currentColumnValues.length == 6 && tableColumns.containsKey(currentColumnValues[2])) {
                String columnName = currentColumnValues[2];
                Column foreignKeyColObj = tableColumns.get(columnName);
                foreignKeyColObj.setForeignKey(true);
                foreignKeyColObj.setForeignKeyColumn(currentColumnValues[5]);
                foreignKeyColObj.setForeignKeyTable(currentColumnValues[4]);
                tableColumns.put(columnName, foreignKeyColObj);
              } else {
                throw new Exception("Syntax error: Error occurred while " +
                    "parsing foreign key syntax");
              }
            } else {
              Column tableColumn = new Column();

              // Ignoring any number of whitespace between words
              String[] currentColumnValues = column.trim().split("\\s+");

              if (currentColumnValues.length >= 2) {
                String columnName = currentColumnValues[0];
                String columnType = currentColumnValues[1];
                tableColumn.setColumnName(columnName);
                tableColumn.setColumnDataType(columnType);
                if (currentColumnValues.length == 3) {
                  tableColumn.setColumnSize(Integer.parseInt(currentColumnValues[2].trim()));
                }

                tableColumns.put(columnName, tableColumn);
              } else {
                throw new Exception("Syntax error: Error occurred while " +
                    "parsing table columns");
              }
            }
          }
          table.create(tableColumns, schemaName, table.getTableName());
          isValidSyntax = true;
        }
      } else {
        throw new Exception("Syntax error: Syntax error: Error occurred due " +
            "to mismatch parenthesis");
      }
    } else {
      throw new Exception("Syntax error: Please check your syntax for Create Table");
    }
    return isValidSyntax;
  }

}
