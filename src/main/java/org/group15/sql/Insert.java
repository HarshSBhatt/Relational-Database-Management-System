package org.group15.sql;

import org.group15.database.Column;
import org.group15.database.Table;
import org.group15.io.TableIO;
import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.io.*;
import java.util.*;

public class Insert {

  private TableIO tableIO;

  private Map<String, Column> columns;

  Column primaryKeyColumn;

  Column foreignKeyColumn;

  FileWriter eventLogsWriter;

  Table table;

  public Insert(FileWriter eventLogsWriter) {
    this.tableIO = new TableIO();
    this.columns = new HashMap<>();
    this.primaryKeyColumn = null;
    this.foreignKeyColumn = null;
    this.eventLogsWriter = eventLogsWriter;
    table = new Table(eventLogsWriter);
  }

  /**
   * Assumptions: No whitespace between column names and values of columns
   * For example: insert into roles (role_id,role_name) values (1,'Admin')
   * This way if we split query on white space, we will get exact 6 parts
   * For example: [insert, into, roles, (role_id,role_name), values, (1,'Admin')]
   */
  public boolean parseInsertTableStatement(String query, String schemaName) throws Exception {
    if (query.contains("(")) {
      // We wil count the occurrence of ')' & '(' in the query
      long countOfOpeningBrace = Helper.getOccurrenceOf(query, '(');
      long countOfClosingBrace = Helper.getOccurrenceOf(query, ')');

      // If ')' & '(' count is different then it is a syntax error
      if (countOfOpeningBrace == countOfClosingBrace) {
        String[] queryParts = query.split("\\s+");

        if (queryParts.length != 6) {
          this.eventLogsWriter.append("Syntax error: Error while parsing insert query").append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Syntax error: Error while parsing insert query");
        }

        if (!queryParts[1].equalsIgnoreCase("INTO")) {
          this.eventLogsWriter.append("Syntax error: INTO keyword not found " +
              "in Insert query").append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Syntax error: INTO keyword not found in " +
              "Insert query");
        }

        if (!queryParts[4].equalsIgnoreCase("VALUES")) {
          this.eventLogsWriter.append("Syntax error: VALUES keyword not found" +
              " in Insert query").append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Syntax error: VALUES keyword not found in " +
              "Insert query");
        }

        String tableName = queryParts[2];
        String queryColumns = queryParts[3];
        String queryValues = queryParts[queryParts.length - 1];

        if (!validatedValuesBetweenParenthesis(queryColumns)) {
          this.eventLogsWriter.append("Syntax error: Error while parsing " +
              "parenthesis of columns").append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Syntax error: Error while parsing parenthesis " +
              "of columns");
        }

        if (!validatedValuesBetweenParenthesis(queryValues)) {
          this.eventLogsWriter.append("Syntax error: Error while parsing " +
              "parenthesis of values").append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Syntax error: Error while parsing parenthesis " +
              "of values");
        }

        // Checking if table exists or not
        if (tableIO.isTableExist(schemaName, tableName)) {
          String[] columnsArray =
              queryColumns.replaceAll("[)(]", "").split(",");
          String[] valuesArray =
              queryValues.replaceAll("[)(]", "").split(",");

          // If column and value length is different, then e will throw error
          if (columnsArray.length == valuesArray.length) {
            // Loading table metadata
            boolean isTableDataLoaded = getTableMetadata(schemaName, tableName);

            if (isTableDataLoaded) {
              // Checking if values has correct type and constraint related to table
              boolean isColAndValCorrectlyMapped =
                  validateColWithVal(columnsArray, valuesArray);

              if (isColAndValCorrectlyMapped) {
                // If it is true, then we will insert data to the file/table
                boolean isDataWritten = table.insert(schemaName, tableName,
                    columnsArray, this.columns, this.primaryKeyColumn, this.foreignKeyColumn);

                if (!isDataWritten) {
                  this.eventLogsWriter.append("Error: Something went wrong " +
                      "while writing data to table").append("\n");
                  this.eventLogsWriter.close();
                  throw new Exception("Error: Something went wrong while " +
                      "writing data to table");
                }
                this.eventLogsWriter.append("Data inserted successfully in " +
                    "the table: ").append(tableName).append("\n");
              } else {
                this.eventLogsWriter.append("Error: Something went wrong while " +
                    "fetching table data").append("\n");
                this.eventLogsWriter.close();
                throw new Exception("Error: Something went wrong while " +
                    "fetching table data");
              }
            } else {
              this.eventLogsWriter.append("Error: Something went wrong while " +
                  "fetching table data").append("\n");
              this.eventLogsWriter.close();
              throw new Exception("Error: Something went wrong while " +
                  "fetching table data");
            }
          } else {
            this.eventLogsWriter.append("Error: Columns and its value " +
                "mismatch").append("\n");
            this.eventLogsWriter.close();
            throw new Exception("Error: Columns and its value mismatch");
          }
        } else {
          this.eventLogsWriter.append("Error: Invalid table name").append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Error: Invalid table name");
        }

      } else {
        this.eventLogsWriter.append("Syntax error: Error occurred due to " +
            "mismatch parenthesis").append("\n");
        this.eventLogsWriter.close();
        throw new Exception("Syntax error: Error occurred due " +
            "to mismatch parenthesis");
      }
    } else {
      this.eventLogsWriter.append("Syntax error: Please check your syntax for" +
          " Insert query").append("\n");
      this.eventLogsWriter.close();
      throw new Exception("Syntax error: Please check your syntax for Insert query");
    }
    return true;
  }


  public boolean getTableMetadata(String schemaName, String tableName) throws Exception {
    String path = Helper.getTableMetadataPath(schemaName, tableName);
    File metadataFile = new File(path);

    if (metadataFile.exists()) {
      BufferedReader br =
          new BufferedReader(new FileReader(metadataFile));

      String line;

      while ((line = br.readLine()) != null) {
        Column column = new Column();
        String[] columnInfo = line.split(AppConstants.DELIMITER_TOKEN);

        //  Looping through file data and creating metadata of type COLUMN
        for (String info : columnInfo) {
          String[] columnKeyValue = info.split("=");
          String columnKey = columnKeyValue[0];
          if (columnKey.equalsIgnoreCase(AppConstants.COLUMN_NAME)) {
            column.setColumnName(columnKeyValue[1]);
          } else if (columnKey.equalsIgnoreCase(AppConstants.COLUMN_DATA_TYPE)) {
            column.setColumnDataType(columnKeyValue[1]);
          } else if (columnKey.equalsIgnoreCase(AppConstants.COLUMN_SIZE)) {
            column.setColumnSize(Integer.parseInt(columnKeyValue[1]));
          } else if (columnKey.equalsIgnoreCase(AppConstants.PK)) {
            column.setPrimaryKey(true);
            this.primaryKeyColumn = column;
          } else if (columnKey.equalsIgnoreCase(AppConstants.FK)) {
            column.setForeignKey(true);
            String[] tableAndColumn = columnKeyValue[1].split("\\.");
            String foreignTable = tableAndColumn[0];
            String foreignColumn = tableAndColumn[1];
            column.setForeignKeyTable(foreignTable);
            column.setForeignKeyColumn(foreignColumn);
            this.foreignKeyColumn = column;
          } else if (columnKey.equalsIgnoreCase(AppConstants.AI)) {
            column.setAutoIncrement(true);
          } else {
            this.eventLogsWriter.append("Something went wrong near: ").append(columnKey).append("\n");
            this.eventLogsWriter.close();
            throw new Exception("Something went wrong near: " + columnKey);
          }
        }
        this.columns.put(column.getColumnName(), column);
      }
      br.close();
      this.eventLogsWriter.append("Metadata fetched successfully for table: ").append(tableName).append("\n");
      return true;
    }

    return false;
  }

  public boolean validateColWithVal(String[] columnsArray,
                                    String[] valuesArray) throws NumberFormatException, IOException {
    for (String col : columnsArray) {
      Set<String> key = this.columns.keySet();

      if (!this.columns.containsKey(col)) {
        return false;
      }

      if (this.primaryKeyColumn != null) {
        if (!key.contains(this.primaryKeyColumn.getColumnName())) {
          this.eventLogsWriter.append("Insert fail: Value for primary key is null").append("\n");
          System.out.println("Insert fail: Value for primary key is null");
          return false;
        }
      }

      if (this.foreignKeyColumn != null) {
        if (!key.contains(this.foreignKeyColumn.getColumnName())) {
          this.eventLogsWriter.append("Insert fail: Value for foreign key is null").append("\n");
          System.out.println("Insert fail: Value for foreign key is null");
          return false;
        }
      }
    }
    for (int i = 0; i < columnsArray.length; i++) {
      String colName = columnsArray[i];
      String colValue = valuesArray[i];
      Column metadata = this.columns.get(colName);
      String columnDataType = metadata.getColumnDataType().toLowerCase();
      Object columnValue;
      switch (columnDataType) {
        case "int":
          columnValue = Integer.parseInt(colValue);
          metadata.setColumnValue(columnValue);
          break;
        case "float":
          columnValue = Float.parseFloat(colValue);
          metadata.setColumnValue(columnValue);
          break;
        default:
          int startIndex = colValue.indexOf("'");
          int endIndex = colValue.lastIndexOf("'");
          if (startIndex > 0 || endIndex < colValue.length() - 1) {
            this.eventLogsWriter.append("Something went wrong while extracting value").append("\n");
            System.out.println("Something went wrong while extracting value");
            return false;
          }
          // If value has only '', then it will have length 2, that is empty
          if (colValue.length() == 2) {
            if (colName.equals(this.primaryKeyColumn.getColumnName())) {
              this.eventLogsWriter.append("Insert fail: Value for primary key" +
                  " is null").append("\n");
              System.out.println("Insert fail: Value for primary key is null");
              return false;
            }
            if (colName.equals(this.foreignKeyColumn.getColumnName())) {
              this.eventLogsWriter.append("Insert fail: Value for foreign key is null").append("\n");
              System.out.println("Insert fail: Value for foreign key is null");
              return false;
            }
          }
          metadata.setColumnValue(colValue.substring(1, colValue.length() - 1));
          break;
      }
    }
    this.eventLogsWriter.append("Columns and its value validated " +
        "successfully").append("\n");
    return true;
  }

  public boolean validatedValuesBetweenParenthesis(String str) {
    int leftParenthesis = str.indexOf("(");
    int rightParenthesis = str.indexOf(")");

    return leftParenthesis != -1 && rightParenthesis != -1 && leftParenthesis <= 0 && rightParenthesis >= str.length() - 1;
  }

}
