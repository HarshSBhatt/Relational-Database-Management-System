package org.group15.database;

import org.group15.io.TableIO;
import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.io.*;
import java.util.*;

public class Table {

  private String tableName;

  TableIO tableIO = new TableIO();

  Map<String, Column> columns;

  List<Map<String, Object>> tableValues;


  public Table() {
    this.columns = new HashMap<>();
    this.tableValues = new ArrayList<>();
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public boolean hasValidForeignKey(Map<String, Column> tableColumns,
                                    String schemaName) throws IOException {
    for (String key : tableColumns.keySet()) {
      Column column = tableColumns.get(key);
      if (column.isForeignKey()) {
        String path = Helper.getTableMetadataPath(schemaName,
            column.getForeignKeyTable());
        File metadataFile = new File(path);
        if (metadataFile.exists()) {
          BufferedReader br =
              new BufferedReader(new FileReader(metadataFile));
          String line;
          while ((line = br.readLine()) != null) {
            if (line.contains("PK")) {
              String[] columnInfo = line.split(AppConstants.DELIMITER_TOKEN);
              for (String info : columnInfo) {
                if (info.contains("column_name")) {
                  String columnName = info.split("=")[1];
                  return columnName.equalsIgnoreCase(column.getForeignKeyColumn());
                }
              }
            }
          }
          br.close();
        } else {
          return false;
        }
      }
    }
    return true;
  }

  public ArrayList<Object> getValuesOfParticularColumn(String schemaName,
                                                       String tableName,
                                                       Column column) throws IOException {
    String tablePath = Helper.getTablePath(schemaName, tableName);

    File tableFile = new File(tablePath);
    ArrayList<Object> columnValues = new ArrayList<>();

    if (tableFile.exists()) {
      BufferedReader br =
          new BufferedReader(new FileReader(tableFile));
      String line;
      while ((line = br.readLine()) != null) {
        String[] columnInfo = line.split(AppConstants.DELIMITER_TOKEN);
        for (String info : columnInfo) {
          String colName = info.split("=")[0];
          String colVal = info.split("=")[1];
          if (colName.equalsIgnoreCase(column.getColumnName())) {
            switch (column.getColumnDataType()) {
              case "int":
                columnValues.add(Integer.parseInt(colVal));
                break;
              case "float":
                columnValues.add(Float.parseFloat(colVal));
                break;
              default:
                columnValues.add(colVal);
                break;
            }
          }
        }
      }
      return columnValues;
    } else {
      System.out.println("Something went wrong! File does not exist");
      return (ArrayList<Object>) Collections.emptyList();
    }
  }

  public void create(Map<String, Column> tableColumns, String schemaName,
                     String tableName) throws Exception {
    if (!tableIO.isMetadataTableExist(schemaName, tableName)) {
      if (hasValidForeignKey(tableColumns, schemaName)) {
        StringBuilder fileContent = new StringBuilder();
        String metaDataPath = Helper.getTableMetadataPath(schemaName,
            tableName);
        String tablePath = Helper.getTablePath(schemaName, tableName);

        File metadataFile = new File(metaDataPath);
        File tableFile = new File(tablePath);

        if (metadataFile.createNewFile() && tableFile.createNewFile()) {
          int pk = 0;
          int ai = 0;
          FileWriter metadataWriter = new FileWriter(metadataFile, true);
          for (String key : tableColumns.keySet()) {
            Column column = tableColumns.get(key);
            fileContent.append("column_name=").append(column.getColumnName()).append(AppConstants.DELIMITER_TOKEN);
            fileContent.append("column_data_type=").append(column.getColumnDataType()).append(AppConstants.DELIMITER_TOKEN);
            if (column.getColumnSize() == 0) {
              fileContent.append("column_size=").append(255);
            } else {
              fileContent.append("column_size=").append(column.getColumnSize());
            }
            if (column.isPrimaryKey()) {
              if (pk < 1) {
                fileContent.append(AppConstants.DELIMITER_TOKEN).append("PK");
                pk++;
              } else {
                throw new Exception("Table can not have more than one primary " +
                    "key");
              }
            }
            if (column.isAutoIncrement()) {
              if (ai < 1) {
                fileContent.append(AppConstants.DELIMITER_TOKEN).append("AI");
                ai++;
              } else {
                throw new Exception("Table can not have more than one " +
                    "AUTO_INCREMENT field");
              }
            }
            if (column.isForeignKey()) {
              fileContent.append(AppConstants.DELIMITER_TOKEN).append("FK=").append(column.getForeignKeyTable()).append(".").append(column.getForeignKeyColumn());
            }
            fileContent.append("\n");
          }
          metadataWriter.append(fileContent);
          metadataWriter.close();
        } else {
          throw new Exception("Error occurred while creating table");
        }
      } else {
        throw new Exception("Error: Wrong foreign key constraint");
      }
    } else {
      throw new Exception("Table already exists");
    }
  }

  public boolean insert(String schemaName, String tableName, String[] columnArray,
                        Map<String, Column> columnsDetails,
                        Column primaryKeyColumn, Column foreignKeyColumn) throws IOException {

    StringBuilder fileContent = new StringBuilder();

    String tablePath = Helper.getTablePath(schemaName, tableName);

    File tableFile = new File(tablePath);

    if (tableFile.exists()) {
      FileWriter tableDataWriter = new FileWriter(tableFile, true);
      for (int i = 0; i < columnArray.length; i++) {
        Column metadata = columnsDetails.get(columnArray[i]);
        Object columnValue = metadata.getColumnValue();
        ArrayList<Object> listOfAvailableValuesInTable;

        if (primaryKeyColumn != null && columnArray[i].equalsIgnoreCase(primaryKeyColumn.getColumnName())) {
          listOfAvailableValuesInTable = getValuesOfParticularColumn(schemaName, tableName,
              primaryKeyColumn);

          // If value is already present, then we will not insert
          if (listOfAvailableValuesInTable.contains(columnValue)) {
            System.out.println("Insert fail: Primary key constraint violated");
            return false;
          }
          if (i == columnArray.length - 1) {
            fileContent.append(columnArray[i]).append("=").append(columnValue);
          } else {
            fileContent.append(columnArray[i]).append("=").append(columnValue).append(AppConstants.DELIMITER_TOKEN);
          }
        } else if (foreignKeyColumn != null && columnArray[i].equalsIgnoreCase(foreignKeyColumn.getColumnName())) {
          listOfAvailableValuesInTable = getValuesOfParticularColumn(schemaName,
              foreignKeyColumn.getForeignKeyTable(),
              foreignKeyColumn);

          // If value is not present, then we will not insert, because it can not be referenced
          if (!listOfAvailableValuesInTable.contains(columnValue)) {
            System.out.println("Insert fail: Foreign key constraint violated");
            return false;
          }
          if (i == columnArray.length - 1) {
            fileContent.append(columnArray[i]).append("=").append(columnValue);
          } else {
            fileContent.append(columnArray[i]).append("=").append(columnValue).append(AppConstants.DELIMITER_TOKEN);
          }
        } else {
          if (i == columnArray.length - 1) {
            fileContent.append(columnArray[i]).append("=").append(columnValue);
          } else {
            fileContent.append(columnArray[i]).append("=").append(columnValue).append(AppConstants.DELIMITER_TOKEN);
          }
        }


      }
      fileContent.append("\n");
      tableDataWriter.append(fileContent).close();
    } else {
      System.out.println("Something went wrong! File does not exist");
      return false;
    }
    return true;
  }

}
