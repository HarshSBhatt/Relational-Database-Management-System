package org.group15.database;

import org.group15.io.TableIO;
import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.io.File;
import java.io.FileWriter;
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

  public void create(Map<String, Column> tableColumns, String schemaName,
                     String tableName) throws Exception {
    if (!tableIO.isExist(schemaName, tableName)) {
      // TODO: CHECK FOR FOREIGN KEY TABLE AND REFERENCE EXIST OR NOT

      StringBuilder fileContent = new StringBuilder();
      String path = Helper.getTableMetadataPath(schemaName, tableName);

      File metadataFile = new File(path);

      if (metadataFile.createNewFile()) {
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
            fileContent.append(AppConstants.DELIMITER_TOKEN).append("PK");
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
      throw new Exception("Table already exists");
    }
  }

}
