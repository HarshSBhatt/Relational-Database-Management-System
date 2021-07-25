package org.group15.database;

import org.group15.io.CustomLock;
import org.group15.io.TableIO;
import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.io.*;
import java.util.*;

public class Table {

  private String tableName;

  CustomLock customLock = new CustomLock();

  TableIO tableIO = new TableIO();

  Map<String, Column> columns;

  List<Map<String, Object>> tableValues;

  FileWriter eventLogsWriter;

  public Table(FileWriter eventLogsWriter) {
    this.columns = new HashMap<>();
    this.tableValues = new ArrayList<>();
    this.eventLogsWriter = eventLogsWriter;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<Map<String, Object>> getTableValues(String schemaName,
                                                  String tableName,
                                                  Map<String, Column> tableColumnsDetails) throws Exception {
    String path = Helper.getTablePath(schemaName, tableName);
    File tableFile = new File(path);

    if (tableFile.exists()) {
      BufferedReader br =
          new BufferedReader(new FileReader(tableFile));

      String line;
      while ((line = br.readLine()) != null) {
        String[] columnInfo = line.split(AppConstants.DELIMITER_TOKEN);
        Map<String, Object> colAndVal = new HashMap<>();

        //  Looping through file data and creating column name and its value map
        for (String info : columnInfo) {
          String[] columnKeyValue = info.split("=");
          colAndVal.put(columnKeyValue[0], columnKeyValue[1]);
        }
        // If some column has null or empty values then we will mark it with DASH
        for (String columnName : tableColumnsDetails.keySet()) {
          if (!(colAndVal.containsKey(columnName))) {
            colAndVal.put(columnName, "-");
          }
        }
        tableValues.add(colAndVal);
      }
      br.close();
      this.eventLogsWriter.append("Table values fetched for table: ").append(tableName).append("\n");

    } else {
      this.eventLogsWriter.append("Something went wrong! Table does not " +
          "exist").append("\n");
      this.eventLogsWriter.close();
      throw new Exception("Table: " + tableName + " does not exist");
    }
    return tableValues;
  }

  public List<Map<String, Object>> getTableValues(String schemaName,
                                                  String tableName,
                                                  Map<String, Column> tableColumnsDetails, Set<String> requiredColumnNames) throws Exception {
    String path = Helper.getTablePath(schemaName, tableName);
    File tableFile = new File(path);

    if (tableFile.exists()) {
      BufferedReader br =
          new BufferedReader(new FileReader(tableFile));

      String line;
      while ((line = br.readLine()) != null) {
        String[] columnInfo = line.split(AppConstants.DELIMITER_TOKEN);
        Map<String, Object> colAndVal = new HashMap<>();

        //  Looping through file data and creating column name and its value map
        for (String info : columnInfo) {
          String[] columnKeyValue = info.split("=");
          // Getting only required columns asked by user
          if (requiredColumnNames.contains(columnKeyValue[0])) {
            colAndVal.put(columnKeyValue[0], columnKeyValue[1]);
          }
        }
        // If some column has null or empty values then we will mark it with DASH
        for (String columnName : tableColumnsDetails.keySet()) {
          if (requiredColumnNames.contains(columnName)) {
            if (!(colAndVal.containsKey(columnName))) {
              colAndVal.put(columnName, "-");
            }
          }
        }
        tableValues.add(colAndVal);
      }
      br.close();
      this.eventLogsWriter.append("Table values fetched for table: ").append(tableName).append("\n");

    } else {
      this.eventLogsWriter.append("Something went wrong! Table does not " +
          "exist").append("\n");
      this.eventLogsWriter.close();
      throw new Exception("Table: " + tableName + " does not exist");
    }
    return tableValues;
  }

  public Map<String, Column> getTableMetadataMap(String schemaName,
                                                 String tableName) throws Exception {

    Map<String, Column> columnsMetaData = new HashMap<>();
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
          } else if (columnKey.equalsIgnoreCase(AppConstants.FK)) {
            column.setForeignKey(true);
            String[] tableAndColumn = columnKeyValue[1].split("\\.");
            String foreignTable = tableAndColumn[0];
            String foreignColumn = tableAndColumn[1];
            column.setForeignKeyTable(foreignTable);
            column.setForeignKeyColumn(foreignColumn);
          } else if (columnKey.equalsIgnoreCase(AppConstants.AI)) {
            column.setAutoIncrement(true);
          } else {
            this.eventLogsWriter.append("Something went wrong near: ").append(columnKey).append("\n");
            this.eventLogsWriter.close();
            throw new Exception("Something went wrong near: " + columnKey);
          }
        }
        columnsMetaData.put(column.getColumnName(), column);
      }
      br.close();
      this.eventLogsWriter.append("Metadata fetched successfully for table: ").append(tableName).append("\n");
      return columnsMetaData;
    }
    return columnsMetaData;
  }

  public Column getColumnObjFromLine(String line) throws Exception {
    Column column = new Column();
    String[] columnInfo = line.split(AppConstants.DELIMITER_TOKEN);
    for (String info : columnInfo) {
      String[] columnKeyValue = info.split("=");
      String columnKey = columnKeyValue[0];
      switch (columnKey) {
        case AppConstants.COLUMN_NAME:
          column.setColumnName(columnKeyValue[1]);
          break;
        case AppConstants.COLUMN_DATA_TYPE:
          column.setColumnDataType(columnKeyValue[1]);
          break;
        case AppConstants.COLUMN_SIZE:
          column.setColumnSize(Integer.parseInt(columnKeyValue[1]));
          break;
        case AppConstants.FK:
          column.setForeignKey(true);
          String[] tableAndColumn = columnKeyValue[1].split("\\.");
          String foreignTable = tableAndColumn[0];
          String foreignColumn = tableAndColumn[1];
          column.setForeignKeyTable(foreignTable);
          column.setForeignKeyColumn(foreignColumn);
          break;
        case AppConstants.PK:
          column.setPrimaryKey(true);
          break;
        default:
          this.eventLogsWriter.append("Something went wrong near: ").append(columnKey).append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Unknown column metadata encountered: " + columnKey);
      }
    }
    return column;
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
          this.eventLogsWriter.append("Failed to validate foreign key").append("\n");
          return false;
        }
      }
    }
    this.eventLogsWriter.append("Foreign key check is performed successfully").append("\n");
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
      this.eventLogsWriter.append("Something went wrong! File does not exist").append("\n");
      System.out.println("Something went wrong! File does not exist");
      return (ArrayList<Object>) Collections.emptyList();
    }
  }

  public void create(Map<String, Column> tableColumns, String schemaName,
                     String tableName) throws Exception {
    if (customLock.isLocked(schemaName, tableName)) {
      System.out.println("Table: " + tableName + " is locked");
      this.eventLogsWriter.append("Table: ").append(tableName).append(" is " +
          "locked").append("\n");
    } else {
      customLock.lock(schemaName, tableName);
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
                fileContent.append("column_size=").append(Math.min(column.getColumnSize(), 255));
              }
              if (column.isPrimaryKey()) {
                if (pk < 1) {
                  fileContent.append(AppConstants.DELIMITER_TOKEN).append("PK");
                  pk++;
                } else {
                  this.eventLogsWriter.append("Table can not have more than one primary key").append("\n");
                  this.eventLogsWriter.close();
                  throw new Exception("Table can not have more than one primary " +
                      "key");
                }
              }
              if (column.isAutoIncrement()) {
                if (ai < 1) {
                  fileContent.append(AppConstants.DELIMITER_TOKEN).append("AI");
                  ai++;
                } else {
                  this.eventLogsWriter.append("Table can not have more than one AUTO_INCREMENT field").append("\n");
                  this.eventLogsWriter.close();
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
            this.eventLogsWriter.append("Error occurred while creating table").append("\n");
            this.eventLogsWriter.close();
            throw new Exception("Error occurred while creating table");
          }
        } else {
          this.eventLogsWriter.append("Error: Wrong foreign key constraint").append("\n");
          this.eventLogsWriter.close();
          throw new Exception("Error: Wrong foreign key constraint");
        }
      } else {
        this.eventLogsWriter.append("Table already exists").append("\n");
        this.eventLogsWriter.close();
        throw new Exception("Table already exists");
      }
      customLock.unlock(schemaName, tableName);
    }
  }

  public boolean insert(String schemaName, String tableName, String[]
      columnArray,
                        Map<String, Column> columnsDetails,
                        Column primaryKeyColumn, Column foreignKeyColumn) throws IOException, InterruptedException {

    StringBuilder fileContent = new StringBuilder();

    String tablePath = Helper.getTablePath(schemaName, tableName);

    File tableFile = new File(tablePath);

    if (customLock.isLocked(schemaName, tableName)) {
      System.out.println("Table: " + tableName + " is locked");
      this.eventLogsWriter.append("Table: ").append(tableName).append(" is " +
          "locked").append("\n");
      return false;
    } else {
      customLock.lock(schemaName, tableName);
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
              this.eventLogsWriter.append("Insert fail: Primary key constraint violated").append("\n");
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
              this.eventLogsWriter.append("Insert fail: Foreign key constraint violated").append("\n");
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
        this.eventLogsWriter.append("Something went wrong! Table does not " +
            "exist").append("\n");
        System.out.println("Something went wrong! Table does not exist");
        return false;
      }
      System.out.println("Inserted row successfully");
      customLock.unlock(schemaName, tableName);
      return true;
    }
  }

  public boolean dropTable(String schemaName, String tableName) throws Exception {
    String schemaPath = Helper.getSchemaPath(schemaName);
    String tableMetadataPath = Helper.getTableMetadataPath(schemaName,
        tableName);
    String tableValuePath = Helper.getTablePath(schemaName, tableName);

    File schemaFolder = new File(schemaPath.concat("/table_metadata"));
    File[] tables = schemaFolder.listFiles();

    if (!schemaFolder.exists()) {
      this.eventLogsWriter.append("Something went wrong! Database: ").append(schemaName).append(" ").append("does not exist").append("\n");
      this.eventLogsWriter.close();
      throw new Exception("Database with name: " + schemaName + " not found");
    }

    if (!tableIO.isTableExist(schemaName, tableName) || !tableIO.isMetadataTableExist(schemaName, tableName)) {
      this.eventLogsWriter.append("Something went wrong! Table: ").append(tableName).append(" ").append("does not exist").append("\n");
      this.eventLogsWriter.close();
      throw new Exception("Table with name: " + tableName + " not found");
    }

    if (customLock.isLocked(schemaName, tableName)) {
      System.out.println("Table: " + tableName + " is locked");
      this.eventLogsWriter.append("Table: ").append(tableName).append(" is " +
          "locked").append("\n");
      return false;
    } else {
      customLock.lock(schemaName, tableName);
      File tableMetadataFile = new File(tableMetadataPath);
      File tableValueFile = new File(tableValuePath);

      BufferedReader br =
          new BufferedReader(new FileReader(tableMetadataFile));

      // Here, we are reading all data from particular table
      String columnName = null;
      String line;
      while ((line = br.readLine()) != null) {
        // Generating column obj from the line
        Column column = getColumnObjFromLine(line);
        if (column.isPrimaryKey()) {
          columnName = column.getColumnName();
        }
      }
      br.close();

      if (columnName == null) {
        if (tableMetadataFile.delete()) {
          tableValueFile.delete();
        }
      } else {
        String foreignKeyColumnName = null;
        String foreignKeyTableName = null;

        for (File table : tables) {
          String currentTableName = table.getName().split("\\.")[0];
          String tablePath = table.getPath();

          // We will read the particular table based on the table path
          File tableFile = new File(tablePath);
          if (!currentTableName.equalsIgnoreCase(tableName)) {
            BufferedReader bufferedReader =
                new BufferedReader(new FileReader(tableFile));

            // Here, we are reading all data from particular table
            String currentTableLine;

            while ((currentTableLine = bufferedReader.readLine()) != null) {
              // Generating column obj from the line
              Column column = getColumnObjFromLine(currentTableLine);
              if (column.isForeignKey()) {
                foreignKeyColumnName = column.getForeignKeyColumn();
                foreignKeyTableName = column.getForeignKeyTable();
                if (foreignKeyColumnName.equals(columnName) && foreignKeyTableName.equals(tableName)) {
                  this.eventLogsWriter.append("Foreign key violation! Table: ").append(tableName).append(" can not be dropped").append(
                      "\n");
                  this.eventLogsWriter.close();
                  throw new Exception("Foreign key violation! Table: " + tableName + " can not be dropped");
                }
              }
            }
            bufferedReader.close();
          }
        }
        if (foreignKeyColumnName == null && foreignKeyTableName == null) {
          if (tableMetadataFile.delete()) {
            tableValueFile.delete();
          }
        }
      }
      customLock.unlock(schemaName, tableName);
      return true;
    }
  }

  public boolean addColumn(String schemaName, String tableName,
                           String columnNameToBeAdded, String[] dataTypeRelatedInfo) throws IOException, InterruptedException {
    if (customLock.isLocked(schemaName, tableName)) {
      System.out.println("Table: " + tableName + " is locked");
      this.eventLogsWriter.append("Table: ").append(tableName).append(" is " +
          "locked").append("\n");
      return false;
    } else {
      customLock.lock(schemaName, tableName);
      StringBuilder fileContent = new StringBuilder();

      String tableMetadataPath = Helper.getTableMetadataPath(schemaName, tableName);

      File tableMetadataFile = new File(tableMetadataPath);

      if (tableMetadataFile.exists()) {
        FileWriter tableDataWriter = new FileWriter(tableMetadataFile, true);
        // Adding new column info to file
        appendColumnInfoToFile(columnNameToBeAdded, dataTypeRelatedInfo, fileContent, tableDataWriter);
      } else {
        this.eventLogsWriter.append("Something went wrong! Table does not " +
            "exist").append("\n");
        System.out.println("Something went wrong! Table does not exist");
        return false;
      }
      customLock.unlock(schemaName, tableName);
      return true;
    }
  }

  public boolean dropColumn(String schemaName, String tableName,
                            String columnNameToBeDropped) throws IOException, InterruptedException {

    if (customLock.isLocked(schemaName, tableName)) {
      System.out.println("Table: " + tableName + " is locked");
      this.eventLogsWriter.append("Table: ").append(tableName).append(" is " +
          "locked").append("\n");
      return false;
    } else {
      customLock.lock(schemaName, tableName);
      String tablePath = Helper.getTablePath(schemaName, tableName);
      String tableMetadataPath = Helper.getTableMetadataPath(schemaName, tableName);

      File tableFile = new File(tablePath);
      File tableMetadataFile = new File(tableMetadataPath);

      FileWriter tableDataWriter;
      StringBuilder newFileContent;

      if (tableFile.exists()) {
        // Dropping column from actual table
        newFileContent = Helper.replaceFileContent(tableFile,
            columnNameToBeDropped, false);
        tableDataWriter = new FileWriter(tableFile);
        tableDataWriter.write(String.valueOf(newFileContent));
        tableDataWriter.close();

        // Dropping column from metadata table
        newFileContent = Helper.replaceFileContent(tableMetadataFile,
            columnNameToBeDropped, true);
        tableDataWriter = new FileWriter(tableMetadataFile);
        tableDataWriter.write(String.valueOf(newFileContent));
        tableDataWriter.close();
      } else {
        this.eventLogsWriter.append("Something went wrong! Table does not " +
            "exist").append("\n");
        System.out.println("Something went wrong! Table does not exist");
        return false;
      }
      customLock.unlock(schemaName, tableName);
      return true;
    }
  }

  public boolean changeColumn(String schemaName, String tableName,
                              String oldColumnName, String newColumnName,
                              String[] dataTypeRelatedInfo) throws IOException, InterruptedException {
    if (customLock.isLocked(schemaName, tableName)) {
      System.out.println("Table: " + tableName + " is locked");
      this.eventLogsWriter.append("Table: ").append(tableName).append(" is " +
          "locked").append("\n");
      return false;
    } else {
      customLock.lock(schemaName, tableName);
      StringBuilder fileContent = new StringBuilder();
      String tablePath = Helper.getTablePath(schemaName, tableName);
      String tableMetadataPath = Helper.getTableMetadataPath(schemaName, tableName);

      File tableFile = new File(tablePath);
      File tableMetadataFile = new File(tableMetadataPath);

      FileWriter tableDataWriter;
      StringBuilder newFileContent;

      if (tableFile.exists()) {
        // Dropping column from metadata table
        newFileContent = Helper.replaceFileContent(tableMetadataFile,
            oldColumnName, true);
        tableDataWriter = new FileWriter(tableMetadataFile);
        tableDataWriter.write(String.valueOf(newFileContent));
        tableDataWriter.close();

        tableDataWriter = new FileWriter(tableMetadataFile, true);
        // Adding new column info to file
        appendColumnInfoToFile(newColumnName, dataTypeRelatedInfo, fileContent, tableDataWriter);

        // Updating column name
        StringBuilder updatedValue = Helper.changeColumnNameInFile(tableFile,
            oldColumnName, newColumnName);
        tableDataWriter = new FileWriter(tableFile);
        tableDataWriter.write(String.valueOf(updatedValue));
        tableDataWriter.close();
      } else {
        this.eventLogsWriter.append("Something went wrong! Table does not " +
            "exist").append("\n");
        System.out.println("Something went wrong! Table does not exist");
        return false;
      }
      customLock.unlock(schemaName, tableName);
      return true;
    }
  }

  public void fetchTableInfo(String columns,
                             String schemaName, String tableName) throws Exception {

    Map<String, Column> tableColumnsDetails = getTableMetadataMap(schemaName,
        tableName);

    if (columns.equalsIgnoreCase("*")) {
      List<Map<String, Object>> mappedValues = getTableValues(schemaName,
          tableName, tableColumnsDetails);

      Helper.printTable(mappedValues);
    } else {
      Set<String> columnNamesInQuery =
          new LinkedHashSet<>(Arrays.asList(columns.split(",\\s*")));

      Set<String> columnsNamesInActualTable = tableColumnsDetails.keySet();

      if (columnsNamesInActualTable.containsAll(columnNamesInQuery)) {
        List<Map<String, Object>> mappedValues = getTableValues(schemaName,
            tableName, tableColumnsDetails, columnNamesInQuery);

        Helper.printTable(mappedValues);
      } else {
        this.eventLogsWriter.append("One of the column does not exist in " +
            "table: ").append(tableName).append("! Please check your query").append("\n");
        this.eventLogsWriter.close();
        throw new Exception("One of the column does not exist in table: " + tableName +
            "! Please check your query");
      }
    }
  }

  public void fetchTableInfo(String columns, String schemaName, String tableName, String conditions) throws Exception {
    Map<String, Column> tableColumnsDetails = getTableMetadataMap(schemaName,
        tableName);

    if (columns.equalsIgnoreCase("*")) {
      List<Map<String, Object>> mappedValues = getTableValues(schemaName,
          tableName, tableColumnsDetails);

      printTableValues(columns, tableName, conditions, tableColumnsDetails,
          mappedValues, true);
    } else {
      Set<String> columnNamesInQuery =
          new LinkedHashSet<>(Arrays.asList(columns.split(",\\s*")));

      Set<String> columnsNamesInActualTable = tableColumnsDetails.keySet();

      if (columnsNamesInActualTable.containsAll(columnNamesInQuery)) {
        List<Map<String, Object>> mappedValues = getTableValues(schemaName,
            tableName, tableColumnsDetails, columnNamesInQuery);

        printTableValues(columns, tableName, conditions, tableColumnsDetails,
            mappedValues, false);

      } else {
        this.eventLogsWriter.append("One of the column does not exist in " +
            "table: ").append(tableName).append("! Please check your query").append("\n");
        this.eventLogsWriter.close();
        throw new Exception("One of the column does not exist in table: " + tableName +
            "! Please check your query");
      }
    }
  }

  public void printTableValues(String columns, String tableName,
                               String conditions,
                               Map<String, Column> tableColumnsDetails,
                               List<Map<String, Object>> mappedValues,
                               boolean isAllOperation) throws Exception {
    List<String> conditionList;

    Map<String, String> conditionMap = new HashMap<>();

    conditionList = Arrays.asList(conditions.trim().split(
        "\\s+"));

    if (conditionList.size() > 1) {
      if (conditions.toUpperCase().contains("AND")) {

        conditionList = Arrays.asList(conditions.trim().split(
            "and"));

        extractConditionAndPrintTable(tableName, tableColumnsDetails,
            conditionList, conditionMap, mappedValues, columns, false, isAllOperation);
      } else if (conditions.toUpperCase().contains("OR")) {

        conditionList = Arrays.asList(conditions.trim().split(
            "or"));

        extractConditionAndPrintTable(tableName, tableColumnsDetails,
            conditionList, conditionMap, mappedValues, columns, true, isAllOperation);
      } else {
        this.eventLogsWriter.append("Condition mentioned is wrong! Please " +
            "check your query").append("\n");
        this.eventLogsWriter.close();
        throw new Exception("Condition mentioned is wrong! Please " +
            "check your query! Please check your query");
      }
    } else {
      conditionList = Arrays.asList(conditions.trim().split(
          "or"));

      extractConditionAndPrintTable(tableName, tableColumnsDetails,
          conditionList, conditionMap, mappedValues, columns, true, isAllOperation);
    }
  }

  public void extractConditionAndPrintTable(String tableName, Map<String,
      Column> tableColumnsDetails, List<String> conditionList, Map<String,
      String> conditionMap, List<Map<String, Object>> mappedValues,
                                            String columns,
                                            boolean isOrCondition,
                                            boolean isAllOperation) throws Exception {
    for (String condition : conditionList) {
      List<String> conditionColAndVAl = Arrays.asList(condition.split("="));
      conditionMap.put(conditionColAndVAl.get(0).trim(),
          conditionColAndVAl.get(1).trim().replaceAll("[^0-9a-zA-Z]+"
              , ""));

      boolean isColExist = Helper.isColumnExist(tableColumnsDetails,
          conditionColAndVAl.get(0).trim());

      if (!isColExist) {
        this.eventLogsWriter.append("Column: ").append(conditionColAndVAl.get(0).trim()).append(" does not exist in " +
            "table: ").append(tableName).append("! Please check your query").append("\n");
        this.eventLogsWriter.close();
        throw new Exception("Column: " + conditionColAndVAl.get(0).trim() + " does not exist in table: " + tableName +
            "! Please check your query");
      }
    }

    List<Map<String, Object>> tableReturnValues = new ArrayList<>();
    for (Map<String, Object> mappedValueMap : mappedValues) {
      boolean isConditionMatched = conditionChecker(conditionMap,
          mappedValueMap, isOrCondition);

      if (isConditionMatched) {
        List<String> columnsList;
        if (isAllOperation) {
          Set<String> colNames = tableColumnsDetails.keySet();
          columnsList = Helper.convertSetToList(colNames);
        } else {
          columnsList = Arrays.asList(columns.split(","));
        }
        mappedValueMap.keySet().retainAll(columnsList);
        tableReturnValues.add(mappedValueMap);
      }
    }

    Helper.printTable(tableReturnValues);
  }

  private boolean conditionChecker(Map<String, String> conditionMap,
                                   Map<String, Object> mappedValueMap, boolean isOrCondition) {
    boolean flag = false;
    for (String key : conditionMap.keySet()) {
      if (isOrCondition) {
        if ((mappedValueMap.get(key).equals(conditionMap.get(key)))) {
          flag = true;
        }
      } else {
        if ((mappedValueMap.get(key).equals(conditionMap.get(key)))) {
          flag = true;
        } else {
          flag = false;
          break;
        }
      }

    }
    return flag;
  }

  public void appendColumnInfoToFile(String newColumnName, String[] dataTypeRelatedInfo, StringBuilder fileContent, FileWriter tableDataWriter) throws IOException {
    fileContent.append("column_name=").append(newColumnName).append(AppConstants.DELIMITER_TOKEN);
    fileContent.append("column_data_type=").append(dataTypeRelatedInfo[0]).append(AppConstants.DELIMITER_TOKEN);
    if (dataTypeRelatedInfo.length == 2) {
      fileContent.append("column_size=").append(Math.min(Integer.parseInt(dataTypeRelatedInfo[1]), 255));
    } else {
      fileContent.append("column_size=").append(255);
    }
    tableDataWriter.append(fileContent).append("\n");
    tableDataWriter.close();
  }

}
