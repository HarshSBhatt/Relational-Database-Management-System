package org.group15.database;

import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.io.*;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

public class ERD {

  FileWriter eventLogsWriter;

  File erdFile;

  File erdFolder;

  Formatter fmtCon;

  Formatter fmtFile;

  private List<Column> columns;

  Table tableObj;

  public ERD(FileWriter eventLogsWriter) {
    this.columns = new ArrayList<>();
    this.eventLogsWriter = eventLogsWriter;
    this.fmtCon = new Formatter(System.out);
    tableObj = new Table(eventLogsWriter);
  }

  public boolean generateERD(String schemaName) throws Exception {
    String schemaPath = Helper.getSchemaPath(schemaName);
    File schemaFolder = new File(schemaPath.concat("/table_metadata"));
    File[] tables = schemaFolder.listFiles();

    if (!schemaFolder.exists()) {
      throw new Exception("Database with name: " + schemaName + " not found");
    }

    if (tables.length < 1) {
      throw new Exception("No tables exist in database with name: " + schemaName);
    }

    this.erdFolder =
        new File(AppConstants.ERD_ROOT_FOLDER_PATH);

    if (this.erdFolder.mkdir()) {
      System.out.println("ERD folder created!");
    }

    // ERD file for particular schema
    String tableFilePath =
        AppConstants.ERD_ROOT_FOLDER_PATH + "/" + schemaName + ".dp15";
    this.erdFile =
        new File(tableFilePath);

    if (this.erdFile.createNewFile()) {
      System.out.println("ERD file created!");
    }

    // Here, we will not append erd, but replace content of existing file  if file is not empty
    this.fmtFile = new Formatter(new FileOutputStream(tableFilePath));

    // Looping through each table inside particular folder
    for (File table : tables) {
      String tableName = table.getName();
      String tablePath = table.getPath();
      // We will read the particular table based on the table path
      File tableFile = new File(tablePath);

      BufferedReader br =
          new BufferedReader(new FileReader(tableFile));

      // Here, we are reading all data from particular table
      String line;
      while ((line = br.readLine()) != null) {
        // Generating column obj from the line
        Column column = tableObj.getColumnObjFromLine(line);

        this.eventLogsWriter.append("Metadata fetched successfully of " +
            "table: ").append(tableName).append(" while generating ERD").append("\n");
        this.columns.add(column);
      }
      br.close();
      writeERDToFile(schemaName, tableName, tablePath);
    }
    this.fmtCon.close();
    this.fmtFile.close();
    this.eventLogsWriter.append("All tables detail fetched successfully to " +
        "generate ERD").append("\n");
    return true;
  }

  private void writeERDToFile(String schemaName, String tableName, String tablePath) {

    String sName = "Schema: ".concat(schemaName);
    String tName = "Table: ".concat(tableName);
    String tPath = "Path: ".concat(tablePath);

    String lineSeparator =
        "=============================================================================================================================================";

    String headingSeparator =
        "---------------------------------------------------------------------------------------------------------------------------------------------";

    this.fmtFile.format(lineSeparator.concat("\n"));
    System.out.println(lineSeparator);

    this.fmtFile.format("%20s%30s%60s\n", sName, tName, tPath);
    System.out.format("%20s%30s%60s\n", sName, tName, tPath);

    this.fmtFile.format(lineSeparator.concat("\n"));
    System.out.println(lineSeparator);

    this.fmtFile.format("%20s%20s%15s%15s%15s%20s%30s\n", "Column Name |", "Data " +
            "type |", "Size |",
        "Primary Key |", "Foreign Key |", "Foreign Column |", "Foreign Table |");
    System.out.format("%20s%20s%15s%15s%15s%20s%30s\n", "Column Name |", "Data " +
            "type |", "Size |",
        "Primary Key |", "Foreign Key |", "Foreign Column |", "Foreign Table |");

    this.fmtFile.format(headingSeparator.concat("\n"));
    System.out.println(headingSeparator);

    for (Column column : this.columns) {
      String columnName = column.getColumnName().concat(" |");
      String columnDataType = column.getColumnDataType().concat(" |");
      String size = String.valueOf(column.getColumnSize()).concat(" |");
      String primaryKey = column.isPrimaryKey() ? "PK".concat(" |") : "- |";
      String foreignKey = "- |", foreignColumn = "- |", foreignTable = "- |";
      if (column.isForeignKey()) {
        foreignKey = "FK |";
        foreignColumn = column.getForeignKeyColumn().concat(" |");
        foreignTable = column.getForeignKeyTable().concat(" |");
      }
      this.fmtFile.format("%20s%20s%15s%15s%15s%20s%30s\n", columnName,
          columnDataType, size, primaryKey, foreignKey, foreignColumn, foreignTable);
      System.out.format("%20s%20s%15s%15s%15s%20s%30s\n", columnName,
          columnDataType, size, primaryKey, foreignKey, foreignColumn, foreignTable);
    }

    this.fmtFile.format(headingSeparator.concat("\n"));
    System.out.println(headingSeparator);

    // Emptied the current column data for the next iteration
    this.columns = new ArrayList<>();
  }

}
