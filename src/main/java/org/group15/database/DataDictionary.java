package org.group15.database;

import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.io.*;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

public class DataDictionary {

  FileWriter eventLogsWriter;

  File ddFile;

  File ddFolder;

  Formatter fmtCon;

  Formatter fmtFile;

  private List<Column> columns;

  Table tableObj;

  public DataDictionary(FileWriter eventLogsWriter) {
    this.columns = new ArrayList<>();
    this.eventLogsWriter = eventLogsWriter;
    this.fmtCon = new Formatter(System.out);
    tableObj = new Table(eventLogsWriter);
  }

  public boolean generateDataDictionary(String schemaName) throws Exception {
    String schemaPath = Helper.getSchemaPath(schemaName);
    File schemaFolder = new File(schemaPath.concat("/table_metadata"));
    File[] tables = schemaFolder.listFiles();

    if (!schemaFolder.exists()) {
      throw new Exception("Database with name: " + schemaName + " not found");
    }

    if (tables.length < 1) {
      throw new Exception("No tables exist in database with name: " + schemaName);
    }

    this.ddFolder =
        new File(AppConstants.DATA_DICTIONARY_ROOT_FOLDER_PATH);

    if (this.ddFolder.mkdir()) {
      System.out.println("Data Dictionary folder created!");
    }

    // ERD file for particular schema
    String tableFilePath =
        AppConstants.DATA_DICTIONARY_ROOT_FOLDER_PATH + "/" + schemaName + ".dp15";
    this.ddFile =
        new File(tableFilePath);

    if (this.ddFile.createNewFile()) {
      System.out.println("Data Dictionary file created!");
    }

    // Here, we will not append dd, but replace content of existing file if file is not empty
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
            "table: ").append(tableName).append(" while generating Data " +
            "Dictionary").append("\n");
        this.columns.add(column);
      }
      writeDDToFile(schemaName, tableName, tablePath);
    }
    this.fmtCon.close();
    this.fmtFile.close();
    this.eventLogsWriter.append("All tables detail fetched successfully to " +
        "generate ERD").append("\n");
    return true;
  }

  private void writeDDToFile(String schemaName, String tableName, String tablePath) {

    String sName = "Schema: ".concat(schemaName);
    String tName = "Table: ".concat(tableName);
    String tPath = "Path: ".concat(tablePath);

    String lineSeparator =
        "==============================================================================================================";

    String headingSeparator =
        "--------------------------------------------------------------------------------------------------------------";

    this.fmtFile.format(lineSeparator.concat("\n"));
    System.out.println(lineSeparator);

    this.fmtFile.format("%20s%30s%60s\n", sName, tName, tPath);
    System.out.format("%20s%30s%60s\n", sName, tName, tPath);

    this.fmtFile.format(lineSeparator.concat("\n"));
    System.out.println(lineSeparator);

    this.fmtFile.format("%40s%40s%30s\n", "Column Name |", "Data " +
        "type |", "Size |");
    System.out.format("%40s%40s%30s\n", "Column Name |", "Data " +
        "type |", "Size |");

    this.fmtFile.format(headingSeparator.concat("\n"));
    System.out.println(headingSeparator);

    for (Column column : this.columns) {
      String columnName = column.getColumnName().concat(" |");
      String columnDataType = column.getColumnDataType().concat(" |");
      String size = String.valueOf(column.getColumnSize()).concat(" |");

      this.fmtFile.format("%40s%40s%30s\n", columnName,
          columnDataType, size);
      System.out.format("%40s%40s%30s\n", columnName,
          columnDataType, size);
    }

    this.fmtFile.format(headingSeparator.concat("\n"));
    System.out.println(headingSeparator);

    // Emptied the current column data for the next iteration
    this.columns = new ArrayList<>();
  }

}
