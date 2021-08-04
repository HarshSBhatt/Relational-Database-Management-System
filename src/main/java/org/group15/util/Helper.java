package org.group15.util;

import org.group15.database.Column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Helper {

  public static String getTablePath(String schemaName, String tableName) {
    return AppConstants.ROOT_FOLDER_PATH + "/" + schemaName + "/tables/" + tableName + ".dp15";
  }

  public static String getTableMetadataPath(String schemaName,
                                            String tableName) {
    return AppConstants.ROOT_FOLDER_PATH + "/" + schemaName + "/table_metadata/" + tableName + ".dp15";
  }

  public static String getSchemaPath(String schemaName) {
    return AppConstants.ROOT_FOLDER_PATH + "/" + schemaName;
  }

  public static String getLockFolderPath(String schemaName) {
    return AppConstants.ROOT_FOLDER_PATH + "/" + schemaName + "/lock";
  }

  public static String getLockFilePath(String schemaName, String tableName) {
    return AppConstants.ROOT_FOLDER_PATH + "/" + schemaName + "/lock/" + tableName + ".dp15";
  }

  public static HashSet<String> getDataTypes() {

    HashSet<String> values = new HashSet<>();

    for (DataType dt : DataType.values()) {
      values.add(dt.name().toLowerCase());
    }

    return values;
  }

  public static long getOccurrenceOf(String input, char search) {
    long count = 0;
    for (int i = 0; i < input.length(); i++) {
      if (input.charAt(i) == search)
        count++;
    }
    return count;
  }

  public static boolean isInteger(String str) {
    if (str == null) {
      return false;
    }
    int length = str.length();
    if (length == 0) {
      return false;
    }
    int i = 0;
    if (str.charAt(0) == '-') {
      if (length == 1) {
        return false;
      }
      i = 1;
    }
    for (; i < length; i++) {
      char c = str.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  }

  public static StringBuilder replaceFileContent(File tableFile,
                                                 String valueToBeReplaced,
                                                 boolean isMetadataFile) throws IOException {
    StringBuilder newFileContent = new StringBuilder();

    BufferedReader br =
        new BufferedReader(new FileReader(tableFile));

    String line;
    while ((line = br.readLine()) != null) {
      String[] columnInfo = line.split(AppConstants.DELIMITER_TOKEN);
      if (isMetadataFile) {
        if (!line.contains(valueToBeReplaced.toLowerCase())) {
          newFileContent.append(line).append("\n");
        }
      } else {
        int i = 0;
        for (String info : columnInfo) {
          String colName = info.split("=")[0];
          if (!colName.equalsIgnoreCase(valueToBeReplaced)) {
            if (i == columnInfo.length - 1) {
              newFileContent.append(info);
            } else {
              newFileContent.append(info).append(AppConstants.DELIMITER_TOKEN);
            }
          }
          i++;
        }
        newFileContent.append("\n");
      }
    }
    return newFileContent;
  }

  public static StringBuilder changeColumnNameInFile(File tableFile,
                                                     String oldColumnName,
                                                     String newColumnName) throws IOException {
    StringBuilder newFileContent = new StringBuilder();

    BufferedReader br =
        new BufferedReader(new FileReader(tableFile));

    String line;
    while ((line = br.readLine()) != null) {
      String[] columnInfo = line.split(AppConstants.DELIMITER_TOKEN);
      int i = 0;
      for (String info : columnInfo) {
        String colName = info.split("=")[0];
        if (colName.equalsIgnoreCase(oldColumnName)) {
          if (i == columnInfo.length - 1) {
            newFileContent.append(info.replaceAll(colName, newColumnName));
          } else {
            newFileContent.append(info.replaceAll(colName, newColumnName)).append(AppConstants.DELIMITER_TOKEN);
          }
        } else {
          if (i == columnInfo.length - 1) {
            newFileContent.append(info);
          } else {
            newFileContent.append(info).append(AppConstants.DELIMITER_TOKEN);
          }
        }
        i++;
      }
      newFileContent.append("\n");
    }
    return newFileContent;
  }

  public static boolean isColumnExist(Map<String, Column> columns,
                                      String existingColumnName) {
    for (String key : columns.keySet()) {
      Column column = columns.get(key);
      if (column.getColumnName().equalsIgnoreCase(existingColumnName)) {
        return true;
      }
    }
    return false;
  }

  public static void printTable(List<Map<String, Object>> values) {
    String lineSeparator =
        "=============================================================================================================================================";

    if (values.isEmpty()) {
      System.out.println("No rows returned");
    } else {
      int i = 0;
      for (Map<String, Object> val : values) {
        Set<String> colNames = val.keySet();
        if (i == 0) {
          for (String colName : colNames) {
            System.out.format("%20s", colName.concat(" |"));
          }
          System.out.println("\n" + lineSeparator);
          for (String colName : colNames) {
            System.out.format("%20s", String.valueOf(val.get(colName)).concat(" |"));
          }
          System.out.print("\n");
        } else {
          for (String colName : colNames) {
            System.out.format("%20s", String.valueOf(val.get(colName)).concat(" |"));
          }
          System.out.print("\n");
        }
        i++;
      }
      System.out.println(lineSeparator);
    }
  }

  public static <T> List<T> convertSetToList(Set<T> set) {
    return new ArrayList<>(set);
  }

  public static <T> Set<T> convertListToSet(List<T> list) {
    return new HashSet<>(list);
  }

}
