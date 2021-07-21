package org.group15.util;

import java.util.HashSet;
import java.util.Locale;

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

}
