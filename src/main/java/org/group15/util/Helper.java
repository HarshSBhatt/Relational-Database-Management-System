package org.group15.util;

import java.util.HashSet;

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
      values.add(dt.name());
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

}
