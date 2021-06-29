package org.group15.util;

public class Helper {
    public static String getTablePath(String schemaName, String tableName) {
        return AppConstants.ROOT_FOLDER_PATH + "/" + schemaName + "/tables/" + tableName;
    }

    public static String getSchemaPath(String schemaName) {
        return AppConstants.ROOT_FOLDER_PATH + "/" + schemaName;
    }
}
