package org.group15.io;

import org.group15.util.Helper;

import java.io.File;

public class TableIO {

  public boolean isMetadataTableExist(String schemaName, String tableName) {
    String path = Helper.getTableMetadataPath(schemaName, tableName);
    File file = new File(path);
    return file.exists();
  }

  public boolean isTableExist(String schemaName, String tableName) {
    String path = Helper.getTablePath(schemaName, tableName);
    File file = new File(path);
    return file.exists();
  }
}
