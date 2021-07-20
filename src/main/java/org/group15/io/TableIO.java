package org.group15.io;

import org.group15.util.Helper;

import java.io.File;

public class TableIO {

  public void read(String schemaName, String tableName) {
    try {
      String path = Helper.getTablePath(schemaName, tableName);
      File file = new File(path);
      if (file.exists()) {
        System.out.println("Logic here");
      } else {
        System.out.println("Table does not exists");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public boolean create(String schemaName, String tableName) {
    try {
      String path = Helper.getTablePath(schemaName, tableName);
      File file = new File(path);
      if (file.exists()) {
        System.out.println("Table already exists");
        return false;
      } else {
        file.createNewFile();
        System.out.println("Table created");
        return true;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  public boolean isExist(String schemaName, String tableName) {
    String path = Helper.getTablePath(schemaName, tableName);
    File file = new File(path);
    return file.exists();
  }

}
