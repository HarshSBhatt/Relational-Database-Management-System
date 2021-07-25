package org.group15.io;

import org.group15.util.Helper;

import java.io.File;
import java.io.IOException;

public class CustomLock {

  public void lock(String schemaName, String tableName) throws IOException {
    String lockFolderPath = Helper.getLockFolderPath(schemaName);
    String lockFilePath = Helper.getLockFilePath(schemaName, tableName);

    File lockFolder = new File(lockFolderPath);
    File lockFile = new File(lockFilePath);

    if (!lockFolder.exists()) {
      lockFolder.mkdirs();
    }

    if (!lockFile.exists()) {
      lockFile.createNewFile();
    }
  }

  public void unlock(String schemaName, String tableName) throws InterruptedException {
    String lockFilePath = Helper.getLockFilePath(schemaName, tableName);

    File lockFile = new File(lockFilePath);

    if (lockFile.exists()) {
      Thread.sleep(2000);
      lockFile.delete();
    }
  }

  public boolean isLocked(String schemaName, String tableName) {
    String lockFilePath = Helper.getLockFilePath(schemaName, tableName);

    File lockFile = new File(lockFilePath);

    return lockFile.exists();
  }

}
