package org.group15.database;

import java.io.FileWriter;

public class ERD {

  FileWriter eventLogsWriter;

  public ERD(FileWriter eventLogsWriter) {
    this.eventLogsWriter = eventLogsWriter;
  }

  public boolean generateERD(String schemaName) {
    // Logic here to generate ERD
    return true;
  }

}
