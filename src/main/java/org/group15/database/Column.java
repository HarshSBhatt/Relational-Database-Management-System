package org.group15.database;

public class Column {

  private String columnName;

  private String columnDataType;

  private Object columnValue;

  private int columnSize;

  private boolean primaryKey;

  private boolean foreignKey;

  private boolean autoIncrement;

  private boolean notNullFlag;

  private boolean uniqueFlag;

  private String defaultValue;

  private String foreignKeyTable;

  private String foreignKeyColumn;

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getColumnDataType() {
    return columnDataType;
  }

  public void setColumnDataType(String columnDataType) {
    this.columnDataType = columnDataType;
  }

  public Object getColumnValue() {
    return columnValue;
  }

  public void setColumnValue(Object columnValue) {
    this.columnValue = columnValue;
  }

  public int getColumnSize() {
    return columnSize;
  }

  public void setColumnSize(int columnSize) {
    this.columnSize = columnSize;
  }

  public boolean isPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(boolean primaryKey) {
    this.primaryKey = primaryKey;
  }

  public boolean isForeignKey() {
    return foreignKey;
  }

  public void setForeignKey(boolean foreignKey) {
    this.foreignKey = foreignKey;
  }

  public boolean isAutoIncrement() {
    return autoIncrement;
  }

  public void setAutoIncrement(boolean autoIncrement) {
    this.autoIncrement = autoIncrement;
  }

  public boolean isNotNullFlag() {
    return notNullFlag;
  }

  public void setNotNullFlag(boolean notNullFlag) {
    this.notNullFlag = notNullFlag;
  }

  public boolean isUniqueFlag() {
    return uniqueFlag;
  }

  public void setUniqueFlag(boolean uniqueFlag) {
    this.uniqueFlag = uniqueFlag;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }

  public String getForeignKeyTable() {
    return foreignKeyTable;
  }

  public void setForeignKeyTable(String foreignKeyTable) {
    this.foreignKeyTable = foreignKeyTable;
  }

  public String getForeignKeyColumn() {
    return foreignKeyColumn;
  }

  public void setForeignKeyColumn(String foreignKeyColumn) {
    this.foreignKeyColumn = foreignKeyColumn;
  }

}
