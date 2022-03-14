package cn.hyf.sqllineage.table;

import java.util.LinkedList;
import java.util.List;

public class Field {
  private String fieldName;
  private List<Field> froms;
  private String tableName;

  public Field(String fieldName) {
    this.fieldName = fieldName;
    this.froms = new LinkedList<>();
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public List<Field> getFroms() {
    return froms;
  }

  public void setFroms(List<Field> froms) {
    this.froms = froms;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void addFrom(Field from) {
    froms.add(from);
  }
}
