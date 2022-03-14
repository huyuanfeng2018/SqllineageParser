package cn.hyf.sqllineage;

import java.util.LinkedList;
import java.util.List;

public class SqllineageResult {
  private String tableName;
  private List<Field> fields = new LinkedList<>();

  public SqllineageResult(String tableName) {
    this.tableName = tableName;
  }

  public List<Field> getFields() {
    return fields;
  }

  public void addField(Field field) {
    fields.add(field);
  }

  @Override
  public String toString() {
    return "SqllineageResult{" +
        "tableName='" + tableName + '\'' +
        ", fields=" + fields +
        '}';
  }

  public static class Field {
    private String fieldName;
    private List<SourceField> from = new LinkedList<>();

    public Field(String fieldName) {
      this.fieldName = fieldName;
    }

    public String getFieldName() {
      return fieldName;
    }

    public List<SourceField> getFrom() {
      return from;
    }

    public void addFrom(SourceField sourceField) {
      this.from.add(sourceField);
    }

    @Override
    public String toString() {
      return "Field{" +
          "fieldName='" + fieldName + '\'' +
          ", from=" + from +
          '}';
    }
  }

  public static class SourceField {
    private String tableName;
    private String fieldName;

    public SourceField(String tableName, String fieldName) {
      this.tableName = tableName;
      this.fieldName = fieldName;
    }

    public String getTableName() {
      return tableName;
    }

    public String getFieldName() {
      return fieldName;
    }

    @Override
    public String toString() {
      return "SourceField{" +
          "tableName='" + tableName + '\'' +
          ", fieldName='" + fieldName + '\'' +
          '}';
    }
  }



}
