package cn.hyf.sqllineage.table;

import com.alibaba.druid.sql.ast.SQLExpr;

import java.util.List;

/**
 * 血缘关系
 *
 */
public class Sqllineage {
  private String tableName;
  private List<LineageColumn> columns;

  public static class LineageColumn{
    private SQLExpr expr;
    /**
     * 当前字段是由哪些字段转化而来
     */
    private List<LineageColumn> from;

    /**
     * 当前字段属于哪一个表，当此值为空表示当前字段是由别的字段转化过来
     */
    private String tableName;


  }
}
