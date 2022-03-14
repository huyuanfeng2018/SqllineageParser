package cn.hyf.sqllineage;

import cn.hutool.core.lang.Pair;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

public class TableNode {

  private SQLTableSource tableSource;

  private List<SQLExprTableSource> exprTableSources = new LinkedList<>();

  /**
   * 当前table中的所有字段信息
   */
  private Multimap<String, TableColumn> columns = Multimaps.newSetMultimap(
      new LinkedHashMap<>(),
      HashSet::new
  );


  public TableNode(SQLTableSource tableSource) {
    this.tableSource = tableSource;
  }

  /**
   * 添加一个字段
   *
   * @param column column
   */
  public void addColumn(TableColumn column) {
    String columnAlias = column.getColumnAlias();
    columns.put(columnAlias, column);
  }

  public List<SQLExprTableSource> getExprTableSources() {
    return exprTableSources;
  }

  /**
   * 添加多个字段
   *
   * @param columns columns
   */
  public void addColumns(Collection<TableColumn> columns) {
    for (TableColumn column : columns) {
      addColumn(column);
    }
  }

  public void addExprTableSource(SQLExprTableSource tableSource) {
    exprTableSources.add(tableSource);
  }

  public void addColumn(String columnName, SQLExpr expr, SQLTableSource from) {
    TableColumn column = new TableColumn(columnName, expr, from);
    expr.accept(column);
    addColumn(column);
  }

  public Collection<TableColumn> getAllColumns() {
    return columns.values();
  }

  public SQLTableSource getTableSource() {
    return tableSource;
  }


  /**
   * ***************-> d -> e
   * 简化链路，如： a -> b -> c -> d  简化为： a -> e,d,b 只保留最终表
   * ***************-> j -> b
   */
  public Collection<TableColumn> simple() {
    Collection<TableColumn> tableColumns = columns.values();
    for (TableColumn tableColumn : tableColumns) {
      List<TableColumn> columns = simpleTableColumn(tableColumn);
      tableColumn.setTableColumns(columns);
    }
    return tableColumns;
  }

  private List<TableColumn> simpleTableColumn(TableColumn column) {

    List<TableColumn> tableColumns = column.getTableColumns();
    List<TableColumn> tableColumnList = new LinkedList<>();

    if (tableColumns.size() > 0) {
      for (TableColumn tableColumn : tableColumns) {
        List<TableColumn> columns = simpleTableColumn(tableColumn);
        tableColumnList.addAll(columns);
      }
      return tableColumnList;
    }

    Pair<String, String> tableNameAndColumnName = column.getTableNameAndColumnName();
    if (tableNameAndColumnName != null) {
      //表示已经到底了，不需要在往下进行遍历了
      return Collections.singletonList(column);
    }

    return Collections.singletonList(column);
  }

}
