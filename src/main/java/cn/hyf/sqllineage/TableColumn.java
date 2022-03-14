package cn.hyf.sqllineage;

import cn.hutool.core.lang.Pair;
import cn.hyf.sqllineage.metadata.WithMetaDataManager;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class TableColumn implements SQLASTVisitor {

  /**
   * 当前字段来自于哪一个具体表的哪个字段,这个字段存在表示不需要往下继续遍历了
   */
  private Pair<String, String> tableNameAndColumnName;

  /**
   * 当前字段别名,也是最终的名称
   */
  private String columnAlias;

  /**
   * 表达式
   */
  private SQLExpr expr;

  /**
   * 当前表来自于哪个子表，这个字段和tableNameAndColumnNames有一个为空
   */
  private List<TableColumn> tableColumns = new LinkedList<>();

  /**
   * 当前字段来自的表
   */
  private SQLTableSource tableSource;

  public TableColumn(Pair<String, String> tableNameAndColumnName, String columnAlias,
                     SQLExpr expr) {
    this.tableNameAndColumnName = tableNameAndColumnName;
    this.columnAlias = columnAlias;
    this.expr = expr;
  }


  public TableColumn(String columnAlias, SQLExpr expr, SQLTableSource tableSource) {
    this.columnAlias = columnAlias;
    this.expr = expr;
    this.tableSource = tableSource;
  }

  public Pair<String, String> getTableNameAndColumnName() {
    return tableNameAndColumnName;
  }

  public List<TableColumn> getTableColumns() {
    return tableColumns;
  }

  public void setTableColumns(List<TableColumn> tableColumns) {
    this.tableColumns = tableColumns;
  }

  @Override
  public boolean visit(SQLAllColumnExpr x) {
    List<TableColumn> columns = getTableColumns(expr);
    tableColumns.addAll(columns);
    return false;
  }

  @Override
  public boolean visit(SQLBinaryOpExpr x) {
    SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
    TableColumn leftVisitor = new TableColumn(null, sqlBinaryOpExpr.getLeft(), tableSource);
    TableColumn rightVisitor = new TableColumn(null, sqlBinaryOpExpr.getRight(), tableSource);
    sqlBinaryOpExpr.getLeft().accept(leftVisitor);
    sqlBinaryOpExpr.getRight().accept(rightVisitor);
    this.tableColumns.add(leftVisitor);
    this.tableColumns.add(rightVisitor);
    return false;
  }

  @Override
  public boolean visit(SQLCaseExpr x) {
    List<TableColumn> columns = new ArrayList<>();
    SQLCaseExpr sqlCaseExpr = (SQLCaseExpr) expr;

    for (SQLCaseExpr.Item item : sqlCaseExpr.getItems()) {
      TableColumn visitor = new TableColumn(null, item.getConditionExpr(), tableSource);
      item.getConditionExpr().accept(visitor);
      columns.add(visitor);
    }
    TableColumn visitor = new TableColumn(null, sqlCaseExpr.getElseExpr(), tableSource);
    sqlCaseExpr.getElseExpr().accept(visitor);
    columns.add(visitor);

    tableColumns.addAll(columns);
    return false;

  }

  @Override
  public boolean visit(SQLIdentifierExpr x) {
    //表示 select a from table1,table2,table3
    //注意！！！ 如果没有table1 table2 table3 的元数据会导致这里获取不到信息
    SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) expr;

    TableNode tableNode = conversionTableSource(this.tableSource);

    //含有此字段的表数量，这里需要判定，如果说有多个表都存在此字段这里需要抛出异常
    int colOfRepetitions = 0;

    for (SQLExprTableSource exprTableSource : tableNode.getExprTableSources()) {
      SQLExpr tableExpr = exprTableSource.getExpr();
      WithMetaDataManager metaDataManager = HiveSQLASTVisitorv.METADATA_MANAGER.get();
      String tableName = exprTableSource.getTableName();
      if (tableExpr instanceof SQLIdentifierExpr) {
        //先查看是否有对应的with表，如果有则取with表中的字段，如果当前不是with表再去查询主元数据获取原生的表
        Collection<TableColumn> columns = metaDataManager.findTableColumnFromWithTable(tableName);
        if (columns.size() > 0) {
          for (TableColumn tableColumn : columns) {
            if (sqlIdentifierExpr.getName().equals(tableColumn.getColumnAlias())) {
              tableColumns.add(tableColumn);
              colOfRepetitions++;
            }
          }
        }

        List<String> allFieldsInfo = metaDataManager.getAllFieldsInfo(tableName);
        for (String columnName : allFieldsInfo) {
          if (sqlIdentifierExpr.getName().equals(columnName)) {
            tableColumns.add(new TableColumn(Pair.of(tableName, columnName), columnName, this.expr));
            colOfRepetitions++;
          }
        }

      } else if (tableExpr instanceof SQLPropertyExpr) {
        SQLPropertyExpr propertyExpr = (SQLPropertyExpr) tableExpr;
        String database = propertyExpr.getOwnerName();
        List<String> allFieldsInfo = metaDataManager.getAllFieldsInfo(tableName, database);
        for (String columnName : allFieldsInfo) {
          if (sqlIdentifierExpr.getName().equals(columnName)) {
            tableColumns.add(new TableColumn(Pair.of(tableName, columnName), columnName, this.expr));
            colOfRepetitions++;
          }
        }
      }
    }

    if (colOfRepetitions > 1) {
      throw new RuntimeException(
          String.format("More than one table has fields :【%s】 ,Please check your SQL statement ：%s",
              sqlIdentifierExpr.getName(), x.getParent().getParent().toString()));
    }
    return false;
  }

  @Override
  public boolean visit(SQLPropertyExpr x) {
    // expr 表达式为： a.b 表示当前从a表b字段，tableSource中获取
    SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) expr;
    this.tableNameAndColumnName = Pair.of(sqlPropertyExpr.getOwnerName(), sqlPropertyExpr.getName());
    WithMetaDataManager metaDataManager = HiveSQLASTVisitorv.METADATA_MANAGER.get();

    TableNode tableNode = conversionTableSource(tableSource);

    for (SQLExprTableSource exprTableSource : tableNode.getExprTableSources()) {
      String ownerName = sqlPropertyExpr.getOwnerName();
      String tableName = exprTableSource.getTableName();
      String alias = exprTableSource.getAlias();
      String alias2 = exprTableSource.getAlias2();
      if (ownerName.equals(tableName) || ownerName.equals(alias) || ownerName.equals(alias2)) {
        //表示当前字段来源于此表, 此处需要判断是否是with表
        Collection<TableColumn> withColumns = metaDataManager.findTableColumnFromWithTable(tableName);
        if (withColumns.size() > 0) {
          for (TableColumn tableColumn : withColumns) {
            if (sqlPropertyExpr.getName().equals(tableColumn.getColumnAlias())) {
              tableColumns.add(tableColumn);
            }
          }
        } else {
          this.tableNameAndColumnName = Pair.of(tableName, sqlPropertyExpr.getName());
        }
      }
    }
    return false;
  }

  @Override
  public boolean visit(SQLMethodInvokeExpr x) {
    List<TableColumn> columns = methodInvokeExprToColumn((SQLMethodInvokeExpr) expr);
    tableColumns.addAll(columns);
    return false;
  }

  @Override
  public boolean visit(SQLCastExpr x) {
    SQLCastExpr sqlCastExpr = (SQLCastExpr) expr;
    SQLExpr expr = sqlCastExpr.getExpr();
    TableColumn tableColumn = new TableColumn(null, expr, tableSource);
    expr.accept(tableColumn);
    tableColumns.add(tableColumn);
    return false;
  }

  @Override
  public boolean visit(SQLQueryExpr x) {
    SQLQueryExpr queryExpr = (SQLQueryExpr) expr;
    SQLSelect select = queryExpr.getSubQuery();
    HiveSQLASTVisitorv hiveSQLASTVisitorv = new HiveSQLASTVisitorv(new TableNode(tableSource));
    select.accept(hiveSQLASTVisitorv);
    TableNode tableNode = hiveSQLASTVisitorv.getTableNode();
    Collection<TableColumn> columns = tableNode.getAllColumns();
    this.tableColumns.addAll(columns);
    return false;
  }


  private List<TableColumn> getTableColumns(SQLExpr expr) {
    List<TableColumn> columns = new LinkedList<>();
    SQLTableSource tableSource = this.tableSource;

    /*-------------构建tableNode---------------*/
    TableNode tableNode = conversionTableSource(tableSource);
    /*----------------------------------------*/

    for (SQLExprTableSource exprTableSource : tableNode.getExprTableSources()) {
      String tableName = exprTableSource.getTableName();
      List<String> allFieldsInfo = HiveSQLASTVisitorv.METADATA_MANAGER.get().getAllFieldsInfo(tableName);

      for (String columnName : allFieldsInfo) {
        columns.add(new TableColumn(Pair.of(tableName, columnName), columnName, expr));
      }
    }
    for (TableColumn tableNodeColumn : tableNode.getAllColumns()) {
      expr.accept(tableNodeColumn);
      columns.add(tableNodeColumn);
    }
    return columns;
  }


  private List<TableColumn> methodInvokeExprToColumn(SQLMethodInvokeExpr expr) {
    List<TableColumn> columns = new LinkedList<>();
    List<SQLExpr> arguments = expr.getArguments();
    //表示当前SQLExpr来自于多个地方
    for (SQLExpr argument : arguments) {
      TableColumn column = new TableColumn(null, argument, tableSource);
      argument.accept(column);
      columns.add(column);
    }
    return columns;
  }

  /**
   * SQLTableSource 转化为tableNode
   *
   * @param tableSource tableSource
   * @return TableNode Conversion
   */
  private TableNode conversionTableSource(SQLTableSource tableSource) {
    TableNode tableNode = new TableNode(tableSource);
    HiveSQLASTVisitorv hiveSQLASTVisitorv = new HiveSQLASTVisitorv(tableNode);
    tableSource.accept(hiveSQLASTVisitorv);
    return tableNode;
  }

  public String getColumnAlias() {
    return columnAlias;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableColumn)) return false;
    TableColumn that = (TableColumn) o;
    return Objects.equals(getTableNameAndColumnName(), that.getTableNameAndColumnName()) &&
        Objects.equals(getColumnAlias(), that.getColumnAlias()) &&
        Objects.equals(expr, that.expr) &&
        Objects.equals(getTableColumns(), that.getTableColumns()) &&
        Objects.equals(tableSource, that.tableSource);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTableNameAndColumnName(), getColumnAlias(), expr, getTableColumns(), tableSource);
  }
}
