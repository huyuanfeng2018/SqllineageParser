package cn.hyf.sqllineage;

import cn.hutool.core.lang.Pair;
import cn.hyf.sqllineage.metadata.TableMetaDataManager;
import cn.hyf.sqllineage.metadata.WithMetaDataManager;
import com.alibaba.druid.sql.ast.SQLAdhocTableSource;
import com.alibaba.druid.sql.ast.SQLArgument;
import com.alibaba.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLKeep;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOver;
import com.alibaba.druid.sql.ast.SQLParameter;
import com.alibaba.druid.sql.ast.SQLPartitionByValue;
import com.alibaba.druid.sql.ast.SQLPartitionSpec;
import com.alibaba.druid.sql.ast.SQLSubPartition;
import com.alibaba.druid.sql.ast.SQLSubPartitionByHash;
import com.alibaba.druid.sql.ast.SQLSubPartitionByList;
import com.alibaba.druid.sql.ast.SQLSubPartitionByRange;
import com.alibaba.druid.sql.ast.SQLTableDataType;
import com.alibaba.druid.sql.ast.SQLUnionDataType;
import com.alibaba.druid.sql.ast.SQLWindow;
import com.alibaba.druid.sql.ast.SQLZOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllExpr;
import com.alibaba.druid.sql.ast.expr.SQLAnyExpr;
import com.alibaba.druid.sql.ast.expr.SQLArrayExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBigIntExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExprGroup;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseStatement;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLContainsExpr;
import com.alibaba.druid.sql.ast.expr.SQLDateExpr;
import com.alibaba.druid.sql.ast.expr.SQLDateTimeExpr;
import com.alibaba.druid.sql.ast.expr.SQLDecimalExpr;
import com.alibaba.druid.sql.ast.expr.SQLDefaultExpr;
import com.alibaba.druid.sql.ast.expr.SQLDoubleExpr;
import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.expr.SQLExtractExpr;
import com.alibaba.druid.sql.ast.expr.SQLFloatExpr;
import com.alibaba.druid.sql.ast.expr.SQLGroupingSetExpr;
import com.alibaba.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.druid.sql.ast.expr.SQLJSONExpr;
import com.alibaba.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.druid.sql.ast.expr.SQLMatchAgainstExpr;
import com.alibaba.druid.sql.ast.expr.SQLNCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLNotExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLRealExpr;
import com.alibaba.druid.sql.ast.expr.SQLSequenceExpr;
import com.alibaba.druid.sql.ast.expr.SQLSizeExpr;
import com.alibaba.druid.sql.ast.expr.SQLSmallIntExpr;
import com.alibaba.druid.sql.ast.expr.SQLSomeExpr;
import com.alibaba.druid.sql.ast.expr.SQLTimeExpr;
import com.alibaba.druid.sql.ast.expr.SQLTimestampExpr;
import com.alibaba.druid.sql.ast.expr.SQLTinyIntExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuesExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterMaterializedViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.druid.sql.ast.statement.SQLCheck;
import com.alibaba.druid.sql.ast.statement.SQLCloneTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnCheck;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLColumnReference;
import com.alibaba.druid.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.druid.sql.ast.statement.SQLCreateMaterializedViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLDefault;
import com.alibaba.druid.sql.ast.statement.SQLExplainAnalyzeStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprHint;
import com.alibaba.druid.sql.ast.statement.SQLExprStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLExternalRecordFormat;
import com.alibaba.druid.sql.ast.statement.SQLForStatement;
import com.alibaba.druid.sql.ast.statement.SQLForeignKeyImpl;
import com.alibaba.druid.sql.ast.statement.SQLIfStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLLateralViewTableSource;
import com.alibaba.druid.sql.ast.statement.SQLLoopStatement;
import com.alibaba.druid.sql.ast.statement.SQLMergeStatement;
import com.alibaba.druid.sql.ast.statement.SQLPartitionRef;
import com.alibaba.druid.sql.ast.statement.SQLPrimaryKeyImpl;
import com.alibaba.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.druid.sql.ast.statement.SQLScriptCommitStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.ast.statement.SQLTableLike;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.ast.statement.SQLUnnestTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.ast.statement.SQLValuesQuery;
import com.alibaba.druid.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.druid.sql.ast.statement.SQLWhileStatement;
import com.alibaba.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.druid.sql.dialect.hive.ast.HiveInputOutputFormat;
import com.alibaba.druid.sql.dialect.hive.stmt.HiveCreateTableStatement;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HiveSQLASTVisitorv implements SQLASTVisitor, Closeable {
  private TableNode tableNode;

  /**
   * 当以 public boolean visit(SQLInsertStatement x) 作为入口进入时候
   * 会初始化对应的columns,如： insert into tablea(a,b,c)
   * 需要将a,b,c 初始化到columns中
   */
  private List<SQLExpr> columns = new LinkedList<>();

  /**
   * 全局的TableMetaDataManager，线程独有,一次解析对应一个线程
   */
  public static ThreadLocal<WithMetaDataManager> METADATA_MANAGER = new ThreadLocal<>();

  public TableNode getTableNode() {
    return tableNode;
  }

  public HiveSQLASTVisitorv(TableNode tableNode) {
    this.tableNode = tableNode;
  }

  public HiveSQLASTVisitorv() {
  }

  @Override
  public String toString() {
    return "HiveSQLASTVisitorv2{" +
        "tableNode=" + tableNode +
        '}';
  }

  private void accept(SQLObject x, TableNode tableNode) {
    x.accept(this);
  }

  private void createSqlNode(SQLTableSource tableSource) {
    if (tableNode == null) {
      tableNode = new TableNode(tableSource);
    }
  }

  @Override
  public boolean visit(HiveCreateTableStatement x) {
    SQLExprTableSource tableSource = x.getTableSource();
    this.tableNode = new TableNode(tableSource);
    List<SQLTableElement> tableElementList = x.getTableElementList();
    for (SQLTableElement sqlTableElement : tableElementList) {
      SQLName name = ((SQLColumnDefinition) sqlTableElement).getName();
      columns.add(name);
    }

    SQLSelect select = x.getSelect();
    select.accept(this);
    return false;
  }


  /**
   * 解析insert into dest select 语句
   * 下面解析对应的select语句
   * <p>
   * 一般etl脚本是以insert语句接一个select语句构成，也会伴随着with语句，当对这种语句进行解析时，此方法为入口方法。
   * 1）先对with语句部分进行解析，此步骤需要将with语句中查询出的表构建一个临时元数据，当后续insert语句中使用到时候，通过这些
   * 元数据通过遍历能找到最终的实体表，具体实现为{@link WithMetaDataManager#buildWithMetaData(SQLWithSubqueryClause)}
   * 2）对具体的select语句进行解析，生成最终的tablenode
   *
   * @param x SQLInsertStatement
   */
  @Override
  public boolean visit(SQLInsertStatement x) {
    //对with信息进行初始化，主要是获取其元数据信息，并且构建metaDataManager。
    WithMetaDataManager withMetaDataManager = new WithMetaDataManager(x.getWith());
    TableMetaDataManager metaDataManager = METADATA_MANAGER.get();
    withMetaDataManager.addDelegates(metaDataManager);
    METADATA_MANAGER.set(withMetaDataManager);

    tableNode = new TableNode(x.getTableSource());
    this.columns = x.getColumns();
    if (columns.size() == 0) {
      //表示当前insert语句类似于  insert into table1 select a,b,c from table2
      //相关字段需要从元数据中获取
      List<SQLExpr> collect = METADATA_MANAGER.get().getAllFieldsInfo(
          tableNode.getTableSource().computeAlias()).stream().map(
          new Function<String, SQLExpr>() {
            @Override
            public SQLExpr apply(String columnName) {
              return new SQLIdentifierExpr(columnName);
            }
          }
      ).collect(Collectors.toList());
      this.columns = collect;

    }
    x.getQuery().accept(this);
    return true;
  }


  /**
   * 对SQLSelectQueryBlock语句进行解析
   * SQLSelectQueryBlock表示当前查询不是left join union等链接查询。
   * 此处是根据selectList构建对应的tableNode
   *
   * @param x x
   */
  @Override
  public boolean visit(SQLSelectQueryBlock x) {
    //判断当前insert到表中的字段以及对应的select中的对应字段
    List<Pair<String, SQLExpr>> insertMappings = new LinkedList<>();

    List<SQLSelectItem> selectList = x.getSelectList();
    if (columns != null && columns.size() > 0) {
      //表示已经指定了插入的字段
      for (int i = 0; i < selectList.size(); i++) {
        insertMappings.add(Pair.of(columns.get(i).toString(), selectList.get(i).getExpr()));
      }

    } else {
      //表示未指定字段，需要从子查询中获取映射
      for (SQLSelectItem sqlSelectItem : selectList) {
        String alias = sqlSelectItem.getAlias();
        insertMappings.add(Pair.of(alias != null ? alias : sqlSelectItem.toString(), sqlSelectItem.getExpr()));
      }
    }

    insertMappings.forEach(
        insertMapping -> {
          String columnName = insertMapping.getKey();
          SQLExpr fromSQLExpr = insertMapping.getValue();
          tableNode.addColumn(columnName, fromSQLExpr, x.getFrom());
        }
    );
    return false;
  }

  @Override
  public boolean visit(SQLSelect x) {
    SQLSelectQuery query = x.getQuery();
    accept(query, tableNode);
    return false;
  }

  @Override
  public boolean visit(SQLExprTableSource x) {
    tableNode.addExprTableSource(x);
    return false;
  }

  @Override
  public boolean visit(SQLSubqueryTableSource x) {
    SQLSelect select = x.getSelect();
    SQLSelectQuery query = select.getQuery();
    accept(query, tableNode);
    return false;
  }

  @Override
  public boolean visit(SQLJoinTableSource x) {
    SQLTableSource left = x.getLeft();
    SQLTableSource right = x.getRight();
    accept(left, tableNode);
    accept(right, tableNode);
    return false;
  }

  @Override
  public boolean visit(SQLUnionQuery x) {
    SQLSelectQuery left = x.getLeft();
    SQLSelectQuery right = x.getRight();
    accept(left, tableNode);
    accept(right, tableNode);
    return false;
  }


  @Override
  public boolean visit(SQLAllColumnExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLBetweenExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLBinaryOpExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCaseExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCaseExpr.Item x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCaseStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCaseStatement.Item x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCastExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCharExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLExistsExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLIdentifierExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLInListExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLIntegerExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLSmallIntExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLBigIntExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLTinyIntExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLNCharExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLNotExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLNullExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLNumberExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLRealExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLPropertyExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLSelectGroupByClause x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLSelectItem x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLAggregateExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLVariantRefExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLQueryExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLUnaryExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLHexExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLOrderBy x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLZOrderBy x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLSelectOrderByItem x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCreateTableStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLColumnDefinition x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLColumnDefinition.Identity x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLDataType x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLInsertStatement.ValuesClause x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLUpdateSetItem x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLUpdateStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCreateViewStatement x) {
    createSqlNode(x.getTableSource());
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCreateViewStatement.Column x) {
    return SQLASTVisitor.super.visit(x);
  }


  @Override
  public boolean visit(SQLJoinTableSource.UDJ x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLSomeExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLAnyExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLAllExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLInSubQueryExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLListExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLDefaultExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLOver x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLKeep x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLColumnPrimaryKey x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLColumnUniqueKey x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLWithSubqueryClause x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLWithSubqueryClause.Entry x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCheck x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLDefault x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLColumnCheck x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLExprHint x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLPrimaryKeyImpl x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLColumnReference x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLForeignKeyImpl x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLBooleanExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLTimestampExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLDateTimeExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLDoubleExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLFloatExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLBinaryExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLArrayExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLGroupingSetExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLIfStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLIfStatement.ElseIf x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLIfStatement.Else x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLLoopStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLParameter x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLBlockStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLSubPartition x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLSubPartitionByHash x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLSubPartitionByRange x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLSubPartitionByList x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLSequenceExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLMergeStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLMergeStatement.MergeUpdateClause x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLMergeStatement.MergeInsertClause x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLDateExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLWhileStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLArgument x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCreateMaterializedViewStatement x) {
//    createSqlNode(x.getTableElementList());
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLBinaryOpExprGroup x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLScriptCommitStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLReplaceStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLIntervalExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLLateralViewTableSource x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLExprStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLAlterViewStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLExternalRecordFormat x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLAlterMaterializedViewStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLMatchAgainstExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLTimeExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLValuesExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLContainsExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLValuesTableSource x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLExtractExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLWindow x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLJSONExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLDecimalExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLUnionDataType x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLSizeExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCurrentTimeExpr x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLAdhocTableSource x) {
    return SQLASTVisitor.super.visit(x);
  }



  @Override
  public boolean visit(HiveInputOutputFormat x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLExplainAnalyzeStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLPartitionRef x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLPartitionRef.Item x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLForStatement x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLUnnestTableSource x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLTableLike x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLValuesQuery x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLPartitionByValue x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLPartitionSpec x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLPartitionSpec.Item x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLTableDataType x) {
    return SQLASTVisitor.super.visit(x);
  }

  @Override
  public boolean visit(SQLCloneTableStatement x) {
    return SQLASTVisitor.super.visit(x);
  }


  /**
   * close
   */
  @Override
  public void close() {
    METADATA_MANAGER.remove();
  }
}
