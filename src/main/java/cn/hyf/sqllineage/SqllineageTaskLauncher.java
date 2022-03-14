package cn.hyf.sqllineage;

import cn.hutool.core.lang.Pair;
import cn.hyf.sqllineage.metadata.TableMetaDataManager;
import cn.hyf.sqllineage.metadata.WithMetaDataManager;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 血缘分析入口
 */
public class SqllineageTaskLauncher {

  private ExecutorService executors = Executors.newFixedThreadPool(1);

  /**
   * 血缘分析入口方法
   *
   * @param tableMetaDataManager 表元数据管理者
   * @param insertSql            插入的sql语句
   */
  public Future<SqllineageResult> submit(TableMetaDataManager tableMetaDataManager, String insertSql) {
    Future<SqllineageResult> future = executors.submit(new Callable<SqllineageResult>() {
      @Override
      public SqllineageResult call() throws Exception {
        HiveSQLASTVisitorv visitor = new HiveSQLASTVisitorv();
        HiveSQLASTVisitorv.METADATA_MANAGER.set(new WithMetaDataManager(tableMetaDataManager));
        try {
          List<SQLStatement> statementList = SQLUtils.parseStatements(insertSql, JdbcConstants.HIVE);
          statementList.get(0).accept(visitor);
        } finally {
          visitor.close();
        }

        TableNode tableNode = visitor.getTableNode();
        Collection<TableColumn> simpleColumn = tableNode.simple();

        SqllineageResult result = new SqllineageResult(tableNode.getTableSource().computeAlias());

        for (TableColumn tableColumn : simpleColumn) {
          String columnAlias = tableColumn.getColumnAlias();
          SqllineageResult.Field field = new SqllineageResult.Field(columnAlias);
          result.addField(field);

          for (TableColumn column : tableColumn.getTableColumns()) {
            Pair<String, String> tableNameAndColumnName = column.getTableNameAndColumnName();
            SqllineageResult.SourceField sourceField = new SqllineageResult.SourceField(tableNameAndColumnName.getKey(),
                tableNameAndColumnName.getValue());
            field.addFrom(sourceField);
          }
        }
        return result;
      }
    });
    return future;
  }


}
