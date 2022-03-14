package cn.hyf.sqllineage.metadata;

import cn.hyf.sqllineage.HiveSQLASTVisitorv;
import cn.hyf.sqllineage.TableColumn;
import cn.hyf.sqllineage.TableNode;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLWithSubqueryClause;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 在存在with语法的查询中，通过此类来对with中表的元数据进行管理
 */
public class WithMetaDataManager implements TableMetaDataManager {

  private List<TableMetaDataManager> delegates = new LinkedList<>();

  /**
   * 解析完成的withtable，key表示tableName value 是解析的结构
   * 如： with tmp as (select * from consumer)
   * key = tmp
   * value =  (select * from consumer) 部分解析出来的tableNode对象
   */
  private Map<String, TableNode> withTableMap = new HashMap<>();


  public WithMetaDataManager(SQLWithSubqueryClause withSubqueryClause) {
    if (withSubqueryClause != null) {
      buildWithMetaData(withSubqueryClause);
    }
  }


  public WithMetaDataManager(TableMetaDataManager... managers) {
    Collections.addAll(delegates, managers);
  }


  public void addDelegates(TableMetaDataManager... delegate) {
    Collections.addAll(delegates, delegate);
  }

  @Override
  public List<String> getAllFieldsInfo(String tableName, String database) {
    //先看delegates中是否存有元数据信息
    for (TableMetaDataManager delegate : delegates) {
      List<String> allFieldsInfo = delegate.getAllFieldsInfo(tableName, database);
      if (allFieldsInfo.size() > 0) {
        return allFieldsInfo;
      }
    }
    return Collections.emptyList();
  }


  public Collection<TableColumn> findTableColumnFromWithTable(String tableName) {
    TableNode tableNode = withTableMap.get(tableName);
    if (tableNode != null) {
      return tableNode.getAllColumns();
    } else {
      return Collections.emptyList();
    }
  }


  /**
   * 构建with语句中的所有表的tableNode。
   *
   * @param withSubqueryClause with语句对象
   */
  private void buildWithMetaData(SQLWithSubqueryClause withSubqueryClause) {
    for (SQLWithSubqueryClause.Entry entry : withSubqueryClause.getEntries()) {
      SQLSelectQuery query = entry.getSubQuery().getQuery();
      TableNode tableNode = new TableNode(entry);
      query.accept(new HiveSQLASTVisitorv(tableNode));
      withTableMap.put(tableNode.getTableSource().computeAlias(), tableNode);
    }
  }


}
