package cn.hyf.sqllineage.metadata;

import java.util.List;

public interface TableMetaDataManager {

  /**
   * 通过传入表名称获取整张表的字段信息
   * 注意 ： 返回的字段需要保证顺序！
   *
   * @param tableName 表名称
   * @return fields
   */
  public List<String> getAllFieldsInfo(String tableName, String database);

  /**
   * 通过传入表名称获取整张表的字段信息
   * 注意 ： 返回的字段需要保证顺序！
   *
   * @param tableName 表名称
   * @return fields
   */
  public default List<String> getAllFieldsInfo(String tableName) {
    return getAllFieldsInfo(tableName, "default");
  }


}
