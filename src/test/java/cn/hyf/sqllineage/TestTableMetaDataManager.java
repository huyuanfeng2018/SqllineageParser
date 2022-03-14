package cn.hyf.sqllineage;


import cn.hyf.sqllineage.metadata.TableMetaDataManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestTableMetaDataManager implements TableMetaDataManager {

  private Map<String, List<String>> tables;

  public TestTableMetaDataManager(Map<String, List<String>> tables) {
    this.tables = tables;
  }

  @Override
  public List<String> getAllFieldsInfo(String tableName, String database) {
    List<String> result = tables.get(tableName);
    if (result != null) {
      return result;
    } else {
      return Collections.emptyList();
    }  }
}
