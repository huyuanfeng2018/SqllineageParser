#### 血缘解析框架使用说明

1）首先需要实现接口TableMetaDataManager

```java
public interface TableMetaDataManager {
  /**
   * 通过传入表名称获取整张表的字段信息
   * 注意 ： 返回的字段需要保证顺序！
   *
   * @param tableName 表名称
   * @return fields
   */
  public List<String> getAllFieldsInfo(String tableName);
}
```

在使用框架之前需要实现TableMetaDataManager接口，当前接口只有一个方法，需要实现此方法，入参为表名称，返回此表的所有字段（注意：1.需要保证返回字段的顺序和数据库中一致 2.在实现中最好是将表的数据信息维护到内存中，而不是通过接口方式调用网络io请求去获取元数据，因为在解析过程中可能会多次调用当前方法，如果每次发起一个io请求可能会导致压力过大，建议将元数据同步到内存中以map形式保存，key存放表名，value存放字段集合）

2）入口类为SqllineageTaskLauncher，下面给一个简单示例：

```java
@Test
  public void testCaseWhenSelect() throws ExecutionException, InterruptedException {
    //构建元数据管理
    TestTableMetaDataManager manager = buildManager(Lists.newArrayList(
        new TableMetaData("item", Lists.newArrayList("i_brand_id", "i_brand", "t_hour", "t_minute", "ext_price")),
        new TableMetaData("query71", Lists.newArrayList("a", "b", "c", "d", "e"))
    ));

    String sql = "insert overwrite table query71\n" +
        "select i_brand_id     brand_id,\n" +
        "       i_brand        brand,\n" +
        "       t_hour,\n" +
        "       t_minute,\n" +
        "       sum(ext_price) ext_price\n" +
        "from item,\n" +
        "     (select ws_ext_sales_price as ext_price,\n" +
        "             ws_sold_date_sk    as sold_date_sk,\n" +
        "             ws_item_sk         as sold_item_sk,\n" +
        "             ws_sold_time_sk    as time_sk\n" +
        "      from web_sales,\n" +
        "           date_dim\n" +
        "      where d_date_sk = ws_sold_date_sk\n" +
        "        and d_moy = 11\n" +
        "        and d_year = 1999\n" +
        "      union all\n" +
        "      select cs_ext_sales_price as ext_price,\n" +
        "             cs_sold_date_sk    as sold_date_sk,\n" +
        "             cs_item_sk         as sold_item_sk,\n" +
        "             cs_sold_time_sk    as time_sk\n" +
        "      from catalog_sales,\n" +
        "           date_dim\n" +
        "      where d_date_sk = cs_sold_date_sk\n" +
        "        and d_moy = 11\n" +
        "        and d_year = 1999\n" +
        "      union all\n" +
        "      select ss_ext_sales_price as ext_price,\n" +
        "             ss_sold_date_sk    as sold_date_sk,\n" +
        "             ss_item_sk         as sold_item_sk,\n" +
        "             ss_sold_time_sk    as time_sk\n" +
        "      from store_sales,\n" +
        "           date_dim\n" +
        "      where d_date_sk = ss_sold_date_sk\n" +
        "        and d_moy = 11\n" +
        "        and d_year = 1999\n" +
        "     ) tmp,\n" +
        "     time_dim\n" +
        "where sold_item_sk = i_item_sk\n" +
        "  and i_manager_id = 1\n" +
        "  and time_sk = t_time_sk\n" +
        "  and (t_meal_time = 'breakfast' or t_meal_time = 'dinner')\n" +
        "group by i_brand, i_brand_id, t_hour, t_minute\n" +
        "order by ext_price desc, i_brand_id\n" +
        ";\n";
    SqllineageTaskLauncher sqllineageTaskLauncher = new SqllineageTaskLauncher();
    SqllineageResult result = sqllineageTaskLauncher.submit(manager, sql).get();
    System.out.println(result);
  }
```

