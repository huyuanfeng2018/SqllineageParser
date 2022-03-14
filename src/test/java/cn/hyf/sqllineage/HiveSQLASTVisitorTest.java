package cn.hyf.sqllineage;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class HiveSQLASTVisitorTest {
  private SqllineageTaskLauncher sqllineageTaskLauncher;

  @Before
  public void init() {
    sqllineageTaskLauncher = new SqllineageTaskLauncher();
  }

  @Test
  public void testInsertSelect() throws ExecutionException, InterruptedException {
    TestTableMetaDataManager manager = new TestTableMetaDataManager(
        new HashMap<String, List<String>>() {{
          put("customer", new LinkedList<String>() {{
            add("c_customer_id");
            add("c_first_name");
            add("c_last_name");
            add("c_preferred_cust_flag");
            add("c_birth_country");
            add("c_login");
            add("c_email_address");
          }});

          put("store_sales", new LinkedList<String>() {{
            add("ss_ext_list_price");
            add("ss_ext_discount_amt");
            add("s");
          }});
          put("date_dim", new LinkedList<String>() {{
            add("d_year");
          }});
        }}
    );
    String sql = "with year_total as (\n" +
        "    select c_customer_id                                customer_id\n" +
        "         , c_first_name                                 customer_first_name\n" +
        "         , c_last_name                                  customer_last_name\n" +
        "         , c_preferred_cust_flag                        customer_preferred_cust_flag\n" +
        "         , c_birth_country                              customer_birth_country\n" +
        "         , c_login                                      customer_login\n" +
        "         , c_email_address                              customer_email_address\n" +
        "         , d_year                                       dyear\n" +
        "         , sum(ss_ext_list_price - ss_ext_discount_amt) year_total\n" +
        "         , 's'                                          sale_type\n" +
        "    from customer\n" +
        "       , store_sales\n" +
        "       , date_dim\n" +
        "    where c_customer_sk = ss_customer_sk\n" +
        "      and ss_sold_date_sk = d_date_sk\n" +
        "    group by c_customer_id\n" +
        "           , c_first_name\n" +
        "           , c_last_name\n" +
        "           , c_preferred_cust_flag\n" +
        "           , c_birth_country\n" +
        "           , c_login\n" +
        "           , c_email_address\n" +
        "           , d_year\n" +
        "    union all\n" +
        "    select c_customer_id                                customer_id\n" +
        "         , c_first_name                                 customer_first_name\n" +
        "         , c_last_name                                  customer_last_name\n" +
        "         , c_preferred_cust_flag                        customer_preferred_cust_flag\n" +
        "         , c_birth_country                              customer_birth_country\n" +
        "         , c_login                                      customer_login\n" +
        "         , c_email_address                              customer_email_address\n" +
        "         , d_year                                       dyear\n" +
        "         , sum(ws_ext_list_price - ws_ext_discount_amt) year_total\n" +
        "         , 'w'                                          sale_type\n" +
        "    from customer\n" +
        "       , web_sales\n" +
        "       , date_dim\n" +
        "    where c_customer_sk = ws_bill_customer_sk\n" +
        "      and ws_sold_date_sk = d_date_sk\n" +
        "    group by c_customer_id\n" +
        "           , c_first_name\n" +
        "           , c_last_name\n" +
        "           , c_preferred_cust_flag\n" +
        "           , c_birth_country\n" +
        "           , c_login\n" +
        "           , c_email_address\n" +
        "           , d_year\n" +
        ")\n" +
        "insert overwrite table query11\n" +
        "select t_s_secyear.customer_id\n" +
        "     , t_s_secyear.customer_first_name\n" +
        "     , t_s_secyear.customer_last_name\n" +
        "     , t_s_secyear.customer_preferred_cust_flag\n" +
        "from year_total t_s_firstyear\n" +
        "   , year_total t_s_secyear\n" +
        "   , year_total t_w_firstyear\n" +
        "   , year_total t_w_secyear\n" +
        "where t_s_secyear.customer_id = t_s_firstyear.customer_id\n" +
        "  and t_s_firstyear.customer_id = t_w_secyear.customer_id\n" +
        "  and t_s_firstyear.customer_id = t_w_firstyear.customer_id\n" +
        "  and t_s_firstyear.sale_type = 's'\n" +
        "  and t_w_firstyear.sale_type = 'w'\n" +
        "  and t_s_secyear.sale_type = 's'\n" +
        "  and t_w_secyear.sale_type = 'w'\n" +
        "  and t_s_firstyear.dyear = 2001\n" +
        "  and t_s_secyear.dyear = 2001 + 1\n" +
        "  and t_w_firstyear.dyear = 2001\n" +
        "  and t_w_secyear.dyear = 2001 + 1\n" +
        "  and t_s_firstyear.year_total > 0\n" +
        "  and t_w_firstyear.year_total > 0\n" +
        "  and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else 0.0 end\n" +
        "    > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else 0.0 end\n" +
        "order by t_s_secyear.customer_id\n" +
        "       , t_s_secyear.customer_first_name\n" +
        "       , t_s_secyear.customer_last_name\n" +
        "       , t_s_secyear.customer_preferred_cust_flag\n" +
        "limit 100;\n";
    Future<SqllineageResult> submit = sqllineageTaskLauncher.submit(manager, sql);
    SqllineageResult result = submit.get();
    System.out.println(result);
  }


  /**
   * 测试字段中嵌套子查询
   */
  @Test
  public void testCaseWhenSelect() throws ExecutionException, InterruptedException {
    //构建元数据
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
        "from default.item,\n" +
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


  /**
   * 测试 select 中的元素通过子查询并且子查询中嵌套union
   */
  @Test
  public void testSQLQueryExprSubUnion() throws ExecutionException, InterruptedException {
    //构建元数据
    TestTableMetaDataManager manager = buildManager(Lists.newArrayList(
        new TableMetaData("tableb", Lists.newArrayList("a", "b", "c", "t_minute", "ext_price")),
        new TableMetaData("tablea", Lists.newArrayList("a", "b", "c", "d")),
        new TableMetaData("catalog_sales", Lists.newArrayList("cs_quantity", "cs_list_price")),
        new TableMetaData("web_sales", Lists.newArrayList("ws_quantity", "ws_list_price"))
    ));

    String sql = "insert into tablea select a,b,c ,(select cs_quantity * cs_list_price sales\n" +
        "      from catalog_sales\n" +
        "         , date_dim\n" +
        "      where d_year = 2000\n" +
        "        and d_moy = 2\n" +
        "        and cs_sold_date_sk = d_date_sk\n" +
        "        and cs_item_sk in (select item_sk from frequent_ss_items)\n" +
        "        and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)\n" +
        "      union all\n" +
        "      select ws_quantity * ws_list_price sales\n" +
        "      from web_sales\n" +
        "         , date_dim\n" +
        "      where d_year = 2000\n" +
        "        and d_moy = 2\n" +
        "        and ws_sold_date_sk = d_date_sk\n" +
        "        and ws_item_sk in (select item_sk from frequent_ss_items)\n" +
        "        and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)) as d from tableb ";
    SqllineageTaskLauncher sqllineageTaskLauncher = new SqllineageTaskLauncher();
    SqllineageResult result = sqllineageTaskLauncher.submit(manager, sql).get();
    System.out.println(result);
  }


  /**
   * 测试create table as select * from schema.table
   */
  @Test
  public void testCreateSelect() throws ExecutionException, InterruptedException {
    //构建元数据
    TestTableMetaDataManager manager = buildManager(Lists.newArrayList(
        new TableMetaData("tableb", Lists.newArrayList("a", "b", "c", "t_minute", "ext_price"))
    ));

    String sql = "create table tablea as select * from tableb ";
    SqllineageTaskLauncher sqllineageTaskLauncher = new SqllineageTaskLauncher();
    SqllineageResult result = sqllineageTaskLauncher.submit(manager, sql).get();
    System.out.println(result);
  }

  /**
   * create table table1(column1_rename,column2_rename) as select column1,column2 from table2;
   * 根据table2的表结构，创建tables1,重命名列，并复制数据
   *
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test
  public void testCreateSelectAppoint() throws ExecutionException, InterruptedException {
    //构建元数据
    TestTableMetaDataManager manager = buildManager(Lists.newArrayList(
        new TableMetaData("table2", Lists.newArrayList("column1", "column2"))
    ));

    String sql = "create table table1(column1_rename,column2_rename) as select column1,column2 from table2;";
    SqllineageTaskLauncher sqllineageTaskLauncher = new SqllineageTaskLauncher();
    SqllineageResult result = sqllineageTaskLauncher.submit(manager, sql).get();
    System.out.println(result);
  }


  /**
   * 测试 select a from tablea,tableb,tablec ,但是a表，b表，c表都拥有a字段，这种情况需要抛出异常
   */
  @Test
  public void test() throws ExecutionException, InterruptedException {
    //构建元数据
    TestTableMetaDataManager manager = buildManager(Lists.newArrayList(
        new TableMetaData("tablea", Lists.newArrayList("a", "b", "c", "t_minute", "ext_price")),
        new TableMetaData("tableb", Lists.newArrayList("a", "b", "c", "d")),
        new TableMetaData("tablec", Lists.newArrayList("a", "cs_list_price"))
    ));

    String sql = "insert into dtest select a from ods.tablea,tableb,tablec ";
    SqllineageTaskLauncher sqllineageTaskLauncher = new SqllineageTaskLauncher();
    SqllineageResult result = sqllineageTaskLauncher.submit(manager, sql).get();
    System.out.println(result);
  }


  public static class TableMetaData {
    private String name;
    private List<String> cols;

    public TableMetaData(String name, List<String> cols) {
      this.name = name;
      this.cols = cols;
    }
  }

  private TestTableMetaDataManager buildManager(List<TableMetaData> tableMetaDatas) {
    HashMap<String, List<String>> map = new HashMap<>();
    for (TableMetaData tableMetaData : tableMetaDatas) {
      map.put(tableMetaData.name, tableMetaData.cols);
    }
    return new TestTableMetaDataManager(map);
  }

}
