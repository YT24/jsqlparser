package com.yt.sqlparser;


import com.yt.sqlparser.bo.SqlColumnMappingBO;
import com.yt.sqlparser.utils.SqlParseUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.SelectUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class SqlAnalysisApplicationTests {



    @Test
    public void testPlainSql() throws Exception {
        String sql = "select\n" +
                "\tsu.id ,\n" +
                "\tsu.username ,\n" +
                "\tsu.age ,\n" +
                "\tsd.name as dept_name,\n" +
                "\tsp.name as post_name,\n" +
                "\tsa.addr as address,\n" +
                "\tCONCAT(sd.name,\"-\",sp.name) as dept_post_name,\n" +
                "\tfrom_unixtime(unix_timestamp(sa.create_time,'yyyyMMddhh:mm:ss')) as cte\n" +
                "from\n" +
                "\tods_sqlparser.sy_user su\n" +
                "left join ods_sqlparser.sy_dept sd on\n" +
                "\tsd.id = su.dept_id\n" +
                "left join ods_sqlparser.sy_post sp on\n" +
                "\tsp.id = su.post_id\n" +
                "left join (\n" +
                "\tselect\n" +
                "\t\tid,\n" +
                "\t\taddr,\n" +
                "\t\tcreate_time\n" +
                "\tfrom\n" +
                "\t\tods_sqlparser.sy_addr) sa on\n" +
                "\tsa.id = su.addr_id;";
        List<SqlColumnMappingBO> bloodRelationResult = SqlParseUtils.getDataLineage(sql);
        System.out.println(bloodRelationResult);
    }

    @Test
    public void testSubQuerySql() throws Exception {
        String sql = "       SELECT t1.customer_id, t1.total_spent, t2.total_orders, date() as curdate " +
                "        FROM ( " +
                "                 SELECT customer_id, SUM(order_total) AS total_spent " +
                "                 FROM y.customer_summary " +
                "                 GROUP BY customer_id " +
                "                 HAVING total_spent > 1000 " +
                "             ) AS t1 " +
                "         JOIN ( select x.customer_id,x.total_orders\n" +
                "from (\n" +
                "select\n" +
                "\tcustomer_id,\n" +
                "\tCOUNT(distinct order_id) as total_orders\n" +
                "from\n" +
                "\ty.customer_summary\n" +
                "group by\n" +
                "\tcustomer_id\n" +
                "\t) x" +
                "        ) AS t2 ON t1.customer_id = t2.customer_id;";
        List<SqlColumnMappingBO> bloodRelationResult = SqlParseUtils.getDataLineage(sql);
        System.out.println(bloodRelationResult);
    }

    /**
     * 解析血缘关系
     * @throws Exception
     */
    @Test
    public void testSql() throws Exception {
        String sql = "select\n" +
                "        cast(catalog_type_id as string) as type_id,\n" +
                "        id,\n" +
                "        concat('【',name,'】') as name ,\n" +
                "        catalog_type_id - 1 as type_id,\n" +
                "        (case source_type when 0 then '内部'when 1 then '外部' else source_type end) type,\n" +
                "        substr(name,5,1) as new_name,\n" +
                "        ,\n" +
                "        1 as one,\n" +
                "        '2' as two,\n" +
                "        true as three,\n" +
                "        null as four\n" +
                "from sy_dataworks.catalog_catalog;" ;
        String sql2 = "select " +
                "t2.a as id_,t2.b as name_,t2.c as age_," +
                "t4.x,t4.y,t4.z " +
                "from ( select t1.id as a,concat(t1.addr,t1.name) as b,(t1.age - 1) as c from (select id,name,age,addr from x.sy_user) t1 ) t2 " +
                "left join ( select t3.id as x,concat(t3.addr,t3.name) as y,(t3.age - 1) as z from (select id,name,age,addr from y.sy_user) t3 ) t4 on t2.a = t4.x" ;
        List<SqlColumnMappingBO> bloodRelationResult = SqlParseUtils.getDataLineage(sql);
        bloodRelationResult.forEach(System.out::println);
    }

    @Test
    public void buildSql(){
        try {
            // 创建主查询的 Select 语句
            Select select = new Select();
            PlainSelect plainSelect = new PlainSelect();

            // 添加主表
            Table mainTable = new Table("orders");
            plainSelect.setFromItem(mainTable);

            // 添加子查询
            SubSelect subSelect = new SubSelect();
            PlainSelect subSelectBody = new PlainSelect();

            // 子查询中设置表名
            Table subTable = new Table("customers");
            subSelectBody.setFromItem(subTable);

            // 子查询中设置 WHERE 条件
            Expression whereConditionSubQuery = new Column("customer_id = 10");
            subSelectBody.setWhere(whereConditionSubQuery);


            // 子查询中选择字段
            SelectItem subSelectColumn = new SelectExpressionItem(new Column("name"));
            subSelectBody.addSelectItems(subSelectColumn);

            subSelect.setSelectBody(subSelectBody);
            subSelect.setAlias(new Alias("customer_sub"));

            // 将子查询添加为 JOIN 条件
            Join join = new Join();
            join.setRightItem(subSelect);
            EqualsTo onExpression = new EqualsTo();
            onExpression.setLeftExpression(new Column("orders.customer_id"));
            onExpression.setRightExpression(new Column("customer_sub.customer_id"));
            join.setOnExpression(onExpression);
            plainSelect.setJoins(Arrays.asList(join));

            // 主查询中设置 WHERE 条件
            EqualsTo equalsTo = new EqualsTo();
            equalsTo.setLeftExpression(new Column("orders.order_date"));
            equalsTo.setRightExpression(new StringValue("'2021-01-01'"));
            plainSelect.setWhere(equalsTo);

            // 主查询中选择字段
            SelectItem mainSelectColumn = new SelectExpressionItem(new Column("orders.order_id"));
            plainSelect.addSelectItems(mainSelectColumn);

            select.setSelectBody(plainSelect);

            // 打印生成的 SQL
            System.out.println(select.toString());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void parseSql() throws JSQLParserException {
        String query = "SELECT id,name,age FROM orders  WHERE id > 10";
        Statement stmt = CCJSqlParserUtil.parse(query);     //报错 说明sql语句错误
        Select selectStatement = (Select) stmt;
        SelectBody selectBody = selectStatement.getSelectBody();
        PlainSelect plainSelect = (PlainSelect) selectBody;
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        List<String> selectItemStrs = selectItems.stream().map(SelectItem::toString).collect(Collectors.toList());
        System.out.println(String.format("字段: %s", String.join(",", selectItemStrs)));
        FromItem fromItem = plainSelect.getFromItem();
        System.out.println(String.format("表名: %s", fromItem.toString()));
        System.out.println(String.format("条件: %s", plainSelect.getWhere().toString()));

        NotEqualsTo notEqualsTo = new NotEqualsTo();
        notEqualsTo.setLeftExpression(new Column("name"));
        notEqualsTo.setRightExpression(new StringValue("'jerry'"));
        AndExpression andExpression = new AndExpression(plainSelect.getWhere(), notEqualsTo);
        plainSelect.setWhere(andExpression);
        System.out.println(plainSelect);
    }
}
