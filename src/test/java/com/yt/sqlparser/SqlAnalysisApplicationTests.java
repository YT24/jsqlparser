package com.yt.sqlparser;


import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

class SqlAnalysisApplicationTests {



    @Test
    public void testPlainSql(){
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
        try {
            Map<String, Set<String>> map = SelectParseHelper.getBloodRelationResult(sql);
            System.out.println(map);   //{depId=[department.id], userName=[user.name], id=[user.id], depName=[department.name]}
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSubQuerySql(){
        String sql = "       SELECT t1.customer_id, t1.total_spent, t2.total_orders, date() as curdate " +
                "        FROM ( " +
                "                 SELECT customer_id, SUM(order_total) AS total_spent " +
                "                 FROM customer_summary " +
                "                 GROUP BY customer_id " +
                "                 HAVING total_spent > 1000 " +
                "             ) AS t1 " +
                "                 JOIN ( " +
                "            SELECT customer_id, COUNT(DISTINCT order_id) AS total_orders " +
                "            FROM customer_summary " +
                "            GROUP BY customer_id " +
                " " +
                "        ) AS t2 ON t1.customer_id = t2.customer_id;";
        try {
            Map<String, Set<String>> map = SelectParseHelper.getBloodRelationResult(sql);
            System.out.println(map);   //{total_spent=[customer_summary.order_total], customer_id=[customer_summary.customer_id], total_orders=[customer_summary.order_id]}
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
