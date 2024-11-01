package com.luop;

import com.sun.deploy.util.StringUtils;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * SQL解析利用开源的JSQLParser来实现，参考网址：https://github.com/JSQLParser/JSqlParser
 *
 * @Description:
 *
 */
public class SelectParseHelper {
    /**
     * 获取血缘关系结果
     * @param sqltxt 要解析的SQL语句
     * @return
     * @throws Exception
     */
    public static Map<String,Set<String>> getBloodRelationResult(String sqltxt) throws Exception{
        if(sqltxt == null || sqltxt.trim().length() == 0){
            return new HashMap<>();
        }
        if(sqltxt.toLowerCase(Locale.ROOT).contains("cluster")){
            sqltxt = sqltxt.replace("cluster", "order");
        }
        //第三方插件解析sql
        Statement stmt = CCJSqlParserUtil.parse(sqltxt);     //报错 说明sql语句错误
        Select selectStatement=(Select)stmt;
        MySelectVisitor mySelectVisitor = new MySelectVisitor();
        SelectBody sBody = selectStatement.getSelectBody();
        Map<String, Set<String>> columnsMappingMap = new HashMap<>();
        if (sBody instanceof SetOperationList) {
            mySelectVisitor.visit((SetOperationList) sBody, columnsMappingMap);
        } else {
            mySelectVisitor.visit((PlainSelect) sBody, columnsMappingMap, false);
        }
        return columnsMappingMap;
    }
}
