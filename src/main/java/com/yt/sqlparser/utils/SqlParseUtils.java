package com.yt.sqlparser.utils;

import com.yt.sqlparser.bo.*;
import com.yt.sqlparser.visitor.SySelectVisitor;
import com.yt.sqlparser.visitor.TableAndConditionVisitor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * SQL解析利用开源的JSQLParser来实现，参考网址：https://github.com/JSQLParser/JSqlParser
 *
 * @Description:
 */
public class SqlParseUtils {
    /**
     * 获取血缘关系结果
     *
     * @param sqltxt 要解析的SQL语句
     * @return
     * @throws Exception
     */
    public static List<SqlColumnMappingBO> getDataLineage(String sqltxt) throws Exception {
        if (sqltxt == null || sqltxt.trim().length() == 0) {
            return new ArrayList<>();
        }
        if (sqltxt.toLowerCase(Locale.ROOT).contains("cluster")) {
            sqltxt = sqltxt.replace("cluster", "order");
        }
        //第三方插件解析sql
        Statement stmt = CCJSqlParserUtil.parse(sqltxt);     //报错 说明sql语句错误
        Select selectStatement = (Select) stmt;
        System.out.println(selectStatement.toString());
        SySelectVisitor mySelectVisitor = new SySelectVisitor();
        SelectBody sBody = selectStatement.getSelectBody();
        List<SqlColumnMappingBO> columnsMappingMap = new ArrayList<>();
        if (sBody instanceof SetOperationList) {
            mySelectVisitor.visit((SetOperationList) sBody, columnsMappingMap);
        } else {
            mySelectVisitor.visit((PlainSelect) sBody, columnsMappingMap, false);
        }
        return columnsMappingMap;
    }

    /**
     * 解析出sql中所有的join关系
     *
     * @param sql
     * @return
     * @throws JSQLParserException
     */
    public static List<JoinConditionBO> parserJoinCondition(String sql) throws JSQLParserException {
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        TableAndConditionVisitor visitor = new TableAndConditionVisitor();
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        FromItem fromItem = plainSelect.getFromItem();
        String alias = Objects.nonNull(fromItem.getAlias()) ? fromItem.getAlias().getName() : null;
        visitor.visit(fromItem, alias, plainSelect.getJoins());
        List<TableParseBO> tableParseBOS = visitor.getTableParseVOS();
        Map<String, TableParseBO> aliasParseVOMap = tableParseBOS.stream().collect(Collectors.toMap(TableParseBO::getAlias, Function.identity()));
        Map<String, TableParseBO> subAliasParseVOMap = tableParseBOS.stream().collect(Collectors.toMap(TableParseBO::getSubQueryAlias, Function.identity()));

        List<JoinConditionBO> joinConditionBOS = visitor.getJoinConditionVOS();
        joinConditionBOS.forEach(joinConditionBO -> {
            TableParseBO leftTableParseBO = Objects.nonNull(aliasParseVOMap.get(joinConditionBO.getLeftTblAlias()))
                    ? aliasParseVOMap.get(joinConditionBO.getLeftTblAlias()) : subAliasParseVOMap.get(joinConditionBO.getLeftTblAlias());
            Optional.ofNullable(leftTableParseBO)
                    .ifPresent(bo -> {
                        joinConditionBO.setLeftDbName(bo.getDbName());
                        joinConditionBO.setLeftTblName(bo.getTblName());
                    });

            TableParseBO rightTableParseBO = Objects.nonNull(aliasParseVOMap.get(joinConditionBO.getRightTblAlias()))
                    ? aliasParseVOMap.get(joinConditionBO.getLeftTblAlias()) : subAliasParseVOMap.get(joinConditionBO.getRightTblAlias());
            Optional.ofNullable(rightTableParseBO)
                    .ifPresent(bo -> {
                        joinConditionBO.setRightDbName(bo.getDbName());
                        joinConditionBO.setRightTblName(bo.getTblName());
                    });
            System.out.println(joinConditionBO);
        });
        joinConditionBOS.stream().distinct().filter(s -> StringUtils.isBlank(s.getLeftTblName()) || StringUtils.isBlank(s.getRightTblName()))
                .forEach(s -> {
                    try {
                        if (StringUtils.isBlank(s.getLeftTblName())) {
                            parseTblNameByTblAliasAndColumn(sql, s.getLeftTblAlias(), s.getLeftColumn(), Boolean.FALSE, s);
                        }
                        if (StringUtils.isBlank(s.getRightTblName())) {
                            parseTblNameByTblAliasAndColumn(sql, s.getRightTblAlias(), s.getRightColumn(), Boolean.TRUE, s);
                        }
                    } catch (JSQLParserException e) {
                        throw new RuntimeException(e);
                    }
                });

        return joinConditionBOS;
    }

    public static List<CreateColumnInfoBO> parserDDL(String ddl) throws JSQLParserException {
        List<CreateColumnInfoBO> createColumnInfoBOS = new ArrayList<>();
        try {
            CreateTable createTable = (CreateTable) CCJSqlParserUtil.parse(ddl);
            List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();

        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

        return createColumnInfoBOS;
    }

    private static void parseTblNameByTblAliasAndColumn(String sql, String tblAlias, String column, boolean right, JoinConditionBO joinConditionBO) throws JSQLParserException {
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        FromItem fromItem = plainSelect.getFromItem();
        List<Join> joins = plainSelect.getJoins();
        List<SelectItem> selectItems = plainSelect.getSelectItems();

        TableParseBO tableParseBO = processJoin(joins, column);
        if (Objects.isNull(tableParseBO)) {
            tableParseBO = processTable(fromItem, selectItems, column);
        }
        Optional.ofNullable(tableParseBO).ifPresent(s -> {
            if (right) {
                joinConditionBO.setRightDbName(s.getDbName());
                joinConditionBO.setRightTblName(s.getTblName());
            } else {
                joinConditionBO.setLeftDbName(s.getDbName());
                joinConditionBO.setLeftTblName(s.getTblName());
            }
        });
    }

    private static TableParseBO processJoin(List<Join> joins, String column) {
        if (CollectionUtils.isNotEmpty(joins)) {
            for (Join join : joins) {
                FromItem rightItem = join.getRightItem();
                if (rightItem instanceof SubSelect) {
                    SubSelect subSelect = (SubSelect) rightItem;
                    SelectBody subSelectBody = subSelect.getSelectBody();
                    PlainSelect plainSelect = (PlainSelect) subSelectBody;
                    return processTable(plainSelect.getFromItem(), plainSelect.getSelectItems(), column);
                }
            }
        }
        return null;

    }

    public static TableParseBO processTable(FromItem fromItem, List<SelectItem> selectItems, String leftColumn) {
        AtomicReference<TableParseBO> tableParseBO = new AtomicReference<>();
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            if (Objects.equals(table.getASTNode().jjtGetFirstToken().toString(), table.getASTNode().jjtGetLastToken().toString())) {
                throw new RuntimeException("FROM clause must be in the format 'dbname.tablename'");
            }
            if (CollectionUtils.isNotEmpty(selectItems)) {
                selectItems.forEach(selectItem -> {
                    SimpleNode astNode = selectItem.getASTNode();
                    if (astNode.jjtGetLastToken().toString().equals(leftColumn)) {
                        tableParseBO.set(TableParseBO.builder().tblName(table.getName())
                                .dbName(table.getASTNode().jjtGetFirstToken().toString()).build());

                    }
                });
            }
        } else if (fromItem instanceof SubSelect) {
            SubSelect subSelect = (SubSelect) fromItem;
            SelectBody subSelectBody = subSelect.getSelectBody();
            PlainSelect plainSelect = (PlainSelect) subSelectBody;
            return processTable(plainSelect.getFromItem(), plainSelect.getSelectItems(), leftColumn);
        }
        return tableParseBO.get();
    }

    /**
     * 判断expression是否为表达式类型
     * @param expression
     * @return
     */
    public static boolean judgeExpression(Expression expression) {
        if (!Objects.isNull(expression)) {
            return expression instanceof net.sf.jsqlparser.expression.Function
                    || expression instanceof CastExpression
                    || expression instanceof CaseExpression
                    || expression instanceof BinaryExpression
                    || expression instanceof AnalyticExpression
                    || expression instanceof Parenthesis
                    || expression instanceof Subtraction
                    || expression instanceof net.sf.jsqlparser.expression.Function;
        }
        return false;
    }

    /**
     * 判断expression是否为值类型,true表示是值类型 false表示不是值类型
     * @param expression
     * @return
     */
    public static boolean judgeValueExpression(Expression expression) {
        if (expression == null) {
            return false;
        }
        return (expression instanceof StringValue)
                || (expression instanceof DoubleValue)
                || (expression instanceof LongValue)
                || (expression instanceof NullValue)
                || expression.toString().contains("\"")
                || expression.toString().contains("'");
    }

    /**
     * 解析sql中所有的表
     *
     * @param statement
     * @return
     */
    public static List<ParseTableBO> parseTables(Statement statement) {
        List<ParseTableBO> tables = new ArrayList<>();
        if (statement instanceof Select) {
            Select select = (Select) statement;
            SelectBody selectBody = select.getSelectBody();
            String alias = null;
            if (selectBody instanceof PlainSelect) {
                PlainSelect plainSelect = (PlainSelect) selectBody;
                FromItem fromItem = plainSelect.getFromItem();
                if(null!=fromItem){
                    alias = fromItem.getAlias() != null ? fromItem.getAlias().getName() : null;
                }
            }
            parseSelectBody(selectBody, alias, tables);
        }
        return tables;
    }

    public static void parseSelectBody(SelectBody selectBody, String parentAlias, List<ParseTableBO> tables) {
        if (selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;
            FromItem fromItem = plainSelect.getFromItem();
            //String alias = fromItem.getAlias() != null ? fromItem.getAlias().getName() : null;
            parseFromItem(fromItem, parentAlias, tables);
            if (plainSelect.getJoins() != null) {
                for (Join join : plainSelect.getJoins()) {
                    FromItem joinFromItem = join.getRightItem();
                    //String parentAlias = joinFromItem instanceof SubSelect ? joinFromItem.getAlias().getName() : alias;
                    parseFromItem(joinFromItem, parentAlias, tables);
                }
            }
        }
    }

    public static void parseFromItem(FromItem fromItem, String parentAlias, List<ParseTableBO> tables) {
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            String tableName = table.getName();
            String tableAlias = table.getAlias() != null ? table.getAlias().getName() : null;
            String dbName = !Objects.equals(tableName, table.getASTNode().jjtGetFirstToken().toString()) ? table.getASTNode().jjtGetFirstToken().toString() : null;
            String parentSubSelectName = parentAlias != null ? parentAlias : tableAlias;
            tables.add(ParseTableBO.builder().dbName(dbName)
                    .tblName(tableName)
                    .tblAliasName(StringUtils.isNotBlank(tableAlias) ? tableAlias : tableName)
                    .parentTblName(parentSubSelectName)
                    .build());
        } else if (fromItem instanceof SubSelect) {
            SubSelect subSelect = (SubSelect) fromItem;
            String alias = subSelect.getAlias().getName() != null ? subSelect.getAlias().getName() : null;
            parseSelectBody(subSelect.getSelectBody(), alias, tables);
        }
    }


    public static String clearString(String s) {
        if (StringUtils.isBlank(s)) {
            return s;
        }
        return s.replace("`", "").replace("'", "").replace("\"", "");
    }
}
