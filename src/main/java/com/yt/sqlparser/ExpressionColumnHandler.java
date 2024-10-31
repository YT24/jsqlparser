package com.yt.sqlparser;

import com.yt.sqlparser.bo.ExpressionBO;
import com.yt.sqlparser.bo.SqlColumnMappingBO;
import com.yt.sqlparser.utils.SqlParseUtils;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExpressionColumnHandler {

    public static final Map<String, Function<ExpressionBO, Expression>> FUNCTION_MAP = new HashMap<>();

    public ExpressionColumnHandler() {
        FUNCTION_MAP.put("net.sf.jsqlparser.schema.Column", this::handleColumnExpression);
        FUNCTION_MAP.put("net.sf.jsqlparser.expression.CaseExpression", this::handleCaseExpression);
        FUNCTION_MAP.put("net.sf.jsqlparser.expression.CastExpression", this::handleCastExpression);
        FUNCTION_MAP.put("net.sf.jsqlparser.expression.Function", this::handleFunctionExpression);
        FUNCTION_MAP.put("net.sf.jsqlparser.expression.BinaryExpression", this::handleBinaryExpression);
        FUNCTION_MAP.put("net.sf.jsqlparser.expression.AnalyticExpression", this::handleAnalyticExpression);
        FUNCTION_MAP.put("net.sf.jsqlparser.expression.Parenthesis", this::handleParenthesis);
        FUNCTION_MAP.put("net.sf.jsqlparser.expression.operators.arithmetic.Subtraction", this::handleSubtraction);
        FUNCTION_MAP.put("net.sf.jsqlparser.expression.DoubleValue", this::handleValueExpression);
        FUNCTION_MAP.put("net.sf.jsqlparser.expression.NullValue", this::handleValueExpression);
        FUNCTION_MAP.put("net.sf.jsqlparser.expression.LongValue", this::handleValueExpression);
        FUNCTION_MAP.put("net.sf.jsqlparser.expression.StringValue", this::handleValueExpression);
    }

    /**
     * 解析表达式
     * @param expression 表达式
     * @param map 列映射
     * @param aliasName 别名
     * @param tableAliasName 表别名
     */
    public static void ananlyzeExpression(Expression expression, List<SqlColumnMappingBO> map, String aliasName, String tableAliasName) {
        Expression handle = handleExpression(expression, map, aliasName, tableAliasName);
        if (handle != null) {
           return;
        }
    }

    /**
     * 解析表达式
     * @param expression 表达式
     * @param map 列映射
     * @param aliasName 别名
     * @param tableAliasName 表别名
     * @return 解析后的表达式
     */
    private static Expression handleExpression(Expression expression, List<SqlColumnMappingBO> map, String aliasName, String tableAliasName) {
        return Optional.ofNullable(FUNCTION_MAP.get(expression.getClass().getName()))
                .map(function -> function.apply(new ExpressionBO(expression, map, aliasName, tableAliasName)))
                .orElse(null);
    }

    /**
     * 解析列表达式 Subtraction
     * @param expression 表达式
     * @return 解析后的表达式
     */
    private Expression handleSubtraction(ExpressionBO expression) {
        // 8. 解析子查询
        Subtraction subtraction = (Subtraction) expression.getExpression();
        expression.setExpression(handleExpression(subtraction.getLeftExpression(), expression.getColumnMappingBOS(), expression.getAliasName(), expression.getTableAliasName()));
        expression.setExpression(handleExpression(subtraction.getRightExpression(), expression.getColumnMappingBOS(), expression.getAliasName(), expression.getTableAliasName()));
        return expression.getExpression();
    }

    /**
     * 解析列表达式
     * @param expression 表达式
     * @return 解析后的表达式
     */
    private Expression handleParenthesis(ExpressionBO expression) {
        Parenthesis parenthesis = (Parenthesis) expression.getExpression();
        expression.setExpression(handleExpression(parenthesis.getExpression(), expression.getColumnMappingBOS(), expression.getAliasName(), expression.getTableAliasName()));
        return expression.getExpression();
    }

    /**
     * 解析列表达式
     * @param expression 表达式
     * @return 解析后的表达式
     */
    private Expression handleAnalyticExpression(ExpressionBO expression) {
        AnalyticExpression analyticExpression = (AnalyticExpression) expression.getExpression();
        expression.setExpression(handleExpression(analyticExpression.getExpression(), expression.getColumnMappingBOS(), expression.getAliasName(), expression.getTableAliasName()));
        return expression.getExpression();
    }

    /**
     * 解析列表达式  解析二元表达式
     * @param expression 表达式
     * @return 解析后的表达式
     */
    private Expression handleBinaryExpression(ExpressionBO expression) {
        Expression leftExpression = ((BinaryExpression) expression.getExpression()).getLeftExpression();
        Expression rightExpression = ((BinaryExpression) expression.getExpression()).getRightExpression();
        expression.setExpression(handleExpression(leftExpression, expression.getColumnMappingBOS(), expression.getAliasName(), expression.getTableAliasName()));
        expression.setExpression(handleExpression(rightExpression, expression.getColumnMappingBOS(), expression.getAliasName(), expression.getTableAliasName()));
        return expression.getExpression();
    }

    /**
     * 解析函数表达式
     * @param expression 表达式
     * @return 解析后的表达式
     */
    private Expression handleFunctionExpression(ExpressionBO expression) {
        //函数
        net.sf.jsqlparser.expression.Function function = (net.sf.jsqlparser.expression.Function) expression.getExpression();
        //没有参数的函数直接返回，没有分析价值
        if (function.getParameters() == null) {
            expression.setExpression(null);
            return expression.getExpression();
        }
        List<Expression> expressions = function.getParameters().getExpressions();
        for (Expression ex : expressions) {
            expression.setExpression(handleExpression(ex, expression.getColumnMappingBOS(), expression.getAliasName(), expression.getTableAliasName()));
        }
        return expression.getExpression();
    }

    /**
     * 解析列表达式
     * @param expressionBO 表达式
     * @return 解析后的表达式
     */
    public Expression handleColumnExpression(ExpressionBO expressionBO) {
        Expression expression = expressionBO.getExpression();

        String aliasName = SqlParseUtils.clearString(expressionBO.getAliasName());
        String tableAliasName = SqlParseUtils.clearString(expressionBO.getTableAliasName());
        List<String> colNames = expressionBO.getColumnMappingBOS().stream().map(SqlColumnMappingBO::getColumnAlias).collect(Collectors.toList());
        Set<String> set = new HashSet<>();
        if (CollectionUtils.isEmpty(expressionBO.getColumnMappingBOS()) || !colNames.contains(aliasName)) {
            String parentColumn = SqlParseUtils.clearString(expression.toString());
            if (!expression.toString().contains(".")) {
                parentColumn = StringUtils.isNotBlank(tableAliasName) ? SqlParseUtils.clearString(tableAliasName + "." + expression.toString()) : SqlParseUtils.clearString(expression.toString());
            }
            set.add(parentColumn);
            expressionBO.getColumnMappingBOS().add(SqlColumnMappingBO.builder().columnAlias(SqlParseUtils.clearString(aliasName)).parentColumns(set).build());
            return null;
        }
        for (SqlColumnMappingBO columnMappingBO : expressionBO.getColumnMappingBOS()) {
            if (columnMappingBO.getColumnAlias().equals(aliasName)) {
                String parentColumn = SqlParseUtils.clearString(expression.toString());
                if (!expression.toString().contains(".")) {
                    parentColumn = StringUtils.isNotBlank(tableAliasName) ? SqlParseUtils.clearString(tableAliasName + "." + expression.toString()) : SqlParseUtils.clearString(expression.toString());
                }
                columnMappingBO.getParentColumns().add(SqlParseUtils.clearString(parentColumn));
            }
        }
        return null;
    }

    /**
     * 解析列表达式 case函数
     * @param expression 表达式
     * @return 解析后的表达式
     */
    private Expression handleCaseExpression(ExpressionBO expression) {
        //case函数
        Expression switchExpression = ((CaseExpression) expression.getExpression()).getSwitchExpression();
        //获取when条件
        List<WhenClause> whenClauses = ((CaseExpression) expression.getExpression()).getWhenClauses();
        //判断else表达式
        Expression elseExpression = ((CaseExpression) expression.getExpression()).getElseExpression();
        expression.setExpression(handleExpression(switchExpression, expression.getColumnMappingBOS(), expression.getAliasName(), expression.getTableAliasName()));
        expression.setExpression(handleExpression(elseExpression, expression.getColumnMappingBOS(), expression.getAliasName(), expression.getTableAliasName()));
        for (WhenClause whenClause : whenClauses) {
            //when条件
            Expression whenExpression = whenClause.getWhenExpression();
            Expression thenExpression = whenClause.getThenExpression();
            //获取when,then表达式
            expression.setExpression(handleExpression(whenExpression, expression.getColumnMappingBOS(), expression.getAliasName(), expression.getTableAliasName()));
            expression.setExpression(handleExpression(thenExpression, expression.getColumnMappingBOS(), expression.getAliasName(), expression.getTableAliasName()));
        }
        return expression.getExpression();
    }

    /**
     * 解析列表达式 cast函数
     * @param expressionBO 表达式
     * @return 解析后的表达式
     */
    private Expression handleCastExpression(ExpressionBO expressionBO) {
        //cast函数
        expressionBO.setExpression(((CastExpression) expressionBO.getExpression()).getLeftExpression());
        return expressionBO.getExpression();
    }

    /**
     * 解析列表达式  解析常量表达式
     * @param expressionBO 表达式
     * @return 解析后的表达式
     */
    private Expression handleValueExpression(ExpressionBO expressionBO) {
        expressionBO.setExpression(null);
        return expressionBO.getExpression();
    }
}
