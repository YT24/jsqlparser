package com.yt.sqlparser.visitor;

import com.yt.sqlparser.ExpressionColumnHandler;
import com.yt.sqlparser.bo.SqlColumnMappingBO;
import com.yt.sqlparser.utils.SqlParseUtils;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;


/**
 * 用递归来解决多层嵌套子查询的问题
 */
public class SySelectVisitor {
    /**
     * 递归visit，通过对子查询visit并合并columnsMapping实现整体语句的解析
     *
     * @param setList
     * @param columnsMapping
     */
    public void visit(SetOperationList setList, List<SqlColumnMappingBO> columnsMapping) throws Exception {
        List<SelectBody> setSelectBodies = setList.getSelects();
        List<List<SqlColumnMappingBO>> subSubQueryColumnsMappings = new ArrayList<>(setSelectBodies.size());
        for (SelectBody subSubselectBody : setSelectBodies) {
            List<SqlColumnMappingBO> map = new ArrayList<>();
            this.visit((PlainSelect) subSubselectBody, map, false);
            subSubQueryColumnsMappings.add(map);
        }
        mergeSetQueryColumnsMappings(subSubQueryColumnsMappings, columnsMapping);
    }

    public void visit(PlainSelect pSelect, List<SqlColumnMappingBO> columnsMapping, boolean isSubQuery) throws Exception {
        processFromItem(pSelect, pSelect.getFromItem(), columnsMapping, isSubQuery);
        List<Join> joins = pSelect.getJoins();
        if (joins != null) {
            for (Join join : joins) {
                //将join的语句当作from语句处理（都可能是table或者subQuery)
                FromItem rightItem = join.getRightItem();
                visitFromItem(columnsMapping, rightItem);
            }
        }
    }

    /**
     * 递归visit，通过对子查询visit并合并columnsMapping实现整体语句的解析
     *
     * @param pSelect
     * @param columnsMapping
     */
    private void processFromItem(PlainSelect pSelect, FromItem fromItem, List<SqlColumnMappingBO> columnsMapping, boolean isSubQuery) throws Exception {
        //解析select字段
        List<SelectItem> selectColumn = pSelect.getSelectItems();
        String tableAliasName = fromItem.getAlias() != null ? fromItem.getAlias().getName() : null;
        for (SelectItem selectItem : selectColumn) {
            SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
            Expression expression = expressionItem.getExpression();
            String aliasName = getAliasName(isSubQuery, expressionItem, expression);
            //analysisFunction(expression, columnsMapping, aliasName, tableAliasName);
            new ExpressionColumnHandler().ananlyzeExpression(expression, columnsMapping, aliasName, tableAliasName);
        }
        if (columnsMapping.isEmpty()) {
            return;
        }
        //解析from语句
        visitFromItem(columnsMapping, fromItem);
    }

    /**
     * 获取别名
     *
     * @param isSubQuery
     * @param expressionItem
     * @param expression
     * @return
     */
    private static String getAliasName(boolean isSubQuery, SelectExpressionItem expressionItem, Expression expression) {
        String aliasName;
        if (isSubQuery) {
            if (expressionItem.getAlias() == null) {
                aliasName = expression.toString();
            } else {
                if (expression.toString().contains(".") && !expression.toString().contains("(")) {
                    aliasName = expression.toString().split("[.]")[0] + "." + expressionItem.getAlias().getName();
                } else {
                    aliasName = expressionItem.getAlias().getName();
                }
            }

        } else {
            if (expressionItem.getAlias() == null) {
                //类似select count(0) from table的情况
                if (expression instanceof Function) {
                    aliasName = expression.toString();
                } else if (expression instanceof LongValue || expression instanceof StringValue || expression instanceof SignedExpression) {
                    aliasName = expression.toString();
                } else if (expression instanceof Column) {
                    aliasName = ((Column) expressionItem.getExpression()).getColumnName();
                } else {
                    aliasName = expression.toString();
                }
            } else {   //有别名
                aliasName = expressionItem.getAlias().getName();
            }
        }
        return aliasName;
    }

    /**
     * 用在join的右子句或者from子句，都可能出现表或者子查询
     *
     * @param columnsMapping
     * @param fromItem
     */
    private void visitFromItem(List<SqlColumnMappingBO> columnsMapping, FromItem fromItem) throws Exception {
        String tableAliasName = null;
        if (fromItem instanceof Table) {
            String tableName = getTableName((Table) fromItem);
            if (fromItem.getAlias() != null) {
                tableAliasName = fromItem.getAlias().getName();
            }
            tableAliasName = Objects.isNull(tableAliasName) ? tableName : tableAliasName;
            setColumnMappingMap(columnsMapping, tableName, tableAliasName);
        } else if (fromItem instanceof SubSelect) {
            List<SqlColumnMappingBO> subQueryColumnsMapping = new ArrayList<>();
            // 如果是UNION则需要进一步处理
            SelectBody subSelectBody = ((SubSelect) fromItem).getSelectBody();
            if (subSelectBody instanceof SetOperationList) {
                tableAliasName = Objects.requireNonNull(fromItem.getAlias()).getName();
                List<SelectBody> setSelectBodies = ((SetOperationList) subSelectBody).getSelects();
                List<List<SqlColumnMappingBO>> subSubQueryColumnsMappings = new ArrayList<>(setSelectBodies.size());
                for (SelectBody subSubselectBody : setSelectBodies) {
                    List<SqlColumnMappingBO> map = new ArrayList<>();
                    this.visit((PlainSelect) subSubselectBody, map, true);
                    updateSubQueryMappingWithSubQueryAliasName(map, tableAliasName);
                    subSubQueryColumnsMappings.add(map);
                }
                mergeUnionQueryColumnsMappings(columnsMapping, subSubQueryColumnsMappings);
            } else {
                tableAliasName = Objects.requireNonNull(fromItem.getAlias()).getName();
                this.visit((PlainSelect) subSelectBody, subQueryColumnsMapping, true);
                //先处理子查询的结果
                updateSubQueryMappingWithSubQueryAliasName(subQueryColumnsMapping, tableAliasName);
                //解析结束，开始合并并替换别名，生成最后的columnsMapping
                mergeSubQueryColumnMapIntoColumnMapping(columnsMapping, subQueryColumnsMapping);
            }
        }
    }

    private void mergeSetQueryColumnsMappings(List<List<SqlColumnMappingBO>> maps, List<SqlColumnMappingBO> resultMap) {
        for (List<SqlColumnMappingBO> map : maps) {
            for (SqlColumnMappingBO key : map) {
                for (SqlColumnMappingBO columnMappingBO : resultMap) {
                    if (columnMappingBO.getColumnAlias().equals(key.getColumnAlias())) {
                        Set<String> columns = columnMappingBO.getParentColumns();
                        columns.addAll(key.getParentColumns());
                    } else {
                        resultMap.add(SqlColumnMappingBO.builder().columnAlias(key.getColumnAlias()).parentColumns(key.getParentColumns()).build());
                    }
                }
            }
        }
    }

    /**
     * 合并union查询的字段映射关系
     *
     * @param columnsMapping             最终的字段映射关系
     * @param subSubQueryColumnsMappings 子查询的字段映射关系
     */
    private void mergeUnionQueryColumnsMappings(List<SqlColumnMappingBO> columnsMapping, List<List<SqlColumnMappingBO>> subSubQueryColumnsMappings) {
        for (SqlColumnMappingBO sqlColumnMappingBO : columnsMapping) {
            // For each parent column in the current SqlColumnMappingBO
            Set<String> updatedParentColumns = new HashSet<>();
            for (String parentColumn : sqlColumnMappingBO.getParentColumns()) {
                // Check each subList in subSubQueryColumnsMappings
                for (int i = 0; i < subSubQueryColumnsMappings.size(); i++) {
                    List<SqlColumnMappingBO> subList = subSubQueryColumnsMappings.get(i);
                    // Iterate through each SqlColumnMappingBO in the subList
                    for (int j = 0; j < subList.size(); j++) {
                        SqlColumnMappingBO subSqlColumnMappingBO = subList.get(j);
                        // If the columnAlias matches the parentColumn
                        if (!parentColumn.equals(subSqlColumnMappingBO.getColumnAlias())) {
                            continue;
                        }
                        // Replace the parentColumn with the current subSqlColumnMappingBO's parentColumns
                        if (subSqlColumnMappingBO.getParentColumns() != null) {
                            updatedParentColumns.addAll(subSqlColumnMappingBO.getParentColumns());
                        }
                        addOtherUnionColumns(updatedParentColumns, i, j, subSubQueryColumnsMappings);
                    }
                }
            }
            // Set the updated parentColumns
            sqlColumnMappingBO.setParentColumns(updatedParentColumns);
        }
    }

    private void addOtherUnionColumns(Set<String> updatedParentColumns, int i, int j, List<List<SqlColumnMappingBO>> subSubQueryColumnsMappings) {
        for (int k = 0; k < subSubQueryColumnsMappings.size(); k++) {
            if (i == k) {
                continue;
            }
            List<SqlColumnMappingBO> subList = subSubQueryColumnsMappings.get(k);
            for (int x = 0; x < subList.size(); x++) {
                if (j == x) {
                    Set<String> parentColumns = subList.get(x).getParentColumns();
                    if (parentColumns != null) {
                        updatedParentColumns.addAll(parentColumns);
                    }
                }
            }
        }
    }


    //处理查询字段里的函数
    private Expression analysisFunction(Expression expression, List<SqlColumnMappingBO> map, String aliasName, String tableAliasName) throws Exception {

        //处理row_number函数
        if (judgeRowNumerFunction(expression)) {
            //Set<String> fields = getRowNumberFunctionFields(expression.toString());
            //putSelectMap(fields, map, aliasName);
            expression = null;
        }

        //1. case 函数
        if (expression instanceof CaseExpression) {
            //case函数
            Expression switchExpression = ((CaseExpression) expression).getSwitchExpression();
            //获取when条件
            List<WhenClause> whenClauses = ((CaseExpression) expression).getWhenClauses();
            //判断else表达式
            Expression elseExpression = ((CaseExpression) expression).getElseExpression();

            if (switchExpression != null) {
                expression = analysisFunction(switchExpression, map, aliasName, tableAliasName);
            }
            if (SqlParseUtils.judgeExpression(elseExpression)) {
                expression = elseExpression;
            } else if (!Objects.isNull(elseExpression) && SqlParseUtils.judgeValueExpression(elseExpression)) {
                putSelectMap(elseExpression, map, aliasName, tableAliasName);
            }
            for (Expression clause : whenClauses) {
                //when条件
                WhenClause whenClause = (WhenClause) clause;
                Expression whenExpression = whenClause.getWhenExpression();
                Expression thenExpression = whenClause.getThenExpression();
                //获取when,then表达式
                expression = analysisFunction(whenExpression, map, aliasName, tableAliasName);
                expression = analysisFunction(thenExpression, map, aliasName, tableAliasName);

            }
        }
        //2. cast 函数
        if (expression instanceof CastExpression) {
            //cast函数
            expression = ((CastExpression) expression).getLeftExpression();
        }

        //3. 普通函数
        if (expression instanceof Function) {
            //函数
            Function function = (Function) expression;
            //没有参数的函数直接返回，没有分析价值
            if (function.getParameters() == null) {
                return expression;
            }
            List<Expression> expressions = function.getParameters().getExpressions();
            for (Expression ex : expressions) {
                if (SqlParseUtils.judgeExpression(ex)) {
                    expression = analysisFunction(ex, map, aliasName, tableAliasName);
                } else {
                    if (SqlParseUtils.judgeValueExpression(ex)) {
                        expression = analysisFunction(ex, map, aliasName, tableAliasName);
                    } else {
                        //函数的参数是常量的直接返回，没有分析价值
                        expression = null;
                    }
                }
            }
        }

        // 4
        if (expression instanceof BinaryExpression) {
            //左侧表达式;
            Expression leftExpression = ((BinaryExpression) expression).getLeftExpression();
            // 右侧表达式
            Expression rightExpression = ((BinaryExpression) expression).getRightExpression();
            if (SqlParseUtils.judgeExpression(leftExpression)) {
                expression = leftExpression;
            } else {
                if (!SqlParseUtils.judgeValueExpression(leftExpression)) {
                    putSelectMap(leftExpression, map, aliasName, tableAliasName);
                }
            }
            if (SqlParseUtils.judgeExpression(rightExpression)) {
                expression = rightExpression;
            } else {
                if (!SqlParseUtils.judgeValueExpression(rightExpression)) {
                    putSelectMap(rightExpression, map, aliasName, tableAliasName);
                } else {
                    expression = null;
                }
            }
        }

        // 5. 解析analytic函数
        if (expression instanceof AnalyticExpression) {
            expression = ((AnalyticExpression) expression).getExpression();
        }

        // 6. 兜底判断
        if (SqlParseUtils.judgeExpression(expression)) {
            expression = analysisFunction(expression, map, aliasName, tableAliasName);
        }

        // 7. t.x -1 这种情况
        if(expression instanceof Parenthesis){
            Parenthesis parenthesis = (Parenthesis) expression;
            expression = parenthesis.getExpression();
            analysisFunction(expression, map, aliasName, tableAliasName);
        }

        // 8. 解析子查询
        if (expression instanceof Subtraction) {
            Subtraction subtraction = (Subtraction) expression;
            Expression leftExpression = subtraction.getLeftExpression();
            expression = analysisFunction(leftExpression, map, aliasName, tableAliasName);
            Expression rightExpression = subtraction.getRightExpression();
            expression = analysisFunction(rightExpression, map, aliasName, tableAliasName);
        }

        // 9. 构建selectMap
        putSelectMap(expression, map, aliasName, tableAliasName);
        if (!SqlParseUtils.judgeValueExpression(expression)) {
            expression = null;
        }
        return expression;
    }

    private void putSelectMap(Set<String> fields, List<SqlColumnMappingBO> map, String aliasName) {
        Map<String, SqlColumnMappingBO> mappingBOMap = map.stream().collect(Collectors.toMap(SqlColumnMappingBO::getColumnAlias, java.util.function.Function.identity()));
        for (String field : fields) {
            if (!mappingBOMap.containsKey(aliasName)) {
                mappingBOMap.get(aliasName).getParentColumns().add(clearString(field));
            } else {
                Set<String> set = new HashSet<>();
                set.add(clearString(field));
                map.add(SqlColumnMappingBO.builder().columnAlias(field).parentColumns(set).build());
            }
        }
    }

    private boolean judgeRowNumerFunction(Expression expression) {
        String expressionString = Objects.nonNull(expression) ? expression.toString().toLowerCase(Locale.ROOT) : null;
        if (StringUtils.isNotBlank(expressionString) && expressionString.contains("row_number()")) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public Set<String> getRowNumberFunctionFields(String expression) throws Exception {
        // Remove unnecessary parts
        String relevantPart = expression
                .replace("row_number() over (", "")
                .replace(")", "");

        // Split into partition and order-by parts
        String[] parts = relevantPart.split("by");
        Set<String> fields = new HashSet<>();
        if (parts.length == 2) {
            fields.add(parts[1].replace("desc", "").trim());
        }
        if (parts.length == 3) {
            fields.add(parts[1].replace("order", "").replace("desc", "").trim());
            fields.add(parts[2].replace("order", "").replace("desc", "").trim());
        }
        return fields;
    }

    //保存查询字段
    private void putSelectMap(Expression expression, List<SqlColumnMappingBO> map, String aliasName, String tableAliasName) {
        if (expression == null || (expression instanceof StringValue)
                || (expression instanceof DoubleValue)
                || (expression instanceof LongValue)
                || (expression instanceof NullValue)) {
            return;
        }
        aliasName = clearString(aliasName);
        tableAliasName = clearString(tableAliasName);
        List<String> colNames = map.stream().map(SqlColumnMappingBO::getColumnAlias).collect(Collectors.toList());
        Set<String> set = new HashSet<>();
        if (CollectionUtils.isEmpty(map) || !colNames.contains(aliasName)) {
            String parentColumn = clearString(expression.toString());
            if (!expression.toString().contains(".")) {
                parentColumn = StringUtils.isNotBlank(tableAliasName) ? clearString(tableAliasName + "." + expression.toString()) : clearString(expression.toString());
            }
            set.add(parentColumn);
            map.add(SqlColumnMappingBO.builder().columnAlias(clearString(aliasName)).parentColumns(set).build());
            return;
        }
        for (SqlColumnMappingBO columnMappingBO : map) {
            if (columnMappingBO.getColumnAlias().equals(aliasName)) {
                String parentColumn = clearString(expression.toString());
                if (!expression.toString().contains(".")) {
                    parentColumn = StringUtils.isNotBlank(tableAliasName) ? clearString(tableAliasName + "." + expression.toString()) : clearString(expression.toString());
                }
                columnMappingBO.getParentColumns().add(clearString(parentColumn));
            }
        }
    }


    /**
     * 将子查询中的字段映射关系 合并到select解析结果中
     * 如：select t1.field1 from (select field1 from table1) as t1，
     * 提取的select结果为：                   field1 -> {t1.field1}
     * 经过子查询别名更新后的子查询字段映射关系为： t1.field1 -> {table1.field1}
     * 合并后的最终结果为：                    field1 -> {table1.field1}
     *
     * @param columnsMapping
     * @param subQueryMapping
     */
    private void mergeSubQueryColumnMapIntoColumnMapping(List<SqlColumnMappingBO> columnsMapping, List<SqlColumnMappingBO> subQueryMapping) {
        Map<String, SqlColumnMappingBO> mappingBOMap = subQueryMapping.stream().map(tar -> {
                    tar.setColumnAlias(tar.getColumnAlias().replace("`", ""));
                    return tar;
                })
                .collect(Collectors.toMap(SqlColumnMappingBO::getColumnAlias, java.util.function.Function.identity()));
        for (SqlColumnMappingBO str : columnsMapping) {
            Set<String> newSet = new HashSet<>();
            Set<String> set = str.getParentColumns();
            for (String s : set) {
                s = clearString(s);
                newSet.addAll(mappingBOMap.containsKey(s) ? mappingBOMap.get(s).getParentColumns() : Collections.singleton(s));
            }
            str.setParentColumns(newSet);
        }
    }

    /**
     * 用子查询的别名来更新子查询中的字段映射关系
     * 如：(select field1 from table1) as t1
     * 提取的子查询字段映射关系为：          field1->{table1.field1}
     * 经过子查询别名映射后，更新为：        t1.field1->{table1.field1}
     *
     * @param columnsMapping
     * @param subQueryAliasName
     */
    private void updateSubQueryMappingWithSubQueryAliasName(List<SqlColumnMappingBO> columnsMapping, String subQueryAliasName) {
        List<SqlColumnMappingBO> map2 = new ArrayList<>();
        for (SqlColumnMappingBO str : columnsMapping) {
            if (str.getColumnAlias().contains(".")) {
                //String[] split = str.getColumnAlias().split("[.]");
                String columnAlias = getColumnAlias(str.getColumnAlias());
                map2.add(SqlColumnMappingBO.builder().columnAlias(subQueryAliasName + "." + columnAlias)
                        .parentColumns(str.getParentColumns()).build());
            } else {
                map2.add(SqlColumnMappingBO.builder().columnAlias(subQueryAliasName + "." + str.getColumnAlias())
                        .parentColumns(str.getParentColumns()).build());
            }
        }
        columnsMapping.clear();
        columnsMapping.addAll(map2);
    }

    private String getColumnAlias(String columnAlias) {
        String[] split = columnAlias.split("[.]");
        ;
        List<String> strs = new ArrayList<>();
        for (int i = 1; i < split.length; i++) {
            strs.add(split[i]);
        }
        return String.join(".", strs);
    }

    private void setColumnMappingMap(List<SqlColumnMappingBO> columnsMappings, String tableName, String tableAliasName) {
        for (SqlColumnMappingBO str : columnsMappings) {
            Set<String> columnSet = new HashSet<>();
            Set<String> set = str.getParentColumns();
            for (String s : set) {
                if (s.contains(".")) {
                    String[] split = s.split("[.]");
                    s = (tableAliasName.equalsIgnoreCase(split[0])) ? tableName + "." + split[1] : s;
                } else {
                    s = tableName + "." + s;
                }
                columnSet.add(s);
            }
            str.setParentColumns(columnSet);
        }
    }

    //获取表名（包含 数据库名，表空间名）
    //TODO: 暂时不处理跨库，仅对表名进行限制
    private String getTableName(Table fromItem) {
        //表名
        String name = fromItem.getName();
        //数据库名
        String databaseName = fromItem.getDatabase().getDatabaseName();
        //表空间名
        String schemaName = fromItem.getSchemaName();
        if (Objects.isNull(databaseName) && !Objects.isNull(schemaName)) {
            return schemaName + "." + name;
        }
        if (!Objects.isNull(databaseName)) {
            return databaseName + "." + (schemaName == null ? "" : schemaName) + "." + name;
        }
        return name;
    }


    private String clearString(String s) {
        if (StringUtils.isBlank(s)) {
            return s;
        }
        return s.replace("`", "").replace("'", "").replace("\"", "");
    }
}
