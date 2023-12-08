package com.yt.sqlparser.vistor;

import com.yt.sqlparser.vo.JoinConditionBO;
import com.yt.sqlparser.vo.TableParseBO;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class TableAndConditionVisitor extends SelectVisitorAdapter {

    private List<TableParseBO> tableParseBOS = new ArrayList<>();

    private List<JoinConditionBO> joinConditionBOS = new ArrayList<>();

    @Override
    public void visit(PlainSelect plainSelect) {
        processTable(plainSelect.getFromItem());
        processJoins(plainSelect.getJoins());
        //processOnExpression(plainSelect.getWhere());
    }

    private void processTable(FromItem fromItem) {
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            if (Objects.equals(table.getASTNode().jjtGetFirstToken().toString(), table.getASTNode().jjtGetLastToken().toString())) {
                throw new RuntimeException("from 必须是库名.表明格式");
            }
            TableParseBO tableParseBO = TableParseBO.builder().dbName(table.getASTNode().jjtGetFirstToken().toString())
                    .tblName(table.getName())
                    .alias(Objects.nonNull(table.getAlias()) ? table.getAlias().getName() : table.getName()).build();
            tableParseBOS.add(tableParseBO);
        } else if (fromItem instanceof SubSelect) {
            ((SubSelect) fromItem).getSelectBody().accept(this);
        }
    }

    private void processJoins(List<Join> joins) {
        if (joins != null) {
            for (Join join : joins) {
                processTable(join.getRightItem());
                processOnExpression(join.getOnExpression());
            }
        }
    }

    private void processOnExpression(Expression onExpression) {
        if (onExpression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) onExpression;
            if(equalsTo.getLeftExpression() instanceof Function || equalsTo.getRightExpression() instanceof Function){
                // on 表达式中包含函数不处理
                return;
            }
            Column leftExpression = (Column) equalsTo.getLeftExpression();
            Column rightExpression = (Column) equalsTo.getRightExpression();
            JoinConditionBO joinConditionBO = JoinConditionBO.builder()
                    .leftTblAlias(leftExpression.getTable().getFullyQualifiedName())
                    .leftColumn(leftExpression.getColumnName())
                    .rightTblAlias(rightExpression.getTable().getFullyQualifiedName())
                    .rightColumn(rightExpression.getColumnName()).build();
            joinConditionBOS.add(joinConditionBO);
        }
        if (onExpression instanceof AndExpression) {
            processOnExpression(((AndExpression) onExpression).getLeftExpression());
            processOnExpression(((AndExpression) onExpression).getRightExpression());
        }
    }

    public List<TableParseBO> getTableParseVOS() {
        return tableParseBOS;
    }

    public List<JoinConditionBO> getJoinConditionVOS() {
        return joinConditionBOS;
    }
}