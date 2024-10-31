package com.yt.sqlparser.visitor;


import com.yt.sqlparser.bo.JoinConditionBO;
import com.yt.sqlparser.bo.TableParseBO;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TableAndConditionVisitor extends SelectVisitorAdapter {

    private List<TableParseBO> tableParseBOS = new ArrayList<>();

    private List<JoinConditionBO> joinConditionBOS = new ArrayList<>();

    /*@Override
    public void visit(PlainSelect plainSelect) {
        FromItem fromItem = plainSelect.getFromItem();
        String alias = Objects.nonNull(fromItem.getAlias()) ? fromItem.getAlias().getName() : null;
        processTable(fromItem,alias);
        processJoins(plainSelect.getJoins());
    }*/

    public void visit(FromItem fromItem, String alias, List<Join> joins) {
        processTable(fromItem, alias);
        processJoins(joins);
    }

    private void processTable(FromItem fromItem, String alias) {
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            if (Objects.equals(table.getASTNode().jjtGetFirstToken().toString(), table.getASTNode().jjtGetLastToken().toString())) {
                throw new RuntimeException("from 必须是库名.表明格式");
            }
            TableParseBO tableParseBO = TableParseBO.builder().dbName(table.getASTNode().jjtGetFirstToken().toString())
                    .tblName(table.getName())
                    .subQueryAlias(alias)
                    .alias(Objects.nonNull(table.getAlias()) ? table.getAlias().getName() : table.getName()).build();
            tableParseBOS.add(tableParseBO);
        } else if (fromItem instanceof SubSelect) {
            //((SubSelect) fromItem).getSelectBody().accept(this);
            PlainSelect plainSelect = (PlainSelect) ((SubSelect) fromItem).getSelectBody();
            FromItem subFromItem = plainSelect.getFromItem();
            this.visit(plainSelect.getFromItem(), alias, plainSelect.getJoins());
        }
    }

    private void processJoins(List<Join> joins) {
        if (joins != null) {
            for (Join join : joins) {
                processTable(join.getRightItem(), join.getRightItem() instanceof SubSelect ? ((SubSelect) join.getRightItem()).getAlias().getName() : null);
                processOnExpression(join.getOnExpression());
            }
        }
    }

    private void processOnExpression(Expression onExpression) {
        if (onExpression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) onExpression;
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