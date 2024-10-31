package com.yt.sqlparser.bo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.sf.jsqlparser.expression.Expression;

import java.util.List;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ExpressionBO {

   private Expression expression;

   private List<SqlColumnMappingBO> columnMappingBOS;

   private String aliasName;

   private String tableAliasName;
}
