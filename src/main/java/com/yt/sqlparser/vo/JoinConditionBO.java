package com.yt.sqlparser.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yangte
 * @description 
 * @date 2023/12/7 09:06
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class JoinConditionBO {

    /**
     * 左表库
     */
    private String leftDbName;

    /**
     * 左表名
     */
    private String leftTblName;

    /**
     * 左表别名
     */
    private String leftTblAlias;

    /**
     * 左表字段
     */
    private String leftColumn;

    /**
     * 右库名
     */
    private String rightDbName;

    /**
     * 右表别名
     */
    private String rightTblName;

    /**
     * 右表别名
     */
    private String rightTblAlias;

    /**
     * 右表字段
     */
    private String rightColumn;
}
