package com.yt.sqlparser.bo;

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
public class TableParseBO {

    /**
     * 数据库名
     */
    private String dbName;

    /**
     * 表名
     */
    private String tblName;

    /**
     * 别名
     */
    private String alias;

    /**
     * 子查询表别名
     */
    private String subQueryAlias;
}
