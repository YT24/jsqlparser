package com.luop.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yangte
 * @description TODO
 * @date 2023/12/7 11:06
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ColumnBO {

    /**
     * 表别名
     */
    private String tblAlias;

    /**
     * 列名
     */
    private String columnName;
}
