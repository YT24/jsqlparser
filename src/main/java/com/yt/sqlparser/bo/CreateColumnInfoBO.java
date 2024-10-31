package com.yt.sqlparser.bo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CreateColumnInfoBO implements Serializable {

    /**
     * 列名
     */
    private String columnName;

    /**
     * 列中文名
     */
    private String columnNameZh;

    /**
     * 列类型
     */
    private String columnType;

    /**
     * 是否分区 0: 不分区 1：分区
     */
    private String isPartition;

    /**
     * 左精度
     */
    private String leftPrecision;

    /**
     * 右精度
     */
    private String rightPrecision;

    /**
     * 是否是主键
     */
    @Builder.Default
    private Boolean primaryKeyFlag = Boolean.FALSE;

    @Builder.Default
    private Boolean manualPrimaryFlag = Boolean.FALSE;


    @Builder.Default
    private Boolean bucketFlag = Boolean.FALSE;


    private Integer bucketSort;

    /**
     * 是否可为空 默认为0:可为空
     */
    private Integer nullableFlag;
}
