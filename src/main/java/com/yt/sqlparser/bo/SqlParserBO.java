package com.yt.sqlparser.bo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SqlParserBO {

    private String dbName;

    private Long tblId;

    private String tblName;

    private Integer tblVersion;

    private Long colId;

    private String colName;

    private String sourceDbName;

    private Long sourceTblId;

    private String sourceTblName;

    private Integer sourceTblVersion;

    private Long sourceColId;

    private String sourceColName;
}
