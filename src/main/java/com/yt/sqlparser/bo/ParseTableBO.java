package com.yt.sqlparser.bo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ParseTableBO {

    private String dbName;

    private String tblName;

    private String tblAliasName;

    private String parentTblName;
}
