package com.yt.sqlparser.bo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SqlColumnMappingBO {

    private String columnAlias;

    private Set<String> parentColumns;

}
