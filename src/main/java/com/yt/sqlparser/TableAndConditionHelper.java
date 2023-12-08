package com.yt.sqlparser;

import com.yt.sqlparser.vo.JoinConditionBO;
import com.yt.sqlparser.vistor.TableAndConditionVisitor;
import com.yt.sqlparser.vo.TableParseBO;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TableAndConditionHelper {

    public static void main(String[] args) throws JSQLParserException {
        String sql = "select \n" +
                "  a.reportid,\n" +
                "  COALESCE(a.hospitalcode, '') AS   hospitalcode,\n" +
                "  COALESCE(a.rptunitid, 0) AS   rptunitid,\n" +
                "  COALESCE(a.sampledate, '') AS  sampledate,\n" +
                "  COALESCE(a.sampleno, '') AS sampleno,\n" +
                "  COALESCE(a.specimen_code, '') AS  specimen_code,\n" +
                "  COALESCE(a.specimen_name, '') AS  specimen_name,\n" +
                "  COALESCE(a.emer_flag, '') AS   emer_flag,\n" +
                "  COALESCE(a.specimen_comm, '') AS  specimen_comm,\n" +
                "  COALESCE(a.req_id, 0) AS   req_id,\n" +
                "  COALESCE(a.barcode, '') AS  barcode,\n" +
                "  COALESCE(a.other_no, '') AS other_no,\n" +
                "  COALESCE(a.pat_cardno, '') AS  pat_cardno,\n" +
                "  COALESCE(a.inp_id, '') AS   inp_id,\n" +
                "  COALESCE(a.inp_date, '') AS inp_date,\n" +
                "  COALESCE(a.pat_phone, '') AS   pat_phone,\n" +
                "  a.pat_id,\n" +
                "  COALESCE(a.pat_no, '') AS   pat_no,\n" +
                "  COALESCE(a.pat_name, '') AS pat_name,\n" +
                "  COALESCE(a.patname_py, '') AS  patname_py,\n" +
                "  COALESCE(a.pat_sex, '') AS  pat_sex,\n" +
                "  COALESCE(a.pat_agestr, '') AS  pat_agestr,\n" +
                "  COALESCE(a.pat_ageyear, 0) AS pat_ageyear,\n" +
                "  COALESCE(a.pat_birth, '') AS   pat_birth,\n" +
                "  COALESCE(a.pat_typecode, '') AS   pat_typecode,\n" +
                "  COALESCE(a.pat_diag, '') AS pat_diag,\n" +
                "  COALESCE(a.pat_diag_icd, '') AS   pat_diag_icd,\n" +
                "  COALESCE(a.pat_height, 0) AS  pat_height,\n" +
                "  COALESCE(a.weight, 0) AS   weight,\n" +
                "  COALESCE(a.abo_bldtype, '') AS abo_bldtype,\n" +
                "  COALESCE(a.rh_bldtype, '') AS  rh_bldtype,\n" +
                "  COALESCE(a.charge_typeno, '') AS  charge_typeno,\n" +
                "  COALESCE(a.req_deptno, '') AS  req_deptno,\n" +
                "  COALESCE(a.req_wardno, '') AS  req_wardno,\n" +
                "  COALESCE(a.req_bedno, '') AS   req_bedno,\n" +
                "  COALESCE(a.req_docno, '') AS   req_docno,\n" +
                "  COALESCE(a.req_dt, '') AS   req_dt,\n" +
                "  COALESCE(a.sampled_dt, '') AS  sampled_dt,\n" +
                "  COALESCE(a.recieve_dt, '') AS  recieve_dt,\n" +
                "  COALESCE(a.input_dt, '') AS input_dt,\n" +
                "  COALESCE(a.test_dt, '') AS  test_dt,\n" +
                "  COALESCE(a.report_dt, '') AS   report_dt,\n" +
                "  COALESCE(a.report_user, '') AS report_user,\n" +
                "  COALESCE(a.report_username, '') AS   report_username,\n" +
                "  COALESCE(a.rechk_dt, '') AS rechk_dt,\n" +
                "  COALESCE(a.rechk_user, '') AS  rechk_user,\n" +
                "  COALESCE(a.rechk_username, '') AS rechk_username,\n" +
                "  COALESCE(a.report_comm, '') AS report_comm,\n" +
                "  COALESCE(a.lab_advice, '') AS  lab_advice,\n" +
                "  COALESCE(a.txtinfo1, '') AS txtinfo1,\n" +
                "  COALESCE(a.txtinfo2, '') AS txtinfo2,\n" +
                "  COALESCE(a.instr_advice, '') AS   instr_advice,\n" +
                "  COALESCE(a.rechk2_dt, '') AS   rechk2_dt,\n" +
                "  COALESCE(a.rechk2_user, '') AS rechk2_user,\n" +
                "  COALESCE(a.rechk2_username, '') AS   rechk2_username,\n" +
                "  COALESCE(a.lastmodify_dt, '') AS  lastmodify_dt,\n" +
                "  COALESCE(a.prereport_dt, '') AS   prereport_dt,\n" +
                "  COALESCE(a.lastprint_dt, '') AS   lastprint_dt,\n" +
                "  COALESCE(a.print_count, 0) AS print_count,\n" +
                "  COALESCE(a.release_dt, '') AS  release_dt,\n" +
                "  COALESCE(a.release_user, '') AS   release_user,\n" +
                "  COALESCE(a.charge_flag, '') AS charge_flag,\n" +
                "  COALESCE(a.alter_flag, '') AS  alter_flag,\n" +
                "  COALESCE(a.req_reason, '') AS  req_reason,\n" +
                "  COALESCE(a.is_changed, '') AS  is_changed,\n" +
                "  COALESCE(a.changed_afterreport, '') AS  changed_afterreport,\n" +
                "  COALESCE(a.del_reason, '') AS  del_reason,\n" +
                "  COALESCE(a.encrypt_flag, '') AS   encrypt_flag,\n" +
                "  COALESCE(a.encrypt_user, '') AS   encrypt_user,\n" +
                "  COALESCE(a.stat_flag, '') AS   stat_flag,\n" +
                "  COALESCE(a.germ_flag, '') AS   germ_flag,\n" +
                "  COALESCE(a.item_num, 0) AS item_num,\n" +
                "  COALESCE(a.redo_flag, '') AS   redo_flag,\n" +
                "  COALESCE(a.unprint_flag, '') AS   unprint_flag,\n" +
                "  COALESCE(a.unprint_reason, '') AS unprint_reason,\n" +
                "  COALESCE(a.upload_time, '') AS upload_time,\n" +
                "  COALESCE(a.reserve1, '') AS reserve1,\n" +
                "  COALESCE(a.reserve2, '') AS reserve2,\n" +
                "  COALESCE(a.reserve3, '') AS reserve3,\n" +
                "  COALESCE(a.reserve4, '') AS reserve4,\n" +
                "  COALESCE(a.reserve5, '') AS reserve5,\n" +
                "  COALESCE(a.test_info, '') AS   test_info,\n" +
                "  COALESCE(a.exprint_count, 0) AS  exprint_count,\n" +
                "  COALESCE(a.unpriceflag, '') AS unpriceflag,\n" +
                "  COALESCE(a.pat_address, '') AS pat_address,\n" +
                "  COALESCE(a.pat_nation, '') AS  pat_nation,\n" +
                "  COALESCE(a.pat_idcardno, '') AS   pat_idcardno,\n" +
                "  COALESCE(a.sampled_user, '') AS   sampled_user,\n" +
                "  COALESCE(a.report_dt1, '') AS  report_dt1,\n" +
                "  COALESCE(a.viewflag, '') AS viewflag,\n" +
                "  COALESCE(a.labhospital, '') AS labhospital,\n" +
                "  COALESCE(a.tubebarcode, '') AS tubebarcode,\n" +
                "  COALESCE(a.markreason, '') AS  markreason,\n" +
                "  COALESCE(a.pat_namew, '') AS   pat_namew,\n" +
                "  COALESCE(a.test_flag, '') AS   test_flag,\n" +
                "  COALESCE(a.report_seq, 0) AS  report_seq,\n" +
                "  COALESCE(a.markreasonpass, '') AS markreasonpass,\n" +
                "  COALESCE(a.bindflag, '') AS bindflag,\n" +
                "  COALESCE(a.input_user, '') AS  input_user,\n" +
                "  COALESCE(a.original_hospital, '') AS original_hospital,\n" +
                "  COALESCE(a.warn_flag, '') AS   warn_flag,\n" +
                "  COALESCE(a.signnamedt, '') AS  signnamedt,\n" +
                "  COALESCE(a.training_user, '') AS  training_user,\n" +
                "  COALESCE(a.pat_enname, '') AS  pat_enname,\n" +
                "  COALESCE(a.pat_othno1, '') AS  pat_othno1,\n" +
                "  COALESCE(a.pat_othno2, '') AS  pat_othno2,\n" +
                "  COALESCE(a.pat_othno3, '') AS  pat_othno3,\n" +
                "  COALESCE(a.physicycle, '') AS  physicycle,\n" +
                "  COALESCE(a.report_serialno, '') AS report_serialno,\n" +
                "  COALESCE(a.pat_diag_tcm, '') AS pat_diag_tcm,\n" +
                "  a.`lastmodify_dt` AS commit_time,\n" +
                "  b.rptunitno,\n" +
                "  b.rptunitname, \n" +
                "  c.patient_id,\n" +
                "  c.ods_patient_id\n" +
                "from ods_rmlis6.lab_report_test20231018 a\n" +
                "left join ods_rmlis6.lab_reportunit b on b.rptunitid  = a.rptunitid \n" +
                "left join (\n" +
                "        select DISTINCT visit_no, patient_id,ods_patient_id from ads.patient_visit_d\n" +
                ") c on c.visit_no = a.pat_no \n" +
                "JOIN (SELECT max(lastmodify_dt) et from ads_rmlis.rmlis_lab_report_table where pt != from_unixtime(unix_timestamp(),'yyyyMMdd')) d \n" +
                "where \n" +
                "a.reportid is not null \n" +
                "and a.pat_id is not null and a.pat_id <> '' \n" +
                "AND (a.`lastmodify_dt` > d.et OR d.et IS NULL)";


        Select select = (Select) CCJSqlParserUtil.parse(sql);
        TableAndConditionVisitor visitor = new TableAndConditionVisitor();
        select.getSelectBody().accept(visitor);
        List<TableParseBO> tableParseBOS = visitor.getTableParseVOS();
        tableParseBOS.stream().forEach(System.out::println);
        System.out.println("--------------------------------");
        Map<String, TableParseBO> tableParseVOMap = tableParseBOS.stream().collect(Collectors.toMap(TableParseBO::getAlias, Function.identity()));
        List<JoinConditionBO> joinConditionBOS = visitor.getJoinConditionVOS();
        joinConditionBOS.forEach(joinConditionBO -> {
            Optional.ofNullable(tableParseVOMap.get(joinConditionBO.getLeftTblAlias()))
                    .ifPresent(leftTableParseBO -> {
                        joinConditionBO.setLeftDbName(leftTableParseBO.getDbName());
                        joinConditionBO.setLeftTblName(leftTableParseBO.getTblName());
                    });

            Optional.ofNullable(tableParseVOMap.get(joinConditionBO.getRightTblAlias()))
                    .ifPresent(rightTableParseBO -> {
                        joinConditionBO.setRightDbName(rightTableParseBO.getDbName());
                        joinConditionBO.setRightTblName(rightTableParseBO.getTblName());
                    });
            System.out.println(joinConditionBO);
        });
        System.out.println("--------------------------------");
        joinConditionBOS.stream().filter(s -> StringUtils.isBlank(s.getLeftTblName()) || StringUtils.isBlank(s.getRightTblName()))
                .forEach(s -> {
                    try {
                        if (StringUtils.isBlank(s.getLeftTblName())) {
                            parseTblNameByTblAliasAndColumn(sql, s.getLeftTblAlias(), s.getLeftColumn(), Boolean.FALSE, s);
                        }
                        if (StringUtils.isBlank(s.getRightTblName())) {
                            parseTblNameByTblAliasAndColumn(sql, s.getRightTblAlias(), s.getRightColumn(), Boolean.TRUE, s);
                        }
                    } catch (JSQLParserException e) {
                        throw new RuntimeException(e);
                    }
                });
        System.out.println("--------------------------------");
        joinConditionBOS.forEach(s -> System.out.println(s));
    }

    private static void parseTblNameByTblAliasAndColumn(String sql, String tblAlias, String column, boolean right, JoinConditionBO joinConditionBO) throws JSQLParserException {
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        FromItem fromItem = plainSelect.getFromItem();
        List<Join> joins = plainSelect.getJoins();
        List<SelectItem> selectItems = plainSelect.getSelectItems();

        TableParseBO tableParseBO = processJoin(joins, column);
        if (Objects.isNull(tableParseBO)){
            tableParseBO = processTable(fromItem, selectItems, column);
        }
        Optional.ofNullable(tableParseBO).ifPresent(s -> {
            if (right) {
                joinConditionBO.setRightDbName(s.getDbName());
                joinConditionBO.setRightTblName(s.getTblName());
            } else {
                joinConditionBO.setLeftDbName(s.getDbName());
                joinConditionBO.setLeftTblName(s.getTblName());
            }
        });
    }

    private static TableParseBO processJoin(List<Join> joins, String column) {
        if (CollectionUtils.isNotEmpty(joins)) {
            for (Join join : joins) {
                FromItem rightItem = join.getRightItem();
                if (rightItem instanceof SubSelect) {
                    SubSelect subSelect = (SubSelect) rightItem;
                    SelectBody subSelectBody = subSelect.getSelectBody();
                    PlainSelect plainSelect = (PlainSelect) subSelectBody;
                    return processTable(plainSelect.getFromItem(), plainSelect.getSelectItems(), column);
                }
            }
        }
        return null;

    }

    private static TableParseBO processTable(FromItem fromItem, List<SelectItem> selectItems, String leftColumn) {
        AtomicReference<TableParseBO> tableParseBO = new AtomicReference<>();
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            if (Objects.equals(table.getASTNode().jjtGetFirstToken().toString(), table.getASTNode().jjtGetLastToken().toString())) {
                throw new RuntimeException("FROM clause must be in the format 'dbname.tablename'");
            }
            if (CollectionUtils.isNotEmpty(selectItems)) {
                selectItems.forEach(selectItem -> {
                    SimpleNode astNode = selectItem.getASTNode();
                    if (astNode.jjtGetLastToken().toString().equals(leftColumn)) {
                        tableParseBO.set(TableParseBO.builder().tblName(table.getName())
                                .dbName(table.getASTNode().jjtGetFirstToken().toString()).build());

                    }
                });
            }
        } else if (fromItem instanceof SubSelect) {
            SubSelect subSelect = (SubSelect) fromItem;
            SelectBody subSelectBody = subSelect.getSelectBody();
            PlainSelect plainSelect = (PlainSelect) subSelectBody;
            return processTable(plainSelect.getFromItem(), plainSelect.getSelectItems(), leftColumn);
        }
        return tableParseBO.get();
    }
}
