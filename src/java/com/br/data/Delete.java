/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.br.data;

import com.br.connection.ConnectDB2;
import com.br.connection.ConnectSQLServer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.jettison.json.JSONArray;

/**
 *
 * @author Wattana
 */
public class Delete {

    public static JSONArray deleteFinanceMaster(String company, String customerid) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {
//                RHCONO, RHDIVI, RHWARE, RHPERI
                Statement stmt = conn.createStatement();
                String query = "DELETE FROM BRLDTA0100.BP_MASTER\n"
                        + "WHERE BPM_CONO = '" + company + "'\n"
                        + "AND BPM_CUNO = '" + customerid + "'";
                System.out.println("deleteFinanceMaster\n" + query);
                stmt.execute(query);

                Map<String, Object> mMap = new HashMap<>();
                mMap.put("result", "ok");
                mMap.put("message", "delete complete");
                mJSonArr.put(mMap);

            } else {
                System.out.println("Server can't connect.");
            }

        } catch (SQLException sqle) {
            throw sqle;
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> mMap = new HashMap<>();
            mMap.put("result", "nok");
            mMap.put("message", e);
            mJSonArr.put(mMap);
            if (conn != null) {
                conn.close();
            }
            throw e;
        } finally {
            if (conn != null) {
                conn.close();
            }
        }

        return mJSonArr;

    }

    public static String deleteStartMonth(String year, String month, String invround) throws Exception {

        String respond = "";
        String check = "0";
        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                if (month.equals("02")) {
                    check = "1";
                    respond = "Please do not delete starting month";
                }
                if (check.equals("0")) {
                    Statement stmt = conn.createStatement();
                    String query = "DELETE FROM BRLDTA0100.BP_STDATE A\n"
                            + "WHERE EXISTS(SELECT A.BPS_CONO,A.BPS_CUNO,A.BPS_STDT,A.BPS_FNDT,A.BPS_STS,A.BPS_RD,A.BPS_PR,C.OKCUNM\n"
                            + "FROM BRLDTA0100.BP_STDATE D,BRLDTA0100.BP_MASTER B, M3FDBPRD.OCUSMA C\n"
                            + "WHERE B.BPM_CUNO = D.BPS_CUNO\n"
                            + "AND D.BPS_CONO = C.OKCONO\n"
                            + "AND D.BPS_CUNO = C.OKCUNO\n"
                            + "AND A.BPS_CONO = D.BPS_CONO\n"
                            + "AND A.BPS_CUNO = D.BPS_CUNO\n"
                            + "AND A.BPS_STDT = D.BPS_STDT\n"
                            + "AND A.BPS_FNDT  = D.BPS_FNDT \n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND  MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + month + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + ")";
                    System.out.println(query);
                    stmt.execute(query);

                    respond = "Delete succesfully";
                }
            } else {
                System.out.println("Server can't connect.");
            }

        } catch (SQLException sqle) {
            throw sqle;
        } catch (Exception e) {
            e.printStackTrace();
            if (conn != null) {
                conn.close();
            }
            throw e;
        } finally {
            if (conn != null) {
                conn.close();
            }
        }

        return respond;

    }

    public static String deleteHeaderMonth(String year, String month,String invround) throws Exception {

        String respond = "";
        String check = "0";
        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

//                if (month.equals("02")) {
//                    check = "1";
//                    respond = "Please do not delete starting month";
//                }
                if (check.equals("0")) {
                    Statement stmt = conn.createStatement();
                    String query = "DELETE FROM BRLDTA0100.BP_HBILL A\n"
                            + "WHERE EXISTS(SELECT A.BPH_CONO,A.BPH_CUNO,A.BPH_STDT,A.BPH_FNDT,A.BPH_STS,C.OKCUNM\n"
                            + "FROM BRLDTA0100.BP_HBILL D,BRLDTA0100.BP_MASTER B, M3FDBPRD.OCUSMA C\n"
                            + "WHERE B.BPM_CUNO = D.BPH_CUNO\n"
                            + "AND D.BPH_CONO = C.OKCONO\n"
                            + "AND D.BPH_CUNO = C.OKCUNO\n"
                            + "AND A.BPH_CONO = D.BPH_CONO\n"
                            + "AND A.BPH_CUNO = D.BPH_CUNO\n"
                            + "AND A.BPH_STDT = D.BPH_STDT\n"
                            + "AND A.BPH_FNDT  = D.BPH_FNDT \n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + month + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + ")";
                    System.out.println(query);
                    stmt.execute(query);

                    respond = "Delete succesfully";
                }
            } else {
                System.out.println("Server can't connect.");
            }

        } catch (SQLException sqle) {
            throw sqle;
        } catch (Exception e) {
            e.printStackTrace();
            if (conn != null) {
                conn.close();
            }
            throw e;
        } finally {
            if (conn != null) {
                conn.close();
            }
        }

        return respond;

    }


}
