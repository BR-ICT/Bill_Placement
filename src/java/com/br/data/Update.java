/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.br.data;

import com.br.connection.ConnectDB2;
import com.br.connection.ConnectSQLServer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.jettison.json.JSONArray;

/**
 *
 * @author Wattana
 */
public class Update {


   
    public static JSONArray calculateEndDate() throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectSQLServer.ConnectionDB();

        try {
            if (conn != null) {
//                RHCONO, RHDIVI, RHWARE, RHPERI
                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.BP_STDATE C\n"
                        + "SET BPS_FNDT = (SELECT YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 10000\n"
                        + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 100\n"
                        + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS))  AS ENDDATE\n"
                        + "FROM BRLDTA0100.BP_STDATE A\n"
                        + "JOIN BRLDTA0100.BP_MASTER B\n"
                        + "ON B.BPM_CUNO = A.BPS_CUNO\n"
                        + "AND B.BPM_CONO = A.BPS_CONO\n"
                        + "AND C.BPS_CUNO = A.BPS_CUNO\n"
                        + "AND C.BPS_CONO = A.BPS_CONO\n"
                        + " FETCH FIRST 1 ROW ONLY)";
                System.out.println("updateRentalDetail\n" + query);
                stmt.execute(query);

                Map<String, Object> mMap = new HashMap<>();
                mMap.put("result", "ok");
                mMap.put("message", "update complete");
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

    public static JSONArray updateFinanceMaster(String company, String customerid, String customertype,
            String roundinv, String roundbill, String roundpay, String roundcasebill, String roundbillspecial,
            String roundcasepay, String roundpayspecial, String caseinvoice, String bildescription, String coldesciption, String colby, String remark) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {
//                RHCONO, RHDIVI, RHWARE, RHPERI
                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.BP_MASTER \n"
                        + "SET BPM_TYPE  = '" + customertype + "' \n"
                        + ",  BPM_RINV =  '" + roundinv + "' \n"
                        + ", BPM_RBIL =  '" + roundbill + "' \n"
                        + ",  BPM_RCOL =  '" + roundpay + "' \n"
                        + ",  BPM_CASEBIL =  '" + roundcasebill + "' \n"
                        + ",  BPM_CASECOL =  '" + roundcasepay + "' \n"
                        + ",  BPM_RBILSP =  '" + roundbillspecial + "' \n"
                        + ",  BPM_RCOLSP =  '" + roundpayspecial + "' \n"
                        + ",  BPM_CASEINV =  '" + caseinvoice + "' \n"
                        + " ,BPM_BILDESC = '" + bildescription + "'\n"
                        + " ,BPM_COLDESC = '" + coldesciption + "'\n"
                        + " ,BPM_COLBY = '" + colby + "'\n"
                        + " ,BPM_REMARK = '" + remark + "'\n"
                        + "WHERE  BPM_CONO =  '" + company + "' \n"
                        + "AND  BPM_CUNO = '" + customerid + "'";
                System.out.println("updateRentalDetail\n" + query);
                stmt.execute(query);

                Map<String, Object> mMap = new HashMap<>();
                mMap.put("result", "ok");
                mMap.put("message", "update complete");
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

    public static JSONArray updateStartFinance(String startdate, String enddate, String customerid, String round, String month, String year) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {
//                RHCONO, RHDIVI, RHWARE, RHPERI
                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.BP_STDATE\n"
                        + "SET BPS_STDT = " + startdate + "\n"
                        + ",BPS_FNDT = " + enddate + "\n"
                        + "WHERE BPS_CUNO = '" + customerid + "'\n"
                        + "AND BPS_RD = '" + round + "'\n"
                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + year + "\n"
                        + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + month;
                System.out.println("updateRentalDetail\n" + query);
                stmt.execute(query);

                Map<String, Object> mMap = new HashMap<>();
                mMap.put("result", "ok");
                mMap.put("message", "update complete");
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

    public static JSONArray updateHeaderFinance(String company, String customerid, String billdate, String paydate, String startdate, String enddate) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {
//                RHCONO, RHDIVI, RHWARE, RHPERI
                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.BP_HBILL\n"
                        + "SET BPH_BPDT = " + billdate + "\n"
                        + ",BPH_CLDT = " + paydate + "\n"
                        + "WHERE BPH_CUNO = '" + customerid + "'\n"
                        + "AND BPH_CONO = " + company + "\n"
                        + "AND BPH_STDT = '" + startdate + "'\n"
                        + "AND BPH_FNDT = '" + enddate + "'";
                System.out.println("updateRentalDetail\n" + query);
                stmt.execute(query);

                Map<String, Object> mMap = new HashMap<>();
                mMap.put("result", "ok");
                mMap.put("message", "update complete");
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

}
