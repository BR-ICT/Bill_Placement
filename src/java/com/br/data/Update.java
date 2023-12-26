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

    public static void Facility(String cono, String divi, String code, String desc, String name, String type, String muun, String rate, String ref1, String ref2) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.A1CASU\n"
                        + "SET A1DESC = '" + desc + "'\n"
                        + ",A1NAME = '" + name + "'\n"
                        + ",A1TYPE = '" + type + "'\n"
                        + ",A1MUUN = '" + muun + "'\n"
                        + ",A1RATE = '" + rate + "'\n"
                        + ",A1REF1 = '" + ref1 + "'\n"
                        + ",A1REF2 = '" + ref2 + "'\n"
                        + "WHERE A1CONO = '" + cono + "'\n"
                        + "AND A1DIVI = '" + divi + "'\n"
                        + "AND A1CODE = '" + code + "'";
                System.out.println("UpdateFacility\n" + query);
                stmt.execute(query);

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

    }

    public static void Type(String cono, String divi, String code, String desc, String ref) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.A1TYPE\n"
                        + "SET ATNAME = '" + desc + "'\n"
                        + ",ATREF1 = '" + ref + "'\n"
                        + "WHERE ATCONO = '" + cono + "'\n"
                        + "AND ATDIVI = '" + divi + "'\n"
                        + "AND ATTYPE = '" + code + "'";
                System.out.println("UpdateType\n" + query);
                stmt.execute(query);

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

    }

    public static void Level(String cono, String divi, String code, String struct, String level, String desc, String ref1, String ref2) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.A2CASL\n"
                        + "SET A2SLVL = '" + level + "'\n"
                        + ",A2DESC = '" + desc + "'\n"
                        + ",A2REF1 = '" + ref1 + "'\n"
                        + ",A2REF2 = '" + ref2 + "'\n"
                        + "WHERE A2CONO = '" + cono + "'\n"
                        + "AND A2STID = '" + struct + "'\n"
                        + "AND A2DIVI = '" + divi + "'\n"
                        + "AND A2CODE = '" + code + "'";
                System.out.println("UpdateLevel\n" + query);
                stmt.execute(query);

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

    }

    public static void Period(String cono, String divi, String code, String year, String month, String desc) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.APERIO\n"
                        + "SET PEYEA4 = '" + year + "'\n"
                        + ",PEMONT = '" + month + "'\n"
                        + ",PEDESC = '" + desc + "'\n"
                        + "WHERE PECONO = '" + cono + "'\n"
                        + "AND PEDIVI = '" + divi + "'\n"
                        + "AND PECODE = '" + code + "'";
                System.out.println("UpdatePeriod\n" + query);
                stmt.execute(query);

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

    }

    public static void WorkCenter(String cono, String divi, String code, String struct, String level, String itm, String str, String desc, String mety, String uehr, String mela, String per,
            String acc, String aac1, String aac2, String boi, String ref1, String ref2, String ref3) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.A3CAWC\n"
                        + "SET A3STID = '" + struct + "'\n"
                        + ",A3ASTR = '" + str + "'\n"
                        + ",A3ADES = '" + desc + "'\n"
                        + ",A3METY = '" + mety + "'\n"
                        + ",A3UEHR = '" + uehr + "'\n"
                        + ",A3MELA = '" + mela + "'\n"
                        + ",A3PERS = '" + per + "'\n"
                        + ",A3ACCT = '" + acc + "'\n"
                        + ",A3AAC1 = '" + aac1 + "'\n"
                        + ",A3AAC2 = '" + aac2 + "'\n"
                        + ",A3ABOI = '" + boi + "'\n"
                        + ",A3ARE1 = '" + ref1 + "'\n"
                        + ",A3ARE2 = '" + ref2 + "'\n"
                        + ",A3ARE3 = '" + ref3 + "'\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3AITM = '" + itm + "'";
                System.out.println("UpdateWorkCenter\n" + query);
                stmt.execute(query);

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

    }

    public static void LevelWorkCenter(String cono, String divi, String code, String struct, String level, String itm, String str, String desc, String mety, String uehr, String mela, String per,
            String acc, String aac1, String aac2, String boi, String ref1, String ref2, String ref3) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.A3CAWC\n"
                        + "SET A3STID = '" + struct + "'\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3AITM = '" + itm + "'";
                System.out.println("LevelWorkCenter\n" + query);
                stmt.execute(query);

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

    }

    public static void Bills(String cono, String divi, String code, String period, String meterstart, String meterend, String qty, String amount, String vat, String total, String totalamt, String type) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.E1ALLO\n"
                        + "SET E1METS = '" + meterstart + "'\n"
                        + ",E1METE = '" + meterend + "'\n"
                        + ",E1EPQT = '" + qty + "'\n"
                        + ",E1EPAT = '" + amount + "'\n"
                        + ",E1EVAT = '" + vat + "'\n"
                        + ",E1ETAT = '" + total + "'\n"
                        + ",E1RAAT = '" + totalamt + "'\n"
                        + "WHERE E1CONO = '" + cono + "'\n"
                        + "AND E1DIVI = '" + divi + "'\n"
                        + "AND E1CODE = '" + code + "'\n"
                        + "AND E1PECO = '" + period + "'\n"
                        + "AND E1TYPE = '" + type + "'";
                System.out.println("UpdateBills\n" + query);
                stmt.execute(query);

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

    }

    public static void Allocation(String cono, String divi, String code, String struct, String level, String itm, String period, String meterstart, String meterend, String rate, String hour, String qty, String pers, String amt, String total, String status, String type) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.E2ALLO\n"
                        + "SET E2STID = '" + struct + "'\n"
                        + ",E2SLVL = '" + level + "'\n"
                        + ",E2METS = '" + meterstart + "'\n"
                        + ",E2METE = '" + meterend + "'\n"
                        + ",E2UEHR = '" + rate + "'\n"
                        + ",E2HOUR = '" + hour + "'\n"
                        + ",E2EPQT = '" + qty + "'\n"
                        //                        + ",E2EPQT = ROUND(" + meterend + " - " + meterstart + ",2)\n"
                        + ",E2PERS = '" + pers + "'\n"
                        + ",E2EAQT = '" + amt + "'\n"
                        + ",E2EAAT = '" + total + "'\n"
                        + "WHERE E2CONO = '" + cono + "'\n"
                        + "AND E2DIVI = '" + divi + "'\n"
                        + "AND E2CODE = '" + code + "'\n"
                        + "AND E2PECO = '" + period + "'\n"
                        + "AND E2AITM = '" + itm + "'\n"
                        + "AND E2TYPE = '" + type + "'";
                System.out.println("UpdateAllocation\n" + query);
                stmt.execute(query);

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

    }

    public static void StatusAllocation(String cono, String divi, String code, String period, String oldstatus, String newstatus, String type) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.E2ALLO\n"
                        + "SET E2STAT = '" + newstatus + "'\n"
                        + "WHERE E2CONO = '" + cono + "'\n"
                        + "AND E2DIVI = '" + divi + "'\n"
                        + "AND E2CODE = '" + code + "'\n"
                        + "AND E2PECO = '" + period + "'\n"
                        + "AND E2STAT = '" + oldstatus + "'\n"
                        + "AND E2TYPE = '" + type + "'";
                System.out.println("UpdateAllocation\n" + query);
                stmt.execute(query);

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

    }

    public static void StatusAllocationLevel(String cono, String divi, String code, String period, String struct, String level, String oldstatus, String newstatus, String type) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLDTA0100.E2ALLO\n"
                        + "SET E2STAT = '" + newstatus + "'\n"
                        + "WHERE E2CONO = '" + cono + "'\n"
                        + "AND E2DIVI = '" + divi + "'\n"
                        + "AND E2CODE = '" + code + "'\n"
                        + "AND E2PECO = '" + period + "'\n"
                        + "AND E2STID = '" + struct + "'\n"
                        + "AND E2SLVL= '" + level + "'\n"
                        + "AND E2STAT = '" + oldstatus + "'\n"
                        + "AND E2TYPE = '" + type + "'";
                System.out.println("UpdateAllocation\n" + query);
                stmt.execute(query);

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

    }

    public static JSONArray updateRentalHead(String cono, String divi, String whs, String period) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectSQLServer.ConnectionDB();

        try {
            if (conn != null) {
//                RHCONO, RHDIVI, RHWARE, RHPERI
                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLAS400.dbo.T_RENTALHEAD \n"
                        + "WHERE RHCONO = '" + cono + "'\n"
                        + "AND RHDIVI = '" + divi + "'\n"
                        + "AND RHWARE = '" + whs + "'\n"
                        + "AND RHPERI = '" + period + "'";
                System.out.println("updateRentalHead\n" + query);
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

    public static JSONArray updateRentalDetail(String cono, String divi, String whs, String period, String line, String duedate, String qty, String kgs, String delQty, String delKgs) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectSQLServer.ConnectionDB();

        try {
            if (conn != null) {
//                RHCONO, RHDIVI, RHWARE, RHPERI
                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLAS400.dbo.T_RENTALDETAIL02 \n"
                        + "SET RDDUE_DATE = '" + duedate + "' \n"
                        + ", RDQTY = '" + qty + "' \n"
                        + ", RDTOTA_KGS = '" + kgs + "' \n"
                        + ", RDDEL_PACK = '" + delQty + "' \n"
                        + ", RDDEL_KGS = '" + delKgs + "' \n"
                        + "WHERE RDCONO = '" + cono + "'\n"
                        + "AND RDDIVI = '" + divi + "'\n"
                        + "AND RDWARE = '" + whs + "'\n"
                        + "AND RDPERI = '" + period + "'\n"
                        + "AND RDLINE = '" + line + "'";
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

    public static JSONArray updateFinanceMaster(String company, String customerid, String customertype, String roundinv, String roundbill, String roundpay, String roundcasebill, String roundbillspecial, String roundcasepay, String roundpayspecial, String caseinvoice) throws Exception {

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

    public static JSONArray updateRentalAddon(String cono, String divi, String whs, String period, String line, String amt) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectSQLServer.ConnectionDB();

        try {
            if (conn != null) {
//                RHCONO, RHDIVI, RHWARE, RHPERI
                Statement stmt = conn.createStatement();
                String query = "UPDATE BRLAS400.dbo.T_RENTALADDON \n"
                        + "SET RAAMT = '" + amt + "' \n"
                        + "WHERE RACONO = '" + cono + "'\n"
                        + "AND RADIVI = '" + divi + "'\n"
                        + "AND RAWARE = '" + whs + "'\n"
                        + "AND RAPERI = '" + period + "'\n"
                        + "AND RALINE = '" + line + "'";
                System.out.println("updateRentalAddon\n" + query);
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
