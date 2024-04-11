/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.br.data;

import com.br.connection.ConnectDB2;
import com.br.connection.ConnectSQLServer;
import static java.lang.Integer.parseInt;
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
public class Insert {

    public static void Facility(String cono, String divi, String code, String desc, String name, String type, String muun, String rate, String ref1, String ref2) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "INSERT INTO BRLDTA0100.A1CASU\n"
                        + "(A1CONO,A1DIVI,A1CODE,A1DESC,A1NAME,A1TYPE,A1MUUN,A1RATE,A1REF1,A1REF2)\n"
                        + "VALUES('" + cono + "'\n"
                        + ",'" + divi + "'\n"
                        + ",'" + code + "'\n"
                        + ",'" + desc + "'\n"
                        + ",'" + name + "'\n"
                        + ",'" + type + "'\n"
                        + ",'" + muun + "'\n"
                        + "," + rate + "\n"
                        + ",'" + ref1 + "'\n"
                        + ",'" + ref2 + "')";
                System.out.println("InsertFacility\n" + query);
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
                String query = "INSERT INTO BRLDTA0100.A1TYPE\n"
                        + "(ATCONO,ATDIVI,ATTYPE,ATNAME,ATREF1)\n"
                        + "VALUES('" + cono + "'\n"
                        + ",'" + divi + "'\n"
                        + ",'" + code + "'\n"
                        + ",'" + desc + "'\n"
                        + ",'" + ref + "')";
                System.out.println("InsertType\n" + query);
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
                String query = "INSERT INTO BRLDTA0100.A2CASL\n"
                        + "(A2CONO,A2DIVI,A2CODE,A2STID,A2SLVL,A2DESC,A2REF1,A2REF2)\n"
                        + "VALUES('" + cono + "'\n"
                        + ",'" + divi + "'\n"
                        + ",'" + code + "'\n"
                        + ",'" + struct + "'\n"
                        + ",'" + level + "'\n"
                        + ",'" + desc + "'\n"
                        + ",'" + ref1 + "'\n"
                        + ",'" + ref2 + "')";
                System.out.println("InsertLevel\n" + query);
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
                String query = "INSERT INTO BRLDTA0100.APERIO\n"
                        + "(PECONO,PEDIVI,PECODE,PEYEA4,PEMONT,PEDESC)\n"
                        + "VALUES('" + cono + "'\n"
                        + ",'" + divi + "'\n"
                        + ",'" + code + "'\n"
                        + ",'" + year + "'\n"
                        + ",'" + month + "'\n"
                        + ",'" + desc + "')";
                System.out.println("InsertPeriod\n" + query);
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
                String query = "INSERT INTO BRLDTA0100.A3CAWC\n"
                        + "(A3CONO,A3DIVI,A3CODE,A3STID,A3SLVL,A3AITM,A3ASTR,A3ADES,A3METY,A3UEHR,A3MELA,A3PERS,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3)\n"
                        + "VALUES('" + cono + "'\n"
                        + ",'" + divi + "'\n"
                        + ",'" + code + "'\n"
                        + ",'" + struct + "'\n"
                        + ",'" + level + "'\n"
                        + ",'" + itm + "'\n"
                        + ",'" + str + "'\n"
                        + ",'" + desc + "'\n"
                        + ",'" + mety + "'\n"
                        + ",'" + uehr + "'\n"
                        + ",'" + mela + "'\n"
                        + ",'" + per + "'\n"
                        + ",'" + acc + "'\n"
                        + ",'" + aac1 + "'\n"
                        + ",'" + aac2 + "'\n"
                        + ",'" + boi + "'\n"
                        + ",'" + ref1 + "'\n"
                        + ",'" + ref2 + "'\n"
                        + ",'" + ref3 + "')";
                System.out.println("InsertWorkCenter\n" + query);
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
                String query = "INSERT INTO BRLDTA0100.E1ALLO\n"
                        + "(E1CONO, E1DIVI, E1CODE, E1PECO, E1METS, E1METE, E1EPQT, E1EPAT, E1EVAT, E1ETAT, E1RAAT, E1TYPE)\n"
                        + "VALUES('" + cono + "'\n"
                        + ",'" + divi + "'\n"
                        + ",'" + code + "'\n"
                        + ",'" + period + "'\n"
                        + ",'" + meterstart + "'\n"
                        + ",'" + meterend + "'\n"
                        + ",'" + qty + "'\n"
                        + ",'" + amount + "'\n"
                        + ",'" + vat + "'\n"
                        + ",'" + total + "'\n"
                        + ",'" + totalamt + "'\n"
                        + "," + type + ")";
                System.out.println("InsertBills\n" + query);
//                stmt.execute(query);

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

//    public static void BillsAllocated(String cono, String divi, String code, String period, String meterstart, String meterend, String qty, String amount, String vat, String total, String totalamt, String type) throws Exception {
//
//        Connection conn = ConnectDB2.ConnectionDB();
//
//        try {
//            if (conn != null) {
//
//                Statement stmt = conn.createStatement();
//                String query = "INSERT INTO BRLDTA0100.E1ALLO\n"
//                        + "(E1CONO, E1DIVI, E1CODE, E1PECO, E1METS, E1METE, E1EPQT, E1EPAT, E1EVAT, E1ETAT, E1TYPE)\n"
//                        + "VALUES('" + cono + "'\n"
//                        + ",'" + divi + "'\n"
//                        + ",'" + code + "'\n"
//                        + ",'" + period + "'\n"
//                        + ",'" + meterstart + "'\n"
//                        + ",'" + meterend + "'\n"
//                        + ",'" + qty + "'\n"
//                        + ",'" + amount + "'\n"
//                        + ",'" + vat + "'\n"
//                        + ",'" + total + "'\n"
//                        + ",'" + totalamt + "'\n"
//                        + "," + type + ")";
//                System.out.println("InsertBills\n" + query);
//                stmt.execute(query);
//
//            } else {
//                System.out.println("Server can't connect.");
//            }
//
//        } catch (SQLException sqle) {
//            throw sqle;
//        } catch (Exception e) {
//            e.printStackTrace();
//            if (conn != null) {
//                conn.close();
//            }
//            throw e;
//        } finally {
//            if (conn != null) {
//                conn.close();
//            }
//        }
//
//    }
    public static String addStartMonth(String year, String month, String invround, String customer, String startdate) throws Exception {

        String respond = "";
        Connection conn = ConnectDB2.ConnectionDB();
        String date2 = startdate.substring(0, 4) + startdate.substring(5, 7) + startdate.substring(8, 10);
        System.out.println(date2);
        String currentmonth = startdate.substring(5, 7);
        String currentyear = startdate.substring(0, 4);
        String query1 = "";
        String query2 = "";
        String query3 = "";
        String query4 = "";
        String querycheck = "";
        String querycheck1 = "1";
        String querycase1 = "";
        String highestyearquery = "";
        String highestmonthquery = "";
        String maxmonth = "";
        String maxyear = "";
        Boolean firsttime = true;
        String querycase2 = "";
        //check if data is existed in master or not
        String check = "0";
        //check if data in the already existed or not
        String check1 = "1";
//        String nextmonth = Integer.toString(parseInt(month) + 1);
//        if ("13".equals(nextmonth)) {
//            nextmonth = "1";
//            year = Integer.toString(parseInt(year) + 1);
//        }
        try {
            if (conn != null) {
                Statement stmtgethighestyear = conn.createStatement();
                highestyearquery = "SELECT MAX(YEAR(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))))\n"
                        + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B-- Master DATA\n"
                        + "WHERE B.BPM_CUNO = A.BPS_CUNO \n"
                        + "AND B.BPM_RINV =" + invround;
                ResultSet maxyear1 = stmtgethighestyear.executeQuery(highestyearquery);
                while (maxyear1.next()) {
                    maxyear = maxyear1.getString(1);
                }
                Statement stmtgethighestmonth = conn.createStatement();
                highestmonthquery = "SELECT MAX(MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))))\n"
                        + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B-- Master DATA\n"
                        + "WHERE B.BPM_CUNO = A.BPS_CUNO \n"
                        + "AND YEAR(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + maxyear + "\n"
                        + "AND B.BPM_RINV =" + invround;
                ResultSet maxmonth1 = stmtgethighestmonth.executeQuery(highestmonthquery);
                while (maxmonth1.next()) {
                    maxmonth = maxmonth1.getString(1);
                }
                Statement stmtcheck1 = conn.createStatement();
                querycheck1 = "SELECT DISTINCT LPAD((MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD')))), 2, '0')\n"
                        + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                        + "Where YEAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + currentyear + "\n"
                        + "AND A.BPS_CONO = B.BPM_CONO \n"
                        + "AND A.BPS_CUNO = B.BPM_CUNO\n"
                        + "AND B.BPM_CUNO = '" + customer + "'";
                ResultSet mRes = stmtcheck1.executeQuery(querycheck1);
                while (mRes.next()) {
                    if (currentmonth.equals(mRes.getString(1))) {
                        respond = "data already generated";
                        check = "1";
                    }
                }
                Statement stmtcheck = conn.createStatement();
                querycheck = "SELECT BPM_CUNO\n"
                        + "FROM BRLDTA0100.BP_MASTER A-- Master DATA\n"
                        + "WHERE BPM_CUNO = '" + customer + "' AND BPM_RINV = '" + invround + "'";
                ResultSet mRes2 = stmtcheck.executeQuery(querycheck);
                while (mRes2.next()) {
                    if (customer.equals(mRes2.getString(1))) {
                        check1 = "0";
                    } else {
                    }
                }
                if ("1".equals(check1)) {
                    respond = "Wrong ID or does not existed in this invoice round!";
                }
                if (check.equals("0") && check1.equals("0")) {
                    while ((Integer.parseInt(currentmonth) <= Integer.parseInt(maxmonth) || Integer.parseInt(currentyear) != Integer.parseInt(maxyear)) && Integer.parseInt(currentyear) <= Integer.parseInt(maxyear)) {
                        if (firsttime) {
                            Statement stmt = conn.createStatement();
                            String step1 = "INSERT INTO BRLDTA0100.BP_STDATE(BPS_CONO,BPS_CUNO,BPS_STDT)\n"
                                    + "SELECT BPM_CONO,BPM_CUNO,'" + date2 + "'\n"
                                    + "FROM BRLDTA0100.BP_MASTER\n"
                                    + "WHERE BPM_CUNO = '" + customer + "'";
                            stmt.execute(step1);
                            firsttime = false;
                        } else if (!firsttime) {
                            Statement stmt = conn.createStatement();
                            String step1 = "INSERT INTO BRLDTA0100.BP_STDATE(BPS_CONO,BPS_CUNO,BPS_STDT,BPS_RD,BPS_STS)\n"
                                    + "SELECT DISTINCT A.BPS_CONO,A.BPS_CUNO, YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAYS )) * 10000\n"
                                    + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD') + 1 DAY))) * 100\n"
                                    + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD') + 1 DAY)))  AS ENDDATE,'1','00'\n"
                                    + "FROM BRLDTA0100.BP_STDATE A\n"
                                    + "JOIN BRLDTA0100.BP_MASTER B ON A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO  = B.BPM_CUNO\n"
                                    + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) <> MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAYS)\n"
                                    + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD')) + 1 DAY) = '" + currentmonth + "'\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY) ='" + currentyear + "'\n"
                                    + "AND B.BPM_CUNO = '" + customer + "'";
                            stmt.execute(step1);
                        }
                        Statement stmt2 = conn.createStatement();
                        Statement stmt3 = conn.createStatement();
                        Statement stmt4 = conn.createStatement();
                        Statement stmt5 = conn.createStatement();
                        Statement stmtcase1 = conn.createStatement();
                        Statement stmtcase2 = conn.createStatement();

//                while (Integer.parseInt(currentmonth) <= Integer.parseInt(month) || Integer.parseInt(currentyear) <= Integer.parseInt(year)) {
                        for (int i = 1; i <= 6; i++) {

//                    UPDATE END DATE OF START DATE TABLE USING MASTER TABLE 7 days
                            if (invround.equals("7")) {
                                query2 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                        + "SET C.BPS_FNDT = (SELECT YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 10000\n"
                                        + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 100\n"
                                        + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS))  AS ENDDATE\n"
                                        + "FROM BRLDTA0100.BP_STDATE A\n"
                                        + "JOIN BRLDTA0100.BP_MASTER B	\n"
                                        + "ON B.BPM_CUNO = A.BPS_CUNO\n"
                                        + "AND B.BPM_CONO = A.BPS_CONO\n"
                                        + "AND C.BPS_CUNO = A.BPS_CUNO\n"
                                        + "AND C.BPS_CONO = A.BPS_CONO\n"
                                        + "AND B.BPM_RINV = '" + invround + "'\n"
                                        + "AND B.BPM_CASEINV = '0'\n"
                                        + "AND A.BPS_CUNO = '" + customer + "'\n"
                                        + " FETCH FIRST 1 ROW ONLY)\n"
                                        + " WHERE EXISTS (SELECT YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 10000\n"
                                        + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 100\n"
                                        + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS))  AS ENDDATE\n"
                                        + "FROM BRLDTA0100.BP_STDATE A\n"
                                        + "JOIN BRLDTA0100.BP_MASTER B	\n"
                                        + "ON B.BPM_CUNO = A.BPS_CUNO\n"
                                        + "AND B.BPM_CONO = A.BPS_CONO\n"
                                        + "AND C.BPS_CUNO = A.BPS_CUNO\n"
                                        + "AND C.BPS_CONO = A.BPS_CONO\n"
                                        + "AND B.BPM_RINV = '" + invround + "'\n"
                                        + "AND B.BPM_CASEINV = '0'\n"
                                        + "AND A.BPS_CUNO = '" + customer + "'\n"
                                        + " FETCH FIRST 1 ROW ONLY)\n"
                                        + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = '" + currentmonth + "'\n"
                                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentyear + "'";
                            }
                            //15 days case
                            if (invround.equals("15")) {
                                query2 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                        + "SET C.BPS_FNDT = (SELECT CASE\n"
                                        + "	WHEN SUBSTRING(A.BPS_STDT,1,4) = '12' AND SUBSTRING(A.BPS_STDT,7,2) = '16' \n"
                                        + "	THEN (SUBSTRING(A.BPS_STDT,1,4) + 1) || '01' || '01' \n"
                                        + "	WHEN SUBSTRING(A.BPS_STDT,7,2) = '01' \n"
                                        + "	THEN SUBSTRING(A.BPS_STDT,1,4) || SUBSTRING(A.BPS_STDT,5,2) || '15' \n"
                                        + "	WHEN SUBSTRING(A.BPS_STDT,7,2) = '16'\n"
                                        + "	THEN YEAR (DATE(DAYS(DATE('2023-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                        + "	+ MONTH(DATE(DAYS(DATE('2023-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                        + "	+ DAY(DATE(DAYS(DATE('2023-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1))\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO = B.BPM_CUNO\n"
                                        + "AND A.BPS_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_RINV = 15 AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                        + "WHERE EXISTS (SELECT CASE\n"
                                        + "	WHEN SUBSTRING(A.BPS_STDT,1,4) = '12' AND SUBSTRING(A.BPS_STDT,7,2) = '16' \n"
                                        + "	THEN (SUBSTRING(A.BPS_STDT,1,4) + 1) || '01' || '01' \n"
                                        + "	WHEN SUBSTRING(A.BPS_STDT,7,2) = '01' \n"
                                        + "	THEN SUBSTRING(A.BPS_STDT,1,4) || SUBSTRING(A.BPS_STDT,5,2) || '15' \n"
                                        + "	WHEN SUBSTRING(A.BPS_STDT,7,2) = '16'\n"
                                        + "	THEN YEAR (DATE(DAYS(DATE('2023-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                        + "	+ MONTH(DATE(DAYS(DATE('2023-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                        + "	+ DAY(DATE(DAYS(DATE('2023-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1))\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO = B.BPM_CUNO\n"
                                        + "AND A.BPS_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_RINV = 15 AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                        + "AND SUBSTRING(C.BPS_STDT,5,2) = " + currentmonth + "\n"
                                        + "AND SUBSTRING(C.BPS_STDT,1,4) =" + currentyear;
                            }
//       
//30 days case
                            if (invround.equals("30")) {
                                query2 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                        + "SET C.BPS_FNDT = (SELECT YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                        + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                        + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1))\n"
                                        + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO = B.BPM_CUNO\n"
                                        + "AND B.BPM_RINV = 30\n"
                                        + "AND B.BPM_CASEINV = 0\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                        + "WHERE EXISTS (SELECT YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                        + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                        + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1))\n"
                                        + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO = B.BPM_CUNO\n"
                                        + "AND B.BPM_RINV = 30\n"
                                        + "AND B.BPM_CASEINV = 0\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                        + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = '" + currentmonth + "'\n"
                                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentyear + "'";
                            }
                            querycase1 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                    + "SET C.BPS_FNDT = (SELECT SUBSTRING(B.BPM_CASEINV,3,2) + \n"
                                    + "MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))+1 month) * 100\n"
                                    + "+ YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))+1 month) * 10000\n"
                                    + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                    + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + currentmonth + "\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + currentyear + "\n"
                                    + "AND B.BPM_CONO = A.BPS_CONO AND B.BPM_CUNO  = A.BPS_CUNO\n"
                                    + "AND SUBSTRING(B.BPM_CASEINV,1,2) = '01'\n"
                                    + "AND B.BPM_RINV = 30\n"
                                    + "AND B.BPM_CUNO = '" + customer + "'\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                    + "WHERE EXISTS (SELECT SUBSTRING(B.BPM_CASEINV,3,2) + \n"
                                    + "MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))+1 month) * 100\n"
                                    + "+ YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))+1 month) * 10000\n"
                                    + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                    + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + currentmonth + "\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + currentyear + "\n"
                                    + "AND B.BPM_CONO = A.BPS_CONO AND B.BPM_CUNO  = A.BPS_CUNO\n"
                                    + "AND SUBSTRING(B.BPM_CASEINV,1,2) = '01'\n"
                                    + "AND B.BPM_RINV = 30\n"
                                    + "AND B.BPM_CUNO = '" + customer + "'\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                    + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = '" + currentmonth + "'\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentyear + "'";
                            querycase2 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                    + "SET C.BPS_FNDT = (SELECT \n"
                                    + "YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2)) * 10000\n"
                                    + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2)) * 100\n"
                                    + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2))\n"
                                    + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                    + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + currentmonth + "\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + currentyear + "\n"
                                    + "AND B.BPM_CONO = A.BPS_CONO AND B.BPM_CUNO  = A.BPS_CUNO\n"
                                    + "AND SUBSTRING(B.BPM_CASEINV,1,2) = '2'\n"
                                    + "AND B.BPM_RINV = 30\n"
                                    + "AND A.BPS_CUNO = '" + customer + "'\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                    + "WHERE EXISTS (SELECT \n"
                                    + "YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2)) * 10000\n"
                                    + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2)) * 100\n"
                                    + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2))\n"
                                    + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                    + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + currentmonth + "\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + currentyear + "\n"
                                    + "AND B.BPM_CONO = A.BPS_CONO AND B.BPM_CUNO  = A.BPS_CUNO\n"
                                    + "AND SUBSTRING(B.BPM_CASEINV,1,2) = '2'\n"
                                    + "AND B.BPM_RINV = 30\n"
                                    + "AND A.BPS_CUNO = '" + customer + "'\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                    + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = '" + currentmonth + "'\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentyear + "'";
                            //Create period
                            query3 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                    + "SET BPS_PR = (SELECT SUBSTRING(A.BPS_STDT,3,8) || SUBSTRING(A.BPS_FNDT,3,8)\n"
                                    + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                    + "WHERE B.BPM_CONO = a.BPS_CONO AND B.BPM_CUNO = A.BPS_CUNO\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO \n"
                                    + "AND C.BPS_CUNO = A.BPS_CUNO\n"
                                    + "AND C.BPS_STDT = A.BPS_STDT \n"
                                    + "AND C.BPS_FNDT = A.BPS_FNDT\n"
                                    + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = '" + currentmonth + "  '\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentyear + "'\n"
                                    + "AND B.BPM_RINV = '" + invround + "'\n"
                                    + "AND B.BPM_CUNO = '" + customer + "'\n"
                                    + ")\n"
                                    + "WHERE EXISTS (SELECT SUBSTRING(A.BPS_STDT,3,8) || SUBSTRING(A.BPS_FNDT,3,8)\n"
                                    + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                    + "WHERE B.BPM_CONO = a.BPS_CONO AND B.BPM_CUNO = A.BPS_CUNO\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO \n"
                                    + "AND C.BPS_CUNO = A.BPS_CUNO\n"
                                    + "AND C.BPS_STDT = A.BPS_STDT \n"
                                    + "AND C.BPS_FNDT = A.BPS_FNDT\n"
                                    + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = '" + currentmonth + "'\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentyear + "'\n"
                                    + "AND B.BPM_RINV = '" + invround + "'\n"
                                    + "AND B.BPM_CUNO = '" + customer + "'\n"
                                    + ")";
                            //Create round date
                            query4 = "UPDATE BRLDTA0100.BP_STDATE T1\n"
                                    + "SET T1.BPS_RD = (SELECT COUNT(*)\n"
                                    + "FROM BRLDTA0100.BP_STDATE T2, BRLDTA0100.BP_MASTER T3\n"
                                    + "WHERE T2.BPS_CUNO=T1.BPS_CUNO\n"
                                    + "AND DATE(TIMESTAMP_FORMAT(cast( T2 .BPS_STDT as varchar(8)), 'YYYYMMDD')) <=  DATE(TIMESTAMP_FORMAT(cast( T1 .BPS_STDT as varchar(8)), 'YYYYMMDD')) \n"
                                    + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T2.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = '" + currentmonth + "'\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentyear + "'\n"
                                    + "AND T3.BPM_CONO  = T2.BPS_CONO\n"
                                    + "AND T3.BPM_CUNO = T2.BPS_CUNO\n"
                                    + "AND T1.BPS_CONO = T3.BPM_CONO\n"
                                    + "AND T1.BPS_CUNO = T2.BPS_CUNO\n"
                                    + "AND T3.BPM_RINV = '" + invround + "'\n"
                                    + "AND T2.BPS_CUNO = '" + customer + "'\n"
                                    + "GROUP BY T2.BPS_CUNO)\n"
                                    + "WHERE EXISTS(SELECT COUNT(*)\n"
                                    + "FROM BRLDTA0100.BP_STDATE T2, BRLDTA0100.BP_MASTER T3\n"
                                    + "WHERE T2.BPS_CUNO=T1.BPS_CUNO\n"
                                    + "AND DATE(TIMESTAMP_FORMAT(cast( T2 .BPS_STDT as varchar(8)), 'YYYYMMDD')) <=  DATE(TIMESTAMP_FORMAT(cast( T1 .BPS_STDT as varchar(8)), 'YYYYMMDD')) \n"
                                    + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T2.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = '" + currentmonth + "'\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentyear + "'\n"
                                    + "AND T3.BPM_CONO  = T2.BPS_CONO\n"
                                    + "AND T3.BPM_CUNO = T2.BPS_CUNO\n"
                                    + "AND T1.BPS_CONO = T3.BPM_CONO\n"
                                    + "AND T1.BPS_CUNO = T2.BPS_CUNO\n"
                                    + "AND T3.BPM_RINV = '" + invround + "'\n"
                                    + "AND T2.BPS_CUNO = '" + customer + "'\n"
                                    + "GROUP BY T2.BPS_CUNO)\n"
                                    + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T1.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = '" + currentmonth + "'\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(T1.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentyear + "'";
//                            stmt.execute(query1);
                            query1 = "INSERT INTO BRLDTA0100.BP_STDATE(BPS_CONO,BPS_CUNO,BPS_STDT,BPS_STS)\n"
                                    + "SELECT DISTINCT A.BPS_CONO,A.BPS_CUNO, YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAYS )) * 10000\n"
                                    + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD') + 1 DAY))) * 100\n"
                                    + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD') + 1 DAY)))  AS ENDDATE,'00'\n"
                                    + "FROM BRLDTA0100.BP_STDATE A\n"
                                    + "JOIN BRLDTA0100.BP_MASTER B ON A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO  = B.BPM_CUNO\n"
                                    + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY) = MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD')))\n"
                                    + "AND BPS_RD = (SELECT MAX(C.BPS_RD) FROM BRLDTA0100.BP_STDATE C,BRLDTA0100.BP_MASTER D WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(c.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentmonth + "'\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentyear + "' AND D.BPM_CONO = C.BPS_CONO AND D.BPM_CUNO = C.BPS_CUNO\n"
                                    + "--AND D.BPM_RINV = 7\n"
                                    + "AND D.BPM_CUNO = '" + customer + "'\n"
                                    + ")\n"
                                    + "AND A.BPS_CUNO = '" + customer + "'\n"
                                    + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentmonth + "'\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + currentyear + "'";
                            stmt2.execute(query2);
                            stmtcase1.execute(querycase1);
                            stmtcase2.execute(querycase2);
                            stmt3.execute(query3);
                            stmt4.execute(query4);
                            stmt5.execute(query1);
//                        currentmonth = Integer.toString(Integer.parseInt(currentmonth) + 1);
//                        if (currentmonth.equals("13")) {
//                            currentmonth = "1";
//                            currentyear = Integer.toString(parseInt(currentyear) + 1);
//                        }

                        }
                        System.out.print(query2);
                        currentmonth = Integer.toString(parseInt(currentmonth) + 1);
                        if (currentmonth.equals("13")) {
                            currentyear = Integer.toString(parseInt(currentyear) + 1);
                            currentmonth = "1";
                        }
                    }
                    respond = "Generate succesfully";
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

    public static String addHeaderMonth(String month1, String year1, String invround, String customer) throws Exception {
        Connection conn = ConnectDB2.ConnectionDB();
        String year = year1;
        String month = month1;
        String query1 = "";
        String querybillnosunday = "";
        String querybillskipsunday = "";
        String querypay0 = "";
        String query4 = "";
        String query5 = "";
        String query6 = "";
        String querycheckcases = "";
        String querypaycase1 = "";
        String querypaycase3 = "";
        String querypaycase4 = "";
        String querypaycase5 = "";
        String querypaycase6 = "";
        String querypaycase7 = "";
        String querypaycase8 = "";
        String querypaycase9 = "";
        String querypaycase10 = "";
        String querypayskipsunday = "";
        String respond = "";
        String check = "0";
        String check1 = "1";
        String highestyearquery = "";
        String highestmonthquery = "";
        String maxmonth = "";
        String maxyear = "";
        String nextmonth = Integer.toString(parseInt(month) + 1);
        String caseforbill = "";
        String caseforpay = "";

//        if ("13".equals(nextmonth)) {
//            nextmonth = "1";
//            year = Integer.toString(parseInt(year) + 1);
//        };
        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                Statement billnosunday = conn.createStatement();
                Statement billskipsunday = conn.createStatement();
                Statement pay0 = conn.createStatement();
                Statement stmt4 = conn.createStatement();
                Statement stmt5 = conn.createStatement();
                Statement stmt6 = conn.createStatement();
                Statement stmtcheckcases = conn.createStatement();
                Statement stmtcasepay1 = conn.createStatement();
                Statement stmtcasepay3 = conn.createStatement();
                Statement stmtcasepay4 = conn.createStatement();
                Statement stmtcasepay5 = conn.createStatement();
                Statement stmtcasepay6 = conn.createStatement();
                Statement stmtcasepay7 = conn.createStatement();
                Statement stmtcasepay8 = conn.createStatement();
                Statement stmtcasepay9 = conn.createStatement();
                Statement stmtcasepay10 = conn.createStatement();
                Statement stmtskipsundaypay = conn.createStatement();
                query4 = "SELECT DISTINCT LPAD((MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD')))), 2, '0')\n"
                        + "FROM BRLDTA0100.BP_HBILL A , BRLDTA0100.BP_MASTER B\n"
                        + "WHERE YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                        + "AND A.BPH_CUNO = B.BPM_CUNO \n"
                        + "AND A.BPH_CONO = B.BPM_CONO\n"
                        + "AND A.BPH_CUNO = '" + customer + "'";
                ResultSet mRes = stmt4.executeQuery(query4);
                while (mRes.next()) {
                    if (nextmonth.equals(mRes.getString(1))) {
                        respond = "data already generated";
                        check = "1";
                    }
                }
                query5 = "SELECT DISTINCT LPAD((MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD')))), 2, '0')\n"
                        + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                        + "Where YEAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                        + "AND A.BPS_CONO = B.BPM_CONO \n"
                        + "AND A.BPS_CUNO = B.BPM_CUNO\n"
                        + "AND B.BPM_CUNO = '" + customer + "'";
                ResultSet mRes2 = stmt5.executeQuery(query5);
                while (mRes2.next()) {
                    if (month.equals(mRes2.getString(1))) {
                        check1 = "0";
                    } else {
                    }
                }
                if ("1".equals(check1)) {
                    respond = "Please Generate Start Date before generate Header!";
                }

                // MAXIMUM YEAR
                Statement stmtgethighestyear = conn.createStatement();
                highestyearquery = "SELECT MAX(YEAR(DATE(TIMESTAMP_FORMAT(cast(BPH_STDT as varchar(8)), 'YYYYMMDD'))))\n"
                        + "FROM BRLDTA0100.BP_HBILL A,BRLDTA0100.BP_MASTER B-- Master DATA\n"
                        + "WHERE B.BPM_CUNO = A.BPH_CUNO \n"
                        + "AND B.BPM_RINV =" + invround;
                ResultSet maxyear1 = stmtgethighestyear.executeQuery(highestyearquery);
                while (maxyear1.next()) {
                    maxyear = maxyear1.getString(1);
                }

                querycheckcases = "SELECT BPM_CASEBIL as Bil, BPM_CASECOL as Col\n"
                        + "FROM BRLDTA0100.BP_MASTER\n"
                        + "WHERE BPM_CUNO  = '" + customer + "'";
                ResultSet cases = stmtcheckcases.executeQuery(querycheckcases);
                while (cases.next()) {
                    caseforbill = cases.getString("Bil");
                    caseforpay = cases.getString("Col");
                }
                //MAXIMUM MONTH
                Statement stmtgethighestmonth = conn.createStatement();
                highestmonthquery = "SELECT MAX(MONTH(DATE(TIMESTAMP_FORMAT(cast(BPH_STDT as varchar(8)), 'YYYYMMDD'))))\n"
                        + "FROM BRLDTA0100.BP_HBILL A,BRLDTA0100.BP_MASTER B-- Master DATA\n"
                        + "WHERE B.BPM_CUNO = A.BPH_CUNO \n"
                        + "AND YEAR(DATE(TIMESTAMP_FORMAT(cast(BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + maxyear + "\n"
                        + "AND B.BPM_RINV =" + invround;
                ResultSet maxmonth1 = stmtgethighestmonth.executeQuery(highestmonthquery);
                while (maxmonth1.next()) {
                    maxmonth = maxmonth1.getString(1);
                }

                if (check.equals("0") && check1.equals("0")) {
                    while ((Integer.parseInt(month) <= Integer.parseInt(maxmonth) || Integer.parseInt(year) != Integer.parseInt(maxyear)) && Integer.parseInt(year) <= Integer.parseInt(maxyear)) {
                        //--Import data from start date 7 days
                        query1 = "INSERT INTO BRLDTA0100.BP_HBILL(BPH_CONO,BPH_CUNO,BPH_STDT,BPH_FNDT,BPH_STS)\n"
                                + "SELECT A.BPS_CONO,A.BPS_CUNO,A.BPS_STDT,A.BPS_FNDT,A.BPS_STS\n"
                                + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                + "WHERE B.BPM_CUNO = A.BPS_CUNO\n"
                                + "--AND B.BPM_RINV = 7\n"
                                + "AND A.BPS_CUNO = '" + customer + "'\n"
                                + "AND  MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + month + "'\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + year + "'";
                        stmt.execute(query1);
                        System.out.println("insert header\n" + query1);

//                    Create billdate Case 0 without Skip sunday
                        switch (caseforbill) {
                            case "0":

                                querybillnosunday = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_BPDT = (SELECT	\n"
                                        + "YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS )) * 10000\n"
                                        + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS)) * 100\n"
                                        + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS))\n"
                                        + "FROM BRLDTA0100.BP_HBILL A\n"
                                        + "JOIN BRLDTA0100.BP_MASTER B\n"
                                        + "ON B.BPM_CUNO = A.BPH_CUNO\n"
                                        + "AND B.BPM_CONO = A.BPH_CONO\n"
                                        + "AND C.BPH_CUNO = A.BPH_CUNO\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO\n"
                                        + "--AND B.BPM_RINV = 7\n"
                                        + "AND B.BPM_CASEBIL = '0'\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + " FETCH FIRST 1 ROW ONLY)\n"
                                        + " WHERE EXISTS(SELECT	\n"
                                        + "YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS )) * 10000\n"
                                        + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS)) * 100\n"
                                        + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS))\n"
                                        + "FROM BRLDTA0100.BP_HBILL A\n"
                                        + "JOIN BRLDTA0100.BP_MASTER B\n"
                                        + "ON B.BPM_CUNO = A.BPH_CUNO\n"
                                        + "AND B.BPM_CONO = A.BPH_CONO\n"
                                        + "AND C.BPH_CUNO = A.BPH_CUNO\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO\n"
                                        + "--AND B.BPM_RINV = 7\n"
                                        + "AND B.BPM_CASEBIL = '0'\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + " FETCH FIRST 1 ROW ONLY)\n"
                                        + " AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) =" + month + "\n"
                                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                                billnosunday.execute(querybillnosunday);
                                break;
//                           Create billdate case 1
                            case "1":
                                query6 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_BPDT =(SELECT \n"
                                        + "CASE WHEN SUBSTRING(B.BPM_RBILSP,3,1) = 'C'\n"
                                        + "---------------------------------------------NORMAL---------------------------------------\n"
                                        + "THEN\n"
                                        + "---------------------------------------------------WEEK1----------------------------\n"
                                        + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%1%'\n"
                                        + "THEN \n"
                                        + "------------------------------------------------RESULT OF WEEK1--------------------------------------\n"
                                        + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS))\n"
                                        + "-------------------------------------------WEEK2----------------------------------------------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%2%'\n"
                                        + "---------------------------------------------------------RESULT WEEK2----------------------------------------------------------\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS))\n"
                                        + "---------------------------------------------------------------------------WEEK3-------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%3%'\n"
                                        + "-------------------------------------------------------------------------RESULT OF WEEK3--------------------------------\n"
                                        + "THEN \n"
                                        + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS))\n"
                                        + "--------------------------------------------------------------------------WEEK 4-----------------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%4%'\n"
                                        + "-------------------------------------------------------------------RESULT WEEK4------------------------------------------------------\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS))\n"
                                        + "ELSE \n"
                                        + " DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS)) DAYS))\n"
                                        + "END\n"
                                        + "WHEN SUBSTRING(B.BPM_RBILSP ,3,1) = 'R'\n"
                                        + "---------------------------SPECIAL CHANGE VALUE IF CASE HAVE 5 WEEKS ON SPECIFIC DATE------\n"
                                        + "THEN \n"
                                        + "CASE WHEN SUBSTRING(BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(BPH_FNDT,1,4)||'-'|| SUBSTRING(BPH_FNDT,5,2)||'-'||'28')) >= 0 AND\n"
                                        + "DAYOFWEEK(DATE(SUBSTRING(BPH_FNDT,1,4)||'-'|| SUBSTRING(BPH_FNDT,5,2)||'-'||'28')) -- DAY OF START OF WEEK5\n"
                                        + " <= DAYS(LAST_DAY(DATE(SUBSTRING(BPH_FNDT,1,4)||'-'|| SUBSTRING(BPH_FNDT,5,2)||'-'||SUBSTRING(BPH_FNDT,7,2))))\n"
                                        + "-DAYS(DATE(SUBSTRING(BPH_FNDT,1,4)||'-'|| SUBSTRING(BPH_FNDT,5,2)||'-'||'01'))-27  -- DAY LEFT IN MONTH\n"
                                        + "THEN \n"
                                        + "---------------------------------------------------WEEK1----------------------------\n"
                                        + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%1%'\n"
                                        + "----------------------------------!!--RESULT OF WEEK 1-!!--------------------\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS))\n"
                                        + "--------------------------------------------------WEEK2---------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))  LIKE '%2%'\n"
                                        + "----------------------------------!!--RESULT OF WEEK 2-!!--------------------\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS))\n"
                                        + "--------------------------------------------------WEEK3---------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%3%'\n"
                                        + "----------------------------------!!--RESULT OF WEEK 3-!!--------------------\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS))\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%4%'\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS))\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%5%'\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS)) DAYS))\n"
                                        + "ELSE DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))-1) * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))-1) * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))-1) * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))-1) * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))-1) * 7) DAYS)) DAYS))\n"
                                        + "END\n"
                                        + "--END OF SECOND PART\n"
                                        + "ELSE \n"
                                        + "---------------------------------------------------WEEK1----------------------------\n"
                                        + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,4,LENGTH(BPM_RBILSP)-LOCATE('OR', BPM_RBILSP)) LIKE '%1%'\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS))\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,4,LENGTH(BPM_RBILSP)-LOCATE('OR', BPM_RBILSP)) LIKE '%2%'\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS))\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,4,LENGTH(BPM_RBILSP)-LOCATE('OR', BPM_RBILSP)) LIKE '%3%'\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS))\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                                        + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS\n"
                                        + "AND SUBSTRING(BPM_RBILSP,4,LENGTH(BPM_RBILSP)-LOCATE('OR', BPM_RBILSP)) LIKE '%4%'\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS))\n"
                                        + "ELSE DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS \n"
                                        + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS)) DAYS))\n"
                                        + "END\n"
                                        + "--END OF FIRST PART\n"
                                        + "END\n"
                                        + "WHEN SUBSTRING(B.BPM_RBILSP ,3,1) = 'N'\n"
                                        + "---------------------------DO NOT COUNT FIRST WEEK IF NOT FULL OR START ON SUNDAY------\n"
                                        + "THEN 'SUNDAY!!'\n"
                                        + "END AS TESTWEEK\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE B.BPM_CUNO = A.BPH_CUNO AND B.BPM_CONO = A.BPH_CONO \n"
                                        + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT\n"
                                        + "AND B.BPM_CASEBIL = '1'\n"
                                        + "AND A.BPH_CUNO = '" + customer + "'\n"
                                        + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + month + "'\n"
                                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT AS varchar(8)), 'YYYYMMDD'))) = '" + year + "'\n"
                                        + ")\n"
                                        + "WHERE EXISTS (SELECT A.BPH_CUNO,A.BPH_STDT,A.BPH_FNDT--,B.BPM_RBILSP\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE B.BPM_CUNO = A.BPH_CUNO AND B.BPM_CONO = A.BPH_CONO \n"
                                        + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT\n"
                                        + "AND B.BPM_CASEBIL = '1'\n"
                                        + "AND A.BPH_CUNO = '" + customer + "'\n"
                                        + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + month + "'\n"
                                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT AS varchar(8)), 'YYYYMMDD'))) = '" + year + "')\n"
                                        + "AND C.BPH_CUNO = '" + customer + "'\n"
                                        + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + month + "'\n"
                                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT AS varchar(8)), 'YYYYMMDD'))) = '" + year + "'";
                                stmt6.execute(query6);
                                break;
                        }
                        switch (caseforpay) {
                            case "0":
                                //Create Pay date case 0
                                querypay0 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_CLDT= (SELECT\n"
                                        + "YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS )) * 10000\n"
                                        + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS)) * 100\n"
                                        + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS))\n"
                                        + "FROM BRLDTA0100.BP_HBILL A\n"
                                        + "JOIN BRLDTA0100.BP_MASTER B\n"
                                        + "ON B.BPM_CUNO = A.BPH_CUNO\n"
                                        + "AND B.BPM_CONO = A.BPH_CONO\n"
                                        + "--AND B.BPM_RINV = 30\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL  = '0'\n"
                                        + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + " WHERE  EXISTS (SELECT\n"
                                        + "YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS )) * 10000\n"
                                        + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS)) * 100\n"
                                        + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS))\n"
                                        + "FROM BRLDTA0100.BP_HBILL A\n"
                                        + "JOIN BRLDTA0100.BP_MASTER B\n"
                                        + "ON B.BPM_CUNO = A.BPH_CUNO\n"
                                        + "AND B.BPM_CONO = A.BPH_CONO\n"
                                        + "--AND B.BPM_RINV = 30\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL  = '0'\n"
                                        + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "AND SUBSTRING(C.BPH_STDT,5,2) = " + month + "\n"
                                        + "AND SUBSTRING(C.BPH_STDT,1,4) = " + year;
                                pay0.execute(querypay0);
                                break;
                            case "1":
//Create pay case 1
                                querypaycase1 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_CLDT = (SELECT CASE WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'C'\n"
                                        + "---------------------------------------------NORMAL---------------------------------------\n"
                                        + "THEN\n"
                                        + "---------------------------------------------------WEEK1----------------------------\n"
                                        + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%1%'\n"
                                        + "THEN \n"
                                        + "------------------------------------------------RESULT OF WEEK1--------------------------------------\n"
                                        + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS+ 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS+ 1 MONTHS)) DAYS))\n"
                                        + "-------------------------------------------WEEK2----------------------------------------------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%2%'\n"
                                        + "---------------------------------------------------------RESULT WEEK2----------------------------------------------------------\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                                        + "---------------------------------------------------------------------------WEEK3-------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%3%'\n"
                                        + "-------------------------------------------------------------------------RESULT OF WEEK3--------------------------------\n"
                                        + "THEN \n"
                                        + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                                        + "--------------------------------------------------------------------------WEEK 4-----------------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%4%'\n"
                                        + "-------------------------------------------------------------------RESULT WEEK4------------------------------------------------------\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                                        + "ELSE \n"
                                        + " DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7 ) DAYS)) DAYS)) * 10000\n"
                                        + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS))\n"
                                        + "END\n"
                                        + "WHEN SUBSTRING(B.BPM_RCOLSP ,3,1) = 'N'\n"
                                        + "---------------------------SPECIAL CHANGE VALUE IF CASE HAVE 5 WEEKS ON SPECIFIC DATE------\n"
                                        + "THEN \n"
                                        + "CASE WHEN DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01')) = 1\n"
                                        + "THEN\n"
                                        + "---------------------------------------------------WEEK1----------------------------\n"
                                        + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%1%'\n"
                                        + "THEN \n"
                                        + "------------------------------------------------RESULT OF WEEK1--------------------------------------\n"
                                        + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                                        + "-------------------------------------------WEEK2----------------------------------------------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%2%'\n"
                                        + "---------------------------------------------------------RESULT WEEK2----------------------------------------------------------\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                                        + "---------------------------------------------------------------------------WEEK3-------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%3%'\n"
                                        + "-------------------------------------------------------------------------RESULT OF WEEK3--------------------------------\n"
                                        + "THEN \n"
                                        + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                                        + "--------------------------------------------------------------------------WEEK 4-----------------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%4%'\n"
                                        + "-------------------------------------------------------------------RESULT WEEK4------------------------------------------------------\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                                        + "ELSE \n"
                                        + " DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS))\n"
                                        + "END\n"
                                        + "--END OF SECOND PART\n"
                                        + "ELSE \n"
                                        + "---------------------------------------------------WEEK1----------------------------\n"
                                        + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%1%'\n"
                                        + "THEN \n"
                                        + "------------------------------------------------RESULT OF WEEK1--------------------------------------\n"
                                        + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                                        + "-------------------------------------------WEEK2----------------------------------------------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%2%'\n"
                                        + "---------------------------------------------------------RESULT WEEK2----------------------------------------------------------\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                                        + "---------------------------------------------------------------------------WEEK3-------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%3%'\n"
                                        + "-------------------------------------------------------------------------RESULT OF WEEK3--------------------------------\n"
                                        + "THEN \n"
                                        + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                                        + "--------------------------------------------------------------------------WEEK 4-----------------------------------------------------\n"
                                        + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                                        + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS)) DAYS\n"
                                        + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%4%'\n"
                                        + "-------------------------------------------------------------------RESULT WEEK4------------------------------------------------------\n"
                                        + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                                        + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                                        + "ELSE \n"
                                        + " DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS)) * 10000\n"
                                        + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS  + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS)) * 100\n"
                                        + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                                        + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS))\n"
                                        + "END\n"
                                        + "--END OF FIRST PART\n"
                                        + "END\n"
                                        + "END AS TESTWEEK\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE B.BPM_CUNO = A.BPH_CUNO AND B.BPM_CONO = A.BPH_CONO \n"
                                        + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT\n"
                                        + "AND B.BPM_CASECOl = '1'\n"
                                        + "AND A.BPH_CUNO = '" + customer + "'\n"
                                        + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + month + "'\n"
                                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT AS varchar(8)), 'YYYYMMDD'))) = '" + year + "'\n"
                                        + ")\n"
                                        + "WHERE EXISTS (SELECT A.BPH_CUNO,A.BPH_STDT,A.BPH_FNDT,A.BPH_BPDT --,B.BPM_RBILSP\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE B.BPM_CUNO = A.BPH_CUNO AND B.BPM_CONO = A.BPH_CONO \n"
                                        + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT\n"
                                        + "AND B.BPM_CASECOL  = '1'\n"
                                        + "AND A.BPH_CUNO = '" + customer + "'\n"
                                        + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + month + "'\n"
                                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT AS varchar(8)), 'YYYYMMDD'))) = '" + year + "'\n"
                                        + ")\n"
                                        + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + month + "'\n"
                                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT as varchar(8)), 'YYYYMMDD'))) = '" + year + "'\n"
                                        + "AND C.BPH_CUNO = '" + customer + "'";
                                stmtcasepay1.execute(querypaycase1);
                                break;
                            case "3":
                                //casepay3
                                querypaycase3 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_CLDT = (SELECT CASE \n"
                                        + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) = '12'\n"
                                        + "	THEN (SUBSTRING(A.BPH_BPDT,0,5) + 1) || '01' || B.BPM_RCOLSP\n"
                                        + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) >= '09'\n"
                                        + "	THEN SUBSTRING(A.BPH_BPDT,0,5) || (SUBSTRING(A.BPH_BPDT,5,2) + 1) || B.BPM_RCOLSP\n"
                                        + "	ELSE SUBSTRING(A.BPH_BPDT,0,5) ||'0' || (SUBSTRING(A.BPH_BPDT,5,2) + 1) || B.BPM_RCOLSP\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                                        + "AND B.BPM_CASECOL = '3'\n"
                                        + "AND SUBSTRING(A.BPH_STDT,5,2) = " + month + "\n"
                                        + "AND SUBSTRING(A.BPH_STDT,1,4) = " + year + "\n"
                                        + "AND B.BPM_CUNO  = '" + customer + "'\n"
                                        + "AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_CONO = C.BPH_CONO  AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT  = C.BPH_FNDT AND A.BPH_BPDT = C.BPH_BPDT )\n"
                                        + "WHERE EXISTS(SELECT CASE \n"
                                        + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) = '12'\n"
                                        + "	THEN (SUBSTRING(A.BPH_BPDT,0,5) + 1) || '01' || B.BPM_RCOLSP\n"
                                        + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) >= '09'\n"
                                        + "	THEN SUBSTRING(A.BPH_BPDT,0,5) || (SUBSTRING(A.BPH_BPDT,5,2) + 1) || B.BPM_RCOLSP\n"
                                        + "	ELSE SUBSTRING(A.BPH_BPDT,0,5) ||'0' || (SUBSTRING(A.BPH_BPDT,5,2) + 1) || B.BPM_RCOLSP\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                                        + "AND B.BPM_CASECOL = '3'\n"
                                        + "AND SUBSTRING(A.BPH_STDT,5,2) =" + month + "\n"
                                        + "AND SUBSTRING(A.BPH_STDT,1,4) =" + year + "\n"
                                        + "AND B.BPM_CUNO  = '" + customer + "'\n"
                                        + "AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_CONO = C.BPH_CONO  AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT  = C.BPH_FNDT AND A.BPH_BPDT = C.BPH_BPDT )\n"
                                        + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) =" + month + "\n"
                                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                                stmtcasepay3.execute(querypaycase3);
                                break;
                            case "4":
//case for paycase 4
                                querypaycase4 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_CLDT= (SELECT \n"
                                        + "CASE\n"
                                        + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'N'\n"
                                        + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'Y' AND SUBSTRING(A.BPH_BPDT,5,2) = '12' \n"
                                        + "	THEN YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2))+ 1 MONTHS) *10000+\n"
                                        + "	MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) +1 MONTHS)*100 +\n"
                                        + "	DAY(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS)\n"
                                        + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'Y' \n"
                                        + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'C' AND (SUBSTRING(A.BPH_BPDT,5,2) = '12' OR SUBSTRING(A.BPH_BPDT,5,2) = '11')\n"
                                        + "	THEN YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2))+ 2 MONTHS) *10000+\n"
                                        + "	MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) +2 MONTHS)*100 +\n"
                                        + "	DAY(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) + 2 MONTHS)\n"
                                        + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'C' \n"
                                        + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 2), 2, '0') || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                                        + "AND B.BPM_CASECOL = '4'\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND A.BPH_CONO = C.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "WHERE EXISTS(SELECT \n"
                                        + "CASE\n"
                                        + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'N'\n"
                                        + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'Y' AND SUBSTRING(A.BPH_BPDT,5,2) = '12' \n"
                                        + "	THEN YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2))+ 1 MONTHS) *10000+\n"
                                        + "	MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) +1 MONTHS)*100 +\n"
                                        + "	DAY(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS)\n"
                                        + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'Y' \n"
                                        + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'C' AND (SUBSTRING(A.BPH_BPDT,5,2) = '12' OR SUBSTRING(A.BPH_BPDT,5,2) = '11')\n"
                                        + "	THEN YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2))+ 2 MONTHS) *10000+\n"
                                        + "	MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) +2 MONTHS)*100 +\n"
                                        + "	DAY(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) + 2 MONTHS)\n"
                                        + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'C' \n"
                                        + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 2), 2, '0') || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                                        + "AND B.BPM_CASECOL = '4'\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND A.BPH_CONO = C.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + " AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + month + "'\n"
                                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                                stmtcasepay4.execute(querypaycase4);
                                break;
                            case "5":
//case for paycase 5
                                querypaycase5 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_CLDT = (SELECT CASE WHEN SUBSTRING(B.BPM_RCOLSP,1,2) < SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                                        + "THEN\n"
                                        + "CASE\n"
                                        + "     -- iF THE DATE IS LESS THAN THE FIRST GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                                        + "WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	-- IF THE DATE GIVEN IS LESS THAN THE SECOND GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                                        + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                                        + "	 --IF THE RESULT GONNA BE NEXT YEAR AND THE RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||(SUBSTRING(BPH_BPDT,5,2) + 1)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12' \n"
                                        + "	 THEN  (SUBSTRING(BPH_BPDT,1,4) + 1) ||'01'||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	 --IF THE RESULT END UP BEING NEXT MONTH AND RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	 THEN  SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "END\n"
                                        + "WHEN  SUBSTRING(B.BPM_RCOLSP,3,2) < SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "THEN CASE\n"
                                        + "     -- iF THE DATE IS LESS THAN THE FIRST GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                                        + "WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2)) + 1 MONTHS <\n"
                                        + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS\n"
                                        + "	 THEN YEAR (DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 10000\n"
                                        + "	 +MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 100\n"
                                        + "	 +DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 1\n"
                                        + "	-- IF THE DATE GIVEN IS LESS THAN THE SECOND GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2)) + 1 MONTHS <\n"
                                        + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS\n"
                                        + "	 THEN YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS) * 10000\n"
                                        + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS) * 100\n"
                                        + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS)\n"
                                        + "	 --IF THE RESULT GONNA BE NEXT YEAR AND THE RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2))+ 1 MONTHS <\n"
                                        + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||(SUBSTRING(BPH_BPDT,5,2))||'-'||SUBSTRING(B.BPM_RCOLSP,3,2))+ 1 MONTHS\n"
                                        + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12' \n"
                                        + "	 THEN YEAR(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                                        + "	 + MONTH(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                                        + "	  + DAY(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                                        + "	 --IF THE RESULT END UP BEING NEXT MONTH AND RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2))+ 1 MONTHS <\n"
                                        + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS\n"
                                        + "	 THEN  YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                                        + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                                        + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                                        + "	 ELSE  YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                                        + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                                        + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                                        + "END\n"
                                        + "END AS TEST \n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL = '5'\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT  AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "WHERE EXISTS (SELECT CASE WHEN SUBSTRING(B.BPM_RCOLSP,1,2) < SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                                        + "THEN\n"
                                        + "CASE\n"
                                        + "     -- iF THE DATE IS LESS THAN THE FIRST GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                                        + "WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	-- IF THE DATE GIVEN IS LESS THAN THE SECOND GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                                        + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                                        + "	 --IF THE RESULT GONNA BE NEXT YEAR AND THE RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||(SUBSTRING(BPH_BPDT,5,2) + 1)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12' \n"
                                        + "	 THEN  (SUBSTRING(BPH_BPDT,1,4) + 1) ||'01'||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	 --IF THE RESULT END UP BEING NEXT MONTH AND RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	 THEN  SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "END\n"
                                        + "WHEN  SUBSTRING(B.BPM_RCOLSP,3,2) < SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "THEN CASE\n"
                                        + "     -- iF THE DATE IS LESS THAN THE FIRST GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                                        + "WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2)) + 1 MONTHS <\n"
                                        + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS\n"
                                        + "	 THEN YEAR (DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 10000\n"
                                        + "	 +MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 100\n"
                                        + "	 +DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 1\n"
                                        + "	-- IF THE DATE GIVEN IS LESS THAN THE SECOND GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2)) + 1 MONTHS <\n"
                                        + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS\n"
                                        + "	 THEN YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS) * 10000\n"
                                        + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS) * 100\n"
                                        + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS)\n"
                                        + "	 --IF THE RESULT GONNA BE NEXT YEAR AND THE RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2))+ 1 MONTHS <\n"
                                        + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||(SUBSTRING(BPH_BPDT,5,2))||'-'||SUBSTRING(B.BPM_RCOLSP,3,2))+ 1 MONTHS\n"
                                        + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12' \n"
                                        + "	 THEN YEAR(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                                        + "	 + MONTH(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                                        + "	  + DAY(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                                        + "	 --IF THE RESULT END UP BEING NEXT MONTH AND RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2))+ 1 MONTHS <\n"
                                        + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS\n"
                                        + "	 THEN  YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                                        + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                                        + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                                        + "	 ELSE  YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                                        + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                                        + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                                        + "END\n"
                                        + "END AS TEST \n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL = '5'\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT  AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "AND SUBSTRING(C.BPH_STDT,1,4) = '" + year + "'\n"
                                        + "AND SUBSTRING(C.BPH_STDT,5,2) =" + month;
                                stmtcasepay5.execute(querypaycase5);
                                break;
                            case "6":
//case for paycase 6
                                querypaycase6 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_CLDT = (SELECT CASE \n"
                                        + "	--CASE WHEN IT MATCHED WITH THE FIRST NUMBER AND NEXT MONTH\n"
                                        + "	WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	AND SUBSTRING(B.BPM_RCOLSP,5,1) = 'Y'\n"
                                        + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 10000\n"
                                        + "	 + MONTH(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 100\n"
                                        + "	  + DAY(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)    as varchar(8)), 'YYYYMMDD')) + 1 MONTH)\n"
                                        + "	  WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	AND SUBSTRING(B.BPM_RCOLSP,5,1) = 'N'\n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                                        + "	WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,6,2)\n"
                                        + "	AND SUBSTRING(B.BPM_RCOLSP,10,1) = 'Y'\n"
                                        + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 10000\n"
                                        + "	 + MONTH(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 100\n"
                                        + "	  + DAY(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH)\n"
                                        + "	  WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,6,2)\n"
                                        + "	AND SUBSTRING(B.BPM_RCOLSP,10,1) = 'N'\n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL = '6'\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT  AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "WHERE EXISTS (SELECT CASE \n"
                                        + "	--CASE WHEN IT MATCHED WITH THE FIRST NUMBER AND NEXT MONTH\n"
                                        + "	WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	AND SUBSTRING(B.BPM_RCOLSP,5,1) = 'Y'\n"
                                        + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 10000\n"
                                        + "	 + MONTH(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 100\n"
                                        + "	  + DAY(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)    as varchar(8)), 'YYYYMMDD')) + 1 MONTH)\n"
                                        + "	  WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                                        + "	AND SUBSTRING(B.BPM_RCOLSP,5,1) = 'N'\n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                                        + "	WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,6,2)\n"
                                        + "	AND SUBSTRING(B.BPM_RCOLSP,10,1) = 'Y'\n"
                                        + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 10000\n"
                                        + "	 + MONTH(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 100\n"
                                        + "	  + DAY(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH)\n"
                                        + "	  WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,6,2)\n"
                                        + "	AND SUBSTRING(B.BPM_RCOLSP,10,1) = 'N'\n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL = '6'\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT  AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                                        + "AND SUBSTRING(C.BPH_STDT,5,2) =" + month;
                                stmtcasepay6.execute(querypaycase6);
                                break;
                            case "7":
                                //case for paycase 7
                                querypaycase7 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_CLDT = (SELECT CASE \n"
                                        + "	--NEW YEAR CASE AND NOT SUNDAY\n"
                                        + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) = 12\n"
                                        + "	THEN (SUBSTRING(A.BPH_BPDT,1,4) + 1) ||'01' || SUBSTRING(A.BPH_BPDT,7,2)\n"
                                        + "	-- OTHER CASE AND NOT SUNDAY\n"
                                        + "	WHEN  SUBSTRING(A.BPH_BPDT,5,2) <> 12 \n"
                                        + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(A.BPH_BPDT,7,2)\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL = 7\n"
                                        + "AND A.BPH_CONO = C.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT )\n"
                                        + "WHERE EXISTS (SELECT CASE \n"
                                        + "	--NEW YEAR CASE AND NOT SUNDAY\n"
                                        + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) = 12\n"
                                        + "	THEN (SUBSTRING(A.BPH_BPDT,1,4) + 1) ||'01' || SUBSTRING(A.BPH_BPDT,7,2)\n"
                                        + "	-- OTHER CASE AND NOT SUNDAY\n"
                                        + "	WHEN  SUBSTRING(A.BPH_BPDT,5,2) <> 12 \n"
                                        + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(A.BPH_BPDT,7,2)\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL = 7\n"
                                        + "AND A.BPH_CONO = C.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT )\n"
                                        + "AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                                        + "AND SUBSTRING(C.BPH_STDT,5,2) =" + month;
                                stmtcasepay7.execute(querypaycase7);
                                break;
                            case "8":
                                //case for pay case8
                                querypaycase8 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_CLDT = (SELECT CASE\n"
                                        + "	-- First half of the month AND RESULT IS NOT SUNDAY\n"
                                        + "WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||'15'\n"
                                        + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||B.BPM_RCOLSP\n"
                                        + "	 -- Last day of the month AND RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||\n"
                                        + "	 DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                                        + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)|| \n"
                                        + "	 DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                                        + "	 -- WHEN LAST DAY OF THE MONTH OF DECEMBER SO NEW YEAR AND NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                                        + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12'\n"
                                        + "	 THEN  (SUBSTRING(BPH_BPDT,1,4) + 1) || '01'||B.BPM_RCOLSP\n"
                                        + "	 -- WHEN NEXT HALF NEXT MONTH IS ANSWER IS NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||B.BPM_RCOLSP\n"
                                        + "	 THEN  SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||B.BPM_RCOLSP\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL = '8'\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "WHERE EXISTS (SELECT CASE\n"
                                        + "	-- First half of the month AND RESULT IS NOT SUNDAY\n"
                                        + "WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||'15'\n"
                                        + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||B.BPM_RCOLSP\n"
                                        + "	 -- Last day of the month AND RESULT IS NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||\n"
                                        + "	 DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                                        + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)|| \n"
                                        + "	 DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                                        + "	 -- WHEN LAST DAY OF THE MONTH OF DECEMBER SO NEW YEAR AND NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                                        + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12'\n"
                                        + "	 THEN  (SUBSTRING(BPH_BPDT,1,4) + 1) || '01'||B.BPM_RCOLSP\n"
                                        + "	 -- WHEN NEXT HALF NEXT MONTH IS ANSWER IS NOT SUNDAY\n"
                                        + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                                        + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||B.BPM_RCOLSP\n"
                                        + "	 THEN  SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||B.BPM_RCOLSP\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL = '8'\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                                        + "AND SUBSTRING(C.BPH_STDT,5,2) =" + month;
                                stmtcasepay8.execute(querypaycase8);
                                break;
                            case "9":
                                //pay case 9
                                querypaycase9 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_CLDT = (SELECT CASE\n"
                                        + "	-- IF THE DATE GIVEN IS LESS THAN SECOND PERIOD BUT IN THE FIRST PEROID AND NOT SUNDAY\n"
                                        + "	WHEN SUBSTRING(BPH_BPDT,7,2) >= SUBSTRING(BPM_RCOLSP,1,2) AND SUBSTRING(BPH_BPDT,7,2) <= SUBSTRING(BPM_RCOLSP,3,2) AND SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,5,2)\n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,5,2)\n"
                                        + "	--IF the DATE GIVEN IS LESS THAN THE FIRST PERIOD AND NOT SUNDAY\n"
                                        + "	WHEN SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,1,2) \n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,1,2)\n"
                                        + "	--IF THE DATE GIVEN IS IN BETWEEN FIRST AND SECOND PERIOD AND IS NOT SUNDAY\n"
                                        + "	WHEN SUBSTRING(BPH_BPDT,7,2) > SUBSTRING(BPM_RCOLSP,3,2) AND SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,5,2)\n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,5,2)\n"
                                        + "	--IF THE DATE GIVEN IS IN LAST PERIOD OR THE DATE GIVEN IS LATER OF THE PERIOD AND IS NOT SUNDAY\n"
                                        + "	WHEN ((SUBSTRING(BPH_BPDT,7,2) >=  SUBSTRING(BPM_RCOLSP,5,2) AND SUBSTRING(BPH_BPDT,7,2) <= SUBSTRING(BPM_RCOLSP,7,2) ) OR\n"
                                        + "	SUBSTRING(BPH_BPDT,7,2) > SUBSTRING(BPM_RCOLSP,7,2))\n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)|| LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(BPM_RCOLSP,1,2)\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL = 9\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "WHERE EXISTS (SELECT CASE\n"
                                        + "	-- IF THE DATE GIVEN IS LESS THAN SECOND PERIOD BUT IN THE FIRST PEROID AND NOT SUNDAY\n"
                                        + "	WHEN SUBSTRING(BPH_BPDT,7,2) >= SUBSTRING(BPM_RCOLSP,1,2) AND SUBSTRING(BPH_BPDT,7,2) <= SUBSTRING(BPM_RCOLSP,3,2) AND SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,5,2)\n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,5,2)\n"
                                        + "	--IF the DATE GIVEN IS LESS THAN THE FIRST PERIOD AND NOT SUNDAY\n"
                                        + "	WHEN SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,1,2) \n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,1,2)\n"
                                        + "	--IF THE DATE GIVEN IS IN BETWEEN FIRST AND SECOND PERIOD AND IS NOT SUNDAY\n"
                                        + "	WHEN SUBSTRING(BPH_BPDT,7,2) > SUBSTRING(BPM_RCOLSP,3,2) AND SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,5,2)\n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,5,2)\n"
                                        + "	--IF THE DATE GIVEN IS IN LAST PERIOD OR THE DATE GIVEN IS LATER OF THE PERIOD AND IS NOT SUNDAY\n"
                                        + "	WHEN ((SUBSTRING(BPH_BPDT,7,2) >=  SUBSTRING(BPM_RCOLSP,5,2) AND SUBSTRING(BPH_BPDT,7,2) <= SUBSTRING(BPM_RCOLSP,7,2) ) OR\n"
                                        + "	SUBSTRING(BPH_BPDT,7,2) > SUBSTRING(BPM_RCOLSP,7,2))\n"
                                        + "	THEN SUBSTRING(BPH_BPDT,1,4)|| LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(BPM_RCOLSP,1,2)\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND B.BPM_CASECOL = 9\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                                        + "AND SUBSTRING(C.BPH_STDT,5,2) =" + month;
                                stmtcasepay9.execute(querypaycase9);
                                break;
//paycase 10
                            case "10":
                                querypaycase10 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                        + "SET C.BPH_CLDT = (SELECT CASE\n"
                                        + "	WHEN B.BPM_RCOLSP = 'N'\n"
                                        + "	THEN\n"
                                        + "	YEAR (DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                        + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                        + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1))\n"
                                        + "	WHEN B.BPM_RCOLSP = 'Y'\n"
                                        + "	THEN \n"
                                        + "	YEAR (DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1)) * 10000\n"
                                        + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1)) * 100\n"
                                        + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1))\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A,BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO  = B.BPM_CONO AND A.BPH_CUNO  = B.BPM_CUNO\n"
                                        + "AND B.BPM_RINV = 30\n"
                                        + "AND B.BPM_CASECOL = '10'\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "WHERE EXISTS (SELECT CASE\n"
                                        + "	WHEN B.BPM_RCOLSP = 'N'\n"
                                        + "	THEN\n"
                                        + "	YEAR (DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                        + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                        + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1))\n"
                                        + "	WHEN B.BPM_RCOLSP = 'Y'\n"
                                        + "	THEN \n"
                                        + "	YEAR (DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1)) * 10000\n"
                                        + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1)) * 100\n"
                                        + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1))\n"
                                        + "END\n"
                                        + "FROM BRLDTA0100.BP_HBILL A,BRLDTA0100.BP_MASTER B\n"
                                        + "WHERE A.BPH_CONO  = B.BPM_CONO AND A.BPH_CUNO  = B.BPM_CUNO\n"
                                        + "AND B.BPM_RINV = 30\n"
                                        + "AND B.BPM_CASECOL = '10'\n"
                                        + "AND B.BPM_CUNO = '" + customer + "'\n"
                                        + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                        + "AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                                        + "AND SUBSTRING(C.BPH_STDT,5,2) =" + month + "";
                                stmtcasepay10.execute(querypaycase10);
                                break;
                        }

                        querypayskipsunday = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                + "SET C.BPH_CLDT = (SELECT CASE\n"
                                + "	WHEN DAYOFWEEK(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) = 1\n"
                                + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')) + 1 DAY)* 10000\n"
                                + "	+ MONTH (DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)* 100\n"
                                + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)\n"
                                + "	ELSE YEAR(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) * 10000\n"
                                + "	+ MONTH(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')))* 100\n"
                                + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')))\n"
                                + "END\n"
                                + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                                + "AND DAYOFWEEK( DATE(TIMESTAMP_FORMAT(cast(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) = 1\n"
                                + "AND B.BPM_CUNO = '" + customer + "'\n"
                                + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                + "WHERE EXISTS (SELECT CASE\n"
                                + "	WHEN DAYOFWEEK(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) = 1\n"
                                + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')) + 1 DAY)* 10000\n"
                                + "	+ MONTH (DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)* 100\n"
                                + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)\n"
                                + "	ELSE YEAR(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) * 10000\n"
                                + "	+ MONTH(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')))* 100\n"
                                + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')))\n"
                                + "END\n"
                                + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                                + "AND DAYOFWEEK( DATE(TIMESTAMP_FORMAT(cast(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) = 1\n"
                                + "AND B.BPM_CUNO = '" + customer + "'\n"
                                + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                + "AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                                + "AND SUBSTRING(C.BPH_STDT,5,2) =" + month;
                        stmtskipsundaypay.execute(querypayskipsunday);
                        querybillskipsunday = "UPDATE BRLDTA0100.BP_HBILL C\n"
                                + "SET C.BPH_BPDT = (SELECT CASE\n"
                                + "	WHEN DAYOFWEEK(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))) = 1\n"
                                + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 DAY)* 10000\n"
                                + "	+ MONTH (DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)* 100\n"
                                + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)\n"
                                + "	ELSE YEAR(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))) * 10000\n"
                                + "	+ MONTH(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD')))* 100\n"
                                + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD')))\n"
                                + "END\n"
                                + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                                + "AND B.BPM_CUNO = '" + customer + "'\n"
                                + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                + "WHERE EXISTS (SELECT CASE\n"
                                + "	WHEN DAYOFWEEK(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))) = 1\n"
                                + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 DAY)* 10000\n"
                                + "	+ MONTH (DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)* 100\n"
                                + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)\n"
                                + "	ELSE YEAR(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))) * 10000\n"
                                + "	+ MONTH(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD')))* 100\n"
                                + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(C.BPH_BPDT as varchar(8)), 'YYYYMMDD')))\n"
                                + "END\n"
                                + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                                + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                                + "AND B.BPM_CUNO = '" + customer + "'\n"
                                + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                                + "--AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                                + "--AND SUBSTRING(C.BPH_STDT,5,2) =" + month;
                        billskipsunday.execute(querybillskipsunday);

                        month = Integer.toString(parseInt(month) + 1);
                        if (month.equals("13")) {
                            year = Integer.toString(parseInt(year) + 1);
                            month = "01";
                        }
                    }
                    respond = "generate successfully!";
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

    public static String addMaster(String company, String customer,
            String invroundinput, String invroundA, String type, String bill, String bilweek1, String bilinput1, String bilinput3,
            String bilfirstweek, String pay, String pay5weekornot, String payweek1, String payweek2, String payday1, String payday2,
            String payday3, String payday4, String paydaysamemonthornot) throws Exception {
        Connection conn = ConnectDB2.ConnectionDB();
        String query1 = "";
        String query4 = "";
        String respond = "";
        String check = "0";
        String Invroundspecial = "0";
        String bilspecial = "0";
        String payspecial = "0";

        if (invroundinput.equals("")) {
            invroundinput = "0";
        }
        if (bill.equals("")) {
            bill = "0";
        }
        if (pay.equals("")) {
            pay = "0";
        }

        if (invroundinput.equals("1")) {
            invroundA = String.format("%02d", Integer.parseInt(invroundA));
            Invroundspecial = "01" + invroundA;
            invroundinput = "30";
        } else if (invroundinput.equals("2")) {
            Invroundspecial = "2";
            invroundinput = "30";
        }

        if (bill.equals("1")) {
            if (bilfirstweek.equals("C")) {
                bilspecial = bilweek1 + "N" + bilfirstweek + bilinput1;
                bilinput1 = "0";
            } else if (bilfirstweek.equals("R")) {
                bilspecial = bilweek1 + "N" + bilfirstweek + bilinput1 + "OR" + bilinput3;
            }
        }

        if (pay.equals("1")) {
            if (pay5weekornot.equals("N")) {
                payspecial = payweek1 + "N" + pay5weekornot + payday1;
            } else if (pay5weekornot.equals("C")) {
                payspecial = payweek1 + "N" + pay5weekornot + payday1;
            }
            payday1 = "0";
        } else if (pay.equals("3")) {
            payspecial = payday1;
            payday1 = "0";
        } else if (pay.equals("4")) {
            payspecial = payday1 + paydaysamemonthornot;
            payday1 = "0";
        } else if (pay.equals("5")) {
            payspecial = payday1 + payday2;
            payday1 = "0";
        } else if (pay.equals("6")) {
            payspecial = payday1 + payday2 + "Y" + payday3 + payday4 + "N";
            payday1 = "0";
        } else if (pay.equals("8")) {
            payspecial = payday1;
            payday1 = "0";
        } else if (pay.equals("9")) {
            payspecial = payday1 + payday2 + payday3 + payday4;
            payday1 = "0";
        } else if (pay.equals("10")) {
            payspecial = paydaysamemonthornot;
            payday1 = "0";
        }
        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                Statement stmt4 = conn.createStatement();
                query4 = "SELECT BPM_CUNO\n"
                        + "FROM BRLDTA0100.BP_MASTER A-- Master DATA\n"
                        + "WHERE BPM_CUNO = '" + customer + "'";
                ResultSet mRes = stmt4.executeQuery(query4);
                while (mRes.next()) {
                    if (customer.equals(mRes.getString(1))) {
                        check = "1";
                        respond = "data already generated";
                    }
                }
                if (check.equals("0")) {
                    //IMPORT NEW DATA
                    query1 = "INSERT INTO BRLDTA0100.BP_MASTER(BPM_CONO,BPM_CUNO,BPM_TYPE,BPM_RINV,BPM_RBIL,BPM_RCOL,BPM_CASEBIL,BPM_CASECOL,BPM_RBILSP,BPM_RCOLSP,BPM_CASEINV)\n"
                            + "values(" + company + ",'" + customer + "'," + type + ",'" + invroundA + "','" + bilinput1 + "','" + payday1 + "','" + bill + "','" + pay + "','" + bilspecial + "','" + payspecial + "','" + Invroundspecial + "')";

                    stmt.execute(query1);
                    respond = "generate successfully!";
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

    public static String importInvoiceData(String month, String year, String invround) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();
        String query1 = "";
        String query2 = "";
        String query3 = "";
        String query4 = "";
        String query5 = "";
        String querycase1 = "";
        String querycase2 = "";
        String respond = "";
        String check = "0";
        String nextmonth = Integer.toString(parseInt(month) + 1);
        if ("13".equals(nextmonth)) {
            nextmonth = "1";
            year = Integer.toString(parseInt(year) + 1);
        };
        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                Statement stmt2 = conn.createStatement();
                Statement stmt3 = conn.createStatement();
                Statement stmt4 = conn.createStatement();
                Statement stmt5 = conn.createStatement();
                Statement stmtcase1 = conn.createStatement();
                Statement stmtcase2 = conn.createStatement();
                query5 = "SELECT DISTINCT (MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))))\n"
                        + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                        + "Where YEAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                        + "AND A.BPS_CONO = B.BPM_CONO \n"
                        + "AND A.BPS_CUNO = B.BPM_CUNO\n"
                        + "AND B.BPM_RINV =" + invround;
                ResultSet mRes = stmt5.executeQuery(query5);
                while (mRes.next()) {
                    if (nextmonth.equals(mRes.getString(1))) {
                        respond = "Month already generated!";
                        check = "1";
                    }
                }
                if ("0".equals(check)) {
                    for (int i = 1; i <= 6; i++) {
                        if (i == 1) {
                            query1 = "INSERT INTO BRLDTA0100.BP_STDATE(BPS_CONO,BPS_CUNO,BPS_STDT,BPS_RD,BPS_STS)\n"
                                    + "SELECT DISTINCT A.BPS_CONO,A.BPS_CUNO, YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAYS )) * 10000\n"
                                    + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD') + 1 DAY))) * 100\n"
                                    + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD') + 1 DAY)))  AS ENDDATE,'1','00'\n"
                                    + "FROM BRLDTA0100.BP_STDATE A\n"
                                    + "JOIN BRLDTA0100.BP_MASTER B ON A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO  = B.BPM_CUNO\n"
                                    + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) <> MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAYS)\n"
                                    + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD')) + 1 DAY) =" + nextmonth + "\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY) =" + year + "\n"
                                    + "AND B.BPM_RINV =" + invround;
                            System.out.println("InsertFacility\n" + query1);
                        } else {
                            query1 = "INSERT INTO BRLDTA0100.BP_STDATE(BPS_CONO,BPS_CUNO,BPS_STDT,BPS_STS)\n"
                                    + "SELECT DISTINCT A.BPS_CONO,A.BPS_CUNO, YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAYS )) * 10000\n"
                                    + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD') + 1 DAY))) * 100\n"
                                    + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD') + 1 DAY)))  AS ENDDATE,'00'\n"
                                    + "FROM BRLDTA0100.BP_STDATE A\n"
                                    + "JOIN BRLDTA0100.BP_MASTER B ON A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO  = B.BPM_CUNO\n"
                                    + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY) = MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD')))\n"
                                    + "AND BPS_RD = (SELECT MAX(C.BPS_RD) FROM BRLDTA0100.BP_STDATE C,BRLDTA0100.BP_MASTER D WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(c.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + nextmonth + "\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + year + " AND D.BPM_CONO = C.BPS_CONO AND D.BPM_CUNO = C.BPS_CUNO\n"
                                    + "AND D.BPM_RINV = " + invround + ")\n"
                                    + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + nextmonth + "\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                                    + "AND B.BPM_RINV =" + invround;
                            System.out.println("InsertFacility\n" + query1);
                        }
//                    UPDATE END DATE OF START DATE TABLE USING MASTER TABLE 7 days
                        if (invround.equals("7")) {
                            query2 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                    + "SET C.BPS_FNDT = (SELECT YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 10000\n"
                                    + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 100\n"
                                    + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS))  AS ENDDATE\n"
                                    + "FROM BRLDTA0100.BP_STDATE A\n"
                                    + "JOIN BRLDTA0100.BP_MASTER B	\n"
                                    + "ON B.BPM_CUNO = A.BPS_CUNO\n"
                                    + "AND B.BPM_CONO = A.BPS_CONO\n"
                                    + "AND C.BPS_CUNO = A.BPS_CUNO\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO\n"
                                    + "AND B.BPM_RINV =" + invround + "\n"
                                    + "AND B.BPM_CASEINV = 0\n"
                                    + " FETCH FIRST 1 ROW ONLY)\n"
                                    + " WHERE EXISTS (SELECT YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 10000\n"
                                    + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 100\n"
                                    + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS))  AS ENDDATE\n"
                                    + "FROM BRLDTA0100.BP_STDATE A\n"
                                    + "JOIN BRLDTA0100.BP_MASTER B	\n"
                                    + "ON B.BPM_CUNO = A.BPS_CUNO\n"
                                    + "AND B.BPM_CONO = A.BPS_CONO\n"
                                    + "AND C.BPS_CUNO = A.BPS_CUNO\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO\n"
                                    + "AND B.BPM_RINV =" + invround + "\n"
                                    + "AND B.BPM_CASEINV = 0\n"
                                    + " FETCH FIRST 1 ROW ONLY)\n"
                                    + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth + "\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                        }
                        //15 days case
                        if (invround.equals("15")) {
                            query2 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                    + "SET C.BPS_FNDT = (SELECT CASE\n"
                                    + "	WHEN SUBSTRING(A.BPS_STDT,1,4) = '12' AND SUBSTRING(A.BPS_STDT,7,2) = '16' \n"
                                    + "	THEN (SUBSTRING(A.BPS_STDT,1,4) + 1) || '01' || '01' \n"
                                    + "	WHEN SUBSTRING(A.BPS_STDT,7,2) = '01' \n"
                                    + "	THEN SUBSTRING(A.BPS_STDT,1,4) || SUBSTRING(A.BPS_STDT,5,2) || '15' \n"
                                    + "	WHEN SUBSTRING(A.BPS_STDT,7,2) = '16'\n"
                                    + "	THEN YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                    + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                    + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1))\n"
                                    + "END\n"
                                    + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                    + "WHERE A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO = B.BPM_CUNO \n"
                                    + "AND B.BPM_RINV =" + invround + " AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT\n"
                                    + "AND B.BPM_CASEINV = 0)\n"
                                    + "WHERE EXISTS (SELECT CASE\n"
                                    + "	WHEN SUBSTRING(A.BPS_STDT,1,4) = '12' AND SUBSTRING(A.BPS_STDT,7,2) = '16' \n"
                                    + "	THEN (SUBSTRING(A.BPS_STDT,1,4) + 1) || '01' || '01' \n"
                                    + "	WHEN SUBSTRING(A.BPS_STDT,7,2) = '01' \n"
                                    + "	THEN SUBSTRING(A.BPS_STDT,1,4) || SUBSTRING(A.BPS_STDT,5,2) || '15' \n"
                                    + "	WHEN SUBSTRING(A.BPS_STDT,7,2) = '16'\n"
                                    + "	THEN YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                    + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                    + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1))\n"
                                    + "END\n"
                                    + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                    + "WHERE A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO = B.BPM_CUNO \n"
                                    + "AND B.BPM_RINV =" + invround + " AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT\n"
                                    + "AND B.BPM_CASEINV = 0)\n"
                                    + "AND SUBSTRING(C.BPS_STDT,5,2) =" + nextmonth + "\n"
                                    + "AND SUBSTRING(C.BPS_STDT,1,4) =" + year;
                        }
//       
//30 days case
                        if (invround.equals("30")) {
                            query2 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                    + "SET C.BPS_FNDT = (SELECT YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                    + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                    + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1))\n"
                                    + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                    + "WHERE A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO = B.BPM_CUNO\n"
                                    + "AND B.BPM_RINV = 30\n"
                                    + "AND B.BPM_CASEINV = 0\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                    + "WHERE EXISTS (SELECT YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                    + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                    + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1))\n"
                                    + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                    + "WHERE A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO = B.BPM_CUNO\n"
                                    + "AND B.BPM_RINV = 30\n"
                                    + "AND B.BPM_CASEINV = 0\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                    + "AND SUBSTRING(C.BPS_STDT,5,2) = " + nextmonth + "\n"
                                    + "AND SUBSTRING(C.BPS_STDT,1,4) = " + year;
                        }
                        querycase1 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                + "SET C.BPS_FNDT = (SELECT SUBSTRING(B.BPM_CASEINV,3,2) + \n"
                                + "MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))+1 month) * 100\n"
                                + "+ YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))+1 month) * 10000\n"
                                + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + nextmonth + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                                + "AND B.BPM_CONO = A.BPS_CONO AND B.BPM_CUNO  = A.BPS_CUNO\n"
                                + "AND SUBSTRING(B.BPM_CASEINV,1,2) = '01'\n"
                                + "AND B.BPM_RINV =" + invround + "\n"
                                + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                + "WHERE EXISTS (SELECT SUBSTRING(B.BPM_CASEINV,3,2) + \n"
                                + "MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))+1 month) * 100\n"
                                + "+ YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))+1 month) * 10000\n"
                                + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + nextmonth + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                                + "AND B.BPM_CONO = A.BPS_CONO AND B.BPM_CUNO  = A.BPS_CUNO\n"
                                + "AND SUBSTRING(B.BPM_CASEINV,1,2) = '01'\n"
                                + "AND B.BPM_RINV =" + invround + "\n"
                                + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                + "AND SUBSTRING(C.BPS_STDT,5,2) =" + nextmonth + "\n"
                                + "AND SUBSTRING(C.BPS_STDT,1,4) =" + year;
                        querycase2 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                + "SET C.BPS_FNDT = (SELECT \n"
                                + "YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2)) * 10000\n"
                                + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2)) * 100\n"
                                + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2))\n"
                                + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + nextmonth + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                                + "AND B.BPM_CONO = A.BPS_CONO AND B.BPM_CUNO  = A.BPS_CUNO\n"
                                + "AND SUBSTRING(B.BPM_CASEINV,1,2) = '2'\n"
                                + "AND B.BPM_RINV = 30\n"
                                + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                + "WHERE EXISTS (SELECT \n"
                                + "YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2)) * 10000\n"
                                + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2)) * 100\n"
                                + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2))\n"
                                + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + nextmonth + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                                + "AND B.BPM_CONO = A.BPS_CONO AND B.BPM_CUNO  = A.BPS_CUNO\n"
                                + "AND SUBSTRING(B.BPM_CASEINV,1,2) = '2'\n"
                                + "AND B.BPM_RINV = 30\n"
                                + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                + "AND SUBSTRING(C.BPS_STDT,5,2) =" + nextmonth + "\n"
                                + "AND SUBSTRING(C.BPS_STDT,1,4) =" + year;
                        //Create period
                        query3 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                                + "SET BPS_PR = (SELECT SUBSTRING(A.BPS_STDT,3,8) || SUBSTRING(A.BPS_FNDT,3,8)\n"
                                + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                + "WHERE B.BPM_CONO = a.BPS_CONO AND B.BPM_CUNO = A.BPS_CUNO\n"
                                + "AND C.BPS_CONO = A.BPS_CONO \n"
                                + "AND C.BPS_CUNO = A.BPS_CUNO\n"
                                + "AND C.BPS_STDT = A.BPS_STDT \n"
                                + "AND C.BPS_FNDT = A.BPS_FNDT\n"
                                + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                                + "AND B.BPM_RINV = " + invround + ")\n"
                                + "WHERE EXISTS (SELECT SUBSTRING(A.BPS_STDT,3,8) || SUBSTRING(A.BPS_FNDT,3,8)\n"
                                + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                + "WHERE B.BPM_CONO = a.BPS_CONO AND B.BPM_CUNO = A.BPS_CUNO\n"
                                + "AND C.BPS_CONO = A.BPS_CONO \n"
                                + "AND C.BPS_CUNO = A.BPS_CUNO\n"
                                + "AND C.BPS_STDT = A.BPS_STDT \n"
                                + "AND C.BPS_FNDT = A.BPS_FNDT\n"
                                + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                                + "AND B.BPM_RINV =" + invround + ")";
                        //Create round date
                        query4 = "UPDATE BRLDTA0100.BP_STDATE T1\n"
                                + "SET T1.BPS_RD = (SELECT COUNT(*)\n"
                                + "FROM BRLDTA0100.BP_STDATE T2, BRLDTA0100.BP_MASTER T3\n"
                                + "WHERE T2.BPS_CUNO=T1.BPS_CUNO\n"
                                + "AND DATE(TIMESTAMP_FORMAT(cast( T2 .BPS_STDT as varchar(8)), 'YYYYMMDD')) <=  DATE(TIMESTAMP_FORMAT(cast( T1 .BPS_STDT as varchar(8)), 'YYYYMMDD')) \n"
                                + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T2.BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                                + "AND T3.BPM_CONO  = T2.BPS_CONO\n"
                                + "AND T3.BPM_CUNO = T2.BPS_CUNO\n"
                                + "AND T1.BPS_CONO = T3.BPM_CONO\n"
                                + "AND T1.BPS_CUNO = T2.BPS_CUNO\n"
                                + "AND T3.BPM_RINV =" + invround + "\n"
                                + "GROUP BY T2.BPS_CUNO)\n"
                                + "WHERE EXISTS(SELECT COUNT(*)\n"
                                + "FROM BRLDTA0100.BP_STDATE T2, BRLDTA0100.BP_MASTER T3\n"
                                + "WHERE T2.BPS_CUNO=T1.BPS_CUNO\n"
                                + "AND DATE(TIMESTAMP_FORMAT(cast( T2 .BPS_STDT as varchar(8)), 'YYYYMMDD')) <=  DATE(TIMESTAMP_FORMAT(cast( T1 .BPS_STDT as varchar(8)), 'YYYYMMDD')) \n"
                                + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T2.BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                                + "AND T3.BPM_CONO  = T2.BPS_CONO\n"
                                + "AND T3.BPM_CUNO = T2.BPS_CUNO\n"
                                + "AND T1.BPS_CONO = T3.BPM_CONO\n"
                                + "AND T1.BPS_CUNO = T2.BPS_CUNO\n"
                                + "AND T1.BPS_STDT = T2.BPS_STDT\n"
                                + "AND T1.BPS_FNDT = T2.BPS_FNDT\n"
                                + "AND T3.BPM_RINV =" + invround + "\n"
                                + "GROUP BY T2.BPS_CUNO)\n"
                                + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T1.BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(T1.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                        stmt.execute(query1);
                        stmt2.execute(query2);
                        stmtcase1.execute(querycase1);
                        stmtcase2.execute(querycase2);
                        stmt3.execute(query3);
                        stmt4.execute(query4);
                    }
                    respond = "Month Generate Successfully";
                }
            } else {
                System.out.println("Server can't connect.");
            }

        } catch (SQLException sqle) {
            throw sqle;
        } catch (Exception e) {
            respond = "Error!";
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

    public static String importStartData(String month, String year, String invround) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();
        String query1 = "";
        String querybillnosunday = "";
        String querybillskipsunday = "";
        String querypay0 = "";
        String query4 = "";
        String query5 = "";
        String query6 = "";
        String query7 = "";
        String querypaycase1 = "";
        String querypaycase3 = "";
        String querypaycase4 = "";
        String querypaycase5 = "";
        String querypaycase6 = "";
        String querypaycase7 = "";
        String querypaycase8 = "";
        String querypaycase9 = "";
        String querypaycase10 = "";
        String querypayskipsunday = "";
        String respond = "";
        String check = "0";
        String check1 = "1";
        String nextmonth = Integer.toString(parseInt(month) + 1);

        if ("13".equals(nextmonth)) {
            nextmonth = "1";
            year = Integer.toString(parseInt(year) + 1);
        };
        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                Statement billnosunday = conn.createStatement();
                Statement billskipsunday = conn.createStatement();
                Statement pay0 = conn.createStatement();
                Statement stmt4 = conn.createStatement();
                Statement stmt5 = conn.createStatement();
                Statement stmt6 = conn.createStatement();
                Statement stmt7 = conn.createStatement();
                Statement stmtcasepay1 = conn.createStatement();
                Statement stmtcasepay3 = conn.createStatement();
                Statement stmtcasepay4 = conn.createStatement();
                Statement stmtcasepay5 = conn.createStatement();
                Statement stmtcasepay6 = conn.createStatement();
                Statement stmtcasepay7 = conn.createStatement();
                Statement stmtcasepay8 = conn.createStatement();
                Statement stmtcasepay9 = conn.createStatement();
                Statement stmtcasepay10 = conn.createStatement();
                Statement stmtskipsundaypay = conn.createStatement();
                query4 = "SELECT DISTINCT MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD')))\n"
                        + "FROM BRLDTA0100.BP_HBILL A , BRLDTA0100.BP_MASTER B\n"
                        + "WHERE YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                        + "AND A.BPH_CUNO = B.BPM_CUNO \n"
                        + "AND A.BPH_CONO = B.BPM_CONO\n"
                        + "AND B.BPM_RINV =" + invround;
                ResultSet mRes = stmt4.executeQuery(query4);
                while (mRes.next()) {
                    if (nextmonth.equals(mRes.getString(1))) {
                        respond = "Month already generated";
                        check = "1";
                    }
                }
                query5 = "SELECT DISTINCT (MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))))\n"
                        + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                        + "Where YEAR(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                        + "AND A.BPS_CONO = B.BPM_CONO \n"
                        + "AND A.BPS_CUNO = B.BPM_CUNO\n"
                        + "AND B.BPM_RINV =" + invround;
                ResultSet mRes2 = stmt5.executeQuery(query5);
                while (mRes2.next()) {
                    if (nextmonth.equals(mRes2.getString(1))) {
                        check1 = "0";
                    } else {
                    }
                }
                if ("1".equals(check1)) {
                    respond = "Please Generate Start Date before generate Header!";
                }
                if (check.equals("0") && check1.equals("0")) {
                    //--Import data from start date 7 days
                    query1 = "INSERT INTO BRLDTA0100.BP_HBILL(BPH_CONO,BPH_CUNO,BPH_STDT,BPH_FNDT,BPH_STS)\n"
                            + "SELECT A.BPS_CONO,A.BPS_CUNO,A.BPS_STDT,A.BPS_FNDT,A.BPS_STS\n"
                            + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                            + "WHERE B.BPM_CUNO = A.BPS_CUNO\n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND  MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + nextmonth + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;

                    System.out.println("InsertFacility\n" + query1);

                    //                    Create billdate Case 0 without Skip sunday
                    querybillnosunday = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_BPDT = (SELECT\n"
                            + "YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS )) * 10000\n"
                            + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS)) * 100\n"
                            + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS))\n"
                            + "FROM BRLDTA0100.BP_HBILL A\n"
                            + "JOIN BRLDTA0100.BP_MASTER B\n"
                            + "ON B.BPM_CUNO = A.BPH_CUNO\n"
                            + "AND B.BPM_CONO = A.BPH_CONO\n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO\n"
                            + "AND C.BPH_CONO = A.BPH_CONO\n"
                            + "AND B.BPM_RINV = " + invround + "\n"
                            + "AND B.BPM_CASEBIL = 0\n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT\n"
                            + " FETCH FIRST 1 ROW ONLY)\n"
                            + " WHERE EXISTS(SELECT\n"
                            + "YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS )) * 10000\n"
                            + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS)) * 100\n"
                            + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPH_FNDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RBIL DAYS))\n"
                            + "FROM BRLDTA0100.BP_HBILL A\n"
                            + "JOIN BRLDTA0100.BP_MASTER B\n"
                            + "ON B.BPM_CUNO = A.BPH_CUNO\n"
                            + "AND B.BPM_CONO = A.BPH_CONO\n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO\n"
                            + "AND C.BPH_CONO = A.BPH_CONO\n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASEBIL = 0\n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT\n"
                            + " FETCH FIRST 1 ROW ONLY)\n"
                            + " AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;

                    //                           Create billdate case 1
                    query6 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_BPDT =(SELECT\n"
                            + "CASE WHEN SUBSTRING(B.BPM_RBILSP,3,1) = 'C'\n"
                            + "---------------------------------------------NORMAL---------------------------------------\n"
                            + "THEN\n"
                            + "---------------------------------------------------WEEK1----------------------------\n"
                            + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%1%'\n"
                            + "THEN \n"
                            + "------------------------------------------------RESULT OF WEEK1--------------------------------------\n"
                            + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS))\n"
                            + "-------------------------------------------WEEK2----------------------------------------------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%2%'\n"
                            + "---------------------------------------------------------RESULT WEEK2----------------------------------------------------------\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS))\n"
                            + "---------------------------------------------------------------------------WEEK3-------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%3%'\n"
                            + "-------------------------------------------------------------------------RESULT OF WEEK3--------------------------------\n"
                            + "THEN \n"
                            + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS))\n"
                            + "--------------------------------------------------------------------------WEEK 4-----------------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%4%'\n"
                            + "-------------------------------------------------------------------RESULT WEEK4------------------------------------------------------\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS))\n"
                            + "ELSE \n"
                            + " DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS)) DAYS)) * 10000\n"
                            + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS)) DAYS))\n"
                            + "END\n"
                            + "WHEN SUBSTRING(B.BPM_RBILSP ,3,1) = 'R'\n"
                            + "---------------------------SPECIAL CHANGE VALUE IF CASE HAVE 5 WEEKS ON SPECIFIC DATE------\n"
                            + "THEN \n"
                            + "CASE WHEN SUBSTRING(BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(BPH_FNDT,1,4)||'-'|| SUBSTRING(BPH_FNDT,5,2)||'-'||'28')) >= 0 AND\n"
                            + "DAYOFWEEK(DATE(SUBSTRING(BPH_FNDT,1,4)||'-'|| SUBSTRING(BPH_FNDT,5,2)||'-'||'28')) -- DAY OF START OF WEEK5\n"
                            + " <= DAYS(LAST_DAY(DATE(SUBSTRING(BPH_FNDT,1,4)||'-'|| SUBSTRING(BPH_FNDT,5,2)||'-'||SUBSTRING(BPH_FNDT,7,2))))\n"
                            + "-DAYS(DATE(SUBSTRING(BPH_FNDT,1,4)||'-'|| SUBSTRING(BPH_FNDT,5,2)||'-'||'01'))-27  -- DAY LEFT IN MONTH\n"
                            + "THEN \n"
                            + "---------------------------------------------------WEEK1----------------------------\n"
                            + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%1%'\n"
                            + "----------------------------------!!--RESULT OF WEEK 1-!!--------------------\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS))\n"
                            + "--------------------------------------------------WEEK2---------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))  LIKE '%2%'\n"
                            + "----------------------------------!!--RESULT OF WEEK 2-!!--------------------\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS))\n"
                            + "--------------------------------------------------WEEK3---------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%3%'\n"
                            + "----------------------------------!!--RESULT OF WEEK 3-!!--------------------\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS))\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%4%'\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS))\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP)) LIKE '%5%'\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (4 * 7) DAYS)) DAYS))\n"
                            + "ELSE DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,4,1)-1) * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))-1) * 7) DAYS)) DAYS)) * 10000\n"
                            + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))-1) * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))-1) * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))-1) * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,LENGTH(BPM_RBILSP))-1) * 7) DAYS)) DAYS))\n"
                            + "END\n"
                            + "--END OF SECOND PART\n"
                            + "ELSE \n"
                            + "---------------------------------------------------WEEK1----------------------------\n"
                            + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,4,LENGTH(BPM_RBILSP)-LOCATE('OR', BPM_RBILSP)) LIKE '%1%'\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (0 * 7) DAYS)) DAYS))\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,4,LENGTH(BPM_RBILSP)-LOCATE('OR', BPM_RBILSP)) LIKE '%2%'\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (1 * 7) DAYS)) DAYS))\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,4,LENGTH(BPM_RBILSP)-LOCATE('OR', BPM_RBILSP)) LIKE '%3%'\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (2 * 7) DAYS)) DAYS))\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_FNDT as varchar(8)), 'YYYYMMDD')) < \n"
                            + "DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS\n"
                            + "AND SUBSTRING(BPM_RBILSP,4,LENGTH(BPM_RBILSP)-LOCATE('OR', BPM_RBILSP)) LIKE '%4%'\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + (3 * 7) DAYS)) DAYS))\n"
                            + "ELSE DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS)) DAYS)) * 10000\n"
                            + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS \n"
                            + "+ (SUBSTRING(B.BPM_RBILSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_FNDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RBILSP,LOCATE('OR', BPM_RBILSP) + 2,1)-1) * 7) DAYS)) DAYS))\n"
                            + "END\n"
                            + "--END OF FIRST PART\n"
                            + "END\n"
                            + "WHEN SUBSTRING(B.BPM_RBILSP ,3,1) = 'N'\n"
                            + "---------------------------DO NOT COUNT FIRST WEEK IF NOT FULL OR START ON SUNDAY------\n"
                            + "THEN 'SUNDAY!!'\n"
                            + "END AS TESTWEEK\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE B.BPM_CUNO = A.BPH_CUNO AND B.BPM_CONO = A.BPH_CONO \n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT\n"
                            + "AND B.BPM_RINV = '" + invround + "'\n"
                            + "AND B.BPM_CASEBIL = '1')\n"
                            + "WHERE EXISTS (SELECT A.BPH_CUNO,A.BPH_FNDT--,B.BPM_RBILSP\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE B.BPM_CUNO = A.BPH_CUNO AND B.BPM_CONO = A.BPH_CONO \n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT\n"
                            + "AND B.BPM_RINV = '" + invround + "'\n"
                            + "AND B.BPM_CASEBIL = '1')\n"
                            + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + nextmonth + "'\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                    //Create Pay date case 0
                    querypay0 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_CLDT= (SELECT\n"
                            + "YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS )) * 10000\n"
                            + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS)) * 100\n"
                            + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS))\n"
                            + "FROM BRLDTA0100.BP_HBILL A\n"
                            + "JOIN BRLDTA0100.BP_MASTER B\n"
                            + "ON B.BPM_CUNO = A.BPH_CUNO\n"
                            + "AND B.BPM_CONO = A.BPH_CONO\n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL  = '0'\n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + " WHERE  EXISTS (SELECT\n"
                            + "YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS )) * 10000\n"
                            + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS)) * 100\n"
                            + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RCOL DAYS))\n"
                            + "FROM BRLDTA0100.BP_HBILL A\n"
                            + "JOIN BRLDTA0100.BP_MASTER B\n"
                            + "ON B.BPM_CUNO = A.BPH_CUNO\n"
                            + "AND B.BPM_CONO = A.BPH_CONO\n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL  = '0'\n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + " AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                    //Create pay case 1
                    querypaycase1 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_CLDT = (SELECT CASE WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'C'\n"
                            + "---------------------------------------------NORMAL---------------------------------------\n"
                            + "THEN\n"
                            + "---------------------------------------------------WEEK1----------------------------\n"
                            + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%1%'\n"
                            + "THEN \n"
                            + "------------------------------------------------RESULT OF WEEK1--------------------------------------\n"
                            + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS+ 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS+ 1 MONTHS)) DAYS))\n"
                            + "-------------------------------------------WEEK2----------------------------------------------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%2%'\n"
                            + "---------------------------------------------------------RESULT WEEK2----------------------------------------------------------\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                            + "---------------------------------------------------------------------------WEEK3-------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%3%'\n"
                            + "-------------------------------------------------------------------------RESULT OF WEEK3--------------------------------\n"
                            + "THEN \n"
                            + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                            + "--------------------------------------------------------------------------WEEK 4-----------------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%4%'\n"
                            + "-------------------------------------------------------------------RESULT WEEK4------------------------------------------------------\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                            + "ELSE \n"
                            + " DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7 ) DAYS)) DAYS)) * 10000\n"
                            + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS))\n"
                            + "END\n"
                            + "WHEN SUBSTRING(B.BPM_RCOLSP ,3,1) = 'N'\n"
                            + "---------------------------SPECIAL CHANGE VALUE IF CASE HAVE 5 WEEKS ON SPECIFIC DATE------\n"
                            + "THEN \n"
                            + "CASE WHEN DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01')) = 1\n"
                            + "THEN\n"
                            + "---------------------------------------------------WEEK1----------------------------\n"
                            + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%1%'\n"
                            + "THEN \n"
                            + "------------------------------------------------RESULT OF WEEK1--------------------------------------\n"
                            + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (0 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                            + "-------------------------------------------WEEK2----------------------------------------------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%2%'\n"
                            + "---------------------------------------------------------RESULT WEEK2----------------------------------------------------------\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                            + "---------------------------------------------------------------------------WEEK3-------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%3%'\n"
                            + "-------------------------------------------------------------------------RESULT OF WEEK3--------------------------------\n"
                            + "THEN \n"
                            + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                            + "--------------------------------------------------------------------------WEEK 4-----------------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%4%'\n"
                            + "-------------------------------------------------------------------RESULT WEEK4------------------------------------------------------\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                            + "ELSE \n"
                            + " DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS)) * 10000\n"
                            + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS))\n"
                            + "END\n"
                            + "--END OF SECOND PART\n"
                            + "ELSE \n"
                            + "---------------------------------------------------WEEK1----------------------------\n"
                            + "CASE WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_FNDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%1%'\n"
                            + "THEN \n"
                            + "------------------------------------------------RESULT OF WEEK1--------------------------------------\n"
                            + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (1 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                            + "-------------------------------------------WEEK2----------------------------------------------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%2%'\n"
                            + "---------------------------------------------------------RESULT WEEK2----------------------------------------------------------\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (2 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                            + "---------------------------------------------------------------------------WEEK3-------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%3%'\n"
                            + "-------------------------------------------------------------------------RESULT OF WEEK3--------------------------------\n"
                            + "THEN \n"
                            + "DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (3 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                            + "--------------------------------------------------------------------------WEEK 4-----------------------------------------------------\n"
                            + "WHEN DATE(TIMESTAMP_FORMAT(cast(A.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 MONTHS < \n"
                            + "DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS)) DAYS\n"
                            + "AND SUBSTRING(B.BPM_RCOLSP,4,5) LIKE '%4%'\n"
                            + "-------------------------------------------------------------------RESULT WEEK4------------------------------------------------------\n"
                            + "THEN DECIMAL(YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS)) DAYS)) * 10000\n"
                            + "+ DECIMAL(MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY (DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK(DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + (4 * 7) DAYS + 1 MONTHS)) DAYS))\n"
                            + "ELSE \n"
                            + " DECIMAL(YEAR ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS)) * 10000\n"
                            + "+DECIMAL(MONTH ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS  + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS)) * 100\n"
                            + "+ DECIMAL(DAY ((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS) + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS + 1 MONTHS\n"
                            + "+ (SUBSTRING(B.BPM_RCOLSP,1,1) - DAYOFWEEK((DATE(SUBSTRING(A.BPH_BPDT,1,4)||'-'|| SUBSTRING(A.BPH_BPDT,5,2)||'-'||'01') + 1 MONTHS)  + ((SUBSTRING(BPM_RCOLSP,4,1)-1) * 7) DAYS)) DAYS))\n"
                            + "END\n"
                            + "--END OF FIRST PART\n"
                            + "END\n"
                            + "END AS TESTWEEK\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE B.BPM_CUNO = A.BPH_CUNO AND B.BPM_CONO = A.BPH_CONO \n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT\n"
                            + "AND B.BPM_RINV = " + invround + "\n"
                            + "AND B.BPM_CASECOl = '1'\n"
                            + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + nextmonth + "'\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT AS varchar(8)), 'YYYYMMDD'))) = '" + year + "')\n"
                            + "WHERE EXISTS (SELECT A.BPH_CUNO,A.BPH_STDT,A.BPH_FNDT,A.BPH_BPDT --,B.BPM_RBILSP\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE B.BPM_CUNO = A.BPH_CUNO AND B.BPM_CONO = A.BPH_CONO \n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT\n"
                            + "AND B.BPM_CASECOL  = '1'\n"
                            + "AND B.BPM_RINV = " + invround + "\n"
                            + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + nextmonth + "'\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT AS varchar(8)), 'YYYYMMDD'))) = '" + year + "')\n"
                            + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + nextmonth + "'\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT as varchar(8)), 'YYYYMMDD'))) = '" + year + "'";
                    //case for paycase 3

                    querypaycase3 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_CLDT = (SELECT CASE \n"
                            + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) = '12'\n"
                            + "	THEN (SUBSTRING(A.BPH_BPDT,0,5) + 1) || '01' || B.BPM_RCOLSP\n"
                            + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) >= '09'\n"
                            + "	THEN SUBSTRING(A.BPH_BPDT,0,5) || (SUBSTRING(A.BPH_BPDT,5,2) + 1) || B.BPM_RCOLSP\n"
                            + "	ELSE SUBSTRING(A.BPH_BPDT,0,5) ||'0' || (SUBSTRING(A.BPH_BPDT,5,2) + 1) || B.BPM_RCOLSP\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_CASECOL = '3'\n"
                            + "AND SUBSTRING(A.BPH_STDT,5,2) =" + nextmonth + "\n"
                            + "AND SUBSTRING(A.BPH_STDT,1,4) =" + year + "\n"
                            + "AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_CONO = C.BPH_CONO  AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT  = C.BPH_FNDT AND A.BPH_BPDT = C.BPH_BPDT )\n"
                            + "WHERE EXISTS(SELECT CASE \n"
                            + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) = '12'\n"
                            + "	THEN (SUBSTRING(A.BPH_BPDT,0,5) + 1) || '01' || B.BPM_RCOLSP\n"
                            + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) >= '09'\n"
                            + "	THEN SUBSTRING(A.BPH_BPDT,0,5) || (SUBSTRING(A.BPH_BPDT,5,2) + 1) || B.BPM_RCOLSP\n"
                            + "	ELSE SUBSTRING(A.BPH_BPDT,0,5) ||'0' || (SUBSTRING(A.BPH_BPDT,5,2) + 1) || B.BPM_RCOLSP\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_CASECOL = '3'\n"
                            + "AND SUBSTRING(A.BPH_STDT,5,2) = " + nextmonth + "\n"
                            + "AND SUBSTRING(A.BPH_STDT,1,4) =" + year + "\n"
                            + "AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_CONO = C.BPH_CONO  AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT  = C.BPH_FNDT AND A.BPH_BPDT = C.BPH_BPDT )\n"
                            + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = " + nextmonth + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;

//case for paycase 4
                    querypaycase4 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_CLDT= (SELECT\n"
                            + "CASE\n"
                            + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'N'\n"
                            + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'Y' AND SUBSTRING(A.BPH_BPDT,5,2) = '12' \n"
                            + "	THEN YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2))+ 1 MONTHS) *10000+\n"
                            + "	MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) +1 MONTHS)*100 +\n"
                            + "	DAY(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS)\n"
                            + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'Y' \n"
                            + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'C' AND (SUBSTRING(A.BPH_BPDT,5,2) = '12' OR SUBSTRING(A.BPH_BPDT,5,2) = '11')\n"
                            + "	THEN YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2))+ 2 MONTHS) *10000+\n"
                            + "	MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) +2 MONTHS)*100 +\n"
                            + "	DAY(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) + 2 MONTHS)\n"
                            + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'C' \n"
                            + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 2), 2, '0') || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_RINV = '" + invround + "'\n"
                            + "AND B.BPM_CASECOL = '4'\n"
                            + "AND A.BPH_CONO = C.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "WHERE EXISTS(SELECT\n"
                            + "CASE\n"
                            + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'N'\n"
                            + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'Y' AND SUBSTRING(A.BPH_BPDT,5,2) = '12' \n"
                            + "	THEN YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2))+ 1 MONTHS) *10000+\n"
                            + "	MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) +1 MONTHS)*100 +\n"
                            + "	DAY(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS)\n"
                            + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'Y' \n"
                            + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'C' AND (SUBSTRING(A.BPH_BPDT,5,2) = '12' OR SUBSTRING(A.BPH_BPDT,5,2) = '11')\n"
                            + "	THEN YEAR(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2))+ 2 MONTHS) *10000+\n"
                            + "	MONTH(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) +2 MONTHS)*100 +\n"
                            + "	DAY(DATE(SUBSTRING(A.BPH_BPDT,1,4) ||'-'|| SUBSTRING(BPH_BPDT,5,2) ||'-'|| SUBSTRING(B.BPM_RCOLSP,1,2)) + 2 MONTHS)\n"
                            + "	WHEN SUBSTRING(B.BPM_RCOLSP,3,1) = 'C' \n"
                            + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 2), 2, '0') || SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_RINV = '" + invround + "'\n"
                            + "AND B.BPM_CASECOL = '4'\n"
                            + "AND A.BPH_CONO = C.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + " AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) = '" + nextmonth + "'\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                    //case for paycase 5
                    querypaycase5 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_CLDT = (SELECT CASE WHEN SUBSTRING(B.BPM_RCOLSP,1,2) < SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                            + "THEN\n"
                            + "CASE\n"
                            + "     -- iF THE DATE IS LESS THAN THE FIRST GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                            + "WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	-- IF THE DATE GIVEN IS LESS THAN THE SECOND GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                            + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                            + "	 --IF THE RESULT GONNA BE NEXT YEAR AND THE RESULT IS NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||(SUBSTRING(BPH_BPDT,5,2) + 1)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12' \n"
                            + "	 THEN  (SUBSTRING(BPH_BPDT,1,4) + 1) ||'01'||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	 --IF THE RESULT END UP BEING NEXT MONTH AND RESULT IS NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	 THEN  SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "END\n"
                            + "WHEN  SUBSTRING(B.BPM_RCOLSP,3,2) < SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "THEN CASE\n"
                            + "     -- iF THE DATE IS LESS THAN THE FIRST GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                            + "WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2)) + 1 MONTHS <\n"
                            + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS\n"
                            + "	 THEN YEAR (DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 10000\n"
                            + "	 +MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 100\n"
                            + "	 +DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 1\n"
                            + "	-- IF THE DATE GIVEN IS LESS THAN THE SECOND GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                            + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2)) + 1 MONTHS <\n"
                            + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS\n"
                            + "	 THEN YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS) * 10000\n"
                            + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS) * 100\n"
                            + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS)\n"
                            + "	 --IF THE RESULT GONNA BE NEXT YEAR AND THE RESULT IS NOT SUNDAY\n"
                            + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2))+ 1 MONTHS <\n"
                            + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||(SUBSTRING(BPH_BPDT,5,2))||'-'||SUBSTRING(B.BPM_RCOLSP,3,2))+ 1 MONTHS\n"
                            + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12' \n"
                            + "	 THEN YEAR(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                            + "	 + MONTH(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                            + "	  + DAY(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                            + "	 --IF THE RESULT END UP BEING NEXT MONTH AND RESULT IS NOT SUNDAY\n"
                            + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2))+ 1 MONTHS <\n"
                            + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS\n"
                            + "	 THEN  YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                            + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                            + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                            + "	 ELSE  YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                            + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                            + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                            + "END\n"
                            + "END AS TEST\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                            + "AND B.BPM_CASECOL = '5'\n"
                            + "AND B.BPM_RINV = '" + invround + "'\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT  AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "WHERE EXISTS (SELECT CASE WHEN SUBSTRING(B.BPM_RCOLSP,1,2) < SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                            + "THEN\n"
                            + "CASE\n"
                            + "     -- iF THE DATE IS LESS THAN THE FIRST GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                            + "WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	-- IF THE DATE GIVEN IS LESS THAN THE SECOND GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                            + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                            + "	 --IF THE RESULT GONNA BE NEXT YEAR AND THE RESULT IS NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||(SUBSTRING(BPH_BPDT,5,2) + 1)||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12' \n"
                            + "	 THEN  (SUBSTRING(BPH_BPDT,1,4) + 1) ||'01'||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	 --IF THE RESULT END UP BEING NEXT MONTH AND RESULT IS NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	 THEN  SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "END\n"
                            + "WHEN  SUBSTRING(B.BPM_RCOLSP,3,2) < SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "THEN CASE\n"
                            + "     -- iF THE DATE IS LESS THAN THE FIRST GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                            + "WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2)) + 1 MONTHS <\n"
                            + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS\n"
                            + "	 THEN YEAR (DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 10000\n"
                            + "	 +MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 100\n"
                            + "	 +DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTH) * 1\n"
                            + "	-- IF THE DATE GIVEN IS LESS THAN THE SECOND GIVEN CASE DATE AND RESULT IS NOT SUNDAY\n"
                            + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2)) + 1 MONTHS <\n"
                            + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS\n"
                            + "	 THEN YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS) * 10000\n"
                            + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS) * 100\n"
                            + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(B.BPM_RCOLSP,1,2)) + 1 MONTHS)\n"
                            + "	 --IF THE RESULT GONNA BE NEXT YEAR AND THE RESULT IS NOT SUNDAY\n"
                            + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2))+ 1 MONTHS <\n"
                            + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||(SUBSTRING(BPH_BPDT,5,2))||'-'||SUBSTRING(B.BPM_RCOLSP,3,2))+ 1 MONTHS\n"
                            + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12' \n"
                            + "	 THEN YEAR(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                            + "	 + MONTH(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                            + "	  + DAY(DATE((SUBSTRING(BPH_BPDT,1,4) + 1) ||'-'||'01'||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                            + "	 --IF THE RESULT END UP BEING NEXT MONTH AND RESULT IS NOT SUNDAY\n"
                            + "	 WHEN DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-'||SUBSTRING(BPH_BPDT,7,2))+ 1 MONTHS <\n"
                            + "	 DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS\n"
                            + "	 THEN  YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                            + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                            + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                            + "	 ELSE  YEAR(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 10000\n"
                            + "	 + MONTH(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS) * 100\n"
                            + "	 + DAY(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||LPAD((SUBSTRING(BPH_BPDT,5,2)), 2, '0')||'-'||SUBSTRING(B.BPM_RCOLSP,3,2)) + 1 MONTHS)\n"
                            + "END\n"
                            + "END AS TEST\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_CASECOL = '5'\n"
                            + "AND B.BPM_RINV = '" + invround + "'\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT  AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "AND SUBSTRING(C.BPH_STDT,1,4) = '" + year + "'\n"
                            + "AND SUBSTRING(C.BPH_STDT,5,2) =" + nextmonth;
                    //case for paycase 6
                    querypaycase6 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_CLDT = (SELECT CASE \n"
                            + "	--CASE WHEN IT MATCHED WITH THE FIRST NUMBER AND NEXT MONTH\n"
                            + "	WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	AND SUBSTRING(B.BPM_RCOLSP,5,1) = 'Y'\n"
                            + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 10000\n"
                            + "	 + MONTH(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 100\n"
                            + "	  + DAY(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)    as varchar(8)), 'YYYYMMDD')) + 1 MONTH)\n"
                            + "	  WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	AND SUBSTRING(B.BPM_RCOLSP,5,1) = 'N'\n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                            + "	WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,6,2)\n"
                            + "	AND SUBSTRING(B.BPM_RCOLSP,10,1) = 'Y'\n"
                            + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 10000\n"
                            + "	 + MONTH(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 100\n"
                            + "	  + DAY(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH)\n"
                            + "	  WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,6,2)\n"
                            + "	AND SUBSTRING(B.BPM_RCOLSP,10,1) = 'N'\n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL = '6'\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT  AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "WHERE EXISTS (SELECT CASE \n"
                            + "	--CASE WHEN IT MATCHED WITH THE FIRST NUMBER AND NEXT MONTH\n"
                            + "	WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	AND SUBSTRING(B.BPM_RCOLSP,5,1) = 'Y'\n"
                            + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 10000\n"
                            + "	 + MONTH(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 100\n"
                            + "	  + DAY(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)    as varchar(8)), 'YYYYMMDD')) + 1 MONTH)\n"
                            + "	  WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,1,2)\n"
                            + "	AND SUBSTRING(B.BPM_RCOLSP,5,1) = 'N'\n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,3,2)\n"
                            + "	WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,6,2)\n"
                            + "	AND SUBSTRING(B.BPM_RCOLSP,10,1) = 'Y'\n"
                            + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 10000\n"
                            + "	 + MONTH(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH) * 100\n"
                            + "	  + DAY(DATE(TIMESTAMP_FORMAT(cast(SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)   as varchar(8)), 'YYYYMMDD')) + 1 MONTH)\n"
                            + "	  WHEN SUBSTRING(A.BPH_BPDT,7,2) = SUBSTRING(B.BPM_RCOLSP,6,2)\n"
                            + "	AND SUBSTRING(B.BPM_RCOLSP,10,1) = 'N'\n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(B.BPM_RCOLSP,8,2)\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL = '6'\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT  AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                            + "AND SUBSTRING(C.BPH_STDT,5,2) =" + nextmonth;

                    //case for paycase 7
                    querypaycase7 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_CLDT = (SELECT CASE \n"
                            + "	--NEW YEAR CASE AND NOT SUNDAY\n"
                            + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) = 12\n"
                            + "	THEN (SUBSTRING(A.BPH_BPDT,1,4) + 1) ||'01' || SUBSTRING(A.BPH_BPDT,7,2)\n"
                            + "	-- OTHER CASE AND NOT SUNDAY\n"
                            + "	WHEN  SUBSTRING(A.BPH_BPDT,5,2) <> 12 \n"
                            + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(A.BPH_BPDT,7,2)\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL = '7'\n"
                            + "AND SUBSTRING(A.BPH_STDT,1,4) =" + year + "\n"
                            + "AND SUBSTRING(A.BPH_STDT,5,2) =" + nextmonth + "\n"
                            + "AND A.BPH_CONO = C.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT )\n"
                            + "WHERE EXISTS (SELECT CASE \n"
                            + "	--NEW YEAR CASE AND NOT SUNDAY\n"
                            + "	WHEN SUBSTRING(A.BPH_BPDT,5,2) = 12\n"
                            + "	THEN (SUBSTRING(A.BPH_BPDT,1,4) + 1) ||'01' || SUBSTRING(A.BPH_BPDT,7,2)\n"
                            + "	-- OTHER CASE AND NOT SUNDAY\n"
                            + "	WHEN  SUBSTRING(A.BPH_BPDT,5,2) <> 12 \n"
                            + "	THEN SUBSTRING(A.BPH_BPDT,1,4) || LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(A.BPH_BPDT,7,2)\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO \n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL = '7'\n"
                            + "AND SUBSTRING(A.BPH_STDT,1,4) =" + year + "\n"
                            + "AND SUBSTRING(A.BPH_STDT,5,2) =" + nextmonth + "\n"
                            + "AND A.BPH_CONO = C.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "AND SUBSTRING(C.BPH_STDT,1,4) = " + year + "\n"
                            + "AND SUBSTRING(C.BPH_STDT,5,2) =" + nextmonth;
                    //case for pay case8
                    querypaycase8 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_CLDT = (SELECT CASE\n"
                            + "	-- First half of the month AND RESULT IS NOT SUNDAY\n"
                            + "WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||'15'\n"
                            + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||B.BPM_RCOLSP\n"
                            + "	 -- Last day of the month AND RESULT IS NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||\n"
                            + "	 DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                            + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)|| \n"
                            + "	 DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                            + "	 -- WHEN LAST DAY OF THE MONTH OF DECEMBER SO NEW YEAR AND NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                            + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12'\n"
                            + "	 THEN  (SUBSTRING(BPH_BPDT,1,4) + 1) || '01'||B.BPM_RCOLSP\n"
                            + "	 -- WHEN NEXT HALF NEXT MONTH IS ANSWER IS NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||B.BPM_RCOLSP\n"
                            + "	 THEN  SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||B.BPM_RCOLSP\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL = '8'\n"
                            + "--AND SUBSTRING(A.BPH_STDT,1,4) = 2023\n"
                            + "--AND SUBSTRING(A.BPH_STDT,5,2) = 2\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "WHERE EXISTS (SELECT A.BPH_CUNO ,A.BPH_STDT,A.BPH_FNDT,A.BPH_BPDT,A.BPH_CLDT,CASE\n"
                            + "	-- First half of the month AND RESULT IS NOT SUNDAY\n"
                            + "WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||'15'\n"
                            + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||B.BPM_RCOLSP\n"
                            + "	 -- Last day of the month AND RESULT IS NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||\n"
                            + "	 DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                            + "	 THEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)|| \n"
                            + "	 DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                            + "	 -- WHEN LAST DAY OF THE MONTH OF DECEMBER SO NEW YEAR AND NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||DAY(DATE((DAYS(DATE(SUBSTRING(BPH_BPDT,1,4)||'-'||SUBSTRING(BPH_BPDT,5,2)||'-01') + 1 MONTH) - 1)))\n"
                            + "	 AND SUBSTRING(BPH_BPDT,5,2) = '12'\n"
                            + "	 THEN  (SUBSTRING(BPH_BPDT,1,4) + 1) || '01'||B.BPM_RCOLSP\n"
                            + "	 -- WHEN NEXT HALF NEXT MONTH IS ANSWER IS NOT SUNDAY\n"
                            + "	 WHEN SUBSTRING(BPH_BPDT,1,4)||SUBSTRING(BPH_BPDT,5,2)||SUBSTRING(BPH_BPDT,7,2) <\n"
                            + "	 SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||B.BPM_RCOLSP\n"
                            + "	 THEN  SUBSTRING(BPH_BPDT,1,4)||LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0')||B.BPM_RCOLSP\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL = '8'\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                            + "AND SUBSTRING(C.BPH_STDT,5,2) =" + nextmonth;
                    //pay case 9
                    querypaycase9 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_CLDT = (SELECT CASE\n"
                            + "	-- IF THE DATE GIVEN IS LESS THAN SECOND PERIOD BUT IN THE FIRST PEROID AND NOT SUNDAY\n"
                            + "	WHEN SUBSTRING(BPH_BPDT,7,2) >= SUBSTRING(BPM_RCOLSP,1,2) AND SUBSTRING(BPH_BPDT,7,2) <= SUBSTRING(BPM_RCOLSP,3,2) AND SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,5,2)\n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,5,2)\n"
                            + "	--IF the DATE GIVEN IS LESS THAN THE FIRST PERIOD AND NOT SUNDAY\n"
                            + "	WHEN SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,1,2) \n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,1,2)\n"
                            + "	--IF THE DATE GIVEN IS IN BETWEEN FIRST AND SECOND PERIOD AND IS NOT SUNDAY\n"
                            + "	WHEN SUBSTRING(BPH_BPDT,7,2) > SUBSTRING(BPM_RCOLSP,3,2) AND SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,5,2)\n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,5,2)\n"
                            + "	--IF THE DATE GIVEN IS IN LAST PERIOD OR THE DATE GIVEN IS LATER OF THE PERIOD AND IS NOT SUNDAY\n"
                            + "	WHEN ((SUBSTRING(BPH_BPDT,7,2) >=  SUBSTRING(BPM_RCOLSP,5,2) AND SUBSTRING(BPH_BPDT,7,2) <= SUBSTRING(BPM_RCOLSP,7,2) ) OR\n"
                            + "	SUBSTRING(BPH_BPDT,7,2) > SUBSTRING(BPM_RCOLSP,7,2))\n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)|| LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(BPM_RCOLSP,1,2)\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL = '9'\n"
                            + "AND SUBSTRING(A.BPH_STDT,1,4) =" + year + "\n"
                            + "AND SUBSTRING(A.BPH_STDT,5,2) =" + nextmonth + "\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "WHERE EXISTS (SELECT CASE\n"
                            + "	-- IF THE DATE GIVEN IS LESS THAN SECOND PERIOD BUT IN THE FIRST PEROID AND NOT SUNDAY\n"
                            + "	WHEN SUBSTRING(BPH_BPDT,7,2) >= SUBSTRING(BPM_RCOLSP,1,2) AND SUBSTRING(BPH_BPDT,7,2) <= SUBSTRING(BPM_RCOLSP,3,2) AND SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,5,2)\n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,5,2)\n"
                            + "	--IF the DATE GIVEN IS LESS THAN THE FIRST PERIOD AND NOT SUNDAY\n"
                            + "	WHEN SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,1,2) \n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,1,2)\n"
                            + "	--IF THE DATE GIVEN IS IN BETWEEN FIRST AND SECOND PERIOD AND IS NOT SUNDAY\n"
                            + "	WHEN SUBSTRING(BPH_BPDT,7,2) > SUBSTRING(BPM_RCOLSP,3,2) AND SUBSTRING(BPH_BPDT,7,2) < SUBSTRING(BPM_RCOLSP,5,2)\n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)|| SUBSTRING(BPH_BPDT,5,2) || SUBSTRING(BPM_RCOLSP,5,2)\n"
                            + "	--IF THE DATE GIVEN IS IN LAST PERIOD OR THE DATE GIVEN IS LATER OF THE PERIOD AND IS NOT SUNDAY\n"
                            + "	WHEN ((SUBSTRING(BPH_BPDT,7,2) >=  SUBSTRING(BPM_RCOLSP,5,2) AND SUBSTRING(BPH_BPDT,7,2) <= SUBSTRING(BPM_RCOLSP,7,2) ) OR\n"
                            + "	SUBSTRING(BPH_BPDT,7,2) > SUBSTRING(BPM_RCOLSP,7,2))\n"
                            + "	THEN SUBSTRING(BPH_BPDT,1,4)|| LPAD((SUBSTRING(BPH_BPDT,5,2) + 1), 2, '0') || SUBSTRING(BPM_RCOLSP,1,2)\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL = '9'\n"
                            + "AND SUBSTRING(A.BPH_STDT,1,4) =" + year + "\n"
                            + "AND SUBSTRING(A.BPH_STDT,5,2) =" + nextmonth + "\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                            + "AND SUBSTRING(C.BPH_STDT,5,2) =" + nextmonth;
                    //paycase 10
                    querypaycase10 = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_CLDT = (SELECT CASE\n"
                            + "	WHEN B.BPM_RCOLSP = 'N'\n"
                            + "	THEN\n"
                            + "	YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                            + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                            + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1))\n"
                            + "	WHEN B.BPM_RCOLSP = 'Y'\n"
                            + "	THEN \n"
                            + "	YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1)) * 10000\n"
                            + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1)) * 100\n"
                            + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1))\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A,BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO  = B.BPM_CONO AND A.BPH_CUNO  = B.BPM_CUNO\n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL = '10'\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "WHERE EXISTS (SELECT CASE\n"
                            + "	WHEN B.BPM_RCOLSP = 'N'\n"
                            + "	THEN\n"
                            + "	YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                            + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                            + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 1 MONTH) - 1))\n"
                            + "	WHEN B.BPM_RCOLSP = 'Y'\n"
                            + "	THEN \n"
                            + "	YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1)) * 10000\n"
                            + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1)) * 100\n"
                            + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPH_BPDT ,1,4)||'-'||SUBSTRING(A.BPH_BPDT ,5,2)||'-01') + 2 MONTH) - 1))\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A,BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO  = B.BPM_CONO AND A.BPH_CUNO  = B.BPM_CUNO\n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + "AND B.BPM_CASECOL = '10'\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                            + "AND SUBSTRING(C.BPH_STDT,5,2) =" + nextmonth + "";

                    querypayskipsunday = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_CLDT = (SELECT CASE\n"
                            + "	WHEN DAYOFWEEK(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) = 1\n"
                            + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')) + 1 DAY)* 10000\n"
                            + "	+ MONTH (DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)* 100\n"
                            + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)\n"
                            + "	ELSE YEAR(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) * 10000\n"
                            + "	+ MONTH(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')))* 100\n"
                            + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')))\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                            + "AND DAYOFWEEK( DATE(TIMESTAMP_FORMAT(cast(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) = 1\n"
                            + "AND B.BPM_RINV = " + invround + "\n"
                            + "AND SUBSTRING(BPH_STDT,1,4) = " + year + "\n"
                            + "AND SUBSTRING(BPH_STDT,5,2) = " + nextmonth + "\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "WHERE EXISTS (SELECT CASE\n"
                            + "	WHEN DAYOFWEEK(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) = 1\n"
                            + "	THEN YEAR(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')) + 1 DAY)* 10000\n"
                            + "	+ MONTH (DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)* 100\n"
                            + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY)\n"
                            + "	ELSE YEAR(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) * 10000\n"
                            + "	+ MONTH(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')))* 100\n"
                            + "	+ DAY(DATE(TIMESTAMP_FORMAT(CAST(BPH_CLDT as varchar(8)), 'YYYYMMDD')))\n"
                            + "END\n"
                            + "FROM BRLDTA0100.BP_HBILL A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPH_CONO = B.BPM_CONO AND A.BPH_CUNO = B.BPM_CUNO\n"
                            + "AND DAYOFWEEK( DATE(TIMESTAMP_FORMAT(cast(BPH_CLDT as varchar(8)), 'YYYYMMDD'))) = 1\n"
                            + "AND B.BPM_RINV = " + invround + "\n"
                            + "AND SUBSTRING(BPH_STDT,1,4) = " + year + "\n"
                            + "AND SUBSTRING(BPH_STDT,5,2) =" + nextmonth + "\n"
                            + "AND C.BPH_CONO = A.BPH_CONO AND A.BPH_CUNO = C.BPH_CUNO AND A.BPH_STDT = C.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT)\n"
                            + "AND SUBSTRING(C.BPH_STDT,1,4) =" + year + "\n"
                            + "AND SUBSTRING(C.BPH_STDT,5,2) =" + nextmonth;

                    querybillskipsunday = "UPDATE BRLDTA0100.BP_HBILL C\n"
                            + "SET C.BPH_BPDT = (SELECT CASE \n"
                            + "WHEN DAYOFWEEK(DATE(TIMESTAMP_FORMAT(cast(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))) = 1 THEN \n"
                            + " YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD')))) * 10000\n"
                            + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 DAYS)) * 100\n"
                            + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 DAYS))\n"
                            + "ELSE \n"
                            + "YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD')))) * 10000\n"
                            + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD')))) * 100\n"
                            + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))))\n"
                            + "END AS ENDDATE\n"
                            + "FROM BRLDTA0100.BP_HBILL A\n"
                            + "JOIN BRLDTA0100.BP_MASTER B\n"
                            + "ON B.BPM_CUNO = A.BPH_CUNO AND B.BPM_CONO = A.BPH_CONO\n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO AND C.BPH_CONO = A.BPH_CONO AND C.BPH_STDT = A.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT \n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + " FETCH FIRST 1 ROW ONLY)\n"
                            + " WHERE EXISTS(SELECT CASE \n"
                            + "WHEN DAYOFWEEK(DATE(TIMESTAMP_FORMAT(cast(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))) = 1 THEN \n"
                            + " YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD')))) * 10000\n"
                            + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 DAYS)) * 100\n"
                            + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD')) + 1 DAYS))\n"
                            + "ELSE \n"
                            + "YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD')))) * 10000\n"
                            + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_BPDT as varchar(8)), 'YYYYMMDD')))) * 100\n"
                            + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast(C.BPH_BPDT as varchar(8)), 'YYYYMMDD'))))\n"
                            + "END AS ENDDATE\n"
                            + "FROM BRLDTA0100.BP_HBILL A\n"
                            + "JOIN BRLDTA0100.BP_MASTER B\n"
                            + "ON B.BPM_CUNO = A.BPH_CUNO AND B.BPM_CONO = A.BPH_CONO\n"
                            + "AND C.BPH_CUNO = A.BPH_CUNO AND C.BPH_CONO = A.BPH_CONO AND C.BPH_STDT = A.BPH_STDT AND A.BPH_FNDT = C.BPH_FNDT \n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + " FETCH FIRST 1 ROW ONLY)\n"
                            + " AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPH_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;

                    stmt.execute(query1);
                    billnosunday.execute(querybillnosunday);
                    stmt6.execute(query6);
                    pay0.execute(querypay0);
                    stmtcasepay1.execute(querypaycase1);
                    stmtcasepay3.execute(querypaycase3);
                    stmtcasepay4.execute(querypaycase4);
                    stmtcasepay5.execute(querypaycase5);
                    stmtcasepay6.execute(querypaycase6);
                    stmtcasepay7.execute(querypaycase7);
                    stmtcasepay8.execute(querypaycase8);
                    stmtcasepay9.execute(querypaycase9);
                    stmtcasepay10.execute(querypaycase10);
                    stmtskipsundaypay.execute(querypayskipsunday);
                    billskipsunday.execute(querybillskipsunday);

                    respond = "generate successfully!";
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

    public static void Allocation(String cono, String divi, String code, String period, String type) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT A2STID,A2SLVL\n"
                        + "FROM BRLDTA0100.A2CASL\n"
                        + "WHERE A2CONO = '" + cono + "'\n"
                        + "AND A2DIVI = '" + divi + "'\n"
                        + "AND A2CODE = '" + code + "'\n"
                        + "ORDER BY A2CODE";
                System.out.println("SelectStruct.ID, Level\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Statement stmt2 = conn.createStatement();
                    String query2 = "INSERT INTO BRLDTA0100.E2ALLO\n"
                            + "SELECT A3CONO,A3DIVI,A3CODE,A3STID,A3SLVL,A3AITM," + period + ",A3MELA,0,A3UEHR,0,0,A3PERS,0,0,'00'," + type + "\n"
                            + "FROM BRLDTA0100.A3CAWC\n"
                            + "WHERE A3CONO = '" + cono + "'\n"
                            + "AND A3DIVI = '" + divi + "'\n"
                            + "AND A3CODE = '" + code + "'\n"
                            + "AND A3STID = '" + mRes.getString("A2STID").trim() + "'\n"
                            + "AND A3SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                            + "ORDER BY A3SLVL,A3AITM";
                    System.out.println("InsertAllocation\n" + query2);
                    stmt2.execute(query2);
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

    }

    public static void AllocationVariance(String cono, String divi, String code, String period, String type) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT A2STID,A2SLVL\n"
                        + "FROM BRLDTA0100.A2CASL\n"
                        + "WHERE A2CONO = '" + cono + "'\n"
                        + "AND A2DIVI = '" + divi + "'\n"
                        + "AND A2CODE = '" + code + "'\n"
                        + "ORDER BY A2CODE";
                System.out.println("SelectStruct.ID, Level\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Statement stmt2 = conn.createStatement();
                    String query2 = "INSERT INTO BRLDTA0100.E2ALLO\n"
                            + "SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,'" + period + "',E2METE,0,E2UEHR,0,0,E2PERS,0,0,'00','1'\n"
                            + "FROM BRLDTA0100.E2ALLO\n"
                            + "WHERE E2CONO = '" + cono + "'\n"
                            + "AND E2DIVI = '" + divi + "'\n"
                            + "AND E2CODE = '" + code + "'\n"
                            + "AND E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                            + "AND E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                            + "AND E2PECO = SUBSTRING(CHAR(DATE(SUBSTRING('" + period + "',0,5)||'-'||SUBSTRING('" + period + "',5,2)||'-'||'01') - 1 MONTH, ISO),0,5)||SUBSTRING(CHAR(DATE(SUBSTRING('" + period + "',0,5)||'-'||SUBSTRING('" + period + "',5,2)||'-'||'01') - 1 MONTH, ISO),6,2)\n"
                            + "AND E2TYPE = '0'";
                    System.out.println("InsertAllocationVariance\n" + query2);
                    stmt2.execute(query2);
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

    }

    public static void BillsAllocated(String cono, String divi, String code, String period, String struct, String level, String type) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT A2STID,A2SLVL\n"
                        + "FROM BRLDTA0100.A2CASL\n"
                        + "WHERE A2CONO = '" + cono + "'\n"
                        + "AND A2DIVI = '" + divi + "'\n"
                        + "AND A2CODE = '" + code + "'\n"
                        //                        + "AND A2STID = '" + struct + "'\n"
                        + "AND A2SLVL <= '" + level + "'\n"
                        + "ORDER BY A2CODE";
                System.out.println("SelectStruct.ID, Level\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Statement stmt2 = conn.createStatement();
                    String query2 = null;
                    ResultSet mRes2 = null;

                    if ("1".equals(mRes.getString("A2SLVL").trim())) {
                        query2 = "SELECT a.*,b.E2EPQT,c.E1RAAT,ROUND((a.E2EPQT / b.E2EPQT) * 100,2) AS USE_PERCENT,ROUND(b.E2EPQT * (((a.E2EPQT / b.E2EPQT) * 100) / 100),2) AS USE_QTY,ROUND(c.E1RAAT * (((a.E2EPQT / b.E2EPQT) * 100) / 100),2) AS USE_AMT\n"
                                + "FROM \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2PERS,E2EPQT,E2TYPE\n"
                                + "FROM BRLDTA0100.E2ALLO) AS a\n"
                                + "INNER JOIN \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2PECO,SUM(E2EPQT) AS E2EPQT,E2TYPE\n"
                                + "FROM BRLDTA0100.E2ALLO\n"
                                + "GROUP BY E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2PECO,E2TYPE) AS b\n"
                                + "ON b.E2CONO = a.E2CONO\n"
                                + "AND b.E2DIVI = a.E2DIVI\n"
                                + "AND b.E2CODE = a.E2CODE\n"
                                + "AND b.E2STID = a.E2STID\n"
                                + "AND b.E2SLVL = a.E2SLVL\n"
                                + "AND b.E2PECO = a.E2PECO\n"
                                + "AND b.E2TYPE = a.E2TYPE\n"
                                + "INNER JOIN\n"
                                + "(SELECT E1CONO,E1DIVI,E1CODE,E1PECO,E1METS,E1METE,E1EPQT,E1EPAT,E1EVAT,E1ETAT,E1RAAT,E1TYPE\n"
                                + "FROM BRLDTA0100.E1ALLO) AS c\n"
                                + "ON c.E1CONO = a.E2CONO\n"
                                + "AND c.E1DIVI = a.E2DIVI\n"
                                + "AND c.E1CODE = a.E2CODE\n"
                                + "AND c.E1PECO = a.E2PECO\n"
                                + "AND c.E1TYPE = a.E2TYPE\n"
                                + "WHERE a.E2CONO = '" + cono + "'\n"
                                + "AND a.E2DIVI = '" + divi + "'\n"
                                + "AND a.E2CODE = '" + code + "'\n"
                                + "AND a.E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                + "AND a.E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                + "AND a.E2PECO = '" + period + "'\n"
                                + "AND a.E2EPQT > 0\n"
                                + "AND a.E2TYPE = '" + type + "'";
                        System.out.println("SelectBillsAllocated Level = 1\n" + query2);
                        mRes2 = stmt2.executeQuery(query2);

                        while (mRes2.next()) {

                            Statement stmt3 = conn.createStatement();
                            String query3 = "UPDATE BRLDTA0100.E2ALLO\n"
                                    + "SET E2PERS = '" + mRes2.getString("USE_PERCENT").trim() + "'\n"
                                    + ",E2EAQT = '" + mRes2.getString("USE_QTY").trim() + "'\n"
                                    + ",E2EAAT = '" + mRes2.getString("USE_AMT").trim() + "'\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    + "AND E2AITM = '" + mRes2.getString("E2AITM").trim() + "'\n"
                                    + "AND E2TYPE = '" + type + "'";
                            System.out.println("UpdateBillsAllocated\n" + query3);
                            stmt3.execute(query3);

                        }

                        Statement stmt4 = conn.createStatement();
                        String query4 = "SELECT TOTAL_QTY,TOTAL_AMT,SUM(USE_QTY) AS USE_QTY,SUM(USE_AMT) AS USE_AMT,ROUND(TOTAL_QTY - SUM(USE_QTY),2) AS SUM_QTY,ROUND(TOTAL_AMT - SUM(USE_AMT),2) AS SUM_AMT\n"
                                + "FROM\n"
                                + "(SELECT a.E2CODE,a.E2PECO,b.E1EPQT AS TOTAL_QTY,b.E1RAAT AS TOTAL_AMT,ROUND((a.E2EPQT / b.E1EPQT) * b.E1EPQT,2) AS USE_QTY,ROUND((a.E2EPQT / b.E1EPQT) * E1RAAT,2) AS USE_AMT\n"
                                + "FROM \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2PERS,E2EPQT,E2TYPE\n"
                                + "FROM BRLDTA0100.E2ALLO) AS a\n"
                                + "INNER JOIN\n"
                                + "(SELECT E1CONO,E1DIVI,E1CODE,E1PECO,E1METS,E1METE,E1EPQT,E1EPAT,E1EVAT,E1ETAT,E1RAAT,E1TYPE\n"
                                + "FROM BRLDTA0100.E1ALLO) AS b\n"
                                + "ON b.E1CONO = a.E2CONO\n"
                                + "AND b.E1DIVI = a.E2DIVI\n"
                                + "AND b.E1CODE = a.E2CODE\n"
                                + "AND b.E1PECO = a.E2PECO\n"
                                + "AND b.E1TYPE = a.E2TYPE\n"
                                + "WHERE a.E2CONO = '" + cono + "'\n"
                                + "AND a.E2DIVI = '" + divi + "'\n"
                                + "AND a.E2CODE = '" + code + "'\n"
                                + "AND a.E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                + "AND a.E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                + "AND a.E2PECO = '" + period + "'\n"
                                + "AND a.E2PERS > 0\n"
                                + "AND a.E2TYPE = '" + type + "') AS a\n"
                                + "GROUP BY TOTAL_QTY,TOTAL_AMT";
                        System.out.println("SelectDiffAllocated\n" + query4);
                        ResultSet mRes3 = stmt4.executeQuery(query4);

                        while (mRes3.next()) {
                            Statement stmt5 = conn.createStatement();
                            String query5 = "UPDATE BRLDTA0100.E2ALLO\n"
                                    + "SET E2EPQT = E2EPQT + " + mRes3.getString("SUM_QTY").trim() + "\n"
                                    + ",E2EAQT = E2EAQT + " + mRes3.getString("SUM_QTY").trim() + "\n"
                                    + ",E2EAAT = E2EAAT + " + mRes3.getString("SUM_AMT").trim() + "\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                    + "AND E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    + "AND E2EAQT > 0\n"
                                    + "AND E2AITM = (SELECT MAX(E2AITM) \n"
                                    + "FROM BRLDTA0100.E2ALLO\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                    + "AND E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    + "AND E2EAQT > 0\n"
                                    + "AND E2TYPE = '" + type + "')\n"
                                    + "AND E2TYPE = '" + type + "'";
                            System.out.println("UpdateDiffAllocated\n" + query5);
                            stmt5.execute(query5);
                        }

                        mRes3.close();

                    } else {

                        query2 = "SELECT a.*,b.E2EPQT,b.E2EAAT,CASE WHEN a.E2CODE = '300' THEN ROUND(b.E2EPQT * (a.E2PERS / 100),4) ELSE ROUND(b.E2EPQT * (a.E2PERS / 100),2) END AS USE_QTY\n"
                                + ",CASE WHEN a.E2CODE = '300' THEN ROUND(b.E2EAAT * (a.E2PERS / 100),4) ELSE ROUND(b.E2EAAT * (a.E2PERS / 100),2) END AS USE_AMT\n"
                                + "FROM \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2PERS,E2EPQT,E2TYPE\n"
                                + "FROM BRLDTA0100.E2ALLO) AS a\n"
                                + "INNER JOIN \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2EPQT,E2EAAT,E2TYPE\n"
                                + "FROM BRLDTA0100.E2ALLO) AS b\n"
                                + "ON b.E2CONO = a.E2CONO\n"
                                + "AND b.E2DIVI = a.E2DIVI\n"
                                + "AND b.E2CODE = a.E2CODE\n"
                                + "AND b.E2AITM = a.E2STID\n"
                                + "AND b.E2PECO = a.E2PECO\n"
                                + "AND b.E2TYPE = a.E2TYPE\n"
                                + "WHERE a.E2CONO = '" + cono + "'\n"
                                + "AND a.E2DIVI = '" + divi + "'\n"
                                + "AND a.E2CODE = '" + code + "'\n"
                                + "AND a.E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                + "AND a.E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                + "AND a.E2PECO = '" + period + "'\n"
                                + "AND a.E2PERS > 0\n"
                                + "AND a.E2TYPE = '" + type + "'";
                        System.out.println("SelectBillsAllocated Level > 1\n" + query2);
                        mRes2 = stmt2.executeQuery(query2);

                        while (mRes2.next()) {

                            Statement stmt3 = conn.createStatement();
                            String query3 = "UPDATE BRLDTA0100.E2ALLO\n"
                                    + "SET E2EPQT = '" + mRes2.getString("USE_QTY").trim() + "'\n"
                                    + ",E2EAQT = '" + mRes2.getString("USE_QTY").trim() + "'\n"
                                    + ",E2EAAT = '" + mRes2.getString("USE_AMT").trim() + "'\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    + "AND E2AITM = '" + mRes2.getString("E2AITM").trim() + "'\n"
                                    + "AND E2TYPE = '" + type + "'";
                            System.out.println("UpdateBillsAllocated\n" + query3);
                            stmt3.execute(query3);

                        }

                        Statement stmt4 = conn.createStatement();
                        String query4 = "SELECT TOTAL_QTY,TOTAL_AMT,SUM(USE_QTY) AS USE_QTY,SUM(USE_AMT) AS USE_AMT,ROUND(TOTAL_QTY - SUM(USE_QTY),2) AS SUM_QTY,ROUND(TOTAL_AMT - SUM(USE_AMT),2) AS SUM_AMT\n"
                                + "FROM\n"
                                + "(SELECT a.E2CODE,a.E2PECO,b.E2EPQT AS TOTAL_QTY,b.E2EAAT AS TOTAL_AMT,CAST(ROUND(b.E2EPQT * (a.E2PERS / 100),2) AS DECIMAL(15,2)) AS USE_QTY,CAST(ROUND(b.E2EAAT * (a.E2PERS / 100),2) AS DECIMAL(15,2)) AS USE_AMT\n"
                                + "FROM \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2PERS,E2EPQT,E2TYPE\n"
                                + "FROM BRLDTA0100.E2ALLO) AS a\n"
                                + "INNER JOIN \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2EPQT,E2EAAT,E2TYPE\n"
                                + "FROM BRLDTA0100.E2ALLO) AS b\n"
                                + "ON b.E2CONO = a.E2CONO\n"
                                + "AND b.E2DIVI = a.E2DIVI\n"
                                + "AND b.E2CODE = a.E2CODE\n"
                                + "AND b.E2AITM = a.E2STID\n"
                                + "AND b.E2PECO = a.E2PECO\n"
                                + "AND b.E2TYPE = a.E2TYPE\n"
                                + "WHERE a.E2CONO = '" + cono + "'\n"
                                + "AND a.E2DIVI = '" + divi + "'\n"
                                + "AND a.E2CODE = '" + code + "'\n"
                                + "AND a.E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                + "AND a.E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                + "AND a.E2PECO = '" + period + "'\n"
                                + "AND a.E2PERS > 0\n"
                                + "AND a.E2TYPE = '" + type + "') AS a\n"
                                + "GROUP BY TOTAL_QTY,TOTAL_AMT";
                        System.out.println("SelectDiffAllocated\n" + query4);
                        ResultSet mRes3 = stmt4.executeQuery(query4);

                        while (mRes3.next()) {
                            Statement stmt5 = conn.createStatement();
                            String query5 = "UPDATE BRLDTA0100.E2ALLO\n"
                                    + "SET E2EPQT = E2EPQT + " + mRes3.getString("SUM_QTY").trim() + "\n"
                                    + ",E2EAQT = E2EAQT + " + mRes3.getString("SUM_QTY").trim() + "\n"
                                    + ",E2EAAT = E2EAAT + " + mRes3.getString("SUM_AMT").trim() + "\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                    + "AND E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    + "AND E2EAQT > 0\n"
                                    + "AND E2AITM = (SELECT MAX(E2AITM) \n"
                                    + "FROM BRLDTA0100.E2ALLO\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                    + "AND E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    + "AND E2EAQT > 0\n"
                                    + "AND E2TYPE = '" + type + "')\n"
                                    + "AND E2TYPE = '" + type + "'";
                            System.out.println("UpdateDiffAllocated\n" + query5);
                            stmt5.execute(query5);
                        }

                        mRes3.close();

                    }

                    mRes2.close();

                }

                mRes.close();

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

    public static void BillsAllocatedVariance(String cono, String divi, String code, String period, String level, String type) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT A2STID,A2SLVL\n"
                        + "FROM BRLDTA0100.A2CASL\n"
                        + "WHERE A2CONO = '" + cono + "'\n"
                        + "AND A2DIVI = '" + divi + "'\n"
                        + "AND A2CODE = '" + code + "'\n"
                        + "AND A2SLVL <= '" + level + "'\n"
                        + "ORDER BY A2CODE";
                System.out.println("SelectStruct.ID, Level\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Statement stmt2 = conn.createStatement();
                    String query2 = null;
                    ResultSet mRes2 = null;

                    if ("1".equals(mRes.getString("A2SLVL").trim())) {
                        query2 = "SELECT a.*,b.E2EPQT,c.E1RAAT,a.E2PERS AS USE_PERCENT,0 AS USE_QTY,ROUND((a.E2PERS * c.E1RAAT) / 100,2) AS USE_AMT\n"
                                + "FROM \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2PERS,E2EPQT,E2TYPE\n"
                                + "FROM BRLDTA0100.E2ALLO) AS a\n"
                                + "INNER JOIN \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2PECO,SUM(E2EPQT) AS E2EPQT\n"
                                + "FROM BRLDTA0100.E2ALLO\n"
                                + "GROUP BY E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2PECO) AS b\n"
                                + "ON b.E2CONO = a.E2CONO\n"
                                + "AND b.E2DIVI = a.E2DIVI\n"
                                + "AND b.E2CODE = a.E2CODE\n"
                                + "AND b.E2STID = a.E2STID\n"
                                + "AND b.E2SLVL = a.E2SLVL\n"
                                + "AND b.E2PECO = a.E2PECO\n"
                                + "INNER JOIN\n"
                                + "(SELECT E1CONO,E1DIVI,E1CODE,E1PECO,E1METS,E1METE,E1EPQT,E1EPAT,E1EVAT,E1ETAT,E1RAAT\n"
                                + "FROM BRLDTA0100.E1ALLO) AS c\n"
                                + "ON c.E1CONO = a.E2CONO\n"
                                + "AND c.E1DIVI = a.E2DIVI\n"
                                + "AND c.E1CODE = a.E2CODE\n"
                                + "AND c.E1PECO = a.E2PECO\n"
                                + "WHERE a.E2CONO = '" + cono + "'\n"
                                + "AND a.E2DIVI = '" + divi + "'\n"
                                + "AND a.E2CODE = '" + code + "'\n"
                                + "AND a.E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                + "AND a.E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                + "AND a.E2PECO = '" + period + "'\n"
                                //                                + "AND a.E2EPQT > 0\n"
                                + "AND a.E2TYPE = '" + type + "'";
                        System.out.println("SelectBillsAllocatedVariance Level = 1\n" + query2);
                        mRes2 = stmt2.executeQuery(query2);

                        while (mRes2.next()) {

                            Statement stmt3 = conn.createStatement();
                            String query3 = "UPDATE BRLDTA0100.E2ALLO\n"
                                    + "SET E2PERS = '" + mRes2.getString("USE_PERCENT").trim() + "'\n"
                                    + ",E2EAQT = '" + mRes2.getString("USE_QTY").trim() + "'\n"
                                    + ",E2EAAT = '" + mRes2.getString("USE_AMT").trim() + "'\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    + "AND E2AITM = '" + mRes2.getString("E2AITM").trim() + "'\n"
                                    + "AND E2TYPE = '" + type + "'";
                            System.out.println("UpdateBillsAllocatedVariance\n" + query3);
                            stmt3.execute(query3);

                        }

                        Statement stmt4 = conn.createStatement();
                        String query4 = "SELECT TOTAL_QTY,TOTAL_AMT,SUM(USE_QTY) AS USE_QTY,SUM(USE_AMT) AS USE_AMT,ROUND(TOTAL_QTY - SUM(USE_QTY),2) AS SUM_QTY,ROUND(TOTAL_AMT - SUM(USE_AMT),2) AS SUM_AMT\n"
                                + "FROM\n"
                                + "(SELECT a.E2CODE,a.E2PECO,b.E1EPQT AS TOTAL_QTY,b.E1RAAT AS TOTAL_AMT,0 AS USE_QTY,ROUND((a.E2PERS * E1RAAT) / 100,2) AS USE_AMT\n"
                                + "FROM \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2PERS,E2EPQT,E2TYPE\n"
                                + "FROM BRLDTA0100.E2ALLO) AS a\n"
                                + "INNER JOIN\n"
                                + "(SELECT E1CONO,E1DIVI,E1CODE,E1PECO,E1METS,E1METE,E1EPQT,E1EPAT,E1EVAT,E1ETAT,E1RAAT\n"
                                + "FROM BRLDTA0100.E1ALLO) AS b\n"
                                + "ON b.E1CONO = a.E2CONO\n"
                                + "AND b.E1DIVI = a.E2DIVI\n"
                                + "AND b.E1CODE = a.E2CODE\n"
                                + "AND b.E1PECO = a.E2PECO\n"
                                + "WHERE a.E2CONO = '" + cono + "'\n"
                                + "AND a.E2DIVI = '" + divi + "'\n"
                                + "AND a.E2CODE = '" + code + "'\n"
                                + "AND a.E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                + "AND a.E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                + "AND a.E2PECO = '" + period + "'\n"
                                + "AND a.E2PERS > 0\n"
                                + "AND a.E2TYPE = '" + type + "') AS a\n"
                                + "GROUP BY TOTAL_QTY,TOTAL_AMT";
                        System.out.println("SelectDiffAllocatedVariance\n" + query4);
                        ResultSet mRes3 = stmt4.executeQuery(query4);

                        while (mRes3.next()) {
                            Statement stmt5 = conn.createStatement();
                            String query5 = "UPDATE BRLDTA0100.E2ALLO\n"
                                    + "SET E2EPQT = E2EPQT + " + mRes3.getString("SUM_QTY").trim() + "\n"
                                    + ",E2EAQT = E2EAQT + " + mRes3.getString("SUM_QTY").trim() + "\n"
                                    + ",E2EAAT = E2EAAT + " + mRes3.getString("SUM_AMT").trim() + "\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                    + "AND E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    //+ "AND E2EAQT > 0\n"
                                    + "AND E2AITM = (SELECT MAX(E2AITM) \n"
                                    + "FROM BRLDTA0100.E2ALLO\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                    + "AND E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    //+ "AND E2EAQT > 0\n"
                                    + ")\n"
                                    + "AND E2TYPE = '" + type + "'";
                            System.out.println("UpdateDiffAllocatedVariance\n" + query5);
                            stmt5.execute(query5);
                        }

                        mRes3.close();

                    } else {

                        query2 = "SELECT a.*,b.E2EPQT,b.E2EAAT,ROUND(b.E2EPQT * (a.E2PERS / 100),2) AS USE_QTY,ROUND(b.E2EAAT * (a.E2PERS / 100),2) AS USE_AMT\n"
                                + "FROM \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2PERS,E2EPQT,E2TYPE\n"
                                + "FROM BRLDTA0100.E2ALLO) AS a\n"
                                + "INNER JOIN \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2EPQT,E2EAAT\n"
                                + "FROM BRLDTA0100.E2ALLO) AS b\n"
                                + "ON b.E2CONO = a.E2CONO\n"
                                + "AND b.E2DIVI = a.E2DIVI\n"
                                + "AND b.E2CODE = a.E2CODE\n"
                                + "AND b.E2AITM = a.E2STID\n"
                                + "AND b.E2PECO = a.E2PECO\n"
                                + "WHERE a.E2CONO = '" + cono + "'\n"
                                + "AND a.E2DIVI = '" + divi + "'\n"
                                + "AND a.E2CODE = '" + code + "'\n"
                                + "AND a.E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                + "AND a.E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                + "AND a.E2PECO = '" + period + "'\n"
                                + "AND a.E2PERS > 0\n"
                                + "AND a.E2TYPE = '" + type + "'";
                        System.out.println("SelectBillsAllocated Level > 1\n" + query2);
                        mRes2 = stmt2.executeQuery(query2);

                        while (mRes2.next()) {

                            Statement stmt3 = conn.createStatement();
                            String query3 = "UPDATE BRLDTA0100.E2ALLO\n"
                                    + "SET E2EPQT = '" + mRes2.getString("USE_QTY").trim() + "'\n"
                                    + ",E2EAQT = '" + mRes2.getString("USE_QTY").trim() + "'\n"
                                    + ",E2EAAT = '" + mRes2.getString("USE_AMT").trim() + "'\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    + "AND E2AITM = '" + mRes2.getString("E2AITM").trim() + "'\n"
                                    + "AND E2TYPE = '" + type + "'";
                            System.out.println("UpdateBillsAllocatedVariance\n" + query3);
                            stmt3.execute(query3);

                        }

                        Statement stmt4 = conn.createStatement();
                        String query4 = "SELECT TOTAL_QTY,TOTAL_AMT,SUM(USE_QTY) AS USE_QTY,SUM(USE_AMT) AS USE_AMT,ROUND(TOTAL_QTY - SUM(USE_QTY),2) AS SUM_QTY,ROUND(TOTAL_AMT - SUM(USE_AMT),2) AS SUM_AMT\n"
                                + "FROM\n"
                                + "(SELECT a.E2CODE,a.E2PECO,b.E2EPQT AS TOTAL_QTY,b.E2EAAT AS TOTAL_AMT,CAST(ROUND(b.E2EPQT * (a.E2PERS / 100),2) AS DECIMAL(15,2)) AS USE_QTY,CAST(ROUND(b.E2EAAT * (a.E2PERS / 100),2) AS DECIMAL(15,2)) AS USE_AMT\n"
                                + "FROM \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2PERS,E2EPQT,E2TYPE\n"
                                + "FROM BRLDTA0100.E2ALLO) AS a\n"
                                + "INNER JOIN \n"
                                + "(SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,E2PECO,E2EPQT,E2EAAT\n"
                                + "FROM BRLDTA0100.E2ALLO) AS b\n"
                                + "ON b.E2CONO = a.E2CONO\n"
                                + "AND b.E2DIVI = a.E2DIVI\n"
                                + "AND b.E2CODE = a.E2CODE\n"
                                + "AND b.E2AITM = a.E2STID\n"
                                + "AND b.E2PECO = a.E2PECO\n"
                                + "WHERE a.E2CONO = '" + cono + "'\n"
                                + "AND a.E2DIVI = '" + divi + "'\n"
                                + "AND a.E2CODE = '" + code + "'\n"
                                + "AND a.E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                + "AND a.E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                + "AND a.E2PECO = '" + period + "'\n"
                                + "AND a.E2PERS > 0\n"
                                + "AND E2TYPE = '" + type + "') AS a\n"
                                + "GROUP BY TOTAL_QTY,TOTAL_AMT";
                        System.out.println("SelectDiffAllocatedVariance\n" + query4);
                        ResultSet mRes3 = stmt4.executeQuery(query4);

                        while (mRes3.next()) {
                            Statement stmt5 = conn.createStatement();
                            String query5 = "UPDATE BRLDTA0100.E2ALLO\n"
                                    + "SET E2EPQT = E2EPQT + " + mRes3.getString("SUM_QTY").trim() + "\n"
                                    + ",E2EAQT = E2EAQT + " + mRes3.getString("SUM_QTY").trim() + "\n"
                                    + ",E2EAAT = E2EAAT + " + mRes3.getString("SUM_AMT").trim() + "\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                    + "AND E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    //+ "AND E2EAQT > 0\n"
                                    + "AND E2AITM = (SELECT MAX(E2AITM) \n"
                                    + "FROM BRLDTA0100.E2ALLO\n"
                                    + "WHERE E2CONO = '" + cono + "'\n"
                                    + "AND E2DIVI = '" + divi + "'\n"
                                    + "AND E2CODE = '" + code + "'\n"
                                    + "AND E2STID = '" + mRes.getString("A2STID").trim() + "'\n"
                                    + "AND E2SLVL = '" + mRes.getString("A2SLVL").trim() + "'\n"
                                    + "AND E2PECO = '" + period + "'\n"
                                    //+ "AND E2EAQT > 0\n"
                                    + "AND E2TYPE = '" + type + "')";
                            System.out.println("UpdateDiffAllocatedVariance\n" + query5);
                            stmt5.execute(query5);
                        }

                        mRes3.close();

                    }

                    mRes2.close();

                }

                mRes.close();

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

    public static JSONArray insertfinancemaster(String company, String customerid, String customertype, String roundinv, String roundbill, String roundpay) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "INSERT INTO BRLDTA0100.BP_MASTER (BPM_CONO,BPM_CUNO,BPM_TYPE,BPM_RINV,BPM_RBIL,BPM_RCOL)\n"
                        + "VALUES ('" + company
                        + "','" + customerid
                        + "','" + customertype
                        + "','" + roundinv
                        + "','" + roundbill
                        + "','" + roundpay + "')";
                System.out.println("insertfinancemaster\n" + query);
                stmt.execute(query);

                Map<String, Object> mMap = new HashMap<>();
                mMap.put("result", "ok");
                mMap.put("message", "insert complete");
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

    public static String insertfinanceheader(String company, String customerid, String startdate, String month, String year, String status, String invround) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();
        String query1 = "";
        String query2 = "";
        String query3 = "";
        String query4 = "";
        String query5 = "";
        String respond = "";
        String check = "0";
        String nextmonth = Integer.toString(parseInt(month) + 1);
        if ("13".equals(nextmonth)) {
            nextmonth = "1";
            year = Integer.toString(parseInt(year) + 1);
        };
        try {
            if (conn != null) {
                Statement stmt = conn.createStatement();
                Statement stmt2 = conn.createStatement();
                Statement stmt3 = conn.createStatement();
                Statement stmt4 = conn.createStatement();
                Statement stmt5 = conn.createStatement();
                for (int i = 1; i <= 6; i++) {
                    if (i == 1) {
                        query1 = "INSERT INTO BRLDTA0100.BP_STDATE(BPS_CONO,BPS_CUNO,BPS_STDT,BPS_RD,BPS_STS)\n"
                                + "VALUES ('" + company + "', '" + customerid + "','" + startdate + "','1','00')";
                        System.out.println("InsertFacility\n" + query1);
                    } else {
                        query1 = "INSERT INTO BRLDTA0100.BP_STDATE(BPS_CONO,BPS_CUNO,BPS_STDT,BPS_STS)\n"
                                + "SELECT DISTINCT BPS_CONO,BPS_CUNO, YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAYS )) * 10000\n"
                                + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( BPS_FNDT as varchar(8)), 'YYYYMMDD') + 1 DAY))) * 100\n"
                                + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast( BPS_FNDT as varchar(8)), 'YYYYMMDD') + 1 DAY)))  AS ENDDATE,'00'\n"
                                + "FROM BRLDTA0100.BP_STDATE\n"
                                + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_FNDT as varchar(8)), 'YYYYMMDD'))+ 1 DAY) = MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD')))\n"
                                + "AND BPS_RD = (SELECT MAX(BPS_RD) FROM BRLDTA0100.BP_STDATE WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + month + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + year + " AND BPS_CUNO  = '" + customerid + "') \n"
                                + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + month + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + year + "\n"
                                + "AND BPS_CUNO  = '" + customerid + "'";
                        System.out.println("InsertFacility\n" + query1);
                    }
//                    UPDATE END DATE OF START DATE TABLE USING MASTER TABLE
                    query2 = "UPDATE BRLDTA0100.BP_STDATE C\n"
                            + "SET C.BPS_FNDT = (SELECT YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 10000\n"
                            + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 100\n"
                            + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS))  AS ENDDATE\n"
                            + "FROM BRLDTA0100.BP_STDATE A\n"
                            + "JOIN BRLDTA0100.BP_MASTER B	\n"
                            + "ON B.BPM_CUNO = A.BPS_CUNO\n"
                            + "AND B.BPM_CONO = A.BPS_CONO\n"
                            + "AND C.BPS_CUNO = A.BPS_CUNO\n"
                            + "AND C.BPS_CONO = A.BPS_CONO\n"
                            + "AND B.BPM_RINV =" + invround + "\n"
                            + " FETCH FIRST 1 ROW ONLY)\n"
                            + " WHERE  MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + month + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(C.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + year + "\n"
                            + "AND C.BPS_CUNO = '" + customerid + "'";

                    //Create period
                    query3 = "UPDATE BRLDTA0100.BP_STDATE\n"
                            + "SET BPS_PR = SUBSTRING(BPS_STDT,3,8) || SUBSTRING(BPS_FNDT,3,8)\n"
                            + "WHERE  MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + month + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                    //Create round date
                    query4 = "UPDATE BRLDTA0100.BP_STDATE T1\n"
                            + "SET BPS_RD = (\n"
                            + "SELECT COUNT(*)\n"
                            + "FROM BRLDTA0100.BP_STDATE T2\n"
                            + "WHERE T2.BPS_CUNO=T1.BPS_CUNO\n"
                            + "AND DATE(TIMESTAMP_FORMAT(cast( T2 .BPS_STDT as varchar(8)), 'YYYYMMDD')) <=  DATE(TIMESTAMP_FORMAT(cast( T1 .BPS_STDT as varchar(8)), 'YYYYMMDD')) \n"
                            + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T2.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = " + month + " \n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + " \n"
                            + "GROUP BY T2.BPS_CUNO )\n"
                            + "WHERE MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T1.BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + month + " \n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                    stmt.execute(query1);
                    stmt2.execute(query2);
                    stmt3.execute(query3);
                    stmt4.execute(query4);
                }
                respond = "Month Generate Successfully";

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
