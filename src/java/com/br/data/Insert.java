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
                                        + "	THEN YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                        + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                        + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1))\n"
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
                                        + "	THEN YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                        + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                        + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1))\n"
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

    public static String addMaster(String company, String divi, String customer,
            String invroundinput, String invroundA, String type, String bill, String bilweek1, String bilinput1, String bilinput3,
            String bilfirstweek, String pay, String pay5weekornot, String payweek1, String payweek2, String payday1, String payday2,
            String payday3, String payday4, String paydaysamemonthornot, String bildesc, String collectiondesc,
            String collectby, String remark, String mode) throws Exception {
        Connection conn = ConnectDB2.ConnectionDB();
        String query1 = "";
        String query4 = "";
        String respond = "";
        String check = "0";
        String Invroundspecial = "0";
        String bilspecial = "0";
        String payspecial = "0";
        if (mode.equals("create")) {
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
                invroundA = "30";
            } else if (invroundinput.equals("2")) {
                Invroundspecial = "2";
                invroundA = "30";
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
        }
        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                Statement stmt4 = conn.createStatement();
                if (mode.equals("create")) {

                    query4 = "SELECT BPM_CUNO\n"
                            + "FROM BRLDTA0100.BP_MASTER A-- Master DATA\n"
                            + "WHERE BPM_CUNO = '" + customer + "'\n"
                            + "AND BPM_CONO = " + company + "\n"
                            + "AND BPM_DIVI = " + divi + "";
                    ResultSet mRes = stmt4.executeQuery(query4);
                    while (mRes.next()) {
                        if (customer.equals(mRes.getString(1))) {
                            check = "1";
                            respond = "data already generated";
                        }
                    }
                    if (check.equals("0")) {
                        //IMPORT NEW DATA
                        query1 = "INSERT INTO BRLDTA0100.BP_MASTER(BPM_CONO,BPM_DIVI,BPM_CUNO,BPM_TYPE,BPM_RINV,BPM_RBIL,BPM_RCOL,BPM_CASEBIL,BPM_CASECOL,BPM_RBILSP,BPM_RCOLSP,BPM_CASEINV,BPM_BILDESC,BPM_COLDESC,BPM_COLBY,BPM_REMARK)\n"
                                + "values(" + company + "," + divi + ",'" + customer + "'," + type + ",'" + invroundA + "','" + bilinput1 + "','" + payday1 + "','" + bill + "','" + pay + "','" + bilspecial + "','" + payspecial + "','" + Invroundspecial + "','" + bildesc + "','" + collectiondesc + "','" + collectby + "','" + remark + "')";

                        stmt.execute(query1);
                        respond = "generate successfully!";

                    }
                } else if (mode.equals("remark")) {

                    query4 = "SELECT COUNT(BPM_CUNO) \n"
                            + "FROM BRLDTA0100.BP_MASTER A-- Master DATA\n"
                            + "WHERE BPM_CUNO = '" + customer + "'\n"
                            + "AND BPM_CONO = '" + company + "'\n"
                            + "AND BPM_DIVI = '" + divi + "'";
                    ResultSet mRes2 = stmt4.executeQuery(query4);
                    while (mRes2.next()) {
                        if ("0".equals(mRes2.getString(1))) {
                            check = "1";
                            respond = "No customer Found";
                        }
                    }
                    if (check.equals("0")) {
                        //IMPORT NEW DATA
                        query1 = "UPDATE BRLDTA0100.BP_MASTER\n"
                                + "SET BPM_REMARK = '" + remark + "'\n"
                                + "WHERE BPM_CUNO = '" + customer + "'\n"
                                + "AND BPM_CONO = '" + company + "'\n"
                                + "AND BPM_DIVI = '" + divi + "'";

                        stmt.execute(query1);
                        respond = "change remark successfully!";
                    }
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

    public static String importInvoiceData(String month, String year, String invround, String cono) throws Exception {

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
                        + "AND A.BPS_CONO =" + cono + "\n"
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
                                    + "AND B.BPM_CONO =" + cono + "\n"
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
                                    + "AND D.BPM_CONO =" + cono + "\n"
                                    + "AND D.BPM_RINV = " + invround + ")\n"
                                    + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + nextmonth + "\n"
                                    + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                                    + "AND B.BPM_CONO =" + cono + "\n"
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
                                    + "AND B.BPM_CONO =" + cono + "\n"
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
                                    + "AND B.BPM_CONO =" + cono + "\n"
                                    + "AND B.BPM_CASEINV = 0 \n"
                                    + " FETCH FIRST 1 ROW ONLY)\n"
                                    + "AND C.BPS_CONO =" + cono + "\n"
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
                                    + "AND B.BPM_CASEINV = 0\n"
                                    + "AND B.BPM_CONO =" + cono + ")\n"
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
                                    + "AND B.BPM_CASEINV = 0 \n"
                                    + "AND B.BPM_CONO =" + cono + ")\n"
                                    + "AND C.BPS_CONO =" + cono + "\n"
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
                                    + "AND B.BPM_CONO=" + cono + "\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                    + "WHERE EXISTS (SELECT YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 10000\n"
                                    + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1)) * 100\n"
                                    + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT,5,2)||'-01') + 1 MONTH) - 1))\n"
                                    + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                    + "WHERE A.BPS_CONO = B.BPM_CONO AND A.BPS_CUNO = B.BPM_CUNO\n"
                                    + "AND B.BPM_RINV = 30\n"
                                    + "AND B.BPM_CASEINV = 0\n"
                                    + "AND B.BPM_CONO= " + cono + "\n"
                                    + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                    + "AND C.BPS_CONO =" + cono + "\n"
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
                                + "AND A.BPS_CONO =" + cono + "\n"
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
                                + "AND B.BPM_CONO =" + cono + "\n"
                                + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                + "AND C.BPS_CONO =" + cono + "\n"
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
                                + "AND B.BPM_CONO =" + cono + "\n"
                                + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                + "WHERE EXISTS (SELECT \n"
                                + "YEAR (DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2)) * 10000\n"
                                + "	+ MONTH(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2)) * 100\n"
                                + "	+ DAY(DATE(DAYS(DATE(SUBSTRING(A.BPS_STDT,1,4)||'-'||SUBSTRING(A.BPS_STDT ,5,2)||'-01') + 2 MONTH) - 2))\n"
                                + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                                + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + nextmonth + "\n"
                                + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                                + "AND A.BPS_CONO =" + cono + "\n"
                                + "AND B.BPM_CONO = A.BPS_CONO AND B.BPM_CUNO  = A.BPS_CUNO\n"
                                + "AND SUBSTRING(B.BPM_CASEINV,1,2) = '2'\n"
                                + "AND B.BPM_RINV = 30\n"
                                + "AND C.BPS_CONO = A.BPS_CONO AND c.BPS_CUNO = A.BPS_CUNO AND A.BPS_STDT = C.BPS_STDT)\n"
                                + "AND C.BPS_CONO =" + cono + "\n"
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
                                + "AND B.BPM_CONO =" + cono + "\n"
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
                                + "AND B.BPM_CONO =" + cono + "\n"
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
                                + "AND T3.BPM_CONO =" + cono + "\n"
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
                                + "AND T3.BPM_CONO =" + cono + "\n"
                                + "AND T3.BPM_RINV =" + invround + "\n"
                                + "GROUP BY T2.BPS_CUNO)\n"
                                + "AND T1.BPS_CONO =" + cono + "\n"
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
                            + "	ELSE BPH_CLDT\n"
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
                            + "	ELSE BPH_CLDT\n"
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

    public static JSONArray insertfinancemaster(String company, String divi, String customerid, String customertype, String roundinv, String roundbill, String roundpay) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "INSERT INTO BRLDTA0100.BP_MASTER (BPM_CONO,BPM_DIVI,BPM_CUNO,BPM_TYPE,BPM_RINV,BPM_RBIL,BPM_RCOL)\n"
                        + "VALUES ('" + company
                        + "','" + divi
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
                                + "AND BPS_CONO =" + company + "\n"
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
                            + "AND C.BPS_CONO =" + company + "\n"
                            + "AND C.BPS_CUNO = '" + customerid + "'";

                    //Create period
                    query3 = "UPDATE BRLDTA0100.BP_STDATE\n"
                            + "SET BPS_PR = SUBSTRING(BPS_STDT,3,8) || SUBSTRING(BPS_FNDT,3,8)\n"
                            + "WHERE  MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + month + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                            + "AND BPS_CONO" + company;
                    //Create round date
                    query4 = "UPDATE BRLDTA0100.BP_STDATE T1\n"
                            + "SET BPS_RD = (\n"
                            + "SELECT COUNT(*)\n"
                            + "FROM BRLDTA0100.BP_STDATE T2\n"
                            + "WHERE T2.BPS_CUNO=T1.BPS_CUNO\n"
                            + "AND DATE(TIMESTAMP_FORMAT(cast( T2 .BPS_STDT as varchar(8)), 'YYYYMMDD')) <=  DATE(TIMESTAMP_FORMAT(cast( T1 .BPS_STDT as varchar(8)), 'YYYYMMDD')) \n"
                            + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T2.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = " + month + " \n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + " \n"
                            + "AND T2.BPS_CONO" + company + "\n"
                            + "GROUP BY T2.BPS_CUNO )\n"
                            + "WHERE MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T1.BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + month + " \n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                            + "AND BPS_CONO" + company;

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
