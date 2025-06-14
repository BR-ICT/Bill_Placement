/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.br.data;

import MForms.Utils.MNEHelper;
import MForms.Utils.MNEProtocol;
import MvxAPI.MvxSockJ;
import com.br.connection.ConnectDB2;
import com.br.connection.ConnectSQLServer;
import java.awt.event.WindowEvent;
import static java.lang.Integer.parseInt;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONArray;

/**
 *
 * @author Wattana
 */
public class Select {

    public static String mneLogOnUrl = "https://bkrmvxm3.bangkokranch.com:21008/mne/servlet/MvxMCSvt"; //PRD
//    public static String mneLogOnUrl = "https://bkrmvxm3.bangkokranch.com:22008/mne/servlet/MvxMCSvt";   // TST 
    static MvxSockJ sock;
    private static String appServer;
    private static int appPort;
    private static String m3id;
    private static String m3pw;
    String chkpms300 = "no";

    public static JSONArray getCompany() throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT CCCONO,CCDIVI,CCCONM,'\"'|| TRIM(CCCONO) || ' : ' || TRIM(CCDIVI) || ' : ' || TRIM(CCCONM) || '\"' AS COMPANY\n"
                        + "FROM M3FDBPRD.CMNDIV\n"
                        + "WHERE CCDIVI != ''\n"
                        + "ORDER BY CCCONO";
                System.out.println("SelectCompany\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("CCCONO", mRes.getString(1).trim());
                    mMap.put("CCDIVI", mRes.getString(2).trim());
                    mMap.put("CCCONM", mRes.getString(3).trim());
                    mMap.put("COMPANY", mRes.getString(4).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray getYearInvoice(String cono, String divi, String invround) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {
                if (!"Special".equals(invround)) {
                    Statement stmt = conn.createStatement();
                    String query = "SELECT DISTINCT SUBSTRING(BPS_STDT,1,4)\n"
                            + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPS_CONO = B.BPM_CONO \n"
                            + "AND A.BPS_DIVI = B.BPM_DIVI\n"
                            + "AND A.BPS_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_RINV = " + invround + "\n"
                            + "AND B.BPM_CONO = " + cono + "\n"
                            + "AND B.BPM_DIVI = " + divi + "\n"
                            + "ORDER By SUBSTRING(BPS_STDT,1,4) ";
                    System.out.println("getYearInvoice normalcase\n" + query);
                    ResultSet mRes = stmt.executeQuery(query);
                    while (mRes.next()) {
                        Map<String, Object> mMap = new HashMap<>();
                        mMap.put("vYear", mRes.getString(1).trim());
                        mJSonArr.put(mMap);
                    }
                } else {
                    Statement stmt = conn.createStatement();
                    String query = "SELECT DISTINCT SUBSTRING(BPS_STDT,1,4)\n"
                            + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B\n"
                            + "WHERE A.BPS_CONO = B.BPM_CONO \n"
                            + "AND A.BPS_DIVI = B.BPM_DIVI\n"
                            + "AND A.BPS_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_CONO = " + cono + "\n"
                            + "AND B.BPM_DIVI = " + divi + "\n"
                            + "AND B.BPM_RINV <> 7\n"
                            + "AND B.BPM_RINV <> 15\n"
                            + "AND B.BPM_RINV <> 30";
                    System.out.println("getYearInvoice specialcase\n" + query);
                    ResultSet mRes = stmt.executeQuery(query);
                    while (mRes.next()) {
                        Map<String, Object> mMap = new HashMap<>();
                        mMap.put("vYear", mRes.getString(1).trim());
                        mJSonArr.put(mMap);
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

        return mJSonArr;

    }

    public static JSONArray getYearHeader(String cono, String divi, String invround) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT DISTINCT SUBSTRING(BPH_STDT,1,4) \n"
                        + "FROM BRLDTA0100.BP_HBILL A,BRLDTA0100.BP_MASTER B\n"
                        + "WHERE A.BPH_CONO = B.BPM_CONO\n"
                        + "AND A.BPH_CUNO = B.BPM_CUNO\n"
                        + "AND A.BPH_DIVI = B.BPM_DIVI\n"
                        + "AND B.BPM_RINV = " + invround + "\n"
                        + "AND B.BPM_CONO = " + cono + "\n"
                        + "AND B.BPM_DIVI = " + divi + "\n"
                        + "ORDER BY SUBSTRING(BPH_STDT,1,4)";
                System.out.println("getYearHeader\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("vYear", mRes.getString(1).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray Company() throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT CCCONO,CCDIVI,CCCONM,'\"'|| TRIM(CCCONO) || ' : ' || TRIM(CCDIVI) || ' : ' || TRIM(CCCONM) || '\"' AS COMPANY\n"
                        + "FROM M3FDBPRD.CMNDIV\n"
                        + "WHERE CCDIVI != ''\n"
                        + "ORDER BY CCCONO";
                System.out.println("SelectCompany\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("CCCONO", mRes.getString(1).trim());
                    mMap.put("CCDIVI", mRes.getString(2).trim());
                    mMap.put("CCCONM", mRes.getString(3).trim());
                    mMap.put("COMPANY", mRes.getString(4).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    static private void callPPS200A(String xPoNo, String xPODATE, String tv1, String tv3, String tv5, String tv7, String tv8, String tv9, String tv10, String cono, String divi) {

        try {

            //m3id = "MPM_1A1";
            //m3id = "MVXSECOFR"; 
            m3id = "PUR_1A1";
            m3pw = "lawson@90";
            appServer = "192.200.9.190";
            appPort = 16105;
            int i = 0;

            MNEHelper mne = new MNEHelper(appServer, appPort, mneLogOnUrl);
            mne.logInToM3(Integer.parseInt(cono), divi, m3id, m3pw);

            if (!mne.logInToM3(Integer.parseInt(cono), divi, m3id, m3pw)) {
                System.out.println(" Can not login to M3 system");
            }

            String PPS200ID = mne.runM3Pgm("PPS200");
            System.out.println("PPS200_ID:" + PPS200ID);

            if ((PPS200ID).equals("")) {
                System.out.println(" ไม่สามารถเปิดโปรแกรม PPS200 ได้");
            }
            //---------------------------------------------------------
            if (mne.panel.equals("PPS200/A")) {

                mne.setField("WAFACI", "1A1");
                mne.setField("WAWHLO", "A31");
                mne.setField("WAPUNO", xPoNo);
                mne.setField("WASUNO", "");
                mne.setField("WADWDT", xPODATE);
                mne.setField("WWPSEQ", "T");
                mne.selectOption("2");

                mne.setField("CMDTP", "KEY");
                mne.setField("FCS", "WWPSEQ");
                mne.setField("Txei", "0");

                String tv2 = "", tv4 = "", tv6 = "";
                //mne.setField("TX60",xPoNo+ "\n" + tv1 + "\n" + tv2 + "\n" + tv3 + "\n" + tv4 + "\n" + tv5 + "\n" + tv6 + "\n" + tv7 + "\n" + tv8 + "\n" + tv9 + "\n" + tv10 + "\n");
                mne.setField("TX60", tv1 + "\n" + tv2 + "\n" + tv3 + "\n" + tv4 + "\n" + tv5 + "\n" + tv6 + "\n" + tv7 + "\n" + tv8 + "\n" + tv9 + "\n" + tv10 + "\n");
                mne.setField("Lncd", "GB");
                mne.pressKey(MNEProtocol.KeyEnter);
                mne.pressKey(MNEProtocol.KeyF03);

                mne.pressKey(MNEProtocol.KeyF03);
                mne.closeProgram(PPS200ID);
            }
            //--------------------------------------------------------
            mne.pressKey(MNEProtocol.KeyF03);
            mne.closeProgram(PPS200ID);

        } catch (Exception e) {
            if (sock != null) {
                System.out.println("ERR: " + sock.mvxGetLastError());
            }
        }
    }

    public static JSONArray getMasterFinance(String cono, String divi) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT master.BPM_CONO,BPM_DIVI,master.BPM_CUNO,MASTER.BPM_TYPE,MASTER.BPM_RINV,\n"
                        + "master.BPM_RBIL,MASTER.BPM_RCOL,CUS.OKCUNM,MASTER.BPM_CASEBIL,MASTER.BPM_CASECOL,\n"
                        + "MASTER.BPM_RBILSP,MASTER.BPM_RCOLSP,BPM_CASEINV\n"
                        + ",COALESCE(MASTER.BPM_BILDESC,'') AS BILDESC,COALESCE(MASTER.BPM_COLDESC,'') AS COLDESC\n"
                        + ",COALESCE(MASTER.BPM_COLBY,'') AS Colby,COALESCE(MASTER.BPM_REMARK,'') AS REMARK\n"
                        + "FROM BRLDTA0100.BP_MASTER master, M3FDBPRD.OCUSMA cus\n"
                        + "WHERE  MASTER.BPM_CUNO = CUS.OKCUNO\n"
                        + "AND MASTER.BPM_CONO = CUS.OKCONO\n"
                        + "AND BPM_CONO =" + cono;
                System.out.println(query);
                ResultSet mRes = stmt.executeQuery(query);
                while (mRes.next()) {
                    String BPM_CASEBIL = mRes.getString("BPM_CASEBIL");
                    if (BPM_CASEBIL != null) {
                        BPM_CASEBIL = BPM_CASEBIL.trim();
                    }
                    String BPM_RBILSP = mRes.getString("BPM_RBILSP");
                    if (BPM_RBILSP != null) {
                        BPM_RBILSP = BPM_RBILSP.trim();
                    }
                    String BPM_CASECOL = mRes.getString("BPM_CASECOL");
                    if (BPM_CASECOL != null) {
                        BPM_CASECOL = BPM_CASECOL.trim();
                    }
                    String BPM_RCOLSP = mRes.getString("BPM_RCOLSP");
                    if (BPM_RCOLSP != null) {
                        BPM_RCOLSP = BPM_RCOLSP.trim();
                    }

                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("RCOMPANY", mRes.getString("BPM_CONO"));
                    mMap.put("BPM_DIVI", mRes.getString("BPM_DIVI"));
                    mMap.put("RCUSTOMERID", mRes.getString("BPM_CUNO"));
                    mMap.put("RCUSTOMERTYPE", mRes.getString("BPM_TYPE"));
                    mMap.put("ROUNDINV", mRes.getString("BPM_RINV"));
                    mMap.put("ROUNDBILL", mRes.getString("BPM_RBIL"));
                    mMap.put("ROUNDPAY", mRes.getString("BPM_RCOL"));
                    mMap.put("RCUSTOMERNAME", mRes.getString("OKCUNM"));
                    mMap.put("ROUNDCASEBILL", BPM_CASEBIL);
                    mMap.put("ROUNDBILLSPECIAL", BPM_RBILSP);
                    mMap.put("ROUNDCASEPAY", BPM_CASECOL);
                    mMap.put("ROUNDPAYSPECIAL", BPM_RCOLSP);
                    mMap.put("ROUNDCASEINV", mRes.getString("BPM_CASEINV"));
                    mMap.put("BILLDESCRIPTION", mRes.getString("BILDESC"));
                    mMap.put("COLDESCRIPTION", mRes.getString("COLDESC"));
                    mMap.put("COLBY", mRes.getString("COLBY"));
                    mMap.put("REMARK", mRes.getString("REMARK"));
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray getStartDate(String month, String year, String invoicerd, String cono, String divi) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();
        String query = "";
        try {
            if (conn != null) {
                if (!"Special".equals(invoicerd)) {
                    Statement stmt2 = conn.createStatement();
                    String query2 = "UPDATE BRLDTA0100.BP_STDATE E\n"
                            + "SET E.BPS_STS = '10'\n"
                            + "WHERE EXISTS(\n"
                            + "SELECT A.BPS_CONO,A.BPS_CUNO,A.BPS_STDT,A.BPS_FNDT,A.BPS_STS,A.BPS_RD,A.BPS_PR,C.OKCUNM,D.HB_BNNO\n"
                            + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B, M3FDBPRD.OCUSMA C,BRLDTA0100.H_BILLNOTE D\n"
                            + "WHERE B.BPM_CUNO = A.BPS_CUNO\n"
                            + "AND B.BPM_CONO = A.BPS_CONO\n"
                            + "AND B.BPM_DIVI = A.BPS_DIVI\n"
                            + "AND A.BPS_CONO = C.OKCONO\n"
                            + "AND A.BPS_CUNO = C.OKCUNO\n"
                            + "AND D.HB_CUNO = A.BPS_CUNO\n"
                            + "AND D.HB_TRDT = A.BPS_STDT\n"
                            + "AND E.BPS_CONO = A.BPS_CONO\n"
                            + "AND E.BPS_CUNO = A.BPS_CUNO \n"
                            + "AND E.BPS_STDT = A.BPS_STDT\n"
                            + "AND E.BPS_FNDT = A.BPS_FNDT\n"
                            + "AND E.BPS_RD = A.BPS_RD\n"
                            + "AND B.BPM_RINV = " + invoicerd + "\n"
                            + "AND A.BPS_CONO = " + cono + "\n"
                            + "AND A.BPS_DIVI = " + divi + "\n"
                            + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + month + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + year + "\n)";
                    stmt2.execute(query2);
                    Statement stmt3 = conn.createStatement();

                    String query3 = "UPDATE BRLDTA0100.BP_STDATE E\n"
                            + "SET BPS_STS = '20'\n"
                            + "WHERE EXISTS(SELECT LB_BNNO,LB_INVDT, SUM(LB_INVAMT) , C.BPS_CUNO, C.BPS_STDT,C.BPS_FNDT,B.LB_STS,A.HB_STS\n"
                            + "FROM BRLDTA0100.H_BILLNOTE A,BRLDTA0100.LN_BILLNOTE B, BRLDTA0100.BP_STDATE C, BRLDTA0100.BP_MASTER D\n"
                            + "WHERE A.HB_BNNO = B.LB_BNNO\n"
                            + "AND C.BPS_CUNO = A.HB_CUNO\n"
                            + "AND A.HB_TRDT = C.BPS_STDT\n"
                            + "AND E.BPS_CONO = C.BPS_CONO AND C.BPS_CUNO = E.BPS_CUNO\n"
                            + "AND E.BPS_DIVI = C.BPS_DIVI\n"
                            + "AND E.BPS_STDT = C.BPS_STDT AND C.BPS_FNDT = E.BPS_FNDT\n"
                            + "AND E.BPS_CONO = D.BPM_CONO AND E.BPS_DIVI = D.BPM_DIVI\n"
                            + "AND E.BPS_CUNO = D.BPM_CUNO AND C.BPS_CONO = E.BPS_CONO\n"
                            + "AND D.BPM_RINV = " + invoicerd + " AND MONTH(DATE(TIMESTAMP_FORMAT(cast(E.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + month + "\n"
                            + "AND YEAR(DATE(TIMESTAMP_FORMAT(cast(E.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + year + "\n"
                            + "AND E.BPS_CONO = " + cono + "\n"
                            + "AND E.BPS_DIVI = " + divi + "\n"
                            + "GROUP BY LB_BNNO,LB_INVDT,C.BPS_CUNO,C.BPS_STDT,B.LB_STS,A.HB_STS,A.HB_STS,C.BPS_FNDT\n"
                            + "ORDER BY LB_BNNO)";
                    stmt3.execute(query3);
                }
                Statement stmt = conn.createStatement();
                if (!"Special".equals(invoicerd)) {
                    query = "SELECT A.BPS_CONO,A.BPS_CUNO,A.BPS_STDT,A.BPS_FNDT,A.BPS_STS,A.BPS_RD,A.BPS_PR,C.OKCUNM\n"
                            + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B, M3FDBPRD.OCUSMA C\n"
                            + "WHERE B.BPM_CUNO = A.BPS_CUNO\n"
                            + "AND B.BPM_CONO  = A.BPS_CONO\n"
                            + "AND B.BPM_DIVI = A.BPS_DIVI\n"
                            + "AND A.BPS_CONO = C.OKCONO\n"
                            + "AND A.BPS_CUNO = C.OKCUNO\n"
                            + "AND B.BPM_RINV = " + invoicerd + " \n"
                            + "AND A.BPS_CONO = " + cono + "\n"
                            + "AND A.BPS_DIVI = " + divi + "\n"
                            + "AND  MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + month + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                } else {
                    query = "SELECT A.BPS_CONO,A.BPS_CUNO,A.BPS_STDT,A.BPS_FNDT,A.BPS_STS,A.BPS_RD,A.BPS_PR,C.OKCUNM\n"
                            + "FROM BRLDTA0100.BP_STDATE A,BRLDTA0100.BP_MASTER B, M3FDBPRD.OCUSMA C\n"
                            + "WHERE B.BPM_CUNO = A.BPS_CUNO\n"
                            + "AND B.BPM_CONO  = A.BPS_CONO\n"
                            + "AND B.BPM_DIVI = A.BPS_DIVI\n"
                            + "AND A.BPS_CONO = C.OKCONO\n"
                            + "AND A.BPS_CUNO = C.OKCUNO\n"
                            + "AND A.BPS_CONO = " + cono + "\n"
                            + "AND A.BPS_DIVI = " + divi + "\n"
                            + "AND  MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) = " + month + "\n"
                            + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPS_STDT as varchar(8)), 'YYYYMMDD'))) =" + year + "\n"
                            + "AND B.BPM_RINV <> 7 AND B.BPM_RINV <> 15 AND B.BPM_RINV <> 30";
                }
                System.out.println(query);
                ResultSet mRes = stmt.executeQuery(query);
                while (mRes.next()) {
                    String value1 = mRes.getString(1);
                    if (value1 != null) {
                        value1 = value1.trim();
                    }
                    String value2 = mRes.getString(2);
                    if (value2 != null) {
                        value2 = value2.trim();
                    }
                    String value3 = mRes.getString(3);
                    if (value3 != null) {
                        value3 = value3.trim();
                    }
                    String value4 = mRes.getString(4);
                    if (value4 != null) {
                        value4 = value4.trim();
                    }
                    String value5 = mRes.getString(5);
                    if (value5 != null) {
                        value5 = value5.trim();
                    }
                    String value6 = mRes.getString(6);
                    if (value6 != null) {
                        value6 = value6.trim();
                    }
                    String value8 = mRes.getString(8);
                    if (value8 != null) {
                        value8 = value8.trim();
                    }
                    Map<String, Object> mMap = new HashMap<>();

                    mMap.put("RCOMPANY", value1);
                    mMap.put("RCUSTOMERID", value2);
                    mMap.put("RSTARTDATE", value3);
                    mMap.put("RENDDATE", value4);
                    mMap.put("RSTATUS", value5);
                    mMap.put("RROUND", value6);
                    mMap.put("RCUSTOMERNAME", value8);
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray getHeaderDate(String cono, String divi, String month, String year, String invoicerd) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {
                Statement stmt2 = conn.createStatement();
                //CHECK STATUS AND UPDATE TO 10 IF THE BILL HAS BEEN FOUND
                String query2 = "UPDATE BRLDTA0100.BP_HBILL E\n"
                        + "SET E.BPH_STS = 10\n"
                        + "WHERE EXISTS(SELECT A.BPH_CONO,A.BPH_DIVI,A.BPH_CUNO,A.BPH_STDT,A.BPH_FNDT,A.BPH_STS,C.OKCUNM,D.HB_BNNO\n"
                        + "FROM BRLDTA0100.BP_HBILL A,BRLDTA0100.BP_MASTER B, M3FDBPRD.OCUSMA C,BRLDTA0100.H_BILLNOTE D\n"
                        + "WHERE B.BPM_CUNO = A.BPH_CUNO\n"
                        + "AND B.BPM_CONO = A.BPH_CONO\n"
                        + "AND B.BPM_DIVI = A.BPH_DIVI\n"
                        + "AND A.BPH_CONO = C.OKCONO\n"
                        + "AND A.BPH_CUNO = C.OKCUNO\n"
                        + "AND D.HB_CUNO = A.BPH_CUNO\n"
                        + "AND D.HB_TRDT = A.BPH_BPDT\n"
                        + "AND E.BPH_CONO = A.BPH_CONO\n"
                        + "AND E.BPH_CUNO = A.BPH_CUNO \n"
                        + "AND E.BPH_STDT = A.BPH_STDT\n"
                        + "AND E.BPH_FNDT = A.BPH_FNDT\n"
                        + "AND E.BPH_DIVI = A.BPH_DIVI\n"
                        + "AND B.BPM_RINV = " + invoicerd + "\n"
                        + "AND B.BPM_CONO = " + cono + "\n"
                        + "AND B.BPM_DIVI = " + divi + "\n"
                        + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD'))) = " + month + "\n"
                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD'))) = " + year + "\n"
                        + ")";
                System.out.println(query2);
                stmt2.execute(query2);
                //CHANGE STATUS TO 20 if it also been found in second step
                Statement stmt3 = conn.createStatement();
                String query3 = "UPDATE BRLDTA0100.BP_HBILL E\n"
                        + "SET BPH_STS = '20'\n"
                        + "WHERE EXISTS(SELECT LB_BNNO,LB_INVDT, SUM(LB_INVAMT) , C.BPH_CUNO, C.BPH_STDT,C.BPH_FNDT,B.LB_STS,A.HB_STS\n"
                        + "FROM BRLDTA0100.H_BILLNOTE A,BRLDTA0100.LN_BILLNOTE B, BRLDTA0100.BP_HBILL C, BRLDTA0100.BP_MASTER D\n"
                        + "WHERE A.HB_BNNO = B.LB_BNNO\n"
                        + "AND C.BPH_CUNO = A.HB_CUNO\n"
                        + "AND C.BPH_CONO  = D.BPM_CONO\n"
                        + "AND C.BPH_DIVI = D.BPM_DIVI\n"
                        + "AND A.HB_TRDT = C.BPH_BPDT\n"
                        + "AND E.BPH_CONO = C.BPH_CONO AND E.BPH_CUNO = C.BPH_CUNO\n"
                        + "AND E.BPH_DIVI = C.BPH_DIVI\n"
                        + "AND E.BPH_STDT = C.BPH_STDT AND E.BPH_FNDT = E.BPH_FNDT\n"
                        + "AND E.BPH_CUNO = D.BPM_CUNO AND E.BPH_CONO = E.BPH_CONO\n"
                        + "AND D.BPM_RINV = " + invoicerd + "\n"
                        + "AND D.BPM_CONO = " + cono + "\n"
                        + "AND D.BPM_DIVI = " + divi + "\n"
                        + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(E.BPH_STDT as varchar(8)), 'YYYYMMDD'))) = " + month + "\n"
                        + "AND YEAR(DATE(TIMESTAMP_FORMAT(cast(E.BPH_STDT as varchar(8)), 'YYYYMMDD'))) = " + year + "\n"
                        + "GROUP BY LB_BNNO,LB_INVDT,C.BPH_CUNO,C.BPH_STDT,B.LB_STS,A.HB_STS,A.HB_STS,C.BPH_FNDT\n"
                        + "ORDER BY LB_BNNO)";
                System.out.println(query3);
                stmt3.execute(query3);
                Statement stmt = conn.createStatement();
                String query = "SELECT A.BPH_CONO ,A.BPH_DIVI,A.BPH_CUNO,A.BPH_STDT,A.BPH_FNDT,A.BPH_BPDT,A.BPH_CLDT,A.BPH_STS,C.OKCUNM,D.BPS_RD\n"
                        + "FROM BRLDTA0100.BP_HBILL A,BRLDTA0100.BP_MASTER B, M3FDBPRD.OCUSMA C,BRLDTA0100.BP_STDATE D\n"
                        + "WHERE B.BPM_CUNO = A.BPH_CUNO\n"
                        + "AND A.BPH_CONO = C.OKCONO\n"
                        + "AND A.BPH_CUNO = C.OKCUNO\n"
                        + "AND A.BPH_CUNO = D.BPS_CUNO\n"
                        + "AND D.BPS_STDT = A.BPH_STDT\n"
                        + "AND D.BPS_FNDT = A.BPH_FNDT\n"
                        + "AND B.BPM_CONO = A.BPH_CONO\n"
                        + "AND B.BPM_DIVI = A.BPH_DIVI\n"
                        + "AND A.BPH_CONO = D.BPS_CONO\n"
                        + "AND A.BPH_DIVI = D.BPS_DIVI\n"
                        + "AND B.BPM_RINV = " + invoicerd + "\n"
                        + "AND B.BPM_CONO = " + cono + "\n"
                        + "AND b.BPM_DIVI = " + divi + "\n"
                        + "AND  MONTH(DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD'))) = " + month + "\n"
                        + "AND YEAR (DATE(TIMESTAMP_FORMAT(cast(A.BPH_STDT as varchar(8)), 'YYYYMMDD'))) =" + year;
                System.out.println(query);
                ResultSet mRes = stmt.executeQuery(query);
                while (mRes.next()) {
                    String value1 = mRes.getString(1);
                    if (value1 != null) {
                        value1 = value1.trim();
                    }
                    String value2 = mRes.getString(2);
                    if (value2 != null) {
                        value2 = value2.trim();
                    }
                    String value3 = mRes.getString(3);
                    if (value3 != null) {
                        value3 = value3.trim();
                    }
                    String value4 = mRes.getString(4);
                    if (value4 != null) {
                        value4 = value4.trim();
                    }
                    String value5 = mRes.getString(5);
                    if (value5 != null) {
                        value5 = value5.trim();
                    }
                    String value6 = mRes.getString(6);
                    if (value6 != null) {
                        value6 = value6.trim();
                    }
                    String value7 = mRes.getString(7);
                    if (value7 != null) {
                        value7 = value7.trim();
                    }
                    String value8 = mRes.getString(8);
                    if (value8 != null) {
                        value8 = value8.trim();
                    }
                    String value9 = mRes.getString(9);
                    if (value9 != null) {
                        value9 = value9.trim();
                    }
                    String value10 = mRes.getString(10);
                    if (value10 != null) {
                        value10 = value10.trim();
                    }
                    Map<String, Object> mMap = new HashMap<>();

                    mMap.put("RCOMPANY", value1);
                    mMap.put("RDIVI", value2);
                    mMap.put("RCUSTOMERID", value3);
                    mMap.put("RSTARTDATE", value4);
                    mMap.put("RENDDATE", value5);
                    mMap.put("RBILLDATE", value6);
                    mMap.put("RPAYDATE", value7);
                    mMap.put("RSTATUS", value8);
                    mMap.put("RCUSTOMERNAME", value9);
                    mMap.put("RROUND", value10);
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static String deleteStartDatecustomer(String cono, String divi, String Customer) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();
        String query1 = "";
        String query4 = "";
        String respond = "No data found!";
        String check = "0";
        String check1 = "1";
        respond = "Month already generated";
        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                Statement stmt4 = conn.createStatement();
                query4 = "SELECT BPS_CUNO\n"
                        + "FROM BRLDTA0100.BP_STDATE bs\n"
                        + "WHERE BPS_CUNO = '" + Customer + "'\n"
                        + "AND BPS_CONO = " + cono + "\n"
                        + "AND BPS_DIVI = " + divi + "";
                ResultSet mRes = stmt4.executeQuery(query4);
                while (mRes.next()) {
                    if (Customer.equals(mRes.getString(1))) {
                        respond = "Found data but something went wrong";
                        check = "1";
                    }
                }
                if (check.equals("1")) {
                    //--Import data from start date 7 days
                    query1 = "DELETE FROM BRLDTA0100.BP_STDATE\n"
                            + "WHERE BPS_CUNO = '" + Customer + "'\n"
                            + "AND BPS_CONO = " + cono + "\n"
                            + "AND BPS_DIVI = " + divi;
                    stmt.execute(query1);
                    System.out.println("Delete start date per customer\n" + query1);
                    respond = "Delete Customer" + Customer + " successfully!";
                } else {
                    respond = "No Data Found";
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

    public static String deleteHeaderDatecustomer(String cono, String divi, String Customer) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();
        String query1 = "";
        String query4 = "";
        String respond = "No data found!";
        String check = "0";
        String check1 = "1";
        respond = "Data not found";
        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                Statement stmt4 = conn.createStatement();
                query4 = "SELECT BPH_CUNO\n"
                        + "FROM BRLDTA0100.BP_HBILL\n"
                        + "WHERE BPH_CUNO = '" + Customer + "'\n"
                        + "AND BPH_CONO = " + cono + "\n"
                        + "AND BPH_DIVI = " + divi;
                ResultSet mRes = stmt4.executeQuery(query4);
                while (mRes.next()) {
                    if (Customer.equals(mRes.getString(1))) {
                        respond = "Found data but something went wrong";
                        check = "1";
                    }
                }
                if (check.equals("1")) {
                    //--Import data from start date 7 days
                    query1 = "DELETE FROM BRLDTA0100.BP_HBILL\n"
                            + "WHERE BPH_CUNO = '" + Customer + "'\n"
                            + "AND BPH_CONO =" + cono + "\n"
                            + "AND BPH_DIVI = " + divi;
                    stmt.execute(query1);
                    System.out.println("Delete start date per customer\n" + query1);
                    respond = "Delete Customer" + Customer + " successfully!";
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

    public static void importInvoiceData1(String month, String year) throws Exception {
        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();
        String query1;
        String nextmonth = Integer.toString(parseInt(month) + 1);
        try {
            if (conn != null) {
                Statement stmt = conn.createStatement();
//                Statement stmt2 = conn.createStatement();
//                Statement stmt3 = conn.createStatement();
//                Statement stmt4 = conn.createStatement();
                for (int i = 1; i <= 6; i++) {
                    if (i == 1) {
                        //For first time only
                        query1 = "INSERT INTO BRLDTA0100.BP_STDATE(BPS_CONO,BPS_CUNO,BPS_STDT,BPS_RD)\n"
                                + "SELECT DISTINCT BPS_CONO,BPS_CUNO, BPS_FNDT,'1'\n"
                                + "FROM BRLDTA0100.BP_STDATE\n"
                                + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) <> MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_FNDT as varchar(8)), 'YYYYMMDD')))\n"
                                + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_FNDT as varchar(8)), 'YYYYMMDD')))  = " + nextmonth;
                    } else {
                        //For other times
                        query1 = "INSERT INTO BRLDTA0100.BP_STDATE(BPS_CONO,BPS_CUNO,BPS_STDT)\n"
                                + "SELECT DISTINCT BPS_CONO,BPS_CUNO, BPS_FNDT\n"
                                + "FROM BRLDTA0100.BP_STDATE\n"
                                + "WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_FNDT as varchar(8)), 'YYYYMMDD'))) = MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD')))\n"
                                + "AND BPS_RD = (SELECT MAX(BPS_RD) FROM BRLDTA0100.BP_STDATE WHERE MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + nextmonth + "')\n"
                                + "AND MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))) = '" + nextmonth + "'";
                    }
                    //UPDATE END DATE OF START DATE TABLE USING MASTER TABLE
//                    String query2 = "UPDATE BRLDTA0100.BP_STDATE C\n"
//                            + "SET BPS_FNDT = (SELECT YEAR(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 10000\n"
//                            + "+ MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS )) * 100\n"
//                            + "+ DAY(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD'))+ B.BPM_RINV DAYS - 1 DAYS))  AS ENDDATE\n"
//                            + "FROM BRLDTA0100.BP_STDATE A\n"
//                            + "JOIN BRLDTA0100.BP_MASTER B\n"
//                            + "ON B.BPM_CUNO = A.BPS_CUNO\n"
//                            + "AND B.BPM_CONO = A.BPS_CONO\n"
//                            + "AND C.BPS_CUNO = A.BPS_CUNO\n"
//                            + "AND C.BPS_CONO = A.BPS_CONO\n"
//                            + " FETCH FIRST 1 ROW ONLY)\n"
//                            + " WHERE  MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast( C.BPS_STDT as varchar(8)), 'YYYYMMDD')))) = " + nextmonth;
//
//                    //Create period
//                    String query3 = "Create period\n"
//                            + "UPDATE BRLDTA0100.BP_STDATE\n"
//                            + "SET BPS_PR = SUBSTRING(BPS_STDT,3,8) || SUBSTRING(BPS_FNDT,3,8)\n"
//                            + "WHERE  MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth;
//
//                    //Create round date
//                    String query4 = "UPDATE BRLDTA0100.BP_STDATE T1\n"
//                            + "SET BPS_RD = (\n"
//                            + "SELECT COUNT(*)\n"
//                            + "FROM BRLDTA0100.BP_STDATE T2\n"
//                            + "WHERE T2.BPS_CUNO=T1.BPS_CUNO\n"
//                            + "AND DATE(TIMESTAMP_FORMAT(cast( T2 .BPS_STDT as varchar(8)), 'YYYYMMDD')) <=  DATE(TIMESTAMP_FORMAT(cast( T1 .BPS_STDT as varchar(8)), 'YYYYMMDD')) \n"
//                            + "AND MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T2.BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth + "\n"
//                            + "GROUP BY T2.BPS_CUNO )\n"
//                            + "WHERE MONTH(CHAR(DATE(TIMESTAMP_FORMAT(cast(T1.BPS_STDT as varchar(8)), 'YYYYMMDD')))) =" + nextmonth;
                    System.out.println(query1);
                    stmt.executeQuery(query1);
                    // ResultSet mRes = stmt.executeQuery(query1);
//                    mRes = stmt2.executeQuery(query2);
//                    mRes = stmt3.executeQuery(query3);
//                    mRes = stmt4.executeQuery(query4);
                }
//                Map<String, Object> mMap = new HashMap<>();
//                mMap.put("result", "ok");
//                mMap.put("message", "update complete");
//                mJSonArr.put(mMap);
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

    }

    private static void updateFAR_LQLDUCK(String xSHDATE, String xCarNo, String cono) {
        try {
            Connection conn = ConnectDB2.ConnectionDB();
            Statement sta = conn.createStatement();

            String Sql = "UPDATE BRLDTA0100.FAR_LQLDUCK01\n"
                    + "SET LQLI_STATUS = '95'\n"
                    + "WHERE CONOID = '" + cono + "'\n"
                    + "AND LQLI_STATUS = '92'\n"
                    + "AND SUBSTRING(CHAR(LQLI_SHDATE,ISO),0,5) || SUBSTRING(CHAR(LQLI_SHDATE,ISO),6,2) || SUBSTRING(CHAR(LQLI_SHDATE,ISO),9,2) = '" + xSHDATE + "'\n"
                    + "AND LQLI_TURCK = " + xCarNo;
            System.out.println(Sql);
            sta.executeUpdate(Sql);

        } catch (Exception ex) {
            Logger.getLogger(Select.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private static void insertFAR_GRNCTL(String xSHDATE, String xCarNo, String xGrnNo, String xPoNo, String xPoLn, String xQty, String xCatW, String xFlock, String cono) {
        try {
            Connection conn = ConnectDB2.ConnectionDB();
            Statement sta = conn.createStatement();

            String Sql = "INSERT INTO BRLDTA0100.FAR_GRNCTL VALUES ('" + xSHDATE + "','" + xCarNo + "','" + xGrnNo + "','" + xPoNo + "','" + xPoLn + "','" + xQty + "','" + xCatW + "','" + xFlock + "','" + cono + "')";
            System.out.println(Sql);
            sta.executeUpdate(Sql);

        } catch (Exception ex) {
            Logger.getLogger(Select.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
//---------------------------------------------------------------------------------

    public static String callPPS300A(String cono, String divi, JSONArray mJSonArr) {
        String xSHDATE = "", xPO = "", xPNLI = "", xQTY = "", xCATW = "", xCARNO = "", place = "", whs = "";
        //String xPNLI="",xQTY="",xCATW="",xCARNO="";
        String GRNDATE, GRNNO = null;

        try {
            //m3id = "MPM_1A1";
            //m3id = "MVXSECOFR"; 
            m3id = "PUR_1A1";
            m3pw = "lawson@90";
            appServer = "192.200.9.190";
            appPort = 16105;
            int i = 0;

            MNEHelper mne = new MNEHelper(appServer, appPort, mneLogOnUrl);
            mne.logInToM3(Integer.parseInt(cono), divi, m3id, m3pw);

            if (!mne.logInToM3(Integer.parseInt(cono), divi, m3id, m3pw)) {
                System.out.println(" Can not login to M3 system");
            }

            String PPS300_LD = mne.runM3Pgm("PPS300");
            System.out.println("PPS300_LD:" + PPS300_LD);

            if ((PPS300_LD).equals("")) {
                System.out.println(" ไม่สามารถเปิดโปรแกรม PPS300 ได้");
            }

            //---------------------------------------------------------
            if (mne.panel.equals("PPS300/A")) {

                for (i = 0; i <= mJSonArr.length() - 1; i++) {
                    xSHDATE = mJSonArr.getJSONObject(i).getString("RSHDATE").trim();
                    xPO = mJSonArr.getJSONObject(i).getString("RSUPP").trim();
                    xPNLI = mJSonArr.getJSONObject(i).getString("RTRUCK").trim();
                    xQTY = mJSonArr.getJSONObject(i).getString("RQTY").trim();
                    xCATW = mJSonArr.getJSONObject(i).getString("RUPRICE").trim();
                    xCARNO = mJSonArr.getJSONObject(i).getString("RLDI").trim();
                    place = mJSonArr.getJSONObject(i).getString("RLOTNO").trim();

                    if ("10".equals(cono) && "WTF".equals(place)) {
                        whs = "A45";
                    } else {
                        whs = "A31";
                    }

                    GRNDATE = xSHDATE.substring(6, 8) + xSHDATE.substring(4, 6) + xSHDATE.trim().substring(2, 4);
                    float xpQTY = Float.parseFloat(xQTY);
                    float xpCATW = Float.parseFloat(xCATW);

                    mne.setField("WWWHLO", whs);
                    mne.setField("WWTRDT", GRNDATE);
                    mne.setField("WWRESP", "PUR_1A1");
                    mne.setField("WWPUNO", xPO);
                    mne.setField("WWPNLI", xPNLI);
                    mne.setField("WWSUDO", "INV-" + xPO);

                    mne.pressKey(MNEProtocol.KeyEnter);
                    System.out.println(mne.getMsg());

                    int c = 0;
                    while ((mne.panel.equals("PPS300/A")) && (c <= 3)) {
                        mne.setField("WWTRDT", GRNDATE);
                        c++;
                        mne.pressKey(MNEProtocol.KeyEnter);
                        System.out.println(mne.getMsg());
                    }

                    if (mne.panel.equals("PPS300/E")) {
                        System.out.println("OK OK :PPS300/E");

                        mne.setField("WBRVQA", xpQTY);
                        mne.setField("WWCAWE", xpCATW);
                        //mne.setField("WLWHSL","A31100");
                        GRNNO = mne.getFieldMap("WWREPN").getValue().substring(0, 7);
//                        jTextGrnno.setText(GRNNO);
                        mne.pressKey(MNEProtocol.KeyEnter);
                        System.out.println(mne.getMsg());

                        c = 0;
                        while ((mne.panel.equals("PPS300/E")) && (c <= 3)) {
                            c++;
                            mne.pressKey(MNEProtocol.KeyEnter);
                            System.out.println(mne.getMsg());
                        }

                        if (mne.panel.equals("PPS300/A")) {
                            System.out.println("UPdate : PPS300/A ");
                            updateFAR_LQLDUCK1(xSHDATE, xCARNO, cono);
                            updateFAR_GRNCTL(xPO, xPNLI, GRNNO, cono);
                        }
                        //mne.closeProgram(PPS300_LD);     
                    }

                }

            }
            //mne.selectOption("2");
            mne.pressKey(MNEProtocol.KeyF03);
            mne.closeProgram(PPS300_LD);
        } catch (Exception e) {
            if (sock != null) {
                System.out.println("ERR: " + sock.mvxGetLastError());
            }
        }

        return GRNNO;
    }
//----------------------------------------------------------------------------------

    private static void updateFAR_GRNCTL(String xPONO, String xPOLN, String xGRNNO, String cono) {
        try {
            Connection conn = ConnectDB2.ConnectionDB();
            Statement sta = conn.createStatement();

            String Sql = "update BRLDTA0100.FAR_GRNCTL\n"
                    + "SET grnc_grn = '" + xGRNNO + "'\n"
                    + "WHERE CONOID = '" + cono + "'\n"
                    + "AND grnc_po = '" + xPONO + "'\n"
                    + "AND grnc_pnli = '" + xPOLN + "'";
            sta.executeUpdate(Sql);

        } catch (Exception ex) {
            Logger.getLogger(Select.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
//----------------------------------------------------------------------------------

    private static void updateFAR_LQLDUCK1(String xSHDATE, String xCarNo, String cono) {
        try {
            Connection conn = ConnectDB2.ConnectionDB();
            Statement sta = conn.createStatement();

            String Sql = "UPDATE BRLDTA0100.FAR_LQLDUCK01\n"
                    + "SET LQLI_STATUS = '97'\n"
                    + "WHERE CONOID = '" + cono + "'\n"
                    + "AND LQLI_STATUS = '95'\n"
                    + "AND SUBSTRING(CHAR(LQLI_SHDATE,ISO),0,5) || SUBSTRING(CHAR(LQLI_SHDATE,ISO),6,2) || SUBSTRING(CHAR(LQLI_SHDATE,ISO),9,2) = '" + xSHDATE + "'\n"
                    + "AND LQLI_TURCK = " + xCarNo;

            System.out.println(Sql);
            sta.executeUpdate(Sql);

        } catch (Exception ex) {
            Logger.getLogger(Select.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
//---------------------------------------------------------------------------------

    public static JSONArray Facility(String cono, String divi) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT CFFACI,CFFACN,CFFACI || ' : ' || CFFACN AS FACILITY \n"
                        + "FROM M3FDBPRD.CFACIL\n"
                        + "WHERE CFCONO = '" + cono + "'\n"
                        + "AND CFDIVI = '" + divi + "'";
                System.out.println("SelectFacility\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("CFFACI", mRes.getString(1).trim());
                    mMap.put("CFFACN", mRes.getString(2).trim());
                    mMap.put("FACILITY", mRes.getString(3).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray getMonthInvoice(String cono, String divi, String year, String invround) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();
        Statement stmt = conn.createStatement();
        try {
            if (conn != null) {
                if (!"Special".equals(invround)) {
                    String query = "SELECT DISTINCT SUBSTRING(a.BPS_STDT,5,2)\n"
                            + "FROM BRLDTA0100.BP_STDATE A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE SUBSTRING(a.BPS_STDT,1,4) = " + year + "\n"
                            + "AND B.BPM_RINV = " + invround + "\n"
                            + "AND B.BPM_CONO = " + cono + "\n"
                            + "AND B.BPM_DIVI = " + divi + "\n"
                            + "AND A.BPS_CONO = B.BPM_CONO \n"
                            + "AND A.BPS_DIVI = B.BPM_DIVI\n"
                            + "AND A.BPS_CUNO = B.BPM_CUNO \n"
                            + "ORDER BY SUBSTRING(a.BPS_STDT,5,2)";
                    System.out.println("getPeriod\n" + query);
                    ResultSet mRes = stmt.executeQuery(query);
                    while (mRes.next()) {
                        Map<String, Object> mMap = new HashMap<>();
                        mMap.put("vMonth", mRes.getString(1).trim());
                        mJSonArr.put(mMap);

                    }
                } else {
                    String query = "SELECT DISTINCT SUBSTRING(a.BPS_STDT,5,2)\n"
                            + "FROM BRLDTA0100.BP_STDATE A, BRLDTA0100.BP_MASTER B\n"
                            + "WHERE SUBSTRING(a.BPS_STDT,1,4) =" + year + "\n"
                            + "AND A.BPS_CONO = B.BPM_CONO\n"
                            + "AND A.BPS_CUNO = B.BPM_CUNO\n"
                            + "AND B.BPM_RINV <> 7 AND B.BPM_RINV <> 15\n"
                            + "AND B.BPM_RINV <> 30";
                    System.out.println("getPeriod\n" + query);
                    ResultSet mRes = stmt.executeQuery(query);
                    while (mRes.next()) {
                        Map<String, Object> mMap = new HashMap<>();
                        mMap.put("vMonth", mRes.getString(1).trim());
                        mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray getMonthMaster(String cono, String divi, String year, String invround) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT DISTINCT SUBSTRING(BPH_STDT,5,2)\n"
                        + "FROM BRLDTA0100.BP_HBILL A,BRLDTA0100.BP_MASTER B\n"
                        + "WHERE A.BPH_CONO = B.BPM_CONO\n"
                        + "AND A.BPH_DIVI = B.BPM_DIVI\n"
                        + "AND A.BPH_CUNO = B.BPM_CUNO\n"
                        + "AND B.BPM_RINV = " + invround + "\n"
                        + "AND B.BPM_CONO = " + cono + "\n"
                        + "AND B.BPM_DIVI = " + divi + "\n"
                        + "AND SUBSTRING(BPH_STDT,1,4) = " + year + "\n"
                        + "ORDER BY SUBSTRING(BPH_STDT,5,2)";
                System.out.println("getPeriod\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("vMonth", mRes.getString(1).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static String Testforreply() throws Exception {
        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();
        String testresponse = "";
        String testresponse2 = "";
        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT DISTINCT MAX(MONTH(DATE(TIMESTAMP_FORMAT(cast(BPS_STDT as varchar(8)), 'YYYYMMDD'))))\n"
                        + "FROM BRLDTA0100.BP_STDATE";
                System.out.println("getPeriod\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    testresponse = (mRes.getString(1).trim());
                    if ("4".equals(testresponse)) {
                        testresponse2 = "correct";
                    } else {
                        testresponse2 = "incorrect";
                    }

                }

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

        return testresponse2;

    }

    public static Boolean checkAuth(String user) throws Exception {

        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT CTL_REM AS CHECK_AUTH\n"
                        + "FROM BRLDTA0100.APPCTL1\n"
                        + "WHERE CTL_CODE = 'BILL'\n"
                        + "AND CTL_REM = 'AP'\n"
                        + "AND CTL_UID = '" + user + "'";
                System.out.println("checkAuth\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    return true;

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

        return false;

    }

    public static String getAddonLine(String cono, String divi, String whs, String period) throws Exception {

        Connection conn = ConnectSQLServer.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT COALESCE(MAX(RALINE),0) + 1 AS LINE\n"
                        + "FROM BRLAS400.dbo.T_RENTALADDON\n"
                        + "WHERE RACONO = '" + cono + "'\n"
                        + "AND RADIVI = '" + divi + "'\n"
                        + "AND RAWARE = '" + whs + "'\n"
                        + "AND RAPERI = '" + period + "'";
                System.out.println("getAddonLine\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    return mRes.getString("LINE").trim();

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

        return null;

    }

    public static JSONArray FacilityInventory(String cono, String divi) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT A1CONO,A1DIVI,A1CODE,A1DESC,A1NAME,A1TYPE,A1MUUN,A1RATE,COALESCE(A1REF1,'') AS A1REF1,COALESCE(A1REF2,'') AS A1REF2,A1CODE || ' : ' || A1DESC AS ALLOCATION\n"
                        + "FROM BRLDTA0100.A1CASU\n"
                        + "WHERE A1CONO = '" + cono + "'\n"
                        + "AND A1DIVI = '" + divi + "'\n"
                        + "AND A1CODE = '300'\n"
                        + "ORDER BY A1CODE";
                System.out.println("SelectFacility\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("A1CONO", mRes.getString(1).trim());
                    mMap.put("A1DIVI", mRes.getString(2).trim());
                    mMap.put("A1CODE", mRes.getString(3).trim());
                    mMap.put("A1DESC", mRes.getString(4).trim());
                    mMap.put("A1NAME", mRes.getString(5).trim());
                    mMap.put("A1TYPE", mRes.getString(6).trim());
                    mMap.put("A1MUUN", mRes.getString(7).trim());
                    mMap.put("A1RATE", mRes.getString(8).trim());
                    mMap.put("A1REF1", mRes.getString(9).trim());
                    mMap.put("A1REF2", mRes.getString(10).trim());
                    mMap.put("ALLOCATION", mRes.getString(11).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray Type(String cono, String divi) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT ATCONO,ATDIVI,ATTYPE,ATNAME,ATREF1,ATTYPE || ' : ' || ATNAME AS TYPE\n"
                        + "FROM BRLDTA0100.A1TYPE\n"
                        + "WHERE ATCONO = '" + cono + "'\n"
                        + "AND ATDIVI = '" + divi + "'\n"
                        + "ORDER BY ATTYPE";
                System.out.println("SelectType\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("ATCONO", mRes.getString(1).trim());
                    mMap.put("ATDIVI", mRes.getString(2).trim());
                    mMap.put("ATTYPE", mRes.getString(3).trim());
                    mMap.put("ATNAME", mRes.getString(4).trim());
                    mMap.put("ATREF1", mRes.getString(5).trim());
                    mMap.put("TYPE", mRes.getString(6).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray Level(String cono, String divi) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT A2CONO,A2DIVI,A2CODE,A2STID,A2SLVL,A2DESC,COALESCE(A2REF1,'') AS A2REF1,COALESCE(A2REF2,'') AS A2REF2\n"
                        + "FROM BRLDTA0100.A2CASL\n"
                        + "WHERE A2CONO = '" + cono + "'\n"
                        + "AND A2DIVI = '" + divi + "'\n"
                        + "ORDER BY A2CODE";
                System.out.println("SelectLevel\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("A2CONO", mRes.getString(1).trim());
                    mMap.put("A2DIVI", mRes.getString(2).trim());
                    mMap.put("A2CODE", mRes.getString(3).trim());
                    mMap.put("A2STID", mRes.getString(4).trim());
                    mMap.put("A2SLVL", mRes.getString(5).trim());
                    mMap.put("A2DESC", mRes.getString(6).trim());
                    mMap.put("A2REF1", mRes.getString(7).trim());
                    mMap.put("A2REF2", mRes.getString(8).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray Period(String cono, String divi) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT PECONO,PEDIVI,PECODE,PEYEA4,PEMONT,PEDESC\n"
                        + "FROM BRLDTA0100.APERIO\n"
                        + "WHERE PECONO = '" + cono + "'\n"
                        + "AND PEDIVI = '" + divi + "'\n"
                        + "ORDER BY PECODE,PEYEA4,PEMONT";
                System.out.println("SelectPeriod\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("PECONO", mRes.getString(1).trim());
                    mMap.put("PEDIVI", mRes.getString(2).trim());
                    mMap.put("PECODE", mRes.getString(3).trim());
                    mMap.put("PEYEA4", mRes.getString(4).trim());
                    mMap.put("PEMONT", mRes.getString(5).trim());
                    mMap.put("PEDESC", mRes.getString(6).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray WorkCenter(String cono, String divi) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT A3CONO,A3DIVI,A3CODE,A3STID,CHAR(A3SLVL) AS A3SLVL,A3AITM,A3ASTR,A3ADES,A3METY,A3UEHR,A3MELA,A3PERS,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM BRLDTA0100.A3CAWC\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "ORDER BY A3CODE,A3STID,A3AITM";
                System.out.println("SelectWorkCenter\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("A3CONO", mRes.getString(1).trim());
                    mMap.put("A3DIVI", mRes.getString(2).trim());
                    mMap.put("A3CODE", mRes.getString(3).trim());
                    mMap.put("A3STID", mRes.getString(4).trim());
                    mMap.put("A3SLVL", mRes.getString(5).trim());
                    mMap.put("A3AITM", mRes.getString(6).trim());
                    mMap.put("A3ASTR", mRes.getString(7).trim());
                    mMap.put("A3ADES", mRes.getString(8).trim());
                    mMap.put("A3METY", mRes.getString(9).trim());
                    mMap.put("A3UEHR", mRes.getString(10).trim());
                    mMap.put("A3MELA", mRes.getString(11).trim());
                    mMap.put("A3PERS", mRes.getString(12).trim());
                    mMap.put("A3ACCT", mRes.getString(13).trim());
                    mMap.put("A3AAC1", mRes.getString(14).trim());
                    mMap.put("A3AAC2", mRes.getString(15).trim());
                    mMap.put("A3ABOI", mRes.getString(16).trim());
                    mMap.put("A3ARE1", mRes.getString(17).trim());
                    mMap.put("A3ARE2", mRes.getString(18).trim());
                    mMap.put("A3ARE3", mRes.getString(19).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray LevelDetail(String cono, String divi, String code, String struct, String level) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT A3CONO,A3DIVI,A3CODE,A3STID,A3SLVL,A3AITM,A3ASTR,A3ADES,A3METY,A3UEHR,A3MELA,A3PERS,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM BRLDTA0100.A3CAWC\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3STID = '" + struct + "'\n"
                        + "AND A3SLVL = '" + level + "'\n"
                        + "ORDER BY A3CODE,A3STID,A3AITM";
                System.out.println("SelectLevelDetail\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("A3CONO", mRes.getString(1).trim());
                    mMap.put("A3DIVI", mRes.getString(2).trim());
                    mMap.put("A3CODE", mRes.getString(3).trim());
                    mMap.put("A3STID", mRes.getString(4).trim());
                    mMap.put("A3SLVL", mRes.getString(5).trim());
                    mMap.put("A3AITM", mRes.getString(6).trim());
                    mMap.put("A3ASTR", mRes.getString(7).trim());
                    mMap.put("A3ADES", mRes.getString(8).trim());
                    mMap.put("A3METY", mRes.getString(9).trim());
                    mMap.put("A3UEHR", mRes.getString(10).trim());
                    mMap.put("A3MELA", mRes.getString(11).trim());
                    mMap.put("A3PERS", mRes.getString(12).trim());
                    mMap.put("A3ACCT", mRes.getString(13).trim());
                    mMap.put("A3AAC1", mRes.getString(14).trim());
                    mMap.put("A3AAC2", mRes.getString(15).trim());
                    mMap.put("A3ABOI", mRes.getString(16).trim());
                    mMap.put("A3ARE1", mRes.getString(17).trim());
                    mMap.put("A3ARE2", mRes.getString(18).trim());
                    mMap.put("A3ARE3", mRes.getString(19).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray Bills(String cono, String divi, String period, String type) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT E1CONO, E1DIVI, E1CODE,TRIM(b.A1CODE || ' : ' || b.A1DESC) AS DESC, E1PECO, E1METS, E1METE, E1EPQT, E1EPAT, E1EVAT, E1ETAT, E1RAAT\n"
                        + "FROM BRLDTA0100.E1ALLO a,BRLDTA0100.A1CASU b\n"
                        + "WHERE E1CONO = '" + cono + "'\n"
                        + "AND E1DIVI = '" + divi + "'\n"
                        + "AND E1PECO = '" + period + "'\n"
                        + "AND E1TYPE = '" + type + "'\n"
                        + "AND b.A1CONO = E1CONO\n"
                        + "AND b.A1CODE = E1CODE\n"
                        + "ORDER BY E1PECO,E1CODE";
                System.out.println("SelectBills\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("E1CONO", mRes.getString(1).trim());
                    mMap.put("E1DIVI", mRes.getString(2).trim());
                    mMap.put("E1CODE", mRes.getString(3).trim());
                    mMap.put("DESC", mRes.getString(4).trim());
                    mMap.put("E1PECO", mRes.getString(5).trim());
                    mMap.put("E1METS", mRes.getString(6).trim());
                    mMap.put("E1METE", mRes.getString(7).trim());
                    mMap.put("E1EPQT", mRes.getString(8).trim());
                    mMap.put("E1EPAT", mRes.getString(9).trim());
                    mMap.put("E1EVAT", mRes.getString(10).trim());
                    mMap.put("E1ETAT", mRes.getString(11).trim());
                    mMap.put("E1RAAT", mRes.getString(12).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray BillsDetail(String cono, String divi, String code, String period) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT E1CONO, E1DIVI, E1CODE, E1PECO, E1METS, E1METE, E1EPQT, E1EPAT, E1EVAT, E1ETAT, E1RAAT\n"
                        + "FROM BRLDTA0100.E1ALLO\n"
                        + "WHERE E1CONO = '" + cono + "'\n"
                        + "AND E1DIVI = '" + divi + "'\n"
                        + "AND E1CODE = '" + code + "'\n"
                        + "AND E1PECO = '" + period + "'\n"
                        + "ORDER BY E1PECO,E1CODE";
                System.out.println("SelectBills\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("E1CONO", mRes.getString(1).trim());
                    mMap.put("E1DIVI", mRes.getString(2).trim());
                    mMap.put("E1CODE", mRes.getString(3).trim());
                    mMap.put("E1PECO", mRes.getString(4).trim());
                    mMap.put("E1METS", mRes.getString(5).trim());
                    mMap.put("E1METE", mRes.getString(6).trim());
                    mMap.put("E1EPQT", mRes.getString(7).trim());
                    mMap.put("E1EPAT", mRes.getString(8).trim());
                    mMap.put("E1EVAT", mRes.getString(9).trim());
                    mMap.put("E1ETAT", mRes.getString(10).trim());
                    mMap.put("E1RAAT", mRes.getString(11).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray Allocation(String cono, String divi, String code, String period, String fromstatus, String tostatus, String type) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,A2DESC,E2PECO,E2STAT\n"
                        + "FROM BRLDTA0100.E2ALLO,BRLDTA0100.A2CASL\n"
                        + "WHERE E2CONO = '" + cono + "'\n"
                        + "AND E2DIVI = '" + divi + "'\n"
                        + "AND E2CODE = '" + code + "'\n"
                        + "AND E2PECO = '" + period + "'\n"
                        + "AND E2TYPE = '" + type + "'\n"
                        + "AND A2CONO = E2CONO\n"
                        + "AND A2DIVI = E2DIVI\n"
                        + "AND A2STID = E2STID\n"
                        + "AND A2CODE = E2CODE\n"
                        + "AND E2STAT BETWEEN '" + fromstatus + "' AND '" + tostatus + "'\n"
                        + "GROUP BY E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,A2DESC,E2PECO,E2STAT\n"
                        + "ORDER BY E2CODE,E2PECO,E2STID,E2SLVL";
                System.out.println("SelectAllocation\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("E2CONO", mRes.getString(1).trim());
                    mMap.put("E2DIVI", mRes.getString(2).trim());
                    mMap.put("E2CODE", mRes.getString(3).trim());
                    mMap.put("E2STID", mRes.getString(4).trim());
                    mMap.put("E2SLVL", mRes.getString(5).trim());
                    mMap.put("A2DESC", mRes.getString(6).trim());
                    mMap.put("E2PECO", mRes.getString(7).trim());
                    mMap.put("E2STAT", mRes.getString(8).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray AllocationDetail(String cono, String divi, String code, String struct, String level, String period, String type) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT E2CONO,E2DIVI,E2CODE,E2STID,E2SLVL,E2AITM,A3ADES,E2PECO,E2METS,E2METE,E2UEHR,E2HOUR,CAST(E2EPQT AS DECIMAL(15,2)) AS E2EPQT,CAST(E2PERS AS DECIMAL(15,2)) AS E2PERS,ROUND(E2EAQT,4) AS E2EAQT,ROUND(E2EAAT,4) AS E2EAAT,E2STAT\n"
                        + "FROM BRLDTA0100.E2ALLO,BRLDTA0100.A3CAWC\n"
                        + "WHERE E2CONO = '" + cono + "'\n"
                        + "AND E2DIVI = '" + divi + "'\n"
                        + "AND E2CODE = '" + code + "'\n"
                        + "AND E2STID = '" + struct + "'\n"
                        + "AND E2SLVL = '" + level + "'\n"
                        + "AND E2PECO = '" + period + "'\n"
                        + "AND E2TYPE = '" + type + "'\n"
                        + "AND A3CONO = E2CONO\n"
                        + "AND A3DIVI = E2DIVI\n"
                        + "AND A3CODE = E2CODE\n"
                        + "AND A3AITM = E2AITM\n"
                        + "ORDER BY E2STID,E2SLVL,E2AITM";
                System.out.println("SelectAllocationDetail\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("E2CONO", mRes.getString(1).trim());
                    mMap.put("E2DIVI", mRes.getString(2).trim());
                    mMap.put("E2CODE", mRes.getString(3).trim());
                    mMap.put("E2STID", mRes.getString(4).trim());
                    mMap.put("E2SLVL", mRes.getString(5).trim());
                    mMap.put("E2AITM", mRes.getString(6).trim());
                    mMap.put("A3ADES", mRes.getString(7).trim());
                    mMap.put("E2PECO", mRes.getString(8).trim());
                    mMap.put("E2METS", mRes.getString(9).trim());
                    mMap.put("E2METE", mRes.getString(10).trim());
                    mMap.put("E2UEHR", mRes.getString(11).trim());
                    mMap.put("E2HOUR", mRes.getString(12).trim());
                    mMap.put("E2EPQT", mRes.getString(13).trim());
                    mMap.put("E2PERS", mRes.getString(14).trim());
                    mMap.put("E2EAQT", mRes.getString(15).trim());
                    mMap.put("E2EAAT", mRes.getString(16).trim());
                    mMap.put("E2STAT", mRes.getString(17).trim());
                    mJSonArr.put(mMap);

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

        return mJSonArr;

    }

    public static JSONArray Voucher(String cono, String divi, String code, String period, String type) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT NO,a.A3ACCT AS ACCC,a.A3AAC1 AS COST,'' AS BONO,SUBSTRING(A3ADES,0,9) AS REF1,A3ARE2 AS REF2,A3ARE3 AS REF3,'Accu. ' || TRIM(c.A1DESC) ||  ' for ' || TRIM(b.E2PECO) AS VOUC,ROUND(b.E2EAAT,4) AS AMT,b.E2STAT\n"
                        + "FROM\n"
                        + "(SELECT NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM \n"
                        + "(SELECT '1' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM BRLDTA0100.A3CAWC\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3SLVL = '2'\n"
                        + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "ORDER BY A3STID,A3AITM) a\n"
                        + "UNION ALL \n"
                        + "(SELECT '2' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM BRLDTA0100.A3CAWC a\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3SLVL = '1'\n"
                        + "AND A3ASTR = '0'\n"
                        + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "ORDER BY A3STID,A3AITM)) AS a\n"
                        + "INNER JOIN\n"
                        + "(SELECT E2CONO, E2DIVI, E2CODE, E2STID, E2SLVL, E2AITM, E2PECO, E2METS, E2METE, E2UEHR, E2HOUR, E2EPQT, E2PERS, E2EAQT, E2EAAT, E2STAT\n"
                        + "FROM BRLDTA0100.E2ALLO a\n"
                        + "WHERE E2CONO = '" + cono + "'\n"
                        + "AND E2DIVI = '" + divi + "'\n"
                        + "AND E2CODE = '" + code + "'\n"
                        + "AND E2PECO = '" + period + "'\n"
                        + "AND E2STAT = '10'\n"
                        + "AND E2TYPE = '" + type + "'\n"
                        //+ "AND E2EAAT > 0\n"
                        + ") AS b\n"
                        + "ON b.E2AITM = a.A3AITM\n"
                        + "INNER JOIN\n"
                        + "(SELECT A1CONO, A1DIVI, A1CODE, A1DESC, A1NAME, A1TYPE, A1MUUN, A1RATE, A1REF1, A1REF2\n"
                        + "FROM BRLDTA0100.A1CASU) AS c\n"
                        + "ON c.A1CONO = b.E2CONO\n"
                        + "AND c.A1DIVI = b.E2DIVI\n"
                        + "AND c.A1CODE = b.E2CODE\n"
                        + "UNION ALL\n"
                        + "SELECT '3' AS NO,c.A1REF1 AS ACCC,'' AS COST,'' AS BONO,'HO' AS REF1,CHAR(b.E2PECO) AS REF2,'' AS REF3,'Accu. ' || TRIM(c.A1DESC) ||  ' for ' || TRIM(b.E2PECO) AS VOUC,ROUND(SUM(E2EAAT) * -1,4) AS AMT,b.E2STAT\n"
                        + "FROM BRLDTA0100.A3CAWC a,BRLDTA0100.E2ALLO b,BRLDTA0100.A1CASU c\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3ASTR = '0'\n"
                        + "AND b.E2CONO = A3CONO\n"
                        + "AND b.E2DIVI = A3DIVI\n"
                        + "AND b.E2CODE = A3CODE\n"
                        + "AND b.E2AITM = A3AITM\n"
                        + "AND b.E2PECO = '" + period + "'\n"
                        + "AND b.E2STAT = '10'\n"
                        + "AND b.E2TYPE = '" + type + "'\n"
                        + "AND c.A1CONO = A3CONO\n"
                        + "AND c.A1DIVI = A3DIVI\n"
                        + "AND c.A1CODE = A3CODE\n"
                        + "GROUP BY c.A1REF1,c.A1DESC,b.E2PECO,b.E2STAT\n"
                        + "ORDER BY NO";
                System.out.println("Voucher\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("NO", mRes.getString(1).trim());
                    mMap.put("ACCC", mRes.getString(2).trim());
                    mMap.put("COST", mRes.getString(3).trim());
                    mMap.put("BONO", mRes.getString(4).trim());
                    mMap.put("REF1", mRes.getString(5).trim());
                    mMap.put("REF2", mRes.getString(6).trim());
                    mMap.put("REF3", mRes.getString(7).trim());
                    mMap.put("VOUC", mRes.getString(8).trim());
                    mMap.put("AMT", mRes.getString(9).trim());
                    mMap.put("E2STAT", mRes.getString(10).trim());
                    mJSonArr.put(mMap);
//                    System.out.println(mJSonArr);
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

        return mJSonArr;

    }

    public static JSONArray VoucherVariance(String cono, String divi, String code, String period, String type) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT NO,a.A3ACCT AS ACCC,COALESCE(a.A3AAC1,'') AS COST,'' AS BONO,SUBSTRING(A3ADES,0,9) AS REF1,A3ARE2 AS REF2,A3ARE3 AS REF3,'Var. ' || TRIM(c.A1DESC) ||  ' for ' || SUBSTRING(CHAR(DATE(SUBSTRING(b.E2PECO,0,5)||'-'||SUBSTRING(b.E2PECO,5,2)||'-'||'01') - 1 MONTH, ISO),0,5)||SUBSTRING(CHAR(DATE(SUBSTRING(b.E2PECO,0,5)||'-'||SUBSTRING(b.E2PECO,5,2)||'-'||'01') - 1 MONTH, ISO),6,2) AS VOUC,ROUND(b.E2EAAT,4) AS AMT,b.E2STAT\n"
                        + "FROM\n"
                        + "(SELECT NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM \n"
                        + "(SELECT '1' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM BRLDTA0100.A3CAWC\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3SLVL = '2'\n"
                        + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "ORDER BY A3STID,A3AITM) a\n"
                        + "UNION ALL \n"
                        + "(SELECT '2' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM BRLDTA0100.A3CAWC a\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3SLVL = '1'\n"
                        + "AND A3ASTR = '0'\n"
                        + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "ORDER BY A3STID,A3AITM)) AS a\n"
                        + "INNER JOIN\n"
                        + "(SELECT E2CONO, E2DIVI, E2CODE, E2STID, E2SLVL, E2AITM, E2PECO, E2METS, E2METE, E2UEHR, E2HOUR, E2EPQT, E2PERS, E2EAQT, E2EAAT, E2STAT\n"
                        + "FROM BRLDTA0100.E2ALLO a\n"
                        + "WHERE E2CONO = '" + cono + "'\n"
                        + "AND E2DIVI = '" + divi + "'\n"
                        + "AND E2CODE = '" + code + "'\n"
                        + "AND E2PECO = '" + period + "'\n"
                        + "AND E2STAT = '10'\n"
                        + "AND E2TYPE = '" + type + "'\n"
                        //+ "AND E2EAAT > 0\n"
                        + ") AS b\n"
                        + "ON b.E2AITM = a.A3AITM\n"
                        + "INNER JOIN\n"
                        + "(SELECT A1CONO, A1DIVI, A1CODE, A1DESC, A1NAME, A1TYPE, A1MUUN, A1RATE, A1REF1, A1REF2, A1REF3\n"
                        + "FROM BRLDTA0100.A1CASU) AS c\n"
                        + "ON c.A1CONO = b.E2CONO\n"
                        + "AND c.A1DIVI = b.E2DIVI\n"
                        + "AND c.A1CODE = b.E2CODE\n"
                        + "UNION ALL\n"
                        + "SELECT '3' AS NO,c.A1REF2 AS ACCC,COALESCE(c.A1REF3,'') AS COST,'' AS BONO,'HO' AS REF1,CHAR(b.E2PECO) AS REF2,'' AS REF3,'Var. ' || TRIM(c.A1DESC) ||  ' for ' || SUBSTRING(CHAR(DATE(SUBSTRING(b.E2PECO,0,5)||'-'||SUBSTRING(b.E2PECO,5,2)||'-'||'01') - 1 MONTH, ISO),0,5)||SUBSTRING(CHAR(DATE(SUBSTRING(b.E2PECO,0,5)||'-'||SUBSTRING(b.E2PECO,5,2)||'-'||'01') - 1 MONTH, ISO),6,2) AS VOUC,ROUND(SUM(E2EAAT) * -1,4) AS AMT,b.E2STAT\n"
                        + "FROM BRLDTA0100.A3CAWC a,BRLDTA0100.E2ALLO b,BRLDTA0100.A1CASU c\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3ASTR = '0'\n"
                        + "AND b.E2CONO = A3CONO\n"
                        + "AND b.E2DIVI = A3DIVI\n"
                        + "AND b.E2CODE = A3CODE\n"
                        + "AND b.E2AITM = A3AITM\n"
                        + "AND b.E2PECO = '" + period + "'\n"
                        + "AND b.E2STAT = '10'\n"
                        + "AND b.E2TYPE = '" + type + "'\n"
                        + "AND c.A1CONO = A3CONO\n"
                        + "AND c.A1DIVI = A3DIVI\n"
                        + "AND c.A1CODE = A3CODE\n"
                        + "GROUP BY c.A1REF2,c.A1REF3,c.A1DESC,b.E2PECO,b.E2STAT\n"
                        + "ORDER BY NO";
                System.out.println("Voucher\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("NO", mRes.getString(1).trim());
                    mMap.put("ACCC", mRes.getString(2).trim());
                    mMap.put("COST", mRes.getString(3).trim());
                    mMap.put("BONO", mRes.getString(4).trim());
                    mMap.put("REF1", mRes.getString(5).trim());
                    mMap.put("REF2", mRes.getString(6).trim());
                    mMap.put("REF3", mRes.getString(7).trim());
                    mMap.put("VOUC", mRes.getString(8).trim());
                    mMap.put("AMT", mRes.getString(9).trim());
                    mMap.put("E2STAT", mRes.getString(10).trim());
                    mJSonArr.put(mMap);
//                    System.out.println(mJSonArr);
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

        return mJSonArr;

    }

    public static List<String> RSVoucher(String cono, String divi, String code, String period, String type) {

        List<String> getList = new ArrayList<String>();
        Connection conn = null;
        try {
            conn = ConnectDB2.ConnectionDB();
            Statement stmt = conn.createStatement();
            String query = "SELECT NO,a.A3ACCT AS ACCC,a.A3AAC1 AS COST,'' AS BONO,SUBSTRING(A3ADES,0,9) AS REF1,A3ARE2 AS REF2,A3ARE3 AS REF3,'Accu. ' || TRIM(c.A1DESC) ||  ' for ' || TRIM(b.E2PECO) AS VOUC,ROUND(b.E2EAAT,4) AS AMT,b.E2STAT\n"
                    + "FROM\n"
                    + "(SELECT NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "FROM \n"
                    + "(SELECT '1' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "FROM BRLDTA0100.A3CAWC\n"
                    + "WHERE A3CONO = '" + cono + "'\n"
                    + "AND A3DIVI = '" + divi + "'\n"
                    + "AND A3CODE = '" + code + "'\n"
                    + "AND A3SLVL = '2'\n"
                    + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "ORDER BY A3STID,A3AITM) a\n"
                    + "UNION ALL \n"
                    + "(SELECT '2' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "FROM BRLDTA0100.A3CAWC a\n"
                    + "WHERE A3CONO = '" + cono + "'\n"
                    + "AND A3DIVI = '" + divi + "'\n"
                    + "AND A3CODE = '" + code + "'\n"
                    + "AND A3SLVL = '1'\n"
                    + "AND A3ASTR = '0'\n"
                    + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "ORDER BY A3STID,A3AITM)) AS a\n"
                    + "INNER JOIN\n"
                    + "(SELECT E2CONO, E2DIVI, E2CODE, E2STID, E2SLVL, E2AITM, E2PECO, E2METS, E2METE, E2UEHR, E2HOUR, E2EPQT, E2PERS, E2EAQT, E2EAAT, E2STAT\n"
                    + "FROM BRLDTA0100.E2ALLO a\n"
                    + "WHERE E2CONO = '" + cono + "'\n"
                    + "AND E2DIVI = '" + divi + "'\n"
                    + "AND E2CODE = '" + code + "'\n"
                    + "AND E2PECO = '" + period + "'\n"
                    + "AND E2STAT = '10'\n"
                    + "AND E2TYPE = '" + type + "'\n"
                    + "AND E2EAAT > 0\n"
                    + ") AS b\n"
                    + "ON b.E2AITM = a.A3AITM\n"
                    + "INNER JOIN\n"
                    + "(SELECT A1CONO, A1DIVI, A1CODE, A1DESC, A1NAME, A1TYPE, A1MUUN, A1RATE, A1REF1, A1REF2\n"
                    + "FROM BRLDTA0100.A1CASU) AS c\n"
                    + "ON c.A1CONO = b.E2CONO\n"
                    + "AND c.A1DIVI = b.E2DIVI\n"
                    + "AND c.A1CODE = b.E2CODE\n"
                    + "UNION ALL\n"
                    + "SELECT '3' AS NO,c.A1REF1 AS ACCC,'' AS COST,'' AS BONO,'HO' AS REF1,CHAR(b.E2PECO) AS REF2,'' AS REF3,'Accu. ' || TRIM(c.A1DESC) ||  ' for ' || TRIM(b.E2PECO) AS VOUC,ROUND(SUM(E2EAAT) * -1,4) AS AMT,b.E2STAT\n"
                    + "FROM BRLDTA0100.A3CAWC a,BRLDTA0100.E2ALLO b,BRLDTA0100.A1CASU c\n"
                    + "WHERE A3CONO = '" + cono + "'\n"
                    + "AND A3DIVI = '" + divi + "'\n"
                    + "AND A3CODE = '" + code + "'\n"
                    + "AND A3ASTR = '0'\n"
                    + "AND b.E2CONO = A3CONO\n"
                    + "AND b.E2DIVI = A3DIVI\n"
                    + "AND b.E2CODE = A3CODE\n"
                    + "AND b.E2AITM = A3AITM\n"
                    + "AND b.E2PECO = '" + period + "'\n"
                    + "AND b.E2STAT = '10'\n"
                    + "AND b.E2TYPE = '" + type + "'\n"
                    + "AND c.A1CONO = A3CONO\n"
                    + "AND c.A1DIVI = A3DIVI\n"
                    + "AND c.A1CODE = A3CODE\n"
                    + "GROUP BY c.A1REF1,c.A1DESC,b.E2PECO,b.E2STAT\n"
                    + "ORDER BY NO";
            System.out.println("RSVoucher\n" + query);
            ResultSet mRes = stmt.executeQuery(query);

            while (mRes.next()) {
                getList.add(
                        mRes.getString("NO").trim() + " ; "
                        + mRes.getString("ACCC").trim() + " ; "
                        + mRes.getString("COST").trim() + " ; "
                        + mRes.getString("BONO").trim() + " ; "
                        + mRes.getString("REF1").trim() + " ; "
                        + mRes.getString("REF2").trim() + " ; "
                        + mRes.getString("REF3").trim() + " ; "
                        + mRes.getString("VOUC").trim() + " ; "
                        + mRes.getString("AMT").trim() + " ; "
                        + mRes.getString("E2STAT").trim());
            }
            return getList;

        } catch (Exception ex) {
            System.out.println(ex.toString());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    System.out.println(ex.toString());
                }
            }
        }
        return null;
    }

    public static List<String> RSVoucherVariance(String cono, String divi, String code, String period, String type) {

        List<String> getList = new ArrayList<String>();
        Connection conn = null;
        try {
            conn = ConnectDB2.ConnectionDB();
            Statement stmt = conn.createStatement();
            String query = "SELECT NO,a.A3ACCT AS ACCC,a.A3AAC1 AS COST,'' AS BONO,SUBSTRING(A3ADES,0,9) AS REF1,A3ARE2 AS REF2,A3ARE3 AS REF3,'Var. ' || TRIM(c.A1DESC) ||  ' for ' || SUBSTRING(CHAR(DATE(SUBSTRING(b.E2PECO,0,5)||'-'||SUBSTRING(b.E2PECO,5,2)||'-'||'01') - 1 MONTH, ISO),0,5)||SUBSTRING(CHAR(DATE(SUBSTRING(b.E2PECO,0,5)||'-'||SUBSTRING(b.E2PECO,5,2)||'-'||'01') - 1 MONTH, ISO),6,2) AS VOUC,ROUND(b.E2EAAT,4) AS AMT,b.E2STAT\n"
                    + "FROM\n"
                    + "(SELECT NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "FROM \n"
                    + "(SELECT '1' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "FROM BRLDTA0100.A3CAWC\n"
                    + "WHERE A3CONO = '" + cono + "'\n"
                    + "AND A3DIVI = '" + divi + "'\n"
                    + "AND A3CODE = '" + code + "'\n"
                    + "AND A3SLVL = '2'\n"
                    + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "ORDER BY A3STID,A3AITM) a\n"
                    + "UNION ALL \n"
                    + "(SELECT '2' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "FROM BRLDTA0100.A3CAWC a\n"
                    + "WHERE A3CONO = '" + cono + "'\n"
                    + "AND A3DIVI = '" + divi + "'\n"
                    + "AND A3CODE = '" + code + "'\n"
                    + "AND A3SLVL = '1'\n"
                    + "AND A3ASTR = '0'\n"
                    + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "ORDER BY A3STID,A3AITM)) AS a\n"
                    + "INNER JOIN\n"
                    + "(SELECT E2CONO, E2DIVI, E2CODE, E2STID, E2SLVL, E2AITM, E2PECO, E2METS, E2METE, E2UEHR, E2HOUR, E2EPQT, E2PERS, E2EAQT, E2EAAT, E2STAT\n"
                    + "FROM BRLDTA0100.E2ALLO a\n"
                    + "WHERE E2CONO = '" + cono + "'\n"
                    + "AND E2DIVI = '" + divi + "'\n"
                    + "AND E2CODE = '" + code + "'\n"
                    + "AND E2PECO = '" + period + "'\n"
                    + "AND E2STAT = '10'\n"
                    + "AND E2TYPE = '" + type + "'\n"
                    //                    + "AND E2EAAT > 0\n"
                    + ") AS b\n"
                    + "ON b.E2AITM = a.A3AITM\n"
                    + "INNER JOIN\n"
                    + "(SELECT A1CONO, A1DIVI, A1CODE, A1DESC, A1NAME, A1TYPE, A1MUUN, A1RATE, A1REF1, A1REF2, A1REF3\n"
                    + "FROM BRLDTA0100.A1CASU) AS c\n"
                    + "ON c.A1CONO = b.E2CONO\n"
                    + "AND c.A1DIVI = b.E2DIVI\n"
                    + "AND c.A1CODE = b.E2CODE\n"
                    + "UNION ALL\n"
                    + "SELECT '3' AS NO,c.A1REF2 AS ACCC,c.A1REF3 AS COST,'' AS BONO,'HO' AS REF1,CHAR(b.E2PECO) AS REF2,'' AS REF3,'Var. ' || TRIM(c.A1DESC) ||  ' for ' || SUBSTRING(CHAR(DATE(SUBSTRING(b.E2PECO,0,5)||'-'||SUBSTRING(b.E2PECO,5,2)||'-'||'01') - 1 MONTH, ISO),0,5)||SUBSTRING(CHAR(DATE(SUBSTRING(b.E2PECO,0,5)||'-'||SUBSTRING(b.E2PECO,5,2)||'-'||'01') - 1 MONTH, ISO),6,2) AS VOUC,ROUND(SUM(E2EAAT) * -1,4) AS AMT,b.E2STAT\n"
                    + "FROM BRLDTA0100.A3CAWC a,BRLDTA0100.E2ALLO b,BRLDTA0100.A1CASU c\n"
                    + "WHERE A3CONO = '" + cono + "'\n"
                    + "AND A3DIVI = '" + divi + "'\n"
                    + "AND A3CODE = '" + code + "'\n"
                    + "AND A3ASTR = '0'\n"
                    + "AND b.E2CONO = A3CONO\n"
                    + "AND b.E2DIVI = A3DIVI\n"
                    + "AND b.E2CODE = A3CODE\n"
                    + "AND b.E2AITM = A3AITM\n"
                    + "AND b.E2PECO = '" + period + "'\n"
                    + "AND b.E2STAT = '10'\n"
                    + "AND b.E2TYPE = '" + type + "'\n"
                    + "AND c.A1CONO = A3CONO\n"
                    + "AND c.A1DIVI = A3DIVI\n"
                    + "AND c.A1CODE = A3CODE\n"
                    + "GROUP BY c.A1REF2,c.A1REF3,c.A1DESC,b.E2PECO,b.E2STAT\n"
                    + "ORDER BY NO";
            System.out.println("RSVoucherVariance\n" + query);
            ResultSet mRes = stmt.executeQuery(query);

            while (mRes.next()) {
                getList.add(
                        mRes.getString("NO").trim() + " ; "
                        + mRes.getString("ACCC").trim() + " ; "
                        + mRes.getString("COST").trim() + " ; "
                        + mRes.getString("BONO").trim() + " ; "
                        + mRes.getString("REF1").trim() + " ; "
                        + mRes.getString("REF2").trim() + " ; "
                        + mRes.getString("REF3").trim() + " ; "
                        + mRes.getString("VOUC").trim() + " ; "
                        + mRes.getString("AMT").trim() + " ; "
                        + mRes.getString("E2STAT").trim());
            }
            return getList;

        } catch (Exception ex) {
            System.out.println(ex.toString());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    System.out.println(ex.toString());
                }
            }
        }
        return null;
    }

    public static JSONArray Inventory(String cono, String divi, String code, String period, String type) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT NO,a.A3ACCT AS ACCC,a.A3AAC1 AS COST,A3ADES AS DESC,A3ARE1 AS REF1,A3ARE2 AS REF2,A3ARE3 AS REF3,'Accu. ' || TRIM(c.A1DESC) ||  ' for ' || TRIM(b.E2PECO) AS VOUC,ROUND(b.E2EAQT,4) AS QTY,b.E2STAT\n"
                        + "FROM\n"
                        + "(SELECT NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM \n"
                        + "(SELECT '1' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM BRLDTA0100.A3CAWC\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3SLVL = '2'\n"
                        + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "ORDER BY A3STID,A3AITM) a\n"
                        + "UNION ALL \n"
                        + "(SELECT '2' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM BRLDTA0100.A3CAWC a\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3SLVL = '1'\n"
                        + "AND A3ASTR = '0'\n"
                        + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "ORDER BY A3STID,A3AITM)) AS a\n"
                        + "INNER JOIN\n"
                        + "(SELECT E2CONO, E2DIVI, E2CODE, E2STID, E2SLVL, E2AITM, E2PECO, E2METS, E2METE, E2UEHR, E2HOUR, E2EPQT, E2PERS, E2EAQT, E2EAAT, E2STAT\n"
                        + "FROM BRLDTA0100.E2ALLO a\n"
                        + "WHERE E2CONO = '" + cono + "'\n"
                        + "AND E2DIVI = '" + divi + "'\n"
                        + "AND E2CODE = '" + code + "'\n"
                        + "AND E2PECO = '" + period + "'\n"
                        + "AND E2STAT = '10'\n"
                        //+ "AND E2EAAT > 0\n"
                        + "AND E2TYPE = '" + type + "'\n"
                        + ") AS b\n"
                        + "ON b.E2AITM = a.A3AITM\n"
                        + "INNER JOIN\n"
                        + "(SELECT A1CONO, A1DIVI, A1CODE, A1DESC, A1NAME, A1TYPE, A1MUUN, A1RATE, A1REF1, A1REF2\n"
                        + "FROM BRLDTA0100.A1CASU) AS c\n"
                        + "ON c.A1CONO = b.E2CONO\n"
                        + "AND c.A1DIVI = b.E2DIVI\n"
                        + "AND c.A1CODE = b.E2CODE\n"
                        + "ORDER BY NO";
                System.out.println("Inventory\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("NO", mRes.getString(1).trim());
                    mMap.put("ACCC", mRes.getString(2).trim());
                    mMap.put("COST", mRes.getString(3).trim());
                    mMap.put("DESC", mRes.getString(4).trim());
                    mMap.put("REF1", mRes.getString(5).trim());
                    mMap.put("REF2", mRes.getString(6).trim());
                    mMap.put("REF3", mRes.getString(7).trim());
                    mMap.put("VOUC", mRes.getString(8).trim());
                    mMap.put("QTY", mRes.getString(9).trim());
                    mMap.put("E2STAT", mRes.getString(10).trim());
                    mJSonArr.put(mMap);
//                    System.out.println(mJSonArr);
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

        return mJSonArr;

    }

    public static List<String> RSInventory(String cono, String divi, String code, String period) {

        List<String> getList = new ArrayList<String>();
        Connection conn = null;
        try {
            conn = ConnectDB2.ConnectionDB();
            Statement stmt = conn.createStatement();
            String query = "SELECT NO,a.A3ACCT AS ACCC,a.A3AAC1 AS COST,A3ADES AS DESC,A3ARE1 AS REF1,A3ARE2 AS REF2,A3ARE3 AS REF3,'Accu. ' || TRIM(c.A1DESC) ||  ' for ' || TRIM(b.E2PECO) AS VOUC,ROUND(b.E2EAQT,4) AS QTY,b.E2STAT\n"
                    + "FROM\n"
                    + "(SELECT NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "FROM \n"
                    + "(SELECT '1' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "FROM BRLDTA0100.A3CAWC\n"
                    + "WHERE A3CONO = '" + cono + "'\n"
                    + "AND A3DIVI = '" + divi + "'\n"
                    + "AND A3CODE = '" + code + "'\n"
                    + "AND A3SLVL = '2'\n"
                    + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "ORDER BY A3STID,A3AITM) a\n"
                    + "UNION ALL \n"
                    + "(SELECT '2' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "FROM BRLDTA0100.A3CAWC a\n"
                    + "WHERE A3CONO = '" + cono + "'\n"
                    + "AND A3DIVI = '" + divi + "'\n"
                    + "AND A3CODE = '" + code + "'\n"
                    + "AND A3SLVL = '1'\n"
                    + "AND A3ASTR = '0'\n"
                    + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                    + "ORDER BY A3STID,A3AITM)) AS a\n"
                    + "INNER JOIN\n"
                    + "(SELECT E2CONO, E2DIVI, E2CODE, E2STID, E2SLVL, E2AITM, E2PECO, E2METS, E2METE, E2UEHR, E2HOUR, E2EPQT, E2PERS, E2EAQT, E2EAAT, E2STAT\n"
                    + "FROM BRLDTA0100.E2ALLO a\n"
                    + "WHERE E2CONO = '" + cono + "'\n"
                    + "AND E2DIVI = '" + divi + "'\n"
                    + "AND E2CODE = '" + code + "'\n"
                    + "AND E2PECO = '" + period + "'\n"
                    + "AND E2STAT = '10'\n"
                    + "AND E2EAQT > 0\n"
                    + ") AS b\n"
                    + "ON b.E2AITM = a.A3AITM\n"
                    + "INNER JOIN\n"
                    + "(SELECT A1CONO, A1DIVI, A1CODE, A1DESC, A1NAME, A1TYPE, A1MUUN, A1RATE, A1REF1, A1REF2\n"
                    + "FROM BRLDTA0100.A1CASU) AS c\n"
                    + "ON c.A1CONO = b.E2CONO\n"
                    + "AND c.A1DIVI = b.E2DIVI\n"
                    + "AND c.A1CODE = b.E2CODE\n"
                    + "ORDER BY NO";
            System.out.println("RSInventory\n" + query);
            ResultSet mRes = stmt.executeQuery(query);

            while (mRes.next()) {
                getList.add(
                        mRes.getString("NO").trim() + " ; "
                        + mRes.getString("ACCC").trim() + " ; "
                        + mRes.getString("COST").trim() + " ; "
                        + mRes.getString("DESC").trim() + " ; "
                        + mRes.getString("REF1").trim() + " ; "
                        + mRes.getString("REF2").trim() + " ; "
                        + mRes.getString("REF3").trim() + " ; "
                        + mRes.getString("VOUC").trim() + " ; "
                        + mRes.getString("QTY").trim() + " ; "
                        + mRes.getString("E2STAT").trim());
            }
            return getList;

        } catch (Exception ex) {
            System.out.println(ex.toString());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    System.out.println(ex.toString());
                }
            }
        }
        return null;
    }

    public static List<String> RSInvenLot(String cono, String divi, String whs, String item) {

        List<String> getList = new ArrayList<String>();
        Connection conn = null;
        try {
            conn = ConnectDB2.ConnectionDB();
            Statement stmt = conn.createStatement();
            String query = "SELECT MLITNO,MLBANO,DECIMAL(MLSTQT-MLALQT,10,2) ONHAND\n"
                    + "FROM M3FDBPRD.MITLOC\n"
                    + "WHERE MLCONO = '" + cono + "'\n"
                    + "AND MLWHLO = '" + whs + "'\n"
                    + "AND MLSTAS = 2\n"
                    + "AND MLITNO = '" + item + "'\n"
                    + "ORDER BY MLPRDT";
            System.out.println("RSInvenLot\n" + query);
            ResultSet mRes = stmt.executeQuery(query);

            while (mRes.next()) {
                getList.add(
                        mRes.getString("MLITNO").trim() + " ; "
                        + mRes.getString("MLBANO").trim() + " ; "
                        + mRes.getString("ONHAND").trim());
            }
            return getList;

        } catch (Exception ex) {
            System.out.println(ex.toString());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    System.out.println(ex.toString());
                }
            }
        }
        return null;
    }

    public static JSONArray Monitor(String cono, String divi, String code, String period, String type) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectDB2.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT NO,a.A3ACCT AS ACCC,a.A3AAC1 AS COST,'' AS BONO,A3ADES AS REF1,A3ARE2 AS REF2,A3ARE3 AS REF3,'Accu. ' || TRIM(c.A1DESC) ||  ' for ' || TRIM(b.E2PECO) AS VOUC,ROUND(b.E2EAQT,4) AS QTY,ROUND(b.E2EAAT,4) AS AMT,b.E2STAT\n"
                        + "FROM\n"
                        + "(SELECT NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM \n"
                        + "(SELECT '1' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM BRLDTA0100.A3CAWC\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3SLVL = '2'\n"
                        + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "ORDER BY A3STID,A3AITM) a\n"
                        + "UNION ALL \n"
                        + "(SELECT '2' AS NO,A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "FROM BRLDTA0100.A3CAWC a\n"
                        + "WHERE A3CONO = '" + cono + "'\n"
                        + "AND A3DIVI = '" + divi + "'\n"
                        + "AND A3CODE = '" + code + "'\n"
                        + "AND A3SLVL = '1'\n"
                        + "AND A3ASTR = '0'\n"
                        + "GROUP BY A3STID,A3AITM,A3ADES,A3ACCT,A3AAC1,A3AAC2,A3ABOI,A3ARE1,A3ARE2,A3ARE3\n"
                        + "ORDER BY A3STID,A3AITM)) AS a\n"
                        + "INNER JOIN\n"
                        + "(SELECT E2CONO, E2DIVI, E2CODE, E2STID, E2SLVL, E2AITM, E2PECO, E2METS, E2METE, E2UEHR, E2HOUR, E2EPQT, E2PERS, E2EAQT, E2EAAT, E2STAT\n"
                        + "FROM BRLDTA0100.E2ALLO a\n"
                        + "WHERE E2CONO = '" + cono + "'\n"
                        + "AND E2DIVI = '" + divi + "'\n"
                        + "AND E2CODE = '" + code + "'\n"
                        + "AND E2PECO = '" + period + "'\n"
                        //+ "AND E2STAT = '10'\n"
                        //+ "AND E2EAAT > 0\n"
                        + "AND E2TYPE = '" + type + "'\n"
                        + ") AS b\n"
                        + "ON b.E2AITM = a.A3AITM\n"
                        + "INNER JOIN\n"
                        + "(SELECT A1CONO, A1DIVI, A1CODE, A1DESC, A1NAME, A1TYPE, A1MUUN, A1RATE, A1REF1, A1REF2\n"
                        + "FROM BRLDTA0100.A1CASU) AS c\n"
                        + "ON c.A1CONO = b.E2CONO\n"
                        + "AND c.A1DIVI = b.E2DIVI\n"
                        + "AND c.A1CODE = b.E2CODE\n"
                        + "ORDER BY NO";
                System.out.println("Monitor\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("NO", mRes.getString(1).trim());
                    mMap.put("ACCC", mRes.getString(2).trim());
                    mMap.put("COST", mRes.getString(3).trim());
                    mMap.put("BONO", mRes.getString(4).trim());
                    mMap.put("REF1", mRes.getString(5).trim());
                    mMap.put("REF2", mRes.getString(6).trim());
                    mMap.put("REF3", mRes.getString(7).trim());
                    mMap.put("VOUC", mRes.getString(8).trim());
                    mMap.put("QTY", mRes.getString(9).trim());
                    mMap.put("AMT", mRes.getString(10).trim());
                    mMap.put("E2STAT", mRes.getString(11).trim());
                    mJSonArr.put(mMap);
//                    System.out.println(mJSonArr);
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

        return mJSonArr;

    }

    public static JSONArray getRentalDetail(String cono, String divi, String whs, String period) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectSQLServer.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT RDCONO, RDDIVI, RDWARE, RDPERI, RDROUN, RDLINE, RDBRL_DOC, CONVERT(VARCHAR, RDDATE, 23) AS RDDATE, RDDOC, RDSTAT, RDAGIN, RDITNO, RDLOTN, RDQTY, RDTOTA_KGS \n"
                        + ", COALESCE(RDDEL_ORDER,'') AS RDDEL_ORDER, COALESCE(RDDEL_BRL_DOC,'') AS RDDEL_BRL_DOC, COALESCE(CONVERT(VARCHAR, RDDATE, 23),'') AS RDDEL_DATE, COALESCE(RDDEL_DOC,'') AS RDDEL_DOC, COALESCE(RDDEL_ITEM,'') AS RDDEL_ITEM, COALESCE(RDDEL_PACK,0) AS RDDEL_PACK, COALESCE(RDDEL_KGS,0) AS RDDEL_KGS, COALESCE(RDDEL_FLAG,'') AS RDDEL_FLAG, RDBAL_QTY, RDBAL_KGS \n"
                        + ", RDDATE_DIFF, RDRENT_ROUND, RDDUE_DATE, RDDUE_DATE1, RDDUE_DATE2, RDDUE_DATE3, RDFRIS_CHK, RDTHPA_CHK, RDREN_FIRST, RDREN_1TO15, RDREN_16TO30, RDREN_31TO14, RDSUM_TOTAL\n"
                        + "FROM T_RENTALDETAIL\n"
                        + "WHERE RDCONO = '" + cono + "'\n"
                        + "AND RDDIVI = '" + divi + "'\n"
                        + "AND RDWARE = '" + whs + "'\n"
                        + "AND RDPERI = '" + period + "'\n"
                        + "ORDER BY RDCONO, RDDIVI, RDWARE, RDPERI, RDROUN, RDLINE";
                System.out.println("RentalHead\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("RDCONO", mRes.getString(1).trim());
                    mMap.put("RDDIVI", mRes.getString(2).trim());
                    mMap.put("RDWARE", mRes.getString(3).trim());
                    mMap.put("RDPERI", mRes.getString(4).trim());
                    mMap.put("RDROUN", mRes.getString(5).trim());
                    mMap.put("RDLINE", mRes.getString(6).trim());
                    mMap.put("RDBRL_DOC", mRes.getString(7).trim());
                    mMap.put("RDDATE", mRes.getString(8).trim());
                    mMap.put("RDDOC", mRes.getString(9).trim());
                    mMap.put("RDSTAT", mRes.getString(10).trim());
                    mMap.put("RDAGIN", mRes.getString(11).trim());
                    mMap.put("RDITNO", mRes.getString(12).trim());
                    mMap.put("RDLOTN", mRes.getString(13).trim());
                    mMap.put("RDQTY", mRes.getString(14).trim());
                    mMap.put("RDTOTA_KGS", mRes.getString(15).trim());
                    mMap.put("RDDEL_ORDER", mRes.getString(16).trim());
                    mMap.put("RDDEL_BRL_DOC", mRes.getString(17).trim());
                    mMap.put("RDDEL_DATE", mRes.getString(18).trim());
                    mMap.put("RDDEL_DOC", mRes.getString(19).trim());
                    mMap.put("RDDEL_ITEM", mRes.getString(20).trim());
                    mMap.put("RDDEL_PACK", mRes.getString(21).trim());
                    mMap.put("RDDEL_KGS", mRes.getString(22).trim());
                    mMap.put("RDDEL_FLAG", mRes.getString(23).trim());
                    mMap.put("RDBAL_QTY", mRes.getString(24).trim());
                    mMap.put("RDDATE_DIFF", mRes.getString(25).trim());
                    mMap.put("RDRENT_ROUND", mRes.getString(26).trim());
                    mMap.put("RDDUE_DATE", mRes.getString(27).trim());
                    mMap.put("RDDUE_DATE1", mRes.getString(28).trim());
                    mMap.put("RDDUE_DATE2", mRes.getString(29).trim());
                    mMap.put("RDDUE_DATE3", mRes.getString(30).trim());
                    mMap.put("RDFRIS_CHK", mRes.getString(31).trim());
                    mMap.put("RDTHPA_CHK", mRes.getString(32).trim());
                    mMap.put("RDREN_FIRST", mRes.getString(33).trim());
                    mMap.put("RDREN_1TO15", mRes.getString(34).trim());
                    mMap.put("RDREN_16TO30", mRes.getString(35).trim());
                    mMap.put("RDREN_31TO14", mRes.getString(36).trim());
                    mMap.put("RDSUM_TOTAL", mRes.getString(37).trim());
                    mJSonArr.put(mMap);
//                    System.out.println(mJSonArr);
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

        return mJSonArr;

    }

    public static JSONArray getViewRentalDetail(String cono, String divi, String whs, String period) throws Exception {

        JSONArray mJSonArr = new JSONArray();
        Connection conn = ConnectSQLServer.ConnectionDB();

        try {
            if (conn != null) {

                Statement stmt = conn.createStatement();
                String query = "SELECT RDCONO, RDDIVI, RDWARE, RDPERI, RDFDAT, RDTDAT, RDDDAT, RDLINE, RDBRL_DOC, COALESCE(CONVERT(VARCHAR,CONVERT(DATETIME, RDDATE), 23), '') AS RDDATE, RDDOC, RDSTAT, RDAGIN, RDITNO, RDITDE, RDLOTN, RDQTY, RDTOTA_KGS\n"
                        + ", COALESCE(CONVERT(VARCHAR,CONVERT(DATETIME, RDDUE_DATE1), 23), '') AS RDDUE_DATE1, COALESCE(CONVERT(VARCHAR,CONVERT(DATETIME, RDDUE_DATE2), 23), '') AS RDDUE_DATE2, COALESCE(CONVERT(VARCHAR,CONVERT(DATETIME, RDDUE_DATE3), 23), '') AS RDDUE_DATE3\n"
                        + ", RDDEL_ORDER, RDDEL_BRL_DOC, COALESCE(CONVERT(VARCHAR,CONVERT(DATETIME, RDDEL_DATE), 23), '') AS RDDEL_DATE, RDDEL_DOC, RDDEL_PACK, RDDEL_KGS, RDBAL_QTY, RDBAL_KGS\n"
                        + ", RDTHPA_CHK, RDREN_FIRST, RDREN_1TO15, RDREN_16TO30, RDREN_31TO14, RDSUM_TOTAL \n"
                        + "FROM BRLAS400.dbo.V_RENTALDETAIL\n"
                        + "WHERE RDCONO = '" + cono + "'\n"
                        + "AND RDDIVI = '" + divi + "'\n"
                        + "AND RDWARE = '" + whs + "'\n"
                        + "AND RDPERI = '" + period + "'\n"
                        + "ORDER BY RDLINE";
                System.out.println("getViewRentalDetail\n" + query);
                ResultSet mRes = stmt.executeQuery(query);

                while (mRes.next()) {
//                    RDCONO, RDDIVI, RDWARE, RDPERI, RDFDAT, RDTDAT, RDDDAT, RDLINE, RDBRL_DOC, RDDATE, RDDOC, RDSTAT, RDAGIN, RDITNO, RDITDE, RDLOTN, RDQTY, RDTOTA_KGS
                    Map<String, Object> mMap = new HashMap<>();
                    mMap.put("RDCONO", mRes.getString(1).trim());
                    mMap.put("RDDIVI", mRes.getString(2).trim());
                    mMap.put("RDWARE", mRes.getString(3).trim());
                    mMap.put("RDPERI", mRes.getString(4).trim());
                    mMap.put("RDFDAT", mRes.getString(5).trim());
                    mMap.put("RDTDAT", mRes.getString(6).trim());
                    mMap.put("RDDDAT", mRes.getString(7).trim());
                    mMap.put("RDLINE", mRes.getString(8).trim());
                    mMap.put("RDBRL_DOC", mRes.getString(9).trim());
                    mMap.put("RDDATE", mRes.getString(10).trim());
                    mMap.put("RDDOC", mRes.getString(11).trim());
                    mMap.put("RDSTAT", mRes.getString(12).trim());
                    mMap.put("RDAGIN", mRes.getString(13).trim());
                    mMap.put("RDITNO", mRes.getString(14).trim());
                    mMap.put("RDITDE", mRes.getString(15).trim());
                    mMap.put("RDLOTN", mRes.getString(16).trim());
                    mMap.put("RDQTY", mRes.getString(17).trim());
                    mMap.put("RDTOTA_KGS", mRes.getString(18).trim());
//                    , RDDUE_DATE1, RDDUE_DATE2, RDDUE_DATE3
                    mMap.put("RDDUE_DATE1", mRes.getString(19).trim());
                    mMap.put("RDDUE_DATE2", mRes.getString(20).trim());
                    mMap.put("RDDUE_DATE3", mRes.getString(21).trim());
//                    , RDDEL_ORDER, RDDEL_BRL_DOC, RDDEL_DATE, RDDEL_DOC, RDDEL_PACK, RDDEL_KGS, RDBAL_QTY, RDBAL_KGS
                    mMap.put("RDDEL_ORDER", mRes.getString(22).trim());
                    mMap.put("RDDEL_BRL_DOC", mRes.getString(23).trim());
                    mMap.put("RDDEL_DATE", mRes.getString(24).trim());
                    mMap.put("RDDEL_DOC", mRes.getString(25).trim());
                    mMap.put("RDDEL_PACK", mRes.getString(26).trim());
                    mMap.put("RDDEL_KGS", mRes.getString(27).trim());
                    mMap.put("RDBAL_QTY", mRes.getString(28).trim());
                    mMap.put("RDBAL_KGS", mRes.getString(29).trim());
//                    , RDTHPA_CHK, RDREN_FIRST, RDREN_1TO15, RDREN_16TO30, RDREN_31TO14, RDSUM_TOTAL
                    mMap.put("RDTHPA_CHK", mRes.getString(30).trim());
                    mMap.put("RDREN_FIRST", mRes.getString(31).trim());
                    mMap.put("RDREN_1TO15", mRes.getString(32).trim());
                    mMap.put("RDREN_16TO30", mRes.getString(33).trim());
                    mMap.put("RDREN_31TO14", mRes.getString(34).trim());
                    mMap.put("RDSUM_TOTAL", mRes.getString(35).trim());
                    mJSonArr.put(mMap);
//                    System.out.println(mJSonArr);
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

        return mJSonArr;

    }

}
