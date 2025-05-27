/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.br.api;

import com.br.data.Delete;
import com.br.data.Insert;
import com.br.data.Select;
import com.br.data.Update;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author Wattana
 */
public class Action extends HttpServlet {

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException, Exception {
            request.setCharacterEncoding("UTF-8");
        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();

        String path = request.getParameter("path");
        // String path = request.getServletPath();
        System.out.println("path: " + path);

        switch (path) {
            case "Testforreply":
                out.print(Select.Testforreply());
                out.flush();
                break;

            case "getHeaderDate":
                out.print(Select.getHeaderDate(request.getParameter("cono"),
                        request.getParameter("divi"),
                        request.getParameter("month"),
                        request.getParameter("year"),
                        request.getParameter("invoicerd")
                ));
                out.flush();
                break;
            case "getYearHeader":
                out.print(Select.getYearHeader(request.getParameter("cono"),
                        request.getParameter("divi"),
                        request.getParameter("invround")));
                out.flush();
                break;
            case "getYearInvoice":
                out.print(Select.getYearInvoice(
                        request.getParameter("cono"),
                        request.getParameter("divi"),
                        request.getParameter("invround")));
                out.flush();
                break;
            case "getMonthInvoice":
                out.print(Select.getMonthInvoice(request.getParameter("cono"),
                        request.getParameter("divi"),
                        request.getParameter("year"),
                        request.getParameter("invround")));
                out.flush();
                break;
            case "getMonthMaster":
                out.print(Select.getMonthMaster(
                        request.getParameter("cono"),
                        request.getParameter("divi"),
                        request.getParameter("year"),
                        request.getParameter("invround")));
                out.flush();
                break;

            case "getCompany":
                out.print(Select.getCompany());
                out.flush();
                break;

            case "getMasterFinance":
                out.print(Select.getMasterFinance(request.getParameter("cono"),
                        request.getParameter("divi")));
                out.flush();
                break;

            case "getStartDate":
                out.print(Select.getStartDate(
                        request.getParameter("month"),
                        request.getParameter("year"),
                        request.getParameter("invoicerd"),
                        request.getParameter("cono"),
                        request.getParameter("divi")));
                out.flush();
                break;
            //INSERT
            case "addStartMonth":
                out.print(Insert.addStartMonth(
                        request.getParameter("cono"),
                        request.getParameter("divi"),
                        request.getParameter("year"),
                        request.getParameter("month"),
                        request.getParameter("invround"),
                        request.getParameter("customer"),
                        request.getParameter("startdate")));
                out.flush();
                break;

            case "insertfinanceheader":
                out.print(Insert.insertfinanceheader(request.getParameter("company"),
                        request.getParameter("divi"),
                        request.getParameter("customerid"),
                        request.getParameter("startdate"),
                        request.getParameter("month"),
                        request.getParameter("year"),
                        request.getParameter("status"),
                        request.getParameter("invoicerd")));
                out.flush();
                break;
            case "importStartData":
                out.print(Insert.importStartData(request.getParameter("cono"),
                         request.getParameter("divi"),
                         request.getParameter("month"),
                        request.getParameter("year"),
                        request.getParameter("invround")));
                out.flush();
                break;
            case "addHeaderMonth":
                out.print(Insert.addHeaderMonth(request.getParameter("cono"),
                        request.getParameter("divi"),
                        request.getParameter("month"),
                        request.getParameter("year"),
                        request.getParameter("invround"),
                        request.getParameter("customer")));
                out.flush();
                break;
            case "addMaster":
                out.print(Insert.addMaster(request.getParameter("company"),
                        request.getParameter("divi"),
                        request.getParameter("customer"),
                        request.getParameter("invroundinput"),
                        request.getParameter("invroundA"),
                        request.getParameter("type"),
                        request.getParameter("bill"),
                        request.getParameter("bilweek1"),
                        request.getParameter("bilinput1"),
                        request.getParameter("bilinput3"),
                        request.getParameter("bilfirstweek"),
                        request.getParameter("pay"),
                        request.getParameter("pay5weekornot"),
                        request.getParameter("payweek1"),
                        request.getParameter("payweek2"),
                        request.getParameter("payday1"),
                        request.getParameter("payday2"),
                        request.getParameter("payday3"),
                        request.getParameter("payday4"),
                        request.getParameter("paydaysamemonthornot"),
                        request.getParameter("bildesc"),
                        request.getParameter("collectiondesc"),
                        request.getParameter("collectby"),
                        request.getParameter("remark"),
                        request.getParameter("mode")));
                out.flush();
                break;

            // Insert
            case "addFinanceMaster":
                out.print(Insert.insertfinancemaster(request.getParameter("company"),
                        request.getParameter("divi"),
                        request.getParameter("customerid"),
                        request.getParameter("customertype"),
                        request.getParameter("roundinv"),
                        request.getParameter("roundbill"),
                        request.getParameter("roundpay")));
                out.flush();
                break;
            case "importInvoiceData":
                out.print(Insert.importInvoiceData(
                        request.getParameter("month"),
                        request.getParameter("year"),
                        request.getParameter("invround"),
                        request.getParameter("company"),
                        request.getParameter("divi")
                ));
                out.flush();
                break;
            // Update

            case "calculateEndDate":
                out.print(Update.calculateEndDate());
                out.flush();
                break;
            case "updateFinanceMaster":
                out.print(Update.updateFinanceMaster(request.getParameter("company"), request.getParameter("divi"),
                        request.getParameter("customerid"), request.getParameter("customertype"),
                        request.getParameter("roundinv"), request.getParameter("roundbill"),
                        request.getParameter("roundpay"), request.getParameter("roundcasebill"),
                        request.getParameter("roundbillspecial"), request.getParameter("roundcasepay"),
                        request.getParameter("roundpayspecial"), request.getParameter("caseinvoice"),
                        request.getParameter("bildescription"), request.getParameter("coldesciption"),
                        request.getParameter("colby"), request.getParameter("remark")));
                out.flush();
                break;
            case "updateHeaderFinance":
                out.print(Update.updateHeaderFinance(request.getParameter("company"),
                        request.getParameter("customerid"), request.getParameter("billdate"),
                        request.getParameter("paydate"), request.getParameter("startdate"), request.getParameter("enddate")));
                out.flush();
                break;
            case "updateStartFinance":
                out.print(Update.updateStartFinance(
                        request.getParameter("startdate"),
                        request.getParameter("enddate"),
                        request.getParameter("customerid"),
                        request.getParameter("round"),
                        request.getParameter("month"),
                        request.getParameter("year"),
                        request.getParameter("company")));
                out.flush();
                break;

            // Delete
            case "deleteFinanceMaster":
                out.print(Delete.deleteFinanceMaster(request.getParameter("company"),
                        request.getParameter("divi"),
                        request.getParameter("customerid")));
                out.flush();
                break;
            case "deleteStartMonth":
                out.print(Delete.deleteStartMonth(request.getParameter("year"),
                        request.getParameter("year"),
                        request.getParameter("year"),
                        request.getParameter("month"),
                        request.getParameter("invround")));
                out.flush();
                break;
            case "deleteHeaderMonth":
                out.print(Delete.deleteHeaderMonth(request.getParameter("cono"),
                        request.getParameter("divi"),
                        request.getParameter("year"),
                        request.getParameter("month"),
                        request.getParameter("invround")));
                out.flush();
                break;

            case "deleteStartDatecustomer":
                out.print(Select.deleteStartDatecustomer(request.getParameter("cono"),
                        request.getParameter("divi"),
                        request.getParameter("customer")));
                out.flush();
                break;
            case "deleteHeaderDatecustomer":
                out.print(Select.deleteHeaderDatecustomer(
                        request.getParameter("cono"),
                        request.getParameter("divi"),
                        request.getParameter("customer")));
                out.flush();
                break;

            default:
                break;
        }

    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            processRequest(request, response);
        } catch (Exception ex) {
            Logger.getLogger(Action.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            processRequest(request, response);
        } catch (Exception ex) {
            Logger.getLogger(Action.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}
