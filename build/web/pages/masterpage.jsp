<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>

<%
    if (session.getAttribute("cono") == null) {
//        response.sendRedirect("login.jsp");
    }
%>

<style>

    td.limitext{
        white-space: nowrap;
        width: 100px;
        overflow: hidden;
        text-overflow:ellipsis;
        /*text-overflow: ellipsis !important;*/
    }

    button {
        outline: none !important;
        border: hidden;
        background: springgreen;
    }

    .ui-widget *, .ui-widget input, .ui-widget select, .ui-widget button {
        font-family: 'Helvetica Neue Light', 'Open Sans', Helvetica;
        font-size: 14px;
        font-weight: 300 !important;
    }

    .details-form-field input,
    .details-form-field select {
        width: 250px;
        float: right;
    }

    .details-form-field {
        margin: 25px 0;
    }

    .details-form-field:first-child {
        margin-top: 10px;
    }

    .details-form-field:last-child {
        margin-bottom: 10px;
    }

    .details-form-field button {
        display: block;
        width: 100px;
        margin: 0 auto;
    }

    input.error, select.error {
        border: 1px solid #ff9999;
        background: #ffeeee;
    }

    label.error {
        float: right;
        margin-left: 100px;
        font-size: .8em;
        color: #ff6666;
    }

    .form-control{
        display:block;
        width:100%;
        height:27px;
        padding:2px 0px;
        font-size:14px;
        line-height:1.42857143;
        color:#555;
        background-color:#fff;
        background-image:none;
        border:1px solid #ccc;
        border-radius:4px;
        -webkit-box-shadow:inset 0 1px 1px rgba(0,0,0,.075);
        box-shadow:inset 0 1px 1px rgba(0,0,0,.075);
        -webkit-transition:border-color ease-in-out .15s,-webkit-box-shadow ease-in-out .15s;
        -o-transition:border-color ease-in-out .15s,box-shadow ease-in-out .15s;
        transition:border-color ease-in-out .15s,box-shadow ease-in-out .15s

    }

    .form-control:focus{
        border-color:#66afe9;
        outline:0;
        -webkit-box-shadow:inset 0 1px 1px rgba(0,0,0,.075),0 0 8px rgba(102,175,233,.6);
        box-shadow:inset 0 1px 1px rgba(0,0,0,.075),0 0 8px rgba(102,175,233,.6)
    }

    .form-control::-moz-placeholder{
        color:#999;
        opacity:1
    }

    .form-control:-ms-input-placeholder{
        color:#999
    }

    .form-control::-webkit-input-placeholder{
        color:#999
    }

    .form-control::-ms-expand{
        background-color:transparent;
        border:0
    }

    .form-control[disabled],.form-control[readonly],fieldset[disabled] .form-control{
        background-color:#eee;
        opacity:1
    }

    .form-control[disabled],fieldset[disabled] .form-control{
        cursor:not-allowed
    }

    td.limitext{
        white-space: nowrap;
        width: 100px;
        overflow: hidden;
        text-overflow:ellipsis;
        /*text-overflow: ellipsis !important;*/
    }

    .wrap-login100 {
        width: 100%;
        background: #de5f5f;
        border-radius: 2px;
        overflow: hidden;
        padding: 10px 10px 10px 10px;

        box-shadow: 0 5px 10px 0px rgba(0, 0, 0, 0.1);
        -moz-box-shadow: 0 5px 10px 0px rgba(0, 0, 0, 0.1);
        -webkit-box-shadow: 0 5px 10px 0px rgba(0, 0, 0, 0.1);
        -o-box-shadow: 0 5px 10px 0px rgba(0, 0, 0, 0.1);
        -ms-box-shadow: 0 5px 10px 0px rgba(0, 0, 0, 0.1);
    }

    td.lvgb{
        background-color: #bdd4ff !important;
    }

    td.lvgb-yello{
        background-color: #fff3bd !important;
    }

    td.lvgb-green{
        background-color: #72fc9e !important;
    }

    .container-w100{
        width: 100%;
        padding-left: 10px;
        padding-right: 10px;

    }

    .jsgrid-table {
        border-collapse: separate;
    }

    .jsgrid-grid-body td, .jsgrid-grid-header td, .jsgrid-grid-header th {
        border-left: 0;
        border-top: 0;
    }

</style>

<script src="./assets/daterangepicker/datetimeui.js"></script>

<section class="container-w100">
    <div class="wrap-login100" style="width: 100%;margin-bottom: 0px;">
        <label for="txtPeriod">Master Table Data</label>
        <br>
        <label for="txtPeriod" id = "vCompanyInputlab">Company(10 DEFAULT):</label>
        <select class="form-control form-control-user" name="company" id="vCompanyInput" style="width: 300px">
            <!--<option value="" selected="selected">Select Company</option>-->
        </select>
        <!--<input type="text" id="vCompanyInput" name="company" style="width: 70px">-->
        <label for="txtPeriod" id = "vCustomerInputlab">Customer Code:</label>
        <input type="text" id="vCustomerInput" name="customer">
        <!--<label for="txtPeriod"  id="vTypeInputlab">Type:</label>-->
        <label for="txtPeriod"  id="vTypeInputlab">Type:</label>
        <select style="width: 40px" name="type"  id="vTypeInput">
            <option value="1" selected="selected">1</option>
            <option value="2">2</option>
            <option value="3">3</option>
        </select>

        <br>
        <label for="txtPeriod" id="vInvoiceInputlab">วิธีคิดวันInvoice</label>
        <select style="width: 200px" name="invoice"  id="vInvoiceInput">
            <option value="" selected="selected">ปกติ</option>
            <option value="1">จบวันที่(ใช้กับ 30 วัน)</option>
            <option value="2">ก่อนวันสุดท้าย 1 วัน(ใช้กับ30)</option>
        </select>
        <label for="txtPeriod" id="vInvoiceAlab">จบ Invoiceวันที่</label>
        <input type="text" id="vInvoiceA" name="InvoiceA" style="width: 70px">
        <br>
        <label for="txtPeriod" id="vBillInputlab">วิธีคิดวันวางบิล</label>
        <select style="width: 100px" name="type"  id="vBillInput">
            <option value="" selected="selected">ปกติ</option>
            <option value="1">วัน A ของสัปดาห์ที่ B</option>
        </select>
        <br>
        <label for="txtPeriod" id="vBilweek1lab">วัน</label>
        <select style="width: 100px" name="type"  id="vBilweek1">
            <option value="2" selected="selected">จันทร์</option>
            <option value="3">อังคาร</option>
            <option value="4">พุธ</option>
            <option value="5">พฤหัสบดี</option>
            <option value="6">ศุกร์</option>
            <option value="7">เสาร์</option>
            <option value="1">อาทิตย์</option>
        </select>
        <label for="txtPeriod" id="vBilInput1lab">รอบวางบิลนับจากบิลสุดท้าย</label>
        <input type="text" id="vBilInput1" name="BilvalueA" style="width: 70px">
        <label for="txtPeriod" id="vBilweek2lab">ถึงวัน</label>
        <select style="width: 100px" name="type"  id="vBilweek2">
            <option value="2" selected="selected">จันทร์</option>
            <option value="3">อังคาร</option>
            <option value="4">พุธ</option>
            <option value="5">พฤหัสบดี</option>
            <option value="6">ศุกร์</option>
            <option value="7">เสาร์</option>
            <option value="1">อาทิตย์</option>
        </select>
        <label for="txtPeriod" id="vBilInput2lab">ที่</label>
        <input type="text" id="vBilInput2" name="BilvalueB" style="width: 70px">
        <label for="txtPeriod" id="vFirstweeklab">เปลี่ยนวิธีนับหาก 5 สัปดาห์หรือไม่</label>
        <select style="width: 200px" name="invoice"  id="vFirstweek">
            <option value="C" selected="selected">แบบปกติ</option>
            <option value="R">เปลี่ยนวิธีนับหากมี 5 สัปดาห์</option>
        </select>
        <label for="txtPeriod" id="vBilInput3lab">หากครบ 5 สัปดาห์เปลี่ยนเป็น</label>
        <input type="text" id="vBilInput3" name="BilvalueC" style="width: 70px">
        <br>
        <label for="txtPeriod" id="vPayInputlab"> วิธีคิดวันชำระ</label>
        <select style="width: 550px" name="type"  id="vPayInput">
            <option value="" selected="selected">ปกติ</option>
            <option value="1">วัน A ของสัปดาห์ที่ B</option>
            <option value="3">วัน A ของทุกเดือน(ใช้ของรอบ 7 วัน)</option>
            <option value="4">วันที่เก็บเงิน A ของทุกเดือน (ใช้กับรอบ 30 วัน)</option>
            <option value="5">สามารถเก็บเงินวันที่ A กับ B เท่านั้น</option>
            <option value="6">หากวางบิลวันที่ A จะเก็บเงินวันที่ B หากวางบิลวันที่ C จะเก็บเงินวันที่ D(หลักๆใช้สำหรับ 15 วัน ที่มี 2 รอบต่อเดือน)</option>
            <option value="8">วันที่เก็บเงิน A กับ วันสิ้นเดือน</option>
            <option value="9">ระหว่างวันที่ AถึงB และ C ถึง D</option>
            <option value="10">สิ้นเดือน</option>
        </select>
        <br>
        <label for="txtPeriod" id="v5weekornotlab">นับสัปดาห์แรกหากไม่ครบหรือไม่</label>
        <select style="width: 100px" name="type"  id="v5weekornot">
            <option value="C" selected="selected">นับแม้ไม่ครบ</option>
            <option value="N" >ไม่นับถ้าไม่ครบ</option>
        </select>
        <label for="txtPeriod"  id="vPayweek1lab">วัน</label>
        <select style="width: 100px" name="type"  id="vPayweek1">
            <option value="2" selected="selected">จันทร์</option>
            <option value="3">อังคาร</option>
            <option value="4">พุธ</option>
            <option value="5">พฤหัสบดี</option>
            <option value="6">ศุกร์</option>
            <option value="7">เสาร์</option>
            <option value="1">อาทิตย์</option>
        </select>
        <label for="txtPeriod" id="vColInput1lab">เลขที่1</label>
        <input type="text" id="vColInput1" name="ColvalueA" style="width: 70px">
        <label for="txtPeriod" id="vPayweek2lab">วัน</label>
        <select style="width: 100px" name="type"  id="vPayweek2">
            <option value="2" selected="selected">จันทร์</option>
            <option value="3">อังคาร</option>
            <option value="4">พุธ</option>
            <option value="5">พฤหัสบดี</option>
            <option value="6">ศุกร์</option>
            <option value="7">เสาร์</option>
            <option value="1">อาทิตย์</option>
        </select>
        <label for="txtPeriod" id="vColInput2lab">เลขที่2</label>
        <input type="text" id="vColInput2" name="ColvalueB" style="width: 70px">
        <label for="txtPeriod" id="vColInput3lab">เลขที่3</label>
        <input type="text" id="vColInput3" name="ColvalueC" style="width: 70px">
        <label for="txtPeriod" id="vColInput4lab">เลขที่4</label>
        <input type="text" id="vColInput4" name="ColvalueD" style="width: 70px">
        <label for="txtPeriod" id="vColInput5lab">เลขที่5</label>
        <!--<input type="text" id="vColInput5" name="ColvalueE" style="width: 70px">-->
        <label for="txtPeriod" id="vNextmonthornotlab">เป็น</label>
        <select style="width: 100px" name="type"  id="vNextmonthornot">
            <option value="N" selected="selected">เดือนเดียวกัน</option>
            <option value="Y">เดือนถัดไป</option>
        </select>
        <br>
        <label for="txtPeriod" id="vBilDescLab">Description Billing:</label>
        <input type="text" id="vBilDesc" name="BilDescvalue" style="width: 400px">
        <br>
        <label for="txtPeriod" id="vColDescLab">Description Collection:</label>
        <input type="text" id="vColDesc" name="ColDescvalue" style="width: 400px">
        <br>
        <label for="txtPeriod" id="vColbylab">Collect By:</label>
        <input type="text" id="vColby" name="ColDescvalue" style="width: 100px">
        <br>
        <label for="txtPeriod" id="vRemarklab">Remark:</label>
        <input type="text" id="vRemark" name="ColDescvalue" style="width: 400px">
        <br>
        <button id="vAddmaster" class="btn btn-success" type="submit" form="">Add Master</button>
        <button id="vMode" class="btn btn-success" form="">Mode:Add</button>
    </div>
    <div id="jsGrid"></div>
</section>

<script>
    // Change the background color of the section element to blue
    $('.wrap-login100').css('background-color', 'yellow');

// Change the text color of the labels to white
    $('.wrap-login100 label').css('color', 'black');

//    var cono = <%out.print(session.getAttribute("cono"));%>
//    var divi = <%out.print(session.getAttribute("divi"));%>
    var auth = "<%out.print(session.getAttribute("auth"));%>";
    var facility = "1A1";
    var cono = "10";
    var divi = "101";
    var warehouse;
    var period = [];
    var mode = "create";

    $('#my-div').css('color', 'white');
    $.ajax({
        url: './Action',
        type: 'GET',
        dataType: 'json',
        data: {
            path: "getMasterFinance"
        },
        async: false
    })
            .done(function (response) {

            });


    var NumberField = jsGrid.NumberField;

    function DecimalField(config) {
        NumberField.call(this, config);
    }

    DecimalField.prototype = new NumberField({

        step: 0.01,

        _createTextBox: function () {
            return NumberField.prototype._createTextBox.call(this)
                    .attr("step", this.step);
        }
    });

    jsGrid.fields.decimal = jsGrid.DecimalField = DecimalField;


    $("#jsGrid").jsGrid({
        width: "100%",
        height: "auto",
        editing: false,
        sorting: true,
        paging: true,
        filtering: true,
        pageSize: 25,
        deleteConfirm: "Do you really want to delete the client?",
        heading: true,
        inserting: false,
        selecting: true,
        pageLoading: false,

        onItemUpdating: function (args) {
            previousItem = args.previousItem;
        },
        onRefreshed: function (args) {
        },
        controller: {
            loadData: function (filter) {
                console.log(filter);
                var data = $.Deferred();
                $.ajax({
                    type: 'GET',
                    url: './Action',
                    dataType: 'json',
                    data: {
                        path: "getMasterFinance"
                    },
                    async: false,
                    timeout: 60000
                }).done(function (response) {
                    console.log(response);
                    response = $.grep(response, function (item) {
                        return(!filter.RCOMPANY || (item.RCOMPANY.indexOf(filter.RCOMPANY) > -1))
                                && (!filter.RCUSTOMERID || (item.RCUSTOMERID.indexOf(filter.RCUSTOMERID) > -1))
                                && (!filter.RCUSTOMERNAME || (item.RCUSTOMERNAME.indexOf(filter.RCUSTOMERNAME) > -1))
                                && (!filter.RCUSTOMERTYPE || (item.RCUSTOMERTYPE.indexOf(filter.RCUSTOMERTYPE) > -1))
                                && (!filter.ROUNDINV || (item.ROUNDINV.indexOf(filter.ROUNDINV) > -1))
                                && (!filter.ROUNDBILL || (item.ROUNDBILL.indexOf(filter.ROUNDBILL) > -1))
                                && (!filter.ROUNDPAY || (item.ROUNDPAY.indexOf(filter.ROUNDPAY) > -1))
                                && (!filter.ROUNDCASEBILL || (item.ROUNDCASEBILL.indexOf(filter.ROUNDCASEBILL) > -1))
                                && (!filter.ROUNDBILLSPECIAL || (item.ROUNDBILLSPECIAL.indexOf(filter.ROUNDBILLSPECIAL) > -1))
                                && (!filter.ROUNDCASEPAY || (item.ROUNDCASEPAY.indexOf(filter.ROUNDCASEPAY) > -1))
                                && (!filter.BILLDESCRIPTION || (item.BILLDESCRIPTION.indexOf(filter.BILLDESCRIPTION) > -1))
                                && (!filter.COLDESCRIPTION || (item.COLDESCRIPTION.indexOf(filter.COLDESCRIPTION) > -1))
                                && (!filter.COLBY || (item.COLBY.indexOf(filter.COLBY) > -1))
                                && (!filter.REMARK || (item.REMARK.indexOf(filter.REMARK) > -1));
                        console.log(data.resolve(response));

                    });
                    data.resolve(response);

                    console.log(response);
                    console.log("response");
                });
                return data.promise();
            },
            insertItem: function (item) {
                console.log(item);
                formData = {};
                formData.company = item.RCOMPANY;
                formData.customerid = item.RCUSTOMERID;
                formData.customertype = item.RCUSTOMERTYPE;
                formData.roundinv = item.ROUNDINV;
                formData.roundbill = item.ROUNDBILL;
                formData.roundpay = item.ROUNDPAY;
                formData.path = "addFinanceMaster";
                $.ajax({
                    url: './Action',
                    type: 'POST',
                    dataType: 'json',
                    data: formData,
                    async: false
                });
                $("#jsGrid").jsGrid("loadData");
            },
            updateItem: function (item) {
//                alert(item.RDTOTA_KGS);
                console.log(item);
                formData = {};
                formData.company = item.RCOMPANY;
                formData.customerid = item.RCUSTOMERID;
                formData.customertype = item.RCUSTOMERTYPE;
                formData.caseinvoice = item.ROUNDCASEINV
                formData.roundinv = item.ROUNDINV;
                formData.roundbill = item.ROUNDBILL;
                formData.roundpay = item.ROUNDPAY;
                formData.roundcasebill = item.ROUNDCASEBILL;
                formData.roundbillspecial = item.ROUNDBILLSPECIAL;
                formData.roundcasepay = item.ROUNDCASEPAY;
                formData.bildescription = item.BILLDESCRIPTION;
                formData.coldesciption = item.COLDESCRIPTION;
                formData.colby = item.COLBY;
                formData.remark = item.REMARK;
                formData.path = "updateFinanceMaster";
                $.ajax({
                    url: './Action',
                    type: 'POST',
                    dataType: 'json',
                    data: formData,
                    async: false
                });
                $("#jsGrid").jsGrid("loadData");
            },
            deleteItem: function (item) {
//                alert(item.RDTOTA_KGS);
                console.log(item);
                formData = {};
                formData.company = item.RCOMPANY;
                formData.customerid = item.RCUSTOMERID;
                formData.path = "deleteFinanceMaster";
                $.ajax({
                    url: './Action',
                    type: 'POST',
                    dataType: 'json',
                    data: formData,
                    async: false
                });
                $("#jsGrid").jsGrid("loadData");
            }

        },
        fields: [
            {type: "control", width: 50
                , itemTemplate: function (_, item) {
                    if (item.IsTotal)
                        return "";
                    return jsGrid.fields.control.prototype.itemTemplate.apply(this, arguments);
                }},

            {title: "บริษัท", name: "RCOMPANY", css: "limitext", type: "text", editing: false, align: "center", width: 50},
            {title: "รหัสลูกค้า", name: "RCUSTOMERID", css: "limitext", type: "text", editing: false, align: "left", width: 50},
            {title: "ชื่อลูกค้า", name: "RCUSTOMERNAME", css: "limitext", type: "text", editing: false, align: "left", width: 100},
            {title: "ประเภทลูกค้า", name: "RCUSTOMERTYPE", css: "lvgb", type: "decimal", editing: true, align: "right", width: 50},
            {title: "ประเภทรอบของInvoice", name: "ROUNDCASEINV", css: "lvgb", type: "text", editing: true, align: "right", width: 50},
            {title: "รอบInv", name: "ROUNDINV", css: "lvgb", type: "decimal", editing: true, align: "right", width: 50},
            {title: "รอบวางบิล", name: "ROUNDBILL", css: "lvgb", type: "decimal", editing: true, align: "right", width: 50},
            {title: "รอบชำระ", name: "ROUNDPAY", css: "lvgb", type: "decimal", editing: true, align: "right", width: 50},
            {title: "ประเภทรอบวางบิลSPECIAL", name: "ROUNDCASEBILL", css: "lvgb", type: "text", editing: true, align: "right", width: 50},
            {title: "รอบวางบิลSPECIAL", name: "ROUNDBILLSPECIAL", css: "lvgb", type: "text", editing: true, align: "right", width: 50},
            {title: "ประเภทรอบชำระSPECIAL", name: "ROUNDCASEPAY", css: "lvgb", type: "text", editing: true, align: "right", width: 50},
            {title: "คำอธิบายวางบิล", name: "BILLDESCRIPTION", css: "lvgb", type: "text", editing: true, align: "right", width: 50},
            {title: "คำอธิบายรอบชำระ", name: "COLDESCRIPTION", css: "lvgb", type: "text", editing: true, align: "right", width: 50},
            {title: "Collected by", name: "COLBY", css: "lvgb", type: "text", editing: true, align: "right", width: 50},
            {title: "Remark", name: "REMARK", css: "lvgb", type: "text", editing: true, align: "right", width: 50}
        ]

    });

    if (auth === 'AP') {
        $("#jsGrid").jsGrid({
            // your jsGrid configuration options
            onRefreshing: function (args) {
                args.cancel = true;
                args.grid.editing = true;

            }
        });
    }
    $("#jsGrid").jsGrid("loadData");

//Change mode
    $("#vMode").click(function myFunction() {
        if (mode === "create") {
            mode = "remark";
            $("#vMode").text("Mode:Remark");
            $("#vAddmaster").text("Change Remark");
//            $("#vCompanyInputlab").hide();
//            $("#vCompanyInput").hide();
//            $("#vCustomerInput").hide();
//            $("#vCustomerInputlab").hide();
            $("#vTypeInput").hide();
            $("#vTypeInputlab").hide();
            $("#vInvoiceInput").hide();
            $("#vBillInput").hide();
            $("#vBilInput1").hide();
            $("#vPayInput").hide();
            $("#vInvoiceA").hide();
            $("#vColInput1").hide();
            $("#vBilDesc").hide();
            $("#vColDesc").hide();
            $("#vColby").hide();
//            $("#vRemark").hide();
            $("#vInvoiceInputlab").hide();
            $("#vInvoiceAlab").hide();
            $("#vBillInputlab").hide();
            $("#vBilInput1lab").hide();
            $("#vBilInput2lab").hide();
            $("#vPayInputlab").hide();
            $("#vColInput1lab").hide();
            $("#vColInput2lab").hide();
            $("#vBilDescLab").hide();
            $("#vColDescLab").hide();
            $("#vColbylab").hide();
//            $("#vRemarklab").hide();
        } else if (mode === "remark") {
            mode = "create";
            $("#vMode").text("Mode:Add");
            $("#vAddmaster").text("Add Master");
//            $("#vCompanyInput").show();
//            $("#vCompanyInputlab").show();
//            $("#vCustomerInput").show();
//            $("#vCustomerInputlab").show();
            $("#vTypeInputlab").show();
            $("#vTypeInput").show();
            $("#vInvoiceInput").show();
            $("#vBillInput").show();
            $("#vBilInput1").show();
            $("#vPayInput").show();
            $("#vInvoiceA").show();
            $("#vColInput1").show();
            $("#vColInput1").show();
            $("#vBilDesc").show();
            $("#vColDesc").show();
            $("#vColby").show();
//            $("#vRemark").show();
            $("#vInvoiceInputlab").show();
            $("#vInvoiceAlab").show();
            $("#vBillInputlab").show();
            $("#vBilInput1lab").show();
            $("#vBilInput2lab").show();
            $("#vPayInputlab").show();
            $("#vColInput1lab").show();
            $("#vColInput2lab").show();
            $("#vBilDescLab").show();
            $("#vColDescLab").show();
            $("#vColbylab").show();
//            $("#vRemarklab").show();

        }
    });


//ADD COMPANY
    $.ajax({
        url: "./Action",
        type: "GET",
        dataType: "json",
        data: {
            path: "getCompany"
        },
        success: function (getdata) {
            $.each(getdata, function (i, obj) {
                var div_data = "<option value=" + obj.CCCONO + ">" + obj.CCCONO + " : " + obj.CCDIVI + " : " + obj.CCCONM + "</option>";
                //console.log(div_data)
                $(div_data).appendTo('#vCompanyInput');
            });
        }
    });

    $("#vInvoiceInput").change(function billchange() {
        invoicereset();
        var invoiceinput = $("#vInvoiceInput").val();
        if (invoiceinput === '') {
            $("#vInvoicelab").show();
            $("#vInvoicelab").text('รอบวัน');
            $("#vInvoiceA").show();
        } else if (invoiceinput === '1') {
            $("#vInvoicelab").show();
            $("#vInvoicelab").text('วันที่');
            $("#vInvoiceA").show();
        }
    });

    $("#vBillInput").change(function billchange()
    {

        var billinput = $("#vBillInput").val();
        billreset();
        if (billinput === '') {
            $("#vBilInput1lab").show();
            $("#vBilInput1lab").text('รอบวางบิลนับจากบิลสุดท้าย');
            $("#vBilInput2lab").show();
            $("#vBilInput1").show();
            $("#vBilInput2lab").text('วัน');
        } else if (billinput === '1') {
            $("#vBilInput1lab").show();
            $("#vBilweek1lab").show();
            $("#vBilweek1").show();
//            $("#vBilweek2lab").show();
//            $("#vBilweek2").show();
            $("#vBilInput1").show();
            $("#vBilInput1lab").show();
//            $("#vBilInput2").show();
//            $("#vBilInput2lab").show();
            $("#vFirstweeklab").show();
            $("#vFirstweek").show();
        } else if (billinput === '2') {
            $("#vBilInput1lab").show();
            $("#vBilweek1lab").show();
            $("#vBilweek1").show();
            $("#vBilweek2lab").show();
            $("#vBilweek2").show();
            $("#vBilInput1").show();
            $("#vBilInput1lab").show();
            $("#vBilInput2").show();
            $("#vBilInput2lab").show();
        }
    });



    $("#vFirstweek").change(function firstweek() {
        var week5 = $("#vFirstweek").val();
        if (week5 === 'C') {
            $("#vBilInput3lab").hide();
            $("#vBilInput3").hide();
        } else {
            $("#vBilInput3lab").show();
            $("#vBilInput3").show();
        }
    });


    $("#vPayInput").change(function paychange()
    {
        payreset();
        var payinput = $("#vPayInput").val();
        if (payinput === '') {
            $("#vColInput1lab").text('ทุก');
            $("#vColInput1lab").show();
            $("#vColInput2lab").text('วัน');
            $("#vColInput2lab").show();
            $("#vColInput1").show();
        } else if (payinput === '1') {
            $("#vPayweek1lab").show();
            $("#vPayweek1").show();
            $("#vColInput1lab").text('ที่');
            $("#vColInput1lab").show();
            $("#vColInput1").show();
            $("#vPayweek2lab").text('หรือหากเป็น 5 สัปดาห์');
            $("#v5weekornot").show();
            $("#v5weekornotlab").show();

        } else if (payinput === '3') {
            $("#vColInput1lab").text('วันที่');
            $("#vColInput1lab").show();
            $("#vColInput1").show();
            $("#vPayweek2lab").text('ของทุกเดือน');
            $("#vPayweek2lab").show();
        } else if (payinput === '4') {
            $("#vColInput1lab").text('วันที่');
            $("#vColInput1lab").show();
            $("#vColInput1").show();
            $("#vPayweek2lab").text('ของทุกเดือน');
            $("#vPayweek2lab").show();
            $("#vNextmonthornotlab").show();
            $("#vNextmonthornot").show();
        } else if (payinput === '5') {
            $("#vColInput1lab").text('รอบเก็บเงินวันที่');
            $("#vColInput1lab").show();
            $("#vColInput1").show();
            $("#vColInput2lab").text('กับวันที่');
            $("#vColInput2lab").show();
            $("#vColInput2").show();
        } else if (payinput === '6') {
            $("#vColInput1lab").text('วางบิลวันที่');
            $("#vColInput1lab").show();
            $("#vColInput1").show();
            $("#vColInput2lab").text('เก็บเงินวันที่');
            $("#vColInput2lab").show();
            $("#vColInput2").show();
            $("#vColInput3lab").text('ของเดือนถัดไปและวางบิลวันที่');
            $("#vColInput3lab").show();
            $("#vColInput3").show();
            $("#vColInput4lab").text('เก็บเงินวันที่');
            $("#vColInput4lab").show();
            $("#vColInput5lab").text('ของเดือนเดียวกัน');
            $("#vColInput5lab").show();
            $("#vColInput4").show();
        } else if (payinput === '8') {
            $("#vColInput1lab").text('วันที่');
            $("#vColInput1lab").show();
            $("#vColInput1").show();
            $("#vColInput2lab").text('กับวันสิ้นเดือน');
            $("#vColInput2lab").show();
        } else if (payinput === '9') {
            $("#vColInput1lab").text('วันที่');
            $("#vColInput1lab").show();
            $("#vColInput1").show();
            $("#vColInput2lab").text('-');
            $("#vColInput2lab").show();
            $("#vColInput2").show();
            $("#vColInput3lab").text('กับวันที่');
            $("#vColInput3lab").show();
            $("#vColInput3").show();
            $("#vColInput4lab").text('-');
            $("#vColInput4lab").show();
            $("#vColInput4").show();
        } else if (payinput === '10') {
            $("#vNextmonthornotlab").show();
            $("#vNextmonthornot").show();
        }
    });

    function resetmaster()
    {
        $("#vCompanyInput").val('10');
        invoicereset();
        billreset();
        payreset();
    }

    ;
    function invoicereset()
    {
        $("#vInvoicelab").hide();
        $("#vInvoiceA").hide();
    }

    ;
    function billreset()
    {
        $("#vFirstweek").val("C");
        $("#vBilInput3").hide();
        $("#vBilInput3lab").hide();
        $("#vBilInput1lab").hide();
        $("#vBilweek1lab").hide();
        $("#vBilweek1").hide();
        $("#vBilweek2lab").hide();
        $("#vBilweek2").hide();
        $("#vBilInput1").hide();
        $("#vBilInput1lab").hide();
        $("#vBilInput2").hide();
        $("#vBilInput2lab").hide();
        $("#vFirstweeklab").hide();
        $("#vFirstweek").hide();
    }

    ;
    function payreset()
    {
        $("#vPayweek1lab").hide();
        $("#vPayweek1").hide();
        $("#vColInput1lab").hide();
        $("#vColInput1").hide();
        $("#vPayweek2lab").hide();
        $("#vPayweek2").hide();
        $("#vColInput2lab").hide();
        $("#vColInput2").hide();
        $("#vColInput3lab").hide();
        $("#vColInput3").hide();
        $("#vColInput4lab").hide();
        $("#vColInput4").hide();
        $("#vColInput5lab").hide();
        $("#vNextmonthornotlab").hide();
        $("#vNextmonthornot").hide();
        $("#v5weekornotlab").hide();
        $("#v5weekornot").hide();
    }
    ;
    $("#vAddmaster").click(function () {
        var customer = $("#vCustomerInput").val();
         var company = $("#vCompanyInput").val();
        if (mode === "create") {
           
            var invoiceinput = $("#vInvoiceInput").val();
            var invoiceday = $("#vInvoiceA").val();
            var type = $("#vTypeInput").val();
            if (company === '') {
                alert("please choose company!");
                return;
            }
            if (customer === '') {
                alert("please fill customer!");
                return;
            }
            if (invoiceinput === '') {
                if (invoiceday === '') {
                    alert("please fill invround!");
                    return;
                }
            }
        }

        $.ajax({
            url: './Action',
            type: 'GET',
            dataType: 'text',
            data: {
                path: "addMaster",
                company: company,
                customer: customer,
                invroundinput: invoiceinput,
                invroundA: invoiceday,
                type: type,
                bill: $("#vBillInput").val(),
                bilweek1: $("#vBilweek1").val(),
                bilinput1: $("#vBilInput1").val(),
                bilinput3: $("#vBilInput3").val(),
                bilfirstweek: $("#vFirstweek").val(),
                pay: $("#vPayInput").val(),
                pay5weekornot: $("#v5weekornot").val(),
                payweek1: $("#vPayweek1").val(),
                payweek2: $("#vPayweek2").val(),
                payday1: $("#vColInput1").val(),
                payday2: $("#vColInput2").val(),
                payday3: $("#vColInput3").val(),
                payday4: $("#vColInput4").val(),
                paydaysamemonthornot: $("#vNextmonthornot").val(),
                bildesc: $("#vBilDesc").val(),
                collectiondesc: $("#vColDesc").val(),
                collectby: $("#vColby").val(),
                remark: $("#vRemark").val(),
                mode: mode
            },
            async: false
        }).done(function (response) {
            console.log(response);
            alert(response);
        });
        $("#jsGrid").jsGrid("loadData");
    });
    resetmaster();
    $("#vInvoicelab").show();
    $("#vInvoicelab").text('รอบวัน');
    $("#vInvoiceA").show();
    $("#vBilInput2lab").show();
    $("#vBilInput1").show();
    $("#vBilInput2lab").text('วัน');
    $("#vBilInput1lab").show();
    $("#vBilInput1lab").text('รอบวางบิลนับจากบิลสุดท้าย');
    $("#vColInput1lab").text('ทุก');
    $("#vColInput1lab").show();
    $("#vColInput2lab").text('วัน');
    $("#vColInput2lab").show();
    $("#vColInput1").show();


</script>  
