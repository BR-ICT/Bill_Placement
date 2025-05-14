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

        <label for="txtPeriod" id="vInvroundlab">Invoice Round!</label>
        <select style="width: 100px" name="invoiceround"  id="vInvround">
            <option value="" selected="selected">Invoice Round!</option>
            <option value="7" >7 Days</option>
            <option value="15" >15 Days</option>
            <option value="30" >30 Days</option>
            <option value="Special" >Special Case</option>
        </select>


        <label for="txtPeriod" id="vYearlab">Year :</label>
        <select style="width: 75px" name="Year"  id="vYear">
            <option value="" selected="selected">Select Year!</option>
        </select>
        <label for="txtPeriod" id="vMonthlab">Month :</label>
        <select style="width: 50px" name="month"  id="vMonth">
            <option value="" selected="selected">Select Month!</option>
        </select>
        <button id="vSearch" class="btn btn-success" type="submit" form="">Search</button> 
        <button id="vGenerate" class="btn btn-success" type="submit" form="">Generate Next Month</button>
        <button id="vDelete" class="btn btn-success" type="submit" form="">Delete This Month</button>
        <br>
        <label for="date" id="vDatelab">Start date:</label>
        <input type="date" id="vDate" name="date">
        <label for="txtPeriod" id="vCustomeridlab">Cutomer ID</label>     
        <input type="text" id="vCustomer" name="Customer" placeholder="Enter your Customer ID!">
        <button id="vAdd" class="btn btn-success" type="submit" form="">Add Data</button>
        <button id="vDeletecustomer" class="btn btn-success" type="submit" form="">Delete Data</button>
        <br>
        <button id="vMode" class="btn btn-success" type="submit" form="">Per Customer</button>
    </div>
    <div id="jsGrid"></div>
</section>

<script>


    var cono = <%out.print(session.getAttribute("cono"));%>
    var divi = <%out.print(session.getAttribute("divi"));%>
    var auth = "<%out.print(session.getAttribute("auth"));%>"
//    var facility = "1A1";
//    var cono = "10";
//    var divi = "101";
//    var warehouse;
//    var period = [];
    var mode = "search";




    var NumberField = jsGrid.NumberField;

    function DecimalField(config) {
        NumberField.call(this, config);
    }

    DecimalField.prototype = new NumberField({

        step: 0.01,

        filterValue: function () {
            return this.filterControl.val() ? parseFloat(this.filterControl.val()) : undefined;
        },

        insertValue: function () {
            return this.insertControl.val() ? parseFloat(this.insertControl.val()) : undefined;
        },

        editValue: function () {
            return this.editControl.val() ? parseFloat(this.editControl.val()) : undefined;
        },

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
        selecting: false,
        pageLoading: false,

        controller: {
            loadData: function (filter) {
                console.log(filter);
                var data = $.Deferred();
                $.ajax({
                    type: 'GET',
                    url: './Action',
                    dataType: 'json',
                    data: {
                        path: "getStartDate",
                        month: $("#vMonth").val(),
                        year: $("#vYear").val(),
                        invoicerd: $("#vInvround").val(),
                        cono: cono,
                        divi: divi
                    },
                    async: false,
                    timeout: 60000
                }).done(function (response) {
                    console.log("response: " + response);
                    response = $.grep(response, function (item) {
                        return(!filter.RCOMPANY || (item.RCOMPANY.indexOf(filter.RCOMPANY) > -1))
                                && (!filter.RCUSTOMERID || (item.RCUSTOMERID.indexOf(filter.RCUSTOMERID) > -1))
                                && (!filter.RCUSTOMERNAME || (item.RCUSTOMERNAME.indexOf(filter.RCUSTOMERNAME) > -1))
                                && (!filter.RSTARTDATE || (item.RSTARTDATE.indexOf(filter.RSTARTDATE) > -1))
                                && (!filter.RENDDATE || (item.RENDDATE.indexOf(filter.RENDDATE) > -1))
                                && (!filter.RROUND || (item.RROUND.indexOf(filter.RROUND) > -1))
                                && (!filter.RSTATUS || (item.RSTATUS.indexOf(filter.RSTATUS) > -1));
                        console.log(data.resolve(response));

                    });
                    data.resolve(response);

//                    console.log(response);
//                    console.log("response");
                });
                return data.promise();
            },

            insertItem: function (item) {
                console.log(item);
                formData = {};
                formData.company = item.RCOMPANY;
                formData.customerid = item.RCUSTOMERID;
                formData.startdate = item.RSTARTDATE;
                formData.status = item.RSTATUS;
                formData.month = $("#vMonth").val();
                formData.year = $("#vYear").val();
                formData.invoicerd = $("#vInvround").val();
                formData.path = "insertfinanceheader";
                $.ajax({
                    type: 'GET',
                    url: './Action',
                    dataType: 'String',
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
                formData.startdate = item.RSTARTDATE;
                formData.enddate = item.RENDDATE;
                formData.customerid = item.RCUSTOMERID;
                formData.round = item.RROUND;
                formData.month = $("#vMonth").val();
                formData.year = $("#vYear").val();
                formData.path = "updateStartFinance";
                console.log(formData);
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
//                console.log(item);
//                formData = {};
//                formData.company = item.RCOMPANY;
//                formData.customerid = item.RCUSTOMERID;
//                formData.path = "deleteFinanceMaster";
//                $.ajax({
//                    url: './Action',
//                    type: 'POST',
//                    dataType: 'json',
//                    data: formData,
//                    async: false
//                });
//                $("#jsGrid").jsGrid("loadData");
            }

        },
        fields: [
            {
                type: "control", width: 10,
                itemTemplate: function (value, item) {
                    // Customize the control column here
                    // Return an empty string to remove the delete button
                    return "";
                }
            },

//            {title: "บริษัท", name: "RCOMPANY", css: "limitext", type: "text", editing: false, align: "center", width: 50},
            {title: "รหัสลูกค้า", name: "RCUSTOMERID", css: "limitext", type: "text", editing: false, align: "left", width: 50},
            {title: "ชื่อลูกค้า", name: "RCUSTOMERNAME", css: "limitext", type: "text", editing: false, align: "left", width: 100},
            {title: "วันเริ่ม Invoice", name: "RSTARTDATE", type: "text", css: "lvgb", editing: true, align: "right", width: 100},
            {title: "วันสิ้นสุด Invoice", name: "RENDDATE", type: "text", css: "lvgb", editing: true, align: "right", width: 100},
            {title: "รอบที่", name: "RROUND", type: "text", css: "lvgb", editing: false, align: "right", width: 25}
            , {title: "สถานะ", name: "RSTATUS", css: "limitext", type: "text", editing: false, align: "right", width: 25}
        ]

    });

    $("#vSearch").click(function () {

        var month = $("#vMonth").val();
        var year = $("#vYear").val();
        var invround = $("#vInvround").val();
        if (year === '') {
            alert("Please select Year!");
            return;
        }
        if (month === '') {
            alert("Please select month!");
            return;
        }
        if (invround === '') {
            alert("Please select Invoice round!");
            return;
        }
        console.log(month);
        console.log(year);


        $("#jsGrid").jsGrid("loadData");
    }
    );
    $("#vGenerate").click(function () {
        var month = $("#vMonth").val();
        var year = $("#vYear").val();
        var invround = $("#vInvround").val();
        if (year === '') {
            alert("Please select Year!");
            return;
        }
        if (month === '') {
            alert("Please select month!");
            return;
        }
        if (invround === '') {
            alert("Please select Invoice round!");
            return;
        }
        $.ajax({
            url: './Action',
            type: 'GET',
            dataType: 'text',
            data: {
                path: "importInvoiceData",
                month: month,
                year: year,
                invround: invround,
                company: cono
            },
            async: false
        }).done(function (response) {
            console.log(response);
            alert(response);
        }, function () {
            $.ajax({
                url: './Action',
                type: 'GET',
                dataType: 'json',
                data: {
                    path: "getMonthInvoice",
                    year: $("#vYear").val(),
                    invround: $("#vInvround").val()
                },
                async: false
            }).done(function (response) {
                console.log(response);
                good = response;

                $('#vMonth').empty().append('<option value="" selected="selected">Select Month!</option>');
                $.each(response, function (i, obj) {
                    var div_data = "<option value=" + obj.vMonth + ">" + obj.vMonth + "</option>";
                    $(div_data).appendTo('#vMonth');
                });
            });
        });
        console.log("month");
        console.log(month);
        if (month === "12") {
            $.ajax({
                url: './Action',
                type: 'GET',
                dataType: 'json',
                data: {
                    path: "getYearInvoice",
                    invround: $("#vInvround").val()
                },
                async: false
            }).done(function (response) {
                console.log(response);
                warehouse = response;
                $('#vYear').empty().append('<option value="" selected="selected">Select Year!!</option>');
                $.each(response, function (i, obj) {
                    var div_data = "<option value=" + obj.vYear + ">" + obj.vYear + "</option>";
                    $(div_data).appendTo('#vYear');
                });
            });
            ;
        }
    });
    $("#vMode").click(function myFunction() {
        if (mode === "search") {
            mode = "new";
            $("#vMode").text("Per Month");
            $("#vYearlab").hide();
            $("#vYear").hide();
            $("#vGenerate").hide();
            $("#vDelete").hide();
            $("#vCustomeridlab").show();
            $("#vCustomer").show();
            $("#vDate").show();
            $("#vDatelab").show();
            $("#vMonthcustomerlab").show();
            $("#vMonthcustomer").show();
            $("#vAdd").show();
            $("#vMonthlab").hide();
            $("#vMonth").hide();
            $("#vSearch").hide();
            $("#vDeletecustomer").show();

        } else if (mode === "new") {
            mode = "search";
            $("#vMode").text("Per Customer");
            $("#vYearlab").show();
            $("#vYear").show();
            $("#vGenerate").show();
            $("#vDelete").show();
            $("#vCustomeridlab").hide();
            $("#vCustomer").hide();
            $("#vDate").hide();
            $("#vDatelab").hide();
            $("#vMonthcustomerlab").hide();
            $("#vMonthcustomer").hide();
            $("#vAdd").hide();
            $("#vMonthlab").show();
            $("#vMonth").show();
            $("#vSearch").show();
            $("#vDeletecustomer").hide();
        }


    });

    $("#vCustomeridlab").hide();
    $("#vCustomer").hide();
    $("#vDate").hide();
    $("#vDatelab").hide();
    $("#vMonthcustomerlab").hide();
    $("#vMonthcustomer").hide();
    $("#vAdd").hide();
    $("#vDeletecustomer").hide();
    $("#vDelete").click(function myFunction()
    {
        var month = $("#vMonth").val();
        var year = $("#vYear").val();
        var invround = $("#vInvround").val();
        if (year === '') {
            alert("Please select Year!");
            return;
        }
        if (month === '') {
            alert("Please select month!");
            return;
        }
        if (invround === '') {
            alert("Please select invoice round!");
            return;
        }
        let text = "Confirm to delete this month?";
        if (confirm(text) === true) {
            var month = $("#vMonth").val();
            var year = $("#vYear").val();
            var invround = $("#vInvround").val();
            if (year === '') {
                alert("Please select Year!");
                return;
            }
            if (month === '') {
                alert("Please select month!");
                return;
            }
            if (invround === '') {
                alert("Please select Invoice month!");
                return;
            }
            $.ajax({
                url: './Action',
                type: 'GET',
                dataType: 'text',
                data: {
                    path: "deleteStartMonth",
                    year: $("#vYear").val(),
                    month: $("#vMonth").val(),
                    invround: $("#vInvround").val()
                },
                async: false
            }).done(function (response) {
                console.log(response);
                alert(response);

            }, function () {
                $.ajax({
                    url: './Action',
                    type: 'GET',
                    dataType: 'json',
                    data: {
                        path: "getMonthInvoice",
                        year: $("#vYear").val(),
                        invround: $("#vInvround").val()
                    },
                    async: false
                }).done(function (response) {
                    console.log(response);
                    good = response;
                    $('#vMonth').empty().append('<option value="" selected="selected">Select Month!</option>');
                    $.each(response, function (i, obj) {
                        var div_data = "<option value=" + obj.vMonth + ">" + obj.vMonth + "</option>";
                        $(div_data).appendTo('#vMonth');
                    });
                    if (month === "01") {
                        $.ajax({
                            url: './Action',
                            type: 'GET',
                            dataType: 'json',
                            data: {
                                path: "getYearInvoice",
                                invround: $("#vInvround").val()
                            },
                            async: false
                        }).done(function (response) {
                            console.log(response);
                            warehouse = response;
                            $('#vYear').empty().append('<option value="" selected="selected">Select Year!!</option>');
                            $.each(response, function (i, obj) {

                                var div_data = "<option value=" + obj.vYear + ">" + obj.vYear + "</option>";
                                $(div_data).appendTo('#vYear');
                            });
                        });
                        ;
                    }
                    ;
                });
            }

            );
            ;
        } else {
            alert("You canceled!");
        }
    });

    $("#vAdd").click(function ()
    {
        var date = $("#vDate").val();
        var customer = $("#vCustomer").val();
        var invround = $("#vInvround").val();
        if (customer === '') {
            alert("Please insert customer ID!");
            return;
        }
        if (date === '') {
            alert("Please select date!");
            return;
        }
        if (invround === '') {
            alert("Please select Invoice round!");
            return;
        }
        $.ajax({
            url: './Action',
            type: 'GET',
            dataType: 'text',
            data: {
                path: "addStartMonth",
                customer: $("#vCustomer").val(),
                startdate: $("#vDate").val(),
                invround: $("#vInvround").val(),
                year: $("#vYear").val(),
                month: $("#vMonth").val()
            },
            async: false
        }).done(function (response) {
            console.log(response);
            alert(response);
        });
    });

    $("#vDeletecustomer").click(function () {
        var customer = $("#vCustomer").val();
        if (customer === '') {
            alert("Please insert customer ID!");
            return;
        }
        $.ajax({
            url: './Action',
            type: 'GET',
            dataType: 'text',
            data: {
                path: "deleteStartDatecustomer",
                customer: $("#vCustomer").val()
            },
            async: false
        }).done(function (response) {
            console.log(response);
            alert(response);
        });
    });
    $("#vYear").change(function ()
    {
        $.ajax({
            url: './Action',
            type: 'GET',
            dataType: 'json',
            data: {
                path: "getMonthInvoice",
                year: $("#vYear").val(),
                invround: $("#vInvround").val()
            },
            async: false
        }).done(function (response) {
            console.log(response);
            month = response;

            $('#vMonth').empty().append('<option value="" selected="selected">Select Month!</option>');
            $.each(response, function (i, obj) {
                var div_data = "<option value=" + obj.vMonth + ">" + obj.vMonth + "</option>";
                $(div_data).appendTo('#vMonth');
            });

        });
    });
    $("#vInvround").change(function ()
    {
        $.ajax({
            url: './Action',
            type: 'GET',
            dataType: 'json',
            data: {
                path: "getYearInvoice",
                invround: $("#vInvround").val()
            },
            async: false
        }).done(function (response) {
            console.log(response);
            warehouse = response;
            $('#vYear').empty().append('<option value="" selected="selected">Select Year!</option>');
            $.each(response, function (i, obj) {
                var div_data = "<option value=" + obj.vYear + ">" + obj.vYear + "</option>";
                $(div_data).appendTo('#vYear');
            });
        });
    });


</script>  
