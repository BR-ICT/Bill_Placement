<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>

<style>

    .wrap-login100 {
        width: 390px;
        background: #fff;
        border-radius: 10px;
        overflow: hidden;
        padding: 30px 55px 33px 55px;
        box-shadow: 0 5px 10px 0px rgba(0, 0, 0, 0.1);
        -moz-box-shadow: 0 5px 10px 0px rgba(0, 0, 0, 0.1);
        -webkit-box-shadow: 0 5px 10px 0px rgba(0, 0, 0, 0.1);
        -o-box-shadow: 0 5px 10px 0px rgba(0, 0, 0, 0.1);
        -ms-box-shadow: 0 5px 10px 0px rgba(0, 0, 0, 0.1);
    }

    .container {
        /*width: 970px;*/
        text-align: -webkit-center;
    }

</style>

<div class="container">
    <div class="wrap-login100" >

        <form id="detailsForm" class="login100-form validate-form" target="_blank" action="Report" method="GET">

            <h3>Rental Report</h3>
            <br>
            <div class="details-form-field">
                <label style="float: left">Warehouse :</label>
                <!--<input id="vFacility" name="E1CODE" type="number" />-->
                <select class="form-control form-control-user" name="warehouse" id="vWarehouse">
                    <option value="" selected="selected">Select Warehouse</option>
                </select>
            </div>
            <div class="details-form-field">
                <label style="float: left">Period :</label>
                <!--<input id="vFacility" name="E1CODE" type="number" />-->
                <select class="form-control form-control-user" name="period"  id="vPeriod">
                    <option value="" selected="selected">Select Period</option>
                </select>
            </div>

        </form>

        <form align="center">
            <hr class="my-4">
            <!--<button class="btn btn-primary mb-2" style="color:#FFFFFF;" form="detailsForm" name="report" value="MO_Report" type="submit">PDF</button>-->
            <button class="btn btn-primary mb-2" style="color:#FFFFFF;" form="detailsForm" name="report" value="Rental_Excel"type="submit">Excel</button>
        </form> 


    </div>
</div>

<script>

//    var cono = <%out.print(session.getAttribute("cono"));%>
//    var divi = <%out.print(session.getAttribute("divi"));%>
    var facility = "1A1";
    var cono = "10"
    var divi = "101"

    $("#vFromDate").datepicker({dateFormat: 'yy-mm-dd'}).datepicker("setDate", new Date($.now()));
    $("#vToDate").datepicker({dateFormat: 'yy-mm-dd'}).datepicker("setDate", new Date($.now()));
    $(function () {
        $("#vFromDate").datepicker({
            clearBtn: true,
            dateFormat: 'yy-mm-dd'
        });
        $("#vToDate").datepicker({
            clearBtn: true,
            dateFormat: 'yy-mm-dd'
        });
    });

    $.ajax({
        url: './Action',
        type: 'GET',
        dataType: 'json',
        data: {
            path: "getWarehouse",
            cono: cono,
            divi: divi,
            fac: facility
        },
        async: false
    }).done(function (response) {
        console.log(response);
        warehouse = response;
        $.each(response, function (i, obj) {
            var div_data = "<option value=" + obj.MWWHLO + ">" + obj.WAREHOUSE + "</option>";
            $(div_data).appendTo('#vWarehouse');
        });
    });

    $("#vWarehouse").change(function () {
        $.ajax({
            url: './Action',
            type: 'GET',
            dataType: 'json',
            data: {
                path: "getPeriod",
                cono: cono,
                divi: divi,
                whs: $("#vWarehouse").val()
            },
            async: false
        }).done(function (response) {
            console.log(response);
            period = response;

            $('#vPeriod').empty().append('<option value="" selected="selected">Select Period</option>');
            $.each(response, function (i, obj) {
                var div_data = "<option value=" + obj.RHPERI + ">" + obj.RHPERI + "</option>";
                $(div_data).appendTo('#vPeriod');
            });
//            $("#jsGrid").jsGrid("refresh");
//            $("#jsGrid").jsGrid("reset");
//            $("#jsGrid").jsGrid("render").done(function () {
//                console.log("rendering completed and data loaded");
//            });
        });
    });


//    $.ajax({
//        url: './Sync',
//        type: 'GET',
//        dataType: 'json',
//        data: {
//            page: "Facility",
//            cono: cono,
//            divi: divi
//        },
//        async: false
//    }).done(function (response) {
//        console.log(response);
//        facility = response;
//
//        $.each(response, function (i, obj) {// CFFACI,CFFACN
//            var div_data = "<option value=" + obj.CFFACI + ">" + obj.FACILITY + "</option>";
////                console.log(div_data);
//            $(div_data).appendTo('#vFacility');
//        });
//
//    });




</script>
