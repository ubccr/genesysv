$(document).ready(function () {
    //Initialize tooltips
    $('.nav-tabs > li a[title]').tooltip();

    //Wizard
    $('a[data-toggle="tab"]').on('show.bs.tab', function (e) {

        var $target = $(e.target);

        if ($target.parent().hasClass('disabled')) {
            return false;
        }
    });

    $(".next-step").click(function (e) {

        var $active = $('.wizard .nav-tabs li.active');
        $active.next().removeClass('disabled');
        nextTab($active);

    });
    $(".prev-step").click(function (e) {

        var $active = $('.wizard .nav-tabs li.active');
        prevTab($active);

    });




});

function nextTab(elem) {
    $(elem).next().find('a[data-toggle="tab"]').click();
}
function prevTab(elem) {
    $(elem).prev().find('a[data-toggle="tab"]').click();
}



$(document).on("click", "input[type=checkbox][id*=attribute_group]", (function(event){
    var checkbox = event.target;
    if (checkbox.checked == true) {
            $('.submit_via_ajax_button').each(function() {
                $(this).removeClass( "disabled" );
            })
            attribute_id = checkbox.id
            label_id="label[for=\"" + attribute_id + "\"]";
            label_field = $(label_id);
            label_val = $.trim(label_field.text())
            p_id =  attribute_id + "___p";
            p_text = '<li class="ui-state-default" style="border-bottom: thin solid;" id="' + p_id + '\"><span class="ui-icon ui-icon-arrowthick-2-n-s"></span>' + label_val + '<i id="'+ p_id + '_reset_sidebar' + '\" class="fa fa-times reset_button pull-right" aria-hidden="true"></i></li>'

            p_ele = $(document.getElementById(p_id));
            $(p_ele).remove();
            $("#attribute___p").append(p_text);
            $("#li-step-submit").removeClass("disabled");

    } else {
        attribute_id = checkbox.id
        p_id =  attribute_id + "___p";
        p_ele = $(document.getElementById(p_id));
        $(p_ele).remove();
        checked = $("input[type=checkbox][id*=attribute_group]:checked");
        if (checked.length == 0) {
            $('input[type="submit"]').each(function() {
            $( this ).addClass( "disabled" );
            $("#li-step-submit").addClass("disabled");
            });
            $('.submit_via_ajax_button').each(function() {
                $( this ).addClass( "disabled" );
            })

        }
    }
}));



    $(document).on("change", ":input", function(event) {
        var field = $(event.target);
        var attr = $(field).attr('groupId');
        if (typeof attr !== typeof undefined && attr !== false) {
            $(field).addClass("MEGselected");
            // MEgroup_1_1
            attr_group = attr.split("_", 2).join("_");
            $("[groupId*=\"" + attr_group + "\"]").each(function() {
                if ($(this).attr('groupId') != attr ) {
                    $(this).attr("disabled", true);
                }
            });


        }

    });

$(document).on("click", '.select-all', (function(event){
      var id = event.target.id;
      var checkboxes =  $("input[type=checkbox][id*=id_" + id + "]")
      for (var i = 0; i < checkboxes.length; i++) {
               if (checkboxes[i].type == 'checkbox') {
                    checkboxes[i].checked = true;
                    attribute_id = checkboxes[i].id
                    label_id="label[for=\"" + attribute_id + "\"]";
                    label_field = $(label_id);
                    label_val = $.trim(label_field.text())
                    p_id =  attribute_id + "___p";
                    p_text = '<li class="ui-state-default" style="border-bottom: thin solid;" id="' + p_id + '\"><span class="ui-icon ui-icon-arrowthick-2-n-s"></span>' + label_val + '<i id="'+ p_id + '_reset_sidebar' + '\" class="fa fa-times reset_button pull-right" aria-hidden="true"></i></li>'
                    p_ele = $(document.getElementById(p_id));
                    $(p_ele).remove();
                    $("#attribute___p").append(p_text);

               }
      }
      $('.submit_via_ajax_button').each(function() {
                $( this ).removeClass( "disabled" );
       })
       $("#user-alert-div").empty();
       $("#li-step-submit").removeClass("disabled");
  }));


 $(document).on("click", '.select-none', (function(event){
      var id = event.target.id;

      var checkboxes =  $("input[type=checkbox][id*=id_" + id + "]")
      for (var i = 0; i < checkboxes.length; i++) {
               if (checkboxes[i].type == 'checkbox') {
                   checkboxes[i].checked = false;
                    attribute_id = checkboxes[i].id
                    p_id =  attribute_id + "___p";
                    p_ele = $(document.getElementById(p_id));
                    $(p_ele).remove();
               }
      }

      if ($("input[type=checkbox][id*='attribute_group']:checked").length == 0) {
          $('.submit_via_ajax_button').each(function() {
                    $( this ).addClass( "disabled" );

            })
      }
     // $("#li-step-submit").addClass("disabled");
  }));



$(document).on("change", ":input", (function(event){
        var id = event.target.id;
        var field = document.getElementById(id);
        var reset_div_id = id+"_reset_div";
        var reset_div = document.getElementById(reset_div_id);
        if (reset_div) {
            var field_jq = $("#"+id);

            label_id="label[for=\"" + id + "\"]";

            label_field = $(label_id);
            label_val = $.trim(label_field.text())

            filter_text = label_val + ": " + field_jq.val()
            p_id =  id + "___p";
            p_text = '<p style="border-bottom: thin solid;" id="' + p_id + '\">' + filter_text + ' <i id="'+ p_id + '_reset_sidebar' + '\" class="fa fa-times reset_button pull-right" aria-hidden="true"></i></p>'
            p_ele = $(document.getElementById(p_id));
            $(p_ele).remove();
            if (field_jq.val() != "") {
                $("#filter___p").append(p_text);
                reset_div = $(reset_div);
                reset_div.show();
            } else {
                reset_id = id + "_reset_main";
                $("#"+reset_id).trigger('click')
            }
        }

  }));


$(document).on("change", "input:file", (function(event){
    upload_field_id = event.target.id;
    var id = upload_field_id.split("___uploadFieldId")[0];
    var fileDisplayArea = $("#"+id)
    var file = document.getElementById(upload_field_id).files[0];
    var textType = /text.*/;
    if (file.type.match(textType)) {
        var reader = new FileReader();
        reader.readAsText(file, "UTF-8");
        reader.onload = function (evt) {
            fileDisplayArea.append(evt.target.result);
            fileDisplayArea.trigger('change');
        }

    } else{
        $("#"+upload_field_id).val('');
        reset_id = id + "_reset_main";
        $("#"+reset_id).trigger('click')
    }



}));

var alert_div = '<div class="alert alert-warning">\
    <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a>\
    <strong>Warning!</strong> You must hit the search button for the changes to be reflected in the results.\
  </div>'


$(document).on("click", '.reset_button', (function(event){
      event.preventDefault();
      id = event.target.id;

      id = id.split('_reset_main')[0];
      id = id.split('___p_reset_sidebar')[0];

      var field = document.getElementById(id);
      var field_jq = $(field);

      if (field.type == "text") {
        $(field_jq).val('');
      } else if (field.type == "select-one") {
        $(field_jq).prop('selectedIndex',0);
      } else if (field.type == "select-multiple") {
        $(field_jq).val("");
      } else if (field.type == "checkbox") {
        $(field_jq).prop('checked', false);
      } else if (field.type == "textarea") {
        $(field_jq).val('');
      }

      var reset_div_id = id+"_reset_div";
      var reset_div = document.getElementById(reset_div_id);
      reset_div = $(reset_div);
      reset_div.hide();
      p_id =  id + "___p";
      p_ele = $(document.getElementById(p_id));
      $(p_ele).remove();
      checked = $("input[type=checkbox][id*=attribute_group]:checked");
        if (checked.length == 0) {
            $('.submit_via_ajax_button').each(function() {
                $( this ).addClass( "disabled" );
            })

        }


     // check if input is ME type
    var attr = field_jq.attr('groupId');
    if (typeof attr !== typeof undefined && attr !== false) {
        field_jq.removeClass("MEGselected");
        attr_group = attr.split("_", 2).join("_");
        any_meg_selected= $("input.MEGselected").length
        if (any_meg_selected == 0)
            $("[groupId*=\"" + attr_group + "\"]").each(function() {
                    $(this).attr("disabled", false);
            });

    }
    $("#user-alert-div").html(alert_div)

  }));
