$(document).ready(function() {

  function numberWithCommas(x) {
      var parts = x.toString().split(".");
      parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      return parts.join(".");
  };

  // Calculate est shares to complete the current round
  var avg_shares_to_solve = function(difficulty){
    return difficulty * window.shares_per_hash;
  };

  // Calculate estimated payout for next round
  var round_payout = function(difficulty, user_shares, shares_to_solve, donate, round_reward, n_multiplier, pplns_total_shares){
    var user_percentage;
    if (pplns_total_shares < (shares_to_solve * n_multiplier)) {
      user_percentage = user_shares / pplns_total_shares;
    } else {
      user_percentage = user_shares / (shares_to_solve * n_multiplier);
    }
    return ((user_percentage * round_reward)) * (1 - (donate/100));
  };

  // Calculate est coins per day
  var daily_est = function(last_10_shares, shares_to_solve, donate, round_reward) {
    var day_shares, daily_percentage;
    day_shares = (last_10_shares * 6 * 24);
    daily_percentage = (day_shares / shares_to_solve);
    return ((daily_percentage * round_reward)) * (1 - (donate/100));
  };

  // Calculate number of shares in pplns
  var shares_in_pplns = function(pplns_window, n_multiplier) {
    return pplns_window * n_multiplier;
  };

  // Grabs a url from the passed in element and wraps that element with a link
  var wrap_link = function(selector, address) {
      var html = $(selector).html();
      var url = $(selector).data('url');
      $(selector).html('<a href="' + url + '/' + address + '">' + html + '</a>');
  };

  // Checks the ref's value and sets the target's html to that val
  var html_from_val = function(ref, target, val_prefix, null_val) {
      if (ref.value == '') {
        $(target).html(null_val);
      } else {
        $(target).html(val_prefix + ref.value);
      }
  };

  // Sets triggers on an element and toggles html of target when showing/hiding
  var flip = function(watch, target, hide_val, show_val) {
    $(watch).on('hide.bs.collapse', function () {
      $(target).html(hide_val);
    });
    $(watch).on('show.bs.collapse', function () {
      $(target).html(show_val);
    });
  };

  // Runs an ajax request to the server to validate a BC address
  var validate_address = function (watch, success_callback) {
    $(watch).on("blur", function () {
      var _that = $(this);
      var currency = _that.attr("name");
      var addr = _that.val();

      var helptext = _that.siblings('span.help-text');
      if (addr == '') {
        helptext.siblings('span').hide();
        helptext.css('display', 'block');
        return
      }
      var checking = _that.siblings('span.checking-address');
      var invalid = _that.siblings('span.invalid-address');
      var valid = _that.siblings('span.valid-address');

      checking.siblings('span').hide();
      checking.css('display', 'block');
      var json = JSON.stringify([currency, addr]);

      var fail = function () {
        invalid.siblings('span').hide();
        invalid.css('display', 'block');
      }

      var success = function () {
        valid.siblings('span').hide();
        valid.css('display', 'block');
        success_callback(_that.val())
      }

      // check if alpha numeric
      var alphanum = new RegExp(/^[a-z0-9]+$/i);
      if (!alphanum.test(addr)) {
        fail();
        return
      }

      $.ajax({
        type: "POST",
        dataType: "json",
        contentType: "application/json; charset=utf-8",
        url: "/validate_address",
        data: json
      }).done(function(data) {
        for (var property in data) {
          if (data.hasOwnProperty(property)) {
              if (property != 'Any') { $('.address-currency').html(property); }
              if (data[property] == true) { success(); } else { fail(); }
          }
        }
      });

    });
  };

////////////////////////////////////////////
// JS for home page
////////////////////////////////////////////

  //  Action stats form based on input val
    $('#statsform').submit(function(){
      var address = $('#inputAddress').val();
      $(this).attr('action', "/" + address);
    });

  // Setup collapse button for configuration guide
  flip('#miner-config', '#config-guide', '[+]', '[-]');

////////////////////////////////////////////
// JS for configuration guide
////////////////////////////////////////////

  new validate_address('.user-address-field', function (valid_address) {
    $('span.mining-username').html(valid_address);
    wrap_link('#stats-link', valid_address);
    wrap_link('#settings-link', valid_address);
  });

  $('.server-region').change(function() {
          html_from_val(this, 'span.stratum-url', '', '')
  });
  $('.stratum-ports').change(function() {
          html_from_val(this, 'span.stratum-port', '', '')
  });
  $('#workername').keyup(function() {
          html_from_val(this, 'span.mining-workername', '.', '.1')
  });
  $('#mining-diff').change(function() {
          html_from_val(this, 'span.mining-diff', 'diff=', 'x')
  });

////////////////////////////////////////////
// JS for user settings
////////////////////////////////////////////

  new validate_address('.address-field', function () {});

  var interval = null;

  ZeroClipboard.config( { moviePath: '//cdnjs.cloudflare.com/ajax/libs/zeroclipboard/1.3.5/ZeroClipboard.swf' } );

  var client = new ZeroClipboard($("#copy-button"));
  client.on("complete", function(client, args) {
    $("#copied-notif").show();
    setTimeout(function(){
      $("#copied-notif").fadeOut();
    }, 1000);
  });

  $("#settings-form").on("submit", function(event) {
    event.preventDefault();

    var textarea = $("#message-div").html();
    $("#message-div").html("<img src='{{ config['assets_address'] | safe }}/img/712.GIF' " +
      "style='margin-left: auto; margin-right:auto; display:block; padding:35px;'>");

    clearInterval(interval);
    $("#message-notif").css('color', '#58CF58');

    // serialize the data in the form
    var serializedData = $(this).serialize();

    $.ajax({
      type: "POST",
      url: "/generate_message",
      data: serializedData,
    }).done(function(data) {
      $("#message-div").html(textarea);
      if('errors' in data) {
        for (var property in data.errors) {
          if (data.errors.hasOwnProperty(property)) {
            $("#"+property).siblings("span.invalid-address").addClass("alert alert-danger").css('color', 'white').show();
          }
        }
      } else {
        $("#message").text(data.msg_str);
        $("#sub-message").val(data.msg_str);
        $("#message-notif").show();
        var seconds_left = 15 * 60;
        interval = setInterval(function () {
          function n(n){
              return n > 9 ? "" + n: "0" + n;
          }
          seconds_left = seconds_left - 1;
          var minutes = parseInt(seconds_left / 60);
          var seconds = n(parseInt(seconds_left % 60));

          // format countdown string + set tag value
          $("#message-notif").html("Valid for " + minutes + ":" + seconds + " minutes");

          if (seconds_left <= 0) {
            $("#message-notif").html("No longer valid");
            $("#message-notif").css('color', 'red');
            clearInterval(interval);
          }
        }, 1000);
      }

    });
  });

});