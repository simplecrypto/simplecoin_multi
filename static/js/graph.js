$(document).ready(function() {
    //Swap classes on nav tabs
    $('.tab').click(function () {
        $(this).siblings().removeClass('active')
        $(this).addClass('active')
    })

    //Swap graph time period
    $(".tab a").on("click", function() {
        var $anchor = $(this);
        if ($anchor.data('html-target') != undefined) {
            $('#' + $anchor.data('html-target') + ' img').show()
            generate_worker_data($anchor.data('html-target'), $anchor.data('target'), $anchor.data('format'), $anchor.data('user'),
                $anchor.data('worker'), $anchor.data('stat-type'));
        } else {
            $('#chart img').show()
            generate_data($anchor.data('target'), $anchor.data('format'), $anchor.data('user'));
        }
    });

    find_seconds = function(request_url) {
      if (request_url == 'hour') {
        seconds = 60;
      } else if (request_url == 'day') {
        seconds = 300;
      } else if (request_url == 'month') {
        seconds = 3600;
      }
    }
});

generate_graph = function(request_url, date_format, user) {
  //Calculate the hash rate based on the number of diff-1 shares generated in a minute
    var calculate_hash = function(sharesPerMin, seconds) {
      var khashes = ((Math.pow(2, 16) * sharesPerMin)/seconds)/1000;
      return khashes
    }
  //Calculate a value to return for the y-scale, in khash or mhash
    var y_scale = function(max_hash, seconds) {
        return (calculate_hash(max_hash, seconds))/1000
    }
  //Calculate a value to return for the y-axis text, khash or mhash
    var generate_y_text = function(max_hash) {
        return "MHashes/sec"
    }

  var clean_data = [];
  var last_10min = 0;
  generate_data = function(request_url, date_format, user) {
    clean_data = [];
    d3.json('/' + user + '/stats/' + request_url, function(data) {
      start = data.start;
      end = data.end;
      step = data.step;
      for (var key in data.workers) {
        var worker = data.workers[key];
        var values = []
        for (var i = start; i <= end; i += step) {

          if (i in worker) {
            values.push([i * 1000, worker[i]]);
            //If this is an hour loop build a total value for last 10min
            if (i > (end - (10 * step)) &&  request_url == 'hour') {
              last_10min += worker[i];
            }
          } else {
            values.push([i * 1000, 0]);
          }
        }

        if (key == "")
          key = "[unnamed]";
          clean_data.push({key: key, seriesIndex: 0, values: values});
      }

      //set seconds to determine hashrate by
      var seconds = find_seconds(request_url);

      //Actually generate/regenerate the graph here
      nv.addGraph(window.generate_graph = function() {
        var chart = nv.models.stackedAreaChart()
                      .x(function(d) { return d[0] })   //We can modify the data accessor functions...
                      .y(function(d) { return +y_scale(d[1], seconds) })   //...in case your data is formatted differently.
                      .useInteractiveGuideline(true)    //Tooltips which show all data points. Very nice!
                      .transitionDuration(500)
                      .showControls(true)       //Allow user to choose 'Stacked', 'Stream', 'Expanded' mode.
                      .clipEdge(true);

        // Format x-axis labels with custom function.
        chart.xAxis
            .tickFormat(function(d) { return d3.time.format(date_format)(new Date(d)) })
            .scale(d3.time.scale())
            .axisLabel('Time')
            .axisLabelDistance(30);;

        chart.yAxis
            .tickFormat(d3.format(',.2f'))
            .axisLabel('MHash/sec')
            .axisLabelDistance(30);

        d3.select('#chart svg')
          .datum(clean_data)
          .call(chart);

        $('#chart img').hide()

        //Hack to update chart when click event occurs
        $(".nv-stackedAreaChart").on("click", function() {
            chart.update();
        });

        nv.utils.windowResize(chart.update);
      });
    });
  }

  // Initial graph generation
  generate_data(request_url, date_format, user);
}


generate_worker_graph = function(target, request_url, date_format, user, worker, stat_type) {

  var clean_data = [];
  generate_worker_data = function(target, request_url, date_format, user, worker, stat_type) {
    clean_data = [];
    d3.json('/' + user + '/' + worker + '/' + stat_type + '/' + request_url, function(data) {

      start = data.start;
      end = data.end;
      step = data.step;
      var values_no_stamp = []
      for (var key in data.workers) {
        var worker = data.workers[key];
        var values = [];

        for (var i = start; i <= end; i += step) {
          if (i in worker) {
            if (worker[i] < 0){ worker[i] = 0; }
            values.push([i * 1000, worker[i]]);
            values_no_stamp.push(worker[i]);
          } else {
            values.push([i * 1000, 0]);
          }
        }

        if (key == "")
          key = "[unnamed]";
          clean_data.push({key: key, seriesIndex: 0, values: values});
      }

      //set seconds to determine hashrate by
      var seconds = find_seconds(request_url);

      //Actually generate/regenerate the graph here
      nv.addGraph(window.generate_graph = function() {
      if (stat_type == 'temp') {
        var chart = nv.models.lineChart()
                      .x(function(d) { return d[0] })   //We can modify the data accessor functions...
                      .y(function(d) { return d[1] })   //...in case your data is formatted differently.
                    .useInteractiveGuideline(true)  //We want nice looking tooltips and a guideline!
                    .transitionDuration(350)  //how fast do you want the lines to transition?
                    .clipEdge(true);

        var axis_label = 'Temperature';

      } else {
        var chart = nv.models.stackedAreaChart()
                      .x(function(d) { return d[0] })   //We can modify the data accessor functions...
                      .y(function(d) { return d[1]/1000 })   //...in case your data is formatted differently.
                      .useInteractiveGuideline(true)    //Tooltips which show all data points. Very nice!
                      .transitionDuration(500)
                      .showControls(true)       //Allow user to choose 'Stacked', 'Stream', 'Expanded' mode.
                      .clipEdge(true);
        var axis_label = 'KHash/sec';
      }

        // Format x-axis labels with custom function.
        chart.xAxis
            .tickFormat(function(d) { return d3.time.format(date_format)(new Date(d)) })
            .scale(d3.time.scale())
            .axisLabel('Time')
            .axisLabelDistance(30);

        chart.yAxis
            .tickFormat(d3.format(',.2f'))
            .axisLabel(axis_label)
            .axisLabelDistance(25);

        chart.forceY([0, d3.max(values_no_stamp)*1.1 ]);

        d3.select('#' + target + ' svg')
          .datum(clean_data)
          .call(chart);

        $('#' + target + ' img').hide()

        //Hack to update chart when click event occurs
        $(".nv-stackedAreaChart").on("click", function() {
            chart.update();
        });

        nv.utils.windowResize(chart.update);
      });
    });
  }

  // Initial graph generation
  generate_worker_data(target, request_url, date_format, user, worker, stat_type);

}


generate_network_graph = function(target, request_url, date_format, graph_type, network_block_time) {

  var clean_data = [];
  generate_network_data = function(target, request_url, date_format, graph_type, network_block_time) {
    clean_data = [];
    d3.json('/network_stats/' + graph_type + '/' + request_url, function(data) {

      start = data.start;
      end = data.end;
      step = data.step;
      var values_no_stamp = []
      for (var key in data.workers) {
        var worker = data.workers[key];
        var values = [];

        for (var i = start; i <= end; i += step) {
          if (i in worker) {
            if (worker[i] < 0){ worker[i] = 0; }
            values.push([i * 1000, worker[i]]);
            values_no_stamp.push(worker[i]);
          } else {
            values.push([i * 1000, 0]);
          }
        }

        if (key == "")
          key = "[unnamed]";
          clean_data.push({key: key, seriesIndex: 0, values: values});
      }

      //set seconds to determine hashrate by
      var seconds = find_seconds(request_url);

      //Actually generate/regenerate the graph here
      nv.addGraph(window.generate_graph = function() {

      // if a block time is passed in, lets get ready to return a calculated hashrate
      // rather than a difficulty
      var multiplier = 1;
      if (network_block_time > 0) {
          multiplier = Math.pow(2, 32) / network_block_time
      }
      var chart = nv.models.lineChart()
                    .x(function(d) { return d[0] })   //We can modify the data accessor functions...
                    .y(function(d) { return d[1] * multiplier })   //...in case your data is formatted differently.
                    .useInteractiveGuideline(true)  //We want nice looking tooltips and a guideline!
                    .transitionDuration(350)  //how fast do you want the lines to transition?
                    .clipEdge(true);

      var axis_label = 'Network Difficulty';


        // Format x-axis labels with custom function.
        chart.xAxis
            .tickFormat(function(d) { return d3.time.format(date_format)(new Date(d)) })
            .scale(d3.time.scale())
            .axisLabel('Time')
            .axisLabelDistance(30);

        chart.yAxis
            .tickFormat(d3.format(',.2f'))
            .axisLabel(axis_label)
            .axisLabelDistance(25);

        chart.forceY([0, d3.max(values_no_stamp)*1.1 ]);

        d3.select('#' + target + ' svg')
          .datum(clean_data)
          .call(chart);

        $('#' + target + ' img').hide()

        nv.utils.windowResize(chart.update);
      });
    });
  }

  // Initial graph generation
  generate_network_data(target, request_url, date_format, graph_type, network_block_time);

}
