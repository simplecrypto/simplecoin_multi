generate_graph = function(request_url, append_to) {

//    request_url = json endpoint to grab data from
//    append_to = a class or id to append the graph to
//    Expects to find a dictionary containing the key 'points'
//    'points' should contain a list of lists.

//Calculate the hash rate based on the number of diff-1 shares generated in a minute
  var calculate_hash = function(sharesPerMin) {
    var khashes = ((Math.pow(2, 16) * sharesPerMin)/60)/1000;
    return khashes
  }
//Calculate a value to return for the y-scale, in khash or mhash
  var y_scale = function(max_hash) {
      var yscale = (calculate_hash(max_hash) );
      if(yscale < 1000) {
          yscale = yscale
      } else if (yscale > 1000) {
          yscale = yscale/1000
      }
      return yscale
  }
//Calculate a value to return for the y-axis text, khash or mhash
  var generate_y_text = function(max_hash) {
      var yaxis_text;
      if((calculate_hash(max_hash) < 1000)) {
          yaxis_text = "KHashes/sec"
      } else {
          yaxis_text = "MHashes/sec"
      }

      return yaxis_text
  }

  var margin = {top: 20, right: 125, bottom: 30, left: 70},
      width = 960 - margin.left - margin.right,
      height = 300 - margin.top - margin.bottom;

  var color_hash = [
            ["One minute Avg", "steelblue"],
            ["1 Hour Avg", "red"]
          ]

  var x = d3.time.scale()
      .range([0, width]);

  var y = d3.scale.linear()
      .range([height, 0]);

  var xAxis = d3.svg.axis()
      .scale(x)
      .orient("bottom");

  var yAxis = d3.svg.axis()
      .scale(y)
      .orient("left");

  var line = d3.svg.line()
      .interpolate("basis")
      .x(function(d) {
          return x(d.time); })
      .y(function(d) { return y(d.shares); });

  var hourAverageLine = d3.svg.line()
      .interpolate("basis")
      .x(function(d) { return x(d[0]); })
      .y(function(d) { return y(d[1]); });

  var svg = d3.select(append_to).append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
    .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
  var hour_line = [];
  var hour_total = 0, minute = 0;
  var hour_avg_list = [], hour_avg_val = 0;
  d3.json(request_url, function(error, data) {
    data['points'].forEach(function(d,i) {

      d.time = new Date(d[0] * 1000);
      d.shares = +y_scale(d[1]);

//    build an avg line from last hour's data
      hour_avg_list.push(d.shares);
      if (i > 58) {
//        build avg from hour_avg_list
          hour_avg_list.forEach(function(d) {
              hour_avg_val += d;
          });
//        build a new list containing 1 hour averages
          hour_line.push([d.time, hour_avg_val/60]);
          hour_avg_val = 0;
//        Pop off first item in list to keep it at 60
          hour_avg_list.shift();
      }

//      Every 60 minutes add a new data point from avg shares
//      if (i != 0) {
//          minute = i%60;
//          if (minute == 0) {
//              hour_line.push([d.time, (hour_total+ d.shares)/60]);
//              hour_total=0;
//          } else {
//              hour_total += d.shares;
////            Catch the last few minutes
//              if (i == data['length']-1) {
//                  hour_line.push([d.time, (hour_total+ d.shares)/minute]);
//              }
//          }
//      } else {
////        Avoid a /0
//          hour_total += d.shares;
//      }
    });

    var yaxis_text = generate_y_text(d3.max(data['points'][0]));

    x.domain(d3.extent(data['points'], function(d) { return d.time; }));
    y.domain([0, d3.max(data['points'], function(d) { return d.shares*1.1; })]);

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis)

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
      .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", -60)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text(yaxis_text);

    svg.append("path")
        .datum(data['points'])
        .attr("class", "line")
        .attr("d", line);

    svg.append("path")
        .datum(hour_line)
        .attr("class", "line2")
        .attr("d", hourAverageLine);

    // add legend
    var legend = svg.append("g")
      .attr("class", "legend")
      .attr("height", 100)
      .attr("width", 100)
      .attr('transform', 'translate(-20,50)')

      legend
        .selectAll('rect')
        .data(color_hash)
        .enter()
        .append("rect")
      .attr("x",  width + 57)
        .attr("y", function(d, i){ return i *  20;})
      .attr("width", 10)
      .attr("height", 10)
      .style("fill", function(d) {
          var color = d[1];
          return color;
        })

      legend.selectAll('text')
        .data(color_hash)
        .enter()
        .append("text")
      .attr("x", width + 70)
        .attr("y", function(d, i){ return i *  20 + 9;})
      .text(function(d) {
          var text = d[0];
          return text;
        });

//    svg.append("svg:circle")
//        .datum(hour_line)
//        .style("fill", "red")
//        .attr("cx", hourAverageLine[0])
//        .attr("cy", hourAverageLine[1])
//        .attr("r", 4.5);

  });


}