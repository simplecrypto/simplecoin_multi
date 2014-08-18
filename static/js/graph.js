generate_graph = function(selector, params) {
    var timespan = "hour";
    // Calculate the hash rate based on the number of diff-1 shares generated
    // in a minute
    var calculate_hash = function(sharesPerMin, seconds) {
        var khashes = ((hashes_per_share * sharesPerMin)/seconds)/1000;
        return khashes
    }
    // Calculate a value to return for the y-scale, in khash or mhash
    var y_scale = function(max_hash, seconds) {
        return (calculate_hash(max_hash, seconds))/1000
    }

    var generate_data = function(timespan) {
        var clean_data = [];
        var date_format;
        var seconds;
        switch(timespan) {
            case "hour":
                date_format = "%H:%M";
                seconds = 60;
                break;
            case "day":
                date_format = "%a %H:%M %p";
                seconds = 300;
                break;
            case "month":
                date_format = "%m/%d %H:%M";
                seconds = 3600;
                break;
        }

        params.window = timespan;
        d3.json('/api/shares?' + $.param(params), function(data) {
            start = data.start;
            end = data.end;
            step = data.step;
            for (var idx in data.workers) {
                var worker = data.workers[idx];
                var values = [];
                for (var i = start; i <= end; i += step) {
                    if (i in worker.values) {
                        values.push([i * 1000, worker.values[i]]);
                    } else {
                        values.push([i * 1000, 0]);
                    }
                }

                clean_data.push({key: worker.data.label, seriesIndex: 0, values: values});
            }

            //Actually generate/regenerate the graph here
            generate_graph = function() {
                var chart = nv.models.stackedAreaChart()
                            .x(function(d) { return d[0] })   //We can modify the data accessor functions...
                            .y(function(d) { return +d[1] / data.scale; })   //...in case your data is formatted differently.
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
                    .axisLabel(data.scale_label)
                    .axisLabelDistance(30);

                d3.select(selector.find('svg')[0])
                    .datum(clean_data)
                    .call(chart);

                selector.find('img').hide()

                //Hack to update chart when click event occurs
                $(".nv-stackedAreaChart").on("click", function() {
                    chart.update();
                });

                nv.utils.windowResize(chart.update);
            }
            nv.addGraph(generate_graph);
        });
    }
    var set_active = function(cls) {
        if (cls != "hour")
            selector.find(".hour").removeClass("active");
        if (cls != "day")
            selector.find(".day").removeClass("active");
        if (cls != "month")
            selector.find(".month").removeClass("active");
        selector.find("." + cls).addClass("active");
        generate_data(cls);
    }
    selector.find('.hour').click(function () { set_active("hour"); });
    selector.find('.day').click(function () { set_active("day"); });
    selector.find('.month').click(function () { set_active("month"); });
    set_active("hour");
}
