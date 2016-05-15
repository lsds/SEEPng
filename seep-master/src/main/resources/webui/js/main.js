var qmon_context;
var qmon_metrics = [];

function add_monitor(monitor_id, mode) { 
	qmon_plot(monitor_id, mode);

	$( ".horizon" ).remove();
	
	d3.select("#stream_monitor")                 
	.selectAll(".horizon")           
	.data(qmon_metrics)
	.enter()                         
	.insert("div", ".bottom")        // Insert the graph in a div  
	.attr("class", "horizon")        // Turn the div into
	.call(
		qmon_context.horizon()          // a horizon graph
		.format(d3.format(".2f")) // Format the values to 2 floating-point decimals
		.height(60)
//		.extent([-3, 3])
//		.colors(["#08519c","#3182bd","#6baed6","#bdd7e7"])
//		.colors(["#08519c","#3182bd","#6baed6","#bdd7e7","#bae4b3","#74c476","#31a354","#006d2c"])
		// hack to set the colours accordingly
		.colors(function () {  
				if (this.getElementsByClassName("title")[0].innerHTML.indexOf("Memory") > -1) {
					return [ "#bae4b3","#74c476","#31a354","#006d2c"];
				}
				else if (this.getElementsByClassName("title")[0].innerHTML.indexOf("CPU") > -1) {
					return [ "#bdd7e7","#6baed6","#3182bd","#08519c"];
				}
				else if (this.getElementsByClassName("title")[0].innerHTML.indexOf("Throughput") > -1) {
					return [ "#ffd8bb","#ffab6c","#e17009","#cc5700"];
				}
			})
		)
	.append('div')
	.attr("class", "cancel_horizon")
	.append('span')
	.attr("class", "icon fa fa-times");
	$('.cancel_horizon').on( "click", function( event ) {
		var i = 0;
		var hor = this.parentNode;
		while(hor != null && !('top' in hor.classList)) {
			hor = hor.previousSibling;
			i++;
		}
		qmon_context.on(qmon_metrics[i-2].id(), null);;
		qmon_metrics.splice(i-2, 1);
		this.parentNode.remove();
		qmon_context.focus();
	});
	  
}
var last_metric;

function qmon_plot(monitor_id, mode) { 
	cy_monitoring.nodes().each(function (i, v) {
		if (v.id() === monitor_id) {
			
			if (mode === 0) {
				par = '&mode=cpu';
				name = v.data().name + ' (CPU)';
			}
			else if (mode === 1) {
				par = '&mode=memory';
				name = v.data().name + ' (Memory)';
			}
                        else if (mode === 2) {
                                var name = v.data().name + ' (Throughput)';
        			var par = '&mode=throughput';
                        }
			
			last_metric = 
				qmon_context.metric(function(start, stop, step, callback) {
					// convert start & stop to milliseconds
					start = +start;
					stop = +stop;
					
/*					var values = [];
					while (start < stop) {
						start += step;
						values.push(Math.random());
					}
					*/
					var url = 'http://' + master_node_ip + ':' + master_node_port + worker_node_throughput+'/'+monitor_id;
					
					$.ajax({
						type: 'GET',
						url: url,
						async: true,
						crossDomain: true,
						contentType: "application/json",
						dataType: 'jsonp',
						data: 'start='+start+'&stop='+stop+'&step='+step+par,
						success: function(data) {
							var values = [];
							var last = 0;

							$.each(data, function(k,v) {
								if (v === null) {
									values.push(last);
								}
								else {
									last = v[v.length-1];
									values.push(v);
								}
							});
							callback(null, values);
							//values.concat(data);
						},
						error: function(e) {
						   console.log(e.message);
						}
					});
					
					//callback(null, values);
				}, name);
			qmon_metrics.push(last_metric);
		}		
	});
}

$(function() {

	// load the actual query plan from the master node
	load_query_plan();
	
	var qmon_number_points = $('#stream_monitor').innerWidth() - 30;
	var qmon_number_ticks =  qmon_number_points / 100;
//	var qmon_distance_points = 15 * 60 * 1000;
	var qmon_distance_points = 1000;
	
	qmon_context = cubism.context()
						.step(qmon_distance_points) // Distance between data points in milliseconds
						.size(qmon_number_points); // Number of data points
						//.stop();   // Fetching from a static data source; don't update values

	d3.select("body").append("div") // Add a vertical rule
	  .attr("class", "rule")        // to the graph
	  .call(qmon_context.rule());

	
	window.draw_monitor_skeleton = function () {
		d3.select("#stream_monitor")       // Select the div on which we want to act           
		  .selectAll(".axis")              // This is a standard D3 mechanism to
		  .data(["top", "bottom"])                   // bind data to a graph. In this case
		  .enter()                         // we're binding the axes "top" and "bottom".
		  .append("div")                   // Create two divs and 
		  .attr("class", function(d) {     // give them the classes
			return d + " axis";            // top axis and bottom axis
		  })                               // respectively 
		  .each(function(d) {              // For each of these axes,
			d3.select(this)                // draw the axes with 4 intervals
			  .call(qmon_context.axis()         // and place them in their proper places
			  .ticks(qmon_number_ticks).orient(d)); 
		  });

		$( ".horizon" ).remove();
		
		qmon_context.on("focus", function(i) {
			d3.selectAll(".value").style("right",                  // Make the rule coincide 
				i == null ? null : qmon_context.size() - i + "px"); // with the mouse
		});
	};
	
	d_query = $( "#query-dialog" ).dialog({
      autoOpen: false,
      height: 280,
      width: 300,
      modal: true,
      close: function() {
        d_query_form[ 0 ].reset();
      }
    });
	
	d_query_form = d_query.find( "form" ).on( "submit", function( event ) {
		event.preventDefault();
	});
});
