/***************************************************
Data flow graphs
****************************************************/

var graph_type_source = {
	weight: 5, 
	color: '#4297d7', 
	shape: 'triangle'
};

var graph_type_query = {
	weight: 7, 
	color: '#e17009', 
	shape: 'ellipse'
};

var graph_edge_defaults = {
	color: '#4297d7', 
	strength: 1 
};

var graph_layout = {
			name: 'breadthfirst',
			fit: true, // whether to fit the viewport to the graph
			ready: undefined, // callback on layoutready
			stop: undefined, // callback on layoutstop
			directed: true, // whether the tree is directed downwards (or edges can point in any direction if false)
			padding: 5, // padding on fit
			circle: false, // put depths in concentric circles if true, put depths top down if false
			roots: undefined, // the roots of the trees
			maximalAdjustments: 0 // how many times to try to position the nodes in a maximal way (i.e. no backtracking)
	};

var graph_style = cytoscape.stylesheet()
	.selector('node')
	  .css({
		'shape': 'data(type.shape)',
		'width': 'mapData(type.weight, 0, 10, 40, 80)',
		'content': 'data(name)',
		'text-valign': 'center',
		'text-outline-width': 2,
		'text-outline-color': 'data(type.color)',
		'background-color': 'data(type.color)',
		'color': '#fff'
	  })
	.selector('.edgehandles-hover')
	  .css({
		'border-width': 3,
		'border-color': '#808080'
	})
	.selector('.edgehandles-source')
	  .css({
		'border-width': 3,
		'border-color': '#808080'
	})
	.selector('.edgehandles-target')
	  .css({
		'border-width': 3,
		'border-color': '#808080'
	})
	.selector('edge')
	  .css({
		'opacity': 0.666,
		'width': 'mapData(type.strength, 0, 10, 3, 10)',
		'target-arrow-shape': 'triangle',
		'source-arrow-shape': 'circle',
		'line-color': 'data(type.color)',
		'source-arrow-color': 'data(type.color)',
		'target-arrow-color': 'data(type.color)'
	  })
    .selector(':selected')
      .css({
        'border-width': 3,
        'border-color': '#808080'
      })
	.selector('.faded')
	  .css({
		'opacity': 0.25,
		'text-opacity': 0
	});


var graph_node_id = 5;
var graph_edge_id = 4;
	
var graph_elements = {
	nodes: [
	  { data: { id: '1', name: 'Source', type: graph_type_source, query: 'injectors.csv' } },
	  { data: { id: '2', name: 'Query 1', type: graph_type_query, query: 'SELECT * <br /> FROM input [ROW 1] <br /> WHERE id = \'A01\''  } },
	  { data: { id: '3', name: 'Query 2', type: graph_type_query, query: 'SELECT * <br /> FROM input [ROW 1] <br /> WHERE id = \'A01\''  } },
	  { data: { id: '4', name: 'Query 3', type: graph_type_query, query: 'SELECT * <br /> FROM input [ROW 1] <br /> WHERE id = \'A01\''  } }
	],
	edges: [
	  { data: { streamid: '1', source: '1', target: '2', type: graph_edge_defaults} },
	  { data: { streamid: '2', source: '1', target: '3', type: graph_edge_defaults } },
	  { data: { streamid: '3', source: '3', target: '4', type: graph_edge_defaults } }
	]
};

var graph_pan_defaults = ({
	zoomFactor: 0.05, // zoom factor per zoom tick
	zoomDelay: 45, // how many ms between zoom ticks
	minZoom: 0.1, // min zoom level
	maxZoom: 10, // max zoom level
	fitPadding: 10, // padding when fitting
	panSpeed: 10, // how many ms in between pan ticks
	panDistance: 10, // max pan distance per tick
	panDragAreaSize: 75, // the length of the pan drag box in which the vector for panning is calculated (bigger = finer control of pan speed and direction)
	panMinPercentSpeed: 0.25, // the slowest speed we can pan by (as a percent of panSpeed)
	panInactiveArea: 8, // radius of inactive area in pan drag box
	panIndicatorMinOpacity: 0.5, // min opacity of pan indicator (the draggable nib); scales from this to 1.0
	autodisableForMobile: false, // disable the panzoom completely for mobile (since we don't really need it with gestures like pinch to zoom)

	// icon class names
	sliderHandleIcon: 'fa fa-minus',
	zoomInIcon: 'fa fa-plus',
	zoomOutIcon: 'fa fa-minus',
	resetIcon: 'fa fa-expand'
});

var graph_context_menu_defaults = {
    menuRadius: 100, // the radius of the circular menu in pixels
    selector: 'node', // elements matching this Cytoscape.js selector will trigger cxtmenus
    commands: [ // an array of commands to list in the menu
        {
            content: 'Add Monitor' ,
            select: function(){ 
				qmon[this.id()] = true;
				draw_graph(qmon);
			}
        },
        {
            content: 'Remove Monitor' ,
            select: function(){ 
				qmon[this.id()] = false;
				draw_graph(qmon);
            }
        }
		
    ], 
    fillColor: '#4297d7', // the background colour of the menu
    activeFillColor: 'rgba(92, 194, 237, 0.75)', // the colour used to indicate the selected command
    activePadding: 20, // additional size in pixels for the active command
    indicatorSize: 24, // the size in pixels of the pointer to the active command
    separatorWidth: 3, // the empty spacing in pixels between successive commands
    spotlightPadding: 4, // extra spacing in pixels between the element and the spotlight
    minSpotlightRadius: 24, // the minimum radius in pixels of the spotlight
    maxSpotlightRadius: 38, // the maximum radius in pixels of the spotlight
    itemColor: 'white', // the colour of text in the command's content
    itemTextShadowColor: 'white', // the text shadow colour of the command's content
    zIndex: 9999 // the z-index of the ui div
};


var edge_handle_defaults = {
  preview: true, // whether to show added edges preview before releasing selection
  handleSize: 0, // the size of the edge handle put on nodes
  handleColor: 'red', // the colour of the handle and the line drawn from it
  handleLineType: 'ghost', // can be 'ghost' for real edge, 'straight' for a straight line, or 'draw' for a draw-as-you-go line
  handleLineWidth: 1, // width of handle line in pixels
  handleNodes: 'node', // selector/filter function for whether edges can be made from a given node
  hoverDelay: 50, // time spend over a target node before it is considered a target selection
  cxt: true, // whether cxt events trigger edgehandles (useful on touch)
  enabled: true, // whether to start the plugin in the enabled state
  toggleOffOnLeave: true, // whether an edge is cancelled by leaving a node (true), or whether you need to go over again to cancel (false; allows multiple edges in one pass)
  edgeType: function( sourceNode, targetNode ){
    // can return 'flat' for flat edges between nodes or 'node' for intermediate node between them
    // returning null/undefined means an edge can't be added between the two nodes
    return 'flat'; 
  },
  loopAllowed: function( node ){
    // for the specified node, return whether edges from itself to itself are allowed
    return false;
  },
  nodeLoopOffset: -50, // offset for edgeType: 'node' loops
  nodeParams: function( sourceNode, targetNode ){
    // for edges between the specified source and target
    // return element object to be passed to cy.add() for intermediary node
    return {};
  },
  edgeParams: function( sourceNode, targetNode, i ){
    // for edges between the specified source and target
    // return element object to be passed to cy.add() for edge
    // NB: i indicates edge index in case of edgeType: 'node'
    return {};
  },
  start: function( sourceNode ){
    // fired when edgehandles interaction starts (drag on handle)
  },
  complete: function( sourceNode, targetNodes, addedEntities ){
    // fired when edgehandles is done and entities are added
	targetNodes.each(function(k, v) {
		graph_elements.edges.push({ data: { streamid: graph_edge_id++, source: sourceNode.id(), target: v.id(), type: graph_edge_defaults}});
	});
  },
  stop: function( sourceNode ){
    // fired when edgehandles interaction is stopped (either complete with added edges or incomplete)
  }
};

var qmon = {1:true}

$(function() {

	$( "#tabs" ).tabs();

	$('#monitoring_graph').cytoscape({
		layout: graph_layout,
		style: graph_style,
		elements: graph_elements,
		ready: function() { 
			window.cy_monitoring = this; 
			cy_monitoring.nodes().qtip({
				content: function(){ return this.data().query },
				position: { my: 'top center', at: 'bottom center'},
				style: { 
					classes: 'qtip-bootstrap',
					tip: { width: 50, height: 20 }
				}
			});
			cy_monitoring.edges().qtip({
				content: function(){ return 'Stream: ' + this.data().streamid },
				position: { my: 'top center', at: 'bottom center'},
				style: { 
					classes: 'qtip-bootstrap',
					tip: { width: 50, height: 20 }
				}
			});

		}
	});
	
	$('#monitoring_graph').cytoscapePanzoom( graph_pan_defaults );
	$('#monitoring_graph').cytoscapeCxtmenu( graph_context_menu_defaults );   
	
	$('#deployment_graph').cytoscape({
		layout: graph_layout,
		style: graph_style,
		elements: graph_elements,
		ready: function() { 
			window.cy_deployment = this; 
			cy_deployment.nodes().qtip({
				content: function(){ return this.data().query },
				position: { my: 'top center', at: 'bottom center'},
				style: { 
					classes: 'qtip-bootstrap',
					tip: { width: 50, height: 20 }
				}
			});
			cy_deployment.edges().qtip({
				content: function(){ return 'Stream: ' + this.data().streamid },
				position: { my: 'top center', at: 'bottom center'},
				style: { 
					classes: 'qtip-bootstrap',
					tip: { width: 50, height: 20 }
				}
			});

			window.cy_deployment.style()
				.selector('edge')
					.css({
						'opacity': 0.666,
						'width': 'mapData('+graph_edge_defaults.strength+', 0, 10, 3, 10)',
						'target-arrow-shape': 'triangle',
						'source-arrow-shape': 'circle',
						'line-color': graph_edge_defaults.color,
						'source-arrow-color': graph_edge_defaults.color,
						'target-arrow-color': graph_edge_defaults.color
					})
				.update();
		}
	});
	$('#deployment_graph').cytoscapePanzoom( graph_pan_defaults );
	$('#deployment_graph').cytoscapeEdgehandles( edge_handle_defaults );

/*
	$( "#tabs" ).tabs({
	  activate: function( event, ui ) {
		if (event.toElement.firstChild.data == "Deployment") {
//			window.cy_deployment.load(graph_elements);
			window.cy_deployment.fit();
		}
		else if (event.toElement.firstChild.data == "Monitoring") {
//			window.cy_monitoring.load(graph_elements);
			window.cy_monitoring.fit();
		}
	  }
	});
*/
	
	var qmon_number_points = $('#tabs').innerWidth();
	var qmon_number_ticks =  qmon_number_points / 100;
//	var qmon_distance_points = 15 * 60 * 1000;
	var qmon_distance_points = 1000;
	
	var context = cubism.context()
						.step(qmon_distance_points) // Distance between data points in milliseconds
						.size(qmon_number_points); // Number of data points
						//.stop();   // Fetching from a static data source; don't update values

	d3.select("body").append("div") // Add a vertical rule
	  .attr("class", "rule")        // to the graph
	  .call(context.rule());

	function plot(series) { 
		console.log(series);
		metrics = []
		$(graph_elements.nodes).each(function (i, v) {
			if (series[v.data.id]) {
				metrics.push(
					context.metric(function(start, stop, step, callback) {
						var values = [];
						// convert start & stop to milliseconds
						start = +start;
						stop = +stop;
						
						while (start < stop) {
							start += step;
							values.push(Math.random());
						}

						callback(null, values);			
						
					}, v.data.name)
				)
			}		
		})
		console.log(metrics);
		return metrics; 
	}
	
	window.draw_graph = function (series) {
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
			  .call(context.axis()         // and place them in their proper places
			  .ticks(qmon_number_ticks).orient(d)); 
		  });

		$( ".horizon" ).remove();
		
		d3.select("#stream_monitor")                 
		  .selectAll(".horizon")           
		  .data(plot(series))
		  .enter()                         
		  .insert("div", ".bottom")        // Insert the graph in a div  
		  .attr("class", "horizon")        // Turn the div into
		  .call(context.horizon()          // a horizon graph
			.format(d3.format("+,.2p")) // Format the values to 2 floating-point decimals
			.height(80)
			.extent([-3, 3])
			.colors(["#08519c","#3182bd","#6baed6","#bdd7e7"])
			//,"#bae4b3","#74c476","#31a354","#006d2c"])
		  );   

		context.on("focus", function(i) {
			d3.selectAll(".value").style("right",                  // Make the rule coincide 
				i == null ? null : context.size() - i + "px"); // with the mouse
		});
	}
	
	draw_graph(qmon);
});
