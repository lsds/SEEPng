var master_node_ip = "localhost";
var master_node_port = 8081;
var master_node_query_plan_url = "/queries";
var worker_node_throughput = "/metrics";

var cy_monitoring;
var d_edge, d_edge_form;
/* Currently Supported Node Types! */
var graph_type_source   = { weight: 5, color: '#4297d7', shape: 'triangle' };
var graph_type_sink     = { weight: 5, color: '#4297d7', shape: 'octagon' };
var graph_type_query    = { weight: 7, color: '#e17009', shape: 'ellipse' };
var graph_edge_defaults = { color: '#4297d7', strength: 1 };

var graph_node_id = 0;
var graph_edge_id = 0;

var graph_elements = {
	nodes: [], edges: []
/*	nodes: [
	  { data: { id: 'inj', name: 'Injectors', ip: '127.0.0.1', port: '8081', monitor_url: '', type: graph_type_source, query: 'injectors.csv' } },
	  { data: { id: 'pro', name: 'Producers', ip: '127.0.0.1', port: '8082', monitor_url: '', type: graph_type_source, query: 'producers.csv' } },
	  { data: { id: 'a01', name: 'BHP A01', ip: '127.0.0.1', port: '8083', monitor_url: '', type: graph_type_query, query: 'SELECT BHP <br /> FROM producers [ROW 1] <br /> WHERE id = \'A01\''  } },
	  { data: { id: 'a04', name: 'BHP A04', ip: '127.0.0.1', port: '8084', monitor_url: '', type: graph_type_query, query: 'SELECT BHP <br /> FROM injectors [ROW 1] <br /> WHERE id = \'A01\''  } },
	  { data: { id: 'join', name: 'Join BHPs', ip: '127.0.0.1', port: '8085', monitor_url: '', type: graph_type_query, query: 'SELECT * <br /> FROM a01 [ROW 1], a04 [ROW 1]<br /> WHERE id = \'A01\''  } }
	],
	edges: [
	  { data: { streamid: 'injectors', source: 'inj', target: 'a04', type: graph_edge_defaults} },
	  { data: { streamid: 'producers', source: 'pro', target: 'a01', type: graph_edge_defaults } },
	  { data: { streamid: 'a01', source: 'a01', target: 'join', type: graph_edge_defaults } },
	  { data: { streamid: 'a04', source: 'a04', target: 'join', type: graph_edge_defaults } }
	]
	*/
};

function load_query_plan() {
	//draw graph
	draw_monitoring_graph();
	
	var url = 'http://' + master_node_ip + ':' + master_node_port + master_node_query_plan_url;
	$.ajax({
		type: 'GET',
		url: url,
		async: true,
		crossDomain: true,
		contentType: "application/json",
		dataType: 'jsonp',
		success: function(data) {
			graph_elements = data;
			$.each(graph_elements, function (k, v) {
				if (k === "edges") {
					$(v).map(function(k2, v2) {

						v2['data']['type'] = window[v2['data']['type']];			

						reload_monitoring_graph();						
					})
				}
				else if (k === "nodes") {
					$(v).map(function(k2, v2) {

						v2['data']['type'] = window[v2['data']['type']];
						
						reload_monitoring_graph();
						
					})
				}
			});
		},
		error: function(e) {
		   console.log(e.message);
		}
	});
}


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

var graph_context_menu_node = {
    menuRadius: 100, // the radius of the circular menu in pixels
    selector: 'node', // elements matching this Cytoscape.js selector will trigger cxtmenus
    commands: [ // an array of commands to list in the menu
        {
            content: 'Monitor CPU' ,
            select: function(){
				add_monitor(this.id(), 0);
			}
        },
        {
            content: 'Monitor Memory' ,
            select: function(){
				add_monitor(this.id(), 1);
			}
        },
        {
            content: 'Monitor Throughput' ,
            select: function(){
				add_monitor(this.id(), 2);
			}
        },

/*        {
            content: 'Show Details' ,
            select: function(){ 
				var q = this.data().query;
				$('#query-dialog_query_id').val(this.data().id);
				$('#query-dialog_field').val(q.replace(/<br \/>/g, "\n"));
				d_query.dialog( "open" );
			}
        },
		*/
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

function reload_monitoring_graph() {
	cy_monitoring.load(graph_elements);
	reload_qtips_monitoring_graph();
}

function reload_qtips_monitoring_graph() {
	$('.qtip').remove();
	cy_monitoring.nodes().qtip({
				content: function(){ return format_highlight_query(this.data().query); },
				position: { my: 'top center', at: 'bottom center'},
				style: { 
					classes: 'qtip-bootstrap',
					tip: { width: 50, height: 20 }
				}
	});
}

function draw_monitoring_graph() {

	$('#monitoring_graph').cytoscape({
		layout: graph_layout,
		style: graph_style,
		elements: graph_elements,
		ready: function() { 
			cy_monitoring = this; 
			cy_monitoring.style()
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
				
			// trigger drawing of monitors
			draw_monitor_skeleton();			
		}
	});
	
	$('#monitoring_graph').cytoscapePanzoom( graph_pan_defaults );
	$('#monitoring_graph').cytoscapeCxtmenu( graph_context_menu_node );   
	
	// layout button
	$('.ui-cytoscape-panzoom').append('<div id="layout_button" class="ui-cytoscape-panzoom-zoom-button" style="top: 222px;"><span class="icon fa fa-random"></span></div>');
	$('#layout_button').bind( "click", function( event ) {
		cy_monitoring.layout();
	});
	
}

function format_query(query) {
//	.replace("Select", "<span style='color:red;'>Select</span>").replace("select", "<span style='color:red;'>select</span>").replace("SELECT", "<span style='color:red;'>SELECT</span>")
	return query
	.replace("From", "<br />From").replace("from", "<br />from").replace("FROM", "<br />FROM")
	.replace("Where", "<br />Where").replace("where", "<br />where").replace("WHERE", "<br />WHERE")
	.replace("Group by", "<br />Group by").replace("group by", "<br />group by").replace("GROUP BY", "<br />GROUP BY").replace("Group By", "<br />Group By")
	.replace("Having", "<br />Having").replace("having", "<br />having").replace("HAVING", "<br />HAVING")
	.replace("match_recognize", "<br />match_recognize").replace("measures", "<br />measures").replace("pattern", "<br />pattern")
	.replace("And", "<br />And").replace("and", "<br />and").replace("AND", "<br />AND")
	.replace("define", "<br />define").replace("after", "<br />after");
}

function format_highlight_query(query) {
	return query
	.replace("Select", "<span style='color:#2F1299;'>Select</span>").replace("select", "<span style='color:#2F1299;'>select</span>").replace("SELECT", "<span style='color:#2F1299;'>SELECT</span>")
	.replace("Where", "<span style='color:#2F1299;'>Where</span>").replace("where", "<span style='color:#2F1299;'>where</span>").replace("WHERE", "<span style='color:#2F1299;'>WHERE</span>")
	.replace("From", "<span style='color:#2F1299;'>From</span>").replace("from", "<span style='color:#2F1299;'>from</span>").replace("FROM", "<span style='color:#2F1299;'>FROM</span>")
	.replace("match_recognize", "<span style='color:#2F1299;'>match_recognize</span>")
	.replace("pattern", "<span style='color:#2F1299;'>pattern</span>")
	.replace("measures", "<span style='color:#2F1299;'>measures</span>")
	.replace("And", "<span style='color:#2F1299;'>And</span>").replace("and", "<span style='color:#2F1299;'>and</span>").replace("AND", "<span style='color:#2F1299;'>AND</span>")
	.replace("define", "<span style='color:#2F1299;'>define</span>")
	.replace("after", "<span style='color:#2F1299;'>after</span>");
}

function clean_query(query) {
	return query.replace(/\n/g, '');
}



