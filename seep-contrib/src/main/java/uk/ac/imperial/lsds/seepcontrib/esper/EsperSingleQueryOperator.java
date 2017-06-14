package uk.ac.imperial.lsds.seepcontrib.esper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.espertech.esper.collection.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import uk.ac.imperial.lsds.seep.infrastructure.api.RestAPIManager;

public class EsperSingleQueryOperator implements SeepTask {

	private static final long serialVersionUID = 1L;

	private final static Logger log = LoggerFactory.getLogger(EsperSingleQueryOperator.class);

	public final static String STREAM_IDENTIFIER = "stream";

	private static Integer gcount = new Integer(0);
	private static Integer bcount = new Integer(0);
	private static Integer scount = new Integer(0);

	// This map contains a list of key,class mappings, which will be
	// registered to the esper engine
	private Map<String,Map<String, Object>> typesPerStream = new LinkedHashMap<String, Map<String,Object>>();

	/*
	 * URL of the ESPER engine instance
	 */
	private String esperEngineURL = "";

	/*
	 * The ESPER engine instance used by this processor, will be fetched based
	 * on the given URL
	 */
	private EPServiceProvider epService = null;

	/*
	 * The actual ESPER query as String
	 */
	private String esperQuery = "";

	private String name = "";

	/*
	 * The query as a statement, built from the query string
	 */
	private EPStatement statement = null;

	private boolean enableLoggingOfMatches = true;
	private List<Pair<Long,OTuple>> matchCache;

	private Queue<ITuple> initCache;
	private boolean initialised = false;

	private API api = null;

	public EsperSingleQueryOperator() {		
	}

	public EsperSingleQueryOperator(String query, String url, String name) {
		this.esperQuery = query;
		this.esperEngineURL = url;
		this.name = name;
		if (enableLoggingOfMatches) {
			this.matchCache = new ArrayList<Pair<Long,OTuple>>();
			//new LinkedList<Pair<Long,OTuple>>();
			//this.matchCache = Collections.synchronizedList(new ArrayList<Pair<Long,OTuple>>());
		}
		this.initCache = new LinkedList<ITuple>();
	}

	public EsperSingleQueryOperator(String query, String url, String streamKey, String name, String[] typeBinding) {
		this(query, url, name);
		this.typesPerStream.put(streamKey, getTypes(typeBinding));
	}

	public EsperSingleQueryOperator(String query, String url, String name, Map<String, String[]> typeBinding) {
		this(query, url, name);
		for (String stream : typeBinding.keySet())
			this.typesPerStream.put(stream, getTypes(typeBinding.get(stream)));
	}

	public void initStatement() {

		if (statement != null) {
			statement.removeAllListeners();
			statement.destroy();
		}

		log.debug("Creating ESPER query...");

		/*
		 * Build the ESPER statement
		 */
		statement = epService.getEPAdministrator().createEPL(this.esperQuery);

		/*
		 * Set a listener called when statement matches
		 */
		statement.addListener(new UpdateListener() {
			@Override
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				if (newEvents == null) {
					// we don't care about events leaving the window (old
					// events)
					return;
				}
				for (EventBean theEvent : newEvents) {
					sendOutput(theEvent);
				}
			}
		});

		initialised = true;
		log.debug("Done with init: {}", this.esperQuery);
	}


	@Override
	public void setUp() {
		/*
		 * Init data structures
		 */
		Configuration configuration = new Configuration();
		configuration.getEngineDefaults().getThreading()
				.setInternalTimerEnabled(false);

		// The data types for the data items
		//
		if (this.typesPerStream != null) {
			for (String stream : this.typesPerStream.keySet()) {
				Map<String, Object> currentTypes = this.typesPerStream.get(stream);
				log.debug("Registering data items as '{}' in esper queries...", stream);
				for (String key : currentTypes.keySet()) {
					Class<?> clazz = (Class<?>) currentTypes.get(key);
					log.debug("  * registering type '{}' for key '{}'",
							clazz.getName(), key);
				}
				configuration.addEventType(stream, currentTypes);
				log.debug("{} attributes registered.", currentTypes.size());
			}
		}

		/*
		 * Get the ESPER engine instance
		 */
		epService = EPServiceProviderManager.getProvider(esperEngineURL,
				configuration);

		/*
		 * Initialise the query statement
		 */
		initStatement();

		/*
		 * Register rest API handler
		 */
		RestAPIManager.restAPIRegistry.put("/query", new RestAPIEsperGetQueryDesc(this));
		RestAPIManager.restAPIRegistry.put("/matches", new RestAPIEsperGetMatches(this));
		RestAPIManager.restAPIRegistry.put("/query_update", new RestAPIEsperPostQueryUpdate(this));

	}

	@Override
	public void processData(ITuple data, API api) {
		if (data == null || data.getData() == null || api == null) {
			log.debug("Bad {} tuple received", (++bcount));
			return;
		}
		this.api = api;

		log.debug("Processing tuple {}", (++gcount));
		log.debug("Received input tuple {}", data.toString());
		log.debug("Map of received input tuple {}", data.getSchema().toString());
		if (!initialised) {
			this.initCache.add(data);
			setUp();
		}
		else {
			while (!this.initCache.isEmpty()) {
				sendData(this.initCache.poll());
			}
			sendData(data);
		}
	}

	@Override
	public void processDataGroup(List<ITuple> d, API api) {
		for (ITuple tuple : d)
			processData(tuple, api);
	}

	@Override
	public void close() {

	}

	protected void sendOutput(EventBean out) {
		log.debug("Query returned a new result event: {}", out);

		//DataTuple output = new DataTuple(api.getDataMapper(), new TuplePayload());
		List<Object> objects = new ArrayList<>();
		
		//System.out.print("outtuple:");
		for (String key : out.getEventType().getPropertyNames()) {
			Object value = out.get(key);
			if (value == null)
				continue;
			objects.add(value);
			//System.out.print(key + ":" + getType(value) + ":" + value + ",");
			Schema.SchemaBuilder.getInstance().newField(getType(value), key);
		} 
		//System.out.println("  ");

		Schema schema = Schema.SchemaBuilder.getInstance().build();
		OTuple outTuple = new OTuple(schema);
		outTuple.setValues(objects.toArray());
		Long otuple_payload_ts = System.currentTimeMillis();

		log.debug("At {}, sending output {}", otuple_payload_ts, schema.names());

		if (this.enableLoggingOfMatches) {
			long cutOffTime = System.currentTimeMillis() - 1000*60*20;
			synchronized (matchCache) {

				matchCache.add(new Pair<Long, OTuple>(otuple_payload_ts, outTuple));

				// Remove old items
				Iterator<Pair<Long, OTuple>> iter = matchCache.iterator();
				boolean run = true;
				while (iter.hasNext() && run) {
					Pair<Long, OTuple> current_pair = iter.next();
					Long t_payload_timestamp = current_pair.getFirst();
					if (t_payload_timestamp < cutOffTime) {
						iter.remove();
					}
					else {
						run = false;
					}
				}
			}
		}

		log.debug("Match cache size: {}", this.matchCache.size());

		log.debug("Sent tuples: {}", (++scount));
		api.send(outTuple);
//		api.send(OTuple.create(schema, out.getEventType().getPropertyNames(), objects.toArray()));;
	}

	public void sendData(ITuple input) {
		String stream = input.getString(STREAM_IDENTIFIER);
		/*if (stream.equals("string")) {
			stream = "input";
		}*/

		Map<String, Object> item = new LinkedHashMap<String, Object>();

		// only previously defined types are available to esper.
		//if (this.typesPerStream.containsKey(stream)) {
			for (String key : this.typesPerStream.get(stream).keySet())
				item.put(key, input.get(key));
			
			log.debug("Sending item {} with name '{}' to esper engine", item,
					stream);

			epService.getEPRuntime().sendEvent(item, stream);
		/*} else {
			System.out.println("ERROR: " + stream);
			for (String key : this.typesPerStream.keySet())
				System.out.println(key);
		}*/

	}

	public Map<String, Object> getTypes(String[] types) {
		Map<String, Object> result = new LinkedHashMap<>();
		for (String def : types) {
			int idx = def.indexOf(":");
			if (idx > 0) {
				String key = def.substring(0, idx);
				String type = def.substring(idx + 1);

				Class<?> clazz = classForName(type);
				if (clazz != null) {
					log.debug("Defining type class '{}' for key '{}'", key,
							clazz);
					result.put(key, clazz);
				} else {
					log.error("Failed to locate class for type '{}'!", type);
				}
			}
		}
		return result;
	}

	protected static Class<?> classForName(String name) {
		//
		// the default packages to look for classes...
		//
		String[] pkgs = new String[] { "", "java.lang" };

		for (String pkg : pkgs) {
			String className = name;
			if (!pkg.isEmpty())
				className = pkg + "." + name;

			try {
				Class<?> clazz = Class.forName(className);
				if (clazz != null)
					return clazz;
			} catch (Exception e) {
			}
		}
		return null;
	}

	public Map<String, Map<String, Object>> getTypesPerStream() {
		return typesPerStream;
	}

	public String getEsperEngineURL() {
		return esperEngineURL;
	}

	public String getEsperQuery() {
		return esperQuery;
	}

	public void initWithNewEsperQuery(String query) {
		log.debug("init with new esper query: {}", query);
		initialised = false;
		this.esperQuery = query;
		RestAPIManager.restAPIRegistry.put("/query", new RestAPIEsperGetQueryDesc(this));
		initStatement();
	}

	private Type getType (Object o) {
		Type data_type;

		if (o instanceof Byte){
			data_type = Type.BYTE;
		}
		else if (o instanceof Integer){
			data_type = Type.INT;
		}
		else if (o instanceof Long){
			data_type = Type.LONG;
		}
		else if (o instanceof String){
			data_type = Type.STRING;
		}
		else if (o instanceof Float){
			data_type = Type.FLOAT;
		}
		else if (o instanceof Double){
			data_type = Type.DOUBLE;
		}
		else if (o instanceof Short){
			data_type = Type.SHORT;
		}
		else if (o instanceof byte[]){
			data_type = Type.BYTES;
		}
		else {
			data_type = Type.INT;
		}

		return data_type;
	}

	public String getName() {
		return name;
	}

	public boolean isEnableLoggingOfMatches() {
		return enableLoggingOfMatches;
	}

	public List<Pair<Long,OTuple>> getMatchCache() {
		return this.matchCache;
	}

}