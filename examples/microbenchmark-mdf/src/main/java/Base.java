import java.util.LinkedList;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.QueryExecutionMode;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.sources.SyntheticSource;



public class Base implements QueryComposer {
	int operatorId = 0;
	int connectionId = 0;

	@Override
	public SeepLogicalQuery compose() {
		LinkedList <Integer> fanout = new LinkedList <Integer>();
		LinkedList <Double> selectivity = new LinkedList <Double>();

		//TODO: populate fanout and selectivity properly.
		fanout.push(2);
		fanout.push(2);
		fanout.push(2);
		selectivity.push(2.0);
		selectivity.push(0.8);
		selectivity.push(0.8);
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();
		
		SyntheticSource synSrc = SyntheticSource.newSource(operatorId++, null);
		LogicalOperator adderOne = queryAPI.newStatelessOperator(new Adder(), operatorId++);
		synSrc.connectTo(adderOne, schema, connectionId++);
		
		
		LogicalOperator choose = expand(fanout, selectivity, 0, adderOne);
		
		LogicalOperator branchone = queryAPI.newStatelessOperator(new Branch1(), operatorId++);
		LogicalOperator snk = queryAPI.newStatelessSink(new Snk(), operatorId++);
		choose.connectTo(branchone, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
		branchone.connectTo(snk, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
		
		SeepLogicalQuery slq = queryAPI.build();
		slq.setExecutionModeHint(QueryExecutionMode.ALL_SCHEDULED);
		
		return slq;
	}

	public LogicalOperator expand (LinkedList <Integer> fanout, LinkedList <Double> selectivity, int location, LogicalOperator parent) {
		if (fanout.size() <= location || selectivity.size() <= location ||
				fanout.get(location) < 1 || selectivity.get(location) < 1) {
			LogicalOperator nullEval = queryAPI.newStatelessOperator(new Evaluator(), operatorId++);
			LogicalOperator nullReturn = queryAPI.newChooseOperator(new Choose(), operatorId++);
			parent.connectTo(nullEval, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
			nullEval.connectTo(nullReturn, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
			return nullreturn;
		}
		Integer thisfanout = fanout.get(location);
		Double thisselectivity = selectivity.get(location);
		LogicalOperator finalChoose = queryAPI.newChooseOperator(new Choose(), operatorId++);
		for (int x = 0; x < thisfanout; x++) {
			LogicalOperator child = queryAPI.newStatelessOperator(new Adder(thisselectivity), operatorId++);
			if (fanout.size() > location+1 && selectivity.size() > location+1) {
				LogicalOperator childChoose = expand(fanout, selectivity, location+1);
				childChoose.connectTo(finalChoose, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
			} else {
				LogicalOperator childEval = queryAPI.newStatelessOperator(new Evaluator(), operatorId++);
				child.connectTo(childEval, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
				childEval.connectTo(finalChoose, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
			}
		}
		return finalChoose;
	}
}
