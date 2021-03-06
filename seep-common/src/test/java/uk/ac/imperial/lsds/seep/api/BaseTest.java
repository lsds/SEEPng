package uk.ac.imperial.lsds.seep.api;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;

public class BaseTest implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		// Declare Source
		LogicalOperator src = queryAPI.newStatelessSource(new CustomSource(), 0);
		// Declare processor
		LogicalOperator p = queryAPI.newStatelessOperator(new Processor(), 1);
		// Declare sink
		LogicalOperator snk = queryAPI.newStatelessSink(new CustomSink(), 2);
		
		Schema srcSchema = queryAPI.schemaBuilder.newField(Type.SHORT, "id").build();
		Schema pSchema = queryAPI.schemaBuilder.newField(Type.SHORT, "id").newField(Type.BYTES, "payload").build();
		
		/** Connect operators **/
		src.connectTo(p, 0, new DataStore(srcSchema, DataStoreType.NETWORK));
		p.connectTo(snk, 0, new DataStore(pSchema, DataStoreType.NETWORK));
		
//		queryAPI.setInitialPhysicalInstancesForLogicalOperator(1, 2);
		
		return QueryBuilder.build();
	}

	
	class CustomSource implements uk.ac.imperial.lsds.seep.api.operator.sources.Source {
		@Override
		public void setUp() {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processData(ITuple data, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processDataGroup(List<ITuple> dataList, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub	
		}
	}
	
	class Processor implements SeepTask{
		@Override
		public void setUp() {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processData(ITuple data, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processDataGroup(List<ITuple> dataList, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub	
		}
	}
	
	class CustomSink implements uk.ac.imperial.lsds.seep.api.operator.sinks.Sink {
		@Override
		public void setUp() {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processData(ITuple data, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processDataGroup(List<ITuple> dataList, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub	
		}
	}
}
