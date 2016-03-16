import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;


public class Snk implements Sink {

	@Override
	public void processData(ITuple data, API api) {
		
		System.out.println("[Sink] Received data size: "+data.getData().length);
		
		//int param1 = data.getInt("param1");
		//int param2 = data.getInt("param2");
		String record = data.getString("record");
		
		//System.out.println("P1: "+param1+" P2: "+param2);
		System.out.println("[Sink] : "+record);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void setUp() {
		System.out.println("I am a Sink!!");
	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		// TODO Auto-generated method stub
		
	}

}
