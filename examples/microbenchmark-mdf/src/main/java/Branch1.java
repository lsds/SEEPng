import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;

import uk.ac.imperial.lsds.seep.api.data.CSVParser;

public class Branch1 implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();
	
	@Override
	public void processData(ITuple data, API api) {
		CSVParser inputParse = CSVParser.getInstance();
		inputParse.setSchema(schema);
		schema.SchemaParser(inputParse);
		int userId = data.getInt("userId");
		long value = data.getLong("value");
		
		value = value / value;
		
		byte[] processedData = OTuple.create(schema, new String[]{"userId", "value"},  new Object[]{userId, value});
		api.send(processedData);
	}

	@Override
	public void setUp() {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		// TODO Auto-generated method stub
		
	}

}
