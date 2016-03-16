import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.operator.sources.FileConfig;
import uk.ac.imperial.lsds.seep.api.operator.sources.FileSource;
import uk.ac.imperial.lsds.seep.api.operator.sinks.MarkerSink;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;


public class Base implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		
		//Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "param1").newField(Type.INT, "param2").build();
		Schema schema = SchemaBuilder.getInstance().newField(Type.STRING, "record").build();
		
		Properties pSrc = new Properties();
		pSrc.setProperty(FileConfig.FILE_PATH, "/home/wculhane/test.txt");
		pSrc.setProperty(FileConfig.TEXT_SOURCE, Boolean.TRUE.toString());
		pSrc.setProperty(FileConfig.SERDE_TYPE, new Integer(SerializerType.NONE.ofType()).toString());
		
		FileSource fileSource = FileSource.newSource(0, pSrc);
		LogicalOperator processor = queryAPI.newStatelessOperator(new Processor(), 1);
        MarkerSink sink = MarkerSink.newSink(2);
		
		fileSource.connectTo(processor, schema, 0);
		Properties pSnk = new Properties();
        pSnk.setProperty(FileConfig.FILE_PATH, "/home/wculhane/output.txt");
		pSnk.setProperty(FileConfig.TEXT_SOURCE, Boolean.TRUE.toString());
		processor.connectTo(sink, 0, new DataStore(schema, DataStoreType.FILE, pSnk));
		
		return queryAPI.build();
	}

}
