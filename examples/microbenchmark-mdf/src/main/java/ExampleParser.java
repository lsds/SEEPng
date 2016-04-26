import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.sun.xml.internal.bind.v2.schemagen.xmlschema.Schema;

import uk.ac.imperial.lsds.seep.api.data.SchemaParser;
import uk.ac.imperial.lsds.seep.api.data.Schema.DefaultParser;

private static class ExampleParser implements SchemaParser {
		private String encoding = Charset.defaultCharset().name();
		private static DefaultParser instance = null;
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();

		private ExampleParser(){}
		
		public static ExampleParser getInstance(){
			if(instance == null){
				instance = new ExampleParser();
			}
			return instance;
		}
		
		public byte[] bytesFromString(String textRecord) {
			String[] parts = textRecord.split(",");
			return OTuple.create(schema, schema.names(), parts);
		}
		
		public String stringFromBytes(byte[] binaryRecord) {
			ITuple data = new ITuple(schema, binaryRecord);
			return data.getInt("userId") + "," + data.getLong("value");
		}
		
		public String getCharsetName() {
			return encoding;
		}
		
		public void setCharset(String newencoding) {
			encoding = newencoding;
		}
	}



