package uk.ac.imperial.lsds.seep.api.data;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.seep.errors.SchemaException;

public class ZCITuple extends ITuple {
	
	private ByteBuffer ptr;
	private int bufferPtrPosition;
	
	public ZCITuple(Schema schema) {
		super(schema);
	}
	
	public void assignBuffer(ByteBuffer ptr) {
		this.ptr = ptr;
	}
	
	public void setBufferPtr(int newPosition) {
		this.bufferPtrPosition = newPosition;
		this.populateOffsets();
	}
	
	public int getInt(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.INT)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.INT+"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getInt();
	}
	
	public int getInt(int idx) {
		int offset = mapIdxToOffset[idx];
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getInt();
	}
	
	public long getLong(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.LONG)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.LONG +"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getLong();
	}
	
	public long getLong(int idx) {
		int offset = mapIdxToOffset[idx];
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getLong();
	}
	
	public float getFloat(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.FLOAT)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.FLOAT +"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getFloat();
	}
	
	public float getFloat(int idx) {
		int offset = mapIdxToOffset[idx];
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getFloat();
	}
	
	public double getDouble(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.DOUBLE)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.DOUBLE +"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getDouble();
	}
	
	public double getDouble(int idx) {
		int offset = mapIdxToOffset[idx];
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getDouble();
	}
	 
	public String getString(String fieldName) {
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.STRING)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.INT+"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return (String)Type.STRING.read(ptr);
	}
	 
	public String getString(int idx) {
		int ptrPosition = bufferPtrPosition + idx;
		ptr.position(ptrPosition);
		return (String)Type.STRING.read(ptr);
	}
	
	public Object get(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if (!mapFieldToOffset.containsKey(fieldName)) {
			this.populateOffsets();
		}
		Type t = schema.getField(fieldName);
		if(t.equals(Type.BYTE)){
			return this.getByte(fieldName);
		} else if(t.equals(Type.INT)){
			return this.getInt(fieldName);
		} else if(t.equals(Type.SHORT)){
			return this.getShort(fieldName);
		} else if(t.equals(Type.LONG)){
			return this.getLong(fieldName);
		} else if(t.equals(Type.STRING)){
			return this.getString(fieldName);
		} else if(t.equals(Type.FLOAT)){
			return this.getFloat(fieldName);
		} else if(t.equals(Type.DOUBLE)){
			return this.getDouble(fieldName);
		}
		return null;
	}

	private void populateOffsets(){
		Type[] fields = schema.fields();
		String[] names = schema.names();
		ptr.position(bufferPtrPosition);
		for(int i = 0; i < fields.length; i++){
			Type t = fields[i];
			mapFieldToOffset.put(names[i], ptr.position() - bufferPtrPosition);
			mapFieldToIdx.put(names[i], i); // assign idx in order
			mapIdxToOffset[i] = ptr.position() - bufferPtrPosition; // store offset in idx
			if(! t.isVariableSize()){
				// if not variable we just get the size of the Type
				ptr.position(ptr.position() + t.sizeOf(null));
			}
			else {
				// if variable we need to read the size from the current offset
				int currpos = ptr.position();
				ptr.position(currpos + ptr.getInt() + Integer.BYTES);
			}
		}
	}
}
