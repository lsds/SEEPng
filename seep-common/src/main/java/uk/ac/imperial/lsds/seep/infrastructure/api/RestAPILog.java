package uk.ac.imperial.lsds.seep.infrastructure.api;

import org.eclipse.jetty.util.MultiMap;
import java.util.LinkedList;

import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;

public class RestAPILog implements RestAPIRegistryEntry {

    private LinkedList<String> entries;

    public RestAPILog() {
        this.entries = new LinkedList<String>();
    }
    
    public void addEntry(String newEntry) {
    	this.entries.add(newEntry);
    }

    @Override
    public Object getAnswer(MultiMap<String> reqParameters) {
        return this.entries;
    }

}

