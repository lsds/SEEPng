package uk.ac.imperial.lsds.seepmaster.ui.web;
/*******************************************************************************
 * Copyright (c) 2016 Imperial College London
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Panagiotis Garefalakis (pg1712@imperial.ac.uk)
 ******************************************************************************/

import java.util.Map;

import org.eclipse.jetty.util.MultiMap;

public interface RestAPIRegistryEntry {
	
	/** Used for GET requests **/
	public Object getAnswer(MultiMap<String> reqParameters);
	/** Used for POST requests **/
	public Object getPostAnswer(Map<String, String[]> reqParameters);
}
