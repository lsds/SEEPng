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
public enum Metric_Mode {

	CPU("cpu"),
	MEMORY("memory"),
	THROUGHPUT("throughput");

	private final String mode;

	Metric_Mode(String mode) {
		this.mode = mode;
	}

	public boolean equalsName(String otherMode) {
		return (otherMode == null) ? false : mode.equals(otherMode);
	}

	public String toString() {
		return this.mode;
	}

	public String ofMode() {
		return this.mode;
	}

	public static Metric_Mode toMode(String name) {
		if (name.compareToIgnoreCase("cpu") == 0)
			return CPU;
		if (name.compareToIgnoreCase("memory") ==0 )
			return MEMORY;
		if (name.compareToIgnoreCase("throughput") == 0 )
			return THROUGHPUT;
		return null;
	}

}
