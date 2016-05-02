/*******************************************************************************
 * Copyright (c) 2014 Imperial College London
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Raul Castro Fernandez - initial API and implementation
 ******************************************************************************/
package uk.ac.imperial.lsds.java2sdg.codegenerator;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewConstructor;
import javassist.NotFoundException;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.SDGNode;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.TaskElementRepr;
import uk.ac.imperial.lsds.java2sdg.bricks2.TaskElement;
import uk.ac.imperial.lsds.seep.api.data.Schema;

public class OperatorClassBuilder {

	private ClassPool cp = null;

	private static Logger LOG = LoggerFactory.getLogger(OperatorClassBuilder.class.getCanonicalName());

	public OperatorClassBuilder() {
		cp = ClassPool.getDefault();
		cp.importPackage("java.util");
		cp.importPackage("uk.ac.imperial.lsds.seep.api");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.API");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.data");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.data.Schema");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.operator");
		cp.importPackage("uk.ac.imperial.lsds.seep.api.operator.sinks");
	}

	/**
	 * Generates a new SEEP Base class Bytecode generated using Javassist
	 * 
	 * @param code
	 * @return CtClass
	 */
	public CtClass generateGenericBase(List<SDGNode> nodes) {
		CtClass cc = cp.makeClass("Base");
		CtClass[] implInterfaces = new CtClass[1];
		StringBuffer code = new StringBuffer();

		String schemaVariable = "schema";
		int count = 0;
		// Logical Operator Creation
		for (SDGNode node : nodes) {
			if (node.getTaskElements().values().iterator().next().isSouce()) {
				String currentSchema = SeepOperatorNewTemplate.getSchemaInstance(
						node.getTaskElements().values().iterator().next().getOutputSchema(),
						new String(schemaVariable + node.getId()));
				if (!currentSchema.toString().isEmpty())
					code.append(currentSchema.toString());
				code.append("LogicalOperator lo" + node.getId() + " = queryAPI.newStatelessSource(new Src(), " + count
						+ ");\n");
			} else if (node.getTaskElements().values().iterator().next().isSink()) {
				code.append("LogicalOperator lo" + node.getId() + " = queryAPI.newStatelessSink(new Snk(), " + count
						+ ");\n");
			} else {
				String currentSchema = SeepOperatorNewTemplate.getSchemaInstance(
						node.getTaskElements().values().iterator().next().getOutputSchema(),
						new String(schemaVariable + node.getId()));
				if (!currentSchema.toString().isEmpty())
					code.append(currentSchema.toString());

				if (node.getStateElement() == null) {
					code.append("LogicalOperator lo" + node.getId() + " = queryAPI.newStatelessOperator(new C_"
							+ node.getId() + "()," + count + ");\n");
				} else {
					LOG.error("Statefull Operator - Not implemented yet");
				}
			}
			count++;
		}
		// Logical Operator Creation
		for (SDGNode node : nodes) {
			if (!node.getTaskElements().values().iterator().next().getDownstreams().isEmpty()) {
				for (int downstream : node.getTaskElements().values().iterator().next().getDownstreams())
					code.append("lo" + node.getId() + ".connectTo( lo" + downstream + ", 0, new DataStore(schema"
							+ node.getId() + ", DataStoreType.NETWORK));\n");
			}
		}
		code.append("return QueryBuilder.build();\n");

		LOG.info("NEW Base Class \n {} \n\n", code);
		try {
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.QueryComposer");
			cc.setInterfaces(implInterfaces);

			CtMethod compose = OperatorMethodBuilder.genBaseCompose(cc, code.toString());
			cc.addMethod(compose);
		} catch (NotFoundException e) {
			LOG.error("Error generating Base class {} ", e.toString());
			e.printStackTrace();
		} catch (CannotCompileException e) {
			LOG.error("Error generating Base class {} ", e.toString());
			e.printStackTrace();
		}
		return cc;
	}

	/**
	 * Generates a new SEEP Stateless Processor class Bytecode generated using
	 * Javassist
	 * 
	 * @param opName,
	 *            code, schema
	 * @return CtClass
	 */

	public CtClass generateSingleStatelessProcessor(String opName, SDGNode node) {
		CtClass cc = cp.makeClass(opName);

		// Assuming its a single TE - Why not include schema in SDGNode??
		TaskElementRepr currentTE = node.getTaskElements().values().iterator().next();
		String processorSchema = SeepOperatorNewTemplate.getSchemaInstance(currentTE.getOutputSchema(), "schema");

		CtClass[] implInterfaces = new CtClass[1];
		try {
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.SeepTask");
			cc.setInterfaces(implInterfaces);

			CtConstructor cCon = CtNewConstructor.defaultConstructor(cc);
			cc.addConstructor(cCon);

			// Schema field - defined globally in the Processor class
			if (processorSchema != null && !processorSchema.isEmpty()) {
				CtField f = CtField.make(processorSchema, cc);
				cc.addField(f);
			}

			LOG.info("NEW Processor Class \n {} \n\n", node.getBuiltCode());
			CtMethod processDataSingle = OperatorMethodBuilder.genProcessorMethod(cc, node.getBuiltCode());
			cc.addMethod(processDataSingle);

			// Mandatory methods
			CtMethod setUp = OperatorMethodBuilder.genSetupMethod(cc, "");
			cc.addMethod(setUp);

			CtMethod close = OperatorMethodBuilder.genCloseMethod(cc, "");
			cc.addMethod(close);

			CtMethod processDataGroup = OperatorMethodBuilder.genProcessorGroupMethod(cc, "");
			cc.addMethod(processDataGroup);

		} catch (CannotCompileException e) {
			LOG.error("Error generating Stateless Processor class {} ", e.toString());
			e.printStackTrace();
		} catch (NotFoundException e) {
			LOG.error("Error generating Stateless Processor class {} ", e.toString());
			e.printStackTrace();
		}
		// cp.insertClassPath(new ClassClassPath(cc.getClass()));
		return cc;
	}

	public CtClass generateStatelessProcessor(String opName, String schema, String[] processorFields,
			String classMethod, String processorCode) {
		CtClass cc = cp.makeClass(opName);

		CtClass[] implInterfaces = new CtClass[1];
		try {
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.SeepTask");
			cc.setInterfaces(implInterfaces);

			CtConstructor cCon = CtNewConstructor.defaultConstructor(cc);
			cc.addConstructor(cCon);

			// Schema field - defined globally in the Processor class
			if (schema != null && !schema.isEmpty()) {
				CtField f = CtField.make(schema, cc);
				cc.addField(f);
			}
			// Global variables
			if (processorFields != null && processorCode.length() > 0) {
				for (String f : processorFields) {
					CtField cfield = CtField.make(f, cc);
					cc.addField(cfield);
				}
			}
			// Class methods
			if (classMethod != null && !classMethod.isEmpty()) {
				CtMethod cMethod = OperatorMethodBuilder.genClassMethod(cc, classMethod);
				cc.addMethod(cMethod);
			}

			CtMethod processDataSingle = OperatorMethodBuilder.genProcessorMethod(cc, processorCode);
			LOG.info("NEW Processor Class\n {} \n\n", processorCode);
			cc.addMethod(processDataSingle);

			// Mandatory methods
			CtMethod setUp = OperatorMethodBuilder.genSetupMethod(cc, "");
			cc.addMethod(setUp);

			CtMethod close = OperatorMethodBuilder.genCloseMethod(cc, "");
			cc.addMethod(close);

			CtMethod processDataGroup = OperatorMethodBuilder.genProcessorGroupMethod(cc, "");
			cc.addMethod(processDataGroup);

		} catch (CannotCompileException e) {
			LOG.error("Error generating Stateless Processor class {} ", e.toString());
			e.printStackTrace();
		} catch (NotFoundException e) {
			LOG.error("Error generating Stateless Processor class {} ", e.toString());
			e.printStackTrace();
		}
		// cp.insertClassPath(new ClassClassPath(cc.getClass()));
		return cc;
	}

	/**
	 * Generates a new SEEP Source class incrementing Periodically all the
	 * NUMBER fields Bytecode generated using Javassist
	 * 
	 * @param code,
	 *            schema
	 * @return CtClass
	 */
	public CtClass generatePeriodicSource(Schema schema) {
		String schemaVariable = "schema";

		String sourceSchema = SeepOperatorNewTemplate.getSchemaInstance(schema, schemaVariable);
		String[] sourceGlobalFields = new String[] { "private boolean working = true;" };
		String sourceExtraMethod = new String(
				"private void waitHere(int time){ try { Thread.sleep((long)time);} catch (InterruptedException e) {e.printStackTrace();}}");

		StringBuilder srcProcessCode = new StringBuilder();
		srcProcessCode.append(SeepOperatorNewTemplate.getSchemaVarsInit(schema) + "waitHere(2000);\n"
				+ "while(working){ byte[] d = OTuple.create(" + schemaVariable + ", new String[]{ "
				+ SeepOperatorNewTemplate.getSchemaNames(schema) + "}, " + " new Object[]{ "
				+ SeepOperatorNewTemplate.getSchemaVarsBoxed(schema) + "});\n");
		srcProcessCode.append("$2.send(d);\n");
		srcProcessCode.append(SeepOperatorNewTemplate.increaseSchemaVars(schema) + " }");

		CtClass cc = cp.makeClass("Src");

		CtClass[] implInterfaces = new CtClass[1];
		try {
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.operator.sources.Source");
			cc.setInterfaces(implInterfaces);

			CtConstructor cCon = CtNewConstructor.defaultConstructor(cc);
			cc.addConstructor(cCon);

			// Schema field - defined globally in the Processor class
			if (schema == null || sourceSchema.isEmpty()) {
				LOG.error("Source Schema cannot be null! ");
				System.exit(-1);
			} else {
				CtField f = CtField.make(sourceSchema, cc);
				cc.addField(f);
			}
			// Global variables
			if (sourceGlobalFields != null && sourceGlobalFields.length > 0) {
				for (String f : sourceGlobalFields) {
					CtField cfield = CtField.make(f, cc);
					cc.addField(cfield);
				}
			}
			// Class methods
			if (sourceExtraMethod != null && !sourceExtraMethod.isEmpty()) {
				CtMethod cMethod = OperatorMethodBuilder.genClassMethod(cc, sourceExtraMethod);
				cc.addMethod(cMethod);
			}
			LOG.info("NEW Source Class\n {} \n\n", srcProcessCode);
			CtMethod processDataSingle = OperatorMethodBuilder.genProcessorMethod(cc, srcProcessCode.toString());
			cc.addMethod(processDataSingle);

			// Mandatory methods
			CtMethod setUp = OperatorMethodBuilder.genSetupMethod(cc, "");
			cc.addMethod(setUp);

			CtMethod close = OperatorMethodBuilder.genCloseMethod(cc, "");
			cc.addMethod(close);

			CtMethod processDataGroup = OperatorMethodBuilder.genProcessorGroupMethod(cc, "");
			cc.addMethod(processDataGroup);

		} catch (CannotCompileException e) {
			LOG.error("Error generating Source class {} ", e.toString());
			e.printStackTrace();
		} catch (NotFoundException e) {
			LOG.error("Error generating Source class {} ", e.toString());
			e.printStackTrace();
		}
		return cc;
	}

	/**
	 * Generates a new Generic SEEP Source class Bytecode generated using
	 * Javassist
	 * 
	 * @param code,
	 *            schema
	 * @return CtClass
	 */
	public CtClass generateSource(String schema, String[] sourceFields, String sourceExtraMethod,
			String processorCode) {
		CtClass cc = cp.makeClass("Src");

		CtClass[] implInterfaces = new CtClass[1];
		try {
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.operator.sources.Source");
			cc.setInterfaces(implInterfaces);

			CtConstructor cCon = CtNewConstructor.defaultConstructor(cc);
			cc.addConstructor(cCon);

			// Schema field - defined globally in the Source class
			if (schema != null && !schema.isEmpty()) {
				CtField f = CtField.make(schema, cc);
				cc.addField(f);
			}
			// Global variables
			if (sourceFields != null && sourceFields.length > 0) {
				for (String f : sourceFields) {
					CtField cfield = CtField.make(f, cc);
					cc.addField(cfield);
				}
			}
			// Class methods
			if (sourceExtraMethod != null && !sourceExtraMethod.isEmpty()) {
				CtMethod cMethod = OperatorMethodBuilder.genClassMethod(cc, sourceExtraMethod);
				cc.addMethod(cMethod);
			}

			CtMethod processDataSingle = OperatorMethodBuilder.genProcessorMethod(cc, processorCode);
			LOG.info("NEW Source Class\n {} \n\n", processorCode);
			cc.addMethod(processDataSingle);

			// Mandatory methods
			CtMethod setUp = OperatorMethodBuilder.genSetupMethod(cc, "");
			cc.addMethod(setUp);

			CtMethod close = OperatorMethodBuilder.genCloseMethod(cc, "");
			cc.addMethod(close);

			CtMethod processDataGroup = OperatorMethodBuilder.genProcessorGroupMethod(cc, "");
			cc.addMethod(processDataGroup);

		} catch (CannotCompileException e) {
			LOG.error("Error generating Source class {} ", e.toString());
			e.printStackTrace();
		} catch (NotFoundException e) {
			LOG.error("Error generating Source class {} ", e.toString());
			e.printStackTrace();
		}
		return cc;
	}

	/**
	 * Generates a new SEEP Sink class periodically reporting throughput
	 * Bytecode generated using Javassist
	 * 
	 * @param code
	 * @return CtClass
	 */
	public CtClass generatePeriodicSink(Schema schema) {
		String schemaVariable = "schema";
		CtClass cc = cp.makeClass("Snk");

		String sinkSchema = SeepOperatorNewTemplate.getSchemaInstance(schema, schemaVariable);
		String[] sinkFields = new String[] { "int PERIOD = 1000;", "int count = 0;", "public long time;" };
		String sinkProcess = new String(
				"count++; if(System.currentTimeMillis() - time > PERIOD){ System.out.println(\"[Sink] e/s: \"+count); count = 0;time = System.currentTimeMillis();}");

		CtClass[] implInterfaces = new CtClass[1];
		try {
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.operator.sinks.Sink");
			cc.setInterfaces(implInterfaces);

			CtConstructor cCon = CtNewConstructor.defaultConstructor(cc);
			cc.addConstructor(cCon);

			// Schema field - defined globally in the Sink class
			if (schema != null && !sinkSchema.isEmpty()) {
				CtField f = CtField.make(sinkSchema, cc);
				cc.addField(f);
			}
			// Global variables
			if (sinkFields != null && sinkFields.length > 0) {
				for (String f : sinkFields) {
					CtField cfield = CtField.make(f, cc);
					cc.addField(cfield);
				}
			}

			CtMethod processDataSingle = OperatorMethodBuilder.genProcessorMethod(cc, sinkProcess);
			LOG.info("NEW Sink Class\n {} \n\n", sinkProcess);
			cc.addMethod(processDataSingle);

			// Mandatory methods
			CtMethod setUp = OperatorMethodBuilder.genSetupMethod(cc, "");
			cc.addMethod(setUp);

			CtMethod close = OperatorMethodBuilder.genCloseMethod(cc, "");
			cc.addMethod(close);

			CtMethod processDataGroup = OperatorMethodBuilder.genProcessorGroupMethod(cc, "");
			cc.addMethod(processDataGroup);

		} catch (CannotCompileException e) {
			LOG.error("Error generating Sink class {} ", e.toString());
			e.printStackTrace();
		} catch (NotFoundException e) {
			LOG.error("Error generating Sink class {} ", e.toString());
			e.printStackTrace();
		}
		// cp.insertClassPath(new ClassClassPath(cc.getClass()));
		return cc;
	}

	/**
	 * Generates a new SEEP Sink class Bytecode generated using Javassist
	 * 
	 * @param code
	 * @return CtClass
	 */
	public CtClass generateSink(String schema, String[] sinkFields, String sinkExtraMethod, String processorCode) {
		CtClass cc = cp.makeClass("Snk");

		CtClass[] implInterfaces = new CtClass[1];
		try {
			implInterfaces[0] = cp.get("uk.ac.imperial.lsds.seep.api.operator.sinks.Sink");
			cc.setInterfaces(implInterfaces);

			CtConstructor cCon = CtNewConstructor.defaultConstructor(cc);
			cc.addConstructor(cCon);

			// Schema field - defined globally in the Processor class
			if (schema != null && !schema.isEmpty()) {
				CtField f = CtField.make(schema, cc);
				cc.addField(f);
			}
			// Global variables
			if (sinkFields != null && sinkFields.length > 0) {
				for (String f : sinkFields) {
					CtField cfield = CtField.make(f, cc);
					cc.addField(cfield);
				}
			}
			// Class methods
			if (sinkExtraMethod != null && !sinkExtraMethod.isEmpty()) {
				CtMethod cMethod = OperatorMethodBuilder.genClassMethod(cc, sinkExtraMethod);
				cc.addMethod(cMethod);
			}

			CtMethod processDataSingle = OperatorMethodBuilder.genProcessorMethod(cc, processorCode);
			LOG.info("NEW Sink Class\n {} \n\n", processorCode);
			cc.addMethod(processDataSingle);

			// Mandatory methods
			CtMethod setUp = OperatorMethodBuilder.genSetupMethod(cc, "");
			cc.addMethod(setUp);

			CtMethod close = OperatorMethodBuilder.genCloseMethod(cc, "");
			cc.addMethod(close);

			CtMethod processDataGroup = OperatorMethodBuilder.genProcessorGroupMethod(cc, "");
			cc.addMethod(processDataGroup);

		} catch (CannotCompileException e) {
			LOG.error("Error generating Sink class {} ", e.toString());
			e.printStackTrace();
		} catch (NotFoundException e) {
			LOG.error("Error generating Sink class {} ", e.toString());
			e.printStackTrace();
		}
		// cp.insertClassPath(new ClassClassPath(cc.getClass()));
		return cc;
	}

}
