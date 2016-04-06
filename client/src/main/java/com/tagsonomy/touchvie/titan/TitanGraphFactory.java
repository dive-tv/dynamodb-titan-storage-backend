/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.tagsonomy.touchvie.titan;

import java.io.FileReader;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.util.stats.MetricManager;

import au.com.bytecode.opencsv.CSVReader;

public class TitanGraphFactory {

	private static enum RelType {
		PLAYS,
		WEARS,
		CONTAINS,
		APPEARS_IN,
		CASTING;

		public static RelType fromId(Integer id) {
			for (RelType type : RelType.values())
				if (type.ordinal() == id)
					return type;
			throw new IllegalArgumentException("Unknown ID " + id);
		}
	};

	private static final int BATCH_SIZE = 10;
	private static final Logger LOG = LoggerFactory.getLogger(TitanGraphFactory.class);
	private static final String CARD = "card";
	private static final String CARD_ID = "card_id";
	private static final String CARD_TYPE = "type";

	public static final MetricRegistry REGISTRY = MetricManager.INSTANCE.getRegistry();
	public static final ConsoleReporter REPORTER = ConsoleReporter.forRegistry(REGISTRY).build();

	private static final String TIMER_LINE = "TouchvieTestFactory.line";
	private static final String TIMER_CREATE = "TouchvieTestFactory.create_";
	private static final String COUNTER_GET = "TouchvieTestFactory.get_";
	private static final AtomicInteger COMPLETED_TASK_COUNT = new AtomicInteger(0);
	private static final int POOL_SIZE = 10;

	public static void load(final TitanGraph graph, final int nodesToLoad, final int edgesToLoad, final boolean report) throws Exception {

		TitanManagement mgmt = graph.openManagement();
		if (mgmt.getGraphIndex(CARD_ID) == null) {
			
			final PropertyKey idKey = mgmt.makePropertyKey(CARD_ID).dataType(String.class).make();
			mgmt.buildIndex(CARD_ID, Vertex.class).addKey(idKey).unique().buildCompositeIndex();
		}
		
		if (mgmt.getGraphIndex(CARD_TYPE) == null) {
			final PropertyKey typeKey = mgmt.makePropertyKey(CARD_TYPE).dataType(Integer.class).make();
			mgmt.buildIndex(CARD_TYPE, Vertex.class).addKey(typeKey).buildCompositeIndex();
		}
		
		for (RelType rel : RelType.values())
			mgmt.makeEdgeLabel(rel.name()).multiplicity(Multiplicity.MULTI).make();

		mgmt.commit();

		// load cards
		int line = 0;
		BlockingQueue<Runnable> creationQueue = new LinkedBlockingQueue<>();
		try (CSVReader reader = new CSVReader(new FileReader("/tmp/card_graph/nodes.csv"), ',')) {
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null && line < nodesToLoad) {
				line++;

				String cardId = nextLine[0];
				Integer cardType = 0;
				try {
					cardType = Integer.parseInt(nextLine[1]);
				} catch (Exception ex) {
					System.err.println("Error parsing int: " + nextLine[1]);
					LOG.error("error loading card {} {}", ex.getMessage(),
							ExceptionUtils.getRootCause(ex).getMessage());
				}
				creationQueue.add(new CardCreationCommand(graph, cardId, cardType));
			}
		}

		// load relationships
		BlockingQueue<Runnable> relQueue = new LinkedBlockingQueue<>();
		line = 0;
		try (CSVReader reader = new CSVReader(new FileReader("/tmp/card_graph/edges.csv"), ',')) {
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null && line < edgesToLoad) {
				line++;

				String srcId = nextLine[0];
				String dstId = nextLine[1];
				RelType relType = RelType.PLAYS;
				try {
					relType = RelType.fromId(Integer.parseInt(nextLine[2]));
				} catch (Exception ex) {
					System.err.println("Error parsing type: " + nextLine[2]);
					LOG.error("error loading card {} {}", ex.getMessage(),
							ExceptionUtils.getRootCause(ex).getMessage());
				}

				RelationshipCommand relCommand = new RelationshipCommand(graph,
						new Relationship(srcId, dstId, relType));
				relQueue.add(relCommand);
			}
		}

		System.out.println("Loading " + creationQueue.size() + " cards...");
		
		ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);
		for (int i = 0; i < POOL_SIZE; i++) {
			executor.execute(new BatchCommand(graph, creationQueue));
		}
		executor.shutdown();
		while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
			LOG.info("Awaiting:" + creationQueue.size());
			if (report) {
				REPORTER.report();
			}
		}

		System.out.println("Loading " + relQueue.size() + " relationships...");
		
		executor = Executors.newSingleThreadExecutor();
		executor.execute(new BatchCommand(graph, relQueue));

		executor.shutdown();
		while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
			LOG.info("Awaiting:" + relQueue.size());
			if (report) {
				REPORTER.report();
			}
		}
		
		System.out.println("Finished");
		LOG.info("TouchvieTestFactory.load complete");
	}

	public static class Relationship {
		final String srcId;
		final String dstId;
		final RelType relType;

		public Relationship(String srcId, String dstId, RelType relType) {
			this.srcId = srcId;
			this.dstId = dstId;
			this.relType = relType;
		}

		public String getSrcId() {
			return srcId;
		}

		public String getDstId() {
			return dstId;
		}

		public RelType getRelType() {
			return relType;
		}

		@Override
		public String toString() {
			return "Relationship [srcId=" + srcId + ", dstId=" + dstId + ", relType=" + relType + "]";
		}
	}

	public static class BatchCommand implements Runnable {
		final TitanGraph graph;
		final BlockingQueue<Runnable> commands;

		public BatchCommand(TitanGraph graph, BlockingQueue<Runnable> commands) {
			this.commands = commands;
			this.graph = graph;
		}

		@Override
		public void run() {
			int i = 0;
			Runnable command = null;
			while ((command = commands.poll()) != null) {
				try {
					command.run();
				} catch (Throwable e) {
					Throwable rootCause = ExceptionUtils.getRootCause(e);
					String rootCauseMessage = null == rootCause ? "" : rootCause.getMessage();
					LOG.error("Error processing card {} {}", e.getMessage(), rootCauseMessage, e);
				}
				if (i++ % BATCH_SIZE == 0) {
					try {
						graph.tx().commit();
					} catch (Throwable e) {
						LOG.error("error processing commit {} {}", e.getMessage(),
								ExceptionUtils.getRootCause(e).getMessage());
					}
				}

			}
			try {
				graph.tx().commit();
			} catch (Throwable e) {
				LOG.error("error processing commit {} {}", e.getMessage(), ExceptionUtils.getRootCause(e).getMessage());
			}
		}

	}

	public static class CardCreationCommand implements Runnable {
		final String cardId;
		final Integer cardType;
		final TitanGraph graph;

		public CardCreationCommand(final TitanGraph graph, final String cardId, Integer cardType) {
			this.cardId = cardId;
			this.cardType = cardType;
			this.graph = graph;
		}

		@Override
		public void run() {
			createCard(graph, cardId, cardType);
		}

		private static Vertex createCard(TitanGraph graph, String cardId, Integer cardType) {
			long start = System.currentTimeMillis();

			Vertex vertex = graph.addVertex();
			vertex.property(CARD_ID, cardId);
			vertex.property(CARD_TYPE, cardType);
			REGISTRY.counter(COUNTER_GET + CARD).inc();
			long end = System.currentTimeMillis();
			long time = end - start;
			REGISTRY.timer(TIMER_CREATE + CARD).update(time, TimeUnit.MILLISECONDS);
			return vertex;
		}
	}

	public static class RelationshipCommand implements Runnable {
		final TitanGraph graph;
		final Relationship rel;

		public RelationshipCommand(final TitanGraph graph, Relationship rel) {
			this.graph = graph;
			this.rel = rel;
		}

		@Override
		public void run() {
			try {
				processLine(graph, rel);
			} catch (Throwable e) {
				Throwable rootCause = ExceptionUtils.getRootCause(e);
				String rootCauseMessage = null == rootCause ? "" : rootCause.getMessage();
				LOG.error("Error processing line {} {}", e.getMessage(), rootCauseMessage, e);
			} finally {
				COMPLETED_TASK_COUNT.incrementAndGet();
			}
		}

	}

	protected static void processLine(final TitanGraph graph, Relationship rel) {
		long start = System.currentTimeMillis();
		process(graph, rel);
		long end = System.currentTimeMillis();
		long time = end - start;
		REGISTRY.timer(TIMER_LINE).update(time, TimeUnit.MILLISECONDS);
	}

	private static void process(TitanGraph graph, Relationship rel) {

		Vertex srcCardVertex = get(graph, CARD_ID, rel.getSrcId());
		Vertex dstCardVertex = get(graph, CARD_ID, rel.getDstId());
		if (srcCardVertex == null) {
			REGISTRY.counter("error.missingCard.src").inc();
			System.err.println("Src card not found: " + rel);
		} else if (dstCardVertex == null) {
			REGISTRY.counter("error.missingCard.dst").inc();
			System.err.println("Dst card not found: " + rel);
		} else {
			srcCardVertex.addEdge(rel.getRelType().name(), dstCardVertex);
			REGISTRY.counter("edges.loaded").inc();
		}
	}

	private static Vertex get(TitanGraph graph, String key, String value) {
		final GraphTraversalSource g = graph.traversal();
		final Iterator<Vertex> it = g.V().has(key, value);
		return it.hasNext() ? it.next() : null;
	}
}
