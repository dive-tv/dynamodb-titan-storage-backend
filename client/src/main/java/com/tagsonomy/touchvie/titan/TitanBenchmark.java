package com.tagsonomy.touchvie.titan;

import java.util.List;

import com.thinkaurelius.titan.core.TitanGraph;

public class TitanBenchmark {

	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("Usage: java TitanBenchmark <graph_config_path> <card_id> ...");
			System.exit(-1);
		}

		try {

			TitanUtil util = new TitanUtil(args[0]);
			TitanGraph graph = util.openGraph();
			for (int i = 1; i < args.length; i++) 
				test(graph, args[i]);

		} catch (Exception ex) {
			System.err.println(ex);
			System.exit(-1);
		}

		System.exit(0);
	}

	public static void test(final TitanGraph graph, final String cardId) throws Exception {

		Long start = System.currentTimeMillis();
		// List<String> cards = graph
		List<Object> cards = graph.traversal().V().has("card_id", cardId).out().out().values("card_id").toList();
		// .toStream()
		// .map(o -> o.toString())
		// .collect(Collectors.toList());
		System.out.println("Took " + (System.currentTimeMillis() - start) + "ms");

		for (Object card : cards)
			System.out.println("  - " + (String) card);
	}
}
