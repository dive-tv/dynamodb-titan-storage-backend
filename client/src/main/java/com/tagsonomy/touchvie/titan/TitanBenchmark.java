package com.tagsonomy.touchvie.titan;

import com.thinkaurelius.titan.core.TitanGraph;

public class TitanBenchmark {

	public static void main(String[] args) {

		if (args.length < 2)
			printUsage();

		try {
			
			String mode = args[0];
			
			TitanUtil util = new TitanUtil(args[1]);
			TitanGraph graph = util.openGraph();
			
			if (mode.equals("-c")) {
				
				TitanGraphFactory.load(graph, Integer.MAX_VALUE, Integer.MAX_VALUE, true);
				
			} else if (mode.equals("-d")) {
				
				util.tearDownGraph(graph);

			} else if (mode.equals("-t")) {
				
				if (args.length < 3)
					printUsage();
				
				for (int i = 2; i < args.length; i++) 
					test(graph, args[i]);	
			} else {
				printUsage();
			}

		} catch (Exception ex) {
			System.err.println(ex);
			System.exit(-1);
		}

		System.exit(0);
	}

	public static void test(final TitanGraph graph, final String cardId) throws Exception {

		Long start = System.currentTimeMillis();
		
		Long count = graph.traversal().V().has("card_id", cardId).out().out().out().count().next();
		
		System.out.println(count + " vertices. Took " + (System.currentTimeMillis() - start) + "ms");

		//for (Object card : cards)
		//	System.out.println("  - " + (String) card);
	}
	
	private static void printUsage() {
		System.out.println("Usage: java TitanBenchmark [-c | -t | -d] <graph_config_path> (<card_id>) ...");
		System.out.println(" -c : create graph");
		System.out.println(" -t : test graph (mandatory card ID list)");
		System.out.println(" -d : delete graph");
		System.exit(-1);
	}
}
