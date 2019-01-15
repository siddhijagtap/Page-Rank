/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */




import scala.Tuple2;

//import com.google.common.collect.Iterables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.CollectionsUtils;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 */
public final class A1PageRank {

	private static final Pattern SPACES = Pattern.compile("\\s+");
	private static final Pattern SEMICOLONS = Pattern.compile(":");


	static void showWarning() {
		String warning = "WARN: This is a naive implementation of PageRank " +
				"and is given as an example! \n" +
				"Please use the PageRank implementation found in " +
				"org.apache.spark.graphx.lib.PageRank for more conventional use.";
		System.err.println(warning);
	}

	private static class Sum implements Function2<Double, Double, Double> {
		@Override
		public Double call(Double a, Double b) {
			return a + b;
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
			System.exit(1);
		}

		showWarning();

		SparkConf sparkConf = new SparkConf().setAppName("JavaPageRank");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		// Loads in input file. It should be in format of:
		//     URL         neighbor URL
		//     URL         neighbor URL
		//     URL         neighbor URL
		//     ...
		JavaRDD<String> lines = ctx.textFile(args[0], 1);

		// Loads all URLs from input file and initialize their neighbors.
		JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) {
				String[] parts = SEMICOLONS.split(s);


				return new Tuple2<String, String>(parts[0], parts[1]);
			}
		}).distinct().groupByKey().cache();

		double node_count=lines.count();
		// Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
		JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
			@Override
			public Double call(Iterable<String> rs) {
				return 1.0/node_count;

			}
		});

		/// Calculates and updates URL ranks continuously using PageRank algorithm.
		for (int current = 0; current < Integer.parseInt(args[1]); current++) {
			// Calculates URL contributions to the rank of other URLs.
			JavaPairRDD<String, Double> contribs = links.join(ranks).values()
					.flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
						@Override
						public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {

							String[] new_parts = SPACES.split((s._1.toString().replaceAll("[^0-9\\s+]","")));
							int count=new_parts.length;
							List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
							for(int i=0;i<count;i++){
								results.add(new Tuple2<String, Double>(new_parts[i], s._2() / count));
							}

							return results;

						}
					});

			// Re-calculates URL ranks based on neighbor contributions.
			ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {


				@Override
				public Double call(Double sum) {
					return sum;
				}
			});
		}


		
		 JavaPairRDD<Double,String> ranks_new = ranks.sortByKey().mapToPair(new PairFunction<Tuple2<String,Double>,Double,String>() {
				public Tuple2<Double,String> call(Tuple2<String ,Double> s) {
					return new Tuple2<Double,String>(s._2,s._1);
				}


			});
		    
		    
		    ranks_new=ranks_new.sortByKey(false, 0);
		    
		    JavaPairRDD<String,Double> ranks_final= ranks_new.mapToPair(new PairFunction<Tuple2<Double,String>,String,Double>() {
				public Tuple2<String,Double> call(Tuple2<Double,String> s) {
					return new Tuple2<String,Double>(s._2,s._1);
				}


			});
		    
		ranks_final.saveAsTextFile("outputfilepagerank1");
		ctx.stop();
	}
}

