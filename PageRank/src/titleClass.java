

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.base.Optional;

import scala.Tuple2;

public class titleClass {

	private static final Pattern SPACES = Pattern.compile(",");
	public static void main(String[] args) throws Exception {
		


		SparkConf sparkConf = new SparkConf().setAppName("titleclass");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);


		JavaRDD<String> titles = ctx.textFile(args[0], 1);
		
		JavaRDD<String> nodes_with_page_ranks = ctx.textFile(args[1], 1);
		
		
		JavaPairRDD<String, Long>indexed_title=titles.zipWithIndex().
				mapValues(new Function<Long,Long>() {
					@Override
					public Long call(Long s) {
						return (long) s+1;
					}
				});;

				JavaPairRDD<String,String> indexed_titles_new = indexed_title.mapToPair(new PairFunction<Tuple2<String,Long>,String,String>() {
					public Tuple2<String,String> call(Tuple2<String ,Long > s) {
						String index=s._2.toString();
						return new Tuple2<String,String>(index,s._1);
					}


				});



				JavaPairRDD<String,String> links = nodes_with_page_ranks.mapToPair(new PairFunction<String,String,String>() {
					@Override
					public Tuple2<String,String> call(String s) {
						
						
					String p=s.replaceAll("[()]","").trim();
							
						String[] separates = SPACES.split(p.trim(),2);
							

						
						return new Tuple2<String,String>(separates[0],separates[1]);
					}
				});



			JavaRDD<Tuple2<String, String>> titles_with_page_rank = indexed_titles_new.join(links).values();
			
			JavaPairRDD<Double, String> ranks_final = titles_with_page_rank.mapToPair(new PairFunction<Tuple2<String,String>,Double,String>(){
				
				public Tuple2<Double,String> call (Tuple2<String,String> s)
				{
					double r=Double.valueOf(s._2);
					return new Tuple2<Double,String>(r,s._1);
					
				}
				
			}); 
			
			
			
			ranks_final= ranks_final.sortByKey(false,0);
		    
			
			
			ranks_final.saveAsTextFile("parsed_output_ideal_pagerank1"); 
			ctx.stop();
	}

}


