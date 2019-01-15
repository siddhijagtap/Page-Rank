
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.io.File;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import com.google.common.base.Optional;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;




import scala.Tuple2;

public class WikiBomb {

	public static class WikiBomb_new implements Serializable {
		private String title;
		private double rank;

		
		public double getRank() {
			return rank;
		}

		public void setRank(double rank) {
			this.rank = rank;
		}
		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title= title;
		}

	}


	private static String title = null;
	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: Java page rank file and Entity to search");
			System.exit(1);
		}

		String keyword=args[1];
		
		SparkConf sparkConf = new SparkConf().setAppName("WikiBomb");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(ctx);



		// Load a text file and convert each line to a Java Bean.
		JavaRDD<WikiBomb_new> lines= ctx.textFile(args[0]).map(
				new Function<String, WikiBomb_new>() {
					@Override
					public WikiBomb_new call(String line) {
						String s=line.replaceAll("[()]","").trim();
						String[] parts = s.split(",",2);
						
						WikiBomb_new wikibomb = new WikiBomb_new();
						wikibomb.setRank(Double.valueOf(parts[0].trim()));
						wikibomb.setTitle(parts[1].trim());
						return wikibomb;
					}
				});
		
		// Apply a schema to an RDD of Java Beans and register it as a table.
		DataFrame schemaPeople = sqlContext.createDataFrame(lines, WikiBomb_new.class);
		schemaPeople.registerTempTable("wikibomb");


		// SQL can be run over RDDs that have been registered as tables.
		DataFrame final_dataframe = sqlContext.sql("SELECT title FROM wikibomb WHERE title LIKE '%"+keyword+"%' ORDER BY rank DESC LIMIT 3");
		

		List<String> output = final_dataframe.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) {
				return "Title: " + row.getString(0);
			}
		}).collect();
		
		for (String title: output) {
			System.out.println(title);
		}
		
		
		ctx.stop();
	}
}

