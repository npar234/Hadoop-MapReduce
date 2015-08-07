

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.*;
import java.io.*;



public class MapReduce {
	public static class Map extends Mapper<LongWritable ,Text, Text, FloatWritable>{
		private Text outText = new Text();
		/* matchMap is a hashmap used find a movie's genre from its movieid
		 * movieMap is a hashmap a genre and a (genre, rating) pair
		 */
		private HashMap<String, FloatWritable> movieMap = new HashMap<String, FloatWritable>();
		private HashMap<Integer, String> matchMap = new HashMap<Integer, String>();
		static int counter = 0;
		static int counter2 = 0;
		static int counter3 = 0;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			counter++;
			/* splits the line and stores each column into an array of strings */
			String line = value.toString();
			String[] columnValues = line.split("::");
			
			/* obtains movieid and rating from their respective columns */
			int movieid = Integer.parseInt(columnValues[1]);
			float rating = Float.parseFloat(columnValues[2]);
			
			/* obtains appropriate genre for the given movieid */
			String genre = matchMap.get(new Integer(movieid));
			String genreValues[] = genre.split("\\|");

		  counter3+=genreValues.length;
			for(int i = 0; i < genreValues.length; i++){
				System.out.print("Key: "+ genreValues[i] + " Value: "+ rating + "  ");
								counter2++;
				if(genreValues[i].equals("Action")){
				  String temper = "Action";
				  outText.set(temper);
				  context.write(outText, new FloatWritable(rating));
				}
				else if(genreValues[i].equals("Adventure")){
				  String temper = "Adventure";
				  outText.set(temper);
				  context.write(outText, new FloatWritable(rating));
				}
				else if(genreValues[i].equals("Animation")){
				  String temper = "Animation";
				  outText.set(temper);
				  context.write(outText, new FloatWritable(rating));
				}
				else if(genreValues[i].equals("Children's")){
				  String temper = "Children's";
				  outText.set(temper);
				  context.write(outText, new FloatWritable(rating));
				}
				else if(genreValues[i].equals("Comedy")){
				  String temper = "Comedy";
				  outText.set(temper);
				  context.write(outText, new FloatWritable(rating));
				}
				else if(genreValues[i].equals("Crime")){
				  String temper = "Crime";
				  outText.set(temper);
				  context.write(outText, new FloatWritable(rating));
				}
				else if(genreValues[i].equals("Documentary")){
				  String temper = "Documentary";
				  outText.set(temper);
				  context.write(outText, new FloatWritable(rating));
				}
				else if(genreValues[i].equals("Drama")){
				  String temper = "Drama";
				  outText.set(temper);
				  context.write(outText, new FloatWritable(rating));
				}
				else if(genreValues[i].equals("Fantasy")){
					  String temper = "Fantasy";
					  outText.set(temper);
					  context.write(outText, new FloatWritable(rating));
				}
				else if(genreValues[i].equals("Film-Noir")){
					  String temper = "Film-Noir";
					  outText.set(temper);
					  context.write(outText, new FloatWritable(rating));
				}
				else if(genreValues[i].equals("Horror")){
					  String temper = "Horror";
					  outText.set(temper);
					  context.write(outText, new FloatWritable(rating));
				}

			}
			
			
			/* print statements to view current values */
			
			/*for(int i = 0; i < columnValues.length; i++){
			System.out.println("ColumnValues["+i+"]: "+ columnValues[i]);
			System.out.println("movieid: "+ movieid);
			System.out.println("rating: " + rating);
			System.out.println("Genre: "+ genre);
			System.out.println("Pairgenre: "+ pair.getGenre()+ " pairrating: "+pair.getRating());
			System.out.println("movieMap.key: "+ genre + " moviemap.value: "+ movieMap.get(genre));}
			*/
			
		}
		
		
		/* the setup function runs only once before the mapping process starts
		 * it opens the movies.dat file and creates a hashmap of the movieids and
		 * their corresponding genres
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			String param = conf.get("matchfile");
			InputStream is = new FileInputStream(param);
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			
			/* reads each line, splits it and hashes id and matching genre */
			String line = null;
			while((line = br.readLine()) != null){
				String[] splitter = line.split("::");
				Integer temp = new Integer(Integer.parseInt(splitter[0]));
				matchMap.put(temp, splitter[2]);
			}
			
			is.close();
			isr.close();
			br.close();	
			System.out.println("Match Map " + matchMap.toString());
		}
		
		/* the cleanup function runs only once after the mapping process is complete
		 * Here it writes all of the mapped (genre, pair) values to the context
		 */
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException{
			System.out.println("counter: "+ counter);
			System.out.println("counter2: "+ counter2);
			System.out.println("counter3: "+ counter3);
			System.out.println("Moviemap: "+ movieMap.size());
			System.out.println("Matchmap: "+ matchMap.size());
		}
	}
	
	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		/* rateMap is a hashmap of (genre, statistics)
		 * the algorithm implemented here allows us to keep track of both the mean
		 * and standard deviation as a stream
		 */
		HashMap<String, Float[]> rateMap = new HashMap<String, Float[]>();
		float[] stats = new float[5];
		
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
			/* this is the part where the problems occur, when key and value are received from the Mapper, they are
			 * different / wrong. However, they are correct in the mapper.
			 */
			System.out.println("");
			for(FloatWritable val: values){
				System.out.print("Key: "+ key + " Value: "+ val);
			}
			for(FloatWritable val: values){				
				/* loops through every genre and updates the statistics: 
				 * mean is calculated by adding to the sum every iteration
				 * standard deviation uses the following formula:
				 * to start: m(k) = 0, s(1) = 0
				 * m(k) = m(k-1) + (x(k) - m(k-1)) / k
				 * s(k) = s(k-1) + (x(k) - m(k-1)*(x(k)-m(k))
				 * Finally, stdev = sqrt(s(k) / (k-1))
				 * 
				 * The float array is organized as follows:
				 * floatarray[0] = calculates the sum for the mean (add x everytime)
				 * floatarray[1] = keeps track of count (add 1 everytime)
				 * floatarray[2] = keeps track of m(k)
				 * floatarray[3] = keeps track of s(k)
				 */
				String currentgenre = key.toString();
				if(rateMap.containsKey(currentgenre)){
					Float[] temp = rateMap.get(currentgenre);
					float currentrating = val.get();
					temp[0] = new Float(temp[0].floatValue() + currentrating);
					temp[1] = new Float(temp[1].floatValue() + 1);
					float t2 = temp[2].floatValue();
					temp[2] = new Float(t2+(currentrating - t2)) / temp[1];
					float t3 = temp[3].floatValue();
					temp[3] = new Float(t3+((currentrating - t2)*(currentrating - temp[2])));
					rateMap.put(currentgenre, temp);
				}
				else{
					Float[] temp = new Float[4];
					temp[0] = new Float(0); /* sum */
					temp[1] = new Float(0); /* count */
					temp[2] = new Float(val.get()); /* m(k) */
					temp[3] = new Float(0); /* s(k) */
					
					rateMap.put(currentgenre, temp);
				}
			}
			System.out.println("");
			/* compute the final results and output them */
			float mean = rateMap.get(key)[0].floatValue();
			context.write(key, new FloatWritable(mean));
			System.out.print("Key: "+ key+ " Mean: "+ mean + "   ");
			//float std = Math.sqrt((double)rateMap.get(key)[3].doubleValue() / (rateMap.get(key)[1] - 1));
			//context.write(key,  new FloatWritable(mean));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		/* movies.dat could be replaced by an args[x] is necessary */
        Configuration conf = new Configuration();
        conf.set("matchfile", "movies.dat");

        /* creates new job from configuration */
        Job job = new Job(conf, "flight count");
        job.setJarByClass(MapReduce.class);
        
        /* defines the outputs from the Mapper */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        /* defines the outputs from the Job */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        /* defines the Mapper and Reduce classes */
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        /* defines the input and output formats for the Job */
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /* defines the input and output files for the job */
        FileInputFormat.addInputPath(job, new Path("x000"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

	}
}
