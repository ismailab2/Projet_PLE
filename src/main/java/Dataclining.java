import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Dataclining {
    public enum Stats { TOTAL, INVALID_JSON, BAD_DECK, VALID }

    public static class JsonWritable implements WritableComparable<JsonWritable> {
        private final Text json = new Text();
        public JsonWritable() { }  
        public JsonWritable(String s) { json.set(s); }
        public void set(String s) { json.set(s); }
        public String getJson() { return json.toString(); }

        @Override
        public void write(DataOutput out) throws IOException {
            json.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            json.readFields(in);
        }

        @Override
        public int compareTo(JsonWritable o) {
            return json.compareTo(o.json);
        }

        @Override
        public int hashCode() {
            return json.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof JsonWritable && json.equals(((JsonWritable) obj).json);
        }

        @Override
        public String toString() {
            return json.toString();
        }
    }

    public static class JsonMapper extends Mapper<LongWritable, Text, JsonWritable, NullWritable> {
        private final JsonWritable outKey = new JsonWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(Stats.TOTAL).increment(1);
            String ligne = value.toString().trim(); 

            try {
                // la ligne doit être un objet JSON valide
                JsonElement json = JsonParser.parseString(ligne);
                if (!json.isJsonObject()) {
                    context.getCounter(Stats.INVALID_JSON).increment(1);
                    return;
                }

                // exactement 2 joueurs
                JsonElement playElement = json.getAsJsonObject().get("players");
                if (playElement == null || !playElement.isJsonArray()) {
                    context.getCounter(Stats.BAD_DECK).increment(1);
                    return;
                }
                JsonArray players = playElement.getAsJsonArray();
                if (players.size() != 2) {
                    context.getCounter(Stats.BAD_DECK).increment(1);
                    return;
                }

                // chaque deck = 16 caractères hexadécimaux (8 cartes de 2 caractères)
                for (JsonElement player : players) {
                    if (!player.isJsonObject()) {
                        context.getCounter(Stats.BAD_DECK).increment(1);
                        return;
                    }
                    JsonElement deckElement = player.getAsJsonObject().get("deck");
                    if (deckElement == null || !deckElement.isJsonPrimitive()
                            || !deckElement.getAsJsonPrimitive().isString()) {
                        context.getCounter(Stats.BAD_DECK).increment(1);
                        return;
                    }
                    String deck = deckElement.getAsString();
                    if (!deck.matches("[0-9a-fA-F]{16}")) {
                        context.getCounter(Stats.BAD_DECK).increment(1);
                        return;
                    }
                    // les 8 cartes doivent être distinctes
                    Set<String> cartes = new HashSet<>();
                    for (int i = 0; i < 16; i += 2) {
                        cartes.add(deck.substring(i, i + 2).toLowerCase());
                    }
                    if (cartes.size() != 8) {
                        context.getCounter(Stats.BAD_DECK).increment(1);
                        return;
                    }
                }

                context.getCounter(Stats.VALID).increment(1);
                outKey.set(ligne);
                context.write(outKey, NullWritable.get());

            } catch (Exception e) {
                // JSON invalide ou structure inattendue : la ligne est ignorée
                context.getCounter(Stats.INVALID_JSON).increment(1);
            }
        }
    }

    // Supprime  une partie des doublons côté map (moins de données au shuffle)
    public static class JsonCombiner extends Reducer<JsonWritable, NullWritable, JsonWritable, NullWritable> {
        @Override
        protected void reduce(JsonWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    // Une clé = une ligne unique 
    public static class JsonReducer extends Reducer<JsonWritable, NullWritable, NullWritable, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(JsonWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            outValue.set(key.getJson());
            context.write(NullWritable.get(), outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Dataclining <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Nettoyage JSON et suppression des doublons");
        job.setJarByClass(Dataclining.class);

        job.setMapperClass(JsonMapper.class);
        job.setCombinerClass(JsonCombiner.class);
        job.setReducerClass(JsonReducer.class);
        job.setNumReduceTasks(42);                       

        job.setMapOutputKeyClass(JsonWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ok = job.waitForCompletion(true);
        System.exit(ok ? 0 : 1);
    }
}