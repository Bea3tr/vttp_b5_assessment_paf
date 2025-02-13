package vttp.batch5.paf.movies.repositories;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.stereotype.Repository;

import com.mongodb.BasicDBObject;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;

@Repository
public class MongoMovieRepository {

    @Autowired
    private MongoTemplate template;

 // TODO: Task 2.3
 // You can add any number of parameters and return any type from the method
 // You can throw any checked exceptions from the method
 // Write the native Mongo query you implement in the method in the comments
 //
 //    native MongoDB query here
 /*
  * db.imdb.insertMany([
    { imdb_id: <id>, title: <title>, directors: <directors>, overview: <overview>, 
      tagline: <tagline>, genres: <genres>, imdb_rating: <imdb_rating>, imdb_votes: <imdb_votes> },
    { ... },
    { ... }
    ])
  */
 public void batchInsertMovies(JsonArray filteredJson) {
    List<Document> toInsert = filteredJson.stream()
        .map(obj -> {
            JsonObject doc = obj.asJsonObject();
            Document in = new Document();
            in.append("_id", doc.getString("imdb_id"));
            in.append("title", doc.getString("title"));
            in.append("directors",doc.getString("director"));
            in.append("overview", doc.getString("overview")); 
            in.append("tagline", doc.getString("tagline"));
            in.append("genres", doc.getString("genres"));
            in.append("imdb_rating", doc.getInt("imdb_rating"));
            in.append("imdb_votes", doc.getInt("imdb_votes"));
            return in;
        }).toList();

    for(int i = 0; i < filteredJson.size(); i += 25) {
        int end = i+25;
        if(end >= filteredJson.size())
            end = filteredJson.size() - 1;

        template.insert(toInsert.subList(i, end), "imdb");
    }
 }

 // TODO: Task 2.4
 // You can add any number of parameters and return any type from the method
 // You can throw any checked exceptions from the method
 // Write the native Mongo query you implement in the method in the comments
 //
 //    native MongoDB query here
 /*
  * db.errors.insert({
    $push: { "imdb_ids": : { $each: ["id1", "id2", "id3"] } },
    error: <error message>,
    timestamp: <current_time>
    })
  */
 public void logError(List<String> errorDetails) {
    int length = errorDetails.size();
    BasicDBObject error = new BasicDBObject("imdb_ids", errorDetails.subList(1, length))
        .append("error", errorDetails.get(0))
        .append("timestamp", new Date());
    
    template.insert(error, "errors");
 }

 // TODO: Task 3
 // Write the native Mongo query you implement in the method in the comments
 //
 //    native MongoDB query here
 /*
  * db.imdb.aggregate([
    { $match: { directors: <director_name> } },
    { $project: {
        director_name: '$directors',
        movies_count: { $sum: 1 }
    }  }
    ])

    db.imdb.aggregate([
    { $group: {
        _id: '$directors',
        movies_count: {$sum: 1},
        imdb_ids : { $push: '$_id' }
    } }
    ])
  */
 public List<Document> getProlificDirectors() {
    GroupOperation groupByDirectors = Aggregation.group("directors")
        .count().as("movies_count")
        .push("_id").as("imdb_ids");

    // List<Document> result = new LinkedList<>();
    // List<String> directors = template.findDistinct(new Query(), "directors", "imdb", String.class);

    // ProjectionOperation projectFields = Aggregation.project()
    //     .and("directors").as("director_name")
    //     .and("total").as("movies_count"); 

    // for(String dir : directors) {
    //     if(dir.equals(""))
    //         continue;
    //     MatchOperation matchByDirector = Aggregation.match(Criteria.where("directors").regex(dir, "i"));
    //     Aggregation pipeline= Aggregation.newAggregation(matchByDirector, projectFields);
    //     Document doc = template.aggregate(pipeline, "imdb", Document.class).getUniqueMappedResult();
    //     result.add(doc);
    // }

    return template.aggregate(Aggregation.newAggregation(groupByDirectors), "imdb", Document.class).getMappedResults();
    }
    
    


}

