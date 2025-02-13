package vttp.batch5.paf.movies.services;

import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Service
public class MovieService {

  @Autowired
  private MongoMovieRepository mongoRepo;

  @Autowired
  private MySQLMovieRepository sqlRepo;

  // TODO: Task 2
    @Transactional(rollbackFor = Exception.class)
    public void loadData(JsonArray filteredJson) throws Exception {
      List<String> in = sqlRepo.batchInsertMovies(filteredJson);
      if(in != null) {
        mongoRepo.logError(in);
        throw new Exception("Data Access Exception");
      }
      mongoRepo.batchInsertMovies(filteredJson);
    }
  

  // TODO: Task 3
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void getProlificDirectors(int limit) {
    JsonArrayBuilder arr = Json.createArrayBuilder();
    List<Document> fromMongo = mongoRepo.getProlificDirectors();
    for(Document doc : fromMongo) {
      if(doc.getString("directors").equals(""))
        continue;
      Double[] res = sqlRepo.getProlificDirectors(doc.getList("imdb_ids", String.class));
    }
    
  }


  // TODO: Task 4
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void generatePDFReport() {

  }

}
