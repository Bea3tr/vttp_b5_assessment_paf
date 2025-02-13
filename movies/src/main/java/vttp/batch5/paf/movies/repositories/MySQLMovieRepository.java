package vttp.batch5.paf.movies.repositories;

import java.util.LinkedList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;

import jakarta.json.JsonArray;
import jakarta.json.JsonObject;

@Repository
public class MySQLMovieRepository {

  @Autowired
  private JdbcTemplate template;

  public static final String INSERT_MOVIES = """
      INSERT IGNORE INTO imdb (imdb_id, vote_average, vote_count, release_date, revenue, budget, runtime)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      """;

  public static final String SELECT_DIRECTORS = """
      SELECT revenue, budget FROM imdb WHERE imdb_id = ?
      """;

  // TODO: Task 2.3
  // You can add any number of parameters and return any type from the method
  public List<String> batchInsertMovies(JsonArray filteredJson) {
    List<Object[]> data = filteredJson.stream()
        .map(obj -> {
          JsonObject doc = obj.asJsonObject();
          return new Object[] {doc.getString("imdb_id"), 
            Float.parseFloat(doc.getJsonNumber("vote_average").toString()), 
            doc.getInt("vote_count"), doc.getString("release_date"), 
            Double.parseDouble(doc.getJsonNumber("revenue").toString()), 
            Double.parseDouble(doc.getJsonNumber("budget").toString()), doc.getInt("runtime")};
        }).toList();

    for(int i = 0; i < filteredJson.size(); i += 25) {
      int end = i+25;
      if(end >= filteredJson.size())
        end = filteredJson.size()-1;

      try {
        template.batchUpdate(INSERT_MOVIES, data.subList(i, end));

      } catch (DataAccessException ex) {
        List<Object[]> errorIn = data.subList(i, end);
        List<String> ids = errorIn.stream()
          .map(objArr -> objArr[0].toString())        
          .toList();

        List<String> errorOut = new LinkedList<>();
        errorOut.add(ex.getMessage());
        for(String id : ids) {
          errorOut.add(id);
        }
        return errorOut;
      }
    }   
    return null;
  }
  
  // TODO: Task 3
  public Double[] getProlificDirectors(List<String> ids) {
    Double revenue = 0.0;
    Double budget = 0.0;
    for(String id : ids) {
      SqlRowSet rs = template.queryForRowSet(SELECT_DIRECTORS, id);
      while(rs.next()) {
        revenue += rs.getDouble("revenue");
        budget += rs.getDouble("budget");
      }
    }
    Double[] result = new Double[] {revenue, budget};
    return result;
  }


}
