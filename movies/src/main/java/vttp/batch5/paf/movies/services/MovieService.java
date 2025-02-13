package vttp.batch5.paf.movies.services;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import net.sf.jasperreports.engine.JasperExportManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.JasperReport;
import net.sf.jasperreports.engine.util.JRLoader;
import net.sf.jasperreports.json.data.JsonDataSource;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Service
public class MovieService {

  @Value("${ds.name}")
  private String name;

  @Value("${ds.batch}")
  private String batch;
  
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
  public JsonArray getProlificDirectors(int limit) {
    JsonArrayBuilder arr = Json.createArrayBuilder();
    List<Document> fromMongo = mongoRepo.getProlificDirectors();
    int i = 0;
    while(i < limit) {
      Document doc = fromMongo.get(i);
      if(doc.getString("_id").equals(""))
        continue;
      Double[] res = sqlRepo.getProlificDirectors(doc.getList("imdb_ids", String.class));
      arr.add(Json.createObjectBuilder()
        .add("director_name", doc.getString("_id"))
        .add("movies_count", doc.getInteger("movies_count"))
        .add("total_revenue", res[0])
        .add("total_budget", res[1])
        .build());
      i++;
    }
    return arr.build();
  }


  // TODO: Task 4
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void generatePDFReport(JsonArray dirArr){
    Path p = Paths.get("../data/director_movies_report.jrxml");
    try {
      JsonObject overall = Json.createObjectBuilder()
        .add("name", name)
        .add("batch", batch)
        .build();

      ByteArrayInputStream overallIs = new ByteArrayInputStream(overall.toString().getBytes());
      JsonDataSource reportDS = new JsonDataSource(overallIs);

      ByteArrayInputStream dirIs = new ByteArrayInputStream(dirArr.toString().getBytes());
      JsonDataSource directorsDS = new JsonDataSource(dirIs);
      // Report's parameters
      Map<String, Object> params = new HashMap<>();
      params.put("DIRECTOR_TABLE_DATASET", directorsDS);

      // Load report
      JasperReport report = (JasperReport) JRLoader.loadObject(p.toFile());

      // Populate report
      JasperPrint print = JasperFillManager.fillReport(report, params, reportDS);
      
      // Generate report
      JasperExportManager.exportReportToPdfFile(print,"../data/report.pdf");
      
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    
    

  }

}
