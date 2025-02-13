package vttp.batch5.paf.movies.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import jakarta.json.JsonArray;
import vttp.batch5.paf.movies.services.MovieService;

@Controller
@RequestMapping("/api")
public class MainController {

  @Autowired
  private MovieService movieSvc;

  // TODO: Task 3
  @GetMapping(path="/summary", produces=MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  public ResponseEntity<String> getJsonArray(@RequestParam Integer count){
    JsonArray arr = movieSvc.getProlificDirectors(count);
    return ResponseEntity.ok().body(arr.toString());
  }
   

  
  // TODO: Task 4
  @GetMapping(path="/summary/pdf", produces = MediaType.TEXT_HTML_VALUE)
  public void generatePdf(@RequestParam Integer count) {
    movieSvc.generatePDFReport(movieSvc.getProlificDirectors(count));
  }


}
