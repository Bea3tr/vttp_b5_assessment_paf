package vttp.batch5.paf.movies.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import vttp.batch5.paf.movies.services.MovieService;

@Controller
@RequestMapping("/api")
public class MainController {

  @Autowired
  private MovieService movieSvc;

  // TODO: Task 3
  @GetMapping(path="/summary", produces=MediaType.APPLICATION_JSON_VALUE)
  public void getJsonArray(@RequestParam Integer count){
    

  }
   

  
  // TODO: Task 4


}
