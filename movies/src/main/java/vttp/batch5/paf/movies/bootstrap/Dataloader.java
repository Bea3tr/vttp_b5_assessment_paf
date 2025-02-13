package vttp.batch5.paf.movies.bootstrap;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonParsingException;
import vttp.batch5.paf.movies.services.MovieService;

@Component
public class Dataloader implements CommandLineRunner {

  @Autowired
  private MovieService movieSvc;

  //TODO: Task 2
  @Override
  public void run(String... args) {
    // Determine if data has been loaded - zip file
    Set<String> ids = new HashSet<>();
    Map<String, String> keyType = new HashMap<>();
    keyType.put("title", "STRING");
    keyType.put("vote_average", "NUMBER");
    keyType.put("vote_count", "NUMBER");
    keyType.put("status", "STRING");
    keyType.put("release_date", "STRING");
    keyType.put("revenue", "NUMBER");
    keyType.put("runtime", "NUMBER");
    keyType.put("budget", "NUMBER");
    keyType.put("imdb_id", "STRING");
    keyType.put("original_language", "STRING");
    keyType.put("overview", "STRING");
    keyType.put("popularity", "NUMBER");
    keyType.put("tagline", "STRING");
    keyType.put("genres", "STRING");
    keyType.put("spoken_languages", "STRING");
    keyType.put("casts", "STRING");
    keyType.put("director", "STRING");
    keyType.put("imdb_rating", "NUMBER");
    keyType.put("imdb_votes", "NUMBER");
    keyType.put("poster", "STRING");
    if((args.length <= 0))
      return;
    System.out.println(">>> Command line started");
    System.out.println(">>> " + args[0]);
    Path p = Paths.get("../" + args[0]);
    try {
      ZipFile zFile = new ZipFile(p.toFile());
      System.out.println(">>> Zipfile entry: " + zFile.entries());
      Enumeration< ? extends ZipEntry> entries = zFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry file = entries.nextElement();
        if(!file.isDirectory()) {
            InputStream is = zFile.getInputStream(file);
            BufferedInputStream bis = new BufferedInputStream(is);
            Scanner scan = new Scanner(bis);
            String line = "";
            JsonArrayBuilder filtered = Json.createArrayBuilder();
            while(line != null) {
              try {
                line = scan.nextLine();
                if(line == null)
                break;

              } catch (NoSuchElementException ex) {
                break;
              }
              
              // Read JsonObject
              try {
                JsonObject obj = Json.createReader(new StringReader(line))
                  .readObject();
              
                // Filter by date
                if(obj.get("release_date").getValueType() == JsonValue.ValueType.NULL)
                  continue;
                
                int year = Integer.parseInt(obj.getString("release_date").split("\\-")[0]);
                if(year < 2018)
                  continue;
                
                JsonObjectBuilder builder = Json.createObjectBuilder();
                // Filter duplicate ids
                if(ids.contains(obj.getString("imdb_id"))){
                  System.out.println(">>> Duplicate id: " + obj.getString("imdb_id"));
                  continue;
                }
                  
                for(String key : obj.keySet()) {
                  if(obj.get(key).getValueType() != JsonValue.ValueType.NULL) 
                    builder.add(key, obj.get(key));

                  else {
                    if(keyType.get(key).equals("STRING"))
                      builder.add(key, "");
                    if(keyType.get(key).equals("NUMBER"))
                      builder.add(key, 0);
                  }
                }
                filtered.add(builder.build());
                ids.add(obj.getString("imdb_id"));
              } catch(JsonParsingException ex){
                continue;
              }
            }
            scan.close();
            zFile.close();

            movieSvc.loadData(filtered.build());
          }
        }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    
  }



}
