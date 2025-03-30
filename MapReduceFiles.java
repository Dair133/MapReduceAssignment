import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Scanner;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class MapReduceFiles {
  //constant for threading strategy
  private static final int MAX_LINE_LENGTH = 80;
  
  //pattern for word extraction as per the requirementonly letters are considered as words
  //punctuation and numbers are ignored
  private static final Pattern WORD_PATTERN = Pattern.compile("\\b([a-zA-Z]+)\\b");

  public static void main(String[] args) {

    //check for input directory with files
    if (args.length != 1) {
      System.err.println("usage: java MapReduceFiles directory_path");
      System.exit(1);
    }

    //record start time for overall execution
    long startTimeOverall = System.currentTimeMillis();

    //get the directory path from command line argument
    String directoryPath = args[0];
    File directory = new File(directoryPath); 
    
    //check if the directory exists and is a valid directory
    if (!directory.exists() || !directory.isDirectory()) {
      System.err.println("Error: The specified path is not a valid directory: " + directoryPath);
      System.exit(1);
    }

    //get all files in the directory
    File[] files = directory.listFiles();
    
    //if any files are not found or wrongg in the directory print error message and exit
    if (files == null || files.length == 0) {
      System.err.println("Error: No files found in the directory: " + directoryPath);
      System.exit(1);
    }

    //load all files into a map with filename as key and file contents as value
    Map<String, String> input = new HashMap<String, String>();
    try {
      //load all files into a map with filename as key and file contents as value
      for (File file : files) {
        //check if the file is a normal file and not a directory
        if (file.isFile()) {
          String filename = file.getAbsolutePath();
          input.put(filename, readFile(filename)); //read the file contents and store in the map
        }
      }
      System.out.println("Loaded " + input.size() + " files for processing"); //print number of files loaded
    }
    catch (IOException ex)
    {
        System.err.println("Error reading files...\n" + ex.getMessage());
        ex.printStackTrace();
        System.exit(0);
    }

    // Brute force Approach
    {
      System.out.println("\n=== APPROACH #1: Brute Force ==="); //print approach to consol
      long startTime = System.currentTimeMillis(); //start time for this approach
      long mapStartTime = System.currentTimeMillis(); //start time for map phase
      
      Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>(); //output map to store the results

      //iterate over all files and their contents
      Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
      //for each file, split the contents into words and store in the output map
      while(inputIter.hasNext()) {
        Map.Entry<String, String> entry = inputIter.next();
        String file = entry.getKey();
        String contents = entry.getValue(); //get the file name and contents


        //split the contents into words using whitespace as delimiter
        String[] words = contents.trim().split("\\s+");

        //for each word, extract only the word part  with no punctuation and store in the output map
        for(String word : words) {
          Matcher matcher = WORD_PATTERN.matcher(word);
          //use regex to extract only the word part 
          if (matcher.find()) {
            word = matcher.group(1).toLowerCase(); //convert to lowercase
            //store the word and file name in the output map
            Map<String, Integer> wordFiles = output.get(word);
            //if the word is not already in the map, create a new entry for it
            if (wordFiles == null) {
              wordFiles = new HashMap<String, Integer>();
              output.put(word, wordFiles);
            }

            //increment the count of occurrences of the word in the file
            Integer occurrences = wordFiles.remove(file);
            if (occurrences == null) {
              wordFiles.put(file, 1); //if the word is not already in the map add with count 1
            } else {
              wordFiles.put(file, occurrences.intValue() + 1); //increase the count by 1 if present
            }
          }
        }
      }

      long mapEndTime = System.currentTimeMillis(); //end time for map phase
      long endTime = System.currentTimeMillis(); //end time for this approach
      
      //show the results to the console for the user
      System.out.println("Total words: " + output.size());
      System.out.println("Map phase: " + (mapEndTime - mapStartTime) + " ms");
      System.out.println("Total execution: " + (endTime - startTime) + " ms");
    }


    // MapReduce approach
    {
      System.out.println("\n=== APPROACH #2: MapReduce ==="); //print approach to console
      long startTime = System.currentTimeMillis(); //start time for this approach
      
      Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>(); //output map to store the results

      // MAP:
      long mapStartTime = System.currentTimeMillis(); //start time for map phase
      List<MappedItem> mappedItems = new LinkedList<MappedItem>(); //list to store the mapped items

      //iterator to iterate over all files and their content
      Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
      //for each file, split the contents into words and store in the mapped items list
      while(inputIter.hasNext()) {
        Map.Entry<String, String> entry = inputIter.next();
        String file = entry.getKey();
        String contents = entry.getValue();

        map(file, contents, mappedItems); //call the map function to split contents into words and store in mapped items list
      }
      long mapEndTime = System.currentTimeMillis(); //end time for map phase

      // GROUP:
      long groupStartTime = System.currentTimeMillis(); //start time for group phase
      Map<String, List<String>> groupedItems = new HashMap<String, List<String>>(); //map to store the grouped item

      Iterator<MappedItem> mappedIter = mappedItems.iterator();//iterator to iterate over all mapped items
      //for each mapped item group by word and store in grouped items map
      while(mappedIter.hasNext()) {
        MappedItem item = mappedIter.next(); //get the next mapped item
        String word = item.getWord(); //get word
        String file = item.getFile();//get file 
        List<String> list = groupedItems.get(word); //get list
        if (list == null) {
          //if doesnt exist create a new list and add to the map
          list = new LinkedList<String>();
          groupedItems.put(word, list);
        }
        //add the file to the list of files for the word
        list.add(file);
      }

      long groupEndTime = System.currentTimeMillis(); //end time for group phase

      // REDUCE:
      long reduceStartTime = System.currentTimeMillis(); //start time for reduce phase
      Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator(); //iterator to iterate over all grouped items

      //while there are grouped items, reduce them and store in the output map
      while(groupedIter.hasNext()) {
        Map.Entry<String, List<String>> entry = groupedIter.next(); //get the next grouped item
        String word = entry.getKey(); //get the word as the key from the entry
        List<String> list = entry.getValue(); //locate the list of files for the word

        reduce(word, list, output); //call the reduce function as reduce the list of files for the word 
      }


      long reduceEndTime = System.currentTimeMillis(); //end time for reduce phase
      long endTime = System.currentTimeMillis(); //end time for this approach

      //Prrint the results to the console for the user
      System.out.println("Total words: " + output.size());
      System.out.println("Map phase: " + (mapEndTime - mapStartTime) + " ms");
      System.out.println("Group phase: " + (groupEndTime - groupStartTime) + " ms");
      System.out.println("Reduce phase: " + (reduceEndTime - reduceStartTime) + " ms");
      System.out.println("Total execution: " + (endTime - startTime) + " ms");
    }

    // Approach 3 - Testing different thread configurations
    System.out.println("\n=== APPROACH #3: Testing Different Thread Configurations ===");
    
    //the parameter values to test defined
    int[] linesPerMapThreadValues = {1000, 2000, 5000, 10000};
    int[] wordsPerReduceThreadValues = {100, 200, 500, 1000};
    
    //store best results for map and reduce phase
    long bestMapTime = Long.MAX_VALUE;
    int bestLinesPerMapThread = 0;
    long bestReduceTime = Long.MAX_VALUE;
    int bestWordsPerReduceThread = 0;
    
    //test map phase configs with fixed reduce config
    System.out.println("\n--- Testing Map Phase Configurations ---");
    for (int linesPerMapThread : linesPerMapThreadValues) {
        //run the test with the current configuration
        long mapTime = runTest(input, linesPerMapThread, 500);
        //compare the time taken with the best time so far
        if (mapTime < bestMapTime) {
            bestMapTime = mapTime;
            bestLinesPerMapThread = linesPerMapThread;
        }
    }
    
    //test reduce phase configs with the best map config
    System.out.println("\n--- Testing Reduce Phase Configurations ---");
    for (int wordsPerReduceThread : wordsPerReduceThreadValues) {
        long reduceTime = runTest(input, bestLinesPerMapThread, wordsPerReduceThread);
        if (reduceTime < bestReduceTime) {
            bestReduceTime = reduceTime;
            bestWordsPerReduceThread = wordsPerReduceThread;
        }
    }
    
    //run one final test with the best config
    System.out.println("\n--- Running Final Test with Optimal Configuration ---");
    //print optimal configuration to console
    System.out.println("Optimal Map Configuration: " + bestLinesPerMapThread + " lines per thread");
    System.out.println("Optimal Reduce Configuration: " + bestWordsPerReduceThread + " words per thread");

    //run one more test with the best configuration
    runTest(input, bestLinesPerMapThread, bestWordsPerReduceThread);
    
    long endTimeOverall = System.currentTimeMillis(); //end time for overall execution
    System.out.println("\nTotal program execution time: " + (endTimeOverall - startTimeOverall) + " ms"); //print total execution time to console
  }
  //method to run the test with the given configuration
  private static long runTest(Map<String, String> input, int linesPerMapThread, int wordsPerReduceThread) {
    //print the config to console during the test
    System.out.println("\nTesting with " + linesPerMapThread + " lines per map thread and " + 
                      wordsPerReduceThread + " words per reduce thread");
    
    long startTime = System.currentTimeMillis(); //start time for this test
    //output map to store the results
    final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

    // MAP:
    long mapStartTime = System.currentTimeMillis(); //start time for map phase
    final List<MappedItem> mappedItems = new CopyOnWriteArrayList<>(); //list to store the mapped items

    //callback to handle the results of the map phase
    final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
      @Override 
      //synchronised method to handle the results of the map phase
      public synchronized void mapDone(String id, List<MappedItem> results) {
        mappedItems.addAll(results); //add the results to the mapped items list
      }
    };

    //prepare all lines from all files using the lines per map thread config
    List<LineChunk> lineChunks = new ArrayList<>();
    int chunkId = 0;

    //iterate over all files and their contents
    for (Map.Entry<String, String> entry : input.entrySet()) {
      String file = entry.getKey();
      String contents = entry.getValue();
      
      //split content into lines
      String[] lines = contents.split("\\r?\\n");
      
      //process lines so split any that are way too long
      List<String> processedLines = new ArrayList<>();
      for (String line : lines) {
        if (line.length() > MAX_LINE_LENGTH) {
          //split at next whitespace after 80 letrters
          int pos = MAX_LINE_LENGTH;
          //find the next whitespace character after the max line length
          while (pos < line.length() && !Character.isWhitespace(line.charAt(pos))) {
            pos++; //increment position until whitespace is found
          }
          //if whitespace is found, split the line into two parts
          if (pos < line.length()) {
            processedLines.add(line.substring(0, pos)); //add the first part to the list
            processedLines.add(line.substring(pos).trim()); //add the second part to the list
          } else {
            processedLines.add(line); //if no whitespace is found, add the whole line to the list
          }
        } else {
          processedLines.add(line); //if the line is not too long, add it to the list
        }
      }
      
      //group lines into chunks
      for (int i = 0; i < processedLines.size(); i += linesPerMapThread) {
        int end = Math.min(i + linesPerMapThread, processedLines.size()); //calculate the end index for the chunk
        List<String> chunk = processedLines.subList(i, end); //get the chunk of lines
        lineChunks.add(new LineChunk(chunkId++, file, chunk)); //create a new line chunk with the lines and file name
      }
    }

    //create and start map threads
    List<Thread> mapCluster = new ArrayList<>();
    //for each line chunk, create a new thread to process the chunk
    for (final LineChunk chunk : lineChunks) {
      Thread t = new Thread(new Runnable() {
        @Override
        //run method to execute the map function in a separate thread
        public void run() {
          mapChunk(chunk, mapCallback);
        }
      });
      mapCluster.add(t); //add the thread to the list of map threads
      t.start(); //start the thread
    }

    //watch for map phase to be over
    for (Thread t : mapCluster) {
      try {
        t.join(); //wait for the thread to finish
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    long mapEndTime = System.currentTimeMillis(); //end time for map phase
    long mapDuration = mapEndTime - mapStartTime; //calculate the duration of the map phase

    // GROUP:
    long groupStartTime = System.currentTimeMillis();
    Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

    Iterator<MappedItem> mappedIter = mappedItems.iterator();
    //for each mapped item, group by word and store in the grouped items map
    while(mappedIter.hasNext()) {
      MappedItem item = mappedIter.next();
      String word = item.getWord();
      String file = item.getFile();
      List<String> list = groupedItems.get(word);
      if (list == null) {
        list = new LinkedList<String>();
        groupedItems.put(word, list);
      }
      list.add(file); //add the file to the list of files for the word
    }
    
    //create batches for reduce phase
    List<ReduceBatch> reduceBatches = new ArrayList<>();
    int batchId = 0; //batch id for each reduce batch
    List<String> currentBatchWords = new ArrayList<>(); //list to store the words for the current batch
    
    //iterate over all grouped items and create batches for reduce phase
    for (Map.Entry<String, List<String>> entry : groupedItems.entrySet()) {
      currentBatchWords.add(entry.getKey());

      //if the current batch size is greater than or equal to the words per reduce thread config create a new batch
      if (currentBatchWords.size() >= wordsPerReduceThread) {
        reduceBatches.add(new ReduceBatch(batchId++, new ArrayList<>(currentBatchWords), groupedItems)); //arraylist to avoid concurrent modification
        currentBatchWords.clear(); //clear the current batch words for the next batch
      }
    }
    
    //add any remaining words as the final batch
    if (!currentBatchWords.isEmpty()) {
      reduceBatches.add(new ReduceBatch(batchId++, currentBatchWords, groupedItems));
    }
    
    long groupEndTime = System.currentTimeMillis(); //end time for group phase

    // REDUCE:
    long reduceStartTime = System.currentTimeMillis(); //start time for reduce phase
    //callback to handle the results of the reduce phase
    final ReduceCallback<Integer, String, Map<String, Integer>> reduceCallback = new ReduceCallback<Integer, String, Map<String, Integer>>() {
      //synchronised to handle the results of the reduce phase
      @Override
      public synchronized void reduceDone(Integer batchId, Map<String, Map<String, Integer>> results) {
        output.putAll(results); //add the results to the output map
      }
    };

    List<Thread> reduceCluster = new ArrayList<>(reduceBatches.size()); //list to store the reduce threads


    //create and start reduce threads
    for (final ReduceBatch batch : reduceBatches) {
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          reduceBatch(batch, reduceCallback);
        }
      });
      reduceCluster.add(t);
      t.start();
    }

    //wait for reducing phase to be over:
    for(Thread t : reduceCluster) {
      try {
        t.join();
      } catch(InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    //end time for reduce phase
    long reduceEndTime = System.currentTimeMillis();
    //hiow long it took to reduce the data
    long reduceDuration = reduceEndTime - reduceStartTime;
    //how long it took to group the data
    long groupDuration = groupEndTime - groupStartTime;
    long endTime = System.currentTimeMillis(); //end time for this test
    long totalDuration = endTime - startTime; //total time taken for this test

    //Print useful info to the console for the user
    System.out.println("Total words: " + output.size());
    System.out.println("Map threads: " + lineChunks.size() + ", Map phase: " + mapDuration + " ms");
    System.out.println("Group phase: " + groupDuration + " ms");
    System.out.println("Reduce threads: " + reduceBatches.size() + ", Reduce phase: " + reduceDuration + " ms");
    System.out.println("Total execution: " + totalDuration + " ms");
    
    return mapDuration + reduceDuration; //return combined time for comparison
  }

  //method to reduce the data in the batch and store in the output map
  private static void reduceBatch(ReduceBatch batch, ReduceCallback<Integer, String, Map<String, Integer>> callback) {
    //create a map to store the results of the reduce phase
    Map<String, Map<String, Integer>> results = new HashMap<>();
    
    for (String word : batch.getWords()) {
      List<String> list = batch.getGroupedItems().get(word); //get the list of files for the word
      
      Map<String, Integer> reducedList = new HashMap<>();
      for (String file : list) {
        Integer occurrences = reducedList.get(file);
        //if the file is not already in the map, add it with count 1
        if (occurrences == null) {
          reducedList.put(file, 1);
        } else {
          reducedList.put(file, occurrences + 1);
        }
      }
      
      results.put(word, reducedList); //add the reduced list to the results map
    }
    
    callback.reduceDone(batch.getId(), results); //.call the callback to handle the results
  }


  //method to map the data in the chunk and store in the callback
  private static void mapChunk(LineChunk chunk, MapCallback<String, MappedItem> callback) {
    List<MappedItem> results = new ArrayList<>();
    String file = chunk.getFile(); //get the file name from the chunk
    
    //iterate over all lines in the chunk and split into words
    for (String line : chunk.getLines()) {
      String[] words = line.trim().split("\\s+"); //split the line into words using whitespace as delimiter
      for (String word : words) {
        //Extract only the word part (no punctuation will be parsed)
        Matcher matcher = WORD_PATTERN.matcher(word);
        //use regex to extract only the word part
        if (matcher.find()) {
          word = matcher.group(1).toLowerCase();
          results.add(new MappedItem(word, file));
        }
      }
    }
    
    callback.mapDone("chunk-" + chunk.getId(), results); //call the callback to handle the results
  }

  //method to map the data in the file and store in the mapped items list
  public static void map(String file, String contents, List<MappedItem> mappedItems) {
    String[] words = contents.trim().split("\\s+");
    for(String word: words) {
      //only the word part
      Matcher matcher = WORD_PATTERN.matcher(word);
      if (matcher.find()) {
        word = matcher.group(1).toLowerCase();//convert to lowercase
        mappedItems.add(new MappedItem(word, file)); //add to the mapped items list
      }
    }
  }


  //method to reduce the data in the list and store in the output map
  public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
    Map<String, Integer> reducedList = new HashMap<String, Integer>(); //create a map to store the results of the reduce phase
    for(String file: list) {
      Integer occurrences = reducedList.get(file);
      if (occurrences == null) {
        reducedList.put(file, 1); //if the file is not already in the map, add it with count 1
      } else {
        reducedList.put(file, occurrences.intValue() + 1); //increase the count by 1 if present
      }
    }
    output.put(word, reducedList); //add the reduced list to the output map
  }


  //callback interface for map phase uused to handle the results of the map phase
  public static interface MapCallback<E, V> {
    public void mapDone(E key, List<V> values); 
  }

  public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
    //split the contents into words using whitespace as delimiter
    String[] words = contents.trim().split("\\s+");
    List<MappedItem> results = new ArrayList<MappedItem>(words.length); //list to store the mapped items
    for(String word: words) {
      // Extract only the word part
      Matcher matcher = WORD_PATTERN.matcher(word);
      if (matcher.find()) {
        word = matcher.group(1).toLowerCase(); //convert to lowercase
        results.add(new MappedItem(word, file)); //add to the mapped items list results
    }
    callback.mapDone(file, results); //call the callback to handle the results
  }
  }

  //callback interface for reduce phase used to handle the results of the reduce phase
  public static interface ReduceCallback<E, K, V> {
    public void reduceDone(E e, Map<K, V> results);//callback method to handle the results of the reduce phase
  }

  //method to reduce the data in the list and store in the callback
  public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {
    Map<String, Integer> reducedList = new HashMap<String, Integer>(); //create a map to store the results of the reduce phase
    //go through the list of files and count the occurrences of each file for the word
    for(String file: list) {
      Integer occurrences = reducedList.get(file);
      if (occurrences == null) {
        reducedList.put(file, 1);
      } else {
        reducedList.put(file, occurrences.intValue() + 1);
      }
    }
    callback.reduceDone(word, reducedList);
  }

  //class to store the line chunk with id, file name and lines
  private static class LineChunk {
    private final int id; //id of the chunk
    private final String file; //file name of the chunk
    private final List<String> lines;//list of lines in the chunk
    
    //constructor
    public LineChunk(int id, String file, List<String> lines) {
      this.id = id;
      this.file = file;
      this.lines = lines;
    }
    
    //getters
    public int getId() { return id; }
    public String getFile() { return file; }
    public List<String> getLines() { return lines; }
  }
  
  private static class ReduceBatch {
    private final int id; //id of the batch
    private final List<String> words; //list of words in the batch
    private final Map<String, List<String>> groupedItems; //map to store the grouped items
    
    //constructor
    public ReduceBatch(int id, List<String> words, Map<String, List<String>> groupedItems) {
      this.id = id;
      this.words = words;
      this.groupedItems = groupedItems;
    }
    
    //getters
    public int getId() { return id; }
    public List<String> getWords() { return words; }
    public Map<String, List<String>> getGroupedItems() { return groupedItems; }
  }

  private static class MappedItem {
    private final String word; //word in the mapped item
    private final String file;//file name in the mapped item


    //constructor
    public MappedItem(String word, String file) {
      this.word = word;
      this.file = file;
    }

    //getters
    public String getWord() {
      return word;
    }

    public String getFile() {
      return file;
    }

    //toString method to print the mapped item
    @Override
    public String toString() {
      return "[\"" + word + "\",\"" + file + "\"]";
    }
  }

  //method to read the file and return the contents as a string
  private static String readFile(String pathname) throws IOException {
    File file = new File(pathname); //create a new file object with the given path
    StringBuilder fileContents = new StringBuilder((int) file.length()); //create a new string builder to store the file contents
    Scanner scanner = new Scanner(new BufferedReader(new FileReader(file))); //scanner to read the file contents
    String lineSeparator = System.getProperty("line.separator"); //get the line separator for the current system

    //read the file contents line by line and append to the string builder
    try {
      //read the first line and append to the string builder
      if (scanner.hasNextLine()) {
        fileContents.append(scanner.nextLine());
      }
      //read the rest of the lines ad append to the string builder with line separator
      while (scanner.hasNextLine()) {
        fileContents.append(lineSeparator + scanner.nextLine());
      }
      return fileContents.toString(); //return the file contents as a string
    } finally {
      scanner.close(); //clso the scanner
    }
  }
}