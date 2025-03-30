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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class MapReduceFiles {

  // Constants for threading strategies
  private static final int MIN_LINES_PER_MAP_THREAD = 1000;
  private static final int MAX_LINES_PER_MAP_THREAD = 10000;
  private static final int MIN_WORDS_PER_REDUCE_THREAD = 100;
  private static final int MAX_WORDS_PER_REDUCE_THREAD = 1000;
  private static final int MAX_LINE_LENGTH = 80;
  
  // Pattern for extracting words (only letters, no punctuation or numbers)
  private static final Pattern WORD_PATTERN = Pattern.compile("\\b([a-zA-Z]+)\\b");

  public static void main(String[] args) {

    if (args.length < 1) {
      System.err.println("usage: java MapReduceFiles file1.txt file2.txt file3.txt ...");
      System.exit(1);
    }

    // Record start time for overall execution
    long startTimeOverall = System.currentTimeMillis();
    
    // Load all files specified in arguments
    Map<String, String> input = new HashMap<String, String>();
    try {
      for (String filename : args) {
        input.put(filename, readFile(filename));
      }
      System.out.println("Loaded " + input.size() + " files for processing");
    }
    catch (IOException ex) {
      System.err.println("Error reading files...\n" + ex.getMessage());
      ex.printStackTrace();
      System.exit(0);
    }

    // APPROACH #1: Brute force
    {
      System.out.println("\n=== APPROACH #1: Brute Force ===");
      long startTime = System.currentTimeMillis();
      
      Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

      long mapStartTime = System.currentTimeMillis();
      Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
      while(inputIter.hasNext()) {
        Map.Entry<String, String> entry = inputIter.next();
        String file = entry.getKey();
        String contents = entry.getValue();

        String[] words = contents.trim().split("\\s+");

        for(String word : words) {
          // Extract only the word part (no punctuation or numbers)
          Matcher matcher = WORD_PATTERN.matcher(word);
          if (matcher.find()) {
            word = matcher.group(1).toLowerCase();
            
            Map<String, Integer> files = output.get(word);
            if (files == null) {
              files = new HashMap<String, Integer>();
              output.put(word, files);
            }

            Integer occurrences = files.remove(file);
            if (occurrences == null) {
              files.put(file, 1);
            } else {
              files.put(file, occurrences.intValue() + 1);
            }
          }
        }
      }
      long mapEndTime = System.currentTimeMillis();

      // show me:
      long endTime = System.currentTimeMillis();
      System.out.println("Total words: " + output.size());
      System.out.println("Map phase: " + (mapEndTime - mapStartTime) + " ms");
      System.out.println("Total execution: " + (endTime - startTime) + " ms");
    }


    // APPROACH #2: MapReduce
    {
      System.out.println("\n=== APPROACH #2: MapReduce ===");
      long startTime = System.currentTimeMillis();
      
      Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

      // MAP:
      long mapStartTime = System.currentTimeMillis();
      List<MappedItem> mappedItems = new LinkedList<MappedItem>();

      Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
      while(inputIter.hasNext()) {
        Map.Entry<String, String> entry = inputIter.next();
        String file = entry.getKey();
        String contents = entry.getValue();

        map(file, contents, mappedItems);
      }
      long mapEndTime = System.currentTimeMillis();

      // GROUP:
      long groupStartTime = System.currentTimeMillis();
      Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

      Iterator<MappedItem> mappedIter = mappedItems.iterator();
      while(mappedIter.hasNext()) {
        MappedItem item = mappedIter.next();
        String word = item.getWord();
        String file = item.getFile();
        List<String> list = groupedItems.get(word);
        if (list == null) {
          list = new LinkedList<String>();
          groupedItems.put(word, list);
        }
        list.add(file);
      }
      long groupEndTime = System.currentTimeMillis();

      // REDUCE:
      long reduceStartTime = System.currentTimeMillis();
      Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
      while(groupedIter.hasNext()) {
        Map.Entry<String, List<String>> entry = groupedIter.next();
        String word = entry.getKey();
        List<String> list = entry.getValue();

        reduce(word, list, output);
      }
      long reduceEndTime = System.currentTimeMillis();

      long endTime = System.currentTimeMillis();
      System.out.println("Total words: " + output.size());
      System.out.println("Map phase: " + (mapEndTime - mapStartTime) + " ms");
      System.out.println("Group phase: " + (groupEndTime - groupStartTime) + " ms");
      System.out.println("Reduce phase: " + (reduceEndTime - reduceStartTime) + " ms");
      System.out.println("Total execution: " + (endTime - startTime) + " ms");
    }


    // APPROACH #3: Distributed MapReduce
    {
      System.out.println("\n=== APPROACH #3: Distributed MapReduce ===");
      
      // We'll test different parameter configurations
      int[] linesPerMapThread = {1000, 2000, 5000, 10000};
      int[] wordsPerReduceThread = {100, 200, 500, 1000};
      
      for (int linesPerThread : linesPerMapThread) {
        for (int wordsPerThread : wordsPerReduceThread) {
          runDistributedMapReduce(input, linesPerThread, wordsPerThread);
        }
      }
    }
    
    long endTimeOverall = System.currentTimeMillis();
    System.out.println("\nTotal program execution time: " + (endTimeOverall - startTimeOverall) + " ms");
  }
  
  private static void runDistributedMapReduce(Map<String, String> input, int linesPerMapThread, int wordsPerReduceThread) {
    System.out.println("\n--- Testing with " + linesPerMapThread + " lines per map thread and " + 
                      wordsPerReduceThread + " words per reduce thread ---");
    long startTime = System.currentTimeMillis();
    
    final Map<String, Map<String, Integer>> output = new ConcurrentHashMap<>();

    // MAP PHASE:
    long mapStartTime = System.currentTimeMillis();
    final List<MappedItem> mappedItems = new CopyOnWriteArrayList<>();

    final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
      @Override
      public synchronized void mapDone(String id, List<MappedItem> results) {
        mappedItems.addAll(results);
      }
    };

    // Prepare all lines from all files
    List<LineChunk> lineChunks = new ArrayList<>();
    int chunkId = 0;
    
    for (Map.Entry<String, String> entry : input.entrySet()) {
      String file = entry.getKey();
      String contents = entry.getValue();
      
      // Split content into lines
      String[] lines = contents.split("\\r?\\n");
      
      // Process lines - split any that are too long
      List<String> processedLines = new ArrayList<>();
      for (String line : lines) {
        if (line.length() > MAX_LINE_LENGTH) {
          // Split at next whitespace after 80 chars
          int pos = MAX_LINE_LENGTH;
          while (pos < line.length() && !Character.isWhitespace(line.charAt(pos))) {
            pos++;
          }
          if (pos < line.length()) {
            processedLines.add(line.substring(0, pos));
            processedLines.add(line.substring(pos).trim());
          } else {
            processedLines.add(line);
          }
        } else {
          processedLines.add(line);
        }
      }
      
      // Group lines into chunks
      for (int i = 0; i < processedLines.size(); i += linesPerMapThread) {
        int end = Math.min(i + linesPerMapThread, processedLines.size());
        List<String> chunk = processedLines.subList(i, end);
        lineChunks.add(new LineChunk(chunkId++, file, chunk));
      }
    }
    
    // Create and start map threads
    List<Thread> mapCluster = new ArrayList<>();
    for (final LineChunk chunk : lineChunks) {
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          mapChunk(chunk, mapCallback);
        }
      });
      mapCluster.add(t);
      t.start();
    }

    // Wait for mapping phase to complete
    for (Thread t : mapCluster) {
      try {
        t.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    long mapEndTime = System.currentTimeMillis();

    // GROUP:
    long groupStartTime = System.currentTimeMillis();
    Map<String, List<String>> groupedItems = new HashMap<>();

    for (MappedItem item : mappedItems) {
      String word = item.getWord();
      String file = item.getFile();
      List<String> list = groupedItems.get(word);
      if (list == null) {
        list = new LinkedList<>();
        groupedItems.put(word, list);
      }
      list.add(file);
    }
    
    // Create batches for reduce phase
    List<ReduceBatch> reduceBatches = new ArrayList<>();
    int batchId = 0;
    List<String> currentBatchWords = new ArrayList<>();
    
    for (Map.Entry<String, List<String>> entry : groupedItems.entrySet()) {
      currentBatchWords.add(entry.getKey());
      
      if (currentBatchWords.size() >= wordsPerReduceThread) {
        reduceBatches.add(new ReduceBatch(batchId++, new ArrayList<>(currentBatchWords), groupedItems));
        currentBatchWords.clear();
      }
    }
    
    // Add any remaining words as the final batch
    if (!currentBatchWords.isEmpty()) {
      reduceBatches.add(new ReduceBatch(batchId++, currentBatchWords, groupedItems));
    }
    long groupEndTime = System.currentTimeMillis();

    // REDUCE:
    long reduceStartTime = System.currentTimeMillis();
    final ReduceCallback<Integer, String, Map<String, Integer>> reduceCallback = new ReduceCallback<Integer, String, Map<String, Integer>>() {
      @Override
      public synchronized void reduceDone(Integer batchId, Map<String, Map<String, Integer>> results) {
        output.putAll(results);
      }
    };

    List<Thread> reduceCluster = new ArrayList<>();
    
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

    // Wait for reducing phase to complete
    for (Thread t : reduceCluster) {
      try {
        t.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    long reduceEndTime = System.currentTimeMillis();

    long endTime = System.currentTimeMillis();
    System.out.println("Total words: " + output.size());
    System.out.println("Map phase (" + lineChunks.size() + " threads): " + (mapEndTime - mapStartTime) + " ms");
    System.out.println("Group phase: " + (groupEndTime - groupStartTime) + " ms");
    System.out.println("Reduce phase (" + reduceBatches.size() + " threads): " + (reduceEndTime - reduceStartTime) + " ms");
    System.out.println("Total execution: " + (endTime - startTime) + " ms");
  }
  
  // Helper class for line chunks
  private static class LineChunk {
    private final int id;
    private final String file;
    private final List<String> lines;
    
    public LineChunk(int id, String file, List<String> lines) {
      this.id = id;
      this.file = file;
      this.lines = lines;
    }
    
    public int getId() { return id; }
    public String getFile() { return file; }
    public List<String> getLines() { return lines; }
  }
  
  // Helper class for reduce batches
  private static class ReduceBatch {
    private final int id;
    private final List<String> words;
    private final Map<String, List<String>> groupedItems;
    
    public ReduceBatch(int id, List<String> words, Map<String, List<String>> groupedItems) {
      this.id = id;
      this.words = words;
      this.groupedItems = groupedItems;
    }
    
    public int getId() { return id; }
    public List<String> getWords() { return words; }
    public Map<String, List<String>> getGroupedItems() { return groupedItems; }
  }
  
  // Process a chunk of lines in map phase
  private static void mapChunk(LineChunk chunk, MapCallback<String, MappedItem> callback) {
    List<MappedItem> results = new ArrayList<>();
    String file = chunk.getFile();
    
    for (String line : chunk.getLines()) {
      String[] words = line.trim().split("\\s+");
      for (String word : words) {
        // Extract only the word part (no punctuation or numbers)
        Matcher matcher = WORD_PATTERN.matcher(word);
        if (matcher.find()) {
          word = matcher.group(1).toLowerCase();
          results.add(new MappedItem(word, file));
        }
      }
    }
    
    callback.mapDone("chunk-" + chunk.getId(), results);
  }
  
  // Process a batch of words in reduce phase
  private static void reduceBatch(ReduceBatch batch, ReduceCallback<Integer, String, Map<String, Integer>> callback) {
    Map<String, Map<String, Integer>> results = new HashMap<>();
    
    for (String word : batch.getWords()) {
      List<String> list = batch.getGroupedItems().get(word);
      
      Map<String, Integer> reducedList = new HashMap<>();
      for (String file : list) {
        Integer occurrences = reducedList.get(file);
        if (occurrences == null) {
          reducedList.put(file, 1);
        } else {
          reducedList.put(file, occurrences + 1);
        }
      }
      
      results.put(word, reducedList);
    }
    
    callback.reduceDone(Integer.valueOf(batch.getId()), results);
  }

  public static void map(String file, String contents, List<MappedItem> mappedItems) {
    String[] words = contents.trim().split("\\s+");
    for (String word : words) {
      // Extract only the word part (no punctuation or numbers)
      Matcher matcher = WORD_PATTERN.matcher(word);
      if (matcher.find()) {
        word = matcher.group(1).toLowerCase();
        mappedItems.add(new MappedItem(word, file));
      }
    }
  }

  public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
    Map<String, Integer> reducedList = new HashMap<String, Integer>();
    for (String file : list) {
      Integer occurrences = reducedList.get(file);
      if (occurrences == null) {
        reducedList.put(file, 1);
      } else {
        reducedList.put(file, occurrences.intValue() + 1);
      }
    }
    output.put(word, reducedList);
  }

  public static interface MapCallback<E, V> {
    public void mapDone(E key, List<V> values);
  }

  public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
    String[] words = contents.trim().split("\\s+");
    List<MappedItem> results = new ArrayList<MappedItem>(words.length);
    for (String word : words) {
      // Extract only the word part (no punctuation or numbers)
      Matcher matcher = WORD_PATTERN.matcher(word);
      if (matcher.find()) {
        word = matcher.group(1).toLowerCase();
        results.add(new MappedItem(word, file));
      }
    }
    callback.mapDone(file, results);
  }

  public static interface ReduceCallback<E, K, V> {
    public void reduceDone(E e, Map<K, V> results);
  }

  public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {
    Map<String, Integer> reducedList = new HashMap<String, Integer>();
    for (String file : list) {
      Integer occurrences = reducedList.get(file);
      if (occurrences == null) {
        reducedList.put(file, 1);
      } else {
        reducedList.put(file, occurrences.intValue() + 1);
      }
    }
    callback.reduceDone(word, reducedList);
  }

  private static class MappedItem {
    private final String word;
    private final String file;

    public MappedItem(String word, String file) {
      this.word = word;
      this.file = file;
    }

    public String getWord() {
      return word;
    }

    public String getFile() {
      return file;
    }

    @Override
    public String toString() {
      return "[\"" + word + "\",\"" + file + "\"]";
    }
  }

  private static String readFile(String pathname) throws IOException {
    File file = new File(pathname);
    StringBuilder fileContents = new StringBuilder((int) file.length());
    Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
    String lineSeparator = System.getProperty("line.separator");

    try {
      if (scanner.hasNextLine()) {
        fileContents.append(scanner.nextLine());
      }
      while (scanner.hasNextLine()) {
        fileContents.append(lineSeparator + scanner.nextLine());
      }
      return fileContents.toString();
    } finally {
      scanner.close();
    }
  }
}