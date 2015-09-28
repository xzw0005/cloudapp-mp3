
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
//import backtype.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;

  // extra
  private FileReader fileReader;
  private boolean isCompleted = false;

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader
    ------------------------------------------------- */
	String fileName = conf.get("inputFile").toString();
	try {
		//FileReader fileReader = new FileReader(fileName);
		fileReader = new FileReader(fileName);
	}
	catch (FileNotFoundException e) {}
	
	// END TODO

    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */
	if (isCompleted) {
		try {
			//Utils.sleep(100);
			Thread.sleep(1000);
		}
		catch (InterruptedException e) {}
	}
	
	
	BufferedReader bufferedReader = new BufferedReader(fileReader);
	String line;
	
	try {
		while ((line = bufferedReader.readLine()) != null) {
			line = line.trim();
			if (line.length() > 0) {
				_collector.emit(new Values(line));
			}
		}
	}
	catch (IOException e) {
		e.printStackTrace();
	}
	finally {
		isCompleted = true;
	}	
	// END TODO
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file
    ------------------------------------------------- */
	try {
		fileReader.close();
	}
	catch (IOException e) {
		e.printStackTrace();
	} 
	
	// END TODO
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
