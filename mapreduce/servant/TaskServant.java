package scs.demos.mapreduce.servant;

import java.util.Properties;
import java.util.Date;
import java.util.*;
import java.io.FileInputStream;
import java.io.IOException;

import scs.demos.mapreduce.TaskPOA;
import scs.demos.mapreduce.TaskStatus;
import scs.demos.mapreduce.Reporter;
import scs.demos.mapreduce.FileSplit;
import scs.demos.mapreduce.IOMapReduceException;

/**
	* Servant generico que implementa a interface scs::demos::mapreduce::Task
	* É especializado para encapsular tarefas map e reduce 
	* @author Sand Luz Correa
	*/

public abstract class TaskServant extends TaskPOA {

	private static int taskId = 0;
	protected FileSplit[] inputSplit = null;
	protected FileSplit[] outputSplit = null; 
	protected TaskStatus status = null;
	protected int id = 0;
	protected int index = -1;
	protected Properties conf = null;	
	protected Reporter reporter = null;
	protected String configFileName;
	protected String execNode;
	protected int workerId;
	protected long initTime;
	protected boolean kill;

	public TaskServant(String configFileName, Reporter reporter) throws IOException {
		try {
			conf = new Properties(); 
			conf.load(new FileInputStream(configFileName));
			this.reporter = reporter;
			this.configFileName = configFileName;
			this.index = -1;
			id = taskId++;
		} catch (IOException e) {
			throw e;
		}
	}
	
	public TaskServant(String configFileName, Reporter reporter, int index) throws IOException {
		try {
			conf = new Properties(); 
			conf.load(new FileInputStream(configFileName));
			this.reporter = reporter;
			this.configFileName = configFileName;
			this.index = index;
			id = taskId++;
		} catch (IOException e) {
			throw e;
		}
	}

	protected abstract boolean doRun() throws IOMapReduceException;

	public boolean run() throws IOMapReduceException {
		return doRun();
		
	}

	public int getId() {
		return id;
	}
	
	public int getIndex() {
		return index;
	}

	public void setStatus(TaskStatus status) {
		this.status = status;
	}

	public TaskStatus getStatus() {
		return status;
	}

	public void setNode(String execNode) {
		this.execNode = execNode;
	}

	public String getNode() {
		return execNode;
	}

	public void setWorkerId(int workerId) {
		this.workerId = workerId;
	}

	public int getWorkerId() {
		return workerId;
	}

	public FileSplit[] getInput() {
		return inputSplit;
	}

	public FileSplit[] getOutput() {
		return outputSplit;
	}

	public Reporter getReporter() {
		return reporter;
	}
	
	public void setInitTime() {
		Date now = new Date();
		this.initTime = now.getTime();
	}

	public long getInitTime() {
		return this.initTime;
	}
	
	public void kill() {
		this.kill = true;
	}
}