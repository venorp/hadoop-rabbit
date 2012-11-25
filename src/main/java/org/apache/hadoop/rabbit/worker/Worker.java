package org.apache.hadoop.rabbit.worker;

public interface Worker {

	public void process(WorkDesc workDesc);
	
}
