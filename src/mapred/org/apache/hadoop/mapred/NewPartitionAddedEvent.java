package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class NewPartitionAddedEvent implements Writable {

	private boolean partitionArrangeFinished = false;
	private int events;
	public int getEvents() {
		return events;
	}
	public void setEvents(int events) {
		this.events = events;
	}

	private Integer[] addedPartitions;
	
	public NewPartitionAddedEvent(boolean partitionArrangeFinished,int events,
			Integer[] addedPartitions) {
		super();
		this.events = events;
		this.partitionArrangeFinished = partitionArrangeFinished;
		this.addedPartitions = addedPartitions;
	}
	public NewPartitionAddedEvent(){
		super();
		partitionArrangeFinished = false;
		events = 0;
		addedPartitions = new Integer[0];
	}
	public void setAddedPartitions(int[] pts){
		this.addedPartitions = new Integer[pts.length];
		System.arraycopy(pts, 0, addedPartitions, 0, pts.length);
	}
	public Integer[] getAddEdPartitions(){
		return this.addedPartitions;
	}
	public boolean partitionArrangeFinished() {
		return partitionArrangeFinished;
	}
	public void setPartitionArrangeFinished(boolean partitionArrangeFinished) {
		this.partitionArrangeFinished = partitionArrangeFinished;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(partitionArrangeFinished);
		out.writeInt(events);
		out.writeInt(this.addedPartitions.length);
		for(int i : addedPartitions){
			out.writeInt(i);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.partitionArrangeFinished = in.readBoolean();
		this.events = in.readInt();
		this.addedPartitions = new Integer[in.readInt()];
		for(int i = 0; i < addedPartitions.length; i++){
			this.addedPartitions[i] = in.readInt();
		}
	}

}
