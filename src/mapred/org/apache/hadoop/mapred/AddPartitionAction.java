package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents a directive from the {@link org.apache.hadoop.mapred.JobTracker}
 * to the {@link org.apache.hadoop.mapred.TaskTracker} to add specified new 
 * partitions into a running {@link org.apache.hadoop.mapred.ReduceTask} 
 * @author youli
 *
 */
public class AddPartitionAction extends TaskTrackerAction {

	private TaskAttemptID taskId;
	private boolean finished = false;
	private int[] parts;
	public AddPartitionAction() {
		super(TaskTrackerAction.ActionType.ADD_PARTITION);
	}
	public AddPartitionAction(TaskAttemptID taskID, int[] pts, boolean finish){
		super(TaskTrackerAction.ActionType.ADD_PARTITION);
		this.taskId = taskID;
		this.parts = pts;//这里要不要克隆下?
		this.finished = finish;
	}
	public TaskAttemptID getTaskId() {
		return taskId;
	}
	public void setTaskId(TaskAttemptID taskId) {
		this.taskId = taskId;
	}
	public int[] getParts() {
		return parts;
	}
	public void setParts(int[] parts) {
		this.parts = parts;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		taskId.write(out);
		out.writeBoolean(finished);
		out.writeInt(parts.length);
		for(int p : parts)
			out.writeInt(p);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		taskId = new TaskAttemptID();
		taskId.readFields(in);
		this.finished = in.readBoolean();
		int len = in.readInt();
		parts = new int[len];
		for(int i  = 0; i < len; i++){
			parts[i] = in.readInt();
		}
	}
	public boolean isFinished() {
		return finished;
	}
	public void setFinished(boolean finished) {
		this.finished = finished;
	}
	

}
