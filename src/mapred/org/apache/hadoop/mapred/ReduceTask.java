/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.IFile.*;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterInt;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

/** A Reduce task. */
class ReduceTask extends Task {

	static { // register a ctor
		WritableFactories.setFactory(ReduceTask.class, new WritableFactory() {
			public Writable newInstance() {
				return new ReduceTask();
			}
		});
	}

	private static final Log LOG = LogFactory
			.getLog(ReduceTask.class.getName());
	private int numMaps;
	private ReduceCopier reduceCopier;

	/** 修改shuffle添加内容 **/
	private AtomicBoolean partitionArrangeFinished = new AtomicBoolean(false);
	private ArrayList<Integer> partitions = new ArrayList<Integer>();// 可能会有多个partition分配给同一个Task

	private CompressionCodec codec;

	{
		getProgress().setStatus("reduce");
		setPhase(TaskStatus.Phase.SHUFFLE); // phase to start with
	}

	private Progress copyPhase;
	private Progress sortPhase;
	private Progress reducePhase;
	private Counters.Counter reduceShuffleBytes = getCounters().findCounter(
			Counter.REDUCE_SHUFFLE_BYTES);
	private Counters.Counter reduceInputKeyCounter = getCounters().findCounter(
			Counter.REDUCE_INPUT_GROUPS);
	private Counters.Counter reduceInputValueCounter = getCounters()
			.findCounter(Counter.REDUCE_INPUT_RECORDS);
	private Counters.Counter reduceOutputCounter = getCounters().findCounter(
			Counter.REDUCE_OUTPUT_RECORDS);
	private Counters.Counter reduceCombineOutputCounter = getCounters()
			.findCounter(Counter.COMBINE_OUTPUT_RECORDS);

	// A custom comparator for map output files. Here the ordering is determined
	// by the file's size and path. In case of files with same size and
	// different
	// file paths, the first parameter is considered smaller than the second
	// one.
	// In case of files with same size and path are considered equal.
	private Comparator<FileStatus> mapOutputFileComparator = new Comparator<FileStatus>() {
		public int compare(FileStatus a, FileStatus b) {
			if (a.getLen() < b.getLen())
				return -1;
			else if (a.getLen() == b.getLen())
				if (a.getPath().toString().equals(b.getPath().toString()))
					return 0;
				else
					return -1;
			else
				return 1;
		}
	};

	// A sorted set for keeping a set of map output files on disk
	private final SortedSet<FileStatus> mapOutputFilesOnDisk = new TreeSet<FileStatus>(
			mapOutputFileComparator);
	private final Map<Integer, SortedSet<FileStatus>> partitionToMapOutputFilesOnDisk = new HashMap<Integer, SortedSet<FileStatus>>();
	private final Map<Integer, SortedSet<FileStatus>> partitionToFinishedMapOutputFilesOnDisk = new HashMap<Integer, SortedSet<FileStatus>>();
	private final Map<Integer, Boolean> partitionToCopyFinished = Collections.synchronizedMap(new HashMap<Integer, Boolean>());
	private final List<Integer> shuffleFinishedPartition = Collections
			.synchronizedList(new LinkedList<Integer>());
	private final Map<Integer, AtomicInteger> partitionToNumMergeThread = new HashMap<Integer, AtomicInteger>();
	private final Object reduceLock = new Object();

	public ReduceTask() {
		super();
	}

	public ReduceTask(String jobFile, TaskAttemptID taskId, int partition,
			int numMaps, int numSlotsRequired) {
		super(jobFile, taskId, partition, numSlotsRequired);
		this.numMaps = numMaps;
		this.partitions.add(partition);
		partitionToMapOutputFilesOnDisk.put(partition, new TreeSet<FileStatus>(
				mapOutputFileComparator));
		partitionToNumMergeThread.put(partition, new AtomicInteger());
		partitionToCopyFinished.put(partition, false);
	}

	private CompressionCodec initCodec() {
		// check if map-outputs are to be compressed
		if (conf.getCompressMapOutput()) {
			Class<? extends CompressionCodec> codecClass = conf
					.getMapOutputCompressorClass(DefaultCodec.class);
			return ReflectionUtils.newInstance(codecClass, conf);
		}

		return null;
	}

	/**
	 * 获取当前已经指派的partition
	 * 
	 * @return
	 */
	public synchronized List<Integer> getPartitions() {
		return new ArrayList<Integer>(this.partitions);
	}

	/**
	 * 新增加partition，并唤醒等待在partitions上添加新的MapOutputLocation
	 * 
	 * @param partition
	 */
	public void addNewPartitions(Integer[] parts) {
		synchronized (partitions) {
			for (int p : parts) {
				if (partitions.contains(p)) {
					LOG.error("dump partition:" + p);
				}
				partitions.add(p);

				partitionToMapOutputFilesOnDisk.put(p, new TreeSet<FileStatus>(
						mapOutputFileComparator));
				partitionToNumMergeThread.put(p, new AtomicInteger());
				partitionToCopyFinished.put(p, false);
			}

			// 监控copy进度
			for (int i = 0; i < numMaps * parts.length; i++) {
				copyPhase.addPhase(); // add sub-phase per file
			}

			partitions.notifyAll();
		}
	}

	@Override
	public TaskRunner createRunner(TaskTracker tracker, TaskInProgress tip,
			TaskTracker.RunningJob rjob) throws IOException {
		return new ReduceTaskRunner(tip, tracker, this.conf, rjob);
	}

	@Override
	public boolean isMapTask() {
		return false;
	}

	public int getNumMaps() {
		return numMaps;
	}

	/**
	 * Localize the given JobConf to be specific for this task.
	 */
	@Override
	public void localizeConfiguration(JobConf conf) throws IOException {
		super.localizeConfiguration(conf);
		conf.setNumMapTasks(numMaps);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(numMaps); // write the number of maps
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		numMaps = in.readInt();

		this.partitions.add(getPartition());
		partitionToMapOutputFilesOnDisk.put(getPartition(),
				new TreeSet<FileStatus>(mapOutputFileComparator));
		partitionToNumMergeThread.put(getPartition(), new AtomicInteger());
		partitionToCopyFinished.put(getPartition(), false);
	}

	// Get the input files for the reducer.
	private Path[] getMapFiles(FileSystem fs, boolean isLocal)
			throws IOException {
		List<Path> fileList = new ArrayList<Path>();
		if (isLocal) {
			// for local jobs
			for (int i = 0; i < numMaps; ++i) {
				fileList.add(mapOutputFile.getInputFile(i));
			}
		} else {
			// for non local jobs
			for (FileStatus filestatus : mapOutputFilesOnDisk) {
				fileList.add(filestatus.getPath());
			}
		}
		return fileList.toArray(new Path[0]);
	}

	private Path[] getMapFiles(int part, FileSystem fs, boolean isLocal)
			throws IOException {
		List<Path> fileList = new ArrayList<Path>();
		if (isLocal) {
			// for local jobs
			for (int i = 0; i < numMaps; ++i) {
				fileList.add(mapOutputFile.getInputFile(i));
			}
		} else {
			SortedSet<FileStatus> myMapOutputFilesOnDisk = partitionToFinishedMapOutputFilesOnDisk
					.remove(part);
			synchronized (myMapOutputFilesOnDisk) {
				for (FileStatus filestatus : myMapOutputFilesOnDisk) {
					fileList.add(filestatus.getPath());
					if (mapOutputFilesOnDisk.remove(filestatus))
						LOG.error("mapOutputFilesOnDisk should not contain any element of the reducing output");
				}
			}

		}
		return fileList.toArray(new Path[0]);
	}

	private class ReduceValuesIterator<KEY, VALUE> extends
			ValuesIterator<KEY, VALUE> {
		public ReduceValuesIterator(RawKeyValueIterator in,
				RawComparator<KEY> comparator, Class<KEY> keyClass,
				Class<VALUE> valClass, Configuration conf, Progressable reporter)
				throws IOException {
			super(in, comparator, keyClass, valClass, conf, reporter);
		}

		@Override
		public VALUE next() {
			reduceInputValueCounter.increment(1);
			return moveToNext();
		}

		protected VALUE moveToNext() {
			return super.next();
		}

		public void informReduceProgress() {
			reducePhase.set(super.in.getProgress().get()); // update progress
			reporter.progress();
		}
	}

	private class SkippingReduceValuesIterator<KEY, VALUE> extends
			ReduceValuesIterator<KEY, VALUE> {
		private SkipRangeIterator skipIt;
		private TaskUmbilicalProtocol umbilical;
		private Counters.Counter skipGroupCounter;
		private Counters.Counter skipRecCounter;
		private long grpIndex = -1;
		private Class<KEY> keyClass;
		private Class<VALUE> valClass;
		private SequenceFile.Writer skipWriter;
		private boolean toWriteSkipRecs;
		private boolean hasNext;
		private TaskReporter reporter;

		public SkippingReduceValuesIterator(RawKeyValueIterator in,
				RawComparator<KEY> comparator, Class<KEY> keyClass,
				Class<VALUE> valClass, Configuration conf,
				TaskReporter reporter, TaskUmbilicalProtocol umbilical)
				throws IOException {
			super(in, comparator, keyClass, valClass, conf, reporter);
			this.umbilical = umbilical;
			this.skipGroupCounter = reporter
					.getCounter(Counter.REDUCE_SKIPPED_GROUPS);
			this.skipRecCounter = reporter
					.getCounter(Counter.REDUCE_SKIPPED_RECORDS);
			this.toWriteSkipRecs = toWriteSkipRecs()
					&& SkipBadRecords.getSkipOutputPath(conf) != null;
			this.keyClass = keyClass;
			this.valClass = valClass;
			this.reporter = reporter;
			skipIt = getSkipRanges().skipRangeIterator();
			mayBeSkip();
		}

		void nextKey() throws IOException {
			super.nextKey();
			mayBeSkip();
		}

		boolean more() {
			return super.more() && hasNext;
		}

		private void mayBeSkip() throws IOException {
			hasNext = skipIt.hasNext();
			if (!hasNext) {
				LOG.warn("Further groups got skipped.");
				return;
			}
			grpIndex++;
			long nextGrpIndex = skipIt.next();
			long skip = 0;
			long skipRec = 0;
			while (grpIndex < nextGrpIndex && super.more()) {
				while (hasNext()) {
					VALUE value = moveToNext();
					if (toWriteSkipRecs) {
						writeSkippedRec(getKey(), value);
					}
					skipRec++;
				}
				super.nextKey();
				grpIndex++;
				skip++;
			}

			// close the skip writer once all the ranges are skipped
			if (skip > 0 && skipIt.skippedAllRanges() && skipWriter != null) {
				skipWriter.close();
			}
			skipGroupCounter.increment(skip);
			skipRecCounter.increment(skipRec);
			reportNextRecordRange(umbilical, grpIndex);
		}

		@SuppressWarnings("unchecked")
		private void writeSkippedRec(KEY key, VALUE value) throws IOException {
			if (skipWriter == null) {
				Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
				Path skipFile = new Path(skipDir, getTaskID().toString());
				skipWriter = SequenceFile.createWriter(
						skipFile.getFileSystem(conf), conf, skipFile, keyClass,
						valClass, CompressionType.BLOCK, reporter);
			}
			skipWriter.append(key, value);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
			throws IOException, InterruptedException, ClassNotFoundException {
		this.umbilical = umbilical;
		job.setBoolean("mapred.skip.on", isSkipping());

		if (isMapOrReduce()) {
			copyPhase = getProgress().addPhase("copy");
			sortPhase = getProgress().addPhase("sort");
			reducePhase = getProgress().addPhase("reduce");
		}
		// start thread that will handle communication with parent
		TaskReporter reporter = new TaskReporter(getProgress(), umbilical,
				jvmContext);
		reporter.startCommunicationThread();
		boolean useNewApi = job.getUseNewReducer();
		initialize(job, getJobID(), reporter, useNewApi);

		// check if it is a cleanupJobTask
		if (jobCleanup) {
			runJobCleanupTask(umbilical, reporter);
			return;
		}
		if (jobSetup) {
			runJobSetupTask(umbilical, reporter);
			return;
		}
		if (taskCleanup) {
			runTaskCleanupTask(umbilical, reporter);
			return;
		}

		// Initialize the codec
		codec = initCodec();

		boolean isLocal = "local"
				.equals(job.get("mapred.job.tracker", "local"));
		if (!isLocal) {
			reduceCopier = new ReduceCopier(umbilical, job, reporter);
			reduceCopier.start();
		}
		if (isLocal) {
			copyPhase.complete(); // copy is already complete
			setPhase(TaskStatus.Phase.SORT);
			statusUpdate(umbilical);
			final FileSystem rfs = FileSystem.getLocal(job).getRaw();
			RawKeyValueIterator rIter = Merger.merge(job, rfs, job
					.getMapOutputKeyClass(), job.getMapOutputValueClass(),
					codec, getMapFiles(rfs, true), !conf
							.getKeepFailedTaskFiles(), job.getInt(
							"io.sort.factor", 100), new Path(getTaskID()
							.toString()), job.getOutputKeyComparator(),
					reporter, spilledRecordsCounter, null);
			mapOutputFilesOnDisk.clear();

			sortPhase.complete(); // sort is complete
			setPhase(TaskStatus.Phase.REDUCE);
			statusUpdate(umbilical);
			Class keyClass = job.getMapOutputKeyClass();
			Class valueClass = job.getMapOutputValueClass();
			RawComparator comparator = job.getOutputValueGroupingComparator();

			if (useNewApi) {
				runNewReducer(job, umbilical, reporter, rIter, comparator,
						keyClass, valueClass, -1);
			} else {
				runOldReducer(job, umbilical, reporter, rIter, comparator,
						keyClass, valueClass);
			}
		} else {
			// 进度控制先不管
			int finished = 0;
			final FileSystem rfs = FileSystem.getLocal(job).getRaw();
			// 这里的循环条件判断与getMapComplemetionEvent处理newpartitionAddedEvent的设置顺序相关才不会出现同步错误
			int firstDumpPartition = -1;
			LOG.info("main run start doing reduce");
			while (!partitionArrangeFinished.get()
					|| finished < getPartitions().size()) {
				int finishedPartition = -1;
				int numMergeThread;
				synchronized (shuffleFinishedPartition) {
					// 每次取出一个完成的，以免处理时间过长，而同步影响原有工作
					if (shuffleFinishedPartition.isEmpty())
						shuffleFinishedPartition.wait();
					finishedPartition = shuffleFinishedPartition.remove(0);
				}
				synchronized (reduceLock) {
					numMergeThread = partitionToNumMergeThread.get(
							finishedPartition).get();
				}
				if (numMergeThread > 0) {
					// 还有mergethread在这个partition上做merge暂时还不能做reduce
					// 将这个partition加到队列尾部
					synchronized (shuffleFinishedPartition) {
						shuffleFinishedPartition.add(
								shuffleFinishedPartition.size(),
								finishedPartition);
						if (firstDumpPartition == -1)// 以防不停地循环而始终占用reduceLock
							firstDumpPartition = finishedPartition;
						else if (finishedPartition == firstDumpPartition) {
							firstDumpPartition = -1;
							shuffleFinishedPartition.wait(1000);
						}
						continue;
					}
				}
				firstDumpPartition = -1;
				if (finishedPartition >= 0) {
					LOG.info("start doing reduce() for partition:"
							+ finishedPartition);
					// reduceCopier.createKVIterator中要负责把使用过的partition的数据清理掉，这里不再清理
					RawKeyValueIterator rIter = reduceCopier.createKVIterator(
							finishedPartition, job, rfs, reporter);
					Class keyClass = job.getMapOutputKeyClass();
					Class valueClass = job.getMapOutputValueClass();
					RawComparator comparator = job
							.getOutputValueGroupingComparator();
					// 还要再改改runNewReducer和runOldReducer让他们不要都输出到同一个文件
					if (useNewApi) {
						runNewReducer(job, umbilical, reporter, rIter,
								comparator, keyClass, valueClass,
								finishedPartition);
					} else {
						runOldReducer(job, umbilical, reporter, rIter,
								comparator, keyClass, valueClass);
					}
					finished++;// 记录已经处理了多少个partition
				}

			}
			// 最后这里还要把几个partition的输出结果再次merge成一个输出文件

		}
		done(umbilical, reporter);
	}

	private class OldTrackingRecordWriter<K, V> implements RecordWriter<K, V> {

		private final RecordWriter<K, V> real;
		private final org.apache.hadoop.mapred.Counters.Counter outputRecordCounter;
		private final org.apache.hadoop.mapred.Counters.Counter fileOutputByteCounter;
		private final Statistics fsStats;

		public OldTrackingRecordWriter(
				org.apache.hadoop.mapred.Counters.Counter outputRecordCounter,
				JobConf job, TaskReporter reporter, String finalName)
				throws IOException {
			this.outputRecordCounter = outputRecordCounter;
			this.fileOutputByteCounter = reporter
					.getCounter(FileOutputFormat.Counter.BYTES_WRITTEN);
			Statistics matchedStats = null;
			if (job.getOutputFormat() instanceof FileOutputFormat) {
				matchedStats = getFsStatistics(
						FileOutputFormat.getOutputPath(job), job);
			}
			fsStats = matchedStats;

			FileSystem fs = FileSystem.get(job);
			long bytesOutPrev = getOutputBytes(fsStats);
			this.real = job.getOutputFormat().getRecordWriter(fs, job,
					finalName, reporter);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		}

		@Override
		public void write(K key, V value) throws IOException {
			long bytesOutPrev = getOutputBytes(fsStats);
			real.write(key, value);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
			outputRecordCounter.increment(1);
		}

		@Override
		public void close(Reporter reporter) throws IOException {
			long bytesOutPrev = getOutputBytes(fsStats);
			real.close(reporter);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		}

		private long getOutputBytes(Statistics stats) {
			return stats == null ? 0 : stats.getBytesWritten();
		}
	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runOldReducer(JobConf job,
			TaskUmbilicalProtocol umbilical, final TaskReporter reporter,
			RawKeyValueIterator rIter, RawComparator<INKEY> comparator,
			Class<INKEY> keyClass, Class<INVALUE> valueClass)
			throws IOException {
		Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = ReflectionUtils
				.newInstance(job.getReducerClass(), job);
		// make output collector
		String finalName = getOutputName(getPartition());

		final RecordWriter<OUTKEY, OUTVALUE> out = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, finalName);

		OutputCollector<OUTKEY, OUTVALUE> collector = new OutputCollector<OUTKEY, OUTVALUE>() {
			public void collect(OUTKEY key, OUTVALUE value) throws IOException {
				out.write(key, value);
				// indicate that progress update needs to be sent
				reporter.progress();
			}
		};

		// apply reduce function
		try {
			// increment processed counter only if skipping feature is enabled
			boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job) > 0
					&& SkipBadRecords.getAutoIncrReducerProcCount(job);

			ReduceValuesIterator<INKEY, INVALUE> values = isSkipping() ? new SkippingReduceValuesIterator<INKEY, INVALUE>(
					rIter, comparator, keyClass, valueClass, job, reporter,
					umbilical) : new ReduceValuesIterator<INKEY, INVALUE>(
					rIter, job.getOutputValueGroupingComparator(), keyClass,
					valueClass, job, reporter);
			values.informReduceProgress();
			while (values.more()) {
				reduceInputKeyCounter.increment(1);
				reducer.reduce(values.getKey(), values, collector, reporter);
				if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}
				values.nextKey();
				values.informReduceProgress();
			}

			// Clean up: repeated in catch block below
			reducer.close();
			out.close(reporter);
			// End of clean up.
		} catch (IOException ioe) {
			try {
				reducer.close();
			} catch (IOException ignored) {
			}

			try {
				out.close(reporter);
			} catch (IOException ignored) {
			}

			throw ioe;
		}
	}

	private class NewTrackingRecordWriter<K, V> extends
			org.apache.hadoop.mapreduce.RecordWriter<K, V> {
		private final org.apache.hadoop.mapreduce.RecordWriter<K, V> real;
		private final org.apache.hadoop.mapreduce.Counter outputRecordCounter;
		private final org.apache.hadoop.mapreduce.Counter fileOutputByteCounter;
		private final Statistics fsStats;

		NewTrackingRecordWriter(
				org.apache.hadoop.mapreduce.Counter recordCounter, JobConf job,
				TaskReporter reporter,
				org.apache.hadoop.mapreduce.TaskAttemptContext taskContext,
				int part) throws InterruptedException, IOException {
			this.outputRecordCounter = recordCounter;
			this.fileOutputByteCounter = reporter
					.getCounter(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.Counter.BYTES_WRITTEN);
			Statistics matchedStats = null;
			// TaskAttemptContext taskContext = new TaskAttemptContext(job,
			// getTaskID());
			if (outputFormat instanceof org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
				matchedStats = getFsStatistics(
						org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
								.getOutputPath(taskContext),
						taskContext.getConfiguration());
			}
			fsStats = matchedStats;

			long bytesOutPrev = getOutputBytes(fsStats);
			this.real = (org.apache.hadoop.mapreduce.RecordWriter<K, V>) outputFormat
					.getRecordWriter(taskContext, part);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			long bytesOutPrev = getOutputBytes(fsStats);
			real.close(context);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			long bytesOutPrev = getOutputBytes(fsStats);
			real.write(key, value);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
			outputRecordCounter.increment(1);
		}

		private long getOutputBytes(Statistics stats) {
			return stats == null ? 0 : stats.getBytesWritten();
		}
	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runNewReducer(JobConf job,
			final TaskUmbilicalProtocol umbilical, final TaskReporter reporter,
			RawKeyValueIterator rIter, RawComparator<INKEY> comparator,
			Class<INKEY> keyClass, Class<INVALUE> valueClass, int part)
			throws IOException, InterruptedException, ClassNotFoundException {
		// wrap value iterator to report progress.
		final RawKeyValueIterator rawIter = rIter;
		rIter = new RawKeyValueIterator() {
			public void close() throws IOException {
				rawIter.close();
			}

			public DataInputBuffer getKey() throws IOException {
				return rawIter.getKey();
			}

			public Progress getProgress() {
				return rawIter.getProgress();
			}

			public DataInputBuffer getValue() throws IOException {
				return rawIter.getValue();
			}

			public boolean next() throws IOException {
				boolean ret = rawIter.next();
				reducePhase.set(rawIter.getProgress().get());
				reporter.progress();
				return ret;
			}
		};
		// make a task context so we can get the classes
		org.apache.hadoop.mapreduce.TaskAttemptContext taskContext = new org.apache.hadoop.mapreduce.TaskAttemptContext(
				job, getTaskID());
		// make a reducer
		org.apache.hadoop.mapreduce.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = (org.apache.hadoop.mapreduce.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>) ReflectionUtils
				.newInstance(taskContext.getReducerClass(), job);
		org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE> trackedRW = new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, taskContext, part);
		job.setBoolean("mapred.skip.on", isSkipping());
		org.apache.hadoop.mapreduce.Reducer.Context reducerContext = createReduceContext(
				reducer, job, getTaskID(), rIter, reduceInputKeyCounter,
				reduceInputValueCounter, trackedRW, committer, reporter,
				comparator, keyClass, valueClass);
		reducer.run(reducerContext);
		trackedRW.close(reducerContext);
	}

	private static enum CopyOutputErrorType {
		NO_ERROR, READ_ERROR, OTHER_ERROR
	};

	private boolean copiedAllPartitions = false;
	private Throwable copyException = null;

	class ReduceCopier<K, V> extends Thread implements MRConstants {

		/** Reference to the umbilical object */
		private TaskUmbilicalProtocol umbilical;
		private final TaskReporter reporter;

		/** Reference to the task object */

		/** Number of ms before timing out a copy */
		private static final int STALLED_COPY_TIMEOUT = 3 * 60 * 1000;

		/** Max events to fetch in one go from the tasktracker */
		private static final int MAX_EVENTS_TO_FETCH = 10000;

		/**
		 * our reduce task instance
		 */
		private ReduceTask reduceTask;

		/**
		 * the list of map outputs currently being copied
		 */
		private List<MapOutputLocation> scheduledCopies;

		/**
		 * the results of dispatched copy attempts
		 */
		private List<CopyResult> copyResults;
		private Map<Integer, List<CopyResult>> partitionToCopyResults;

		int numEventsFetched = 0;
		private Object copyResultsOrNewEventsLock = new Object();

		/**
		 * the number of outputs to copy in parallel
		 */
		private int numCopiers;

		/**
		 * a number that is set to the max #fetches we'd schedule and then pause
		 * the schduling
		 */
		private int maxInFlight;

		/**
		 * busy hosts from which copies are being backed off Map of host -> next
		 * contact time
		 */
		private Map<String, Long> penaltyBox;

		/**
		 * the set of unique hosts from which we are copying
		 */
		private Set<String> uniqueHosts;

		/**
		 * A reference to the RamManager for writing the map outputs to.
		 */

		private ShuffleRamManager ramManager;

		/**
		 * A reference to the local file system for writing the map outputs to.
		 */
		private FileSystem localFileSys;

		private FileSystem rfs;
		/**
		 * Number of files to merge at a time
		 */
		private int ioSortFactor;

		/**
		 * A reference to the throwable object (if merge throws an exception)
		 */
		private volatile Throwable mergeThrowable;

		/**
		 * A flag to indicate when to exit localFS merge
		 */
		private volatile boolean exitLocalFSMerge = false;

		/**
		 * A flag to indicate when to exit getMapEvents thread
		 */
		private volatile boolean exitGetMapEvents = false;

		/**
		 * When we accumulate maxInMemOutputs number of files in ram, we
		 * merge/spill
		 */
		private final int maxInMemOutputs;

		/**
		 * Usage threshold for in-memory output accumulation.
		 */
		private final float maxInMemCopyPer;

		/**
		 * Maximum memory usage of map outputs to merge from memory into the
		 * reduce, in bytes.
		 */
		private final long maxInMemReduce;

		/**
		 * The threads for fetching the files.
		 */
		private List<MapOutputCopier> copiers = null;

		/**
		 * The object for metrics reporting.
		 */
		private ShuffleClientInstrumentation shuffleClientMetrics;

		/**
		 * the minimum interval between tasktracker polls
		 */
		private static final long MIN_POLL_INTERVAL = 1000;

		/**
		 * a list of map output locations for fetch retrials
		 */
		private List<MapOutputLocation> retryFetches = new ArrayList<MapOutputLocation>();

		/**
		 * The set of required map outputs
		 */
		private Set<MapOutputLocation> copiedMapOutputs = Collections
				.synchronizedSet(new TreeSet<MapOutputLocation>());
		private Map<Integer, Set<MapOutputLocation>> partitionToCopiedMapOutputs = new HashMap<Integer, Set<MapOutputLocation>>();
		/**
		 * The set of obsolete map taskids.
		 */
		private Set<TaskAttemptID> obsoleteMapIds = Collections
				.synchronizedSet(new TreeSet<TaskAttemptID>());

		private Random random = null;

		/**
		 * the max of all the map completion times
		 */
		private int maxMapRuntime;

		/**
		 * Maximum number of fetch-retries per-map before reporting it.
		 */
		private int maxFetchFailuresBeforeReporting;

		/**
		 * Maximum number of fetch failures before reducer aborts.
		 */
		private final int abortFailureLimit;

		/**
		 * Initial penalty time in ms for a fetch failure.
		 */
		private static final long INITIAL_PENALTY = 10000;

		/**
		 * Penalty growth rate for each fetch failure.
		 */
		private static final float PENALTY_GROWTH_RATE = 1.3f;

		/**
		 * Default limit for maximum number of fetch failures before reporting.
		 */
		private final static int REPORT_FAILURE_LIMIT = 10;

		/**
		 * Combiner runner, if a combiner is needed
		 */
		private CombinerRunner combinerRunner;

		/**
		 * Resettable collector used for combine.
		 */
		private CombineOutputCollector combineCollector = null;

		/**
		 * Maximum percent of failed fetch attempt before killing the reduce
		 * task.
		 */
		private static final float MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT = 0.5f;

		/**
		 * Minimum percent of progress required to keep the reduce alive.
		 */
		private static final float MIN_REQUIRED_PROGRESS_PERCENT = 0.5f;

		/**
		 * Maximum percent of shuffle execution time required to keep the
		 * reducer alive.
		 */
		private static final float MAX_ALLOWED_STALL_TIME_PERCENT = 0.5f;

		/**
		 * Minimum number of map fetch retries.
		 */
		private static final int MIN_FETCH_RETRIES_PER_MAP = 2;

		/**
		 * The minimum percentage of maps yet to be copied, which indicates end
		 * of shuffle
		 */
		private static final float MIN_PENDING_MAPS_PERCENT = 0.25f;
		/**
		 * Maximum no. of unique maps from which we failed to fetch map-outputs
		 * even after {@link #maxFetchRetriesPerMap} retries; after this the
		 * reduce task is failed.
		 */
		private int maxFailedUniqueFetches = 5;

		/**
		 * The maps from which we fail to fetch map-outputs even after
		 * {@link #maxFetchRetriesPerMap} retries.
		 */
		Set<TaskID> fetchFailedMaps = new TreeSet<TaskID>();

		/**
		 * A map of taskId -> no. of failed fetches
		 */
		Map<TaskAttemptID, Integer> mapTaskToFailedFetchesMap = new HashMap<TaskAttemptID, Integer>();

		/**
		 * Initial backoff interval (milliseconds)
		 */
		private static final int BACKOFF_INIT = 4000;

		/**
		 * The interval for logging in the shuffle
		 */
		private static final int MIN_LOG_TIME = 60000;

		/**
		 * List of in-memory map-outputs.
		 */
		private final List<MapOutput> mapOutputsFilesInMemory = Collections
				.synchronizedList(new LinkedList<MapOutput>());
		private final Map<Integer, List<MapOutput>> partitionToMapOutputFilesInMemory = new HashMap<Integer, List<MapOutput>>();
		private final Map<Integer, List<MapOutput>> partitionToFinishedMapOutputFilesInMemory = new HashMap<Integer, List<MapOutput>>();
		/** 修改动态shuffle内容 **/
		private final Map<Integer, Map<String, List<MapOutputLocation>>> partitionToMapLocations = new HashMap<Integer, Map<String, List<MapOutputLocation>>>();

		/**
		 * The map for (Hosts, List of MapIds from this Host) maintaining map
		 * output locations
		 */
		private final Map<String, List<MapOutputLocation>> mapLocations = new ConcurrentHashMap<String, List<MapOutputLocation>>();
		/**persistentMapLocation只要保存一份TaskID-host-URL*/
		private final Map<TaskAttemptID, MapOutputLocation> persistentMapLocations = new ConcurrentHashMap<TaskAttemptID, MapOutputLocation>();

		class ShuffleClientInstrumentation implements MetricsSource {
			final MetricsRegistry registry = new MetricsRegistry("shuffleInput");
			final MetricMutableCounterLong inputBytes = registry.newCounter(
					"shuffle_input_bytes", "", 0L);
			final MetricMutableCounterInt failedFetches = registry.newCounter(
					"shuffle_failed_fetches", "", 0);
			final MetricMutableCounterInt successFetches = registry.newCounter(
					"shuffle_success_fetches", "", 0);
			private volatile int threadsBusy = 0;

			@SuppressWarnings("deprecation")
			ShuffleClientInstrumentation(JobConf conf) {
				registry.tag("user", "User name", conf.getUser())
						.tag("jobName", "Job name", conf.getJobName())
						.tag("jobId", "Job ID",
								ReduceTask.this.getJobID().toString())
						.tag("taskId", "Task ID", getTaskID().toString())
						.tag("sessionId", "Session ID", conf.getSessionId());
			}

			// @Override
			void inputBytes(long numBytes) {
				inputBytes.incr(numBytes);
			}

			// @Override
			void failedFetch() {
				failedFetches.incr();
			}

			// @Override
			void successFetch() {
				successFetches.incr();
			}

			// @Override
			synchronized void threadBusy() {
				++threadsBusy;
			}

			// @Override
			synchronized void threadFree() {
				--threadsBusy;
			}

			@Override
			public void getMetrics(MetricsBuilder builder, boolean all) {
				MetricsRecordBuilder rb = builder.addRecord(registry.name());
				rb.addGauge("shuffle_fetchers_busy_percent", "",
						numCopiers == 0 ? 0 : 100. * threadsBusy / numCopiers);
				registry.snapshot(rb, all);
			}

		}

		private ShuffleClientInstrumentation createShuffleClientInstrumentation() {
			return DefaultMetricsSystem.INSTANCE.register(
					"ShuffleClientMetrics", "Shuffle input metrics",
					new ShuffleClientInstrumentation(conf));
		}

		/** Represents the result of an attempt to copy a map output */
		private class CopyResult {

			// the map output location against which a copy attempt was made
			private final MapOutputLocation loc;

			// the size of the file copied, -1 if the transfer failed
			private final long size;

			// a flag signifying whether a copy result is obsolete
			private static final int OBSOLETE = -2;

			private CopyOutputErrorType error = CopyOutputErrorType.NO_ERROR;

			CopyResult(MapOutputLocation loc, long size) {
				this.loc = loc;
				this.size = size;
			}

			CopyResult(MapOutputLocation loc, long size,
					CopyOutputErrorType error) {
				this.loc = loc;
				this.size = size;
				this.error = error;
			}

			public boolean getSuccess() {
				return size >= 0;
			}

			public boolean isObsolete() {
				return size == OBSOLETE;
			}

			public long getSize() {
				return size;
			}

			public String getHost() {
				return loc.getHost();
			}

			public MapOutputLocation getLocation() {
				return loc;
			}

			public CopyOutputErrorType getError() {
				return error;
			}
		}

		private int nextMapOutputCopierId = 0;
		private boolean reportReadErrorImmediately;

		/**
		 * Abstraction to track a map-output.
		 */
		private class MapOutputLocation implements Comparable {
			TaskAttemptID taskAttemptId;
			TaskID taskId;
			String ttHost;
			URL taskOutput;

			public MapOutputLocation(TaskAttemptID taskAttemptId,
					String ttHost, URL taskOutput) {
				this.taskAttemptId = taskAttemptId;
				this.taskId = this.taskAttemptId.getTaskID();
				this.ttHost = ttHost;
				this.taskOutput = taskOutput;
			}

			public boolean equals(Object o) {
				if (!(o instanceof ReduceTask.ReduceCopier.MapOutputLocation)) {
					return false;
				}
				if (((ReduceTask.ReduceCopier.MapOutputLocation) o)
						.getTaskAttemptId() == null
						|| this.taskAttemptId == null)
					return false;
				if (((ReduceTask.ReduceCopier.MapOutputLocation) o)
						.getTaskAttemptId().toString()
						.equals(taskAttemptId.toString())
						&& ((ReduceTask.ReduceCopier.MapOutputLocation) o)
								.getOutputLocation().equals(taskOutput))
					return true;
				return false;

			}

			public TaskAttemptID getTaskAttemptId() {
				return taskAttemptId;
			}

			public TaskID getTaskId() {
				return taskId;
			}

			public String getHost() {
				return ttHost;
			}

			public URL getOutputLocation() {
				return taskOutput;
			}

			@Override
			public int compareTo(Object o) {
				/**
				 * if(!(o instanceof
				 * ReduceTask.ReduceCopier.MapOutputLocation)){ return -1; }
				 * if(((ReduceTask.ReduceCopier.MapOutputLocation)o).
				 * getTaskAttemptId()==null ||this.taskAttemptId == null) return
				 * -1; if(((ReduceTask.ReduceCopier.MapOutputLocation)o).
				 * getTaskAttemptId
				 * ().toString().equals(taskAttemptId.toString()) &&
				 * ((ReduceTask
				 * .ReduceCopier.MapOutputLocation)o).getOutputLocation
				 * ().equals(taskOutput) ) return 0; return 1;
				 */
				return ((ReduceTask.ReduceCopier.MapOutputLocation) o)
						.getTaskId().compareTo(getTaskID().getTaskID());
			}
		}

		/** Describes the output of a map; could either be on disk or in-memory. */
		private class MapOutput {
			final TaskID mapId;
			final TaskAttemptID mapAttemptId;

			final Path file;
			final Configuration conf;

			byte[] data;
			final boolean inMemory;
			long compressedSize;

			public MapOutput(TaskID mapId, TaskAttemptID mapAttemptId,
					Configuration conf, Path file, long size) {
				this.mapId = mapId;
				this.mapAttemptId = mapAttemptId;

				this.conf = conf;
				this.file = file;
				this.compressedSize = size;

				this.data = null;

				this.inMemory = false;
			}
			public String toString(){
				StringBuilder sb = new StringBuilder();
				sb.append("mapid:"+mapId+", mapAttempId:"+mapAttemptId+", file:"+file+", data:"+data
						+", inMemory:"+inMemory+", compressedSize:"+compressedSize);
				return sb.toString();
			}
			public MapOutput(TaskID mapId, TaskAttemptID mapAttemptId,
					byte[] data, int compressedLength) {
				this.mapId = mapId;
				this.mapAttemptId = mapAttemptId;

				this.file = null;
				this.conf = null;

				this.data = data;
				this.compressedSize = compressedLength;

				this.inMemory = true;
			}

			public void discard() throws IOException {
				if (inMemory) {
					data = null;
				} else {
					FileSystem fs = file.getFileSystem(conf);
					fs.delete(file, true);
				}
			}
		}

		class ShuffleRamManager implements RamManager {
			/*
			 * Maximum percentage of the in-memory limit that a single shuffle
			 * can consume
			 */
			private static final float MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION = 0.25f;

			/*
			 * Maximum percentage of shuffle-threads which can be stalled
			 * simultaneously after which a merge is triggered.
			 */
			private static final float MAX_STALLED_SHUFFLE_THREADS_FRACTION = 0.75f;

			private final long maxSize;
			private final long maxSingleShuffleLimit;

			private long size = 0;

			private Object dataAvailable = new Object();
			private long fullSize = 0;
			private int numPendingRequests = 0;
			private int numRequiredMapOutputs = 0;
			private int numClosed = 0;
			private boolean closed = false;

			public ShuffleRamManager(Configuration conf) throws IOException {
				final float maxInMemCopyUse = conf.getFloat(
						"mapred.job.shuffle.input.buffer.percent", 0.70f);
				if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
					throw new IOException(
							"mapred.job.shuffle.input.buffer.percent"
									+ maxInMemCopyUse);
				}
				// Allow unit tests to fix Runtime memory
				maxSize = (int) (conf.getInt(
						"mapred.job.reduce.total.mem.bytes", (int) Math.min(
								Runtime.getRuntime().maxMemory(),
								Integer.MAX_VALUE)) * maxInMemCopyUse);
				maxSingleShuffleLimit = (long) (maxSize * MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION);
				LOG.info("ShuffleRamManager: MemoryLimit=" + maxSize
						+ ", MaxSingleShuffleLimit=" + maxSingleShuffleLimit);
			}

			public synchronized boolean reserve(int requestedSize,
					InputStream in) throws InterruptedException {
				// Wait till the request can be fulfilled...
				while ((size + requestedSize) > maxSize) {

					// Close the input...
					if (in != null) {
						try {
							in.close();
						} catch (IOException ie) {
							LOG.info("Failed to close connection with: " + ie);
						} finally {
							in = null;
						}
					}

					// Track pending requests
					synchronized (dataAvailable) {
						++numPendingRequests;
						dataAvailable.notify();
					}

					// Wait for memory to free up
					wait();

					// Track pending requests
					synchronized (dataAvailable) {
						--numPendingRequests;
					}
				}

				size += requestedSize;

				return (in != null);
			}

			public synchronized void unreserve(int requestedSize) {
				size -= requestedSize;

				synchronized (dataAvailable) {
					fullSize -= requestedSize;
					--numClosed;
				}

				// Notify the threads blocked on RamManager.reserve
				notifyAll();
			}

			public boolean waitForDataToMerge() throws InterruptedException {
				boolean done = false;
				synchronized (dataAvailable) {
					// Start in-memory merge if manager has been closed or...
					while (!closed
							&&
							// In-memory threshold exceeded and at least two
							// segments
							// have been fetched
							(getPercentUsed() < maxInMemCopyPer || numClosed < 2)
							&&
							// More than "mapred.inmem.merge.threshold" map
							// outputs
							// have been fetched into memory
							(maxInMemOutputs <= 0 || numClosed < maxInMemOutputs)
							&&
							// More than MAX... threads are blocked on the
							// RamManager
							// or the blocked threads are the last map outputs
							// to be
							// fetched. If numRequiredMapOutputs is zero, either
							// setNumCopiedMapOutputs has not been called (no
							// map ouputs
							// have been fetched, so there is nothing to merge)
							// or the
							// last map outputs being transferred without
							// contention, so a merge would be premature.
							(numPendingRequests < numCopiers
									* MAX_STALLED_SHUFFLE_THREADS_FRACTION && (0 == numRequiredMapOutputs || numPendingRequests < numRequiredMapOutputs))) {
						dataAvailable.wait();
					}
					done = closed;
				}
				return done;
			}

			public void closeInMemoryFile(int requestedSize) {
				synchronized (dataAvailable) {
					fullSize += requestedSize;
					++numClosed;
					dataAvailable.notify();
				}
			}

			public void setNumCopiedMapOutputs(int numRequiredMapOutputs) {
				synchronized (dataAvailable) {
					this.numRequiredMapOutputs = numRequiredMapOutputs;
					dataAvailable.notify();
				}
			}

			public void close() {
				synchronized (dataAvailable) {
					closed = true;
					LOG.info("Closed ram manager");
					dataAvailable.notify();
				}
			}

			private float getPercentUsed() {
				return (float) fullSize / maxSize;
			}

			boolean canFitInMemory(long requestedSize) {
				return (requestedSize < Integer.MAX_VALUE && requestedSize < maxSingleShuffleLimit);
			}
		}

		/** Copies map outputs as they become available */
		private class MapOutputCopier extends Thread {
			// basic/unit connection timeout (in milliseconds)
			private final static int UNIT_CONNECT_TIMEOUT = 30 * 1000;
			// default read timeout (in milliseconds)
			private final static int DEFAULT_READ_TIMEOUT = 3 * 60 * 1000;
			private final int shuffleConnectionTimeout;
			private final int shuffleReadTimeout;

			private MapOutputLocation currentLocation = null;
			private int id = nextMapOutputCopierId++;
			private Reporter reporter;
			private boolean readError = false;

			// Decompression of map-outputs
			private CompressionCodec codec = null;
			private Decompressor decompressor = null;

			private final SecretKey jobTokenSecret;

			private int part = -1;

			public MapOutputCopier(JobConf job, Reporter reporter,
					SecretKey jobTokenSecret) {
				setName("MapOutputCopier " + reduceTask.getTaskID() + "." + id);
				LOG.debug(getName() + " created");
				this.reporter = reporter;

				this.jobTokenSecret = jobTokenSecret;

				shuffleConnectionTimeout = job.getInt(
						"mapreduce.reduce.shuffle.connect.timeout",
						STALLED_COPY_TIMEOUT);
				shuffleReadTimeout = job.getInt(
						"mapreduce.reduce.shuffle.read.timeout",
						DEFAULT_READ_TIMEOUT);

				if (job.getCompressMapOutput()) {
					Class<? extends CompressionCodec> codecClass = job
							.getMapOutputCompressorClass(DefaultCodec.class);
					codec = ReflectionUtils.newInstance(codecClass, job);
					decompressor = CodecPool.getDecompressor(codec);
				}
			}

			/**
			 * Fail the current file that we are fetching
			 * 
			 * @return were we currently fetching?
			 */
			public synchronized boolean fail() {
				if (currentLocation != null) {
					finish(-1, CopyOutputErrorType.OTHER_ERROR);
					return true;
				} else {
					return false;
				}
			}

			/**
			 * Get the current map output location.
			 */
			public synchronized MapOutputLocation getLocation() {
				return currentLocation;
			}

			private synchronized void start(MapOutputLocation loc) {
				currentLocation = loc;
				part = getLocationPartition(loc);
			}

			private synchronized void finish(long size,
					CopyOutputErrorType error) {
				if (currentLocation != null) {
					LOG.debug(getName() + " finishing " + currentLocation
							+ " =" + size);
					synchronized (copyResultsOrNewEventsLock) {
						CopyResult res = new CopyResult(currentLocation, size,
								error);
						copyResults.add(res);
						partitionToCopyResults.get(part).add(res);
						copyResultsOrNewEventsLock.notifyAll();
					}
					currentLocation = null;
					part = -1;
				}
			}

			/**
			 * Loop forever and fetch map outputs as they become available. The
			 * thread exits when it is interrupted by {@link ReduceTaskRunner}
			 */
			@Override
			public void run() {
				while (true) {
					try {
						MapOutputLocation loc = null;
						long size = -1;

						synchronized (scheduledCopies) {
							while (scheduledCopies.isEmpty()) {
								scheduledCopies.wait();
							}
							loc = scheduledCopies.remove(0);
						}
						CopyOutputErrorType error = CopyOutputErrorType.OTHER_ERROR;
						readError = false;
						try {
							shuffleClientMetrics.threadBusy();
							start(loc);
							size = copyOutput(loc);
							shuffleClientMetrics.successFetch();
							error = CopyOutputErrorType.NO_ERROR;
						} catch (IOException e) {
							LOG.warn(reduceTask.getTaskID() + " copy failed: "
									+ loc.getTaskAttemptId() + " from "
									+ loc.getHost());
							LOG.warn(StringUtils.stringifyException(e));
							shuffleClientMetrics.failedFetch();
							if (readError) {
								error = CopyOutputErrorType.READ_ERROR;
							}
							// Reset
							size = -1;
						} finally {
							shuffleClientMetrics.threadFree();
							finish(size, error);
						}
					} catch (InterruptedException e) {
						break; // ALL DONE
					} catch (FSError e) {
						LOG.error("Task: " + reduceTask.getTaskID()
								+ " - FSError: "
								+ StringUtils.stringifyException(e));
						try {
							umbilical.fsError(reduceTask.getTaskID(),
									e.getMessage(), jvmContext);
						} catch (IOException io) {
							LOG.error("Could not notify TT of FSError: "
									+ StringUtils.stringifyException(io));
						}
					} catch (Throwable th) {
						String msg = getTaskID()
								+ " : Map output copy failure : "
								+ StringUtils.stringifyException(th);
						reportFatalError(getTaskID(), th, msg);
					}
				}

				if (decompressor != null) {
					CodecPool.returnDecompressor(decompressor);
				}

			}

			/**
			 * Copies a a map output from a remote host, via HTTP.
			 * 
			 * @param currentLocation
			 *            the map output location to be copied
			 * @return the path (fully qualified) of the copied file
			 * @throws IOException
			 *             if there is an error copying the file
			 * @throws InterruptedException
			 *             if the copier should give up
			 */
			private long copyOutput(MapOutputLocation loc) throws IOException,
					InterruptedException {
				// check if we still need to copy the output from this location
				// LOG.info("--------"+"coping"
				// +loc.getOutputLocation()+"copied map out puts "+
				// copiedMapOutputs.size());
				// if(copiedMapOutputs.size()>0)
				// LOG.info("-------copied map out puts"+copiedMapOutputs.iterator().next());

				if (copiedMapOutputs.contains(loc)
						|| obsoleteMapIds.contains(loc.getTaskAttemptId())) {
					return CopyResult.OBSOLETE;
				}

				// a temp filename. If this file gets created in ramfs, we're
				// fine,
				// else, we will check the localFS to find a suitable final
				// location
				// for this path
				TaskAttemptID reduceId = reduceTask.getTaskID();
				// int part = getLocationPartition(loc);
				Path filename = new Path(String.format("%s/map_%d_%s.out",
						TaskTracker.OUTPUT, loc.getTaskId().getId(), part));

				// Copy the map output to a temp file whose name is unique to
				// this attempt
				Path tmpMapOutput = new Path(filename + "-" + id);

				// Copy the map output
				MapOutput mapOutput = getMapOutput(loc, tmpMapOutput, reduceId
						.getTaskID().getId());
				if (mapOutput == null) {
					throw new IOException("Failed to fetch map-output for "
							+ loc.getTaskAttemptId() + " from " + loc.getHost());
				}

				// The size of the map-output
				long bytes = mapOutput.compressedSize;

				// lock the ReduceTask while we do the rename
				synchronized (ReduceTask.this) {
					if (copiedMapOutputs.contains(loc)) {
						mapOutput.discard();
						return CopyResult.OBSOLETE;
					}

					// Special case: discard empty map-outputs
					if (bytes == 0) {
						try {
							mapOutput.discard();
						} catch (IOException ioe) {
							LOG.info("Couldn't discard output of "
									+ loc.getTaskId());
						}

						// Note that we successfully copied the map-output
						noteCopiedMapOutput(loc);

						return bytes;
					}

					// Process map-output
					if (mapOutput.inMemory) {
						// Save it in the synchronized list of map-outputs
						synchronized(mapOutputsFilesInMemory){
							mapOutputsFilesInMemory.add(mapOutput);
							partitionToMapOutputFilesInMemory.get(part).add(
								mapOutput);
						}
						
					} else {
						// Rename the temporary file to the final file;
						// ensure it is on the same partition
						tmpMapOutput = mapOutput.file;
						filename = new Path(tmpMapOutput.getParent(),
								filename.getName());
						if (!localFileSys.rename(tmpMapOutput, filename)) {
							localFileSys.delete(tmpMapOutput, true);
							bytes = -1;
							throw new IOException(
									"Failed to rename map output "
											+ tmpMapOutput + " to " + filename);
						}

						synchronized (mapOutputFilesOnDisk) {
							addToMapOutputFilesOnDisk(localFileSys
									.getFileStatus(filename));
							SortedSet<FileStatus> ondisk = partitionToMapOutputFilesOnDisk
							.get(part);
							addToMapOutputFilesOnDisk(
									localFileSys.getFileStatus(filename),
									ondisk);
						}
						/**
						SortedSet<FileStatus> ondisk = partitionToMapOutputFilesOnDisk
								.get(part);
						synchronized (ondisk) {
							addToMapOutputFilesOnDisk(
									localFileSys.getFileStatus(filename),
									ondisk);
						}*/
					}

					LOG.info("--------------successfully copied map out puts from "
							+ loc.getOutputLocation());
					// Note that we successfully copied the map-output
					noteCopiedMapOutput(loc);
					noteCopiedMapOutputWithPart(loc, part);
				}

				return bytes;
			}

			/**
			 * Save the map taskid whose output we just copied. This function
			 * assumes that it has been synchronized on ReduceTask.this.
			 * 
			 * @param loc
			 *            map taskid
			 */
			private void noteCopiedMapOutput(MapOutputLocation loc) {
				copiedMapOutputs.add(loc);
				ramManager.setNumCopiedMapOutputs(numMaps
						- copiedMapOutputs.size());
			}

			private void noteCopiedMapOutputWithPart(MapOutputLocation loc,
					int part) {
				partitionToCopiedMapOutputs.get(part).add(loc);
				ramManager.setNumCopiedMapOutputs(numMaps
						- copiedMapOutputs.size());// 这个在这里肯定不准，不过应该不影响正确性
			}

			/**
			 * Get the map output into a local file (either in the inmemory fs
			 * or on the local fs) from the remote server. We use the file
			 * system so that we generate checksum files on the data.
			 * 
			 * @param mapOutputLoc
			 *            map-output to be fetched
			 * @param filename
			 *            the filename to write the data into
			 * @param connectionTimeout
			 *            number of milliseconds for connection timeout
			 * @param readTimeout
			 *            number of milliseconds for read timeout
			 * @return the path of the file that got created
			 * @throws IOException
			 *             when something goes wrong
			 */
			private MapOutput getMapOutput(MapOutputLocation mapOutputLoc,
					Path filename, int reduce) throws IOException,
					InterruptedException {
				// Connect
				URL url = mapOutputLoc.getOutputLocation();
				HttpURLConnection connection = (HttpURLConnection) url
						.openConnection();

				InputStream input = setupSecureConnection(mapOutputLoc,
						connection);

				// Validate response code
				int rc = connection.getResponseCode();
				if (rc != HttpURLConnection.HTTP_OK) {
					throw new IOException("Got invalid response code " + rc
							+ " from " + url + ": "
							+ connection.getResponseMessage());
				}

				// Validate header from map output
				TaskAttemptID mapId = null;
				try {
					mapId = TaskAttemptID.forName(connection
							.getHeaderField(FROM_MAP_TASK));
				} catch (IllegalArgumentException ia) {
					LOG.warn("Invalid map id ", ia);
					return null;
				}
				TaskAttemptID expectedMapId = mapOutputLoc.getTaskAttemptId();
				if (!mapId.equals(expectedMapId)) {
					LOG.warn("data from wrong map:" + mapId
							+ " arrived to reduce task " + reduce
							+ ", where as expected map output should be from "
							+ expectedMapId);
					return null;
				}

				long decompressedLength = Long.parseLong(connection
						.getHeaderField(RAW_MAP_OUTPUT_LENGTH));
				long compressedLength = Long.parseLong(connection
						.getHeaderField(MAP_OUTPUT_LENGTH));

				if (compressedLength < 0 || decompressedLength < 0) {
					LOG.warn(getName()
							+ " invalid lengths in map output header: id: "
							+ mapId + " compressed len: " + compressedLength
							+ ", decompressed len: " + decompressedLength);
					return null;
				}
				int forReduce = (int) Integer.parseInt(connection
						.getHeaderField(FOR_REDUCE_TASK));

				/**
				 * if (forReduce != reduce) {
				 * 
				 * LOG.warn("data for the wrong reduce: " + forReduce +
				 * " with compressed len: " + compressedLength +
				 * ", decompressed len: " + decompressedLength +
				 * " arrived to reduce task " + reduce); return null; }
				 */
				if (!getPartitions().contains(forReduce)) {
					LOG.warn("data for the wrong reduce: " + forReduce
							+ " with compressed len: " + compressedLength
							+ ", decompressed len: " + decompressedLength
							+ " arrived to reduce task " + reduce);
					return null;
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug("header: " + mapId + ", compressed len: "
							+ compressedLength + ", decompressed len: "
							+ decompressedLength);
				}

				// We will put a file in memory if it meets certain criteria:
				// 1. The size of the (decompressed) file should be less than
				// 25% of
				// the total inmem fs
				// 2. There is space available in the inmem fs

				// Check if this map-output can be saved in-memory
				boolean shuffleInMemory = ramManager
						.canFitInMemory(decompressedLength);

				// Shuffle
				MapOutput mapOutput = null;
				if (shuffleInMemory) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Shuffling " + decompressedLength
								+ " bytes (" + compressedLength
								+ " raw bytes) " + "into RAM from "
								+ mapOutputLoc.getTaskAttemptId());
					}

					mapOutput = shuffleInMemory(mapOutputLoc, connection,
							input, (int) decompressedLength,
							(int) compressedLength);
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Shuffling " + decompressedLength
								+ " bytes (" + compressedLength
								+ " raw bytes) " + "into Local-FS from "
								+ mapOutputLoc.getTaskAttemptId());
					}

					mapOutput = shuffleToDisk(mapOutputLoc, input, filename,
							compressedLength);
				}

				return mapOutput;
			}

			private InputStream setupSecureConnection(
					MapOutputLocation mapOutputLoc, URLConnection connection)
					throws IOException {

				// generate hash of the url
				String msgToEncode = SecureShuffleUtils.buildMsgFrom(connection
						.getURL());
				String encHash = SecureShuffleUtils.hashFromString(msgToEncode,
						jobTokenSecret);

				// put url hash into http header
				connection.setRequestProperty(
						SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);

				InputStream input = getInputStream(connection,
						shuffleConnectionTimeout, shuffleReadTimeout);

				// get the replyHash which is HMac of the encHash we sent to the
				// server
				String replyHash = connection
						.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH);
				if (replyHash == null) {
					throw new IOException(
							"security validation of TT Map output failed");
				}
				if (LOG.isDebugEnabled())
					LOG.debug("url=" + msgToEncode + ";encHash=" + encHash
							+ ";replyHash=" + replyHash);
				// verify that replyHash is HMac of encHash
				SecureShuffleUtils.verifyReply(replyHash, encHash,
						jobTokenSecret);
				if (LOG.isDebugEnabled())
					LOG.debug("for url=" + msgToEncode
							+ " sent hash and receievd reply");
				return input;
			}

			/**
			 * The connection establishment is attempted multiple times and is
			 * given up only on the last failure. Instead of connecting with a
			 * timeout of X, we try connecting with a timeout of x < X but
			 * multiple times.
			 */
			private InputStream getInputStream(URLConnection connection,
					int connectionTimeout, int readTimeout) throws IOException {
				int unit = 0;
				if (connectionTimeout < 0) {
					throw new IOException("Invalid timeout " + "[timeout = "
							+ connectionTimeout + " ms]");
				} else if (connectionTimeout > 0) {
					unit = (UNIT_CONNECT_TIMEOUT > connectionTimeout) ? connectionTimeout
							: UNIT_CONNECT_TIMEOUT;
				}
				// set the read timeout to the total timeout
				connection.setReadTimeout(readTimeout);
				// set the connect timeout to the unit-connect-timeout
				connection.setConnectTimeout(unit);
				while (true) {
					try {
						connection.connect();
						break;
					} catch (IOException ioe) {
						// update the total remaining connect-timeout
						connectionTimeout -= unit;

						// throw an exception if we have waited for timeout
						// amount of time
						// note that the updated value if timeout is used here
						if (connectionTimeout == 0) {
							throw ioe;
						}

						// reset the connect timeout for the last try
						if (connectionTimeout < unit) {
							unit = connectionTimeout;
							// reset the connect time out for the final connect
							connection.setConnectTimeout(unit);
						}
					}
				}
				try {
					return connection.getInputStream();
				} catch (IOException ioe) {
					readError = true;
					throw ioe;
				}
			}

			private MapOutput shuffleInMemory(MapOutputLocation mapOutputLoc,
					URLConnection connection, InputStream input,
					int mapOutputLength, int compressedLength)
					throws IOException, InterruptedException {
				// Reserve ram for the map-output
				boolean createdNow = ramManager.reserve(mapOutputLength, input);

				// Reconnect if we need to
				if (!createdNow) {
					// Reconnect
					try {
						connection = mapOutputLoc.getOutputLocation()
								.openConnection();
						input = setupSecureConnection(mapOutputLoc, connection);
					} catch (IOException ioe) {
						LOG.info("Failed reopen connection to fetch map-output from "
								+ mapOutputLoc.getHost());

						// Inform the ram-manager
						ramManager.closeInMemoryFile(mapOutputLength);
						ramManager.unreserve(mapOutputLength);

						throw ioe;
					}
				}

				IFileInputStream checksumIn = new IFileInputStream(input,
						compressedLength, conf);

				input = checksumIn;

				// Are map-outputs compressed?
				if (codec != null) {
					decompressor.reset();
					input = codec.createInputStream(input, decompressor);
				}

				// Copy map-output into an in-memory buffer
				byte[] shuffleData = new byte[mapOutputLength];
				MapOutput mapOutput = new MapOutput(mapOutputLoc.getTaskId(),
						mapOutputLoc.getTaskAttemptId(), shuffleData,
						compressedLength);

				int bytesRead = 0;
				try {
					int n = input.read(shuffleData, 0, shuffleData.length);
					while (n > 0) {
						bytesRead += n;
						shuffleClientMetrics.inputBytes(n);

						// indicate we're making progress
						reporter.progress();
						n = input.read(shuffleData, bytesRead,
								(shuffleData.length - bytesRead));
					}

					if (LOG.isDebugEnabled()) {
						LOG.debug("Read " + bytesRead
								+ " bytes from map-output for "
								+ mapOutputLoc.getTaskAttemptId());
					}

					input.close();
				} catch (IOException ioe) {
					LOG.info(
							"Failed to shuffle from "
									+ mapOutputLoc.getTaskAttemptId(), ioe);

					// Inform the ram-manager
					ramManager.closeInMemoryFile(mapOutputLength);
					ramManager.unreserve(mapOutputLength);

					// Discard the map-output
					try {
						mapOutput.discard();
					} catch (IOException ignored) {
						LOG.info("Failed to discard map-output from "
								+ mapOutputLoc.getTaskAttemptId(), ignored);
					}
					mapOutput = null;

					// Close the streams
					IOUtils.cleanup(LOG, input);

					// Re-throw
					readError = true;
					throw ioe;
				}

				// Close the in-memory file
				ramManager.closeInMemoryFile(mapOutputLength);

				// Sanity check
				if (bytesRead != mapOutputLength) {
					// Inform the ram-manager
					ramManager.unreserve(mapOutputLength);

					// Discard the map-output
					try {
						mapOutput.discard();
					} catch (IOException ignored) {
						// IGNORED because we are cleaning up
						LOG.info("Failed to discard map-output from "
								+ mapOutputLoc.getTaskAttemptId(), ignored);
					}
					mapOutput = null;

					throw new IOException("Incomplete map output received for "
							+ mapOutputLoc.getTaskAttemptId() + " from "
							+ mapOutputLoc.getOutputLocation() + " ("
							+ bytesRead + " instead of " + mapOutputLength
							+ ")");
				}

				// TODO: Remove this after a 'fix' for HADOOP-3647
				if (LOG.isDebugEnabled()) {
					if (mapOutputLength > 0) {
						DataInputBuffer dib = new DataInputBuffer();
						dib.reset(shuffleData, 0, shuffleData.length);
						LOG.debug("Rec #1 from "
								+ mapOutputLoc.getTaskAttemptId() + " -> ("
								+ WritableUtils.readVInt(dib) + ", "
								+ WritableUtils.readVInt(dib) + ") from "
								+ mapOutputLoc.getHost());
					}
				}

				return mapOutput;
			}

			private MapOutput shuffleToDisk(MapOutputLocation mapOutputLoc,
					InputStream input, Path filename, long mapOutputLength)
					throws IOException {
				// Find out a suitable location for the output on
				// local-filesystem
				Path localFilename = lDirAlloc.getLocalPathForWrite(filename
						.toUri().getPath(), mapOutputLength, conf);

				MapOutput mapOutput = new MapOutput(mapOutputLoc.getTaskId(),
						mapOutputLoc.getTaskAttemptId(), conf,
						localFileSys.makeQualified(localFilename),
						mapOutputLength);

				// Copy data to local-disk
				OutputStream output = null;
				long bytesRead = 0;
				try {
					output = rfs.create(localFilename);

					byte[] buf = new byte[64 * 1024];
					int n = -1;
					try {
						n = input.read(buf, 0, buf.length);
					} catch (IOException ioe) {
						readError = true;
						throw ioe;
					}
					while (n > 0) {
						bytesRead += n;
						shuffleClientMetrics.inputBytes(n);
						output.write(buf, 0, n);

						// indicate we're making progress
						reporter.progress();
						try {
							n = input.read(buf, 0, buf.length);
						} catch (IOException ioe) {
							readError = true;
							throw ioe;
						}
					}

					LOG.info("Read " + bytesRead
							+ " bytes from map-output for "
							+ mapOutputLoc.getTaskAttemptId());

					output.close();
					input.close();
				} catch (IOException ioe) {
					LOG.info(
							"Failed to shuffle from "
									+ mapOutputLoc.getTaskAttemptId(), ioe);

					// Discard the map-output
					try {
						mapOutput.discard();
					} catch (IOException ignored) {
						LOG.info("Failed to discard map-output from "
								+ mapOutputLoc.getTaskAttemptId(), ignored);
					}
					mapOutput = null;

					// Close the streams
					IOUtils.cleanup(LOG, input, output);

					// Re-throw
					throw ioe;
				}

				// Sanity check
				if (bytesRead != mapOutputLength) {
					try {
						mapOutput.discard();
					} catch (Exception ioe) {
						// IGNORED because we are cleaning up
						LOG.info("Failed to discard map-output from "
								+ mapOutputLoc.getTaskAttemptId(), ioe);
					} catch (Throwable t) {
						String msg = getTaskID()
								+ " : Failed in shuffle to disk :"
								+ StringUtils.stringifyException(t);
						reportFatalError(getTaskID(), t, msg);
					}
					mapOutput = null;

					throw new IOException("Incomplete map output received for "
							+ mapOutputLoc.getTaskAttemptId() + " from "
							+ mapOutputLoc.getOutputLocation() + " ("
							+ bytesRead + " instead of " + mapOutputLength
							+ ")");
				}

				return mapOutput;

			}

		} // MapOutputCopier

		private void configureClasspath(JobConf conf) throws IOException {

			// get the task and the current classloader which will become the
			// parent
			Task task = ReduceTask.this;
			ClassLoader parent = conf.getClassLoader();

			// get the work directory which holds the elements we are
			// dynamically
			// adding to the classpath
			File workDir = new File(task.getJobFile()).getParentFile();
			ArrayList<URL> urllist = new ArrayList<URL>();

			// add the jars and directories to the classpath
			String jar = conf.getJar();
			if (jar != null) {
				File jobCacheDir = new File(new Path(jar).getParent()
						.toString());

				File[] libs = new File(jobCacheDir, "lib").listFiles();
				if (libs != null) {
					for (int i = 0; i < libs.length; i++) {
						urllist.add(libs[i].toURL());
					}
				}
				urllist.add(new File(jobCacheDir, "classes").toURL());
				urllist.add(jobCacheDir.toURL());

			}
			urllist.add(workDir.toURL());

			// create a new classloader with the old classloader as its parent
			// then set that classloader as the one used by the current jobconf
			URL[] urls = urllist.toArray(new URL[urllist.size()]);
			URLClassLoader loader = new URLClassLoader(urls, parent);
			conf.setClassLoader(loader);
		}

		public ReduceCopier(TaskUmbilicalProtocol umbilical, JobConf conf,
				TaskReporter reporter) throws ClassNotFoundException,
				IOException {

			configureClasspath(conf);
			this.reporter = reporter;
			this.shuffleClientMetrics = createShuffleClientInstrumentation();
			this.umbilical = umbilical;
			this.reduceTask = ReduceTask.this;

			this.scheduledCopies = new ArrayList<MapOutputLocation>(100);
			this.copyResults = new ArrayList<CopyResult>(100);
			this.partitionToCopyResults = new HashMap<Integer, List<CopyResult>>();
			List<Integer> parts = getPartitions();
			for (int p : parts) {
				this.partitionToCopyResults.put(p,
						new ArrayList<CopyResult>(30));
			}
			this.numCopiers = conf.getInt("mapred.reduce.parallel.copies", 5);
			this.maxInFlight = 4 * numCopiers;
			Counters.Counter combineInputCounter = reporter
					.getCounter(Task.Counter.COMBINE_INPUT_RECORDS);
			this.combinerRunner = CombinerRunner.create(conf, getTaskID(),
					combineInputCounter, reporter, null);
			if (combinerRunner != null) {
				combineCollector = new CombineOutputCollector(
						reduceCombineOutputCounter, reporter, conf);
			}

			this.ioSortFactor = conf.getInt("io.sort.factor", 10);

			this.abortFailureLimit = Math.max(30, numMaps / 10);

			this.maxFetchFailuresBeforeReporting = conf.getInt(
					"mapreduce.reduce.shuffle.maxfetchfailures",
					REPORT_FAILURE_LIMIT);

			this.maxFailedUniqueFetches = Math.min(numMaps,
					this.maxFailedUniqueFetches);
			this.maxInMemOutputs = conf.getInt("mapred.inmem.merge.threshold",
					1000);
			this.maxInMemCopyPer = conf.getFloat(
					"mapred.job.shuffle.merge.percent", 0.66f);
			final float maxRedPer = conf.getFloat(
					"mapred.job.reduce.input.buffer.percent", 0f);
			if (maxRedPer > 1.0 || maxRedPer < 0.0) {
				throw new IOException("mapred.job.reduce.input.buffer.percent"
						+ maxRedPer);
			}
			this.maxInMemReduce = (int) Math.min(Runtime.getRuntime()
					.maxMemory() * maxRedPer, Integer.MAX_VALUE);

			// Setup the RamManager
			ramManager = new ShuffleRamManager(conf);

			localFileSys = FileSystem.getLocal(conf);

			rfs = ((LocalFileSystem) localFileSys).getRaw();

			// hosts -> next contact time
			this.penaltyBox = new LinkedHashMap<String, Long>();

			// hostnames
			this.uniqueHosts = new HashSet<String>();

			// Seed the random number generator with a reasonably globally
			// unique seed
			long randomSeed = System.nanoTime()
					+ (long) Math.pow(this.reduceTask.getPartition(),
							(this.reduceTask.getPartition() % 10));
			this.random = new Random(randomSeed);
			this.maxMapRuntime = 0;
			this.reportReadErrorImmediately = conf.getBoolean(
					"mapreduce.reduce.shuffle.notify.readerror", true);

			// 动态shuffle
			partitionToCopiedMapOutputs.put(partitions.get(0), Collections
					.synchronizedSet(new TreeSet<MapOutputLocation>()));
			partitionToMapLocations.put(partitions.get(0),
					new ConcurrentHashMap<String, List<MapOutputLocation>>());
			partitionToMapOutputFilesInMemory.put(partitions.get(0),
					Collections.synchronizedList(new LinkedList<MapOutput>()));
			partitionToFinishedCopy.put(partitions.get(0), 0);
		}

		private boolean busyEnough(int numInFlight) {
			return numInFlight > maxInFlight;
		}

		private int getLocationPartition(MapOutputLocation loc) {
			String url = loc.getOutputLocation().toString();
			return Integer.parseInt(url.substring(url.indexOf("reduce=") + 7,
					url.length()));
		}

		public void run() {
			try {
				// copiedAllPartitions现在没有使用，直觉会有用的
				copiedAllPartitions = fetchOutputs();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				copyException = e;
			}
		}

		private Map<Integer, Integer> partitionToFinishedCopy = new HashMap<Integer, Integer>();

		/**
		 * 启动MapOtputCopier,LocalFSMerger,InMemFSMergeThread,LocalFSMerger,GetMapEventsThread;
		 * 调度mapoutputlocation到scheduledcopies里面给mapoutputcopier去拷贝
		 * 遍历检查CopyResult,看拷贝完成情况
		 * @return
		 * @throws IOException
		 */
		public boolean fetchOutputs() throws IOException {
			int totalFailures = 0;
			int numInFlight = 0, numCopied = 0;
			DecimalFormat mbpsFormat = new DecimalFormat("0.00");
			final Progress copyPhase = reduceTask.getProgress().phase();
			LocalFSMerger localFSMergerThread = null;
			InMemFSMergeThread inMemFSMergeThread = null;
			GetMapEventsThread getMapEventsThread = null;

			for (int i = 0; i < numMaps; i++) {
				copyPhase.addPhase(); // add sub-phase per file
			}

			copiers = new ArrayList<MapOutputCopier>(numCopiers);

			// start all the copying threads
			for (int i = 0; i < numCopiers; i++) {
				MapOutputCopier copier = new MapOutputCopier(conf, reporter,
						reduceTask.getJobTokenSecret());
				copiers.add(copier);
				copier.start();
			}

			// start the on-disk-merge thread
			localFSMergerThread = new LocalFSMerger(
					(LocalFileSystem) localFileSys);
			// start the in memory merger thread
			inMemFSMergeThread = new InMemFSMergeThread();
			localFSMergerThread.start();
			inMemFSMergeThread.start();

			// start the map events thread
			getMapEventsThread = new GetMapEventsThread();
			getMapEventsThread.start();

			// start the clock for bandwidth measurement
			long startTime = System.currentTimeMillis();
			long currentTime = startTime;
			long lastProgressTime = startTime;
			long lastOutputTime = 0;

			// loop until we get all required outputs
			while (mergeThrowable == null) {
				synchronized (partitions) {// 这里和获取事件处理那儿有些同步处理，注意！
					if (copiedMapOutputs.size() >= numMaps * partitions.size()) {
						if (partitionArrangeFinished.get())
							break;// partition都分配完了，且数据也拷贝完了
						else {// 已经分配的partition已经拷贝完成，等待分配新的partition
							try {
								partitions.wait();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								LOG.error(e);
							}
						}
					}
				}
				LOG.info("----!!!!!!!!!!!------partitions:" + partitions
						+ ", orign parttion:" + getPartition() + "\n"
						+ " copiedmapoutputs.size=" + copiedMapOutputs.size()
						+ " numMaps" + numMaps + " partitions.size="
						+ partitions.size() + " finished:"
						+ partitionArrangeFinished);

				int numEventsAtStartOfScheduling;
				synchronized (copyResultsOrNewEventsLock) {
					numEventsAtStartOfScheduling = numEventsFetched;
				}

				currentTime = System.currentTimeMillis();
				boolean logNow = false;
				if (currentTime - lastOutputTime > MIN_LOG_TIME) {
					lastOutputTime = currentTime;
					logNow = true;
				}
				if (logNow) {
					LOG.info(reduceTask.getTaskID() + " Need another "
							+ (numMaps - copiedMapOutputs.size())
							+ " map output(s) " + "where " + numInFlight
							+ " is already in progress");
				}

				// Put the hash entries for the failed fetches.
				Iterator<MapOutputLocation> locItr = retryFetches.iterator();

				while (locItr.hasNext()) {
					MapOutputLocation loc = locItr.next();
					List<MapOutputLocation> locList = mapLocations.get(loc
							.getHost());

					List<MapOutputLocation> myLocList = partitionToMapLocations
							.get(getLocationPartition(loc)).get(loc.getHost());
					// Check if the list exists. Map output location mapping is
					// cleared
					// once the jobtracker restarts and is rebuilt from scratch.
					// Note that map-output-location mapping will be recreated
					// and hence
					// we continue with the hope that we might find some
					// locations
					// from the rebuild map.
					if (locList != null) {
						// Add to the beginning of the list so that this map is
						// tried again before the others and we can hasten the
						// re-execution of this map should there be a problem
						locList.add(0, loc);
						myLocList.add(0, loc);
					}
				}

				if (retryFetches.size() > 0) {
					LOG.info(reduceTask.getTaskID() + ": " + "Got "
							+ retryFetches.size()
							+ " map-outputs from previous failures");
				}
				// clear the "failed" fetches hashmap
				retryFetches.clear();

				// now walk through the cache and schedule what we can
				int numScheduled = 0;
				int numDups = 0;

				/**调度mapoutputlocation到scheduledcopies里面给mapoutputcopier来拷贝*/
				synchronized (scheduledCopies) {

					// Randomize the map output locations to prevent
					// all reduce-tasks swamping the same tasktracker
					List<String> hostList = new ArrayList<String>();
					hostList.addAll(mapLocations.keySet());

					Collections.shuffle(hostList, this.random);

					Iterator<String> hostsItr = hostList.iterator();

					while (hostsItr.hasNext()) {

						String host = hostsItr.next();

						List<MapOutputLocation> knownOutputsByLoc = null;
						List<Integer> parts = getPartitions();
						for (int p : parts) {// FIFO
							knownOutputsByLoc = partitionToMapLocations.get(p)
									.get(host);
							if (knownOutputsByLoc != null
									&& knownOutputsByLoc.size() != 0) {
								break;
								// 直接break掉可能会带来前面的partition的location不能被分配
								// 而阻塞后面的partition的location分配
							}
						}

						// Check if the list exists. Map output location mapping
						// is
						// cleared once the jobtracker restarts and is rebuilt
						// from
						// scratch.
						// Note that map-output-location mapping will be
						// recreated and
						// hence we continue with the hope that we might find
						// some
						// locations from the rebuild map and add then for
						// fetching.
						if (knownOutputsByLoc == null
								|| knownOutputsByLoc.size() == 0) {
							continue;
						}

						// for(MapOutputLocation lo: knownOutputsByLoc){
						// LOG.info(lo.getOutputLocation()+"\n");
						// }
						// Identify duplicate hosts here
						if (uniqueHosts.contains(host)) {
							numDups += knownOutputsByLoc.size();
							continue;
						}

						Long penaltyEnd = penaltyBox.get(host);
						boolean penalized = false;

						if (penaltyEnd != null) {
							if (currentTime < penaltyEnd.longValue()) {
								penalized = true;
							} else {
								penaltyBox.remove(host);
							}
						}

						if (penalized)
							continue;

						synchronized (knownOutputsByLoc) {

							locItr = knownOutputsByLoc.iterator();

							while (locItr.hasNext()) {

								MapOutputLocation loc = locItr.next();

								// Do not schedule fetches from OBSOLETE maps
								if (obsoleteMapIds.contains(loc
										.getTaskAttemptId())) {
									locItr.remove();
									continue;
								}

								uniqueHosts.add(host);
								scheduledCopies.add(loc);
								locItr.remove(); // remove from knownOutputs
								numInFlight++;
								numScheduled++;

								break; // we have a map from this host
							}
						}
					}
					scheduledCopies.notifyAll();
				}

				if (numScheduled > 0 || logNow) {
					LOG.info(reduceTask.getTaskID() + " Scheduled "
							+ numScheduled + " outputs (" + penaltyBox.size()
							+ " slow hosts and" + numDups + " dup hosts)");
				}

				if (penaltyBox.size() > 0 && logNow) {
					LOG.info("Penalized(slow) Hosts: ");
					for (String host : penaltyBox.keySet()) {
						LOG.info(host + " Will be considered after: "
								+ ((penaltyBox.get(host) - currentTime) / 1000)
								+ " seconds.");
					}
				}

				// if we have no copies in flight and we can't schedule anything
				// new, just wait for a bit
				try {
					if (numInFlight == 0 && numScheduled == 0) {
						// we should indicate progress as we don't want TT to
						// think
						// we're stuck and kill us
						reporter.progress();
						Thread.sleep(5000);
					}
				} catch (InterruptedException e) {
				} // IGNORE

				while (numInFlight > 0 && mergeThrowable == null) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(reduceTask.getTaskID() + " numInFlight = "
								+ numInFlight);
					}
					// the call to getCopyResult will either
					// 1) return immediately with a null or a valid CopyResult
					// object,
					// or
					// 2) if the numInFlight is above maxInFlight, return with a
					// CopyResult object after getting a notification from a
					// fetcher thread,
					// So, when getCopyResult returns null, we can be sure that
					// we aren't busy enough and we should go and get more
					// mapcompletion
					// events from the tasktracker
					CopyResult cr = getCopyResult(numInFlight,
							numEventsAtStartOfScheduling);

					if (cr == null) {
						break;
					}

					if (cr.getSuccess()) { // a successful copy
						numCopied++;
						lastProgressTime = System.currentTimeMillis();
						reduceShuffleBytes.increment(cr.getSize());

						long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
						float mbs = ((float) reduceShuffleBytes.getCounter())
								/ (1024 * 1024);
						float transferRate = mbs / secsSinceStart;

						copyPhase.startNextPhase();
						copyPhase.setStatus("copy (" + numCopied + " of "
								+ numMaps + " at "
								+ mbpsFormat.format(transferRate) + " MB/s)");

						// Note successful fetch for this mapId to invalidate
						// (possibly) old fetch-failures
						fetchFailedMaps.remove(cr.getLocation().getTaskId());

						// 处理正确copy完的copyresult，并且在这里处理某一个partition完成的处理逻辑
						// 当一个partition已经拷贝完成后它的驻留在内存和磁盘上的结果都不应该再次被选做
						// merge的对象，需要同步或屏蔽起来
						int part = getLocationPartition(cr.getLocation());
						partitionToFinishedCopy.put(part,
								partitionToFinishedCopy.get(part) + 1);
						if (partitionToFinishedCopy.get(part) == numMaps) {
							LOG.info("partition " + part
									+ "'s copy has finished");
							synchronized (reduceLock) {
								LOG.info("got reducelock");
								partitionToCopyFinished.put(part, true);
							}
							LOG.info("release reducelock");
							synchronized (mapOutputFilesOnDisk) {
								LOG.info("got partitionToMapOutputFilesOnDisklock");
								partitionToFinishedMapOutputFilesOnDisk.put(
										part, partitionToMapOutputFilesOnDisk
												.remove(part));
								mapOutputFilesOnDisk
										.removeAll(partitionToFinishedMapOutputFilesOnDisk
												.get(part));
								LOG.info("mapoutputfiles ondisk is empty"+mapOutputFilesOnDisk.isEmpty());
							}
							LOG.info("release partitionToMapOutputFilesOnDisklock");
							synchronized (mapOutputsFilesInMemory) {
								LOG.info("got partitionToMapOutputFilesInMemory lock");
								List<MapOutput> inmem = partitionToMapOutputFilesInMemory
										.remove(part);
								partitionToFinishedMapOutputFilesInMemory.put(
										part, inmem);
								mapOutputsFilesInMemory.removeAll(inmem);
								LOG.info("mapoutputfiles inmemory is empty"+mapOutputsFilesInMemory.isEmpty());
							}
							LOG.info("release partitionToMapOutputFilesInMemory lock");
							synchronized (shuffleFinishedPartition) {
								LOG.info("got shuffleFinishedPartition lock");
								shuffleFinishedPartition.add(0, part);// 加到队列首部
								LOG.info("nofity shuffleFinsihsed parttion:"
										+ part);
								shuffleFinishedPartition.notifyAll();
							}
							LOG.info("release shuffleFinishedPartition lock");
						}
					} else if (cr.isObsolete()) {
						// ignore
						LOG.info(reduceTask.getTaskID()
								+ " Ignoring obsolete copy result for Map Task: "
								+ cr.getLocation().getTaskAttemptId()
								+ " from host: " + cr.getHost());
					} else {
						retryFetches.add(cr.getLocation());

						// note the failed-fetch
						TaskAttemptID mapTaskId = cr.getLocation()
								.getTaskAttemptId();
						TaskID mapId = cr.getLocation().getTaskId();

						totalFailures++;
						Integer noFailedFetches = mapTaskToFailedFetchesMap
								.get(mapTaskId);
						noFailedFetches = (noFailedFetches == null) ? 1
								: (noFailedFetches + 1);
						mapTaskToFailedFetchesMap.put(mapTaskId,
								noFailedFetches);
						LOG.info("Task " + getTaskID() + ": Failed fetch #"
								+ noFailedFetches + " from " + mapTaskId);

						if (noFailedFetches >= abortFailureLimit) {
							LOG.fatal(noFailedFetches
									+ " failures downloading " + getTaskID()
									+ ".");
							umbilical.shuffleError(getTaskID(),
									"Exceeded the abort failure limit;"
											+ " bailing-out.", jvmContext);
						}

						checkAndInformJobTracker(
								noFailedFetches,
								mapTaskId,
								cr.getError().equals(
										CopyOutputErrorType.READ_ERROR));

						// note unique failed-fetch maps
						if (noFailedFetches == maxFetchFailuresBeforeReporting) {
							fetchFailedMaps.add(mapId);

							// did we have too many unique failed-fetch maps?
							// and did we fail on too many fetch attempts?
							// and did we progress enough
							// or did we wait for too long without any progress?

							// check if the reducer is healthy
							boolean reducerHealthy = (((float) totalFailures / (totalFailures + numCopied)) < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);

							// check if the reducer has progressed enough
							boolean reducerProgressedEnough = (((float) numCopied / numMaps) >= MIN_REQUIRED_PROGRESS_PERCENT);

							// check if the reducer is stalled for a long time
							// duration for which the reducer is stalled
							int stallDuration = (int) (System
									.currentTimeMillis() - lastProgressTime);
							// duration for which the reducer ran with progress
							int shuffleProgressDuration = (int) (lastProgressTime - startTime);
							// min time the reducer should run without getting
							// killed
							int minShuffleRunDuration = (shuffleProgressDuration > maxMapRuntime) ? shuffleProgressDuration
									: maxMapRuntime;
							boolean reducerStalled = (((float) stallDuration / minShuffleRunDuration) >= MAX_ALLOWED_STALL_TIME_PERCENT);

							// kill if not healthy and has insufficient progress
							if ((fetchFailedMaps.size() >= maxFailedUniqueFetches || fetchFailedMaps
									.size() == (numMaps - copiedMapOutputs
									.size()))
									&& !reducerHealthy
									&& (!reducerProgressedEnough || reducerStalled)) {
								LOG.fatal("Shuffle failed with too many fetch failures "
										+ "and insufficient progress!"
										+ "Killing task " + getTaskID() + ".");
								umbilical.shuffleError(getTaskID(),
										"Exceeded MAX_FAILED_UNIQUE_FETCHES;"
												+ " bailing-out.", jvmContext);
							}

						}

						currentTime = System.currentTimeMillis();
						long currentBackOff = (long) (INITIAL_PENALTY * Math
								.pow(PENALTY_GROWTH_RATE, noFailedFetches));

						penaltyBox.put(cr.getHost(), currentTime
								+ currentBackOff);
						LOG.warn(reduceTask.getTaskID() + " adding host "
								+ cr.getHost()
								+ " to penalty box, next contact in "
								+ (currentBackOff / 1000) + " seconds");
					}
					uniqueHosts.remove(cr.getHost());
					numInFlight--;
				}
			}

			// all done, inform the copiers to exit
			exitGetMapEvents = true;
			try {
				getMapEventsThread.join();
				LOG.info("getMapsEventsThread joined.");
			} catch (InterruptedException ie) {
				LOG.info("getMapsEventsThread threw an exception: "
						+ StringUtils.stringifyException(ie));
			}

			synchronized (copiers) {
				synchronized (scheduledCopies) {
					for (MapOutputCopier copier : copiers) {
						copier.interrupt();
					}
					copiers.clear();
				}
			}

			// copiers are done, exit and notify the waiting merge threads
			synchronized (mapOutputFilesOnDisk) {
				exitLocalFSMerge = true;
				mapOutputFilesOnDisk.notify();
			}
			/** this notify is not needed because LocalFSMerger is waited on 
			 * mapOutputFilesOnDisk rather than elements of partitionToMapOutputFilesOnDisk
			synchronized (partitionToMapOutputFilesOnDisk) {
				List<Integer> parts = getPartitions();
				SortedSet<FileStatus> ondisk = null;
				for (int p : parts) {
					ondisk = partitionToMapOutputFilesOnDisk.get(p);
					if (ondisk != null) {
						LOG.info("nofity thread wating for ondisk:" + ondisk);
						synchronized (ondisk) {
							ondisk.notify();
						}
					}
				}

			}
			 */
			ramManager.close();

			// Do a merge of in-memory files (if there are any)
			if (mergeThrowable == null) {
				try {
					// Wait for the on-disk merge to complete
					localFSMergerThread.join();
					LOG.info("Interleaved on-disk merge complete: "
							+ mapOutputFilesOnDisk.size() + " files left.");

					// wait for an ongoing merge (if it is in flight) to
					// complete
					inMemFSMergeThread.join();
					LOG.info("In-memory merge complete: "
							+ mapOutputsFilesInMemory.size() + " files left.");
				} catch (InterruptedException ie) {
					LOG.warn(reduceTask.getTaskID()
							+ " Final merge of the inmemory files threw an exception: "
							+ StringUtils.stringifyException(ie));
					// check if the last merge generated an error
					if (mergeThrowable != null) {
						mergeThrowable = ie;
					}
					return false;
				}
			}
			return mergeThrowable == null
					&& copiedMapOutputs.size() == numMaps * partitions.size();
		}

		// Notify the JobTracker
		// after every read error, if 'reportReadErrorImmediately' is true or
		// after every 'maxFetchFailuresBeforeReporting' failures
		protected void checkAndInformJobTracker(int failures,
				TaskAttemptID mapId, boolean readError) {
			if ((reportReadErrorImmediately && readError)
					|| ((failures % maxFetchFailuresBeforeReporting) == 0)) {
				synchronized (ReduceTask.this) {
					taskStatus.addFetchFailedMap(mapId);
					reporter.progress();
					LOG.info("Failed to fetch map-output from "
							+ mapId
							+ " even after MAX_FETCH_RETRIES_PER_MAP retries... "
							+ " or it is a read error, "
							+ " reporting to the JobTracker");
				}
			}
		}

		private long createInMemorySegments(
				List<Segment<K, V>> inMemorySegments, long leaveBytes)
				throws IOException {
			long totalSize = 0L;
			synchronized (mapOutputsFilesInMemory) {
				// fullSize could come from the RamManager, but files can be
				// closed but not yet present in mapOutputsFilesInMemory
				long fullSize = 0L;
				for (MapOutput mo : mapOutputsFilesInMemory) {
					fullSize += mo.data.length;
				}
				while (fullSize > leaveBytes) {
					MapOutput mo = mapOutputsFilesInMemory.remove(0);
					totalSize += mo.data.length;
					fullSize -= mo.data.length;
					Reader<K, V> reader = new InMemoryReader<K, V>(ramManager,
							mo.mapAttemptId, mo.data, 0, mo.data.length);
					Segment<K, V> segment = new Segment<K, V>(reader, true);
					inMemorySegments.add(segment);
				}
			}
			return totalSize;
		}

		private long createInMemorySegments(
				List<MapOutput> myMapOutputsFilesInMemory,
				List<Segment<K, V>> inMemorySegments, long leaveBytes,boolean comparediff)
				throws IOException {
			long totalSize = 0L;
			synchronized (mapOutputsFilesInMemory) {
				//assert(myMapOutputsFilesInMemory.size() == mapOutputsFilesInMemory.size());
				// fullSize could come from the RamManager, but files can be
				// closed but not yet present in mapOutputsFilesInMemory
				long fullSize = 0L;
				for (MapOutput mo : myMapOutputsFilesInMemory) {
					fullSize += mo.data.length;
				}
				while (fullSize > leaveBytes) {
					MapOutput mo = myMapOutputsFilesInMemory.remove(0);
					totalSize += mo.data.length;
					fullSize -= mo.data.length;
					Reader<K, V> reader = new InMemoryReader<K, V>(ramManager,
							mo.mapAttemptId, mo.data, 0, mo.data.length);
					Segment<K, V> segment = new Segment<K, V>(reader, true);
					inMemorySegments.add(segment);
					if (comparediff && !mapOutputsFilesInMemory.remove(mo)) {
						LOG.error("mapoutput:" + mo.toString()
								+ " is in mymapoutputfilesinmemory "
								+ "but not orginal mapoutputfilesinmemory\n"+getMapOutputListString(mapOutputsFilesInMemory));
					}
				}

			}

			return totalSize;
		}
private String getMapOutputListString(List<MapOutput> lmo){
	StringBuilder sb = new StringBuilder();
	synchronized(lmo){
		for(MapOutput mo:lmo){
			sb.append(mo.toString());
			sb.append("\n");
		}
	}
	return sb.toString();
}
		/**
		 * Create a RawKeyValueIterator from copied map outputs. All copying
		 * threads have exited, so all of the map outputs are available either
		 * in memory or on disk. We also know that no merges are in progress, so
		 * synchronization is more lax, here.
		 * 
		 * The iterator returned must satisfy the following constraints: 1.
		 * Fewer than io.sort.factor files may be sources 2. No more than
		 * maxInMemReduce bytes of map outputs may be resident in memory when
		 * the reduce begins
		 * 
		 * If we must perform an intermediate merge to satisfy (1), then we can
		 * keep the excluded outputs from (2) in memory and include them in the
		 * first merge pass. If not, then said outputs must be written to disk
		 * first.
		 */
		@SuppressWarnings("unchecked")
		private RawKeyValueIterator createKVIterator(int part, JobConf job,
				FileSystem fs, Reporter reporter) throws IOException {
 
			// merge config params
			Class<K> keyClass = (Class<K>) job.getMapOutputKeyClass();
			Class<V> valueClass = (Class<V>) job.getMapOutputValueClass();
			boolean keepInputs = job.getKeepFailedTaskFiles();
			final Path tmpDir = new Path(getTaskID().toString());
			final RawComparator<K> comparator = (RawComparator<K>) job
					.getOutputKeyComparator();

			// segments required to vacate memory
			List<Segment<K, V>> memDiskSegments = new ArrayList<Segment<K, V>>();
			long inMemToDiskBytes = 0;
			List<MapOutput> inMemMapOutput = new ArrayList<MapOutput>();
			List<MapOutput> mapOutputsFilesInMemory = partitionToFinishedMapOutputFilesInMemory
					.get(part);
			if (mapOutputsFilesInMemory.size() > 0) {
				TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;
				// 这里不需要同步partitionToMapOutputFilesInMemory，因为调用之前就已经确定了，所要值的存在性
				inMemMapOutput = partitionToFinishedMapOutputFilesInMemory
						.remove(part);
				//这里是用来处理shuffle完成的partition不应该使用普通的createInMemorySegments
				//从而和普通的争夺mapoutputfilesinmemory的锁
				inMemToDiskBytes = createInMemorySegments(inMemMapOutput,
						memDiskSegments, maxInMemReduce,false);
				final int numMemDiskSegments = memDiskSegments.size();
				//应用ioSortFactor与真正的内存片段数对比，因为这里马上就要做最后的reduce了，很快都会merge起来的
				int numMapoutputFilesOnDisk = partitionToFinishedMapOutputFilesOnDisk.get(part)==null?
						0:partitionToFinishedMapOutputFilesOnDisk.get(part).size();
				//if (numMemDiskSegments > 0 && ioSortFactor > mapOutputFilesOnDisk.size()) {
				if (numMemDiskSegments > 0 && ioSortFactor > numMapoutputFilesOnDisk) {
					// must spill to disk, but can't retain in-mem for
					// intermediate merge
					final Path outputPath = mapOutputFile.getInputFileForWrite(
							mapId, inMemToDiskBytes);// 这里调用了原来的getInputFileForWrite而没有添加
					// 区别的id, 不会出现覆盖以前的spill文件问题。因为这里只在run（）的主线程里调用了
					// 我在InMemoryMerge和spillToDisk中都为各个文件添加了特定的后缀
					// 这个不添加后缀的名字一定是没有被使用过的
					final RawKeyValueIterator rIter = Merger.merge(job, fs,
							keyClass, valueClass, memDiskSegments,
							numMemDiskSegments, tmpDir, comparator, reporter,
							spilledRecordsCounter, null);
					final Writer writer = new Writer(job, fs, outputPath,
							keyClass, valueClass, codec, null);
					try {
						Merger.writeFile(rIter, writer, reporter, job);
						//addToMapOutputFilesOnDisk(fs.getFileStatus(outputPath));
						addToMapOutputFilesOnDisk(fs.getFileStatus(outputPath),
								partitionToFinishedMapOutputFilesOnDisk
										.get(part));// 应该是放到finished集合里面,不需要再放到原有集合里面了
					} catch (Exception e) {
						if (null != outputPath) {
							fs.delete(outputPath, true);
						}
						throw new IOException("Final merge failed", e);
					} finally {
						if (null != writer) {
							writer.close();
						}
					}
					LOG.info("Merged " + numMemDiskSegments + " segments, "
							+ inMemToDiskBytes + " bytes to disk to satisfy "
							+ "reduce memory limit");
					inMemToDiskBytes = 0;
					memDiskSegments.clear();

				} else if (inMemToDiskBytes != 0) {
					LOG.info("Keeping " + numMemDiskSegments + " segments, "
							+ inMemToDiskBytes + " bytes in memory for "
							+ "intermediate, on-disk merge");
				}
			}

			// segments on disk
			List<Segment<K, V>> diskSegments = new ArrayList<Segment<K, V>>();
			long onDiskBytes = inMemToDiskBytes;
			// Path[] onDisk = getMapFiles(fs, false);
			Path[] onDisk = getMapFiles(part, fs, false);
			for (Path file : onDisk) {
				onDiskBytes += fs.getFileStatus(file).getLen();
				diskSegments.add(new Segment<K, V>(job, fs, file, codec,
						keepInputs));
			}
			LOG.info("Merging " + onDisk.length + " files, " + onDiskBytes
					+ " bytes from disk");
			Collections.sort(diskSegments, new Comparator<Segment<K, V>>() {
				public int compare(Segment<K, V> o1, Segment<K, V> o2) {
					if (o1.getLength() == o2.getLength()) {
						return 0;
					}
					return o1.getLength() < o2.getLength() ? -1 : 1;
				}
			});

			// build final list of segments from merged backed by disk + in-mem
			List<Segment<K, V>> finalSegments = new ArrayList<Segment<K, V>>();
			long inMemBytes = createInMemorySegments(inMemMapOutput,
					finalSegments, 0,false);
			LOG.info("Merging " + finalSegments.size() + " segments, "
					+ inMemBytes + " bytes from memory into reduce");
			if (0 != onDiskBytes) {
				final int numInMemSegments = memDiskSegments.size();
				diskSegments.addAll(0, memDiskSegments);
				memDiskSegments.clear();
				RawKeyValueIterator diskMerge = Merger.merge(job, fs, keyClass,
						valueClass, codec, diskSegments, ioSortFactor,
						numInMemSegments, tmpDir, comparator, reporter, false,
						spilledRecordsCounter, null);
				diskSegments.clear();
				if (0 == finalSegments.size()) {
					return diskMerge;
				}
				finalSegments.add(new Segment<K, V>(new RawKVIteratorReader(
						diskMerge, onDiskBytes), true));
			}
			return Merger.merge(job, fs, keyClass, valueClass, finalSegments,
					finalSegments.size(), tmpDir, comparator, reporter,
					spilledRecordsCounter, null);
		}

		class RawKVIteratorReader extends IFile.Reader<K, V> {

			private final RawKeyValueIterator kvIter;

			public RawKVIteratorReader(RawKeyValueIterator kvIter, long size)
					throws IOException {
				super(null, null, size, null, spilledRecordsCounter);
				this.kvIter = kvIter;
			}

			public boolean next(DataInputBuffer key, DataInputBuffer value)
					throws IOException {
				if (kvIter.next()) {
					final DataInputBuffer kb = kvIter.getKey();
					final DataInputBuffer vb = kvIter.getValue();
					final int kp = kb.getPosition();
					final int klen = kb.getLength() - kp;
					key.reset(kb.getData(), kp, klen);
					final int vp = vb.getPosition();
					final int vlen = vb.getLength() - vp;
					value.reset(vb.getData(), vp, vlen);
					bytesRead += klen + vlen;
					return true;
				}
				return false;
			}

			public long getPosition() throws IOException {
				return bytesRead;
			}

			public void close() throws IOException {
				kvIter.close();
			}
		}

		private CopyResult getCopyResult(int numInFlight,
				int numEventsAtStartOfScheduling) {
			boolean waitedForNewEvents = false;

			synchronized (copyResultsOrNewEventsLock) {
				while (copyResults.isEmpty()) {
					try {
						// The idea is that if we have scheduled enough, we can
						// wait until
						// we hear from one of the copiers, or until there are
						// new
						// map events ready to be scheduled
						if (busyEnough(numInFlight)) {
							// All of the fetcher threads are busy. So, no sense
							// trying
							// to schedule more until one finishes.
							copyResultsOrNewEventsLock.wait();
						} else if (numEventsFetched == numEventsAtStartOfScheduling
								&& !waitedForNewEvents) {
							// no sense trying to schedule more, since there are
							// no
							// new events to even try to schedule.
							// We could handle this with a normal wait() without
							// a timeout,
							// but since this code is being introduced in a
							// stable branch,
							// we want to be very conservative. A 2-second wait
							// is enough
							// to prevent the busy-loop experienced before.
							waitedForNewEvents = true;
							copyResultsOrNewEventsLock.wait(2000);
						} else {
							return null;
						}
					} catch (InterruptedException e) {
					}
				}
				List<Integer> parts = getPartitions();
				for (int part : parts) {
					if (partitionToCopyResults.get(part).size() > 0) {
						CopyResult res = partitionToCopyResults.get(part)
								.remove(0);
						copyResults.remove(res);
						return res;
					}
				}
				// return copyResults.remove(0);
				return null;
			}
		}

		private void addToMapOutputFilesOnDisk(FileStatus status) {
			synchronized (mapOutputFilesOnDisk) {
				mapOutputFilesOnDisk.add(status);
				mapOutputFilesOnDisk.notify();
			}
		}

		private void addToMapOutputFilesOnDisk(FileStatus status,
				SortedSet<FileStatus> ondisk) {
			synchronized (ondisk) {
				ondisk.add(status);
				ondisk.notify();
			}
		}

		/**
		 * Starts merging the local copy (on disk) of the map's output so that
		 * most of the reducer's input is sorted i.e overlapping shuffle and
		 * merge phases.
		 */
		private class LocalFSMerger extends Thread {
			private LocalFileSystem localFileSys;

			public LocalFSMerger(LocalFileSystem fs) {
				this.localFileSys = fs;
				setName("Thread for merging on-disk files");
				setDaemon(true);
			}

			/**
			 * DLB这里会有许多小的磁盘文件，参数ioSortFactor是对整体的spill文件个数起作用
			 * 而这里应该对单个partition的spill文件数起作用
			 * 这时就需要用户对这个ioSortFactor有新的认识
			 */
			@SuppressWarnings("unchecked")
			public void run() {
				try {
					LOG.info(reduceTask.getTaskID() + " Thread started: "
							+ getName());
					while (!exitLocalFSMerge) {
						synchronized (mapOutputFilesOnDisk) {
							while (!exitLocalFSMerge
									&& mapOutputFilesOnDisk.size() < (2 * ioSortFactor - 1)) {
								LOG.info(reduceTask.getTaskID()
										+ " Thread waiting: " + getName());
								mapOutputFilesOnDisk.wait();
							}
						}
						if (exitLocalFSMerge) {// to avoid running one extra
												// time in the end
							break;
						}
						List<Path> mapFiles = new ArrayList<Path>();
						long approxOutputSize = 0;
						int bytesPerSum = reduceTask.getConf().getInt(
								"io.bytes.per.checksum", 512);
						LOG.info(reduceTask.getTaskID() + "We have  "
								+ mapOutputFilesOnDisk.size()
								+ " map outputs on disk. "
								+ "Triggering merge of " + ioSortFactor
								+ " files");
						// 1. Prepare the list of files to be merged. This list
						// is prepared
						// using a list of map output files on disk. Currently
						// we merge
						// io.sort.factor files into 1.
						List<Integer> parts = getPartitions();
						int i = 0;// 总共加起来不超过iosortfactor
						for (int part : parts) {
							synchronized (reduceLock) {
								LOG.info("local fs merger got reduce lock");
								if (partitionToCopyFinished.get(part))
									//return;//我怎么会在这里写个return！！应该是break
									break;
								partitionToNumMergeThread.get(part)
										.incrementAndGet();
							}
							LOG.info("local fs merger release reduce lock");
							
							synchronized (mapOutputFilesOnDisk) {
								SortedSet<FileStatus> myMapOutputFilesOnDisk = partitionToMapOutputFilesOnDisk
								.get(part);
								for (; i < ioSortFactor; ++i) {
									FileStatus filestatus;
									try{
										filestatus = myMapOutputFilesOnDisk.first();
									}catch(NoSuchElementException nsee){
										break;
									}
									myMapOutputFilesOnDisk.remove(filestatus);
									mapOutputFilesOnDisk.remove(filestatus);

									mapFiles.add(filestatus.getPath());
									approxOutputSize += filestatus.getLen();
								}
							}

							// sanity check
							if (mapFiles.size() == 0) {
								break;
							}

							// add the checksum length
							approxOutputSize += ChecksumFileSystem
									.getChecksumLength(approxOutputSize,
											bytesPerSum);

							// 2. Start the on-disk merge process
							Path outputPath = lDirAlloc.getLocalPathForWrite(
									mapFiles.get(0).toString(),
									approxOutputSize, conf).suffix(".merged");
							Writer writer = new Writer(conf, rfs, outputPath,
									conf.getMapOutputKeyClass(),
									conf.getMapOutputValueClass(), codec, null);
							RawKeyValueIterator iter = null;
							Path tmpDir = new Path(reduceTask.getTaskID()
									.toString());
							try {
								iter = Merger.merge(conf, rfs, conf
										.getMapOutputKeyClass(), conf
										.getMapOutputValueClass(), codec,
										mapFiles.toArray(new Path[mapFiles
												.size()]), true, ioSortFactor,
										tmpDir, conf.getOutputKeyComparator(),
										reporter, spilledRecordsCounter, null);

								Merger.writeFile(iter, writer, reporter, conf);
								writer.close();
							} catch (Exception e) {
								localFileSys.delete(outputPath, true);
								throw new IOException(
										StringUtils.stringifyException(e));
							}

							synchronized (reduceLock) {
								LOG.info("local fs merger got reduce lock 2");
								// Note the output of the merge
								FileStatus status = localFileSys
										.getFileStatus(outputPath);
								SortedSet<FileStatus> ondisk = partitionToMapOutputFilesOnDisk
										.get(part);
								if (ondisk != null) {
									/**
									synchronized (ondisk) {
										addToMapOutputFilesOnDisk(status,
												ondisk);
									}*/
									synchronized (mapOutputFilesOnDisk) {
										addToMapOutputFilesOnDisk(status,
												ondisk);
										addToMapOutputFilesOnDisk(status);
									}
								} else {
									ondisk = partitionToFinishedMapOutputFilesOnDisk
											.get(part);
									synchronized (ondisk) {
										addToMapOutputFilesOnDisk(status,
												ondisk);
									}
								}
								partitionToNumMergeThread.get(part)
										.decrementAndGet();
							}
							LOG.info("local fs merger release reduce lock2");
							LOG.info(reduceTask.getTaskID()
									+ " Finished merging partition:"
									+ part
									+ " total"
									+ mapFiles.size()
									+ " map output files on disk of total-size "
									+ approxOutputSize
									+ "."
									+ " Local output file is "
									+ outputPath
									+ " of size "
									+ localFileSys.getFileStatus(outputPath)
											.getLen());

							// 恢复初始状态
							mapFiles.clear();
							approxOutputSize = 0;
						}

					}
				} catch (Exception e) {
					LOG.warn(reduceTask.getTaskID()
							+ " Merging of the local FS files threw an exception: "
							+ StringUtils.stringifyException(e));
					if (mergeThrowable == null) {
						mergeThrowable = e;
					}
				} catch (Throwable t) {
					String msg = getTaskID()
							+ " : Failed to merge on the local FS"
							+ StringUtils.stringifyException(t);
					reportFatalError(getTaskID(), t, msg);
				}
			}
		}

		private class InMemFSMergeThread extends Thread {

			public InMemFSMergeThread() {
				setName("Thread for merging in memory files");
				setDaemon(true);
			}

			public void run() {
				LOG.info(reduceTask.getTaskID() + " Thread started: "
						+ getName());
				try {
					boolean exit = false;
					do {
						exit = ramManager.waitForDataToMerge();
						if (!exit) {
							List<Integer> parts = getPartitions();
							for (int part : parts) {
								if (partitionToCopyFinished.get(part))
									continue;
								doInMemMerge(part);
							}
						}
					} while (!exit);
				} catch (Exception e) {
					LOG.warn(reduceTask.getTaskID()
							+ " Merge of the inmemory files threw an exception: "
							+ StringUtils.stringifyException(e));
					ReduceCopier.this.mergeThrowable = e;
				} catch (Throwable t) {
					String msg = getTaskID() + " : Failed to merge in memory"
							+ StringUtils.stringifyException(t);
					reportFatalError(getTaskID(), t, msg);
				}
			}

			private AtomicInteger spillFileSyncNum = new AtomicInteger(0);

			@SuppressWarnings("unchecked")
			private void doInMemMerge(int part) throws IOException {
				List<MapOutput> mapOutputsFilesInMemory = partitionToMapOutputFilesInMemory
						.get(part);
				if (mapOutputsFilesInMemory == null
						|| mapOutputsFilesInMemory.size() == 0) {
					return;
				}

				synchronized (reduceLock) {
					LOG.info("in mem reduce got reduce lock");
					if (partitionToCopyFinished.get(part))
						return;
					partitionToNumMergeThread.get(part).incrementAndGet();
				}
				LOG.info("in mem reduce release reduce lock");
				// name this output file same as the name of the first file that
				// is
				// there in the current list of inmem files (this is guaranteed
				// to
				// be absent on the disk currently. So we don't overwrite a
				// prev.
				// created spill). Also we need to create the output file now
				// since
				// it is not guaranteed that this file will be present after
				// merge
				// is called (we delete empty files as soon as we see them
				// in the merge method)

				// figure out the mapId
				TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;

				List<Segment<K, V>> inMemorySegments = new ArrayList<Segment<K, V>>();
				long mergeOutputSize = createInMemorySegments(
						mapOutputsFilesInMemory, inMemorySegments, 0,true);
				int noInMemorySegments = inMemorySegments.size();

				// Path outputPath = mapOutputFile.getInputFileForWrite(mapId,
				// mergeOutputSize);

				Path outputPath = mapOutputFile.getInputFileForWrite(mapId,
						spillFileSyncNum.addAndGet(1), mergeOutputSize);

				Writer writer = new Writer(conf, rfs, outputPath,
						conf.getMapOutputKeyClass(),
						conf.getMapOutputValueClass(), codec, null);

				RawKeyValueIterator rIter = null;
				try {
					LOG.info("Initiating in-memory merge with "
							+ noInMemorySegments + " segments...");

					rIter = Merger.merge(conf, rfs,
							(Class<K>) conf.getMapOutputKeyClass(),
							(Class<V>) conf.getMapOutputValueClass(),
							inMemorySegments, inMemorySegments.size(),
							new Path(reduceTask.getTaskID().toString()),
							conf.getOutputKeyComparator(), reporter,
							spilledRecordsCounter, null);

					if (combinerRunner == null) {
						Merger.writeFile(rIter, writer, reporter, conf);
					} else {
						combineCollector.setWriter(writer);
						combinerRunner.combine(rIter, combineCollector);
					}
					writer.close();

					LOG.info(reduceTask.getTaskID()
							+ " Merge of the partition:" + part + " total"
							+ noInMemorySegments + " files in-memory complete."
							+ " Local file is " + outputPath + " of size "
							+ localFileSys.getFileStatus(outputPath).getLen());
				} catch (Exception e) {
					// make sure that we delete the ondisk file that we created
					// earlier when we invoked cloneFileAttributes
					localFileSys.delete(outputPath, true);
					throw (IOException) new IOException(
							"Intermediate merge failed").initCause(e);
				}

				synchronized (reduceLock) {
					LOG.info("in mem reduce got reduce lock 2");
					// Note the output of the merge
					FileStatus status = localFileSys.getFileStatus(outputPath);
					SortedSet<FileStatus> ondisk = partitionToMapOutputFilesOnDisk
							.get(part);
					if (ondisk != null) {
						/**
						synchronized (ondisk) {
							addToMapOutputFilesOnDisk(status, ondisk);
						}*/
						synchronized (mapOutputFilesOnDisk) {
							addToMapOutputFilesOnDisk(status, ondisk);
							addToMapOutputFilesOnDisk(status);
						}
						LOG.info("added into mapoutputfilesondisk");
					} else {
						ondisk = partitionToFinishedMapOutputFilesOnDisk
								.get(part);
						synchronized (ondisk) {
							addToMapOutputFilesOnDisk(status, ondisk);
						}
						LOG.info("added into finished mapoutputfilesondisk");
					}

					partitionToNumMergeThread.get(part).decrementAndGet();
				}
				LOG.info("in mem reduce release reduce lock2");
			}
		}

		private class GetMapEventsThread extends Thread {

			private IntWritable fromEventId = new IntWritable(0);
			private static final long SLEEP_TIME = 1000;

			public GetMapEventsThread() {
				setName("Thread for polling Map Completion Events");
				setDaemon(true);
			}

			@Override
			public void run() {

				LOG.info(reduceTask.getTaskID() + " Thread started: "
						+ getName());

				do {
					try {
						int numNewMaps = getMapCompletionEvents();
						if (numNewMaps > 0) {
							synchronized (copyResultsOrNewEventsLock) {
								numEventsFetched += numNewMaps;
								copyResultsOrNewEventsLock.notifyAll();
							}
						}
						if (LOG.isDebugEnabled()) {
							if (numNewMaps > 0) {
								LOG.debug(reduceTask.getTaskID() + ": "
										+ "Got " + numNewMaps
										+ " new map-outputs");
							}
						}
						Thread.sleep(SLEEP_TIME);
					} catch (InterruptedException e) {
						LOG.warn(reduceTask.getTaskID()
								+ " GetMapEventsThread returning after an "
								+ " interrupted exception");
						return;
					} catch (Throwable t) {
						String msg = reduceTask.getTaskID()
								+ " GetMapEventsThread Ignoring exception : "
								+ StringUtils.stringifyException(t);
						reportFatalError(getTaskID(), t, msg);
					}
				} while (!exitGetMapEvents);

				LOG.info("GetMapEventsThread exiting");

			}

			private IntWritable partsFromEventId = new IntWritable(0);

			/**
			 * Queries the {@link TaskTracker} for a set of map-completion
			 * events from a given event ID.
			 * 
			 * @throws IOException
			 */
			private int getMapCompletionEvents() throws IOException {

				int numNewMaps = 0;

				MapTaskCompletionEventsUpdate update = umbilical
						.getMapCompletionEvents(reduceTask.getJobID(),
								fromEventId.get(), MAX_EVENTS_TO_FETCH,
								reduceTask.getTaskID(), jvmContext,
								partsFromEventId.get());
				TaskCompletionEvent events[] = update
						.getMapTaskCompletionEvents();

				NewPartitionAddedEvent npae = update
						.getNewPartitionAddedEvent();
				// Check if the reset is required.
				// Since there is no ordering of the task completion events at
				// the
				// reducer, the only option to sync with the new jobtracker is
				// to reset
				// the events index
				if (update.shouldReset()) {
					fromEventId.set(0);
					obsoleteMapIds.clear(); // clear the obsolete map
					mapLocations.clear(); // clear the map locations mapping
					partitionToMapLocations.clear();
					persistentMapLocations.clear();// 与mapLocations保持增量同步
					partsFromEventId.set(0);
				}

				// Update the last seen event ID
				fromEventId.set(fromEventId.get() + events.length);

				partsFromEventId.set(partsFromEventId.get() + npae.getEvents());

				// 处理NewPartitionAddedEvent
				// LOG.info("------------in reduce task, start get add new partition"+
				// " npae is null?" + (npae == null));
				if ( !partitionArrangeFinished.get()) {
					Integer[] parts = npae.getAddEdPartitions();
					LOG.info("---------num new part:" + parts.length);
					// 这里可能会重复多次获取同一个cocurrentlist List<MapOutputLocation> loc =
					// mapLocations.get(originalMapOutputLoc.getHost());
					// 带来一些效率问题
					if (parts.length > 0) {
						for (Map.Entry<TaskAttemptID, MapOutputLocation> en : persistentMapLocations
								.entrySet()) {
							TaskAttemptID taskId = en.getKey();
							MapOutputLocation originalMapOutputLoc = en
									.getValue();
							List<MapOutputLocation> loc = mapLocations
									.get(originalMapOutputLoc.getHost());

							if (loc == null) {// 这里永远不应该走到，loc不应该为null这里只是保险
								LOG.info("add previous maptasks' mapoutputlocation for newly added partition:"
										+ parts);
								loc = Collections
										.synchronizedList(new LinkedList<MapOutputLocation>());
								mapLocations.put(
										originalMapOutputLoc.getHost(), loc);
							}

							for (int p : parts) {
								MapOutputLocation mapOutput = new MapOutputLocation(
										originalMapOutputLoc.getTaskAttemptId(),
										originalMapOutputLoc.getHost(),
										new URL(
												originalMapOutputLoc
														.getOutputLocation()
														.toString()
														.substring(
																0,
																originalMapOutputLoc
																		.getOutputLocation()
																		.toString()
																		.indexOf(
																				"&reduce"))
														+ "&reduce=" + p));
								loc.add(mapOutput);

								/** 建立partition 到 maplocations的映射 **/
								Map<String, List<MapOutputLocation>> hloc = partitionToMapLocations
										.get(p);
								List<MapOutputLocation> mloc = null;
								if (hloc == null) {
									hloc = new ConcurrentHashMap<String, List<MapOutputLocation>>();
									partitionToMapLocations.put(p, hloc);
								}
								mloc = hloc.get(originalMapOutputLoc.getHost());
								if (mloc == null) {
									mloc = Collections
											.synchronizedList(new ArrayList<MapOutputLocation>());
									hloc.put(originalMapOutputLoc.getHost(),
											mloc);
								}
								mloc.add(mapOutput);

								numNewMaps++;
								LOG.info("--------------------------------added mapoutputlocation "
										+ mapOutput);
							}
						}
					}

					// 更新copyresult,maplocations,mapoutputfilesinmem,copyresult
					for (int p : parts) {
						partitionToCopiedMapOutputs
								.put(p,
										Collections
												.synchronizedSet(new TreeSet<MapOutputLocation>()));
						partitionToMapLocations
								.put(p,
										new ConcurrentHashMap<String, List<MapOutputLocation>>());
						partitionToMapOutputFilesInMemory.put(p, Collections
								.synchronizedList(new LinkedList<MapOutput>()));
						partitionToCopyResults.put(p,
								new ArrayList<CopyResult>(30));
						partitionToFinishedCopy.put(p, 0);
					}
					addNewPartitions(parts);

					partitionArrangeFinished.set(npae
							.partitionArrangeFinished());
					LOG.info("!!!!!!!!!!!!!!!!!!!finished="
							+ partitionArrangeFinished.get());
				}

				// Process the TaskCompletionEvents:
				// 1. Save the SUCCEEDED maps in knownOutputs to fetch the
				// outputs.
				// 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to
				// stop
				// fetching from those maps.
				// 3. Remove TIPFAILED maps from neededOutputs since we don't
				// need their
				// outputs at all.
				for (TaskCompletionEvent event : events) {
					switch (event.getTaskStatus()) {
					case SUCCEEDED: {
						URI u = URI.create(event.getTaskTrackerHttp());
						String host = u.getHost();
						TaskAttemptID taskId = event.getTaskAttemptId();

						List<MapOutputLocation> loc = mapLocations.get(host);
						// List<MapOutputLocation> mloc =
						// partitionToMapLocations.get(0);
						if (loc == null) {
							loc = Collections
									.synchronizedList(new LinkedList<MapOutputLocation>());
							mapLocations.put(host, loc);
						}
						synchronized (partitions) {// 为每一个指定的partition生成对应的MapOutputLocation
							for (Integer part : partitions) {
								URL mapOutputLocation = new URL(
										event.getTaskTrackerHttp()
												+ "/mapOutput?job="
												+ taskId.getJobID() + "&map="
												+ taskId + "&reduce=" + part);
								MapOutputLocation maploc = new MapOutputLocation(
										taskId, host, mapOutputLocation);
								loc.add(maploc);

								/** 建立partition 到 maplocations的映射 **/
								Map<String, List<MapOutputLocation>> hloc = partitionToMapLocations
										.get(part);
								List<MapOutputLocation> mloc = null;
								if (hloc == null) {
									hloc = new ConcurrentHashMap<String, List<MapOutputLocation>>();
									partitionToMapLocations.put(part, hloc);
								}
								mloc = hloc.get(host);
								if (mloc == null) {
									mloc = Collections
											.synchronizedList(new ArrayList<MapOutputLocation>());
									hloc.put(host, mloc);
								}
								mloc.add(maploc);

								if (!persistentMapLocations.containsKey(taskId)) {
									persistentMapLocations.put(taskId, maploc);
								}
								// persistentMapLocation只要保存一份TaskID-host-URL
								// with partitio不要每个partition都保存
								numNewMaps++;// 这个是用来加到munEventsFetched上来判断是否有新的可调度的MapOutputlocation,因此这里应该增加计数
							}
						}

					}
						break;
					case FAILED:
					case KILLED:
					case OBSOLETE: {
						obsoleteMapIds.add(event.getTaskAttemptId());
						LOG.info("Ignoring obsolete output of "
								+ event.getTaskStatus() + " map-task: '"
								+ event.getTaskAttemptId() + "'");
					}
						break;
					case TIPFAILED: {
						// 一个map 失败后它的spill在这里设置变成已经拷贝完的.
						// 最终应该是将未拷贝的partition设置为拷贝完，现在只保证copiedmapoutputs大小正确
						// 还有问题，在现在这种情况下partitions的大小是可变的，在arrange完成之前是不能确定并将未完成的直接加进去
						// copiedMapOutputs.add(event.getTaskAttemptId().getTaskID());
						TaskID taskId = event.getTaskAttemptId().getTaskID();
						int[] prevCopied = new int[partitions.size()];
						int numPrevCopied = 0;
						for (MapOutputLocation mol : copiedMapOutputs) {
							if (mol.getTaskId().equals(taskId))
								numPrevCopied++;
						}
						int numNotCopiedYet = partitions.size() - numPrevCopied;
						for (int i = 0; i < numNotCopiedYet; i++) {
							// copiedMapOutputs.add(e)
						}
					}
						break;
					}
				}
				return numNewMaps;
			}
		}
	}

	/**
	 * Return the exponent of the power of two closest to the given positive
	 * value, or zero if value leq 0. This follows the observation that the msb
	 * of a given value is also the closest power of two, unless the bit
	 * following it is set.
	 */
	private static int getClosestPowerOf2(int value) {
		if (value <= 0)
			throw new IllegalArgumentException("Undefined for " + value);
		final int hob = Integer.highestOneBit(value);
		return Integer.numberOfTrailingZeros(hob)
				+ (((hob >>> 1) & value) == 0 ? 0 : 1);
	}
}
