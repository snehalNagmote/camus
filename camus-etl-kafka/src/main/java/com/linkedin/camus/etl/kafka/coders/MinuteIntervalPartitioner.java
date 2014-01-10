package com.linkedin.camus.etl.kafka.coders;

import java.util.Calendar;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.mortbay.log.Log;

import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

public class MinuteIntervalPartitioner implements Partitioner {
	Logger logger = Logger.getLogger(MinuteIntervalPartitioner.class);
	protected static final String OUTPUT_DATE_FORMAT = "YYYY/MM/dd/HH/mm";
	protected DateTimeFormatter outputDateFormatter = null;

	@Override
	public String encodePartition(JobContext context, IEtlKey key) {
		long outfilePartitionMs = EtlMultiOutputFormat
				.getEtlOutputFileTimePartitionMins(context) * 60000L;
		logger.info("key.getTime()" + key.getTime());
		logger.info("DateUtils partition"+DateUtils.getPartition(outfilePartitionMs, key.getTime()));
		return "" + DateUtils.getPartition(outfilePartitionMs, key.getTime());
	}

	@Override
	public String generatePartitionedPath(JobContext context, String topic,
			int brokerId, int partitionId, String encodedPartition) {
		// We only need to initialize outputDateFormatter with the default
		// timeZone once.
		if (outputDateFormatter == null) {
			outputDateFormatter = DateUtils.getDateTimeFormatter(
					OUTPUT_DATE_FORMAT, DateTimeZone.forID(EtlMultiOutputFormat
							.getDefaultTimeZone(context)));
		}

		StringBuilder sb = new StringBuilder();
		sb.append(topic).append("/");
		sb.append(EtlMultiOutputFormat.getDestPathTopicSubDir(context)).append(
				"/");
		DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
		Calendar calendar=bucket.toGregorianCalendar();
		int unroundedMinutes = calendar.get(Calendar.MINUTE);
		int mod = unroundedMinutes % 15;
		calendar.add(Calendar.MINUTE, mod < 8 ? -mod : (15 - mod));
		calendar.add(Calendar.MINUTE, mod < 8 ? -mod : (15 - mod));
		sb.append(calendar.get(Calendar.YEAR)).append("/");
		sb.append(calendar.get(Calendar.MONTH) + 1).append("/");
		sb.append(calendar.get(Calendar.DATE)).append("/");
		sb.append(calendar.get(Calendar.HOUR)).append("/");
		sb.append(calendar.get(Calendar.MINUTE)).append("/");
		logger.info(bucket.toString(OUTPUT_DATE_FORMAT));
		return sb.toString();
		//sb.append(bucket.toString(OUTPUT_DATE_FORMAT));
	}
	
}
