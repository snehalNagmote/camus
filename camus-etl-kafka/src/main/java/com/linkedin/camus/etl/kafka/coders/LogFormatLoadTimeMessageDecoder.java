package com.linkedin.camus.etl.kafka.coders;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;

public class LogFormatLoadTimeMessageDecoder extends
		MessageDecoder<byte[], String> {

	private static org.apache.log4j.Logger log = Logger
			.getLogger(LogFormatLoadTimeMessageDecoder.class);

	// Property for format of timestamp in JSON timestamp field.
	public static final String CAMUS_MESSAGE_TIMESTAMP_FORMAT = "camus.message.timestamp.format";
	public static final String DEFAULT_TIMESTAMP_FORMAT = "[dd/MMM/yyyy:HH:mm:ss Z]";

	// Property for the JSON field name of the timestamp.
	public static final String CAMUS_MESSAGE_TIMESTAMP_FIELD = "camus.message.timestamp.field";
	public static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";

	private String timestampFormat;
	private String timestampField;

	@Override
	public void init(Properties props, String topicName) {
		this.props = props;
		this.topicName = topicName;

		timestampFormat = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FORMAT,
				DEFAULT_TIMESTAMP_FORMAT);
		timestampField = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD,
				DEFAULT_TIMESTAMP_FIELD);
	}

	//Pass the payloadString as it is
	@Override
	public CamusWrapper<String> decode(byte[] payload) {
		long timestamp = 0;
		String payloadString;
		payloadString = new String(payload);
		// If timestamp wasn't set in the above block,
		// then set it to current time.
		if (timestamp == 0) {
			timestamp = System.currentTimeMillis();
			log.info("Getting current load time"+timestamp);
			
		}

		return new CamusWrapper<String>(payloadString, timestamp);
	}

}
