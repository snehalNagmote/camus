#!/bin/bash
hadoop jar camus-example/target/camus-example-0.1.0-SNAPSHOT-shaded.jar com.linkedin.camus.etl.kafka.CamusJob -P camus-example/src/main/resources/camus.properties
