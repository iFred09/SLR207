# Top-level Makefile for a no-Maven Java MapReduce demo

# === User-configurable vars ===
# file to process and thread count for MapReduce
FILE     ?= texts/CC-MAIN-20230321002050-20230321032050-00472.warc.wet
THREADS  ?= 4
MASTER_IP?= 127.0.0.1

# where sources live and where classes go
SRC_DIR  := src/main/java
BIN_DIR  := bin

# any 3rd-party jars you need (e.g. Paho MQTT client)
LIB_JARS := lib/org.eclipse.paho.client.mqttv3-1.2.5.jar

# classpath for compile & run
CLASSPATH := $(BIN_DIR):$(LIB_JARS)

# find all your .java files
JAVA_SRCS := $(shell find $(SRC_DIR) -name '*.java')

.PHONY: all compile run-mapreduce run-countandfreq run-multinodes clean help

all: compile

## Compile all .java into $(BIN_DIR)
compile:
	@echo "Compiling sources..."
	@mkdir -p $(BIN_DIR)
	@javac -d $(BIN_DIR) -cp "$(CLASSPATH)" $(JAVA_SRCS)

## Run the MapReduce driver (needs FILE and THREADS)
run-mapreduce: compile
	@echo "Running mapreduce.MapReduce on '$(FILE)' with $(THREADS) threads"
	@java -cp "$(CLASSPATH)" mapreduce.MapReduce "$(FILE)" $(THREADS)

## Run the word-count & frequency test harness
run-countandfreq: compile
	@echo "Running mapreduce.TestWordCountAndFrequency"
	@java -cp "$(CLASSPATH)" mapreduce.TestWordCountAndFrequency

## Run the multi-node MapReduce (needs FILE and MASTER_IP)
run-multinodes: compile
	@echo "Running mapreduce.MapReduceMultiNodes on '$(FILE)' with master '$(MASTER_IP)'"
	@java -cp "$(CLASSPATH)" mapreduce.MapReduceMultiNodes "$(FILE)" $(MASTER_IP)

## Wipe out compiled classes
clean:
	@echo "Cleaning up..."
	@rm -rf $(BIN_DIR)

## Show this help text
help:
	@echo "Usage: make [target] [VARIABLE=...]"
	@echo ""
	@echo "Targets:"
	@echo "  all               (default) compile"
	@echo "  compile           compile all Java sources"
	@echo "  run-mapreduce     run mapreduce.MapReduce with FILE=$(FILE) THREADS=$(THREADS)"
	@echo "  run-countandfreq  run TestWordCountAndFrequency"
	@echo "  run-multinodes    run MapReduceMultiNodes with FILE=$(FILE) MASTER_IP=$(MASTER_IP)"
	@echo "  clean             remove compiled classes"
	@echo "  help              this message"
	@echo ""
	@echo "You can override the input file, thread count, or master IP, e.g.:"
	@echo "  make run-mapreduce FILE=path/to/file.wet THREADS=8"
	@echo "  make run-multinodes FILE=path/to/file.wet MASTER_IP=192.168.1.10"
