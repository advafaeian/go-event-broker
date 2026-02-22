# Go Event Broker

Go-event-broker is a lightweight Kafka clone written in Go as part of the [Codecrafters challenge](https://app.codecrafters.io/courses/kafka/overview).

I'm Documenting the progress while completing each stage of the challenge.

---

## Progress


### 🏗️ Core Setup
- 🟢 Bind to a port
- 🟢️ Send Correlation ID
- 🟢 Parse Correlation ID
- 🟢 Parse API Version
- 🟢 Handle `ApiVersions` requests  

### 🧩 Concurrent Clients
- 🟢 Serial requests  
- 🟢 Concurrent requests  

### 🗂️ Listing Partitions
- 🟢️ Include `DescribeTopicPartitions` in `ApiVersions`  
- 🟢️ List for an unknown topic  
- 🟢️ List for a single partition  
- 🟢️ List for multiple partitions  
- ⚪️ List for multiple topics  

### 📦 Consuming Messages
- ⚪️ Include `Fetch` in `ApiVersions`  
- ⚪️ Fetch with no topics  
- ⚪️ Fetch with an unknown topic  
- ⚪️ Fetch with an empty topic  
- ⚪️ Fetch single message from disk  
- ⚪️ Fetch multiple messages from disk  

### 🚀 Producing Messages
- ⚪️ Include `Produce` in `ApiVersions`  
- ⚪️ Respond for invalid topic or partition  
- ⚪️ Respond for valid topic and partition  
- ⚪️ Produce a single record  
- ⚪️ Produce multiple records  
- ⚪️ Produce to multiple partitions  
- ⚪️ Produce to multiple partitions of multiple topics  
