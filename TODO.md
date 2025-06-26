# Open issues
* tests with embedded kafka 
* Fail Fast (use technology)
  ```
          var props = filterProperties.properties();
        props.put("request.timeout.ms", 3000);
        props.put("connections.max.idle.ms", 5000);

        System.out.println(props);
        AdminClient adminClient = AdminClient.create(props);
        try {
            var result = adminClient.describeCluster().nodes().get(3, TimeUnit.SECONDS);

            if (result != null && result.size() > 1) {
                System.out.println("EVERITHING IS FINE");
            } else {
                System.out.println("Something is rotten ...");
            }
        } catch (Exception e)
        {
            System.out.println("Throw exception " + e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
  ```
* AVRO Support
* Evaluate different naming strategies for schema registry. Especially if different types need to be sent to the same topic (DDD-Events of different types but with strict order)
* Kafka Consumer
* Configure client.id
## JLegMed specific
* Health check for docker or other applications 
* Import KAFKA-broker from environment
* Check archetypes <image.directory>${artifactId}</image.directory>
