Build Collector
=================

This project uses Spring Boot to package the collector as an executable JAR with dependencies.

Building and Deploying
--------------------------------------

Run mvn install to package the collector into an executable JAR file. Copy this file to your server and launch it using
java -JAR bamboo-collector.jar. You will need to provide an application.properties file that contains information about how
to connect to the Dashboard MongoDB database instance, as well as properties the Bamboo collector requires. See
the Spring Boot [documentation](http://docs.spring.io/spring-boot/docs/current-SNAPSHOT/reference/htmlsingle/#boot-features-external-config-application-property-files)
for information about sourcing this properties file.

###Sample application.properties file
--------------------------------------

    #Database Name 
    spring.data.mongodb.database=dashboarddb

    #Database HostName - default is localhost
    spring.data.mongodb.host=10.0.1.1

    #Database Port - default is 27017
    spring.data.mongodb.port=9999

    #Database Username - default is blank
    spring.data.mongodb.username=db

    #Database Password - default is blank
    spring.data.mongodb.password=dbpass

    #Collector schedule (required)
    bamboo.cron=0 0/5 * * * *

    #Bamboo server (required) - Can provide multiple
    bamboo.servers[0]=http://Bamboo.company.com

    #Determines if build console log is collected - defaults to false
    bamboo.saveLog=true

    #Bamboo auth - set username/apiKey to use HTTP Basic Auth (blank=no auth)
    bamboo.username=
    bamboo.apiKey=