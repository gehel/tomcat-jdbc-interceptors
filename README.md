Tomcat interceptors
===================

This is my attempt at a [Tomcat JDBC interceptor](http://tomcat.apache.org/tomcat-7.0-doc/jdbc-pool.html#JDBC_interceptors). This interceptor will collect timing and number of calls metrics and publish them to a [Statsd](https://github.com/etsy/statsd) server.

The following parameters are required to configure the interceptor:

* *hostname:* the hostname of the Statsd server (for example: `localhost`)
* *port:* the port of the Statsd server (for example: `8125`)
* *sampleRate:* to make sure we don't impact performances too badly, we only get statistics for a fraction of the requests (for example: `0.1`) 
* *prefix:* the prefix used to classify the metrics (for example: `myapp.jdbc`)

The interceptor can be configured as follow:
```xml
<Resource name="jdbc/TestDB"
          type="javax.sql.DataSource"
          factory="org.apache.tomcat.jdbc.pool.DataSourceFactory"
          jdbcInterceptors="ch.ledcom.tomcat.interceptors.StatsdInterceptor(hostname=localhost,port=8125,sampleRate=0.1,prefix=myapplication.jdbc)"
          username="myusername"
          password="password"
          [...]
          driverClassName="com.mysql.jdbc.Driver"
          url="jdbc:mysql://localhost:3306/mysql"/>
```

Of course, you will need to add the jar to the Tomcat lib directory. The jar containing this interceptor can be downloaded from [Maven Repo1](http://repo1.maven.org/maven2/ch/ledcom/tomcat/interceptors/tomcat-jdbc-interceptors/).

Maven generated site available as [GitHub pages](http://gehel.github.com/tomcat-jdbc-interceptors/).
