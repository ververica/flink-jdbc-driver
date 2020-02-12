# Flink JDBC Driver

Flink JDBC driver is a Java library for accessing and manipulating [Apache Flink](https://flink.apache.org/) clusters by connecting to a [Flink SQL gateway](https://github.com/ververica/flink-sql-gateway) as the JDBC server.

This project is at an early stage. Feel free to file an issue if you meet any problems or have any suggestions.

# Usage

Before using Flink JDBC driver, you need to start a [Flink SQL gateway](https://github.com/ververica/flink-sql-gateway) as the JDBC server and binds it with your Flink cluster. We now assume that you have a gateway started and connected to a running Flink cluster.

## Use with a JDBC Tool
### Use with Beeline

Beeline is the command line tool for accessing [Apache Hive](https://hive.apache.org/), but it also supports general JDBC drivers. To install Hive and beeline, see [Hive documentation](https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-InstallationandConfiguration).

1. Download flink-jdbc-driver-(VERSION).jar from the [download page](https://github.com/ververica/flink-jdbc-driver/releases) and add it to `$HIVE_HOME/lib`.
2. Run beeline and connect to a Flink SQL gateway. You can specify the planner (`blink` or `old`) in the query parameter of the url. As Flink SQL gateway currently ignores user names and passwords, just leave them empty.
    ```
    beeline> !connect jdbc:flink://localhost:8083?planner=blink
    ```
3. Execute any statement you want.

**Sample Commands**
```
Beeline version 2.2.0 by Apache Hive
beeline> !connect jdbc:flink://localhost:8083?planner=blink
Connecting to jdbc:flink://localhost:8083?planner=blink
Enter username for jdbc:flink://localhost:8083?planner=blink: 
Enter password for jdbc:flink://localhost:8083?planner=blink: 
Connected to: Apache Flink (version 1.10.0)
Driver: Flink Driver (version 0.1)
Transaction isolation: TRANSACTION_REPEATABLE_READ
0: jdbc:flink://localhost:8083> CREATE TABLE T(
. . . . . . . . . . . . . . . >   a INT,
. . . . . . . . . . . . . . . >   b VARCHAR(10)
. . . . . . . . . . . . . . . > ) WITH (
. . . . . . . . . . . . . . . >   'connector.type' = 'filesystem',
. . . . . . . . . . . . . . . >   'connector.path' = 'file:///tmp/T.csv',
. . . . . . . . . . . . . . . >   'format.type' = 'csv',
. . . . . . . . . . . . . . . >   'format.derive-schema' = 'true'
. . . . . . . . . . . . . . . > );
No rows affected (0.158 seconds)
0: jdbc:flink://localhost:8083> INSERT INTO T VALUES (1, 'Hi'), (2, 'Hello');
No rows affected (4.747 seconds)
0: jdbc:flink://localhost:8083> SELECT * FROM T;
+----+--------+--+
| a  |   b    |
+----+--------+--+
| 1  | Hi     |
| 2  | Hello  |
+----+--------+--+
2 rows selected (0.994 seconds)
0: jdbc:flink://localhost:8083> 
```

### Use with Tableau
[Tableau](https://www.tableau.com/) is an interactive data visualization software. It supports *Other Database (JDBC)* connection from version 2018.3. You'll need Tableau with version >= 2018.3 to use Flink JDBC driver. For general usage of *Other Database (JDBC)* in Tableau, see [Tableau documentation](https://help.tableau.com/current/pro/desktop/en-us/examples_otherdatabases_jdbc.htm).

1. Download flink-jdbc-driver-(VERSION).jar from the [download page](https://github.com/ververica/flink-jdbc-driver/releases) and add it to Tableau driver path.
    * Windows: `C:\Program Files\Tableau\Drivers`
    * Mac: `~/Library/Tableau/Drivers`
    * Linux: `/opt/tableau/tableau_driver/jdbc`
2. Select *Other Database (JDBC)* under *Connect* and fill in the url of Flink SQL gateway. You can specify the planner (`blink` or `old`) in the query parameter of the url. Select *SQL92* dialect and leave user name and password empty.
3. Hit *Login* button and use Tableau as usual.

### Use with other JDBC Tools
Flink JDBC driver is a library for accessing Flink clusters through the JDBC API. Any tool supporting JDBC API can be used with Flink JDBC driver and [Flink SQL gateway](https://github.com/ververica/flink-sql-gateway). See the documentation of your desired tool on how to use a JDBC driver.

## Use with Java

Flink JDBC driver is a library for accessing Flink clusters through the JDBC API. For the general usage of JDBC in Java, see [JDBC tutorial](https://docs.oracle.com/javase/tutorial/jdbc/index.html) or [Oracle JDBC documentation](https://www.oracle.com/technetwork/java/javase/tech/index-jsp-136101.html).

1. Download flink-jdbc-driver-(VERSION).jar from the [download page](https://github.com/ververica/flink-jdbc-driver/releases) and add it to your classpath.
2. Connect to a Flink SQL gateway in your Java code. You can specify the planner (`blink` or `old`) in the query parameter of the url.
3. Execute any statement you want.

**Sample.java**
```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Sample {
	public static void main(String[] args) throws Exception {
		Connection connection = DriverManager.getConnection("jdbc:flink://localhost:8083?planner=blink");
		Statement statement = connection.createStatement();

		statement.executeUpdate("CREATE TABLE T(\n" +
			"  a INT,\n" +
			"  b VARCHAR(10)\n" +
			") WITH (\n" +
			"  'connector.type' = 'filesystem',\n" +
			"  'connector.path' = 'file:///tmp/T.csv',\n" +
			"  'format.type' = 'csv',\n" +
			"  'format.derive-schema' = 'true'\n" +
			")");
		statement.executeUpdate("INSERT INTO T VALUES (1, 'Hi'), (2, 'Hello')");
		ResultSet rs = statement.executeQuery("SELECT * FROM T");
		while (rs.next()) {
			System.out.println(rs.getInt(1) + ", " + rs.getString(2));
		}

		statement.close();
		connection.close();
	}
}
```

**Output**
```
1, Hi
2, Hello
```
