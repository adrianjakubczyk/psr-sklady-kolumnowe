import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;

import java.time.Duration;

public class Main {
    final private static Duration TIMEOUT = Duration.ofSeconds(10);
    final private static String KEYSPACE = "transport_company";

    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder().build()) {

            SimpleStatement drop = SchemaBuilder.dropKeyspace(KEYSPACE).ifExists().build().setTimeout(TIMEOUT);
            session.execute(drop);
            System.out.println("Statement \"" + drop.getQuery() + "\" executed successfully");

            SimpleStatement create = SchemaBuilder.createKeyspace(KEYSPACE).withSimpleStrategy(1).build().setTimeout(TIMEOUT);
            session.execute(create);
            System.out.println("Statement \"" + create.getQuery() + "\" executed successfully");

            session.execute(new SimpleStatementBuilder("USE " + KEYSPACE + ";").build().setTimeout(TIMEOUT));
            System.out.println("Using: " + KEYSPACE);

            createTable(session);


            Menu menu = new Menu(session);
            menu.selectOperation();


            SimpleStatement dropTable = SchemaBuilder.dropTable("package").build().setTimeout(TIMEOUT);
            session.execute(dropTable);

        }
    }

    public static void createTable(CqlSession session) {
        CreateType createType = SchemaBuilder.createType("address").withField("street", DataTypes.TEXT)
                .withField("houseNumber", DataTypes.INT).withField("apartmentNumber", DataTypes.INT);

        session.execute(createType.build());

        CreateTable createTable = SchemaBuilder.createTable("package")
                .withPartitionKey("id", DataTypes.INT)
                .withColumn("weight", DataTypes.DOUBLE)
                .withColumn("size_x", DataTypes.DOUBLE)
                .withColumn("size_y", DataTypes.DOUBLE)
                .withColumn("size_z", DataTypes.DOUBLE)
                .withColumn("address_sender", QueryBuilder.udt("address"))
                .withColumn("address_receiver", QueryBuilder.udt("address"))
                .withColumn("is_fragile", DataTypes.BOOLEAN);
        session.execute(createTable.build());
    }
}
