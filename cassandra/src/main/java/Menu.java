import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;

import java.util.Scanner;

public class Menu {

    final private Scanner scanner = new Scanner(System.in);
    final private CqlSession session;
    private int id = 0;

    public Menu(CqlSession session) {
        this.session = session;
    }

    private void printMenu() {
        int i = 1;
        System.out.println("===================================================");
        System.out.println("Please choose operation:");
        System.out.println(i++ + ": Add Package");
        System.out.println(i++ + ": Change Package receiver's address");
        System.out.println(i++ + ": Select by ID");
        System.out.println(i++ + ": Select heavier than");
        System.out.println(i++ + ": Select All");
        System.out.println(i++ + ": Delete by ID");
        System.out.println(i++ + ": Calculate average weight");
        System.out.println(i + ": Calculate average weight");

        System.out.println("9: Exit");
        System.out.println("===================================================");
    }

    public void selectOperation() {
        boolean doExit = false;
        while (!doExit) {
            printMenu();
            int op = scanner.nextInt();
            scanner.nextLine();
            switch (op) {
                case 1:
                    System.out.println("Add Package");
                    addPackage();
                    break;
                case 2:
                    System.out.println("Change Package receiver's address");
                    updateAddress();
                    break;
                case 3:
                    System.out.println("Select by ID");
                    selectById();
                    break;
                case 4:
                    System.out.println("Select heavier than");
                    selectHeavier();
                    break;
                case 5:
                    System.out.println("Select All");
                    selectAll();
                    break;
                case 6:
                    System.out.println("Delete");
                    deleteById();
                    break;
                case 7:
                    System.out.println("Calculate average weight");
                    calculateAvgWeight();
                    break;
                case 8:
                    System.out.println("Calculate average weight");
                    //calculateAvgAgeClient();
                    break;
                case 9:
                    System.out.println("Exit");
                    doExit = true;
                    break;
                default:
                    System.out.println("Not a recognizable choice");
                    break;
            }
        }

    }

    private void addPackage() {
        System.out.println("Weight:");
        double weight = scanner.nextDouble();
        System.out.println("Size_x:");
        double size_x = scanner.nextDouble();
        System.out.println("Size_y:");
        double size_y = scanner.nextDouble();
        System.out.println("Size_z:");
        double size_z = scanner.nextDouble();
        System.out.println("Is the package fragile:");
        boolean fragile = scanner.nextBoolean();
        scanner.nextLine();

        System.out.println("Sender's street:");
        String sStreet = scanner.nextLine();
        System.out.println("Sender's house number:");
        int sHouse = scanner.nextInt();
        System.out.println("Sender's apartment number:");
        int sApartment = scanner.nextInt();
        scanner.nextLine();

        System.out.println("Receiver's street:");
        String rStreet = scanner.nextLine();
        System.out.println("Receiver's house number:");
        int rHouse = scanner.nextInt();
        System.out.println("Receiver's apartment number:");
        int rApartment = scanner.nextInt();
        scanner.nextLine();


        Insert insert = QueryBuilder.insertInto("transport_company", "package")
                .value("id", QueryBuilder.raw(String.valueOf(id++)))
                .value("weight", QueryBuilder.raw(String.valueOf(weight)))
                .value("size_x", QueryBuilder.raw(String.valueOf(size_x)))
                .value("size_y", QueryBuilder.raw(String.valueOf(size_y)))
                .value("size_z", QueryBuilder.raw(String.valueOf(size_z)))
                .value("address_sender", QueryBuilder.raw("{street : '" + sStreet + "', houseNumber : " + sHouse + ", apartmentNumber : " + sApartment + "}"))
                .value("address_receiver", QueryBuilder.raw("{street : '" + rStreet + "', houseNumber : " + rHouse + ", apartmentNumber : " + rApartment + "}"))
                .value("is_fragile", QueryBuilder.raw(String.valueOf(fragile)));
        session.execute(insert.build());

    }

    private void updateAddress() {
        System.out.println("Package's id:");
        int id = scanner.nextInt();
        scanner.nextLine();

        System.out.println("Receiver's street:");
        String rStreet = scanner.nextLine();
        System.out.println("Receiver's house number:");
        int rHouse = scanner.nextInt();
        System.out.println("Receiver's apartment number:");
        int rApartment = scanner.nextInt();
        scanner.nextLine();

        Update update = QueryBuilder.update("package").setColumn("address_receiver", QueryBuilder.raw("{street : '" + rStreet + "', houseNumber : " + rHouse + ", apartmentNumber : " + rApartment + "}")).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
        session.execute(update.build());
    }

    private void selectById() {
        System.out.println("Package's id:");
        int id = scanner.nextInt();
        scanner.nextLine();

        Select query = QueryBuilder.selectFrom("package").all().whereColumn("id").isEqualTo(QueryBuilder.literal(id));
        ResultSet resultSet = session.execute(query.build());

        for (Row row : resultSet) {
            System.out.print("package: ");
            System.out.print(row.getInt("id") + ", ");
            System.out.print(row.getDouble("weight") + ", ");
            System.out.print(row.getDouble("size_x") + ", ");
            System.out.print(row.getDouble("size_y") + ", ");
            System.out.print(row.getDouble("size_z") + ", ");
            System.out.print(row.getBoolean("is_fragile") + ", ");
            UdtValue address_sender = row.getUdtValue("address_sender");
            System.out.print("{" + address_sender.getString("street") + ", " + address_sender.getInt("houseNumber") + ", "
                    + address_sender.getInt("apartmentNumber") + "}" + ", ");
            UdtValue address_receiver = row.getUdtValue("address_receiver");
            System.out.print("{" + address_receiver.getString("street") + ", " + address_receiver.getInt("houseNumber") + ", "
                    + address_receiver.getInt("apartmentNumber") + "}" + ", ");
            System.out.println();
        }
    }

    private void selectHeavier() {
        System.out.println("Package's weight lower bound:");
        double weight = scanner.nextDouble();
        scanner.nextLine();

        Select query = QueryBuilder.selectFrom("package").all().whereColumn("weight").isGreaterThan(QueryBuilder.literal(weight)).allowFiltering();
        ResultSet resultSet = session.execute(query.build());

        for (Row row : resultSet) {
            System.out.print("package: ");
            System.out.print(row.getInt("id") + ", ");
            System.out.print(row.getDouble("weight") + ", ");
            System.out.print(row.getDouble("size_x") + ", ");
            System.out.print(row.getDouble("size_y") + ", ");
            System.out.print(row.getDouble("size_z") + ", ");
            System.out.print(row.getBoolean("is_fragile") + ", ");
            UdtValue address_sender = row.getUdtValue("address_sender");
            System.out.print("{" + address_sender.getString("street") + ", " + address_sender.getInt("houseNumber") + ", "
                    + address_sender.getInt("apartmentNumber") + "}" + ", ");
            UdtValue address_receiver = row.getUdtValue("address_receiver");
            System.out.print("{" + address_receiver.getString("street") + ", " + address_receiver.getInt("houseNumber") + ", "
                    + address_receiver.getInt("apartmentNumber") + "}" + ", ");
            System.out.println();
        }
    }

    private void selectAll() {
        Select query = QueryBuilder.selectFrom("package").all();
        SimpleStatement statement = query.build();
        ResultSet resultSet = session.execute(statement);
        for (Row row : resultSet) {
            System.out.print("package: ");
            System.out.print(row.getInt("id") + ", ");
            System.out.print(row.getDouble("weight") + ", ");
            System.out.print(row.getDouble("size_x") + ", ");
            System.out.print(row.getDouble("size_y") + ", ");
            System.out.print(row.getDouble("size_z") + ", ");
            System.out.print(row.getBoolean("is_fragile") + ", ");
            UdtValue address_sender = row.getUdtValue("address_sender");
            System.out.print("{" + address_sender.getString("street") + ", " + address_sender.getInt("houseNumber") + ", "
                    + address_sender.getInt("apartmentNumber") + "}" + ", ");
            UdtValue address_receiver = row.getUdtValue("address_receiver");
            System.out.print("{" + address_receiver.getString("street") + ", " + address_receiver.getInt("houseNumber") + ", "
                    + address_receiver.getInt("apartmentNumber") + "}" + ", ");
            System.out.println();
        }
        System.out.println("Statement \"" + statement.getQuery() + "\" executed successfully");
    }

    private void deleteById() {
        System.out.println("Package's id:");
        int id = scanner.nextInt();
        scanner.nextLine();

        Delete delete = QueryBuilder.deleteFrom("package").whereColumn("id").isEqualTo(QueryBuilder.literal(id));
        session.execute(delete.build());
    }

    private void calculateAvgWeight() {

        ResultSet resultSet = session.execute("SELECT AVG(weight) AS Average FROM package");
        for (Row row : resultSet) {
            System.out.println(row.getDouble("Average"));
        }

    }

}
