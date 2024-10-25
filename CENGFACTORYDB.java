package ceng.ceng351.cengfactorydb;

import java.sql.*;
import java.util.*;


public class CENGFACTORYDB implements ICENGFACTORYDB{

    private static Connection connection = null;



    /**
     * Place your initialization code inside if required.
     *
     * <p>
     * This function will be called before all other operations. If your implementation
     * need initialization , necessary operations should be done inside this function. For
     * example, you can set your connection to the database server inside this function.
     */
    public void initialize() {

        String user = "e2547875";
        String password = "IM5JR5UxW47L";
        String url = "jdbc:mysql://144.122.71.128:8080/db2547875";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection =  DriverManager.getConnection(url, user, password);
        }
        catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }


    /**
     * Should create the necessary tables when called.
     *
     * @return the number of tables that are created successfully.
     */
    public int createTables() {

        int total = 0;
        String sql;

        try { // FACTORY
            sql = "CREATE TABLE Factory(" +
                    "factoryId INT," +
                    "factoryName TEXT," +
                    "factoryType TEXT," +
                    "country TEXT," +
                    "PRIMARY KEY (factoryId)" +
                    ");";

            connection.prepareStatement(sql).execute();
            total += 1;

        }   catch (SQLException e) {
            e.printStackTrace();
        }




        try { // EMPLOYEE
            sql = "CREATE TABLE Employee(" +
                    "employeeId INT," +
                    "employeeName TEXT," +
                    "department TEXT," +
                    "salary INT," +
                    "PRIMARY KEY (employeeId)" +
                    ");";
            connection.prepareStatement(sql).execute();
            total += 1;

        }   catch (SQLException e) {
            e.printStackTrace();
        }



        try { // WORKSIN
            sql = "CREATE TABLE Works_In(" +
                    "factoryId INT," +
                    "employeeId INT," +
                    "startDate DATE," +
                    "PRIMARY KEY (factoryId, employeeId)," +
                    "FOREIGN KEY (factoryId) REFERENCES Factory ON DELETE CASCADE," +
                    "FOREIGN KEY (employeeId) REFERENCES Employee ON DELETE CASCADE" +
                    ");";
            connection.prepareStatement(sql).execute();
            total += 1;

        }   catch (SQLException e) {
            e.printStackTrace();
        }



        try { // PRODUCT
            sql = "CREATE TABLE Product(" +
                    "productId INT," +
                    "productName TEXT," +
                    "productType TEXT," +
                    "PRIMARY KEY (productId)" +
                    ");";
            connection.prepareStatement(sql).execute();
            total += 1;

        }   catch (SQLException e) {
            e.printStackTrace();
        }




        try { // PRODUCE
            sql = "CREATE TABLE Produce(" +
                    "factoryId INT," +
                    "productId INT," +
                    "amount INT," +
                    "productionCost INT," +
                    "PRIMARY KEY (factoryId, productId)," +
                    "FOREIGN KEY (factoryId) REFERENCES Factory ON DELETE CASCADE," +
                    "FOREIGN KEY (productId) REFERENCES Product ON DELETE CASCADE" +
                    ");";
            connection.prepareStatement(sql).execute();
            total += 1;

        }   catch (SQLException e) {
            e.printStackTrace();
        }





        try { // SHIPMENT
            sql = "CREATE TABLE Shipment(" +
                    "factoryId INT," +
                    "productId INT," +
                    "amount INT," +
                    "pricePerUnit INT," +
                    "PRIMARY KEY (factoryId, productId)," +
                    "FOREIGN KEY (factoryId) REFERENCES Factory ON DELETE CASCADE," +
                    "FOREIGN KEY (productId) REFERENCES Product ON DELETE CASCADE" +
                    ");";
            connection.prepareStatement(sql).execute();
            total += 1;

        }   catch (SQLException e) {
            e.printStackTrace();
        }



        return total;

    }


    /**
     * Should drop the tables if exists when called.
     *
     * @return the number of tables are dropped successfully.
     */
    public int dropTables() {

        int total = 0;

        String sql;


        try {
            sql = "DROP TABLE Works_In;";
            connection.prepareStatement(sql).execute();
            total += 1;

        } catch (SQLException e) {
            e.printStackTrace();
        }


        try {
            sql = "DROP TABLE Produce;";
            connection.prepareStatement(sql).execute();
            total += 1;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            sql = "DROP TABLE Shipment;";
            connection.prepareStatement(sql).execute();
            total += 1;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            sql = "DROP TABLE Factory;";
            connection.prepareStatement(sql).execute();
            total += 1;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            sql = "DROP TABLE Employee;";
            connection.prepareStatement(sql).execute();
            total += 1;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            sql = "DROP TABLE Product;";
            connection.prepareStatement(sql).execute();
            total += 1;

        } catch (SQLException e) {
            e.printStackTrace();
        }


        return total;


    }

    /**
     * Should insert an array of Factory into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertFactory(Factory[] factories) {

        int total = 0;
        String sql;

        for (Factory fac : factories) {

            try{
                sql = "INSERT INTO Factory VALUES (" +
                        fac.getFactoryId() + ", '" +
                        fac.getFactoryName() + "', '" +
                        fac.getFactoryType() + "', '" +
                        fac.getCountry() + "');";

                connection.prepareStatement(sql).execute();
                total++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return total;

    }

    /**
     * Should insert an array of Employee into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertEmployee(Employee[] employees) {

        int total = 0;
        String sql;

        for (Employee emp : employees) {

            try{

                sql = "INSERT INTO Employee VALUES (" +
                        emp.getEmployeeId() + ", '" +
                        emp.getEmployeeName() + "', '" +
                        emp.getDepartment() + "', " +
                        emp.getSalary() + ");";

                connection.prepareStatement(sql).execute();
                total++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return total;

    }

    /**
     * Should insert an array of WorksIn into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertWorksIn(WorksIn[] worksIns) {

        int total = 0;
        String sql;

        for (WorksIn work : worksIns) {

            try{

                sql = "INSERT INTO Works_In VALUES (" +
                       work.getFactoryId() + ", " +
                       work.getEmployeeId() + ", '" +
                       work.getStartDate() + "');";

                connection.prepareStatement(sql).execute();
                total++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return total;

    }

    /**
     * Should insert an array of Product into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertProduct(Product[] products) {

        int total = 0;
        String sql;

        for (Product pro : products) {

            try{

                sql = "INSERT INTO Product VALUES (" +
                       pro.getProductId() + ", '" +
                       pro.getProductName() + "', '" +
                       pro.getProductType() + "');";

                connection.prepareStatement(sql).execute();
                total++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return total;

    }


    /**
     * Should insert an array of Produce into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertProduce(Produce[] produces) {

        int total = 0;
        String sql;

        for (Produce pro : produces) {

            try{

                sql = "INSERT INTO Produce VALUES (" +
                       pro.getFactoryId() + ", " +
                       pro.getProductId() + ", " +
                       pro.getAmount() + ", " +
                       pro.getProductionCost() + ");";

                connection.prepareStatement(sql).execute();
                total++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return total;

    }


    /**
     * Should insert an array of Shipment into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertShipment(Shipment[] shipments) {

        int total = 0;
        String sql;

        for (Shipment ship : shipments) {

            try{

                sql = "INSERT INTO Shipment VALUES (" +
                       ship.getFactoryId() + ", " +
                       ship.getProductId() + ", " +
                       ship.getAmount() + ", " +
                       ship.getPricePerUnit() + ");";

                connection.prepareStatement(sql).execute();
                total++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return total;

    }

    /**
     * Should return all factories that are located in a particular country.
     *
     * @return Factory[]
     */
    public Factory[] getFactoriesForGivenCountry(String country) {

        String query = "SELECT DISTINCT * " +
                "FROM Factory f " +
                "WHERE f.country = '" + country + "' " +
                "ORDER BY f.factoryId ASC;";

        try {

            PreparedStatement prSt = connection.prepareStatement(query);
            ResultSet result = prSt.executeQuery();

            ArrayList<Factory> answer = new ArrayList<Factory>();

            while (result.next())
                answer.add(new Factory(
                        result.getInt("factoryId"),
                        result.getString("factoryName"),
                        result.getString("factoryType"),
                        result.getString("country")
                ));

            return answer.toArray(new Factory[0]); //if no error

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new Factory[0]; // if error came

    }



    /**
     * Should return all factories without any working employees.
     *
     * @return Factory[]
     */
    public Factory[] getFactoriesWithoutAnyEmployee() {

        String query = "SELECT DISTINCT * " +
                "FROM Factory f " +
                "WHERE f.factoryId NOT IN (SELECT DISTINCT f1.factoryId " +
                                            "FROM Factory f1, Works_In w1 " +
                                            "WHERE f1.factoryId = w1.factoryId) " +
                "ORDER BY f.factoryId ASC;";


        try {

            PreparedStatement prSt = connection.prepareStatement(query);
            ResultSet result = prSt.executeQuery();

            ArrayList<Factory> answer = new ArrayList<Factory>();

            while (result.next())
                answer.add(new Factory(
                        result.getInt("factoryId"),
                        result.getString("factoryName"),
                        result.getString("factoryType"),
                        result.getString("country")
                ));

            return answer.toArray(new Factory[0]); //if no error

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new Factory[0]; // if error came

    }

    /**
     * Should return all factories that produce all products for a particular productType
     *
     * @return Factory[]
     */
    public Factory[] getFactoriesProducingAllForGivenType(String productType) {

        String query = "SELECT DISTINCT * " +
                "FROM Factory f " +
                "WHERE NOT EXISTS (SELECT productId " +
                "FROM Product p " +
                "WHERE p.productType = '" + productType + "' " +
                "EXCEPT " +
                "SELECT p.productId " +
                "FROM Produce p " +
                "WHERE f.factoryId = p.factoryId) " +
                "ORDER BY f.factoryId ASC;";


        try {

            PreparedStatement prSt = connection.prepareStatement(query);
            ResultSet result = prSt.executeQuery();

            ArrayList<Factory> answer = new ArrayList<Factory>();

            while (result.next())
                answer.add(new Factory(
                        result.getInt("factoryId"),
                        result.getString("factoryName"),
                        result.getString("factoryType"),
                        result.getString("country")
                ));

            return answer.toArray(new Factory[0]); //if no error

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new Factory[0]; // if error came

    }


    /**
     * Should return the products that are produced in a particular factory but
     * don’t have any shipment from that factory.
     *
     * @return Product[]
     */
    public Product[] getProductsProducedNotShipped() {

        String query = "SELECT DISTINCT * " +
                "FROM Product p " +
                "WHERE EXISTS " +
                    "(SELECT DISTINCT p1.factoryId " +
                    "FROM Produce p1 " +
                    "WHERE p.productId = p1.productId " +
                    "EXCEPT " +
                    "SELECT DISTINCT s1.factoryId " +
                    "FROM Shipment s1 " +
                    "WHERE p.productId = s1.productId) " +
                "ORDER BY p.productId ASC;";

        try {

            PreparedStatement prSt = connection.prepareStatement(query);
            ResultSet result = prSt.executeQuery();

            ArrayList<Product> answer = new ArrayList<Product>();

            while (result.next())
                answer.add(new Product(
                        result.getInt("productId"),
                        result.getString("productName"),
                        result.getString("productType")
                    ));

            return answer.toArray(new Product[0]); //if no error

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new Product[0]; // if error came
    }


    /**
     * For a given factoryId and department, should return the average salary of
     *     the employees working in that factory and that specific department.
     *
     * @return double
     */
    public double getAverageSalaryForFactoryDepartment(int factoryId, String department) {

        String query = "SELECT AVG(E.salary) AS avg " +
                "FROM Employee E, Works_In W " +
                "WHERE E.employeeId = W.employeeId AND W.factoryId = " + factoryId + " AND " +
                "E.department = '" + department + "';";

        try {

            PreparedStatement prSt = connection.prepareStatement(query);
            ResultSet result = prSt.executeQuery();

            double answer = 0.0;

            if (result.next())
                answer = result.getDouble("avg");

            return answer; //if no error

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return 0.0; // if error came
    }


    /**
     * Should return the most profitable products for each factory
     *
     * @return QueryResult.MostValueableProduct[]
     */
    public QueryResult.MostValueableProduct[] getMostValueableProducts() {

        String query = "SELECT DISTINCT pr.factoryId, pr.productId, p.productName, p.productType, " +
                "((COALESCE(s.amount,0)*COALESCE(s.pricePerUnit,0)) - (pr.amount*pr.productionCost)) AS profit " +
                "FROM Product p, (Produce pr LEFT JOIN Shipment s ON  " +
                "pr.factoryId = s.factoryId AND pr.productId = s.productId) " +
                "WHERE p.productId = pr.productId AND " +
                "((COALESCE(s.amount,0)*COALESCE(s.pricePerUnit,0)) - (pr.amount*pr.productionCost)) >= (" +
                    "SELECT MAX((COALESCE(s1.amount,0)*COALESCE(s1.pricePerUnit,0)) - (pr1.amount*pr1.productionCost)) " +
                    "FROM Produce pr1 LEFT JOIN Shipment s1 ON " +
                    "pr1.factoryId = s1.factoryId AND pr1.productId = s1.productId " +
                    "WHERE pr1.factoryId = pr.factoryId) " +
                "ORDER BY profit DESC, pr.factoryId ASC;";


        try {

            PreparedStatement prSt = connection.prepareStatement(query);
            ResultSet result = prSt.executeQuery();

            ArrayList<QueryResult.MostValueableProduct> answer = new ArrayList<QueryResult.MostValueableProduct>();

            while (result.next())
                answer.add(new QueryResult.MostValueableProduct(
                        result.getInt("factoryId"),
                        result.getInt("productId"),
                        result.getString("productName"),
                        result.getString("productType"),
                        result.getDouble("profit")
                    ));

            return answer.toArray(new QueryResult.MostValueableProduct[0]); //if no error

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new QueryResult.MostValueableProduct[0]; // if error came
    }

    /**
     * For each product, return the factories that gather the highest profit
     * for that product
     *
     * @return QueryResult.MostValueableProduct[]
     */
    public QueryResult.MostValueableProduct[] getMostValueableProductsOnFactory() {

        String query = "SELECT DISTINCT pr.factoryId, pr.productId, p.productName, p.productType, " +
                "((COALESCE(s.amount,0)*COALESCE(s.pricePerUnit,0)) - (pr.amount*pr.productionCost)) AS profit " +
                "FROM Product p, (Produce pr LEFT JOIN Shipment s ON  " +
                "pr.factoryId = s.factoryId AND pr.productId = s.productId) " +
                "WHERE p.productId = pr.productId AND " +
                "((COALESCE(s.amount,0)*COALESCE(s.pricePerUnit,0)) - (pr.amount*pr.productionCost)) >= (" +
                "SELECT MAX((COALESCE(s1.amount,0)*COALESCE(s1.pricePerUnit,0)) - (pr1.amount*pr1.productionCost)) " +
                "FROM Produce pr1 LEFT JOIN Shipment s1 ON " +
                "pr1.factoryId = s1.factoryId AND pr1.productId = s1.productId " +
                "WHERE pr1.productId = pr.productId) " +
                "ORDER BY profit DESC, pr.factoryId ASC;";

        try {

            PreparedStatement prSt = connection.prepareStatement(query);
            ResultSet result = prSt.executeQuery();

            ArrayList<QueryResult.MostValueableProduct> answer = new ArrayList<QueryResult.MostValueableProduct>();

            while (result.next())
                answer.add(new QueryResult.MostValueableProduct(
                        result.getInt("factoryId"),
                        result.getInt("productId"),
                        result.getString("productName"),
                        result.getString("productType"),
                        result.getDouble("profit")
                ));

            return answer.toArray(new QueryResult.MostValueableProduct[0]); //if no error

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new QueryResult.MostValueableProduct[0]; // if error came
    }


    /**
     * For each department, should return all employees that are paid under the
     *     average salary for that department. You consider the employees
     *     that do not work earning ”0”.
     *
     * @return QueryResult.LowSalaryEmployees[]
     */
    public QueryResult.LowSalaryEmployees[] getLowSalaryEmployeesForDepartments() {

        String query = "(SELECT DISTINCT * " +
                "FROM Employee e " +
                "WHERE e.employeeId IN ( " +
                    "SELECT w1.employeeId " +
                    "FROM Works_In w1) " +
                "AND e.salary < ( " +
                        "(SELECT SUM(ALL salary) " +
                        "FROM Employee e2 " +
                        "WHERE e2.department = e.department AND e2.employeeId IN ( " +
                            "SELECT w2.employeeId " +
                            "FROM Works_In w2)) / " +
                        "(SELECT COUNT(*) " +
                        "FROM Employee e2 " +
                        "WHERE e2.department = e.department))) " +

                "UNION " +

                "(SELECT DISTINCT e.employeeId, e.employeeName, e.department, 0 AS salary " +
                "FROM Employee e " +
                "WHERE e.employeeId NOT IN ( " +
                    "SELECT w1.employeeId " +
                    "FROM Works_In w1)" +
                "AND 0 < ( " +
                    "(SELECT SUM(ALL salary) " +
                    "FROM Employee e2 " +
                    "WHERE e2.department = e.department AND e2.employeeId IN ( " +
                        "SELECT w2.employeeId " +
                        "FROM Works_In w2)) / " +
                    "(SELECT COUNT(*) " +
                    "FROM Employee e2 " +
                    "WHERE e2.department = e.department))) " +
                "ORDER BY employeeId ASC;";

        try {

            PreparedStatement prSt = connection.prepareStatement(query);
            ResultSet result = prSt.executeQuery();

            ArrayList<QueryResult.LowSalaryEmployees> answer = new ArrayList<QueryResult.LowSalaryEmployees>();

            while (result.next())
                answer.add(new QueryResult.LowSalaryEmployees(
                        result.getInt("employeeId"),
                        result.getString("employeeName"),
                        result.getString("department"),
                        result.getInt("salary")
                    ));

            return answer.toArray(new QueryResult.LowSalaryEmployees[0]); //if no error

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new QueryResult.LowSalaryEmployees[0]; // if error came
    }


    /**
     * For the products of given productType, increase the productionCost of every unit by a given percentage.
     *
     * @return number of rows affected
     */
    public int increaseCost(String productType, double percentage) {

        String query = "UPDATE Produce pr " +
                "SET productionCost = productionCost + (productionCost * " + percentage + ") " +
                "WHERE EXISTS (" +
                    "SELECT * " +
                    "FROM Product p1 " +
                    "WHERE pr.productId = p1.productId AND " +
                    "p1.productType = '" + productType + "');" ;

        try {
            return connection.prepareStatement(query).executeUpdate(); // if no error

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return 0; // if error came
    }


    /**
     * Deleting all employees that have not worked since the given date.
     *
     * @return number of rows affected
     */
    public int deleteNotWorkingEmployees(String givenDate) {

        String query = "DELETE FROM Employee " +
                "WHERE employeeId NOT IN ( " +
                    "SELECT DISTINCT w1.employeeId " +
                    "FROM Works_In w1 " +
                    "WHERE w1.startDate >= '" + givenDate + "');";

        try {
            return connection.prepareStatement(query).executeUpdate(); // if no error

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return 0; // if error came
    }


    /*
      *****************************************************
      *****************************************************
      *****************************************************
      *****************************************************
       THE METHODS AFTER THIS LINE WILL NOT BE GRADED.
       YOU DON'T HAVE TO SOLVE THEM, LEAVE THEM AS IS IF YOU WANT.
       IF YOU HAVE ANY QUESTIONS, REACH ME VIA EMAIL.
      *****************************************************
      *****************************************************
      *****************************************************
      *****************************************************
     */

    /**
     * For each department, find the rank of the employees in terms of
     * salary. Peers are considered tied and receive the same rank. After
     * that, there should be a gap.
     *
     * @return QueryResult.EmployeeRank[]
     */
    public QueryResult.EmployeeRank[] calculateRank() {
        return new QueryResult.EmployeeRank[0];
    }

    /**
     * For each department, find the rank of the employees in terms of
     * salary. Everything is the same but after ties, there should be no
     * gap.
     *
     * @return QueryResult.EmployeeRank[]
     */
    public QueryResult.EmployeeRank[] calculateRank2() {
        return new QueryResult.EmployeeRank[0];
    }

    /**
     * For each factory, calculate the most profitable 4th product.
     *
     * @return QueryResult.FactoryProfit
     */
    public QueryResult.FactoryProfit calculateFourth() {
        return new QueryResult.FactoryProfit(0,0,0);
    }

    /**
     * Determine the salary variance between an employee and another
     * one who began working immediately after the first employee (by
     * startDate), for all employees.
     * return QueryResult.SalaryVariant[]
     */
    public QueryResult.SalaryVariant[] calculateVariance() {
        return new QueryResult.SalaryVariant[0];
    }

    /**
     * Create a method that is called once and whenever a Product starts
     * losing money, deletes it from Produce table
     * return void
     */
    public void deleteLosing() {

    }
}
