import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/*
 Project: Implementing ACID properties using JDBC and Postgre SQL
 Author: MD Rafat Hossain
*/

public class Main {
    // Storing Database connection parameters
    private static  String URL = "jdbc:postgresql://localhost:5432/DBMS_Project";
    private static  String USERNAME = "postgres";
    private static  String PASSWORD = "root";

    // Method to establish a connection to the database
    public static Connection getConnection() {
        try {
            // Load the PostgreSQL JDBC driver
            Class.forName("org.postgresql.Driver");

            // Establish the connection
            Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            return connection;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    //Main method
    public static void main(String[] args) {
    	try {
    		Connection connection = getConnection();
    		
    		//deleteProductFromProdStock(connection, "p1"); 		
    		
    		//deleteDepoFromDepotStock(connection, "d100");
          	
          	//updateProductID(connection, "p1", "pp1");
          	
          	//updateDepotID(connection, "d1", "dd1");
          	
          	//addProduct(connection, "p100", "cd", 5, "d2", 50);
          	
          	addDepot(connection, "d100", "Chicago", 100, "p1", 100);
          	
            connection.close();
          } catch (SQLException e) {
              e.printStackTrace();
          }
    }
    
    private static void deleteProductFromProdStock(Connection con, String prodid) throws SQLException {
    	try {
            con.setAutoCommit(false);
            PreparedStatement deleteFromStock = con.prepareStatement("Delete from Stock where prodid=?");
            PreparedStatement deleteFromProduct = con.prepareStatement("Delete from Product where prodid=?");
            deleteFromStock.setString(1, prodid);
            deleteFromProduct.setString(1, prodid);
            deleteFromStock.executeUpdate();
            deleteFromProduct.executeUpdate();
            con.commit();
            System.out.println(prodid+" has been deleted from Product and Stock");
            deleteFromStock.close();
            deleteFromProduct.close();
        } catch (SQLException e) {
            con.rollback();
            System.out.println(e.getMessage());
        } finally {
            con.setAutoCommit(true);
        }
    	
    }
    private static void deleteDepoFromDepotStock(Connection con, String depid) throws SQLException {
    	try {
            con.setAutoCommit(false);
            PreparedStatement deleteFromStock = con.prepareStatement("Delete from Stock where depid=?");
            PreparedStatement deleteFromDepot = con.prepareStatement("Delete from Depot where depid=?");
            deleteFromStock.setString(1, depid);
            deleteFromDepot.setString(1, depid);
            deleteFromStock.executeUpdate();
            deleteFromDepot.executeUpdate();
            con.commit();
            System.out.println(depid+" has been deleted from Depot and Stock");
            deleteFromStock.close();
            deleteFromDepot.close();
        } catch (SQLException e) {
            con.rollback();
            System.out.println(e.getMessage());
        } finally {
            con.setAutoCommit(true);
           
        }
    	
    }
    
    private static void updateProductID(Connection con, String prodid, String newProdid) throws SQLException{
    	try {
            con.setAutoCommit(false);
            PreparedStatement updateProdIDInProduct = con.prepareStatement("Update Product set prodid=? where prodid=?");
            PreparedStatement updateProdIDInStock = con.prepareStatement("Update Stock set prodid=? where prodid=?");
            updateProdIDInProduct.setString(1, prodid);
            updateProdIDInProduct.setString(2, newProdid);
            updateProdIDInStock.setString(1, prodid);
            updateProdIDInStock.setString(2, newProdid);
            updateProdIDInProduct.executeUpdate();
            updateProdIDInStock.executeUpdate();
            con.commit();
            System.out.println(prodid+" has been Updated to " + newProdid + " in Product and Stock");
            updateProdIDInProduct.close();
            updateProdIDInStock.close();
        } catch (SQLException e) {
            con.rollback();
            System.out.println(e.getMessage());
        } finally {
            con.setAutoCommit(true);
           
        }
    }
    private static void updateDepotID(Connection con, String depid, String newDepid) throws SQLException{
    	try {
            con.setAutoCommit(false);
            PreparedStatement updateDepIDInDepot = con.prepareStatement("Update Depot set depid=? where depid=?");
            PreparedStatement updateDepIDInStock = con.prepareStatement("Update Stock set depid=? where depid=?");
            updateDepIDInDepot.setString(1, depid);
            updateDepIDInDepot.setString(2, newDepid);
            updateDepIDInStock.setString(1, depid);
            updateDepIDInStock.setString(2, newDepid);
            updateDepIDInDepot.executeUpdate();
            updateDepIDInStock.executeUpdate();
            con.commit();
            System.out.println(depid+" has been Updated to " + newDepid + " in Product and Stock");
            updateDepIDInDepot.close();
            updateDepIDInStock.close();
        } catch (SQLException e) {
            con.rollback();
            System.out.println(e.getMessage());
        } finally {
            con.setAutoCommit(true);
           
        }
    }
    private static void addProduct(Connection con, String prodid, String pname, double price, String depid, int quantity) throws SQLException{
    	try {
            con.setAutoCommit(false);
            PreparedStatement addProdInProduct = con.prepareStatement("insert into Product values (?, ?, ?)");
            PreparedStatement addProdInStock = con.prepareStatement("insert into Stock values (?, ?, ?)");
            addProdInProduct.setString(1, prodid);
            addProdInProduct.setString(2, pname);
            addProdInProduct.setDouble(3, price);
            addProdInStock.setString(1, prodid);
            addProdInStock.setString(2, depid);
            addProdInStock.setInt(3, quantity);
            addProdInProduct.executeUpdate();
            addProdInStock.executeUpdate();
            con.commit();
            System.out.println(prodid+" has been added to Product and Stock");
            addProdInProduct.close();
            addProdInStock.close();
        } catch (SQLException e) {
            con.rollback();
            System.out.println(e.getMessage());
        } finally {
            con.setAutoCommit(true);
           
        }
    }
    private static void addDepot(Connection con, String depid, String addr, int volume, String prodid, int quantity) throws SQLException{
    	try {
            con.setAutoCommit(false);
            PreparedStatement addDepoInDepot = con.prepareStatement("insert into Depot values (?, ?, ?)");
            PreparedStatement addDepoInStock = con.prepareStatement("insert into Stock values (?, ?, ?)");
            addDepoInDepot.setString(1, depid);
            addDepoInDepot.setString(2, addr);
            addDepoInDepot.setInt(3, volume);
            addDepoInStock.setString(1, prodid);
            addDepoInStock.setString(2, depid);
            addDepoInStock.setInt(3, quantity);
            addDepoInDepot.executeUpdate();
            addDepoInStock.executeUpdate();
            con.commit();
            System.out.println(depid+" has been added to Depot and Stock");
            addDepoInDepot.close();
            addDepoInStock.close();
        } catch (SQLException e) {
            con.rollback();
            System.out.println(e.getMessage());
        } finally {
            con.setAutoCommit(true);
           
        }
    }
        
        
    
}
