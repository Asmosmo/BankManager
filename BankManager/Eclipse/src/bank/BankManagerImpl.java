package bank;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.Date;
import java.util.List;
import java.util.Random;

import java.sql.Connection;

/**
 * A simple implementation of the ReservationManager interface. Each object of
 * this class must create a dedicated connection to the database.
 * <p>
 * <b>Note: DO NOT alter this class's interface.</b>
 * 
 * @author Busca
 * 
 */
public class BankManagerImpl implements BankManager {

    // CLASS FIELDS
    //
	private final Connection connection;
    // example of a create table statement executed by createDB()
    private static final String CREATE_TABLE_DUMMY = "create table DUMMY (" + 
	    "ATT int, " + 
	    "primary key (ATT)" + 
	    ")";

    /**
     * Creates a new ReservationManager object. This creates a new connection to
     * the specified database.
     * 
     * @param url
     *            the url of the database to connect to
     * @param user
     *            the login name of the user
     * @param password
     *            his password
     */
    public BankManagerImpl(String url, String user, String password) throws SQLException {
    	
    	connection = DriverManager.getConnection(url, user, password);
    }

    @Override
    public void createDB() throws SQLException {
	// TODO Auto-generated method stub
    	Statement statement = connection.createStatement();
    	//int Result = statement.executeUpdate("CREATE DATABASE IF NOT EXISTS bank");
    	
    	String sql = "CREATE TABLE IF NOT EXISTS ACCOUNT  (id INTEGER not NULL, owner VARCHAR(255), balance DOUBLE,PRIMARY KEY(id))";
    	statement.executeUpdate(sql);
    }

    @Override
    public boolean createAccount(int number) throws SQLException {
	// TODO Auto-generated method stub
    	PreparedStatement pst = null;
    	//Random rand = new Random();
    	double balance=0;
    	//balance=rand.nextInt(5000);
         pst = connection.prepareStatement("INSERT INTO ACCOUNT(id,balance) VALUES(?,?)");
         pst.setInt(1,number);
         	pst.setDouble(2,balance);
         pst.executeUpdate();
	return false;
    }

    @Override
    public double getBalance(int number) throws SQLException {
	// TODO Auto-generated method stub
    	PreparedStatement pst = null;
    	ResultSet rs = null;
    	pst = connection.prepareStatement("SELECT balance FROM ACCOUNT where id = ?");
    	pst.setInt(1,number);
    	rs = pst.executeQuery();
    	
    	return rs.getDouble(1);
    }

    @Override
    public double addBalance(int number, double amount) throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public boolean transfer(int from, int to, double amount) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public List<Operation> getOperations(int number, Date from, Date to) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

}
