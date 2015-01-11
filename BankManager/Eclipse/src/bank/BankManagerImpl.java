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
    private static final String CREATE_TABLE_TRANSFER = "create table TRANSFER (" +
    		"id int NOT NULL AUTO_INCREMENT," +
    		"account_from INTEGER NOT NULL, " +
    		"account_to INTEGER NOT NULL," +
    		"amount DOUBLE NOT NULL," +
    		"date DATETIME NOT NULL," +
    		"primary key (id)," +
    		"foreign key (account_from) references ACCOUNT(id)," +
    		"foreign key (account_to) references ACCOUNT(id)" +
    		")";
    
   private static final String CREATE_TABLE_ACCOUNT = "CREATE TABLE ACCOUNT  (" +
   		"id INTEGER not NULL," +
   		" owner VARCHAR(255)," +
   		" balance DOUBLE," +
   		"PRIMARY KEY(id))";
   
   private static final String CREATE_TABLE_OPERATION = "create table OPERATION (" +
		   "id int NOT NULL AUTO_INCREMENT, " +
		   "amount DOUBLE NOT NULL," +
		   "date DATETIME NOT NULL," +
		   "account int NOT NULL," +
		   "primary key (id)," +
		   "foreign key (account) references ACCOUNT(id)"+
		   ")";
   private static final String CREATE_TRIGGER_BALANCE = "create trigger POSITIVE_BALANCE " +
		   "BEFORE UPDATE ON ACCOUNT " +
		   "FOR EACH ROW BEGIN "
		   + "IF (NEW.balance >= 0) THEN INSERT INTO OPERATION(amount,date,account) VALUES (NEW.balance - OLD.balance, CURRENT_TIMESTAMP, NEW.id); "
		   + "ELSE " +
		   "SET NEW.balance = OLD.balance; END IF; END";

   
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
    	statement.executeUpdate("DROP TABLE IF EXISTS TRANSFER");
    	
    	statement.executeUpdate("DROP TABLE IF EXISTS OPERATION");
    	
    	
    	statement.executeUpdate("drop table if exists ACCOUNT");
    	
    	statement.executeUpdate(CREATE_TABLE_ACCOUNT);
    	statement.executeUpdate(CREATE_TABLE_TRANSFER);
    	statement.executeUpdate(CREATE_TABLE_OPERATION);
    	statement.executeUpdate(CREATE_TRIGGER_BALANCE);
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
    	 rs.next();
    	return rs.getDouble(1);
    }

    @Override
    public double addBalance(int number, double amount) throws SQLException {
	// TODO Auto-generated method stub
    	PreparedStatement pst = null;
    	
    	pst = connection.prepareStatement("UPDATE ACCOUNT SET balance=balance+? WHERE id=?");
    	pst.setDouble(1, amount);
    	pst.setInt(2, number);
    	pst.executeUpdate();
    	
    	
	return getBalance(number);
    }

    @Override
    public boolean transfer(int from, int to, double amount) throws SQLException {
	// TODO Auto-generated method stub
    	connection.setAutoCommit(false);
    	Statement stmt = connection.createStatement();
    	double balance_from = getBalance(from);
    	double balance_to = getBalance(to);
    	stmt.executeUpdate("UPDATE ACCOUNT SET balance=balance+"+(-amount)+" WHERE id="+from);
    	stmt.executeUpdate("UPDATE ACCOUNT SET balance=balance+"+amount+" WHERE id="+to);
    	if(balance_from < amount){
    	
    	connection.rollback();
    	}
    	else {
    	connection.commit();
    	}
    	return getBalance(from) == balance_from-amount && getBalance(to) == balance_to+amount;
    }

    @Override
    public List<Operation> getOperations(int number, Date from, Date to) throws SQLException {
	// TODO Auto-generated method stub
    	
	return null;
    }

}
