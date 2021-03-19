/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Ship");
				System.out.println("2. Add Captain");
				System.out.println("3. Add Cruise");
				System.out.println("4. Book Cruise");
				System.out.println("5. List number of available seats for a given Cruise.");
				System.out.println("6. List total number of repairs per Ship in descending order");
				System.out.println("7. Find total number of passengers with a given status");
				System.out.println("8. < EXIT");
				
				switch (readChoice()){
					case 1: AddShip(esql); break;
					case 2: AddCaptain(esql); break;
					case 3: AddCruise(esql); break;
					case 4: BookCruise(esql); break;
					case 5: ListNumberOfAvailableSeats(esql); break;
					case 6: ListsTotalNumberOfRepairsPerShip(esql); break;
					case 7: FindPassengersCountWithStatus(esql); break;
					case 8: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void AddShip(DBproject esql) {//1
		try{
			System.out.print("Enter ID: ");
			int idInput = Integer.parseInt(in.readLine());
			System.out.print("Enter make: ");
			String makeInput = in.readLine();
			System.out.print("Enter model: ");
			String modelInput = in.readLine();
			System.out.print("Enter age: ");
			int ageInput = Integer.parseInt(in.readLine());
			System.out.print("Enter seats: ");
			int seatInput = Integer.parseInt(in.readLine());
			String query = "INSERT INTO Ship (id, make, model, age, seats) VALUES (" + idInput + ", \'" + makeInput + "\', \'" + modelInput + "\', " + ageInput + ", " + seatInput + ");";
			System.out.println(query);
			esql.executeUpdate(query);
		} catch (Exception e) {
			System.err.println("Error, Ship was not added");
		}
	}

	public static void AddCaptain(DBproject esql) {//2
		try {
		System.out.print("Enter id: ");
		int idInput = Integer.parseInt(in.readLine());
		System.out.print("Enter full name: ");
		String nameInput = in.readLine();
		System.out.print("Enter nationality: ");
		String natInput = in.readLine();
		String query = "INSERT INTO Captain (id, fullname, nationality) VALUES ('" + idInput + "', \'" + nameInput + "\', \'" + natInput + "\');";
		System.out.println(query);
		esql.executeUpdate(query);
		} catch (Exception e) {
			System.out.println("Error");
		}
	}

	public static void AddCruise(DBproject esql) {//3
		try {
			System.out.print("Enter cnum: ");
			int cnumInput = Integer.parseInt(in.readLine());
			System.out.print("Enter cost: ");
			int costInput = Integer.parseInt(in.readLine());
			System.out.print("Enter num_sold: ");
			int numSoldInput = Integer.parseInt(in.readLine());
			System.out.print("Enter num_stops: ");
			int numStopsInput = Integer.parseInt(in.readLine());
			System.out.print("Enter actual_departure_date (YYYY-MM-DD hh:mm): ");
			String date1 = in.readLine();
			System.out.print("Enter actual_arrival_date (YYYY-MM-DD hh:mm): ");
			String date2 = in.readLine();
			System.out.print("Enter arrival_port (port code): ");
			String port1 = in.readLine();
			System.out.print("Enter departure_port (port code): ");
			String port2 = in.readLine();
			String query = "INSERT INTO Cruise (cnum, cost, num_sold, num_stops, actual_departure_date, actual_arrival_date, arrival_port, departure_port) VALUES (" + cnumInput + ", " + costInput + ", " + numSoldInput + ", " + numStopsInput + ", \'" + date1 + "\', \'" + date2 + "', \'" + port1 + "\', \'" + port2 + "\');";
			System.out.println(query);
			esql.executeUpdate(query); 
		} catch (Exception e) {
			System.err.println("Error, cannot add Cruise");
		}
	}


	public static void BookCruise(DBproject esql) {//4
		// Given a customer and a Cruise that he/she wants to book, add a reservation to the DB
		try {
			//determine status of reservation
			System.out.print("Enter customer id: ");
			int custID = Integer.parseInt(in.readLine());
			System.out.print("Enter cnum: ");
			int cnumInput = Integer.parseInt(in.readLine());
			String query = "SELECT R.status FROM Reservation R, Customer C WHERE C.id = " + custID + " AND C.id = R.ccid AND R.cid = " + cnumInput + ";";
			System.out.println(query);
			int rowCount = esql.executeQuery(query);
			System.out.println("If rows = 0, then reservation does not exist.");
			System.out.println("rows: " + rowCount);
			//add reservation to database with appropriate status
			System.out.print("Enter rnum: "); //reservation number
			int rnumInput = Integer.parseInt(in.readLine());
			System.out.print("Enter status: ");
			String statusInput = in.readLine();
			String query2 =  "INSERT INTO Reservation (rnum, ccid, cid, status) VALUES (" + rnumInput + ", " + custID + ", " + cnumInput + ", '" + statusInput +				 "');";
			System.out.println(query2);
			esql.executeUpdate(query2);
		} catch (Exception e) {
			System.err.println("Error, cannot book cruise");
		}
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//5
		// For Cruise number and date, find the number of availalbe seats (i.e. total Ship capacity minus booked seats )
		try {
			System.out.print("Enter cnum: ");
                        int cnumInput = Integer.parseInt(in.readLine());
                        System.out.print("Enter date (YYYY-MM-DD hh:mm): ");
                        String dateInput = in.readLine();
			String query = "SELECT S.seats FROM Ship S, Cruise C, CruiseInfo C2 WHERE C.cnum = " + cnumInput + "  AND C.actual_departure_date = \'" + dateInput + "\' AND C2.cruise_id = C.cnum AND C2.ship_id = S.id;";
			System.out.println(query);
			esql.executeQueryAndPrintResult(query);
			String query2 = "SELECT C.num_sold FROM Cruise C, CruiseInfo C2 WHERE C.num_sold > 0 AND C2.cruise_id = C.cnum AND C.cnum = " + cnumInput + ";";
			System.out.println(query2);
			esql.executeQueryAndPrintResult(query2);
			
			String queryFinal = " SELECT (SELECT S.seats FROM Ship S, Cruise C, CruiseInfo C2 WHERE C.cnum = " + cnumInput + "  AND C.actual_departure_date = \'" + dateInput + "\' AND C2.cruise_id = C.cnum AND C2.ship_id = S.id) - (SELECT C.num_sold FROM Cruise C, CruiseInfo C2 WHERE C.num_sold > 0 AND C2.cruise_id = C.cnum AND C.cnum = " + cnumInput + ");";
			System.out.println(queryFinal);
			esql.executeQueryAndPrintResult(queryFinal);
			//System.out.println("rowCount1: " + rowCount1);
			//System.out.println(query2);
			//int rowCount2 = esql.executeQuery(query2);
			//System.out.println("rowCount2 : " + rowCount2);
			//System.out.println("Number of seats available: " + (rowCount1 - rowCount2));
		} catch (Exception e) {
			System.err.println("Error");
		}
	}

	public static void ListsTotalNumberOfRepairsPerShip(DBproject esql) {//6
		// Count number of repairs per Ships and list them in descending order
		try {
			String query = "SELECT S.id, count(R.rid) FROM Repairs R, Ship S WHERE R.ship_id = S.id GROUP BY S.id ORDER BY count DESC;";
			esql.executeQueryAndPrintResult(query); 
		} catch (Exception e) {
			System.err.println("Error, could not list total number of repairs per ship");
		}
	}

	
	public static void FindPassengersCountWithStatus(DBproject esql) {//7
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.
		try {
			System.out.print("Enter status: ");
			String input = in.readLine();
			System.out.print("Enter cnum: ");
			int input2 = Integer.parseInt(in.readLine());
			String query2 = "SELECT COUNT(*) FROM Reservation WHERE status = \'" + input + "\' AND cid = " + input2 + ";";
			System.out.println(query2);
			//String query2 = "SELECT C.cust_id, count (*) FROM Customer C, Reservation R WHERE C2.cnum = " + input2 + " AND R.status = " + input + " AND R.ccid = C.id;";
			esql.executeQueryAndPrintResult(query2);
			//System.out.println("Rows: " + rowCount);
		} catch (Exception e) {
			System.err.println("Error, could not find passengers with the given status");
		}
	}
}
