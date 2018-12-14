package database;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.PreparedStatement;

public class Main {

	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException{
		Connection conn=null;
		Statement stmt = null;
		
		try{
			String driver = "com.mysql.jdbc.Driver";
			String url = "jdbc:mysql://localhost:3306/test";
			String username = "root";
			String password = "";
			Class.forName(driver);

			conn = DriverManager.getConnection(url,username,password);
			System.out.println("Connection Successfull");
		}catch(Exception e){
			System.out.println(e);
		}
		// For atomicity
		conn.setAutoCommit(false);

		// For isolation 
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

		try{
			stmt = conn.createStatement();
			
			stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=0");//to disable foreign key check
			stmt.executeUpdate("DROP TABLE IF EXISTS Movie_Genre CASCADE");
			stmt.executeUpdate("DROP TABLE IF EXISTS Studio CASCADE");
			stmt.executeUpdate("DROP TABLE IF EXISTS Movie CASCADE");
			stmt.executeUpdate("DROP TABLE IF EXISTS Movie_Director CASCADE");
			stmt.executeUpdate("DROP TABLE IF EXISTS Movie_Format CASCADE");
			stmt.executeUpdate("DROP TABLE IF EXISTS Director CASCADE");
			stmt.executeUpdate("DROP TABLE IF EXISTS Format CASCADE");
			/*
			 *  CREATE TABLES
			 * */
			stmt.executeUpdate("CREATE TABLE Movie_Genre("
					+ "genre_id int not null auto_increment,"
					+ "genre_name varchar(255),"
					+ "PRIMARY KEY(genre_id))");
			stmt.executeUpdate("CREATE TABLE Studio("
					+ "studio_id int not null auto_increment,"
					+ "studio_name varchar(255),"
					+ "PRIMARY KEY(studio_id))");
			stmt.executeUpdate("CREATE TABLE Movie("
					+ "movie_id int not null auto_increment,"
					+ "movie_title varchar(255),"
					+ "genre_id int,"
					+ "studio_id int,"
					+ "PRIMARY KEY(movie_id),"
					+ "FOREIGN KEY(genre_id) REFERENCES movie_genre(genre_id) ON UPDATE CASCADE,"
					+ "FOREIGN KEY(studio_id) REFERENCES studio(studio_id)ON UPDATE CASCADE)");
			stmt.executeUpdate("CREATE TABLE Movie_Director("
					+ "movie_id int,"
					+ "director_id int,"
					+ "PRIMARY KEY(movie_id,director_id))");
			stmt.executeUpdate("CREATE TABLE Movie_Format("
					+ "movie_id int,"
					+ "format_id int,"
					+ "price float,"
					+ "PRIMARY KEY(movie_id,format_id))");
			stmt.executeUpdate("CREATE TABLE Director("
					+ "director_id int null auto_increment,"
					+ "director_name varchar(255),"
					+ "PRIMARY KEY(director_id))");
			stmt.executeUpdate("CREATE TABLE Format("
					+ "format_id int not null auto_increment,"
					+ "format_name varchar(255),"
					+ "PRIMARY KEY(format_id))");
			System.out.println("Table created successfully");
			/*
			 *  INSERT DATA INTO TABLES
			 */
			System.out.println("Insering Data into tables");
			stmt.executeUpdate("INSERT INTO Movie_Genre(genre_name)VALUES"
					+ "('Action and Adventure'),"
					+ "('Animation'),"
					+ "('Comedy'),"
					+ "('Documentary'),"
					+ "('Drama');");
			System.out.println("Data inserted successfully into Movie_Genre");
			
			stmt.executeUpdate("INSERT INTO Studio(studio_name)VALUES"
					+ "('Lionsgate'),"
					+ "('Walt Disney Studios Home Entertainment'),"
					+ "('Universal Pictures'),"
					+ "('20th Century Fox'),"
					+ "('Paramount')");
			System.out.println("Data inserted successfully into Studio");
			
			stmt.executeUpdate("INSERT INTO Movie(movie_title,genre_id,studio_id)VALUES"
					+ "('Holiday Collection Movie',1,3),"
					+ "('Guardians Of The Galaxy',1,2),"
					+ "('Beauty And The Beast\t',1,1),"
					+ "('Mission: Impossible\t',1,5),"
					+ "('Spectre\t\t',1,2),"
					+ "('A Goofy Movie\t\t',2,1),"
					+ "('Bad Moms\t\t',4,2),"
					+ "('Austin Powers\t\t',2,3),"
					+ "('Finding Dory\t\t',2,3),"
					+ "('Tarzan\t\t\t',3,4),"
					+ "('The Witch\t\t',3,5),"
					+ "('The Spiderwick Chronicles',4,2)");
			System.out.println("Data inserted successfully into Movie");
			
			stmt.executeUpdate("INSERT INTO Movie_Director(movie_id,director_id)VALUES"
					+ "(1,1),"
					+ "(2,2),"
					+ "(3,1),"
					+ "(4,2),"
					+ "(5,1),"
					+ "(6,4),"
					+ "(7,3),"
					+ "(8,5),"
					+ "(8,1),"
					+ "(10,4),"
					+ "(11,1),"
					+ "(12,2),"
					+ "(6,1),"
					+ "(5,4)");
			System.out.println("Data inserted successfully into Movie_Director");
			
			stmt.executeUpdate("INSERT INTO Movie_Format(movie_id,format_id,price)VALUES"
					+ "(1,4,29.99),"
					+ "(11,1,9.99),"
					+ "(12,3,12.99),"
					+ "(4,4,21.99),"
					+ "(5,2,9.99),"
					+ "(6,1,19.99),"
					+ "(7,5,39.99),"
					+ "(8,4,99.99),"
					+ "(9,5,19.99),"
					+ "(2,3,19.99),"
					+ "(3,2,99.99),"
					+ "(5,1,19.99),"
					+ "(10,3,19.99)");
			
			System.out.println("Data inserted successfully into Movie_Format");
			
			stmt.executeUpdate("INSERT INTO Director(director_name)VALUES"
					+ "('Bernardo Bertolucci'),"
					+ "('Sam Mendes'),"
					+ "('Phil Roman'),"
					+ "('Kevin Lima'),"
					+ "('John Musker')");
			System.out.println("Data inserted successfully into Director");
			
			stmt.executeUpdate("INSERT INTO Format(format_name)VALUES"
					+ "('Amazon Video'),"
					+ "('Blu-ray'),"
					+ "('Blu-ray 3D'),"
					+ "('DVD\t'),"
					+ "('Movie HD')");
			System.out.println("Data inserted successfully into Format");
			
			/*
			 *	SHOW TABLES 
			 */
			ResultSet rs = stmt.executeQuery("Select * from Movie_Genre");
			System.out.println("\tTable Movie_Genre");
			System.out.println("genre_id " + "\tgenre_name ");
			while(rs.next()) {
				System.out.println( " "+rs.getInt("genre_id") 
						+ "\t\t " + rs.getString("genre_name")); 
			} 

			ResultSet rs1 = stmt.executeQuery("Select * from Studio");
			System.out.println("\tTable Studio");
			System.out.println("studio_id " + "\tstudio_name ");
			while(rs1.next()) {
				System.out.println( " "+rs1.getInt("studio_id") 
						+ "\t\t " + rs1.getString("studio_name")); 
			}
			
			ResultSet rs2 = stmt.executeQuery("Select * from Movie");
			System.out.println("\t\t\t\tTable Movie");
			System.out.println("movie_id " + "\t\tmovie_title "
					 + "\t\tgenre_id " + "\tstudio_id ");
			while(rs2.next()) {
				System.out.println( " "+rs2.getInt("movie_id") 
						+ "\t\t " + rs2.getString("movie_title")
						+"\t\t " + rs2.getInt("genre_id")
						+"\t " + rs2.getInt("studio_id")); 
			} 
			
			ResultSet rs3 = stmt.executeQuery("Select * from Movie_Director");
			System.out.println("\tTable Movie_Director");
			System.out.println("movie_id " + "\tdirector_id ");
			while(rs3.next()) {
				System.out.println( " "+rs3.getInt("movie_id") 
						+ "\t\t " + rs3.getInt("director_id"));						 
			} 
			
			ResultSet rs4 = stmt.executeQuery("Select * from Movie_Format");
			System.out.println("\tTable Movie_format");
			System.out.println("movie_id " + "\tformat_id " + "\tprice ");
			while(rs4.next()) {
				System.out.println( " "+rs4.getInt("movie_id") 
						+ "\t\t " + rs4.getInt("format_id")
						+ "\t\t " + rs4.getString("price"));						 
			} 
			
			ResultSet rs5 = stmt.executeQuery("Select * from Director");
			System.out.println("\tTable Director");
			System.out.println("director_id " + "\tdirector_name ");
			while(rs5.next()) {
				System.out.println( " "+rs5.getInt("director_id") 
						+ "\t\t " + rs5.getString("director_name"));		 
			} 
			
			ResultSet rs6 = stmt.executeQuery("Select * from Format");
			System.out.println("\tTable Format");
			System.out.println("format_id " + "\tformat_name ");
			while(rs6.next()) {
				System.out.println( " "+rs6.getInt("format_id") 
						+ "\t\t " + rs6.getString("format_name"));		 
			} 	
			
			System.out.println("*****************************");
			/*
			 * UPDATE QUERY
			 * */
			stmt.executeUpdate("UPDATE Format set format_name='DVD\t' where format_id =5");
			System.out.println("Data updated successfully into Format using \"UPDATE QUERY\"");
			
			ResultSet rs7 = stmt.executeQuery("Select * from Format");
			System.out.println("\tTable Format");
			System.out.println("format_id " + "\tformat_name ");
			while(rs7.next()) {
				System.out.println( " "+rs7.getInt("format_id") 
						+ "\t\t " + rs7.getString("format_name"));		 
			} 	
			System.out.println("*****************************");
			/*
			 * ALTER QUERY
			 * */
			stmt.executeUpdate("ALTER table Format add format_changed varchar(255)");
			System.out.println("Added new column successfully into Format using \"ALTER TABLE QUERY\"");
			
			/*
			 * UPDATE QUERY
			 * */
			stmt.executeUpdate("UPDATE Format set format_changed='no'");
			stmt.executeUpdate("UPDATE Format set format_changed='yes' where format_id=5");
					
			
			ResultSet rs8 = stmt.executeQuery("Select * from Format");
			System.out.println("\tTable Format");
			System.out.println("format_id " + "\tformat_name " + "\tformat_changed ");
			while(rs8.next()) {
				System.out.println( " "+rs8.getInt("format_id") 
						+"\t\t " +  rs8.getString("format_name")
						+ "\t " +  rs8.getString("format_changed"));		 
			} 	
			
			/*
			 * DELETE QUERY
			 * */
			stmt.executeUpdate("ALTER table Format drop column format_changed");
			System.out.println("Table Format after removing \"format_changed\" column");
			ResultSet rs9 = stmt.executeQuery("Select * from Format");
			System.out.println("\tTable Format");
			System.out.println("format_id " + "\tformat_name ");
			while(rs9.next()) {
				System.out.println( " "+rs9.getInt("format_id") 
						+ "\t\t " + rs9.getString("format_name"));		 
			} 
			
			conn.commit();
		}catch(SQLException e){
			System.out.println("Catch Exception" + e);
			conn.rollback();
			stmt.close();
			conn.close();
		} finally{
			stmt.close();
			conn.close();
		}
	}
}
