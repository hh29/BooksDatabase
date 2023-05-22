/*
 * BooksDatabaseService.java
 *
 * The service threads for the books database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: <1793538>
 *
 */

import java.awt.*;
import java.io.*;
import java.io.OutputStreamWriter;

import java.net.Socket;

import java.util.StringTokenizer;

import java.sql.*;
import javax.sql.rowset.*;
import javax.xml.transform.Result;
//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
//these clasess are not exported by the module. Instead, one needs to impor
//javax.sql.rowset.* as above.



public class BooksDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2]; //One slot for author's name and one for library's name.
    private ResultSet outcome   = null;

    //JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;



    //Class constructor
    public BooksDatabaseService(Socket aSocket){
        super("BooksDatabaseService");
        serviceSocket = aSocket;
        //TO BE COMPLETED

    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr[0] = ""; //For author
        this.requestStr[1] = ""; //For library

        String tmp = "";
        try {
            InputStream socketStream = this.serviceSocket.getInputStream();
            InputStreamReader socketReader = new InputStreamReader(socketStream);

            StringBuffer stringBuffer = new StringBuffer();
            char x;
            while (true) //Read until terminator character is found
            {
                x = (char) socketReader.read();
                if (x == '#')
                    break;
                stringBuffer.append(x);
            }
            StringTokenizer string = new StringTokenizer(stringBuffer.toString(), ";");

            this.requestStr[0] = string.nextToken();
            this.requestStr[1] = string.nextToken();

            //TO BE COMPLETED
        }catch(IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;

        this.outcome = null;

        String sql = "SELECT book.title, book.publisher, book.genre, book.rrp, COUNT(bookcopy.copyid) AS num_copies" +
                " FROM book JOIN bookcopy ON book.bookid = bookcopy.bookid" +
                "\n" +
                "JOIN author ON book.authorid = author.authorid\n" +
                "JOIN library ON library.libraryid = bookcopy.libraryid  WHERE familyname = ? AND city = ?" +
                " GROUP BY book.title, book.publisher, book.genre, book.rrp;";


        try {
            //Connect to the database
                Class.forName("org.postgresql.Driver");
                Connection con = DriverManager.getConnection(URL, USERNAME, PASSWORD);

            //Make the query
            //TO BE COMPLETED
            PreparedStatement stmt = con.prepareStatement(sql);
            stmt.setString(1, this.requestStr[0]);
            stmt.setString(2,this.requestStr[1]);
            ResultSet rs = stmt.executeQuery();


            //Process query
            //TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.
            RowSetFactory aFactory = RowSetProvider.newFactory();
            CachedRowSet crs = aFactory.createCachedRowSet();
            crs.populate(rs);
            this.outcome = crs;

            //Clean up
            //TO BE COMPLETED
            rs.close();
            stmt.close();
            con.close();

        } catch (Exception e)
        { System.out.println(e); }

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
            //Return outcome
            //TO BE COMPLETED
            ObjectOutputStream outcomeStreamWriter = new ObjectOutputStream (serviceSocket.getOutputStream()) ;
            outcomeStreamWriter.writeObject (this.outcome);
            outcomeStreamWriter.flush();

            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);

            //Terminating connection of the service socket
            this.serviceSocket.close();


        }catch (IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
    }


    //The service thread run() method
    public void run()
    {
        try {
            System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
                    + "author->" + this.requestStr[0] + "; library->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
