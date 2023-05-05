import java.net.*;  
import java.io.*;  
import java.util.*;
public class client{  

    //Global variables
    Socket s;
    BufferedReader in;
    DataOutputStream out;
    String str;
    String jobs;
    boolean run = true;
    boolean getsloop = true;
    String largestserver;
    int servertype;
    int jobID;
    String username = System.getProperty("user.name");

    //gets IP address and port and establishes a connection with server
    public client(String address, int port) {

        try{
            s = new Socket(address, port);
           // System.out.println("Connection Established");
            in = new BufferedReader(new InputStreamReader(s.getInputStream()));
            out = new DataOutputStream(s.getOutputStream());
        }
        catch(IOException ioException){
            System.out.println(ioException);
        }
        
    }
    //run client and do initial handshake
    public void auth() throws IOException{
        //First step handshake
        out.write(("HELO\n").getBytes());
        out.flush();
        str = svrrecievString();
        //second step handshake
        out.write(("AUTH " + username + "\n").getBytes());
        out.flush();
        str = svrrecievString();
        //final step handshake
        out.write(("REDY\n").getBytes());
        out.flush();
        str = svrrecievString();
        String firstjob = str;
        //Gets all to get server records
        out.write(("GETS All\n").getBytes());
        out.flush();
        str= svrrecievString();
        //Storing number of records from the GETS command
        String[] recs = str.split("\s");
        int nRecs = Integer.parseInt(recs[1]);
        out.write(("OK\n").getBytes());
        out.flush();
        //add server info into arraylist
        ArrayList<String[]> serverinfo = new ArrayList<String[]>();
        //System.out.println("Number of records: " + nRecs);
        for (int i = 0; i < nRecs; i++){
            str = svrrecievString();
           // System.out.println(str);
            serverinfo.add(str.split("\s"));
        }
        //send OK to get the . response at end of GETS All
        out.write(("OK\n").getBytes());
        out.flush();
        str = svrrecievString();
       
        //set the str back to original response to REDY
        str = firstjob;

        int maxcore = 0;
        // Finding maxcore in serverinfo arraylist
        for(String[] num : serverinfo){
            int currentcore = Integer.parseInt(num[4]);
            if(currentcore > maxcore) maxcore = currentcore;
        }

        //finding first server name with matching core
        String firstservername = "";
        for(String[] num : serverinfo){
            if(num[4].equals(String.valueOf(maxcore))){
                firstservername = num[0];
                break;
            }
        }


        //filling an Array with all servers with that match maxcore
        ArrayList<String[]> servercount = new ArrayList<String[]>();
        for(String[] num : serverinfo){
            if(num[4].equals(String.valueOf(maxcore)) && num[0].equals(firstservername)){
                servercount.add(num);
            }
        }

        //Getting amount of servers that match maxcore
        int serveramount = servercount.size();

        while (run){
            if (str.equals("OK")){
                out.write(("REDY\n").getBytes());
                out.flush();
                str = svrrecievString();
                //System.out.println(str);
            }
            //breaks out of loop when run out of jobs
            if (str.equals("NONE")){
                run = false;
                break;
            }

            //schedule jobs
            largestserver = servercount.get(0)[0];
            String [] strArr = str.split("\s");
            //System.out.println("Adding job: "+ str);
            jobID = Integer.parseInt(strArr[2]);
            int element = jobID % serveramount;
            servertype = Integer.parseInt(servercount.get(element)[1]);
            //System.out.println("This is Int: "+servertype);
            //System.out.println("This is String: " + String.valueOf(servertype));
            if (strArr[0].equals("JOBN")){    
                out.write(("SCHD " + jobID + " " + largestserver + " " + servertype + "\n").getBytes());
                out.flush();
                str = svrrecievString();
            }
            if(strArr[0].equals("JCPL")){
                out.write(("REDY\n").getBytes());
                out.flush();
                str = svrrecievString();
            }
        }
        closeConnection();
    }
    //stop the socket and terminate nicely
    public void closeConnection () {
        try{
            out.write(("QUIT\n").getBytes());
            in.close();
            out.close();
            s.close();
            //System.out.println("Terminating here");
        } catch(IOException ioException){
            System.out.println(ioException);
        }

    }

    //Messages recieved from the socket
    public String svrrecievString(){

        String handstr = "";
        try {
        handstr = in.readLine();
        str = handstr;
        }catch (IOException ioException){
            System.out.println(ioException);
        }

        return str;
    }

    public static void main (String args[]) throws IOException{
        client client = new client("127.0.0.1", 50000);
        client.auth();
    }
}


