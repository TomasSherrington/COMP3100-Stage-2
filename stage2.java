import java.net.*;  
import java.io.*;  
import java.util.*;
public class stage2{  

    //Global variables
    Socket s;
    BufferedReader in;
    DataOutputStream out;
    String str;
    boolean run = true;
    boolean first = true;
    String username = System.getProperty("user.name");
    int jobID = 0, core = 0, memory = 0, disk = 0;
    int serverID;
    int count;
    String[] jobarray;
    String type;
    String[] serverarray;
    String[] firstcapable;

    //gets IP address and port and establishes a connection with server
    public stage2(String address, int port) {

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
            if (str.contains("JOBN")){
                jobarray = str.split(" ");
                jobID = Integer.parseInt(jobarray[2]);
                core = Integer.parseInt(jobarray[4]);
                memory = Integer.parseInt(jobarray[5]);
                disk = Integer.parseInt(jobarray[6]);
                //Sends Gets Capable
                out.write(("GETS Capable" + " " + core + " " + memory + " " + disk + "\n").getBytes());
                out.flush();
                str = svrrecievString();
                serverarray = str.split(" ");
                count = Integer.parseInt(serverarray[1]);
                out.write(("OK\n").getBytes());
                out.flush();
                str = svrrecievString();
                for(int i=0;i<count;i++){
                    firstcapable = str.split(" ");
                    if(first){
                        type = firstcapable[0];
                        serverID = Integer.parseInt(firstcapable[1]);
                    }
                    first = false;
                }
                out.write(("OK\n").getBytes());
                out.flush();
                str = svrrecievString();
                out.write(("SCHD " + jobID + " " + type + " " + serverID + "\n").getBytes());
                out.flush();
                str = svrrecievString();
            }
            if(str.contains("JCPL")){
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
        stage2 client = new stage2("127.0.0.1", 50000);
        client.auth();
    }
}


