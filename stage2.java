import java.net.*;  
import java.io.*;  
import java.util.*;


public class stage2{  

    //Global variables
    Socket s;
    BufferedReader in;
    DataOutputStream out;
    String str;
    String server;
    boolean run = true;
    boolean first = true;
    String username = System.getProperty("user.name");
    int jobID = 0, core, ram, disk;
    int serverID;
    int count;
    String[] jobarray;
    String type;
    String[] serverarray;
    String[] firstavail;

    //gets IP address and port and establishes a connection with server
    public stage2(String address, int port) {

        try{
            s = new Socket(address, port);
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
        str = svrreceiveString();

        //second step handshake
        out.write(("AUTH " + username + "\n").getBytes());
        out.flush();
        str = svrreceiveString();

        //final step handshake
        out.write(("REDY\n").getBytes());
        out.flush();
        str = svrreceiveString();


        while (run){
            if (str.equals("OK")){
                out.write(("REDY\n").getBytes());
                out.flush();
                str = svrreceiveString();
            }

            //breaks out of loop when run out of jobs
            if (str.contains("NONE")){
                run = false;
                break;
            }

            if (str.contains("JOBN")){
                //splits jobn
                jobarray = str.split(" ");

                //saves the job info
                jobID = Integer.parseInt(jobarray[2]);
                core = Integer.parseInt(jobarray[4]);
                ram = Integer.parseInt(jobarray[5]);
                disk = Integer.parseInt(jobarray[6]);

                //Sends Gets Avail for specific job size
                out.write(("GETS Avail" + " " + jobarray[4] + " " + jobarray[5] + " " + jobarray[6] + "\n").getBytes());
                out.flush();

                //Saves amount of Avail servers
                server = svrreceiveString();
                serverarray = server.split(" ");
                count = Integer.parseInt(serverarray[1]);
                out.write(("OK\n").getBytes());
                out.flush();

                ArrayList<String> servers = new ArrayList<>(count);
                    //stores Avail servers to an arrayList
                    for (int i = 0; i< count; i++){
                        servers.add(svrreceiveString());
                    }
                //If Gets Avail has no servers does Capable instead
                if(count ==0){
                    svrreceiveString();
                    out.write(("GETS Capable" + " " + jobarray[4] + " " + jobarray[5] + " " + jobarray[6] + "\n").getBytes());
                    out.flush();

                    //Gets amount of Capable servers
                    server = svrreceiveString();
                    serverarray = server.split(" ");
                    count = Integer.parseInt(serverarray[1]);
                    out.write(("OK\n").getBytes());
                    out.flush();

                    //Stores capable servers to an arraylist
                    for (int i = 0; i< count; i++){
                        servers.add(svrreceiveString());
                    }
                }
                // save server type and ID
                type = servers.get(0).split(" ")[0];
                serverID = Integer.parseInt(servers.get(0).split(" ")[1]); 
                out.write(("OK\n").getBytes());
                out.flush();
                str = svrreceiveString();

                // Schedules the job
                out.write(("SCHD " + jobID + " " + type + " " + serverID + "\n").getBytes());
                out.flush();
                str = svrreceiveString();
            }
            //Gets new job if JCPL is received
            if(str.contains("JCPL")){
                out.write(("REDY\n").getBytes());
                out.flush();
                str = svrreceiveString();
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
        } catch(IOException ioException){
            System.out.println(ioException);
        }

    }

    //Messages recieved from the socket
    public String svrreceiveString(){

        String handstr = "";
        try {
        handstr = in.readLine();
        }catch (IOException ioException){
            System.out.println(ioException);
        }

        return handstr;
    }

    public static void main (String args[]) throws IOException{
        stage2 client = new stage2("127.0.0.1", 50000);
        client.auth();
    }
}


