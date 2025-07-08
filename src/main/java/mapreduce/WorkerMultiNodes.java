package mapreduce;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class WorkerMultiNodes {
    private final String masterHost;
    private final int masterPort;
    private final int listenPort;
    private final Map<String,Integer> localMap = new HashMap<>();
    private final Map<String,List<Integer>> partitions = new ConcurrentHashMap<>();
    private final List<InetSocketAddress> workerAddrs = new ArrayList<>();
    private int myIndex = -1;

    public WorkerMultiNodes(String host,int port,int listenPort) {
        this.masterHost = host;
        this.masterPort = port;
        this.listenPort = listenPort;
    }

    /**
     * Démarre le serveur de partitions, charge workers.txt, se connecte au Master.
     */
    public void start() {
        // Ouvre un ServerSocket pour shuffle
        try (ServerSocket peerServer=new ServerSocket(listenPort)){
            new Thread(()->{
                while(true) try {
                    Socket s=peerServer.accept(); receivePartition(s);
                } catch(IOException e) { System.err.println("Shuffle accept error: " + e.getMessage()); }
            }).start();
        }catch(IOException e){
            System.err.println("Cannot start shuffle server: " + e.getMessage()); return;
        }
        // Charge la liste des Workers depuis workers.txt
        try {
            List<String> lines = Files.readAllLines(Paths.get("workers.txt"));
            for(int i=0;i<lines.size();i++){
                String[] hp=lines.get(i).split(":");
                workerAddrs.add(new InetSocketAddress(hp[0],Integer.parseInt(hp[1])));
                if(Integer.parseInt(hp[1])==listenPort) myIndex=i;
            }
        }catch(IOException e){
            System.err.println("Cannot read workers.txt: " + e.getMessage()); return;
        }
        // Connexion au Master
        try(Socket sock=new Socket(masterHost,masterPort);
            BufferedReader in=new BufferedReader(new InputStreamReader(sock.getInputStream()));
            PrintWriter out=new PrintWriter(sock.getOutputStream(),true)){
            out.println("REGISTER " + listenPort);
            String req;
            while((req=in.readLine())!=null){
                String[] parts=req.split(" ");
                switch(parts[0]){
                    case "MAP": map(parts[1],out); break;
                    case "SHUFFLE": shuffle(out); break;
                    case "REDUCE": reduce(out); break;
                }
            }
        }catch(IOException e){
            System.err.println("Master connection error: " + e.getMessage());
        }
    }

    private void map(String filename,PrintWriter out){
        try{
            byte[] bytes=Files.readAllBytes(Paths.get("texts/"+filename));
            for(String w:new String(bytes).split("\\s+")) localMap.merge(w,1,Integer::sum);
        }catch(IOException e){System.err.println("Map read error: " + e.getMessage());}
        out.println("SIGNAL:MAP_DONE");
    }

    private void shuffle(PrintWriter out){
        localMap.forEach((k,c)->{
            int t=Math.floorMod(k.hashCode(),workerAddrs.size());
            if(t==myIndex) partitions.computeIfAbsent(k,kk->new ArrayList<>()).add(c);
            else{
                InetSocketAddress a=workerAddrs.get(t);
                try(Socket s=new Socket(a.getHostString(),a.getPort());
                    PrintWriter pw=new PrintWriter(s.getOutputStream(),true)){
                    pw.println(k+":"+c);
                }catch(IOException e){System.err.println("Shuffle send error: " + e.getMessage());}
            }
        });
        try{Thread.sleep(500);}catch(InterruptedException ignored){}
        out.println("SIGNAL:SHUFFLE_DONE");
    }

    private void receivePartition(Socket s){
        try(BufferedReader br=new BufferedReader(new InputStreamReader(s.getInputStream()))) {
            String l;
            while((l=br.readLine())!=null){
                String[] kv=l.split(":");
                partitions.computeIfAbsent(kv[0],xx->new ArrayList<>()).add(Integer.parseInt(kv[1]));
            }
        }catch(IOException e){System.err.println("Shuffle receive error: " + e.getMessage());}
    }

    private void reduce(PrintWriter out){
        partitions.forEach((k,l)->System.out.println(k+" -> "+l.stream().mapToInt(i->i).sum()));
        out.println("SIGNAL:REDUCE_DONE");
    }

    public static void main(String[] args){
        if(args.length<3){System.err.println("Usage: WorkerMultiNodes <masterHost> <masterPort> <listenPort>");return;}
        new WorkerMultiNodes(args[0],Integer.parseInt(args[1]),Integer.parseInt(args[2])).start();
    }
}