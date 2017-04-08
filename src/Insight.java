/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package insight;

import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.text.*;

/**
 *
 * @author Muqeet
 * 
 * 
 * 
 */

class Host {
        
      String hostname;  
      Integer count;  
       
    /*  public int compare(Host a, Host b)
      {
          return a.count - b.count;
          
      }      */
      
      public Host(String name, Integer value)
      {
          
          hostname = name;
          count = value;
          
      }
        
    }

class Resource {
        
      String name;  
      BigInteger bytes;  
       
      
      public Resource(String name, BigInteger value)
      {
          
          this.name = name;
          bytes = value;
          
      }
        
    }

class Period{
    
    String start;
    BigInteger count;
    
    public Period(String start, BigInteger count)
    {
        this.start = start;
        this.count = count;
        
        
    }
}

class LogInfo {
    
    Date fstAttmpt; //logs the first failed login attempt
    Date sndAttmpt; //logs the second failed login attempt
    
    Date logStart; //logs the start of logging period
}

public class Insight {

    /**
     * @param args the command line arguments
     */
    public static final long ONE_SECOND_IN_MILLIS = 1000;//millisecs
    
    public static boolean isFailedLogin(String line)
    {   
        String[] parts = line.split(" ");
        
        String code = parts[8];
        
        if ( Integer.valueOf(code) == 401 )
            return true;
        else
            return false;
    }
    
    public static boolean isValidLogin(String line)
    {   
        String[] parts = line.split(" ");
        
        String code = parts[8];
        
        if ( Integer.valueOf(code) == 200 )
            return true;
        else
            return false;
    }
    
    public static String getHostName(String line)
    {
        //this function returns the hostname extracted from line
        
     String[] parts=line.split(" ");
        
     return parts[0];   
        
        
    }
     
     public static String getResource(String line)
     {
         
         String[] parts = line.split(" ");
         
         return parts[6];
         
     }        
     
     public static String getBytes(String line)
     {
         
          String[] parts = line.split(" ");
       
          
          return parts[9];
     }
     
     public static String getTimeStamp(String line)
     {
         
         String[] parts = line.split(" ");
         
         String timeStamp = parts[3].substring(1);
         
         return timeStamp;
         
     }
    
    public static void writeToFile(ArrayList<String> list, String fileName)
    {
        
        try {
            // Assume default encoding.
            FileWriter fileWriter =
                new FileWriter(fileName);

            // Always wrap FileWriter in BufferedWriter.
            BufferedWriter bufferedWriter =
                new BufferedWriter(fileWriter);

           for(int i=0;i<list.size(); i++)
           {
            bufferedWriter.write(list.get(i));
            bufferedWriter.newLine();
           }
            // Always close files.
            bufferedWriter.close();
        }
        catch(IOException ex) {
            System.out.println
		(
                "Error writing to file '"
                + fileName + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }
           
    }
    
    public static void mostActiveHosts(String fileName, String outputFile)
    {
        //this function calculates the 10 most active hosts
        
        //read the data from the file log.txt
        String line=null;
        
        //create a map
            HashMap<String,Integer> map = new HashMap<String,Integer>(); 
        
        //create the min priority queue
              
         PriorityQueue<Host> queue=new PriorityQueue<Host>(50,new Comparator<Host>() {
             
             public int compare(Host a, Host b)
             {
                 int val= a.count - b.count;
		 if(val!=0)
		     return val;
		 else
		     return b.hostname.compareTo(a.hostname);
             }
             
         });
        
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                
            String hostname = getHostName(line);
             
            //calculate frequency for each host    
                
            if(map.containsKey(hostname)) 
            {
                map.put(hostname, map.get(hostname) + 1);
                
                
            }
            else
                map.put(hostname,1);
                          
            }   

            //close file.
            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + fileName + "'");                  
            // Or we could just do this: 
            // ex.printStackTrace();
        }
        
        //now process the map and maintain the priority queue of size 10
        
        for(Map.Entry<String,Integer> entry: map.entrySet())
        {
            Host host = new Host(entry.getKey(),entry.getValue());
            
            //add to the queue
            queue.offer(host);
            
            if(queue.size()>10)
                queue.poll();
        }
        
        
        //prepare 10 most active hosts for printing
        ArrayList<String> list=new ArrayList<String>();
        
        while(queue.size()>0)
        {
            Host h = queue.poll();
            String output = h.hostname + "," + String.valueOf(h.count);
            list.add(output);
            
            
        }
        
        //reverse the result
        Collections.reverse(list);
        
        //write to file
        writeToFile(list,outputFile);
    }
    
    
    public static void topResources(String fileName, String outputFile)
    {
	
        //this function calculates the 10 most bandwidth-intensive resources
        
        //read the data from the file log.txt
        String line=null;
        
        //create a map
            HashMap<String,BigInteger> map = new HashMap<String,BigInteger>(); 
        
        //create the min priority queue
              
         PriorityQueue<Resource> queue=new PriorityQueue<Resource>(50,new Comparator<Resource>() {
             
             public int compare(Resource a, Resource b)
             {
                 int val= a.bytes.compareTo(b.bytes);

		 if(val != 0)
		     return val;
		 else
		     return b.name.compareTo(a.name);
                     
             }
             
         });
        
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {

				
            String resource = getResource(line);
            
            Long bytes;
            
            if(getBytes(line).equals("-"))
             bytes= Long.valueOf(0);
            else
            bytes=Long.parseLong(getBytes(line));
             
            //calculate frequency for each resource based on bytes sent    
                
            if(map.containsKey(resource)) 
            {
                map.put(resource, map.get(resource).add(BigInteger.valueOf(bytes)));
                
                
            }
            else
                map.put(resource,BigInteger.valueOf(bytes));
                          
            }   

            //close file.
            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + fileName + "'");                  
            
        }
        
        //now process the map and maintain the priority queue of size 10
        
        for(Map.Entry<String,BigInteger> entry: map.entrySet())
        {
            Resource resource = new Resource(entry.getKey(),entry.getValue());
            
            //add to the queue
            queue.offer(resource);
            
            if(queue.size()>10)
                queue.poll();
        }
        
        
        //prepare 10 most bandwidth intensive resources for printing
        ArrayList<String> list=new ArrayList<String>();
        
        while(queue.size()>0)
        {
            Resource r = queue.poll();
            String output = r.name;
            list.add(output);
            
            
        }
        
        //reverse the result
        Collections.reverse(list);
        
        //write to file
        writeToFile(list,outputFile);        
    }
    

    public static void adjustWnd(Date d1, LinkedList<Date> Wnd)
    {
	// a helper method to adjust length of Wnd based on date d1
        while(Wnd.size()>0)
          {
                    if(Wnd.getFirst().getTime()<d1.getTime())
			Wnd.remove();
                    else
			{
                           
                            break;

		        }
          }

	
    }


    public static BigInteger countWndLength(Date d1, LinkedList<Date> Wnd)
    {

                BigInteger no = new BigInteger("0");
		BigInteger one = new BigInteger("1");
		Collections.reverse(Wnd);
		for(Date d: Wnd )
		    {
                          
                        long diff = d.getTime() - d1.getTime();
		        long diffSecs = diff/1000;

			if(diffSecs>=3600)
			    no=no.add(one);
			else
			    break;

		    }
		Collections.reverse(Wnd);

                return no;
	
    }
	
    public static void busiestPeriods(String fileName, String outputFile )
    {
        //this function calculates the 10 busiest 60 min periods

	SimpleDateFormat ft= new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
       
        String line=null;
        
        //create the min priority queue for calculating 10 busiest periods
              
         PriorityQueue<Period> queue=new PriorityQueue<Period>(50,new Comparator<Period>() {
             
             public int compare(Period a, Period b)
             {
                 int val= a.count.compareTo(b.count);

		 if(val!=0)
		     return val;
		 else
		     {
			 String dateOne = a.start;
			 String dateTwo = b.start;
                        
                         Date d1=null;
			 Date d2=null;

			 try{
			     d1 = ft.parse(dateOne);
			     d2 = ft.parse(dateTwo);

			     }
			 catch(Exception e)
			     {
				 System.out.println(e);
			     }

			 return d2.compareTo(d1);
			 
                     }
             }
             
         });
         
         //create a list to keep track of visits in a 60 min window
         LinkedList<Date> Wnd=new LinkedList<Date>();
         
                 
          
          Date d1= null;
          Date d2=null;
          BigInteger count=null;
        
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);
            
            String lastTimeStamp=null;
           
                    
            if((line = bufferedReader.readLine()) != null)
              lastTimeStamp= getTimeStamp(line);
            
           // System.out.println("lasttimestamp is" + lastTimeStamp);
            d1 = ft.parse(lastTimeStamp);
            BigInteger one = new BigInteger("1");
            count = new BigInteger("1");
            Wnd.add(d1);
            
            while((line = bufferedReader.readLine()) != null) {
                
            String timeStamp = getTimeStamp(line);
            
            //calculate the difference between two timestamps
            d2 = ft.parse(timeStamp);
         
            long diff = d2.getTime() - d1.getTime();
            
            long diffSecs = diff/(1000);
            
            if(diffSecs<3600)
            {
                //add one to the count
                count = count.add(one);
                
                //add to the Wnd
                Wnd.add(d2);
                
            }
            
            else 
            {
                //put in the queue
                Period period = new Period(ft.format(d1), count);
                queue.add(period);
                if(queue.size()>10)
                   queue.poll();
                
               //now find the new start time 
                
                    Date afterAddingSec = new Date(d1.getTime() + ONE_SECOND_IN_MILLIS);
                    d1=afterAddingSec;
                    diff = d2.getTime() - d1.getTime();
                    diffSecs = diff/(1000);
                    
                    if(diffSecs<3600)
                      count = one;
                    else
                      count=new BigInteger("0");  
                
                //calculate new count associated with new start time
                //calculate how many entries in Wnd should be retained
		    
		    adjustWnd(d1,Wnd);

		    
		    
		BigInteger no =countWndLength(d1,Wnd);
		BigInteger size = new BigInteger(String.valueOf(Wnd.size()));
		no = size.subtract(no);

		count=count.add(no);
                
                Wnd.add(d2);
                
            }
                   
        }
            //close file.
            bufferedReader.close();  
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + fileName + "'");                  
            
        }
        catch(Exception e)
        {
            System.out.println(e);
        }
        
         //process the last 60 min interval
        
        while(Wnd.size()>0)
        {
            //put in the queue
                Period period = new Period(ft.format(d1), count);
                queue.add(period);
                if(queue.size()>10)
                   queue.poll();
            
              
            //calculate new start time and new count associated with it
                
                Date afterAddingSec = new Date(d1.getTime() + ONE_SECOND_IN_MILLIS);
		//	BigInteger no = new BigInteger("0");
		//	BigInteger one =new BigInteger("1");
                d1=afterAddingSec;
                
	       
		adjustWnd(d1,Wnd);

		
		 BigInteger no = countWndLength(d1,Wnd);
	         BigInteger size = new BigInteger(String.valueOf(Wnd.size()));
                 no=size.subtract(no);
		 count = no;    
            
            
        }
        
     
        //prepare 10 busiest 60 min periods for printing
        ArrayList<String> list=new ArrayList<String>();
        
        while(queue.size()>0)
        {
            Period period = queue.poll();
            String output = period.start + " -0400" + "," + period.count;
            list.add(output);
            
            
        }
        
        //reverse the result
        Collections.reverse(list);
        
        //write to file
        writeToFile(list,outputFile);      
        
    }
    
    public static void detectFailedAttempts(String fileName, String outputFile)
    {
        
        
            //create list for logging acess
            ArrayList<String> list = new ArrayList<String>();
        
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);
        
            String line=null;
            //create a map for storing logging info for each host 
            HashMap<String,LogInfo> map = new HashMap<String,LogInfo>();
            
                    
            SimpleDateFormat ft = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
            
             while((line = bufferedReader.readLine()) != null) {
                 
                 String hostname = getHostName(line);
                 boolean validLogin = isValidLogin(line);
                 boolean failedLogin= isFailedLogin(line);
                 
                 
                 
                 //get time and date
                 String time = getTimeStamp(line);
                         
                 Date d = ft.parse(time);
               
                 //check whether events need to be logged
                 if( !map.containsKey(hostname) ) //this host does not already exist
                 {
                    // System.out.println("here in host"); 
                     
                     if(failedLogin == true)
                     {  
                        
                         
                         LogInfo info=new LogInfo();
                         
                         info.fstAttmpt=d;
                         info.logStart=null;
                         info.sndAttmpt=null;
                         
                         //put in the map
                         map.put(hostname,info);
                         
                     }
                     
                 }
                 
               else  //the host already exists
                 {     
               //System.out.println("before log");  
               LogInfo info   = map.get(hostname);
               
              //check whether need to log events
               Date logdate = info.logStart;
               long diff=0;
               long diffSecs=0;
               
               if(logdate != null)
               {
                   diff = d.getTime() - logdate.getTime();
                   diffSecs = diff/1000;
               }
               
               
               if(logdate!=null && diffSecs<=300) 
               {
                   
                   //in logging phase
                   //log all the events here
                   list.add(line);
                   
                   
                   
               }
                
               else
               {
                    
                //not in logging phase
		   // System.out.println("not in logging");
                 if(validLogin==true) //overwrite previous logged failed attempts
                 {
                     
                         info=new LogInfo();
                         
                         info.fstAttmpt=null;
                         info.logStart=null;
                         info.sndAttmpt=null;
                         
                         //put in the map
                         map.put(hostname,info);
                     
                     
                     
                     
                 }
                 
               
                 
                 if(failedLogin==true)
                 {
                     
                 
                     //get info from map
                     info=map.get(hostname); 
                     Date first = info.fstAttmpt;
                     Date second = info.sndAttmpt;
                    // Date log =info.logStart;
                     
                     
                     
                     if (first==null && second==null) // no failed login attempts
                     {
                         LogInfo newinfo = new LogInfo();
                         newinfo.fstAttmpt = d;
                         newinfo.sndAttmpt=null;
                         newinfo.logStart =null;
                         
                         map.put(hostname,newinfo);
                     }
                     
                     if(first!= null && second == null) //one failed attempt  was made previously
                     {
                          diff = d.getTime() - first.getTime();
                          diffSecs = diff/1000;
                         
                          //consider two cases
                         if(diffSecs<20)
                         {
                             LogInfo newinfo=new LogInfo();
                             newinfo.fstAttmpt=first;
                             newinfo.sndAttmpt=d;
                             newinfo.logStart=null;
                             
                             map.put(hostname,newinfo);
                         }
                         else
                         {
                             LogInfo newinfo=new LogInfo();
                             newinfo.fstAttmpt=d;
                             newinfo.sndAttmpt=null;
                             newinfo.logStart=null;
                             
                             map.put(hostname,newinfo);
                             
                             
                         }  
                         
                     }  
                      if(first!= null && second!=null) //two failed attempts were made previously
                      {
                          long diffOne = d.getTime() - first.getTime();
                          long diffOneSecs = diffOne/1000;
                          
                          long diffTwo = d.getTime() - second.getTime();
                          long diffTwoSecs = diffTwo/1000;
                          
                          //now consider three cases
                          
                          if (diffOneSecs<20) //start logging
                          {
                             LogInfo newinfo=new LogInfo();
                             newinfo.fstAttmpt=first;
                             newinfo.sndAttmpt=second;
                             newinfo.logStart=d;
                             
                             map.put(hostname,newinfo);    
                              
                          }
                          
                          if(diffOneSecs>20 && diffTwoSecs<20)
                          {
                             LogInfo newinfo=new LogInfo();
                             newinfo.fstAttmpt=second;
                             newinfo.sndAttmpt=d;
                             newinfo.logStart=null;
                             
                             map.put(hostname,newinfo); 
                              
                              
                          }
                          
                          if(diffTwoSecs>20)
                          {
                             LogInfo newinfo=new LogInfo();
                             newinfo.fstAttmpt=d;
                             newinfo.sndAttmpt=null;
                             newinfo.logStart=null;
                             
                             map.put(hostname,newinfo); 
                              
                              
                              
                          }   
                              
                      }
                         
                         
                     }
                           
                 }
                 }  
             }
            //close file.
            bufferedReader.close(); 
            
           }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + fileName + "'");                  
            
        }
        catch(Exception e)
        {
            System.out.println(e);
        }
        
        //now print the logged events
        
        writeToFile(list,outputFile);
        
        
    }
    
    public static void main(String[] args) {
        // TODO code application logic here

	String inputFile = args[0];
	  mostActiveHosts(inputFile,args[1]);
        
	topResources(inputFile,args[2]);
        
         busiestPeriods(inputFile,args[3]);
        
	 detectFailedAttempts(inputFile,args[4]);
        
       
        
    }
    
}
