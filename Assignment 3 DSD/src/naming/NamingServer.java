package naming;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import common.Path;
import rmi.RMIException;
import rmi.Skeleton;
import storage.Command;
import storage.Storage;

/** Naming server.

    <p>
    Each instance of the filesystem is centered on a single naming server. The
    naming server maintains the filesystem directory tree. It does not store any
    file data - this is done by separate storage servers. The primary purpose of
    the naming server is to map each file name (path) to the storage server
    which hosts the file's contents.

    <p>
    The naming server provides two interfaces, <code>Service</code> and
    <code>Registration</code>, which are accessible through RMI. Storage servers
    use the <code>Registration</code> interface to inform the naming server of
    their existence. Clients use the <code>Service</code> interface to perform
    most filesystem operations. The documentation accompanying these interfaces
    provides details on the methods supported.

    <p>
    Stubs for accessing the naming server must typically be created by directly
    specifying the remote network address. To make this possible, the client and
    registration interfaces are available at well-known ports defined in
    <code>NamingStubs</code>.
 */
public class NamingServer implements Service, Registration
{
	private Skeleton<Service> serviceSkeleton;
	private Skeleton<Registration> registrationSkeleton;
	
	private List<Path> pathArrayList = new ArrayList<>();
	private Map<Path, Boolean> pathFolderMap = new HashMap<>();
	private Map<Path, Storage> storageMap = new HashMap<>();
	private Map<Path, Command> commandMap = new HashMap<>();
	
    private List<Storage> storageStubs = new ArrayList<>();
    /**
     * Contains all of the Command stubs which have registered with this NamingServer
     */
    private List<Command> commandStubs = new ArrayList<>();
	
    /** Creates the naming server object.

        <p>
        The naming server is not started.
     */
    public NamingServer()
    {
        serviceSkeleton = new Skeleton<>(Service.class, this, new InetSocketAddress("127.0.0.1", NamingStubs.SERVICE_PORT));
        registrationSkeleton = new Skeleton<>(Registration.class, this, new InetSocketAddress("127.0.0.1", NamingStubs.REGISTRATION_PORT));
        
        Service serviceStub = NamingStubs.service("127.0.0.1");
    	Registration regStub = NamingStubs.registration("127.0.0.1");
    }

    /** Starts the naming server.

        <p>
        After this method is called, it is possible to access the client and
        registration interfaces of the naming server remotely.

        @throws RMIException If either of the two skeletons, for the client or
                             registration server interfaces, could not be
                             started. The user should not attempt to start the
                             server again if an exception occurs.
     */
    public synchronized void start() throws RMIException
    {
        serviceSkeleton.start();
        registrationSkeleton.start();
    }

    /** Stops the naming server.

        <p>
        This method waits for both the client and registration interface
        skeletons to stop. It attempts to interrupt as many of the threads that
        are executing naming server code as possible. After this method is
        called, the naming server is no longer accessible remotely. The naming
        server should not be restarted.
     */
    public void stop()
    {
    	 serviceSkeleton.stop();
         registrationSkeleton.stop();
         stopped(null);
    }

    /** Indicates that the server has completely shut down.

        <p>
        This method should be overridden for error reporting and application
        exit purposes. The default implementation does nothing.

        @param cause The cause for the shutdown, or <code>null</code> if the
                     shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following methods are documented in Service.java.
    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException
    {

     	String localPath;

         
         if(path.toString().endsWith("/")) {
        	 localPath = path.toString();
         } else {
        	 localPath = path.toString() + "/";
         }
    	
    	boolean isDirectory = false, isPresent = false;
        for(Path p: pathArrayList) {
        	if(p.toString().startsWith(localPath)) {
        		isDirectory = true;
        		
        	}
        	if(p.toString().startsWith(path.toString())) {
        		isPresent = true;
        	}
        	
        	if(isDirectory) {
        		break;
        	}
        }
        
        if(pathFolderMap.containsKey(path)) {
        	return pathFolderMap.get(path);
        }
        
        if(!isPresent) {
        	throw new FileNotFoundException();
        }
        
    	return isDirectory;
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException
    {
    	List<String> listPaths = new ArrayList<>();
        if(directory == null) {
        	throw new NullPointerException();
        }
        
        boolean res = isDirectory(directory);
        if(!res) {
        	throw new FileNotFoundException();
        }
        
        //Logic here
        
        for(Path p: pathArrayList) { 
        	if(p.toString().startsWith(directory.toString())) {
        		//Start taking substrings

        		String[] pathSplits = Arrays.stream(p.toString().split("/")).filter(s -> !s.trim().equals("")).toArray(String[]::new);

        		String[] dirSplits = Arrays.stream(directory.toString().split("/")).filter(s -> !s.trim().equals("")).toArray(String[]::new); 
                
         
                

        		if(!listPaths.contains(pathSplits[dirSplits.length])) {
        			listPaths.add(pathSplits[dirSplits.length]);
        		}
        	}
        }

        
        return (String[]) listPaths.toArray(new String[listPaths.size()]);
    }

    @Override
    public boolean createFile(Path file)
        throws RMIException, FileNotFoundException
    {
    	boolean result = false;
    	
    	if(file.isRoot()) {
    		return false;
    	}


    	if(!exists(file.parent())) {

    		throw new FileNotFoundException();
    	}
    	
    	if(exists(file)) {

    		return false;
    	}
    	

    	
    	String dirPath = file.toString().substring(0, file.toString().lastIndexOf("/"));
    	
    	if(dirPath.trim().isEmpty()) {
    		dirPath = "/";
    	}
    	
    
    	for(Path p: pathArrayList) {
    		
    		if(p.toString().equals(dirPath)) {
    			if(!pathFolderMap.get(p)) {
    				throw new FileNotFoundException();
    			} else {
    				
    				result = true;
    				break;
    			}
    		} else if(p.toString().startsWith(dirPath) && p.toString().length() > dirPath.toString().length()) {
    			
    			result = true;
				break;
    			
    		}
    		
    		
    	
    	}
    	
    	if(result) {
    		
			Command cs = commandStubs.get(0);
			Storage ss = storageStubs.get(0);

			storageMap.put(file, ss);
 		   	commandMap.put(file, cs);
 		   	cs.create(file);
 		   	
 		   pathArrayList.add(file);
 		   pathFolderMap.put(file, false);
			
			
    	}
    
    	
    	return result;
    	
    }
    
    public boolean exists(Path file) {
    	
 	   boolean flag = false;
    	
        for(Path path: pathArrayList) {
     	   if(!path.toString().equals("/")) {
     		   
	        		   if(path.toString().startsWith(file.toString())) {
	        			   flag = true;
	        			   break;
	        		   }
	        		   
	 
     	   }
        }
        
        return flag;
    }

    @Override
    public boolean createDirectory(Path directory) throws FileNotFoundException
    {
    	boolean result = false;
    	
    	if(directory.isRoot()) {
    		return false;
    	}

    	if(!exists(directory.parent())) {

    		throw new FileNotFoundException();
    	}
    	
    	if(exists(directory)) {
    		return false;
    	}
    	
    	String dirPath = directory.toString().substring(0, directory.toString().lastIndexOf("/"));
    	
    	if(dirPath.isEmpty()) {
    		dirPath = "/";
    	}
    	
    	for(Path p: pathArrayList) {
    		
    		if(p.toString().equals(dirPath)) {
    			if(!pathFolderMap.get(p)) {
    				throw new FileNotFoundException();
    			} else {
    				
    				result = true;
    				break;
    			}
    		} else if(p.toString().startsWith(dirPath) && p.toString().length() > dirPath.toString().length()) {
    			
    			result = true;
				break;
    			
    		}
    		
    		
    	
    	}
    	
    	if(result) {
    		pathArrayList.add(directory);
			pathFolderMap.put(directory, true);
    	}
    	
    	
    	return result;
    	
    }

    @Override
    public boolean delete(Path path) throws FileNotFoundException
    {
    	
    	if(path.isRoot()) {
    		return false;
    	}


    	if(!exists(path.parent()) && !exists(path)) {
    		throw new FileNotFoundException();
    	}
    	
        Command c = commandMap.get(path);
        boolean res = false;
		try {
			res = c.delete(path);
			
			if(res) {
				pathArrayList.remove(path);
				commandMap.remove(path);
				storageMap.remove(path);
				pathFolderMap.remove(path);
			}
			
		} catch (RMIException e) {
			res = false;			
		}
        
        return res;
        
        
    }

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException
    {

        if (file == null) {
            throw new NullPointerException();
        }
        if (!exists(file)) {
            throw new FileNotFoundException();
        }
        if (!pathFolderMap.containsKey(file)) {
            throw new FileNotFoundException();
        }

        return storageMap.get(file);
    }

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub,
                           Path[] files)
    {
    	
    	
       if(client_stub == null || command_stub == null || files == null) {
    	   throw new NullPointerException();
       }
       
       
       if(!storageStubs.contains(client_stub)) {
           storageStubs.add(client_stub);

       } else {
    	   throw new IllegalStateException();
       }
       
       if(!commandStubs.contains(command_stub)) {
           commandStubs.add(command_stub);

       } else {
    	   throw new IllegalStateException();
       }
       
       Path[] delFiles = {};
       List<Path> delArrayList = new ArrayList<>();
       
       if(pathArrayList.size() == 0) {
    	   pathArrayList.addAll(Arrays.asList(files));
    	   for(Path p: pathArrayList) {
    		   pathFolderMap.put(p, false);
    		   storageMap.put(p, client_stub);
    		   commandMap.put(p, command_stub);
    	   }
       } else {

           
           //Get Paths to be updated
           for(Path path: files) {
        	   if(!path.toString().equals("/")) {
        		   
	        	   boolean flag = false;
	        	   for(Path p2: pathArrayList) {
	        		   if(p2.toString().startsWith(path.toString())) {
	        			   flag = true;
	        			   delArrayList.add(path);
	        			   break;
	        		   }
	        		   
	        		   
	        	   }
	        	   
	        	   if(!flag) {
	    			   pathArrayList.add(path);
	    			   pathFolderMap.put(path, false);
	    			   storageMap.put(path, client_stub);
	        		   commandMap.put(path, command_stub);
	    		   }
        	   }
           }
    	   
   
       }
       
       return (Path[]) delArrayList.toArray(new Path[delArrayList.size()]);
              
    }
}
