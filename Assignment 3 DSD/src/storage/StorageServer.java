package storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.UnknownHostException;
import java.rmi.RemoteException;

import common.Path;
import naming.Registration;
import rmi.RMIException;
import rmi.Skeleton;
import rmi.Stub;

/** Storage server.

    <p>
    Storage servers respond to client file access requests. The files accessible
    through a storage server are those accessible under a given directory of the
    local filesystem.
 */
public class StorageServer implements Storage, Command
{
	
	private File root;
	
	Skeleton<Command> commandSkeleton; 
	Skeleton<Storage> storageSkeleton;
	
	private boolean isRegistered = false;
	
    /** Creates a storage server, given a directory on the local filesystem.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
    */
    public StorageServer(File root) throws RemoteException
    {
    	if(root == null) {
    		throw new NullPointerException();
    	}
    	
    	this.root = root;
    }

    /** Starts the storage server and registers it with the given naming
        server.

        @param hostname The externally-routable hostname of the local host on
                        which the storage server is running. This is used to
                        ensure that the stub which is provided to the naming
                        server by the <code>start</code> method carries the
                        externally visible hostname or address of this storage
                        server.
        @param naming_server Remote interface for the naming server with which
                             the storage server is to register.
        @throws UnknownHostException If a stub cannot be created for the storage
                                     server because a valid address has not been
                                     assigned.
        @throws FileNotFoundException If the directory with which the server was
                                      created does not exist or is in fact a
                                      file.
        @throws RMIException If the storage server cannot be started, or if it
                             cannot be registered.
     */
    public synchronized void start(String hostname, Registration naming_server)
        throws RMIException, UnknownHostException, FileNotFoundException
    {
    	
    	commandSkeleton =  new Skeleton<>(Command.class, this);
    	storageSkeleton =  new Skeleton<>(Storage.class, this);
    	
    	commandSkeleton.start();
    	storageSkeleton.start();
    	
    	Command commandStub = Stub.create(Command.class, commandSkeleton, hostname);
    	Storage storageStub = Stub.create(Storage.class, storageSkeleton, hostname);

    	
    	Path[] pathList = Path.list(root);
    	

    	
    	Path[] duplicateFiles = naming_server.register(storageStub, commandStub, pathList);
    	isRegistered = true;
    	
    	
    	for(Path p: duplicateFiles) {
    		if(!delete(p)) {
    			System.out.println("FNF Exception");
    			throw new FileNotFoundException("Error in deleting file on Storage Server");
    		}
    	}
    	
    }

    /** Stops the storage server.

        <p>
        The server should not be restarted.
     */
    public void stop()
    {
    	commandSkeleton.stop();
    	storageSkeleton.stop();
    }

    /** Called when the storage server has shut down.

        @param cause The cause for the shutdown, if any, or <code>null</code> if
                     the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException
    {
    	File file2 = file.toFile(root);
        if(file2.exists() && !file2.isDirectory()) {
        	return file2.length();
        } else {
        	throw new FileNotFoundException();
        }
    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length)
        throws FileNotFoundException, IOException
    {

    	
    	File file2 = file.toFile(root);
        if(file2.exists() && !file2.isDirectory()) {
        	if(file2.canRead()) {
        		
            	
            	if(offset < 0 || length < 0 || offset + length > file2.length()) {
            		throw new IndexOutOfBoundsException();
            	}
        		
        	  RandomAccessFile raf = new RandomAccessFile(file2.getAbsolutePath(), "r");

              byte[] bytesRead = new byte[length];
              raf.seek(offset);
              raf.readFully(bytesRead, 0, length);

              return bytesRead;
        	}  else {
        		throw new IOException();
        	}
        } else {
        	throw new FileNotFoundException();
        }
    }

    @Override
    public synchronized void write(Path file, long offset, byte[] data)
        throws FileNotFoundException, IOException
    {
    	
    
    	File file2 = file.toFile(root);
        if(file2.exists() && !file2.isDirectory()) {
        	
        	if(file2.canWrite()) {
        		
        		if(offset < 0) {
            		throw new IndexOutOfBoundsException();
            	}
            	       		
        	  RandomAccessFile raf = new RandomAccessFile(file2.getAbsolutePath(), "rw");
              raf.seek(offset);
              raf.write(data);
        	} else {
        		throw new IOException();
        	}
        } else {
        	throw new FileNotFoundException();
        }
    }

    // The following methods are documented in Command.java.
    @Override
    public synchronized boolean create(Path file)
    {
    	if(file.isRoot()) {
    		return false;
    	}
    	
    	if(file.toFile(root).exists()) {
    		return false;
    	}
    	
    	File dirs = file.parent().toFile(root);
    	dirs.mkdirs();
    	try {
			return file.toFile(root).createNewFile();
		} catch (IOException e) {
			
		}
        return false;
    }

    @Override
    public synchronized boolean delete(Path path)
    {
    	if(path.isRoot()) {
    		return false;
    	}
    	
    	if(!path.toFile(root).exists()) {
    		return false;
    	}
    	
    	boolean result = false;
    	
        File file = path.toFile(root);
        
        if(file.isDirectory()) {
        	//Call Subdir method here
        	result = deleteSubdir(file);
        } else {
        	result = deleteFile(path);
        }        
        
        return result;
    }
    
    public boolean deleteSubdir(File file) {
    	
    	File[] files = file.listFiles();
    	
    	for(File f : files) {
    		
    		if(f.isDirectory()) {
    			deleteSubdir(f);
    		}
    		
    		f.delete();
    	}
    	file.delete();
    	
    	return true;
    }
    
    public boolean deleteFile(Path p) {
    	
    	if(p.isRoot()) {
    		return false;
    	}
    	
    	File file = p.toFile(root);
    	
    	
    	if(!file.isDirectory()) {
    	
	    	if(file.delete()) {
	    		
	    		deleteFile(p.parent());
	    	}
    	} else {
        	if(file.listFiles().length == 0) {
        		if(file.delete()) {           		
        			deleteFile(p.parent());
            	}
    		}
    	}
    	return true;
    }
    
    public boolean isRegistered() {
    	return isRegistered;
    }
    
}
