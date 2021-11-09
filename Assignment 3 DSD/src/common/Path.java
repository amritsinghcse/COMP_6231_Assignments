package common;

import java.io.*;
import java.util.*;

/** Distributed filesystem paths.

    <p>
    Objects of type <code>Path</code> are used by all filesystem interfaces.
    Path objects are immutable.

    <p>
    The string representation of paths is a forward-slash-delimeted sequence of
    path components. The root directory is represented as a single forward
    slash.

    <p>
    The colon (<code>:</code>) and forward slash (<code>/</code>) characters are
    not permitted within path components. The forward slash is the delimeter,
    and the colon is reserved as a delimeter for application use.
 */
public class Path implements Iterable<String>, Serializable
{
	
	private String pathVar = "";
	private final String ROOT = "/";
	
    /** Creates a new path which represents the root directory. */
    public Path()
    {
    	pathVar = ROOT;
    }

    /** Creates a new path by appending the given component to an existing path.

        @param path The existing path.
        @param component The new component.
        @throws IllegalArgumentException If <code>component</code> includes the
                                         separator, a colon, or
                                         <code>component</code> is the empty
                                         string.
    */
    public Path(Path path, String component)
    {
        if(component.isEmpty()) {
        	throw new IllegalArgumentException("Component is empty. Please pass valid component.");
        } 
        if(component.contains("/")) {
        	throw new IllegalArgumentException("Invalid character '/'. Please pass valid component.");
        } 
        if(component.contains(":")) {
        	throw new IllegalArgumentException("Invalid character ':'. Please pass valid component.");
        }
        
        if(path.pathVar.equals(ROOT)) {
        	pathVar = ROOT + component;
        } else {
        	pathVar = path.pathVar + "/" + component;
        }

        
    }

    /** Creates a new path from a path string.

        <p>
        The string is a sequence of components delimited with forward slashes.
        Empty components are dropped. The string must begin with a forward
        slash.

        @param path The path string.
        @throws IllegalArgumentException If the path string does not begin with
                                         a forward slash, or if the path
                                         contains a colon character.
     */
    public Path(String path)
    {
        if(!path.startsWith("/")) {
        	throw new IllegalArgumentException("Path should begin with '/'. Please enter valid path.");
        } 

        if (path.contains(":")) {
        	throw new IllegalArgumentException("Invalid character ':'. Please enter valid path.");
        }
        
        
        if(!path.isEmpty()) {
        	String[] components = path.split("/");
        	if(components.length > 0) {
        	for(String comp: components) {
        		if(!comp.trim().isEmpty()) {
        			pathVar += "/" + comp;
        		}
        	}
        	} else {
        		pathVar = ROOT;
        	}
        } else {
        	pathVar = ROOT;
        }
        
    }

    /** Returns an iterator over the components of the path.

        <p>
        The iterator cannot be used to modify the path object - the
        <code>remove</code> method is not supported.

        @return The iterator.
     */
    @Override
    public Iterator<String> iterator()
    {
    	String[] components =  pathVar.split("/");
    	
    	List<String> componentsList = new ArrayList<>();
    	for(int i = 0; i < components.length; i++) {
    		if(!components[i].isEmpty()) {
    			componentsList.add(components[i]);
    		}
    	}

        Iterator<String> iterator = new Iterator<String>() {
			private int i = 0;
			@Override
			public String next() {
				// TODO Auto-generated method stub
				if(i < componentsList.size() ) {
					return componentsList.get(i++);

				} else {
					throw new NoSuchElementException();
				}
			}
			
			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return componentsList.size() > i;
			}

		/*	@Override
			public void remove() {
				// TODO Auto-generated method stub
				throw new UnsupportedOperationException("Remove operation is not supported.");
			}*/
			
			
		};
		
		return iterator;
    }

    /** Lists the paths of all files in a directory tree on the local
        filesystem.

        @param directory The root directory of the directory tree.
        @return An array of relative paths, one for each file in the directory
                tree.
        @throws FileNotFoundException If the root directory does not exist.
        @throws IllegalArgumentException If <code>directory</code> exists but
                                         does not refer to a directory.
     */
    public static Path[] list(File directory) throws FileNotFoundException
    {
        if(!directory.exists()) {
        	throw new FileNotFoundException("Root directory does not exist.");
        } 
        if(!directory.isDirectory()) {
        	throw new IllegalArgumentException("File is not a directory.");
        }

        List<Path> listOfPaths = getRelativePaths(directory, directory);
        return (Path[]) listOfPaths.toArray(new Path[listOfPaths.size()]);
        
    }
    
    public static List<Path> getRelativePaths(File root, File directory) {
    	List<Path> pathList= new ArrayList<>();
    	
        File[] files = directory.listFiles();

    	for (File f: files) {
        	if(f.isDirectory()) {
        		//call a recursive method here
        		pathList.addAll(getRelativePaths(root, f));
        	} else {
        		String path = f.getAbsolutePath().substring(root.getAbsolutePath().length());
        		path = path.replaceAll("\\\\", "/");
        		Path p = new Path(path);
        		pathList.add(p);
        	}
        }
    	
    	return pathList;
    }
    
   

    /** Determines whether the path represents the root directory.

        @return <code>true</code> if the path does represent the root directory,
                and <code>false</code> if it does not.
     */
    public boolean isRoot()
    {
        return pathVar.equals(ROOT);
    }

    /** Returns the path to the parent of this path.

        @throws IllegalArgumentException If the path represents the root
                                      
                                         directory, and therefore has no parent.
     */
    public Path parent()
    {
        if(pathVar.equals(ROOT)) {
        	throw new IllegalArgumentException("Path has no parent.");
        }
        
        String[] components= pathVar.split("/");
        StringBuilder parentPath = new StringBuilder();
        for(int i = 0; i < components.length - 1; i++) {
        	parentPath.append("/").append(components[i]);
        }
        return new Path(parentPath.toString());
    }

    /** Returns the last component in the path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no last
                                         component.
     */
    public String last()
    {
    	  if(pathVar.equals("/")) {
          	throw new IllegalArgumentException("Path has no last component.");
          }
    	  
    	  return pathVar.split("/")[pathVar.split("/").length - 1];
    }

    /** Determines if the given path is a subpath of this path.

        <p>
        The other path is a subpath of this path if is a prefix of this path.
        Note that by this definition, each path is a subpath of itself.

        @param other The path to be tested.
        @return <code>true</code> If and only if the other path is a subpath of
                this path.
     */
    public boolean isSubpath(Path other)
    {
        return pathVar.startsWith(other.pathVar);
    }

    /** Converts the path to <code>File</code> object.

        @param root The resulting <code>File</code> object is created relative
                    to this directory.
        @return The <code>File</code> object.
     */
    public File toFile(File root)
    {
        String path = root.getAbsolutePath() + "/" + pathVar;
        return new File(path);
    }

    /** Compares two paths for equality.

        <p>
        Two paths are equal if they share all the same components.

        @param other The other path.
        @return <code>true</code> if and only if the two paths are equal.
     */
    @Override
    public boolean equals(Object other)
    {
        return pathVar.equals(((Path)other).pathVar);
    }

    /** Returns the hash code of the path. */
    @Override
    public int hashCode()
    {
        return pathVar.hashCode();
    }

    /** Converts the path to a string.

        <p>
        The string may later be used as an argument to the
        <code>Path(String)</code> constructor.

        @return The string representation of the path.
     */
    @Override
    public String toString()
    {
       return pathVar;
    }
}
