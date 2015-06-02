package smallworld.util;

import java.util.HashSet;
import java.util.Set;

public class Pair<A, B> {
    private A first;
    private B second;

    public Pair(A first, B second) {
    	super();
    	this.first = first;
    	this.second = second;
    }

    public int hashCode() {
    	int hashFirst = first != null ? first.hashCode() : 0;
    	int hashSecond = second != null ? second.hashCode() : 0;

    	return (hashFirst + hashSecond) * hashSecond + hashFirst;
    }

    public boolean equals(Object other) {
    	if (other instanceof Pair) {
    		@SuppressWarnings("rawtypes")
			Pair otherPair = (Pair) other;
    		return 
    		((  this.first == otherPair.first ||
    			( this.first != null && otherPair.first != null &&
    			  this.first.equals(otherPair.first))) &&
    		 (	this.second == otherPair.second ||
    			( this.second != null && otherPair.second != null &&
    			  this.second.equals(otherPair.second))) );
    	}

    	return false;
    }

    public String toString()
    { 
           return "(" + first + ", " + second + ")"; 
    }

    public A getFirst() {
    	return first;
    }

    public void setFirst(A first) {
    	this.first = first;
    }

    public B getSecond() {
    	return second;
    }

    public void setSecond(B second) {
    	this.second = second;
    }
    
    public static void main(String[] args) {
    	Set<Pair<Long, Long>> pairs = new HashSet<Pair<Long, Long>>();
		
    	pairs.add(new Pair<Long, Long>(new Long(1), new Long(1)));
    	pairs.add(new Pair<Long, Long>(new Long(1), new Long(2)));
    	pairs.add(new Pair<Long, Long>(new Long(3), new Long(4)));
    	
    	System.out.println(pairs.contains(new Pair<Long, Long>(new Long(1), new Long(1))));
    	System.out.println(pairs.contains(new Pair<Long, Long>(new Long(3), new Long(4))));
    	System.out.println(pairs.contains(new Pair<Long, Long>(new Long(1), new Long(4))));
    }
}