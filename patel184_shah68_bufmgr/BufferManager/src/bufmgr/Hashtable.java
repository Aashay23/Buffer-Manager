package bufmgr;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;


public class Hashtable{

	static private class lNode{
		Object key;
		Object value;
		lNode next;
	}
	private lNode[] store;
	private int size = 1033;
	private int max = size-1;
	Random rand = new Random();
	private int a = rand.nextInt(max)+1;
	private int b = rand.nextInt(max-1)+1;

	public Hashtable(){
		store = new lNode[size];

	}
	private lNode add(Object key, Object value){
		lNode temp = new lNode();
		temp.key = key;
		temp.value = value;
		return temp;
	}

	private int hash(Object key){
		int hashKey = (Math.abs((a*key.hashCode()+b)%size));
		return hashKey;
	}
	private void hashprint(){
		for(int i = 0; i < size; i++){
			lNode current = store[i];
			while(current != null){
				System.out.print("  (" + current.key + "," + current.value + ")");
				current = current.next;
			}
		}
	}
	
	private boolean search(int bucket, Object key){
		lNode current = store[bucket];
		while(current != null){
			if(current.key.hashCode() == key.hashCode()){
		//		System.out.println("in here bro\n");
				return false;
			}
			current = current.next;
		}
		return true;
	}
	public void put(Object key, Object value){
		int bucket = hash(key);
		if(search(bucket, key)){
			store[bucket] = add(key, value);
		}else{
			System.out.println("same key");
		}
		System.out.println("bucket" + bucket);

	}
	public boolean contains(Object key){
		int bucket = hash(key);
		lNode current = store[bucket];
		while(current != null){
			if(current.key.hashCode() == key.hashCode()){
				return true;
			}
			current = current.next;
		}
		return false;
	}
	public Object get(Object key){
		int bucket = hash(key);
		lNode current = store[bucket];
		while(current != null){
			if(current.key.hashCode() == key.hashCode()){
				//System.out.println("in here bro\n");
				
				return current.value;
			}
			current = current.next;
		}
		return null;		
	}
	
	public void remove(Object key){
		int bucket = hash(key);
		System.out.println("bucket " + bucket);
		//lNode current = store[bucket];
		//System.out.println("\n store " + store[bucket].key);
		if(store[bucket] == null){
			System.out.println("its empty");
			return;
		}
		if(!search(bucket,key)){
			store[bucket] = store[bucket].next;
			return;
		}else{
			System.out.println("key not found");
		}

	}

	public static void main(String[] args) {
		Hashtable h = new Hashtable();
		h.put(10, 40);
		h.put(13, 50);
		h.put(50,100);
		h.put(50,100);
		System.out.println(h.get(10));
		System.out.println(h.get(13));
		System.out.println(h.get(50));
		h.remove(13);
		h.remove(15);
		h.hashprint();

	}


}