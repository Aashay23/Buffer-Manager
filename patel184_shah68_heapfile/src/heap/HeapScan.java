package heap;

import global.Minibase;
import global.Page;
import global.PageId;
import global.RID;

public class HeapScan{
	private Page page;
	private PageId pageid;
	private RID rid;
	private HFPage hfpage;
	private PageId firstpageid;
	private int flag;
/**

* Constructs a file scan by pinning the directoy header page and initializing

* iterator fields.

*/

protected HeapScan(HeapFile hf){
	rid = new RID();
	hfpage = new HFPage();
	page = new Page();
	flag = 0;
	firstpageid = new PageId();
	firstpageid = hf.getFirstPageId();
	try{
		Minibase.BufferManager.pinPage(hf.getFirstPageId(), page, false);
	}catch(Exception e){
		if(flag == 1){
			System.out.println("HeapScan - pinpage - exception");
	
		}
	}
}


protected void finalize() throws Throwable{
	
}

/**

* Closes the file scan, releasing any pinned pages.

*/
public void close(){
	try{
		Minibase.BufferManager.unpinPage(firstpageid, true);
	}catch(Exception e){
		if(flag == 1){
			System.out.println("HeapScan - unpinpage - exception");
	
		}
	}
}


/**

* Returns true if there are more records to scan, false otherwise.

*/
public boolean hasNext(){
	rid = hfpage.firstRecord();
	boolean has = false;
	try{
		has = hfpage.hasNext(rid);
	}catch(Exception e){
		System.out.println("HasNext - catch");
	}
	if(has){
		//rid = hfpage.nextRecord(rid);
		return true;
	}else{
		return false;
	}
	
}

 

/**

* Gets the next record in the file scan.

* @param rid output parameter that identifies the returned record

* @throws IllegalStateException if the scan has no more elements

*/
public Tuple getNext(RID rid){
	byte[] arr = new byte[rid.getLength()];
	try{
		rid = hfpage.nextRecord(rid);
	}catch(Exception e){
		flag = 1;
		System.out.println("NEXT RECORD: " + rid);
	}
	if(flag == 1){
		return null;
	}else{
		try{
			arr = hfpage.selectRecord(rid);
		}catch(Exception e){
			System.out.println("Cannot select a record.");
			return null;
		}
		Tuple tuple = new Tuple(arr);
		return tuple;
	}
}
}
