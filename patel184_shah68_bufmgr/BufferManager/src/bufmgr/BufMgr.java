package bufmgr;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import chainexception.ChainException;

import diskmgr.BufferPoolExceededException;
import diskmgr.DiskMgr;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.FreePageException;
import diskmgr.InvalidPageNumberException;
import diskmgr.PagePinnedException;
import diskmgr.PageUnpinnedException;

import global.Minibase;
import global.Page;
import global.PageId;

public class BufMgr {
	private Page[] bufPool;
    private descriptors[] bufDescr;
    private Hashtable htable;
    private Queue<Integer> queue;
    private int storeBufs;
    private int countPages;
    private DiskMgr dm;
    private int cflag = 0;
/**
 * Create the BufMgr object.
 * Allocate pages (frames) for the buffer pool in main memory and
 * make the buffer manage aware that the replacement policy is
 * specified by replacerArg (i.e. LH, Clock, LRU, MRU etc.).
 *
 * @param numbufs number of buffers in the buffer pool
 * @param prefetchSize number of pages to be prefetched
 * @param replacementPolicy Name of the replacement policy
 */
public BufMgr(int numbufs, int prefetchSize, String replacementPolicy) {
	storeBufs = numbufs;
 	bufPool = new Page[numbufs];
 	bufDescr = new descriptors[numbufs];
 	htable = new Hashtable();
 	if(replacementPolicy.equals("Clock")){
 		System.out.println("Clock policy");
 		queue = new LinkedList<Integer>();
        for (int i = 0; i < numbufs; i++){
            queue.add(i);
        }
 		cflag = 1;
 	}else{
 		System.out.println("another policy");
 	}
};


/** 
* Pin a page.
* First check if this page is already in the buffer pool.
* If it is, increment the pin_count and retur a pointer to this 
* page.n
* If the pin_count was 0 before the call, the page was a 
* replacement candidate, but is no longer a candidate.
* If the page is not in the pool, choose a frame (from the 
* set of replacement candidates) to hold this page, read the 
* page (using the appropriate method from {\em diskmgr} package) and pin it.
* Also, must write out the old page in chosen frame if it is dirty 
* before reading new page.__ (You can assume that emptyPage==false for
* this assignment.)
*
* @param pageno page number in the Minibase.
* @param page the pointer point to the page.
* @param emptyPage true (empty page); false (non-empty page)
 * @throws DiskMgrException 
 * @throws IOException 
 * @throws FileIOException 
 * @throws InvalidPageNumberException 
 * @throws PageUnpinnedException 
*/
 public void pinPage(PageId pageno, Page page, boolean emptyPage) throws DiskMgrException, InvalidPageNumberException, FileIOException, IOException, PageUnpinnedException {
	 System.out.println("in the pinpage");
	// System.out.println("Index: " + (Integer) htable.get(pageno));
	 if(htable.contains(pageno)){
		 System.out.println("hashtable contains in pin");
		 int index = (Integer) htable.get(pageno);
		 int pin_count = bufDescr[index].getPin_count();
		 if(pin_count == 0){
			 //System.out.println("in pin pin count");
			 queue.remove(pageno.pid);
		 }
		 //System.out.println("outsied if");
		 pin_count++;
		 bufDescr[index].setPin_count(pin_count);
		 System.out.println("set the pin count: " + pin_count);
	 }else{
		 System.out.println("In else!");
		 	/*int index = (Integer) htable.get(pageno.pid);
		 	if(bufDescr[index] != null && bufDescr[index].isDirtyBit()){
		 		System.out.println("flushPage");
		 		//flushPage();
		 	}
		 	try{
		 		htable.remove(bufDescr[index].getPageNumber());
		 	}catch(Exception e){
		 		System.out.println("IN CATCH");
		 	}
		 	try{
		 		Minibase.DiskManager.read_page(pageno, page);
		 	}catch(Exception e){
		 		System.out.println("BLAH");
		 	}
		 	htable.put(pageno, index);
		 	bufDescr[index].setDirtyBit(false);
		 	bufDescr[index].setPin_count(0);
		 	bufPool[index] = page;*/
		 
			try{
				Minibase.DiskManager.read_page(pageno, page);
			}catch(Exception e){
				throw new DiskMgrException(e, "can't read");
			}
			int index = -1;
			try{
				index = queue.poll();
			}catch(Exception e){
				throw new PageUnpinnedException(null, "page cant pin");
			}
			bufPool[index] = page;
			bufDescr[index] = new descriptors(0, pageno, false);
			htable.put(pageno.pid, index);
			countPages++;
	 }
 };
 
/**
* Unpin a page specified by a pageId.
* This method should be called with dirty==true if the client has
* modified the page.
* If so, this call should set the dirty bit 
* for this frame.
* Further, if pin_count>0, this method should 
* decrement it. 
*If pin_count=0 before this call, throw an exception
* to report error.
*(For testing purposes, we ask you to throw
* an exception named PageUnpinnedException in case of error.)
*
* @param pageno page number in the Minibase.
* @param dirty the dirty bit of the frame
 * @throws PageUnpinnedException 
*/
public void unpinPage(PageId pageno, boolean dirty) throws PageUnpinnedException {
	System.out.println("in unpin");
	//System.out.println("pageno " + pageno.pid + "dirty " + dirty);
	if(htable.contains(pageno.pid)){
		//System.out.println("in here");
		int index = (Integer)htable.get(pageno);
		//System.out.println("index in unpin " + index);
		int pc = bufDescr[index].getPin_count();
		//System.out.println("pin_count in unpin " + bufDescr[index]);
		if(pc == 0){
	//		throw new PageUnpinnedException(null, "Page can't be unPinned");
		}
		if(dirty == true){
			//System.out.println("its dirty in unpin");
			bufDescr[index].setDirtyBit(dirty);
		}
		//System.out.println("bufDescr in unpin " + bufDescr[index]);
		if(pc > 0){
			pc--;
			if(pc == 0){
				queue.add(pageno.pid);
			}
		}
		//System.out.println("in pin count");

		bufDescr[index].setPin_count(pc);
	}else{
		System.out.println("hi");
		//throw new PageUnpinnedException(null, "page can't be unpinned in else");
	}
};
 
/** 
* Allocate new pages.
* Call DB object to allocate a run of new pages and 
* find a frame in the buffer pool for the first page
* and pin it. (This call allows a client of the Buffer Manager
* to allocate pages on disk.) If buffer is full, i.e., you 
* can't find a frame for the first page, ask DB to deallocate 
* all these pages, and return null.
*
* @param firstpage the address of the first page.
* @param howmany total number of allocated new pages.
*
* @return the first page id of the new pages.__ null, if error.
 * @throws IOException 
 * @throws PageUnpinnedException 
 * @throws FileIOException 
 * @throws InvalidPageNumberException 
 * @throws DiskMgrException 
*/
public PageId newPage(Page firstpage, int howmany) throws DiskMgrException, InvalidPageNumberException, FileIOException, PageUnpinnedException, IOException {
	System.out.println("in newPage"); 
	PageId id = new PageId();
	 try{
	  //System.out.println("PageId: " + id + " firstpage: " + firstpage + " howmany: " + howmany);
	  Minibase.DiskManager.allocate_page(id, howmany);
	 }catch(Exception e){
	  System.out.println("Allocate exception.");
	 }
	 if(countPages == storeBufs){
	  try {
	   Minibase.DiskManager.deallocate_page(id);
	  } catch (Exception e) {
	   System.out.println("De-allocate exception.");
	  }
	  return null;
	 }else{
		 int index = queue.poll();
		 //System.out.println("index " + bufPool[index]);
		 bufPool[index] = firstpage; 
		 pinPage(id, firstpage, false);
	 }
	 countPages++;
	 return id;
}; 
/**
* This method should be called to delete a page that is on disk.
* This routine must call the method in diskmgr package to 
* deallocate the page. 
*
* @param globalPageId the page number in the data base.
 * @throws FreePageException 
*/
public void freePage(PageId globalPageId) throws FreePageException {
	if(htable.get(globalPageId.pid) == null){
		try {
			Minibase.DiskManager.deallocate_page(new PageId(globalPageId.pid));
		}catch (ChainException e) {
			e.printStackTrace();
		}
	}else{
		try{
			int free = (Integer) htable.get(globalPageId);
			if(bufDescr[free].getPin_count() > 0){
				throw new PagePinnedException(null,"freePage failed.");
			}
			if(bufDescr[free].getPin_count() != 0){
				unpinPage(bufDescr[free].getPageNumber(), bufDescr[free].isDirtyBit());
			}
			if(bufDescr[free].isDirtyBit()){
				try{
					flushPage(globalPageId);
				}catch(Exception e){
					System.out.println("catch-flushPage in freePage");
				}
			}
			htable.remove(globalPageId.pid);
			bufPool[free] = null;
			bufDescr[free] = null;
			countPages--;
		}catch(Exception e){
			throw new FreePageException(null, "FreePage Exception.");
		}
	}
};
 
/**
* Used to flush a particular page of the buffer pool to disk.
* This method calls the write_page method of the diskmgr package.
*
* @param pageid the page number in the database.
 * @throws DiskMgrException 
*/
public void flushPage(PageId pageid) throws DiskMgrException {
	Page temp = new Page();
	temp = null;
	int id;
	id = (Integer) htable.get(pageid);
	if(bufPool[id] != null){
		temp = new Page(bufPool[id].getpage().clone());
	}
	try{
		if(temp != null){
			Minibase.DiskManager.write_page(pageid, temp);
			if(bufDescr[id] != null){
				bufDescr[id].setDirtyBit(false);
			}
		}else{
			throw new FileIOException(null,"Page not flushed.");
		}
	}catch(Exception e){
		throw new DiskMgrException(e, "flushPage failed.");
	}
};
 
/**
* Used to flush all dirty pages in the buffer poll to disk
 * @throws DiskMgrException 
*
*/
public void flushAllPages() throws DiskMgrException {
	int i=0;
	while(i < storeBufs){
		flushPage(bufDescr[i].getPageNumber());
	}
};
 
/**
* Gets the total number of buffer frames.
*/
public int getNumBuffers() {
	return storeBufs;
}
 
/**
* Gets the total number of unpinned buffer frames.
*/
public int getNumUnpinned() {
	System.out.println("countPages " + countPages);
	return 0;
//	System.out.println("queue size " + queue.size());
	//return 0;
	}
 
};