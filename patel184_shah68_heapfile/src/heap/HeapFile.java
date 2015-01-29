package heap;

import java.io.IOException;
import chainexception.ChainException;
import global.Minibase;
import global.Page;
import global.PageId;
import global.RID;


public class HeapFile{
	private int pflag = 0;
	private HFPage hfpage;
	private HFPage newhfpage;
	private PageId firstpageid;
	private String firstPage;
	private int count;
	private PageId newpageid;
	private int length;
	private int recordCount = 0;
	
public HeapScan openScan() throws IOException, ChainException{
	try{
		HeapScan scan = new HeapScan(this);
		return scan;
	}catch(Exception e){
		System.out.println("EXCEPTION.");
		return null;
	}
}

public PageId getHFPage(){
	if(count == 0){
		System.out.println("hfpage");
		return hfpage.getCurPage();
	}else{
		System.out.println("newhfpage");
		return newhfpage.getCurPage();
	}
}

public PageId getFirstPageId(){
	return firstpageid;
}

/**
   * If the given name already denotes a file, this opens it; otherwise, this
   * creates a new empty file. A null name produces a temporary heap file which
   * requires no DB entry.
   */
public HeapFile(String name){
	firstPage = name;
	newpageid = new PageId();
	firstpageid = new PageId();
	count = 0;
	length = 0;
	if(pflag == 1){
		System.out.println("in HeapFile.");
	}
	if(Minibase.DiskManager.get_file_entry(name) == null){
		if(pflag == 1){
			System.out.println("in HeapFile - if");
		}
		Page page = new Page();
		PageId pageid = new PageId();
		hfpage = new HFPage();
		try{
			pageid = Minibase.BufferManager.newPage(page, 1);
			firstpageid = pageid;
			if(pflag == 1){
				System.out.println("newPage" + " pageid: " + pageid);
			}
		}catch(Exception e){
			if(pflag == 1){
				System.out.println("newPage - exception");
			}		
		}
		hfpage.setCurPage(pageid);
		try{
			Minibase.BufferManager.unpinPage(pageid, true);
			if(pflag == 1){
				System.out.println("unpin");
			}
		}catch(Exception e){
			if(pflag == 1){
				System.out.println("unpin - exception");
			}
		}
		try{
			Minibase.DiskManager.add_file_entry(name, pageid);
			if(pflag == 1){
				System.out.println("add_file_entry " + "name: " + name + " pageid: " + pageid);
			}
		}catch(Exception e){
			if(pflag == 1){
				System.out.println("add_file_entry - exception");
			}
		}
	}else{
		if(pflag == 1){
			System.out.println("in HeapFile - else - open: " + name);
		}		
		Minibase.DiskManager.openDB(name);
	}
}

 
/**
   * Deletes the heap file from the database, freeing all of its pages.
   */
  public void deleteFile() {
	  Page page = new Page();
	  try{
		  Minibase.BufferManager.pinPage(firstpageid, page, false);
	  }catch(Exception e){
		  System.out.println("Cannot pin in Deleted");
	  }
	  try{
		  Minibase.BufferManager.flushAllPages();
	  }catch(Exception e){
		  System.out.println("cannot flush in delete");
	  }
	  try{
		  Minibase.BufferManager.unpinPage(firstpageid, false);
	  }catch(Exception e){
		  System.out.println("Cannot unpin in delete");
	  }
	  
  }
 
  /**
   * Inserts a new record into the file and returns its RID.
   *
   * @throws IllegalArgumentException if the record is too large
   */
  public RID insertRecord(byte[] record) throws IllegalArgumentException{
	  if (record.length > HFPage.PAGE_SIZE - HFPage.HEADER_SIZE) {
		  throw new IllegalArgumentException("Record too large");
	  }
	  PageId pageid = new PageId();
	  RID rid = new RID();
	  Page page = new Page();
	  pageid = getAvailPage(record.length);
	  try{
		  if(pflag == 1){
				System.out.println("in insertRecord - hf.insertRecord - try: " + pageid);
			}
		  if(count == 0){
			  rid = hfpage.insertRecord(record);
			  recordCount++;
		  }else{
			  rid = newhfpage.insertRecord(record);
			  recordCount++;
		  }
	  }catch(Exception e){
		  if(pflag == 1){
				System.out.println("in insertRecord - hf.insertRecord - exception");
		  }
	  }
	byte[] bytearray = new byte[rid.getLength()];
	//bytearray = selectRecord(rid);
	//System.out.println("Bytearray: " + bytearray + " rid: " + rid + " record: " + record + " hfpage: " + hfpage.getCurPage());
	//System.out.println("RID: " + rid);
	if(count == 0){
		//System.out.println("Bytearray: " + bytearray + " rid: " + rid + " record: " + record + " hfpage: " + hfpage.getCurPage());
	}else{
		//System.out.println("Bytearray: " + bytearray + " rid: " + rid + " record: " + record + " newhfpage: " + newhfpage.getCurPage());
	}
	/*if(bytearray == record){
		System.out.println("YES!");
	}else{
		System.out.println("NO!");
	}*/
	if(pflag == 1){
		  int i=0;
		  for(i=0;i<record.length;i++){
			System.out.println("Record " + i + ": " + record[i] + " rid: " + rid);
		  }
	  }
	return rid;
  }
   
  /**
   * Reads a record from the file, given its id.
   *
   * @throws IllegalArgumentException if the rid is invalid
   */
  public byte[] selectRecord(RID rid){
	PageId pageid = new PageId();
	Page page = new Page();
	byte[] rarray = new byte[rid.getLength()];
	RID rid1 = new RID();
	RID rid2 = new RID();
	HFPage temp = new HFPage();
	//pageid = hfpage.getNextPage();
	//temp.setCurPage(pageid);
	rid1 = hfpage.firstRecord();
	rid2 = newhfpage.firstRecord();
	while(!(rid1.equals(rid))){
		if(hfpage.nextRecord(rid1) == null && count != 0){
			System.out.println("sR - while - if");
			pageid = hfpage.getNextPage();
			newhfpage.setCurPage(pageid);
			//rarray = newhfpage.selectRecord(rid);
			rid2 = newhfpage.nextRecord(rid2);
			rid1 = rid2;
			int i=0;
			  for(i=0;i<rarray.length;i++){
				System.out.println("selectRecord-hfpage " + i + ": " + rarray[i] + " RID: " + rid);
			}
		}
		//rarray = hfpage.selectRecord(rid);
		else{
			rid1 = hfpage.nextRecord(rid1);
		}
			//break;
	}
	System.out.println("RID: " + rid + " RID1: " + rid1);
	
	/*pageid = Minibase.DiskManager.get_file_entry(firstPage);
	try{
		Minibase.BufferManager.pinPage(pageid, page, false);
	}catch(Exception e){
		System.out.println("selectRecord - catch");
	}
	if(count == 0){
		rarray = hfpage.selectRecord(rid);
		int i=0;
		  for(i=0;i<rarray.length;i++){
			System.out.println("selectRecord-hfpage " + i + ": " + rarray[i] + " RID: " + rid);
		  }
		//System.out.println("Rarray: " + rarray + " rid: " + rid + " hfpage: " + hfpage.getCurPage());
	}else{
		rarray = newhfpage.selectRecord(rid);	
		int i=0;
		  for(i=0;i<rarray.length;i++){
			System.out.println("selectRecord-newhfpage " + i + ": " + rarray[i] + " RID: " + rid);
		  }
		//System.out.println("Rarray: " + rarray + " rid: " + rid + " newhfpage: " + newhfpage.getCurPage());
	}
	try{
		Minibase.BufferManager.unpinPage(pageid, false);
	}catch(Exception e){
		System.out.println("selectRecord - catch");
	}*/
	return rarray;
  }
 
/**
   * Updates the specified record in the heap file.
   *
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public boolean updateRecord(RID rid, byte[] newRecord){
	  /*PageId pageid = new PageId();
	  pageid = Minibase.DiskManager.get_file_entry(firstPage);
	  */
	  return true;
  }
 
  /**
   * Deletes the specified record from the heap file.
   *
   * @throws IllegalArgumentException if the rid is invalid
   */
  public boolean deleteRecord(RID rid){
	  
	  return true;
  }
 
/**
   * Gets the number of records in the file.
   */
  public int getRecCnt(){
	return recordCount;
  }
 
  /**
   * Searches the directory for a data page with enough free space to store a
   * record of the given size. If no suitable page is found, this creates a new
   * data page.
   */
  protected PageId getAvailPage(int reclen){
	int avlflag=0;
	PageId pageid = new PageId();
	hfpage.setType((short) 1);
	try{
		pageid = Minibase.DiskManager.get_file_entry(firstPage);
		if(pageid == null){
			if(avlflag == 1){
				System.out.println("in getAvailPage - pageid == null ");
			}	
		}else{
			PageId pid = new PageId();
			if(count == 0){
				length = hfpage.getFreeSpace();
			}else{
				length = newhfpage.getFreeSpace();
			}
			if(count == 0){
				newpageid = hfpage.getCurPage();
			}
			if(reclen < length){
				if(avlflag == 1){
					if(count == 0){
						System.out.println("in getAvailPage - try - else - if: " + hfpage.getFreeSpace() + " PAGEID: " + newpageid + " RECLeN: " + reclen + "Get type: " + hfpage.getType());
					}else{
						System.out.println("in getAvailPage - - else - if: " + newhfpage.getFreeSpace() + " PAGEID: " + newpageid + " RECLeN: " + reclen + "Get type: " + newhfpage.getType());
					}
				}
				return newpageid;
			}else{
				newhfpage = new HFPage();
				Page page = new Page();
				try{
					pid = Minibase.BufferManager.newPage(page, 1);
					if(avlflag == 1){
						System.out.println("getAvailPage - else - newPage" + " pageid: " + pid + " freeSpace: " + newhfpage.getFreeSpace());
					}
					newhfpage.setCurPage(pid);
				}catch(Exception e){
					if(avlflag == 1){
						System.out.println("newPage - exception");
					}		
				}
				try{
					Minibase.BufferManager.unpinPage(pid, true);
					if(avlflag == 1){
						System.out.println("unpin");
					}
				}catch(Exception e){
					if(avlflag == 1){
						System.out.println("unpin - exception");
					}
				}
				try{
					Minibase.DiskManager.add_file_entry(firstPage, pageid);
					if(avlflag == 1){
						System.out.println("add_file_entry " + "name: " + firstPage + " pageid: " + pageid);
					}
				}catch(Exception e){
					if(avlflag == 1){
						System.out.println("add_file_entry - exception");
					}
				}
				if(count == 0){
					hfpage.setNextPage(pid);
					newhfpage.setPrevPage(newpageid);
					newpageid = newhfpage.getCurPage();
					//System.out.println("hfpage.pid " + pid);
				}else{
					HFPage temp = new HFPage();
					temp.setCurPage(newpageid);
					temp.setNextPage(pid);
					newhfpage.setPrevPage(newpageid);
					newpageid = newhfpage.getCurPage();	
				}
				//System.out.println("newpageid after curpage " + newpageid);
				newhfpage.setType((short) 1);
				count = 1;
				//hfpage.print();
				//newhfpage.print();
			} 
		}
		if(avlflag == 1){
			System.out.println("in getAvailPage - try: " + newhfpage.getCurPage());
		}
		return newhfpage.getCurPage();
	}catch(Exception e){
		if(avlflag == 1){
			System.out.println("in getAvailPage - catch");
		}
		return null;
	}
  }
}