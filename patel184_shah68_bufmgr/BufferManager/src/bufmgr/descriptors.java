package bufmgr;

import global.PageId;

public class descriptors {
        private int pin_count;
        private PageId pageNumber;
        private boolean dirtyBit;

        public descriptors() {
                this.pin_count = 0;
                this.pageNumber = null;
                this.dirtyBit = false;

        }

        public descriptors(int pin_count, PageId pageNumber, boolean dirtyBit) {
                this.pin_count = pin_count;
                this.pageNumber = pageNumber;
                this.dirtyBit = dirtyBit;
        }

        public void setPin_count(int pin_count) {
                this.pin_count = pin_count;
        }

        public int getPin_count() {
                return pin_count;
        }
        
        public PageId getPageNumber(){
        	return pageNumber;
        }
        
        public int incPin(){
        	return pin_count++;
        }
        
        public int decPin(){
        	return pin_count--;
        }
        
        public void setDirtyBit(boolean dirtyBit){
        	this.dirtyBit = dirtyBit;
        }
        
        public boolean isDirtyBit() {
            return dirtyBit;
        }

}