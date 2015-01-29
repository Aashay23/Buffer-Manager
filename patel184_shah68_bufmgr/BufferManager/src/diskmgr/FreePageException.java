package diskmgr;
import chainexception.*;

public class FreePageException extends ChainException {
	public FreePageException(Exception e, String name)
    { 
      super(e, name); 
    }
}