package randomworkers.jfr;

import jdk.jfr.*;

@Label("Random Workers Application Message Serialized")
@Category("UIGC")
@StackTrace(false)
public class AppMsgSerialization extends Event {

    @Label("Size")
    @DataAmount
    public long size;

}
