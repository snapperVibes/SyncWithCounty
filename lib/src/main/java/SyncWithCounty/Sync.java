package SyncWithCounty;
import jep.SubInterpreter;

public class Sync implements AutoCloseable {

    private final SubInterpreter py = new SubInterpreter();


    Sync(){
        py.runScript("/home/qrs/code/JavaStuff/SyncWithCounty/lib/src/main/python/__init__.py");
    }


    public Object run() {
        return py.getValue("sync()");
    }

    @Override
    public void close() {
        py.close();
    }
}
