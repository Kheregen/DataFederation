package interfaces;


import java.io.File;

/**
 *
 * @author Jan Kasztura
 */
public interface DataFederationTool {

    public void federate(File csvFile, int databaseColumn, int csvColumn, ResultCollector resultCollector)throws Exception;
}
