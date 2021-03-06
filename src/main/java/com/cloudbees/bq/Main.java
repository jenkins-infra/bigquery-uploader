package com.cloudbees.bq;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static com.cloudbees.bq.BigQueryConfig.WRITE_APPEND;

/**
 * @author Vivek Pandey
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    @Option(name="-projectId",usage="BigQuery projectId (Required)")
    public String projectId;

    @Option(name="-datasetId",usage="BigQuery datasetId (Required)")
    public String datasetId;

    @Option(name="-tableId",usage="BigQuery tableId (Required)")
    public String tableId;

    @Option(name="-bqFile",usage="BigQuery record file (JSON object separated by new line)")
    public File bqFile;


    @Option(name="-credentialFile",usage="BigQuery JSON credential file")
    public File credentialFile;

    @Option(name="-schemaFile",usage="Create BigQuery table using provided schama", metaVar = "SCHEMA_JSON_FILE")
    public File schemaFile;

    @Option(name="-templateSuffix",usage="Template suffix to be used with this upload (streaming upload only)")
    public String templateSuffix;

    @Option(name="-uploadType",usage="Upload type, must be census or extension")
    public String uploadType;


    @Option(name="-insertIdField",usage="Top level JSON field to use for insertId (streaming upload only)")
    public String insertIdField;

    @Option(name="-writeDisposition",usage="How the table data should be updated, possible values WRITE_TRUNCATE, WRITE_EMPTY and WRITE_APPEND(default)")
    public String writeDisposition=WRITE_APPEND;

    @Option(name="-createTable",usage="Create new table using the given -tableId and -schemaFile")
    public Boolean createTable=false;


    @Option(name="-streamingUpload",usage="Create new table using streaming upload")
    public Boolean streamingUpload=false;

    @Option(name="-pollingInterval", usage="Submitted job polling interval(in seconds)")
    public int pollingInterval=1;

    @Option(name = "-h", aliases = {"-help"}, usage = "Print help message", help = true)

    public boolean help;
    public static void main(String[] args) throws CmdLineException {
        Main main = new Main();
        CmdLineParser p = new CmdLineParser(main);
        p.parseArgument(args);
        if(main.help){
            p.printUsage(System.err);
        }else if(main.validate(p)){
            main.run();
        }
    }

    private boolean validate(CmdLineParser p){
        if(projectId == null){
            System.err.println("Please provide Google BigQuery projectId with -projectId option");
            p.printUsage(System.err);
            return false;
        }
        if(datasetId == null){
            System.err.println("Please provide Google BigQuery datasetId with -datasetId option");
            p.printUsage(System.err);
            return false;
        }
        if(tableId == null){
            System.err.println("Please provide Google BigQuery tableId with -tableId option");
            p.printUsage(System.err);
            return false;
        }
        if (bqFile==null && schemaFile ==null) {
            System.err.println("Nothing to do. must provide -bqFile option");
            p.printUsage(System.err);
            return false;
        }
        if(uploadType == null || !uploadType.equals("census") && !uploadType.equals("extension")){
            System.err.println("Please provide uploadType, must be one of census or extension");
            p.printUsage(System.err);
            return false;
        }
        return true;

    }
    private void run() {
        BigQueryConfig config = new BigQueryConfig
                .Builder(projectId, datasetId, tableId, credentialFile)
                .templateSuffix(templateSuffix)
                .insertIdField(insertIdField)
                .schema(schemaFile)
                .createTable(createTable)
                .streamingUpload(streamingUpload)
                .pollingIntervalInSec(pollingInterval)
                .writeDisposition(writeDisposition)
                .uploadType(uploadType)
                .build();
        run(config);
    }

    public void run(BigQueryConfig config){
        long start = System.currentTimeMillis();
        try {
            BigQueryApi app = new BigQueryApi(config);

            //create new schemaFile
            if (config.isCreateTable()) {
                if (config.getSchema() == null) {
                    System.err.println("-schemaFile required with -createTable option");
                    return;
                }
                app.createTable(tableId, config.getSchema());
            }

            if (bqFile != null) {
                app.upload(bqFile);
            }
        }catch (Exception e){

        }finally {
            long timeTaken = (System.currentTimeMillis()-start);
            LOGGER.info(String.format("Total time taken: %02d min, %02d sec",
                    TimeUnit.MILLISECONDS.toMinutes(timeTaken),
                    TimeUnit.MILLISECONDS.toSeconds(timeTaken) -
                            TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(timeTaken))));
        }
    }

}
