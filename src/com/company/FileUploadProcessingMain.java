package com.company;

import lombok.Setter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.AbstractConfiguration;

import com.company.FileDataHelper;

public class FileUploadProcessingMain {
    private static Logger LOG;

    static {
        initLogging();
    }

    @Setter
    private static FileDataHelper filesHelper = new FileDataHelper();

    private static void initLogging() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        AbstractConfiguration config = (AbstractConfiguration) ctx.getConfiguration();
        config.getRootLogger().start();
        ctx.updateLoggers();

        LOG = LogManager.getLogger(FileUploadProcessingMain.class);
    }

    public static void main(String[] args) throws Exception {
//        ProcessCommand cmd = ProcessCli.parse(args);
//        if (cmd == null) {
//            ProcessCli.printHelp();
//            System.exit(1);
//        }
        //FilesRecord record = filesHelper.retrieveFileRecord(cmd.getFileId());
        LOG.info("Got record from database: " );
        FileUploadProcessor.processFile(record, cmd.getProcessState());
        System.exit(0);
    }




}
