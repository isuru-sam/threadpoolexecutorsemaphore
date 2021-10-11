package cms.samples;

private void parseBulkXmlFile(String path, String bccode, String runid, AtomicLong skippedCount, AtomicLong successCount, ConcurrentMap<String, String> bcCodeMap, int limit) throws Exception {
    XMLInputFactory xif = XMLInputFactory.newInstance();
    XMLStreamReader xsr = xif.createXMLStreamReader(new FileReader(path));
    TransformerFactory tf = TransformerFactory.newInstance();
    Transformer t = tf.newTransformer();
    long start = System.currentTimeMillis();
    MmbeThreadPoolExecutor threadPoolExecutor = (MmbeThreadPoolExecutor) applicationContext.getBean("mmbeThreadPoolExecutor");
    int count = 0;
    int i = 0;
    List<String> xmlStringList = new ArrayList<>();
    List<String> greenLightXmlList = new ArrayList<>();
    while (xsr.hasNext()) {
        int event = xsr.next();
        if (event == XMLStreamConstants.START_ELEMENT) {
            if (xsr.getLocalName().equals("T")) {
                StringWriter sw = new StringWriter();
                count++;
                t.transform(new StAXSource(xsr), new StreamResult(sw));
                if (limit != 50) {Cms
                    xmlStringList.add(sw.toString());

                    if (count == batchSize) {


                        MmbeXmlProcessor mmbeXmlProcessorTask = (MmbeXmlProcessor) applicationContext.getBean("mmbeXmlProcessor");
                        mmbeXmlProcessorTask.setAccountXmls(xmlStringList);
                        mmbeXmlProcessorTask.setBccode(bccode);
                        mmbeXmlProcessorTask.setRunId(runid);
                        mmbeXmlProcessorTask.setExpectedMerchantId(merchantId);

                        mmbeXmlProcessorTask.setSuccessCount(successCount);

                        mmbeXmlProcessorTask.setBcCodeList(bcCodeMap);

                        mmbeXmlProcessorTask.setLimit(limit);
                        threadPoolExecutor.submit(mmbeXmlProcessorTask);
                        logger.info("Batch" + (++i) + "submitted to threadpool");
                        count = 0;
                        xmlStringList = new ArrayList<>();
                    }
                } else {
                    greenLightXmlList.add(sw.toString());
                }

            }
        } else if (event == XMLStreamConstants.END_DOCUMENT) {
            if (limit != 50) {
                if (xmlStringList.size() != 0) {
                    MmbeXmlProcessor mmbeXmlProcessorTask = (MmbeXmlProcessor) applicationContext.getBean("mmbeXmlProcessor");

                    mmbeXmlProcessorTask.setAccountXmls(xmlStringList);
                    mmbeXmlProcessorTask.setBccode(bccode);
                    mmbeXmlProcessorTask.setRunId(runid);
                    mmbeXmlProcessorTask.setExpectedMerchantId(merchantId);
                    mmbeXmlProcessorTask.setSuccessCount(successCount);
                    mmbeXmlProcessorTask.setBcCodeList(bcCodeMap);
                    mmbeXmlProcessorTask.setLimit(limit);
                    threadPoolExecutor.submit(mmbeXmlProcessorTask);
                    logger.info("Batch" + (++i) + "submitted to threadpool");
                }
            } else {
                MmbeXmlProcessor mmbeXmlProcessorTask = (MmbeXmlProcessor) applicationContext.getBean("mmbeXmlProcessor");

                mmbeXmlProcessorTask.setAccountXmls(greenLightXmlList);
                mmbeXmlProcessorTask.setBccode(bccode);
                mmbeXmlProcessorTask.setRunId(runid);
                mmbeXmlProcessorTask.setExpectedMerchantId(merchantId);

                mmbeXmlProcessorTask.setSuccessCount(successCount);

                mmbeXmlProcessorTask.setBcCodeList(bcCodeMap);
                mmbeXmlProcessorTask.setLimit(limit);
                threadPoolExecutor.submit(mmbeXmlProcessorTask);
                logger.info("Greeenlight Batch" + (++i) + "submitted to threadpool");
            }
        }
    }


    threadPoolExecutor.shutdown();


    threadPoolExecutor.awaitTermination(batchTimeout, TimeUnit.SECONDS);


    long end = System.currentTimeMillis();
    logger.info("Total elapsed time" + (end - start) / 1000);
    logger.info("Skipped count is" + skippedCount);
    logger.info("Success count is" + successCount);
    StatementRun statementRun = new StatementRun();
    statementRun.setId(runid);
    bcCodeMap.keySet().forEach(key -> mmbeMigrationService.deleteLabelsByBCCode(key));
    mmbeMigrationService.updateStatementCounts(statementRun, successCount.longValue(), skippedCount.longValue());
    if (threadPoolExecutor.getUncaughtExceptions().size() > 0) {

        throw new RuntimeException(threadPoolExecutor.getUncaughtExceptions().size() +
                " exceptions occured. First exception:", threadPoolExecutor.getUncaughtExceptions().get(0));
    }


}

