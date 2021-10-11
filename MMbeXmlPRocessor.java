package cms.samples;

package be.speos.mmbe.mmbeloader.parser;

import be.speos.mmbe.mmbeloader.model.Account;
import be.speos.mmbe.mmbeloader.model.Statement;
import be.speos.mmbe.mmbeloader.model.StatementRun;
import be.speos.mmbe.mmbeloader.model.StatementSummary;
import be.speos.mmbe.mmbeloader.service.LoaderBGProcessorService;
import be.speos.mmbe.mmbeloader.util.DateUtil;
import be.speos.mmbe.mmbeloader.util.MmbeSequenceKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Scope("prototype")
public class MmbeXmlProcessor implements Runnable {
    private List<String> accountXmls;
    private String bccode;
    private String runId;
    private String expectedMerchantId;

    private AtomicLong successCount;
    private ConcurrentMap<String, String> bcCodeList;

    private int limit;
    private Logger logger = LoggerFactory.getLogger(MmbeXmlProcessor.class);

    @Autowired
    private LoaderBGProcessorService loaderBGProcessorService;


    public MmbeXmlProcessor() {

    }

    /*public static void main(String args[]) throws Exception {
        String s=FileUtils.readFileToString(new File("D:\\Projects\\mmbebenchmarks\\3error\\error.xml"), Charset.defaultCharset());
        Statement statement = XMLParser.parseAccountXmlDtd(s,"a","e","d","e");
        int x=9;
    }*/

    private void initGreenLight() {

        XMLParser.AccountInfo accountInfo = null;
        try {
            String accountx = accountXmls.get(0);
            accountInfo = XMLParser.parseAccountXmlDtd(accountx, bccode, runId, expectedMerchantId, loaderBGProcessorService.getNextSequenceId(MmbeSequenceKeys.STATEMENT_ID));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Account accountDB = loaderBGProcessorService.findAccountByExternalKey(accountInfo.getExternalKey());
        if (accountDB != null) {
            loaderBGProcessorService.deleteGreenLightAccountByExternalKey(accountInfo.getExternalKey());
        }

    }

    public void run() {
        List<Account> accountList = new ArrayList<>();
        List<Statement> finalStatementList = new ArrayList<>();
        List<StatementSummary> finalStatementSummaryList = new ArrayList<>();
        List<String> finalDeleteStatementList = new ArrayList<>();
        List<String> finalDeleteStatementSummaryList = new ArrayList<>();
        if (limit == 50) {
            initGreenLight();
            logger.info("GreenLight initiated");
        }

        for (String account : accountXmls) {
            try {
                long start = System.currentTimeMillis();
                logger.debug("Start acccount time" + start);
                XMLParser.AccountInfo accountInfo = XMLParser.parseAccountXmlDtd(account, bccode, runId, expectedMerchantId, loaderBGProcessorService.getNextSequenceId(MmbeSequenceKeys.STATEMENT_ID));


                Account accountDB = loaderBGProcessorService.findAccountByExternalKey(accountInfo.getExternalKey());


                if (accountDB != null) {
                    if (limit == 50) {
                        throw new RuntimeException("GreenLight account for " + bccode + " exists.Please delete it manually and rerun the greenlight batch");
                    }
                    List<StatementSummary> statementSummaryList = loaderBGProcessorService.fetchStatementSummaryEntity(accountInfo.getExternalKey());

                    Iterator<StatementSummary> iterator = statementSummaryList.iterator();
                    while (iterator.hasNext()) {
                        StatementSummary sum = iterator.next();
                        StatementRun sr = loaderBGProcessorService.findStatementRunById(sum.getStatementRunId()).orElseThrow(() -> new RuntimeException(sum.get_id() + "StatementRun id not found for" + accountInfo.getExternalKey()));
                        if (bccode.equalsIgnoreCase(sr.getBillingCycleCode())) {
                            finalDeleteStatementList.add(sum.get_id());
                            finalDeleteStatementSummaryList.add(sum.get_id());
                            iterator.remove();
                        }
                    }

                    accountInfo.getStatement().getStatementMetaData().setAccountId(accountDB.get_id());
                    accountDB.setLastModifiedTime(DateUtil.getDateString(new Date()));
                    statementSummaryList.add(0, accountInfo.getStatementSummary());

                    if (statementSummaryList.size() > limit) {
                        //statementSummaryList.sort((o1, o2) -> o2.getStatementDate().compareTo(o1.getStatementDate()));

                        for (int k = limit; k < statementSummaryList.size(); k++) {
                            StatementSummary lastStatement = statementSummaryList.get(k);
                            finalDeleteStatementList.add(lastStatement.get_id());
                            finalDeleteStatementSummaryList.add(lastStatement.get_id());
                            bcCodeList.putIfAbsent(loaderBGProcessorService.findStatementRunById(lastStatement.getStatementRunId()).get().getBillingCycleCode(), "");
                        }
                    }


                } else {

                    accountDB = new Account();
                    accountDB.set_id(loaderBGProcessorService.getNextSequenceId(MmbeSequenceKeys.ACCOUNT_ID));
                    accountDB.setCreatedTime(DateUtil.getDateString(new Date()));
                    accountDB.setMerchantId(expectedMerchantId);
                    accountDB.setExternalKey(accountInfo.getExternalKey());

                    accountInfo.getStatement().getStatementMetaData().setAccountId(accountDB.get_id());


                }

                finalStatementSummaryList.add(accountInfo.getStatementSummary());
                finalStatementList.add(accountInfo.getStatement());
                accountList.add(accountDB);
                finalStatementList.add(accountInfo.getStatement());
                finalStatementSummaryList.add(accountInfo.getStatementSummary());
                logger.debug("End account processing time" + (System.currentTimeMillis() - start) / 1000 + "seconds");

            } catch (Exception e) {

                throw new RuntimeException(e);
            }
        }
        if (limit == 50) {

            Account start = accountList.get(0);
            for (Statement s : finalStatementList) {
                s.getStatementMetaData().setAccountId(start.get_id());
            }
            accountList = new ArrayList<>();
            accountList.add(start);

        }
        if (finalDeleteStatementList.size() > 0) {
            loaderBGProcessorService.bulkDeleteStatements(finalDeleteStatementList);
        }
        if (finalDeleteStatementSummaryList.size() > 0) {
            loaderBGProcessorService.bulkDeleteStatementSummaries(finalDeleteStatementSummaryList);
        }
        if (accountList.size() > 0) {
            loaderBGProcessorService.bulkUpsertAccounts(accountList);
        }
        if (finalStatementSummaryList.size() > 0) {
            loaderBGProcessorService.bulkUpsertStatementSummaries(finalStatementSummaryList);
        }
        if (finalStatementList.size() > 0) {
            loaderBGProcessorService.bulkUpsertStatements(finalStatementList);
        }

        successCount.addAndGet(accountList.size());


    }

    public void setLimit(int limit) {
        this.limit = limit;
    }


    public void setAccountXmls(List<String> accountXmls) {
        this.accountXmls = accountXmls;
    }

    public void setBccode(String bccode) {
        this.bccode = bccode;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public void setExpectedMerchantId(String expectedMerchantId) {
        this.expectedMerchantId = expectedMerchantId;
    }


    public void setSuccessCount(AtomicLong successCount) {
        this.successCount = successCount;
    }

    public void setBcCodeList(ConcurrentMap<String, String> bcCodeList) {
        this.bcCodeList = bcCodeList;
    }
}
