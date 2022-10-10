package org.morris.unofficial;

import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.lambda.runtime.tests.EventLoader;
import org.junit.Assert;
import org.junit.Test;
import org.morris.unofficial.events.CrawlMetroEvent;
import org.morris.unofficial.utils.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;

public class CrawlMetroEventLoaderTest {
    private final String TRIGGERED_CRAWL_METRO_EVENT_JSON_PATH = "/notifications/scheduled-event-metro-crawl.json";
    private final String LATEST_CRAWL_METRO_DOCUMENT_PATH_1 = "/s3-event-crawl-metro-latest-1.txt";
    private final String RECENT_CRAWL_METRO_DOCUMENT_PATH_1 = "/s3-event-crawl-metro-recent-1.txt";
    private final String RECENT_CRAWL_METRO_DOCUMENT_PATH_2 = "/s3-event-crawl-metro-recent-2.txt";

    @Test
    public void testTriggeredCrawlEventJson() {
        ScheduledEvent scheduledEvent = EventLoader.loadScheduledEvent(TRIGGERED_CRAWL_METRO_EVENT_JSON_PATH);
        Assert.assertNotNull(scheduledEvent);
        Assert.assertEquals("Scheduled Event", scheduledEvent.getDetailType());
        Assert.assertEquals("aws.events", scheduledEvent.getSource());
    }

    @Test
    public void testLatestCrawledDocumentTxtInputStream() {
        InputStream latestDocumentInputStream = FileUtils.getResourceAsStream(LATEST_CRAWL_METRO_DOCUMENT_PATH_1);
        Assert.assertNotNull(latestDocumentInputStream);
    }

    @Test
    public void testLatestCrawledDocumentTxtUrl() {
        URL latestDocumentTxtUrl = FileUtils.getResource(LATEST_CRAWL_METRO_DOCUMENT_PATH_1);
        Assert.assertNotNull(latestDocumentTxtUrl);
        File file = new File(latestDocumentTxtUrl.getFile());
        Assert.assertTrue(file.exists() && file.canRead());
    }

    @Test
    public void testRecentCrawledDocumentTxtInputStreams() {
        InputStream recentDocumentInputStream1 = FileUtils.getResourceAsStream(RECENT_CRAWL_METRO_DOCUMENT_PATH_1);
        InputStream recentDocumentInputStream2 = FileUtils.getResourceAsStream(RECENT_CRAWL_METRO_DOCUMENT_PATH_2);
        Assert.assertNotNull(recentDocumentInputStream1);
        Assert.assertNotNull(recentDocumentInputStream2);
    }

    @Test
    public void testRecentCrawledDocumentTxtUrls() {
        URL recentDocumentTextUrl1 = FileUtils.getResource(RECENT_CRAWL_METRO_DOCUMENT_PATH_1);
        URL recentDocumentTextUrl2 = FileUtils.getResource(RECENT_CRAWL_METRO_DOCUMENT_PATH_2);
        Assert.assertNotNull(recentDocumentTextUrl1);
        Assert.assertNotNull(recentDocumentTextUrl2);

        File file1 = new File(recentDocumentTextUrl1.getFile());
        File file2 = new File(recentDocumentTextUrl2.getFile());
        Assert.assertTrue(file1.exists() && file1.canRead());
        Assert.assertTrue(file2.exists() && file2.canRead());
    }

    @Test
    public void testLatestDocumentMatchesRecentDocument() throws IOException {
        URL latestDocumentTxtUrl = FileUtils.getResource(LATEST_CRAWL_METRO_DOCUMENT_PATH_1);
        File latestDocumentTxtFile = new File(latestDocumentTxtUrl.getFile());
        String latestDocumentStr = new String(Files.readAllBytes(latestDocumentTxtFile.toPath()))
                .split(CrawlMetroEvent.END_ROUTES_MARKER)[0];

        // should match exact document
        URL recentDocumentTxtUrl1 = FileUtils.getResource(RECENT_CRAWL_METRO_DOCUMENT_PATH_1);
        File recentDocumentTxtFile1 = new File(recentDocumentTxtUrl1.getFile());
        String recentDocumentStr1 = new String(Files.readAllBytes(recentDocumentTxtFile1.toPath()))
                .split(CrawlMetroEvent.END_ROUTES_MARKER)[0];

        // should not match as there's content missing in recent document file 2
        URL recentDocumentTxtUrl2 = FileUtils.getResource(RECENT_CRAWL_METRO_DOCUMENT_PATH_2);
        File recentDocumentTxtFile2 = new File(recentDocumentTxtUrl2.getFile());
        String recentDocumentStr2 = new String(Files.readAllBytes(recentDocumentTxtFile2.toPath()))
                .split(CrawlMetroEvent.END_ROUTES_MARKER)[0];

        Assert.assertNotNull(latestDocumentStr);
        Assert.assertNotNull(recentDocumentStr1);
        Assert.assertNotNull(recentDocumentStr2);

        Assert.assertEquals(latestDocumentStr, recentDocumentStr1);
        Assert.assertNotEquals(latestDocumentStr, recentDocumentStr2);
    }
}
