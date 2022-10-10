package org.morris.unofficial.events;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import org.joda.time.DateTime;

import java.io.File;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class CrawlMetroEvent {
    final String REGION = System.getenv("REGION"); // region
    final String BUCKET = System.getenv("UNPROCESSED_BUCKET_NAME");
    final String GET_REQUEST = "GET";
    final String ROUTES_DOC_FILE = "routes_doc.txt";
    final String TMP_ROUTES_DOC_FILE = "/tmp/routes_doc.txt";
    final String TMP_RECENT_ROUTES_DOC_FILE = "/tmp/recent_routes_doc.txt";
    final String END_ROUTES_MARKER = "<!-- end #routes -->";
    final String METRO_SCHEDULE_URL = "https://kingcounty.gov/depts/transportation/metro/schedules-maps.aspx";

    public String handleRequest(ScheduledEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log(String.format("Seattle Metro crawl event triggered: %s", event.getId()));

        printMetroDumpToTmp(logger);

        // Do we need to crawl the data - if no unprocessed document exists then yes
        // If document exists, let's match up against new crawled .txt in /tmp
        if (!bucketContainsDocuments()) {
            // contains no documents, crawl immediately
            logger.log("no unprocessed documents, crawling seattle metro...");
            putS3File();
        } else {
            logger.log("getting lastest document");
            S3Object latestDocumentObject = getMostRecentDocumentObject(logger);
            logger.log("latest document date: " + latestDocumentObject.getObjectMetadata().getLastModified());

            // compare the text from the latest object and dumped document sitting in /tmp
            boolean isScanMatch = scanLatestMetroDocumentAgainstRecentlyCrawledDocument(logger);
            logger.log("Scanned Match: " + isScanMatch);
        }
        return "success";
    }

    private boolean scanLatestMetroDocumentAgainstRecentlyCrawledDocument(LambdaLogger logger) {
        try {
            // split the texts after routes end - we're only checking that routes match. Any content in the
            // document after the routes may be dynamic and changed often.
            String latestDocumentTxt = new String(Files.readAllBytes(Paths.get(TMP_RECENT_ROUTES_DOC_FILE)))
                    .split(END_ROUTES_MARKER)[0];

            String recentDocumentTxt = new String(Files.readAllBytes(Paths.get(TMP_ROUTES_DOC_FILE)))
                    .split(END_ROUTES_MARKER)[0];
            if (latestDocumentTxt.equals(recentDocumentTxt)) {
                return true;
            }
        } catch (IOException e) {
            logger.log(String.format("Error reading and comparing latest document and recent dump from /tmp directory: %s", e.getMessage()));
            return false;
        }
        return false;
    }

    private S3Object getMostRecentDocumentObject(LambdaLogger logger) {
        // we'll just need to pull the latest document by pulling the document from 7 days ago
        // which is the last document upload
        return getMostRecentObjectInBucket(logger);
    }

    private S3Object getMostRecentObjectInBucket(LambdaLogger logger) {
        AmazonS3 s3Client = getS3Client();
        // prefix example bucketName/docs/2022/10/8 - get current prefix based on today's date

        // this function is triggered because it's crawl day and crawl day is every 7 days, so let's
        // build the date from 7 days ago, the last crawl day, and query for lastest document
        String documentFromSevenDaysAgoUri = getSevenDayPastPrefix() + ROUTES_DOC_FILE;
        logger.log("BucketFrom7DaysAgoUri: " + documentFromSevenDaysAgoUri);
        GetObjectRequest getObjectRequest = new GetObjectRequest(BUCKET, documentFromSevenDaysAgoUri);
        S3Object object = s3Client.getObject(getObjectRequest);

        try {
            // print content to /tmp
            BufferedReader recentDocumentReader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            String recentDocumentLine;
            StringBuffer content = new StringBuffer();
            while ((recentDocumentLine = recentDocumentReader.readLine()) != null) {
                content.append(recentDocumentLine);
            }
            recentDocumentReader.close();
            // write to file
            PrintWriter printWriter = new PrintWriter(TMP_RECENT_ROUTES_DOC_FILE);
            printWriter.println(content);
        } catch (IOException e) {
            logger.log("Error processing recent document content: " + e.getMessage());
        }


        s3Client.shutdown();
        return object;
    }

    private boolean bucketContainsDocuments() {
        AmazonS3 s3Client = getS3Client();
        Bucket unprocessedBucket = null;
        List<Bucket> buckets = s3Client.listBuckets();
        for (Bucket bucket : buckets) {
            if (bucket.getName().equals(BUCKET)) {
                unprocessedBucket = bucket;
            }
        }
        // see if bucket contains object(s) (document)
        if (unprocessedBucket != null) {
            ObjectListing objectListing = s3Client.listObjects(unprocessedBucket.getName());
            return objectListing.getObjectSummaries().size() > 0;
        }
        s3Client.shutdown();
        return false;
    }

    /**
     * Queries the Seattle Metro website, and reads the content from a {@link InputStreamReader},
     * and writes the page dump content to /tmp file.
     */
    private void printMetroDumpToTmp(LambdaLogger logger) {
        try {
            URL metroScheduleUrl = new URL(METRO_SCHEDULE_URL);
            HttpURLConnection connection = (HttpURLConnection) metroScheduleUrl.openConnection();
            connection.setRequestMethod(GET_REQUEST);

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer content = new StringBuffer();
            while ((line = in.readLine()) != null) {
                content.append(line);
            }
            in.close();
            connection.disconnect();

            // write to file
            PrintWriter printWriter = new PrintWriter(TMP_ROUTES_DOC_FILE);
            printWriter.println(content);
        } catch (IOException e) {
            logger.log(String.format("Error writing route document dump to '%s': ", TMP_ROUTES_DOC_FILE + e.getMessage()));
        }
    }

    /**
     * Loads the SEA metro document dump to the unprocessed bucket in S3.
     */
    private void putS3File() {
        String fileName = getPrefix() + new File(TMP_ROUTES_DOC_FILE).getName();
        PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET, fileName, new File(TMP_ROUTES_DOC_FILE));
        AmazonS3 s3 = getS3Client();
        s3.putObject(putObjectRequest);
        s3.shutdown();
    }

    /**
     * Get {@link AmazonS3} client
     * @return {@link AmazonS3}
     */
    private AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withRegion(REGION)
                .build();
    }

    /**
     * Creates a prefix for the unprocessed documents in the unprocessed S3 bucket for
     * better query results and future data processing and recording insights.
     *
     * @return {@link String} a prefixed string in form YYYY/DD(D)/MM(M) 2022/8/10 or (2022/12/1)
     */
    private String getPrefix() {
        String year = String.valueOf(DateTime.now().getYear());
        String month = String.valueOf(DateTime.now().getMonthOfYear());
        String day = String.valueOf(DateTime.now().getDayOfMonth());

        return String.format("docs/%s/%s/%s/", year, month, day);
    }

    private String getSevenDayPastPrefix() {
        String year = String.valueOf(DateTime.now().minusDays(7).getYear());
        String month = String.valueOf(DateTime.now().minusDays(7).getMonthOfYear());
        String day = String.valueOf(DateTime.now().minusDays(7).getDayOfMonth());

        return String.format("docs/%s/%s/%s/", year, month, day);
    }
}
