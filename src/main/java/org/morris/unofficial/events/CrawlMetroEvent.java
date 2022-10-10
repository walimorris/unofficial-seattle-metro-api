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
import java.io.InputStream;
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
        if (!bucketContainsDocuments()) {
            putS3File(); // contains no documents, crawl immediately
        } else {
            logger.log("getting latest document");
            S3Object latestDocumentObject = getMostRecentDocumentObject(logger);
            logger.log("latest document date: " + latestDocumentObject.getObjectMetadata().getLastModified());

            boolean isScanMatch = scanLatestMetroDocumentAgainstRecentlyCrawledDocument(logger);
            logger.log("Scanned Match: " + isScanMatch);
        }
        return "success";
    }

    /**
     * Scans the latest unprocessed crawled document and recently crawled document from the SEA metro
     * site. Both documents should have already been written to both /tmp/recent_routes_doc.txt and
     * /tmp/routes_doc.txt, respectively. The text in both instances are split after a string of
     * unique characters, as we only want the document up until the end of the routes. Any data after
     * this point may be dynamic and changed often.
     *
     * @param logger {@link LambdaLogger}
     *
     * @return boolean
     */
    private boolean scanLatestMetroDocumentAgainstRecentlyCrawledDocument(LambdaLogger logger) {
        try {
            String latestDocumentTxt = new String(Files.readAllBytes(Paths.get(TMP_RECENT_ROUTES_DOC_FILE)))
                    .split(END_ROUTES_MARKER)[0];

            String recentDocumentTxt = new String(Files.readAllBytes(Paths.get(TMP_ROUTES_DOC_FILE)))
                    .split(END_ROUTES_MARKER)[0];
            if (latestDocumentTxt.equals(recentDocumentTxt)) {
                return true;
            }
        } catch (IOException e) {
            logger.log(String.format("Error scanning latest documents from /tmp directory: %s", e.getMessage()));
            return false;
        }
        return false;
    }

    /**
     * Returns the latest {@link S3Object} which is a dump of the unprocessed data from SEA metro site.
     * The latest document will be an {@link S3Object} from 7 days prior to the current date. This is
     * due to the {@link CrawlMetroEvent} being triggered every 7 days and uploading the crawled data
     * to the unprocessed S3 Bucket.
     * <p></p>
     * <p>
     *     prefix example: bucketName/docs/2022/10/8 or bucketName/docs/2022/1/1
     * </p>
     *
     * @param logger {@link LambdaLogger}
     * @return {@link S3Object}
     */
    private S3Object getMostRecentDocumentObject(LambdaLogger logger) {
        AmazonS3 s3Client = getS3Client();
        String documentFromSevenDaysAgoUri = getSevenDayPastPrefix() + ROUTES_DOC_FILE;
        GetObjectRequest getObjectRequest = new GetObjectRequest(BUCKET, documentFromSevenDaysAgoUri);
        S3Object object = s3Client.getObject(getObjectRequest);
        printToFile(object.getObjectContent(), TMP_RECENT_ROUTES_DOC_FILE, logger);
        s3Client.shutdown();
        return object;
    }

    /**
     * Checks if the unprocessed documents {@link Bucket} exists. If so, checks that
     * the bucket contains objects (documents from SEA metro dump).
     *
     * @return boolean
     */
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

            printToFile(connection.getInputStream(), TMP_ROUTES_DOC_FILE, logger);
            connection.disconnect();
        } catch (IOException e) {
            logger.log(String.format("Error writing route document dump to '%s': ", TMP_ROUTES_DOC_FILE + e.getMessage()));
        }
    }

    /**
     * Prints the contents of a {@link InputStream} to a file path.
     *
     * @param inputStream {@link InputStream} content
     * @param strPath {@link String} Path to save content as file
     * @param logger {@link LambdaLogger}
     */
    private void printToFile(InputStream inputStream, String strPath, LambdaLogger logger) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            StringBuffer content = new StringBuffer();
            while ((line = in.readLine()) != null) {
                content.append(line);
            }
            in.close();

            // write to file
            PrintWriter printWriter = new PrintWriter(strPath);
            printWriter.println(content);
        } catch (IOException e) {
            logger.log(String.format("Error writing route document dump to '%s': ", strPath + e.getMessage()));
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
        String year = String.valueOf(DateTime.now()
                .getYear());

        String month = String.valueOf(DateTime.now()
                .getMonthOfYear());

        String day = String.valueOf(DateTime.now()
                .getDayOfMonth());

        return String.format("docs/%s/%s/%s/", year, month, day);
    }

    /**
     * Creates a prefix for the unprocessed documents seven days prior to the current day in the
     * unprocessed S3 bucket for better query results.
     *
     * @return {@link String} a prefixed string seven days prior to the current date in form
     * YYYY/DD(D)/MM(M) 2022/8/10 or (2022/12/1)
     */
    private String getSevenDayPastPrefix() {
        String year = String.valueOf(DateTime.now()
                .minusDays(7)
                .getYear());

        String month = String.valueOf(DateTime.now()
                .minusDays(7)
                .getMonthOfYear());

        String day = String.valueOf(DateTime.now()
                .minusDays(7)
                .getDayOfMonth());

        return String.format("docs/%s/%s/%s/", year, month, day);
    }
}
