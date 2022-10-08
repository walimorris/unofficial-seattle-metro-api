package org.morris.unofficial.events;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.joda.time.DateTime;

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.List;

public class CrawlMetroEvent {
    final String REGION = System.getenv("REGION"); // region
    final String BUCKET = System.getenv("UNPROCESSED_BUCKET_NAME");
    final String GET_REQUEST = "GET";
    final String ROUTES_DOC_FILE = "/tmp/routes_doc.txt";
    final String METRO_SCHEDULE_URL = "https://kingcounty.gov/depts/transportation/metro/schedules-maps.aspx";

    public String handleRequest(ScheduledEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log(String.format("Seattle Metro crawl event triggered: %s", event.getId()));

        printMetroDumpToTmp(logger);

        // Do we need to crawl the data - if no unprocessed document exists then yes
        // If document exists, let's match up against new crawled .txt in /tmp

        if (!bucketContainsDocuments()) {
            // contains no documents, crawl immediately
            putS3File();
        } else {
            if (bucketContainsDocuments()) {
                // documents exists - crawl and match against last crawled document
                // if they match then there's no use recording the document as it'll
                // only be a copy
                S3Object latestDocumentObject = getMostRecentDocument();

                // compare the text from the latest object and dumped document sitting in /tmp
            }
        }
        return "success";
    }

    private S3Object getMostRecentDocument() {
        AmazonS3 s3Client = getS3Client();
        ObjectListing objectListing = s3Client.listObjects(BUCKET);
        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();

        // Get the first date by pulling the first objects last modified date
        Date currentDate = objectSummaries.get(0).getLastModified();
        S3ObjectSummary latestObjectSummary = null;
        for (S3ObjectSummary objectSummary : objectSummaries.subList(1, objectSummaries.size())) {
            if (objectSummary.getLastModified().compareTo(currentDate) > 0) {
                // it is greater - last modified date comes after first date
                latestObjectSummary = objectSummary;
                // update the date
                currentDate = latestObjectSummary.getLastModified();
            }
        }
        // return the object
        S3Object latestDocumentObject = null;
        if (latestObjectSummary != null) {
            GetObjectRequest getObjectRequest = new GetObjectRequest(latestObjectSummary.getKey(), BUCKET);
            latestDocumentObject = s3Client.getObject(getObjectRequest);
        }
        return latestDocumentObject;
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
            PrintWriter printWriter = new PrintWriter(ROUTES_DOC_FILE);
            printWriter.println(content);
        } catch (IOException e) {
            logger.log(String.format("Error writing route document dump to '%s': ", ROUTES_DOC_FILE + e.getMessage()));
        }
    }

    /**
     * Loads the SEA metro document dump to the unprocessed bucket in S3.
     */
    private void putS3File() {
        String fileName = getPrefix() + new File(ROUTES_DOC_FILE).getName();
        PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET, fileName, new File(ROUTES_DOC_FILE));
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
}
