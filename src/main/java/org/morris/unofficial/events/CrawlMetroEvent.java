package org.morris.unofficial.events;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.joda.time.DateTime;

import java.io.InputStreamReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;

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
        putS3File();
        return "success";
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
