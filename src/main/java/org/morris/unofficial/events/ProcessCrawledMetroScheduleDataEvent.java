package org.morris.unofficial.events;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.morris.unofficial.models.MetroLine;
import org.morris.unofficial.utils.ProcessEventUtils;

import java.io.IOException;

public class ProcessCrawledMetroScheduleDataEvent {
    final private static String PROCESSED_BUCKET = System.getenv("PROCESSED_BUCKET_NAME");

    public String handleRequest(S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();
        AmazonS3 s3Client = ProcessEventUtils.getS3Client();

        // lets get the processed data in a file
        MetroLine metroLine = getMetroLineAsPojoFromJson(event, s3Client, logger);
        String metroLineAsJsonString = parseMetroLinePojoAsJsonString(metroLine, logger);
        logger.log(metroLineAsJsonString);

        // shutdown s3 client
        if (s3Client != null) {
            s3Client.shutdown();
        }

        return "success";
    }

    private String parseMetroLinePojoAsJsonString(MetroLine metroLinePojo, LambdaLogger logger) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            return objectMapper.writeValueAsString(metroLinePojo);
        } catch (IOException e) {
            logger.log(String.format("Error mapping pojo '%s' to json string: " + e.getMessage(), MetroLine.class));
        }
        return null;
    }

    private MetroLine getMetroLineAsPojoFromJson(S3Event event, AmazonS3 client, LambdaLogger logger) {
        MetroLine metroLine;
        ObjectMapper metroLineObjectMapper = new ObjectMapper();
        S3Object processedJsonObject = getTriggeredEventObject(event, client);

        try {
            metroLine = metroLineObjectMapper.readValue(processedJsonObject.getObjectContent(),
                    MetroLine.class);
        } catch (IOException e) {
            logger.log(String.format("Error mapping processed metro data to pojo '%s'" + e.getMessage(), MetroLine.class));
            return null;
        }
        return metroLine;
    }

    private S3Object getTriggeredEventObject(S3Event event, AmazonS3 client) {
        String eventKey = event.getRecords()
                .get(0)
                .getS3()
                .getObject()
                .getKey();

        GetObjectRequest getObjectRequest = new GetObjectRequest(PROCESSED_BUCKET, eventKey);
        return client.getObject(getObjectRequest);
    }
}
