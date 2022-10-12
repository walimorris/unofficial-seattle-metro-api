package org.morris.unofficial.events;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.morris.unofficial.models.MetroLine;
import org.morris.unofficial.utils.ProcessEventUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProcessCrawledMetroScheduleDataEvent {
    final private static String PROCESSED_BUCKET = System.getenv("PROCESSED_BUCKET_NAME");
    final private static String ROUTES_JSON_FILE = "/tmp/routes_doc.json";

    public String handleRequest(S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();
        AmazonS3 s3Client = ProcessEventUtils.getS3Client();

        // lets get the processed data in a file
        List<MetroLine> metroLines = getMetroLineAsPojoFromJson(event, s3Client, logger);
        if (metroLines != null) {
            JSONArray metroLineJsonArray = parseMetroLinePojoListAsJsonArray(metroLines);
            logger.log(metroLineJsonArray.toString());
        }

        // shutdown s3 client
        if (s3Client != null) {
            s3Client.shutdown();
        }
        return "success";
    }

    /**
     * Gets a JSONArray from a list of MetroLine POJOs.
     *
     * @param metroLinePojoList {@link List} of MetroLine POJOs
     * @return {@link JSONArray}
     *
     * @see MetroLine
     */
    private JSONArray parseMetroLinePojoListAsJsonArray(List<MetroLine> metroLinePojoList) {
        List<JSONObject> metroLineJsonObjects = new ArrayList<>();
        for (MetroLine metroLine : metroLinePojoList) {

            JSONObject metroLineJsonObject = new JSONObject();
            metroLineJsonObject.put("line", metroLine.getLine());
            metroLineJsonObject.put("line_name", metroLine.getLineName());
            metroLineJsonObject.put("line_schedule_url", metroLine.getLineScheduleUrl());

            metroLineJsonObjects.add(metroLineJsonObject);

        }
        return new JSONArray(metroLineJsonObjects);
    }

    /**
     * Reads in a Json file from a triggered S3Event that uploads the part 1 of processed data
     * from the SEA metro site crawl. This Json content is then converted to a {@link List} of
     * MetroLine POJOs.
     *
     * @param event {@link S3Event}
     * @param client {@link AmazonS3} client
     * @param logger {@link LambdaLogger} logger
     *
     * @return {@link List} of MetroLine POJOs
     *
     * @see MetroLine
     * @see TypeReference
     */
    private List<MetroLine> getMetroLineAsPojoFromJson(S3Event event, AmazonS3 client, LambdaLogger logger) {
        List<MetroLine> metroLines;
        S3Object processedJsonObject;
        ObjectMapper metroLineObjectMapper = new ObjectMapper();

        try {
            processedJsonObject = getTriggeredEventObject(event, client);
            File routesJsonFile = new File(ROUTES_JSON_FILE);
            FileUtils.copyInputStreamToFile(processedJsonObject.getObjectContent(), routesJsonFile);
            metroLines = metroLineObjectMapper.readValue(routesJsonFile, new TypeReference<List<MetroLine>>(){});
            processedJsonObject.close();
        } catch (IOException e) {
            logger.log(String.format("Error mapping processed metro data to pojo '%s'" + e.getMessage(), MetroLine.class));
            return null;
        }
        return metroLines;
    }

    /**
     * Gets the first and only {@link S3Object} from a S3Event.
     *
     * @param event triggered {@link S3Event}
     * @param client {@link AmazonS3} client
     *
     * @return {@link S3Object}
     */
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
