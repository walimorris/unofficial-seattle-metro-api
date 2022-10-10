package org.morris.unofficial.events;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;

public class ProcessCrawledMetroDataEvent {
    final private static String REGION = System.getenv("REGION");
    final private static String UNPROCESSED_BUCKET = System.getenv("UNPROCESSED_BUCKET_NAME");
    final private static String PROCESSED_BUCKET = System.getenv("PROCESSED_BUCKET_NAME");
    final private static String JSON_ROUTES_DOC_FILE = "/tmp/routes_doc.json";
    final private static String METRO_PREFIX = "https://kingcounty.gov";

    public String handleRequest(S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();

        InputStreamReader unprocessedDocumentInputStreamReader = getUnprocessedDocumentInputStreamReader(event);
        String content = collectInitialContent(unprocessedDocumentInputStreamReader, logger);

        // remove the first index in split as it contains content before
        // the first route details
        String[] routesMarkupArray = crawlDocumentAndCreateRoutesArray(content);

        // example route markup: "><strong>A Line</strong> - Tukwila International Boulevard Station, Federal Way Transit Center</a> <div class="route_more_button"> <div class="btn-group"> <button type="button" class="btn btn-default btn-sm dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false" title="More options"><span class="visuallyhidden">More</span><span class="glyphicon glyphicon-option-horizontal pull-right"></span></button> <ul class="dropdown-menu dropdown-menu-right"> <li><a href="/depts/transportation/metro/schedules-maps/hastop/a-line.aspx">Online schedule</a></li> <li class="map"><a data-fancybox="" data-src="/~/media/depts/metro/maps/route/09172022/large/m671.jpg" href="javascript:;">Route map</a></li> <li class="pdf-timetable"><a href="/~/media/depts/metro/schedules/pdf/09172022/rt-a-line.pdf" target="_blank">PDF timetable</a></li> <li class="night-owl-route-info"><a href="/depts/transportation/metro/travel-options/bus/night-owl.aspx">About Night Owl service</a></li> <li role="separator" class="divider"></li> <li><a href="/depts/transportation/metro/alerts-updates/sign-up.aspx">Sign up for transit alerts</a></li> <li><a href="https://tripplanner.kingcounty.gov" target="_blank">Plan a trip</a></li> </ul> </div> </div> <div id="advisory-a-line" class="alert alert-warning" style="margin-top: 6px;"> <p class="small"><a href="/depts/transportation/metro/alerts-updates/service-advisories.aspx">See all service advisories</a></p> </div> </li> <li id="route672" class="route-item rapidride-route not-night-owl weekday saturday sunday not-suspended esn"> <div id="alert-b-line" class="alert-icon hidden-print"><a href="/depts/transportation/metro/alerts-updates/service-advisories.aspx?search=b-line">Advisory for the B Line</a></div> <a href="/depts/transportation/metro/schedules-maps/hastop/b-line.aspx" class="
        // Create routes map containing line -> lines count, route name, url to route schedule, url to route pdf
        Map<String, Map<String, String>> routeLines = createRoutesContent(routesMarkupArray);

        Map<String, String> routes = routeLines.get("lines");
        Map<String, String> routeUrls = routeLines.get("lineUrls");
        List<JSONObject> lineObjects = collectLineJSONObjectsInList(routes, routeUrls);

        // feed into an array to prepare to input to file
        JSONArray lineObjectsArray = collectLineJSONObjectsInArray(lineObjects);
        printTransformedMetroDataToTmp(lineObjectsArray, logger);
        putS3File();

        return "success";
    }

    private String[] crawlDocumentAndCreateRoutesArray(String content) {
        String[] routeListSplit = content.split("Route list");
        String routeContent = routeListSplit[1];

        String[] routeNameSplit = routeContent.split("route-name");

        // remove the first index in split as it contains content before
        // the first route details
        return Arrays.copyOfRange(routeNameSplit, 1, routeNameSplit.length);
    }

    private Map<String, Map<String, String>> createRoutesContent(String[] routesMarkupArray) {
        Map<String, Map<String, String>> routeLines = new HashMap<>();

        // map of each line
        Map<String, String> lines = new HashMap<>();
        Map<String, String> lineUrls = new HashMap<>();
        routeLines.put("lines", lines);
        routeLines.put("lineUrls", lineUrls);

        for (String routeMarkup : routesMarkupArray) {
            String[] routeLineSplit = routeMarkup.split("</strong> -");
            String routeLine = getRouteLine(routeLineSplit[0]);

            String nextContent = routeLineSplit[1].trim();
            String routeName = getRouteName(nextContent);
            lines.put(routeLine, routeName);

            nextContent = getNextContentAfterRoute(nextContent);
            String routeScheduleUrl = getRouteScheduleUrl(nextContent);

            // prefix the url with the metro site top level domain before routes schedules url
            lineUrls.put(routeName, METRO_PREFIX + routeScheduleUrl);
        }
        return routeLines;
    }

    private String collectInitialContent(InputStreamReader inputStreamReader, LambdaLogger logger) {
        String content = null;
        try {
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            String separator = System.getProperty("line.separator");
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(separator);
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            bufferedReader.close();
            content = stringBuilder.toString();
        } catch (IOException e) {
            logger.log(String.format("Error collecting initial content from document upload: %s", e.getMessage()));
        }
        return content;
    }

    private JSONArray collectLineJSONObjectsInArray(List<JSONObject> lineObjectsList) {
        return new JSONArray(lineObjectsList);
    }

    private void printTransformedMetroDataToTmp(JSONArray lineObjectsArray, LambdaLogger logger) {
        try(PrintWriter output = new PrintWriter(new FileWriter(JSON_ROUTES_DOC_FILE))) {
            output.write(lineObjectsArray.toString());
        } catch (IOException e) {
            logger.log(String.format("Unable to write transformed JSON data to file '%s': ", JSON_ROUTES_DOC_FILE) + e.getMessage());
        }
    }

    /**
     * Load newly transformed JSON file in /tmp to processed s3 bucket.
     */
    private void putS3File() {
        String fileName = getPrefix() + new File(JSON_ROUTES_DOC_FILE).getName();
        PutObjectRequest putObjectRequest = new PutObjectRequest(PROCESSED_BUCKET, fileName, new File(JSON_ROUTES_DOC_FILE));
        AmazonS3 s3 = getS3Client();
        s3.putObject(putObjectRequest);
        s3.shutdown();
    }

    private List<JSONObject> collectLineJSONObjectsInList(Map<String, String> routes, Map<String, String> routeUrls) {

        // Add (line, line_name, line_schedule_url) as properties to each json object
        // we can get the line url by getting the url value from line name key in
        // routeUrls map yo
        List<JSONObject> lineObjects = new ArrayList<>();
        routes.forEach((key, value) -> {
            JSONObject obj = new JSONObject();
            obj.put("line", key);
            obj.put("line_name", value);
            obj.put("line_schedule_url", routeUrls.get(value));
            lineObjects.add(obj);
        });
        return lineObjects;
    }

    private InputStreamReader getUnprocessedDocumentInputStreamReader(S3Event event) {
        // Only one event will be processed from triggered reader
        String unprocessedDocumentKey = event.getRecords()
                .get(0)
                .getS3()
                .getObject()
                .getKey();

        // get the object from the unprocessed document bucket
        GetObjectRequest getObjectRequest = new GetObjectRequest(UNPROCESSED_BUCKET, unprocessedDocumentKey);
        S3Object unprocessedDocument = getS3Client().getObject(getObjectRequest);
        return new InputStreamReader(unprocessedDocument.getObjectContent(),
                StandardCharsets.UTF_8);
    }

    private AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withRegion(REGION)
                .build();
    }

    private String getRouteLine(String markup) {
        return markup.split("><strong>")[1];
    }

    private String getRouteName(String markup) {
        return markup.split("</a>")[0].trim()
                .replace("P&amp;R", "")
                .replace("<span class=\"sr-only\">also known as SVT</span>", "");
    }

    private String getRouteScheduleUrl(String markup) {
        String split1 = markup.split("href=\"")[1];
        return split1.split("\">")[0];
    }

    private String getNextContentAfterRoute(String markup) {
        return markup.split("</a>")[1].trim();
    }

    private String getPrefix() {
        String year = String.valueOf(DateTime.now().getYear());
        String month = String.valueOf(DateTime.now().getMonthOfYear());
        String day = String.valueOf(DateTime.now().getDayOfMonth());

        return String.format("docs/%s/%s/%s/", year, month, day);
    }
}
