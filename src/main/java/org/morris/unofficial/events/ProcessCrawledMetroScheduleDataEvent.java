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
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.morris.unofficial.models.MetroLine;
import org.morris.unofficial.utils.ProcessEventUtils;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.KeyPhrase;
import software.amazon.awssdk.services.comprehend.model.DominantLanguage;
import software.amazon.awssdk.services.comprehend.model.DetectDominantLanguageResponse;
import software.amazon.awssdk.services.comprehend.model.DetectDominantLanguageRequest;
import software.amazon.awssdk.services.comprehend.model.DetectKeyPhrasesRequest;
import software.amazon.awssdk.services.comprehend.model.DetectKeyPhrasesResponse;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ProcessCrawledMetroScheduleDataEvent {
    final private static String PROCESSED_BUCKET = System.getenv("PROCESSED_BUCKET_NAME");
    final private static String ROUTES_JSON_FILE = "/tmp/routes_doc.json";
    final static private String LINE_SCHEDULE_TXT_FILE = "/tmp/line_schedule_doc.txt";
    final static private String LINE_SCHEDULE_PDF_FILE = "/tmp/line_schedule_doc.pdf";
    final static private String LINE_SCHEDULE_PDF_CONTENT_TXT_FILE = "/tmp/line_schedule_pdf_content.txt";

    public String handleRequest(S3Event event, Context context) throws IOException {
        LambdaLogger logger = context.getLogger();
        AmazonS3 s3Client = ProcessEventUtils.getS3Client();

        // lets get the processed data in a file
        List<MetroLine> metroLines = getMetroLineAsPojoFromJson(event, s3Client, logger);
        if (metroLines != null) {
            JSONArray metroLineJsonArray = parseMetroLinePojoListAsJsonArray(metroLines);

            // iterate each MetroLine object in jsonArray - limited to 1 object currently for testing purposes
            // otherwise requests will be too expensive
            for (int i = 0; i < 1 /*metroLineJsonArray.length()*/; i++) {
                JSONObject metroLineObject = metroLineJsonArray.getJSONObject(i);

                // get the line schedule
                String lineScheduleUrl = getLineScheduleUrl(metroLineObject);
                String line = getLine(metroLineObject);
                logger.log("line: " + line + "line url: " + lineScheduleUrl);

                // query the scheduleUrl and obtain the pdf document with schedules
                String lineSchedulePdfUrl = queryLineScheduleUrlForPdfScheduleUrl(lineScheduleUrl, line, logger);

                // write pdf schedule to /tmp
                ProcessEventUtils.printToPdfFile(LINE_SCHEDULE_PDF_FILE, lineSchedulePdfUrl);
                String pdfScheduleContent = readPdfFileContent(logger);

                // write schedule content to .txt file in /tmp
                if (pdfScheduleContent != null) {
                    InputStream pdfScheduleContentInputStream = new ByteArrayInputStream(pdfScheduleContent.getBytes());
                    ProcessEventUtils.printToFile(pdfScheduleContentInputStream, LINE_SCHEDULE_PDF_CONTENT_TXT_FILE, logger);

                    String pdfScheduleTextContent = ProcessEventUtils.readFileAsStringBuffer(LINE_SCHEDULE_PDF_CONTENT_TXT_FILE, logger);
                    if (pdfScheduleTextContent != null) {
                        List<KeyPhrase> scheduleKeyPhraseList = comprehendKeyPhraseList(pdfScheduleTextContent);
                        logger.log(scheduleKeyPhraseList.toString());
                    }
                }
            }
        }

        // shutdown s3 client
        if (s3Client != null) {
            s3Client.shutdown();
        }
        return "success";
    }

    private List<KeyPhrase> comprehendKeyPhraseList(String scheduleContent) {
        List<KeyPhrase> keyPhraseFilterList1;
        ComprehendClient comprehendClient = ComprehendClient.builder()
                .region(ProcessEventUtils.getRegionV2())
                .build();

        // detect english key phrase in schedule content
        DetectKeyPhrasesRequest detectKeyPhrasesRequest = DetectKeyPhrasesRequest.builder()
                .text(scheduleContent)
                .languageCode("en")
                .build();

        DetectKeyPhrasesResponse detectKeyPhrasesResponse = comprehendClient.detectKeyPhrases(detectKeyPhrasesRequest);
        List<KeyPhrase> keyPhraseList = detectKeyPhrasesResponse.keyPhrases();
        keyPhraseFilterList1 = new ArrayList<>();

        for (KeyPhrase keyPhrase : keyPhraseList) {
            DetectDominantLanguageRequest detectDominantLanguageRequest = DetectDominantLanguageRequest.builder()
                    .text(keyPhrase.text())
                    .build();
            DetectDominantLanguageResponse detectDominantLanguageResponse = comprehendClient.detectDominantLanguage(detectDominantLanguageRequest);
            List<DominantLanguage> dominantLanguageList = detectDominantLanguageResponse.languages();

            // remove key phrases with spanish text language code
            boolean containsSpanishLanguageCode = false;
            for (DominantLanguage dominantLanguage : dominantLanguageList) {
                if (dominantLanguage.languageCode().equals("es")) {
                    containsSpanishLanguageCode = true;
                    break;
                }
            }
            // add english key phrases only to filter list 1
            if (!containsSpanishLanguageCode) {
                keyPhraseFilterList1.add(keyPhrase);
            }
        }
        // return key phrases with schedule stops and times
        return comprehendKeyPhraseListWithStopsAndTimes(keyPhraseFilterList1);
    }

    private List<KeyPhrase> comprehendKeyPhraseListWithStopsAndTimes(List<KeyPhrase> initialKeyPhraseList) {
        List<KeyPhrase> keyPhraseListWithStopsAndTimes = new ArrayList<>();
        String timeRegex = "^.*(?:\\d|[01]\\d|2[0-3]):[0-5]\\d.*$";
        for (KeyPhrase keyPhrase : initialKeyPhraseList) {
            if (keyPhrase.text().contains("#") || keyPhrase.text().matches(timeRegex)) {
                keyPhraseListWithStopsAndTimes.add(keyPhrase);
            }
        }
        return keyPhraseListWithStopsAndTimes;
    }

    /**
     * Reads the pdf content of a file located in {@code /tmp/line_schedule_doc.pdf} and returns the
     * text representation of the contents in readable form.
     *
     * @param logger {@link LambdaLogger}
     * @return {@link String} contents of pdf file in readable form
     */
    private String readPdfFileContent(LambdaLogger logger) throws IOException {
        String parsedText;
        PDDocument doc = null;
        try {
            File pdfDump = new File(LINE_SCHEDULE_PDF_FILE);
            PDFTextStripper pdfTextStripper = new PDFTextStripper();
            doc = PDDocument.load(pdfDump);
            parsedText = pdfTextStripper.getText(doc);
        } catch (IOException e) {
            logger.log(String.format("Error reading pdf file from '%s'" + e.getMessage(), LINE_SCHEDULE_PDF_FILE));
            if (doc != null) {
                doc.close();
            }
            return null;
        }
        doc.close();
        return parsedText;
    }

    /**
     * Queries the line schedule url to obtain the markup document and obtains the line's pdf schedule url.
     *
     * @param lineScheduleUrl {@link String} url to the line's schedule
     * @param line {@link String} the line# name of the MetroLine (ex: 190)
     * @param logger {@link LambdaLogger}
     *
     * @return {@link String} the line's url to its schedule pdf file
     */
    private String queryLineScheduleUrlForPdfScheduleUrl(String lineScheduleUrl, String line, LambdaLogger logger) {
        // dump the schedule url document to /tmp for processing
        try {
            URL url = new URL(lineScheduleUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(ProcessEventUtils.GET_REQUEST);

            ProcessEventUtils.printToFile(connection.getInputStream(), LINE_SCHEDULE_TXT_FILE, logger);
            connection.disconnect();
        } catch (IOException e) {
            logger.log(String.format("Error writing route document dump to '%s': ", LINE_SCHEDULE_TXT_FILE + e.getMessage()));
        }

        // read the dump into a string to begin transforming to pull the schedule pdf url
        String scheduleUrlDocumentDump = null;
        try {
            scheduleUrlDocumentDump = new String(Files.readAllBytes(Paths.get(LINE_SCHEDULE_TXT_FILE)));
        } catch (IOException e) {
            logger.log(String.format("Error reading schedule document dump from: %s: " + e.getMessage(), LINE_SCHEDULE_TXT_FILE));
        }
        if (scheduleUrlDocumentDump != null) {
            return extractPdfUrlFromScheduleDocumentDump(scheduleUrlDocumentDump, line);
        }
        return null;
    }

    /**
     * Extracts the url to a MetroLine's schedule pdf file from the document dump of the line's schedule markup page.
     *
     * @param scheduleDocumentDump {@link String} of the schedule markup document page
     * @param line {@link String} the line# name of the MetroLine (ex: 190)
     *
     * @return {@link String} url to the schedule's pdf document
     */
    private String extractPdfUrlFromScheduleDocumentDump(String scheduleDocumentDump, String line) {
        String nextContent = scheduleDocumentDump
                .split("id=\"pdf-timetable-link\">")[1]
                .trim()
                .split("target=\"_blank\">PDF timetable")[0]
                .trim()
                .replace("'", "")
                .replace(" ", "")
                .split("\\(")[1]
                .split("\\)")[0]
                .replace("+routeName+", line);

        return ProcessEventUtils.METRO_TOP_LEVEL_URL + nextContent;
    }

    /**
     * Gets metro line url from MetroLine {@link JSONObject}
     *
     * @param metroLineObject {@link JSONObject} from MetroLine Properties
     *
     * @return {@link String} the Metro line's url
     * @see MetroLine
     */
    private String getLineScheduleUrl(JSONObject metroLineObject) {
        return metroLineObject.getString("line_schedule_url");
    }

    /**
     * Gets metro line's line number from MetroLine {@link JSONObject}
     *
     * @param metroLineObject {@link JSONObject} from MetroLine Properties
     *
     * @return {@link String} the Metro line's line number
     * @see MetroLine
     */
    private String getLine(JSONObject metroLineObject) {
        return metroLineObject.getString("line");
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
