package org.morris.unofficial.events;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.DocumentLocation;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionResult;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionResult;
import com.amazonaws.services.textract.model.NotificationChannel;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.morris.unofficial.models.KeyPhraseType;
import org.morris.unofficial.models.MetroLine;
import org.morris.unofficial.utils.ProcessEventUtils;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.ComprehendException;
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProcessCrawledMetroScheduleDataEvent {
    final private static String PROCESSED_BUCKET = System.getenv("PROCESSED_BUCKET_NAME");
    final private static String SCHEDULES_BUCKET = System.getenv("SCHEDULES_BUCKET_NAME");
    final private static String ROUTES_JSON_FILE = "/tmp/routes_doc.json";
    final private static String LINE_SCHEDULE_TXT_FILE = "/tmp/line_schedule_doc.txt";
    final private static String LINE_SCHEDULE_PDF_FILE = "/tmp/line_schedule_doc.pdf";
    final private static String LINE_SCHEDULE_PDF_CONTENT_TXT_FILE = "/tmp/line_schedule_pdf_content.txt";

    final public static String IN_PROGRESS = "IN_PROGRESS";
    final public static String CLIENT_REQUEST_TOKEN = "MetroLineRequestToken";

    public String handleRequest(S3Event event, Context context) throws InterruptedException {
        LambdaLogger logger = context.getLogger();
        AmazonS3 s3Client = ProcessEventUtils.getS3Client();
        AmazonTextract textractClient = ProcessEventUtils.getAmazonTextractClient(true);
        AmazonSNS snsClient = ProcessEventUtils.getAmazonSNSClient();
        AmazonIdentityManagement identityManagmentClient = ProcessEventUtils.getAmazonIdentityManagementClient();

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

                // write pdf schedule to /tmp and put to s3 schedules bucket
                ProcessEventUtils.printToPdfFile(LINE_SCHEDULE_PDF_FILE, lineSchedulePdfUrl, logger);
                ProcessEventUtils.putS3File(LINE_SCHEDULE_PDF_FILE, SCHEDULES_BUCKET, line);
                String pdfScheduleKey = ProcessEventUtils.getSchedulePdfKey(LINE_SCHEDULE_PDF_FILE, line);

                // get schedule pdf textract blocks to begin pulling keyphrases
                List<Block> pdfScheduleTextBlocks = detectPdfTextBlocksWithTextract(textractClient, pdfScheduleKey, logger);
                if (pdfScheduleTextBlocks != null) {
                    for (Block block : pdfScheduleTextBlocks) {
                        logger.log("block---------------------------------------------");
                        logger.log(block.getText());
                        logger.log("end-----------------------------------------------");
                    }
                }

//                String pdfScheduleContent = readPdfFileContent(logger);
//
//                // write schedule content to .txt file in /tmp
//                if (pdfScheduleContent != null) {
//                    InputStream pdfScheduleContentInputStream = new ByteArrayInputStream(pdfScheduleContent.getBytes());
//                    ProcessEventUtils.printToFile(pdfScheduleContentInputStream, LINE_SCHEDULE_PDF_CONTENT_TXT_FILE, logger);
//
//                    List<KeyPhrase> scheduleKeyPhraseList = comprehendKeyPhraseList(pdfScheduleContent, logger);
//                    if (scheduleKeyPhraseList != null) {
//                        // clean key phrases of any extra characters orr improper format
//                        List<KeyPhrase> formattedKeyPhraseList = formatKeyPhraseList(scheduleKeyPhraseList, logger);
//                        if (formattedKeyPhraseList != null) {
//                            // receive list of stops and stop-times to begin building a structure that holds each stop
//                            // and its respective stop times
//                            List<KeyPhrase> formattedStopKeyPhrases = getFormattedStopKeyPhrases(formattedKeyPhraseList, KeyPhraseType.STOPS);
//                            List<KeyPhrase> formattedStopTimeKeyPhrases = getFormattedStopKeyPhrases(formattedKeyPhraseList, KeyPhraseType.STOP_TIMES);
//                        }
//                    }
//                }
            }
        }

        // shutdown all clients
        ProcessEventUtils.shutdownClients(new ArrayList<>(Arrays.asList(s3Client, textractClient,
                snsClient, identityManagmentClient)));

        return "success";
    }

    /**
     * Get List of {@link Block} from the MetroLine Schedule PDF file by utilizing {@link AmazonTextract}
     * machine learning service to analyze the text with the given schedule pdf file. The schedule PDF file
     * is written to /tmp directory and uploaded to S3 where Textract can analyze the text.
     *
     * @param logger {@link LambdaLogger}
     * @return {@link Block}
     *
     * @see MetroLine
     * @see StartDocumentTextDetectionRequest
     * @see GetDocumentTextDetectionRequest
     * @see GetDocumentTextDetectionResult#getBlocks()
     */
    private List<Block> detectPdfTextBlocksWithTextract(AmazonTextract textractClient, String objectKey, LambdaLogger logger) throws InterruptedException {
        String textractTopicArn = ProcessEventUtils.getTextractTopicArn();
        String txfftxyyftArn = ProcessEventUtils.getTXFFTXYYFTRole();

        // send detection to SNS notification channel - topic arn and role arn is needed
        NotificationChannel notificationChannel = new NotificationChannel()
                .withSNSTopicArn(textractTopicArn)
                .withRoleArn(txfftxyyftArn);

        // create document location to feed to document text detection request
        com.amazonaws.services.textract.model.S3Object textractObject = ProcessEventUtils.getTextractS3Object(objectKey, SCHEDULES_BUCKET);
        DocumentLocation documentLocation = new DocumentLocation().withS3Object(textractObject);

        StartDocumentTextDetectionRequest detectDocumentTextRequest = new StartDocumentTextDetectionRequest()
                .withDocumentLocation(documentLocation)
                .withNotificationChannel(notificationChannel)
                .withClientRequestToken(CLIENT_REQUEST_TOKEN);

        StartDocumentTextDetectionResult detectDocumentTextResult = textractClient.startDocumentTextDetection(detectDocumentTextRequest);
        String jobId = detectDocumentTextResult.getJobId();

        GetDocumentTextDetectionRequest getDocumentTextDetectionRequest = new GetDocumentTextDetectionRequest()
                .withJobId(jobId);
        GetDocumentTextDetectionResult getDocumentTextDetectionResult = textractClient.getDocumentTextDetection(getDocumentTextDetectionRequest);

        // wait for succeeded status before getting detection result blocks
        String jobStatus = getDocumentTextDetectionResult.getJobStatus();
        while (jobStatus.equalsIgnoreCase(IN_PROGRESS)) {
            logger.log("Waiting for text detection status---------------------------");
            TimeUnit.SECONDS.sleep(5);
            jobStatus = getDocumentTextDetectionResult.getJobStatus();
        }
        if (!jobStatus.equalsIgnoreCase("Succeeded")) {
            logger.log("Text detected failed with result" + jobStatus);
            return null;
        }
        return getDocumentTextDetectionResult.getBlocks();
    }

    /**
     * Gets a {@link List} of specific key phrases based on the keyPhraseType.
     * stops: gets a list of key phrases based on stop lines and their values.
     * stop-times: gets a list of key phrases based on stop times and their values.
     *
     * @param formattedKeyPhrasesList {@link List} of all formatted KeyPhrases
     * @param type {@link KeyPhraseType}
     * @return {@link List} of KeyPhrases
     *
     * @see KeyPhrase
     * @see MetroLine
     */
    private List<KeyPhrase> getFormattedStopKeyPhrases(List<KeyPhrase> formattedKeyPhrasesList, KeyPhraseType type) {
        List<KeyPhrase> phrases = new ArrayList<>();
        String mark = type.getType().equals("stop-times") ? ":" : null;
        for (KeyPhrase keyPhrase : formattedKeyPhrasesList) {
            if (mark != null) {
                if (keyPhrase.text().contains(mark)) {
                    phrases.add(keyPhrase);
                }
            } else {
                if (!keyPhrase.text().matches("^(?:\\d|[01]\\d|2[0-3]):[0-5]\\d$")) {
                    phrases.add(keyPhrase);
                }
            }
        }
        return phrases;
    }

    /**
     * Formats the generated key phrases from Comprehend NLP client by removing extra symbols, characters,
     * and spaces. This routine helps prepare and transform MetroLine schedule data.
     *
     * @param unformattedKeyPhraseList {@link List} of unformatted {@link KeyPhrase}
     * @param logger {@link LambdaLogger}
     *
     * @return {@link List} of formatted {@link KeyPhrase}
     * @see MetroLine
     * @see ComprehendClient
     */
    private List<KeyPhrase> formatKeyPhraseList(List<KeyPhrase> unformattedKeyPhraseList, LambdaLogger logger) {
        List<KeyPhrase> formattedKeyPhraseList = new ArrayList<>();
        try {
            for (KeyPhrase keyPhrase : unformattedKeyPhraseList) {
                if (keyPhrase.text().contains("#")) {
                    StringBuilder phrase = new StringBuilder();

                    // remove digit symbol, space and any characters after space
                    phrase.append(keyPhrase.text().replace("#", "").trim());
                    if (phrase.toString().contains(" ")) {
                        phrase = new StringBuilder(phrase.toString().split(" ")[0].trim());
                    }

                    keyPhrase = KeyPhrase.builder()
                            .text(phrase.toString())
                            .build();
                    formattedKeyPhraseList.add(keyPhrase);

                } else {
                    if (keyPhrase.text().contains(":")) {
                        // example 12:45 | 5:25
                        if (keyPhrase.text().matches("^(?:\\d|[01]\\d|2[0-3]):[0-5]\\d$")) {
                            formattedKeyPhraseList.add(keyPhrase);
                        } else {
                            StringBuilder phrase = new StringBuilder();
                            // remove any characters after time formatted phrase
                            keyPhrase = KeyPhrase.builder()
                                    .text(keyPhrase.text().split(" ")[0])
                                    .build();

                            String[] phraseParts = keyPhrase.text().split(":");
                            phrase.append(phraseParts[0])
                                    .append(":"); // ex: '5:' or '11:'

                            // only append 2 characters
                            phrase.append(phraseParts[1], 0, 2);
                            keyPhrase = KeyPhrase.builder()
                                    .text(phrase.toString())
                                    .build();
                            formattedKeyPhraseList.add(keyPhrase);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.log("Error formatting list of keyPhrases: " + e.getMessage());
            return null;
        }
        return formattedKeyPhraseList;
    }

    /**
     * Builds a list of KeyPhrases from the text content of a MetroLine schedule that contains the stop#(s)
     * and stop times of each stop in the MetroLine.
     *
     * @param scheduleContent {@link String} of the MetroLines schedule content read from a pdf file
     * @param logger {@link LambdaLogger}
     * @return {@link List} of KeyPhrases from the text
     *
     * @see ProcessCrawledMetroScheduleDataEvent#readPdfFileContent(LambdaLogger)
     * @see MetroLine
     * @see KeyPhrase
     * @see ComprehendClient
     * @see DetectDominantLanguageRequest
     */
    private List<KeyPhrase> comprehendKeyPhraseList(String scheduleContent, LambdaLogger logger) {
        List<KeyPhrase> keyPhraseFilterList1;
        ComprehendClient comprehendClient = null;

        try {
            comprehendClient = ComprehendClient.builder()
                    .region(ProcessEventUtils.getRegionV2())
                    .build();
        } catch (ComprehendException e) {
            logger.log("Error building comprehend client: " + e.getMessage());
        }
        if (comprehendClient != null) {

            // build detect KeyPhrases request
            DetectKeyPhrasesRequest detectKeyPhrasesRequest = DetectKeyPhrasesRequest.builder()
                    .text(scheduleContent)
                    .languageCode("en")
                    .build();

            // detect english key phrase in schedule content
            DetectKeyPhrasesResponse detectKeyPhrasesResponse = comprehendClient.detectKeyPhrases(detectKeyPhrasesRequest);
            List<KeyPhrase> keyPhraseList = detectKeyPhrasesResponse.keyPhrases();
            keyPhraseFilterList1 = new ArrayList<>();

            // analyze each KeyPhrase for dominant language
            for (KeyPhrase keyPhrase : keyPhraseList) {
                DetectDominantLanguageRequest detectDominantLanguageRequest = DetectDominantLanguageRequest.builder()
                        .text(keyPhrase.text())
                        .build();
                DetectDominantLanguageResponse detectDominantLanguageResponse = comprehendClient.detectDominantLanguage(detectDominantLanguageRequest);
                List<DominantLanguage> dominantLanguageList = detectDominantLanguageResponse.languages();

                // remove key phrases with spanish text language code as dominant language
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
            comprehendClient.close();
            // return key phrases with schedule stops and times
            return comprehendKeyPhraseListWithStopsAndTimes(keyPhraseFilterList1, logger);
        }
        return null;
    }

    /**
     * Builds a {@link List} of KeyPhrases of the MetroLine's stops and times using AWS Comprehend
     * service.
     * <p></p>
     * {@code The given regex checks for time format in form 5:25 or 12:06 including leading zeros.}
     * {@code The given regex allows for any number of trailing characters before or after the time format.}
     * {@code Ex: x = any character - x5:25xxx or 5:25x or xxx5:25}
     * <p></p>
     * {@code Regex v1.0: ^.*(?:\\d|[01]\\d|2[0-3]):[0-5]\\d.*$}
     *
     * @param initialKeyPhraseList {@link List} of {@link KeyPhrase}s
     * @return {@link List} of KeyPhrases
     *
     * @see KeyPhrase
     * @see ComprehendClient
     */
    private List<KeyPhrase> comprehendKeyPhraseListWithStopsAndTimes(List<KeyPhrase> initialKeyPhraseList, LambdaLogger logger) {
        List<KeyPhrase> keyPhraseListWithStopsAndTimes = new ArrayList<>();
//        String timeRegex = "^.*(?:\\d|[01]\\d|2[0-3]):[0-5]\\d.*?$";
        for (KeyPhrase keyPhrase : initialKeyPhraseList) {
            if (keyPhrase.text().contains("#") || keyPhrase.text().contains(":")) {
                logger.log("keyPhrase: " + keyPhrase);
                keyPhraseListWithStopsAndTimes.add(keyPhrase);
            } else {
                logger.log("not adding key phrase: " + keyPhrase.text());
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
