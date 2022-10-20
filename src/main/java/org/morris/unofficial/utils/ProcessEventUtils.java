package org.morris.unofficial.utils;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.io.File;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ProcessEventUtils {
    final static public String REGION = System.getenv("REGION"); // region
    final static public String DEFAULT_REGION = "us-west-2";
    final static public String GET_REQUEST = "GET";
    final static public String METRO_TOP_LEVEL_URL = "https://kingcounty.gov";

    /**
     * Get {@link AmazonS3} client
     * @return {@link AmazonS3}
     */
    public static AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withRegion(getRegion())
                .build();
    }

    /**
     * Get region or default to us-west-2
     *
     * @return {@link String} region
     */
    public static String getRegion() {
        return REGION == null ? DEFAULT_REGION : REGION;
    }

    public static Region getRegionV2() {
        return REGION == null ? Region.of(DEFAULT_REGION) : Region.of(REGION);
    }

    /**
     * Creates a prefix for the unprocessed documents in the unprocessed S3 bucket for
     * better query results and future data processing and recording insights.
     *
     * @return {@link String} a prefixed string in form YYYY/DD(D)/MM(M) 2022/8/10 or (2022/12/1)
     */
    public static String getPrefix() {
        String year = String.valueOf(DateTime.now()
                .getYear());

        String month = String.valueOf(DateTime.now()
                .getMonthOfYear());

        String day = String.valueOf(DateTime.now()
                .getDayOfMonth());

        return String.format("docs/%s/%s/%s/", year, month, day);
    }

    /**
     * Creates a prefix date for the unprocessed documents x days prior to the current
     * day in the unprocessed S3 bucket for better query results, x is the number of days.
     *
     * @param days the number of days prior to the current date
     *
     * @return {@link String} a prefixed string date x days prior to the current date in form
     * YYYY/DD(D)/MM(M) 2022/8/10 or (2022/12/1)
     */
    public static String getPrefix(int days) {
        String year = String.valueOf(DateTime.now()
                .minusDays(days)
                .getYear());

        String month = String.valueOf(DateTime.now()
                .minusDays(days)
                .getMonthOfYear());

        String day = String.valueOf(DateTime.now()
                .minusDays(days)
                .getDayOfMonth());

        return String.format("docs/%s/%s/%s/", year, month, day);
    }

    /**
     * Prints the contents of a {@link InputStream} to a file path in /tmp directory.
     *
     * @param inputStream {@link InputStream} content
     * @param strPath {@link String} Path to save content as file
     * @param logger {@link LambdaLogger}
     */
    public static void printToFile(InputStream inputStream, String strPath, LambdaLogger logger) {
        purgeTmpDirectoryFile(strPath, logger);
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
     * Prints the contents of a pdf file, given the files url, to a PDF file in /tmp directory.
     *
     * @param pdfPath {@link String} path to save pdf file in /tmp
     * @param pdfUrl {@link String} the url of the given pdf file
     *
     * @see URL
     * @see InputStream#read(byte[])
     * @see OutputStream#write(int)
     */
    public static void printToPdfFile(String pdfPath, String pdfUrl, LambdaLogger logger) {
        // delete file if it exists
        purgeTmpDirectoryFile(pdfPath, logger);
        try {
            OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(Paths.get(pdfPath)));
            URL metroScheduleUrl = new URL(pdfUrl);
            InputStream in = metroScheduleUrl.openStream();

            int length;
            byte[] buffer = new byte[1024];
            while ((length = in.read(buffer)) > -1) {
                outputStream.write(buffer, 0, length);
            }
            outputStream.close();
            in.close();
            System.out.printf("File written to: %s%n", pdfPath);
        } catch (IOException e) {
            System.out.printf("Error writing route document dump to '%s': " + e.getMessage() + "%n", pdfPath);
        }
    }

    public static StringBuffer readFileAsStringBuffer(String filePath, LambdaLogger logger) throws IOException {
        StringBuffer stringBuffer = null;
        BufferedReader bufferedReader = null;
        try {
            File dump = new File(filePath);
            bufferedReader = new BufferedReader(new InputStreamReader(Files.newInputStream(dump.toPath())));
            stringBuffer = new StringBuffer();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuffer.append(line);
            }
            logger.log(stringBuffer.toString());
            bufferedReader.close();
        } catch (IOException e) {
            logger.log(String.format("Error reading file '%s' as string: " + e.getMessage(), filePath));
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }
        return stringBuffer;
    }

    /**
     * Removes a file within the /tmp directory given the path.
     *
     * @param path path to file in /tmp
     * @param logger {@link LambdaLogger}
     */
    public static void purgeTmpDirectoryFile(String path, LambdaLogger logger) {
        try {
            FileUtils.forceDelete(new File(path));
        } catch (IOException e) {
            logger.log("Error cleaning /tmp directory: " + e.getMessage());
        }
    }
}
