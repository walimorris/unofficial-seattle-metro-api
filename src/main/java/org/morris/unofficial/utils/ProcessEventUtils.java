package org.morris.unofficial.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.joda.time.DateTime;

public class ProcessEventUtils {
    final static private String REGION = System.getenv("REGION"); // region
    final static private String DEFAULT_REGION = "us-west-2";

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
    public static String getPastPrefix(int days) {
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
}
