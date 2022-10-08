package org.morris.unofficial.events;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;

public class LoadTransformedMetroDataEvent {

    public String handleRequest(S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log(event.toString());
        logger.log("inside: " + LoadTransformedMetroDataEvent.class);

        return "success";
    }
}
