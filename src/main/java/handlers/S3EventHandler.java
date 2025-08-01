package handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;

public class S3EventHandler implements RequestHandler<S3EventNotification, String> {
    
    private final AmazonSNS snsClient = AmazonSNSClientBuilder.defaultClient();
    
    @Override
    public String handleRequest(S3EventNotification event, Context context) {
        event.getRecords().forEach( data -> {
            String bucket = data.getS3().getBucket().getName();
            String key = data.getS3().getObject().getKey();
            String message = "New file uploaded: " + bucket + "/" + key;
            
            snsClient.publish(new PublishRequest()
                    .withTopicArn(System.getenv("TOPIC_ARN"))
                    .withSubject("S3 Upload Notification")
                    .withMessage(message));
        });
        return "Notification sent.";
    }
}