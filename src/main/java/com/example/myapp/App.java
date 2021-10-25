package com.example.myapp;

// snippet-start:[s3.java2.list_objects.import]
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import java.util.List;
// snippet-end:[s3.java2.list_objects.import]
import software.amazon.awssdk.core.SdkBytes;

// snippet-start:[rekognition.java2.detect_labels.import]
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.rekognition.model.DetectTextRequest;
import software.amazon.awssdk.services.rekognition.model.DetectTextResponse;
import software.amazon.awssdk.services.rekognition.model.TextDetection;
import software.amazon.awssdk.services.rekognition.model.RekognitionException;
// snippet-end:[rekognition.java2.detect_labels.import]

// snippet-start:[sqs.java2.send_recieve_messages.import]
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
// snippet-end:[sqs.java2.send_recieve_messages.import]
import software.amazon.awssdk.services.sqs.model.SqsException;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.*;

/**
 * To run this AWS code example, ensure that you have setup your development environment, including your AWS credentials.
 *
 * For information, see this documentation topic:
 *
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class App {

    public static void main(String[] args) {

        String bucketName = args[0];
        Region region = Region.US_WEST_2;


        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        RekognitionClient rekClient = RekognitionClient.builder()
                .region(region)
                .build();
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_2)
                .build();
        
            String queueUrl = "https://sqs.us-east-2.amazonaws.com/537983741076/fifoQueue.fifo";
                    
            // Receive messages from the queue
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .build();
            
        
            // Print out the messages
            while (true) {
                try{
                    List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
                    if (!messages.isEmpty()){
                        if(messages.get(0).body().equals("-1")){
                            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(messages.get(0).receiptHandle())
                            .build();
                            sqsClient.deleteMessage(deleteRequest);
                            break;
                        }
                        System.out.println("\n" +messages.get(0).body());

                        GetObjectRequest getRequest = GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(messages.get(0).body())
                            .build();
                        ResponseInputStream<GetObjectResponse> sourceImage = s3.getObject(getRequest);
                        detectImageText(rekClient, sourceImage, bucketName);

                        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(messages.get(0).receiptHandle())
                        .build();
                        sqsClient.deleteMessage(deleteRequest);
                    }
                } catch(SqsException e) {
                    System.err.println(e.awsErrorDetails().errorMessage());
                    System.exit(1);
                }

            }

        s3.close();
        rekClient.close();
        sqsClient.close();
    }

    public static void detectImageText(RekognitionClient rekClient, ResponseInputStream<GetObjectResponse> sourceImage, String bucketName) {

        try {
            
            SdkBytes sourceBytes = SdkBytes.fromInputStream(sourceImage);
            
            // Create an Image object for the source image
            Image s3Image = Image.builder()
                    .bytes(sourceBytes)
                    .build();

            
            DetectTextRequest textRequest = DetectTextRequest.builder()
                    .image(s3Image)
                    .build();
            DetectTextResponse textResponse = rekClient.detectText(textRequest);
            List<TextDetection> textCollection = textResponse.textDetections();

            FileWriter out = new FileWriter("output.txt");

            System.out.println("Detected lines and words");
            out.write("Detected lines and words");
            for (TextDetection text: textCollection) {
                if (text.confidence() > 90){
                    System.out.println("Detected: " + text.detectedText());
                    System.out.println("Confidence: " + text.confidence().toString());
                    System.out.println("Id : " + text.id());
                    System.out.println("Parent Id: " + text.parentId());
                    System.out.println("Type: " + text.type());
                    System.out.println();

                    out.write("Detected: " + text.detectedText());
                    out.write("Confidence: " + text.confidence().toString());
                    out.write("Id : " + text.id());
                    out.write("Parent Id: " + text.parentId());
                    out.write("Type: " + text.type());
                    out.write("\n");
                }

            }

            out.close();

        } catch (RekognitionException | IOException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

}